package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	var cfg Config
	flag.StringVar(&cfg.ListenAddr, "listen", envOr("MEMORY_LISTEN", ":7437"), "HTTP listen address")
	flag.StringVar(&cfg.DBPath, "db", envOr("MEMORY_DB_PATH", "/data/memory.db"), "SQLite database path")
	dedupeStr := flag.String("dedupe-window", envOr("MEMORY_DEDUPE_WINDOW", "15m"), "Dedup window duration")
	flag.Parse()

	var err error
	cfg.DedupeWindow, err = time.ParseDuration(*dedupeStr)
	if err != nil {
		slog.Error("invalid dedupe window", "value", *dedupeStr, "error", err)
		os.Exit(1)
	}

	// Structured logging.
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Initialize tracing.
	tracingFns, err := initTracing(ctx)
	if err != nil {
		slog.Error("failed to initialize tracing", "error", err)
		os.Exit(1)
	}
	defer func() {
		flushCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		tracingFns.ForceFlush(flushCtx)
		tracingFns.Shutdown(flushCtx)
	}()

	// Open database.
	store, err := NewStore(cfg.DBPath, cfg.DedupeWindow)
	if err != nil {
		slog.Error("failed to open database", "path", cfg.DBPath, "error", err)
		os.Exit(1)
	}
	defer store.Close()

	// Start HTTP server.
	server := NewServer(store)
	httpServer := &http.Server{
		Addr:         cfg.ListenAddr,
		Handler:      traceContextMiddleware(server),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		slog.Info("agentops-memory starting",
			"listen", cfg.ListenAddr,
			"db", cfg.DBPath,
			"dedupe_window", cfg.DedupeWindow.String(),
		)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("server error", "error", err)
			os.Exit(1)
		}
	}()

	// Wait for shutdown signal.
	<-ctx.Done()
	slog.Info("shutting down...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		slog.Error("server shutdown error", "error", err)
	}

	slog.Info("agentops-memory stopped")
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func init() {
	// Ensure /data exists for default DB path.
	if err := os.MkdirAll("/data", 0o755); err != nil {
		// Non-fatal: might not have permission, DBPath might be overridden.
		fmt.Fprintf(os.Stderr, "warning: could not create /data: %v\n", err)
	}
}
