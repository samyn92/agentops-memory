package main

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// Server is the HTTP handler for the memory service.
type Server struct {
	store *Store
	mux   *http.ServeMux
}

// NewServer creates a new memory HTTP server.
func NewServer(store *Store) *Server {
	s := &Server{store: store, mux: http.NewServeMux()}
	s.routes()
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func (s *Server) routes() {
	s.mux.HandleFunc("GET /health", s.handleHealth)

	// Sessions.
	s.mux.HandleFunc("POST /sessions", s.handleCreateSession)
	s.mux.HandleFunc("POST /sessions/{id}/end", s.handleEndSession)
	s.mux.HandleFunc("GET /sessions/recent", s.handleRecentSessions)

	// Observations.
	s.mux.HandleFunc("POST /observations", s.handleAddObservation)
	s.mux.HandleFunc("GET /observations/recent", s.handleRecentObservations)
	s.mux.HandleFunc("GET /observations/{id}", s.handleGetObservation)
	s.mux.HandleFunc("PATCH /observations/{id}", s.handleUpdateObservation)
	s.mux.HandleFunc("DELETE /observations/{id}", s.handleDeleteObservation)

	// Search.
	s.mux.HandleFunc("GET /search", s.handleSearch)

	// Context.
	s.mux.HandleFunc("GET /context", s.handleContext)

	// Timeline.
	s.mux.HandleFunc("GET /timeline", s.handleTimeline)

	// Stats.
	s.mux.HandleFunc("GET /stats", s.handleStats)

	// Export / Import.
	s.mux.HandleFunc("GET /export", s.handleExport)
	s.mux.HandleFunc("POST /import", s.handleImport)
}

// ---------- Health ----------

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// ---------- Sessions ----------

func (s *Server) handleCreateSession(w http.ResponseWriter, r *http.Request) {
	ctx, span := tracer.Start(r.Context(), "memory.create_session",
		trace.WithAttributes(attrMemoryOp.String("create_session")))
	defer span.End()

	var req CreateSessionRequest
	if err := decodeJSON(r, &req); err != nil {
		span.SetStatus(codes.Error, err.Error())
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	span.SetAttributes(
		attrSessionID.String(req.ID),
		attrMemoryProject.String(req.Project),
	)

	sess, err := s.store.CreateSession(req)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	slog.InfoContext(ctx, "session created", "session_id", req.ID, "project", req.Project)
	writeJSON(w, http.StatusCreated, sess)
}

func (s *Server) handleEndSession(w http.ResponseWriter, r *http.Request) {
	sessionID := r.PathValue("id")
	ctx, span := tracer.Start(r.Context(), "memory.end_session",
		trace.WithAttributes(
			attrMemoryOp.String("end_session"),
			attrSessionID.String(sessionID),
		))
	defer span.End()

	var req EndSessionRequest
	if err := decodeJSON(r, &req); err != nil {
		span.SetStatus(codes.Error, err.Error())
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	span.SetAttributes(attrSessionMsgCount.Int(len(req.Messages)))

	resp, err := s.store.EndSession(sessionID, req.Messages)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	slog.InfoContext(ctx, "session ended", "session_id", sessionID, "messages", len(req.Messages))
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleRecentSessions(w http.ResponseWriter, r *http.Request) {
	project := r.URL.Query().Get("project")
	limit := queryInt(r, "limit", 5)

	_, span := tracer.Start(r.Context(), "memory.recent_sessions",
		trace.WithAttributes(
			attrMemoryOp.String("recent_sessions"),
			attrMemoryProject.String(project),
		))
	defer span.End()

	sessions, err := s.store.RecentSessions(project, limit)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if sessions == nil {
		sessions = []Session{}
	}
	writeJSON(w, http.StatusOK, sessions)
}

// ---------- Observations ----------

func (s *Server) handleAddObservation(w http.ResponseWriter, r *http.Request) {
	ctx, span := tracer.Start(r.Context(), "memory.add_observation",
		trace.WithAttributes(attrMemoryOp.String("add_observation")))
	defer span.End()

	var req CreateObservationRequest
	if err := decodeJSON(r, &req); err != nil {
		span.SetStatus(codes.Error, err.Error())
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	span.SetAttributes(
		attrMemoryProject.String(req.Project),
		attrObsType.String(req.Type),
	)

	resp, err := s.store.AddObservation(req)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	span.SetAttributes(
		attrObsAction.String(resp.Action),
		attrObsID.Int64(resp.ID),
	)

	slog.InfoContext(ctx, "observation saved",
		"id", resp.ID, "action", resp.Action,
		"type", req.Type, "title", req.Title, "project", req.Project)
	writeJSON(w, http.StatusCreated, resp)
}

func (s *Server) handleRecentObservations(w http.ResponseWriter, r *http.Request) {
	project := r.URL.Query().Get("project")
	obsType := r.URL.Query().Get("type")
	scope := r.URL.Query().Get("scope")
	limit := queryInt(r, "limit", 20)

	_, span := tracer.Start(r.Context(), "memory.recent_observations",
		trace.WithAttributes(
			attrMemoryOp.String("recent_observations"),
			attrMemoryProject.String(project),
		))
	defer span.End()

	observations, err := s.store.RecentObservations(project, obsType, scope, limit)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if observations == nil {
		observations = []Observation{}
	}
	writeJSON(w, http.StatusOK, observations)
}

func (s *Server) handleGetObservation(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid observation id")
		return
	}

	_, span := tracer.Start(r.Context(), "memory.get_observation",
		trace.WithAttributes(
			attrMemoryOp.String("get_observation"),
			attrObsID.Int64(id),
		))
	defer span.End()

	obs, err := s.store.GetObservation(id)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		writeError(w, http.StatusNotFound, "observation not found")
		return
	}
	writeJSON(w, http.StatusOK, obs)
}

func (s *Server) handleUpdateObservation(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid observation id")
		return
	}

	ctx, span := tracer.Start(r.Context(), "memory.update_observation",
		trace.WithAttributes(
			attrMemoryOp.String("update_observation"),
			attrObsID.Int64(id),
		))
	defer span.End()

	var req UpdateObservationRequest
	if err := decodeJSON(r, &req); err != nil {
		span.SetStatus(codes.Error, err.Error())
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	obs, err := s.store.UpdateObservation(id, req)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	slog.InfoContext(ctx, "observation updated", "id", id)
	writeJSON(w, http.StatusOK, obs)
}

func (s *Server) handleDeleteObservation(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid observation id")
		return
	}

	hard := r.URL.Query().Get("hard") == "true"

	ctx, span := tracer.Start(r.Context(), "memory.delete_observation",
		trace.WithAttributes(
			attrMemoryOp.String("delete_observation"),
			attrObsID.Int64(id),
			attribute.Bool("memory.observation.hard_delete", hard),
		))
	defer span.End()

	if err := s.store.DeleteObservation(id, hard); err != nil {
		span.SetStatus(codes.Error, err.Error())
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	slog.InfoContext(ctx, "observation deleted", "id", id, "hard", hard)
	w.WriteHeader(http.StatusNoContent)
}

// ---------- Search ----------

func (s *Server) handleSearch(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query().Get("q")
	project := r.URL.Query().Get("project")
	obsType := r.URL.Query().Get("type")
	scope := r.URL.Query().Get("scope")
	limit := queryInt(r, "limit", 10)

	_, span := tracer.Start(r.Context(), "memory.search",
		trace.WithAttributes(
			attrMemoryOp.String("search"),
			attrMemoryProject.String(project),
			attrSearchQuery.String(truncateForAttr(query, 200)),
		))
	defer span.End()

	results, err := s.store.Search(query, project, obsType, scope, limit)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if results == nil {
		results = []SearchResult{}
	}

	span.SetAttributes(attrSearchResultCount.Int(len(results)))

	// Record each result as a span event for detailed search introspection.
	for _, r := range results {
		span.AddEvent("search.result", trace.WithAttributes(
			attrInjectedObsID.Int64(r.ID),
			attrInjectedObsType.String(r.Type),
			attrInjectedObsTitle.String(truncateForAttr(r.Title, 100)),
			attrInjectedObsRank.Float64(r.Rank),
		))
	}

	writeJSON(w, http.StatusOK, results)
}

// ---------- Context (the critical endpoint) ----------

func (s *Server) handleContext(w http.ResponseWriter, r *http.Request) {
	project := r.URL.Query().Get("project")
	scope := r.URL.Query().Get("scope")
	query := r.URL.Query().Get("query")
	limit := queryInt(r, "limit", 5)

	queryUsed := query != ""
	_, span := tracer.Start(r.Context(), "memory.fetch_context",
		trace.WithAttributes(
			attrMemoryOp.String("fetch_context"),
			attrMemoryProject.String(project),
			attrContextQueryUsed.Bool(queryUsed),
		))
	defer span.End()

	if queryUsed {
		span.SetAttributes(attrMemoryQuery.String(truncateForAttr(query, 500)))
	}

	resp, injectionDetails, err := s.store.FetchContext(project, scope, query, limit)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	span.SetAttributes(attrContextResultCount.Int(len(resp.RecentObservations)))

	// Record each injected observation as a span event.
	// This is the critical OTEL data — you can see exactly what the agent gets
	// and whether the ranking was relevant.
	for _, d := range injectionDetails {
		span.AddEvent("context.injected", trace.WithAttributes(
			attrInjectedObsID.Int64(d.ObservationID),
			attrInjectedObsType.String(d.Type),
			attrInjectedObsTitle.String(truncateForAttr(d.Title, 100)),
			attrInjectedObsRank.Float64(d.Rank),
			attrInjectedMethod.String(d.Method),
		))
	}

	// Also log a summary for structured logging (useful without a trace viewer).
	if len(injectionDetails) > 0 {
		methods := map[string]int{}
		for _, d := range injectionDetails {
			methods[d.Method]++
		}
		slog.Info("context fetched",
			"project", project,
			"query_used", queryUsed,
			"results", len(injectionDetails),
			"fts5_hits", methods["fts5_bm25"],
			"recency_hits", methods["recency"],
		)
	}

	if resp.RecentSessions == nil {
		resp.RecentSessions = []ContextSession{}
	}
	if resp.RecentObservations == nil {
		resp.RecentObservations = []ContextObservation{}
	}
	writeJSON(w, http.StatusOK, resp)
}

// ---------- Timeline ----------

func (s *Server) handleTimeline(w http.ResponseWriter, r *http.Request) {
	obsID := queryInt64(r, "observation_id", 0)
	before := queryInt(r, "before", 3)
	after := queryInt(r, "after", 3)

	if obsID == 0 {
		writeError(w, http.StatusBadRequest, "observation_id required")
		return
	}

	_, span := tracer.Start(r.Context(), "memory.timeline",
		trace.WithAttributes(
			attrMemoryOp.String("timeline"),
			attrObsID.Int64(obsID),
		))
	defer span.End()

	entries, err := s.store.Timeline(obsID, before, after)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		if strings.Contains(err.Error(), "not found") {
			writeError(w, http.StatusNotFound, err.Error())
		} else {
			writeError(w, http.StatusInternalServerError, err.Error())
		}
		return
	}
	if entries == nil {
		entries = []TimelineEntry{}
	}
	writeJSON(w, http.StatusOK, entries)
}

// ---------- Stats ----------

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	project := r.URL.Query().Get("project")

	_, span := tracer.Start(r.Context(), "memory.stats",
		trace.WithAttributes(
			attrMemoryOp.String("stats"),
			attrMemoryProject.String(project),
		))
	defer span.End()

	stats, err := s.store.Stats(project)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, stats)
}

// ---------- Export / Import ----------

func (s *Server) handleExport(w http.ResponseWriter, r *http.Request) {
	project := r.URL.Query().Get("project")

	_, span := tracer.Start(r.Context(), "memory.export",
		trace.WithAttributes(
			attrMemoryOp.String("export"),
			attrMemoryProject.String(project),
		))
	defer span.End()

	data, err := s.store.Export(project)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, data)
}

func (s *Server) handleImport(w http.ResponseWriter, r *http.Request) {
	ctx, span := tracer.Start(r.Context(), "memory.import",
		trace.WithAttributes(attrMemoryOp.String("import")))
	defer span.End()

	var data ExportData
	if err := decodeJSON(r, &data); err != nil {
		span.SetStatus(codes.Error, err.Error())
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	result, err := s.store.Import(data)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	slog.InfoContext(ctx, "import completed",
		"sessions", result.ImportedSessions,
		"observations", result.ImportedObservations)
	writeJSON(w, http.StatusOK, result)
}

// ---------- HTTP Helpers ----------

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func decodeJSON(r *http.Request, v any) error {
	defer r.Body.Close()
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("invalid JSON: %w", err)
	}
	return nil
}

func queryInt(r *http.Request, key string, defaultVal int) int {
	s := r.URL.Query().Get(key)
	if s == "" {
		return defaultVal
	}
	v, err := strconv.Atoi(s)
	if err != nil {
		return defaultVal
	}
	return v
}

func queryInt64(r *http.Request, key string, defaultVal int64) int64 {
	s := r.URL.Query().Get(key)
	if s == "" {
		return defaultVal
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return defaultVal
	}
	return v
}

func truncateForAttr(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen]
}
