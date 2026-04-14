package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

var tracer trace.Tracer

// Span attribute keys for memory operations.
var (
	attrMemoryOp      = attribute.Key("memory.operation")
	attrMemoryProject = attribute.Key("memory.project")
	attrMemoryQuery   = attribute.Key("memory.query")

	// Context injection tracing — the key spans for checking relevancy.
	attrContextResultCount = attribute.Key("memory.context.result_count") // how many injected
	attrContextQueryUsed   = attribute.Key("memory.context.query_used")   // was a query provided?

	// Observation-level injection detail (recorded as span events).
	attrInjectedObsID    = attribute.Key("memory.injected.observation_id")
	attrInjectedObsType  = attribute.Key("memory.injected.type")
	attrInjectedObsTitle = attribute.Key("memory.injected.title")
	attrInjectedObsRank  = attribute.Key("memory.injected.rank")
	attrInjectedMethod   = attribute.Key("memory.injected.method")

	// Search tracing.
	attrSearchQuery       = attribute.Key("memory.search.query")
	attrSearchResultCount = attribute.Key("memory.search.result_count")

	// GenAI retrieval semantic conventions (OTEL GenAI spec).
	attrGenAIOperationName     = attribute.Key("gen_ai.operation.name")
	attrGenAIRetrievalQuery    = attribute.Key("gen_ai.retrieval.query.text")
	attrGenAIRetrievalDocCount = attribute.Key("gen_ai.retrieval.document_count")

	// Observation write tracing.
	attrObsAction = attribute.Key("memory.observation.action") // created, updated, deduplicated
	attrObsType   = attribute.Key("memory.observation.type")
	attrObsID     = attribute.Key("memory.observation.id")

	// Session tracing.
	attrSessionID       = attribute.Key("memory.session.id")
	attrSessionMsgCount = attribute.Key("memory.session.message_count")
)

type tracingFuncs struct {
	ForceFlush func(ctx context.Context) error
	Shutdown   func(ctx context.Context) error
}

func initTracing(ctx context.Context) (*tracingFuncs, error) {
	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if endpoint == "" {
		tracer = otel.Tracer("agentops-memory")
		noop := func(ctx context.Context) error { return nil }
		slog.Info("tracing disabled (OTEL_EXPORTER_OTLP_ENDPOINT not set)")
		return &tracingFuncs{ForceFlush: noop, Shutdown: noop}, nil
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName("agentops-memory"),
			semconv.ServiceVersion("0.1.0"),
		),
		resource.WithProcessRuntimeName(),
		resource.WithHost(),
	)
	if err != nil {
		return nil, fmt.Errorf("create resource: %w", err)
	}

	exporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(endpoint),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("create exporter: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter,
			sdktrace.WithBatchTimeout(2*time.Second),
		),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	tracer = tp.Tracer("agentops-memory")

	slog.Info("tracing enabled", "endpoint", endpoint)
	return &tracingFuncs{
		ForceFlush: tp.ForceFlush,
		Shutdown:   tp.Shutdown,
	}, nil
}

// traceContextMiddleware extracts W3C traceparent from incoming HTTP requests
// and injects the remote span context into the request context. This connects
// memory service spans to the calling agent's trace — the critical link for
// distributed tracing across runtime → memory.
func traceContextMiddleware(next http.Handler) http.Handler {
	propagator := propagation.TraceContext{}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := propagator.Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// recordError sets the span status to Error, records the error as an event,
// and sets error.type per OTel semantic conventions.
func recordError(span trace.Span, err error) {
	span.SetStatus(codes.Error, err.Error())
	span.RecordError(err)
	span.SetAttributes(attribute.String("error.type", memErrorType(err)))
}

// memErrorType classifies an error into a short error.type string.
func memErrorType(err error) string {
	if err == nil {
		return ""
	}
	s := err.Error()
	switch {
	case strings.Contains(s, "not found"):
		return "not_found"
	case strings.Contains(s, "database is locked"):
		return "db_locked"
	case strings.Contains(s, "constraint"):
		return "constraint_violation"
	case strings.Contains(s, "validation"):
		return "validation_error"
	case strings.Contains(s, "invalid JSON"):
		return "invalid_input"
	default:
		return "error"
	}
}
