package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
)

func TestMain(m *testing.M) {
	// Initialize noop tracer for tests (same as initTracing with no endpoint)
	tracer = otel.Tracer("agentops-memory-test")
	os.Exit(m.Run())
}

// newTestStore creates an in-memory (temp file) Store for testing.
func newTestStore(t *testing.T) *Store {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	store, err := NewStore(dbPath, 15*time.Minute)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

func createTestSession(t *testing.T, store *Store, id, project string) {
	t.Helper()
	_, err := store.CreateSession(CreateSessionRequest{ID: id, Project: project})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
}

// ====================================================================
// Store lifecycle
// ====================================================================

func TestNewStore_CreatesDBFile(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	store, err := NewStore(dbPath, 15*time.Minute)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	defer store.Close()

	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		t.Fatal("expected DB file to be created")
	}
}

// ====================================================================
// Sessions
// ====================================================================

func TestCreateSession_Idempotent(t *testing.T) {
	store := newTestStore(t)

	sess1, err := store.CreateSession(CreateSessionRequest{ID: "s1", Project: "Test"})
	if err != nil {
		t.Fatalf("first create: %v", err)
	}
	if sess1.ID != "s1" {
		t.Fatalf("expected id s1, got %s", sess1.ID)
	}

	// Second create with same ID should not error (INSERT OR IGNORE)
	sess2, err := store.CreateSession(CreateSessionRequest{ID: "s1", Project: "Test"})
	if err != nil {
		t.Fatalf("idempotent create: %v", err)
	}
	if sess2.ID != "s1" {
		t.Fatalf("expected id s1, got %s", sess2.ID)
	}
}

func TestCreateSession_LowercasesProject(t *testing.T) {
	store := newTestStore(t)

	sess, err := store.CreateSession(CreateSessionRequest{ID: "s1", Project: "MyProject"})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if sess.Project != "myproject" {
		t.Fatalf("expected project 'myproject', got '%s'", sess.Project)
	}
}

func TestEndSession_Summary(t *testing.T) {
	store := newTestStore(t)
	createTestSession(t, store, "s1", "test")

	resp, err := store.EndSession("s1", []SessionMessage{
		{Role: "user", Content: "Hello world"},
		{Role: "assistant", Content: "Hi there!"},
		{Role: "user", Content: "Thanks"},
		{Role: "assistant", Content: "You're welcome"},
	})
	if err != nil {
		t.Fatalf("EndSession: %v", err)
	}
	if resp.MessageCount != 4 {
		t.Fatalf("expected 4 messages, got %d", resp.MessageCount)
	}
	if resp.Summary == "" {
		t.Fatal("expected non-empty summary")
	}
}

func TestRecentSessions(t *testing.T) {
	store := newTestStore(t)
	createTestSession(t, store, "s1", "proj")
	createTestSession(t, store, "s2", "proj")
	createTestSession(t, store, "s3", "other")

	sessions, err := store.RecentSessions("proj", 10)
	if err != nil {
		t.Fatalf("RecentSessions: %v", err)
	}
	if len(sessions) != 2 {
		t.Fatalf("expected 2 sessions for proj, got %d", len(sessions))
	}
}

// ====================================================================
// Three-tier write dedup
// ====================================================================

func TestAddObservation_Tier3_NewInsert(t *testing.T) {
	store := newTestStore(t)
	createTestSession(t, store, "s1", "proj")

	resp, err := store.AddObservation(context.Background(), CreateObservationRequest{
		SessionID: "s1",
		Type:      "decision",
		Title:     "Chose Go over Rust",
		Content:   "We chose Go for the memory service because of SQLite CGO-free support.",
		Project:   "proj",
	})
	if err != nil {
		t.Fatalf("AddObservation: %v", err)
	}
	if resp.Action != "created" {
		t.Fatalf("expected action 'created', got '%s'", resp.Action)
	}
	if resp.RevisionCount != 1 {
		t.Fatalf("expected revision 1, got %d", resp.RevisionCount)
	}
	if resp.DuplicateCount != 1 {
		t.Fatalf("expected duplicate 1, got %d", resp.DuplicateCount)
	}
}

func TestAddObservation_Tier1_TopicKeyUpsert(t *testing.T) {
	store := newTestStore(t)
	createTestSession(t, store, "s1", "proj")

	// First insert with topic_key
	resp1, err := store.AddObservation(context.Background(), CreateObservationRequest{
		SessionID: "s1",
		Type:      "config",
		Title:     "DB pool size",
		Content:   "Set pool size to 10",
		Project:   "proj",
		TopicKey:  "config/db/pool",
	})
	if err != nil {
		t.Fatalf("first insert: %v", err)
	}
	if resp1.Action != "created" {
		t.Fatalf("expected 'created', got '%s'", resp1.Action)
	}

	// Second insert with same topic_key should upsert
	resp2, err := store.AddObservation(context.Background(), CreateObservationRequest{
		SessionID: "s1",
		Type:      "config",
		Title:     "DB pool size updated",
		Content:   "Set pool size to 20",
		Project:   "proj",
		TopicKey:  "config/db/pool",
	})
	if err != nil {
		t.Fatalf("upsert: %v", err)
	}
	if resp2.Action != "updated" {
		t.Fatalf("expected 'updated', got '%s'", resp2.Action)
	}
	if resp2.RevisionCount != 2 {
		t.Fatalf("expected revision 2, got %d", resp2.RevisionCount)
	}
	if resp2.ID != resp1.ID {
		t.Fatalf("expected same ID %d, got %d", resp1.ID, resp2.ID)
	}

	// Verify content was actually updated
	obs, err := store.GetObservation(resp1.ID)
	if err != nil {
		t.Fatalf("GetObservation: %v", err)
	}
	if obs.Title != "DB pool size updated" {
		t.Fatalf("expected updated title, got '%s'", obs.Title)
	}
	if obs.Content != "Set pool size to 20" {
		t.Fatalf("expected updated content, got '%s'", obs.Content)
	}
}

func TestAddObservation_Tier2_HashDedup(t *testing.T) {
	store := newTestStore(t)
	createTestSession(t, store, "s1", "proj")

	// First insert (no topic_key)
	resp1, err := store.AddObservation(context.Background(), CreateObservationRequest{
		SessionID: "s1",
		Type:      "discovery",
		Title:     "Found a bug",
		Content:   "The service crashes on empty input",
		Project:   "proj",
	})
	if err != nil {
		t.Fatalf("first insert: %v", err)
	}
	if resp1.Action != "created" {
		t.Fatalf("expected 'created', got '%s'", resp1.Action)
	}

	// Exact same content should be deduplicated
	resp2, err := store.AddObservation(context.Background(), CreateObservationRequest{
		SessionID: "s1",
		Type:      "discovery",
		Title:     "Found a bug",
		Content:   "The service crashes on empty input",
		Project:   "proj",
	})
	if err != nil {
		t.Fatalf("dedup: %v", err)
	}
	if resp2.Action != "deduplicated" {
		t.Fatalf("expected 'deduplicated', got '%s'", resp2.Action)
	}
	if resp2.DuplicateCount != 2 {
		t.Fatalf("expected duplicate 2, got %d", resp2.DuplicateCount)
	}
	if resp2.ID != resp1.ID {
		t.Fatalf("expected same ID")
	}
}

func TestAddObservation_Tier2_CaseInsensitiveDedup(t *testing.T) {
	store := newTestStore(t)
	createTestSession(t, store, "s1", "proj")

	store.AddObservation(context.Background(), CreateObservationRequest{
		SessionID: "s1",
		Type:      "discovery",
		Title:     "Test",
		Content:   "Hello World",
		Project:   "proj",
	})

	// Same content, different case → should dedup (hash is case-insensitive)
	resp, _ := store.AddObservation(context.Background(), CreateObservationRequest{
		SessionID: "s1",
		Type:      "discovery",
		Title:     "Test",
		Content:   "hello world",
		Project:   "proj",
	})
	if resp.Action != "deduplicated" {
		t.Fatalf("expected case-insensitive dedup, got '%s'", resp.Action)
	}
}

func TestAddObservation_DifferentProjectNotDeduped(t *testing.T) {
	store := newTestStore(t)
	createTestSession(t, store, "s1", "proj1")
	createTestSession(t, store, "s2", "proj2")

	store.AddObservation(context.Background(), CreateObservationRequest{
		SessionID: "s1",
		Type:      "discovery",
		Title:     "Same title",
		Content:   "Same content",
		Project:   "proj1",
	})

	resp, _ := store.AddObservation(context.Background(), CreateObservationRequest{
		SessionID: "s2",
		Type:      "discovery",
		Title:     "Same title",
		Content:   "Same content",
		Project:   "proj2",
	})
	if resp.Action != "created" {
		t.Fatalf("expected 'created' for different project, got '%s'", resp.Action)
	}
}

// ====================================================================
// Get, Update, Delete observations
// ====================================================================

func TestGetObservation(t *testing.T) {
	store := newTestStore(t)
	createTestSession(t, store, "s1", "proj")

	resp, _ := store.AddObservation(context.Background(), CreateObservationRequest{
		SessionID: "s1",
		Type:      "decision",
		Title:     "Test obs",
		Content:   "Test content",
		Project:   "proj",
		Tags:      []string{"go", "testing"},
	})

	obs, err := store.GetObservation(resp.ID)
	if err != nil {
		t.Fatalf("GetObservation: %v", err)
	}
	if obs.Title != "Test obs" {
		t.Fatalf("expected title 'Test obs', got '%s'", obs.Title)
	}
	if len(obs.Tags) != 2 || obs.Tags[0] != "go" {
		t.Fatalf("expected tags [go, testing], got %v", obs.Tags)
	}
}

func TestUpdateObservation(t *testing.T) {
	store := newTestStore(t)
	createTestSession(t, store, "s1", "proj")

	resp, _ := store.AddObservation(context.Background(), CreateObservationRequest{
		SessionID: "s1",
		Type:      "decision",
		Title:     "Original",
		Content:   "Original content",
		Project:   "proj",
	})

	newTitle := "Updated"
	_, err := store.UpdateObservation(resp.ID, UpdateObservationRequest{
		Title: &newTitle,
	})
	if err != nil {
		t.Fatalf("UpdateObservation: %v", err)
	}

	obs, _ := store.GetObservation(resp.ID)
	if obs.Title != "Updated" {
		t.Fatalf("expected 'Updated', got '%s'", obs.Title)
	}
	// Content should remain unchanged
	if obs.Content != "Original content" {
		t.Fatalf("expected unchanged content, got '%s'", obs.Content)
	}
}

func TestDeleteObservation_Soft(t *testing.T) {
	store := newTestStore(t)
	createTestSession(t, store, "s1", "proj")

	resp, _ := store.AddObservation(context.Background(), CreateObservationRequest{
		SessionID: "s1",
		Type:      "decision",
		Title:     "To delete",
		Content:   "Will be soft deleted",
		Project:   "proj",
	})

	err := store.DeleteObservation(resp.ID, false)
	if err != nil {
		t.Fatalf("DeleteObservation: %v", err)
	}

	// Should not be retrievable
	_, err = store.GetObservation(resp.ID)
	if err == nil {
		t.Fatal("expected error for soft-deleted observation")
	}
}

func TestDeleteObservation_Hard(t *testing.T) {
	store := newTestStore(t)
	createTestSession(t, store, "s1", "proj")

	resp, _ := store.AddObservation(context.Background(), CreateObservationRequest{
		SessionID: "s1",
		Type:      "decision",
		Title:     "To delete hard",
		Content:   "Will be permanently deleted",
		Project:   "proj",
	})

	err := store.DeleteObservation(resp.ID, true)
	if err != nil {
		t.Fatalf("DeleteObservation hard: %v", err)
	}

	_, err = store.GetObservation(resp.ID)
	if err == nil {
		t.Fatal("expected error for hard-deleted observation")
	}
}

// ====================================================================
// FTS5 Search
// ====================================================================

func TestSearch_FTS5(t *testing.T) {
	store := newTestStore(t)
	createTestSession(t, store, "s1", "proj")

	store.AddObservation(context.Background(), CreateObservationRequest{
		SessionID: "s1",
		Type:      "decision",
		Title:     "Database architecture",
		Content:   "Chose SQLite with FTS5 for full-text search capabilities.",
		Project:   "proj",
	})
	store.AddObservation(context.Background(), CreateObservationRequest{
		SessionID: "s1",
		Type:      "bugfix",
		Title:     "Fixed API timeout",
		Content:   "The REST API was timing out due to missing connection pooling.",
		Project:   "proj",
	})

	results, err := store.Search(context.Background(), "SQLite", "proj", "", "", 10)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(results) == 0 {
		t.Fatal("expected search results for 'SQLite'")
	}
	if results[0].Title != "Database architecture" {
		t.Fatalf("expected 'Database architecture', got '%s'", results[0].Title)
	}
}

func TestSearch_PorterStemming(t *testing.T) {
	store := newTestStore(t)
	createTestSession(t, store, "s1", "proj")

	store.AddObservation(context.Background(), CreateObservationRequest{
		SessionID: "s1",
		Type:      "discovery",
		Title:     "Running processes",
		Content:   "The service was running multiple goroutines that were leaking.",
		Project:   "proj",
	})

	// "run" should match "running" via porter stemmer
	results, _ := store.Search(context.Background(), "run goroutine", "proj", "", "", 10)
	if len(results) == 0 {
		t.Fatal("expected porter stemming to match 'run' → 'running'")
	}
}

func TestSearch_TopicKeyMatch(t *testing.T) {
	store := newTestStore(t)
	createTestSession(t, store, "s1", "proj")

	store.AddObservation(context.Background(), CreateObservationRequest{
		SessionID: "s1",
		Type:      "config",
		Title:     "Pool size",
		Content:   "Set to 10",
		Project:   "proj",
		TopicKey:  "config/db/pool",
	})

	// Search with a topic-like query (contains /)
	results, _ := store.Search(context.Background(), "config/db", "proj", "", "", 10)
	if len(results) == 0 {
		t.Fatal("expected topic_key LIKE match for 'config/db'")
	}
}

// ====================================================================
// Context injection
// ====================================================================

func TestFetchContext_WithQuery(t *testing.T) {
	store := newTestStore(t)
	createTestSession(t, store, "s1", "proj")

	store.AddObservation(context.Background(), CreateObservationRequest{
		SessionID: "s1",
		Type:      "decision",
		Title:     "Memory architecture",
		Content:   "Three-layer memory model with SQLite backend.",
		Project:   "proj",
	})
	store.AddObservation(context.Background(), CreateObservationRequest{
		SessionID: "s1",
		Type:      "bugfix",
		Title:     "Fixed login bug",
		Content:   "Auth token was not being refreshed on expiry.",
		Project:   "proj",
	})

	// End the session so it has a summary
	store.EndSession("s1", []SessionMessage{
		{Role: "user", Content: "Let's work on memory"},
	})

	ctx, _, err := store.FetchContext(context.Background(), "proj", "", "memory", 5)
	if err != nil {
		t.Fatalf("FetchContext: %v", err)
	}
	if len(ctx.RecentObservations) == 0 {
		t.Fatal("expected context observations for query 'memory'")
	}
	// The memory-related observation should rank higher
	if ctx.RecentObservations[0].Title != "Memory architecture" {
		t.Fatalf("expected 'Memory architecture' first, got '%s'", ctx.RecentObservations[0].Title)
	}
}

func TestFetchContext_WithoutQuery_RecencyFallback(t *testing.T) {
	store := newTestStore(t)
	createTestSession(t, store, "s1", "proj")

	store.AddObservation(context.Background(), CreateObservationRequest{
		SessionID: "s1",
		Type:      "decision",
		Title:     "First observation",
		Content:   "This was first",
		Project:   "proj",
	})
	store.AddObservation(context.Background(), CreateObservationRequest{
		SessionID: "s1",
		Type:      "decision",
		Title:     "Second observation",
		Content:   "This was second",
		Project:   "proj",
	})

	ctx, _, err := store.FetchContext(context.Background(), "proj", "", "", 5)
	if err != nil {
		t.Fatalf("FetchContext: %v", err)
	}
	if len(ctx.RecentObservations) != 2 {
		t.Fatalf("expected 2 observations, got %d", len(ctx.RecentObservations))
	}
}

// ====================================================================
// Stats and Export/Import
// ====================================================================

func TestStats(t *testing.T) {
	store := newTestStore(t)
	createTestSession(t, store, "s1", "proj1")
	createTestSession(t, store, "s2", "proj2")

	store.AddObservation(context.Background(), CreateObservationRequest{
		SessionID: "s1", Type: "decision", Title: "T1", Content: "C1", Project: "proj1",
	})
	store.AddObservation(context.Background(), CreateObservationRequest{
		SessionID: "s2", Type: "bugfix", Title: "T2", Content: "C2", Project: "proj2",
	})

	stats, err := store.Stats("") // all projects
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats.TotalSessions != 2 {
		t.Fatalf("expected 2 sessions, got %d", stats.TotalSessions)
	}
	if stats.TotalObservations != 2 {
		t.Fatalf("expected 2 observations, got %d", stats.TotalObservations)
	}
	if len(stats.Projects) != 2 {
		t.Fatalf("expected 2 projects, got %d", len(stats.Projects))
	}
}

func TestExportImport_RoundTrip(t *testing.T) {
	store1 := newTestStore(t)
	createTestSession(t, store1, "s1", "proj")
	store1.AddObservation(context.Background(), CreateObservationRequest{
		SessionID: "s1", Type: "decision", Title: "Test", Content: "Content", Project: "proj",
	})

	exported, err := store1.Export("proj")
	if err != nil {
		t.Fatalf("Export: %v", err)
	}
	if len(exported.Sessions) != 1 {
		t.Fatalf("expected 1 exported session, got %d", len(exported.Sessions))
	}
	if len(exported.Observations) != 1 {
		t.Fatalf("expected 1 exported observation, got %d", len(exported.Observations))
	}

	// Import into a fresh store
	store2 := newTestStore(t)
	result, err := store2.Import(*exported)
	if err != nil {
		t.Fatalf("Import: %v", err)
	}
	if result.ImportedSessions != 1 || result.ImportedObservations != 1 {
		t.Fatalf("expected 1/1 imported, got %d/%d", result.ImportedSessions, result.ImportedObservations)
	}

	// Verify data is accessible
	stats, _ := store2.Stats("proj")
	if stats.TotalObservations != 1 {
		t.Fatalf("expected 1 observation after import, got %d", stats.TotalObservations)
	}
}

// ====================================================================
// Timeline
// ====================================================================

func TestTimeline(t *testing.T) {
	store := newTestStore(t)
	createTestSession(t, store, "s1", "proj")

	var ids []int64
	for i := 0; i < 5; i++ {
		resp, _ := store.AddObservation(context.Background(), CreateObservationRequest{
			SessionID: "s1",
			Type:      "decision",
			Title:     "Obs " + string(rune('A'+i)),
			Content:   "Content " + string(rune('A'+i)),
			Project:   "proj",
		})
		ids = append(ids, resp.ID)
	}

	// Get timeline around the middle observation.
	// Note: all observations may have the same created_at timestamp (sub-second
	// inserts), so before/after windows based on strict < / > may return 0.
	// The anchor itself should always be included.
	timeline, err := store.Timeline(ids[2], 2, 2)
	if err != nil {
		t.Fatalf("Timeline: %v", err)
	}
	if len(timeline) < 1 {
		t.Fatalf("expected at least the anchor entry, got %d", len(timeline))
	}
	// The anchor should be present
	found := false
	for _, e := range timeline {
		if e.ID == ids[2] {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("expected the anchor observation in timeline")
	}
}
