package main

import (
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

// Store wraps the SQLite database with all memory operations.
type Store struct {
	db           *sql.DB
	dedupeWindow time.Duration
}

// NewStore opens (or creates) the SQLite database and runs migrations.
func NewStore(dbPath string, dedupeWindow time.Duration) (*Store, error) {
	db, err := sql.Open("sqlite", dbPath+"?_journal_mode=WAL&_foreign_keys=ON&_busy_timeout=5000")
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}
	// Single writer, multiple readers.
	db.SetMaxOpenConns(1)

	s := &Store{db: db, dedupeWindow: dedupeWindow}
	if err := s.migrate(); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}
	if err := s.initFTS(); err != nil {
		db.Close()
		return nil, fmt.Errorf("init FTS5: %w", err)
	}
	return s, nil
}

// Close shuts down the database.
func (s *Store) Close() error { return s.db.Close() }

// ---------- Schema ----------

func (s *Store) migrate() error {
	_, err := s.db.Exec(schema)
	return err
}

const schema = `
CREATE TABLE IF NOT EXISTS sessions (
    id            TEXT PRIMARY KEY,
    project       TEXT NOT NULL,
    started_at    TEXT NOT NULL DEFAULT (datetime('now')),
    ended_at      TEXT,
    summary       TEXT,
    message_count INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_sessions_project ON sessions(project);

CREATE TABLE IF NOT EXISTS observations (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id      TEXT NOT NULL REFERENCES sessions(id),
    type            TEXT NOT NULL,
    title           TEXT NOT NULL,
    content         TEXT NOT NULL,
    tags            TEXT,
    project         TEXT NOT NULL,
    scope           TEXT NOT NULL DEFAULT 'project',
    topic_key       TEXT,
    normalized_hash TEXT,
    revision_count  INTEGER NOT NULL DEFAULT 1,
    duplicate_count INTEGER NOT NULL DEFAULT 1,
    last_seen_at    TEXT NOT NULL DEFAULT (datetime('now')),
    promoted_to     TEXT,
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at      TEXT NOT NULL DEFAULT (datetime('now')),
    deleted_at      TEXT
);
CREATE INDEX IF NOT EXISTS idx_obs_project   ON observations(project)         WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_obs_session   ON observations(session_id);
CREATE INDEX IF NOT EXISTS idx_obs_type      ON observations(type)            WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_obs_topic     ON observations(topic_key)       WHERE topic_key IS NOT NULL AND deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_obs_hash      ON observations(normalized_hash) WHERE deleted_at IS NULL;
`

// FTS5 virtual table cannot use IF NOT EXISTS, so we create it separately.
func init() {
	// Appended after main schema via initFTS.
}

func (s *Store) initFTS() error {
	// Check if FTS table already exists.
	var name string
	err := s.db.QueryRow(`SELECT name FROM sqlite_master WHERE type='table' AND name='observations_fts'`).Scan(&name)
	if err == nil {
		return nil // already exists
	}

	fts := `
CREATE VIRTUAL TABLE observations_fts USING fts5(
    title, content, type, project, topic_key,
    content=observations, content_rowid=id,
    tokenize='porter unicode61'
);

-- Sync triggers
CREATE TRIGGER observations_ai AFTER INSERT ON observations BEGIN
    INSERT INTO observations_fts(rowid, title, content, type, project, topic_key)
    VALUES (new.id, new.title, new.content, new.type, new.project, new.topic_key);
END;
CREATE TRIGGER observations_ad AFTER DELETE ON observations BEGIN
    INSERT INTO observations_fts(observations_fts, rowid, title, content, type, project, topic_key)
    VALUES ('delete', old.id, old.title, old.content, old.type, old.project, old.topic_key);
END;
CREATE TRIGGER observations_au AFTER UPDATE ON observations BEGIN
    INSERT INTO observations_fts(observations_fts, rowid, title, content, type, project, topic_key)
    VALUES ('delete', old.id, old.title, old.content, old.type, old.project, old.topic_key);
    INSERT INTO observations_fts(rowid, title, content, type, project, topic_key)
    VALUES (new.id, new.title, new.content, new.type, new.project, new.topic_key);
END;

-- Rebuild index from existing data (idempotent on empty table).
INSERT INTO observations_fts(observations_fts) VALUES('rebuild');
`
	_, err = s.db.Exec(fts)
	return err
}

// ---------- Sessions ----------

func (s *Store) CreateSession(req CreateSessionRequest) (*Session, error) {
	now := time.Now().UTC().Format(time.RFC3339)
	project := strings.ToLower(req.Project)
	_, err := s.db.Exec(
		`INSERT OR IGNORE INTO sessions (id, project, started_at) VALUES (?, ?, ?)`,
		req.ID, project, now,
	)
	if err != nil {
		return nil, err
	}
	return &Session{ID: req.ID, Project: project, StartedAt: now}, nil
}

func (s *Store) EndSession(sessionID string, messages []SessionMessage) (*EndSessionResponse, error) {
	now := time.Now().UTC().Format(time.RFC3339)
	summary := buildSessionSummary(messages)

	_, err := s.db.Exec(
		`UPDATE sessions SET ended_at = ?, summary = ?, message_count = ? WHERE id = ?`,
		now, summary, len(messages), sessionID,
	)
	if err != nil {
		return nil, err
	}
	return &EndSessionResponse{
		SessionID:    sessionID,
		Summary:      summary,
		MessageCount: len(messages),
	}, nil
}

func (s *Store) RecentSessions(project string, limit int) ([]Session, error) {
	if limit <= 0 {
		limit = 5
	}
	rows, err := s.db.Query(
		`SELECT id, project, started_at, ended_at, summary, message_count
		 FROM sessions WHERE project = ? ORDER BY started_at DESC LIMIT ?`,
		project, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var sessions []Session
	for rows.Next() {
		var sess Session
		if err := rows.Scan(&sess.ID, &sess.Project, &sess.StartedAt, &sess.EndedAt, &sess.Summary, &sess.MessageCount); err != nil {
			return nil, err
		}
		sessions = append(sessions, sess)
	}
	return sessions, rows.Err()
}

// buildSessionSummary creates a deterministic summary from conversation messages.
// No LLM call — extracts structure from the conversation itself.
func buildSessionSummary(messages []SessionMessage) string {
	if len(messages) == 0 {
		return ""
	}

	var userMsgs []string
	var assistantMsgs int
	for _, m := range messages {
		switch m.Role {
		case "user":
			userMsgs = append(userMsgs, m.Content)
		case "assistant":
			assistantMsgs++
		}
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Session: %d messages (%d user, %d assistant). ", len(messages), len(userMsgs), assistantMsgs))

	// First user message captures the opening intent.
	if len(userMsgs) > 0 {
		first := truncate(userMsgs[0], 200)
		sb.WriteString(fmt.Sprintf("Started with: %s", first))
	}
	// Last user message captures the final topic.
	if len(userMsgs) > 1 {
		last := truncate(userMsgs[len(userMsgs)-1], 200)
		sb.WriteString(fmt.Sprintf(" Ended with: %s", last))
	}
	return sb.String()
}

// ---------- Observations ----------

// AddObservation implements three-tier write: topic_key upsert → hash dedup → new insert.
func (s *Store) AddObservation(req CreateObservationRequest) (*CreateObservationResponse, error) {
	scope := req.Scope
	if scope == "" {
		scope = "project"
	}
	project := strings.ToLower(req.Project)
	hash := normalizeHash(req.Content)

	tagsJSON, _ := json.Marshal(req.Tags)
	if req.Tags == nil {
		tagsJSON = nil
	}

	now := time.Now().UTC().Format(time.RFC3339)

	// Tier 1: topic_key upsert.
	if req.TopicKey != "" {
		var existingID int64
		err := s.db.QueryRow(
			`SELECT id FROM observations
			 WHERE topic_key = ? AND project = ? AND scope = ? AND deleted_at IS NULL
			 ORDER BY created_at DESC LIMIT 1`,
			req.TopicKey, project, scope,
		).Scan(&existingID)
		if err == nil {
			// Upsert: update in place.
			_, err = s.db.Exec(
				`UPDATE observations SET
				    type = ?, title = ?, content = ?, tags = ?,
				    normalized_hash = ?, revision_count = revision_count + 1,
				    last_seen_at = ?, updated_at = ?
				 WHERE id = ?`,
				req.Type, req.Title, req.Content, tagsJSON,
				hash, now, now, existingID,
			)
			if err != nil {
				return nil, err
			}
			var rev int
			s.db.QueryRow(`SELECT revision_count FROM observations WHERE id = ?`, existingID).Scan(&rev)
			return &CreateObservationResponse{ID: existingID, Action: "updated", RevisionCount: rev, DuplicateCount: 1}, nil
		}
	}

	// Tier 2: hash-based dedup within window.
	windowExpr := fmt.Sprintf("-%d seconds", int(s.dedupeWindow.Seconds()))
	var existingID int64
	err := s.db.QueryRow(
		`SELECT id FROM observations
		 WHERE normalized_hash = ? AND project = ? AND scope = ? AND type = ? AND title = ?
		   AND deleted_at IS NULL
		   AND created_at >= datetime('now', ?)
		 ORDER BY created_at DESC LIMIT 1`,
		hash, project, scope, req.Type, req.Title, windowExpr,
	).Scan(&existingID)
	if err == nil {
		// Dedup: just bump count.
		_, err = s.db.Exec(
			`UPDATE observations SET duplicate_count = duplicate_count + 1, last_seen_at = ? WHERE id = ?`,
			now, existingID,
		)
		if err != nil {
			return nil, err
		}
		var dup int
		s.db.QueryRow(`SELECT duplicate_count FROM observations WHERE id = ?`, existingID).Scan(&dup)
		return &CreateObservationResponse{ID: existingID, Action: "deduplicated", RevisionCount: 1, DuplicateCount: dup}, nil
	}

	// Tier 3: new insert.
	result, err := s.db.Exec(
		`INSERT INTO observations (session_id, type, title, content, tags, project, scope, topic_key, normalized_hash, last_seen_at, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		req.SessionID, req.Type, req.Title, req.Content, tagsJSON,
		project, scope, nilIfEmpty(req.TopicKey), hash, now, now, now,
	)
	if err != nil {
		return nil, err
	}
	id, _ := result.LastInsertId()
	return &CreateObservationResponse{ID: id, Action: "created", RevisionCount: 1, DuplicateCount: 1}, nil
}

func (s *Store) GetObservation(id int64) (*Observation, error) {
	return s.scanObservation(
		`SELECT id, session_id, type, title, content, tags, project, scope, topic_key,
		        normalized_hash, revision_count, duplicate_count, last_seen_at,
		        promoted_to, created_at, updated_at
		 FROM observations WHERE id = ? AND deleted_at IS NULL`, id,
	)
}

func (s *Store) RecentObservations(project, obsType, scope string, limit int) ([]Observation, error) {
	if limit <= 0 {
		limit = 20
	}

	query := `SELECT id, session_id, type, title, content, tags, project, scope, topic_key,
	                 normalized_hash, revision_count, duplicate_count, last_seen_at,
	                 promoted_to, created_at, updated_at
	          FROM observations WHERE project = ? AND deleted_at IS NULL`
	args := []any{project}

	if obsType != "" {
		query += ` AND type = ?`
		args = append(args, obsType)
	}
	if scope != "" {
		query += ` AND scope = ?`
		args = append(args, scope)
	}
	query += ` ORDER BY created_at DESC LIMIT ?`
	args = append(args, limit)

	return s.scanObservations(query, args...)
}

func (s *Store) UpdateObservation(id int64, req UpdateObservationRequest) (*Observation, error) {
	now := time.Now().UTC().Format(time.RFC3339)

	// Build dynamic SET clause from non-nil fields.
	sets := []string{"updated_at = ?"}
	args := []any{now}

	if req.Type != nil {
		sets = append(sets, "type = ?")
		args = append(args, *req.Type)
	}
	if req.Title != nil {
		sets = append(sets, "title = ?")
		args = append(args, *req.Title)
	}
	if req.Content != nil {
		sets = append(sets, "content = ?", "normalized_hash = ?")
		args = append(args, *req.Content, normalizeHash(*req.Content))
	}
	if req.Tags != nil {
		tagsJSON, _ := json.Marshal(req.Tags)
		sets = append(sets, "tags = ?")
		args = append(args, tagsJSON)
	}

	args = append(args, id)
	_, err := s.db.Exec(
		fmt.Sprintf(`UPDATE observations SET %s WHERE id = ? AND deleted_at IS NULL`, strings.Join(sets, ", ")),
		args...,
	)
	if err != nil {
		return nil, err
	}
	return s.GetObservation(id)
}

func (s *Store) DeleteObservation(id int64, hard bool) error {
	if hard {
		_, err := s.db.Exec(`DELETE FROM observations WHERE id = ?`, id)
		return err
	}
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := s.db.Exec(`UPDATE observations SET deleted_at = ? WHERE id = ?`, now, id)
	return err
}

// ---------- Search (FTS5 BM25) ----------

// Search performs FTS5 full-text search with BM25 relevance ranking.
// Returns results ordered by relevance (lower rank = more relevant).
func (s *Store) Search(query, project, obsType, scope string, limit int) ([]SearchResult, error) {
	if limit <= 0 {
		limit = 10
	}
	sanitized := sanitizeFTS(query)
	if sanitized == "" {
		return nil, nil
	}

	var results []SearchResult
	seen := make(map[int64]bool)

	// Phase 1: direct topic_key match (if query contains a slash — likely a topic path).
	if strings.Contains(query, "/") {
		rows, err := s.db.Query(
			`SELECT id, type, title, content, topic_key FROM observations
			 WHERE topic_key LIKE ? AND project = ? AND deleted_at IS NULL
			 ORDER BY updated_at DESC LIMIT ?`,
			"%"+query+"%", project, limit,
		)
		if err == nil {
			defer rows.Close()
			for rows.Next() {
				var r SearchResult
				var tk sql.NullString
				if err := rows.Scan(&r.ID, &r.Type, &r.Title, &r.Content, &tk); err == nil {
					r.Rank = -1000 // top priority
					r.TopicKey = tk.String
					results = append(results, r)
					seen[r.ID] = true
				}
			}
		}
	}

	// Phase 2: FTS5 MATCH with BM25 ranking.
	ftsQuery := `SELECT o.id, o.type, o.title, o.content, o.topic_key, bm25(observations_fts) as rank
	             FROM observations_fts fts
	             JOIN observations o ON o.id = fts.rowid
	             WHERE observations_fts MATCH ? AND o.deleted_at IS NULL`
	ftsArgs := []any{sanitized}

	if project != "" {
		ftsQuery += ` AND o.project = ?`
		ftsArgs = append(ftsArgs, project)
	}
	if obsType != "" {
		ftsQuery += ` AND o.type = ?`
		ftsArgs = append(ftsArgs, obsType)
	}
	if scope != "" {
		ftsQuery += ` AND o.scope = ?`
		ftsArgs = append(ftsArgs, scope)
	}
	ftsQuery += ` ORDER BY rank LIMIT ?`
	ftsArgs = append(ftsArgs, limit)

	rows, err := s.db.Query(ftsQuery, ftsArgs...)
	if err != nil {
		return results, err // return topic_key results even if FTS fails
	}
	defer rows.Close()

	for rows.Next() {
		var r SearchResult
		var tk sql.NullString
		if err := rows.Scan(&r.ID, &r.Type, &r.Title, &r.Content, &tk, &r.Rank); err != nil {
			continue
		}
		if seen[r.ID] {
			continue
		}
		r.TopicKey = tk.String
		results = append(results, r)
		if len(results) >= limit {
			break
		}
	}
	return results, rows.Err()
}

// ---------- Context (the critical endpoint — relevance-aware) ----------

// FetchContext returns observations and session summaries for injection into the agent prompt.
// When query is non-empty, uses FTS5 BM25 for relevance ranking. Otherwise falls back to recency.
func (s *Store) FetchContext(project, scope, query string, limit int) (*ContextResponse, []ContextInjectionDetail, error) {
	if limit <= 0 {
		limit = 5
	}

	resp := &ContextResponse{}
	var injectionDetails []ContextInjectionDetail

	// 1. Recent session summaries (always recency-based).
	sessRows, err := s.db.Query(
		`SELECT summary FROM sessions
		 WHERE project = ? AND summary IS NOT NULL AND summary != ''
		 ORDER BY started_at DESC LIMIT 3`,
		project,
	)
	if err == nil {
		defer sessRows.Close()
		for sessRows.Next() {
			var summary string
			if err := sessRows.Scan(&summary); err == nil {
				resp.RecentSessions = append(resp.RecentSessions, ContextSession{Summary: summary})
			}
		}
	}

	// 2. Observations — relevance-ranked if query provided, otherwise recency.
	if query != "" {
		sanitized := sanitizeFTS(query)
		if sanitized != "" {
			obsQuery := `SELECT o.id, o.type, o.title, o.content, bm25(observations_fts) as rank
			             FROM observations_fts fts
			             JOIN observations o ON o.id = fts.rowid
			             WHERE observations_fts MATCH ? AND o.project = ? AND o.deleted_at IS NULL`
			obsArgs := []any{sanitized, project}
			if scope != "" {
				obsQuery += ` AND o.scope = ?`
				obsArgs = append(obsArgs, scope)
			}
			obsQuery += ` ORDER BY rank LIMIT ?`
			obsArgs = append(obsArgs, limit)

			rows, err := s.db.Query(obsQuery, obsArgs...)
			if err == nil {
				defer rows.Close()
				for rows.Next() {
					var id int64
					var obsType, title, content string
					var rank float64
					if err := rows.Scan(&id, &obsType, &title, &content, &rank); err == nil {
						resp.RecentObservations = append(resp.RecentObservations, ContextObservation{
							Type:    obsType,
							Title:   title,
							Content: truncate(content, 300),
						})
						injectionDetails = append(injectionDetails, ContextInjectionDetail{
							ObservationID: id,
							Type:          obsType,
							Title:         title,
							Rank:          rank,
							Method:        "fts5_bm25",
						})
					}
				}
			}

			// Backfill with recent if FTS returned fewer than limit.
			if len(resp.RecentObservations) < limit {
				backfillIDs := make(map[int64]bool)
				for _, d := range injectionDetails {
					backfillIDs[d.ObservationID] = true
				}
				remaining := limit - len(resp.RecentObservations)
				s.backfillRecent(project, scope, remaining, backfillIDs, resp, &injectionDetails)
			}
		}
	}

	// Pure recency fallback (no query provided or empty sanitized query).
	if len(resp.RecentObservations) == 0 {
		s.backfillRecent(project, scope, limit, nil, resp, &injectionDetails)
	}

	return resp, injectionDetails, nil
}

// backfillRecent adds recent observations not already in the result set.
func (s *Store) backfillRecent(project, scope string, limit int, exclude map[int64]bool, resp *ContextResponse, details *[]ContextInjectionDetail) {
	query := `SELECT id, type, title, content FROM observations
	          WHERE project = ? AND deleted_at IS NULL`
	args := []any{project}
	if scope != "" {
		query += ` AND scope = ?`
		args = append(args, scope)
	}
	query += ` ORDER BY created_at DESC LIMIT ?`
	args = append(args, limit+len(exclude)) // over-fetch to account for exclusions

	rows, err := s.db.Query(query, args...)
	if err != nil {
		return
	}
	defer rows.Close()

	added := 0
	for rows.Next() && added < limit {
		var id int64
		var obsType, title, content string
		if err := rows.Scan(&id, &obsType, &title, &content); err != nil {
			continue
		}
		if exclude != nil && exclude[id] {
			continue
		}
		resp.RecentObservations = append(resp.RecentObservations, ContextObservation{
			Type:    obsType,
			Title:   title,
			Content: truncate(content, 300),
		})
		*details = append(*details, ContextInjectionDetail{
			ObservationID: id,
			Type:          obsType,
			Title:         title,
			Rank:          0, // no rank for recency
			Method:        "recency",
		})
		added++
	}
}

// ContextInjectionDetail captures what was injected and why — for OTEL spans.
type ContextInjectionDetail struct {
	ObservationID int64   `json:"observation_id"`
	Type          string  `json:"type"`
	Title         string  `json:"title"`
	Rank          float64 `json:"rank"`
	Method        string  `json:"method"` // fts5_bm25 or recency
}

// ---------- Timeline ----------

func (s *Store) Timeline(observationID int64, before, after int) ([]TimelineEntry, error) {
	if before <= 0 {
		before = 3
	}
	if after <= 0 {
		after = 3
	}

	// Get the anchor observation's created_at and project.
	var anchorTime, project string
	err := s.db.QueryRow(
		`SELECT created_at, project FROM observations WHERE id = ? AND deleted_at IS NULL`,
		observationID,
	).Scan(&anchorTime, &project)
	if err != nil {
		return nil, fmt.Errorf("observation %d not found: %w", observationID, err)
	}

	// Before (older).
	beforeRows, err := s.db.Query(
		`SELECT id, type, title, content, created_at FROM observations
		 WHERE project = ? AND created_at < ? AND deleted_at IS NULL
		 ORDER BY created_at DESC LIMIT ?`,
		project, anchorTime, before,
	)
	if err != nil {
		return nil, err
	}
	defer beforeRows.Close()

	var entries []TimelineEntry
	var beforeEntries []TimelineEntry
	for beforeRows.Next() {
		var e TimelineEntry
		if err := beforeRows.Scan(&e.ID, &e.Type, &e.Title, &e.Content, &e.CreatedAt); err == nil {
			beforeEntries = append(beforeEntries, e)
		}
	}
	// Reverse to get chronological order.
	for i := len(beforeEntries) - 1; i >= 0; i-- {
		entries = append(entries, beforeEntries[i])
	}

	// The anchor itself.
	var anchor TimelineEntry
	s.db.QueryRow(
		`SELECT id, type, title, content, created_at FROM observations WHERE id = ?`,
		observationID,
	).Scan(&anchor.ID, &anchor.Type, &anchor.Title, &anchor.Content, &anchor.CreatedAt)
	entries = append(entries, anchor)

	// After (newer).
	afterRows, err := s.db.Query(
		`SELECT id, type, title, content, created_at FROM observations
		 WHERE project = ? AND created_at > ? AND deleted_at IS NULL
		 ORDER BY created_at ASC LIMIT ?`,
		project, anchorTime, after,
	)
	if err != nil {
		return nil, err
	}
	defer afterRows.Close()
	for afterRows.Next() {
		var e TimelineEntry
		if err := afterRows.Scan(&e.ID, &e.Type, &e.Title, &e.Content, &e.CreatedAt); err == nil {
			entries = append(entries, e)
		}
	}
	return entries, nil
}

// ---------- Stats ----------

func (s *Store) Stats(project string) (*Stats, error) {
	stats := &Stats{}

	if project != "" {
		s.db.QueryRow(`SELECT COUNT(*) FROM sessions WHERE project = ?`, project).Scan(&stats.TotalSessions)
		s.db.QueryRow(`SELECT COUNT(*) FROM observations WHERE project = ? AND deleted_at IS NULL`, project).Scan(&stats.TotalObservations)
	} else {
		s.db.QueryRow(`SELECT COUNT(*) FROM sessions`).Scan(&stats.TotalSessions)
		s.db.QueryRow(`SELECT COUNT(*) FROM observations WHERE deleted_at IS NULL`).Scan(&stats.TotalObservations)
	}

	rows, err := s.db.Query(`SELECT DISTINCT project FROM sessions ORDER BY project`)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var p string
			if err := rows.Scan(&p); err == nil {
				stats.Projects = append(stats.Projects, p)
			}
		}
	}
	if stats.Projects == nil {
		stats.Projects = []string{}
	}
	return stats, nil
}

// ---------- Export / Import ----------

func (s *Store) Export(project string) (*ExportData, error) {
	data := &ExportData{
		ExportedAt: time.Now().UTC().Format(time.RFC3339),
	}

	// Sessions.
	sessQuery := `SELECT id, project, started_at, ended_at, summary, message_count FROM sessions`
	sessArgs := []any{}
	if project != "" {
		sessQuery += ` WHERE project = ?`
		sessArgs = append(sessArgs, project)
	}
	sessQuery += ` ORDER BY started_at`

	rows, err := s.db.Query(sessQuery, sessArgs...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var sess Session
		if err := rows.Scan(&sess.ID, &sess.Project, &sess.StartedAt, &sess.EndedAt, &sess.Summary, &sess.MessageCount); err == nil {
			data.Sessions = append(data.Sessions, sess)
		}
	}

	// Observations.
	obsQuery := `SELECT id, session_id, type, title, content, tags, project, scope, topic_key,
	                    normalized_hash, revision_count, duplicate_count, last_seen_at,
	                    promoted_to, created_at, updated_at
	             FROM observations WHERE deleted_at IS NULL`
	obsArgs := []any{}
	if project != "" {
		obsQuery += ` AND project = ?`
		obsArgs = append(obsArgs, project)
	}
	obsQuery += ` ORDER BY created_at`

	observations, err := s.scanObservations(obsQuery, obsArgs...)
	if err != nil {
		return nil, err
	}
	data.Observations = observations
	if data.Sessions == nil {
		data.Sessions = []Session{}
	}
	if data.Observations == nil {
		data.Observations = []Observation{}
	}
	return data, nil
}

func (s *Store) Import(data ExportData) (*ImportResult, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	result := &ImportResult{}

	for _, sess := range data.Sessions {
		_, err := tx.Exec(
			`INSERT OR IGNORE INTO sessions (id, project, started_at, ended_at, summary, message_count)
			 VALUES (?, ?, ?, ?, ?, ?)`,
			sess.ID, sess.Project, sess.StartedAt, sess.EndedAt, sess.Summary, sess.MessageCount,
		)
		if err != nil {
			slog.Warn("import session failed", "id", sess.ID, "error", err)
			continue
		}
		result.ImportedSessions++
	}

	for _, obs := range data.Observations {
		tagsJSON, _ := json.Marshal(obs.Tags)
		if obs.Tags == nil {
			tagsJSON = nil
		}
		_, err := tx.Exec(
			`INSERT INTO observations (session_id, type, title, content, tags, project, scope,
			    topic_key, normalized_hash, revision_count, duplicate_count, last_seen_at,
			    promoted_to, created_at, updated_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			obs.SessionID, obs.Type, obs.Title, obs.Content, tagsJSON,
			obs.Project, obs.Scope, nilIfEmpty(obs.TopicKey), obs.NormalizedHash,
			obs.RevisionCount, obs.DuplicateCount, obs.LastSeenAt,
			nilIfEmpty(obs.PromotedTo), obs.CreatedAt, obs.UpdatedAt,
		)
		if err != nil {
			slog.Warn("import observation failed", "title", obs.Title, "error", err)
			continue
		}
		result.ImportedObservations++
	}

	return result, tx.Commit()
}

// ---------- Helpers ----------

func (s *Store) scanObservation(query string, args ...any) (*Observation, error) {
	var obs Observation
	var tagsJSON, topicKey, hash, lastSeen, promotedTo sql.NullString
	err := s.db.QueryRow(query, args...).Scan(
		&obs.ID, &obs.SessionID, &obs.Type, &obs.Title, &obs.Content, &tagsJSON,
		&obs.Project, &obs.Scope, &topicKey, &hash,
		&obs.RevisionCount, &obs.DuplicateCount, &lastSeen,
		&promotedTo, &obs.CreatedAt, &obs.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	obs.TopicKey = topicKey.String
	obs.NormalizedHash = hash.String
	obs.LastSeenAt = lastSeen.String
	obs.PromotedTo = promotedTo.String
	if tagsJSON.Valid {
		json.Unmarshal([]byte(tagsJSON.String), &obs.Tags)
	}
	return &obs, nil
}

func (s *Store) scanObservations(query string, args ...any) ([]Observation, error) {
	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var observations []Observation
	for rows.Next() {
		var obs Observation
		var tagsJSON, topicKey, hash, lastSeen, promotedTo sql.NullString
		if err := rows.Scan(
			&obs.ID, &obs.SessionID, &obs.Type, &obs.Title, &obs.Content, &tagsJSON,
			&obs.Project, &obs.Scope, &topicKey, &hash,
			&obs.RevisionCount, &obs.DuplicateCount, &lastSeen,
			&promotedTo, &obs.CreatedAt, &obs.UpdatedAt,
		); err != nil {
			return nil, err
		}
		obs.TopicKey = topicKey.String
		obs.NormalizedHash = hash.String
		obs.LastSeenAt = lastSeen.String
		obs.PromotedTo = promotedTo.String
		if tagsJSON.Valid {
			json.Unmarshal([]byte(tagsJSON.String), &obs.Tags)
		}
		observations = append(observations, obs)
	}
	return observations, rows.Err()
}

// sanitizeFTS wraps each word in double quotes to prevent FTS5 operator injection.
func sanitizeFTS(query string) string {
	words := strings.Fields(query)
	if len(words) == 0 {
		return ""
	}
	quoted := make([]string, len(words))
	for i, w := range words {
		w = strings.ReplaceAll(w, `"`, `""`) // escape any embedded quotes
		quoted[i] = `"` + w + `"`
	}
	return strings.Join(quoted, " ")
}

// normalizeHash computes SHA-256 of lowercased, whitespace-collapsed content.
func normalizeHash(content string) string {
	normalized := strings.ToLower(strings.Join(strings.Fields(content), " "))
	h := sha256.Sum256([]byte(normalized))
	return fmt.Sprintf("%x", h)
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}

func nilIfEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
