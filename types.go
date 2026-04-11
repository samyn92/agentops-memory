package main

import "time"

// --- Sessions ---

type CreateSessionRequest struct {
	ID      string `json:"id"`
	Project string `json:"project"`
}

type Session struct {
	ID           string  `json:"id"`
	Project      string  `json:"project"`
	StartedAt    string  `json:"started_at"`
	EndedAt      *string `json:"ended_at,omitempty"`
	Summary      *string `json:"summary,omitempty"`
	MessageCount int     `json:"message_count"`
}

type EndSessionRequest struct {
	Messages []SessionMessage `json:"messages"`
}

type SessionMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type EndSessionResponse struct {
	SessionID    string `json:"session_id"`
	Summary      string `json:"summary"`
	MessageCount int    `json:"message_count"`
}

// --- Observations ---

type CreateObservationRequest struct {
	SessionID string   `json:"session_id"`
	Type      string   `json:"type"`
	Title     string   `json:"title"`
	Content   string   `json:"content"`
	Project   string   `json:"project"`
	Tags      []string `json:"tags,omitempty"`
	Scope     string   `json:"scope,omitempty"`
	TopicKey  string   `json:"topic_key,omitempty"`
}

type CreateObservationResponse struct {
	ID             int64  `json:"id"`
	Action         string `json:"action"` // created, updated, deduplicated
	RevisionCount  int    `json:"revision_count"`
	DuplicateCount int    `json:"duplicate_count"`
}

type Observation struct {
	ID             int64    `json:"id"`
	SessionID      string   `json:"session_id"`
	Type           string   `json:"type"`
	Title          string   `json:"title"`
	Content        string   `json:"content"`
	Tags           []string `json:"tags,omitempty"`
	Project        string   `json:"project"`
	Scope          string   `json:"scope"`
	TopicKey       string   `json:"topic_key,omitempty"`
	NormalizedHash string   `json:"normalized_hash,omitempty"`
	RevisionCount  int      `json:"revision_count"`
	DuplicateCount int      `json:"duplicate_count"`
	LastSeenAt     string   `json:"last_seen_at,omitempty"`
	PromotedTo     string   `json:"promoted_to,omitempty"`
	CreatedAt      string   `json:"created_at"`
	UpdatedAt      string   `json:"updated_at"`
}

type UpdateObservationRequest struct {
	Type    *string  `json:"type,omitempty"`
	Title   *string  `json:"title,omitempty"`
	Content *string  `json:"content,omitempty"`
	Tags    []string `json:"tags,omitempty"`
}

type SearchResult struct {
	ID       int64   `json:"id"`
	Type     string  `json:"type"`
	Title    string  `json:"title"`
	Content  string  `json:"content"`
	Rank     float64 `json:"rank"`
	TopicKey string  `json:"topic_key,omitempty"`
}

// --- Context ---

type ContextResponse struct {
	RecentSessions     []ContextSession     `json:"recent_sessions"`
	RecentObservations []ContextObservation `json:"recent_observations"`
}

type ContextSession struct {
	Summary string `json:"summary"`
}

type ContextObservation struct {
	Type    string `json:"type"`
	Title   string `json:"title"`
	Content string `json:"content"`
}

// --- Timeline ---

type TimelineEntry struct {
	ID        int64  `json:"id"`
	Type      string `json:"type"`
	Title     string `json:"title"`
	Content   string `json:"content"`
	CreatedAt string `json:"created_at"`
}

// --- Stats ---

type Stats struct {
	TotalSessions     int      `json:"total_sessions"`
	TotalObservations int      `json:"total_observations"`
	Projects          []string `json:"projects"`
}

// --- Export / Import ---

type ExportData struct {
	ExportedAt   string        `json:"exported_at"`
	Sessions     []Session     `json:"sessions"`
	Observations []Observation `json:"observations"`
}

type ImportResult struct {
	ImportedSessions     int `json:"imported_sessions"`
	ImportedObservations int `json:"imported_observations"`
}

// --- Config ---

type Config struct {
	ListenAddr   string        // e.g. ":7437"
	DBPath       string        // e.g. "/data/memory.db"
	DedupeWindow time.Duration // e.g. 15m
}
