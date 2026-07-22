package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	loggingproxy "github.com/mrexodia/logging-proxy"
)

type ObserverLogger struct {
	cfg        Config
	upstream   *url.URL
	client     *http.Client
	mu         sync.Mutex
	states     map[string]*requestState
	maxBodyLen int64

	// endpoints that returned 404 (upstream is not llama.cpp); polled once, then skipped
	epMu              sync.Mutex
	disabledEndpoints map[string]bool
}

type requestState struct {
	id       string
	shortID  string
	dir      string
	created  time.Time
	metadata loggingproxy.RequestMetadata

	mu               sync.Mutex
	summary          Summary
	model            string
	stop             chan struct{}
	once             sync.Once
	propsLoadedSaved bool
	logger           *ObserverLogger
}

type Summary struct {
	RequestID                string         `json:"request_id"`
	Model                    string         `json:"model,omitempty"`
	Endpoint                 string         `json:"endpoint,omitempty"`
	Method                   string         `json:"method,omitempty"`
	SourceURL                string         `json:"source_url,omitempty"`
	TargetURL                string         `json:"target_url,omitempty"`
	StartedAt                time.Time      `json:"started_at"`
	ResponseHeadersAt        *time.Time     `json:"response_headers_at,omitempty"`
	CompletedAt              *time.Time     `json:"completed_at,omitempty"`
	Completed                bool           `json:"completed"`
	Error                    string         `json:"error,omitempty"`
	ResponseStatus           string         `json:"response_status,omitempty"`
	ResponseStatusCode       int            `json:"response_status_code,omitempty"`
	UpstreamHeaderDurationMS int64          `json:"upstream_header_duration_ms,omitempty"`
	DurationMS               int64          `json:"duration_ms,omitempty"`
	FirstSSEEventMS          *int64         `json:"first_sse_event_ms,omitempty"`
	FirstContentTokenMS      *int64         `json:"first_content_token_ms,omitempty"`
	LastSSEEventMS           *int64         `json:"last_sse_event_ms,omitempty"`
	FinishReason             string         `json:"finish_reason,omitempty"`
	Usage                    map[string]any `json:"usage,omitempty"`
	FinalTimings             map[string]any `json:"final_timings,omitempty"`
	RequestOptions           map[string]any `json:"request_options,omitempty"`
	SSEEvents                int            `json:"sse_events"`
	SSEDone                  bool           `json:"sse_done"`
	UsageReceived            bool           `json:"usage_received"`
	ProtocolCompleted        bool           `json:"protocol_completed"`
	PromptProgressEvents     int            `json:"prompt_progress_events"`
	ContentEvents            int            `json:"content_events"`
	ReasoningEvents          int            `json:"reasoning_events"`
	ToolCallEvents           int            `json:"tool_call_events"`
	BytesRequest             int64          `json:"bytes_request"`
	BytesResponse            int64          `json:"bytes_response"`
	PollSamples              int            `json:"poll_samples"`
	LastMetrics              map[string]any `json:"last_metrics,omitempty"`
	PropsLoaded              bool           `json:"props_loaded"`
	PropsLoadedAt            *time.Time     `json:"props_loaded_at,omitempty"`
	PropsLoadedStatusCode    int            `json:"props_loaded_status_code,omitempty"`
	StallDetected            bool           `json:"stall_detected"`
	StallReason              string         `json:"stall_reason,omitempty"`
}

type requestLogMetadata struct {
	StreamType   string                       `json:"stream_type"`
	Metadata     loggingproxy.RequestMetadata `json:"metadata"`
	Timestamp    time.Time                    `json:"timestamp"`
	StartedAt    time.Time                    `json:"started_at"`
	CompletedAt  *time.Time                   `json:"completed_at,omitempty"`
	DurationMS   int64                        `json:"duration_ms,omitempty"`
	BytesWritten int64                        `json:"bytes_written"`
	Completed    bool                         `json:"completed"`
	Error        string                       `json:"error,omitempty"`
	Filename     string                       `json:"filename"`
}

func NewObserverLogger(cfg Config) (*ObserverLogger, error) {
	if err := os.MkdirAll(cfg.Logging.LogDir, 0755); err != nil {
		return nil, err
	}
	upstream, err := url.Parse(cfg.Upstream.BaseURL)
	if err != nil {
		return nil, err
	}
	return &ObserverLogger{
		cfg:      cfg,
		upstream: upstream,
		client: &http.Client{
			Timeout: cfg.PollTimeout(),
		},
		states:            map[string]*requestState{},
		maxBodyLen:        cfg.Observer.MaxParseBytes,
		disabledEndpoints: map[string]bool{},
	}, nil
}

func (l *ObserverLogger) LogRequest(metadata loggingproxy.RequestMetadata, timestamp time.Time, rawRequestStream io.ReadCloser) {
	defer rawRequestStream.Close()
	state := l.getState(metadata, timestamp)
	state.writeMetadata("request", timestamp, false, 0, "")

	data, err := readLimited(rawRequestStream, l.maxBodyLen)
	completedAt := time.Now()
	if err == nil {
		err = os.WriteFile(filepath.Join(state.dir, "request.http"), data, 0644)
	}
	if err != nil {
		state.writeMetadata("request", timestamp, false, int64(len(data)), err.Error())
		state.setError(err.Error())
		return
	}

	state.writeMetadata("request", timestamp, true, int64(len(data)), "")
	state.update(func(s *Summary) {
		s.BytesRequest = int64(len(data))
		_ = completedAt
	})

	info := parseRequestInfo(data, metadata)
	state.update(func(s *Summary) {
		s.Model = info.Model
		s.Endpoint = info.Endpoint
		s.RequestOptions = info.Options
	})
	state.model = info.Model
	state.writeSummary()
	if info.Model != "" && info.Endpoint != "" {
		state.startPoller()
	}
}

func (l *ObserverLogger) LogResponse(metadata loggingproxy.RequestMetadata, timestamp time.Time, rawResponseStream io.ReadCloser) {
	defer rawResponseStream.Close()
	state := l.getState(metadata, timestamp)
	state.metadata = metadata
	state.writeMetadata("response", timestamp, false, 0, "")
	state.update(func(s *Summary) {
		s.ResponseHeadersAt = metadata.UpstreamResponseAt
		s.ResponseStatus = metadata.ResponseStatus
		s.ResponseStatusCode = metadata.ResponseStatusCode
		s.UpstreamHeaderDurationMS = metadata.UpstreamHeaderDurationMS
	})

	file, err := os.Create(filepath.Join(state.dir, "response.http"))
	if err != nil {
		state.writeMetadata("response", timestamp, false, 0, err.Error())
		state.setError(err.Error())
		state.stopPoller()
		return
	}
	defer file.Close()

	eventsFile, err := os.OpenFile(filepath.Join(state.dir, "sse_events.jsonl"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		state.writeMetadata("response", timestamp, false, 0, err.Error())
		state.setError(err.Error())
		state.stopPoller()
		return
	}
	defer eventsFile.Close()

	reader := bufio.NewReader(rawResponseStream)
	bytesWritten := int64(0)
	inHeaders := true
	var headerBuf bytes.Buffer
	for {
		line, readErr := reader.ReadBytes('\n')
		if len(line) > 0 {
			if inHeaders {
				headerBuf.Write(line)
				if isBlankHTTPLine(line) {
					n, _ := file.Write(headerBuf.Bytes())
					bytesWritten += int64(n)
					inHeaders = false
				}
			} else {
				n, _ := file.Write(line)
				bytesWritten += int64(n)
				state.handleSSELine(eventsFile, line)
			}
		}
		if readErr != nil {
			if readErr != io.EOF {
				state.writeMetadata("response", timestamp, false, bytesWritten, readErr.Error())
				state.setError(readErr.Error())
			} else {
				completedAt := time.Now()
				state.writeMetadata("response", timestamp, true, bytesWritten, "")
				state.update(func(s *Summary) {
					s.Completed = true
					s.CompletedAt = &completedAt
					s.DurationMS = completedAt.Sub(s.StartedAt).Milliseconds()
					s.BytesResponse = bytesWritten
					s.ProtocolCompleted = s.SSEDone && s.UsageReceived && s.FinishReason != ""
					if !s.ProtocolCompleted && s.Error == "" {
						s.Error = "response stream ended before SSE protocol completed"
					}
				})
			}
			break
		}
	}
	state.stopPoller()
	state.snapshotPropsIfAvailable()
	state.writeSummary()
}

func (l *ObserverLogger) getState(metadata loggingproxy.RequestMetadata, timestamp time.Time) *requestState {
	l.mu.Lock()
	defer l.mu.Unlock()
	if state := l.states[metadata.ID]; state != nil {
		return state
	}
	shortID := metadata.ID
	if len(shortID) > 8 {
		shortID = shortID[:8]
	}
	dir := filepath.Join(l.cfg.Logging.LogDir, timestamp.Format("2006-01-02_15-04-05.000")+"_"+shortID)
	if err := os.MkdirAll(dir, 0755); err != nil {
		log.Printf("failed to create request dir %s: %v", dir, err)
	}
	state := &requestState{
		id:       metadata.ID,
		shortID:  shortID,
		dir:      dir,
		created:  timestamp,
		metadata: metadata,
		stop:     make(chan struct{}),
		logger:   l,
	}
	state.summary = Summary{
		RequestID:                metadata.ID,
		Method:                   metadata.Method,
		SourceURL:                metadata.SourceURL,
		TargetURL:                metadata.DestinationURL,
		StartedAt:                metadata.RequestStartedAt,
		ResponseHeadersAt:        metadata.UpstreamResponseAt,
		ResponseStatus:           metadata.ResponseStatus,
		ResponseStatusCode:       metadata.ResponseStatusCode,
		UpstreamHeaderDurationMS: metadata.UpstreamHeaderDurationMS,
	}
	if state.summary.StartedAt.IsZero() {
		state.summary.StartedAt = timestamp
	}
	l.states[metadata.ID] = state
	return state
}

func (s *requestState) writeMetadata(streamType string, timestamp time.Time, completed bool, bytesWritten int64, errMsg string) {
	path := filepath.Join(s.dir, streamType+"_metadata.json")
	meta := requestLogMetadata{
		StreamType:   streamType,
		Metadata:     s.metadata,
		Timestamp:    timestamp,
		StartedAt:    timestamp,
		BytesWritten: bytesWritten,
		Completed:    completed,
		Filename:     streamType + ".http",
		Error:        errMsg,
	}
	if completed {
		completedAt := time.Now()
		meta.CompletedAt = &completedAt
		meta.DurationMS = completedAt.Sub(timestamp).Milliseconds()
	}
	writeJSONAtomic(path, meta)
}

func (s *requestState) update(fn func(*Summary)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fn(&s.summary)
}

func (s *requestState) setError(msg string) {
	s.update(func(summary *Summary) {
		summary.Error = msg
	})
	s.writeSummary()
}

func (s *requestState) writeSummary() {
	s.mu.Lock()
	copy := s.summary
	s.mu.Unlock()
	writeJSONAtomic(filepath.Join(s.dir, "summary.json"), copy)
}

func readLimited(r io.Reader, limit int64) ([]byte, error) {
	var buf bytes.Buffer
	_, err := io.Copy(&buf, io.LimitReader(r, limit+1))
	if err != nil {
		return buf.Bytes(), err
	}
	if int64(buf.Len()) > limit {
		return buf.Bytes()[:limit], fmt.Errorf("stream exceeds max_parse_bytes=%d", limit)
	}
	return buf.Bytes(), nil
}

func writeJSONAtomic(path string, value any) {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		log.Printf("failed to create dir for %s: %v", path, err)
		return
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".*.tmp")
	if err != nil {
		log.Printf("failed to create temp file for %s: %v", path, err)
		return
	}
	tmpPath := tmp.Name()
	enc := json.NewEncoder(tmp)
	enc.SetIndent("", "  ")
	if err := enc.Encode(value); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
		log.Printf("failed to write json %s: %v", path, err)
		return
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		log.Printf("failed to close json %s: %v", path, err)
		return
	}
	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		log.Printf("failed to replace json %s: %v", path, err)
	}
}

func appendJSONLine(file *os.File, value any) {
	data, err := json.Marshal(value)
	if err != nil {
		return
	}
	_, _ = file.Write(data)
	_, _ = file.Write([]byte("\n"))
}

func isBlankHTTPLine(line []byte) bool {
	trimmed := strings.TrimRight(string(line), "\r\n")
	return trimmed == ""
}

type requestInfo struct {
	Model    string
	Endpoint string
	Options  map[string]any
}

func parseRequestInfo(data []byte, metadata loggingproxy.RequestMetadata) requestInfo {
	info := requestInfo{Options: map[string]any{}}
	if u, err := url.Parse(metadata.DestinationURL); err == nil {
		info.Endpoint = u.Path
	}
	idx := bytes.Index(data, []byte("\r\n\r\n"))
	if idx < 0 {
		return info
	}
	body := data[idx+4:]
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		return info
	}
	if model, ok := payload["model"].(string); ok {
		info.Model = model
	}
	for key, value := range payload {
		if skipRequestOption(key) {
			continue
		}
		info.Options[key] = value
	}
	return info
}

func skipRequestOption(key string) bool {
	switch key {
	case "messages", "prompt", "input", "suffix", "tools", "functions":
		return true
	default:
		return false
	}
}

func (s *requestState) handleSSELine(file *os.File, line []byte) {
	text := strings.TrimSpace(string(line))
	if !strings.HasPrefix(text, "data:") {
		return
	}
	payload := strings.TrimSpace(strings.TrimPrefix(text, "data:"))
	now := time.Now()
	elapsed := now.Sub(s.summary.StartedAt).Milliseconds()
	if payload == "[DONE]" {
		appendJSONLine(file, map[string]any{"ts": now, "elapsed_ms": elapsed, "type": "done"})
		s.update(func(summary *Summary) {
			summary.SSEEvents++
			summary.SSEDone = true
			summary.LastSSEEventMS = ptrInt64(elapsed)
			summary.ProtocolCompleted = summary.SSEDone && summary.UsageReceived && summary.FinishReason != ""
		})
		s.writeSummary()
		return
	}
	var obj map[string]any
	if err := json.Unmarshal([]byte(payload), &obj); err != nil {
		appendJSONLine(file, map[string]any{"ts": now, "elapsed_ms": elapsed, "type": "invalid", "error": err.Error(), "data": payload})
		return
	}
	eventType := classifySSE(obj)
	appendJSONLine(file, map[string]any{"ts": now, "elapsed_ms": elapsed, "type": eventType, "data": obj})
	s.update(func(summary *Summary) {
		summary.SSEEvents++
		if summary.FirstSSEEventMS == nil {
			summary.FirstSSEEventMS = ptrInt64(elapsed)
		}
		summary.LastSSEEventMS = ptrInt64(elapsed)
		if _, ok := obj["prompt_progress"]; ok {
			summary.PromptProgressEvents++
		}
		if hasContentDelta(obj) {
			summary.ContentEvents++
			if summary.FirstContentTokenMS == nil {
				summary.FirstContentTokenMS = ptrInt64(elapsed)
			}
		}
		if hasReasoningDelta(obj) {
			summary.ReasoningEvents++
		}
		if hasToolCallDelta(obj) {
			summary.ToolCallEvents++
		}
		if reason := finishReason(obj); reason != "" {
			summary.FinishReason = reason
		}
		if usage, ok := obj["usage"].(map[string]any); ok {
			summary.Usage = usage
			summary.UsageReceived = true
		}
		if timings, ok := obj["timings"].(map[string]any); ok {
			summary.FinalTimings = timings
		}
		summary.ProtocolCompleted = summary.SSEDone && summary.UsageReceived && summary.FinishReason != ""
	})
	s.writeSummary()
}

func classifySSE(obj map[string]any) string {
	if _, ok := obj["usage"]; ok {
		return "usage"
	}
	if _, ok := obj["prompt_progress"]; ok {
		return "prompt_progress"
	}
	if reason := finishReason(obj); reason != "" {
		return "finish"
	}
	if hasContentDelta(obj) {
		return "content"
	}
	if hasToolCallDelta(obj) {
		return "tool_call"
	}
	if hasReasoningDelta(obj) {
		return "reasoning"
	}
	return "chunk"
}

func hasContentDelta(obj map[string]any) bool {
	choices, ok := obj["choices"].([]any)
	if !ok || len(choices) == 0 {
		return false
	}
	choice, ok := choices[0].(map[string]any)
	if !ok {
		return false
	}
	delta, ok := choice["delta"].(map[string]any)
	if !ok {
		return false
	}
	content, _ := delta["content"].(string)
	return content != ""
}

func hasReasoningDelta(obj map[string]any) bool {
	delta := firstDelta(obj)
	if delta == nil {
		return false
	}
	content, _ := delta["reasoning_content"].(string)
	return content != ""
}

func hasToolCallDelta(obj map[string]any) bool {
	delta := firstDelta(obj)
	if delta == nil {
		return false
	}
	calls, ok := delta["tool_calls"].([]any)
	return ok && len(calls) > 0
}

func firstDelta(obj map[string]any) map[string]any {
	choices, ok := obj["choices"].([]any)
	if !ok || len(choices) == 0 {
		return nil
	}
	choice, ok := choices[0].(map[string]any)
	if !ok {
		return nil
	}
	delta, ok := choice["delta"].(map[string]any)
	if !ok {
		return nil
	}
	return delta
}

func finishReason(obj map[string]any) string {
	choices, ok := obj["choices"].([]any)
	if !ok || len(choices) == 0 {
		return ""
	}
	choice, ok := choices[0].(map[string]any)
	if !ok {
		return ""
	}
	if reason, ok := choice["finish_reason"].(string); ok {
		return reason
	}
	return ""
}

func ptrInt64(v int64) *int64 {
	return &v
}

func strconvFormatFloat(v float64) string {
	return strconv.FormatFloat(v, 'f', -1, 64)
}

func (s *requestState) contextDone() <-chan struct{} {
	return s.stop
}

func (s *requestState) newPollContext() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		select {
		case <-s.contextDone():
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx, cancel
}
