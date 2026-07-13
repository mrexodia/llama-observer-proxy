package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
)

type InjectMiddleware struct {
	Next              http.Handler
	Enabled           bool
	DiagnosticOptions map[string]any
}

func (m InjectMiddleware) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !m.Enabled || !isLLMRequest(r) || hasEncodedBody(r) {
		m.Next.ServeHTTP(w, r)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	_ = r.Body.Close()

	patched, changed := injectDiagnosticOptions(body, m.DiagnosticOptions)
	if !changed {
		r.Body = io.NopCloser(bytes.NewReader(body))
		r.ContentLength = int64(len(body))
		m.Next.ServeHTTP(w, r)
		return
	}

	r.Body = io.NopCloser(bytes.NewReader(patched))
	r.ContentLength = int64(len(patched))
	r.Header.Set("Content-Length", stringInt(len(patched)))
	r.Header.Set("Content-Type", "application/json")

	if clientRequestedStream(body) {
		m.Next.ServeHTTP(w, r)
		return
	}

	// The client asked for a non-streaming response but diagnostics forced
	// stream=true upstream. Buffer the SSE stream (the logging layer below
	// still records it raw) and fold it back into a single JSON completion.
	buffer := newResponseBuffer()
	m.Next.ServeHTTP(buffer, r)
	if !buffer.isSSE() {
		buffer.writeThrough(w)
		return
	}
	assembled, ok := reassembleSSE(buffer.body.Bytes())
	if !ok {
		buffer.writeThrough(w)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", stringInt(len(assembled)))
	w.WriteHeader(buffer.status)
	_, _ = w.Write(assembled)
}

func clientRequestedStream(body []byte) bool {
	var payload struct {
		Stream bool `json:"stream"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return false
	}
	return payload.Stream
}

func isLLMRequest(r *http.Request) bool {
	if r.Method != http.MethodPost {
		return false
	}
	path := strings.TrimRight(r.URL.Path, "/")
	return strings.HasSuffix(path, "/v1/chat/completions") ||
		strings.HasSuffix(path, "/chat/completions") ||
		strings.HasSuffix(path, "/v1/responses") ||
		strings.HasSuffix(path, "/responses")
}

func hasEncodedBody(r *http.Request) bool {
	enc := strings.TrimSpace(r.Header.Get("Content-Encoding"))
	return enc != "" && !strings.EqualFold(enc, "identity")
}

func injectDiagnosticOptions(body []byte, opts map[string]any) ([]byte, bool) {
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		return body, false
	}
	if _, ok := payload["model"].(string); !ok {
		return body, false
	}
	if opts == nil {
		opts = DefaultDiagnosticOptions()
	}
	for key, value := range opts {
		if key == "stream_options" {
			merged := map[string]any{}
			if existing, ok := payload[key].(map[string]any); ok {
				for k, v := range existing {
					merged[k] = v
				}
			}
			if add, ok := value.(map[string]any); ok {
				for k, v := range add {
					merged[k] = v
				}
			}
			payload[key] = merged
			continue
		}
		payload[key] = value
	}
	patched, err := json.Marshal(payload)
	if err != nil {
		return body, false
	}
	return patched, true
}

func DefaultDiagnosticOptions() map[string]any {
	return map[string]any{
		"stream":            true,
		"timings_per_token": true,
		"return_progress":   true,
		"stream_options": map[string]any{
			"include_usage": true,
		},
	}
}

func MergeOptions(dst, src map[string]any) map[string]any {
	for key, value := range src {
		if srcMap, ok := value.(map[string]any); ok {
			if dstMap, ok := dst[key].(map[string]any); ok {
				MergeOptions(dstMap, srcMap)
				continue
			}
		}
		dst[key] = value
	}
	return dst
}

func stringInt(n int) string {
	return strconv.Itoa(n)
}
