package wecom

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/chenhg5/cc-connect/core"
)

const (
	// WeComSmartBotThinkingPlaceholder is the official collapsed thinking
	// spinner affordance supported by WeCom WebSocket stream replies.
	WeComSmartBotThinkingPlaceholder = "<think></think>"

	wsStreamContentLimit    = 20480
	wsStreamMaxDuration     = 10 * time.Minute
	wsStreamErrCodeBadReqID = 846605
	wsStreamErrCodeExpired  = 846608
	wsStreamEmptyFallback   = "（已完成）"
	wsStreamDiscardFallback = "[已结束]"
)

var (
	// ErrStreamExpired indicates that the WeCom single-stream 10-minute window
	// (errcode=846608) or callback req_id validity (errcode=846605) has expired,
	// so callers should fall back to proactive Send (aibot_send_msg).
	ErrStreamExpired = errors.New("wecom-ws: stream expired")

	// ErrEmptyStreamClosingFrame indicates a finish=true stream frame carried no
	// visible characters, which WeCom silently ignores and leaves spinning.
	ErrEmptyStreamClosingFrame = errors.New("wecom-ws: closing stream frame requires visible content")
)

var (
	_ core.PreviewStarter          = (*WSPlatform)(nil)
	_ core.MessageUpdater          = (*WSPlatform)(nil)
	_ core.PreviewFinisher         = (*WSPlatform)(nil)
	_ core.PreviewFinishPreference = (*WSPlatform)(nil)
	_ core.PreviewCleaner          = (*WSPlatform)(nil)
)

// wsStreamHandle binds a callback req_id and generated stream.id across the
// lifecycle of a single WeCom WebSocket streaming reply bubble.
type wsStreamHandle struct {
	mu          sync.Mutex
	replyCtx    wsReplyContext
	streamID    string
	startedAt   time.Time
	lastVisible string
	finished    bool
	expired     bool
}

// StreamID returns the generated WeCom stream identifier for diagnostics/tests.
func (h *wsStreamHandle) StreamID() string {
	if h == nil {
		return ""
	}
	return h.streamID
}

// IsStreamExpired reports whether err indicates an expired or unusable WeCom
// stream handle (errcode 846608 / 846605 or local 10-minute window expiration).
func IsStreamExpired(err error) bool {
	if errors.Is(err, ErrStreamExpired) {
		return true
	}
	var ackErr *wsAckError
	if errors.As(err, &ackErr) {
		return ackErr.Code == wsStreamErrCodeExpired || ackErr.Code == wsStreamErrCodeBadReqID
	}
	return false
}

// KeepPreviewOnFinish instructs core.streamPreview to finalize the WeCom bubble
// in place via FinishPreview instead of deleting it and sending a duplicate message.
func (p *WSPlatform) KeepPreviewOnFinish() bool {
	return true
}

// SendPreviewStart opens a WeCom WebSocket stream bubble (finish=false).
// When content is empty, whitespace-only, or "<think></think>", the official
// WeCom collapsed thinking spinner placeholder "<think></think>" is sent.
func (p *WSPlatform) SendPreviewStart(ctx context.Context, rctx any, content string) (any, error) {
	rc, ok := rctx.(wsReplyContext)
	if !ok {
		return nil, fmt.Errorf("wecom-ws: invalid reply context type %T", rctx)
	}
	if strings.TrimSpace(rc.reqID) == "" {
		return nil, fmt.Errorf("%w: callback req_id is empty", ErrStreamExpired)
	}

	initialContent := WeComSmartBotThinkingPlaceholder
	lastVisible := ""
	if content != WeComSmartBotThinkingPlaceholder && hasVisibleStreamChar(content) {
		initialContent = truncateStreamContent(content, wsStreamContentLimit)
		if !isThinkWrappedPlaceholder(initialContent) {
			lastVisible = initialContent
		}
	}

	streamID := p.generateReqID("stream")
	frame, err := buildWSStreamFrame(rc.reqID, streamID, initialContent, false)
	if err != nil {
		return nil, err
	}
	if err := p.sendStreamFrame(ctx, frame, rc.reqID); err != nil {
		slog.Warn("wecom-ws: stream start failed", "user", rc.userID, "stream_id", streamID, "error", err)
		return nil, err
	}

	return &wsStreamHandle{
		replyCtx:    rc,
		streamID:    streamID,
		startedAt:   p.currentTime(),
		lastVisible: lastVisible,
	}, nil
}

// UpdateMessage updates an open WeCom stream bubble in place (finish=false)
// using the same streamID. If another frame on the same handle is currently in
// flight, intermediate updates yield non-blockingly to avoid WeCom errcode=6000
// data-version conflicts and to keep FinishPreview responsive.
func (p *WSPlatform) UpdateMessage(ctx context.Context, previewHandle any, content string) error {
	h, ok := previewHandle.(*wsStreamHandle)
	if !ok || h == nil {
		return fmt.Errorf("wecom-ws: invalid stream preview handle %T", previewHandle)
	}

	if !h.mu.TryLock() {
		return nil
	}
	defer h.mu.Unlock()

	if h.finished {
		return nil
	}
	if h.expired {
		return ErrStreamExpired
	}
	if p.currentTime().Sub(h.startedAt) >= wsStreamMaxDuration {
		h.expired = true
		return fmt.Errorf("%w: stream exceeded %s limit", ErrStreamExpired, wsStreamMaxDuration)
	}
	if !hasVisibleStreamChar(content) {
		return nil
	}

	wireContent := truncateStreamContent(content, wsStreamContentLimit)
	frame, err := buildWSStreamFrame(h.replyCtx.reqID, h.streamID, wireContent, false)
	if err != nil {
		return err
	}
	if err := p.sendStreamFrame(ctx, frame, h.replyCtx.reqID); err != nil {
		if IsStreamExpired(err) {
			h.expired = true
		}
		return err
	}
	if wireContent != WeComSmartBotThinkingPlaceholder && !isThinkWrappedPlaceholder(wireContent) {
		h.lastVisible = wireContent
	}
	return nil
}

// FinishPreview seals an open WeCom stream bubble with finish=true, stopping
// the client loading spinner. Literal <think>/</think> tags in the final body
// are defused, and blank payloads are replaced with a visible fallback so the
// bubble never spins forever.
func (p *WSPlatform) FinishPreview(ctx context.Context, previewHandle any, content string) error {
	h, ok := previewHandle.(*wsStreamHandle)
	if !ok || h == nil {
		return fmt.Errorf("wecom-ws: invalid stream preview handle %T", previewHandle)
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	if h.finished {
		return nil
	}
	if h.expired {
		return ErrStreamExpired
	}
	if p.currentTime().Sub(h.startedAt) >= wsStreamMaxDuration {
		h.expired = true
		return fmt.Errorf("%w: stream exceeded %s limit", ErrStreamExpired, wsStreamMaxDuration)
	}

	if !hasVisibleStreamChar(content) {
		content = wsStreamEmptyFallback
	}
	defused := defuseThinkTags(content)
	if !hasVisibleStreamChar(defused) {
		defused = wsStreamEmptyFallback
	}

	var remainder string
	sealBody := defused
	if len(defused) > wsStreamContentLimit {
		if h.replyCtx.chatID != "" {
			chunks := splitByBytes(defused, wsStreamContentLimit)
			sealBody = chunks[0]
			if !hasVisibleStreamChar(sealBody) {
				sealBody = truncateStreamContent(defused, wsStreamContentLimit)
			} else if len(chunks) > 1 {
				remainder = strings.Join(chunks[1:], "")
			}
		} else {
			sealBody = truncateStreamContent(defused, wsStreamContentLimit)
		}
	}

	frame, err := buildWSStreamFrameRaw(h.replyCtx.reqID, h.streamID, sealBody, true)
	if err != nil {
		return err
	}
	if err := p.sendStreamFrame(ctx, frame, h.replyCtx.reqID); err != nil {
		if IsStreamExpired(err) {
			h.expired = true
		}
		return err
	}
	h.finished = true
	h.lastVisible = sealBody

	if hasVisibleStreamChar(remainder) {
		if err := p.Send(ctx, h.replyCtx, remainder); err != nil {
			slog.Warn("wecom-ws: failed to send stream overflow remainder", "user", h.replyCtx.userID, "error", err)
			return err
		}
	}
	return nil
}

// DeletePreviewMessage seals an unclosed stream bubble when cc-connect discards
// a preview (WeCom WebSocket does not support deleting an already-opened stream
// message, so sealing stops the client spinner cleanly).
func (p *WSPlatform) DeletePreviewMessage(ctx context.Context, previewHandle any) error {
	h, ok := previewHandle.(*wsStreamHandle)
	if !ok || h == nil {
		return nil
	}
	h.mu.Lock()
	alreadyDone := h.finished || h.expired
	fallback := h.lastVisible
	h.mu.Unlock()
	if alreadyDone {
		return nil
	}
	if !hasVisibleStreamChar(fallback) {
		fallback = wsStreamDiscardFallback
	}
	return p.FinishPreview(ctx, h, fallback)
}

func (p *WSPlatform) sendStreamFrame(ctx context.Context, frame map[string]any, reqID string) error {
	err := p.writeAndWaitAckStrict(ctx, frame, reqID, p.streamAckTimeout())
	if err == nil {
		return nil
	}
	if IsStreamExpired(err) {
		return fmt.Errorf("%w: %w", ErrStreamExpired, err)
	}
	return err
}

func (p *WSPlatform) streamAckTimeout() time.Duration {
	if p.ackTimeout > 0 {
		return p.ackTimeout
	}
	return wsAckTimeout
}

func (p *WSPlatform) currentTime() time.Time {
	if p.now != nil {
		return p.now()
	}
	return time.Now()
}

func buildWSStreamFrame(reqID, streamID, content string, finish bool) (map[string]any, error) {
	body, err := buildWSStreamBody(streamID, content, finish)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(reqID) == "" {
		return nil, fmt.Errorf("%w: callback req_id is empty", ErrStreamExpired)
	}
	return map[string]any{
		"cmd":     "aibot_respond_msg",
		"headers": map[string]string{"req_id": reqID},
		"body":    body,
	}, nil
}

func buildWSStreamFrameRaw(reqID, streamID, preparedContent string, finish bool) (map[string]any, error) {
	if strings.TrimSpace(reqID) == "" {
		return nil, fmt.Errorf("%w: callback req_id is empty", ErrStreamExpired)
	}
	if strings.TrimSpace(streamID) == "" {
		return nil, errors.New("wecom-ws: stream_id is empty")
	}
	if finish && !hasVisibleStreamChar(preparedContent) {
		return nil, ErrEmptyStreamClosingFrame
	}
	return map[string]any{
		"cmd":     "aibot_respond_msg",
		"headers": map[string]string{"req_id": reqID},
		"body": map[string]any{
			"msgtype": "stream",
			"stream": map[string]any{
				"id":      streamID,
				"finish":  finish,
				"content": preparedContent,
			},
		},
	}, nil
}

func buildWSStreamBody(streamID, content string, finish bool) (map[string]any, error) {
	if strings.TrimSpace(streamID) == "" {
		return nil, errors.New("wecom-ws: stream_id is empty")
	}
	if finish {
		content = defuseThinkTags(content)
	}
	content = truncateStreamContent(content, wsStreamContentLimit)
	if finish && !hasVisibleStreamChar(content) {
		return nil, ErrEmptyStreamClosingFrame
	}
	return map[string]any{
		"msgtype": "stream",
		"stream": map[string]any{
			"id":      streamID,
			"finish":  finish,
			"content": content,
		},
	}, nil
}

// defuseThinkTags inserts U+200B zero-width space right after '<' in <think and
// </think (case-insensitive) so literal <think> tags in final answers are never
// mis-rendered by WeCom clients as collapsed thinking blocks.
//
// Byte offsets are scanned directly on s without strings.ToLower(s), avoiding
// multi-byte Unicode case-folding length skew (e.g. U+212A KELVIN SIGN).
func defuseThinkTags(s string) string {
	if !strings.Contains(s, "<") {
		return s
	}
	const zwsp = "\u200b"
	var b strings.Builder
	last := 0
	for i := 0; i < len(s); i++ {
		if s[i] != '<' {
			continue
		}
		j := i + 1
		if j < len(s) && s[j] == '/' {
			j++
		}
		if !hasASCIIThinkPrefix(s[j:]) {
			continue
		}
		b.WriteString(s[last : i+1])
		b.WriteString(zwsp)
		last = i + 1
	}
	if last == 0 {
		return s
	}
	b.WriteString(s[last:])
	return b.String()
}

func hasASCIIThinkPrefix(s string) bool {
	return len(s) >= 5 &&
		(s[0] == 't' || s[0] == 'T') &&
		(s[1] == 'h' || s[1] == 'H') &&
		(s[2] == 'i' || s[2] == 'I') &&
		(s[3] == 'n' || s[3] == 'N') &&
		(s[4] == 'k' || s[4] == 'K')
}

func hasVisibleStreamChar(s string) bool {
	for _, r := range s {
		if !unicode.IsSpace(r) && !unicode.IsControl(r) && r != '\u200b' {
			return true
		}
	}
	return false
}

func isThinkWrappedPlaceholder(s string) bool {
	trimmed := strings.TrimSpace(s)
	return len(trimmed) >= len("<think></think>") &&
		strings.HasPrefix(trimmed, "<think>") &&
		strings.HasSuffix(trimmed, "</think>")
}

func truncateStreamContent(s string, maxBytes int) string {
	if maxBytes <= 0 || len(s) <= maxBytes {
		return s
	}
	const ellipsis = "…"
	if maxBytes <= len(ellipsis) {
		end := maxBytes
		for end > 0 && !utf8.RuneStart(s[end]) {
			end--
		}
		return s[:end]
	}
	limit := maxBytes - len(ellipsis)
	for limit > 0 && !utf8.RuneStart(s[limit]) {
		limit--
	}
	if limit > 0 && s[limit-1] == '<' {
		limit--
	}
	return s[:limit] + ellipsis
}
