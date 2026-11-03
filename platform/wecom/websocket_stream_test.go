package wecom

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/gorilla/websocket"
)

type capturedWSFrame struct {
	Cmd     string         `json:"cmd"`
	Headers wsFrameHeaders `json:"headers"`
	Stream  *struct {
		ID      string `json:"id"`
		Finish  bool   `json:"finish"`
		Content string `json:"content"`
	}
	Markdown *struct {
		Content string `json:"content"`
	}
	ChatID  string
	MsgType string
}

type mockWeComStreamServer struct {
	t      *testing.T
	server *httptest.Server
	mu     sync.Mutex
	frames []capturedWSFrame
	nextFn func(frame capturedWSFrame) (int, string)
}

func newMockWeComStreamServer(t *testing.T, ackFn func(frame capturedWSFrame) (int, string)) (*mockWeComStreamServer, *WSPlatform) {
	t.Helper()
	m := &mockWeComStreamServer{
		t:      t,
		nextFn: ackFn,
	}
	upgrader := websocket.Upgrader{}
	m.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			var raw wsFrame
			if err := conn.ReadJSON(&raw); err != nil {
				return
			}
			var body struct {
				ChatID  string `json:"chatid"`
				MsgType string `json:"msgtype"`
				Stream  *struct {
					ID      string `json:"id"`
					Finish  bool   `json:"finish"`
					Content string `json:"content"`
				} `json:"stream"`
				Markdown *struct {
					Content string `json:"content"`
				} `json:"markdown"`
			}
			_ = json.Unmarshal(raw.Body, &body)
			captured := capturedWSFrame{
				Cmd:      raw.Cmd,
				Headers:  raw.Headers,
				Stream:   body.Stream,
				Markdown: body.Markdown,
				ChatID:   body.ChatID,
				MsgType:  body.MsgType,
			}

			m.mu.Lock()
			m.frames = append(m.frames, captured)
			fn := m.nextFn
			m.mu.Unlock()

			code, msg := 0, "ok"
			if fn != nil {
				code, msg = fn(captured)
			}
			_ = conn.WriteJSON(map[string]any{
				"headers": raw.Headers,
				"errcode": code,
				"errmsg":  msg,
			})
		}
	}))
	t.Cleanup(m.server.Close)

	wsURL := "ws" + m.server.URL[len("http"):]
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial mock websocket: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	p := &WSPlatform{
		conn:       conn,
		ackTimeout: time.Second,
	}
	go func() {
		for {
			var frame wsFrame
			if err := conn.ReadJSON(&frame); err != nil {
				return
			}
			p.handleFrame(frame)
		}
	}()
	return m, p
}

func (m *mockWeComStreamServer) snapshot() []capturedWSFrame {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]capturedWSFrame, len(m.frames))
	copy(out, m.frames)
	return out
}

func TestWSPlatform_StreamLifecycleSingleBubbleInPlace(t *testing.T) {
	srv, p := newMockWeComStreamServer(t, nil)
	ctx := context.Background()
	rctx := wsReplyContext{reqID: "cb_req_100", chatID: "chat_1", chatType: "single", userID: "alice"}

	handle, err := p.SendPreviewStart(ctx, rctx, "")
	if err != nil {
		t.Fatalf("SendPreviewStart() error = %v", err)
	}

	if err := p.UpdateMessage(ctx, handle, "正在分析租务合同"); err != nil {
		t.Fatalf("UpdateMessage(1) error = %v", err)
	}
	if err := p.UpdateMessage(ctx, handle, "正在分析租务合同，已定位到计费规则。"); err != nil {
		t.Fatalf("UpdateMessage(2) error = %v", err)
	}
	if err := p.FinishPreview(ctx, handle, "最终结论：计费规则正常，详见 `billing.go:42`。"); err != nil {
		t.Fatalf("FinishPreview() error = %v", err)
	}
	// Straggling update after FinishPreview must be ignored so sealed bubble is never reopened.
	if err := p.UpdateMessage(ctx, handle, "straggler chunk"); err != nil {
		t.Fatalf("post-finish UpdateMessage() error = %v", err)
	}

	frames := srv.snapshot()
	if len(frames) != 4 {
		t.Fatalf("frame count = %d, want 4; frames=%+v", len(frames), frames)
	}

	streamID := frames[0].Stream.ID
	if streamID == "" {
		t.Fatal("opening stream frame had empty stream.id")
	}
	for i, f := range frames {
		if f.Cmd != "aibot_respond_msg" {
			t.Fatalf("frame[%d].Cmd = %q, want aibot_respond_msg", i, f.Cmd)
		}
		if f.Headers.ReqID != "cb_req_100" {
			t.Fatalf("frame[%d].ReqID = %q, want cb_req_100", i, f.Headers.ReqID)
		}
		if f.Stream == nil || f.Stream.ID != streamID {
			t.Fatalf("frame[%d] stream ID mismatch: got %+v, want %q", i, f.Stream, streamID)
		}
	}

	// Frame 0: opening <think></think> spinner with finish=false (NOT defused!)
	if frames[0].Stream.Finish || frames[0].Stream.Content != "<think></think>" {
		t.Fatalf("frame[0] = %+v, want finish=false content=<think></think>", frames[0].Stream)
	}
	// Frames 1..2: intermediate full-replacement updates with finish=false
	if frames[1].Stream.Finish || frames[1].Stream.Content != "正在分析租务合同" {
		t.Fatalf("frame[1] = %+v", frames[1].Stream)
	}
	if frames[2].Stream.Finish || frames[2].Stream.Content != "正在分析租务合同，已定位到计费规则。" {
		t.Fatalf("frame[2] = %+v", frames[2].Stream)
	}
	// Frame 3: sealing frame with finish=true
	if !frames[3].Stream.Finish || !strings.Contains(frames[3].Stream.Content, "最终结论") {
		t.Fatalf("frame[3] = %+v, want finish=true with final text", frames[3].Stream)
	}
}

func TestDefuseThinkTags_PreservesUnicodeByteOffsetsAndDefusesClosingFrames(t *testing.T) {
	// U+212A KELVIN SIGN ('K') is 3 bytes in UTF-8 and lowercases to 1-byte 'k',
	// which panics or corrupts byte offsets if strings.ToLower(s) is indexed directly.
	input := "KK前缀 <think>折叠风险</think> 与 <THINK attr='1'>大写</Think> 以及普通 <div> 和 a < b 或 KK<x"
	got := defuseThinkTags(input)

	if !utf8.ValidString(got) {
		t.Fatalf("defuseThinkTags produced invalid UTF-8: %q", got)
	}
	if strings.Contains(got, "<think") || strings.Contains(got, "</think") ||
		strings.Contains(got, "<THINK") || strings.Contains(got, "</Think") {
		t.Fatalf("literal think tags were not defused: %q", got)
	}
	if !strings.Contains(got, "<\u200bthink>折叠风险<\u200b/think>") {
		t.Fatalf("expected zero-width space injected into <think>...</think>, got %q", got)
	}
	if !strings.Contains(got, "<\u200bTHINK attr='1'>大写<\u200b/Think>") {
		t.Fatalf("expected mixed-case think tags defused, got %q", got)
	}
	if !strings.Contains(got, "<div>") || !strings.Contains(got, "a < b") || !strings.HasSuffix(got, "KK<x") {
		t.Fatalf("non-think brackets were unexpectedly altered: %q", got)
	}

	// Verify FinishPreview applies defuseThinkTags on the wire.
	srv, p := newMockWeComStreamServer(t, nil)
	ctx := context.Background()
	rctx := wsReplyContext{reqID: "cb_think", chatID: "chat_1", userID: "alice"}
	handle, err := p.SendPreviewStart(ctx, rctx, "<think></think>")
	if err != nil {
		t.Fatalf("SendPreviewStart() error = %v", err)
	}
	if err := p.FinishPreview(ctx, handle, "请勿输出 <think>思考块</think>"); err != nil {
		t.Fatalf("FinishPreview() error = %v", err)
	}
	frames := srv.snapshot()
	if frames[0].Stream.Content != "<think></think>" {
		t.Fatalf("opening frame must keep real <think></think>, got %q", frames[0].Stream.Content)
	}
	if gotClose := frames[1].Stream.Content; gotClose != "请勿输出 <\u200bthink>思考块<\u200b/think>" {
		t.Fatalf("closing frame content = %q", gotClose)
	}
}

func TestWSPlatform_BlankClosingFrameInterception(t *testing.T) {
	// 1. Wire-level builder must reject blank finish=true payloads outright.
	for _, blank := range []string{"", "   ", "\n\t\r", "\u200b"} {
		if _, err := buildWSStreamBody("stream_1", blank, true); !errors.Is(err, ErrEmptyStreamClosingFrame) {
			t.Fatalf("buildWSStreamBody(%q, finish=true) err = %v, want ErrEmptyStreamClosingFrame", blank, err)
		}
	}

	// 2. FinishPreview must substitute a visible fallback so an open WeCom bubble never spins forever.
	srv, p := newMockWeComStreamServer(t, nil)
	ctx := context.Background()
	rctx := wsReplyContext{reqID: "cb_blank", chatID: "chat_1", userID: "alice"}
	handle, err := p.SendPreviewStart(ctx, rctx, "")
	if err != nil {
		t.Fatalf("SendPreviewStart() error = %v", err)
	}
	if err := p.FinishPreview(ctx, handle, "   \n\t "); err != nil {
		t.Fatalf("FinishPreview(blank) error = %v", err)
	}
	frames := srv.snapshot()
	if len(frames) != 2 {
		t.Fatalf("frames = %+v, want 2 frames", frames)
	}
	if !frames[1].Stream.Finish || !hasVisibleStreamChar(frames[1].Stream.Content) {
		t.Fatalf("blank FinishPreview sent invisible closing frame: %+v", frames[1].Stream)
	}
}

func TestWSPlatform_StreamExpired846608And10MinFallbackToSend(t *testing.T) {
	srv, p := newMockWeComStreamServer(t, func(frame capturedWSFrame) (int, string) {
		if frame.Cmd == "aibot_respond_msg" && frame.Stream != nil && frame.Stream.Finish {
			return wsStreamErrCodeExpired, "stream message expired (>10min)"
		}
		return 0, "ok"
	})
	ctx := context.Background()
	rctx := wsReplyContext{reqID: "cb_exp_1", chatID: "chat_exp", chatType: "single", userID: "alice"}

	handle, err := p.SendPreviewStart(ctx, rctx, "<think></think>")
	if err != nil {
		t.Fatalf("SendPreviewStart() error = %v", err)
	}

	err = p.FinishPreview(ctx, handle, "长耗时分析最终报告")
	if !errors.Is(err, ErrStreamExpired) || !IsStreamExpired(err) {
		t.Fatalf("FinishPreview() on 846608 err = %v, want ErrStreamExpired", err)
	}

	// Caller falls back to proactive Send (aibot_send_msg)
	if err := p.Send(ctx, rctx, "长耗时分析最终报告"); err != nil {
		t.Fatalf("fallback Send() error = %v", err)
	}

	frames := srv.snapshot()
	last := frames[len(frames)-1]
	if last.Cmd != "aibot_send_msg" || last.ChatID != "chat_exp" || last.Markdown == nil || last.Markdown.Content != "长耗时分析最终报告" {
		t.Fatalf("expected fallback aibot_send_msg frame, got %+v", last)
	}

	// Local 10-minute wall-clock guard also returns ErrStreamExpired without hitting network.
	now := time.Now()
	p.now = func() time.Time { return now }
	handle2, err := p.SendPreviewStart(ctx, wsReplyContext{reqID: "cb_exp_2", chatID: "chat_exp", userID: "alice"}, "")
	if err != nil {
		t.Fatalf("SendPreviewStart(2) error = %v", err)
	}
	now = now.Add(10*time.Minute + time.Second)
	if err := p.UpdateMessage(ctx, handle2, "late chunk"); !errors.Is(err, ErrStreamExpired) {
		t.Fatalf("UpdateMessage after 10m err = %v, want ErrStreamExpired", err)
	}
	if err := p.FinishPreview(ctx, handle2, "late final"); !errors.Is(err, ErrStreamExpired) {
		t.Fatalf("FinishPreview after 10m err = %v, want ErrStreamExpired", err)
	}
}

func TestWSPlatform_FinishPreviewSplitsOversizedPayloadWithoutDataLoss(t *testing.T) {
	srv, p := newMockWeComStreamServer(t, nil)
	ctx := context.Background()
	rctx := wsReplyContext{reqID: "cb_big", chatID: "chat_big", chatType: "single", userID: "alice"}

	handle, err := p.SendPreviewStart(ctx, rctx, "")
	if err != nil {
		t.Fatalf("SendPreviewStart() error = %v", err)
	}

	big := strings.Repeat("泊寓租务规则核查结果正常。\n", 700) // ~27,300 UTF-8 bytes (> 20480)
	if err := p.FinishPreview(ctx, handle, big); err != nil {
		t.Fatalf("FinishPreview(oversized) error = %v", err)
	}

	frames := srv.snapshot()
	if len(frames) < 3 {
		t.Fatalf("expected opening + sealing stream + overflow aibot_send_msg, got %d frames", len(frames))
	}
	if !frames[1].Stream.Finish || len(frames[1].Stream.Content) > wsStreamContentLimit || !utf8.ValidString(frames[1].Stream.Content) {
		t.Fatalf("sealing frame violated 20480-byte or UTF-8 limit: len=%d finish=%v", len(frames[1].Stream.Content), frames[1].Stream.Finish)
	}
	var rebuilt strings.Builder
	rebuilt.WriteString(frames[1].Stream.Content)
	for _, f := range frames[2:] {
		if f.Cmd != "aibot_send_msg" || f.Markdown == nil {
			t.Fatalf("unexpected overflow frame: %+v", f)
		}
		rebuilt.WriteString(f.Markdown.Content)
	}
	if rebuilt.String() != big {
		t.Fatalf("reassembled content len=%d != original len=%d", rebuilt.Len(), len(big))
	}
}
