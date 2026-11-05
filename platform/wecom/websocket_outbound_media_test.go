package wecom

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chenhg5/cc-connect/core"
	"github.com/gorilla/websocket"
)

func TestWSUploadChunkLimitsAndNoRetry(t *testing.T) {
	for _, tc := range []struct{ chunks, parallel int }{{1, 1}, {4, 4}, {5, 3}, {10, 3}, {11, 2}, {40, 2}} {
		t.Run(fmt.Sprint(tc.chunks), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			started := make(chan int, tc.chunks)
			release := make(chan struct{})
			done := make(chan error, 1)
			var active, maximum atomic.Int32
			seen := make([]atomic.Int32, tc.chunks)
			go func() {
				done <- uploadWSChunks(ctx, tc.chunks, func(ctx context.Context, index int) error {
					seen[index].Add(1)
					n := active.Add(1)
					defer active.Add(-1)
					for {
						old := maximum.Load()
						if old >= n || maximum.CompareAndSwap(old, n) {
							break
						}
					}
					started <- index
					select {
					case <-release:
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				})
			}()
			for i := 0; i < tc.parallel; i++ {
				select {
				case <-started:
				case <-ctx.Done():
					t.Fatal("chunks did not upload concurrently")
				}
			}
			if maximum.Load() != int32(tc.parallel) {
				t.Fatalf("concurrency=%d", maximum.Load())
			}
			close(release)
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			for i := range seen {
				if seen[i].Load() != 1 {
					t.Fatalf("chunk %d attempted %d times", i, seen[i].Load())
				}
			}
		})
	}
	var calls atomic.Int32
	err := uploadWSChunks(context.Background(), 10, func(context.Context, int) error { calls.Add(1); return errors.New("lost ACK") })
	if err == nil || calls.Load() > 3 {
		t.Fatalf("failed chunks retried: calls=%d err=%v", calls.Load(), err)
	}
}

func directMediaWS(t *testing.T, handle func(wsFrame) (map[string]any, bool)) *WSPlatform {
	t.Helper()
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			var frame wsFrame
			if err := conn.ReadJSON(&frame); err != nil {
				return
			}
			response, ack := handle(frame)
			if !ack {
				continue
			}
			response["headers"] = frame.Headers
			if err := conn.WriteJSON(response); err != nil {
				return
			}
		}
	}))
	t.Cleanup(server.Close)
	conn, _, err := websocket.DefaultDialer.Dial("ws"+server.URL[len("http"):], nil)
	if err != nil {
		t.Fatal(err)
	}
	p := &WSPlatform{conn: conn}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			var frame wsFrame
			if err := conn.ReadJSON(&frame); err != nil {
				return
			}
			p.handleFrame(frame)
		}
	}()
	t.Cleanup(func() { conn.Close(); <-done })
	return p
}

func TestWSLargeImageBecomesFileAndReassemblesAllChunks(t *testing.T) {
	data := bytes.Repeat([]byte{7}, wecomWSImageMaxBytes+1)
	var mu sync.Mutex
	chunks := make(map[int][]byte)
	var gotType, gotName string
	var total int
	var sent bool
	p := directMediaWS(t, func(frame wsFrame) (map[string]any, bool) {
		mu.Lock()
		defer mu.Unlock()
		response := map[string]any{"errcode": 0}
		switch frame.Cmd {
		case "aibot_upload_media_init":
			var body struct {
				Type        string
				Filename    string
				TotalChunks int `json:"total_chunks"`
			}
			if err := json.Unmarshal(frame.Body, &body); err != nil {
				t.Error(err)
			}
			gotType, gotName, total = body.Type, body.Filename, body.TotalChunks
			response["body"] = map[string]string{"upload_id": "upload"}
		case "aibot_upload_media_chunk":
			var body struct {
				Index int    `json:"chunk_index"`
				Data  string `json:"base64_data"`
			}
			if err := json.Unmarshal(frame.Body, &body); err != nil {
				t.Error(err)
			}
			decoded, err := base64.StdEncoding.DecodeString(body.Data)
			if err != nil {
				t.Error(err)
			}
			if _, duplicate := chunks[body.Index]; duplicate {
				t.Error("duplicate chunk")
			}
			chunks[body.Index] = decoded
		case "aibot_upload_media_finish":
			if len(chunks) != total {
				t.Error("finish before all chunk ACKs")
			}
			response["body"] = map[string]string{"media_id": "media"}
		case "aibot_send_msg":
			var body struct {
				Type string `json:"msgtype"`
			}
			json.Unmarshal(frame.Body, &body)
			sent = body.Type == "file"
		}
		return response, true
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := p.SendImage(ctx, wsReplyContext{chatID: "chat"}, core.ImageAttachment{FileName: "chart.png", MimeType: "image/png", Data: data}); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	var assembled []byte
	for index := 0; index < total; index++ {
		assembled = append(assembled, chunks[index]...)
	}
	if gotType != "file" || gotName != "chart.png" || !sent || !bytes.Equal(assembled, data) {
		t.Fatalf("bad downgrade/upload: type=%s name=%s sent=%v chunks=%d", gotType, gotName, sent, len(chunks))
	}
}

func TestWSMediaDeliveryStatesAndLostFinalAckNoRetry(t *testing.T) {
	for _, tc := range []struct {
		name, state                       string
		rejectUpload, rejectSend, dropAck bool
	}{
		{"upload-rejected", "DefinitelyFailed", true, false, false},
		{"send-rejected", "DefinitelyFailed", false, true, false},
		{"send-ack-lost", "Unknown", false, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var sends atomic.Int32
			p := directMediaWS(t, func(frame wsFrame) (map[string]any, bool) {
				response := map[string]any{"errcode": 0}
				switch frame.Cmd {
				case "aibot_upload_media_init":
					if tc.rejectUpload {
						response["errcode"] = 500
					}
					response["body"] = map[string]string{"upload_id": "upload"}
				case "aibot_upload_media_finish":
					response["body"] = map[string]string{"media_id": "media"}
				case "aibot_send_msg":
					sends.Add(1)
					if tc.dropAck {
						return nil, false
					}
					if tc.rejectSend {
						response["errcode"] = 500
					}
				}
				return response, true
			})
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()
			err := p.SendFile(ctx, wsReplyContext{chatID: "chat"}, core.FileAttachment{FileName: "a.txt", Data: []byte("a")})
			var receipt interface{ DeliveryState() string }
			if !errors.As(err, &receipt) || receipt.DeliveryState() != tc.state {
				t.Fatalf("state=%v err=%v", receipt, err)
			}
			want := int32(1)
			if tc.rejectUpload {
				want = 0
			}
			if sends.Load() != want {
				t.Fatalf("final send attempted %d times", sends.Load())
			}
		})
	}
	p := &WSPlatform{}
	err := p.SendFile(context.Background(), wsReplyContext{chatID: "chat"}, core.FileAttachment{Data: make([]byte, wecomWSFileMaxBytes+1)})
	var receipt interface{ DeliveryState() string }
	if !errors.As(err, &receipt) || receipt.DeliveryState() != "DefinitelyFailed" {
		t.Fatalf("oversized file not rejected before upload: %v", err)
	}
}
