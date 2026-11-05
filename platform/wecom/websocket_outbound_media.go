package wecom

import (
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chenhg5/cc-connect/core"
)

const (
	wecomWSUploadChunkSize = 512 * 1024
	wecomWSUploadMaxChunks = 100
	wecomWSImageMaxBytes   = 10 * 1024 * 1024
	wecomWSFileMaxBytes    = 20 * 1024 * 1024
)

// SendImage uploads and sends an image through the WeCom AI Bot WebSocket API.
func (p *WSPlatform) SendImage(ctx context.Context, rctx any, img core.ImageAttachment) error {
	rc, ok := rctx.(wsReplyContext)
	if !ok {
		return mediaFailure(fmt.Errorf("wecom-ws: SendImage: invalid reply context type %T", rctx))
	}
	if rc.chatID == "" {
		return mediaFailure(fmt.Errorf("wecom-ws: chatID is empty, cannot send image"))
	}
	if len(img.Data) == 0 {
		return mediaFailure(fmt.Errorf("wecom-ws: image data is empty"))
	}

	if len(img.Data) > wecomWSImageMaxBytes {
		return p.SendFile(ctx, rctx, core.FileAttachment{FileName: wsImageFileName(img), MimeType: img.MimeType, Data: img.Data})
	}
	mediaID, err := p.uploadWSMedia(ctx, "image", wsImageFileName(img), img.Data)
	if err != nil {
		return mediaFailure(fmt.Errorf("wecom-ws: send image: %w", err))
	}
	if err := p.sendWSMediaMessage(ctx, rc.chatID, "image", mediaID); err != nil {
		return fmt.Errorf("wecom-ws: send image: %w", err)
	}
	return nil
}

func (p *WSPlatform) uploadWSMedia(ctx context.Context, mediaType, filename string, data []byte) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	totalChunks := (len(data) + wecomWSUploadChunkSize - 1) / wecomWSUploadChunkSize
	if totalChunks == 0 {
		return "", fmt.Errorf("empty media data")
	}
	if totalChunks > wecomWSUploadMaxChunks {
		return "", fmt.Errorf("media too large: %d chunks exceeds maximum %d", totalChunks, wecomWSUploadMaxChunks)
	}

	sum := md5.Sum(data)
	initReqID := p.generateReqID("aibot_upload_media_init")
	initFrame := map[string]any{
		"cmd":     "aibot_upload_media_init",
		"headers": map[string]string{"req_id": initReqID},
		"body": map[string]any{
			"type":         mediaType,
			"filename":     filename,
			"total_size":   len(data),
			"total_chunks": totalChunks,
			"md5":          hex.EncodeToString(sum[:]),
		},
	}
	initResp, err := p.writeAndWaitMediaFrame(ctx, initFrame, initReqID)
	if err != nil {
		return "", fmt.Errorf("upload init: %w", err)
	}
	var initBody struct {
		UploadID string `json:"upload_id"`
	}
	if err := json.Unmarshal(initResp.Body, &initBody); err != nil {
		return "", fmt.Errorf("decode upload init response: %w", err)
	}
	if initBody.UploadID == "" {
		return "", fmt.Errorf("upload init: empty upload_id")
	}

	err = uploadWSChunks(ctx, totalChunks, func(ctx context.Context, i int) error {
		start := i * wecomWSUploadChunkSize
		end := start + wecomWSUploadChunkSize
		if end > len(data) {
			end = len(data)
		}
		reqID := p.generateReqID("aibot_upload_media_chunk")
		chunkFrame := map[string]any{
			"cmd":     "aibot_upload_media_chunk",
			"headers": map[string]string{"req_id": reqID},
			"body": map[string]any{
				"upload_id":   initBody.UploadID,
				"chunk_index": i,
				"base64_data": base64.StdEncoding.EncodeToString(data[start:end]),
			},
		}
		if _, err := p.writeAndWaitMediaFrame(ctx, chunkFrame, reqID); err != nil {
			return fmt.Errorf("upload chunk %d: %w", i, err)
		}
		return nil
	})
	if err != nil {
		return "", err
	}

	finishReqID := p.generateReqID("aibot_upload_media_finish")
	finishFrame := map[string]any{
		"cmd":     "aibot_upload_media_finish",
		"headers": map[string]string{"req_id": finishReqID},
		"body": map[string]any{
			"upload_id": initBody.UploadID,
		},
	}
	finishResp, err := p.writeAndWaitMediaFrame(ctx, finishFrame, finishReqID)
	if err != nil {
		return "", fmt.Errorf("upload finish: %w", err)
	}
	var finishBody struct {
		MediaID string `json:"media_id"`
	}
	if err := json.Unmarshal(finishResp.Body, &finishBody); err != nil {
		return "", fmt.Errorf("decode upload finish response: %w", err)
	}
	if finishBody.MediaID == "" {
		return "", fmt.Errorf("upload finish: empty media_id")
	}
	return finishBody.MediaID, nil
}

func (p *WSPlatform) sendWSMediaMessage(ctx context.Context, chatID, mediaType, mediaID string) error {
	if err := ctx.Err(); err != nil {
		return mediaFailure(err)
	}
	reqID := p.generateReqID("aibot_send_msg")
	frame := map[string]any{
		"cmd":     "aibot_send_msg",
		"headers": map[string]string{"req_id": reqID},
		"body": map[string]any{
			"chatid":  chatID,
			"msgtype": mediaType,
			mediaType: map[string]string{
				"media_id": mediaID,
			},
		},
	}
	result, err := p.writeAndWaitMediaResult(ctx, frame, reqID)
	if err != nil {
		return &wsMediaDeliveryError{state: "Unknown", err: err}
	}
	if result.err != nil {
		return mediaFailure(result.err)
	}
	return nil
}

func wsImageFileName(img core.ImageAttachment) string {
	name := filepath.Base(strings.TrimSpace(img.FileName))
	if name != "" && name != "." {
		return name
	}
	switch strings.ToLower(img.MimeType) {
	case "image/jpeg", "image/jpg":
		return "image.jpg"
	case "image/gif":
		return "image.gif"
	case "image/webp":
		return "image.webp"
	default:
		return "image.png"
	}
}

// SendFile uploads and sends a file through the WeCom AI Bot WebSocket API.
func (p *WSPlatform) SendFile(ctx context.Context, rctx any, file core.FileAttachment) error {
	rc, ok := rctx.(wsReplyContext)
	if !ok {
		return mediaFailure(fmt.Errorf("wecom-ws: SendFile: invalid reply context type %T", rctx))
	}
	if rc.chatID == "" {
		return mediaFailure(fmt.Errorf("wecom-ws: chatID is empty, cannot send file"))
	}
	if len(file.Data) == 0 {
		return mediaFailure(fmt.Errorf("wecom-ws: file data is empty"))
	}

	if len(file.Data) > wecomWSFileMaxBytes {
		return mediaFailure(fmt.Errorf("wecom-ws: file exceeds 20 MiB quota"))
	}
	mediaID, err := p.uploadWSMedia(ctx, "file", wsFileFileName(file), file.Data)
	if err != nil {
		return mediaFailure(fmt.Errorf("wecom-ws: send file: %w", err))
	}
	if err := p.sendWSMediaMessage(ctx, rc.chatID, "file", mediaID); err != nil {
		return fmt.Errorf("wecom-ws: send file: %w", err)
	}
	return nil
}

func wsFileFileName(file core.FileAttachment) string {
	name := filepath.Base(strings.TrimSpace(file.FileName))
	if name != "" && name != "." {
		return name
	}
	switch strings.ToLower(file.MimeType) {
	case "text/html":
		return "file.html"
	case "application/pdf":
		return "file.pdf"
	case "text/plain":
		return "file.txt"
	case "text/markdown":
		return "file.md"
	case "application/json":
		return "file.json"
	case "application/zip":
		return "file.zip"
	default:
		return "file.bin"
	}
}

var _ core.ImageSender = (*WSPlatform)(nil)
var _ core.FileSender = (*WSPlatform)(nil)

// wsMediaDeliveryError is queryable across package boundaries via errors.As.
// Unknown means the final message may have reached WeCom; it must not be retried.
type wsMediaDeliveryError struct {
	state string
	err   error
}

func (e *wsMediaDeliveryError) Error() string         { return e.err.Error() }
func (e *wsMediaDeliveryError) Unwrap() error         { return e.err }
func (e *wsMediaDeliveryError) DeliveryState() string { return e.state }
func mediaFailure(err error) error                    { return &wsMediaDeliveryError{state: "DefinitelyFailed", err: err} }

func wsUploadConcurrency(chunks int) int {
	if chunks <= 4 {
		return chunks
	}
	if chunks <= 10 {
		return 3
	}
	return 2
}

// Each slice is attempted once. A lost ACK aborts this upload; it is never
// retried with a fresh request ID. Finish waits until every slice ACK succeeds.
func uploadWSChunks(parent context.Context, total int, send func(context.Context, int) error) error {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	var next atomic.Int64
	var wg sync.WaitGroup
	var once sync.Once
	var firstErr error
	for worker := 0; worker < wsUploadConcurrency(total); worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				index := int(next.Add(1) - 1)
				if index >= total {
					return
				}
				if err := send(ctx, index); err != nil {
					once.Do(func() { firstErr = err; cancel() })
					return
				}
			}
		}()
	}
	wg.Wait()
	if firstErr != nil {
		return firstErr
	}
	return ctx.Err()
}

// Media writes must honor the upload budget even when the socket stops reading.
// The same mutex used by writeJSON protects setting/resetting its write deadline.
func (p *WSPlatform) writeAndWaitMediaResult(ctx context.Context, frame map[string]any, reqID string) (wsAckResult, error) {
	ch := make(chan wsAckResult, 1)
	p.pendingAcks.Store(reqID, ch)
	defer p.pendingAcks.Delete(reqID)
	p.mu.Lock()
	err := ctx.Err()
	if err == nil {
		if p.conn == nil {
			err = fmt.Errorf("wecom-ws: not connected")
		} else {
			deadline := time.Now().Add(wsMediaAckTimeout)
			if budget, ok := ctx.Deadline(); ok && budget.Before(deadline) {
				deadline = budget
			}
			err = p.conn.SetWriteDeadline(deadline)
			if err == nil {
				err = p.conn.WriteJSON(frame)
			}
			_ = p.conn.SetWriteDeadline(time.Time{})
		}
	}
	p.mu.Unlock()
	if err != nil {
		return wsAckResult{}, err
	}
	timer := time.NewTimer(wsMediaAckTimeout)
	defer timer.Stop()
	select {
	case result := <-ch:
		return result, nil
	case <-ctx.Done():
		return wsAckResult{}, ctx.Err()
	case <-timer.C:
		return wsAckResult{}, errWSAckTimeout
	}
}

func (p *WSPlatform) writeAndWaitMediaFrame(ctx context.Context, frame map[string]any, reqID string) (wsFrame, error) {
	result, err := p.writeAndWaitMediaResult(ctx, frame, reqID)
	if err != nil {
		return wsFrame{}, err
	}
	if result.err != nil {
		return wsFrame{}, result.err
	}
	return result.frame, nil
}
