package blobs_test

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/7ar-chat/harness/core/blobs"
)

// memBlobs is an in-memory blobs.Blobs for tests: it records every Put and can
// fail on demand.
type memBlobs struct {
	cap    int64
	putErr error

	puts    int
	lastKey string
	stored  map[string][]byte
}

func newMemBlobs(cap int64) *memBlobs {
	return &memBlobs{cap: cap, stored: map[string][]byte{}}
}

func (m *memBlobs) Put(_ context.Context, key string, data []byte) error {
	m.puts++
	m.lastKey = key
	if m.putErr != nil {
		return m.putErr
	}
	if m.cap > 0 && int64(len(data)) > m.cap {
		return blobs.ErrTooLarge
	}
	m.stored[key] = append([]byte(nil), data...)
	return nil
}

func (m *memBlobs) Get(_ context.Context, key string) ([]byte, error) {
	data, ok := m.stored[key]
	if !ok {
		return nil, blobs.ErrNotFound
	}
	return data, nil
}

func (m *memBlobs) Has(_ context.Context, key string) (bool, error) {
	_, ok := m.stored[key]
	return ok, nil
}

func (m *memBlobs) Delete(_ context.Context, key string) error {
	delete(m.stored, key)
	return nil
}

func (m *memBlobs) MaxBytes() int64 { return m.cap }

// streamBlobs is a memBlobs that also implements blobs.StreamPutter, like a
// backend that can take a reader directly. It records the reader's byte count
// at call time so a test can see the reader arrived unread.
type streamBlobs struct {
	*memBlobs
	streamErr error

	streamCalls  int
	lastKey      string
	lastSize     int64
	readerAtCall int
}

func (s *streamBlobs) PutStream(_ context.Context, key string, r io.Reader, size int64) error {
	s.streamCalls++
	s.lastKey = key
	s.lastSize = size
	if cr, ok := r.(*countingReader); ok {
		s.readerAtCall = cr.n
	}
	if s.streamErr != nil {
		return s.streamErr
	}
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	s.stored[key] = data
	return nil
}

// countingReader counts the bytes read from it.
type countingReader struct {
	r io.Reader
	n int
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += n
	return n, err
}

func TestPutReaderStreamsToABackendThatSupportsIt(t *testing.T) {
	// Arrange
	ctx := context.Background()
	backend := &streamBlobs{memBlobs: newMemBlobs(0)}
	r := &countingReader{r: strings.NewReader("hello")}

	// Act
	err := blobs.PutReader(ctx, backend, "preview/tag/abc.pdf", r, 5)

	// Assert
	if err != nil {
		t.Fatalf("PutReader() error = %v, want nil", err)
	}
	if backend.streamCalls != 1 {
		t.Fatalf("PutStream calls = %d, want 1", backend.streamCalls)
	}
	if backend.puts != 0 {
		t.Fatalf("Put calls = %d, want 0 when the backend streams", backend.puts)
	}
	if backend.lastKey != "preview/tag/abc.pdf" || backend.lastSize != 5 {
		t.Fatalf("PutStream got key %q size %d, want %q and 5", backend.lastKey, backend.lastSize, "preview/tag/abc.pdf")
	}
	if backend.readerAtCall != 0 {
		t.Fatalf("reader had already given %d bytes when PutStream was called, want it unread", backend.readerAtCall)
	}
	if got := string(backend.stored["preview/tag/abc.pdf"]); got != "hello" {
		t.Fatalf("stored content = %q, want %q", got, "hello")
	}
}

func TestPutReaderReadsIntoPutWhenTheBackendDoesNotStream(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name    string
		key     string
		payload string
		size    int64
	}{
		{"empty content", "empty", "", 0},
		{"small content", "small", "hello", 5},
		{"content of any size without a cap", "large", strings.Repeat("x", 1<<16), 1 << 16},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// Arrange
			backend := newMemBlobs(0) // no cap

			// Act
			err := blobs.PutReader(ctx, backend, c.key, strings.NewReader(c.payload), c.size)

			// Assert
			if err != nil {
				t.Fatalf("PutReader() error = %v, want nil", err)
			}
			if backend.puts != 1 {
				t.Fatalf("Put calls = %d, want 1", backend.puts)
			}
			if backend.lastKey != c.key {
				t.Fatalf("Put key = %q, want %q", backend.lastKey, c.key)
			}
			if got := string(backend.stored[c.key]); got != c.payload {
				t.Fatalf("stored content = %q (%d bytes), want %q (%d bytes)", got, len(got), c.payload, len(c.payload))
			}
		})
	}
}

func TestPutReaderEnforcesTheBackendCap(t *testing.T) {
	const max = 8
	cases := []struct {
		name    string
		size    int64
		wantErr error
	}{
		{"below the cap", max - 1, nil},
		{"equal to the cap", max, nil},
		{"above the cap", max + 1, blobs.ErrTooLarge},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// Arrange
			ctx := context.Background()
			backend := newMemBlobs(max)
			payload := strings.Repeat("x", int(c.size))

			// Act
			err := blobs.PutReader(ctx, backend, "capped", strings.NewReader(payload), c.size)

			// Assert
			if c.wantErr != nil {
				if !errors.Is(err, c.wantErr) {
					t.Fatalf("PutReader() error = %v, want %v", err, c.wantErr)
				}
				if backend.puts != 0 || len(backend.stored) != 0 {
					t.Fatalf("Put calls = %d, stored keys = %d, want nothing stored", backend.puts, len(backend.stored))
				}
				return
			}
			if err != nil {
				t.Fatalf("PutReader() error = %v, want nil", err)
			}
			if got := string(backend.stored["capped"]); got != payload {
				t.Fatalf("stored content = %q (%d bytes), want %d bytes", got, len(got), c.size)
			}
		})
	}
}

func TestPutReaderRejectsAReaderThatDoesNotHoldSizeBytes(t *testing.T) {
	cases := []struct {
		name    string
		payload string
		size    int64
	}{
		{"fewer bytes than size", "abc", 5},
		{"more bytes than size", "abcdef", 5},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// Arrange
			ctx := context.Background()
			backend := newMemBlobs(0) // no cap, so only the length check can fail

			// Act
			err := blobs.PutReader(ctx, backend, "k", strings.NewReader(c.payload), c.size)

			// Assert
			if err == nil {
				t.Fatalf("PutReader() error = nil, want an error for %d bytes with size %d", len(c.payload), c.size)
			}
			if errors.Is(err, blobs.ErrTooLarge) {
				t.Fatalf("PutReader() error = %v, want a length mismatch, not ErrTooLarge", err)
			}
			if backend.puts != 0 || len(backend.stored) != 0 {
				t.Fatalf("Put calls = %d, stored keys = %d, want nothing stored", backend.puts, len(backend.stored))
			}
		})
	}
}

func TestPutReaderReturnsPutErrorsUnchanged(t *testing.T) {
	// Arrange
	ctx := context.Background()
	putErr := errors.New("disk full")
	backend := newMemBlobs(0)
	backend.putErr = putErr

	// Act
	err := blobs.PutReader(ctx, backend, "k", strings.NewReader("hello"), 5)

	// Assert
	if err != putErr {
		t.Fatalf("PutReader() error = %v, want the backend's error unchanged", err)
	}
	if len(backend.stored) != 0 {
		t.Fatalf("stored keys = %d, want nothing stored", len(backend.stored))
	}
}

func TestPutReaderReturnsPutStreamErrorsUnchanged(t *testing.T) {
	// Arrange
	ctx := context.Background()
	streamErr := errors.New("upload aborted")
	backend := &streamBlobs{memBlobs: newMemBlobs(0), streamErr: streamErr}

	// Act
	err := blobs.PutReader(ctx, backend, "k", strings.NewReader("hello"), 5)

	// Assert
	if err != streamErr {
		t.Fatalf("PutReader() error = %v, want the backend's error unchanged", err)
	}
	if backend.puts != 0 {
		t.Fatalf("Put calls = %d, want 0 when the backend streams", backend.puts)
	}
	if len(backend.stored) != 0 {
		t.Fatalf("stored keys = %d, want nothing stored", len(backend.stored))
	}
}
