package blobs_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"

	"github.com/7ar-chat/harness/core/blobs"
)

// memStore is an in-memory blobs.Blobs that records what Put stored.
type memStore struct {
	mu       sync.Mutex
	blobs    map[string][]byte
	max      int64
	putCalls int
	putErr   error
}

func newMemStore(max int64) *memStore {
	return &memStore{blobs: map[string][]byte{}, max: max}
}

func (m *memStore) Put(_ context.Context, key string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.putCalls++
	if m.putErr != nil {
		return m.putErr
	}
	m.blobs[key] = append([]byte(nil), data...)
	return nil
}

func (m *memStore) Get(_ context.Context, key string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	data, ok := m.blobs[key]
	if !ok {
		return nil, blobs.ErrNotFound
	}
	return append([]byte(nil), data...), nil
}

func (m *memStore) Has(_ context.Context, key string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.blobs[key]
	return ok, nil
}

func (m *memStore) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.blobs, key)
	return nil
}

func (m *memStore) MaxBytes() int64 { return m.max }

func (m *memStore) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.blobs)
}

func (m *memStore) content(key string) ([]byte, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	data, ok := m.blobs[key]
	return data, ok
}

// streamStore is a memStore that also implements blobs.StreamPutter, recording
// the call and how much of the reader had been consumed when it was made.
type streamStore struct {
	*memStore
	streamErr error

	mu        sync.Mutex
	calls     int
	key       string
	size      int64
	entryRead int
	data      []byte
}

func (s *streamStore) PutStream(_ context.Context, key string, r io.Reader, size int64) error {
	entryRead := 0
	if rr, ok := r.(*recordingReader); ok {
		entryRead = rr.reads
	}
	s.mu.Lock()
	s.calls++
	s.key = key
	s.size = size
	s.entryRead = entryRead
	s.mu.Unlock()
	if s.streamErr != nil {
		return s.streamErr
	}
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.data = data
	s.mu.Unlock()
	s.memStore.mu.Lock()
	s.memStore.blobs[key] = data
	s.memStore.mu.Unlock()
	return nil
}

func (s *streamStore) streamCall() (calls int, key string, size int64, entryRead int, data []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls, s.key, s.size, s.entryRead, s.data
}

// recordingReader counts its Read calls so a test can prove the reader reaches
// a streaming backend untouched.
type recordingReader struct {
	r     io.Reader
	reads int
}

func (r *recordingReader) Read(p []byte) (int, error) {
	r.reads++
	return r.r.Read(p)
}

func newRecordingReader(s string) *recordingReader {
	return &recordingReader{r: bytes.NewBufferString(s)}
}

func TestPutReaderHandsAStreamPutterTheUnreadReader(t *testing.T) {
	// Arrange
	ctx := context.Background()
	st := &streamStore{memStore: newMemStore(0)}
	rr := newRecordingReader("hello")

	// Act
	err := blobs.PutReader(ctx, st, "k1", rr, 5)

	// Assert
	if err != nil {
		t.Fatalf("PutReader: %v", err)
	}
	calls, key, size, entryRead, data := st.streamCall()
	if calls != 1 {
		t.Fatalf("PutStream calls = %d, want 1", calls)
	}
	if key != "k1" || size != 5 {
		t.Errorf("PutStream got key %q size %d, want key %q size %d", key, size, "k1", int64(5))
	}
	if entryRead != 0 {
		t.Errorf("the reader was read %d times before PutStream, want 0", entryRead)
	}
	if string(data) != "hello" {
		t.Errorf("streamed data = %q, want %q", data, "hello")
	}
	if st.putCalls != 0 {
		t.Errorf("Put calls = %d, want 0: a StreamPutter stores the stream itself", st.putCalls)
	}
}

func TestPutReaderReadsIntoMemoryWithoutAStreamPutter(t *testing.T) {
	cases := []struct {
		name    string
		max     int64
		content string
	}{
		{"no cap", 0, "hello"},
		{"no cap, empty", 0, ""},
		{"no cap accepts a large blob", 0, string(bytes.Repeat([]byte("x"), 1<<20))},
		{"below the cap", 10, "hello"},
		{"exactly the cap", 5, "hello"},
	}
	for _, c := range cases {
		// Arrange
		st := newMemStore(c.max)

		// Act
		err := blobs.PutReader(context.Background(), st, "k1", bytes.NewBufferString(c.content), int64(len(c.content)))

		// Assert
		if err != nil {
			t.Fatalf("%s: PutReader: %v", c.name, err)
		}
		if st.putCalls != 1 {
			t.Errorf("%s: Put calls = %d, want 1", c.name, st.putCalls)
		}
		got, ok := st.content("k1")
		if !ok || !bytes.Equal(got, []byte(c.content)) {
			t.Errorf("%s: stored %q (present %v), want %q", c.name, got, ok, c.content)
		}
	}
}

func TestPutReaderRefusesASizeAboveTheCap(t *testing.T) {
	// Arrange
	st := newMemStore(4)

	// Act
	err := blobs.PutReader(context.Background(), st, "k1", bytes.NewBufferString("hello"), 5)

	// Assert
	if !errors.Is(err, blobs.ErrTooLarge) {
		t.Fatalf("PutReader = %v, want ErrTooLarge", err)
	}
	if st.putCalls != 0 || st.count() != 0 {
		t.Errorf("Put calls = %d, stored %d blobs, want 0 and 0", st.putCalls, st.count())
	}
}

func TestPutReaderRejectsAReaderThatDoesNotHoldSizeBytes(t *testing.T) {
	cases := []struct {
		name    string
		content string
		size    int64
	}{
		{"fewer bytes than size", "hel", 5},
		{"no bytes at all", "", 5},
		{"more bytes than size", "hellooo", 5},
	}
	for _, c := range cases {
		// Arrange
		st := newMemStore(0)

		// Act
		err := blobs.PutReader(context.Background(), st, "k1", bytes.NewBufferString(c.content), c.size)

		// Assert
		if err == nil {
			t.Fatalf("%s: PutReader accepted %d bytes for size %d", c.name, len(c.content), c.size)
		}
		if st.putCalls != 0 || st.count() != 0 {
			t.Errorf("%s: Put calls = %d, stored %d blobs, want 0 and 0", c.name, st.putCalls, st.count())
		}
	}
}

func TestPutReaderReturnsTheStoreErrorUnchanged(t *testing.T) {
	// Arrange
	boom := errors.New("boom")
	putStore := newMemStore(0)
	putStore.putErr = boom
	streamFailing := &streamStore{memStore: newMemStore(0), streamErr: boom}
	cases := []struct {
		name   string
		b      blobs.Blobs
		stored func() int
	}{
		{"Put", putStore, putStore.count},
		{"PutStream", streamFailing, streamFailing.count},
	}
	for _, c := range cases {
		// Act
		err := blobs.PutReader(context.Background(), c.b, "k1", bytes.NewBufferString("hello"), 5)

		// Assert
		if err != boom {
			t.Errorf("%s: PutReader = %v, want the store's error unchanged", c.name, err)
		}
		if n := c.stored(); n != 0 {
			t.Errorf("%s: stored %d blobs, want 0", c.name, n)
		}
	}
}
