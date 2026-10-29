package requests

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/wangluozhe/chttp"
	"github.com/wangluozhe/chttp/httptest"
	"github.com/wangluozhe/requests/url"
)

// countingSession returns a session whose base transport counts dials, so
// connection reuse / release is observable from the test.
func countingSession(t *testing.T) (*Session, *int64) {
	t.Helper()
	session := NewSession()
	var dials int64
	dial := session.transport.DialContext
	session.transport.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		atomic.AddInt64(&dials, 1)
		return dial(ctx, network, addr)
	}
	return session, &dials
}

func okHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Length", "2")
	w.Write([]byte("ok"))
}

// A keep-alive connection is reused while the session lives, and Close()
// releases it so the next request dials a fresh one.
func TestSessionCloseReleasesIdleConnections(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(okHandler))
	defer server.Close()

	session, dials := countingSession(t)
	for i := 0; i < 2; i++ {
		if _, err := session.Get(server.URL, nil); err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
	}
	if got := atomic.LoadInt64(dials); got != 1 {
		t.Fatalf("keep-alive reuse: expected 1 dial, got %d", got)
	}

	session.Close()
	session.Close() // idempotent
	if !session.IsClosed() {
		t.Fatal("IsClosed() = false after Close()")
	}

	if _, err := session.Get(server.URL, nil); err != nil {
		t.Fatalf("request after Close(): %v", err)
	}
	if got := atomic.LoadInt64(dials); got != 2 {
		t.Fatalf("after Close() the idle connection must be gone: expected 2 dials, got %d", got)
	}
}

// IsolatedCookies requests neither read nor write Session.Cookies; a plain
// request still gets the native persistent jar behaviour.
func TestIsolatedCookiesIgnoreSessionJar(t *testing.T) {
	var mu sync.Mutex
	var seen []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen = append(seen, r.Header.Get("Cookie"))
		mu.Unlock()
		w.Header().Set("Set-Cookie", "srv=1; Path=/")
		w.Header().Set("Content-Length", "2")
		w.Write([]byte("ok"))
	}))
	defer server.Close()

	session := NewSession()

	first := url.NewRequest()
	first.IsolatedCookies = true
	first.Cookies = map[string]string{"a": "1"}
	if _, err := session.Get(server.URL, first); err != nil {
		t.Fatalf("isolated request 1: %v", err)
	}

	second := url.NewRequest()
	second.IsolatedCookies = true
	if _, err := session.Get(server.URL, second); err != nil {
		t.Fatalf("isolated request 2: %v", err)
	}

	third := url.NewRequest()
	if _, err := session.Get(server.URL, third); err != nil {
		t.Fatalf("native request 1: %v", err)
	}
	fourth := url.NewRequest()
	if _, err := session.Get(server.URL, fourth); err != nil {
		t.Fatalf("native request 2: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 4 {
		t.Fatalf("expected 4 recorded requests, got %d", len(seen))
	}
	if !strings.Contains(seen[0], "a=1") {
		t.Fatalf("request cookies not sent: %q", seen[0])
	}
	if strings.Contains(seen[1], "srv=1") || strings.Contains(seen[1], "a=1") {
		t.Fatalf("isolated request inherited cookie state: %q", seen[1])
	}
	if strings.Contains(seen[2], "srv=1") {
		t.Fatalf("isolated response must not be stored in the session jar: %q", seen[2])
	}
	if !strings.Contains(seen[3], "srv=1") {
		t.Fatalf("native session jar must persist cookies: %q", seen[3])
	}
}

// Send() must not write the default HeadersID into the shared session field.
func TestSendDoesNotMutateSessionHTTP2Settings(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(okHandler))
	defer server.Close()

	session := NewSession()
	session.HTTP2Settings = &http.HTTP2Settings{}
	if _, err := session.Get(server.URL, nil); err != nil {
		t.Fatalf("request: %v", err)
	}
	if session.HTTP2Settings.HeadersID != 0 {
		t.Fatalf("Session.HTTP2Settings.HeadersID mutated to %d", session.HTTP2Settings.HeadersID)
	}
}

// Concurrent isolated-cookie requests on one session must not share cookie
// state (run with -race).
func TestConcurrentIsolatedCookieRequests(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := r.Header.Get("Cookie")
		w.Header().Set("Content-Length", fmt.Sprint(len(body)))
		w.Write([]byte(body))
	}))
	defer server.Close()

	session := NewSession()
	var wg sync.WaitGroup
	for worker := 0; worker < 16; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			value := fmt.Sprintf("t%d", worker)
			want := "tok=" + value
			for i := 0; i < 20; i++ {
				request := url.NewRequest()
				request.IsolatedCookies = true
				request.Cookies = map[string]string{"tok": value}
				response, err := session.Get(server.URL, request)
				if err != nil {
					t.Errorf("worker %d: %v", worker, err)
					return
				}
				if got := response.Text(); got != want {
					t.Errorf("worker %d: cookie echo %q, want %q", worker, got, want)
					return
				}
			}
		}(worker)
	}
	wg.Wait()
}
