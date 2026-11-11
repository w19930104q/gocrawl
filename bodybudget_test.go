package gocrawl

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/PuerkitoBio/goquery"
)

// budgetTestExt records hook calls and can override Fetch/Visit/FetchedRobots.
type budgetTestExt struct {
	DefaultExtender

	mu sync.Mutex

	fetchCounts      map[string]int
	visitCounts      map[string]int
	visitedCounts    map[string]int
	disallowedCounts map[string]int
	robotsFetched    map[string]int
	robotsDigests    map[string]string
	errs             []*CrawlError
	requestGetCount  int

	hookFetch         func(*URLContext, string, bool) (*http.Response, error)
	hookVisit         func(*URLContext, *http.Response, *goquery.Document) (interface{}, bool)
	hookFetchedRobots func(*URLContext, *http.Response)

	cachedRobots []byte
	serveCache   bool
}

func newBudgetTestExt() *budgetTestExt {
	return &budgetTestExt{
		fetchCounts:      make(map[string]int),
		visitCounts:      make(map[string]int),
		visitedCounts:    make(map[string]int),
		disallowedCounts: make(map[string]int),
		robotsFetched:    make(map[string]int),
		robotsDigests:    make(map[string]string),
	}
}

func (x *budgetTestExt) Fetch(ctx *URLContext, userAgent string, headRequest bool) (*http.Response, error) {
	x.mu.Lock()
	x.fetchCounts[ctx.URL().Path]++
	h := x.hookFetch
	x.mu.Unlock()
	if h != nil {
		return h(ctx, userAgent, headRequest)
	}
	return x.DefaultExtender.Fetch(ctx, userAgent, headRequest)
}

func (x *budgetTestExt) RequestRobots(ctx *URLContext, robotAgent string) ([]byte, bool) {
	x.mu.Lock()
	defer x.mu.Unlock()
	if x.serveCache {
		return x.cachedRobots, false
	}
	return x.DefaultExtender.RequestRobots(ctx, robotAgent)
}

func (x *budgetTestExt) RequestGet(ctx *URLContext, headRes *http.Response) bool {
	x.mu.Lock()
	x.requestGetCount++
	x.mu.Unlock()
	return x.DefaultExtender.RequestGet(ctx, headRes)
}

func (x *budgetTestExt) FetchedRobots(ctx *URLContext, res *http.Response) {
	host := ctx.URL().Host
	x.mu.Lock()
	x.robotsFetched[host]++
	h := x.hookFetchedRobots
	x.mu.Unlock()
	if h != nil {
		h(ctx, res)
		return
	}
	d, err := digestReader(res.Body)
	x.mu.Lock()
	if err == nil {
		x.robotsDigests[host] = d
	}
	x.mu.Unlock()
}

func (x *budgetTestExt) Visit(ctx *URLContext, res *http.Response, doc *goquery.Document) (interface{}, bool) {
	x.mu.Lock()
	x.visitCounts[ctx.URL().Path]++
	h := x.hookVisit
	x.mu.Unlock()
	if h != nil {
		return h(ctx, res, doc)
	}
	return x.DefaultExtender.Visit(ctx, res, doc)
}

func (x *budgetTestExt) Visited(ctx *URLContext, harvested interface{}) {
	x.mu.Lock()
	x.visitedCounts[ctx.URL().Path]++
	x.mu.Unlock()
}

func (x *budgetTestExt) Disallowed(ctx *URLContext) {
	x.mu.Lock()
	x.disallowedCounts[ctx.URL().Path]++
	x.mu.Unlock()
}

func (x *budgetTestExt) Error(err *CrawlError) {
	x.mu.Lock()
	x.errs = append(x.errs, err)
	x.mu.Unlock()
}

func (x *budgetTestExt) fetchCount(path string) int {
	x.mu.Lock()
	defer x.mu.Unlock()
	return x.fetchCounts[path]
}

func (x *budgetTestExt) visitCount(path string) int {
	x.mu.Lock()
	defer x.mu.Unlock()
	return x.visitCounts[path]
}

func (x *budgetTestExt) visitedCount(path string) int {
	x.mu.Lock()
	defer x.mu.Unlock()
	return x.visitedCounts[path]
}

func (x *budgetTestExt) disallowedCount(path string) int {
	x.mu.Lock()
	defer x.mu.Unlock()
	return x.disallowedCounts[path]
}

func (x *budgetTestExt) robotsFetchedCount(host string) int {
	x.mu.Lock()
	defer x.mu.Unlock()
	return x.robotsFetched[host]
}

func (x *budgetTestExt) robotsDigest(host string) (string, bool) {
	x.mu.Lock()
	defer x.mu.Unlock()
	d, ok := x.robotsDigests[host]
	return d, ok
}

func (x *budgetTestExt) errSnapshot() []*CrawlError {
	x.mu.Lock()
	defer x.mu.Unlock()
	return append([]*CrawlError(nil), x.errs...)
}

func (x *budgetTestExt) getRequestGetCount() int {
	x.mu.Lock()
	defer x.mu.Unlock()
	return x.requestGetCount
}

func digestReader(r io.Reader) (string, error) {
	h := sha256.New()
	if _, err := io.Copy(h, r); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func digestBytes(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func countKind(errs []*CrawlError, kind CrawlErrorKind) int {
	n := 0
	for _, e := range errs {
		if e.Kind == kind {
			n++
		}
	}
	return n
}

func newBudgetCrawler(ext *budgetTestExt, maxReplayBuffer, maxBodyBytes int) *Crawler {
	opts := NewOptions(ext)
	opts.CrawlDelay = 0
	opts.MaxReplayBuffer = maxReplayBuffer
	opts.MaxBodyBytes = maxBodyBytes
	opts.LogFlags = LogNone
	return NewCrawlerWithOptions(opts)
}

// usePrivateTempDir points TMPDIR at a fresh directory and returns it. The
// caller can assert that no temporary files are left behind.
func usePrivateTempDir(t *testing.T) (string, func()) {
	t.Helper()
	dir := t.TempDir()
	old, had := os.LookupEnv("TMPDIR")
	if err := os.Setenv("TMPDIR", dir); err != nil {
		t.Fatalf("set TMPDIR: %v", err)
	}
	restore := func() {
		if had {
			_ = os.Setenv("TMPDIR", old)
		} else {
			_ = os.Unsetenv("TMPDIR")
		}
	}
	return dir, restore
}

func assertNoFiles(t *testing.T, dir string) {
	t.Helper()
	ents, err := ioutil.ReadDir(dir)
	if err != nil {
		t.Fatalf("read temp dir: %v", err)
	}
	if len(ents) != 0 {
		names := make([]string, 0, len(ents))
		for _, e := range ents {
			names = append(names, e.Name())
		}
		t.Fatalf("temporary files left behind: %v", names)
	}
}

type budgetServer struct {
	robots []byte
	pages  map[string][]byte
	// chunked pages are written in two flushes without Content-Length.
	chunked map[string][]byte
	// gzip pages are served with Content-Encoding: gzip.
	gzip map[string][]byte
}

func newBudgetServer(t *testing.T, cfg budgetServer) *httptest.Server {
	t.Helper()
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		if path == "/robots.txt" {
			if cfg.robots != nil {
				w.Header().Set("Content-Type", "text/plain")
				_, _ = w.Write(cfg.robots)
			}
			return
		}
		if b, ok := cfg.pages[path]; ok {
			w.Header().Set("Content-Type", "text/html")
			_, _ = w.Write(b)
			return
		}
		if b, ok := cfg.chunked[path]; ok {
			w.Header().Set("Content-Type", "text/html")
			w.WriteHeader(http.StatusOK)
			half := len(b) / 2
			_, _ = w.Write(b[:half])
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			_, _ = w.Write(b[half:])
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			return
		}
		if b, ok := cfg.gzip[path]; ok {
			var buf bytes.Buffer
			zw := gzip.NewWriter(&buf)
			_, _ = zw.Write(b)
			_ = zw.Close()
			w.Header().Set("Content-Type", "text/html")
			w.Header().Set("Content-Encoding", "gzip")
			_, _ = w.Write(buf.Bytes())
			return
		}
		http.NotFound(w, r)
	})
	return httptest.NewServer(handler)
}

// --- direct collector unit tests -----------------------------------------

func TestCollectBudgetBodyBoundaries(t *testing.T) {
	dir, restore := usePrivateTempDir(t)
	defer restore()

	t.Run("in memory at replay buffer limit", func(t *testing.T) {
		body := []byte("12345678")
		sb, err := collectBudgetBody(bytes.NewReader(body), len(body), len(body)+1)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if sb.path != "" || sb.file != nil {
			t.Fatalf("body should stay in memory, got path %q", sb.path)
		}
		if sb.size != len(body) {
			t.Fatalf("size: want %d, got %d", len(body), sb.size)
		}
		r, err := sb.newReader()
		if err != nil {
			t.Fatal(err)
		}
		d, err := digestReader(r)
		_ = r.Close()
		if err != nil {
			t.Fatal(err)
		}
		if d != digestBytes(body) {
			t.Fatalf("digest mismatch")
		}
		_ = sb.Close()
		assertNoFiles(t, dir)
	})

	t.Run("spill just above replay buffer limit", func(t *testing.T) {
		body := []byte("123456789")
		sb, err := collectBudgetBody(bytes.NewReader(body), len(body)-1, len(body))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if sb.path == "" {
			t.Fatal("body should have been spilled to a temporary file")
		}
		if len(sb.data) != 0 {
			t.Fatalf("spilled body should not keep a replay buffer, got %d bytes", len(sb.data))
		}
		r, err := sb.newReader()
		if err != nil {
			t.Fatal(err)
		}
		d, err := digestReader(r)
		_ = r.Close()
		if err != nil {
			t.Fatal(err)
		}
		if d != digestBytes(body) {
			t.Fatalf("digest mismatch")
		}
		path := sb.path
		if err := sb.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("temporary file was not removed: %v", err)
		}
		assertNoFiles(t, dir)
	})

	t.Run("exactly body limit", func(t *testing.T) {
		body := []byte("0123456789")
		sb, err := collectBudgetBody(bytes.NewReader(body), 2, len(body))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if sb.size != len(body) {
			t.Fatalf("size: want %d, got %d", len(body), sb.size)
		}
		_ = sb.Close()
		assertNoFiles(t, dir)
	})

	t.Run("just over body limit", func(t *testing.T) {
		body := []byte("0123456789X")
		_, err := collectBudgetBody(bytes.NewReader(body), 2, len(body)-1)
		if !errors.Is(err, ErrBodyTooLarge) {
			t.Fatalf("want ErrBodyTooLarge, got %v", err)
		}
		assertNoFiles(t, dir)
	})

	t.Run("read failure", func(t *testing.T) {
		rd := &failingReader{data: []byte("0123456789"), err: errTestRead}
		if _, err := collectBudgetBody(rd, 2, 100); !errors.Is(err, errTestRead) {
			t.Fatalf("want read error, got %v", err)
		}
		assertNoFiles(t, dir)
	})

	t.Run("spill failure", func(t *testing.T) {
		notDir := dir + "/not-a-dir"
		if err := ioutil.WriteFile(notDir, []byte("x"), 0600); err != nil {
			t.Fatal(err)
		}
		old, had := os.LookupEnv("TMPDIR")
		if err := os.Setenv("TMPDIR", notDir); err != nil {
			t.Fatal(err)
		}
		defer func() {
			if had {
				_ = os.Setenv("TMPDIR", old)
			} else {
				_ = os.Unsetenv("TMPDIR")
			}
		}()
		_, err := collectBudgetBody(bytes.NewReader([]byte("0123456789")), 2, 100)
		if !errors.Is(err, ErrBodySpill) {
			t.Fatalf("want ErrBodySpill, got %v", err)
		}
	})
}

var errTestRead = errors.New("test read failure")

type failingReader struct {
	data   []byte
	err    error
	closes int
}

func (r *failingReader) Read(p []byte) (int, error) {
	if len(r.data) > 0 {
		n := copy(p, r.data)
		r.data = r.data[n:]
		return n, nil
	}
	return 0, r.err
}

func (r *failingReader) Close() error {
	r.closes++
	return nil
}

func TestBodyBudgetAliasOptionNames(t *testing.T) {
	dir, restore := usePrivateTempDir(t)
	defer restore()

	body := []byte(strings.Repeat("a", 100))
	ext := newBudgetTestExt()
	srv := newBudgetServer(t, budgetServer{pages: map[string][]byte{"/page": body}})
	defer srv.Close()

	opts := NewOptions(ext)
	opts.CrawlDelay = 0
	opts.LogFlags = LogNone
	opts.MaxReplayBufferBytes = 16
	opts.MaxBodySize = len(body) - 1
	c := NewCrawlerWithOptions(opts)
	if err := c.Run(srv.URL + "/page"); err != nil {
		t.Fatalf("run: %v", err)
	}
	errs := ext.errSnapshot()
	if len(errs) != 1 || errs[0].Kind != CekBodyTooLarge {
		t.Fatalf("alias limits should be honoured, got %v", errs)
	}
	if c.visits != 0 {
		t.Fatalf("failed page must not be visited")
	}
	assertNoFiles(t, dir)
}

// --- web page budget tests -----------------------------------------------

func TestBodyBudgetPageBoundaries(t *testing.T) {
	tests := []struct {
		name        string
		body        []byte
		maxReplay   int
		maxBody     int
		wantVisit   bool
		wantKind    CrawlErrorKind
		wantErrKind bool
	}{
		{
			name:      "body exactly at L is spilled and still complete",
			body:      []byte(strings.Repeat("x", 100)),
			maxReplay: 50,
			maxBody:   100,
			wantVisit: true,
		},
		{
			name:      "body exactly at M stays in memory",
			body:      []byte(strings.Repeat("x", 100)),
			maxReplay: 100,
			maxBody:   100,
			wantVisit: true,
		},
		{
			name:        "body above L fails",
			body:        []byte(strings.Repeat("x", 101)),
			maxReplay:   50,
			maxBody:     100,
			wantErrKind: true,
			wantKind:    CekBodyTooLarge,
		},
		{
			name:        "body far above L fails",
			body:        []byte(strings.Repeat("x", 500)),
			maxReplay:   50,
			maxBody:     100,
			wantErrKind: true,
			wantKind:    CekBodyTooLarge,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir, restore := usePrivateTempDir(t)
			defer restore()

			ext := newBudgetTestExt()
			var gotDigest string
			var gotReadErr error
			ext.hookVisit = func(_ *URLContext, res *http.Response, _ *goquery.Document) (interface{}, bool) {
				gotDigest, gotReadErr = digestReader(res.Body)
				return nil, true
			}

			srv := newBudgetServer(t, budgetServer{
				pages: map[string][]byte{"/page": tc.body},
			})
			defer srv.Close()

			c := newBudgetCrawler(ext, tc.maxReplay, tc.maxBody)
			if err := c.Run(srv.URL + "/page"); err != nil {
				t.Fatalf("run: %v", err)
			}

			errs := ext.errSnapshot()
			if tc.wantErrKind {
				if len(errs) != 1 {
					t.Fatalf("want exactly one error, got %d: %v", len(errs), errs)
				}
				if errs[0].Kind != tc.wantKind {
					t.Fatalf("want error kind %v, got %v (%v)", tc.wantKind, errs[0].Kind, errs[0])
				}
				if errors.Is(errs[0].Err, ErrBodyTooLarge) != (tc.wantKind == CekBodyTooLarge) {
					t.Fatalf("underlying error should identify the failure: %v", errs[0].Err)
				}
			} else if len(errs) != 0 {
				t.Fatalf("want no error, got %v", errs)
			}

			if tc.wantVisit {
				if ext.visitCount("/page") != 1 || ext.visitedCount("/page") != 1 {
					t.Fatalf("want one Visit and one Visited, got %d/%d", ext.visitCount("/page"), ext.visitedCount("/page"))
				}
				if gotReadErr != nil {
					t.Fatalf("visit read: %v", gotReadErr)
				}
				if gotDigest != digestBytes(tc.body) {
					t.Fatalf("Visit did not receive the complete body")
				}
				if c.visits != 1 {
					t.Fatalf("want one successful visit, got %d", c.visits)
				}
			} else {
				if ext.visitCount("/page") != 0 || ext.visitedCount("/page") != 0 {
					t.Fatalf("Visit/Visited must not run, got %d/%d", ext.visitCount("/page"), ext.visitedCount("/page"))
				}
				if c.visits != 0 {
					t.Fatalf("failed page must not count as a visit, got %d", c.visits)
				}
			}
			assertNoFiles(t, dir)
		})
	}
}

func TestBodyBudgetNoLinkExtractionOnFailure(t *testing.T) {
	dir, restore := usePrivateTempDir(t)
	defer restore()

	tail := `<a href="/child">child</a>`
	body := []byte(strings.Repeat("x", 300) + tail)

	ext := newBudgetTestExt()
	srv := newBudgetServer(t, budgetServer{
		pages: map[string][]byte{
			"/page":  body,
			"/child": []byte("<html>child</html>"),
		},
	})
	defer srv.Close()

	c := newBudgetCrawler(ext, 10, len(body)-1)
	if err := c.Run(srv.URL + "/page"); err != nil {
		t.Fatalf("run: %v", err)
	}

	errs := ext.errSnapshot()
	if len(errs) != 1 || errs[0].Kind != CekBodyTooLarge {
		t.Fatalf("want one BodyTooLarge error, got %v", errs)
	}
	if ext.fetchCount("/child") != 0 || ext.visitCount("/child") != 0 {
		t.Fatalf("links must not be extracted when the body fails, child fetched/visited %d/%d",
			ext.fetchCount("/child"), ext.visitCount("/child"))
	}
	if c.visits != 0 {
		t.Fatalf("want no successful visit, got %d", c.visits)
	}
	assertNoFiles(t, dir)
}

func TestBodyBudgetSpilledParserAndLinks(t *testing.T) {
	dir, restore := usePrivateTempDir(t)
	defer restore()

	tail := `<a href="/child">child</a>`
	body := []byte(strings.Repeat("x", 200) + tail)

	ext := newBudgetTestExt()
	ext.hookVisit = func(_ *URLContext, res *http.Response, _ *goquery.Document) (interface{}, bool) {
		if _, err := digestReader(res.Body); err != nil {
			t.Fatalf("read body: %v", err)
		}
		return nil, true
	}

	srv := newBudgetServer(t, budgetServer{
		pages: map[string][]byte{
			"/page":  body,
			"/child": []byte("<html>child</html>"),
		},
	})
	defer srv.Close()

	// M is smaller than the body, so both parsing and visiting replay from
	// the temporary file. The link must still be found.
	c := newBudgetCrawler(ext, 16, len(body))
	if err := c.Run(srv.URL + "/page"); err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(ext.errSnapshot()) != 0 {
		t.Fatalf("unexpected errors: %v", ext.errSnapshot())
	}
	if ext.visitedCount("/page") != 1 || ext.visitedCount("/child") != 1 {
		t.Fatalf("want page and child visited, got %d/%d",
			ext.visitedCount("/page"), ext.visitedCount("/child"))
	}
	if c.visits != 2 {
		t.Fatalf("want 2 visits, got %d", c.visits)
	}
	assertNoFiles(t, dir)
}

func TestBodyBudgetUnknownLength(t *testing.T) {
	body := []byte(strings.Repeat("y", 128))

	for _, tc := range []struct {
		name    string
		limit   int
		wantErr bool
	}{
		{name: "chunked exactly at L", limit: len(body)},
		{name: "chunked above L", limit: len(body) - 1, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir, restore := usePrivateTempDir(t)
			defer restore()

			ext := newBudgetTestExt()
			var gotDigest string
			var contentLength int64
			ext.hookFetch = func(ctx *URLContext, userAgent string, headRequest bool) (*http.Response, error) {
				res, err := ext.DefaultExtender.Fetch(ctx, userAgent, headRequest)
				if err == nil && ctx.URL().Path == "/page" {
					contentLength = res.ContentLength
				}
				return res, err
			}
			ext.hookVisit = func(_ *URLContext, res *http.Response, _ *goquery.Document) (interface{}, bool) {
				var err error
				gotDigest, err = digestReader(res.Body)
				if err != nil {
					t.Fatalf("read body: %v", err)
				}
				return nil, true
			}

			srv := newBudgetServer(t, budgetServer{
				chunked: map[string][]byte{"/page": body},
			})
			defer srv.Close()

			c := newBudgetCrawler(ext, 16, tc.limit)
			if err := c.Run(srv.URL + "/page"); err != nil {
				t.Fatalf("run: %v", err)
			}
			if contentLength != -1 {
				t.Fatalf("expected an unknown body length, got Content-Length %d", contentLength)
			}

			errs := ext.errSnapshot()
			if tc.wantErr {
				if len(errs) != 1 || errs[0].Kind != CekBodyTooLarge {
					t.Fatalf("want one BodyTooLarge error, got %v", errs)
				}
				if ext.visitedCount("/page") != 0 || c.visits != 0 {
					t.Fatalf("failed page must not be visited")
				}
			} else {
				if len(errs) != 0 {
					t.Fatalf("unexpected errors: %v", errs)
				}
				if gotDigest != digestBytes(body) {
					t.Fatalf("Visit did not receive the complete body")
				}
				if ext.visitedCount("/page") != 1 || c.visits != 1 {
					t.Fatalf("want one visit, got %d/%d", ext.visitedCount("/page"), c.visits)
				}
			}
			assertNoFiles(t, dir)
		})
	}
}

func TestBodyBudgetGzip(t *testing.T) {
	body := []byte(strings.Repeat("z", 4096))
	for _, tc := range []struct {
		name    string
		limit   int
		wantErr bool
	}{
		{name: "decompressed size exactly at L", limit: len(body)},
		{name: "decompressed size above L", limit: len(body) - 1, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir, restore := usePrivateTempDir(t)
			defer restore()

			ext := newBudgetTestExt()
			var gotDigest string
			var uncompressed bool
			ext.hookFetch = func(ctx *URLContext, userAgent string, headRequest bool) (*http.Response, error) {
				res, err := ext.DefaultExtender.Fetch(ctx, userAgent, headRequest)
				if err == nil && ctx.URL().Path == "/page" {
					uncompressed = res.Uncompressed
				}
				return res, err
			}
			ext.hookVisit = func(_ *URLContext, res *http.Response, _ *goquery.Document) (interface{}, bool) {
				var err error
				gotDigest, err = digestReader(res.Body)
				if err != nil {
					t.Fatalf("read body: %v", err)
				}
				return nil, true
			}

			srv := newBudgetServer(t, budgetServer{
				gzip: map[string][]byte{"/page": body},
			})
			defer srv.Close()

			c := newBudgetCrawler(ext, 64, tc.limit)
			if err := c.Run(srv.URL + "/page"); err != nil {
				t.Fatalf("run: %v", err)
			}
			if !uncompressed {
				t.Fatalf("expected the HTTP client to decompress the gzip body")
			}

			errs := ext.errSnapshot()
			if tc.wantErr {
				if len(errs) != 1 || errs[0].Kind != CekBodyTooLarge {
					t.Fatalf("want one BodyTooLarge error, got %v", errs)
				}
				if c.visits != 0 || ext.visitedCount("/page") != 0 {
					t.Fatalf("failed page must not be visited")
				}
			} else {
				if len(errs) != 0 {
					t.Fatalf("unexpected errors: %v", errs)
				}
				if gotDigest != digestBytes(body) {
					t.Fatalf("Visit must receive the complete decompressed body")
				}
				if c.visits != 1 || ext.visitedCount("/page") != 1 {
					t.Fatalf("want one visit")
				}
			}
			assertNoFiles(t, dir)
		})
	}
}

func TestBodyBudgetReadFailure(t *testing.T) {
	dir, restore := usePrivateTempDir(t)
	defer restore()

	ext := newBudgetTestExt()
	rd := &failingReader{data: []byte(strings.Repeat("q", 64)), err: errTestRead}
	ext.hookFetch = func(ctx *URLContext, _ string, _ bool) (*http.Response, error) {
		if ctx.IsRobotsURL() {
			return &http.Response{
				StatusCode: 200,
				Header:     make(http.Header),
				Body:       ioutil.NopCloser(strings.NewReader("")),
				Request:    &http.Request{URL: ctx.URL()},
			}, nil
		}
		return &http.Response{
			StatusCode: 200,
			Header:     make(http.Header),
			Body:       rd,
			Request:    &http.Request{URL: ctx.URL()},
		}, nil
	}

	c := newBudgetCrawler(ext, 8, 1024)
	if err := c.Run("http://example.invalid/page"); err != nil {
		t.Fatalf("run: %v", err)
	}

	errs := ext.errSnapshot()
	if len(errs) != 1 || errs[0].Kind != CekReadBody {
		t.Fatalf("want exactly one ReadBody error, got %v", errs)
	}
	if !errors.Is(errs[0].Err, errTestRead) {
		t.Fatalf("underlying read error lost: %v", errs[0].Err)
	}
	if ext.visitCount("/page") != 0 || ext.visitedCount("/page") != 0 {
		t.Fatalf("Visit/Visited must not run on a read failure")
	}
	if c.visits != 0 {
		t.Fatalf("read failure must not count as a visit, got %d", c.visits)
	}
	if rd.closes != 1 {
		t.Fatalf("original body must be closed exactly once, got %d", rd.closes)
	}
	assertNoFiles(t, dir)
}

func TestBodyBudgetSpillFailure(t *testing.T) {
	dir, restore := usePrivateTempDir(t)
	defer restore()

	// Make TMPDIR point at a regular file so creating a temporary file fails.
	notDir := dir + "/not-a-dir"
	if err := ioutil.WriteFile(notDir, []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	old, had := os.LookupEnv("TMPDIR")
	if err := os.Setenv("TMPDIR", notDir); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if had {
			_ = os.Setenv("TMPDIR", old)
		} else {
			_ = os.Unsetenv("TMPDIR")
		}
	}()

	body := []byte(strings.Repeat("s", 64))
	ext := newBudgetTestExt()
	srv := newBudgetServer(t, budgetServer{pages: map[string][]byte{"/page": body}})
	defer srv.Close()

	c := newBudgetCrawler(ext, 8, len(body))
	if err := c.Run(srv.URL + "/page"); err != nil {
		t.Fatalf("run: %v", err)
	}

	errs := ext.errSnapshot()
	if len(errs) != 1 || errs[0].Kind != CekBodySpill {
		t.Fatalf("want exactly one BodySpill error, got %v", errs)
	}
	if !errors.Is(errs[0].Err, ErrBodySpill) {
		t.Fatalf("underlying spill error lost: %v", errs[0].Err)
	}
	if ext.visitCount("/page") != 0 || ext.visitedCount("/page") != 0 {
		t.Fatalf("Visit/Visited must not run on a spill failure")
	}
	if c.visits != 0 {
		t.Fatalf("spill failure must not count as a visit")
	}
}

func TestBodyBudgetVisitPanicCleansUp(t *testing.T) {
	dir, restore := usePrivateTempDir(t)
	defer restore()

	ext := newBudgetTestExt()
	ext.hookVisit = func(_ *URLContext, _ *http.Response, _ *goquery.Document) (interface{}, bool) {
		panic("visit panic")
	}

	body := []byte(strings.Repeat("p", 128))
	u, _ := url.Parse("http://example.invalid/page")
	ctx := &URLContext{url: u, normalizedURL: u}
	res := &http.Response{
		StatusCode: 200,
		Header:     make(http.Header),
		Body:       ioutil.NopCloser(bytes.NewReader(body)),
		Request:    &http.Request{URL: u},
	}
	w := &worker{
		opts:    NewOptions(ext),
		logFunc: func(LogFlags, string, ...interface{}) {},
	}
	w.opts.MaxReplayBuffer = 8
	w.opts.MaxBodyBytes = len(body)

	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected panic to propagate")
			}
		}()
		_, _ = w.visitURL(ctx, res)
	}()

	assertNoFiles(t, dir)
}

// --- robots tests ---------------------------------------------------------

func TestBodyBudgetRobotsFailureFailsClosed(t *testing.T) {
	dir, restore := usePrivateTempDir(t)
	defer restore()

	// Host A's robots.txt is valid but larger than L.
	longRobots := []byte("User-agent: *\nDisallow: /page\n" + strings.Repeat("# padding\n", 100))
	shortRobots := []byte("User-agent: *\nAllow: /\n")

	ext := newBudgetTestExt()
	srvA := newBudgetServer(t, budgetServer{
		robots: longRobots,
		pages:  map[string][]byte{"/page": []byte("<html>a</html>")},
	})
	defer srvA.Close()
	srvB := newBudgetServer(t, budgetServer{
		robots: shortRobots,
		pages:  map[string][]byte{"/b": []byte("<html>b</html>")},
	})
	defer srvB.Close()

	const maxReplay, maxBody = 32, 64
	c := newBudgetCrawler(ext, maxReplay, maxBody)
	if err := c.Run([]string{srvA.URL + "/page", srvB.URL + "/b"}); err != nil {
		t.Fatalf("run: %v", err)
	}

	errs := ext.errSnapshot()
	if len(errs) != 1 || errs[0].Kind != CekBodyTooLarge {
		t.Fatalf("want one BodyTooLarge error for host A's robots.txt, got %v", errs)
	}
	if errors.Is(errs[0].Err, ErrBodyTooLarge) == false {
		t.Fatalf("underlying error should identify the body limit: %v", errs[0].Err)
	}

	// Host A must fail closed: its page is disallowed, not visited.
	if ext.disallowedCount("/page") != 1 {
		t.Fatalf("host A page should be disallowed, got %d", ext.disallowedCount("/page"))
	}
	if ext.visitCount("/page") != 0 || ext.visitedCount("/page") != 0 {
		t.Fatalf("host A page must not be visited")
	}
	// The failed robots.txt must not reach FetchedRobots (cache unchanged).
	if ext.robotsFetchedCount(mustHost(t, srvA.URL)) != 0 {
		t.Fatalf("FetchedRobots must not be called for the failed host")
	}

	// Host B is unaffected: its page is visited.
	if ext.visitCount("/b") != 1 || ext.visitedCount("/b") != 1 {
		t.Fatalf("host B page should be visited")
	}
	if ext.robotsFetchedCount(mustHost(t, srvB.URL)) != 1 {
		t.Fatalf("host B robots.txt should have been fetched")
	}
	if c.visits != 1 {
		t.Fatalf("want exactly one successful visit, got %d", c.visits)
	}
	assertNoFiles(t, dir)
}

func mustHost(t *testing.T, raw string) string {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return u.Host
}

func TestBodyBudgetRobotsSpillCompleteBody(t *testing.T) {
	dir, restore := usePrivateTempDir(t)
	defer restore()

	robots := []byte("User-agent: *\nDisallow: /private\n" + strings.Repeat("# padding\n", 20))
	body := []byte("<html>page</html>")

	ext := newBudgetTestExt()
	ext.hookFetchedRobots = func(_ *URLContext, res *http.Response) {
		d, err := digestReader(res.Body)
		if err != nil {
			t.Fatalf("read robots body: %v", err)
		}
		ext.mu.Lock()
		ext.robotsDigests["hook"] = d
		ext.mu.Unlock()
	}

	srv := newBudgetServer(t, budgetServer{
		robots: robots,
		pages:  map[string][]byte{"/page": body},
	})
	defer srv.Close()

	// robots.txt is bigger than M but smaller than L: FetchedRobots must
	// still receive the complete body from the temporary file.
	c := newBudgetCrawler(ext, 16, len(robots))
	if err := c.Run(srv.URL + "/page"); err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(ext.errSnapshot()) != 0 {
		t.Fatalf("unexpected errors: %v", ext.errSnapshot())
	}
	ext.mu.Lock()
	d := ext.robotsDigests["hook"]
	ext.mu.Unlock()
	if d != digestBytes(robots) {
		t.Fatalf("FetchedRobots did not receive the complete robots body")
	}
	if ext.visitedCount("/page") != 1 {
		t.Fatalf("page should be allowed by robots.txt")
	}
	assertNoFiles(t, dir)
}

func TestBodyBudgetRobotsSpillFailureFailsClosed(t *testing.T) {
	dir, restore := usePrivateTempDir(t)
	defer restore()

	// Make TMPDIR point at a regular file so the robots body cannot be
	// spilled to a temporary file.
	notDir := dir + "/not-a-dir"
	if err := ioutil.WriteFile(notDir, []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	old, had := os.LookupEnv("TMPDIR")
	if err := os.Setenv("TMPDIR", notDir); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if had {
			_ = os.Setenv("TMPDIR", old)
		} else {
			_ = os.Unsetenv("TMPDIR")
		}
	}()

	robots := []byte("User-agent: *\nDisallow: /other\n")
	ext := newBudgetTestExt()
	srv := newBudgetServer(t, budgetServer{
		robots: robots,
		pages:  map[string][]byte{"/page": []byte("<html>page</html>")},
	})
	defer srv.Close()

	c := newBudgetCrawler(ext, 8, len(robots))
	if err := c.Run(srv.URL + "/page"); err != nil {
		t.Fatalf("run: %v", err)
	}
	errs := ext.errSnapshot()
	if len(errs) != 1 || errs[0].Kind != CekBodySpill {
		t.Fatalf("want one BodySpill error for robots.txt, got %v", errs)
	}
	if ext.disallowedCount("/page") != 1 || ext.visitedCount("/page") != 0 {
		t.Fatalf("host page must be disallowed when robots spill fails")
	}
	if ext.robotsFetchedCount(mustHost(t, srv.URL)) != 0 {
		t.Fatalf("FetchedRobots must not be called when robots spill fails")
	}
}

func TestBodyBudgetCachedRobotsUnaffected(t *testing.T) {
	dir, restore := usePrivateTempDir(t)
	defer restore()

	ext := newBudgetTestExt()
	ext.serveCache = true
	ext.cachedRobots = []byte("User-agent: *\nDisallow: /page\n")

	srv := newBudgetServer(t, budgetServer{
		robots: []byte("User-agent: *\nAllow: /\n"),
		pages:  map[string][]byte{"/page": []byte("<html>page</html>")},
	})
	defer srv.Close()

	c := newBudgetCrawler(ext, 8, 32)
	if err := c.Run(srv.URL + "/page"); err != nil {
		t.Fatalf("run: %v", err)
	}
	if ext.fetchCount("/robots.txt") != 0 {
		t.Fatalf("cached robots should not be fetched")
	}
	if ext.robotsFetchedCount(mustHost(t, srv.URL)) != 0 {
		t.Fatalf("FetchedRobots must not run for a cached robots.txt")
	}
	if ext.disallowedCount("/page") != 1 || ext.visitedCount("/page") != 0 {
		t.Fatalf("cached robots policy should disallow the page")
	}
	assertNoFiles(t, dir)
}

// --- legacy behaviour and HEAD semantics ---------------------------------

func TestBodyBudgetDisabledLegacyBehaviour(t *testing.T) {
	big := []byte(strings.Repeat("l", 1<<20))
	ext := newBudgetTestExt()
	var gotDigest string
	ext.hookVisit = func(_ *URLContext, res *http.Response, _ *goquery.Document) (interface{}, bool) {
		var err error
		gotDigest, err = digestReader(res.Body)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		return nil, true
	}
	srv := newBudgetServer(t, budgetServer{pages: map[string][]byte{"/page": big}})
	defer srv.Close()

	c := newBudgetCrawler(ext, 0, 0)
	if err := c.Run(srv.URL + "/page"); err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(ext.errSnapshot()) != 0 {
		t.Fatalf("unexpected errors: %v", ext.errSnapshot())
	}
	if gotDigest != digestBytes(big) {
		t.Fatalf("legacy visit must receive the whole body")
	}
	if c.visits != 1 {
		t.Fatalf("want one visit, got %d", c.visits)
	}
}

func TestBodyBudgetDisabledLegacyReadFailure(t *testing.T) {
	ext := newBudgetTestExt()
	rd := &failingReader{data: []byte(strings.Repeat("q", 64)), err: errTestRead}
	ext.hookFetch = func(ctx *URLContext, _ string, _ bool) (*http.Response, error) {
		if ctx.IsRobotsURL() {
			return &http.Response{
				StatusCode: 200,
				Header:     make(http.Header),
				Body:       ioutil.NopCloser(strings.NewReader("")),
				Request:    &http.Request{URL: ctx.URL()},
			}, nil
		}
		return &http.Response{
			StatusCode: 200,
			Header:     make(http.Header),
			Body:       rd,
			Request:    &http.Request{URL: ctx.URL()},
		}, nil
	}

	c := newBudgetCrawler(ext, 0, 0)
	if err := c.Run("http://example.invalid/page"); err != nil {
		t.Fatalf("run: %v", err)
	}

	errs := ext.errSnapshot()
	if countKind(errs, CekReadBody) != 1 {
		t.Fatalf("want one legacy ReadBody error, got %v", errs)
	}
	// Historical behaviour: even after a read error, Visit and Visited run
	// with the partial body. Visit returns findLinks=true with a nil
	// document, so the historical ProcessLinks error is also reported.
	if len(errs) != 2 || errs[1].Kind != CekProcessLinks {
		t.Fatalf("want legacy ProcessLinks error too, got %v", errs)
	}
	if ext.visitCount("/page") != 1 || ext.visitedCount("/page") != 1 {
		t.Fatalf("legacy hooks should run, got %d/%d", ext.visitCount("/page"), ext.visitedCount("/page"))
	}
	if c.visits != 1 {
		t.Fatalf("legacy read failure still counts as a visit, got %d", c.visits)
	}
	if rd.closes != 1 {
		t.Fatalf("original body must be closed exactly once, got %d", rd.closes)
	}
}

func TestBodyBudgetHeadSemantics(t *testing.T) {
	dir, restore := usePrivateTempDir(t)
	defer restore()

	body := []byte(strings.Repeat("h", 128))
	ext := newBudgetTestExt()
	var gotDigest string
	ext.hookVisit = func(_ *URLContext, res *http.Response, _ *goquery.Document) (interface{}, bool) {
		var err error
		gotDigest, err = digestReader(res.Body)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		return nil, true
	}

	srv := newBudgetServer(t, budgetServer{pages: map[string][]byte{"/page": body}})
	defer srv.Close()

	c := newBudgetCrawler(ext, 32, len(body))
	c.Options.HeadBeforeGet = true
	if err := c.Run(srv.URL + "/page"); err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(ext.errSnapshot()) != 0 {
		t.Fatalf("unexpected errors: %v", ext.errSnapshot())
	}
	if ext.getRequestGetCount() != 1 {
		t.Fatalf("RequestGet should be called after HEAD, got %d", ext.getRequestGetCount())
	}
	if ext.fetchCount("/page") != 2 {
		t.Fatalf("want HEAD + GET fetches, got %d", ext.fetchCount("/page"))
	}
	if gotDigest != digestBytes(body) || ext.visitedCount("/page") != 1 {
		t.Fatalf("GET body should still be visited complete")
	}
	assertNoFiles(t, dir)
}
