package gocrawl

import (
	"bytes"
	"compress/gzip"
	"crypto/sha1"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/PuerkitoBio/goquery"
)

// stubBodyTempFiles redirects the body budget spill files to a test-managed
// directory and returns that directory along with a counter of created files.
func stubBodyTempFiles(t *testing.T) (string, *int32) {
	dir := t.TempDir()
	cnt := new(int32)
	old := createBodyTempFile
	createBodyTempFile = func() (*os.File, error) {
		atomic.AddInt32(cnt, 1)
		return ioutil.TempFile(dir, "gocrawl-body-*")
	}
	t.Cleanup(func() { createBodyTempFile = old })
	return dir, cnt
}

// failBodyTempFiles makes the creation of spill files fail.
func failBodyTempFiles(t *testing.T) {
	old := createBodyTempFile
	createBodyTempFile = func() (*os.File, error) {
		return nil, errors.New("cannot create temp file")
	}
	t.Cleanup(func() { createBodyTempFile = old })
}

// assertNoTempResidue asserts that no spill file remains in dir.
func assertNoTempResidue(t *testing.T, dir string) {
	t.Helper()
	ents, e := ioutil.ReadDir(dir)
	if e != nil {
		t.Fatalf("cannot read temp dir: %s", e)
	}
	if len(ents) > 0 {
		names := make([]string, len(ents))
		for i, ent := range ents {
			names[i] = ent.Name()
		}
		t.Errorf("expected no temp file residue, found: %v", names)
	}
}

// digestOf returns a printable digest of b, used to assert that a hook
// received the complete body.
func digestOf(b []byte) string {
	return fmt.Sprintf("%x:%d", sha1.Sum(b), len(b))
}

// readDigest reads r fully and returns its digest.
func readDigest(r io.Reader) (string, error) {
	bd, e := ioutil.ReadAll(r)
	if e != nil {
		return "", e
	}
	return digestOf(bd), nil
}

// paddedPage returns an HTML page of exactly size bytes, containing a link
// to href if it is not empty.
func paddedPage(size int, href string) []byte {
	page := "<html><body>"
	if href != "" {
		page += `<a href="` + href + `">link</a>`
	}
	end := "</body></html>"
	if len(page)+len(end) > size {
		size = len(page) + len(end)
	}
	page += string(bytes.Repeat([]byte("x"), size-len(page)-len(end))) + end
	return []byte(page)
}

// newBudgetTestServer starts a test server serving the given routes (path ->
// body). If flush is set, the response is flushed before writing so that it
// is chunked (unknown content length).
func newBudgetTestServer(t *testing.T, routes map[string][]byte, flush bool) *httptest.Server {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bd, ok := routes[r.URL.Path]
		if !ok {
			http.NotFound(w, r)
			return
		}
		if flush {
			w.(http.Flusher).Flush()
		}
		w.Write(bd)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// budgetSpyResult collects the data asserted after a crawl run.
type budgetSpyResult struct {
	visitDigests  []string
	robotsDigests []string
	errors        []*CrawlError
}

// newBudgetSpy creates a spy extender that records the digest of the body
// received by Visit and FetchedRobots, and all errors.
func newBudgetSpy() (*spyExtender, *budgetSpyResult) {
	res := &budgetSpyResult{}
	spy := newSpy(new(DefaultExtender), true)
	spy.setExtensionMethod(eMKVisit, func(ctx *URLContext, r *http.Response, doc *goquery.Document) (interface{}, bool) {
		d, e := readDigest(r.Body)
		if e != nil {
			d = "ERROR: " + e.Error()
		}
		res.visitDigests = append(res.visitDigests, d)
		return nil, true // let the worker process the links
	})
	spy.setExtensionMethod(eMKFetchedRobots, func(ctx *URLContext, r *http.Response) {
		d, e := readDigest(r.Body)
		if e != nil {
			d = "ERROR: " + e.Error()
		}
		res.robotsDigests = append(res.robotsDigests, d)
	})
	spy.setExtensionMethod(eMKError, func(ce *CrawlError) {
		res.errors = append(res.errors, ce)
	})
	return spy, res
}

func runBudgetCrawler(t *testing.T, spy *spyExtender, memLimit, maxBytes int64, seeds interface{}) error {
	opts := NewOptions(spy)
	opts.CrawlDelay = time.Millisecond
	opts.WorkerIdleTTL = time.Second
	opts.MaxBodyMemBytes = memLimit
	opts.MaxBodyBytes = maxBytes
	opts.LogFlags = LogError
	c := NewCrawlerWithOptions(opts)
	return c.Run(seeds)
}

// countErrors returns the number of recorded errors of the given kind.
func (r *budgetSpyResult) countErrors(kind CrawlErrorKind) int {
	cnt := 0
	for _, ce := range r.errors {
		if ce.Kind == kind {
			cnt++
		}
	}
	return cnt
}

// TestBodyBudgetBoundaries covers the M (memory) and L (total) boundaries:
// at exactly M the body stays in memory, above M it spills to a temp file,
// at exactly L it is still accepted, above L a single recognizable error is
// notified and the page is not visited.
func TestBodyBudgetBoundaries(t *testing.T) {
	dir, tmpCnt := stubBodyTempFiles(t)

	const M, L = 1024, 4096
	cases := []struct {
		name       string
		size       int
		wantSpill  bool
		checkSpill bool
		wantVisit  bool
	}{
		{"below-M", M - 1, false, true, true},
		{"exactly-M", M, false, true, true},
		{"above-M", M + 1, true, true, true},
		{"below-L", L - 1, true, true, true},
		{"exactly-L", L, true, true, true},
		// Over L, whether a spill file was created depends on read chunk
		// boundaries; only the absence of residue is asserted (below).
		{"above-L", L + 1, false, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := atomic.LoadInt32(tmpCnt)
			body := paddedPage(tc.size, "")
			srv := newBudgetTestServer(t, map[string][]byte{
				"/robots.txt": []byte("User-agent: *\nAllow: /\n"),
				"/page":       body,
			}, false)

			spy, res := newBudgetSpy()
			if err := runBudgetCrawler(t, spy, M, L, srv.URL+"/page"); err != nil {
				t.Fatalf("run failed: %s", err)
			}

			spilled := atomic.LoadInt32(tmpCnt) > before
			if tc.checkSpill && spilled != tc.wantSpill {
				t.Errorf("size %d: expected spill=%v, got spill=%v", tc.size, tc.wantSpill, spilled)
			}
			if tc.wantVisit {
				if n := spy.getCallCount(eMKVisit); n != 1 {
					t.Errorf("size %d: expected 1 Visit call, got %d", tc.size, n)
				}
				if n := spy.getCallCount(eMKVisited); n != 1 {
					t.Errorf("size %d: expected 1 Visited call, got %d", tc.size, n)
				}
				if len(res.visitDigests) != 1 || res.visitDigests[0] != digestOf(body) {
					t.Errorf("size %d: expected visit digest %s, got %v", tc.size, digestOf(body), res.visitDigests)
				}
				if len(res.errors) != 0 {
					t.Errorf("size %d: expected no error, got %v", tc.size, res.errors)
				}
			} else {
				// Over the limit: a single recognizable error, no
				// Visit/Visited, no success counted.
				if n := res.countErrors(CekBodyTooLarge); n != 1 {
					t.Errorf("size %d: expected 1 BodyTooLarge error, got %d", tc.size, n)
				}
				if len(res.errors) != 1 {
					t.Errorf("size %d: expected exactly 1 error, got %d", tc.size, len(res.errors))
				}
				if !errors.Is(res.errors[0].Err, ErrBodyTooLarge) {
					t.Errorf("size %d: expected error to match ErrBodyTooLarge, got %v", tc.size, res.errors[0].Err)
				}
				if n := spy.getCallCount(eMKVisit); n != 0 {
					t.Errorf("size %d: expected 0 Visit call, got %d", tc.size, n)
				}
				if n := spy.getCallCount(eMKVisited); n != 0 {
					t.Errorf("size %d: expected 0 Visited call, got %d", tc.size, n)
				}
			}
		})
	}
	assertNoTempResidue(t, dir)
}

// TestBodyBudgetSpillReplay verifies that a body larger than M (but within
// L) is spilled to a temporary file, that parsing extracts the links from
// the complete body, and that Visit receives the complete body - without
// reading it all back into memory.
func TestBodyBudgetSpillReplay(t *testing.T) {
	dir, tmpCnt := stubBodyTempFiles(t)

	page2 := paddedPage(100, "")
	page1 := paddedPage(2048, "/page2")
	srv := newBudgetTestServer(t, map[string][]byte{
		"/robots.txt": []byte("User-agent: *\nAllow: /\n"),
		"/page1":      page1,
		"/page2":      page2,
	}, false)

	spy, res := newBudgetSpy()
	if err := runBudgetCrawler(t, spy, 512, 1<<20, srv.URL+"/page1"); err != nil {
		t.Fatalf("run failed: %s", err)
	}

	if n := atomic.LoadInt32(tmpCnt); n != 1 {
		t.Errorf("expected 1 spill file created, got %d", n)
	}
	// Both pages were visited, with their complete body.
	if n := spy.getCallCount(eMKVisit); n != 2 {
		t.Errorf("expected 2 Visit calls, got %d", n)
	}
	if n := spy.getCallCount(eMKVisited); n != 2 {
		t.Errorf("expected 2 Visited calls, got %d", n)
	}
	want := []string{digestOf(page1), digestOf(page2)}
	if len(res.visitDigests) != 2 || res.visitDigests[0] != want[0] || res.visitDigests[1] != want[1] {
		t.Errorf("expected visit digests %v, got %v", want, res.visitDigests)
	}
	// The link was extracted from the spilled body.
	if n := spy.getCallCount(eMKEnqueued); n != 3 { // robots, page1, page2
		t.Errorf("expected 3 Enqueued calls, got %d", n)
	}
	if len(res.errors) != 0 {
		t.Errorf("expected no error, got %v", res.errors)
	}
	assertNoTempResidue(t, dir)
}

// TestBodyBudgetUnknownLength verifies that the L limit is enforced even
// when the response has no known content length (chunked transfer).
func TestBodyBudgetUnknownLength(t *testing.T) {
	dir, _ := stubBodyTempFiles(t)

	// Over the limit with unknown length: single error, no visit.
	big := paddedPage(2048, "")
	srv := newBudgetTestServer(t, map[string][]byte{
		"/robots.txt": []byte("User-agent: *\nAllow: /\n"),
		"/page":       big,
	}, true) // flush => chunked, unknown length
	if cl := srv.Config; cl == nil {
		t.Fatal("no server")
	}

	spy, res := newBudgetSpy()
	if err := runBudgetCrawler(t, spy, 512, 1024, srv.URL+"/page"); err != nil {
		t.Fatalf("run failed: %s", err)
	}
	if n := res.countErrors(CekBodyTooLarge); n != 1 {
		t.Errorf("expected 1 BodyTooLarge error, got %d", n)
	}
	if n := spy.getCallCount(eMKVisit); n != 0 {
		t.Errorf("expected 0 Visit call, got %d", n)
	}

	// Within the limit with unknown length: complete body.
	small := paddedPage(900, "")
	srv2 := newBudgetTestServer(t, map[string][]byte{
		"/robots.txt": []byte("User-agent: *\nAllow: /\n"),
		"/page":       small,
	}, true)
	spy2, res2 := newBudgetSpy()
	if err := runBudgetCrawler(t, spy2, 512, 1024, srv2.URL+"/page"); err != nil {
		t.Fatalf("run failed: %s", err)
	}
	if len(res2.visitDigests) != 1 || res2.visitDigests[0] != digestOf(small) {
		t.Errorf("expected visit digest %s, got %v", digestOf(small), res2.visitDigests)
	}
	assertNoTempResidue(t, dir)
}

// gzipBody compresses b and returns the compressed bytes.
func gzipBody(t *testing.T, b []byte) []byte {
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	if _, e := gw.Write(b); e != nil {
		t.Fatalf("gzip write: %s", e)
	}
	if e := gw.Close(); e != nil {
		t.Fatalf("gzip close: %s", e)
	}
	return buf.Bytes()
}

// TestBodyBudgetGzip verifies that with the default HTTP client, the budget
// applies to the decompressed body size.
func TestBodyBudgetGzip(t *testing.T) {
	dir, tmpCnt := stubBodyTempFiles(t)

	// Highly compressible body: small on the wire, large once decompressed.
	payload := paddedPage(8192, "")
	gz := gzipBody(t, payload)
	if len(gz) >= len(payload) {
		t.Fatalf("expected compressed body to be smaller, got %d >= %d", len(gz), len(payload))
	}

	newGzipServer := func() *httptest.Server {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/robots.txt" {
				w.Write([]byte("User-agent: *\nAllow: /\n"))
				return
			}
			w.Header().Set("Content-Encoding", "gzip")
			w.Write(gz)
		}))
		t.Cleanup(srv.Close)
		return srv
	}

	// Decompressed size over L: single BodyTooLarge error, no visit, even
	// though the compressed body is well under L.
	srv := newGzipServer()
	spy, res := newBudgetSpy()
	if err := runBudgetCrawler(t, spy, 512, 4096, srv.URL+"/page"); err != nil {
		t.Fatalf("run failed: %s", err)
	}
	if n := res.countErrors(CekBodyTooLarge); n != 1 {
		t.Errorf("expected 1 BodyTooLarge error, got %d", n)
	}
	if len(res.errors) != 1 {
		t.Errorf("expected exactly 1 error, got %d", len(res.errors))
	}
	if n := spy.getCallCount(eMKVisit); n != 0 {
		t.Errorf("expected 0 Visit call, got %d", n)
	}
	if n := spy.getCallCount(eMKVisited); n != 0 {
		t.Errorf("expected 0 Visited call, got %d", n)
	}

	// Decompressed size within L (but over M): spilled, and Visit receives
	// the complete decompressed body.
	srv2 := newGzipServer()
	spy2, res2 := newBudgetSpy()
	if err := runBudgetCrawler(t, spy2, 1024, 16384, srv2.URL+"/page"); err != nil {
		t.Fatalf("run failed: %s", err)
	}
	if len(res2.visitDigests) != 1 || res2.visitDigests[0] != digestOf(payload) {
		t.Errorf("expected visit digest %s, got %v", digestOf(payload), res2.visitDigests)
	}
	if n := atomic.LoadInt32(tmpCnt); n == 0 {
		t.Error("expected the decompressed body to spill to a temp file")
	}
	assertNoTempResidue(t, dir)
}

// errBody is a body that fails after having delivered some bytes.
type errBody struct {
	data   []byte
	off    int
	closed bool
}

func (b *errBody) Read(p []byte) (int, error) {
	if b.off >= len(b.data) {
		return 0, errors.New("read failure")
	}
	n := copy(p, b.data[b.off:])
	b.off += n
	return n, nil
}

func (b *errBody) Close() error {
	b.closed = true
	return nil
}

// trackBody is a body that records whether it was closed.
type trackBody struct {
	*bytes.Reader
	closed *bool
}

func (b *trackBody) Close() error {
	*b.closed = true
	return nil
}

// fetchResponse builds a minimal 200 OK response with the given body.
func fetchResponse(rawURL string, body io.ReadCloser) *http.Response {
	u, _ := url.Parse(rawURL)
	return &http.Response{
		Status:     "200 OK",
		StatusCode: 200,
		Body:       body,
		Request:    &http.Request{Method: "GET", URL: u},
	}
}

// TestBodyBudgetReadFailure verifies that a body read failure notifies a
// single recognizable error, does not call Visit/Visited, closes the
// original body and leaves no temp file residue.
func TestBodyBudgetReadFailure(t *testing.T) {
	dir, _ := stubBodyTempFiles(t)

	body := &errBody{data: paddedPage(600, "")}
	srv := newBudgetTestServer(t, map[string][]byte{
		"/robots.txt": []byte("User-agent: *\nAllow: /\n"),
	}, false)

	spy, res := newBudgetSpy()
	spy.setExtensionMethod(eMKFetch, func(ctx *URLContext, agent string, head bool) (*http.Response, error) {
		if ctx.IsRobotsURL() {
			return new(DefaultExtender).Fetch(ctx, agent, head)
		}
		return fetchResponse(srv.URL+"/page", body), nil
	})
	if err := runBudgetCrawler(t, spy, 512, 4096, srv.URL+"/page"); err != nil {
		t.Fatalf("run failed: %s", err)
	}

	if n := res.countErrors(CekReadBody); n != 1 {
		t.Errorf("expected 1 ReadBody error, got %d", n)
	}
	if len(res.errors) != 1 {
		t.Errorf("expected exactly 1 error, got %d", len(res.errors))
	}
	if n := spy.getCallCount(eMKVisit); n != 0 {
		t.Errorf("expected 0 Visit call, got %d", n)
	}
	if n := spy.getCallCount(eMKVisited); n != 0 {
		t.Errorf("expected 0 Visited call, got %d", n)
	}
	if !body.closed {
		t.Error("expected the original body to be closed")
	}
	assertNoTempResidue(t, dir)
}

// TestBodyBudgetSpillFailure verifies that a temp file creation failure
// notifies a single recognizable error and does not call Visit/Visited.
func TestBodyBudgetSpillFailure(t *testing.T) {
	failBodyTempFiles(t)

	body := paddedPage(2048, "")
	srv := newBudgetTestServer(t, map[string][]byte{
		"/robots.txt": []byte("User-agent: *\nAllow: /\n"),
		"/page":       body,
	}, false)

	spy, res := newBudgetSpy()
	if err := runBudgetCrawler(t, spy, 512, 4096, srv.URL+"/page"); err != nil {
		t.Fatalf("run failed: %s", err)
	}
	if n := res.countErrors(CekReadBody); n != 1 {
		t.Errorf("expected 1 ReadBody error, got %d", n)
	}
	if len(res.errors) != 1 {
		t.Errorf("expected exactly 1 error, got %d", len(res.errors))
	}
	if n := spy.getCallCount(eMKVisit); n != 0 {
		t.Errorf("expected 0 Visit call, got %d", n)
	}
	if n := spy.getCallCount(eMKVisited); n != 0 {
		t.Errorf("expected 0 Visited call, got %d", n)
	}
}

// TestBodyBudgetRobotsTooLarge verifies that a robots.txt body over the
// limit notifies a single error, does not call FetchedRobots, and disallows
// the pages of that host, while other hosts keep crawling.
func TestBodyBudgetRobotsTooLarge(t *testing.T) {
	dir, _ := stubBodyTempFiles(t)

	bigRobots := bytes.Repeat([]byte("# comment padding\n"), 256) // > 4K
	pageA := paddedPage(100, "")
	srvA := newBudgetTestServer(t, map[string][]byte{
		"/robots.txt": bigRobots,
		"/page":       pageA,
	}, false)

	pageB := paddedPage(100, "")
	srvB := newBudgetTestServer(t, map[string][]byte{
		"/robots.txt": []byte("User-agent: *\nAllow: /\n"),
		"/page":       pageB,
	}, false)

	spy, res := newBudgetSpy()
	err := runBudgetCrawler(t, spy, 1024, 2048, []string{srvA.URL + "/page", srvB.URL + "/page"})
	if err != nil {
		t.Fatalf("run failed: %s", err)
	}

	// A single recognizable error for host A's robots.txt.
	if n := res.countErrors(CekBodyTooLarge); n != 1 {
		t.Errorf("expected 1 BodyTooLarge error, got %d", n)
	}
	if len(res.errors) != 1 {
		t.Errorf("expected exactly 1 error, got %d", len(res.errors))
	}
	// FetchedRobots only for host B, with the complete robots body.
	if n := spy.getCallCount(eMKFetchedRobots); n != 1 {
		t.Errorf("expected 1 FetchedRobots call, got %d", n)
	}
	if len(res.robotsDigests) != 1 || res.robotsDigests[0] != digestOf([]byte("User-agent: *\nAllow: /\n")) {
		t.Errorf("unexpected robots digests: %v", res.robotsDigests)
	}
	// Host A's page was disallowed, not visited; host B's page was visited.
	if n := spy.getCallCount(eMKDisallowed); n != 1 {
		t.Errorf("expected 1 Disallowed call, got %d", n)
	}
	if n := spy.getCallCount(eMKVisit); n != 1 {
		t.Errorf("expected 1 Visit call, got %d", n)
	}
	if len(res.visitDigests) != 1 || res.visitDigests[0] != digestOf(pageB) {
		t.Errorf("expected host B page digest %s, got %v", digestOf(pageB), res.visitDigests)
	}
	assertNoTempResidue(t, dir)
}

// TestBodyBudgetRobotsSpill verifies that a robots.txt body larger than M
// (but within L) is spilled, that FetchedRobots receives the complete body,
// and that the policies are enforced.
func TestBodyBudgetRobotsSpill(t *testing.T) {
	dir, tmpCnt := stubBodyTempFiles(t)

	robots := append([]byte("User-agent: *\nDisallow: /private\n"), bytes.Repeat([]byte("# pad\n"), 200)...)
	ok := paddedPage(100, "")
	srv := newBudgetTestServer(t, map[string][]byte{
		"/robots.txt": robots,
		"/ok":         ok,
		"/private":    paddedPage(100, ""),
	}, false)

	spy, res := newBudgetSpy()
	err := runBudgetCrawler(t, spy, 512, 1<<20, []string{srv.URL + "/ok", srv.URL + "/private"})
	if err != nil {
		t.Fatalf("run failed: %s", err)
	}

	if n := atomic.LoadInt32(tmpCnt); n != 1 {
		t.Errorf("expected 1 spill file created, got %d", n)
	}
	// FetchedRobots received the complete robots.txt body.
	if n := spy.getCallCount(eMKFetchedRobots); n != 1 {
		t.Errorf("expected 1 FetchedRobots call, got %d", n)
	}
	if len(res.robotsDigests) != 1 || res.robotsDigests[0] != digestOf(robots) {
		t.Errorf("expected robots digest %s, got %v", digestOf(robots), res.robotsDigests)
	}
	// /ok visited, /private disallowed per the spilled robots.txt policy.
	if n := spy.getCallCount(eMKVisit); n != 1 {
		t.Errorf("expected 1 Visit call, got %d", n)
	}
	if len(res.visitDigests) != 1 || res.visitDigests[0] != digestOf(ok) {
		t.Errorf("expected visit digest %s, got %v", digestOf(ok), res.visitDigests)
	}
	if n := spy.getCallCount(eMKDisallowed); n != 1 {
		t.Errorf("expected 1 Disallowed call, got %d", n)
	}
	if len(res.errors) != 0 {
		t.Errorf("expected no error, got %v", res.errors)
	}
	assertNoTempResidue(t, dir)
}

// TestBodyBudgetCachedRobotsUnchanged verifies that the cached robots.txt
// path (RequestRobots returning data) is not affected by the body budget.
func TestBodyBudgetCachedRobotsUnchanged(t *testing.T) {
	dir, tmpCnt := stubBodyTempFiles(t)

	ok := paddedPage(100, "")
	srv := newBudgetTestServer(t, map[string][]byte{
		"/ok":      ok,
		"/private": paddedPage(100, ""),
	}, false)

	spy, res := newBudgetSpy()
	spy.setExtensionMethod(eMKRequestRobots, func(ctx *URLContext, agent string) ([]byte, bool) {
		return []byte("User-agent: *\nDisallow: /private\n"), false
	})
	err := runBudgetCrawler(t, spy, 64, 128, []string{srv.URL + "/ok", srv.URL + "/private"})
	if err != nil {
		t.Fatalf("run failed: %s", err)
	}

	// No robots.txt fetch, no FetchedRobots, cached policy enforced.
	if n := spy.getCallCount(eMKFetchedRobots); n != 0 {
		t.Errorf("expected 0 FetchedRobots call, got %d", n)
	}
	if n := spy.getCallCount(eMKVisit); n != 1 {
		t.Errorf("expected 1 Visit call, got %d", n)
	}
	if n := spy.getCallCount(eMKDisallowed); n != 1 {
		t.Errorf("expected 1 Disallowed call, got %d", n)
	}
	// The page body (100 bytes, within L=128 but over M=64) spilled.
	if n := atomic.LoadInt32(tmpCnt); n != 1 {
		t.Errorf("expected 1 spill file created, got %d", n)
	}
	if len(res.visitDigests) != 1 || res.visitDigests[0] != digestOf(ok) {
		t.Errorf("expected visit digest %s, got %v", digestOf(ok), res.visitDigests)
	}
	assertNoTempResidue(t, dir)
}

// TestBodyBudgetNoSuccessCounted verifies that a page over the limit is not
// counted as a visit (MaxVisits is not consumed by it).
func TestBodyBudgetNoSuccessCounted(t *testing.T) {
	dir, _ := stubBodyTempFiles(t)

	big := paddedPage(2048, "")
	small := paddedPage(100, "")
	srv := newBudgetTestServer(t, map[string][]byte{
		"/robots.txt": []byte("User-agent: *\nAllow: /\n"),
		"/big":        big,
		"/small":      small,
	}, false)

	spy, res := newBudgetSpy()
	opts := NewOptions(spy)
	opts.CrawlDelay = time.Millisecond
	opts.WorkerIdleTTL = time.Second
	opts.MaxBodyMemBytes = 512
	opts.MaxBodyBytes = 1024
	opts.MaxVisits = 1
	opts.LogFlags = LogError
	c := NewCrawlerWithOptions(opts)
	// /big fails the budget and must not consume the single allowed visit:
	// /small is then visited and MaxVisits is reached.
	if err := c.Run([]string{srv.URL + "/big", srv.URL + "/small"}); err != ErrMaxVisits {
		t.Fatalf("expected ErrMaxVisits, got %v", err)
	}
	if n := spy.getCallCount(eMKVisit); n != 1 {
		t.Errorf("expected 1 Visit call, got %d", n)
	}
	if len(res.visitDigests) != 1 || res.visitDigests[0] != digestOf(small) {
		t.Errorf("expected visit digest %s, got %v", digestOf(small), res.visitDigests)
	}
	if n := res.countErrors(CekBodyTooLarge); n != 1 {
		t.Errorf("expected 1 BodyTooLarge error, got %d", n)
	}
	assertNoTempResidue(t, dir)
}

// TestBodyBudgetHeadBeforeGet verifies that the HEAD-before-GET semantics
// are preserved with the budget enabled.
func TestBodyBudgetHeadBeforeGet(t *testing.T) {
	dir, _ := stubBodyTempFiles(t)

	body := paddedPage(1500, "")
	srv := newBudgetTestServer(t, map[string][]byte{
		"/robots.txt": []byte("User-agent: *\nAllow: /\n"),
		"/page":       body,
	}, false)

	spy, res := newBudgetSpy()
	opts := NewOptions(spy)
	opts.CrawlDelay = time.Millisecond
	opts.WorkerIdleTTL = time.Second
	opts.HeadBeforeGet = true
	opts.MaxBodyMemBytes = 512
	opts.MaxBodyBytes = 4096
	opts.LogFlags = LogError
	c := NewCrawlerWithOptions(opts)
	if err := c.Run(srv.URL + "/page"); err != nil {
		t.Fatalf("run failed: %s", err)
	}

	if n := spy.getCallCount(eMKRequestGet); n != 1 {
		t.Errorf("expected 1 RequestGet call, got %d", n)
	}
	if n := spy.getCallCount(eMKVisit); n != 1 {
		t.Errorf("expected 1 Visit call, got %d", n)
	}
	if len(res.visitDigests) != 1 || res.visitDigests[0] != digestOf(body) {
		t.Errorf("expected visit digest %s, got %v", digestOf(body), res.visitDigests)
	}
	assertNoTempResidue(t, dir)
}

// TestBodyBudgetCallbackPanic verifies that when a callback panics, the
// original body is closed and the temp file is cleaned up.
func TestBodyBudgetCallbackPanic(t *testing.T) {
	dir, tmpCnt := stubBodyTempFiles(t)

	body := paddedPage(2048, "")
	closed := false
	res := fetchResponse("http://hostx/page", &trackBody{bytes.NewReader(body), &closed})

	spy := newSpy(new(DefaultExtender), true)
	spy.setExtensionMethod(eMKVisit, func(ctx *URLContext, r *http.Response, doc *goquery.Document) (interface{}, bool) {
		panic("boom in Visit")
	})

	opts := NewOptions(spy)
	opts.MaxBodyMemBytes = 512
	opts.MaxBodyBytes = 4096
	w := &worker{
		host:    "hostx",
		opts:    opts,
		logFunc: func(LogFlags, string, ...interface{}) {},
	}
	ctx := &URLContext{}
	ctx.url, _ = url.Parse("http://hostx/page")

	panicked := false
	func() {
		defer func() {
			panicked = recover() != nil
		}()
		w.visitURL(ctx, res)
	}()

	if !panicked {
		t.Error("expected the Visit panic to propagate")
	}
	if n := atomic.LoadInt32(tmpCnt); n != 1 {
		t.Errorf("expected 1 spill file created, got %d", n)
	}
	if !closed {
		t.Error("expected the original body to be closed")
	}
	assertNoTempResidue(t, dir)
}

// TestBodyBudgetOffOldBehavior verifies that with the budget disabled (the
// default), the old behavior is preserved: a read error still calls Visit
// with a nil document and Visited.
func TestBodyBudgetOffOldBehavior(t *testing.T) {
	body := &errBody{data: []byte("partial")}
	srv := newBudgetTestServer(t, map[string][]byte{
		"/robots.txt": []byte("User-agent: *\nAllow: /\n"),
	}, false)

	var visitDoc *goquery.Document
	visited := 0
	spy, res := newBudgetSpy()
	spy.setExtensionMethod(eMKFetch, func(ctx *URLContext, agent string, head bool) (*http.Response, error) {
		if ctx.IsRobotsURL() {
			return new(DefaultExtender).Fetch(ctx, agent, head)
		}
		return fetchResponse(srv.URL+"/page", body), nil
	})
	spy.setExtensionMethod(eMKVisit, func(ctx *URLContext, r *http.Response, doc *goquery.Document) (interface{}, bool) {
		visitDoc = doc
		return nil, false
	})
	spy.setExtensionMethod(eMKVisited, func(ctx *URLContext, harvested interface{}) {
		visited++
	})

	// Budget disabled (both limits at 0).
	if err := runBudgetCrawler(t, spy, 0, 0, srv.URL+"/page"); err != nil {
		t.Fatalf("run failed: %s", err)
	}

	if n := res.countErrors(CekReadBody); n != 1 {
		t.Errorf("expected 1 ReadBody error, got %d", n)
	}
	if n := spy.getCallCount(eMKVisit); n != 1 {
		t.Errorf("expected 1 Visit call (old behavior), got %d", n)
	}
	if visitDoc != nil {
		t.Error("expected nil goquery document on read error (old behavior)")
	}
	if visited != 1 {
		t.Errorf("expected 1 Visited call (old behavior), got %d", visited)
	}
	if !body.closed {
		t.Error("expected the original body to be closed")
	}
}
