package gocrawl

import (
	"bytes"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"path"

	"github.com/PuerkitoBio/goquery"
	"github.com/andybalholm/cascadia"
	"github.com/temoto/robotstxt"
	"golang.org/x/net/html"
)

// The worker is dedicated to fetching and visiting a given host, respecting
// this host's robots.txt crawling policies.
type worker struct {
	// Worker identification
	host  string
	index int

	// Communication channels and sync
	push    chan<- *workerResponse
	pop     popChannel
	stop    chan struct{}
	enqueue chan<- interface{}
	wg      *sync.WaitGroup

	// Robots validation
	robotsGroup *robotstxt.Group

	// Logging
	logFunc func(LogFlags, string, ...interface{})

	// Implementation fields
	wait           <-chan time.Time
	lastFetch      *FetchInfo
	lastCrawlDelay time.Duration
	opts           *Options
}

// Start crawling the host.
func (w *worker) run() {
	defer func() {
		w.logFunc(LogInfo, "worker done.")
		w.wg.Done()
	}()

	// Enter loop to process URLs until stop signal is received
	for {
		var idleChan <-chan time.Time

		w.logFunc(LogInfo, "waiting for pop...")

		// Initialize the idle timeout channel, if required
		if w.opts.WorkerIdleTTL > 0 {
			idleChan = time.After(w.opts.WorkerIdleTTL)
		}

		select {
		case <-w.stop:
			w.logFunc(LogInfo, "stop signal received.")
			return

		case <-idleChan:
			w.logFunc(LogInfo, "idle timeout received.")
			w.sendResponse(nil, false, nil, true)
			return

		case batch := <-w.pop:

			// Got a batch of urls to crawl, loop and check at each iteration if a stop
			// is received.
			for _, ctx := range batch {
				w.logFunc(LogInfo, "popped: %s", ctx.url)

				if ctx.IsRobotsURL() {
					w.requestRobotsTxt(ctx)
				} else if w.isAllowedPerRobotsPolicies(ctx.url) {
					w.requestURL(ctx, ctx.HeadBeforeGet)
				} else {
					// Must still notify Crawler that this URL was processed, although not visited
					w.opts.Extender.Disallowed(ctx)
					w.sendResponse(ctx, false, nil, false)
				}

				// No need to check for idle timeout here, no idling while looping through
				// a batch of URLs.
				select {
				case <-w.stop:
					w.logFunc(LogInfo, "stop signal received.")
					return
				default:
					// Nothing, just continue...
				}
			}
		}
	}
}

// Checks if the given URL can be fetched based on robots.txt policies.
func (w *worker) isAllowedPerRobotsPolicies(u *url.URL) bool {
	if w.robotsGroup != nil {
		// Is this URL allowed per robots.txt policy?
		ok := w.robotsGroup.Test(u.Path)
		if !ok {
			w.logFunc(LogIgnored, "ignored on robots.txt policy: %s", u.String())
		}
		return ok
	}

	// No robots.txt = everything is allowed
	return true
}

// Process the specified URL.
func (w *worker) requestURL(ctx *URLContext, headRequest bool) {
	if res, ok := w.fetchURL(ctx, w.opts.UserAgent, headRequest); ok {
		var harvested interface{}
		var visited bool

		// Close the body on function end. This is the body returned by
		// Fetch; when the budget is enabled, the worker may replace
		// res.Body with a replay reader for the hooks, but the original
		// body still gets closed here.
		defer res.Body.Close()

		// Any 2xx status code is good to go
		if res.StatusCode >= 200 && res.StatusCode < 300 {
			// Success, visit the URL
			harvested, visited = w.visitURL(ctx, res)
		} else {
			// Error based on status code received
			w.opts.Extender.Error(newCrawlErrorMessage(ctx, res.Status, CekHttpStatusCode))
			w.logFunc(LogError, "ERROR status code for %s: %s", ctx.url, res.Status)
		}
		w.sendResponse(ctx, visited, harvested, false)
	}
}

// Process the robots.txt URL.
func (w *worker) requestRobotsTxt(ctx *URLContext) {
	// Ask if it should be fetched
	if robData, reqRob := w.opts.Extender.RequestRobots(ctx, w.opts.RobotUserAgent); !reqRob {
		w.logFunc(LogInfo, "using robots.txt from cache")
		w.robotsGroup = w.getRobotsTxtGroup(ctx, robData, nil)

	} else if res, ok := w.fetchURL(ctx, w.opts.UserAgent, false); ok {
		// Close the body on function end
		defer res.Body.Close()

		if maxReplayBuffer, maxBodyBytes, enabled := bodyBudget(w.opts); enabled {
			w.robotsGroup = w.getRobotsTxtGroupBudgeted(ctx, res, maxReplayBuffer, maxBodyBytes)
		} else {
			w.robotsGroup = w.getRobotsTxtGroup(ctx, nil, res)
		}
	}
}

// Get the robots.txt group for this crawler, without a body budget. This is
// the historical implementation: the whole body is read in memory and the
// FetchedRobots hook receives a complete, rewound body.
func (w *worker) getRobotsTxtGroup(ctx *URLContext, b []byte, res *http.Response) (g *robotstxt.Group) {
	var data *robotstxt.RobotsData
	var e error

	if res != nil {
		var buf bytes.Buffer
		io.Copy(&buf, res.Body)
		res.Body = ioutil.NopCloser(bytes.NewReader(buf.Bytes()))
		data, e = robotstxt.FromResponse(res)
		// Rewind the res.Body (by re-creating it from the bytes)
		res.Body = ioutil.NopCloser(bytes.NewReader(buf.Bytes()))
		// Error or not, the robots.txt has been fetched, so notify
		w.opts.Extender.FetchedRobots(ctx, res)
	} else {
		data, e = robotstxt.FromBytes(b)
	}

	// If robots data cannot be parsed, will return nil, which will allow access by default.
	// Reasonable, since by default no robots.txt means full access, so invalid
	// robots.txt is similar behavior.
	if e != nil {
		w.opts.Extender.Error(newCrawlError(nil, e, CekParseRobots))
		w.logFunc(LogError, "ERROR parsing robots.txt for host %s: %s", w.host, e)
	} else {
		g = data.FindGroup(w.opts.RobotUserAgent)
	}
	return g
}

// getRobotsTxtGroupBudgeted reads the robots.txt body under the configured
// body budget. On a size/read/spill error it fails closed for this host: a
// deny-all group is returned and FetchedRobots is not called, so any cached
// robots data is left untouched. Other hosts use their own worker and are
// not affected.
func (w *worker) getRobotsTxtGroupBudgeted(ctx *URLContext, res *http.Response, maxReplayBuffer, maxBodyBytes int) *robotstxt.Group {
	body, err := collectBudgetBody(res.Body, maxReplayBuffer, maxBodyBytes)
	if err != nil {
		w.reportBodyError(ctx, err, true)
		return denyAllRobotsGroup()
	}
	defer body.Close()

	// Parse the robots.txt from a complete reader.
	parseReader, err := body.newReader()
	if err != nil {
		w.reportBodyError(ctx, err, true)
		return denyAllRobotsGroup()
	}
	oldBody := res.Body
	res.Body = parseReader
	data, parseErr := func() (*robotstxt.RobotsData, error) {
		defer parseReader.Close()
		return robotstxt.FromResponse(res)
	}()
	res.Body = oldBody

	// The hook receives the complete body, rewound to its start.
	hookReader, err := body.newReader()
	if err != nil {
		w.reportBodyError(ctx, err, true)
		return denyAllRobotsGroup()
	}
	defer hookReader.Close()
	res.Body = hookReader
	w.opts.Extender.FetchedRobots(ctx, res)

	// If robots data cannot be parsed, return nil, which allows access by
	// default. This matches the historical behaviour; only body-budget
	// failures fail closed.
	if parseErr != nil {
		w.opts.Extender.Error(newCrawlError(nil, parseErr, CekParseRobots))
		w.logFunc(LogError, "ERROR parsing robots.txt for host %s: %s", w.host, parseErr)
		return nil
	}
	return data.FindGroup(w.opts.RobotUserAgent)
}

var robotsDenyAllData = func() *robotstxt.RobotsData {
	data, err := robotstxt.FromBytes([]byte("User-agent: *\nDisallow: /\n"))
	if err != nil {
		panic(err)
	}
	return data
}()

// denyAllRobotsGroup returns a group that disallows every path. It is used
// when the robots.txt body cannot be retrieved or stored within the budget.
func denyAllRobotsGroup() *robotstxt.Group {
	return robotsDenyAllData.FindGroup("*")
}

// Set the crawl delay between this request and the next.
func (w *worker) setCrawlDelay() {
	var robDelay time.Duration

	if w.robotsGroup != nil {
		robDelay = w.robotsGroup.CrawlDelay
	}
	w.lastCrawlDelay = w.opts.Extender.ComputeDelay(w.host,
		&DelayInfo{
			w.opts.CrawlDelay,
			robDelay,
			w.lastCrawlDelay,
		},
		w.lastFetch)
	w.logFunc(LogInfo, "using crawl-delay: %v", w.lastCrawlDelay)
}

// Request the specified URL and return the response.
func (w *worker) fetchURL(ctx *URLContext, agent string, headRequest bool) (res *http.Response, ok bool) {
	var e error
	var silent bool

	for {
		// Wait for crawl delay, if one is pending.
		w.logFunc(LogTrace, "waiting for crawl delay")
		if w.wait != nil {
			<-w.wait
			w.wait = nil
		}

		// Compute the next delay
		w.setCrawlDelay()

		// Compute the fetch duration
		now := time.Now()

		// Request the URL
		if res, e = w.opts.Extender.Fetch(ctx, agent, headRequest); e != nil {
			// Check if this is an ErrEnqueueRedirect, in which case we will enqueue
			// the redirect-to URL.
			if ue, ok := e.(*url.Error); ok {
				// We have a *url.Error, check if it was returned because of an ErrEnqueueRedirect
				if ue.Err == ErrEnqueueRedirect {
					// Do not notify this error outside of this if block, this is not a
					// "real" error. We either enqueue the new URL, or fail to parse it,
					// and then stop processing the current URL.
					silent = true
					// Parse the URL in the context of the original URL (so that relative URLs are ok).
					// Absolute URLs that point to another host are ok too.
					if ur, e := ctx.url.Parse(ue.URL); e != nil {
						// Notify error
						w.opts.Extender.Error(newCrawlError(nil, e, CekParseRedirectURL))
						w.logFunc(LogError, "ERROR parsing redirect URL %s: %s", ue.URL, e)
					} else {
						w.logFunc(LogTrace, "redirect to %s from %s, linked from %s", ur, ctx.URL(), ctx.SourceURL())
						// Enqueue the redirect-to URL with the original source
						rCtx := ctx.cloneForRedirect(ur, w.opts.URLNormalizationFlags)
						w.enqueue <- rCtx
					}
				}
			}

			// No fetch, so set to nil
			w.lastFetch = nil

			if !silent {
				// Notify error
				w.opts.Extender.Error(newCrawlError(ctx, e, CekFetch))
				w.logFunc(LogError, "ERROR fetching %s: %s", ctx.url, e)
			}

			// Return from this URL crawl
			w.sendResponse(ctx, false, nil, false)
			return nil, false

		}
		// Get the fetch duration
		fetchDuration := time.Now().Sub(now)
		// Crawl delay starts now.
		w.wait = time.After(w.lastCrawlDelay)

		// Keep trace of this last fetch info
		w.lastFetch = &FetchInfo{
			ctx,
			fetchDuration,
			res.StatusCode,
			headRequest,
		}

		if headRequest {
			// Close the HEAD request's body
			defer res.Body.Close()
			// Next up is GET request, maybe
			headRequest = false
			// Ask caller if we should proceed with a GET
			if !w.opts.Extender.RequestGet(ctx, res) {
				w.logFunc(LogIgnored, "ignored on HEAD filter policy: %s", ctx.url)
				w.sendResponse(ctx, false, nil, false)
				ok = false
				break
			}
		} else {
			ok = true
			break
		}
	}
	return
}

// Send a response to the crawler.
func (w *worker) sendResponse(ctx *URLContext, visited bool, harvested interface{}, idleDeath bool) {
	// Push harvested urls back to crawler, even if empty (uses the channel communication
	// to decrement reference count of pending URLs)
	if ctx == nil || !isRobotsURL(ctx.url) {
		// If a stop signal has been received, ignore the response, since the push
		// channel may be full and could block indefinitely.
		select {
		case <-w.stop:
			w.logFunc(LogInfo, "ignoring send response, will stop.")
			return
		default:
			// Nothing, just continue...
		}

		// No stop signal, send the response
		res := &workerResponse{
			ctx,
			visited,
			harvested,
			w.host,
			idleDeath,
		}
		w.push <- res
	}
}

// Report a body budget failure exactly once through the Error hook. The
// kind identifies whether the body was too large, could not be stored, or
// could not be read.
func (w *worker) reportBodyError(ctx *URLContext, err error, robots bool) {
	kind := CekReadBody
	switch {
	case errors.Is(err, ErrBodyTooLarge):
		kind = CekBodyTooLarge
	case errors.Is(err, ErrBodySpill):
		kind = CekBodySpill
	}
	w.opts.Extender.Error(newCrawlError(ctx, err, kind))
	if robots {
		w.logFunc(LogError, "ERROR reading robots.txt body for host %s: %s", w.host, err)
	} else {
		w.logFunc(LogError, "ERROR reading body %s: %s", ctx.url, err)
	}
}

// Process the response for a URL and return the harvested value and whether
// the URL was successfully visited.
func (w *worker) visitURL(ctx *URLContext, res *http.Response) (interface{}, bool) {
	maxReplayBuffer, maxBodyBytes, enabled := bodyBudget(w.opts)
	if !enabled {
		return w.visitURLUnbounded(ctx, res), true
	}

	body, err := collectBudgetBody(res.Body, maxReplayBuffer, maxBodyBytes)
	if err != nil {
		// Once the body cannot be read/stored within the budget, the page
		// is not visited: no Visit/Visited hook, no link harvesting, and
		// the crawler must not count it as a successful visit.
		w.reportBodyError(ctx, err, false)
		return nil, false
	}
	defer body.Close()

	var doc *goquery.Document

	// Parse the document from a complete reader. The reader is separate
	// from the one handed to the hooks, so each consumer starts at byte 0
	// even when the body is backed by a temporary file.
	parseReader, err := body.newReader()
	if err != nil {
		w.reportBodyError(ctx, err, false)
		return nil, false
	}
	node, parseErr := func() (*html.Node, error) {
		defer parseReader.Close()
		return html.Parse(parseReader)
	}()
	if parseErr != nil {
		w.opts.Extender.Error(newCrawlError(ctx, parseErr, CekParseBody))
		w.logFunc(LogError, "ERROR parsing %s: %s", ctx.url, parseErr)
	} else {
		doc = goquery.NewDocumentFromNode(node)
		doc.Url = res.Request.URL
	}

	// The visitor receives the complete body, rewound to its start, even
	// when it was spilled to a temporary file.
	hookReader, err := body.newReader()
	if err != nil {
		w.reportBodyError(ctx, err, false)
		return nil, false
	}
	defer hookReader.Close()
	res.Body = hookReader

	var harvested interface{}
	var doLinks bool

	// Visit the document (with nil goquery doc if failed to load)
	if harvested, doLinks = w.opts.Extender.Visit(ctx, res, doc); doLinks {
		// Links were not processed by the visitor, so process links
		if doc != nil {
			harvested = w.processLinks(doc)
		} else {
			w.opts.Extender.Error(newCrawlErrorMessage(ctx, "No goquery document to process links.", CekProcessLinks))
			w.logFunc(LogError, "ERROR processing links %s", ctx.url)
		}
	}
	// Notify that this URL has been visited
	w.opts.Extender.Visited(ctx, harvested)

	return harvested, true
}

// visitURLUnbounded is the historical, unbudgeted implementation. It reads
// the whole body in memory and always lets the Visit/Visited hooks run.
func (w *worker) visitURLUnbounded(ctx *URLContext, res *http.Response) interface{} {
	var doc *goquery.Document
	var harvested interface{}
	var doLinks bool

	// Load a goquery document and call the visitor function
	if bd, e := ioutil.ReadAll(res.Body); e != nil {
		w.opts.Extender.Error(newCrawlError(ctx, e, CekReadBody))
		w.logFunc(LogError, "ERROR reading body %s: %s", ctx.url, e)
	} else {
		if node, e := html.Parse(bytes.NewBuffer(bd)); e != nil {
			w.opts.Extender.Error(newCrawlError(ctx, e, CekParseBody))
			w.logFunc(LogError, "ERROR parsing %s: %s", ctx.url, e)
		} else {
			doc = goquery.NewDocumentFromNode(node)
			doc.Url = res.Request.URL
		}
		// Re-assign the body so it can be consumed by the visitor function
		res.Body = ioutil.NopCloser(bytes.NewBuffer(bd))
	}

	// Visit the document (with nil goquery doc if failed to load)
	if harvested, doLinks = w.opts.Extender.Visit(ctx, res, doc); doLinks {
		// Links were not processed by the visitor, so process links
		if doc != nil {
			harvested = w.processLinks(doc)
		} else {
			w.opts.Extender.Error(newCrawlErrorMessage(ctx, "No goquery document to process links.", CekProcessLinks))
			w.logFunc(LogError, "ERROR processing links %s", ctx.url)
		}
	}
	// Notify that this URL has been visited
	w.opts.Extender.Visited(ctx, harvested)

	return harvested
}

func handleBaseTag(root *url.URL, baseHref string, aHref string) string {
	resolvedBase, err := root.Parse(baseHref)
	if err != nil {
		return ""
	}

	parsedURL, err := url.Parse(aHref)
	if err != nil {
		return ""
	}
	// If a[href] starts with a /, it overrides the base[href]
	if parsedURL.Host == "" && !strings.HasPrefix(aHref, "/") {
		aHref = path.Join(resolvedBase.Path, aHref)
	}

	resolvedURL, err := resolvedBase.Parse(aHref)
	if err != nil {
		return ""
	}
	return resolvedURL.String()
}

var (
	aHrefMatcher    = cascadia.MustCompile("a[href]")
	baseHrefMatcher = cascadia.MustCompile("base[href]")
)

// Scrape the document's content to gather all links
func (w *worker) processLinks(doc *goquery.Document) (result []*url.URL) {
	baseURL, _ := doc.FindMatcher(baseHrefMatcher).Attr("href")
	urls := doc.FindMatcher(aHrefMatcher).Map(func(_ int, s *goquery.Selection) string {
		val, _ := s.Attr("href")
		if baseURL != "" {
			val = handleBaseTag(doc.Url, baseURL, val)
		}
		return val
	})
	for _, s := range urls {
		// If href starts with "#", then it points to this same exact URL, ignore (will fail to parse anyway)
		if len(s) > 0 && !strings.HasPrefix(s, "#") {
			if parsed, e := url.Parse(s); e == nil {
				parsed = doc.Url.ResolveReference(parsed)
				result = append(result, parsed)
			} else {
				w.logFunc(LogIgnored, "ignore on unparsable policy %s: %s", s, e.Error())
			}
		}
	}
	return
}
