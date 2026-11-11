package gocrawl

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
)

// createBodyTempFile creates the temporary file used to spill a body that
// exceeds the in-memory replay budget. It is a package-level variable so
// that tests can redirect it to a controlled directory or make it fail.
var createBodyTempFile = func() (*os.File, error) {
	return ioutil.TempFile("", "gocrawl-body-*")
}

// budgetBody holds a fetched body that fits within the configured budget.
// The body is either fully in memory (data) or spilled to a temporary file
// (tmp). It can be replayed multiple times via reader without loading the
// whole spilled body back into memory.
type budgetBody struct {
	data []byte   // the full body, when it fits in the memory budget
	tmp  *os.File // the temporary file holding the body, when spilled
	size int64    // the total number of bytes in the body
}

// reader returns a ReadCloser positioned at the start of the body. For a
// spilled body, the returned ReadCloser streams from the temporary file; the
// callers must not use multiple readers concurrently.
func (b *budgetBody) reader() (io.ReadCloser, error) {
	if b.tmp != nil {
		if _, e := b.tmp.Seek(0, io.SeekStart); e != nil {
			return nil, e
		}
		return b.tmp, nil
	}
	return ioutil.NopCloser(bytes.NewReader(b.data)), nil
}

// close releases all resources held by the body: it closes and removes the
// temporary file, if any, and releases the in-memory copy.
func (b *budgetBody) close() {
	if b.tmp != nil {
		nm := b.tmp.Name()
		b.tmp.Close()
		os.Remove(nm)
		b.tmp = nil
	}
	b.data = nil
}

// readBodyWithBudget reads r fully, enforcing the body budget: at most
// maxBytes are accepted (if maxBytes > 0), and at most memLimit bytes are
// kept in memory for replay (if memLimit > 0), the rest being spilled to a
// temporary file. A memLimit <= 0 means the whole body is kept in memory,
// a maxBytes <= 0 means there is no limit on the body size.
//
// It does not close r, this remains the caller's responsibility. On error,
// any temporary file already created is closed and removed.
func readBodyWithBudget(r io.Reader, memLimit, maxBytes int64) (bb *budgetBody, err error) {
	var tmp *os.File

	// On error, make sure the temporary file is cleaned up.
	defer func() {
		if err != nil && tmp != nil {
			nm := tmp.Name()
			tmp.Close()
			os.Remove(nm)
		}
	}()

	var buf bytes.Buffer
	var n int64
	p := make([]byte, 32*1024)
	for {
		nr, er := r.Read(p)
		if nr > 0 {
			n += int64(nr)
			if maxBytes > 0 && n > maxBytes {
				return nil, fmt.Errorf("%w: %d bytes limit reached", ErrBodyTooLarge, maxBytes)
			}
			chunk := p[:nr]
			if tmp == nil && memLimit > 0 && int64(buf.Len()+nr) > memLimit {
				// The in-memory replay budget is exceeded, spill to a
				// temporary file.
				var f *os.File
				if f, err = createBodyTempFile(); err != nil {
					return nil, err
				}
				tmp = f
				if buf.Len() > 0 {
					if _, err = tmp.Write(buf.Bytes()); err != nil {
						return nil, err
					}
					// Release the in-memory copy.
					buf = bytes.Buffer{}
				}
			}
			if tmp != nil {
				if _, err = tmp.Write(chunk); err != nil {
					return nil, err
				}
			} else {
				buf.Write(chunk)
			}
		}
		if er != nil {
			if er == io.EOF {
				break
			}
			return nil, er
		}
	}

	bb = &budgetBody{tmp: tmp, size: n}
	if tmp == nil {
		bb.data = buf.Bytes()
	}
	return bb, nil
}
