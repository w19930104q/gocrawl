package gocrawl

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
)

const (
	bodyReadChunk   = 32 * 1024
	bodyTempPattern = "gocrawl-body-"
)

// bodyBudget returns the effective (maxReplayBuffer, maxBodyBytes, enabled)
// values for the specified options.
//
// A zero (or negative) value for both limits leaves the budget disabled,
// which preserves the historical behaviour of reading the whole body in
// memory. If only one limit is set, the other defaults to it; this makes
// each limit meaningful on its own. If the replay buffer is larger than the
// body limit, it is capped at the body limit, enforcing 0 < M <= L.
func bodyBudget(o *Options) (maxReplayBuffer, maxBodyBytes int, enabled bool) {
	maxReplayBuffer = o.MaxReplayBuffer
	if maxReplayBuffer <= 0 {
		maxReplayBuffer = o.MaxReplayBufferBytes
	}
	maxBodyBytes = o.MaxBodyBytes
	if maxBodyBytes <= 0 {
		maxBodyBytes = o.MaxBodySize
	}
	if maxReplayBuffer <= 0 && maxBodyBytes <= 0 {
		return 0, 0, false
	}
	if maxReplayBuffer <= 0 {
		maxReplayBuffer = maxBodyBytes
	}
	if maxBodyBytes <= 0 {
		maxBodyBytes = maxReplayBuffer
	}
	if maxReplayBuffer > maxBodyBytes {
		maxReplayBuffer = maxBodyBytes
	}
	return maxReplayBuffer, maxBodyBytes, true
}

// storedBody is the result of reading a body under a budget. Small bodies
// are kept in memory; bodies bigger than the replay buffer are written to a
// temporary file and are never read back entirely into memory.
type storedBody struct {
	data []byte
	path string
	file *os.File
	size int
}

// newReader returns an independent reader positioned at the start of the
// stored body. For spilled bodies, the reader reads from the temporary file.
func (sb *storedBody) newReader() (io.ReadCloser, error) {
	if sb.path == "" {
		return ioutil.NopCloser(bytes.NewReader(sb.data)), nil
	}
	f, err := os.Open(sb.path)
	if err != nil {
		return nil, fmt.Errorf("cannot replay stored body: %w", err)
	}
	return f, nil
}

// Close releases the temporary file, if any, and removes it from disk.
func (sb *storedBody) Close() error {
	var err error
	if sb.file != nil {
		_ = sb.file.Close()
		sb.file = nil
	}
	if sb.path != "" {
		if rerr := os.Remove(sb.path); rerr != nil && !os.IsNotExist(rerr) {
			err = rerr
		}
		sb.path = ""
	}
	return err
}

// collectBudgetBody reads src until EOF, enforcing maxBodyBytes and keeping
// at most maxReplayBuffer bytes in memory. Reading stops as soon as the body
// is known to exceed maxBodyBytes (at most maxBodyBytes+1 bytes are read).
func collectBudgetBody(src io.Reader, maxReplayBuffer, maxBodyBytes int) (*storedBody, error) {
	sb := &storedBody{}
	ok := false
	defer func() {
		if !ok {
			_ = sb.Close()
		}
	}()

	// Do not use a working buffer larger than the replay budget: the bytes
	// held between reads are part of what M bounds.
	chunkSize := bodyReadChunk
	if maxReplayBuffer < chunkSize {
		chunkSize = maxReplayBuffer
	}
	if chunkSize < 1 {
		chunkSize = 1
	}
	buf := make([]byte, chunkSize)
	emptyReads := 0

	for {
		// Read one byte past the allowed size, to distinguish a body of
		// exactly maxBodyBytes from a bigger one.
		remaining := maxBodyBytes - sb.size + 1
		want := chunkSize
		if remaining < want {
			want = remaining
		}

		n, err := src.Read(buf[:want])
		if n > 0 {
			emptyReads = 0
			sb.size += n
			if sb.size > maxBodyBytes {
				return nil, fmt.Errorf("%w: got more than %d bytes", ErrBodyTooLarge, maxBodyBytes)
			}

			// Spill to a temporary file once the retained body no longer
			// fits in the replay buffer. The bytes already buffered are
			// moved to the file and the in-memory copy is released.
			if sb.file == nil && sb.size > maxReplayBuffer {
				f, ferr := ioutil.TempFile("", bodyTempPattern)
				if ferr != nil {
					return nil, fmt.Errorf("%w: %v", ErrBodySpill, ferr)
				}
				sb.file = f
				sb.path = f.Name()
				if len(sb.data) > 0 {
					if _, werr := f.Write(sb.data); werr != nil {
						return nil, fmt.Errorf("%w: %v", ErrBodySpill, werr)
					}
				}
				sb.data = nil
			}

			if sb.file != nil {
				if _, werr := sb.file.Write(buf[:n]); werr != nil {
					return nil, fmt.Errorf("%w: %v", ErrBodySpill, werr)
				}
			} else {
				sb.data = append(sb.data, buf[:n]...)
			}
		}

		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		if n == 0 {
			// Guard against readers that return (0, nil) forever.
			emptyReads++
			if emptyReads >= 100 {
				return nil, io.ErrNoProgress
			}
		}
	}

	if sb.file != nil {
		if cerr := sb.file.Close(); cerr != nil {
			sb.file = nil
			return nil, fmt.Errorf("%w: %v", ErrBodySpill, cerr)
		}
		sb.file = nil
	}
	ok = true
	return sb, nil
}
