package blockcodecs

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

type encoder struct {
	w     io.Writer
	codec Codec

	closed bool
	header [10]byte

	buf []byte
	pos int

	scratch []byte
}

const (
	// defaultBufferSize is 32KB, same as size of buffer used in io.Copy.
	defaultBufferSize = 32 << 10
)

var (
	_ io.WriteCloser = (*encoder)(nil)
)

func (e *encoder) Write(p []byte) (int, error) {
	if e.closed {
		return 0, errors.New("blockcodecs: encoder is closed")
	}

	n := len(p)

	// Complete current block
	if e.pos != 0 {
		m := copy(e.buf[e.pos:], p)
		p = p[m:]
		e.pos += m

		if e.pos == len(e.buf) {
			e.pos = 0

			if err := e.doFlush(e.buf); err != nil {
				return 0, err
			}
		}
	}

	// Copy huge input directly to output
	for len(p) >= len(e.buf) {
		if e.pos != 0 {
			panic("broken invariant")
		}

		var chunk []byte
		if len(p) > len(e.buf) {
			chunk = p[:len(e.buf)]
			p = p[len(e.buf):]
		} else {
			chunk = p
			p = nil
		}

		if err := e.doFlush(chunk); err != nil {
			return 0, err
		}
	}

	// Store suffix in buffer
	m := copy(e.buf, p)
	e.pos += m
	if m != len(p) {
		panic("broken invariant")
	}

	return n, nil
}

func (e *encoder) Close() error {
	if e.closed {
		return nil
	}

	if err := e.Flush(); err != nil {
		return err
	}

	e.closed = true

	return e.doFlush(nil)
}

func (e *encoder) doFlush(block []byte) error {
	var err error
	e.scratch, err = e.codec.Encode(e.scratch, block)
	if err != nil {
		return fmt.Errorf("blockcodecs: block compression error: %w", err)
	}

	binary.LittleEndian.PutUint16(e.header[:2], uint16(e.codec.ID()))
	binary.LittleEndian.PutUint64(e.header[2:], uint64(len(e.scratch)))

	if _, err := e.w.Write(e.header[:]); err != nil {
		return err
	}

	if _, err := e.w.Write(e.scratch); err != nil {
		return err
	}

	return nil
}

func (e *encoder) Flush() error {
	if e.closed {
		return errors.New("blockcodecs: flushing closed encoder")
	}

	if e.pos == 0 {
		return nil
	}

	err := e.doFlush(e.buf[:e.pos])
	e.pos = 0
	return err
}

func NewEncoder(w io.Writer, codec Codec) io.WriteCloser {
	return NewEncoderBuffer(w, codec, defaultBufferSize)
}

func NewEncoderBuffer(w io.Writer, codec Codec, bufferSize int) io.WriteCloser {
	return &encoder{w: w, codec: codec, buf: make([]byte, bufferSize)}
}
