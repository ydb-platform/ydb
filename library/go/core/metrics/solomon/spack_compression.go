package solomon

import (
	"encoding/binary"
	"io"

	"github.com/OneOfOne/xxhash"
	"github.com/pierrec/lz4"
)

type CompressionType uint8

const (
	CompressionNone CompressionType = 0x0
	CompressionZlib CompressionType = 0x1
	CompressionZstd CompressionType = 0x2
	CompressionLz4  CompressionType = 0x3
)

const (
	compressionFrameLength = 512 * 1024
	hashTableSize          = 64 * 1024
)

type noCompressionWriteCloser struct {
	underlying io.Writer
	written    int
}

func (w *noCompressionWriteCloser) Write(p []byte) (int, error) {
	n, err := w.underlying.Write(p)
	w.written += n
	return n, err
}

func (w *noCompressionWriteCloser) Close() error {
	return nil
}

type lz4CompressionWriteCloser struct {
	underlying io.Writer
	buffer     []byte
	table      []int
	written    int
}

func (w *lz4CompressionWriteCloser) flushFrame() (written int, err error) {
	src := w.buffer
	dst := make([]byte, lz4.CompressBlockBound(len(src)))

	sz, err := lz4.CompressBlock(src, dst, w.table)
	if err != nil {
		return written, err
	}

	if sz == 0 {
		dst = src
	} else {
		dst = dst[:sz]
	}

	err = binary.Write(w.underlying, binary.LittleEndian, uint32(len(dst)))
	if err != nil {
		return written, err
	}
	w.written += 4

	err = binary.Write(w.underlying, binary.LittleEndian, uint32(len(src)))
	if err != nil {
		return written, err
	}
	w.written += 4

	n, err := w.underlying.Write(dst)
	if err != nil {
		return written, err
	}
	w.written += n

	checksum := xxhash.Checksum32S(dst, 0x1337c0de)
	err = binary.Write(w.underlying, binary.LittleEndian, checksum)
	if err != nil {
		return written, err
	}
	w.written += 4

	w.buffer = w.buffer[:0]

	return written, nil
}

func (w *lz4CompressionWriteCloser) Write(p []byte) (written int, err error) {
	q := p[:]
	for len(q) > 0 {
		space := compressionFrameLength - len(w.buffer)
		if space == 0 {
			n, err := w.flushFrame()
			if err != nil {
				return written, err
			}
			w.written += n
			space = compressionFrameLength
		}
		length := len(q)
		if length > space {
			length = space
		}
		w.buffer = append(w.buffer, q[:length]...)
		q = q[length:]
	}
	return written, nil
}

func (w *lz4CompressionWriteCloser) Close() error {
	var err error
	if len(w.buffer) > 0 {
		n, err := w.flushFrame()
		if err != nil {
			return err
		}
		w.written += n
	}
	err = binary.Write(w.underlying, binary.LittleEndian, uint32(0))
	if err != nil {
		return nil
	}
	w.written += 4

	err = binary.Write(w.underlying, binary.LittleEndian, uint32(0))
	if err != nil {
		return nil
	}
	w.written += 4

	err = binary.Write(w.underlying, binary.LittleEndian, uint32(0))
	if err != nil {
		return nil
	}
	w.written += 4

	return nil
}

func newCompressedWriter(w io.Writer, compression CompressionType) io.WriteCloser {
	switch compression {
	case CompressionNone:
		return &noCompressionWriteCloser{w, 0}
	case CompressionZlib:
		panic("zlib compression not supported")
	case CompressionZstd:
		panic("zstd compression not supported")
	case CompressionLz4:
		return &lz4CompressionWriteCloser{
			w,
			make([]byte, 0, compressionFrameLength),
			make([]int, hashTableSize),
			0,
		}
	default:
		panic("unsupported compression algorithm")
	}
}
