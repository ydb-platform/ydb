package blockcodecs

import (
	"encoding/binary"
	"fmt"
	"io"
)

type Decoder struct {
	// optional
	codec Codec

	r        io.Reader
	header   [10]byte
	eof      bool
	checkEOF bool

	pos    int
	buffer []byte

	scratch []byte
}

func (d *Decoder) getCodec(id CodecID) (Codec, error) {
	if d.codec != nil {
		if id != d.codec.ID() {
			return nil, fmt.Errorf("blockcodecs: received block codec differs from provided: %d != %d", id, d.codec.ID())
		}

		return d.codec, nil
	}

	if codec := FindCodec(id); codec != nil {
		return codec, nil
	}

	return nil, fmt.Errorf("blockcodecs: received block with unsupported codec %d", id)
}

// SetCheckUnderlyingEOF changes EOF handling.
//
// Blockcodecs format contains end of stream separator. By default Decoder will stop right after
// that separator, without trying to read following bytes from underlying reader.
//
// That allows reading sequence of blockcodecs streams from one underlying stream of bytes,
// but messes up HTTP keep-alive, when using blockcodecs together with net/http connection pool.
//
// Setting CheckUnderlyingEOF to true, changes that. After encoutering end of stream block,
// Decoder will perform one more Read from underlying reader and check for io.EOF.
func (d *Decoder) SetCheckUnderlyingEOF(checkEOF bool) {
	d.checkEOF = checkEOF
}

func (d *Decoder) Read(p []byte) (int, error) {
	if d.eof {
		return 0, io.EOF
	}

	if d.pos == len(d.buffer) {
		if _, err := io.ReadFull(d.r, d.header[:]); err != nil {
			return 0, fmt.Errorf("blockcodecs: invalid header: %w", err)
		}

		codecID := CodecID(binary.LittleEndian.Uint16(d.header[:2]))
		size := int(binary.LittleEndian.Uint64(d.header[2:]))

		codec, err := d.getCodec(codecID)
		if err != nil {
			return 0, err
		}

		if limit := int(maxDecompressedBlockSize.Load()); size > limit {
			return 0, fmt.Errorf("blockcodecs: block size exceeds limit: %d > %d", size, limit)
		}

		if len(d.scratch) < size {
			d.scratch = append(d.scratch, make([]byte, size-len(d.scratch))...)
		}
		d.scratch = d.scratch[:size]

		if _, err := io.ReadFull(d.r, d.scratch[:]); err != nil {
			return 0, fmt.Errorf("blockcodecs: truncated block: %w", err)
		}

		decodedSize, err := codec.DecodedLen(d.scratch[:])
		if err != nil {
			return 0, fmt.Errorf("blockcodecs: corrupted block: %w", err)
		}

		if decodedSize == 0 {
			if d.checkEOF {
				var scratch [1]byte
				n, err := d.r.Read(scratch[:])
				if n != 0 {
					return 0, fmt.Errorf("blockcodecs: data after EOF block")
				}
				if err != nil && err != io.EOF {
					return 0, fmt.Errorf("blockcodecs: error after EOF block: %v", err)
				}
			}

			d.eof = true
			return 0, io.EOF
		}

		if limit := int(maxDecompressedBlockSize.Load()); decodedSize > limit {
			return 0, fmt.Errorf("blockcodecs: decoded block size exceeds limit: %d > %d", decodedSize, limit)
		}

		decodeInto := func(buf []byte) error {
			out, err := codec.Decode(buf, d.scratch)
			if err != nil {
				return fmt.Errorf("blockcodecs: corrupted block: %w", err)
			} else if len(out) != decodedSize {
				return fmt.Errorf("blockcodecs: incorrect block size: %d != %d", len(out), decodedSize)
			}

			return nil
		}

		if len(p) >= decodedSize {
			if err := decodeInto(p[:decodedSize]); err != nil {
				return 0, err
			}

			return decodedSize, nil
		}

		if len(d.buffer) < decodedSize {
			d.buffer = append(d.buffer, make([]byte, decodedSize-len(d.buffer))...)
		}
		d.buffer = d.buffer[:decodedSize]
		d.pos = decodedSize

		if err := decodeInto(d.buffer); err != nil {
			return 0, err
		}

		d.pos = 0
	}

	n := copy(p, d.buffer[d.pos:])
	d.pos += n
	return n, nil
}

// NewDecoder creates decoder that supports input in any of registered codecs.
func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{r: r}
}

// NewDecoderCodec creates decode that tries to decode input using provided codec.
func NewDecoderCodec(r io.Reader, codec Codec) *Decoder {
	return &Decoder{r: r, codec: codec}
}
