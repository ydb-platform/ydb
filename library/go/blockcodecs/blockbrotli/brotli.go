package blockbrotli

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/andybalholm/brotli"

	"a.yandex-team.ru/library/go/blockcodecs"
)

type brotliCodec int

func (b brotliCodec) ID() blockcodecs.CodecID {
	switch b {
	case 1:
		return 48947
	case 10:
		return 43475
	case 11:
		return 7241
	case 2:
		return 63895
	case 3:
		return 11408
	case 4:
		return 47136
	case 5:
		return 45284
	case 6:
		return 63219
	case 7:
		return 59675
	case 8:
		return 40233
	case 9:
		return 10380
	default:
		panic("unsupported level")
	}
}

func (b brotliCodec) Name() string {
	return fmt.Sprintf("brotli_%d", b)
}

func (b brotliCodec) DecodedLen(in []byte) (int, error) {
	return blockcodecs.DecodedLen(in)
}

func (b brotliCodec) Encode(dst, src []byte) ([]byte, error) {
	if cap(dst) < 8 {
		dst = make([]byte, 8)
	}

	dst = dst[:8]
	binary.LittleEndian.PutUint64(dst, uint64(len(src)))

	wb := bytes.NewBuffer(dst)
	w := brotli.NewWriterLevel(wb, int(b))

	if _, err := w.Write(src); err != nil {
		return nil, err
	}

	if err := w.Close(); err != nil {
		return nil, err
	}

	return wb.Bytes(), nil
}

func (b brotliCodec) Decode(dst, src []byte) ([]byte, error) {
	if len(src) < 8 {
		return nil, fmt.Errorf("short block: %d < 8", len(src))
	}

	rb := bytes.NewBuffer(src[8:])
	r := brotli.NewReader(rb)

	_, err := io.ReadFull(r, dst)
	if err != nil {
		return nil, err
	}

	return dst, nil
}

func init() {
	for i := 1; i <= 11; i++ {
		blockcodecs.Register(brotliCodec(i))
	}
}
