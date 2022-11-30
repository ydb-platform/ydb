package blockzstd

import (
	"encoding/binary"
	"fmt"

	"github.com/klauspost/compress/zstd"

	"a.yandex-team.ru/library/go/blockcodecs"
)

type zstdCodec int

func (z zstdCodec) ID() blockcodecs.CodecID {
	switch z {
	case 1:
		return 55019
	case 3:
		return 23308
	case 7:
		return 33533
	default:
		panic("unsupported level")
	}
}

func (z zstdCodec) Name() string {
	return fmt.Sprintf("zstd08_%d", z)
}

func (z zstdCodec) DecodedLen(in []byte) (int, error) {
	return blockcodecs.DecodedLen(in)
}

func (z zstdCodec) Encode(dst, src []byte) ([]byte, error) {
	if cap(dst) < 8 {
		dst = make([]byte, 8)
	}

	dst = dst[:8]
	binary.LittleEndian.PutUint64(dst, uint64(len(src)))

	w, err := zstd.NewWriter(nil,
		zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(int(z))),
		zstd.WithEncoderConcurrency(1))
	if err != nil {
		return nil, err
	}

	defer w.Close()
	return w.EncodeAll(src, dst), nil
}

func (z zstdCodec) Decode(dst, src []byte) ([]byte, error) {
	if len(src) < 8 {
		return nil, fmt.Errorf("short block: %d < 8", len(src))
	}

	r, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
	if err != nil {
		return nil, err
	}

	defer r.Close()
	return r.DecodeAll(src[8:], dst[:0])
}

func init() {
	for _, i := range []int{1, 3, 7} {
		blockcodecs.Register(zstdCodec(i))
		blockcodecs.RegisterAlias(fmt.Sprintf("zstd_%d", i), zstdCodec(i))
	}
}
