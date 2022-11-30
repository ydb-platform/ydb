package blocklz4

import (
	"encoding/binary"

	"github.com/pierrec/lz4"

	"a.yandex-team.ru/library/go/blockcodecs"
)

type lz4Codec struct{}

func (l lz4Codec) ID() blockcodecs.CodecID {
	return 6051
}

func (l lz4Codec) Name() string {
	return "lz4-fast14-safe"
}

func (l lz4Codec) DecodedLen(in []byte) (int, error) {
	return blockcodecs.DecodedLen(in)
}

func (l lz4Codec) Encode(dst, src []byte) ([]byte, error) {
	dst = dst[:cap(dst)]

	n := lz4.CompressBlockBound(len(src)) + 8
	if len(dst) < n {
		dst = append(dst, make([]byte, n-len(dst))...)
	}
	binary.LittleEndian.PutUint64(dst, uint64(len(src)))

	m, err := lz4.CompressBlock(src, dst[8:], nil)
	if err != nil {
		return nil, err
	}

	return dst[:8+m], nil
}

func (l lz4Codec) Decode(dst, src []byte) ([]byte, error) {
	n, err := lz4.UncompressBlock(src[8:], dst)
	if err != nil {
		return nil, err
	}
	return dst[:n], nil
}

type lz4HCCodec struct {
	lz4Codec
}

func (l lz4HCCodec) ID() blockcodecs.CodecID {
	return 62852
}

func (l lz4HCCodec) Name() string {
	return "lz4-hc-safe"
}

func (l lz4HCCodec) Encode(dst, src []byte) ([]byte, error) {
	dst = dst[:cap(dst)]

	n := lz4.CompressBlockBound(len(src)) + 8
	if len(dst) < n {
		dst = append(dst, make([]byte, n-len(dst))...)
	}
	binary.LittleEndian.PutUint64(dst, uint64(len(src)))

	m, err := lz4.CompressBlockHC(src, dst[8:], 0)
	if err != nil {
		return nil, err
	}

	return dst[:8+m], nil
}

func init() {
	blockcodecs.Register(lz4Codec{})
	blockcodecs.Register(lz4HCCodec{})
}
