package blocksnappy

import (
	"github.com/golang/snappy"

	"a.yandex-team.ru/library/go/blockcodecs"
)

type snappyCodec struct{}

func (s snappyCodec) ID() blockcodecs.CodecID {
	return 50986
}

func (s snappyCodec) Name() string {
	return "snappy"
}

func (s snappyCodec) DecodedLen(in []byte) (int, error) {
	return snappy.DecodedLen(in)
}

func (s snappyCodec) Encode(dst, src []byte) ([]byte, error) {
	return snappy.Encode(dst, src), nil
}

func (s snappyCodec) Decode(dst, src []byte) ([]byte, error) {
	return snappy.Decode(dst, src)
}

func init() {
	blockcodecs.Register(snappyCodec{})
}
