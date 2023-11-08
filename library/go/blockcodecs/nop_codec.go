package blockcodecs

type nopCodec struct{}

func (n nopCodec) ID() CodecID {
	return 54476
}

func (n nopCodec) Name() string {
	return "null"
}

func (n nopCodec) DecodedLen(in []byte) (int, error) {
	return len(in), nil
}

func (n nopCodec) Encode(dst, src []byte) ([]byte, error) {
	return append(dst[:0], src...), nil
}

func (n nopCodec) Decode(dst, src []byte) ([]byte, error) {
	return append(dst[:0], src...), nil
}

func init() {
	Register(nopCodec{})
}
