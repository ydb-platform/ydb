package proto

import (
	"bytes"
	"encoding/binary"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/ch-go/internal/gold"
)

func TestMain(m *testing.M) {
	// Explicitly registering flags for golden files.
	gold.Init()

	os.Exit(m.Run())
}

type staticAware struct {
	AwareDecoder
}

func (s staticAware) Decode(r *Reader) error {
	return s.AwareDecoder.DecodeAware(r, Version)
}

func aware(v AwareDecoder) Decoder {
	return staticAware{AwareDecoder: v}
}

type columnAware struct {
	Column
	rows int
}

func (c columnAware) Decode(r *Reader) error {
	return c.DecodeColumn(r, c.rows)
}

func colAware(v Column, rows int) Decoder {
	return columnAware{
		Column: v,
		rows:   rows,
	}
}

func requireNoShortRead(t testing.TB, buf []byte, v Decoder) {
	t.Helper()

	for i := 0; i < len(buf); i++ {
		b := buf[:i]
		r := NewReader(bytes.NewReader(b))
		require.Error(t, v.Decode(r), "decode on short buffer should fail")
	}
}

func skipCode(t testing.TB, buf []byte, code int) []byte {
	t.Helper()

	v, n := binary.Uvarint(buf)
	if int(v) != code {
		t.Fatalf("code mismatch: %d (got) != %d (expected)", v, code)
	}

	return buf[n:]
}

func requireDecode(t testing.TB, buf []byte, v Decoder) {
	t.Helper()

	r := NewReader(bytes.NewReader(buf))
	require.NoError(t, v.Decode(r))
}
