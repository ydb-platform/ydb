package compress

import (
	"bytes"
	"encoding/binary"
	"io"
	"testing"

	"github.com/go-faster/city"
	"github.com/stretchr/testify/require"
)

func FuzzWriter_Compress(f *testing.F) {
	f.Add([]byte("Hello, world!"))
	f.Add([]byte{})
	f.Add([]byte{1, 2, 3, 4, 5})
	f.Fuzz(func(t *testing.T, data []byte) {
		w := NewWriter()
		require.NoError(t, w.Compress(LZ4, data))

		r := NewReader(bytes.NewReader(w.Data))
		out := make([]byte, len(data))
		_, err := io.ReadFull(r, out)
		require.NoError(t, err)
		require.Equal(t, data, out)
	})
}

func FuzzReader_Read(f *testing.F) {
	for _, data := range [][]byte{
		{},
		[]byte("Hello, world!"),
		{1, 2, 3, 4, 5},
	} {
		w := NewWriter()
		require.NoError(f, w.Compress(LZ4, data))
		f.Add(w.Data)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > headerSize {
			h := city.CH128(data[hMethod:])
			binary.LittleEndian.PutUint64(data[0:8], h.Low)
			binary.LittleEndian.PutUint64(data[8:16], h.High)
		}

		r := NewReader(bytes.NewReader(data))
		out := make([]byte, len(data))
		_, _ = io.ReadFull(r, out)
	})
}
