package proto

import (
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
)

func FuzzQuery_DecodeAware(f *testing.F) {
	data, err := hex.DecodeString(queryCreateDatabaseHex)
	require.NoError(f, err)

	f.Add(data)
	f.Fuzz(func(t *testing.T, data []byte) {
		var v Query
		r := NewReader(bytes.NewReader(data))
		_, _ = r.UVarInt() // skip code

		if err := v.DecodeAware(r, queryProtoVersion); err != nil {
			t.Skip()
		}

		b := new(Buffer)
		v.EncodeAware(b, queryProtoVersion)

		br := NewReader(bytes.NewReader(b.Buf))
		_, _ = br.UVarInt() // skip code

		var dec Query
		if err := dec.DecodeAware(br, queryProtoVersion); err != nil {
			t.Fatal(err)
		}
	})
}
