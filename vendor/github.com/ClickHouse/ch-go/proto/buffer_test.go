package proto

import (
	"io"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ClickHouse/ch-go/internal/gold"
)

func TestBuffer(t *testing.T) {
	const (
		vStr    string = "Hello, world!"
		vInt    int    = 1
		vInt8   int8   = 2
		vInt16  int16  = 3
		vInt32  int32  = 4
		vInt64  int64  = 5
		vUInt8  uint8  = 1
		vUInt16 uint16 = 2
		vUInt32 uint32 = 3
		vUInt64 uint64 = 4

		vUVarInt1 uint64 = 100
		vUVarInt2 uint64 = 200
		vLen      int    = 114

		vBool1 bool = true
		vBool2 bool = false

		vByte byte = 1

		vFloat32 float32 = 1.12345
		vFloat64 float64 = 500.345
	)
	var (
		vInt128  = Int128{110, 14879}
		vUInt128 = UInt128{100, 504124}
		vRaw     = []byte{1, 2, 3, 4}
	)

	var b Buffer
	b.PutString(vStr)
	b.PutInt(vInt)
	b.PutInt8(vInt8)
	b.PutInt16(vInt16)
	b.PutInt32(vInt32)
	b.PutInt64(vInt64)
	b.PutInt128(vInt128)
	b.PutUInt8(vUInt8)
	b.PutUInt16(vUInt16)
	b.PutUInt32(vUInt32)
	b.PutUInt64(vUInt64)
	b.PutUInt128(vUInt128)
	b.PutUVarInt(vUVarInt1)
	b.PutUVarInt(vUVarInt2)
	b.PutLen(vLen)
	b.PutRaw(vRaw)
	b.PutBool(vBool1)
	b.PutBool(vBool2)
	b.PutByte(vByte)
	b.PutFloat32(vFloat32)
	b.PutFloat64(vFloat64)

	t.Run("Golden", func(t *testing.T) {
		gold.Bytes(t, b.Buf, "buffer")
	})
	t.Run("Reader", func(t *testing.T) {
		r := b.Reader()
		{
			v, err := r.Str()
			assert.NoError(t, err)
			assert.Equal(t, vStr, v)
		}
		{
			v, err := r.Int()
			assert.NoError(t, err)
			assert.Equal(t, 1, v)
		}
		{
			v, err := r.Int8()
			assert.NoError(t, err)
			assert.Equal(t, vInt8, v)
		}
		{
			v, err := r.Int16()
			assert.NoError(t, err)
			assert.Equal(t, vInt16, v)
		}
		{
			v, err := r.Int32()
			assert.NoError(t, err)
			assert.Equal(t, vInt32, v)
		}
		{
			v, err := r.Int64()
			assert.NoError(t, err)
			assert.Equal(t, vInt64, v)
		}
		{
			v, err := r.Int128()
			assert.NoError(t, err)
			assert.Equal(t, vInt128, v)
		}
		{
			v, err := r.UInt8()
			assert.NoError(t, err)
			assert.Equal(t, vUInt8, v)
		}
		{
			v, err := r.UInt16()
			assert.NoError(t, err)
			assert.Equal(t, vUInt16, v)
		}
		{
			v, err := r.UInt32()
			assert.NoError(t, err)
			assert.Equal(t, vUInt32, v)
		}
		{
			v, err := r.UInt64()
			assert.NoError(t, err)
			assert.Equal(t, vUInt64, v)
		}
		{
			v, err := r.UInt128()
			assert.NoError(t, err)
			assert.Equal(t, vUInt128, v)
		}
		{
			v, err := r.UVarInt()
			assert.NoError(t, err)
			assert.Equal(t, vUVarInt1, v)
		}
		{
			v, err := r.UVarInt()
			assert.NoError(t, err)
			assert.Equal(t, vUVarInt2, v)
		}
		{
			v, err := r.StrLen()
			assert.NoError(t, err)
			assert.Equal(t, vLen, v)
		}
		{
			v, err := r.ReadRaw(len(vRaw))
			assert.NoError(t, err)
			assert.Equal(t, vRaw, v)
		}
		{
			v, err := r.Bool()
			assert.NoError(t, err)
			assert.Equal(t, vBool1, v)
		}
		{
			v, err := r.Bool()
			assert.NoError(t, err)
			assert.Equal(t, vBool2, v)
		}
		{
			v, err := r.Byte()
			assert.NoError(t, err)
			assert.Equal(t, vByte, v)
		}
		{
			v, err := r.Float32()
			assert.NoError(t, err)
			assert.Equal(t, vFloat32, v)
		}
		{
			v, err := r.Float64()
			assert.NoError(t, err)
			assert.Equal(t, vFloat64, v)
		}
		assert.ErrorIs(t, r.ReadFull([]byte{1}), io.EOF)
	})
}
