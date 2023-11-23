package protoscan

import (
	"encoding/binary"
	"io"
	"math"
)

// Fixed32 reads a fixed 4 byte value as a uint32. This proto type is
// more efficient than uint32 if values are often greater than 2^28.
func (b *base) Fixed32() (uint32, error) {
	if len(b.Data) < b.Index+4 {
		return 0, io.ErrUnexpectedEOF
	}

	v := binary.LittleEndian.Uint32(b.Data[b.Index:])
	b.Index += 4
	return v, nil
}

// Fixed64 reads a fixed 8 byte value as an uint64. This proto type is
// more efficient than uint64 if values are often greater than 2^56.
func (b *base) Fixed64() (uint64, error) {
	if len(b.Data) < b.Index+8 {
		return 0, io.ErrUnexpectedEOF
	}

	v := binary.LittleEndian.Uint64(b.Data[b.Index:])
	b.Index += 8
	return v, nil
}

// Sfixed32 reads a fixed 4 byte value signed value.
func (b *base) Sfixed32() (int32, error) {
	v, err := b.Fixed32()
	return int32(v), err
}

// Sfixed64 reads a fixed 8 byte signed value.
func (b *base) Sfixed64() (int64, error) {
	v, err := b.Fixed64()
	return int64(v), err
}

// Varint32 reads up to 32-bits of variable-length encoded data.
// Note that negative int32 values could still be encoded
// as 64-bit varints due to their leading 1s.
func (b *base) Varint32() (uint32, error) {
	var v uint32
	var err error

	b.Index, v, err = varint32(b.Data, b.Index)
	return v, err
}

func varint32(data []byte, index int) (int, uint32, error) {
	var val uint32

	// go versions at least <= 1.15 will not inline functions with
	// for loops. https://github.com/golang/go/issues/14768
	// for shift := uint(0); ; shift += 7 {
	shift := uint(0)
loop:
	if shift >= 32 {
		return index, 0, ErrIntOverflow
	}
	if len(data) <= index {
		return index, 0, io.ErrUnexpectedEOF
	}
	d := data[index]
	index++
	val |= uint32(d&0x7F) << shift
	if d >= 0x80 {
		shift += 7
		goto loop
	}

	return index, val, nil
}

// Varint64 reads up to 64-bits of variable-length encoded data.
func (b *base) Varint64() (uint64, error) {
	var v uint64
	var err error

	b.Index, v, err = varint64(b.Data, b.Index)
	return v, err
}

func varint64(data []byte, index int) (int, uint64, error) {
	var val uint64
	// go versions at least <= 1.15 will not inline functions with
	// for loops. https://github.com/golang/go/issues/14768
	// for shift := uint(0); ; shift += 7 {
	shift := uint(0)
loop:
	if shift >= 64 {
		return 0, 0, ErrIntOverflow
	}
	if len(data) <= index {
		return 0, 0, io.ErrUnexpectedEOF
	}
	d := data[index]
	index++
	val |= uint64(d&0x7F) << shift
	if d >= 0x80 {
		shift += 7
		goto loop
	}

	return index, val, nil
}

// Double values are encoded as a fixed length of 8 bytes in their IEEE-754 format.
func (b *base) Double() (float64, error) {
	v, err := b.Fixed64()
	return math.Float64frombits(v), err
}

// Float values are encoded as a fixed length of 4 bytes in their IEEE-754 format.
func (b *base) Float() (float32, error) {
	v, err := b.Fixed32()
	return math.Float32frombits(v), err
}

// Int32 reads a variable-length encoding of up to 4 bytes. This field type is
// best used if the field only has positive numbers, otherwise use sint32.
// Note, this field can also by read as an Int64.
func (b *base) Int32() (int32, error) {
	var v uint64
	var err error
	b.Index, v, err = varint64(b.Data, b.Index)

	return int32(v), err
}

// Int64 reads a variable-length encoding of up to 8 bytes. This field type is
// best used if the field only has positive numbers, otherwise use sint64.
func (b *base) Int64() (int64, error) {
	var v uint64
	var err error
	b.Index, v, err = varint64(b.Data, b.Index)

	return int64(v), err
}

// Uint32 reads a variable-length encoding of up to 4 bytes.
func (b *base) Uint32() (uint32, error) {
	var v uint32
	var err error

	b.Index, v, err = varint32(b.Data, b.Index)
	return v, err
}

// Uint64 reads a variable-length encoding of up to 8 bytes.
func (b *base) Uint64() (uint64, error) {
	var v uint64
	var err error

	b.Index, v, err = varint64(b.Data, b.Index)
	return v, err
}

// Sint32 uses variable-length encoding with zig-zag encoding for signed values.
// This field type more efficiently encodes negative numbers than regular int32s.
func (b *base) Sint32() (int32, error) {
	var v uint64
	var err error

	b.Index, v, err = varint64(b.Data, b.Index)
	return int32(unZig64(v)), err
}

// Sint64 uses variable-length encoding with zig-zag encoding for signed values.
// This field type more efficiently encodes negative numbers than regular int64s.
func (b *base) Sint64() (int64, error) {
	var v uint64
	var err error

	b.Index, v, err = varint64(b.Data, b.Index)
	return unZig64(v), err
}

// Bool is encoded as 0x01 or 0x00 plus the field+type prefix byte. 2 bytes total.
func (b *base) Bool() (bool, error) {
	if len(b.Data) <= b.Index {
		return false, io.ErrUnexpectedEOF
	}
	if d := b.Data[b.Index]; d&0x80 == 0 {
		b.Index++
		return d == 1, nil
	}
	var v uint64
	var err error
	b.Index, v, err = varint64(b.Data, b.Index)
	return v == 1, err
}

// String reads a string type. This data will always contain UTF-8 encoded or
// 7-bit ASCII text.
func (m *Message) String() (string, error) {
	b, err := m.Bytes()
	return string(b), err
}

// Bytes returns the encode sequence of bytes.
// NOTE: this value is NOT copied.
func (m *Message) Bytes() ([]byte, error) {
	l, err := m.packedLength()
	if err != nil {
		return nil, err
	}

	b := m.Data[m.Index : m.Index+l]
	m.Index += l
	return b, nil
}

func unZig64(v uint64) int64 {
	return int64((v >> 1) ^ uint64((int64(v&1)<<63)>>63))
}
