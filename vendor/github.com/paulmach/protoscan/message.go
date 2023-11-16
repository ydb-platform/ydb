package protoscan

import (
	"errors"
	"io"
)

//go:generate protoc --go_out=internal/testmsg internal/testmsg/types.proto
//go:generate go run internal/gen_repeated.go

// ErrIntOverflow is returned when scanning an integer with varint encoding and the
// value is too long for the integer type.
var ErrIntOverflow = errors.New("protoscan: integer overflow")

// ErrInvalidLength is returned when a length is not valid, usually resulting
// from scanning the incorrect type.
var ErrInvalidLength = errors.New("protoscan: invalid length")

// The WireType describes the encoding method for the next value in the stream.
const (
	WireTypeVarint          = 0
	WireType64bit           = 1
	WireTypeLengthDelimited = 2
	WireTypeStartGroup      = 3 // deprecated by protobuf, not supported
	WireTypeEndGroup        = 4 // deprecated by protobuf, not supported
	WireType32bit           = 5
)

// base has all the methods for reading packable fields (the numbers) so they
// can be shared between message and iterator.
type base struct {
	Data  []byte
	Index int
}

// Message is a container for a protobuf message type that is ready for scanning.
type Message struct {
	base
	err error

	fieldNumber int
	wireType    int
}

// New creates a new Message scanner for the given encoded protobuf data.
func New(data []byte) *Message {
	return &Message{
		base: base{
			Data:  data,
			Index: 0,
		},
	}
}

// Next will move the scanner to the next value. This function should be used in a for loop.
//
//  for msg.Next() {
//    switch msg.FieldNumber() {
//    case 1:
//      v, err := msg.Float()
//    default:
//      msg.Skip()
//    }
//  }
func (m *Message) Next() bool {
	if m.err != nil {
		return false
	}
	if m.Index < len(m.Data) {
		val, err := m.Varint64()
		if err != nil {
			m.err = err
			return false
		}
		m.fieldNumber = int(val >> 3)
		m.wireType = int(val & 0x7)
		return true
	}

	return false
}

// Err will return any errors that were encountered during scanning.
// Errors could be due to reading the incorrect types or forgetting to skip and unused value.
func (m *Message) Err() error {
	return m.err
}

// FieldNumber returns the number for the current value being scanned.
// These numbers are defined in the protobuf definition file used to encode the message.
func (m *Message) FieldNumber() int {
	return m.fieldNumber
}

// WireType returns the 'type' of the data at the current location.
func (m *Message) WireType() int {
	return m.wireType
}

// Skip will move the scanner past the current value if it is not needed.
// If a value is not parsed this method must be called to move the decoder past the value.
func (m *Message) Skip() {
	switch m.wireType {
	case WireTypeVarint:
		_, m.err = m.Varint64()
	case WireType64bit:
		if len(m.Data) <= m.Index+8 {
			m.err = io.ErrUnexpectedEOF
			return
		}
		m.Index += 8
	case WireTypeLengthDelimited:
		l, err := m.packedLength()
		if err != nil {
			m.err = err
			return
		}
		m.Index += l
	case WireType32bit:
		if len(m.Data) <= m.Index+4 {
			m.err = io.ErrUnexpectedEOF
			return
		}
		m.Index += 4
	}
}

// Message will return a pointer to an embedded message that can then
// be scanned in kind of a recursive fashion. Will reuse the provided
// Message object if provided.
func (m *Message) Message(msg *Message) (*Message, error) {
	l, err := m.packedLength()
	if err != nil {
		return nil, err
	}

	if msg == nil {
		msg = New(m.Data[m.Index : m.Index+l])
	} else {
		msg.Reset(m.Data[m.Index : m.Index+l])
	}

	m.Index += l
	return msg, nil
}

// MessageData returns the encoded data a message. This data can
// then be decoded using conventional tools.
func (m *Message) MessageData() ([]byte, error) {
	l, err := m.packedLength()
	if err != nil {
		return nil, err
	}

	postIndex := m.Index + l
	if len(m.Data) < postIndex {
		return nil, io.ErrUnexpectedEOF
	}

	d := m.Data[m.Index:postIndex]
	m.Index = postIndex
	return d, nil
}

// Reset will set the index to 0 so the message can be read again.
// Optionally pass in new data to reuse the Message object.
func (m *Message) Reset(newData []byte) {
	if newData != nil {
		m.Data = newData
	}
	m.err = nil
	m.Index = 0
	m.fieldNumber = 0
	m.wireType = 0
}

func (m *Message) packedLength() (int, error) {
	var err error
	var l64 uint64
	m.Index, l64, err = varint64(m.Data, m.Index)
	if err != nil {
		return 0, err
	}

	l := int(l64)
	if l < 0 {
		return 0, ErrInvalidLength
	}

	postIndex := m.Index + l
	if postIndex < 0 {
		// because there could be overflow...
		return 0, ErrInvalidLength
	}

	if len(m.Data) < postIndex {
		return 0, io.ErrUnexpectedEOF
	}

	return l, nil
}

func (m *Message) count(l int) int {
	var count int
	for _, b := range m.Data[m.Index : m.Index+l] {
		if b < 128 {
			count++
		}
	}

	return count
}
