package protoscan

// An Iterator allows for moving across a packed repeated field
// in a 'controlled' fashion.
type Iterator struct {
	base

	fieldNumber int
}

// Iterator will use the current field. The field must be a packed
// repeated field.
func (m *Message) Iterator(iter *Iterator) (*Iterator, error) {
	// TODO: validate wiretype makes sense

	l, err := m.packedLength()
	if err != nil {
		return nil, err
	}

	if iter == nil {
		iter = &Iterator{}
	}
	iter.base = base{
		Data:  m.Data[m.Index : m.Index+l],
		Index: 0,
	}
	iter.fieldNumber = m.fieldNumber
	m.Index += l

	return iter, nil
}

// HasNext is used in a 'for' loop to read through all the elements.
// Returns false when all the items have been read.
// This method does NOT need to be called, reading a value automatically
// moves in the index forward. This behavior is different than Message.Next().
func (i *Iterator) HasNext() bool {
	return i.base.Index < len(i.base.Data)
}

// Skip will move the interator forward 'count' value(s) without actually reading it.
// Must provide the correct wireType. For a new iterator 'count' will move the
// pointer so the next value call with be the 'counth' value.
// double, float, fixed, sfixed are WireType32bit or WireType64bit,
// all others int, uint, sint types are WireTypeVarint.
// The function will panic for any other value.
func (i *Iterator) Skip(wireType int, count int) {
	if wireType == WireTypeVarint {
		for j := 0; j < count; j++ {
			for i.Data[i.Index] >= 128 {
				i.Index++
			}
			i.Index++
		}
		return
	} else if wireType == WireType32bit {
		i.Index += 4 * count
		return
	} else if wireType == WireType64bit {
		i.Index += 8 * count
		return
	}

	panic("invalid wire type for a packed repeated field")
}

// Count returns the total number of values in this repeated field.
// The answer depends on the type/encoding or the field:
// double, float, fixed, sfixed are WireType32bit or WireType64bit,
// all others int, uint, sint types are WireTypeVarint.
// The function will panic for any other value.
func (i *Iterator) Count(wireType int) int {
	if wireType == WireTypeVarint {
		var count int
		for _, b := range i.Data {
			if b < 128 {
				count++
			}
		}

		return count
	}
	if wireType == WireType32bit {
		return len(i.base.Data) / 4
	}
	if wireType == WireType64bit {
		return len(i.base.Data) / 8
	}

	panic("invalid wire type for a packed repeated field")
}

// FieldNumber returns the number for the current repeated field.
// These numbers are defined in the protobuf definition file used to encode the message.
func (i *Iterator) FieldNumber() int {
	return i.fieldNumber
}
