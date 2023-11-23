package wkbcommon

import (
	"io"

	"github.com/paulmach/orb"
)

func readCollection(r io.Reader, order byteOrder, buf []byte) (orb.Collection, error) {
	num, err := readUint32(r, order, buf[:4])
	if err != nil {
		return nil, err
	}

	alloc := num
	if alloc > MaxMultiAlloc {
		// invalid data can come in here and allocate tons of memory.
		alloc = MaxMultiAlloc
	}
	result := make(orb.Collection, 0, alloc)

	d := NewDecoder(r)
	for i := 0; i < int(num); i++ {
		geom, _, err := d.Decode()
		if err != nil {
			return nil, err
		}

		result = append(result, geom)
	}

	return result, nil
}

func (e *Encoder) writeCollection(c orb.Collection, srid int) error {
	err := e.writeTypePrefix(geometryCollectionType, len(c), srid)
	if err != nil {
		return err
	}

	for _, geom := range c {
		err := e.Encode(geom, 0)
		if err != nil {
			return err
		}
	}

	return nil
}
