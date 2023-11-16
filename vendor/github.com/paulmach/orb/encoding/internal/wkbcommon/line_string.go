package wkbcommon

import (
	"errors"
	"io"
	"math"

	"github.com/paulmach/orb"
)

func unmarshalLineString(order byteOrder, data []byte) (orb.LineString, error) {
	ps, err := unmarshalPoints(order, data)
	if err != nil {
		return nil, err
	}

	return orb.LineString(ps), nil
}

func readLineString(r io.Reader, order byteOrder, buf []byte) (orb.LineString, error) {
	num, err := readUint32(r, order, buf[:4])
	if err != nil {
		return nil, err
	}

	alloc := num
	if alloc > MaxPointsAlloc {
		// invalid data can come in here and allocate tons of memory.
		alloc = MaxPointsAlloc
	}
	result := make(orb.LineString, 0, alloc)

	for i := 0; i < int(num); i++ {
		p, err := readPoint(r, order, buf)
		if err != nil {
			return nil, err
		}

		result = append(result, p)
	}

	return result, nil
}

func (e *Encoder) writeLineString(ls orb.LineString, srid int) error {
	err := e.writeTypePrefix(lineStringType, len(ls), srid)
	if err != nil {
		return err
	}

	for _, p := range ls {
		e.order.PutUint64(e.buf, math.Float64bits(p[0]))
		e.order.PutUint64(e.buf[8:], math.Float64bits(p[1]))
		_, err = e.w.Write(e.buf)
		if err != nil {
			return err
		}
	}

	return nil
}

func unmarshalMultiLineString(order byteOrder, data []byte) (orb.MultiLineString, error) {
	if len(data) < 4 {
		return nil, ErrNotWKB
	}
	num := unmarshalUint32(order, data)
	data = data[4:]

	alloc := num
	if alloc > MaxMultiAlloc {
		// invalid data can come in here and allocate tons of memory.
		alloc = MaxMultiAlloc
	}
	result := make(orb.MultiLineString, 0, alloc)

	for i := 0; i < int(num); i++ {
		ls, _, err := ScanLineString(data)
		if err != nil {
			return nil, err
		}

		data = data[16*len(ls)+9:]
		result = append(result, ls)
	}

	return result, nil
}

func readMultiLineString(r io.Reader, order byteOrder, buf []byte) (orb.MultiLineString, error) {
	num, err := readUint32(r, order, buf[:4])
	if err != nil {
		return nil, err
	}

	alloc := num
	if alloc > MaxMultiAlloc {
		// invalid data can come in here and allocate tons of memory.
		alloc = MaxMultiAlloc
	}
	result := make(orb.MultiLineString, 0, alloc)

	for i := 0; i < int(num); i++ {
		lOrder, typ, _, err := readByteOrderType(r, buf)
		if err != nil {
			return nil, err
		}

		if typ != lineStringType {
			return nil, errors.New("expect multilines to contains lines, did not find a line")
		}

		ls, err := readLineString(r, lOrder, buf)
		if err != nil {
			return nil, err
		}

		result = append(result, ls)
	}

	return result, nil
}

func (e *Encoder) writeMultiLineString(mls orb.MultiLineString, srid int) error {
	err := e.writeTypePrefix(multiLineStringType, len(mls), srid)
	if err != nil {
		return err
	}

	for _, ls := range mls {
		err := e.Encode(ls, 0)
		if err != nil {
			return err
		}
	}

	return nil
}
