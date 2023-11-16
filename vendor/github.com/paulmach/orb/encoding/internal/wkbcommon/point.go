package wkbcommon

import (
	"encoding/binary"
	"errors"
	"io"
	"math"

	"github.com/paulmach/orb"
)

func unmarshalPoints(order byteOrder, data []byte) ([]orb.Point, error) {
	if len(data) < 4 {
		return nil, ErrNotWKB
	}
	num := unmarshalUint32(order, data)
	data = data[4:]

	if len(data) < int(num*16) {
		return nil, ErrNotWKB
	}

	alloc := num
	if alloc > MaxPointsAlloc {
		// invalid data can come in here and allocate tons of memory.
		alloc = MaxPointsAlloc
	}
	result := make([]orb.Point, 0, alloc)

	if order == littleEndian {
		for i := 0; i < int(num); i++ {
			result = append(result, orb.Point{})
			result[i][0] = math.Float64frombits(binary.LittleEndian.Uint64(data[16*i:]))
			result[i][1] = math.Float64frombits(binary.LittleEndian.Uint64(data[16*i+8:]))
		}
	} else {
		for i := 0; i < int(num); i++ {
			result = append(result, orb.Point{})
			result[i][0] = math.Float64frombits(binary.BigEndian.Uint64(data[16*i:]))
			result[i][1] = math.Float64frombits(binary.BigEndian.Uint64(data[16*i+8:]))
		}
	}

	return result, nil
}

func unmarshalPoint(order byteOrder, buf []byte) (orb.Point, error) {
	if len(buf) < 16 {
		return orb.Point{}, ErrNotWKB
	}

	var p orb.Point
	if order == littleEndian {
		p[0] = math.Float64frombits(binary.LittleEndian.Uint64(buf))
		p[1] = math.Float64frombits(binary.LittleEndian.Uint64(buf[8:]))
	} else {
		p[0] = math.Float64frombits(binary.BigEndian.Uint64(buf))
		p[1] = math.Float64frombits(binary.BigEndian.Uint64(buf[8:]))
	}

	return p, nil
}

func readPoint(r io.Reader, order byteOrder, buf []byte) (orb.Point, error) {
	var p orb.Point

	for i := 0; i < 2; i++ {
		if _, err := io.ReadFull(r, buf); err != nil {
			return orb.Point{}, err
		}
		if order == littleEndian {
			p[i] = math.Float64frombits(binary.LittleEndian.Uint64(buf))
		} else {
			p[i] = math.Float64frombits(binary.BigEndian.Uint64(buf))
		}
	}

	return p, nil
}

func (e *Encoder) writePoint(p orb.Point, srid int) error {
	var err error
	if srid != 0 {
		e.order.PutUint32(e.buf, pointType|ewkbType)
		e.order.PutUint32(e.buf[4:], uint32(srid))
		_, err = e.w.Write(e.buf[:8])
	} else {
		e.order.PutUint32(e.buf, pointType)
		_, err = e.w.Write(e.buf[:4])
	}
	if err != nil {
		return err
	}

	e.order.PutUint64(e.buf, math.Float64bits(p[0]))
	e.order.PutUint64(e.buf[8:], math.Float64bits(p[1]))
	_, err = e.w.Write(e.buf)
	return err
}

func unmarshalMultiPoint(order byteOrder, data []byte) (orb.MultiPoint, error) {
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
	result := make(orb.MultiPoint, 0, alloc)

	for i := 0; i < int(num); i++ {
		p, _, err := ScanPoint(data)
		if err != nil {
			return nil, err
		}

		data = data[21:]
		result = append(result, p)
	}

	return result, nil
}

func readMultiPoint(r io.Reader, order byteOrder, buf []byte) (orb.MultiPoint, error) {
	num, err := readUint32(r, order, buf[:4])
	if err != nil {
		return nil, err
	}

	alloc := num
	if alloc > MaxPointsAlloc {
		// invalid data can come in here and allocate tons of memory.
		alloc = MaxPointsAlloc
	}
	result := make(orb.MultiPoint, 0, alloc)

	for i := 0; i < int(num); i++ {
		pOrder, typ, _, err := readByteOrderType(r, buf)
		if err != nil {
			return nil, err
		}

		if typ != pointType {
			return nil, errors.New("expect multipoint to contains points, did not find a point")
		}

		p, err := readPoint(r, pOrder, buf)
		if err != nil {
			return nil, err
		}

		result = append(result, p)
	}

	return result, nil
}

func (e *Encoder) writeMultiPoint(mp orb.MultiPoint, srid int) error {
	err := e.writeTypePrefix(multiPointType, len(mp), srid)
	if err != nil {
		return err
	}

	for _, p := range mp {
		err := e.Encode(p, 0)
		if err != nil {
			return err
		}
	}

	return nil
}
