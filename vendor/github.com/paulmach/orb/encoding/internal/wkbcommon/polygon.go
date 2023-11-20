package wkbcommon

import (
	"errors"
	"io"
	"math"

	"github.com/paulmach/orb"
)

func unmarshalPolygon(order byteOrder, data []byte) (orb.Polygon, error) {
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
	result := make(orb.Polygon, 0, alloc)

	for i := 0; i < int(num); i++ {
		ps, err := unmarshalPoints(order, data)
		if err != nil {
			return nil, err
		}

		data = data[16*len(ps)+4:]
		result = append(result, orb.Ring(ps))
	}

	return result, nil
}

func readPolygon(r io.Reader, order byteOrder, buf []byte) (orb.Polygon, error) {
	num, err := readUint32(r, order, buf[:4])
	if err != nil {
		return nil, err
	}

	alloc := num
	if alloc > MaxMultiAlloc {
		// invalid data can come in here and allocate tons of memory.
		alloc = MaxMultiAlloc
	}
	result := make(orb.Polygon, 0, alloc)

	for i := 0; i < int(num); i++ {
		ls, err := readLineString(r, order, buf)
		if err != nil {
			return nil, err
		}

		result = append(result, orb.Ring(ls))
	}

	return result, nil
}

func (e *Encoder) writePolygon(p orb.Polygon, srid int) error {
	err := e.writeTypePrefix(polygonType, len(p), srid)
	if err != nil {
		return err
	}

	for _, r := range p {
		e.order.PutUint32(e.buf, uint32(len(r)))
		_, err := e.w.Write(e.buf[:4])
		if err != nil {
			return err
		}
		for _, p := range r {
			e.order.PutUint64(e.buf, math.Float64bits(p[0]))
			e.order.PutUint64(e.buf[8:], math.Float64bits(p[1]))
			_, err = e.w.Write(e.buf)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func unmarshalMultiPolygon(order byteOrder, data []byte) (orb.MultiPolygon, error) {
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
	result := make(orb.MultiPolygon, 0, alloc)

	for i := 0; i < int(num); i++ {
		p, _, err := ScanPolygon(data)
		if err != nil {
			return nil, err
		}

		l := 9
		for _, r := range p {
			l += 4 + 16*len(r)
		}
		data = data[l:]

		result = append(result, p)
	}

	return result, nil
}

func readMultiPolygon(r io.Reader, order byteOrder, buf []byte) (orb.MultiPolygon, error) {
	num, err := readUint32(r, order, buf[:4])
	if err != nil {
		return nil, err
	}

	alloc := num
	if alloc > MaxMultiAlloc {
		// invalid data can come in here and allocate tons of memory.
		alloc = MaxMultiAlloc
	}
	result := make(orb.MultiPolygon, 0, alloc)

	for i := 0; i < int(num); i++ {
		pOrder, typ, _, err := readByteOrderType(r, buf)
		if err != nil {
			return nil, err
		}

		if typ != polygonType {
			return nil, errors.New("expect multipolygons to contains polygons, did not find a polygon")
		}

		p, err := readPolygon(r, pOrder, buf)
		if err != nil {
			return nil, err
		}

		result = append(result, p)
	}

	return result, nil
}

func (e *Encoder) writeMultiPolygon(mp orb.MultiPolygon, srid int) error {
	err := e.writeTypePrefix(multiPolygonType, len(mp), srid)
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
