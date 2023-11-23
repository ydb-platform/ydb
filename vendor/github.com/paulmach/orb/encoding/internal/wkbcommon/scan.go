package wkbcommon

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"io"

	"github.com/paulmach/orb"
)

var (
	// ErrUnsupportedDataType is returned by Scan methods when asked to scan
	// non []byte data from the database. This should never happen
	// if the driver is acting appropriately.
	ErrUnsupportedDataType = errors.New("wkbcommon: scan value must be []byte")

	// ErrNotWKB is returned when unmarshalling WKB and the data is not valid.
	ErrNotWKB = errors.New("wkbcommon: invalid data")

	// ErrNotWKBHeader is returned when unmarshalling first few bytes and there
	// is an issue.
	ErrNotWKBHeader = errors.New("wkbcommon: invalid header data")

	// ErrIncorrectGeometry is returned when unmarshalling WKB data into the wrong type.
	// For example, unmarshaling linestring data into a point.
	ErrIncorrectGeometry = errors.New("wkbcommon: incorrect geometry")

	// ErrUnsupportedGeometry is returned when geometry type is not supported by this lib.
	ErrUnsupportedGeometry = errors.New("wkbcommon: unsupported geometry")
)

// Scan will scan the input []byte data into a geometry.
// This could be into the orb geometry type pointer or, if nil,
// the scanner.Geometry attribute.
func Scan(g, d interface{}) (orb.Geometry, int, bool, error) {
	if d == nil {
		return nil, 0, false, nil
	}

	data, ok := d.([]byte)
	if !ok {
		return nil, 0, false, ErrUnsupportedDataType
	}

	if data == nil {
		return nil, 0, false, nil
	}

	if len(data) < 5 {
		return nil, 0, false, ErrNotWKB
	}

	// go-pg will return ST_AsBinary(*) data as `\xhexencoded` which
	// needs to be converted to true binary for further decoding.
	// Code detects the \x prefix and then converts the rest from Hex to binary.
	if data[0] == byte('\\') && data[1] == byte('x') {
		n, err := hex.Decode(data, data[2:])
		if err != nil {
			return nil, 0, false, fmt.Errorf("thought the data was hex with prefix, but it is not: %v", err)
		}
		data = data[:n]
	}

	// also possible is just straight hex encoded.
	// In this case the bo bit can be '0x00' or '0x01'
	if data[0] == '0' && (data[1] == '0' || data[1] == '1') {
		n, err := hex.Decode(data, data)
		if err != nil {
			return nil, 0, false, fmt.Errorf("thought the data was hex, but it is not: %v", err)
		}
		data = data[:n]
	}

	switch g := g.(type) {
	case nil:
		m, srid, err := Unmarshal(data)
		if err != nil {
			return nil, 0, false, err
		}

		return m, srid, true, nil
	case *orb.Point:
		p, srid, err := ScanPoint(data)
		if err != nil {
			return nil, 0, false, err
		}

		*g = p
		return p, srid, true, nil
	case *orb.MultiPoint:
		m, srid, err := ScanMultiPoint(data)
		if err != nil {
			return nil, 0, false, err
		}

		*g = m
		return m, srid, true, nil
	case *orb.LineString:
		l, srid, err := ScanLineString(data)
		if err != nil {
			return nil, 0, false, err
		}

		*g = l
		return l, srid, true, nil
	case *orb.MultiLineString:
		m, srid, err := ScanMultiLineString(data)
		if err != nil {
			return nil, 0, false, err
		}

		*g = m
		return m, srid, true, nil
	case *orb.Ring:
		m, srid, err := Unmarshal(data)
		if err != nil {
			return nil, 0, false, err
		}

		if p, ok := m.(orb.Polygon); ok && len(p) == 1 {
			*g = p[0]
			return p[0], srid, true, nil
		}

		return nil, 0, false, ErrIncorrectGeometry
	case *orb.Polygon:
		p, srid, err := ScanPolygon(data)
		if err != nil {
			return nil, 0, false, err
		}

		*g = p
		return p, srid, true, nil
	case *orb.MultiPolygon:
		m, srid, err := ScanMultiPolygon(data)
		if err != nil {
			return nil, 0, false, err
		}

		*g = m
		return m, srid, true, nil
	case *orb.Collection:
		c, srid, err := ScanCollection(data)
		if err != nil {
			return nil, 0, false, err
		}

		*g = c
		return c, srid, true, nil
	case *orb.Bound:
		m, srid, err := Unmarshal(data)
		if err != nil {
			return nil, 0, false, err
		}

		*g = m.Bound()
		return *g, srid, true, nil
	}

	return nil, 0, false, ErrIncorrectGeometry
}

// ScanPoint takes binary wkb and decodes it into a point.
func ScanPoint(data []byte) (orb.Point, int, error) {
	order, typ, srid, geomData, err := unmarshalByteOrderType(data)
	if err != nil {
		return orb.Point{}, 0, err
	}

	switch typ {
	case pointType:
		p, err := unmarshalPoint(order, geomData)
		if err != nil {
			return orb.Point{}, 0, err
		}

		return p, srid, nil
	case multiPointType:
		mp, err := unmarshalMultiPoint(order, geomData)
		if err != nil {
			return orb.Point{}, 0, err
		}
		if len(mp) == 1 {
			return mp[0], srid, nil
		}
	}

	return orb.Point{}, 0, ErrIncorrectGeometry
}

// ScanMultiPoint takes binary wkb and decodes it into a multi-point.
func ScanMultiPoint(data []byte) (orb.MultiPoint, int, error) {
	m, srid, err := Unmarshal(data)
	if err != nil {
		return nil, 0, err
	}

	switch p := m.(type) {
	case orb.Point:
		return orb.MultiPoint{p}, srid, nil
	case orb.MultiPoint:
		return p, srid, nil
	}

	return nil, 0, ErrIncorrectGeometry
}

// ScanLineString takes binary wkb and decodes it into a line string.
func ScanLineString(data []byte) (orb.LineString, int, error) {
	order, typ, srid, data, err := unmarshalByteOrderType(data)
	if err != nil {
		return nil, 0, err
	}

	switch typ {
	case lineStringType:
		ls, err := unmarshalLineString(order, data)
		if err != nil {
			return nil, 0, err
		}

		return ls, srid, nil
	case multiLineStringType:
		mls, err := unmarshalMultiLineString(order, data)
		if err != nil {
			return nil, 0, err
		}
		if len(mls) == 1 {
			return mls[0], srid, nil
		}
	}

	return nil, 0, ErrIncorrectGeometry
}

// ScanMultiLineString takes binary wkb and decodes it into a multi-line string.
func ScanMultiLineString(data []byte) (orb.MultiLineString, int, error) {
	order, typ, srid, data, err := unmarshalByteOrderType(data)
	if err != nil {
		return nil, 0, err
	}

	switch typ {
	case lineStringType:
		ls, err := unmarshalLineString(order, data)
		if err != nil {
			return nil, 0, err
		}

		return orb.MultiLineString{ls}, srid, nil
	case multiLineStringType:
		ls, err := unmarshalMultiLineString(order, data)
		if err != nil {
			return nil, 0, err
		}

		return ls, srid, nil
	}

	return nil, 0, ErrIncorrectGeometry
}

// ScanPolygon takes binary wkb and decodes it into a polygon.
func ScanPolygon(data []byte) (orb.Polygon, int, error) {
	order, typ, srid, data, err := unmarshalByteOrderType(data)
	if err != nil {
		return nil, 0, err
	}

	switch typ {
	case polygonType:
		p, err := unmarshalPolygon(order, data)
		if err != nil {
			return nil, 0, err
		}

		return p, srid, nil
	case multiPolygonType:
		mp, err := unmarshalMultiPolygon(order, data)
		if err != nil {
			return nil, 0, err
		}
		if len(mp) == 1 {
			return mp[0], srid, nil
		}
	}

	return nil, 0, ErrIncorrectGeometry
}

// ScanMultiPolygon takes binary wkb and decodes it into a multi-polygon.
func ScanMultiPolygon(data []byte) (orb.MultiPolygon, int, error) {
	order, typ, srid, data, err := unmarshalByteOrderType(data)
	if err != nil {
		return nil, 0, err
	}

	switch typ {
	case polygonType:
		p, err := unmarshalPolygon(order, data)
		if err != nil {
			return nil, 0, err
		}
		return orb.MultiPolygon{p}, srid, nil
	case multiPolygonType:
		mp, err := unmarshalMultiPolygon(order, data)
		if err != nil {
			return nil, 0, err
		}

		return mp, srid, nil
	}

	return nil, 0, ErrIncorrectGeometry
}

// ScanCollection takes binary wkb and decodes it into a collection.
func ScanCollection(data []byte) (orb.Collection, int, error) {
	m, srid, err := NewDecoder(bytes.NewReader(data)).Decode()
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return nil, 0, ErrNotWKB
	}

	if err != nil {
		return nil, 0, err
	}

	switch p := m.(type) {
	case orb.Collection:
		return p, srid, nil
	}

	return nil, 0, ErrIncorrectGeometry
}
