package wkb

import (
	"database/sql"
	"database/sql/driver"

	"github.com/paulmach/orb"
	"github.com/paulmach/orb/encoding/internal/wkbcommon"
)

var (
	_ sql.Scanner  = &GeometryScanner{}
	_ driver.Value = value{}
)

// GeometryScanner is a thing that can scan in sql query results.
// It can be used as a scan destination:
//
//	s := &wkb.GeometryScanner{}
//	err := db.QueryRow("SELECT latlon FROM foo WHERE id=?", id).Scan(s)
//	...
//	if s.Valid {
//	  // use s.Geometry
//	} else {
//	  // NULL value
//	}
type GeometryScanner struct {
	g        interface{}
	Geometry orb.Geometry
	Valid    bool // Valid is true if the geometry is not NULL
}

// Scanner will return a GeometryScanner that can scan sql query results.
// The geometryScanner.Geometry attribute will be set to the value.
// If g is non-nil, it MUST be a pointer to an orb.Geometry
// type like a Point or LineString. In that case the value will be written to
// g and the Geometry attribute.
//
//	var p orb.Point
//	err := db.QueryRow("SELECT latlon FROM foo WHERE id=?", id).Scan(wkb.Scanner(&p))
//	...
//	// use p
//
// If the value may be null check Valid first:
//
//	var point orb.Point
//	s := wkb.Scanner(&point)
//	err := db.QueryRow("SELECT latlon FROM foo WHERE id=?", id).Scan(&s)
//	...
//	if s.Valid {
//	  // use p
//	} else {
//	  // NULL value
//	}
//
// Deprecated behavior: Scanning directly from MySQL columns is supported.
// By default MySQL returns geometry data as WKB but prefixed with a 4 byte SRID.
// To support this, if the data is not valid WKB, the code will strip the
// first 4 bytes and try again. This works for most use cases.
//
// For supported behavior see `ewkb.ScannerPrefixSRID`
func Scanner(g interface{}) *GeometryScanner {
	return &GeometryScanner{g: g}
}

// Scan will scan the input []byte data into a geometry.
// This could be into the orb geometry type pointer or, if nil,
// the scanner.Geometry attribute.
func (s *GeometryScanner) Scan(d interface{}) error {
	if d == nil {
		return nil
	}

	data, ok := d.([]byte)
	if !ok {
		return ErrUnsupportedDataType
	}

	s.Geometry = nil
	s.Valid = false

	g, _, valid, err := wkbcommon.Scan(s.g, d)
	if err == wkbcommon.ErrNotWKBHeader {
		var e error
		g, _, valid, e = wkbcommon.Scan(s.g, data[4:])
		if e != wkbcommon.ErrNotWKBHeader {
			err = e // nil or incorrect type, e.g. decoding line string
		}
	}

	if err != nil {
		return mapCommonError(err)
	}

	s.Geometry = g
	s.Valid = valid

	return nil
}

type value struct {
	v orb.Geometry
}

// Value will create a driver.Valuer that will WKB the geometry
// into the database query.
func Value(g orb.Geometry) driver.Valuer {
	return value{v: g}

}

func (v value) Value() (driver.Value, error) {
	val, err := Marshal(v.v)
	if val == nil {
		return nil, err
	}
	return val, err
}
