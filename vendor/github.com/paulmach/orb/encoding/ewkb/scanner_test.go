package ewkb

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/paulmach/orb"
)

var (
	testPolygon = orb.Polygon{{
		{30, 10}, {40, 40}, {20, 40}, {10, 20}, {30, 10},
	}}
	testPolygonData = []byte{
		//01    02    03    04    05    06    07    08
		0x01, 0x03, 0x00, 0x00, 0x20,
		0xE6, 0x10, 0x00, 0x00,
		0x01, 0x00, 0x00, 0x00, // Number of Rings 1
		0x05, 0x00, 0x00, 0x00, // Number of Points 5
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x3e, 0x40, // X1 30
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x24, 0x40, // Y1 10
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x44, 0x40, // X2 40
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x44, 0x40, // Y2 40
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x34, 0x40, // X3 20
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x44, 0x40, // Y3 40
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x24, 0x40, // X4 10
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x34, 0x40, // Y4 20
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x3e, 0x40, // X5 30
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x24, 0x40, // Y5 10
	}
)

var (
	testMultiPolygon = orb.MultiPolygon{
		{{{30, 20}, {45, 40}, {10, 40}, {30, 20}}},
		{{{15, 5}, {40, 10}, {10, 20}, {5, 10}, {15, 5}}},
	}
	testMultiPolygonData = []byte{
		//01    02    03    04    05    06    07    08
		0x01, 0x06, 0x00, 0x00, 0x20,
		0xE6, 0x10, 0x00, 0x00,
		0x02, 0x00, 0x00, 0x00, // Number of Polygons (2)
		0x01,                   // Byte Encoding Little
		0x03, 0x00, 0x00, 0x00, // Type Polygon1 (3)
		0x01, 0x00, 0x00, 0x00, // Number of Lines (1)
		0x04, 0x00, 0x00, 0x00, // Number of Points (4)
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x3e, 0x40, // X1 30
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x34, 0x40, // Y1 20
		0x00, 0x00, 0x00, 0x00, 0x00, 0x80, 0x46, 0x40, // X2 45
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x44, 0x40, // Y2 40
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x24, 0x40, // X3 10
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x44, 0x40, // Y3 40
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x3e, 0x40, // X4 30
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x34, 0x40, // X4 20
		0x01,                   // Byte Encoding Little
		0x03, 0x00, 0x00, 0x00, // Type Polygon2 (3)
		0x01, 0x00, 0x00, 0x00, // Number of Lines (1)
		0x05, 0x00, 0x00, 0x00, // Number of Points (5)
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2e, 0x40, // X1 15
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x14, 0x40, // Y1  5
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x44, 0x40, // X2 40
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x24, 0x40, // Y2 10
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x24, 0x40, // X3 10
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x34, 0x40, // Y3 20
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x14, 0x40, // X4  5
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x24, 0x40, // Y4 10
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2e, 0x40, // X5 15
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x14, 0x40, // Y5  5
	}

	testMultiPolygonSingle = orb.MultiPolygon{
		{
			{{20, 35}, {10, 30}, {10, 10}, {30, 5}, {45, 20}, {20, 35}},
			{{30, 20}, {20, 15}, {20, 25}, {30, 20}}},
	}
	testMultiPolygonSingleData = []byte{
		//01    02    03    04    05    06    07    08
		0x01, 0x06, 0x00, 0x00, 0x20,
		0xE6, 0x10, 0x00, 0x00,
		0x01, 0x00, 0x00, 0x00, // Number of Polygons (1)
		0x01,                   // Byte order marker little
		0x03, 0x00, 0x00, 0x00, // Type Polygon(3)
		0x02, 0x00, 0x00, 0x00, // Number of Lines(2)
		0x06, 0x00, 0x00, 0x00, // Number of Points(6)
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x34, 0x40, // X1 20
		0x00, 0x00, 0x00, 0x00, 0x00, 0x80, 0x41, 0x40, // Y1 35
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x24, 0x40, // X2 10
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x3e, 0x40, // Y2 30
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x24, 0x40, // X3 10
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x24, 0x40, // Y3 10
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x3e, 0x40, // X4 30
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x14, 0x40, // Y4 5
		0x00, 0x00, 0x00, 0x00, 0x00, 0x80, 0x46, 0x40, // X5 45
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x34, 0x40, // Y5 20
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x34, 0x40, // X6 20
		0x00, 0x00, 0x00, 0x00, 0x00, 0x80, 0x41, 0x40, // Y6 35
		0x04, 0x00, 0x00, 0x00, // Number of Points(4)
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x3e, 0x40, // X1 30
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x34, 0x40, // Y1 20
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x34, 0x40, // X2 20
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2e, 0x40, // Y2 15
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x34, 0x40, // X3 20
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x39, 0x40, // Y3 25
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x3e, 0x40, // X4 30
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x34, 0x40, // Y4 20
	}
)

var (
	testCollection = orb.Collection{
		orb.Point{4, 6},
		orb.LineString{{4, 6}, {7, 10}},
	}
	testCollectionData = []byte{
		//01    02    03    04    05    06    07    08
		0x01, 0x07, 0x00, 0x00, 0x20,
		0xE6, 0x10, 0x00, 0x00,
		0x02, 0x00, 0x00, 0x00, // Number of Geometries in Collection
		0x01,                   // Byte order marker little
		0x01, 0x00, 0x00, 0x00, // Type (1) Point
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x40, // X1 4
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x18, 0x40, // Y1 6
		0x01,                   // Byte order marker little
		0x02, 0x00, 0x00, 0x00, // Type (2) Line
		0x02, 0x00, 0x00, 0x00, // Number of Points (2)
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x40, // X1 4
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x18, 0x40, // Y1 6
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x1c, 0x40, // X2 7
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x24, 0x40, // Y2 10
	}
)

func TestScanNil(t *testing.T) {
	testPoint := orb.Point{1, 2}

	s := Scanner(nil)
	err := s.Scan(MustMarshal(testPoint, 4326))
	if err != nil {
		t.Fatalf("scan error: %v", err)
	}

	if !orb.Equal(s.Geometry, testPoint) {
		t.Errorf("incorrect geometry: %v != %v", s.Geometry, testPoint)
	}

	if s.SRID != 4326 {
		t.Errorf("incorrect srid: %v != %v", s.SRID, 4326)
	}

	t.Run("scan nil data", func(t *testing.T) {
		var p orb.Point
		s := Scanner(&p)

		err := s.Scan(nil)
		if err != nil {
			t.Errorf("should noop for nil data: %v", err)
		}

		if s.Valid {
			t.Errorf("valid should be false for nil values")
		}
	})

	t.Run("scan nil byte interface", func(t *testing.T) {
		var p orb.Point
		s := Scanner(&p)

		var b []byte
		err := s.Scan(b)
		if err != nil {
			t.Errorf("should noop for nil data: %v", err)
		}

		if s.Valid {
			t.Errorf("valid should be false for nil values")
		}
	})

	t.Run("unknown geometry type", func(t *testing.T) {
		s := Scanner(nil)

		b := []byte{
			//01    02    03    04    05    06    07    08
			0x01, 0x08, 0x00, 0x00, 0x00, // CircularString type
			0x46, 0x81, 0xF6, 0x23, 0x2E, 0x4A, 0x5D, 0xC0,
			0x03, 0x46, 0x1B, 0x3C, 0xAF, 0x5B, 0x40, 0x40,
		}
		err := s.Scan(b)
		if err != ErrUnsupportedGeometry {
			t.Errorf("incorrect error: %v != %v", err, ErrUnsupportedGeometry)
		}

		if s.Valid {
			t.Errorf("valid should be false errors")
		}
	})
}

func TestScanHexData(t *testing.T) {
	cases := []struct {
		name     string
		data     []byte
		expected orb.Point
	}{
		{
			name:     "point lower case",
			data:     []byte(`\x0101000020e6100000e0d57267266e4840b22ac24d46b50240`),
			expected: orb.Point{48.860547, 2.338513},
		},
		{
			name:     "point upper case",
			data:     []byte(`\x0101000020E6100000E0D57267266E4840B22AC24D46B50240`),
			expected: orb.Point{48.860547, 2.338513},
		},
		{
			name:     "no prefix, point lower case",
			data:     []byte(`0101000020e6100000e0d57267266e4840b22ac24d46b50240`),
			expected: orb.Point{48.860547, 2.338513},
		},
		{
			name:     "no prefix, point upper case",
			data:     []byte(`0101000020E6100000E0D57267266E4840B22AC24D46B50240`),
			expected: orb.Point{48.860547, 2.338513},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var p orb.Point
			s := Scanner(&p)

			err := s.Scan(tc.data)
			if err != nil {
				t.Fatalf("scan error: %v", err)
			}

			if !p.Equal(tc.expected) {
				t.Errorf("unequal data")
				t.Log(p)
				t.Log(tc.expected)
			}

			if s.SRID != 4326 {
				t.Errorf("incorrect SRID: %v != %v", s.SRID, 4326)
			}
		})
	}
}

func TestScanHexData_errors(t *testing.T) {
	cases := []struct {
		name     string
		data     []byte
		expected orb.Point
	}{
		{
			name:     "not hex data",
			data:     []byte(`\xZZ0101000000e0d57267266e4840b22ac24d46b50240`),
			expected: orb.Point{48.860547, 2.338513},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var p orb.Point
			s := Scanner(&p)

			err := s.Scan(tc.data)
			if err == nil {
				t.Fatalf("should have error, but no error")
			}
		})
	}
}

func TestScanPoint(t *testing.T) {
	cases := []struct {
		name     string
		data     []byte
		srid     int
		expected orb.Point
	}{
		{
			name:     "point",
			data:     MustMarshal(orb.Point{4, 5}, 4326),
			srid:     4326,
			expected: orb.Point{4, 5},
		},
		{
			name:     "single multi-point",
			data:     MustMarshal(orb.MultiPoint{{1, 2}}, 4326),
			srid:     4326,
			expected: orb.Point{1, 2},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var p orb.Point
			s := Scanner(&p)
			err := s.Scan(tc.data)
			if err != nil {
				t.Fatalf("scan error: %v", err)
			}

			if !p.Equal(tc.expected) {
				t.Errorf("unequal data")
				t.Log(p)
				t.Log(tc.expected)
			}

			if p != s.Geometry {
				t.Errorf("should set to scanner's geometry attribute")
			}

			if !s.Valid {
				t.Errorf("should set valid to true")
			}

			if s.SRID != tc.srid {
				t.Errorf("incorrect SRID: %v != %v", s.SRID, tc.srid)
			}
		})
	}
}

func TestScanPoint_errors(t *testing.T) {
	// error conditions
	cases := []struct {
		name string
		data interface{}
		err  error
	}{
		{
			name: "incorrect data",
			data: 123,
			err:  ErrUnsupportedDataType,
		},
		{
			name: "not wkb, too short",
			data: []byte{0, 0, 0, 0, 1, 192, 94, 157, 24, 227, 60, 152, 15, 64, 66, 222, 128, 39},
			err:  ErrNotEWKB,
		},
		{
			name: "invalid first byte",
			data: []byte{3, 1, 0, 0, 0, 15, 152, 60, 227, 24, 157, 94, 192, 205, 11, 17, 39, 128, 222, 66, 64},
			err:  ErrNotEWKB,
		},
		{
			name: "incorrect geometry",
			data: MustMarshal(orb.LineString{{0, 0}, {1, 2}}, 4326),
			err:  ErrIncorrectGeometry,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var p orb.Point
			s := Scanner(&p)
			err := s.Scan(tc.data)
			if err != tc.err {
				t.Errorf("incorrect error: %v != %v", err, tc.err)
			}

			if s.Geometry != nil {
				t.Errorf("geometry should be nil on errors")
			}

			if s.Valid {
				t.Errorf("valid should be false on errors")
			}
		})
	}
}

func TestScannerPrefixSRID(t *testing.T) {
	cases := []struct {
		name     string
		data     []byte
		srid     int
		expected orb.Geometry
	}{
		{
			name:     "point",
			data:     append([]byte{230, 16, 0, 0}, MustMarshal(orb.Point{4, 5}, 0)...),
			srid:     4326,
			expected: orb.Point{4, 5},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := ScannerPrefixSRID(nil)
			err := s.Scan(tc.data)
			if err != nil {
				t.Fatalf("scan error: %v", err)
			}

			if !orb.Equal(s.Geometry, tc.expected) {
				t.Errorf("unequal data")
				t.Log(s.Geometry)
				t.Log(tc.expected)
			}

			if s.SRID != tc.srid {
				t.Errorf("incorrect SRID: %v != %v", s.SRID, tc.srid)
			}
		})
	}
}

func TestValuePrefixSRID(t *testing.T) {
	cases := []struct {
		name     string
		geom     orb.Geometry
		srid     int
		expected []byte
	}{
		{
			name:     "point",
			geom:     orb.Point{4, 5},
			srid:     4326,
			expected: append([]byte{230, 16, 0, 0}, MustMarshal(orb.Point{4, 5}, 0)...),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v := ValuePrefixSRID(tc.geom, tc.srid)
			data, err := v.Value()
			if err != nil {
				t.Fatalf("value error: %v", err)
			}

			if !bytes.Equal(data.([]byte), tc.expected) {
				t.Errorf("unequal data")
				t.Log(data)
				t.Log(tc.expected)
			}
		})
	}
}

func TestScanMultiPoint(t *testing.T) {
	cases := []struct {
		name     string
		data     []byte
		srid     int
		expected orb.MultiPoint
	}{
		{
			name:     "multi point",
			data:     MustMarshal(orb.MultiPoint{{1, 2}, {3, 4}}, 4326),
			srid:     4326,
			expected: orb.MultiPoint{{1, 2}, {3, 4}},
		},
		{
			name:     "point should covert to multi point",
			data:     MustMarshal(orb.Point{1, 2}, 4326),
			srid:     4326,
			expected: orb.MultiPoint{{1, 2}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var mp orb.MultiPoint
			s := Scanner(&mp)
			err := s.Scan(tc.data)
			if err != nil {
				t.Fatalf("scan error: %v", err)
			}

			if !mp.Equal(tc.expected) {
				t.Errorf("unequal data")
				t.Log(mp)
				t.Log(tc.expected)
			}

			if !reflect.DeepEqual(mp, s.Geometry) {
				t.Errorf("should set to scanner's geometry attribute")
			}

			if !s.Valid {
				t.Errorf("should set valid to true")
			}

			if s.SRID != tc.srid {
				t.Errorf("incorrect SRID: %v != %v", s.SRID, tc.srid)
			}
		})
	}
}

func TestScanMultiPoint_errors(t *testing.T) {
	cases := []struct {
		name string
		data interface{}
		err  error
	}{
		{
			name: "does not like line string",
			data: MustMarshal(orb.LineString{{0, 0}}, 4326),
			err:  ErrIncorrectGeometry,
		},
		{
			name: "incorrect data",
			data: 123,
			err:  ErrUnsupportedDataType,
		},
		{
			name: "not wkb",
			data: []byte{0, 0, 0, 0, 1, 192, 94},
			err:  ErrNotEWKB,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var mp orb.MultiPoint
			s := Scanner(&mp)
			err := s.Scan(tc.data)
			if err != tc.err {
				t.Errorf("incorrect error: %v != %v", err, tc.err)
			}

			if s.Geometry != nil {
				t.Errorf("geometry should be nil on errors")
			}

			if s.Valid {
				t.Errorf("valid should be false on errors")
			}
		})
	}
}

func TestScanLineString(t *testing.T) {
	cases := []struct {
		name     string
		data     []byte
		srid     int
		expected orb.LineString
	}{
		{
			name:     "line string",
			data:     MustMarshal(orb.LineString{{1, 2}, {3, 4}}, 4326),
			srid:     4326,
			expected: orb.LineString{{1, 2}, {3, 4}},
		},
		{
			name:     "single multi line string",
			data:     MustMarshal(orb.MultiLineString{{{1, 2}, {3, 4}}}, 4326),
			srid:     4326,
			expected: orb.LineString{{1, 2}, {3, 4}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var ls orb.LineString
			s := Scanner(&ls)
			err := s.Scan(tc.data)
			if err != nil {
				t.Fatalf("scan error: %v", err)
			}

			if !ls.Equal(tc.expected) {
				t.Errorf("unequal data")
				t.Log(ls)
				t.Log(tc.expected)
			}

			if !reflect.DeepEqual(ls, s.Geometry) {
				t.Errorf("should set to scanner's geometry attribute")
			}

			if !s.Valid {
				t.Errorf("should set valid to true")
			}

			if s.SRID != tc.srid {
				t.Errorf("incorrect SRID: %v != %v", s.SRID, tc.srid)
			}
		})
	}
}

func TestScanLineString_errors(t *testing.T) {
	cases := []struct {
		name string
		data interface{}
		err  error
	}{
		{
			name: "does not like multi point",
			data: MustMarshal(orb.MultiPoint{{1, 2}, {3, 4}}, 4326),
			err:  ErrIncorrectGeometry,
		},
		{
			name: "incorrect data",
			data: 123,
			err:  ErrUnsupportedDataType,
		},
		{
			name: "not wkb",
			data: []byte{0, 0, 0, 0, 2, 192, 94},
			err:  ErrNotEWKB,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var ls orb.LineString
			s := Scanner(&ls)
			err := s.Scan(tc.data)
			if err != tc.err {
				t.Errorf("incorrect error: %v != %v", err, tc.err)
			}

			if s.Geometry != nil {
				t.Errorf("geometry should be nil on errors")
			}

			if s.Valid {
				t.Errorf("valid should be false on errors")
			}
		})
	}
}

func TestScanMultiLineString(t *testing.T) {
	cases := []struct {
		name     string
		data     []byte
		srid     int
		expected orb.MultiLineString
	}{
		{
			name:     "line string",
			data:     MustMarshal(orb.LineString{{1, 2}, {3, 4}}, 4326),
			srid:     4326,
			expected: orb.MultiLineString{{{1, 2}, {3, 4}}},
		},
		{
			name:     "multi line string",
			data:     MustMarshal(orb.MultiLineString{{{1, 2}, {3, 4}}, {{5, 6}, {7, 8}}}, 4326),
			srid:     4326,
			expected: orb.MultiLineString{{{1, 2}, {3, 4}}, {{5, 6}, {7, 8}}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var mls orb.MultiLineString
			s := Scanner(&mls)
			err := s.Scan(tc.data)
			if err != nil {
				t.Fatalf("scan error: %v", err)
			}

			if !mls.Equal(tc.expected) {
				t.Errorf("unequal data")
				t.Log(mls)
				t.Log(tc.expected)
			}

			if !reflect.DeepEqual(mls, s.Geometry) {
				t.Errorf("should set to scanner's geometry attribute")
			}

			if !s.Valid {
				t.Errorf("should set valid to true")
			}

			if s.SRID != tc.srid {
				t.Errorf("incorrect SRID: %v != %v", s.SRID, tc.srid)
			}
		})
	}
}

func TestScanMultiLineString_errors(t *testing.T) {
	cases := []struct {
		name string
		data interface{}
		err  error
	}{
		{
			name: "does not like multi point",
			data: MustMarshal(orb.MultiPoint{{1, 2}}, 4326),
			err:  ErrIncorrectGeometry,
		},
		{
			name: "incorrect data",
			data: 123,
			err:  ErrUnsupportedDataType,
		},
		{
			name: "not wkb",
			data: []byte{0, 0, 0, 0, 5, 192, 94},
			err:  ErrNotEWKB,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var mls orb.MultiLineString
			s := Scanner(&mls)
			err := s.Scan(tc.data)
			if err != tc.err {
				t.Errorf("incorrect error: %v != %v", err, tc.err)
			}

			if s.Geometry != nil {
				t.Errorf("geometry should be nil on errors")
			}

			if s.Valid {
				t.Errorf("valid should be false on errors")
			}
		})
	}
}

func TestScanRing(t *testing.T) {
	cases := []struct {
		name     string
		data     []byte
		srid     int
		expected orb.Ring
	}{
		{
			name:     "polygon",
			data:     MustMarshal(orb.Polygon{{{0, 0}, {0, 1}, {1, 0}, {0, 0}}}, 1234),
			srid:     1234,
			expected: orb.Ring{{0, 0}, {0, 1}, {1, 0}, {0, 0}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var r orb.Ring
			s := Scanner(&r)
			err := s.Scan(tc.data)
			if err != nil {
				t.Fatalf("scan error: %v", err)
			}

			if !r.Equal(tc.expected) {
				t.Errorf("unequal data")
				t.Log(r)
				t.Log(tc.expected)
			}

			if !reflect.DeepEqual(r, s.Geometry) {
				t.Errorf("should set to scanner's geometry attribute")
			}

			if !s.Valid {
				t.Errorf("should set valid to true")
			}

			if s.SRID != tc.srid {
				t.Errorf("incorrect SRID: %v != %v", s.SRID, tc.srid)
			}
		})
	}
}

func TestScanRing_errors(t *testing.T) {
	cases := []struct {
		name string
		data interface{}
		err  error
	}{
		{
			name: "does not like line strings",
			data: MustMarshal(orb.LineString{{0, 0}, {0, 1}, {1, 0}, {0, 0}}, 4326),
			err:  ErrIncorrectGeometry,
		},
		{
			name: "incorrect data",
			data: 123,
			err:  ErrUnsupportedDataType,
		},
		{
			name: "not wkb",
			data: []byte{0, 0, 0, 0, 1, 192, 94},
			err:  ErrNotEWKB,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var r orb.Ring
			s := Scanner(&r)
			err := s.Scan(tc.data)
			if err != tc.err {
				t.Errorf("incorrect error: %v != %v", err, tc.err)
			}

			if s.Geometry != nil {
				t.Errorf("geometry should be nil on errors")
			}

			if s.Valid {
				t.Errorf("valid should be false on errors")
			}
		})
	}
}

func TestScanPolygon(t *testing.T) {
	cases := []struct {
		name     string
		data     []byte
		srid     int
		expected orb.Polygon
	}{
		{
			name:     "polygon",
			data:     testPolygonData,
			srid:     4326,
			expected: testPolygon,
		},
		{
			name:     "single multi polygon",
			data:     testMultiPolygonSingleData,
			srid:     4326,
			expected: testMultiPolygonSingle[0],
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var p orb.Polygon
			s := Scanner(&p)
			err := s.Scan(tc.data)
			if err != nil {
				t.Fatalf("scan error: %v", err)
			}

			if !p.Equal(tc.expected) {
				t.Errorf("unequal data")
				t.Log(p)
				t.Log(tc.expected)
			}

			if !reflect.DeepEqual(p, s.Geometry) {
				t.Errorf("should set to scanner's geometry attribute")
			}

			if !s.Valid {
				t.Errorf("should set valid to true")
			}

			if s.SRID != tc.srid {
				t.Errorf("incorrect SRID: %v != %v", s.SRID, tc.srid)
			}
		})
	}
}

func TestScanPolygon_errors(t *testing.T) {
	cases := []struct {
		name string
		data interface{}
		err  error
	}{
		{
			name: "does not like line strings",
			data: MustMarshal(orb.LineString{{0, 0}, {0, 1}, {1, 0}, {0, 0}}, 4326),
			err:  ErrIncorrectGeometry,
		},
		{
			name: "incorrect data",
			data: 123,
			err:  ErrUnsupportedDataType,
		},
		{
			name: "not wkb",
			data: []byte{0, 0, 0, 0, 3, 192, 94},
			err:  ErrNotEWKB,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var p orb.Polygon
			s := Scanner(&p)
			err := s.Scan(tc.data)
			if err != tc.err {
				t.Errorf("incorrect error: %v != %v", err, tc.err)
			}

			if s.Geometry != nil {
				t.Errorf("geometry should be nil on errors")
			}

			if s.Valid {
				t.Errorf("valid should be false on errors")
			}
		})
	}
}

func TestScanMultiPolygon(t *testing.T) {
	cases := []struct {
		name     string
		data     []byte
		srid     int
		expected orb.MultiPolygon
	}{
		{
			name:     "multi polygon",
			data:     testMultiPolygonData,
			srid:     4326,
			expected: testMultiPolygon,
		},
		{
			name:     "single multi polygon",
			data:     testMultiPolygonSingleData,
			srid:     4326,
			expected: testMultiPolygonSingle,
		},
		{
			name:     "polygon",
			data:     testPolygonData,
			srid:     4326,
			expected: orb.MultiPolygon{testPolygon},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var mp orb.MultiPolygon
			s := Scanner(&mp)
			err := s.Scan(tc.data)
			if err != nil {
				t.Fatalf("scan error: %v", err)
			}

			if !mp.Equal(tc.expected) {
				t.Errorf("unequal data")
				t.Log(mp)
				t.Log(tc.expected)
			}

			if !reflect.DeepEqual(mp, s.Geometry) {
				t.Errorf("should set to scanner's geometry attribute")
			}

			if !s.Valid {
				t.Errorf("should set valid to true")
			}

			if s.SRID != tc.srid {
				t.Errorf("incorrect SRID: %v != %v", s.SRID, tc.srid)
			}
		})
	}
}

func TestScanMultiPolygon_errors(t *testing.T) {
	cases := []struct {
		name string
		data interface{}
		err  error
	}{
		{
			name: "does not like line strings",
			data: MustMarshal(orb.LineString{{0, 0}, {0, 1}, {1, 0}, {0, 0}}, 4326),
			err:  ErrIncorrectGeometry,
		},
		{
			name: "incorrect data",
			data: 123,
			err:  ErrUnsupportedDataType,
		},
		{
			name: "not wkb",
			data: []byte{0, 0, 0, 0, 6, 192, 94},
			err:  ErrNotEWKB,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var mp orb.MultiPolygon
			s := Scanner(&mp)
			err := s.Scan(tc.data)
			if err != tc.err {
				t.Errorf("incorrect error: %v != %v", err, tc.err)
			}

			if s.Geometry != nil {
				t.Errorf("geometry should be nil on errors")
			}

			if s.Valid {
				t.Errorf("valid should be false on errors")
			}
		})
	}
}

func TestScanCollection(t *testing.T) {
	cases := []struct {
		name     string
		data     []byte
		srid     int
		expected orb.Collection
	}{
		{
			name:     "collection",
			data:     testCollectionData,
			srid:     4326,
			expected: testCollection,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var c orb.Collection
			s := Scanner(&c)
			err := s.Scan(tc.data)
			if err != nil {
				t.Fatalf("scan error: %v", err)
			}

			if !c.Equal(tc.expected) {
				t.Errorf("unequal data")
				t.Log(c)
				t.Log(tc.expected)
			}

			if !reflect.DeepEqual(c, s.Geometry) {
				t.Errorf("should set to scanner's geometry attribute")
			}

			if !s.Valid {
				t.Errorf("should set valid to true")
			}

			if s.SRID != tc.srid {
				t.Errorf("incorrect SRID: %v != %v", s.SRID, tc.srid)
			}
		})
	}
}

func TestScanCollection_errors(t *testing.T) {
	cases := []struct {
		name string
		data interface{}
		err  error
	}{
		{
			name: "does not like line strings",
			data: MustMarshal(orb.LineString{{0, 0}, {0, 1}, {1, 0}, {0, 0}}, 4326),
			err:  ErrIncorrectGeometry,
		},
		{
			name: "incorrect data",
			data: 123,
			err:  ErrUnsupportedDataType,
		},
		{
			name: "not wkb",
			data: []byte{0, 0, 0, 0, 7, 192, 94},
			err:  ErrNotEWKB,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var c orb.Collection
			s := Scanner(&c)
			err := s.Scan(tc.data)
			if err != tc.err {
				t.Errorf("incorrect error: %v != %v", err, tc.err)
			}

			if s.Geometry != nil {
				t.Errorf("geometry should be nil on errors")
			}

			if s.Valid {
				t.Errorf("valid should be false on errors")
			}
		})
	}
}

func TestScanBound(t *testing.T) {
	cases := []struct {
		name     string
		data     []byte
		srid     int
		expected orb.Bound
	}{
		{
			name:     "point",
			data:     MustMarshal(orb.Point{1, 2}, 4326),
			srid:     4326,
			expected: orb.Point{1, 2}.Bound(),
		},
		{
			name:     "linestring",
			data:     MustMarshal(orb.LineString{{0, 0}, {0, 1}, {1, 0}, {0, 0}}, 4326),
			srid:     4326,
			expected: orb.LineString{{0, 0}, {0, 1}, {1, 0}, {0, 0}}.Bound(),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var b orb.Bound
			s := Scanner(&b)
			err := s.Scan(tc.data)
			if err != nil {
				t.Fatalf("scan error: %v", err)
			}

			if !b.Equal(tc.expected) {
				t.Errorf("unequal data")
				t.Log(b)
				t.Log(tc.expected)
			}

			if !reflect.DeepEqual(b, s.Geometry) {
				t.Errorf("should set to scanner's geometry attribute")
			}

			if !s.Valid {
				t.Errorf("should set valid to true")
			}

			if s.SRID != tc.srid {
				t.Errorf("incorrect SRID: %v != %v", s.SRID, tc.srid)
			}
		})
	}
}

func TestScanBound_errors(t *testing.T) {
	cases := []struct {
		name string
		data interface{}
		err  error
	}{
		{
			name: "incorrect data",
			data: 123,
			err:  ErrUnsupportedDataType,
		},
		{
			name: "not wkb",
			data: []byte{0, 0, 0, 0, 1, 192, 94},
			err:  ErrNotEWKB,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var b orb.Bound
			s := Scanner(&b)
			err := s.Scan(tc.data)
			if err != tc.err {
				t.Errorf("incorrect error: %v != %v", err, tc.err)
			}

			if s.Geometry != nil {
				t.Errorf("geometry should be nil on errors")
			}

			if s.Valid {
				t.Errorf("valid should be false on errors")
			}
		})
	}
}

func TestValue(t *testing.T) {
	t.Run("marshalls geometry", func(t *testing.T) {
		testPoint := orb.Point{1, 2}
		testPointData := MustMarshal(testPoint, 4326)
		val, err := Value(testPoint, 4326).Value()
		if err != nil {
			t.Errorf("value error: %v", err)
		}

		if !bytes.Equal(val.([]byte), testPointData) {
			t.Errorf("incorrect marshal")
			t.Log(val)
			t.Log(testPointData)
		}
	})

	t.Run("nil value in should set nil value", func(t *testing.T) {
		val, err := Value(nil, 4326).Value()
		if err != nil {
			t.Errorf("value error: %v", err)
		}

		if val != nil {
			t.Errorf("should be nil value: %[1]T, %[1]v", val)
		}
	})
}

func TestValue_nil(t *testing.T) {
	var (
		mp    orb.MultiPoint
		ls    orb.LineString
		mls   orb.MultiLineString
		r     orb.Ring
		poly  orb.Polygon
		mpoly orb.MultiPolygon
		c     orb.Collection
	)

	cases := []struct {
		name string
		geom orb.Geometry
	}{
		{
			name: "nil multi point",
			geom: mp,
		},
		{
			name: "nil line string",
			geom: ls,
		},
		{
			name: "nil multi line string",
			geom: mls,
		},
		{
			name: "nil ring",
			geom: r,
		},
		{
			name: "nil polygon",
			geom: poly,
		},
		{
			name: "nil multi polygon",
			geom: mpoly,
		},
		{
			name: "nil collection",
			geom: c,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			val, err := Value(tc.geom, 4326).Value()
			if err != nil {
				t.Errorf("value error: %v", err)
			}

			if val != nil {
				t.Errorf("should be nil value: %[1]T, %[1]v", val)
			}
		})
	}
}

func BenchmarkScan_point(b *testing.B) {
	p := orb.Point{1, 2}
	data, err := Marshal(p, 4326)
	if err != nil {
		b.Fatal(err)
	}

	var r orb.Point
	s := Scanner(&r)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := s.Scan(data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecode_point(b *testing.B) {
	p := orb.Point{1, 2}
	data, err := Marshal(p, 4326)
	if err != nil {
		b.Fatal(err)
	}

	r := bytes.NewReader(data)
	d := NewDecoder(r)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := d.Decode()
		if err != nil {
			b.Fatal(err)
		}

		r.Reset(data)
	}
}

func BenchmarkScan_lineString(b *testing.B) {
	var ls orb.LineString
	for i := 0; i < 100; i++ {
		ls = append(ls, orb.Point{float64(i), float64(i)})
	}
	data, err := Marshal(ls, 4326)
	if err != nil {
		b.Fatal(err)
	}

	var r orb.LineString
	s := Scanner(&r)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := s.Scan(data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecode_lineString(b *testing.B) {
	var ls orb.LineString
	for i := 0; i < 100; i++ {
		ls = append(ls, orb.Point{float64(i), float64(i)})
	}
	data, err := Marshal(ls, 4326)
	if err != nil {
		b.Fatal(err)
	}

	r := bytes.NewReader(data)
	d := NewDecoder(r)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := d.Decode()
		if err != nil {
			b.Fatal(err)
		}

		r.Reset(data)
	}
}

func BenchmarkScan_multiLineString(b *testing.B) {
	var mls orb.MultiLineString
	for i := 0; i < 10; i++ {
		var ls orb.LineString
		for j := 0; j < 100; j++ {
			ls = append(ls, orb.Point{float64(i), float64(i)})
		}
		mls = append(mls, ls)
	}
	data, err := Marshal(mls, 4326)
	if err != nil {
		b.Fatal(err)
	}

	var r orb.MultiLineString
	s := Scanner(&r)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := s.Scan(data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecode_multiLineString(b *testing.B) {
	var mls orb.MultiLineString
	for i := 0; i < 10; i++ {
		var ls orb.LineString
		for j := 0; j < 100; j++ {
			ls = append(ls, orb.Point{float64(i), float64(i)})
		}
		mls = append(mls, ls)
	}
	data, err := Marshal(mls, 4326)
	if err != nil {
		b.Fatal(err)
	}

	r := bytes.NewReader(data)
	d := NewDecoder(r)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := d.Decode()
		if err != nil {
			b.Fatal(err)
		}

		r.Reset(data)
	}
}

func BenchmarkScan_polygon(b *testing.B) {
	var p orb.Polygon
	for i := 0; i < 1; i++ {
		var r orb.Ring
		for j := 0; j < 6; j++ {
			r = append(r, orb.Point{float64(i), float64(i)})
		}
		p = append(p, r)
	}
	data, err := Marshal(p, 4326)
	if err != nil {
		b.Fatal(err)
	}

	var r orb.Polygon
	s := Scanner(&r)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := s.Scan(data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecode_polygon(b *testing.B) {
	var p orb.Polygon
	for i := 0; i < 1; i++ {
		var r orb.Ring
		for j := 0; j < 6; j++ {
			r = append(r, orb.Point{float64(i), float64(i)})
		}
		p = append(p, r)
	}
	data, err := Marshal(p, 4326)
	if err != nil {
		b.Fatal(err)
	}

	r := bytes.NewReader(data)
	d := NewDecoder(r)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := d.Decode()
		if err != nil {
			b.Fatal(err)
		}

		r.Reset(data)
	}
}
