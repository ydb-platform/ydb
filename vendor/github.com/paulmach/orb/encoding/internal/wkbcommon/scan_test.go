package wkbcommon

import (
	"testing"

	"github.com/paulmach/orb"
)

var SRID = []byte{215, 15, 0, 0}

func TestScanPoint(t *testing.T) {
	cases := []struct {
		name     string
		srid     int
		data     []byte
		expected orb.Point
	}{
		{
			name:     "point",
			data:     testPointData,
			expected: testPoint,
		},
		{
			name:     "single multi-point",
			data:     testMultiPointSingleData,
			expected: testMultiPointSingle[0],
		},
		{
			name:     "ewkb",
			srid:     4326,
			data:     MustMarshal(testMultiPointSingle[0], 4326),
			expected: testMultiPointSingle[0],
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			g, srid, valid, err := Scan(&orb.Point{}, tc.data)
			if err != nil {
				t.Fatalf("scan error: %v", err)
			}

			if !valid {
				t.Errorf("valid should be true")
			}

			if tc.srid != 0 && tc.srid != srid {
				t.Errorf("incorrect SRID: %v != %v", srid, tc.srid)
			}

			p := g.(orb.Point)
			if !p.Equal(tc.expected) {
				t.Errorf("unequal data")
				t.Log(p)
				t.Log(tc.expected)
			}
		})
	}
}

func TestScanPoint_errors(t *testing.T) {
	// error conditions
	cases := []struct {
		name string
		data []byte
		err  error
	}{
		{
			name: "not wkb",
			data: []byte{0, 0, 0, 0, 1, 192, 94, 157, 24, 227, 60, 152, 15, 64, 66, 222, 128, 39},
			err:  ErrNotWKB,
		},
		{
			name: "invalid first byte",
			data: []byte{3, 1, 0, 0, 0, 15, 152, 60, 227, 24, 157, 94, 192, 205, 11, 17, 39, 128, 222, 66, 64},
			err:  ErrNotWKBHeader,
		},
		{
			name: "incorrect geometry",
			data: testLineStringData,
			err:  ErrIncorrectGeometry,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, _, err := Scan(&orb.Point{}, tc.data)
			if err != tc.err {
				t.Errorf("incorrect error: %v != %v", err, tc.err)
			}
		})
	}
}

func TestScanMultiPoint(t *testing.T) {
	cases := []struct {
		name     string
		srid     int
		data     []byte
		expected orb.MultiPoint
	}{
		{
			name:     "multi point",
			data:     testMultiPointData,
			expected: testMultiPoint,
		},
		{
			name:     "multi point as ewkb",
			srid:     4326,
			data:     MustMarshal(testMultiPoint, 4326),
			expected: testMultiPoint,
		},
		{
			name:     "point should covert to multi point",
			data:     testPointData,
			expected: orb.MultiPoint{testPoint},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			g, srid, valid, err := Scan(&orb.MultiPoint{}, tc.data)
			if err != nil {
				t.Fatalf("scan error: %v", err)
			}

			if !valid {
				t.Errorf("valid should be true")
			}

			if tc.srid != 0 && tc.srid != srid {
				t.Errorf("incorrect SRID: %v != %v", srid, tc.srid)
			}

			mp := g.(orb.MultiPoint)
			if !mp.Equal(tc.expected) {
				t.Errorf("unequal data")
				t.Log(mp)
				t.Log(tc.expected)
			}
		})
	}
}

func TestScanMultiPoint_errors(t *testing.T) {
	cases := []struct {
		name string
		data []byte
		err  error
	}{
		{
			name: "does not like line string",
			data: testLineStringData,
			err:  ErrIncorrectGeometry,
		},
		{
			name: "not wkb",
			data: []byte{0, 0, 0, 0, 1, 192, 94},
			err:  ErrNotWKB,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, _, err := Scan(&orb.MultiPoint{}, tc.data)
			if err != tc.err {
				t.Errorf("incorrect error: %v != %v", err, tc.err)
			}
		})
	}
}

func TestScanLineString(t *testing.T) {
	cases := []struct {
		name     string
		srid     int
		data     []byte
		expected orb.LineString
	}{
		{
			name:     "line string",
			data:     testLineStringData,
			expected: testLineString,
		},
		{
			name:     "line string as ewkb",
			srid:     4326,
			data:     MustMarshal(testLineString, 4326),
			expected: testLineString,
		},
		{
			name:     "single multi line string",
			data:     testMultiLineStringSingleData,
			expected: testMultiLineStringSingle[0],
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			g, srid, valid, err := Scan(&orb.LineString{}, tc.data)
			if err != nil {
				t.Fatalf("scan error: %v", err)
			}

			if !valid {
				t.Errorf("valid should be true")
			}

			if tc.srid != 0 && tc.srid != srid {
				t.Errorf("incorrect SRID: %v != %v", srid, tc.srid)
			}

			ls := g.(orb.LineString)
			if !ls.Equal(tc.expected) {
				t.Errorf("unequal data")
				t.Log(ls)
				t.Log(tc.expected)
			}
		})
	}
}

func TestScanLineString_errors(t *testing.T) {
	cases := []struct {
		name string
		data []byte
		err  error
	}{
		{
			name: "does not like multi point",
			data: testMultiPointData,
			err:  ErrIncorrectGeometry,
		},
		{
			name: "not wkb",
			data: []byte{0, 0, 0, 0, 2, 192, 94},
			err:  ErrNotWKB,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, _, err := Scan(&orb.LineString{}, tc.data)
			if err != tc.err {
				t.Errorf("incorrect error: %v != %v", err, tc.err)
			}
		})
	}
}

func TestScanMultiLineString(t *testing.T) {
	cases := []struct {
		name     string
		srid     int
		data     []byte
		expected orb.MultiLineString
	}{
		{
			name:     "line string",
			data:     testLineStringData,
			expected: orb.MultiLineString{testLineString},
		},
		{
			name:     "multi line string",
			data:     testMultiLineStringData,
			expected: testMultiLineString,
		},
		{
			name:     "multi line string as ewkb",
			srid:     4326,
			data:     MustMarshal(testMultiLineString, 4326),
			expected: testMultiLineString,
		},
		{
			name:     "single multi line string",
			data:     testMultiLineStringSingleData,
			expected: testMultiLineStringSingle,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			g, srid, valid, err := Scan(&orb.MultiLineString{}, tc.data)
			if err != nil {
				t.Fatalf("scan error: %v", err)
			}

			if !valid {
				t.Errorf("valid should be true")
			}

			if tc.srid != 0 && tc.srid != srid {
				t.Errorf("incorrect SRID: %v != %v", srid, tc.srid)
			}

			mls := g.(orb.MultiLineString)
			if !mls.Equal(tc.expected) {
				t.Errorf("unequal data")
				t.Log(mls)
				t.Log(tc.expected)
			}
		})
	}
}

func TestScanMultiLineString_errors(t *testing.T) {
	cases := []struct {
		name string
		data []byte
		err  error
	}{
		{
			name: "does not like multi point",
			data: testMultiPointData,
			err:  ErrIncorrectGeometry,
		},
		{
			name: "not wkb",
			data: []byte{0, 0, 0, 0, 5, 192, 94},
			err:  ErrNotWKB,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, _, err := Scan(&orb.MultiLineString{}, tc.data)
			if err != tc.err {
				t.Errorf("incorrect error: %v != %v", err, tc.err)
			}
		})
	}
}

func TestScanPolygon(t *testing.T) {
	cases := []struct {
		name     string
		srid     int
		data     []byte
		expected orb.Polygon
	}{
		{
			name:     "polygon",
			data:     testPolygonData,
			expected: testPolygon,
		},
		{
			name:     "polygon as ewkb",
			data:     MustMarshal(testPolygon, 4326),
			expected: testPolygon,
		},
		{
			name:     "single multi polygon",
			data:     testMultiPolygonSingleData,
			expected: testMultiPolygonSingle[0],
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			g, srid, valid, err := Scan(&orb.Polygon{}, tc.data)
			if err != nil {
				t.Fatalf("scan error: %v", err)
			}

			if !valid {
				t.Errorf("valid should be true")
			}

			if tc.srid != 0 && tc.srid != srid {
				t.Errorf("incorrect SRID: %v != %v", srid, tc.srid)
			}

			p := g.(orb.Polygon)
			if !p.Equal(tc.expected) {
				t.Errorf("unequal data")
				t.Log(p)
				t.Log(tc.expected)
			}
		})
	}
}

func TestScanPolygon_errors(t *testing.T) {
	cases := []struct {
		name string
		data []byte
		err  error
	}{
		{
			name: "does not like line strings",
			data: testLineStringData,
			err:  ErrIncorrectGeometry,
		},
		{
			name: "not wkb",
			data: []byte{0, 0, 0, 0, 3, 192, 94},
			err:  ErrNotWKB,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, _, err := Scan(&orb.Polygon{}, tc.data)
			if err != tc.err {
				t.Errorf("incorrect error: %v != %v", err, tc.err)
			}
		})
	}
}

func TestScanMultiPolygon(t *testing.T) {
	cases := []struct {
		name     string
		srid     int
		data     []byte
		expected orb.MultiPolygon
	}{
		{
			name:     "multi polygon",
			data:     testMultiPolygonData,
			expected: testMultiPolygon,
		},
		{
			name:     "multi polygon as ewkb",
			srid:     4326,
			data:     MustMarshal(testMultiPolygon, 4326),
			expected: testMultiPolygon,
		},
		{
			name:     "single multi polygon",
			data:     testMultiPolygonSingleData,
			expected: testMultiPolygonSingle,
		},
		{
			name:     "polygon",
			data:     testPolygonData,
			expected: orb.MultiPolygon{testPolygon},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			g, srid, valid, err := Scan(&orb.MultiPolygon{}, tc.data)
			if err != nil {
				t.Fatalf("scan error: %v", err)
			}

			if !valid {
				t.Errorf("valid should be true")
			}

			if tc.srid != 0 && tc.srid != srid {
				t.Errorf("incorrect SRID: %v != %v", srid, tc.srid)
			}

			mp := g.(orb.MultiPolygon)
			if !mp.Equal(tc.expected) {
				t.Errorf("unequal data")
				t.Log(mp)
				t.Log(tc.expected)
			}
		})
	}
}

func TestScanMultiPolygon_errors(t *testing.T) {
	cases := []struct {
		name string
		data []byte
		err  error
	}{
		{
			name: "does not like line strings",
			data: testLineStringData,
			err:  ErrIncorrectGeometry,
		},
		{
			name: "not wkb",
			data: []byte{0, 0, 0, 0, 6, 192, 94},
			err:  ErrNotWKB,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, _, err := Scan(&orb.MultiPolygon{}, tc.data)
			if err != tc.err {
				t.Errorf("incorrect error: %v != %v", err, tc.err)
			}
		})
	}
}

func TestScanCollection(t *testing.T) {
	cases := []struct {
		name     string
		srid     int
		data     []byte
		expected orb.Collection
	}{
		{
			name:     "collection",
			data:     testCollectionData,
			expected: testCollection,
		},
		{
			name:     "collection as ewkb",
			srid:     4326,
			data:     MustMarshal(testCollection, 4326),
			expected: testCollection,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			g, srid, valid, err := Scan(&orb.Collection{}, tc.data)
			if err != nil {
				t.Fatalf("scan error: %v", err)
			}

			if !valid {
				t.Errorf("valid should be true")
			}

			if tc.srid != 0 && tc.srid != srid {
				t.Errorf("incorrect SRID: %v != %v", srid, tc.srid)
			}

			c := g.(orb.Collection)
			if !c.Equal(tc.expected) {
				t.Errorf("unequal data")
				t.Log(c)
				t.Log(tc.expected)
			}
		})
	}
}

func TestScanCollection_errors(t *testing.T) {
	cases := []struct {
		name string
		data []byte
		err  error
	}{
		{
			name: "does not like line strings",
			data: testLineStringData,
			err:  ErrIncorrectGeometry,
		},
		{
			name: "not wkb",
			data: []byte{0, 0, 0, 0, 7, 192, 94},
			err:  ErrNotWKB,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, _, err := Scan(&orb.Collection{}, tc.data)
			if err != tc.err {
				t.Errorf("incorrect error: %v != %v", err, tc.err)
			}
		})
	}
}
