package wkt

import (
	"bytes"
	"testing"

	"github.com/paulmach/orb"
)

func TestMarshal(t *testing.T) {
	cases := []struct {
		name     string
		geo      orb.Geometry
		expected []byte
	}{
		{
			name:     "point",
			geo:      orb.Point{1, 2},
			expected: []byte("POINT(1 2)"),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v := Marshal(tc.geo)
			if !bytes.Equal(v, tc.expected) {
				t.Log(string(v))
				t.Log(string(tc.expected))
				t.Errorf("incorrect wkt marshalling")
			}
		})
	}
}

func TestMarshalString(t *testing.T) {
	cases := []struct {
		name     string
		geo      orb.Geometry
		expected string
	}{
		{
			name:     "point",
			geo:      orb.Point{1, 2},
			expected: "POINT(1 2)",
		},
		{
			name:     "multipoint",
			geo:      orb.MultiPoint{{1, 2}, {0.5, 1.5}},
			expected: "MULTIPOINT((1 2),(0.5 1.5))",
		},
		{
			name:     "multipoint empty",
			geo:      orb.MultiPoint{},
			expected: "MULTIPOINT EMPTY",
		},
		{
			name:     "linestring",
			geo:      orb.LineString{{1, 2}, {0.5, 1.5}},
			expected: "LINESTRING(1 2,0.5 1.5)",
		},
		{
			name:     "linestring empty",
			geo:      orb.LineString{},
			expected: "LINESTRING EMPTY",
		},
		{
			name:     "multilinestring",
			geo:      orb.MultiLineString{{{1, 2}, {3, 4}}, {{5, 6}, {7, 8}}},
			expected: "MULTILINESTRING((1 2,3 4),(5 6,7 8))",
		},
		{
			name:     "multilinestring empty",
			geo:      orb.MultiLineString{},
			expected: "MULTILINESTRING EMPTY",
		},
		{
			name:     "ring",
			geo:      orb.Ring{{0, 0}, {1, 0}, {1, 1}, {0, 0}},
			expected: "POLYGON((0 0,1 0,1 1,0 0))",
		},
		{
			name:     "polygon",
			geo:      orb.Polygon{{{1, 2}, {3, 4}}, {{5, 6}, {7, 8}}},
			expected: "POLYGON((1 2,3 4),(5 6,7 8))",
		},
		{
			name:     "polygon empty",
			geo:      orb.Polygon{},
			expected: "POLYGON EMPTY",
		},
		{
			name:     "multipolygon",
			geo:      orb.MultiPolygon{{{{1, 2}, {3, 4}}}, {{{5, 6}, {7, 8}}, {{1, 2}, {5, 4}}}},
			expected: "MULTIPOLYGON(((1 2,3 4)),((5 6,7 8),(1 2,5 4)))",
		},
		{
			name:     "multipolygon empty",
			geo:      orb.MultiPolygon{},
			expected: "MULTIPOLYGON EMPTY",
		},
		{
			name:     "collection",
			geo:      orb.Collection{orb.Point{1, 2}, orb.LineString{{3, 4}, {5, 6}}},
			expected: "GEOMETRYCOLLECTION(POINT(1 2),LINESTRING(3 4,5 6))",
		},
		{
			name:     "collection empty",
			geo:      orb.Collection{},
			expected: "GEOMETRYCOLLECTION EMPTY",
		},
		{
			name:     "bound",
			geo:      orb.Bound{Min: orb.Point{0, 0}, Max: orb.Point{1, 2}},
			expected: "POLYGON((0 0,1 0,1 2,0 2,0 0))",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v := MarshalString(tc.geo)
			if v != tc.expected {
				t.Log(v)
				t.Log(tc.expected)
				t.Errorf("incorrect wkt marshalling")
			}
		})
	}
}
