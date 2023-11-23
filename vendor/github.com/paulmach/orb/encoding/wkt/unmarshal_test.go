package wkt

import (
	"testing"

	"github.com/paulmach/orb"
)

func TestTrimSpaceBrackets(t *testing.T) {
	cases := []struct {
		name     string
		s        string
		expected string
	}{
		{
			name:     "empty string",
			s:        "",
			expected: "",
		},
		{
			name:     "blank string",
			s:        "   ",
			expected: "",
		},
		{
			name:     "single point",
			s:        "(1 2)",
			expected: "1 2",
		},
		{
			name:     "double brackets",
			s:        "((1 2),(0.5 1.5))",
			expected: "(1 2),(0.5 1.5)",
		},
		{
			name:     "multiple values",
			s:        "(1 2,0.5 1.5)",
			expected: "1 2,0.5 1.5",
		},
		{
			name:     "multiple points",
			s:        "((1 2,3 4),(5 6,7 8))",
			expected: "(1 2,3 4),(5 6,7 8)",
		},
		{
			name:     "triple brackets",
			s:        "(((1 2,3 4)),((5 6,7 8),(1 2,5 4)))",
			expected: "((1 2,3 4)),((5 6,7 8),(1 2,5 4))",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v, err := trimSpaceBrackets(tc.s)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if v != tc.expected {
				t.Log(trimSpaceBrackets(tc.s))
				t.Log(tc.expected)
				t.Errorf("trim space and brackets error")
			}
		})
	}
}

func TestTrimSpaceBrackets_errors(t *testing.T) {
	cases := []struct {
		name string
		s    string
		err  error
	}{
		{
			name: "no brackets",
			s:    "1 2",
			err:  ErrNotWKT,
		},
		{
			name: "no start bracket",
			s:    "1 2)",
			err:  ErrNotWKT,
		},
		{
			name: "no end bracket",
			s:    "(1 2",
			err:  ErrNotWKT,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := trimSpaceBrackets(tc.s)
			if err != tc.err {
				t.Fatalf("wrong error: %v != %v", err, tc.err)
			}
		})
	}
}

func TestUnmarshalPoint(t *testing.T) {
	cases := []struct {
		name     string
		s        string
		expected orb.Point
	}{
		{
			name:     "int",
			s:        "POINT(1 2)",
			expected: orb.Point{1, 2},
		},
		{
			name:     "float64",
			s:        "POINT(1.34 2.35)",
			expected: orb.Point{1.34, 2.35},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			geom, err := UnmarshalPoint(tc.s)
			if err != nil {
				t.Fatal(err)
			}

			if !geom.Equal(tc.expected) {
				t.Log(geom)
				t.Log(tc.expected)
				t.Errorf("incorrect wkt unmarshalling")
			}
		})
	}
}

func TestUnmarshalPoint_errors(t *testing.T) {
	cases := []struct {
		name string
		s    string
		err  error
	}{
		{
			name: "just name",
			s:    "POINT",
			err:  ErrNotWKT,
		},
		{
			name: "too many points",
			s:    "POINT(1.34 2.35 3.36)",
			err:  ErrNotWKT,
		},
		{
			name: "not a point",
			s:    "MULTIPOINT((1.34 2.35))",
			err:  ErrIncorrectGeometry,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := UnmarshalPoint(tc.s)
			if err != tc.err {
				t.Fatalf("incorrect error: %v != %v", err, tc.err)
			}
		})
	}
}

func TestUnmarshalMultiPoint(t *testing.T) {
	cases := []struct {
		name     string
		s        string
		expected orb.MultiPoint
	}{
		{
			name:     "empty",
			s:        "MULTIPOINT EMPTY",
			expected: orb.MultiPoint{},
		},
		{
			name:     "1 point",
			s:        "MULTIPOINT((1 2))",
			expected: orb.MultiPoint{{1, 2}},
		},
		{
			name:     "2 points",
			s:        "MULTIPOINT((1 2),(0.5 1.5))",
			expected: orb.MultiPoint{{1, 2}, {0.5, 1.5}},
		},
		{
			name:     "spaces",
			s:        "MULTIPOINT((1 2)  ,	(0.5 1.5))",
			expected: orb.MultiPoint{{1, 2}, {0.5, 1.5}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			geom, err := UnmarshalMultiPoint(tc.s)
			if err != nil {
				t.Fatal(err)
			}

			if !geom.Equal(tc.expected) {
				t.Log(geom)
				t.Log(tc.expected)
				t.Errorf("incorrect wkt unmarshalling")
			}
		})
	}
}

func TestUnmarshalMultiPoint_errors(t *testing.T) {
	cases := []struct {
		name string
		s    string
		err  error
	}{
		{
			name: "just name",
			s:    "MULTIPOINT",
			err:  ErrNotWKT,
		},
		{
			name: "too many points",
			s:    "MULTIPOINT((1 2),(3 4 5))",
			err:  ErrNotWKT,
		},
		{
			name: "not a multipoint",
			s:    "POINT(1 2)",
			err:  ErrIncorrectGeometry,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := UnmarshalMultiPoint(tc.s)
			if err != tc.err {
				t.Fatalf("incorrect error: %v != %v", err, tc.err)
			}
		})
	}
}

func TestUnmarshalLineString(t *testing.T) {
	cases := []struct {
		name     string
		s        string
		expected orb.LineString
	}{
		{
			name:     "empty",
			s:        "LINESTRING EMPTY",
			expected: orb.LineString{},
		},
		{
			name:     "2 points",
			s:        "LINESTRING(1 2,0.5 1.5)",
			expected: orb.LineString{{1, 2}, {0.5, 1.5}},
		},
		{
			name:     "spaces",
			s:        "LINESTRING(1 2 , 0.5 1.5)",
			expected: orb.LineString{{1, 2}, {0.5, 1.5}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			geom, err := UnmarshalLineString(tc.s)
			if err != nil {
				t.Fatal(err)
			}

			if !geom.Equal(tc.expected) {
				t.Log(geom)
				t.Log(tc.expected)
				t.Errorf("incorrect wkt unmarshalling")
			}
		})
	}
}

func TestUnmarshalLineString_errors(t *testing.T) {
	cases := []struct {
		name string
		s    string
		err  error
	}{
		{
			name: "just name",
			s:    "LINESTRING",
			err:  ErrNotWKT,
		},
		{
			name: "too many points",
			s:    "LINESTRING(1 2,3 4 5)",
			err:  ErrNotWKT,
		},
		{
			name: "not a multipoint",
			s:    "POINT(1 2)",
			err:  ErrIncorrectGeometry,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := UnmarshalLineString(tc.s)
			if err != tc.err {
				t.Fatalf("incorrect error: %v != %v", err, tc.err)
			}
		})
	}
}

func TestUnmarshalMultiLineString(t *testing.T) {
	cases := []struct {
		name     string
		s        string
		expected orb.MultiLineString
	}{
		{
			name:     "empty",
			s:        "MULTILINESTRING EMPTY",
			expected: orb.MultiLineString{},
		},
		{
			name:     "2 lines",
			s:        "MULTILINESTRING((1 2,3 4),(5 6,7 8))",
			expected: orb.MultiLineString{{{1, 2}, {3, 4}}, {{5, 6}, {7, 8}}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			geom, err := UnmarshalMultiLineString(tc.s)
			if err != nil {
				t.Fatal(err)
			}

			if !geom.Equal(tc.expected) {
				t.Log(geom)
				t.Log(tc.expected)
				t.Errorf("incorrect wkt unmarshalling")
			}
		})
	}
}

func TestUnmarshalMultiLineString_errors(t *testing.T) {
	cases := []struct {
		name string
		s    string
		err  error
	}{
		{
			name: "just name",
			s:    "MULTILINESTRING",
			err:  ErrNotWKT,
		},
		{
			name: "too many points",
			s:    "MULTILINESTRING((1 2,3 4 5))",
			err:  ErrNotWKT,
		},
		{
			name: "not a multi linestring",
			s:    "POINT(1 2)",
			err:  ErrIncorrectGeometry,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := UnmarshalMultiLineString(tc.s)
			if err != tc.err {
				t.Fatalf("incorrect error: %v != %v", err, tc.err)
			}
		})
	}
}

func TestUnmarshalPolygon(t *testing.T) {
	cases := []struct {
		name     string
		s        string
		expected orb.Polygon
	}{
		{
			name:     "empty",
			s:        "POLYGON EMPTY",
			expected: orb.Polygon{},
		},
		{
			name:     "one ring",
			s:        "POLYGON((0 0,1 0,1 1,0 0))",
			expected: orb.Polygon{{{0, 0}, {1, 0}, {1, 1}, {0, 0}}},
		},
		{
			name:     "two rings",
			s:        "POLYGON((1 2,3 4),(5 6,7 8))",
			expected: orb.Polygon{{{1, 2}, {3, 4}}, {{5, 6}, {7, 8}}},
		},
		{
			name:     "two rings with spaces",
			s:        "POLYGON((1 2,3 4)   ,   (5 6  ,  7 8))",
			expected: orb.Polygon{{{1, 2}, {3, 4}}, {{5, 6}, {7, 8}}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			geom, err := UnmarshalPolygon(tc.s)
			if err != nil {
				t.Fatal(err)
			}

			if !geom.Equal(tc.expected) {
				t.Log(geom)
				t.Log(tc.expected)
				t.Errorf("incorrect wkt unmarshalling")
			}
		})
	}
}

func TestUnmarshalPolygon_errors(t *testing.T) {
	cases := []struct {
		name string
		s    string
		err  error
	}{
		{
			name: "just name",
			s:    "POLYGON",
			err:  ErrNotWKT,
		},
		{
			name: "too many points",
			s:    "POLYGON((1 2,3 4 5))",
			err:  ErrNotWKT,
		},
		{
			name: "not a polygon",
			s:    "POINT(1 2)",
			err:  ErrIncorrectGeometry,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := UnmarshalPolygon(tc.s)
			if err != tc.err {
				t.Fatalf("incorrect error: %v != %v", err, tc.err)
			}
		})
	}
}

func TestUnmarshalMutilPolygon(t *testing.T) {
	cases := []struct {
		name     string
		s        string
		expected orb.MultiPolygon
	}{
		{
			name:     "empty",
			s:        "MULTIPOLYGON EMPTY",
			expected: orb.MultiPolygon{},
		},
		{
			name:     "multi-polygon",
			s:        "MULTIPOLYGON(((1 2,3 4)),((5 6,7 8),(1 2,5 4)))",
			expected: orb.MultiPolygon{{{{1, 2}, {3, 4}}}, {{{5, 6}, {7, 8}}, {{1, 2}, {5, 4}}}},
		},
		{
			name:     "multi-polygon with spaces",
			s:        "MULTIPOLYGON(((1 2,3 4))  , 		((5 6,7 8),  (1 2,5 4)))",
			expected: orb.MultiPolygon{{{{1, 2}, {3, 4}}}, {{{5, 6}, {7, 8}}, {{1, 2}, {5, 4}}}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			geom, err := UnmarshalMultiPolygon(tc.s)
			if err != nil {
				t.Fatal(err)
			}
			if !geom.Equal(tc.expected) {
				t.Log(geom)
				t.Log(tc.expected)
				t.Errorf("incorrect wkt unmarshalling")
			}
		})
	}
}

func TestUnmarshalMultiPolygon_errors(t *testing.T) {
	cases := []struct {
		name string
		s    string
		err  error
	}{
		{
			name: "just name",
			s:    "MULTIPOLYGON",
			err:  ErrNotWKT,
		},
		{
			name: "too many points",
			s:    "MULTIPOLYGON(((1 2,3 4 5)))",
			err:  ErrNotWKT,
		},
		{
			name: "missing trailing )",
			s:    "MULTIPOLYGON(((0 1,3 0,4 3,0 4,0 1)), ((3 4,6 3,5 5,3 4)), ((0 0,-1 -2,-3 -2,-2 -1,0 0))",
			err:  ErrNotWKT,
		},
		{
			name: "not a multi polygon",
			s:    "POINT(1 2)",
			err:  ErrIncorrectGeometry,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := UnmarshalMultiPolygon(tc.s)
			if err != tc.err {
				t.Fatalf("incorrect error: %v != %v", err, tc.err)
			}
		})
	}
}

func TestUnmarshalCollection(t *testing.T) {
	cases := []struct {
		name     string
		s        string
		expected orb.Collection
	}{
		{
			name:     "empty",
			s:        "GEOMETRYCOLLECTION EMPTY",
			expected: orb.Collection{},
		},
		{
			name:     "point and line",
			s:        "GEOMETRYCOLLECTION(POINT(1 2),LINESTRING(3 4,5 6))",
			expected: orb.Collection{orb.Point{1, 2}, orb.LineString{{3, 4}, {5, 6}}},
		},
		{
			name: "lots of things",
			s:    "GEOMETRYCOLLECTION(POINT(1 2),LINESTRING(3 4,5 6),MULTILINESTRING((1 2,3 4),(5 6,7 8)),POLYGON((0 0,1 0,1 1,0 0)),POLYGON((1 2,3 4),(5 6,7 8)),MULTIPOLYGON(((1 2,3 4)),((5 6,7 8),(1 2,5 4))))",
			expected: orb.Collection{
				orb.Point{1, 2},
				orb.LineString{{3, 4}, {5, 6}},
				orb.MultiLineString{{{1, 2}, {3, 4}}, {{5, 6}, {7, 8}}},
				orb.Polygon{{{0, 0}, {1, 0}, {1, 1}, {0, 0}}},
				orb.Polygon{{{1, 2}, {3, 4}}, {{5, 6}, {7, 8}}},
				orb.MultiPolygon{{{{1, 2}, {3, 4}}}, {{{5, 6}, {7, 8}}, {{1, 2}, {5, 4}}}},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			geom, err := UnmarshalCollection(tc.s)
			if err != nil {
				t.Fatal(err)
			}
			if !geom.Equal(tc.expected) {
				t.Log(geom)
				t.Log(tc.expected)
				t.Errorf("incorrect wkt unmarshalling")
			}
		})
	}
}

func TestUnmarshalCollection_errors(t *testing.T) {
	cases := []struct {
		name string
		s    string
		err  error
	}{
		{
			name: "just name",
			s:    "GEOMETRYCOLLECTION",
			err:  ErrNotWKT,
		},
		{
			name: "too many points",
			s:    "GEOMETRYCOLLECTION(POINT(1 2 3))",
			err:  ErrNotWKT,
		},
		{
			name: "missing trailing paren",
			s:    "GEOMETRYCOLLECTION(POINT(1 2 3)",
			err:  ErrNotWKT,
		},
		{
			name: "not a geometry collection",
			s:    "POINT(1 2)",
			err:  ErrIncorrectGeometry,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := UnmarshalCollection(tc.s)
			if err != tc.err {
				t.Fatalf("incorrect error: %v != %v", err, tc.err)
			}
		})
	}
}
