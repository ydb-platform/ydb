package clip

import (
	"testing"

	"github.com/paulmach/orb"
)

func TestGeometry(t *testing.T) {
	bound := orb.Bound{Min: orb.Point{-1, -1}, Max: orb.Point{1, 1}}
	for _, g := range orb.AllGeometries {
		Geometry(bound, g)
	}

	cases := []struct {
		name   string
		input  orb.Geometry
		output orb.Geometry
	}{
		{
			name:   "only one multipoint in bound",
			input:  orb.MultiPoint{{0, 0}, {5, 5}},
			output: orb.Point{0, 0},
		},
		{
			name: "only one multilinestring in bound",
			input: orb.MultiLineString{
				{{0, 0}, {5, 5}},
				{{6, 6}, {7, 7}},
			},
			output: orb.LineString{{0, 0}, {1, 1}},
		},
		{
			name: "only one multipolygon in bound",
			input: orb.MultiPolygon{
				{{{0, 0}, {1, 0}, {1, 1}, {0, 1}, {0, 0}}},
				{{{2, 2}, {3, 2}, {3, 3}, {2, 3}, {2, 2}}},
			},
			output: orb.Polygon{{{0, 0}, {1, 0}, {1, 1}, {0, 1}, {0, 0}}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result := Geometry(bound, tc.input)

			if !orb.Equal(result, tc.output) {
				t.Errorf("not equal")
				t.Logf("%v", result)
				t.Logf("%v", tc.output)
			}
		})
	}
}

func TestRing(t *testing.T) {
	cases := []struct {
		name   string
		bound  orb.Bound
		input  orb.Ring
		output orb.Ring
	}{
		{
			name:  "regular clip",
			bound: orb.Bound{Min: orb.Point{0, 0}, Max: orb.Point{1.5, 1.5}},
			input: orb.Ring{
				{1, 1}, {2, 1}, {2, 2}, {1, 2}, {1, 1},
			},
			output: orb.Ring{
				{1, 1}, {1.5, 1}, {1.5, 1.5}, {1, 1.5}, {1, 1},
			},
		},
		{
			name:  "bound to the top",
			bound: orb.Bound{Min: orb.Point{-1, 3}, Max: orb.Point{3, 4}},
			input: orb.Ring{
				{1, 1}, {2, 1}, {2, 2}, {1, 2}, {1, 1},
			},
			output: orb.Ring{},
		},
		{
			name:  "bound in lower left",
			bound: orb.Bound{Min: orb.Point{-1, -1}, Max: orb.Point{0, 0}},
			input: orb.Ring{
				{1, 1}, {2, 1}, {2, 2}, {1, 2}, {1, 1},
			},
			output: orb.Ring{},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result := Ring(tc.bound, tc.input)

			if !result.Equal(tc.output) {
				t.Errorf("not equal")
				t.Logf("%v", result)
				t.Logf("%v", tc.output)
			}
		})
	}
}

func TestMultiLineString(t *testing.T) {
	bound := orb.Bound{Min: orb.Point{0, 0}, Max: orb.Point{2, 2}}
	cases := []struct {
		name   string
		open   bool
		input  orb.MultiLineString
		output orb.MultiLineString
	}{
		{
			name: "regular closed bound clip",
			input: orb.MultiLineString{
				{{1, 1}, {2, 1}, {2, 2}, {3, 3}},
			},
			output: orb.MultiLineString{
				{{1, 1}, {2, 1}, {2, 2}, {2, 2}},
			},
		},
		{
			name: "open bound clip",
			open: true,
			input: orb.MultiLineString{
				{{1, 1}, {2, 1}, {2, 2}, {3, 3}},
			},
			output: orb.MultiLineString{
				{{1, 1}, {2, 1}},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result := MultiLineString(bound, tc.input, OpenBound(tc.open))

			if !result.Equal(tc.output) {
				t.Errorf("not equal")
				t.Logf("%v", result)
				t.Logf("%v", tc.output)
			}
		})
	}

}

func TestBound(t *testing.T) {
	cases := []struct {
		name string
		b1   orb.Bound
		b2   orb.Bound
		rs   orb.Bound
	}{
		{
			name: "normal intersection",
			b1:   orb.Bound{Min: orb.Point{0, 1}, Max: orb.Point{3, 4}},
			b2:   orb.Bound{Min: orb.Point{1, 2}, Max: orb.Point{4, 5}},
			rs:   orb.Bound{Min: orb.Point{1, 2}, Max: orb.Point{3, 4}},
		},
		{
			name: "1 contains 2",
			b1:   orb.Bound{Min: orb.Point{0, 1}, Max: orb.Point{3, 4}},
			b2:   orb.Bound{Min: orb.Point{1, 2}, Max: orb.Point{2, 3}},
			rs:   orb.Bound{Min: orb.Point{1, 2}, Max: orb.Point{2, 3}},
		},
		{
			name: "no overlap",
			b1:   orb.Bound{Min: orb.Point{0, 1}, Max: orb.Point{3, 4}},
			b2:   orb.Bound{Min: orb.Point{4, 5}, Max: orb.Point{5, 6}},
			rs:   orb.Bound{Min: orb.Point{1, 1}, Max: orb.Point{0, 0}}, // empty
		},
		{
			name: "same bound",
			b1:   orb.Bound{Min: orb.Point{0, 1}, Max: orb.Point{3, 4}},
			b2:   orb.Bound{Min: orb.Point{0, 1}, Max: orb.Point{3, 4}},
			rs:   orb.Bound{Min: orb.Point{0, 1}, Max: orb.Point{3, 4}},
		},
		{
			name: "1 is empty",
			b1:   orb.Bound{Min: orb.Point{1, 1}, Max: orb.Point{0, 0}},
			b2:   orb.Bound{Min: orb.Point{0, 1}, Max: orb.Point{3, 4}},
			rs:   orb.Bound{Min: orb.Point{0, 1}, Max: orb.Point{3, 4}},
		},
		{
			name: "both are empty",
			b1:   orb.Bound{Min: orb.Point{1, 1}, Max: orb.Point{0, 0}},
			b2:   orb.Bound{Min: orb.Point{1, 1}, Max: orb.Point{0, 0}},
			rs:   orb.Bound{Min: orb.Point{1, 1}, Max: orb.Point{0, 0}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r1 := Bound(tc.b1, tc.b2)
			r2 := Bound(tc.b1, tc.b2)

			if tc.rs.IsEmpty() && (!r1.IsEmpty() || !r2.IsEmpty()) {
				t.Errorf("should be empty")
				t.Logf("%v", r1)
				t.Logf("%v", r2)
			}

			if !tc.rs.IsEmpty() {
				if !r1.Equal(tc.rs) {
					t.Errorf("r1 not equal")
					t.Logf("%v", r1)
					t.Logf("%v", tc.rs)
				}
				if !r2.Equal(tc.rs) {
					t.Errorf("r2 not equal")
					t.Logf("%v", r2)
					t.Logf("%v", tc.rs)
				}
			}
		})
	}
}
