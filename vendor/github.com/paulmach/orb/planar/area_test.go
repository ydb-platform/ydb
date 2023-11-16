package planar

import (
	"testing"

	"github.com/paulmach/orb"
)

func TestCentroidArea(t *testing.T) {
	for _, g := range orb.AllGeometries {
		CentroidArea(g)
	}
}

func TestCentroidArea_MultiPoint(t *testing.T) {
	mp := orb.MultiPoint{{0, 0}, {1, 1.5}, {2, 0}}

	centroid, area := CentroidArea(mp)
	expected := orb.Point{1, 0.5}
	if !centroid.Equal(expected) {
		t.Errorf("incorrect centroid: %v != %v", centroid, expected)
	}

	if area != 0 {
		t.Errorf("area should be 0: %f", area)
	}
}

func TestCentroidArea_LineString(t *testing.T) {
	cases := []struct {
		name   string
		ls     orb.LineString
		result orb.Point
	}{
		{
			name:   "simple",
			ls:     orb.LineString{{0, 0}, {3, 4}},
			result: orb.Point{1.5, 2},
		},
		{
			name:   "empty line",
			ls:     orb.LineString{},
			result: orb.Point{0, 0},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if c, _ := CentroidArea(tc.ls); !c.Equal(tc.result) {
				t.Errorf("wrong centroid: %v != %v", c, tc.result)
			}
		})
	}
}

func TestCentroidArea_MultiLineString(t *testing.T) {
	cases := []struct {
		name   string
		ls     orb.MultiLineString
		result orb.Point
	}{
		{
			name:   "simple",
			ls:     orb.MultiLineString{{{0, 0}, {3, 4}}},
			result: orb.Point{1.5, 2},
		},
		{
			name:   "two lines",
			ls:     orb.MultiLineString{{{0, 0}, {0, 1}}, {{1, 0}, {1, 1}}},
			result: orb.Point{0.5, 0.5},
		},
		{
			name:   "multiple empty lines",
			ls:     orb.MultiLineString{{{1, 0}}, {{2, 1}}},
			result: orb.Point{1.5, 0.5},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if c, _ := CentroidArea(tc.ls); !c.Equal(tc.result) {
				t.Errorf("wrong centroid: %v != %v", c, tc.result)
			}
		})
	}
}

func TestCentroid_Ring(t *testing.T) {
	cases := []struct {
		name   string
		ring   orb.Ring
		result orb.Point
	}{
		{
			name:   "triangle, cw",
			ring:   orb.Ring{{0, 0}, {1, 3}, {2, 0}, {0, 0}},
			result: orb.Point{1, 1},
		},
		{
			name:   "triangle, ccw",
			ring:   orb.Ring{{0, 0}, {2, 0}, {1, 3}, {0, 0}},
			result: orb.Point{1, 1},
		},
		{
			name:   "square, cw",
			ring:   orb.Ring{{0, 0}, {0, 1}, {1, 1}, {1, 0}, {0, 0}},
			result: orb.Point{0.5, 0.5},
		},
		{
			name:   "non-closed square, cw",
			ring:   orb.Ring{{0, 0}, {0, 1}, {1, 1}, {1, 0}},
			result: orb.Point{0.5, 0.5},
		},
		{
			name:   "triangle, ccw",
			ring:   orb.Ring{{0, 0}, {1, 0}, {1, 1}, {0, 1}, {0, 0}},
			result: orb.Point{0.5, 0.5},
		},
		{
			name:   "redudent points",
			ring:   orb.Ring{{0, 0}, {1, 0}, {2, 0}, {1, 3}, {0, 0}},
			result: orb.Point{1, 1},
		},
		{
			name: "3 points",
			ring: orb.Ring{{0, 0}, {1, 0}, {0, 0}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if c, _ := CentroidArea(tc.ring); !c.Equal(tc.result) {
				t.Errorf("wrong centroid: %v != %v", c, tc.result)
			}

			// check that is recenters to deal with roundoff
			for i := range tc.ring {
				tc.ring[i][0] += 1e8
				tc.ring[i][1] -= 1e8
			}

			tc.result[0] += 1e8
			tc.result[1] -= 1e8

			if c, _ := CentroidArea(tc.ring); !c.Equal(tc.result) {
				t.Errorf("wrong centroid: %v != %v", c, tc.result)
			}
		})
	}
}

func TestArea_Ring(t *testing.T) {
	cases := []struct {
		name   string
		ring   orb.Ring
		result float64
	}{
		{
			name:   "simple box, ccw",
			ring:   orb.Ring{{0, 0}, {1, 0}, {1, 1}, {0, 1}, {0, 0}},
			result: 1,
		},
		{
			name:   "simple box, cc",
			ring:   orb.Ring{{0, 0}, {0, 1}, {1, 1}, {1, 0}, {0, 0}},
			result: -1,
		},
		{
			name:   "even number of points",
			ring:   orb.Ring{{0, 0}, {1, 0}, {1, 1}, {0.4, 1}, {0, 1}, {0, 0}},
			result: 1,
		},
		{
			name:   "3 points",
			ring:   orb.Ring{{0, 0}, {1, 0}, {0, 0}},
			result: 0.0,
		},
		{
			name:   "4 points",
			ring:   orb.Ring{{0, 0}, {1, 0}, {1, 1}, {0, 0}},
			result: 0.5,
		},
		{
			name:   "6 points",
			ring:   orb.Ring{{1, 1}, {2, 1}, {2, 1.5}, {2, 2}, {1, 2}, {1, 1}},
			result: 1.0,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, val := CentroidArea(tc.ring)
			if val != tc.result {
				t.Errorf("wrong area: %v != %v", val, tc.result)
			}

			// check that is recenters to deal with roundoff
			for i := range tc.ring {
				tc.ring[i][0] += 1e15
				tc.ring[i][1] -= 1e15
			}

			_, val = CentroidArea(tc.ring)
			if val != tc.result {
				t.Errorf("wrong area: %v != %v", val, tc.result)
			}

			// check that are rendant last point is implicit
			tc.ring = tc.ring[:len(tc.ring)-1]
			_, val = CentroidArea(tc.ring)
			if val != tc.result {
				t.Errorf("wrong area: %v != %v", val, tc.result)
			}
		})
	}
}

func TestCentroid_RingAdv(t *testing.T) {
	ring := orb.Ring{{0, 0}, {0, 1}, {1, 1}, {1, 0.5}, {2, 0.5}, {2, 1}, {3, 1}, {3, 0}, {0, 0}}

	// +-+ +-+
	// | | | |
	// | +-+ |
	// |     |
	// +-----+

	centroid, area := CentroidArea(ring)

	expected := orb.Point{1.5, 0.45}
	if !centroid.Equal(expected) {
		t.Errorf("incorrect centroid: %v != %v", centroid, expected)
	}

	if area != -2.5 {
		t.Errorf("incorrect area: %v != 2.5", area)
	}
}

func TestCentroidArea_Polygon(t *testing.T) {
	t.Run("polygon with hole", func(t *testing.T) {
		r1 := orb.Ring{{0, 0}, {4, 0}, {4, 3}, {0, 3}, {0, 0}}
		r1.Reverse()

		r2 := orb.Ring{{2, 1}, {3, 1}, {3, 2}, {2, 2}, {2, 1}}
		poly := orb.Polygon{r1, r2}

		centroid, area := CentroidArea(poly)
		if !centroid.Equal(orb.Point{21.5 / 11.0, 1.5}) {
			t.Errorf("%v", 21.5/11.0)
			t.Errorf("incorrect centroid: %v", centroid)
		}

		if area != 11 {
			t.Errorf("incorrect area: %v != 11", area)
		}
	})

	t.Run("collapsed", func(t *testing.T) {
		e := orb.Point{0.5, 1}
		c, _ := CentroidArea(orb.Polygon{{{0, 1}, {1, 1}, {0, 1}}})
		if !c.Equal(e) {
			t.Errorf("incorrect point: %v != %v", c, e)
		}
	})

	t.Run("empty right half", func(t *testing.T) {
		poly := orb.Polygon{
			{{0, 0}, {4, 0}, {4, 4}, {0, 4}, {0, 0}},
			{{2, 0}, {2, 4}, {4, 4}, {4, 0}, {2, 0}},
		}

		centroid, area := CentroidArea(poly)
		if v := (orb.Point{1, 2}); !centroid.Equal(v) {
			t.Errorf("incorrect centroid: %v != %v", centroid, v)
		}

		if area != 8 {
			t.Errorf("incorrect area: %v != 8", area)
		}
	})
}

func TestCentroidArea_Bound(t *testing.T) {
	b := orb.Bound{Min: orb.Point{0, 2}, Max: orb.Point{1, 3}}
	centroid, area := CentroidArea(b)

	expected := orb.Point{0.5, 2.5}
	if !centroid.Equal(expected) {
		t.Errorf("incorrect centroid: %v != %v", centroid, expected)
	}

	if area != 1 {
		t.Errorf("incorrect area: %f != 1", area)
	}

	b = orb.Bound{Min: orb.Point{0, 2}, Max: orb.Point{0, 2}}
	centroid, area = CentroidArea(b)

	expected = orb.Point{0, 2}
	if !centroid.Equal(expected) {
		t.Errorf("incorrect centroid: %v != %v", centroid, expected)
	}

	if area != 0 {
		t.Errorf("area should be zero: %f", area)
	}
}
