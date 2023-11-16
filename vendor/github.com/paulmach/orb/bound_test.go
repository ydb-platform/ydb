package orb

import (
	"testing"
)

func TestBoundExtend(t *testing.T) {
	bound := Bound{Min: Point{0, 0}, Max: Point{3, 5}}

	if r := bound.Extend(Point{2, 1}); !r.Equal(bound) {
		t.Errorf("extend incorrect: %v != %v", r, bound)
	}

	answer := Bound{Min: Point{0, -1}, Max: Point{6, 5}}
	if r := bound.Extend(Point{6, -1}); !r.Equal(answer) {
		t.Errorf("extend incorrect: %v != %v", r, answer)
	}
}

func TestBoundUnion(t *testing.T) {
	b1 := Bound{Min: Point{0, 0}, Max: Point{1, 1}}
	b2 := Bound{Min: Point{0, 0}, Max: Point{2, 2}}

	expected := Bound{Min: Point{0, 0}, Max: Point{2, 2}}
	if b := b1.Union(b2); !b.Equal(expected) {
		t.Errorf("union incorrect: %v != %v", b, expected)
	}

	if b := b2.Union(b1); !b.Equal(expected) {
		t.Errorf("union incorrect: %v != %v", b, expected)
	}
}

func TestBoundContains(t *testing.T) {
	bound := Bound{Min: Point{-2, -1}, Max: Point{2, 1}}

	cases := []struct {
		name   string
		point  Point
		result bool
	}{
		{
			name:   "middle",
			point:  Point{0, 0},
			result: true,
		},
		{
			name:   "left border",
			point:  Point{-1, 0},
			result: true,
		},
		{
			name:   "ne corner",
			point:  Point{2, 1},
			result: true,
		},
		{
			name:   "above",
			point:  Point{0, 3},
			result: false,
		},
		{
			name:   "below",
			point:  Point{0, -3},
			result: false,
		},
		{
			name:   "left",
			point:  Point{-3, 0},
			result: false,
		},
		{
			name:   "right",
			point:  Point{3, 0},
			result: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v := bound.Contains(tc.point)
			if v != tc.result {
				t.Errorf("incorrect contains: %v != %v", v, tc.result)
			}
		})
	}
}

func TestBoundIntersects(t *testing.T) {
	bound := Bound{Min: Point{0, 2}, Max: Point{1, 3}}

	cases := []struct {
		name   string
		bound  Bound
		result bool
	}{
		{
			name:   "outside, top right",
			bound:  Bound{Min: Point{5, 7}, Max: Point{6, 8}},
			result: false,
		},
		{
			name:   "outside, top left",
			bound:  Bound{Min: Point{-6, 7}, Max: Point{-5, 8}},
			result: false,
		},
		{
			name:   "outside, above",
			bound:  Bound{Min: Point{0, 7}, Max: Point{0.5, 8}},
			result: false,
		},
		{
			name:   "over the middle",
			bound:  Bound{Min: Point{0, 0.5}, Max: Point{1, 4}},
			result: true,
		},
		{
			name:   "over the left",
			bound:  Bound{Min: Point{-1, 2}, Max: Point{1, 4}},
			result: true,
		},
		{
			name:   "completely inside",
			bound:  Bound{Min: Point{0.3, 2.3}, Max: Point{0.6, 2.6}},
			result: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v := bound.Intersects(tc.bound)
			if v != tc.result {
				t.Errorf("incorrect result: %v != %v", v, tc.result)
			}
		})
	}

	a := Bound{Min: Point{7, 6}, Max: Point{8, 7}}
	b := Bound{Min: Point{6.1, 6.1}, Max: Point{8.1, 8.1}}

	if !a.Intersects(b) || !b.Intersects(a) {
		t.Errorf("expected to intersect")
	}

	a = Bound{Min: Point{1, 2}, Max: Point{4, 3}}
	b = Bound{Min: Point{2, 1}, Max: Point{3, 4}}

	if !a.Intersects(b) || !b.Intersects(a) {
		t.Errorf("expected to intersect")
	}
}

func TestBoundIsEmpty(t *testing.T) {
	cases := []struct {
		name   string
		bound  Bound
		result bool
	}{
		{
			name:   "regular bound",
			bound:  Bound{Min: Point{5, 7}, Max: Point{6, 8}},
			result: false,
		},
		{
			name:   "single point",
			bound:  Bound{Min: Point{5, 7}, Max: Point{6, 8}},
			result: false,
		},
		{
			name:   "horizontal bar",
			bound:  Bound{Min: Point{5, 7}, Max: Point{6, 8}},
			result: false,
		},
		{
			name:   "vertical bar",
			bound:  Bound{Min: Point{5, 7}, Max: Point{6, 8}},
			result: false,
		},
		{
			name:   "vertical bar",
			bound:  Bound{Min: Point{5, 7}, Max: Point{6, 8}},
			result: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v := tc.bound.IsEmpty()
			if v != tc.result {
				t.Errorf("incorrect result: %v != %v", v, tc.result)
			}

		})
	}

	// negative/malformed area
	bound := Bound{Min: Point{1, 1}, Max: Point{0, 2}}
	if !bound.IsEmpty() {
		t.Error("expected true, got false")
	}

	// negative/malformed area
	bound = Bound{Min: Point{1, 3}, Max: Point{1, 2}}
	if !bound.IsEmpty() {
		t.Error("expected true, got false")
	}
}

func TestBoundPad(t *testing.T) {
	bound := Bound{Min: Point{1, 1}, Max: Point{2, 2}}

	bound = bound.Pad(0.5)
	if !bound.Min.Equal(Point{0.5, 0.5}) {
		t.Errorf("incorrect min: %v", bound.Min)
	}

	if !bound.Max.Equal(Point{2.5, 2.5}) {
		t.Errorf("incorrect min: %v", bound.Max)
	}
}

func TestBoundCenter(t *testing.T) {
	bound := Bound{Min: Point{1, 1}, Max: Point{2, 2}}

	if c := bound.Center(); !c.Equal(Point{1.5, 1.5}) {
		t.Errorf("incorrect center: %v", c)
	}
}

func TestBoundIsZero(t *testing.T) {
	bound := Bound{Min: Point{1, 2}, Max: Point{1, 2}}
	if bound.IsZero() {
		t.Error("expected false, got true")
	}

	bound = Bound{}
	if !bound.IsZero() {
		t.Error("expected true, got false")
	}
}

func TestBoundToRing(t *testing.T) {
	bound := Bound{Min: Point{1, 1}, Max: Point{2, 2}}

	if bound.ToRing().Orientation() != CCW {
		t.Errorf("orientation should be ccw")
	}
}

func TestBoundToPolygon(t *testing.T) {
	bound := Bound{Min: Point{1, 1}, Max: Point{2, 2}}

	if bound.ToPolygon()[0].Orientation() != CCW {
		t.Errorf("orientation should be ccw")
	}
}
