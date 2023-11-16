package orb

import (
	"testing"
)

func TestMultiPolygon_Bound(t *testing.T) {
	// should be union of polygons
	mp := MultiPolygon{
		{{{0, 0}, {0, 2}, {2, 2}, {2, 0}, {0, 0}}},
		{{{1, 1}, {1, 3}, {3, 3}, {3, 1}, {1, 1}}},
	}

	b := mp.Bound()
	if !b.Equal(Bound{Min: Point{0, 0}, Max: Point{3, 3}}) {
		t.Errorf("incorrect bound: %v", b)
	}
}

func TestMultiPolygon_Equal(t *testing.T) {
	cases := []struct {
		name     string
		mp1      MultiPolygon
		mp2      MultiPolygon
		expected bool
	}{
		{
			name: "same multipolygon",
			mp1: MultiPolygon{
				{{{0, 0}, {0, 2}, {2, 2}, {2, 0}, {0, 0}}},
				{{{1, 1}, {1, 3}, {3, 3}, {3, 1}, {1, 1}}},
			},
			mp2: MultiPolygon{
				{{{0, 0}, {0, 2}, {2, 2}, {2, 0}, {0, 0}}},
				{{{1, 1}, {1, 3}, {3, 3}, {3, 1}, {1, 1}}},
			},
			expected: true,
		},
		{
			name: "different number or rings",
			mp1: MultiPolygon{
				{{{0, 0}, {0, 2}, {2, 2}, {2, 0}, {0, 0}}},
				{{{1, 1}, {1, 3}, {3, 3}, {3, 1}, {1, 1}}},
			},
			mp2: MultiPolygon{
				{{{0, 0}, {0, 2}, {2, 2}, {2, 0}, {0, 0}}},
			},
			expected: false,
		},
		{
			name: "inner rings are different",
			mp1: MultiPolygon{
				{{{0, 0}, {0, 2}, {2, 2}, {2, 0}, {0, 0}}},
				{{{1, 1}, {1, 3}, {3, 3}, {3, 1}, {1, 1}}},
			},
			mp2: MultiPolygon{
				{{{0, 0}, {0, 2}, {2, 2}, {2, 0}, {0, 0}}},
				{{{1, 1}, {1, 2}, {2, 2}, {2, 1}, {1, 1}}},
			},
			expected: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if v := tc.mp1.Equal(tc.mp2); v != tc.expected {
				t.Errorf("mp1 != mp2: %v != %v", v, tc.expected)
			}

			if v := tc.mp2.Equal(tc.mp1); v != tc.expected {
				t.Errorf("mp2 != mp1: %v != %v", v, tc.expected)
			}
		})
	}
}

func TestMultiPolygon_Clone(t *testing.T) {
	cases := []struct {
		name     string
		mp       MultiPolygon
		expected MultiPolygon
	}{
		{
			name: "normal multipolygon",
			mp: MultiPolygon{
				{{{0, 0}, {0, 2}, {2, 2}, {2, 0}, {0, 0}}},
				{{{1, 1}, {1, 3}, {3, 3}, {3, 1}, {1, 1}}},
			},
			expected: MultiPolygon{
				{{{0, 0}, {0, 2}, {2, 2}, {2, 0}, {0, 0}}},
				{{{1, 1}, {1, 3}, {3, 3}, {3, 1}, {1, 1}}},
			},
		},
		{
			name:     "nil should return nil",
			mp:       nil,
			expected: nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := tc.mp.Clone()
			if !c.Equal(tc.expected) {
				t.Errorf("not cloned correctly: %v", c)
			}
		})
	}
}
