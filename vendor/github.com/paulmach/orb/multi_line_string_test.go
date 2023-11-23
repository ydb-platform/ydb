package orb

import "testing"

func TestMultiLineString_Bound(t *testing.T) {
	// should be union of line strings
	mls := MultiLineString{
		{{0, 0}, {0, 2}, {2, 2}, {2, 0}, {0, 0}},
		{{1, 1}, {1, 3}, {3, 3}, {3, 1}, {1, 1}},
	}

	b := mls.Bound()
	if !b.Equal(Bound{Min: Point{0, 0}, Max: Point{3, 3}}) {
		t.Errorf("incorrect bound: %v", b)
	}
}

func TestMultiLineString_Equal(t *testing.T) {
	cases := []struct {
		name     string
		mls1     MultiLineString
		mls2     MultiLineString
		expected bool
	}{
		{
			name: "same multi line string",
			mls1: MultiLineString{
				{{0, 0}, {0, 2}, {2, 2}, {2, 0}, {0, 0}},
				{{1, 1}, {1, 3}, {3, 3}, {3, 1}, {1, 1}},
			},
			mls2: MultiLineString{
				{{0, 0}, {0, 2}, {2, 2}, {2, 0}, {0, 0}},
				{{1, 1}, {1, 3}, {3, 3}, {3, 1}, {1, 1}},
			},
			expected: true,
		},
		{
			name: "different number or line strings",
			mls1: MultiLineString{
				{{0, 0}, {0, 2}, {2, 2}, {2, 0}, {0, 0}},
				{{1, 1}, {1, 3}, {3, 3}, {3, 1}, {1, 1}},
			},
			mls2: MultiLineString{
				{{0, 0}, {0, 2}, {2, 2}, {2, 0}, {0, 0}},
			},
			expected: false,
		},
		{
			name: "second line is different",
			mls1: MultiLineString{
				{{0, 0}, {0, 2}, {2, 2}, {2, 0}, {0, 0}},
				{{1, 1}, {1, 3}, {3, 3}, {3, 1}, {1, 1}},
			},
			mls2: MultiLineString{
				{{0, 0}, {0, 2}, {2, 2}, {2, 0}, {0, 0}},
				{{1, 1}, {1, 2}, {2, 2}, {2, 1}, {1, 1}},
			},
			expected: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if v := tc.mls1.Equal(tc.mls2); v != tc.expected {
				t.Errorf("mls1 != mls2: %v != %v", v, tc.expected)
			}

			if v := tc.mls2.Equal(tc.mls1); v != tc.expected {
				t.Errorf("mls2 != mls1: %v != %v", v, tc.expected)
			}
		})
	}
}

func TestMultiLineString_Clone(t *testing.T) {
	cases := []struct {
		name     string
		mls      MultiLineString
		expected MultiLineString
	}{
		{
			name: "normal multi line string",
			mls: MultiLineString{
				{{0, 0}, {0, 2}, {2, 2}, {2, 0}, {0, 0}},
				{{1, 1}, {1, 3}, {3, 3}, {3, 1}, {1, 1}},
			},
			expected: MultiLineString{
				{{0, 0}, {0, 2}, {2, 2}, {2, 0}, {0, 0}},
				{{1, 1}, {1, 3}, {3, 3}, {3, 1}, {1, 1}},
			},
		},
		{
			name:     "nil should return nil",
			mls:      nil,
			expected: nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := tc.mls.Clone()
			if !c.Equal(tc.expected) {
				t.Errorf("not cloned correctly: %v", c)
			}
		})
	}
}
