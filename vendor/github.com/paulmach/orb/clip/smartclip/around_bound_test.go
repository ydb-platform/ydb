package smartclip

import (
	"reflect"
	"testing"

	"github.com/paulmach/orb"
)

func TestNexts(t *testing.T) {
	for i, next := range nexts[orb.CW] {
		if next == -1 {
			continue
		}

		if i != nexts[orb.CCW][next] {
			t.Errorf("incorrect %d: %d != %d", i, i, nexts[orb.CCW][next])
		}
	}
}

func TestAroundBound(t *testing.T) {
	cases := []struct {
		name     string
		box      orb.Bound
		input    orb.Ring
		output   orb.Ring
		expected orb.Orientation
	}{
		{
			name:     "simple ccw",
			box:      orb.Bound{Min: orb.Point{-1, -1}, Max: orb.Point{1, 1}},
			input:    orb.Ring{{-1, -1}, {1, 1}},
			output:   orb.Ring{{-1, -1}, {1, 1}, {0, 1}, {-1, 1}, {-1, 0}, {-1, -1}},
			expected: orb.CCW,
		},
		{
			name:     "simple cw",
			box:      orb.Bound{Min: orb.Point{-1, -1}, Max: orb.Point{1, 1}},
			input:    orb.Ring{{-1, -1}, {1, 1}},
			output:   orb.Ring{{-1, -1}, {1, 1}, {1, 0}, {1, -1}, {0, -1}, {-1, -1}},
			expected: orb.CW,
		},
		{
			name:     "wrap edge around whole box ccw",
			box:      orb.Bound{Min: orb.Point{1, 1}, Max: orb.Point{6, 6}},
			input:    orb.Ring{{1, 3}, {1, 2}},
			output:   orb.Ring{{1, 3}, {1, 2}, {1, 1}, {3.5, 1}, {6, 1}, {6, 3.5}, {6, 6}, {3.5, 6}, {1, 6}, {1, 3}},
			expected: orb.CCW,
		},
		{
			name:     "wrap around whole box ccw",
			box:      orb.Bound{Min: orb.Point{-1, -1}, Max: orb.Point{1, 1}},
			input:    orb.Ring{{-1, 0.5}, {0, 0.5}, {0, -0.5}, {-1, -0.5}},
			output:   orb.Ring{{-1, 0.5}, {0, 0.5}, {0, -0.5}, {-1, -0.5}, {-1, -1}, {0, -1}, {1, -1}, {1, 0}, {1, 1}, {0, 1}, {-1, 1}, {-1, 0.5}},
			expected: orb.CCW,
		},
		{
			name:     "wrap around whole box cw",
			box:      orb.Bound{Min: orb.Point{-1, -1}, Max: orb.Point{1, 1}},
			input:    orb.Ring{{-1, -0.5}, {0, -0.5}, {0, 0.5}, {-1, 0.5}},
			output:   orb.Ring{{-1, -0.5}, {0, -0.5}, {0, 0.5}, {-1, 0.5}, {-1, 1}, {0, 1}, {1, 1}, {1, 0}, {1, -1}, {0, -1}, {-1, -1}, {-1, -0.5}},
			expected: orb.CW,
		},
		{
			name:     "already cw with endpoints in same section",
			box:      orb.Bound{Min: orb.Point{-1, -1}, Max: orb.Point{1, 1}},
			input:    orb.Ring{{-1, 0.5}, {0, 0.5}, {0, -0.5}, {-1, -0.5}},
			output:   orb.Ring{{-1, 0.5}, {0, 0.5}, {0, -0.5}, {-1, -0.5}, {-1, 0.5}},
			expected: orb.CW,
		},
		{
			name:     "cw but want ccw with endpoints in same section",
			box:      orb.Bound{Min: orb.Point{-1, -1}, Max: orb.Point{1, 1}},
			input:    orb.Ring{{-1, 0.5}, {0, 0.5}, {0, -0.5}, {-1, -0.5}},
			output:   orb.Ring{{-1, 0.5}, {0, 0.5}, {0, -0.5}, {-1, -0.5}, {-1, -1}, {0, -1}, {1, -1}, {1, 0}, {1, 1}, {0, 1}, {-1, 1}, {-1, 0.5}},
			expected: orb.CCW,
		},
		{
			name:     "one point on edge ccw",
			box:      orb.Bound{Min: orb.Point{-1, -1}, Max: orb.Point{1, 1}},
			input:    orb.Ring{{-1, 0.0}, {-0.5, -0.5}, {0, 0}, {-0.5, 0.5}, {-1, 0.0}},
			output:   orb.Ring{{-1, 0.0}, {-0.5, -0.5}, {0, 0}, {-0.5, 0.5}, {-1, 0.0}},
			expected: orb.CCW,
		},
		{
			name:     "one point on edge cw",
			box:      orb.Bound{Min: orb.Point{-1, -1}, Max: orb.Point{1, 1}},
			input:    orb.Ring{{-1, 0.0}, {-0.5, -0.5}, {0, 0}, {-0.5, 0.5}, {-1, 0.0}},
			output:   orb.Ring{{-1, 0.0}, {-0.5, -0.5}, {0, 0}, {-0.5, 0.5}, {-1, 0.0}, {-1, 1}, {0, 1}, {1, 1}, {1, 0}, {1, -1}, {0, -1}, {-1, -1}, {-1, 0}},
			expected: orb.CW,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out := aroundBound(tc.box, tc.input, tc.expected)
			if !reflect.DeepEqual(out, tc.output) {
				t.Errorf("does not match")
				t.Logf("%v", out)
				t.Logf("%v", tc.output)
			}
		})
	}
}
