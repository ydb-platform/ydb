package orb

import (
	"testing"
)

func TestLineStringReverse(t *testing.T) {
	t.Run("1 point line", func(t *testing.T) {
		ls := LineString{{1, 2}}
		rs := ls.Clone()
		rs.Reverse()

		if !rs.Equal(ls) {
			t.Errorf("1 point lines should be equal if reversed")
		}
	})

	cases := []struct {
		name   string
		input  LineString
		output LineString
	}{
		{
			name:   "2 point line",
			input:  LineString{{1, 2}, {3, 4}},
			output: LineString{{3, 4}, {1, 2}},
		},
		{
			name:   "3 point line",
			input:  LineString{{1, 2}, {3, 4}, {5, 6}},
			output: LineString{{5, 6}, {3, 4}, {1, 2}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reversed := tc.input
			reversed.Reverse()

			if !reversed.Equal(tc.output) {
				t.Errorf("line should be reversed: %v", reversed)
			}

			if !tc.input.Equal(reversed) {
				t.Errorf("should reverse inplace")
			}
		})
	}
}
