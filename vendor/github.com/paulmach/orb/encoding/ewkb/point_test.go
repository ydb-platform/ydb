package ewkb

import (
	"testing"

	"github.com/paulmach/orb"
	"github.com/paulmach/orb/encoding/internal/wkbcommon"
)

func TestPoint(t *testing.T) {
	cases := []struct {
		name     string
		data     []byte
		srid     int
		expected orb.Point
	}{
		{
			name:     "point",
			data:     MustDecodeHex("0101000020E6100000000000000000F03F0000000000000040"),
			srid:     4326,
			expected: orb.Point{1, 2},
		},
		{
			name:     "zero point",
			data:     MustDecodeHex("01010000200400000000000000000000000000000000000000"),
			srid:     4,
			expected: orb.Point{0, 0},
		},
		{
			name:     "srid 4269",
			data:     MustDecodeHex("0101000020AD1000000000000000C05EC00000000000004140"),
			srid:     4269,
			expected: orb.Point{-123, 34},
		},
		{
			name:     "srid 4267",
			data:     MustDecodeHex("0101000020AB1000000000000000C05EC00000000000004140"),
			srid:     4267,
			expected: orb.Point{-123, 34},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			compare(t, tc.expected, tc.srid, tc.data)
		})
	}
}

func TestPointToHex(t *testing.T) {
	cases := []struct {
		name     string
		srid     int
		data     orb.Point
		expected string
	}{
		{
			name:     "point",
			srid:     4326,
			data:     orb.Point{1, 2},
			expected: "0101000020e6100000000000000000f03f0000000000000040",
		},
		{
			name:     "zero point",
			srid:     4,
			data:     orb.Point{0, 0},
			expected: "01010000200400000000000000000000000000000000000000",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := MustMarshalToHex(tc.data, tc.srid)
			if s != tc.expected {
				t.Errorf("incorrect hex: %v", s)
			}
		})
	}
}

func TestMultiPoint(t *testing.T) {
	large := orb.MultiPoint{}
	for i := 0; i < wkbcommon.MaxPointsAlloc+100; i++ {
		large = append(large, orb.Point{float64(i), float64(-i)})
	}

	cases := []struct {
		name     string
		data     []byte
		srid     int
		expected orb.MultiPoint
	}{
		{
			name:     "large multi point",
			data:     MustMarshal(large, 4326),
			srid:     4326,
			expected: large,
		},
		{
			name:     "one point",
			data:     MustDecodeHex("0104000020e6100000010000000101000000000000000000f03f0000000000000040"),
			srid:     4326,
			expected: orb.MultiPoint{{1, 2}},
		},
		{
			name:     "two points",
			data:     MustDecodeHex("0104000020e6100000020000000101000000000000000000f03f0000000000000040010100000000000000000008400000000000001040"),
			srid:     4326,
			expected: orb.MultiPoint{{1, 2}, {3, 4}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			compare(t, tc.expected, tc.srid, tc.data)
		})
	}
}
