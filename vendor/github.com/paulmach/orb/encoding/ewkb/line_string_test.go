package ewkb

import (
	"testing"

	"github.com/paulmach/orb"
	"github.com/paulmach/orb/encoding/internal/wkbcommon"
)

func TestLineString(t *testing.T) {
	large := orb.LineString{}
	for i := 0; i < wkbcommon.MaxPointsAlloc+100; i++ {
		large = append(large, orb.Point{float64(i), float64(-i)})
	}

	cases := []struct {
		name     string
		data     []byte
		srid     int
		expected orb.LineString
	}{
		{
			name:     "large line string",
			data:     MustMarshal(large, 4326),
			srid:     4326,
			expected: large,
		},
		{
			name:     "line string",
			data:     MustDecodeHex("0102000020E610000002000000CDCCCCCCCC0C5FC00000000000004540713D0AD7A3005EC01F85EB51B8FE4440"),
			srid:     4326,
			expected: orb.LineString{{-124.2, 42}, {-120.01, 41.99}},
		},
		{
			name:     "another line string",
			data:     MustDecodeHex("0020000002000010e6000000023ff0000000000000400000000000000040080000000000004010000000000000"),
			srid:     4326,
			expected: orb.LineString{{1, 2}, {3, 4}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			compare(t, tc.expected, tc.srid, tc.data)
		})
	}
}

func TestMultiLineString(t *testing.T) {
	large := orb.MultiLineString{}
	for i := 0; i < wkbcommon.MaxMultiAlloc+100; i++ {
		large = append(large, orb.LineString{})
	}

	cases := []struct {
		name     string
		data     []byte
		srid     int
		expected orb.MultiLineString
	}{
		{
			name:     "large",
			srid:     4326,
			data:     MustMarshal(large, 4326),
			expected: large,
		},
		{
			name:     "one string",
			data:     MustDecodeHex("0105000020e610000001000000010200000002000000000000000000f03f000000000000004000000000000008400000000000001040"),
			srid:     4326,
			expected: orb.MultiLineString{{{1, 2}, {3, 4}}},
		},
		{
			name:     "two strings",
			data:     MustDecodeHex("0020000005000010e6000000020000000002000000023ff000000000000040000000000000004008000000000000401000000000000000000000020000000240140000000000004018000000000000401c0000000000004020000000000000"),
			srid:     4326,
			expected: orb.MultiLineString{{{1, 2}, {3, 4}}, {{5, 6}, {7, 8}}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			compare(t, tc.expected, tc.srid, tc.data)
		})
	}
}
