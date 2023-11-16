package ewkb

import (
	"testing"

	"github.com/paulmach/orb"
	"github.com/paulmach/orb/encoding/internal/wkbcommon"
)

func TestPolygon(t *testing.T) {
	large := orb.Polygon{}
	for i := 0; i < wkbcommon.MaxMultiAlloc+100; i++ {
		large = append(large, orb.Ring{})
	}

	cases := []struct {
		name     string
		data     []byte
		srid     int
		expected orb.Polygon
	}{
		{
			name:     "large",
			data:     MustMarshal(large, 4326),
			srid:     4326,
			expected: large,
		},
		{
			name:     "srid 4326",
			data:     MustDecodeHex("0103000020E61000000100000005000000000000000000000000000000000000000000000000000000000000000000F03F000000000000F03F000000000000F03F000000000000F03F000000000000000000000000000000000000000000000000"),
			srid:     4326,
			expected: orb.Polygon{{{0, 0}, {0, 1}, {1, 1}, {1, 0}, {0, 0}}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			compare(t, tc.expected, tc.srid, tc.data)
		})
	}
}

func TestMultiPolygon(t *testing.T) {
	large := orb.MultiPolygon{}
	for i := 0; i < wkbcommon.MaxMultiAlloc+100; i++ {
		large = append(large, orb.Polygon{})
	}

	cases := []struct {
		name     string
		data     []byte
		srid     int
		expected orb.MultiPolygon
	}{
		{
			name:     "large",
			srid:     4326,
			data:     MustMarshal(large, 4326),
			expected: large,
		},
		{
			name:     "srid 3426",
			data:     MustDecodeHex("0106000020620D00000100000001030000000100000004000000000000000000000000000000000000000000000000000000000000000000F03F0000000000000040000000000000004000000000000000000000000000000000"),
			srid:     3426,
			expected: orb.MultiPolygon{{{{0, 0}, {0, 1}, {2, 2}, {0, 0}}}},
		},
		{
			name:     "another polygon",
			data:     MustDecodeHex("0106000020e61000000100000001030000000100000004000000000000000000f03f00000000000000400000000000000840000000000000104000000000000014400000000000001840000000000000f03f0000000000000040"),
			srid:     4326,
			expected: orb.MultiPolygon{{{{1, 2}, {3, 4}, {5, 6}, {1, 2}}}},
		},
		{
			name: "another polygon",
			data: MustDecodeHex("0020000006000010e600000002000000000300000001000000043ff0000000000000400000000000000040080000000000004010000000000000401400000000000040180000000000003ff00000000000004000000000000000000000000300000003000000043ff0000000000000400000000000000040080000000000004010000000000000401400000000000040180000000000003ff000000000000040000000000000000000000440260000000000004028000000000000402a000000000000402c000000000000402e0000000000004030000000000000402600000000000040280000000000000000000440350000000000004036000000000000403700000000000040380000000000004039000000000000403a00000000000040350000000000004036000000000000"),
			srid: 4326,
			expected: orb.MultiPolygon{
				{{{1, 2}, {3, 4}, {5, 6}, {1, 2}}},
				{
					{{1, 2}, {3, 4}, {5, 6}, {1, 2}},
					{{11, 12}, {13, 14}, {15, 16}, {11, 12}},
					{{21, 22}, {23, 24}, {25, 26}, {21, 22}},
				},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			compare(t, tc.expected, tc.srid, tc.data)
		})
	}
}
