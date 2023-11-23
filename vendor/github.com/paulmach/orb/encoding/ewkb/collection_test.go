package ewkb

import (
	"testing"

	"github.com/paulmach/orb"
	"github.com/paulmach/orb/encoding/internal/wkbcommon"
)

func TestCollection(t *testing.T) {
	large := orb.Collection{}
	for i := 0; i < wkbcommon.MaxMultiAlloc+100; i++ {
		large = append(large, orb.Point{float64(i), float64(-i)})
	}

	cases := []struct {
		name     string
		srid     int
		data     []byte
		expected orb.Collection
	}{
		{
			name:     "large",
			srid:     123,
			data:     MustMarshal(large, 123),
			expected: large,
		},
		{
			name:     "collection with point",
			data:     MustDecodeHex("0107000020e6100000010000000101000000000000000000f03f0000000000000040"),
			srid:     4326,
			expected: orb.Collection{orb.Point{1, 2}},
		},
		{
			name: "collection with point and line",
			data: MustDecodeHex("0020000007000010e60000000200000000013ff000000000000040000000000000000000000002000000023ff0000000000000400000000000000040080000000000004010000000000000"),
			srid: 4326,
			expected: orb.Collection{
				orb.Point{1, 2},
				orb.LineString{{1, 2}, {3, 4}},
			},
		},
		{
			name: "collection with point and line and polygon",
			data: MustDecodeHex("0107000020e6100000030000000101000000000000000000f03f0000000000000040010200000002000000000000000000f03f00000000000000400000000000000840000000000000104001030000000300000004000000000000000000f03f00000000000000400000000000000840000000000000104000000000000014400000000000001840000000000000f03f000000000000004004000000000000000000264000000000000028400000000000002a400000000000002c400000000000002e4000000000000030400000000000002640000000000000284004000000000000000000354000000000000036400000000000003740000000000000384000000000000039400000000000003a4000000000000035400000000000003640"),
			srid: 4326,
			expected: orb.Collection{
				orb.Point{1, 2},
				orb.LineString{{1, 2}, {3, 4}},
				orb.Polygon{
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
