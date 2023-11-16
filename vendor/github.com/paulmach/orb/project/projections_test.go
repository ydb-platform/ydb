package project

import (
	"math"
	"testing"

	"github.com/paulmach/orb"
	"github.com/paulmach/orb/internal/mercator"
)

func TestMercator(t *testing.T) {
	for _, city := range mercator.Cities {
		g := orb.Point{
			city[1],
			city[0],
		}

		p := WGS84.ToMercator(g)
		g = Mercator.ToWGS84(p)

		if math.Abs(g[1]-city[0]) > mercator.Epsilon {
			t.Errorf("latitude miss match: %f != %f", g[1], city[0])
		}

		if math.Abs(g[0]-city[1]) > mercator.Epsilon {
			t.Errorf("longitude miss match: %f != %f", g[0], city[1])
		}
	}
}

func TestMercatorScaleFactor(t *testing.T) {
	cases := []struct {
		name   string
		point  orb.Point
		factor float64
	}{
		{
			name:   "30 deg",
			point:  orb.Point{0, 30.0},
			factor: 1.154701,
		},
		{
			name:   "45 deg",
			point:  orb.Point{0, 45.0},
			factor: 1.414214,
		},
		{
			name:   "60 deg",
			point:  orb.Point{0, 60.0},
			factor: 2,
		},
		{
			name:   "80 deg",
			point:  orb.Point{0, 80.0},
			factor: 5.758770,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if f := MercatorScaleFactor(tc.point); math.Abs(tc.factor-f) > mercator.Epsilon {
				t.Errorf("incorrect factor: %v != %v", f, tc.factor)
			}
		})
	}
}
