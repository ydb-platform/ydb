package project

import (
	"fmt"
	"math"

	"github.com/paulmach/orb"
)

const earthRadiusPi = orb.EarthRadius * math.Pi

// Mercator performs the Spherical Pseudo-Mercator projection used by most web maps.
var Mercator = struct {
	ToWGS84 orb.Projection
}{
	ToWGS84: func(p orb.Point) orb.Point {
		return orb.Point{
			180.0 * p[0] / earthRadiusPi,
			180.0 / math.Pi * (2*math.Atan(math.Exp(p[1]/orb.EarthRadius)) - math.Pi/2.0),
		}
	},
}

// WGS84 is what common uses lon/lat projection.
var WGS84 = struct {
	// ToMercator projections from WGS to Mercator, used by most web maps
	ToMercator orb.Projection
}{
	ToMercator: func(g orb.Point) orb.Point {
		y := math.Log(math.Tan((90.0+g[1])*math.Pi/360.0)) * orb.EarthRadius
		return orb.Point{
			earthRadiusPi / 180.0 * g[0],
			math.Max(-earthRadiusPi, math.Min(y, earthRadiusPi)),
		}
	},
}

// MercatorScaleFactor returns the mercator scaling factor for a given degree latitude.
func MercatorScaleFactor(g orb.Point) float64 {
	if g[1] < -90.0 || g[1] > 90.0 {
		panic(fmt.Sprintf("orb: latitude out of range, given %f", g[1]))
	}

	return 1.0 / math.Cos(g[1]/180.0*math.Pi)
}
