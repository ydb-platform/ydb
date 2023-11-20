package geo

import (
	"math"

	"github.com/paulmach/orb"
)

// NewBoundAroundPoint creates a new bound given a center point,
// and a distance from the center point in meters.
func NewBoundAroundPoint(center orb.Point, distance float64) orb.Bound {
	radDist := distance / orb.EarthRadius
	radLat := deg2rad(center[1])
	radLon := deg2rad(center[0])
	minLat := radLat - radDist
	maxLat := radLat + radDist

	var minLon, maxLon float64
	if minLat > minLatitude && maxLat < maxLatitude {
		deltaLon := math.Asin(math.Sin(radDist) / math.Cos(radLat))
		minLon = radLon - deltaLon
		if minLon < minLongitude {
			minLon += 2 * math.Pi
		}
		maxLon = radLon + deltaLon
		if maxLon > maxLongitude {
			maxLon -= 2 * math.Pi
		}
	} else {
		minLat = math.Max(minLat, minLatitude)
		maxLat = math.Min(maxLat, maxLatitude)
		minLon = minLongitude
		maxLon = maxLongitude
	}

	return orb.Bound{
		Min: orb.Point{rad2deg(minLon), rad2deg(minLat)},
		Max: orb.Point{rad2deg(maxLon), rad2deg(maxLat)},
	}
}

// BoundPad expands the bound in all directions by the given amount of meters.
func BoundPad(b orb.Bound, meters float64) orb.Bound {
	dy := meters / 111131.75
	dx := dy / math.Cos(deg2rad(b.Max[1]))
	dx = math.Max(dx, dy/math.Cos(deg2rad(b.Min[1])))

	b.Min[0] -= dx
	b.Min[1] -= dy

	b.Max[0] += dx
	b.Max[1] += dy

	b.Min[0] = math.Max(b.Min[0], -180)
	b.Min[1] = math.Max(b.Min[1], -90)

	b.Max[0] = math.Min(b.Max[0], 180)
	b.Max[1] = math.Min(b.Max[1], 90)

	return b
}

// BoundHeight returns the approximate height in meters.
func BoundHeight(b orb.Bound) float64 {
	return 111131.75 * (b.Max[1] - b.Min[1])
}

// BoundWidth returns the approximate width in meters
// of the center of the bound.
func BoundWidth(b orb.Bound) float64 {
	c := (b.Min[1] + b.Max[1]) / 2.0

	s1 := orb.Point{b.Min[0], c}
	s2 := orb.Point{b.Max[0], c}

	return Distance(s1, s2)
}

//MinLatitude is the minimum possible latitude
var minLatitude = deg2rad(-90)

//MaxLatitude is the maxiumum possible latitude
var maxLatitude = deg2rad(90)

//MinLongitude is the minimum possible longitude
var minLongitude = deg2rad(-180)

//MaxLongitude is the maxiumum possible longitude
var maxLongitude = deg2rad(180)

func deg2rad(d float64) float64 {
	return d * math.Pi / 180.0
}

func rad2deg(r float64) float64 {
	return 180.0 * r / math.Pi
}
