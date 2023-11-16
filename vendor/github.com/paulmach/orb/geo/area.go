// Package geo computes properties on geometries assuming they are lon/lat data.
package geo

import (
	"fmt"
	"math"

	"github.com/paulmach/orb"
)

// Area returns the area of the geometry on the earth.
func Area(g orb.Geometry) float64 {
	if g == nil {
		return 0
	}

	switch g := g.(type) {
	case orb.Point, orb.MultiPoint, orb.LineString, orb.MultiLineString:
		return 0
	case orb.Ring:
		return math.Abs(ringArea(g))
	case orb.Polygon:
		return polygonArea(g)
	case orb.MultiPolygon:
		return multiPolygonArea(g)
	case orb.Collection:
		return collectionArea(g)
	case orb.Bound:
		return Area(g.ToRing())
	}

	panic(fmt.Sprintf("geometry type not supported: %T", g))
}

// SignedArea will return the signed area of the ring.
// Will return negative if the ring is in the clockwise direction.
// Will implicitly close the ring.
func SignedArea(r orb.Ring) float64 {
	return ringArea(r)
}

func ringArea(r orb.Ring) float64 {
	if len(r) < 3 {
		return 0
	}
	var lo, mi, hi int

	l := len(r)
	if r[0] != r[len(r)-1] {
		// if not a closed ring, add an implicit calc for that last point.
		l++
	}

	// To support implicit closing of ring, replace references to
	// the last point in r to the first 1.

	area := 0.0
	for i := 0; i < l; i++ {
		if i == l-3 { // i = N-3
			lo = l - 3
			mi = l - 2
			hi = 0
		} else if i == l-2 { // i = N-2
			lo = l - 2
			mi = 0
			hi = 0
		} else if i == l-1 { // i = N-1
			lo = 0
			mi = 0
			hi = 1
		} else { // i = 0 to N-3
			lo = i
			mi = i + 1
			hi = i + 2
		}

		area += (deg2rad(r[hi][0]) - deg2rad(r[lo][0])) * math.Sin(deg2rad(r[mi][1]))
	}

	return -area * orb.EarthRadius * orb.EarthRadius / 2
}

func polygonArea(p orb.Polygon) float64 {
	if len(p) == 0 {
		return 0
	}

	sum := math.Abs(ringArea(p[0]))
	for i := 1; i < len(p); i++ {
		sum -= math.Abs(ringArea(p[i]))
	}

	return sum
}

func multiPolygonArea(mp orb.MultiPolygon) float64 {
	sum := 0.0
	for _, p := range mp {
		sum += polygonArea(p)
	}

	return sum
}

func collectionArea(c orb.Collection) float64 {
	area := 0.0
	for _, g := range c {
		area += Area(g)
	}

	return area
}
