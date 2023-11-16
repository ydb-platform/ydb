package length

import (
	"fmt"

	"github.com/paulmach/orb"
)

// Length returns the length of the boundary of the geometry
// using 2d euclidean geometry.
func Length(g orb.Geometry, df orb.DistanceFunc) float64 {
	if g == nil {
		return 0
	}

	switch g := g.(type) {
	case orb.Point:
		return 0
	case orb.MultiPoint:
		return 0
	case orb.LineString:
		return lineStringLength(g, df)
	case orb.MultiLineString:
		sum := 0.0
		for _, ls := range g {
			sum += lineStringLength(ls, df)
		}

		return sum
	case orb.Ring:
		return lineStringLength(orb.LineString(g), df)
	case orb.Polygon:
		return polygonLength(g, df)
	case orb.MultiPolygon:
		sum := 0.0
		for _, p := range g {
			sum += polygonLength(p, df)
		}

		return sum
	case orb.Collection:
		sum := 0.0
		for _, c := range g {
			sum += Length(c, df)
		}

		return sum
	case orb.Bound:
		return Length(g.ToRing(), df)
	}

	panic(fmt.Sprintf("geometry type not supported: %T", g))
}

func lineStringLength(ls orb.LineString, df orb.DistanceFunc) float64 {
	sum := 0.0
	for i := 1; i < len(ls); i++ {
		sum += df(ls[i], ls[i-1])
	}

	return sum
}

func polygonLength(p orb.Polygon, df orb.DistanceFunc) float64 {
	sum := 0.0
	for _, r := range p {
		sum += lineStringLength(orb.LineString(r), df)
	}

	return sum
}
