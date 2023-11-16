package planar

import (
	"math"

	"github.com/paulmach/orb"
)

// Distance returns the distance between two points in 2d euclidean geometry.
func Distance(p1, p2 orb.Point) float64 {
	d0 := (p1[0] - p2[0])
	d1 := (p1[1] - p2[1])
	return math.Sqrt(d0*d0 + d1*d1)
}

// DistanceSquared returns the square of the distance between two points in 2d euclidean geometry.
func DistanceSquared(p1, p2 orb.Point) float64 {
	d0 := (p1[0] - p2[0])
	d1 := (p1[1] - p2[1])
	return d0*d0 + d1*d1
}
