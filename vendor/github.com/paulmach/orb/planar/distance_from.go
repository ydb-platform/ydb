package planar

import (
	"fmt"
	"math"

	"github.com/paulmach/orb"
)

// DistanceFromSegment returns the point's distance from the segment [a, b].
func DistanceFromSegment(a, b, point orb.Point) float64 {
	return math.Sqrt(DistanceFromSegmentSquared(a, b, point))
}

// DistanceFromSegmentSquared returns point's squared distance from the segement [a, b].
func DistanceFromSegmentSquared(a, b, point orb.Point) float64 {
	x := a[0]
	y := a[1]
	dx := b[0] - x
	dy := b[1] - y

	if dx != 0 || dy != 0 {
		t := ((point[0]-x)*dx + (point[1]-y)*dy) / (dx*dx + dy*dy)

		if t > 1 {
			x = b[0]
			y = b[1]
		} else if t > 0 {
			x += dx * t
			y += dy * t
		}
	}

	dx = point[0] - x
	dy = point[1] - y

	return dx*dx + dy*dy
}

// DistanceFrom returns the distance from the boundary of the geometry in
// the units of the geometry.
func DistanceFrom(g orb.Geometry, p orb.Point) float64 {
	d, _ := DistanceFromWithIndex(g, p)
	return d
}

// DistanceFromWithIndex returns the minimum euclidean distance
// from the boundary of the geometry plus the index of the sub-geometry
// that was the match.
func DistanceFromWithIndex(g orb.Geometry, p orb.Point) (float64, int) {
	if g == nil {
		return math.Inf(1), -1
	}

	switch g := g.(type) {
	case orb.Point:
		return Distance(g, p), 0
	case orb.MultiPoint:
		return multiPointDistanceFrom(g, p)
	case orb.LineString:
		return lineStringDistanceFrom(g, p)
	case orb.MultiLineString:
		dist := math.Inf(1)
		index := -1
		for i, ls := range g {
			if d, _ := lineStringDistanceFrom(ls, p); d < dist {
				dist = d
				index = i
			}
		}

		return dist, index
	case orb.Ring:
		return lineStringDistanceFrom(orb.LineString(g), p)
	case orb.Polygon:
		return polygonDistanceFrom(g, p)
	case orb.MultiPolygon:
		dist := math.Inf(1)
		index := -1
		for i, poly := range g {
			if d, _ := polygonDistanceFrom(poly, p); d < dist {
				dist = d
				index = i
			}
		}

		return dist, index
	case orb.Collection:
		dist := math.Inf(1)
		index := -1
		for i, ge := range g {
			if d, _ := DistanceFromWithIndex(ge, p); d < dist {
				dist = d
				index = i
			}
		}

		return dist, index
	case orb.Bound:
		return DistanceFromWithIndex(g.ToRing(), p)
	}

	panic(fmt.Sprintf("geometry type not supported: %T", g))
}

func multiPointDistanceFrom(mp orb.MultiPoint, p orb.Point) (float64, int) {
	dist := math.Inf(1)
	index := -1

	for i := range mp {
		if d := DistanceSquared(mp[i], p); d < dist {
			dist = d
			index = i
		}
	}

	return math.Sqrt(dist), index
}

func lineStringDistanceFrom(ls orb.LineString, p orb.Point) (float64, int) {
	dist := math.Inf(1)
	index := -1

	for i := 0; i < len(ls)-1; i++ {
		if d := segmentDistanceFromSquared(ls[i], ls[i+1], p); d < dist {
			dist = d
			index = i
		}
	}

	return math.Sqrt(dist), index
}

func polygonDistanceFrom(p orb.Polygon, point orb.Point) (float64, int) {
	if len(p) == 0 {
		return math.Inf(1), -1
	}

	dist, index := lineStringDistanceFrom(orb.LineString(p[0]), point)
	for i := 1; i < len(p); i++ {
		d, i := lineStringDistanceFrom(orb.LineString(p[i]), point)
		if d < dist {
			dist = d
			index = i
		}
	}

	return dist, index
}

func segmentDistanceFromSquared(p1, p2, point orb.Point) float64 {
	x := p1[0]
	y := p1[1]
	dx := p2[0] - x
	dy := p2[1] - y

	if dx != 0 || dy != 0 {
		t := ((point[0]-x)*dx + (point[1]-y)*dy) / (dx*dx + dy*dy)

		if t > 1 {
			x = p2[0]
			y = p2[1]
		} else if t > 0 {
			x += dx * t
			y += dy * t
		}
	}

	dx = point[0] - x
	dy = point[1] - y

	return dx*dx + dy*dy
}
