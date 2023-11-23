package simplify

import (
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/planar"
)

var _ orb.Simplifier = &DouglasPeuckerSimplifier{}

// A DouglasPeuckerSimplifier wraps the DouglasPeucker function.
type DouglasPeuckerSimplifier struct {
	Threshold float64
}

// DouglasPeucker creates a new DouglasPeuckerSimplifier.
func DouglasPeucker(threshold float64) *DouglasPeuckerSimplifier {
	return &DouglasPeuckerSimplifier{
		Threshold: threshold,
	}
}

func (s *DouglasPeuckerSimplifier) simplify(ls orb.LineString, wim bool) (orb.LineString, []int) {
	mask := make([]byte, len(ls))
	mask[0] = 1
	mask[len(mask)-1] = 1

	found := dpWorker(ls, s.Threshold, mask)
	var indexMap []int
	if wim {
		indexMap = make([]int, 0, found)
	}

	count := 0
	for i, v := range mask {
		if v == 1 {
			ls[count] = ls[i]
			count++
			if wim {
				indexMap = append(indexMap, i)
			}
		}
	}

	return ls[:count], indexMap
}

// dpWorker does the recursive threshold checks.
// Using a stack array with a stackLength variable resulted in
// 4x speed improvement over calling the function recursively.
func dpWorker(ls orb.LineString, threshold float64, mask []byte) int {
	found := 2

	var stack []int
	stack = append(stack, 0, len(ls)-1)

	for len(stack) > 0 {
		start := stack[len(stack)-2]
		end := stack[len(stack)-1]

		// modify the line in place
		maxDist := 0.0
		maxIndex := 0

		for i := start + 1; i < end; i++ {
			dist := planar.DistanceFromSegmentSquared(ls[start], ls[end], ls[i])
			if dist > maxDist {
				maxDist = dist
				maxIndex = i
			}
		}

		if maxDist > threshold*threshold {
			found++
			mask[maxIndex] = 1

			stack[len(stack)-1] = maxIndex
			stack = append(stack, maxIndex, end)
		} else {
			stack = stack[:len(stack)-2]
		}
	}

	return found
}

// Simplify will run the simplification for any geometry type.
func (s *DouglasPeuckerSimplifier) Simplify(g orb.Geometry) orb.Geometry {
	return simplify(s, g)
}

// LineString will simplify the linestring using this simplifier.
func (s *DouglasPeuckerSimplifier) LineString(ls orb.LineString) orb.LineString {
	return lineString(s, ls)
}

// MultiLineString will simplify the multi-linestring using this simplifier.
func (s *DouglasPeuckerSimplifier) MultiLineString(mls orb.MultiLineString) orb.MultiLineString {
	return multiLineString(s, mls)
}

// Ring will simplify the ring using this simplifier.
func (s *DouglasPeuckerSimplifier) Ring(r orb.Ring) orb.Ring {
	return ring(s, r)
}

// Polygon will simplify the polygon using this simplifier.
func (s *DouglasPeuckerSimplifier) Polygon(p orb.Polygon) orb.Polygon {
	return polygon(s, p)
}

// MultiPolygon will simplify the multi-polygon using this simplifier.
func (s *DouglasPeuckerSimplifier) MultiPolygon(mp orb.MultiPolygon) orb.MultiPolygon {
	return multiPolygon(s, mp)
}

// Collection will simplify the collection using this simplifier.
func (s *DouglasPeuckerSimplifier) Collection(c orb.Collection) orb.Collection {
	return collection(s, c)
}
