package simplify

import (
	"github.com/paulmach/orb"
)

var _ orb.Simplifier = &RadialSimplifier{}

// A RadialSimplifier wraps the Radial functions
type RadialSimplifier struct {
	DistanceFunc orb.DistanceFunc
	Threshold    float64 // euclidean distance
}

// Radial creates a new RadialSimplifier.
func Radial(df orb.DistanceFunc, threshold float64) *RadialSimplifier {
	return &RadialSimplifier{
		DistanceFunc: df,
		Threshold:    threshold,
	}
}

func (s *RadialSimplifier) simplify(ls orb.LineString, wim bool) (orb.LineString, []int) {
	var indexMap []int
	if wim {
		indexMap = append(indexMap, 0)
	}

	count := 1
	current := 0
	for i := 1; i < len(ls); i++ {
		if s.DistanceFunc(ls[current], ls[i]) > s.Threshold {
			current = i
			ls[count] = ls[i]
			count++
			if wim {
				indexMap = append(indexMap, current)
			}
		}
	}

	if current != len(ls)-1 {
		ls[count] = ls[len(ls)-1]
		count++
		if wim {
			indexMap = append(indexMap, len(ls)-1)
		}
	}

	return ls[:count], indexMap
}

// Simplify will run the simplification for any geometry type.
func (s *RadialSimplifier) Simplify(g orb.Geometry) orb.Geometry {
	return simplify(s, g)
}

// LineString will simplify the linestring using this simplifier.
func (s *RadialSimplifier) LineString(ls orb.LineString) orb.LineString {
	return lineString(s, ls)
}

// MultiLineString will simplify the multi-linestring using this simplifier.
func (s *RadialSimplifier) MultiLineString(mls orb.MultiLineString) orb.MultiLineString {
	return multiLineString(s, mls)
}

// Ring will simplify the ring using this simplifier.
func (s *RadialSimplifier) Ring(r orb.Ring) orb.Ring {
	return ring(s, r)
}

// Polygon will simplify the polygon using this simplifier.
func (s *RadialSimplifier) Polygon(p orb.Polygon) orb.Polygon {
	return polygon(s, p)
}

// MultiPolygon will simplify the multi-polygon using this simplifier.
func (s *RadialSimplifier) MultiPolygon(mp orb.MultiPolygon) orb.MultiPolygon {
	return multiPolygon(s, mp)
}

// Collection will simplify the collection using this simplifier.
func (s *RadialSimplifier) Collection(c orb.Collection) orb.Collection {
	return collection(s, c)
}
