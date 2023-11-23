// Package clip is a library for clipping geometry to a bounding box.
package clip

import (
	"fmt"
	"math"

	"github.com/paulmach/orb"
)

// Geometry will clip the geometry to the bounding box using the
// correct functions for the type.
// This operation will modify the input of '1d or 2d geometry' by using as a
// scratch space so clone if necessary.
func Geometry(b orb.Bound, g orb.Geometry) orb.Geometry {
	if g == nil {
		return nil
	}

	if !b.Intersects(g.Bound()) {
		return nil
	}

	switch g := g.(type) {
	case orb.Point:
		return g // Intersect check above
	case orb.MultiPoint:
		mp := MultiPoint(b, g)
		if len(mp) == 1 {
			return mp[0]
		}

		if mp == nil {
			return nil
		}

		return mp
	case orb.LineString:
		mls := LineString(b, g)
		if len(mls) == 1 {
			return mls[0]
		}

		if len(mls) == 0 {
			return nil
		}
		return mls
	case orb.MultiLineString:
		mls := MultiLineString(b, g)
		if len(mls) == 1 {
			return mls[0]
		}

		if mls == nil {
			return nil
		}

		return mls
	case orb.Ring:
		r := Ring(b, g)
		if r == nil {
			return nil
		}

		return r
	case orb.Polygon:
		p := Polygon(b, g)
		if p == nil {
			return nil
		}

		return p
	case orb.MultiPolygon:
		mp := MultiPolygon(b, g)
		if len(mp) == 1 {
			return mp[0]
		}

		if mp == nil {
			return nil
		}

		return mp
	case orb.Collection:
		c := Collection(b, g)
		if len(c) == 1 {
			return c[0]
		}

		if c == nil {
			return nil
		}

		return c
	case orb.Bound:
		b = Bound(b, g)
		if b.IsEmpty() {
			return nil
		}

		return b
	}

	panic(fmt.Sprintf("geometry type not supported: %T", g))
}

// MultiPoint returns a new set with the points outside the bound removed.
func MultiPoint(b orb.Bound, mp orb.MultiPoint) orb.MultiPoint {
	var result orb.MultiPoint
	for _, p := range mp {
		if b.Contains(p) {
			result = append(result, p)
		}
	}

	return result
}

// LineString clips the linestring to the bounding box.
func LineString(b orb.Bound, ls orb.LineString, opts ...Option) orb.MultiLineString {
	open := false
	if len(opts) > 0 {
		o := &options{}
		for _, opt := range opts {
			opt(o)
		}

		open = o.openBound
	}

	result := line(b, ls, open)
	if len(result) == 0 {
		return nil
	}

	return result
}

// MultiLineString clips the linestrings to the bounding box
// and returns a linestring union.
func MultiLineString(b orb.Bound, mls orb.MultiLineString, opts ...Option) orb.MultiLineString {
	open := false
	if len(opts) > 0 {
		o := &options{}
		for _, opt := range opts {
			opt(o)
		}

		open = o.openBound
	}

	var result orb.MultiLineString
	for _, ls := range mls {
		r := line(b, ls, open)
		if len(r) != 0 {
			result = append(result, r...)
		}
	}
	return result
}

// Ring clips the ring to the bounding box and returns another ring.
// This operation will modify the input by using as a scratch space
// so clone if necessary.
func Ring(b orb.Bound, r orb.Ring) orb.Ring {
	result := ring(b, r)
	if len(result) == 0 {
		return nil
	}

	return result
}

// Polygon clips the polygon to the bounding box excluding the inner rings
// if they do not intersect the bounding box.
// This operation will modify the input by using as a scratch space
// so clone if necessary.
func Polygon(b orb.Bound, p orb.Polygon) orb.Polygon {
	if len(p) == 0 {
		return nil
	}

	r := Ring(b, p[0])
	if r == nil {
		return nil
	}

	result := orb.Polygon{r}
	for i := 1; i < len(p); i++ {
		r := Ring(b, p[i])
		if r != nil {
			result = append(result, r)
		}
	}

	return result
}

// MultiPolygon clips the multi polygon to the bounding box excluding
// any polygons if they don't intersect the bounding box.
// This operation will modify the input by using as a scratch space
// so clone if necessary.
func MultiPolygon(b orb.Bound, mp orb.MultiPolygon) orb.MultiPolygon {
	var result orb.MultiPolygon
	for _, polygon := range mp {
		p := Polygon(b, polygon)
		if p != nil {
			result = append(result, p)
		}
	}

	return result
}

// Collection clips each element in the collection to the bounding box.
// It will exclude elements if they don't intersect the bounding box.
// This operation will modify the input of '2d geometry' by using as a
// scratch space so clone if necessary.
func Collection(b orb.Bound, c orb.Collection) orb.Collection {
	var result orb.Collection
	for _, g := range c {
		clipped := Geometry(b, g)
		if clipped != nil {
			result = append(result, clipped)
		}
	}

	return result
}

// Bound intersects the two bounds. May result in an
// empty/degenerate bound.
func Bound(b, bound orb.Bound) orb.Bound {
	if b.IsEmpty() && bound.IsEmpty() {
		return bound
	}

	if b.IsEmpty() {
		return bound
	} else if bound.IsEmpty() {
		return b
	}

	return orb.Bound{
		Min: orb.Point{
			math.Max(b.Min[0], bound.Min[0]),
			math.Max(b.Min[1], bound.Min[1]),
		},
		Max: orb.Point{
			math.Min(b.Max[0], bound.Max[0]),
			math.Min(b.Max[1], bound.Max[1]),
		},
	}
}
