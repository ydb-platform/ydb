// Package project defines projections to and from Mercator and WGS84
// along with helpers to apply them to orb geometry types.
package project

import "github.com/paulmach/orb"

// Geometry is a helper to project any geomtry.
func Geometry(g orb.Geometry, proj orb.Projection) orb.Geometry {
	if g == nil {
		return nil
	}

	switch g := g.(type) {
	case orb.Point:
		return Point(g, proj)
	case orb.MultiPoint:
		return MultiPoint(g, proj)
	case orb.LineString:
		return LineString(g, proj)
	case orb.MultiLineString:
		return MultiLineString(g, proj)
	case orb.Ring:
		return Ring(g, proj)
	case orb.Polygon:
		return Polygon(g, proj)
	case orb.MultiPolygon:
		return MultiPolygon(g, proj)
	case orb.Collection:
		return Collection(g, proj)
	case orb.Bound:
		return Bound(g, proj)
	}

	panic("geometry type not supported")
}

// Point is a helper to project an a point
func Point(p orb.Point, proj orb.Projection) orb.Point {
	return proj(p)
}

// MultiPoint is a helper to project an entire multi point.
func MultiPoint(mp orb.MultiPoint, proj orb.Projection) orb.MultiPoint {
	for i := range mp {
		mp[i] = proj(mp[i])
	}

	return mp
}

// LineString is a helper to project an entire line string.
func LineString(ls orb.LineString, proj orb.Projection) orb.LineString {
	return orb.LineString(MultiPoint(orb.MultiPoint(ls), proj))
}

// MultiLineString is a helper to project an entire multi linestring.
func MultiLineString(mls orb.MultiLineString, proj orb.Projection) orb.MultiLineString {
	for i := range mls {
		mls[i] = LineString(mls[i], proj)
	}

	return mls
}

// Ring is a helper to project an entire ring.
func Ring(r orb.Ring, proj orb.Projection) orb.Ring {
	return orb.Ring(LineString(orb.LineString(r), proj))
}

// Polygon is a helper to project an entire polygon.
func Polygon(p orb.Polygon, proj orb.Projection) orb.Polygon {
	for i := range p {
		p[i] = Ring(p[i], proj)
	}

	return p
}

// MultiPolygon is a helper to project an entire multi polygon.
func MultiPolygon(mp orb.MultiPolygon, proj orb.Projection) orb.MultiPolygon {
	for i := range mp {
		mp[i] = Polygon(mp[i], proj)
	}

	return mp
}

// Collection is a helper to project a rectangle.
func Collection(c orb.Collection, proj orb.Projection) orb.Collection {
	for i := range c {
		c[i] = Geometry(c[i], proj)
	}

	return c
}

// Bound is a helper to project a rectangle.
func Bound(bound orb.Bound, proj orb.Projection) orb.Bound {
	min := proj(bound.Min)
	return orb.Bound{Min: min, Max: min}.Extend(proj(bound.Max))
}
