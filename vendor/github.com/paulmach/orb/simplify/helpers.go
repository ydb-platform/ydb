// Package simplify implements several reducing/simplifying functions for `orb.Geometry` types.
package simplify

import "github.com/paulmach/orb"

type simplifier interface {
	simplify(orb.LineString, bool) (orb.LineString, []int)
}

func simplify(s simplifier, geom orb.Geometry) orb.Geometry {
	if geom == nil {
		return nil
	}

	switch g := geom.(type) {
	case orb.Point:
		return g
	case orb.MultiPoint:
		if g == nil {
			return nil
		}
		return g
	case orb.LineString:
		g = lineString(s, g)
		if len(g) == 0 {
			return nil
		}
		return g
	case orb.MultiLineString:
		g = multiLineString(s, g)
		if len(g) == 0 {
			return nil
		}
		return g
	case orb.Ring:
		g = ring(s, g)
		if len(g) == 0 {
			return nil
		}
		return g
	case orb.Polygon:
		g = polygon(s, g)
		if len(g) == 0 {
			return nil
		}
		return g
	case orb.MultiPolygon:
		g = multiPolygon(s, g)
		if len(g) == 0 {
			return nil
		}
		return g
	case orb.Collection:
		g = collection(s, g)
		if len(g) == 0 {
			return nil
		}
		return g
	case orb.Bound:
		return g
	}

	panic("unsupported type")
}

func lineString(s simplifier, ls orb.LineString) orb.LineString {
	return runSimplify(s, ls)
}

func multiLineString(s simplifier, mls orb.MultiLineString) orb.MultiLineString {
	for i := range mls {
		mls[i] = runSimplify(s, mls[i])
	}
	return mls
}

func ring(s simplifier, r orb.Ring) orb.Ring {
	return orb.Ring(runSimplify(s, orb.LineString(r)))
}

func polygon(s simplifier, p orb.Polygon) orb.Polygon {
	count := 0
	for i := range p {
		r := orb.Ring(runSimplify(s, orb.LineString(p[i])))
		if i != 0 && len(r) <= 2 {
			continue
		}

		p[count] = r
		count++
	}
	return p[:count]
}

func multiPolygon(s simplifier, mp orb.MultiPolygon) orb.MultiPolygon {
	count := 0
	for i := range mp {
		p := polygon(s, mp[i])
		if len(p[0]) <= 2 {
			continue
		}

		mp[count] = p
		count++
	}
	return mp[:count]
}

func collection(s simplifier, c orb.Collection) orb.Collection {
	for i := range c {
		c[i] = simplify(s, c[i])
	}
	return c
}

func runSimplify(s simplifier, ls orb.LineString) orb.LineString {
	if len(ls) <= 2 {
		return ls
	}
	ls, _ = s.simplify(ls, false)
	return ls
}
