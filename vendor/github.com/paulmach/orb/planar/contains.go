package planar

import (
	"math"

	"github.com/paulmach/orb"
)

// RingContains returns true if the point is inside the ring.
// Points on the boundary are considered in.
func RingContains(r orb.Ring, point orb.Point) bool {
	if !r.Bound().Contains(point) {
		return false
	}

	c, on := rayIntersect(point, r[0], r[len(r)-1])
	if on {
		return true
	}

	for i := 0; i < len(r)-1; i++ {
		inter, on := rayIntersect(point, r[i], r[i+1])
		if on {
			return true
		}

		if inter {
			c = !c
		}
	}

	return c
}

// PolygonContains checks if the point is within the polygon.
// Points on the boundary are considered in.
func PolygonContains(p orb.Polygon, point orb.Point) bool {
	if !RingContains(p[0], point) {
		return false
	}

	for i := 1; i < len(p); i++ {
		if RingContains(p[i], point) {
			return false
		}
	}

	return true
}

// MultiPolygonContains checks if the point is within the multi-polygon.
// Points on the boundary are considered in.
func MultiPolygonContains(mp orb.MultiPolygon, point orb.Point) bool {
	for _, p := range mp {
		if PolygonContains(p, point) {
			return true
		}
	}

	return false
}

// Original implementation: http://rosettacode.org/wiki/Ray-casting_algorithm#Go
func rayIntersect(p, s, e orb.Point) (intersects, on bool) {
	if s[0] > e[0] {
		s, e = e, s
	}

	if p[0] == s[0] {
		if p[1] == s[1] {
			// p == start
			return false, true
		} else if s[0] == e[0] {
			// vertical segment (s -> e)
			// return true if within the line, check to see if start or end is greater.
			if s[1] > e[1] && s[1] >= p[1] && p[1] >= e[1] {
				return false, true
			}

			if e[1] > s[1] && e[1] >= p[1] && p[1] >= s[1] {
				return false, true
			}
		}

		// Move the y coordinate to deal with degenerate case
		p[0] = math.Nextafter(p[0], math.Inf(1))
	} else if p[0] == e[0] {
		if p[1] == e[1] {
			// matching the end point
			return false, true
		}

		p[0] = math.Nextafter(p[0], math.Inf(1))
	}

	if p[0] < s[0] || p[0] > e[0] {
		return false, false
	}

	if s[1] > e[1] {
		if p[1] > s[1] {
			return false, false
		} else if p[1] < e[1] {
			return true, false
		}
	} else {
		if p[1] > e[1] {
			return false, false
		} else if p[1] < s[1] {
			return true, false
		}
	}

	rs := (p[1] - s[1]) / (p[0] - s[0])
	ds := (e[1] - s[1]) / (e[0] - s[0])

	if rs == ds {
		return false, true
	}

	return rs <= ds, false
}
