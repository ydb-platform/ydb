// Package smartclip performs a more advanced clipping algorithm so
// it can deal with correctly oriented open rings and polygon.
package smartclip

import (
	"fmt"
	"sort"

	"github.com/paulmach/orb"
	"github.com/paulmach/orb/clip"
)

// Geometry will do a smart more involved clipping and wrapping of the geometry.
// It will return simple OGC geometries. Rings that are NOT closed AND have an
// endpoint in the bound will be implicitly closed.
func Geometry(box orb.Bound, g orb.Geometry, o orb.Orientation) orb.Geometry {
	if g == nil {
		return nil
	}

	if g.Dimensions() != 2 {
		return clip.Geometry(box, g)
	}

	var mp orb.MultiPolygon
	switch g := g.(type) {
	case orb.Ring:
		mp = Ring(box, g, o)
	case orb.Polygon:
		mp = Polygon(box, g, o)
	case orb.MultiPolygon:
		mp = MultiPolygon(box, g, o)
	case orb.Bound:
		return clip.Geometry(box, g)
	case orb.Collection:
		var result orb.Collection
		for _, c := range g {
			c := Geometry(box, c, o)
			if c != nil {
				result = append(result, c)
			}
		}

		if len(result) == 1 {
			return result[0]
		}

		return result
	default:
		panic(fmt.Sprintf("geometry type not supported: %T", g))
	}

	if mp == nil {
		return nil
	}

	if len(mp) == 1 {
		return mp[0]
	}

	return mp
}

// Ring will smart clip a ring to the boundary. This may result multiple rings so
// a multipolygon is possible. Rings that are NOT closed AND have an endpoint in
// the bound will be implicitly closed.
func Ring(box orb.Bound, r orb.Ring, o orb.Orientation) orb.MultiPolygon {
	if len(r) == 0 {
		return nil
	}

	open, closed := clipRings(box, []orb.Ring{r})
	if len(open) == 0 {
		// nothing was clipped
		if len(closed) == 0 {
			return nil // everything outside bound
		}

		return orb.MultiPolygon{{r}} // everything inside bound
	}

	// in a well defined ring there will be no closed sections
	return smartWrap(box, open, o)
}

// Polygon will smart clip a polygon to the bound.
// Rings that are NOT closed AND have an endpoint in the bound will be
// implicitly closed.
func Polygon(box orb.Bound, p orb.Polygon, o orb.Orientation) orb.MultiPolygon {
	if len(p) == 0 {
		return nil
	}

	open, closed := clipRings(box, p)
	if len(open) == 0 {
		// nothing was clipped
		if len(closed) == 0 {
			return nil // everything outside bound
		}

		return orb.MultiPolygon{p} // everything inside bound
	}

	result := smartWrap(box, open, o)
	if len(result) == 1 {
		result[0] = append(result[0], closed...)
	} else {
		for _, i := range closed {
			result = addToMultiPolygon(result, i)
		}
	}

	return result
}

// MultiPolygon will smart clip a multipolygon to the bound.
// Rings that are NOT closed AND have an endpoint in the bound will be
// implicitly closed.
func MultiPolygon(box orb.Bound, mp orb.MultiPolygon, o orb.Orientation) orb.MultiPolygon {
	if len(mp) == 0 {
		return nil
	}

	// outer rings
	outerRings := make([]orb.Ring, 0, len(mp))
	for _, p := range mp {
		outerRings = append(outerRings, p[0])
	}

	outers, closedOuters := clipRings(box, outerRings)
	if len(outers) == 0 {
		// nothing was clipped
		if len(closedOuters) == 0 {
			return nil // everything outside bound
		}

		return mp // everything inside bound
	}

	// inner rings
	var innerRings []orb.Ring
	for _, p := range mp {
		for _, r := range p[1:] {
			innerRings = append(innerRings, r)
		}
	}

	inners, closedInners := clipRings(box, innerRings)

	// smart wrap everything that touches the edges
	result := smartWrap(box, append(outers, inners...), o)
	for _, o := range closedOuters {
		result = append(result, orb.Polygon{o})
	}

	for _, i := range closedInners {
		result = addToMultiPolygon(result, i)
	}

	return result
}

// clipRings will take a set of rings and clip them to the boundary.
// It returns the open lineStrings with endpoints on the boundary and
// the closed interior rings.
func clipRings(box orb.Bound, rings []orb.Ring) (open []orb.LineString, closed []orb.Ring) {
	var result []orb.LineString
	for _, r := range rings {
		if !r.Closed() && (box.Contains(r[0]) || box.Contains(r[len(r)-1])) {
			r = append(r, r[0])
		}
		out := clip.LineString(box, orb.LineString(r), clip.OpenBound(true))
		if len(out) == 0 {
			continue // outside of bound
		}

		if r.Closed() {
			// if the input was a closed ring where the endpoints were within the bound,
			// then join the sections.

			// This operation is O(n^2), however, n is the number of segments, not edges
			// so I think it's manageable.
			for i := 0; i < len(out); i++ {
				end := out[i][len(out[i])-1]
				if end[0] == box.Min[0] || box.Max[0] == end[0] ||
					end[1] == box.Min[1] || box.Max[1] == end[1] {
					// endpoint must be within the bound to try join
					continue
				}

				for j := 0; j < len(out); j++ {
					if i == j {
						continue
					}

					if out[j][0] == end {
						out[i] = append(out[i], out[j][1:]...)
						i--

						out[j] = out[len(out)-1]
						out = out[:len(out)-1]
					}
				}
			}
		}

		result = append(result, out...)
	}

	at := 0
	for _, ls := range result {
		// closed ring, so completely inside bound
		// unless it touches a boundary
		if ls[0] == ls[len(ls)-1] && pointSide(box, ls[0]) == notOnSide {
			closed = append(closed, orb.Ring(ls))
		} else {
			result[at] = ls
			at++
		}
	}

	return result[:at], closed
}

type endpoint struct {
	Point    orb.Point
	Start    bool
	Used     bool
	Side     uint8
	Index    int
	OtherEnd int
}

func (e *endpoint) Before(mls []orb.LineString) orb.Point {
	ls := mls[e.Index]

	if e.Start {
		return ls[0]
	}

	return ls[len(ls)-2]
}

var emptyTwoRing = orb.Ring{{}, {}}

// smartWrap takes the open lineStrings with endpoints on the boundary and
// connects them correctly.
func smartWrap(box orb.Bound, input []orb.LineString, o orb.Orientation) orb.MultiPolygon {
	points := make([]*endpoint, 0, 2*len(input)+2)

	for i, r := range input {
		// start
		points = append(points, &endpoint{
			Point:    r[0],
			Start:    true,
			Side:     pointSide(box, r[0]),
			Index:    i,
			OtherEnd: 2*i + 1,
		})

		// end
		points = append(points, &endpoint{
			Point:    r[len(r)-1],
			Start:    false,
			Side:     pointSide(box, r[len(r)-1]),
			Index:    i,
			OtherEnd: 2 * i,
		})
	}

	if o == orb.CCW {
		sort.Sort(&sortableEndpoints{
			mls: input,
			eps: points,
		})
	} else {
		sort.Sort(sort.Reverse(&sortableEndpoints{
			mls: input,
			eps: points,
		}))
	}

	var (
		result  orb.MultiPolygon
		current orb.Ring
	)

	// this operation is O(n^2). Technically we could use a linked list
	// and remove points instead of marking them as "used".
	// However since n is 2x the number of segements I think we're okay.
	for i := 0; i < 2*len(points); i++ {
		ep := points[i%len(points)]
		if ep.Used {
			continue
		}

		if !ep.Start {
			if len(current) == 0 {
				current = orb.Ring(input[ep.Index])
				ep.Used = true
			}
			continue
		}

		if len(current) == 0 {
			continue
		}
		ep.Used = true

		// previous was end, connect to this start
		var r orb.Ring
		if ep.Point == current[len(current)-1] {
			r = emptyTwoRing
		} else {
			r = aroundBound(box, orb.Ring{ep.Point, current[len(current)-1]}, o)
		}

		if ep.Point.Equal(current[0]) {
			// loop complete!!
			current = append(current, r[2:]...)
			result = append(result, orb.Polygon{current})
			current = nil

			i = -1 // start over looking for unused endpoints
		} else {
			if len(r) > 2 {
				current = append(current, r[2:len(r)-1]...)
			}
			current = append(current, input[ep.Index]...)

			points[ep.OtherEnd].Used = true
			i = ep.OtherEnd
		}
	}

	return result
}

const notOnSide = 0xFF

//    4
//   +-+
// 1 | | 3
//   +-+
//    2
func pointSide(b orb.Bound, p orb.Point) uint8 {
	if p[1] == b.Max[1] {
		return 4
	} else if p[1] == b.Min[1] {
		return 2
	} else if p[0] == b.Max[0] {
		return 3
	} else if p[0] == b.Min[0] {
		return 1
	}

	return notOnSide
}

type sortableEndpoints struct {
	mls []orb.LineString
	eps []*endpoint
}

func (e *sortableEndpoints) Len() int {
	return len(e.eps)
}

// Less sorts the points around the bound.
// First comparing what side it's on and then the actual point to determine the order.
// If two points are the same, we sort by the edge attached to the point so lines that are
// "above" are shorted first.
func (e *sortableEndpoints) Less(i, j int) bool {
	if e.eps[i].Side != e.eps[j].Side {
		return e.eps[i].Side < e.eps[j].Side
	}

	switch e.eps[i].Side {
	case 1:
		if e.eps[i].Point[1] != e.eps[j].Point[1] {
			return e.eps[i].Point[1] >= e.eps[j].Point[1]
		}

		return e.eps[i].Before(e.mls)[1] >= e.eps[j].Before(e.mls)[1]
	case 2:
		if e.eps[i].Point[0] != e.eps[j].Point[0] {
			return e.eps[i].Point[0] < e.eps[j].Point[0]
		}

		return e.eps[i].Before(e.mls)[0] < e.eps[j].Before(e.mls)[0]
	case 3:
		if e.eps[i].Point[1] != e.eps[j].Point[1] {
			return e.eps[i].Point[1] < e.eps[j].Point[1]
		}
		return e.eps[i].Before(e.mls)[1] < e.eps[j].Before(e.mls)[1]
	case 4:
		if e.eps[i].Point[0] != e.eps[j].Point[0] {
			return e.eps[i].Point[0] >= e.eps[j].Point[0]
		}

		return e.eps[i].Before(e.mls)[0] >= e.eps[j].Before(e.mls)[0]
	}

	panic("unreachable")
}

func (e *sortableEndpoints) Swap(i, j int) {
	e.eps[e.eps[i].OtherEnd].OtherEnd, e.eps[e.eps[j].OtherEnd].OtherEnd = j, i
	e.eps[i], e.eps[j] = e.eps[j], e.eps[i]
}

// addToMultiPolygon does a lookup to see which polygon the ring intersects.
// This should work fine if the input is well formed.
func addToMultiPolygon(mp orb.MultiPolygon, ring orb.Ring) orb.MultiPolygon {
	for i := range mp {
		if polygonContains(mp[i][0], ring) {
			mp[i] = append(mp[i], ring)
			return mp
		}
	}

	// ring is not in any polygons?
	// skip it, TODO: is this correct?
	// If input is well formed, I think it is. If it isn't, ¯\_(ツ)_/¯

	return mp
}

func polygonContains(outer orb.Ring, r orb.Ring) bool {
	for _, p := range r {
		inside := false

		x, y := p[0], p[1]
		i, j := 0, len(outer)-1
		for i < len(outer) {
			xi, yi := outer[i][0], outer[i][1]
			xj, yj := outer[j][0], outer[j][1]

			if ((yi > y) != (yj > y)) &&
				(x < (xj-xi)*(y-yi)/(yj-yi)+xi) {
				inside = !inside
			}

			j = i
			i++
		}

		if inside {
			return true
		}
	}

	return false
}
