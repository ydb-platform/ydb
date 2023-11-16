package clip

import (
	"github.com/paulmach/orb"
)

// Code based on https://github.com/mapbox/lineclip

// line will clip a line into a set of lines
// along the bounding box boundary.
func line(box orb.Bound, in orb.LineString, open bool) orb.MultiLineString {
	if len(in) == 0 {
		return nil
	}

	var out orb.MultiLineString
	line := 0

	var codeA int
	if open {
		codeA = bitCodeOpen(box, in[0])
	} else {
		codeA = bitCode(box, in[0])
	}

	loopTo := len(in)
	for i := 1; i < loopTo; i++ {
		a := in[i-1]
		b := in[i]

		var codeB int
		if open {
			codeB = bitCodeOpen(box, b)
		} else {
			codeB = bitCode(box, b)
		}
		endCode := codeB

		// loops through all the intersection of the line and box.
		// eg. across a corner could have two intersections.
		for {
			if codeA|codeB == 0 {
				// both points are in the box, accept
				out = push(out, line, a)
				if codeB != endCode { // segment went outside
					out = push(out, line, b)
					if i < loopTo-1 {
						line++
					}
				} else if i == loopTo-1 {
					out = push(out, line, b)
				}
				break
			} else if codeA&codeB != 0 {
				// both on one side of the box.
				// segment not part of the final result.
				break
			} else if codeA != 0 {
				// A is outside, B is inside, clip edge
				a = intersect(box, codeA, a, b)
				codeA = bitCode(box, a)
			} else {
				// B is outside, A is inside, clip edge
				b = intersect(box, codeB, a, b)
				codeB = bitCode(box, b)
			}
		}

		codeA = endCode // new start is the old end
	}

	return out
}

func push(out orb.MultiLineString, i int, p orb.Point) orb.MultiLineString {
	if i >= len(out) {
		out = append(out, orb.LineString{})
	}

	out[i] = append(out[i], p)
	return out
}

// ring will clip the Ring into a smaller ring around the bounding box boundary.
func ring(box orb.Bound, in orb.Ring) orb.Ring {
	var out orb.Ring
	if len(in) == 0 {
		return in
	}

	f := in[0]
	l := in[len(in)-1]

	initClosed := false
	if f == l {
		initClosed = true
	}

	for edge := 1; edge <= 8; edge <<= 1 {
		out = out[:0]

		loopTo := len(in)

		// if we're not a nice closed ring, don't implicitly close it.
		prev := in[loopTo-1]
		if !initClosed {
			prev = in[0]
		}

		prevInside := bitCode(box, prev)&edge == 0

		for i := 0; i < loopTo; i++ {
			p := in[i]
			inside := bitCode(box, p)&edge == 0

			// if segment goes through the clip window, add an intersection
			if inside != prevInside {
				i := intersect(box, edge, prev, p)
				out = append(out, i)
			}
			if inside {
				out = append(out, p)
			}

			prev = p
			prevInside = inside
		}

		if len(out) == 0 {
			return nil
		}

		in, out = out, in
	}
	out = in // swap back

	if initClosed {
		// need to make sure our output is also closed.
		if l := len(out); l != 0 {
			f := out[0]
			l := out[l-1]

			if f != l {
				out = append(out, f)
			}
		}
	}

	return out
}

// bitCode returns the point position relative to the bbox:
//         left  mid  right
//    top  1001  1000  1010
//    mid  0001  0000  0010
// bottom  0101  0100  0110
func bitCode(b orb.Bound, p orb.Point) int {
	code := 0
	if p[0] < b.Min[0] {
		code |= 1
	} else if p[0] > b.Max[0] {
		code |= 2
	}

	if p[1] < b.Min[1] {
		code |= 4
	} else if p[1] > b.Max[1] {
		code |= 8
	}

	return code
}

func bitCodeOpen(b orb.Bound, p orb.Point) int {
	code := 0
	if p[0] <= b.Min[0] {
		code |= 1
	} else if p[0] >= b.Max[0] {
		code |= 2
	}

	if p[1] <= b.Min[1] {
		code |= 4
	} else if p[1] >= b.Max[1] {
		code |= 8
	}

	return code
}

// intersect a segment against one of the 4 lines that make up the bbox
func intersect(box orb.Bound, edge int, a, b orb.Point) orb.Point {
	if edge&8 != 0 {
		// top
		return orb.Point{a[0] + (b[0]-a[0])*(box.Max[1]-a[1])/(b[1]-a[1]), box.Max[1]}
	} else if edge&4 != 0 {
		// bottom
		return orb.Point{a[0] + (b[0]-a[0])*(box.Min[1]-a[1])/(b[1]-a[1]), box.Min[1]}
	} else if edge&2 != 0 {
		// right
		return orb.Point{box.Max[0], a[1] + (b[1]-a[1])*(box.Max[0]-a[0])/(b[0]-a[0])}
	} else if edge&1 != 0 {
		// left
		return orb.Point{box.Min[0], a[1] + (b[1]-a[1])*(box.Min[0]-a[0])/(b[0]-a[0])}
	}

	panic("no edge??")
}
