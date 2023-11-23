package smartclip

import (
	"sort"

	"github.com/paulmach/orb"
)

// aroundBound will connect the endpoints of the linestring provided
// by wrapping the line around the bounds in the direction provided.
// Will append to the input.
func aroundBound(
	box orb.Bound,
	in orb.Ring,
	o orb.Orientation,
) orb.Ring {
	if o != orb.CCW && o != orb.CW {
		panic("invalid orientation")
	}

	if len(in) == 0 {
		return nil
	}

	next := nexts[o]

	f := in[0]
	l := in[len(in)-1]

	target := bitCodeOpen(box, f)
	current := bitCodeOpen(box, l)

	if target == 0 || current == 0 {
		panic("endpoints must be outside bound")
	}

	if current == target {
		// endpoints long an edge. Need to figure out what order they're in
		// to figure out if we just need to connect them or go all the way around.
		points := []*endpoint{
			{
				Point: f,
				Start: true,
				Side:  pointSide(box, f),
				Index: 0,
			},
			{
				Point: l,
				Start: false,
				Side:  pointSide(box, l),
				Index: 0,
			},
		}

		se := &sortableEndpoints{
			mls: []orb.LineString{orb.LineString(in)},
			eps: points,
		}

		if o == orb.CCW {
			sort.Sort(se)
		} else {
			sort.Sort(sort.Reverse(se))
		}

		if !points[0].Start {
			if f != in[len(in)-1] {
				in = append(in, f)
			}
			return in
		}
	}

	// move to next and go until we're all the way around.
	current = next[current]
	for target != current {
		in = append(in, pointFor(box, current))
		current = next[current]
	}

	// add first point to the end to make it a ring
	in = append(in, f)
	return in
}

//         left  mid  right
//    top  1001  1000  1010
//    mid  0001  0000  0010
// bottom  0101  0100  0110

// on the boundary is outside
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

// pointFor returns a representative point for the side of the given bitCode.
func pointFor(b orb.Bound, code int) orb.Point {
	switch code {
	case 1:
		return orb.Point{b.Min[0], (b.Max[1] + b.Min[1]) / 2}
	case 2:
		return orb.Point{b.Max[0], (b.Max[1] + b.Min[1]) / 2}
	case 4:
		return orb.Point{(b.Max[0] + b.Min[0]) / 2, b.Min[1]}
	case 5:
		return orb.Point{b.Min[0], b.Min[1]}
	case 6:
		return orb.Point{b.Max[0], b.Min[1]}
	case 8:
		return orb.Point{(b.Max[0] + b.Min[0]) / 2, b.Max[1]}
	case 9:
		return orb.Point{b.Min[0], b.Max[1]}
	case 10:
		return orb.Point{b.Max[0], b.Max[1]}
	}

	panic("invalid code")
}

//         left  mid  right
//    top     9     8    10
//    mid     1     0     2
// bottom     5     4     6

// nexts takes a bitcode index and jumps to the next corner.
var nexts = map[orb.Orientation][11]int{
	orb.CW: {
		-1,
		9, // 1
		6, // 2
		-1,
		5, // 4
		1, // 5
		4, // 6
		-1,
		10, // 8
		8,  // 9
		2,  // 10
	},
	orb.CCW: {
		-1,
		5,  // 1
		10, // 2
		-1,
		6, // 4
		4, // 5
		2, // 6
		-1,
		9, // 8
		1, // 9
		8, // 10
	},
}
