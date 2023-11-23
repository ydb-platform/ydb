package simplify_test

import (
	"fmt"

	"github.com/paulmach/orb"
	"github.com/paulmach/orb/planar"
	"github.com/paulmach/orb/simplify"
)

func ExampleDouglasPeuckerSimplifier() {
	//  +
	//   \
	//    \
	//     +
	//      \
	//       \
	//  +-----+
	original := orb.LineString{{0, 0}, {2, 0}, {1, 1}, {0, 2}}

	// low threshold just removes the colinear point
	reduced := simplify.DouglasPeucker(0.0).Simplify(original.Clone())
	fmt.Println(reduced)

	// high threshold just leaves start and end
	reduced = simplify.DouglasPeucker(2).Simplify(original)
	fmt.Println(reduced)

	// Output:
	// [[0 0] [2 0] [0 2]]
	// [[0 0] [0 2]]
}

func ExampleRadialSimplifier() {
	//  +
	//   \
	//    \
	//     +
	//     |
	//  +--+
	original := orb.LineString{{0, 0}, {1, 0}, {1, 1}, {0, 2}}

	// will remove the points within 1.0 of the previous point
	// in this case just the second point
	reduced := simplify.Radial(planar.Distance, 1.0).Simplify(original.Clone())
	fmt.Println(reduced)

	// will remove the 2nd and 3rd point since it's within 1.5 or the first point.
	reduced = simplify.Radial(planar.Distance, 1.5).Simplify(original)
	fmt.Println(reduced)
}
