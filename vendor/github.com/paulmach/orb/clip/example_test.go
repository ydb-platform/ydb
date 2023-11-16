package clip_test

import (
	"fmt"

	"github.com/paulmach/orb"
	"github.com/paulmach/orb/clip"
)

func ExampleGeometry() {
	bound := orb.Bound{Min: orb.Point{0, 0}, Max: orb.Point{30, 30}}

	ls := orb.LineString{
		{-10, 10}, {10, 10}, {10, -10}, {20, -10}, {20, 10},
		{40, 10}, {40, 20}, {20, 20}, {20, 40}, {10, 40},
		{10, 20}, {5, 20}, {-10, 20}}

	// returns an orb.Geometry interface.
	clipped := clip.Geometry(bound, ls)

	fmt.Println(clipped)
	// Output:
	// [[[0 10] [10 10] [10 0]] [[20 0] [20 10] [30 10]] [[30 20] [20 20] [20 30]] [[10 30] [10 20] [5 20] [0 20]]]
}
