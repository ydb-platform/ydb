package resample_test

import (
	"fmt"

	"github.com/paulmach/orb"
	"github.com/paulmach/orb/planar"
	"github.com/paulmach/orb/resample"
)

func ExampleResample() {
	ls := orb.LineString{
		{0, 0}, {1, 0}, {2, 0}, {3, 0}, {4, 0},
		{5, 0}, {6, 0}, {7, 0}, {8, 0}, {9, 0}, {10, 0},
	}

	// downsample in this case so there are 2.5 units between points
	ls = resample.ToInterval(ls, planar.Distance, 2.5)
	fmt.Println(ls)

	// Output:
	// [[0 0] [2.5 0] [5 0] [7.5 0] [10 0]]
}

func ExampleToInterval() {
	ls := orb.LineString{{0, 0}, {10, 0}}

	// upsample so there are 6 total points
	ls = resample.Resample(ls, planar.Distance, 6)
	fmt.Println(ls)

	// Output:
	// [[0 0] [2 0] [4 0] [6 0] [8 0] [10 0]]
}
