package planar_test

import (
	"fmt"

	"github.com/paulmach/orb"
	"github.com/paulmach/orb/planar"
)

func ExampleArea() {
	// +
	// |\
	// | \
	// |  \
	// +---+

	r := orb.Ring{{0, 0}, {3, 0}, {0, 4}, {0, 0}}
	a := planar.Area(r)

	fmt.Println(a)
	// Output:
	// 6
}

func ExampleDistance() {
	d := planar.Distance(orb.Point{0, 0}, orb.Point{3, 4})

	fmt.Println(d)
	// Output:
	// 5
}

func ExampleLength() {
	// +
	// |\
	// | \
	// |  \
	// +---+

	r := orb.Ring{{0, 0}, {3, 0}, {0, 4}, {0, 0}}
	l := planar.Length(r)

	fmt.Println(l)
	// Output:
	// 12
}
