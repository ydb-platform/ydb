package quadtree_test

import (
	"fmt"
	"math/rand"

	"github.com/paulmach/orb"
	"github.com/paulmach/orb/quadtree"
)

func ExampleQuadtree_Find() {
	r := rand.New(rand.NewSource(42)) // to make things reproducible

	qt := quadtree.New(orb.Bound{Min: orb.Point{0, 0}, Max: orb.Point{1, 1}})

	// add 1000 random points
	for i := 0; i < 1000; i++ {
		err := qt.Add(orb.Point{r.Float64(), r.Float64()})
		if err != nil {
			panic(err)
		}
	}

	nearest := qt.Find(orb.Point{0.5, 0.5})
	fmt.Printf("nearest: %+v\n", nearest)

	// Output:
	// nearest: [0.4930591659434973 0.5196585530161364]
}

func ExampleQuadtree_Matching() {
	r := rand.New(rand.NewSource(42)) // to make things reproducible

	type dataPoint struct {
		orb.Pointer
		visible bool
	}

	qt := quadtree.New(orb.Bound{Min: orb.Point{0, 0}, Max: orb.Point{1, 1}})

	// add 100 random points
	for i := 0; i < 100; i++ {
		err := qt.Add(dataPoint{orb.Point{r.Float64(), r.Float64()}, false})
		if err != nil {
			panic(err)
		}
	}

	err := qt.Add(dataPoint{orb.Point{0, 0}, true})
	if err != nil {
		panic(err)
	}

	nearest := qt.Matching(
		orb.Point{0.5, 0.5},
		func(p orb.Pointer) bool { return p.(dataPoint).visible },
	)

	fmt.Printf("nearest: %+v\n", nearest)

	// Output:
	// nearest: {Pointer:[0 0] visible:true}
}

func ExampleQuadtree_InBound() {
	r := rand.New(rand.NewSource(52)) // to make things reproducible

	qt := quadtree.New(orb.Bound{Min: orb.Point{0, 0}, Max: orb.Point{1, 1}})

	// add 1000 random points
	for i := 0; i < 1000; i++ {
		err := qt.Add(orb.Point{r.Float64(), r.Float64()})
		if err != nil {
			panic(err)
		}
	}

	bounded := qt.InBound(nil, orb.Point{0.5, 0.5}.Bound().Pad(0.05))
	fmt.Printf("in bound: %v\n", len(bounded))

	// Output:
	// in bound: 10
}
