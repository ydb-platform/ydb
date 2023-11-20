package geojson_test

import (
	"fmt"
	"log"

	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geojson"
	"github.com/paulmach/orb/planar"
	"github.com/paulmach/orb/quadtree"
)

type CentroidPoint struct {
	*geojson.Feature
}

func (cp CentroidPoint) Point() orb.Point {
	// this is where you would decide how to define
	// the representative point of the feature.
	c, _ := planar.CentroidArea(cp.Geometry)
	return c
}

func Example_centroid() {
	qt := quadtree.New(orb.Bound{Min: orb.Point{0, 0}, Max: orb.Point{1, 1}})

	// feature with center {0.5, 0.5} but centroid {0.25, 0.25}
	f := geojson.NewFeature(orb.MultiPoint{{0, 0}, {0, 0}, {0, 0}, {1, 1}})
	f.Properties["centroid"] = "0.25"
	err := qt.Add(CentroidPoint{f})
	if err != nil {
		log.Fatalf("unexpected error: %v", err)
	}

	// feature with centroid {0.6, 0.6}
	f = geojson.NewFeature(orb.Point{0.6, 0.6})
	f.Properties["centroid"] = "0.6"
	err = qt.Add(CentroidPoint{f})
	if err != nil {
		log.Fatalf("unexpected error: %v", err)
	}

	feature := qt.Find(orb.Point{0.5, 0.5}).(CentroidPoint).Feature
	fmt.Printf("centroid=%s", feature.Properties["centroid"])

	// Output:
	// centroid=0.6
}
