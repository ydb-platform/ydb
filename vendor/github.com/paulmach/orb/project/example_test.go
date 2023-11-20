package project_test

import (
	"fmt"

	"github.com/paulmach/orb"
	"github.com/paulmach/orb/planar"
	"github.com/paulmach/orb/project"
)

func ExamplePoint_toMercator() {
	sf := orb.Point{-122.416667, 37.783333}
	merc := project.Geometry(sf, project.WGS84.ToMercator)

	fmt.Println(merc)
	// Output:
	// [-1.3627361035049736e+07 4.548863085837512e+06]
}

func ExamplePolygon() {
	poly := orb.Polygon{
		{
			{-122.4163816, 37.7792782},
			{-122.4162786, 37.7787626},
			{-122.4151027, 37.7789118},
			{-122.4152143, 37.7794274},
			{-122.4163816, 37.7792782},
		},
	}

	merc := project.Polygon(poly, project.WGS84.ToMercator)
	centroid, _ := planar.CentroidArea(merc)

	centroid = project.Mercator.ToWGS84(centroid)
	fmt.Println(centroid)
	// Output:
	// [-122.41574403384001 37.77909471899779]
}
