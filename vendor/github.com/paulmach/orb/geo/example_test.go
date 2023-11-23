package geo_test

import (
	"fmt"

	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geo"
)

func ExampleArea() {
	poly := orb.Polygon{
		{
			{-122.4163816, 37.7792782},
			{-122.4162786, 37.7787626},
			{-122.4151027, 37.7789118},
			{-122.4152143, 37.7794274},
			{-122.4163816, 37.7792782},
		},
	}
	a := geo.Area(poly)

	fmt.Printf("%f m^2", a)
	// Output:
	// 6073.368008 m^2
}

func ExampleDistance() {
	oakland := orb.Point{-122.270833, 37.804444}
	sf := orb.Point{-122.416667, 37.783333}

	d := geo.Distance(oakland, sf)

	fmt.Printf("%0.3f meters", d)
	// Output:
	// 13042.047 meters
}

func ExampleLength() {

	poly := orb.Polygon{
		{
			{-122.4163816, 37.7792782},
			{-122.4162786, 37.7787626},
			{-122.4151027, 37.7789118},
			{-122.4152143, 37.7794274},
			{-122.4163816, 37.7792782},
		},
	}
	l := geo.Length(poly)

	fmt.Printf("%0.0f meters", l)
	// Output:
	// 325 meters
}
