package orb

import (
	"fmt"
	"testing"
)

func TestGeometryDimensions(t *testing.T) {
	cases := []struct {
		Geometry   Geometry
		Dimensions int
	}{
		{Point{}, 0},
		{MultiPoint{}, 0},
		{LineString{}, 1},
		{MultiLineString{}, 1},
		{Ring{}, 2},
		{Polygon{}, 2},
		{MultiPolygon{}, 2},
		{Bound{}, 2},
		{Collection{Point{}, LineString{}}, 1},
	}

	for _, tc := range cases {
		t.Run(fmt.Sprintf("type: %T", tc.Geometry), func(t *testing.T) {
			if v := tc.Geometry.Dimensions(); v != tc.Dimensions {
				t.Errorf("incorrect dimensions: %v", v)
			}
		})
	}
}

func TestCollectionBound(t *testing.T) {
	// from the empty Point we get the zero bound.
	expected := Bound{}

	b2 := Collection(AllGeometries).Bound()
	if !b2.Equal(expected) {
		t.Errorf("wrong bound: %v != %v", b2, expected)
	}
}
