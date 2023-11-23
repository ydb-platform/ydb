package tilecover

import (
	"testing"

	"github.com/paulmach/orb"
)

func TestRing_error(t *testing.T) {
	// not a closed ring
	f := loadFeature(t, "./testdata/line.geojson")
	l := f.Geometry.(orb.LineString)

	_, err := Ring(orb.Ring(l), 25)
	if err != ErrUnevenIntersections {
		t.Errorf("incorrect error: %v", err)
	}
}
