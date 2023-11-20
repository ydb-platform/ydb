package project

import (
	"testing"

	"github.com/paulmach/orb"
)

func TestGeometry(t *testing.T) {
	for _, g := range orb.AllGeometries {
		// should not panic with unsupported type
		Geometry(g, Mercator.ToWGS84)
	}
}
