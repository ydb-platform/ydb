package tilecover

import (
	"testing"

	"github.com/paulmach/orb"
)

func TestGeometry(t *testing.T) {
	for _, g := range orb.AllGeometries {
		_, err := Geometry(g, 1)
		if err != nil {
			t.Fatalf("unexpected error for %T: %v", g, err)
		}
	}
}
