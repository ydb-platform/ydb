package geo

import (
	"testing"

	"github.com/paulmach/orb"
)

func TestLength(t *testing.T) {
	for _, g := range orb.AllGeometries {
		// should not panic with unsupported type
		Length(g)
	}
}

func TestLengthHaversine(t *testing.T) {
	for _, g := range orb.AllGeometries {
		// should not panic with unsupported type
		LengthHaversine(g)
	}
}
