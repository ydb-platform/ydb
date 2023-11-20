package orb

import (
	"fmt"
	"testing"
)

func TestEqual(t *testing.T) {
	for _, g := range AllGeometries {
		// this closure is necessary if tests are run in parallel, maybe
		func(geom Geometry) {
			t.Run(fmt.Sprintf("%T", g), func(t *testing.T) {
				if !Equal(geom, geom) {
					t.Errorf("%T not equal", g)
				}
			})
		}(g)
	}
}

func TestEqualRing(t *testing.T) {
	if Equal(Ring{}, LineString{}) {
		t.Errorf("should return false since different types")
	}

	if Equal(Ring{}, Polygon{}) {
		t.Errorf("should return false since different types")
	}

	if Equal(Polygon{}, Ring{}) {
		t.Errorf("should return false since different types")
	}

	if Equal(Polygon{}, Bound{}) {
		t.Errorf("should return false since different types")
	}

	if Equal(Bound{}, Polygon{}) {
		t.Errorf("should return false since different types")
	}
}
