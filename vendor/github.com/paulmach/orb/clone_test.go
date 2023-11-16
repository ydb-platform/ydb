package orb

import (
	"fmt"
	"testing"
)

func TestClone(t *testing.T) {
	for _, g := range AllGeometries {
		// this closure is necessary if tests are run in parallel, maybe
		func(geom Geometry) {
			t.Run(fmt.Sprintf("%T", g), func(t *testing.T) {
				// should not panic
				Clone(geom)
			})
		}(g)
	}
}
