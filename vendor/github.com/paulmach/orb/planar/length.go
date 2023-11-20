package planar

import (
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/internal/length"
)

// Length returns the length of the boundary of the geometry
// using 2d euclidean geometry.
func Length(g orb.Geometry) float64 {
	return length.Length(g, Distance)
}
