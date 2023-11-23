package planar

import (
	"testing"

	"github.com/paulmach/orb"
)

func TestDistance(t *testing.T) {
	p1 := orb.Point{0, 0}
	p2 := orb.Point{3, 4}

	if d := Distance(p1, p2); d != 5 {
		t.Errorf("point, distanceFrom expected 5, got %f", d)
	}

	if d := Distance(p2, p1); d != 5 {
		t.Errorf("point, distanceFrom expected 5, got %f", d)
	}
}
