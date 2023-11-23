package project

import (
	"math"
	"testing"

	"github.com/paulmach/orb/internal/mercator"
)

func TestDefineDeg2Rad(t *testing.T) {
	if math.Abs(deg2rad(0.0)) > mercator.Epsilon {
		t.Error("define, deg2rad error")
	}

	if math.Abs(deg2rad(180.0)-math.Pi) > mercator.Epsilon {
		t.Error("define, deg2rad error")
	}

	if math.Abs(deg2rad(360.0)-2*math.Pi) > mercator.Epsilon {
		t.Error("define, deg2rad error")
	}
}

func TestDefineRad2Deg(t *testing.T) {
	if math.Abs(rad2deg(0.0)-0.0) > mercator.Epsilon {
		t.Error("define, rad2deg error")
	}

	if math.Abs(rad2deg(math.Pi)-180.0) > mercator.Epsilon {
		t.Error("define, rad2deg error")
	}

	if math.Abs(rad2deg(2*math.Pi)-360.0) > mercator.Epsilon {
		t.Error("define, rad2deg error")
	}
}
