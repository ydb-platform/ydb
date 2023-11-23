package mvt

import (
	"math"
	"testing"

	"github.com/paulmach/orb/maptile"
	"github.com/paulmach/orb/project"
)

func TestPowerOfTwoProjection(t *testing.T) {
	const epsilon = 1e-6

	// verify tile coord does not overflow int32
	tile := maptile.New(1730576, 798477, 21)
	proj := newProjection(tile, 4096)

	center := tile.Center()
	planar := proj.ToTile(center)

	if planar[0] != 2048 {
		t.Errorf("incorrect lon projection: %v", planar[0])
	}

	if planar[1] != 2048 {
		t.Errorf("incorrect lat projection: %v", planar[1])
	}

	geo := proj.ToWGS84(planar)
	if math.Abs(geo[0]-center[0]) > epsilon {
		t.Errorf("lon miss match: %f != %f", geo[0], center[0])
	}

	if math.Abs(geo[1]-center[1]) > epsilon {
		t.Errorf("lat miss match: %f != %f", geo[1], center[1])
	}
}

func TestNonPowerOfTwoProjection(t *testing.T) {
	tile := maptile.New(8956, 12223, 15)
	regProj := newProjection(tile, 4096)
	nonProj := nonPowerOfTwoProjection(tile, 4096)

	expected := loadGeoJSON(t, tile)
	layers := NewLayers(loadGeoJSON(t, tile))

	// loopy de loop of projections
	for _, l := range layers {
		for _, f := range l.Features {
			f.Geometry = project.Geometry(f.Geometry, regProj.ToTile)
		}
	}

	for _, l := range layers {
		for _, f := range l.Features {
			f.Geometry = project.Geometry(f.Geometry, nonProj.ToWGS84)
		}
	}

	for _, l := range layers {
		for _, f := range l.Features {
			f.Geometry = project.Geometry(f.Geometry, nonProj.ToTile)
		}
	}

	for _, l := range layers {
		for _, f := range l.Features {
			f.Geometry = project.Geometry(f.Geometry, regProj.ToWGS84)
		}
	}

	result := layers.ToFeatureCollections()

	xEpsilon, yEpsilon := tileEpsilon(tile)
	for key := range expected {
		for i := range expected[key].Features {
			r := result[key].Features[i]
			e := expected[key].Features[i]

			compareOrbGeometry(t, r.Geometry, e.Geometry, xEpsilon, yEpsilon)
		}
	}
}
