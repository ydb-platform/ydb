package maptile

import (
	"github.com/paulmach/orb/geojson"
)

// Set is a map/hash of tiles.
type Set map[Tile]bool

// ToFeatureCollection converts a set of tiles into a feature collection.
// This method is mostly useful for debugging output.
func (s Set) ToFeatureCollection() *geojson.FeatureCollection {
	fc := geojson.NewFeatureCollection()
	fc.Features = make([]*geojson.Feature, 0, len(s))
	for t := range s {
		fc.Append(geojson.NewFeature(t.Bound().ToPolygon()))
	}

	return fc
}

// Merge will merge the given set into the existing set.
func (s Set) Merge(set Set) {
	for t, v := range set {
		if v {
			s[t] = true
		}
	}
}
