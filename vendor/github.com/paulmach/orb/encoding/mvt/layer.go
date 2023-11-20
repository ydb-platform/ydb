package mvt

import (
	"github.com/paulmach/orb/geojson"
	"github.com/paulmach/orb/maptile"
	"github.com/paulmach/orb/project"
)

const (
	// DefaultExtent for mapbox vector tiles. (https://www.mapbox.com/vector-tiles/specification/)
	DefaultExtent = 4096
)

// Layer is intermediate MVT layer to be encoded/decoded or projected.
type Layer struct {
	Name     string
	Version  uint32
	Extent   uint32
	Features []*geojson.Feature
}

// NewLayer is a helper to create a Layer from a feature collection
// and a name, it sets the default extent and version to 1.
func NewLayer(name string, fc *geojson.FeatureCollection) *Layer {
	return &Layer{
		Name:     name,
		Version:  1,
		Extent:   DefaultExtent,
		Features: fc.Features,
	}
}

// ProjectToTile will project all the geometries in the layer
// to tile coordinates based on the extent and the mercator projection.
func (l *Layer) ProjectToTile(tile maptile.Tile) {
	p := newProjection(tile, l.Extent)
	for _, f := range l.Features {
		f.Geometry = project.Geometry(f.Geometry, p.ToTile)
	}
}

// ProjectToWGS84 will project all the geometries backed to WGS84 from
// the extent and mercator projection.
func (l *Layer) ProjectToWGS84(tile maptile.Tile) {
	p := newProjection(tile, l.Extent)
	for _, f := range l.Features {
		f.Geometry = project.Geometry(f.Geometry, p.ToWGS84)
	}
}

// Layers is a set of layers.
type Layers []*Layer

// NewLayers creates a set of layers given a set of feature collections.
func NewLayers(layers map[string]*geojson.FeatureCollection) Layers {
	result := make(Layers, 0, len(layers))
	for name, fc := range layers {
		result = append(result, NewLayer(name, fc))
	}

	return result
}

// ToFeatureCollections converts the layers to sets of geojson
// feature collections.
func (ls Layers) ToFeatureCollections() map[string]*geojson.FeatureCollection {
	result := make(map[string]*geojson.FeatureCollection, len(ls))
	for _, l := range ls {
		result[l.Name] = &geojson.FeatureCollection{
			Features: l.Features,
		}
	}

	return result
}

// ProjectToTile will project all the geometries in all layers
// to tile coordinates based on the extent and the mercator projection.
func (ls Layers) ProjectToTile(tile maptile.Tile) {
	for _, l := range ls {
		l.ProjectToTile(tile)
	}
}

// ProjectToWGS84 will project all the geometries in all the layers backed
// to WGS84 from the extent and mercator projection.
func (ls Layers) ProjectToWGS84(tile maptile.Tile) {
	for _, l := range ls {
		l.ProjectToWGS84(tile)
	}
}
