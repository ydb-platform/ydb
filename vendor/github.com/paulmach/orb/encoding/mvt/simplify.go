package mvt

import (
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/planar"
)

// Simplify will run all the geometry of all the layers through the provided simplifer.
func (ls Layers) Simplify(s orb.Simplifier) {
	for _, l := range ls {
		l.Simplify(s)
	}
}

// Simplify will run the layer geometries through the simplifier.
func (l *Layer) Simplify(s orb.Simplifier) {
	count := 0
	for _, f := range l.Features {
		g := s.Simplify(f.Geometry)
		if g == nil {
			continue
		}

		f.Geometry = g
		l.Features[count] = f
		count++
	}

	l.Features = l.Features[:count]
}

// RemoveEmpty will remove line strings shorter/smaller than the limits.
func (ls Layers) RemoveEmpty(lineLimit, areaLimit float64) {
	for _, l := range ls {
		l.RemoveEmpty(lineLimit, areaLimit)
	}
}

// RemoveEmpty will remove line strings shorter/smaller than the limits.
func (l *Layer) RemoveEmpty(lineLimit, areaLimit float64) {
	count := 0
	for i := 0; i < len(l.Features); i++ {
		f := l.Features[i]
		if f.Geometry == nil {
			continue
		}

		switch f.Geometry.Dimensions() {
		case 0: // point geometry
			l.Features[count] = f
			count++
		case 1: // line geometry
			length := planar.Length(f.Geometry)
			if length >= lineLimit {
				l.Features[count] = f
				count++
			}
		case 2:
			area := planar.Area(f.Geometry)
			if area >= areaLimit {
				l.Features[count] = f
				count++
			}
		}
	}

	l.Features = l.Features[:count]
}
