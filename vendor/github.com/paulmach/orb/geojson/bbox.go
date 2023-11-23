package geojson

import "github.com/paulmach/orb"

// BBox is for the geojson bbox attribute which is an array with all axes
// of the most southwesterly point followed by all axes of the more northeasterly point.
type BBox []float64

// NewBBox creates a bbox from a a bound.
func NewBBox(b orb.Bound) BBox {
	return []float64{
		b.Min[0], b.Min[1],
		b.Max[0], b.Max[1],
	}
}

// Valid checks if the bbox is present and has at least 4 elements.
func (bb BBox) Valid() bool {
	if bb == nil {
		return false
	}

	return len(bb) >= 4 && len(bb)%2 == 0
}

// Bound returns the orb.Bound for the BBox.
func (bb BBox) Bound() orb.Bound {
	if !bb.Valid() {
		return orb.Bound{}
	}

	mid := len(bb) / 2

	return orb.Bound{
		Min: orb.Point{bb[0], bb[1]},
		Max: orb.Point{bb[mid], bb[mid+1]},
	}
}
