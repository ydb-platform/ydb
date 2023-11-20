package tilecover

import (
	"errors"
	"sort"

	"github.com/paulmach/orb"
	"github.com/paulmach/orb/maptile"
)

// ErrUnevenIntersections can be returned when clipping polygons
// and there are issues with the geometries, like the rings are not closed.
var ErrUnevenIntersections = errors.New("tilecover: uneven intersections, ring not closed?")

// Ring creates a tile cover for the ring.
func Ring(r orb.Ring, z maptile.Zoom) (maptile.Set, error) {
	if len(r) == 0 {
		return make(maptile.Set), nil
	}

	return Polygon(orb.Polygon{r}, z)
}

// Polygon creates a tile cover for the polygon.
func Polygon(p orb.Polygon, z maptile.Zoom) (maptile.Set, error) {
	set := make(maptile.Set)

	err := polygon(set, p, z)
	if err != nil {
		return nil, err
	}

	return set, nil
}

// MultiPolygon creates a tile cover for the multi-polygon.
func MultiPolygon(mp orb.MultiPolygon, z maptile.Zoom) (maptile.Set, error) {
	set := make(maptile.Set)
	for _, p := range mp {
		err := polygon(set, p, z)
		if err != nil {
			return nil, err
		}
	}

	return set, nil
}

func polygon(set maptile.Set, p orb.Polygon, zoom maptile.Zoom) error {
	intersections := make([][2]uint32, 0)

	for _, r := range p {
		ring := line(set, orb.LineString(r), zoom, make([][2]uint32, 0))

		pi := len(ring) - 2
		for i := range ring {
			pi = (pi + 1) % len(ring)
			ni := (i + 1) % len(ring)
			y := ring[i][1]

			// add interesction if it's not local extremum or duplicate
			if (ring[pi][1] < y || ring[ni][1] < y) && // not local minimum
				(y < ring[pi][1] || y < ring[ni][1]) && // not local maximum
				y != ring[ni][1] {

				intersections = append(intersections, ring[i])
			}
		}
	}

	if len(intersections)%2 != 0 {
		return ErrUnevenIntersections
	}

	// sort by y, then x
	sort.Slice(intersections, func(i, j int) bool {
		it := intersections[i]
		jt := intersections[j]

		if it[1] != jt[1] {
			return it[1] < jt[1]
		}

		return it[0] < jt[0]
	})

	for i := 0; i < len(intersections); i += 2 {
		// fill tiles between pairs of intersections
		y := intersections[i][1]
		for x := intersections[i][0] + 1; x < intersections[i+1][0]; x++ {
			set[maptile.New(x, y, zoom)] = true
		}
	}

	return nil
}
