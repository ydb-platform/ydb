package tilecover

import "github.com/paulmach/orb/maptile"

// MergeUp will merge up the tiles in a given set up to the
// the give min zoom. Tiles will be merged up only if all 4 siblings
// are in the set. The tiles in the input set are expected
// to all be of the same zoom, e.g. outputs of the Geometry function.
func MergeUp(set maptile.Set, min maptile.Zoom) maptile.Set {
	max := maptile.Zoom(1)
	for t, v := range set {
		if v {
			max = t.Z
			break
		}
	}

	if min == max {
		return set
	}

	merged := make(maptile.Set)
	for z := max; z > min; z-- {
		parentSet := make(maptile.Set)
		for t, v := range set {
			if !v {
				continue
			}

			sibs := t.Siblings()
			s0 := set[sibs[0]]
			s1 := set[sibs[1]]
			s2 := set[sibs[2]]
			s3 := set[sibs[3]]
			if s0 && s1 && s2 && s3 {
				set[sibs[0]] = false
				set[sibs[1]] = false
				set[sibs[2]] = false
				set[sibs[3]] = false

				parent := t.Parent()
				if z-1 == min {
					merged[parent] = true
				} else {
					parentSet[parent] = true
				}
			} else {
				if s0 {
					merged[sibs[0]] = true
					set[sibs[0]] = false
				}
				if s1 {
					merged[sibs[1]] = true
					set[sibs[1]] = false
				}
				if s2 {
					merged[sibs[2]] = true
					set[sibs[2]] = false
				}
				if s3 {
					merged[sibs[3]] = true
					set[sibs[3]] = false
				}
			}
		}

		set = parentSet
		if len(set) < 4 {
			for t := range set {
				merged[t] = true
			}
			break
		}
	}

	return merged
}

// MergeUpPartial will merge up the tiles in a given set up to the
// the give min zoom. Tiles will be merged up if `count` siblings are in the
// set. The tiles in the input set are expected to all be of the same
// zoom, e.g. outputs of the Geometry function.
func MergeUpPartial(set maptile.Set, min maptile.Zoom, count int) maptile.Set {
	max := maptile.Zoom(1)
	for t, v := range set {
		if v {
			max = t.Z
			break
		}
	}

	if min == max {
		return set
	}

	merged := make(maptile.Set)
	for z := max; z > min; z-- {
		parentSet := make(maptile.Set)
		for t, v := range set {
			if !v {
				continue
			}

			sibs := t.Siblings()
			s0 := set[sibs[0]]
			s1 := set[sibs[1]]
			s2 := set[sibs[2]]
			s3 := set[sibs[3]]

			c := 0
			if s0 {
				c++
			}
			if s1 {
				c++
			}
			if s2 {
				c++
			}
			if s3 {
				c++
			}

			if c >= count {
				set[sibs[0]] = false
				set[sibs[1]] = false
				set[sibs[2]] = false
				set[sibs[3]] = false

				parent := t.Parent()
				if z-1 == min {
					merged[parent] = true
				} else {
					parentSet[parent] = true
				}
			} else {
				if s0 {
					merged[sibs[0]] = true
					set[sibs[0]] = false
				}
				if s1 {
					merged[sibs[1]] = true
					set[sibs[1]] = false
				}
				if s2 {
					merged[sibs[2]] = true
					set[sibs[2]] = false
				}
				if s3 {
					merged[sibs[3]] = true
					set[sibs[3]] = false
				}
			}
		}

		set = parentSet
		if len(set) < count {
			for t := range set {
				merged[t] = true
			}
			break
		}
	}

	return merged
}
