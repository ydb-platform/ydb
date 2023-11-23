// Package resample has a couple functions for resampling line geometry
// into more or less evenly spaces points.
package resample

import (
	"github.com/paulmach/orb"
)

// Resample converts the line string into totalPoints-1 evenly spaced segments.
// This function will modify the linestring input.
func Resample(ls orb.LineString, df orb.DistanceFunc, totalPoints int) orb.LineString {
	if totalPoints <= 0 {
		return nil
	}

	ls, ret := resampleEdgeCases(ls, totalPoints)
	if ret {
		return ls
	}

	// precomputes the total distance and intermediate distances
	total, dists := precomputeDistances(ls, df)
	return resample(ls, dists, total, totalPoints)
}

// ToInterval coverts the line string into evenly spaced points of
// about the given distance.
// This function will modify the linestring input.
func ToInterval(ls orb.LineString, df orb.DistanceFunc, dist float64) orb.LineString {
	if dist <= 0 {
		return nil
	}

	// precomputes the total distance and intermediate distances
	total, dists := precomputeDistances(ls, df)

	totalPoints := int(total/dist) + 1
	ls, ret := resampleEdgeCases(ls, totalPoints)
	if ret {
		return ls
	}

	return resample(ls, dists, total, totalPoints)
}

func resample(ls orb.LineString, dists []float64, totalDistance float64, totalPoints int) orb.LineString {
	points := make([]orb.Point, 1, totalPoints)
	points[0] = ls[0] // start stays the same

	step := 1
	dist := 0.0

	currentDistance := totalDistance / float64(totalPoints-1)
	// declare here and update had nice performance benefits need to retest
	currentSeg := [2]orb.Point{}
	for i := 0; i < len(ls)-1; i++ {
		currentSeg[0] = ls[i]
		currentSeg[1] = ls[i+1]

		currentSegDistance := dists[i]
		nextDistance := dist + currentSegDistance

		for currentDistance <= nextDistance {
			// need to add a point
			percent := (currentDistance - dist) / currentSegDistance
			points = append(points, orb.Point{
				currentSeg[0][0] + percent*(currentSeg[1][0]-currentSeg[0][0]),
				currentSeg[0][1] + percent*(currentSeg[1][1]-currentSeg[0][1]),
			})

			// move to the next distance we want
			step++
			currentDistance = totalDistance * float64(step) / float64(totalPoints-1)
			if step == totalPoints-1 { // weird round off error on my machine
				currentDistance = totalDistance
			}
		}

		// past the current point in the original segment, so move to the next one
		dist = nextDistance
	}

	// end stays the same, to handle round off errors
	if totalPoints != 1 { // for 1, we want the first point
		points[totalPoints-1] = ls[len(ls)-1]
	}

	return orb.LineString(points)
}

// resampleEdgeCases is used to handle edge case for
// resampling like not enough points and the line string is all the same point.
// will return nil if there are no edge cases. If return true if
// one of these edge cases was found and handled.
func resampleEdgeCases(ls orb.LineString, totalPoints int) (orb.LineString, bool) {
	// degenerate case
	if len(ls) <= 1 {
		return ls, true
	}

	// if all the points are the same, treat as special case.
	equal := true
	for _, point := range ls {
		if !ls[0].Equal(point) {
			equal = false
			break
		}
	}

	if equal {
		if totalPoints > len(ls) {
			// extend to be requested length
			for len(ls) != totalPoints {
				ls = append(ls, ls[0])
			}

			return ls, true
		}

		// contract to be requested length
		ls = ls[:totalPoints]
		return ls, true
	}

	return ls, false
}

// precomputeDistances precomputes the total distance and intermediate distances.
func precomputeDistances(ls orb.LineString, df orb.DistanceFunc) (float64, []float64) {
	total := 0.0
	dists := make([]float64, len(ls)-1)
	for i := 0; i < len(ls)-1; i++ {
		dists[i] = df(ls[i], ls[i+1])
		total += dists[i]
	}

	return total, dists
}
