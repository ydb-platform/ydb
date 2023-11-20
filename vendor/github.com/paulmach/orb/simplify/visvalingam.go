package simplify

import (
	"math"

	"github.com/paulmach/orb"
)

var _ orb.Simplifier = &VisvalingamSimplifier{}

// A VisvalingamSimplifier is a reducer that
// performs the vivalingham algorithm.
type VisvalingamSimplifier struct {
	Threshold float64
	ToKeep    int
}

// Visvalingam creates a new VisvalingamSimplifier.
func Visvalingam(threshold float64, minPointsToKeep int) *VisvalingamSimplifier {
	return &VisvalingamSimplifier{
		Threshold: threshold,
		ToKeep:    minPointsToKeep,
	}
}

// VisvalingamThreshold runs the Visvalingam-Whyatt algorithm removing
// triangles whose area is below the threshold.
func VisvalingamThreshold(threshold float64) *VisvalingamSimplifier {
	return Visvalingam(threshold, 0)
}

// VisvalingamKeep runs the Visvalingam-Whyatt algorithm removing
// triangles of minimum area until we're down to `toKeep` number of points.
func VisvalingamKeep(toKeep int) *VisvalingamSimplifier {
	return Visvalingam(math.MaxFloat64, toKeep)
}

func (s *VisvalingamSimplifier) simplify(ls orb.LineString, wim bool) (orb.LineString, []int) {
	var indexMap []int
	if len(ls) <= s.ToKeep {
		if wim {
			// create identify map
			indexMap = make([]int, len(ls))
			for i := range ls {
				indexMap[i] = i
			}
		}
		return ls, indexMap
	}

	// edge cases checked, get on with it
	threshold := s.Threshold * 2 // triangle area is doubled to save the multiply :)
	removed := 0

	// build the initial minheap linked list.
	heap := minHeap(make([]*visItem, 0, len(ls)))

	linkedListStart := &visItem{
		area:       math.Inf(1),
		pointIndex: 0,
	}
	heap.Push(linkedListStart)

	// internal path items
	items := make([]visItem, len(ls))

	previous := linkedListStart
	for i := 1; i < len(ls)-1; i++ {
		item := &items[i]

		item.area = doubleTriangleArea(ls, i-1, i, i+1)
		item.pointIndex = i
		item.previous = previous

		heap.Push(item)
		previous.next = item
		previous = item
	}

	// final item
	endItem := &items[len(ls)-1]
	endItem.area = math.Inf(1)
	endItem.pointIndex = len(ls) - 1
	endItem.previous = previous

	previous.next = endItem
	heap.Push(endItem)

	// run through the reduction process
	for len(heap) > 0 {
		current := heap.Pop()
		if current.area > threshold || len(ls)-removed <= s.ToKeep {
			break
		}

		next := current.next
		previous := current.previous

		// remove current element from linked list
		previous.next = current.next
		next.previous = current.previous
		removed++

		// figure out the new areas
		if previous.previous != nil {
			area := doubleTriangleArea(ls,
				previous.previous.pointIndex,
				previous.pointIndex,
				next.pointIndex,
			)

			area = math.Max(area, current.area)
			heap.Update(previous, area)
		}

		if next.next != nil {
			area := doubleTriangleArea(ls,
				previous.pointIndex,
				next.pointIndex,
				next.next.pointIndex,
			)

			area = math.Max(area, current.area)
			heap.Update(next, area)
		}
	}

	item := linkedListStart

	count := 0
	for item != nil {
		ls[count] = ls[item.pointIndex]
		count++

		if wim {
			indexMap = append(indexMap, item.pointIndex)
		}
		item = item.next
	}

	return ls[:count], indexMap
}

// Stuff to create the priority queue, or min heap.
// Rewriting it here, vs using the std lib, resulted in a 50% performance bump!
type minHeap []*visItem

type visItem struct {
	area       float64 // triangle area
	pointIndex int     // index of point in original path

	// to keep a virtual linked list to help rebuild the triangle areas as we remove points.
	next     *visItem
	previous *visItem

	index int // interal index in heap, for removal and update
}

func (h *minHeap) Push(item *visItem) {
	item.index = len(*h)
	*h = append(*h, item)
	h.up(item.index)
}

func (h *minHeap) Pop() *visItem {
	removed := (*h)[0]
	lastItem := (*h)[len(*h)-1]
	(*h) = (*h)[:len(*h)-1]

	if len(*h) > 0 {
		lastItem.index = 0
		(*h)[0] = lastItem
		h.down(0)
	}

	return removed
}

func (h minHeap) Update(item *visItem, area float64) {
	if item.area > area {
		// area got smaller
		item.area = area
		h.up(item.index)
	} else {
		// area got larger
		item.area = area
		h.down(item.index)
	}
}

func (h minHeap) up(i int) {
	object := h[i]
	for i > 0 {
		up := ((i + 1) >> 1) - 1
		parent := h[up]

		if parent.area <= object.area {
			// parent is smaller so we're done fixing up the heap.
			break
		}

		// swap nodes
		parent.index = i
		h[i] = parent

		object.index = up
		h[up] = object

		i = up
	}
}

func (h minHeap) down(i int) {
	object := h[i]
	for {
		right := (i + 1) << 1
		left := right - 1

		down := i
		child := h[down]

		// swap with smallest child
		if left < len(h) && h[left].area < child.area {
			down = left
			child = h[down]
		}

		if right < len(h) && h[right].area < child.area {
			down = right
			child = h[down]
		}

		// non smaller, so quit
		if down == i {
			break
		}

		// swap the nodes
		child.index = i
		h[child.index] = child

		object.index = down
		h[down] = object

		i = down
	}
}

func doubleTriangleArea(ls orb.LineString, i1, i2, i3 int) float64 {
	a := ls[i1]
	b := ls[i2]
	c := ls[i3]

	return math.Abs((b[0]-a[0])*(c[1]-a[1]) - (b[1]-a[1])*(c[0]-a[0]))
}

// Simplify will run the simplification for any geometry type.
func (s *VisvalingamSimplifier) Simplify(g orb.Geometry) orb.Geometry {
	return simplify(s, g)
}

// LineString will simplify the linestring using this simplifier.
func (s *VisvalingamSimplifier) LineString(ls orb.LineString) orb.LineString {
	return lineString(s, ls)
}

// MultiLineString will simplify the multi-linestring using this simplifier.
func (s *VisvalingamSimplifier) MultiLineString(mls orb.MultiLineString) orb.MultiLineString {
	return multiLineString(s, mls)
}

// Ring will simplify the ring using this simplifier.
func (s *VisvalingamSimplifier) Ring(r orb.Ring) orb.Ring {
	return ring(s, r)
}

// Polygon will simplify the polygon using this simplifier.
func (s *VisvalingamSimplifier) Polygon(p orb.Polygon) orb.Polygon {
	return polygon(s, p)
}

// MultiPolygon will simplify the multi-polygon using this simplifier.
func (s *VisvalingamSimplifier) MultiPolygon(mp orb.MultiPolygon) orb.MultiPolygon {
	return multiPolygon(s, mp)
}

// Collection will simplify the collection using this simplifier.
func (s *VisvalingamSimplifier) Collection(c orb.Collection) orb.Collection {
	return collection(s, c)
}
