// Package quadtree implements a quadtree using rectangular partitions.
// Each point exists in a unique node in the tree or as leaf nodes.
// This implementation is based off of the d3 implementation:
// https://github.com/mbostock/d3/wiki/Quadtree-Geom
package quadtree

import (
	"errors"
	"math"

	"github.com/paulmach/orb"
	"github.com/paulmach/orb/planar"
)

var (
	// ErrPointOutsideOfBounds is returned when trying to add a point
	// to a quadtree and the point is outside the bounds used to create the tree.
	ErrPointOutsideOfBounds = errors.New("quadtree: point outside of bounds")
)

// Quadtree implements a two-dimensional recursive spatial subdivision
// of orb.Pointers. This implementation uses rectangular partitions.
type Quadtree struct {
	bound orb.Bound
	root  *node
}

// A FilterFunc is a function that filters the points to search for.
type FilterFunc func(p orb.Pointer) bool

// node represents a node of the quad tree. Each node stores a Value
// and has links to its 4 children
type node struct {
	Value    orb.Pointer
	Children [4]*node
}

// New creates a new quadtree for the given bound. Added points
// must be within this bound.
func New(bound orb.Bound) *Quadtree {
	return &Quadtree{bound: bound}
}

// Bound returns the bounds used for the quad tree.
func (q *Quadtree) Bound() orb.Bound {
	return q.bound
}

// Add puts an object into the quad tree, must be within the quadtree bounds.
// This function is not thread-safe, ie. multiple goroutines cannot insert into
// a single quadtree.
func (q *Quadtree) Add(p orb.Pointer) error {
	if p == nil {
		return nil
	}

	point := p.Point()
	if !q.bound.Contains(point) {
		return ErrPointOutsideOfBounds
	}

	if q.root == nil {
		q.root = &node{
			Value: p,
		}
		return nil
	} else if q.root.Value == nil {
		q.root.Value = p
		return nil
	}

	q.add(q.root, p, p.Point(),
		// q.bound.Left(), q.bound.Right(),
		// q.bound.Bottom(), q.bound.Top(),
		q.bound.Min[0], q.bound.Max[0],
		q.bound.Min[1], q.bound.Max[1],
	)

	return nil
}

// add is the recursive search to find a place to add the point
func (q *Quadtree) add(n *node, p orb.Pointer, point orb.Point, left, right, bottom, top float64) {
	i := 0

	// figure which child of this internal node the point is in.
	if cy := (bottom + top) / 2.0; point[1] <= cy {
		top = cy
		i = 2
	} else {
		bottom = cy
	}

	if cx := (left + right) / 2.0; point[0] >= cx {
		left = cx
		i++
	} else {
		right = cx
	}

	if n.Children[i] == nil {
		n.Children[i] = &node{Value: p}
		return
	} else if n.Children[i].Value == nil {
		n.Children[i].Value = p
		return
	}

	// proceed down to the child to see if it's a leaf yet and we can add the pointer there.
	q.add(n.Children[i], p, point, left, right, bottom, top)
}

// Remove will remove the pointer from the quadtree. By default it'll match
// using the points, but a FilterFunc can be provided for a more specific test
// if there are elements with the same point value in the tree. For example:
//
//	func(pointer orb.Pointer) {
//		return pointer.(*MyType).ID == lookingFor.ID
//	}
func (q *Quadtree) Remove(p orb.Pointer, eq FilterFunc) bool {
	if eq == nil {
		point := p.Point()
		eq = func(pointer orb.Pointer) bool {
			return point.Equal(pointer.Point())
		}
	}

	b := q.bound
	v := &findVisitor{
		point:          p.Point(),
		filter:         eq,
		closestBound:   &b,
		minDistSquared: math.MaxFloat64,
	}

	newVisit(v).Visit(q.root,
		// q.bound.Left(), q.bound.Right(),
		// q.bound.Bottom(), q.bound.Top(),
		q.bound.Min[0], q.bound.Max[0],
		q.bound.Min[1], q.bound.Max[1],
	)

	if v.closest == nil {
		return false
	}

	v.closest.Value = nil

	// if v.closest is NOT a leaf node, values will be shuffled up into this node.
	// if v.closest IS a leaf node, the call is a no-op but we can't delete
	// the now empty node because we don't know the parent here.
	//
	// Future adds will reuse this node if applicable.
	// Removing v.closest parent will cause this node to be removed,
	// but the parent will be a leaf with a nil value.
	removeNode(v.closest)
	return true
}

// removeNode is the recursive fixing up of the tree when we remove a node.
// It will pull up a child value into it's place. It will try to remove leaf nodes
// that are now empty, since their values got pulled up.
func removeNode(n *node) bool {
	i := -1
	if n.Children[0] != nil {
		i = 0
	} else if n.Children[1] != nil {
		i = 1
	} else if n.Children[2] != nil {
		i = 2
	} else if n.Children[3] != nil {
		i = 3
	}

	if i == -1 {
		// all children are nil, can remove.
		// n.value ==  nil because it "pulled up" (or removed) by the caller.
		return true
	}

	n.Value = n.Children[i].Value
	n.Children[i].Value = nil

	removeThisChild := removeNode(n.Children[i])
	if removeThisChild {
		n.Children[i] = nil
	}

	return false
}

// Find returns the closest Value/Pointer in the quadtree.
// This function is thread safe. Multiple goroutines can read from
// a pre-created tree.
func (q *Quadtree) Find(p orb.Point) orb.Pointer {
	return q.Matching(p, nil)
}

// Matching returns the closest Value/Pointer in the quadtree for which
// the given filter function returns true. This function is thread safe.
// Multiple goroutines can read from a pre-created tree.
func (q *Quadtree) Matching(p orb.Point, f FilterFunc) orb.Pointer {
	if q.root == nil {
		return nil
	}

	b := q.bound
	v := &findVisitor{
		point:          p,
		filter:         f,
		closestBound:   &b,
		minDistSquared: math.MaxFloat64,
	}

	newVisit(v).Visit(q.root,
		// q.bound.Left(), q.bound.Right(),
		// q.bound.Bottom(), q.bound.Top(),
		q.bound.Min[0], q.bound.Max[0],
		q.bound.Min[1], q.bound.Max[1],
	)

	if v.closest == nil {
		return nil
	}
	return v.closest.Value
}

// KNearest returns k closest Value/Pointer in the quadtree.
// This function is thread safe. Multiple goroutines can read from a pre-created tree.
// An optional buffer parameter is provided to allow for the reuse of result slice memory.
// The points are returned in a sorted order, nearest first.
// This function allows defining a maximum distance in order to reduce search iterations.
func (q *Quadtree) KNearest(buf []orb.Pointer, p orb.Point, k int, maxDistance ...float64) []orb.Pointer {
	return q.KNearestMatching(buf, p, k, nil, maxDistance...)
}

// KNearestMatching returns k closest Value/Pointer in the quadtree for which
// the given filter function returns true. This function is thread safe.
// Multiple goroutines can read from a pre-created tree. An optional buffer
// parameter is provided to allow for the reuse of result slice memory.
// The points are returned in a sorted order, nearest first.
// This function allows defining a maximum distance in order to reduce search iterations.
func (q *Quadtree) KNearestMatching(buf []orb.Pointer, p orb.Point, k int, f FilterFunc, maxDistance ...float64) []orb.Pointer {
	if q.root == nil {
		return nil
	}

	b := q.bound
	v := &nearestVisitor{
		point:          p,
		filter:         f,
		k:              k,
		maxHeap:        make(maxHeap, 0, k+1),
		closestBound:   &b,
		maxDistSquared: math.MaxFloat64,
	}

	if len(maxDistance) > 0 {
		v.maxDistSquared = maxDistance[0] * maxDistance[0]
	}

	newVisit(v).Visit(q.root,
		// q.bound.Left(), q.bound.Right(),
		// q.bound.Bottom(), q.bound.Top(),
		q.bound.Min[0], q.bound.Max[0],
		q.bound.Min[1], q.bound.Max[1],
	)

	//repack result
	if cap(buf) < len(v.maxHeap) {
		buf = make([]orb.Pointer, len(v.maxHeap))
	} else {
		buf = buf[:len(v.maxHeap)]
	}

	for i := len(v.maxHeap) - 1; i >= 0; i-- {
		buf[i] = v.maxHeap[0].point
		v.maxHeap.Pop()
	}

	return buf
}

// InBound returns a slice with all the pointers in the quadtree that are
// within the given bound. An optional buffer parameter is provided to allow
// for the reuse of result slice memory. This function is thread safe.
// Multiple goroutines can read from a pre-created tree.
func (q *Quadtree) InBound(buf []orb.Pointer, b orb.Bound) []orb.Pointer {
	return q.InBoundMatching(buf, b, nil)
}

// InBoundMatching returns a slice with all the pointers in the quadtree that are
// within the given bound and matching the give filter function. An optional buffer
// parameter is provided to allow for the reuse of result slice memory. This function
// is thread safe.  Multiple goroutines can read from a pre-created tree.
func (q *Quadtree) InBoundMatching(buf []orb.Pointer, b orb.Bound, f FilterFunc) []orb.Pointer {
	if q.root == nil {
		return nil
	}

	var p []orb.Pointer
	if len(buf) > 0 {
		p = buf[:0]
	}
	v := &inBoundVisitor{
		bound:    &b,
		pointers: p,
		filter:   f,
	}

	newVisit(v).Visit(q.root,
		// q.bound.Left(), q.bound.Right(),
		// q.bound.Bottom(), q.bound.Top(),
		q.bound.Min[0], q.bound.Max[0],
		q.bound.Min[1], q.bound.Max[1],
	)

	return v.pointers
}

// The visit stuff is a more go like (hopefully) implementation of the
// d3.quadtree.visit function. It is not exported, but if there is a
// good use case, it could be.

type visitor interface {
	// Bound returns the current relevant bound so we can prune irrelevant nodes
	// from the search. Using a pointer was benchmarked to be 5% faster than
	// having to copy the bound on return. go1.9
	Bound() *orb.Bound
	Visit(n *node)

	// Point should return the specific point being search for, or null if there
	// isn't one (ie. searching by bound). This helps guide the search to the
	// best child node first.
	Point() orb.Point
}

// visit provides a framework for walking the quad tree.
// Currently used by the `Find` and `InBound` functions.
type visit struct {
	visitor visitor
}

func newVisit(v visitor) *visit {
	return &visit{
		visitor: v,
	}
}

func (v *visit) Visit(n *node, left, right, bottom, top float64) {
	b := v.visitor.Bound()
	// if left > b.Right() || right < b.Left() ||
	// 	bottom > b.Top() || top < b.Bottom() {
	// 	return
	// }
	if left > b.Max[0] || right < b.Min[0] ||
		bottom > b.Max[1] || top < b.Min[1] {
		return
	}

	if n.Value != nil {
		v.visitor.Visit(n)
	}

	if n.Children[0] == nil && n.Children[1] == nil &&
		n.Children[2] == nil && n.Children[3] == nil {
		// no children check
		return
	}

	cx := (left + right) / 2.0
	cy := (bottom + top) / 2.0

	i := childIndex(cx, cy, v.visitor.Point())
	for j := i; j < i+4; j++ {
		if n.Children[j%4] == nil {
			continue
		}

		if k := j % 4; k == 0 {
			v.Visit(n.Children[0], left, cx, cy, top)
		} else if k == 1 {
			v.Visit(n.Children[1], cx, right, cy, top)
		} else if k == 2 {
			v.Visit(n.Children[2], left, cx, bottom, cy)
		} else if k == 3 {
			v.Visit(n.Children[3], cx, right, bottom, cy)
		}
	}
}

type findVisitor struct {
	point          orb.Point
	filter         FilterFunc
	closest        *node
	closestBound   *orb.Bound
	minDistSquared float64
}

func (v *findVisitor) Bound() *orb.Bound {
	return v.closestBound
}

func (v *findVisitor) Point() orb.Point {
	return v.point
}

func (v *findVisitor) Visit(n *node) {
	// skip this pointer if we have a filter and it doesn't match
	if v.filter != nil && !v.filter(n.Value) {
		return
	}

	point := n.Value.Point()
	if d := planar.DistanceSquared(point, v.point); d < v.minDistSquared {
		v.minDistSquared = d
		v.closest = n

		d = math.Sqrt(d)
		v.closestBound.Min[0] = v.point[0] - d
		v.closestBound.Max[0] = v.point[0] + d
		v.closestBound.Min[1] = v.point[1] - d
		v.closestBound.Max[1] = v.point[1] + d
	}
}

// type pointsQueueItem struct {
// 	point    orb.Pointer
// 	distance float64 // distance to point and priority inside the queue
// 	index    int     // point index in queue
// }

// type pointsQueue []pointsQueueItem

// func newPointsQueue(capacity int) pointsQueue {
// 	// We make capacity+1 because we need additional place for the greatest element
// 	return make([]pointsQueueItem, 0, capacity+1)
// }

// func (pq pointsQueue) Len() int { return len(pq) }

// func (pq pointsQueue) Less(i, j int) bool {
// 	// We want pop longest distances so Less was inverted
// 	return pq[i].distance > pq[j].distance
// }

// func (pq pointsQueue) Swap(i, j int) {
// 	pq[i], pq[j] = pq[j], pq[i]
// 	pq[i].index = i
// 	pq[j].index = j
// }

// func (pq *pointsQueue) Push(x interface{}) {
// 	n := len(*pq)
// 	item := x.(pointsQueueItem)
// 	item.index = n
// 	*pq = append(*pq, item)
// }

// func (pq *pointsQueue) Pop() interface{} {
// 	old := *pq
// 	n := len(old)
// 	item := old[n-1]
// 	item.index = -1
// 	*pq = old[0 : n-1]
// 	return item
// }

type nearestVisitor struct {
	point          orb.Point
	filter         FilterFunc
	k              int
	maxHeap        maxHeap
	closestBound   *orb.Bound
	maxDistSquared float64
}

func (v *nearestVisitor) Bound() *orb.Bound {
	return v.closestBound
}

func (v *nearestVisitor) Point() orb.Point {
	return v.point
}

func (v *nearestVisitor) Visit(n *node) {
	// skip this pointer if we have a filter and it doesn't match
	if v.filter != nil && !v.filter(n.Value) {
		return
	}

	point := n.Value.Point()
	if d := planar.DistanceSquared(point, v.point); d < v.maxDistSquared {
		v.maxHeap.Push(n.Value, d)
		if len(v.maxHeap) > v.k {

			v.maxHeap.Pop()

			// Actually this is a hack. We know how heap works and obtain
			// top element without function call
			top := v.maxHeap[0]

			v.maxDistSquared = top.distance

			// We have filled queue, so we start to restrict searching range
			d = math.Sqrt(top.distance)
			v.closestBound.Min[0] = v.point[0] - d
			v.closestBound.Max[0] = v.point[0] + d
			v.closestBound.Min[1] = v.point[1] - d
			v.closestBound.Max[1] = v.point[1] + d
		}
	}
}

type inBoundVisitor struct {
	bound    *orb.Bound
	pointers []orb.Pointer
	filter   FilterFunc
}

func (v *inBoundVisitor) Bound() *orb.Bound {
	return v.bound
}

func (v *inBoundVisitor) Point() (p orb.Point) {
	return
}

func (v *inBoundVisitor) Visit(n *node) {
	if v.filter != nil && !v.filter(n.Value) {
		return
	}

	p := n.Value.Point()
	if v.bound.Min[0] > p[0] || v.bound.Max[0] < p[0] ||
		v.bound.Min[1] > p[1] || v.bound.Max[1] < p[1] {
		return

	}
	v.pointers = append(v.pointers, n.Value)
}

func childIndex(cx, cy float64, point orb.Point) int {
	i := 0
	if point[1] <= cy {
		i = 2
	}

	if point[0] >= cx {
		i++
	}

	return i
}
