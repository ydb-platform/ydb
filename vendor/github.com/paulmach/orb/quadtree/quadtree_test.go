package quadtree

import (
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/paulmach/orb"
	"github.com/paulmach/orb/planar"
)

func TestNew(t *testing.T) {
	bound := orb.Bound{Min: orb.Point{0, 2}, Max: orb.Point{1, 3}}
	qt := New(bound)

	if !qt.Bound().Equal(bound) {
		t.Errorf("should use provided bound, got %v", qt.Bound())
	}
}

func TestQuadtreeAdd(t *testing.T) {
	p := orb.Point{}
	qt := New(orb.Bound{Min: orb.Point{0, 0}, Max: orb.Point{1, 1}})
	for i := 0; i < 10; i++ {
		// should be able to insert the same point over and over.
		err := qt.Add(p)
		if err != nil {
			t.Fatalf("unexpected error for %v: %v", p, err)
		}
	}
}

func TestQuadtreeRemove(t *testing.T) {
	r := rand.New(rand.NewSource(42))

	qt := New(orb.Bound{Min: orb.Point{0, 0}, Max: orb.Point{1, 1}})
	mp := orb.MultiPoint{}
	for i := 0; i < 1000; i++ {
		mp = append(mp, orb.Point{r.Float64(), r.Float64()})
		err := qt.Add(mp[i])
		if err != nil {
			t.Fatalf("unexpected error for %v: %v", mp[i], err)
		}
	}

	for i := 0; i < 1000; i += 3 {
		qt.Remove(mp[i], nil)
		mp[i] = orb.Point{-10000, -10000}
	}

	// make sure finding still works for 1000 random points
	for i := 0; i < 1000; i++ {
		p := orb.Point{r.Float64(), r.Float64()}

		f := qt.Find(p)
		_, j := planar.DistanceFromWithIndex(mp, p)

		if e := mp[j]; !e.Equal(f.Point()) {
			t.Errorf("index: %d, unexpected point %v != %v", i, e, f.Point())
		}
	}
}

type PExtra struct {
	p  orb.Point
	id string
}

func (p *PExtra) Point() orb.Point {
	return p.p
}

func (p *PExtra) String() string {
	return fmt.Sprintf("%v: %v", p.id, p.p)
}

func TestQuadtreeRemoveAndAdd_inOrder(t *testing.T) {
	seed := time.Now().UnixNano()
	t.Logf("seed: %v", seed)
	r := rand.New(rand.NewSource(seed))

	qt := New(orb.Bound{Min: orb.Point{0, 0}, Max: orb.Point{1, 1}})
	p1 := &PExtra{p: orb.Point{r.Float64(), r.Float64()}, id: "1"}
	p2 := &PExtra{p: orb.Point{p1.p[0], p1.p[1]}, id: "2"}
	p3 := &PExtra{p: orb.Point{p1.p[0], p1.p[1]}, id: "3"}

	err := qt.Add(p1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	err = qt.Add(p2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	err = qt.Add(p3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// rm 3
	found := qt.Remove(p3, func(p orb.Pointer) bool {
		return p.(*PExtra).id == p3.id
	})
	if !found {
		t.Error("didn't find/remove point")
	}

	// leaf node doesn't actually get removed
	if c := countNodes(qt.root); c != 3 {
		t.Errorf("incorrect number of nodes: %v != 3", c)
	}

	// 3 again
	found = qt.Remove(p3, func(p orb.Pointer) bool {
		return p.(*PExtra).id == p3.id
	})
	if found {
		t.Errorf("should not find already removed node")
	}

	// rm 2
	found = qt.Remove(p2, func(p orb.Pointer) bool {
		return p.(*PExtra).id == p2.id
	})
	if !found {
		t.Error("didn't find/remove point")
	}

	if c := countNodes(qt.root); c != 2 {
		t.Errorf("incorrect number of nodes: %v != 2", c)
	}

	// rm 1
	found = qt.Remove(p1, func(p orb.Pointer) bool {
		return p.(*PExtra).id == p1.id
	})
	if !found {
		t.Error("didn't find/remove point")
	}

	if c := countNodes(qt.root); c != 1 {
		t.Errorf("incorrect number of nodes: %v != 1", c)
	}
}

func TestQuadtreeRemoveAndAdd_sameLoc(t *testing.T) {
	seed := time.Now().UnixNano()
	t.Logf("seed: %v", seed)
	r := rand.New(rand.NewSource(seed))

	qt := New(orb.Bound{Min: orb.Point{0, 0}, Max: orb.Point{1, 1}})
	p1 := &PExtra{p: orb.Point{r.Float64(), r.Float64()}, id: "1"}
	p2 := &PExtra{p: orb.Point{p1.p[0], p1.p[1]}, id: "2"}
	p3 := &PExtra{p: orb.Point{p1.p[0], p1.p[1]}, id: "3"}
	p4 := &PExtra{p: orb.Point{p1.p[0], p1.p[1]}, id: "4"}
	p5 := &PExtra{p: orb.Point{p1.p[0], p1.p[1]}, id: "5"}

	err := qt.Add(p1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	err = qt.Add(p2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	err = qt.Add(p3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// remove middle point
	found := qt.Remove(p2, func(p orb.Pointer) bool {
		return p.(*PExtra).id == p2.id
	})
	if !found {
		t.Error("didn't find/remove point")
	}

	if c := countNodes(qt.root); c != 2 {
		t.Errorf("incorrect number of nodes: %v != 2", c)
	}

	// remove first point
	found = qt.Remove(p1, func(p orb.Pointer) bool {
		return p.(*PExtra).id == p1.id
	})
	if !found {
		t.Error("didn't find/remove point")
	}

	if c := countNodes(qt.root); c != 1 {
		t.Errorf("incorrect number of nodes: %v != 1", c)
	}

	// add a 4th point
	err = qt.Add(p4)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// remove third point
	found = qt.Remove(p3, func(p orb.Pointer) bool {
		return p.(*PExtra).id == p3.id
	})
	if !found {
		t.Error("didn't find/remove point")
	}

	if c := countNodes(qt.root); c != 1 {
		t.Errorf("incorrect number of nodes: %v != 1", c)
	}

	// add a 5th point
	err = qt.Add(p5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// remove the 5th point
	found = qt.Remove(p5, func(p orb.Pointer) bool {
		return p.(*PExtra).id == p5.id
	})
	if !found {
		t.Error("didn't find/remove point")
	}

	// 5 is a tail point, so its not does not actually get removed
	if c := countNodes(qt.root); c != 2 {
		t.Errorf("incorrect number of nodes: %v != 2", c)
	}

	// add a 3th point again
	err = qt.Add(p3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// should reuse the tail point left by p5
	if c := countNodes(qt.root); c != 2 {
		t.Errorf("incorrect number of nodes: %v != 2", c)
	}

	// remove p4/root
	found = qt.Remove(p4, func(p orb.Pointer) bool {
		return p.(*PExtra).id == p4.id
	})
	if !found {
		t.Error("didn't find/remove point")
	}

	if c := countNodes(qt.root); c != 1 {
		t.Errorf("incorrect number of nodes: %v != 1", c)
	}

	// remove p3/root
	found = qt.Remove(p3, func(p orb.Pointer) bool {
		return p.(*PExtra).id == p3.id
	})
	if !found {
		t.Error("didn't find/remove point")
	}

	// just the root, can't remove it
	if c := countNodes(qt.root); c != 1 {
		t.Errorf("incorrect number of nodes: %v != 1", c)
	}

	// add back a point to be put in the root
	err = qt.Add(p3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if c := countNodes(qt.root); c != 1 {
		t.Errorf("incorrect number of nodes: %v != 1", c)
	}
}

func TestQuadtreeRemoveAndAdd_random(t *testing.T) {
	seed := time.Now().UnixNano()
	t.Logf("seed: %v", seed)
	r := rand.New(rand.NewSource(seed))

	const runs = 10
	const perRun = 300 // add 300, remove 300/2

	bounds := orb.Bound{Min: orb.Point{0, 0}, Max: orb.Point{3000, 3000}}
	qt := New(bounds)
	points := make([]*PExtra, 0, 3000)
	id := 0

	for i := 0; i < runs; i++ {
		for j := 0; j < perRun; j++ {
			x := r.Int63n(30)
			y := r.Int63n(30)
			id++
			p := &PExtra{p: orb.Point{float64(x), float64(y)}, id: fmt.Sprintf("%d", id)}

			err := qt.Add(p)
			if err != nil {
				t.Fatalf("unexpected error for %v: %v", p, err)
			}

			points = append(points, p)

		}

		for j := 0; j < perRun/2; j++ {
			k := r.Int() % len(points)
			remP := points[k]
			points = append(points[:k], points[k+1:]...)
			qt.Remove(remP, func(p orb.Pointer) bool {
				return p.(*PExtra).id == remP.id
			})
		}
	}

	left := len(qt.InBound(nil, bounds))
	expected := runs * perRun / 2
	if left != expected {
		t.Errorf("incorrect number of points in tree: %d != %d", left, expected)
	}
}

func TestQuadtreeFind(t *testing.T) {
	points := orb.MultiPoint{}
	dim := 17

	for i := 0; i < dim*dim; i++ {
		points = append(points, orb.Point{float64(i % dim), float64(i / dim)})
	}

	qt := New(points.Bound())
	for _, p := range points {
		err := qt.Add(p)
		if err != nil {
			t.Fatalf("unexpected error for %v: %v", p, err)
		}
	}

	cases := []struct {
		point    orb.Point
		expected orb.Point
	}{
		{point: orb.Point{0.1, 0.1}, expected: orb.Point{0, 0}},
		{point: orb.Point{3.1, 2.9}, expected: orb.Point{3, 3}},
		{point: orb.Point{7.1, 7.1}, expected: orb.Point{7, 7}},
		{point: orb.Point{0.1, 15.9}, expected: orb.Point{0, 16}},
		{point: orb.Point{15.9, 15.9}, expected: orb.Point{16, 16}},
	}

	for i, tc := range cases {
		if v := qt.Find(tc.point); !v.Point().Equal(tc.expected) {
			t.Errorf("incorrect point on %d, got %v", i, v)
		}
	}
}

func TestQuadtreeFind_Random(t *testing.T) {
	r := rand.New(rand.NewSource(42))

	qt := New(orb.Bound{Min: orb.Point{0, 0}, Max: orb.Point{1, 1}})
	mp := orb.MultiPoint{}
	for i := 0; i < 1000; i++ {
		mp = append(mp, orb.Point{r.Float64(), r.Float64()})
		err := qt.Add(mp[i])
		if err != nil {
			t.Fatalf("unexpected error for %v: %v", mp[i], err)
		}
	}

	for i := 0; i < 1000; i++ {
		p := orb.Point{r.Float64(), r.Float64()}

		f := qt.Find(p)
		_, j := planar.DistanceFromWithIndex(mp, p)

		if e := mp[j]; !e.Equal(f.Point()) {
			t.Errorf("index: %d, unexpected point %v != %v", i, e, f.Point())
		}
	}
}

func TestQuadtreeMatching(t *testing.T) {
	type dataPointer struct {
		orb.Pointer
		visible bool
	}

	qt := New(orb.Bound{Min: orb.Point{0, 0}, Max: orb.Point{1, 1}})
	err := qt.Add(dataPointer{orb.Point{0, 0}, false})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	err = qt.Add(dataPointer{orb.Point{1, 1}, true})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	cases := []struct {
		name     string
		filter   FilterFunc
		point    orb.Point
		expected orb.Pointer
	}{
		{
			name:     "no filtred",
			point:    orb.Point{0.1, 0.1},
			expected: orb.Point{0, 0},
		},
		{
			name:     "with filter",
			filter:   func(p orb.Pointer) bool { return p.(dataPointer).visible },
			point:    orb.Point{0.1, 0.1},
			expected: orb.Point{1, 1},
		},
		{
			name:     "match none filter",
			filter:   func(p orb.Pointer) bool { return false },
			point:    orb.Point{0.1, 0.1},
			expected: nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v := qt.Matching(tc.point, tc.filter)

			// case 1: exact match, important for testing `nil`
			if v == tc.expected {
				return
			}

			// case 2: match on returned orb.Point value
			if !v.Point().Equal(tc.expected.Point()) {
				t.Errorf("incorrect point %v != %v", v, tc.expected)
			}
		})
	}
}

func TestQuadtreeKNearest(t *testing.T) {
	type dataPointer struct {
		orb.Pointer
		visible bool
	}

	q := New(orb.Bound{Max: orb.Point{5, 5}})

	pointers := []dataPointer{
		{orb.Point{0, 0}, false},
		{orb.Point{1, 1}, true},
		{orb.Point{2, 2}, false},
		{orb.Point{3, 3}, true},
		{orb.Point{4, 4}, false},
		{orb.Point{5, 5}, true},
	}

	for _, p := range pointers {
		err := q.Add(p)
		if err != nil {
			t.Fatalf("unexpected error for %v: %v", p, err)
		}
	}

	filters := map[bool]FilterFunc{
		false: nil,
		true:  func(p orb.Pointer) bool { return p.(dataPointer).visible },
	}

	cases := []struct {
		name     string
		filtered bool
		point    orb.Point
		expected []orb.Point
	}{
		{
			name:     "unfiltered",
			filtered: false,
			point:    orb.Point{0.1, 0.1},
			expected: []orb.Point{{0, 0}, {1, 1}},
		},
		{
			name:     "filtered",
			filtered: true,
			point:    orb.Point{0.1, 0.1},
			expected: []orb.Point{{1, 1}, {3, 3}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if !tc.filtered {
				v := q.KNearest(nil, tc.point, 2)
				if len(v) != len(tc.expected) {
					t.Errorf("incorrect response length: %d != %d", len(v), len(tc.expected))
				}
			}

			v := q.KNearestMatching(nil, tc.point, 2, filters[tc.filtered])
			if len(v) != len(tc.expected) {
				t.Errorf("incorrect response length: %d != %d", len(v), len(tc.expected))
			}

			result := make([]orb.Point, 0)
			for _, p := range v {
				result = append(result, p.Point())
			}

			sort.Slice(result, func(i, j int) bool {
				return result[i][0] < result[j][0]
			})

			sort.Slice(tc.expected, func(i, j int) bool {
				return tc.expected[i][0] < tc.expected[j][0]
			})

			if !reflect.DeepEqual(result, tc.expected) {
				t.Log(result)
				t.Log(tc.expected)
				t.Errorf("incorrect results")
			}
		})
	}
}

func TestQuadtreeKNearest_sorted(t *testing.T) {
	q := New(orb.Bound{Max: orb.Point{5, 5}})
	for i := 0; i <= 5; i++ {
		err := q.Add(orb.Point{float64(i), float64(i)})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	nearest := q.KNearest(nil, orb.Point{2.25, 2.25}, 5)

	expected := []orb.Point{{2, 2}, {3, 3}, {1, 1}, {4, 4}, {0, 0}}
	for i, p := range expected {
		if n := nearest[i].Point(); !n.Equal(p) {
			t.Errorf("incorrect point %d: %v", i, n)
		}
	}
}

func TestQuadtreeKNearest_sorted2(t *testing.T) {
	q := New(orb.Bound{Max: orb.Point{8, 8}})
	for i := 0; i <= 7; i++ {
		err := q.Add(orb.Point{float64(i), float64(i)})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	nearest := q.KNearest(nil, orb.Point{5.25, 5.25}, 3)

	expected := []orb.Point{{5, 5}, {6, 6}, {4, 4}}
	for i, p := range expected {
		if n := nearest[i].Point(); !n.Equal(p) {
			t.Errorf("incorrect point %d: %v", i, n)
		}
	}
}

func TestQuadtreeKNearest_DistanceLimit(t *testing.T) {
	type dataPointer struct {
		orb.Pointer
		visible bool
	}

	q := New(orb.Bound{Max: orb.Point{5, 5}})

	pointers := []dataPointer{
		{orb.Point{0, 0}, false},
		{orb.Point{1, 1}, true},
		{orb.Point{2, 2}, false},
		{orb.Point{3, 3}, true},
		{orb.Point{4, 4}, false},
		{orb.Point{5, 5}, true},
	}

	for _, p := range pointers {
		err := q.Add(p)
		if err != nil {
			t.Fatalf("unexpected error for %v: %v", p, err)
		}
	}

	filters := map[bool]FilterFunc{
		false: nil,
		true:  func(p orb.Pointer) bool { return p.(dataPointer).visible },
	}

	cases := []struct {
		name     string
		filtered bool
		distance float64
		point    orb.Point
		expected []orb.Point
	}{
		{
			name:     "filtered",
			filtered: true,
			distance: 5,
			point:    orb.Point{0.1, 0.1},
			expected: []orb.Point{{1, 1}, {3, 3}},
		},
		{
			name:     "unfiltered",
			filtered: false,
			distance: 1,
			point:    orb.Point{0.1, 0.1},
			expected: []orb.Point{{0, 0}},
		},
	}

	var v []orb.Pointer
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v = q.KNearestMatching(v, tc.point, 5, filters[tc.filtered], tc.distance)
			if len(v) != len(tc.expected) {
				t.Errorf("incorrect response length: %d != %d", len(v), len(tc.expected))
			}

			result := make([]orb.Point, 0)
			for _, p := range v {
				result = append(result, p.Point())
			}

			sort.Slice(result, func(i, j int) bool {
				return result[i][0] < result[j][0]
			})

			sort.Slice(tc.expected, func(i, j int) bool {
				return tc.expected[i][0] < tc.expected[j][0]
			})

			if !reflect.DeepEqual(result, tc.expected) {
				t.Log(result)
				t.Log(tc.expected)
				t.Errorf("incorrect results")
			}
		})
	}
}

func TestQuadtreeInBoundMatching(t *testing.T) {
	type dataPointer struct {
		orb.Pointer
		visible bool
	}

	q := New(orb.Bound{Max: orb.Point{5, 5}})

	pointers := []dataPointer{
		{orb.Point{0, 0}, false},
		{orb.Point{1, 1}, true},
		{orb.Point{2, 2}, false},
		{orb.Point{3, 3}, true},
		{orb.Point{4, 4}, false},
		{orb.Point{5, 5}, true},
	}

	for _, p := range pointers {
		err := q.Add(p)
		if err != nil {
			t.Fatalf("unexpected error for %v: %v", p, err)
		}
	}

	filters := map[bool]FilterFunc{
		false: nil,
		true:  func(p orb.Pointer) bool { return p.(dataPointer).visible },
	}

	cases := []struct {
		name     string
		filtered bool
		expected []orb.Point
	}{
		{
			name:     "unfiltered",
			filtered: false,
			expected: []orb.Point{{0, 0}, {1, 1}, {2, 2}},
		},
		{
			name:     "filtered",
			filtered: true,
			expected: []orb.Point{{1, 1}},
		},
	}

	bound := orb.Bound{Min: orb.Point{0, 0}, Max: orb.Point{2, 2}}

	var v []orb.Pointer
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v = q.InBoundMatching(v, bound, filters[tc.filtered])
			if len(v) != len(tc.expected) {
				t.Errorf("incorrect response length: %d != %d", len(v), len(tc.expected))
			}

			result := make([]orb.Point, 0)
			for _, p := range v {
				result = append(result, p.Point())
			}

			sort.Slice(result, func(i, j int) bool {
				return result[i][0] < result[j][0]
			})

			sort.Slice(tc.expected, func(i, j int) bool {
				return tc.expected[i][0] < tc.expected[j][0]
			})

			if !reflect.DeepEqual(result, tc.expected) {
				t.Log(result)
				t.Log(tc.expected)
				t.Errorf("incorrect results")
			}
		})
	}

}

func TestQuadtreeInBound_Random(t *testing.T) {
	r := rand.New(rand.NewSource(43))

	qt := New(orb.Bound{Min: orb.Point{0, 0}, Max: orb.Point{1, 1}})
	mp := orb.MultiPoint{}
	for i := 0; i < 1000; i++ {
		mp = append(mp, orb.Point{r.Float64(), r.Float64()})
		err := qt.Add(mp[i])
		if err != nil {
			t.Fatalf("unexpected error for %v: %v", mp[i], err)
		}
	}

	for i := 0; i < 1000; i++ {
		p := orb.Point{r.Float64(), r.Float64()}

		b := orb.Bound{Min: p, Max: p}
		b = b.Pad(0.1)
		ps := qt.InBound(nil, b)

		// find the right answer brute force
		var list []orb.Pointer
		for _, p := range mp {
			if b.Contains(p) {
				list = append(list, p)
			}
		}

		if len(list) != len(ps) {
			t.Errorf("index: %d, lengths not equal %v != %v", i, len(list), len(ps))
		}
	}
}

func countNodes(n *node) int {
	if n == nil {
		return 0
	}

	c := 1
	c += countNodes(n.Children[0])
	c += countNodes(n.Children[1])
	c += countNodes(n.Children[2])
	c += countNodes(n.Children[3])

	return c
}
