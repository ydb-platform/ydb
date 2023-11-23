package orb

import (
	"testing"
)

func TestMultiPointBound(t *testing.T) {
	mp := MultiPoint{{0.5, .2}, {-1, 0}, {1, 10}, {1, 8}}

	expected := Bound{Min: Point{-1, 0}, Max: Point{1, 10}}
	if b := mp.Bound(); !b.Equal(expected) {
		t.Errorf("incorrect bound, %v != %v", b, expected)
	}

	mp = MultiPoint{}
	if b := mp.Bound(); !b.IsEmpty() {
		t.Errorf("expect empty multi point to have empty bounds: %v", b)
	}
}

func TestMultiPointEquals(t *testing.T) {
	p1 := MultiPoint{{0.5, .2}, {-1, 0}, {1, 10}}
	p2 := MultiPoint{{0.5, .2}, {-1, 0}, {1, 10}}

	if !p1.Equal(p2) {
		t.Error("should be equal")
	}

	p2[1] = Point{1, 0}
	if p1.Equal(p2) {
		t.Error("should not be equal")
	}

	p1[1] = Point{1, 0}
	p1 = append(p1, Point{0, 0})
	if p2.Equal(p1) {
		t.Error("should not be equal")
	}
}

func TestMultiPointClone(t *testing.T) {
	p1 := MultiPoint{{0, 0}, {0.5, .2}, {1, 0}}

	p2 := p1.Clone()
	p2 = append(p2, Point{0, 0})
	if len(p1) == len(p2) {
		t.Errorf("clone length %d == %d", len(p1), len(p2))
	}

	if p2.Equal(p1) {
		t.Error("clone should be equal")
	}
}
