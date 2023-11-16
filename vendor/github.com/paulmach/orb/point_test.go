package orb

import (
	"testing"
)

func TestPoint(t *testing.T) {
	p := Point{1, 2}
	if v := p.Lon(); v != 1 {
		t.Errorf("incorrect lon: %v != 1", v)
	}

	if v := p.Lat(); v != 2 {
		t.Errorf("incorrect lat: %v != 2", v)
	}
}
func TestPointEqual(t *testing.T) {
	p1 := Point{1, 0}
	p2 := Point{1, 0}

	p3 := Point{2, 3}
	p4 := Point{2, 4}

	if !p1.Equal(p2) {
		t.Errorf("expected: %v == %v", p1, p2)
	}

	if p2.Equal(p3) {
		t.Errorf("expected: %v != %v", p2, p3)
	}

	if p3.Equal(p4) {
		t.Errorf("expected: %v != %v", p3, p4)
	}
}
