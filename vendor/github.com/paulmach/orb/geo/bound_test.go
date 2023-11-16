package geo

import (
	"math"
	"testing"

	"github.com/paulmach/orb"
)

func TestBoundAroundPoint(t *testing.T) {
	p := orb.Point{
		5.42553,
		50.0359,
	}

	b := NewBoundAroundPoint(p, 1000000)
	if b.Center()[1] != p[1] {
		t.Errorf("should have correct center lat point")
	}

	if b.Center()[0] != p[0] {
		t.Errorf("should have correct center lon point")
	}

	//Given point is 968.9 km away from center
	if !b.Contains(orb.Point{3.412, 58.3838}) {
		t.Errorf("should have point included in bound")
	}

	b = NewBoundAroundPoint(p, 10000.0)
	if b.Center()[1] != p[1] {
		t.Errorf("should have correct center lat point")
	}

	if b.Center()[0] != p[0] {
		t.Errorf("should have correct center lon point")
	}

	//Given point is 968.9 km away from center
	if b.Contains(orb.Point{3.412, 58.3838}) {
		t.Errorf("should not have point included in bound")
	}
}

func TestBoundPad(t *testing.T) {
	cases := []struct {
		name  string
		bound orb.Bound
	}{
		{
			name:  "test bound",
			bound: orb.MultiPoint{{-122.559, 37.887}, {-122.521, 37.911}}.Bound(),
		},
		{
			name:  "no height",
			bound: orb.MultiPoint{{-122.559, 15}, {-122.521, 15}}.Bound(),
		},
		{
			name:  "no area",
			bound: orb.Bound{Min: orb.Point{20, -15}, Max: orb.Point{20, -15}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b2 := BoundPad(tc.bound, 100)

			v1 := BoundHeight(tc.bound) + 200
			v2 := BoundHeight(b2)
			if math.Abs(v1-v2) > 1.0 {
				t.Errorf("height incorrected: %f != %f", v1, v2)
			}

			v1 = BoundWidth(tc.bound) + 200
			v2 = BoundWidth(b2)
			if math.Abs(v1-v2) > 1.0 {
				t.Errorf("height incorrected: %f != %f", v1, v2)
			}
		})
	}

	b1 := orb.Bound{Min: orb.Point{-180, -90}, Max: orb.Point{180, 90}}
	b2 := BoundPad(b1, 100)
	if !b1.Equal(b2) {
		t.Errorf("should be extend bound around fill earth: %v", b2)
	}
}
