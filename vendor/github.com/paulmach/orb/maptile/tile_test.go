package maptile

import (
	"fmt"
	"math"
	"reflect"
	"testing"

	"github.com/paulmach/orb"
	"github.com/paulmach/orb/internal/mercator"
)

func TestQuadKey(t *testing.T) {
	for i := 0; i < 30; i++ {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			tile := New(uint32(i), uint32(i), Zoom(i))
			result := FromQuadkey(tile.Quadkey(), Zoom(i))

			if result != tile {
				t.Errorf("incorrect tile: %v != %v", result, tile)
			}
		})
	}
}

func TestValid(t *testing.T) {
	if New(10, 10, 1).Valid() {
		t.Errorf("should not be valid")
	}

	if !New(15, 15, 4).Valid() {
		t.Errorf("should be valid")
	}

	if New(16, 16, 4).Valid() {
		t.Errorf("should not be valid")
	}
}

func TestAt(t *testing.T) {
	tile := At(orb.Point{0, 0}, 28)
	if b := tile.Bound(); b.Top() != 0 || b.Left() != 0 {
		t.Errorf("incorrect tile bound: %v", b)
	}

	// specific case
	if tile := At(orb.Point{-87.65005229999997, 41.850033}, 20); tile.X != 268988 || tile.Y != 389836 {
		t.Errorf("projection incorrect: %v", tile)
	}

	if tile := At(orb.Point{-87.65005229999997, 41.850033}, 28); tile.X != 68861112 || tile.Y != 99798110 {
		t.Errorf("projection incorrect: %v", tile)
	}

	for _, city := range mercator.Cities {
		tile := At(orb.Point{city[1], city[0]}, 31)
		c := tile.Center()

		if math.Abs(c[1]-city[0]) > mercator.Epsilon {
			t.Errorf("latitude miss match: %f != %f", c[1], city[0])
		}

		if math.Abs(c[0]-city[1]) > mercator.Epsilon {
			t.Errorf("longitude miss match: %f != %f", c[0], city[1])
		}
	}

	// test polar regions
	if tile := At(orb.Point{0, 89.9}, 30); tile.Y != 0 {
		t.Errorf("top of the world error: %d != %d", tile.Y, 0)
	}

	if tile := At(orb.Point{0, -89.9}, 30); tile.Y != (1<<30)-1 {
		t.Errorf("bottom of the world error: %d != %d", tile.Y, (1<<30)-1)
	}
}

func TestTileQuadkey(t *testing.T) {
	// default level
	level := Zoom(30)
	for _, city := range mercator.Cities {
		tile := At(orb.Point{city[1], city[0]}, level)
		p := tile.Center()

		if math.Abs(p[1]-city[0]) > mercator.Epsilon {
			t.Errorf("latitude miss match: %f != %f", p[1], city[0])
		}

		if math.Abs(p[0]-city[1]) > mercator.Epsilon {
			t.Errorf("longitude miss match: %f != %f", p[0], city[1])
		}
	}
}

func TestTileBound(t *testing.T) {
	bound := Tile{7, 8, 9}.Bound()

	level := Zoom(9 + 5) // we're testing point +5 zoom, in same tile
	factor := uint32(5)

	// edges should be within the bound
	p := Tile{7<<factor + 1, 8<<factor + 1, level}.Center()
	if !bound.Contains(p) {
		t.Errorf("should contain point")
	}

	p = Tile{7<<factor - 1, 8<<factor - 1, level}.Center()
	if bound.Contains(p) {
		t.Errorf("should not contain point")
	}

	p = Tile{8<<factor - 1, 9<<factor - 1, level}.Center()
	if !bound.Contains(p) {
		t.Errorf("should contain point")
	}

	p = Tile{8<<factor + 1, 9<<factor + 1, level}.Center()
	if bound.Contains(p) {
		t.Errorf("should not contain point")
	}

	expected := orb.Bound{Min: orb.Point{-180, -85.05112877980659}, Max: orb.Point{180, 85.05112877980659}}
	if b := New(0, 0, 0).Bound(); !b.Equal(expected) {
		t.Errorf("should be full earth, got %v", b)
	}
}

func TestFraction(t *testing.T) {
	p := Fraction(orb.Point{-180, 0}, 30)
	if p[0] != 0 {
		t.Errorf("should have left at zero: %f", p[0])
	}

	p = Fraction(orb.Point{180, 0}, 30)
	if p[0] != 1<<30 {
		t.Errorf("incorrect x: %f != %v", p[0], 1<<30)
	}

	p = Fraction(orb.Point{360, 0}, 30)
	if p[0] != 1<<30+1<<29 {
		t.Errorf("incorrect x: %f != %v", p[0], 1<<30+1<<29)
	}
}

func TestContains(t *testing.T) {
	tile := New(2, 2, 10)

	if !tile.Contains(tile) {
		t.Errorf("should contain self")
	}

	for _, c := range tile.Children() {
		if !tile.Contains(c) {
			t.Errorf("should contain child: %v", c)
		}
	}

	// it should not contain parent
	if tile.Contains(tile.Parent()) {
		t.Errorf("should not contain parent")
	}
}

func TestChildrenTilesSubRange(t *testing.T) {
	type args struct {
		tile      Tile
		zoomStart Zoom
		zoomEnd   Zoom
	}
	tests := []struct {
		name string
		args args
		want Tiles
	}{
		{
			name: "self zoom",
			args: args{
				tile:      New(0, 0, 0),
				zoomStart: 0,
				zoomEnd:   0,
			},
			want: Tiles{New(0, 0, 0)},
		},
		{
			name: "1 level zooming including zoom 0",
			args: args{
				tile:      New(0, 0, 0),
				zoomStart: 0,
				zoomEnd:   1,
			},
			want: Tiles{New(0, 0, 0), New(0, 0, 1), New(0, 1, 1), New(1, 0, 1), New(1, 1, 1)},
		},
		{
			name: "1 level zooming excluding zoom 0",
			args: args{
				tile:      New(0, 0, 0),
				zoomStart: 1,
				zoomEnd:   1,
			},
			want: Tiles{New(0, 0, 1), New(0, 1, 1), New(1, 0, 1), New(1, 1, 1)},
		},
		{
			name: "2 level zooming",
			args: args{
				tile:      New(0, 0, 0),
				zoomStart: 2,
				zoomEnd:   2,
			},
			want: Tiles{
				New(0, 0, 2),
				New(0, 1, 2),
				New(0, 2, 2),
				New(0, 3, 2),
				New(1, 0, 2),
				New(1, 1, 2),
				New(1, 2, 2),
				New(1, 3, 2),
				New(2, 0, 2),
				New(2, 1, 2),
				New(2, 2, 2),
				New(2, 3, 2),
				New(3, 0, 2),
				New(3, 1, 2),
				New(3, 2, 2),
				New(3, 3, 2),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !reflect.DeepEqual(tt.want, ChildrenInZoomRange(tt.args.tile, tt.args.zoomStart, tt.args.zoomEnd)) {
				t.Errorf("tiles not equal")
			}
		})
	}
}

func TestChildrenTilesSubRangeInvalidParams_ZoomStart_Larger_Than_ZoomEnd(t *testing.T) {
	// No need to check whether `recover()` is nil. Just turn off the panic.
	defer func() { _ = recover() }()
	tile := New(0, 0, 0)
	ChildrenInZoomRange(tile, 10, 8)
	// Never reaches here if `ChildrenInZoomRange` panics.
	t.Errorf("did not panic")
}

func TestChildrenTilesSubRangeInvalidParams_TileZ_Larger_Than_ZoomStart(t *testing.T) {
	// No need to check whether `recover()` is nil. Just turn off the panic.
	defer func() { _ = recover() }()
	tile := New(0, 0, 10)
	ChildrenInZoomRange(tile, 9, 12)
	// Never reaches here if `ChildrenInZoomRange` panics.
	t.Errorf("did not panic")
}

func TestRange(t *testing.T) {
	tile := New(4, 4, 5)
	min, max := tile.Range(3)
	if min != New(1, 1, 3) || max != New(1, 1, 3) {
		t.Errorf("should be parent if zoom lower")
	}

	tile = New(4, 2, 5)
	min, max = tile.Range(7)
	if min != New(16, 8, 7) {
		t.Errorf("min incorrect: %v", min)
	}

	if max != New(19, 11, 7) {
		t.Errorf("max incorrect: %v", max)
	}
}

func TestSharedParent(t *testing.T) {
	p := orb.Point{-122.2711, 37.8044}
	one := At(p, 15)
	two := At(p, 15)

	expected := one

	one.Z = 25
	one.X = (one.X << 10) | 0x25A
	one.Y = (one.Y << 10) | 0x14B

	two.Z = 21
	two.X = (two.X << 6) | 0x15
	two.Y = (two.Y << 6) | 0x26

	if tile := one.SharedParent(two); tile != expected {
		t.Errorf("incorrect shared: %v != %v", tile, expected)
	}

	if tile := two.SharedParent(one); tile != expected {
		t.Errorf("incorrect shared: %v != %v", tile, expected)
	}

	children := one.Children()
	if tile := children[1].SharedParent(children[2]); tile != one {
		t.Errorf("should map back to shared parent: %v != %v", tile, one)
	}
}

func TestChildren(t *testing.T) {
	tile := New(1, 1, 1)

	children := tile.Children()
	if children[0] != New(2, 2, 2) {
		t.Errorf("incorrect tile: %v", children[0])
	}
	if children[1] != New(3, 2, 2) {
		t.Errorf("incorrect tile: %v", children[1])
	}
	if children[2] != New(3, 3, 2) {
		t.Errorf("incorrect tile: %v", children[2])
	}
	if children[3] != New(2, 3, 2) {
		t.Errorf("incorrect tile: %v", children[3])
	}

	if len(children) != 4 {
		t.Errorf("should have 4 children: %v", len(children))
	}
}

func TestSiblings(t *testing.T) {
	tile := New(4, 7, 7)

	siblings := tile.Siblings()
	if siblings[0] != New(4, 6, 7) {
		t.Errorf("incorrect tile: %v", siblings[0])
	}
	if siblings[1] != New(5, 6, 7) {
		t.Errorf("incorrect tile: %v", siblings[1])
	}
	if siblings[2] != New(5, 7, 7) {
		t.Errorf("incorrect tile: %v", siblings[2])
	}
	if siblings[3] != New(4, 7, 7) {
		t.Errorf("incorrect tile: %v", siblings[3])
	}

	if len(siblings) != 4 {
		t.Errorf("should have 4 children: %v", len(siblings))
	}
}

func BenchmarkSharedParent_SameZoom(b *testing.B) {
	p := orb.Point{-122.2711, 37.8044}
	one := At(p, 10)
	two := At(p, 10)

	one.Z = 20
	one.X = (one.X << 10) | 0x25A
	one.Y = (one.X << 10) | 0x14B

	two.Z = 20
	two.X = (two.X << 10) | 0x15B
	two.Y = (two.X << 10) | 0x26A

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		one.SharedParent(two)
	}
}

func BenchmarkSharedParent_DifferentZoom(b *testing.B) {
	p := orb.Point{-122.2711, 37.8044}
	one := At(p, 10)
	two := At(p, 10)

	one.Z = 20
	one.X = (one.X << 10) | 0x25A
	one.Y = (one.X << 10) | 0x14B

	two.Z = 18
	two.X = (two.X << 8) | 0x03B
	two.Y = (two.X << 8) | 0x0CA

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		one.SharedParent(two)
	}
}
