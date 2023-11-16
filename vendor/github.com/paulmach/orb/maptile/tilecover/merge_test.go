package tilecover

import (
	"testing"

	"github.com/paulmach/orb/maptile"
)

func TestMergeUp(t *testing.T) {
	f := loadFeature(t, "./testdata/line.geojson")

	tiles, _ := Geometry(f.Geometry, 15)
	c1 := len(MergeUpPartial(tiles, 1, 1))

	tiles, _ = Geometry(f.Geometry, 15)
	c2 := len(MergeUpPartial(tiles, 1, 2))

	tiles, _ = Geometry(f.Geometry, 15)
	c3 := len(MergeUpPartial(tiles, 1, 3))

	tiles, _ = Geometry(f.Geometry, 15)
	c4 := len(MergeUpPartial(tiles, 1, 4))

	tiles, _ = Geometry(f.Geometry, 15)
	c := len(MergeUp(tiles, 1))

	if c1 > c2 {
		t.Errorf("c1 should be bigger than c2: %v != %v", c1, c2)
	}

	if c2 > c3 {
		t.Errorf("c2 should be bigger than c3: %v != %v", c2, c3)
	}

	if c3 > c4 {
		t.Errorf("c3 should be bigger than c4: %v != %v", c3, c4)
	}

	if c4 != c {
		t.Errorf("count 4 should be same as mergeUp: %v != %v", c4, c)
	}
}

func BenchmarkMergeUp_z0z10(b *testing.B) {
	g := loadFeature(b, "./testdata/russia.geojson").Geometry
	tiles, _ := Geometry(g, 10)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		MergeUp(cloneSet(tiles), 0)
	}
}

func BenchmarkMergeUp_z8z9(b *testing.B) {
	g := loadFeature(b, "./testdata/russia.geojson").Geometry
	tiles, _ := Geometry(g, 9)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		MergeUp(cloneSet(tiles), 8)
	}
}

func BenchmarkMergeUpPartial4_z0z10(b *testing.B) {
	g := loadFeature(b, "./testdata/russia.geojson").Geometry
	tiles, _ := Geometry(g, 10)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		MergeUpPartial(cloneSet(tiles), 0, 4)
	}
}

func BenchmarkMergeUpPartial4_z8z9(b *testing.B) {
	g := loadFeature(b, "./testdata/russia.geojson").Geometry
	tiles, _ := Geometry(g, 9)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		MergeUpPartial(cloneSet(tiles), 8, 4)
	}
}

func cloneSet(t maptile.Set) maptile.Set {
	r := make(maptile.Set, len(t))
	for k, v := range t {
		if v {
			r[k] = v
		}
	}

	return r
}
