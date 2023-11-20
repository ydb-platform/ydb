package tilecover

import (
	"testing"

	"github.com/paulmach/orb"
)

func BenchmarkPoint(b *testing.B) {
	p := orb.Point{-77.15664982795715, 38.87419791355846}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := Geometry(p, 6)
		if err != nil {
			b.Fatalf("unexpected error: %v", err)
		}
	}
}

func BenchmarkRoad_z6(b *testing.B) {
	g := loadFeature(b, "./testdata/road.geojson").Geometry

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := Geometry(g, 6)
		if err != nil {
			b.Fatalf("unexpected error: %v", err)
		}
	}
}

func BenchmarkRoad_z18(b *testing.B) {
	g := loadFeature(b, "./testdata/road.geojson").Geometry

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := Geometry(g, 18)
		if err != nil {
			b.Fatalf("unexpected error: %v", err)
		}
	}
}

func BenchmarkRoad_z28(b *testing.B) {
	g := loadFeature(b, "./testdata/road.geojson").Geometry

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := Geometry(g, 28)
		if err != nil {
			b.Fatalf("unexpected error: %v", err)
		}
	}
}

func BenchmarkRussia_z6(b *testing.B) {
	g := loadFeature(b, "./testdata/russia.geojson").Geometry

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := Geometry(g, 6)
		if err != nil {
			b.Fatalf("unexpected error: %v", err)
		}
	}
}

func BenchmarkRussia_z8(b *testing.B) {
	g := loadFeature(b, "./testdata/russia.geojson").Geometry

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := Geometry(g, 8)
		if err != nil {
			b.Fatalf("unexpected error: %v", err)
		}
	}
}

func BenchmarkRussia_z10(b *testing.B) {
	g := loadFeature(b, "./testdata/russia.geojson").Geometry

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := Geometry(g, 10)
		if err != nil {
			b.Fatalf("unexpected error: %v", err)
		}
	}
}

func BenchmarkRussia_z0z9(b *testing.B) {
	g := loadFeature(b, "./testdata/russia.geojson").Geometry

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tiles, _ := Geometry(g, 9)
		MergeUp(tiles, 0)
	}
}

func BenchmarkRussiaLine_z6(b *testing.B) {
	g := loadFeature(b, "./testdata/russia.geojson").Geometry
	g = orb.LineString(g.(orb.Polygon)[0])

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := Geometry(g, 6)
		if err != nil {
			b.Fatalf("unexpected error: %v", err)
		}
	}
}

func BenchmarkRussiaLine_z8(b *testing.B) {
	g := loadFeature(b, "./testdata/russia.geojson").Geometry
	g = orb.LineString(g.(orb.Polygon)[0])

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := Geometry(g, 8)
		if err != nil {
			b.Fatalf("unexpected error: %v", err)
		}
	}
}

func BenchmarkRussiaLine_z10(b *testing.B) {
	g := loadFeature(b, "./testdata/russia.geojson").Geometry
	g = orb.LineString(g.(orb.Polygon)[0])

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := Geometry(g, 10)
		if err != nil {
			b.Fatalf("unexpected error: %v", err)
		}
	}
}
