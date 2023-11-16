package simplify

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/paulmach/orb"
	"github.com/paulmach/orb/planar"
)

func TestDouglasPeucker_BenchmarkData(t *testing.T) {
	cases := []struct {
		threshold float64
		length    int
	}{
		{0.1, 1118},
		{0.5, 257},
		{1.0, 144},
		{1.5, 95},
		{2.0, 71},
		{3.0, 46},
		{4.0, 39},
		{5.0, 33},
	}

	ls := benchmarkData()

	for i, tc := range cases {
		r := DouglasPeucker(tc.threshold).LineString(ls.Clone())
		if len(r) != tc.length {
			t.Errorf("%d: reduced poorly, %d != %d", i, len(r), tc.length)
		}
	}
}

func BenchmarkDouglasPeucker(b *testing.B) {
	ls := benchmarkData()

	var data []orb.LineString
	for i := 0; i < b.N; i++ {
		data = append(data, ls.Clone())
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		DouglasPeucker(0.1).LineString(data[i])
	}
}

func TestRadial_BenchmarkData(t *testing.T) {
	cases := []struct {
		threshold float64
		length    int
	}{
		{0.1, 8282},
		{0.5, 2023},
		{1.0, 1043},
		{1.5, 703},
		{2.0, 527},
		{3.0, 350},
		{4.0, 262},
		{5.0, 209},
	}

	ls := benchmarkData()
	for i, tc := range cases {
		r := Radial(planar.Distance, tc.threshold).LineString(ls.Clone())
		if len(r) != tc.length {
			t.Errorf("%d: data reduced poorly: %v != %v", i, len(r), tc.length)
		}
	}
}

func BenchmarkRadial(b *testing.B) {
	ls := benchmarkData()

	var data []orb.LineString
	for i := 0; i < b.N; i++ {
		data = append(data, ls.Clone())
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Radial(planar.Distance, 0.1).LineString(data[i])
	}
}

func BenchmarkRadial_DisanceSquared(b *testing.B) {
	ls := benchmarkData()

	var data []orb.LineString
	for i := 0; i < b.N; i++ {
		data = append(data, ls.Clone())
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Radial(planar.DistanceSquared, 0.1*0.1).LineString(data[i])
	}
}

func TestVisvalingam_BenchmarkData(t *testing.T) {
	cases := []struct {
		threshold float64
		length    int
	}{
		{0.1, 867},
		{0.5, 410},
		{1.0, 293},
		{1.5, 245},
		{2.0, 208},
		{3.0, 169},
		{4.0, 151},
		{5.0, 135},
	}

	ls := benchmarkData()
	for i, tc := range cases {
		r := VisvalingamThreshold(tc.threshold).LineString(ls.Clone())
		if len(r) != tc.length {
			t.Errorf("%d: data reduced poorly: %v != %v", i, len(r), tc.length)
		}
	}
}

func BenchmarkVisvalingam_Threshold(b *testing.B) {
	ls := benchmarkData()

	var data []orb.LineString
	for i := 0; i < b.N; i++ {
		data = append(data, ls.Clone())
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		VisvalingamThreshold(0.1).LineString(data[i])
	}
}

func BenchmarkVisvalingam_Keep(b *testing.B) {
	ls := benchmarkData()
	toKeep := int(float64(len(ls)) / 1.616)

	var data []orb.LineString
	for i := 0; i < b.N; i++ {
		data = append(data, ls.Clone())
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		VisvalingamKeep(toKeep).LineString(data[i])
	}
}

func benchmarkData() orb.LineString {
	// Data taken from the simplify-js example at http://mourner.github.io/simplify-js/
	f, err := os.Open("testdata/lisbon2portugal.json")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	var points []float64
	err = json.NewDecoder(f).Decode(&points)
	if err != nil {
		panic(err)
	}

	var ls orb.LineString
	for i := 0; i < len(points); i += 2 {
		ls = append(ls, orb.Point{points[i], points[i+1]})
	}

	return ls
}
