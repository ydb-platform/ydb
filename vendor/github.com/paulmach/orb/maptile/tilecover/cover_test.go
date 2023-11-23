package tilecover

import (
	"encoding/json"
	"io/ioutil"
	"math"
	"sort"
	"strings"
	"testing"

	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geo"
	"github.com/paulmach/orb/geojson"
	"github.com/paulmach/orb/maptile"
	"github.com/paulmach/orb/planar"
)

func TestTestdata(t *testing.T) {
	cases := []struct {
		name     string
		min, max maptile.Zoom
	}{
		{
			name: "blocky",
			min:  6, max: 6,
		},
		{
			name: "building",
			min:  18, max: 18,
		},
		{
			name: "degenring",
			min:  11, max: 15,
		},
		{
			name: "donut",
			min:  16, max: 16,
		},
		{
			name: "edgeline",
			min:  14, max: 14,
		},
		{
			name: "line",
			min:  1, max: 12,
		},
		{
			name: "multiline",
			min:  1, max: 8,
		},
		{
			name: "multipoint",
			min:  1, max: 12,
		},
		{
			name: "polygon",
			min:  1, max: 15,
		},
		{
			name: "pyramid",
			min:  10, max: 10,
		},
		{
			name: "russia",
			min:  6, max: 6,
		},
		{
			name: "small_poly",
			min:  10, max: 10,
		},
		{
			name: "spiked",
			min:  10, max: 10,
		},
		{
			name: "tetris",
			min:  10, max: 10,
		},
		{
			name: "uk",
			min:  7, max: 9,
		},
		{
			name: "zero",
			min:  10, max: 10,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := loadFeature(t, "./testdata/"+tc.name+".geojson")

			// ts := Geometry(f.Geometry, tc.max)
			// blob, _ := json.MarshalIndent(MergeUp(ts, tc.min).ToFeatureCollection(), "", "  ")
			// ioutil.WriteFile("./testdata/"+tc.name+"_out.geojson", blob, 0644)

			expected := loadFeatureCollection(t, "./testdata/"+tc.name+"_out.geojson")

			tiles, _ := Geometry(f.Geometry, tc.max)
			result := MergeUp(tiles, tc.min).ToFeatureCollection()
			compareFeatureCollections(t, tc.name, result, expected)

			tiles, _ = Geometry(f.Geometry, tc.max)
			result = MergeUpPartial(tiles, tc.min, 4).ToFeatureCollection()
			compareFeatureCollections(t, tc.name, result, expected)
		})
	}
}

func TestCountries(t *testing.T) {
	files, err := ioutil.ReadDir("./testdata/world")
	if err != nil {
		t.Errorf("could not read directory: %v", err)
	}

	var countries []string
	for _, info := range files {
		if !strings.Contains(info.Name(), "_out") {
			countries = append(countries, strings.Split(info.Name(), ".")[0])
		}
	}

	for _, country := range countries {
		t.Run(country, func(t *testing.T) {
			f := loadFeature(t, "./testdata/world/"+country+".geo.json")
			tiles, _ := Geometry(f.Geometry, 6)
			tiles = MergeUp(tiles, 1)

			expected := loadFeatureCollection(t, "./testdata/world/"+country+"_out.geojson")
			compareFeatureCollections(t, country, tiles.ToFeatureCollection(), expected)
		})
	}
}

func compareFeatureCollections(t testing.TB, name string, result, expected *geojson.FeatureCollection) {
	sortFC(result)
	sortFC(expected)

	t.Helper()

	if len(result.Features) != len(expected.Features) {
		t.Errorf("feature count mismatch: %v != %v", len(result.Features), len(expected.Features))
		output(t, name, result)
		return
	}

	failure := false
	for i := range result.Features {
		r := result.Features[i].Geometry.(orb.Polygon)
		e := expected.Features[i].Geometry.(orb.Polygon)

		rc, ra := planar.CentroidArea(r)
		ec, ea := planar.CentroidArea(e)

		if delta := math.Abs(ra - ea); delta > 0.01 {
			failure = true
			t.Errorf("f %d: area not equal: %v", i, delta)
		}

		if dist := geo.Distance(rc, ec); dist > 1 {
			failure = true
			t.Errorf("f %d: centroid far apart: %v", i, dist)
		}
	}

	if failure {
		output(t, name, result)
	}
}

func sortFC(fc *geojson.FeatureCollection) {
	sort.Slice(fc.Features, func(i, j int) bool {
		a := fc.Features[i].Geometry.(orb.Polygon)[0]
		b := fc.Features[j].Geometry.(orb.Polygon)[0]
		if a[0][0] != b[0][0] {
			return a[0][0] < b[0][0]
		}

		return a[0][1] < b[0][1]
	})
}

func loadFeature(t testing.TB, path string) *geojson.Feature {
	t.Helper()
	data, err := ioutil.ReadFile(path)
	if err != nil {
		t.Fatalf("unable to read file: %v", err)
	}

	f, err := geojson.UnmarshalFeature(data)
	if err == nil {
		return f
	}

	fc, err := geojson.UnmarshalFeatureCollection(data)
	if err == nil {
		if len(fc.Features) != 1 {
			t.Fatalf("must have 1 feature: %v", len(fc.Features))
		}
		return fc.Features[0]
	}

	g, err := geojson.UnmarshalGeometry(data)
	if err != nil {
		t.Fatalf("unable to unmarshal feature: %v", err)
	}

	return geojson.NewFeature(g.Geometry())
}

func loadFeatureCollection(t testing.TB, path string) *geojson.FeatureCollection {
	t.Helper()
	data, err := ioutil.ReadFile(path)
	if err != nil {
		t.Fatalf("unable to read file: %v", err)
	}

	fc, err := geojson.UnmarshalFeatureCollection(data)
	if err != nil {
		t.Fatalf("unable to unmarshal feature: %v", err)
	}

	count := 0
	for i := range fc.Features {
		if fc.Features[i].Properties["name"] != "original" {
			fc.Features[count] = fc.Features[i]
			count++
		}
	}
	fc.Features = fc.Features[:count]

	return fc
}

// output gets called if there is a test failure for debugging.
func output(t testing.TB, name string, r *geojson.FeatureCollection) {
	f := loadFeature(t, "./testdata/"+name+".geojson")
	if f.Properties == nil {
		f.Properties = make(geojson.Properties)
	}

	f.Properties["fill"] = "#FF0000"
	f.Properties["fill-opacity"] = "0.5"
	f.Properties["stroke"] = "#FF0000"
	f.Properties["name"] = "original"
	r.Append(f)

	data, err := json.MarshalIndent(r, "", " ")
	if err != nil {
		t.Fatalf("error marshalling json: %v", err)
	}

	err = ioutil.WriteFile("failure_"+name+".geojson", data, 0644)
	if err != nil {
		t.Fatalf("write file failure: %v", err)
	}
}
