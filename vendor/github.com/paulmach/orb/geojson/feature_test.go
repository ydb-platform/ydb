package geojson

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/paulmach/orb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
)

func TestNewFeature(t *testing.T) {
	f := NewFeature(orb.Point{1, 2})

	if f.Type != "Feature" {
		t.Errorf("incorrect feature: %v != Feature", f.Type)
	}
}

func TestFeatureMarshalJSON(t *testing.T) {
	f := NewFeature(orb.Point{1, 2})
	blob, err := f.MarshalJSON()

	if err != nil {
		t.Fatalf("error marshalling to json: %v", err)
	}

	if !bytes.Contains(blob, []byte(`"properties":null`)) {
		t.Errorf("json should set properties to null if there are none")
	}
}

func TestFeatureMarshalJSON_BBox(t *testing.T) {
	f := NewFeature(orb.Bound{Min: orb.Point{1, 1}, Max: orb.Point{2, 2}})

	// bbox empty
	f.BBox = nil
	blob, err := f.MarshalJSON()
	if err != nil {
		t.Fatalf("error marshalling to json: %v", err)
	}

	if bytes.Contains(blob, []byte(`"bbox"`)) {
		t.Errorf("should not set the bbox value")
	}

	// some bbox
	f.BBox = []float64{1, 2, 3, 4}
	blob, err = f.MarshalJSON()
	if err != nil {
		t.Fatalf("error marshalling to json: %v", err)
	}

	if !bytes.Contains(blob, []byte(`"bbox":[1,2,3,4]`)) {
		t.Errorf("should set type to polygon coords: %v", string(blob))
	}
}

func TestFeatureMarshalJSON_Bound(t *testing.T) {
	f := NewFeature(orb.Bound{Min: orb.Point{1, 1}, Max: orb.Point{2, 2}})
	blob, err := f.MarshalJSON()

	if err != nil {
		t.Fatalf("error marshalling to json: %v", err)
	}

	if !bytes.Contains(blob, []byte(`"type":"Polygon"`)) {
		t.Errorf("should set type to polygon")
	}

	if !bytes.Contains(blob, []byte(`"coordinates":[[[1,1],[2,1],[2,2],[1,2],[1,1]]]`)) {
		t.Errorf("should set type to polygon coords: %v", string(blob))
	}
}

func TestFeatureMarshal(t *testing.T) {
	f := NewFeature(orb.Point{1, 2})
	blob, err := json.Marshal(f)

	if err != nil {
		t.Fatalf("should marshal to json just fine but got %v", err)
	}

	if !bytes.Contains(blob, []byte(`"properties":null`)) {
		t.Errorf("json should set properties to null if there are none")
	}
	if !bytes.Contains(blob, []byte(`"type":"Feature"`)) {
		t.Errorf("json should set properties to null if there are none")
	}
}

func TestFeature_marshalValue(t *testing.T) {
	f := NewFeature(orb.Point{1, 2})

	// json
	blob, err := json.Marshal(*f)
	if err != nil {
		t.Fatalf("should marshal to json just fine but got %v", err)
	}

	if !bytes.Contains(blob, []byte(`"properties":null`)) {
		t.Errorf("json should set properties to null if there are none")
	}

	// bson
	blob, err = bson.Marshal(*f)
	if err != nil {
		t.Fatalf("should marshal to bson just fine but got %v", err)
	}

	if !bytes.Contains(blob, append([]byte{byte(bsontype.Null)}, []byte("properties")...)) {
		t.Errorf("json should set properties to null if there are none")
	}
}

func TestUnmarshalFeature(t *testing.T) {
	rawJSON := `
	  { "type": "Feature",
	    "geometry": {"type": "Point", "coordinates": [102.0, 0.5]},
	    "properties": {"prop0": "value0"}
	  }`

	f, err := UnmarshalFeature([]byte(rawJSON))
	if err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}

	if f.Type != "Feature" {
		t.Errorf("should have type of Feature got: %v", f.Type)
	}

	if len(f.Properties) != 1 {
		t.Errorf("should have 1 property but got: %v", f.Properties)
	}

	// not a feature
	data, _ := NewFeatureCollection().MarshalJSON()
	_, err = UnmarshalFeature(data)
	if err == nil {
		t.Error("should return error if not a feature")
	}

	if !strings.Contains(err.Error(), "not a feature") {
		t.Errorf("incorrect error: %v", err)
	}

	// invalid json
	_, err = UnmarshalFeature([]byte(`{"type": "Feature",`)) // truncated
	if err == nil {
		t.Errorf("should return error for invalid json")
	}

	f = &Feature{}
	err = f.UnmarshalJSON([]byte(`{"type": "Feature",`)) // truncated
	if err == nil {
		t.Errorf("should return error for invalid json")
	}
}

func TestUnmarshalFeature_GeometryCollection(t *testing.T) {
	rawJSON := `
	  { "type": "Feature",
	    "geometry": {"type":"GeometryCollection","geometries":[{"type": "Point", "coordinates": [102.0, 0.5]}]}
	  }`

	f, err := UnmarshalFeature([]byte(rawJSON))
	if err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}

	wantType := orb.Collection{}.GeoJSONType()
	if f.Geometry.GeoJSONType() != wantType {
		t.Fatalf("invalid GeoJSONType: %v", f.Geometry.GeoJSONType())
	}
}

func TestUnmarshalFeature_missingGeometry(t *testing.T) {
	t.Run("empty geometry", func(t *testing.T) {
		rawJSON := `{ "type": "Feature", "geometry": {} }`

		_, err := UnmarshalFeature([]byte(rawJSON))
		if err != ErrInvalidGeometry {
			t.Fatalf("incorrect unmarshal error: %v", err)
		}
	})

	t.Run("missing geometry", func(t *testing.T) {
		rawJSON := `{ "type": "Feature" }`

		f, err := UnmarshalFeature([]byte(rawJSON))
		if err != nil {
			t.Fatalf("should not error: %v", err)
		}

		if f == nil {
			t.Fatalf("feature should not be nil")
		}
	})
}

func TestUnmarshalBSON_missingGeometry(t *testing.T) {
	t.Run("missing geometry", func(t *testing.T) {
		f := NewFeature(nil)
		f.Geometry = nil

		data, err := bson.Marshal(f)
		if err != nil {
			t.Fatalf("marshal error: %v", err)
		}

		nf := &Feature{}
		err = bson.Unmarshal(data, &nf)
		if err != nil {
			t.Fatalf("unmarshal error: %v", err)
		}

		if f.Geometry != nil {
			t.Fatalf("geometry should be nil")
		}

		if f == nil {
			t.Fatalf("feature should not be nil")
		}
	})
}

func TestUnmarshalFeature_BBox(t *testing.T) {
	rawJSON := `
	  { "type": "Feature",
	    "geometry": {"type": "Point", "coordinates": [102.0, 0.5]},
		"bbox": [1,2,3,4],
	    "properties": {"prop0": "value0"}
	  }`

	f, err := UnmarshalFeature([]byte(rawJSON))
	if err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}

	if !f.BBox.Valid() {
		t.Errorf("bbox should be valid: %v", f.BBox)
	}
}

func TestMarshalFeatureID(t *testing.T) {
	f := &Feature{
		ID: "asdf",
	}

	data, err := f.MarshalJSON()
	if err != nil {
		t.Fatalf("should marshal, %v", err)
	}

	if !bytes.Equal(data, []byte(`{"id":"asdf","type":"Feature","geometry":null,"properties":null}`)) {
		t.Errorf("data not correct")
		t.Logf("%v", string(data))
	}

	f.ID = 123
	data, err = f.MarshalJSON()
	if err != nil {
		t.Fatalf("should marshal, %v", err)

	}

	if !bytes.Equal(data, []byte(`{"id":123,"type":"Feature","geometry":null,"properties":null}`)) {
		t.Errorf("data not correct")
		t.Logf("%v", string(data))
	}
}

func TestUnmarshalFeatureID(t *testing.T) {
	rawJSON := `
	  { "type": "Feature",
	    "id": 123,
	    "geometry": {"type": "Point", "coordinates": [102.0, 0.5]}
	  }`

	f, err := UnmarshalFeature([]byte(rawJSON))
	if err != nil {
		t.Fatalf("should unmarshal feature without issue, err %v", err)
	}

	if v, ok := f.ID.(float64); !ok || v != 123 {
		t.Errorf("should parse id as number, got %T %f", f.ID, v)
	}

	rawJSON = `
	  { "type": "Feature",
	    "id": "abcd",
	    "geometry": {"type": "Point", "coordinates": [102.0, 0.5]}
	  }`

	f, err = UnmarshalFeature([]byte(rawJSON))
	if err != nil {
		t.Fatalf("should unmarshal feature without issue, err %v", err)
	}

	if v, ok := f.ID.(string); !ok || v != "abcd" {
		t.Errorf("should parse id as string, got %T %s", f.ID, v)
	}
}

func TestMarshalRing(t *testing.T) {
	ring := orb.Ring{{0, 0}, {1, 1}, {2, 1}, {0, 0}}

	f := NewFeature(ring)
	data, err := f.MarshalJSON()
	if err != nil {
		t.Fatalf("should marshal, %v", err)
	}

	if !bytes.Equal(data, []byte(`{"type":"Feature","geometry":{"type":"Polygon","coordinates":[[[0,0],[1,1],[2,1],[0,0]]]},"properties":null}`)) {
		t.Errorf("data not correct")
		t.Logf("%v", string(data))
	}
}

// uncomment to test/benchmark custom json marshalling
// func init() {
// 	var c = jsoniter.Config{
// 		EscapeHTML:              true,
// 		SortMapKeys:             false,
// 		ValidateJsonRawMessage:  false,
// 		MarshalFloatWith6Digits: true,
// 	}.Froze()

// 	CustomJSONMarshaler = c
// 	CustomJSONUnmarshaler = c
// }

func BenchmarkFeatureMarshalJSON(b *testing.B) {
	data, err := ioutil.ReadFile("../encoding/mvt/testdata/16-17896-24449.json")
	if err != nil {
		b.Fatalf("could not open file: %v", err)
	}

	tile := map[string]*FeatureCollection{}
	err = json.Unmarshal(data, &tile)
	if err != nil {
		b.Fatalf("could not unmarshal: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := marshalJSON(tile)
		if err != nil {
			b.Fatalf("marshal error: %v", err)
		}
	}
}

func BenchmarkFeatureUnmarshalJSON(b *testing.B) {
	data, err := ioutil.ReadFile("../encoding/mvt/testdata/16-17896-24449.json")
	if err != nil {
		b.Fatalf("could not open file: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tile := map[string]*FeatureCollection{}
		err = unmarshalJSON(data, &tile)
		if err != nil {
			b.Fatalf("could not unmarshal: %v", err)
		}
	}
}

func BenchmarkFeatureMarshalBSON(b *testing.B) {
	data, err := ioutil.ReadFile("../encoding/mvt/testdata/16-17896-24449.json")
	if err != nil {
		b.Fatalf("could not open file: %v", err)
	}

	tile := map[string]*FeatureCollection{}
	err = json.Unmarshal(data, &tile)
	if err != nil {
		b.Fatalf("could not unmarshal: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := bson.Marshal(tile)
		if err != nil {
			b.Fatalf("marshal error: %v", err)
		}
	}
}

func BenchmarkFeatureUnmarshalBSON(b *testing.B) {
	data, err := ioutil.ReadFile("../encoding/mvt/testdata/16-17896-24449.json")
	if err != nil {
		b.Fatalf("could not open file: %v", err)
	}

	tile := map[string]*FeatureCollection{}
	err = json.Unmarshal(data, &tile)
	if err != nil {
		b.Fatalf("could not unmarshal: %v", err)
	}

	bdata, err := bson.Marshal(tile)
	if err != nil {
		b.Fatalf("could not marshal: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tile := map[string]*FeatureCollection{}
		err = bson.Unmarshal(bdata, &tile)
		if err != nil {
			b.Fatalf("could not unmarshal: %v", err)
		}
	}
}
