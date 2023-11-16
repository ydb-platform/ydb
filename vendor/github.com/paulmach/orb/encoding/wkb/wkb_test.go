package wkb

import (
	"bytes"
	"encoding/binary"
	"io/ioutil"
	"testing"

	"github.com/paulmach/orb"
	"github.com/paulmach/orb/encoding/internal/wkbcommon"
)

func TestMarshal(t *testing.T) {
	for _, g := range orb.AllGeometries {
		_, err := Marshal(g, binary.BigEndian)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
}

func TestMustMarshal(t *testing.T) {
	for _, g := range orb.AllGeometries {
		MustMarshal(g, binary.BigEndian)
	}
}

func BenchmarkEncode_Point(b *testing.B) {
	g := orb.Point{1, 2}
	e := NewEncoder(ioutil.Discard)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := e.Encode(g)
		if err != nil {
			b.Fatalf("unexpected error: %v", err)
		}
	}
}

func BenchmarkEncode_LineString(b *testing.B) {
	g := orb.LineString{
		{1, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 5},
		{1, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 5},
	}
	e := NewEncoder(ioutil.Discard)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := e.Encode(g)
		if err != nil {
			b.Fatalf("unexpected error: %v", err)
		}
	}
}

func compare(t testing.TB, e orb.Geometry, b []byte) {
	t.Helper()

	// Decoder
	g, err := NewDecoder(bytes.NewReader(b)).Decode()
	if err != nil {
		t.Fatalf("decoder: read error: %v", err)
	}

	if !orb.Equal(g, e) {
		t.Errorf("decoder: incorrect geometry: %v != %v", g, e)
	}

	// Umarshal
	g, err = Unmarshal(b)
	if err != nil {
		t.Fatalf("unmarshal: read error: %v", err)
	}

	if !orb.Equal(g, e) {
		t.Errorf("unmarshal: incorrect geometry: %v != %v", g, e)
	}

	var data []byte
	if b[0] == 0 {
		data, err = Marshal(g, binary.BigEndian)
	} else {
		data, err = Marshal(g, binary.LittleEndian)
	}
	if err != nil {
		t.Fatalf("marshal error: %v", err)
	}

	if !bytes.Equal(data, b) {
		t.Logf("%v", data)
		t.Logf("%v", b)
		t.Errorf("marshal: incorrent encoding")
	}

	// preallocation
	if l := wkbcommon.GeomLength(e, false); len(data) != l {
		t.Errorf("prealloc length: %v != %v", len(data), l)
	}

	// Scanner
	var sg orb.Geometry

	switch e.(type) {
	case orb.Point:
		var p orb.Point
		err = Scanner(&p).Scan(b)
		sg = p
	case orb.MultiPoint:
		var mp orb.MultiPoint
		err = Scanner(&mp).Scan(b)
		sg = mp
	case orb.LineString:
		var ls orb.LineString
		err = Scanner(&ls).Scan(b)
		sg = ls
	case orb.MultiLineString:
		var mls orb.MultiLineString
		err = Scanner(&mls).Scan(b)
		sg = mls
	case orb.Polygon:
		var p orb.Polygon
		err = Scanner(&p).Scan(b)
		sg = p
	case orb.MultiPolygon:
		var mp orb.MultiPolygon
		err = Scanner(&mp).Scan(b)
		sg = mp
	case orb.Collection:
		var c orb.Collection
		err = Scanner(&c).Scan(b)
		sg = c
	default:
		t.Fatalf("unknown type: %T", e)
	}

	if err != nil {
		t.Errorf("scan error: %v", err)
	}

	if sg.GeoJSONType() != e.GeoJSONType() {
		t.Errorf("scanning to wrong type: %v != %v", sg.GeoJSONType(), e.GeoJSONType())
	}

	if !orb.Equal(sg, e) {
		t.Errorf("scan: incorrect geometry: %v != %v", sg, e)
	}
}
