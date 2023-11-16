package ewkb

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"io/ioutil"
	"testing"

	"github.com/paulmach/orb"
	"github.com/paulmach/orb/encoding/internal/wkbcommon"
)

func TestMarshal(t *testing.T) {
	for _, g := range orb.AllGeometries {
		_, err := Marshal(g, 0, binary.BigEndian)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
}

func TestMustMarshal(t *testing.T) {
	for _, g := range orb.AllGeometries {
		MustMarshal(g, 0, binary.BigEndian)
	}
}

func MustDecodeHex(s string) []byte {
	b, err := hex.DecodeString(s)
	if err != nil {
		panic(err)
	}

	return b
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

func compare(t testing.TB, e orb.Geometry, s int, b []byte) {
	t.Helper()

	// Decoder
	g, srid, err := NewDecoder(bytes.NewReader(b)).Decode()
	if err != nil {
		t.Fatalf("decoder: read error: %v", err)
	}

	if !orb.Equal(g, e) {
		t.Errorf("decoder: incorrect geometry: %v != %v", g, e)
	}

	if srid != s {
		t.Errorf("decoder: incorrect srid: %v != %v", srid, s)
	}

	// Umarshal
	g, srid, err = Unmarshal(b)
	if err != nil {
		t.Fatalf("unmarshal: read error: %v", err)
	}

	if !orb.Equal(g, e) {
		t.Errorf("unmarshal: incorrect geometry: %v != %v", g, e)
	}

	if srid != s {
		t.Errorf("decoder: incorrect srid: %v != %v", srid, s)
	}

	// Marshal
	var data []byte
	if b[0] == 0 {
		data, err = Marshal(g, srid, binary.BigEndian)
	} else {
		data, err = Marshal(g, srid, binary.LittleEndian)
	}
	if err != nil {
		t.Fatalf("marshal error: %v", err)
	}

	if !bytes.Equal(data, b) {
		t.Logf("%v", data)
		t.Logf("%v", b)
		t.Errorf("marshal: incorrent encoding")
	}

	// Encode
	buf := bytes.NewBuffer(nil)
	en := NewEncoder(buf)
	if b[0] == 0 {
		en.SetByteOrder(binary.BigEndian)
	} else {
		en.SetByteOrder(binary.LittleEndian)
	}

	en.SetSRID(s)
	err = en.Encode(e)
	if err != nil {
		t.Errorf("encode error: %v", err)
	}

	if !bytes.Equal(data, buf.Bytes()) {
		t.Logf("%v", data)
		t.Logf("%v", b)
		t.Errorf("encode: incorrent encoding")
	}

	// pass in srid
	buf.Reset()
	en.SetSRID(10101)
	err = en.Encode(e, s)
	if err != nil {
		t.Errorf("encode with srid error: %v", err)
	}

	if !bytes.Equal(data, buf.Bytes()) {
		t.Logf("%v", data)
		t.Logf("%v", b)
		t.Errorf("encode with srid: incorrent encoding")
	}

	// preallocation
	if l := wkbcommon.GeomLength(e, s != 0); len(data) != l {
		t.Errorf("prealloc length: %v != %v", len(data), l)
	}
}
