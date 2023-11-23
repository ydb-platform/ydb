package wkbcommon

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/paulmach/orb"
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

	var data []byte
	if b[0] == 0 {
		data, err = Marshal(g, s, binary.BigEndian)
	} else {
		data, err = Marshal(g, s, binary.LittleEndian)
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
	if l := GeomLength(e, srid != 0); len(data) != l {
		t.Errorf("prealloc length: %v != %v", len(data), l)
	}
}
