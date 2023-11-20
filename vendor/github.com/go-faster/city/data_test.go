package city_test

import (
	_ "embed"
	"encoding/json"
	"testing"

	"github.com/go-faster/city"
)

// Generated with:
// 	go run ./internal/citygen > _testdata/data.json
// Not worth it to use go:generate here.

//go:embed _testdata/data.json
var testData []byte

func TestData(t *testing.T) {
	var data struct {
		Seed    city.U128
		Entries []struct {
			Input             string
			City32            uint32
			City64            uint64
			City128           city.U128
			City128Seed       city.U128
			ClickHouse64      uint64
			ClickHouse128     city.U128
			ClickHouse128Seed city.U128
		}
	}
	if err := json.Unmarshal(testData, &data); err != nil {
		t.Fatal(err)
	}

	for _, e := range data.Entries {
		input := []byte(e.Input)

		if v := city.Hash32(input); v != e.City32 {
			t.Errorf("Hash32(%q) %d (got) != %d (expected)", input, v, e.City32)
		}
		if v := city.Hash64(input); v != e.City64 {
			t.Errorf("Hash64(%q) %d (got) != %d (expected)", input, v, e.City64)
		}
		if v := city.Hash128(input); v != e.City128 {
			t.Errorf("Hash128(%q) %v (got) != %v (expected)", input, v, e.City128)
		}
		if v := city.Hash128Seed(input, data.Seed); v != e.City128Seed {
			t.Errorf("Hash128Seed(%q, %v)  %v (got) != %v (expected)", input, data.Seed, v, e.City128Seed)
		}
		if v := city.CH64(input); v != e.ClickHouse64 {
			t.Errorf("CH64(%q) %v (got) != %v (expected)", input, v, e.City64)
		}
		if v := city.CH128(input); v != e.ClickHouse128 {
			t.Errorf("CH128(%q) %v (got) != %v (expected)", input, v, e.ClickHouse128)
		}
		if v := city.CH128Seed(input, data.Seed); v != e.ClickHouse128Seed {
			t.Errorf("CH128Seed(%q, %v) %v (got) != %v (expected)", input, data.Seed, v, e.ClickHouse128Seed)
		}
	}
}
