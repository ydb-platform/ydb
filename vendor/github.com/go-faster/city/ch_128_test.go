package city

import (
	"bufio"
	"bytes"
	_ "embed"
	"fmt"
	"strconv"
	"strings"
	"testing"
)

//go:embed _testdata/ch128.csv
var ch128data []byte

func TestCH128(t *testing.T) {
	r := bufio.NewScanner(bytes.NewReader(ch128data))
	for r.Scan() {
		elems := strings.Split(r.Text(), ",")

		s := []byte(elems[0])

		lo, _ := strconv.ParseUint(elems[1], 16, 64)
		hi, _ := strconv.ParseUint(elems[2], 16, 64)

		v := CH128(s)
		if lo != v.Low || hi != v.High {
			t.Errorf("mismatch %d", len(s))
		}
	}
}

func benchmarkClickHouse128(n int) func(b *testing.B) {
	return func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(n))
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			CH128(data[:n])
		}
	}
}

func BenchmarkClickHouse128(b *testing.B) {
	setup()

	for _, size := range []int{
		16, 64, 256, 1024,
	} {
		b.Run(fmt.Sprintf("%d", size), benchmarkClickHouse128(size))
	}
}
