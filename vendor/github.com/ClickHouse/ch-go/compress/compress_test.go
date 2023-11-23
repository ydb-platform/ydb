package compress

import (
	"bytes"
	"io"
	"math/rand"
	"os"
	"strings"
	"testing"

	"github.com/go-faster/city"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/ch-go/internal/gold"
)

func TestMain(m *testing.M) {
	// Explicitly registering flags for golden files.
	gold.Init()

	os.Exit(m.Run())
}

func TestCompress(t *testing.T) {
	data := []byte(strings.Repeat("Hello!\n", 25))
	gold.Bytes(t, data, "data_raw")

	for i := range MethodValues() {
		m := MethodValues()[i]
		t.Run(m.String(), func(t *testing.T) {
			w := NewWriter()
			require.NoError(t, w.Compress(m, data))

			gold.Bytes(t, w.Data, "data_compressed_"+strings.ToLower(m.String()))

			br := bytes.NewReader(nil)
			r := NewReader(br)
			out := make([]byte, len(data))

			for i := 0; i < 10; i++ {
				br.Reset(w.Data)
				_, err := io.ReadFull(r, out)
				require.NoError(t, err)
				require.Equal(t, data, out)
			}

			t.Run("NoShortRead", func(t *testing.T) {
				for i := 0; i < len(w.Data); i++ {
					b := w.Data[:i]
					r := NewReader(bytes.NewReader(b))
					_, err := io.ReadFull(r, out)
					require.Error(t, err)
				}
			})
			t.Run("CheckHash", func(t *testing.T) {
				// Corrupt bytes of data or checksum.
				var ref city.U128
				for i := 0; i < len(w.Data); i++ {
					b := append([]byte{}, w.Data...) // clone
					b[i]++
					r := NewReader(bytes.NewReader(b))
					_, err := io.ReadFull(r, out)
					require.Error(t, err)
					if i > headerSize {
						var badData *CorruptedDataErr
						require.ErrorAs(t, err, &badData)
						if ref.High == 0 && ref.Low == 0 {
							// Pick first reference.
							ref = badData.Reference
						}
						require.Equal(t, FormatU128(ref), FormatU128(badData.Reference))
						require.Equal(t, ref, badData.Reference)
					}
				}
			})
		})
	}
}

func BenchmarkWriter_Compress(b *testing.B) {
	// Highly compressible data.
	data := bytes.Repeat([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}, 1800)

	for i := range MethodValues() {
		m := MethodValues()[i]
		b.Run(m.String(), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(data)))

			w := NewWriter()

			// First round to warmup.
			if err := w.Compress(m, data); err != nil {
				b.Fatal(err)
			}

			for i := 0; i < b.N; i++ {
				if err := w.Compress(m, data); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func randData(n int) []byte {
	s := rand.NewSource(10)
	r := rand.New(s)
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		panic(err)
	}
	return buf
}

func BenchmarkReader_Read(b *testing.B) {
	// Not compressible data.
	data := randData(1024 * 20)

	for i := range MethodValues() {
		m := MethodValues()[i]
		b.Run(m.String(), func(b *testing.B) {
			w := NewWriter()
			if err := w.Compress(m, data); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.SetBytes(int64(len(w.Data)))

			out := make([]byte, len(data))

			br := bytes.NewReader(nil)
			r := NewReader(br)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				br.Reset(w.Data)
				if _, err := io.ReadFull(r, out); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
