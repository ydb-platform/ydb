package solomon

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_metrics_encode(t *testing.T) {
	expectHeader := []byte{
		0x53, 0x50, // magic
		0x01, 0x01, // version
		0x18, 0x00, // header size
		0x0,                // time precision
		0x0,                // compression algorithm
		0x7, 0x0, 0x0, 0x0, // label names size
		0x8, 0x0, 0x0, 0x0, // label values size
		0x1, 0x0, 0x0, 0x0, // metric count
		0x1, 0x0, 0x0, 0x0, // point count
		// label names pool
		0x73, 0x65, 0x6e, 0x73, 0x6f, 0x72, 0x0, // "sensor"
		// label values pool
		0x6d, 0x79, 0x67, 0x61, 0x75, 0x67, 0x65, 0x0, // "gauge"
	}

	testCases := []struct {
		name               string
		metrics            *Metrics
		expectCommonTime   []byte
		expectCommonLabels []byte
		expectMetrics      [][]byte
		expectWritten      int
	}{
		{
			"common-ts+gauge",
			&Metrics{
				metrics: []Metric{
					func() Metric {
						g := NewGauge("mygauge", 43)
						return &g
					}(),
				},
				timestamp: timeAsRef(time.Unix(1500000000, 0)),
			},
			[]byte{0x0, 0x2f, 0x68, 0x59}, // common time  /1500000000
			[]byte{0x0},                   // common labels count and indexes
			[][]byte{
				{
					0x5, // types
					0x0, // flags
					0x1, // labels index size
					0x0, // indexes of name labels
					0x0, // indexes of value labels

					0x00, 0x00, 0x00, 0x00, 0x00, 0x80, 0x45, 0x40, // 43  // metrics value

				},
			},
			57,
		},
		{
			"gauge+ts",
			&Metrics{
				metrics: []Metric{
					func() Metric {
						g := NewGauge("mygauge", 43, WithTimestamp(time.Unix(1657710476, 0)))
						return &g
					}(),
				},
			},
			[]byte{0x0, 0x0, 0x0, 0x0}, // common time
			[]byte{0x0},                // common labels count and indexes
			[][]byte{
				{
					0x6, // uint8(typeGauge << 2) | uint8(valueTypeOneWithTS)
					0x0, // flags
					0x1, // labels index size
					0x0, // indexes of name labels
					0x0, // indexes of value labels

					0x8c, 0xa7, 0xce, 0x62, //metric ts
					0x00, 0x00, 0x00, 0x00, 0x00, 0x80, 0x45, 0x40, // 43  // metrics value

				},
			},
			61,
		},
		{
			"common-ts+gauge+ts",
			&Metrics{
				metrics: []Metric{
					func() Metric {
						g := NewGauge("mygauge", 43, WithTimestamp(time.Unix(1657710476, 0)))
						return &g
					}(),
					func() Metric {
						g := NewGauge("mygauge", 42, WithTimestamp(time.Unix(1500000000, 0)))
						return &g
					}(),
				},
				timestamp: timeAsRef(time.Unix(1500000000, 0)),
			},
			[]byte{0x0, 0x2f, 0x68, 0x59}, // common time  /1500000000
			[]byte{0x0},                   // common labels count and indexes
			[][]byte{
				{
					0x6, // types
					0x0, // flags
					0x1, // labels index size
					0x0, // indexes of name labels
					0x0, // indexes of value labels

					0x8c, 0xa7, 0xce, 0x62, //metric ts
					0x00, 0x00, 0x00, 0x00, 0x00, 0x80, 0x45, 0x40, // 43  // metrics value

				},
				{
					0x6, // types
					0x0, // flags
					0x1, // labels index size
					0x0, // indexes of name labels
					0x0, // indexes of value labels

					0x0, 0x2f, 0x68, 0x59, // metric ts
					0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x45, 0x40, //42 // metrics value

				},
			},
			78,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			ctx := context.Background()

			written, err := NewSpackEncoder(ctx, CompressionNone, tc.metrics).Encode(&buf)

			assert.NoError(t, err)
			assert.Equal(t, tc.expectWritten, written)

			body := buf.Bytes()
			setMetricsCount(expectHeader, len(tc.metrics.metrics))

			require.True(t, bytes.HasPrefix(body, expectHeader))
			body = body[len(expectHeader):]

			require.True(t, bytes.HasPrefix(body, tc.expectCommonTime))
			body = body[len(tc.expectCommonTime):]

			require.True(t, bytes.HasPrefix(body, tc.expectCommonLabels))
			body = body[len(tc.expectCommonLabels):]

			expectButMissing := [][]byte{}
			for range tc.expectMetrics {
				var seen bool
				var val []byte
				for _, v := range tc.expectMetrics {
					val = v
					if bytes.HasPrefix(body, v) {
						body = bytes.Replace(body, v, []byte{}, 1)
						seen = true
						break
					}
				}
				if !seen {
					expectButMissing = append(expectButMissing, val)
				}
			}
			assert.Empty(t, body, "unexpected bytes seen")
			assert.Empty(t, expectButMissing, "missing metrics bytes")
		})
	}
}

func setMetricsCount(header []byte, count int) {
	header[16] = uint8(count)
	header[20] = uint8(count)
}
