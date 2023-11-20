package solomon

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb/library/go/core/metrics"
)

func Test_streamJson(t *testing.T) {
	testCases := []struct {
		name          string
		registry      *Registry
		expect        string
		expectWritten int
		expectErr     error
	}{
		{
			"success",
			func() *Registry {
				r := NewRegistry(NewRegistryOpts())

				cnt := r.Counter("mycounter")
				cnt.Add(42)

				gg := r.Gauge("mygauge")
				gg.Set(2)

				return r
			}(),
			`{"metrics":[{"type":"COUNTER","labels":{"sensor":"mycounter"},"value":42},{"type":"DGAUGE","labels":{"sensor":"mygauge"},"value":2}]}`,
			133,
			nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			ctx := context.Background()

			written, err := tc.registry.StreamJSON(ctx, w)

			if tc.expectErr == nil {
				assert.NoError(t, err)
			} else {
				assert.EqualError(t, err, tc.expectErr.Error())
			}

			assert.Equal(t, tc.expectWritten, written)
			assert.Equal(t, len(tc.expect), w.Body.Len())

			if tc.expect != "" {
				var expectedObj, givenObj map[string]interface{}
				err = json.Unmarshal([]byte(tc.expect), &expectedObj)
				assert.NoError(t, err)
				err = json.Unmarshal(w.Body.Bytes(), &givenObj)
				assert.NoError(t, err)

				sameMap(t, expectedObj, givenObj)
			}
		})
	}
}

func Test_streamSpack(t *testing.T) {
	testCases := []struct {
		name                 string
		registry             *Registry
		compression          CompressionType
		expectHeader         []byte
		expectLabelNamesPool [][]byte
		expectValueNamesPool [][]byte
		expectCommonTime     []byte
		expectCommonLabels   []byte
		expectMetrics        [][]byte
		expectWritten        int
	}{
		{
			"counter",
			func() *Registry {
				r := NewRegistry(NewRegistryOpts())

				cnt := r.Counter("counter")
				cnt.Add(42)

				return r
			}(),
			CompressionNone,
			[]byte{
				0x53, 0x50, // magic
				0x01, 0x01, // version
				0x18, 0x00, // header size
				0x0,                // time precision
				0x0,                // compression algorithm
				0x7, 0x0, 0x0, 0x0, // label names size
				0x8, 0x0, 0x0, 0x0, // label values size
				0x1, 0x0, 0x0, 0x0, // metric count
				0x1, 0x0, 0x0, 0x0, // point count
			},
			[][]byte{ // label names pool
				{0x73, 0x65, 0x6e, 0x73, 0x6f, 0x72}, // "sensor"
			},
			[][]byte{ // label values pool
				{0x63, 0x6f, 0x75, 0x6e, 0x74, 0x65, 0x72}, // "counter"
			},
			[]byte{0x0, 0x0, 0x0, 0x0}, // common time
			[]byte{0x0},                // common labels count and indexes
			[][]byte{
				{
					0x9, // types
					0x0, // flags
					0x1, // labels index size
					0x0, // indexes of name labels
					0x0, // indexes of value labels

					0x2a, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // 42 // metrics value
				},
			},
			57,
		},
		{
			"counter_lz4",
			func() *Registry {
				r := NewRegistry(NewRegistryOpts())

				cnt := r.Counter("counter")
				cnt.Add(0)

				return r
			}(),
			CompressionLz4,
			[]byte{
				0x53, 0x50, // magic
				0x01, 0x01, // version
				0x18, 0x00, // header size
				0x0,                // time precision
				0x3,                // compression algorithm
				0x7, 0x0, 0x0, 0x0, // label names size
				0x8, 0x0, 0x0, 0x0, // label values size
				0x1, 0x0, 0x0, 0x0, // metric count
				0x1, 0x0, 0x0, 0x0, // point count
				0x23, 0x00, 0x00, 0x00, // compressed length
				0x21, 0x00, 0x00, 0x00, // uncompressed length
				0xf0, 0x12,
			},
			[][]byte{ // label names pool
				{0x73, 0x65, 0x6e, 0x73, 0x6f, 0x72}, // "sensor"
			},
			[][]byte{ // label values pool
				{0x63, 0x6f, 0x75, 0x6e, 0x74, 0x65, 0x72}, // "counter"
			},
			[]byte{0x0, 0x0, 0x0, 0x0}, // common time
			[]byte{0x0},                // common labels count and indexes
			[][]byte{
				{
					0x9, // types
					0x0, // flags
					0x1, // labels index size
					0x0, // indexes of name labels
					0x0, // indexes of value labels

					0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 0 //metrics value
					0x10, 0x11, 0xa4, 0x22, // checksum
					0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // end stream
				},
			},
			83,
		},
		{
			"rate",
			func() *Registry {
				r := NewRegistry(NewRegistryOpts())

				cnt := r.Counter("counter")
				Rated(cnt)
				cnt.Add(0)

				return r
			}(),
			CompressionNone,
			[]byte{
				0x53, 0x50, // magic
				0x01, 0x01, // version
				0x18, 0x00, // header size
				0x0,                // time precision
				0x0,                // compression algorithm
				0x7, 0x0, 0x0, 0x0, // label names size
				0x8, 0x0, 0x0, 0x0, // label values size
				0x1, 0x0, 0x0, 0x0, // metric count
				0x1, 0x0, 0x0, 0x0, // point count
			},
			[][]byte{ // label names pool
				{0x73, 0x65, 0x6e, 0x73, 0x6f, 0x72}, // "sensor"
			},
			[][]byte{ // label values pool
				{0x63, 0x6f, 0x75, 0x6e, 0x74, 0x65, 0x72}, // "counter"
			},
			[]byte{0x0, 0x0, 0x0, 0x0}, // common time
			[]byte{0x0},                // common labels count and indexes
			[][]byte{
				{
					0xd, // types
					0x0, // flags
					0x1, // labels index size
					0x0, // indexes of name labels
					0x0, // indexes of value labels

					0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, //42 // metrics value
				},
			},
			57,
		},
		{
			"timer",
			func() *Registry {
				r := NewRegistry(NewRegistryOpts())

				t := r.Timer("timer")
				t.RecordDuration(2 * time.Second)

				return r
			}(),
			CompressionNone,
			[]byte{
				0x53, 0x50, // magic
				0x01, 0x01, // version
				0x18, 0x00, // header size
				0x0,                // time precision
				0x0,                // compression algorithm
				0x7, 0x0, 0x0, 0x0, // label names size
				0x6, 0x0, 0x0, 0x0, // label values size
				0x1, 0x0, 0x0, 0x0, // metric count
				0x1, 0x0, 0x0, 0x0, // point count
			},
			[][]byte{ // label names pool
				{0x73, 0x65, 0x6e, 0x73, 0x6f, 0x72}, // "sensor"
			},
			[][]byte{ // label values pool
				{0x74, 0x69, 0x6d, 0x65, 0x72}, // "timer"
			},
			[]byte{0x0, 0x0, 0x0, 0x0}, // common time
			[]byte{0x0},                // common labels count and indexes
			[][]byte{
				{
					0x5, // types
					0x0, // flags
					0x1, // labels index size
					0x0, // indexes of name labels
					0x0, // indexes of value labels

					0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x40, //2.0 // metrics value
				},
			},
			55,
		},
		{
			"gauge",
			func() *Registry {
				r := NewRegistry(NewRegistryOpts())

				g := r.Gauge("gauge")
				g.Set(42)

				return r
			}(),
			CompressionNone,
			[]byte{
				0x53, 0x50, // magic
				0x01, 0x01, // version
				0x18, 0x00, // header size
				0x0,                // time precision
				0x0,                // compression algorithm
				0x7, 0x0, 0x0, 0x0, // label names size
				0x6, 0x0, 0x0, 0x0, // label values size
				0x1, 0x0, 0x0, 0x0, // metric count
				0x1, 0x0, 0x0, 0x0, // point count
			},
			[][]byte{ // label names pool
				{0x73, 0x65, 0x6e, 0x73, 0x6f, 0x72}, // "sensor"
			},
			[][]byte{ // label values pool
				{0x67, 0x61, 0x75, 0x67, 0x65}, // "gauge"
			},
			[]byte{0x0, 0x0, 0x0, 0x0}, // common time
			[]byte{0x0},                // common labels count and indexes
			[][]byte{
				{
					0x5, // types
					0x0, // flags
					0x1, // labels index size
					0x0, // indexes of name labels
					0x0, // indexes of value labels

					0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x45, 0x40, //42 // metrics value

				},
			},
			55,
		},
		{
			"histogram",
			func() *Registry {
				r := NewRegistry(NewRegistryOpts())

				_ = r.Histogram("histogram", metrics.NewBuckets(0, 0.1, 0.11))

				return r
			}(),
			CompressionNone,
			[]byte{
				0x53, 0x50, // magic
				0x01, 0x01, // version
				0x18, 0x00, // header size
				0x0,                // time precision
				0x0,                // compression algorithm
				0x7, 0x0, 0x0, 0x0, // label names size
				0xa, 0x0, 0x0, 0x0, // label values size
				0x1, 0x0, 0x0, 0x0, // metric count
				0x1, 0x0, 0x0, 0x0, // point count
			},
			[][]byte{ // label names pool
				{0x73, 0x65, 0x6e, 0x73, 0x6f, 0x72}, // "sensor"
			},
			[][]byte{ // label values pool
				{0x68, 0x69, 0x73, 0x74, 0x6F, 0x67, 0x72, 0x61, 0x6D}, // "histogram"
			},
			[]byte{0x0, 0x0, 0x0, 0x0}, // common time
			[]byte{0x0},                // common labels count and indexes
			[][]byte{
				{
					/*types*/ 0x15,
					/*flags*/ 0x0,
					/*labels*/ 0x1, // ?
					/*name*/ 0x0,
					/*value*/ 0x0,
					/*buckets count*/ 0x3,
					/*upper bound 0*/ 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
					/*upper bound 1*/ 0x9a, 0x99, 0x99, 0x99, 0x99, 0x99, 0xb9, 0x3f,
					/*upper bound 2*/ 0x29, 0x5c, 0x8f, 0xc2, 0xf5, 0x28, 0xbc, 0x3f,
					/*counter 0*/ 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
					/*counter 1*/ 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
					/*counter 2*/ 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
				},
			},
			100,
		},
		{
			"rate_histogram",
			func() *Registry {
				r := NewRegistry(NewRegistryOpts())

				h := r.Histogram("histogram", metrics.NewBuckets(0, 0.1, 0.11))
				Rated(h)

				return r
			}(),
			CompressionNone,
			[]byte{
				0x53, 0x50, // magic
				0x01, 0x01, // version
				0x18, 0x00, // header size
				0x0,                // time precision
				0x0,                // compression algorithm
				0x7, 0x0, 0x0, 0x0, // label names size
				0xa, 0x0, 0x0, 0x0, // label values size
				0x1, 0x0, 0x0, 0x0, // metric count
				0x1, 0x0, 0x0, 0x0, // point count
			},
			[][]byte{ // label names pool
				{0x73, 0x65, 0x6e, 0x73, 0x6f, 0x72}, // "sensor"
			},
			[][]byte{ // label values pool
				{0x68, 0x69, 0x73, 0x74, 0x6F, 0x67, 0x72, 0x61, 0x6D}, // "histogram"
			},
			[]byte{0x0, 0x0, 0x0, 0x0}, // common time
			[]byte{0x0},                // common labels count and indexes
			[][]byte{
				{
					/*types*/ 0x19,
					/*flags*/ 0x0,
					/*labels*/ 0x1, // ?
					/*name*/ 0x0,
					/*value*/ 0x0,
					/*buckets count*/ 0x3,
					/*upper bound 0*/ 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
					/*upper bound 1*/ 0x9a, 0x99, 0x99, 0x99, 0x99, 0x99, 0xb9, 0x3f,
					/*upper bound 2*/ 0x29, 0x5c, 0x8f, 0xc2, 0xf5, 0x28, 0xbc, 0x3f,
					/*counter 0*/ 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
					/*counter 1*/ 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
					/*counter 2*/ 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
				},
			},
			100,
		},
		{
			"counter+timer",
			func() *Registry {
				r := NewRegistry(NewRegistryOpts())

				cnt := r.Counter("counter")
				cnt.Add(42)

				t := r.Timer("timer")
				t.RecordDuration(2 * time.Second)

				return r
			}(),
			CompressionNone,
			[]byte{
				0x53, 0x50, // magic
				0x01, 0x01, // version
				0x18, 0x00, // header size
				0x0,                // time precision
				0x0,                // compression algorithm
				0x7, 0x0, 0x0, 0x0, // label names size
				0xe, 0x0, 0x0, 0x0, // label values size
				0x2, 0x0, 0x0, 0x0, // metric count
				0x2, 0x0, 0x0, 0x0, // point count
			},
			[][]byte{ // label names pool
				{0x73, 0x65, 0x6e, 0x73, 0x6f, 0x72}, // "sensor"
			},
			[][]byte{ // label values pool
				{0x63, 0x6f, 0x75, 0x6e, 0x74, 0x65, 0x72}, // "counter"
				{0x74, 0x69, 0x6d, 0x65, 0x72},             // "timer"
			},
			[]byte{0x0, 0x0, 0x0, 0x0}, // common time
			[]byte{0x0},                // common labels count and indexes
			[][]byte{
				{
					/*types*/ 0x9,
					/*flags*/ 0x0,
					/*labels*/ 0x1, // ?
					/*name*/ 0x0,
					/*value*/ 0x0,
					/*metrics value*/ 0x2a, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, //42
				},
				{
					/*types*/ 0x5,
					/*flags*/ 0x0,
					/*labels*/ 0x1, // ?
					/*name*/ 0x0,
					/*value*/ 0x1,
					/*metrics value*/ 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x40, //2.0

				},
			},
			76,
		},
		{
			"gauge+histogram",
			func() *Registry {
				r := NewRegistry(NewRegistryOpts())

				g := r.Gauge("gauge")
				g.Set(42)

				_ = r.Histogram("histogram", metrics.NewBuckets(0, 0.1, 0.11))

				return r
			}(),
			CompressionNone,
			[]byte{
				0x53, 0x50, // magic
				0x01, 0x01, // version
				0x18, 0x00, // header size
				0x0,                // time precision
				0x0,                // compression algorithm
				0x7, 0x0, 0x0, 0x0, // label names size
				0x10, 0x0, 0x0, 0x0, // label values size
				0x2, 0x0, 0x0, 0x0, // metric count
				0x2, 0x0, 0x0, 0x0, // point count
			},
			[][]byte{ // label names pool
				{0x73, 0x65, 0x6e, 0x73, 0x6f, 0x72}, // "sensor"
			},
			[][]byte{ // label values pool
				{0x67, 0x61, 0x75, 0x67, 0x65},                         // "gauge"
				{0x68, 0x69, 0x73, 0x74, 0x6F, 0x67, 0x72, 0x61, 0x6D}, // "histogram"
			},
			[]byte{0x0, 0x0, 0x0, 0x0}, // common time
			[]byte{0x0},                // common labels count and indexes
			[][]byte{
				{

					/*types*/ 0x5,
					/*flags*/ 0x0,
					/*labels*/ 0x1, // ?
					/*name*/ 0x0,
					/*value*/ 0x0,
					/*metrics value*/ 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x45, 0x40, //42
				},
				{
					/*types*/ 0x15,
					/*flags*/ 0x0,
					/*labels*/ 0x1, // ?
					/*name*/ 0x0,
					/*value*/ 0x1,
					/*buckets count*/ 0x3,
					/*upper bound 0*/ 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
					/*upper bound 1*/ 0x9a, 0x99, 0x99, 0x99, 0x99, 0x99, 0xb9, 0x3f,
					/*upper bound 2*/ 0x29, 0x5c, 0x8f, 0xc2, 0xf5, 0x28, 0xbc, 0x3f,
					/*counter 0*/ 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
					/*counter 1*/ 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
					/*counter 2*/ 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
				},
			},
			119,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			ctx := context.Background()

			written, err := tc.registry.StreamSpack(ctx, w, tc.compression)

			assert.NoError(t, err)
			assert.Equal(t, tc.expectWritten, written)
			body := w.Body.Bytes()
			require.True(t, bytes.HasPrefix(body, tc.expectHeader))
			body = body[len(tc.expectHeader):]

			t.Logf("expectLabelNamesPool: %v", tc.expectLabelNamesPool)
			labelNamesPoolBytes := body[:len(bytes.Join(tc.expectLabelNamesPool, []byte{0x0}))+1]
			labelNamesPool := bytes.Split(bytes.Trim(labelNamesPoolBytes, "\x00"), []byte{0x0})
			require.ElementsMatch(t, tc.expectLabelNamesPool, labelNamesPool)
			body = body[len(labelNamesPoolBytes):]

			t.Logf("expectValueNamesPool: %v", tc.expectValueNamesPool)
			valueNamesPoolBytes := body[:len(bytes.Join(tc.expectValueNamesPool, []byte{0x0}))+1]
			valueNamesPool := bytes.Split(bytes.Trim(valueNamesPoolBytes, "\x00"), []byte{0x0})
			require.ElementsMatch(t, tc.expectValueNamesPool, valueNamesPool)
			body = body[len(valueNamesPoolBytes):]

			require.True(t, bytes.HasPrefix(body, tc.expectCommonTime))
			body = body[len(tc.expectCommonTime):]

			require.True(t, bytes.HasPrefix(body, tc.expectCommonLabels))
			body = body[len(tc.expectCommonLabels):]

			expectButMissing := [][]byte{}
			for idx := range tc.expectMetrics {
				var seen bool
				var val []byte
				for _, v := range tc.expectMetrics {
					val = v[:]
					fixValueNameIndex(idx, val)
					if bytes.HasPrefix(body, val) {
						body = bytes.Replace(body, val, []byte{}, 1)
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

func fixValueNameIndex(idx int, metric []byte) {
	// ASSUMPTION_FOR_TESTS: the size of the index is always equal to one
	// That is, the number of points in the metric is always one
	metric[4] = uint8(idx) // fix value name index
}

func sameMap(t *testing.T, expected, actual map[string]interface{}) bool {
	if !assert.Len(t, actual, len(expected)) {
		return false
	}

	for k := range expected {
		actualMetric := actual[k]
		if !assert.NotNil(t, actualMetric, "expected key %q not found", k) {
			return false
		}

		if !assert.ElementsMatch(t, expected[k], actualMetric, "%q must have same elements", k) {
			return false
		}
	}
	return true
}
