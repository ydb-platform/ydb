package httppuller

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/ydb-platform/ydb/library/go/core/metrics"
	"github.com/ydb-platform/ydb/library/go/core/metrics/solomon"
	"github.com/ydb-platform/ydb/library/go/httputil/headers"
)

type testMetricsData struct {
	Metrics []struct {
		Type      string            `json:"type"`
		Labels    map[string]string `json:"labels"`
		Value     float64           `json:"value"`
		Histogram struct {
			Bounds  []float64 `json:"bounds"`
			Buckets []int64   `json:"buckets"`
			Inf     int64     `json:"inf"`
		} `json:"hist"`
	} `json:"metrics"`
}

type testStreamer struct{}

func (s testStreamer) StreamJSON(context.Context, io.Writer) (int, error) { return 0, nil }
func (s testStreamer) StreamSpack(context.Context, io.Writer, solomon.CompressionType) (int, error) {
	return 0, nil
}

func TestHandler_NewHandler(t *testing.T) {
	assert.PanicsWithValue(t, nilRegistryPanicMsg, func() { NewHandler(nil) })
	assert.PanicsWithValue(t, nilRegistryPanicMsg, func() { var s *solomon.Registry; NewHandler(s) })
	assert.PanicsWithValue(t, nilRegistryPanicMsg, func() { var ts *testStreamer; NewHandler(ts) })
	assert.NotPanics(t, func() { NewHandler(&solomon.Registry{}) })
	assert.NotPanics(t, func() { NewHandler(&testStreamer{}) })
	assert.NotPanics(t, func() { NewHandler(testStreamer{}) })
}

func TestHandler_ServeHTTP(t *testing.T) {
	testCases := []struct {
		name                    string
		registry                *solomon.Registry
		expectStatus            int
		expectedApplicationType headers.ContentType
		expectBody              []byte
	}{
		{
			"success_json",
			func() *solomon.Registry {
				r := solomon.NewRegistry(solomon.NewRegistryOpts())

				cnt := r.Counter("mycounter")
				cnt.Add(42)

				gg := r.Gauge("mygauge")
				gg.Set(2.4)

				hs := r.Histogram("myhistogram", metrics.NewBuckets(1, 2, 3))
				hs.RecordValue(0.5)
				hs.RecordValue(1.5)
				hs.RecordValue(1.7)
				hs.RecordValue(2.2)
				hs.RecordValue(42)

				return r
			}(),
			http.StatusOK,
			headers.TypeApplicationJSON,
			[]byte(`
				{
					"metrics": [
						{
						  	"type": "COUNTER",
						  	"labels": {
								"sensor": "mycounter"
						  	},
						  	"value": 42
						},
						{
						  	"type": "DGAUGE",
						  	"labels": {
								"sensor": "mygauge"
						  	},
						  	"value": 2.4
						},
						{
						  	"type": "HIST",
						  	"labels": {
								"sensor": "myhistogram"
						  	},
						  	"hist": {
								"bounds": [
							  		1,
							  		2,
							  		3
								],
								"buckets": [
							  		1,
							  		2,
							  		1
								],
								"inf": 1
						  	}
						}
					]
				}
			`),
		},
		{
			"success_spack",
			func() *solomon.Registry {
				r := solomon.NewRegistry(solomon.NewRegistryOpts())
				_ = r.Histogram("histogram", metrics.NewBuckets(0, 0.1, 0.11))
				return r
			}(),
			http.StatusOK,
			headers.TypeApplicationXSolomonSpack,
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
				// label names pool
				0x73, 0x65, 0x6e, 0x73, 0x6f, 0x72, 0x0, // "sensor"
				// label values pool
				0x68, 0x69, 0x73, 0x74, 0x6F, 0x67, 0x72, 0x61, 0x6D, 0x0, // "histogram"
				// common time
				0x0, 0x0, 0x0, 0x0,
				// common labels
				0x0,
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
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			r, _ := http.NewRequest("GET", "/metrics", nil)

			var h http.Handler
			if tc.expectedApplicationType == headers.TypeApplicationXSolomonSpack {
				h = NewHandler(tc.registry, WithSpack())
			} else {
				h = NewHandler(tc.registry)
			}

			r.Header.Set(headers.AcceptKey, tc.expectedApplicationType.String())
			h.ServeHTTP(w, r)
			assert.Equal(t, tc.expectStatus, w.Code)
			assert.Equal(t, tc.expectedApplicationType.String(), w.Header().Get(headers.ContentTypeKey))

			if tc.expectedApplicationType == headers.TypeApplicationXSolomonSpack {
				assert.EqualValues(t, tc.expectBody, w.Body.Bytes())
			} else {
				var expectedObj, givenObj testMetricsData
				err := json.Unmarshal(tc.expectBody, &expectedObj)
				assert.NoError(t, err)
				err = json.Unmarshal(w.Body.Bytes(), &givenObj)
				assert.NoError(t, err)

				sort.Slice(expectedObj.Metrics, func(i, j int) bool {
					return expectedObj.Metrics[i].Type < expectedObj.Metrics[j].Type
				})
				sort.Slice(givenObj.Metrics, func(i, j int) bool {
					return givenObj.Metrics[i].Type < givenObj.Metrics[j].Type
				})

				assert.EqualValues(t, expectedObj, givenObj)
			}
		})
	}
}
