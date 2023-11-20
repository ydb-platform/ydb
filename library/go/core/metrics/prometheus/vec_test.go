package prometheus

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/ydb-platform/ydb/library/go/core/metrics"
)

func TestCounterVec(t *testing.T) {
	rg := NewRegistry(NewRegistryOpts())
	vec := rg.CounterVec("ololo", []string{"shimba", "looken"})
	mt := vec.With(map[string]string{
		"shimba": "boomba",
		"looken": "tooken",
	})

	assert.IsType(t, &CounterVec{}, vec)
	assert.IsType(t, &Counter{}, mt)

	vec.Reset()

	metrics, err := rg.Gather()
	assert.NoError(t, err)
	assert.Empty(t, metrics)
}

func TestCounterVec_RegisterAgain(t *testing.T) {
	rg := NewRegistry(NewRegistryOpts())
	vec1 := rg.CounterVec("ololo", []string{"shimba", "looken"}).(*CounterVec)
	vec2 := rg.CounterVec("ololo", []string{"shimba", "looken"}).(*CounterVec)
	assert.Same(t, vec1.vec, vec2.vec)
}

func TestGaugeVec(t *testing.T) {
	rg := NewRegistry(NewRegistryOpts())
	vec := rg.GaugeVec("ololo", []string{"shimba", "looken"})
	mt := vec.With(map[string]string{
		"shimba": "boomba",
		"looken": "tooken",
	})

	assert.IsType(t, &GaugeVec{}, vec)
	assert.IsType(t, &Gauge{}, mt)

	vec.Reset()

	metrics, err := rg.Gather()
	assert.NoError(t, err)
	assert.Empty(t, metrics)
}

func TestGaugeVec_RegisterAgain(t *testing.T) {
	rg := NewRegistry(NewRegistryOpts())
	vec1 := rg.GaugeVec("ololo", []string{"shimba", "looken"}).(*GaugeVec)
	vec2 := rg.GaugeVec("ololo", []string{"shimba", "looken"}).(*GaugeVec)
	assert.Same(t, vec1.vec, vec2.vec)
}

func TestTimerVec(t *testing.T) {
	rg := NewRegistry(NewRegistryOpts())
	vec := rg.TimerVec("ololo", []string{"shimba", "looken"})
	mt := vec.With(map[string]string{
		"shimba": "boomba",
		"looken": "tooken",
	})

	assert.IsType(t, &TimerVec{}, vec)
	assert.IsType(t, &Timer{}, mt)

	vec.Reset()

	metrics, err := rg.Gather()
	assert.NoError(t, err)
	assert.Empty(t, metrics)
}

func TestTimerVec_RegisterAgain(t *testing.T) {
	rg := NewRegistry(NewRegistryOpts())
	vec1 := rg.TimerVec("ololo", []string{"shimba", "looken"}).(*TimerVec)
	vec2 := rg.TimerVec("ololo", []string{"shimba", "looken"}).(*TimerVec)
	assert.Same(t, vec1.vec, vec2.vec)
}

func TestHistogramVec(t *testing.T) {
	rg := NewRegistry(NewRegistryOpts())
	buckets := metrics.NewBuckets(1, 2, 3)
	vec := rg.HistogramVec("ololo", buckets, []string{"shimba", "looken"})
	mt := vec.With(map[string]string{
		"shimba": "boomba",
		"looken": "tooken",
	})

	assert.IsType(t, &HistogramVec{}, vec)
	assert.IsType(t, &Histogram{}, mt)

	vec.Reset()

	metrics, err := rg.Gather()
	assert.NoError(t, err)
	assert.Empty(t, metrics)
}

func TestHistogramVec_RegisterAgain(t *testing.T) {
	rg := NewRegistry(NewRegistryOpts())
	buckets := metrics.NewBuckets(1, 2, 3)
	vec1 := rg.HistogramVec("ololo", buckets, []string{"shimba", "looken"}).(*HistogramVec)
	vec2 := rg.HistogramVec("ololo", buckets, []string{"shimba", "looken"}).(*HistogramVec)
	assert.Same(t, vec1.vec, vec2.vec)
}

func TestDurationHistogramVec(t *testing.T) {
	rg := NewRegistry(NewRegistryOpts())
	buckets := metrics.NewDurationBuckets(1, 2, 3)
	vec := rg.DurationHistogramVec("ololo", buckets, []string{"shimba", "looken"})
	mt := vec.With(map[string]string{
		"shimba": "boomba",
		"looken": "tooken",
	})

	assert.IsType(t, &DurationHistogramVec{}, vec)
	assert.IsType(t, &Histogram{}, mt)

	vec.Reset()

	metrics, err := rg.Gather()
	assert.NoError(t, err)
	assert.Empty(t, metrics)
}

func TestDurationHistogramVec_RegisterAgain(t *testing.T) {
	rg := NewRegistry(NewRegistryOpts())
	buckets := metrics.NewDurationBuckets(1, 2, 3)
	vec1 := rg.DurationHistogramVec("ololo", buckets, []string{"shimba", "looken"}).(*DurationHistogramVec)
	vec2 := rg.DurationHistogramVec("ololo", buckets, []string{"shimba", "looken"}).(*DurationHistogramVec)
	assert.Same(t, vec1.vec, vec2.vec)
}
