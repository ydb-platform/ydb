from library.python.monlib.labels cimport TLabels, TLabel
from library.python.monlib.metric cimport (
    TGauge, TCounter,
    TRate, THistogram,
    IHistogramCollectorPtr, ExponentialHistogram,
    IHistogramSnapshotPtr
)

from library.python.monlib.metric_registry cimport TMetricRegistry

from util.generic.string cimport TStringBuf, TString
from util.generic.maybe cimport TMaybe
from util.generic.ptr cimport THolder

from cython.operator cimport dereference as deref

import pytest
import unittest


cdef extern from "<utility>" namespace "std" nogil:
    cdef IHistogramCollectorPtr&& move(IHistogramCollectorPtr t)


class TestMetric(unittest.TestCase):
    def test_labels(self):
        cdef TLabels labels = TLabels()
        cdef TString name = "foo"
        cdef TString value = "bar"

        labels.Add(name, value)

        cdef TMaybe[TLabel] label = labels.Find(name)

        assert label.Defined()
        assert label.GetRef().Name() == "foo"
        assert label.GetRef().Value() == "bar"

    def test_metric_registry(self):
        cdef TLabels labels = TLabels()

        labels.Add(TString("common"), TString("label"))

        cdef THolder[TMetricRegistry] registry
        registry.Reset(new TMetricRegistry(labels))

        assert deref(registry.Get()).CommonLabels() == labels

        cdef TLabels metric_labels = TLabels()
        metric_labels.Add(TString("name"), TString("gauge"))
        g = deref(registry.Get()).Gauge(metric_labels)
        assert g.Get() == 0.

        metric_labels = TLabels()
        metric_labels.Add(TString("name"), TString("counter"))
        c = deref(registry.Get()).Counter(metric_labels)
        assert c.Get() == 0.

        metric_labels = TLabels()
        metric_labels.Add(TString("name"), TString("rate"))
        r = deref(registry.Get()).Rate(metric_labels)
        assert r.Get() == 0.

        metric_labels = TLabels()
        metric_labels.Add(TString("name"), TString("int_gauge"))
        ig = deref(registry.Get()).IntGauge(metric_labels)
        assert ig.Get() == 0

    def test_metric_registry_throws_on_duplicate(self):
        cdef THolder[TMetricRegistry] registry
        registry.Reset(new TMetricRegistry())

        cdef TLabels metric_labels = TLabels()
        metric_labels.Add(TString("my"), TString("metric"))
        g = deref(registry.Get()).Gauge(metric_labels)
        with pytest.raises(RuntimeError):
            deref(registry.Get()).Counter(metric_labels)

    def test_counter_histogram(self):
        cdef THolder[TMetricRegistry] registry
        registry.Reset(new TMetricRegistry())

        cdef TLabels metric_labels = TLabels()
        metric_labels.Add(TString("name"), TString("histogram"))

        cdef IHistogramCollectorPtr collector = move(ExponentialHistogram(6, 2, 3))
        collector_ptr = collector.Get()
        hist = registry.Get().HistogramCounter(metric_labels, move(collector))
        hist.Record(1)
        hist.Record(1000, 4)

        cdef IHistogramSnapshotPtr snapshot = collector_ptr.Snapshot()
        assert deref(snapshot.Get()).Count() == 6
        assert snapshot.Get().Value(0) == 1

    def test_rate_histogram(self):
        cdef THolder[TMetricRegistry] registry
        registry.Reset(new TMetricRegistry())

        cdef TLabels metric_labels = TLabels()
        metric_labels.Add(TString("name"), TString("histogram"))

        cdef IHistogramCollectorPtr collector = move(ExponentialHistogram(6, 2, 3))
        collector_ptr = collector.Get()
        hist = registry.Get().HistogramRate(metric_labels, move(collector))
        hist.Record(1)
        hist.Record(1000, 4)

        cdef IHistogramSnapshotPtr snapshot = collector_ptr.Snapshot()
        assert deref(snapshot.Get()).Count() == 6
        assert snapshot.Get().Value(0) == 1
        assert snapshot.Get().Value(5) == 4
        assert snapshot.Get().Value(5) == 4
