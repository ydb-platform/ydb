from util.datetime.base cimport TInstant

from library.python.monlib.labels cimport ILabels, TLabels
from library.python.monlib.metric_consumer cimport IMetricConsumer
from library.python.monlib.metric cimport (
        TGauge, TIntGauge, TRate, TCounter, THistogram,
        IHistogramCollectorPtr)


cdef extern from "library/cpp/monlib/metrics/metric_registry.h" namespace "NMonitoring" nogil:
    cdef cppclass TMetricRegistry:
        TMetricRegistry() except +
        TMetricRegistry(const TLabels&) except +

        TGauge* Gauge(const TLabels&) except +
        TIntGauge* IntGauge(const TLabels&) except +
        TCounter* Counter(const TLabels&) except +
        TRate* Rate(const TLabels&) except +
        THistogram* HistogramCounter(const TLabels&, IHistogramCollectorPtr collector) except +
        THistogram* HistogramRate(const TLabels&, IHistogramCollectorPtr collector) except +

        void Reset() except +

        void Accept(TInstant time, IMetricConsumer* consumer) except +
        void Append(TInstant time, IMetricConsumer* consumer) except +

        const TLabels& CommonLabels() const

        void RemoveMetric(const TLabels&)
