from libcpp cimport bool

from util.system.types cimport ui64, ui32, i64
from util.generic.ptr cimport THolder, TIntrusivePtr
from util.generic.vector cimport TVector


cdef extern from "library/cpp/monlib/metrics/histogram_collector.h" namespace "NMonitoring" nogil:
    ctypedef double TBucketBound
    ctypedef ui64 TBucketValue

    cdef cppclass IHistogramSnapshot:
        ui32 Count() const
        TBucketBound UpperBound(ui32 index) const
        TBucketValue Value(ui32 index) const

    ctypedef TIntrusivePtr[IHistogramSnapshot] IHistogramSnapshotPtr

    cdef cppclass IHistogramCollector:
        void Collect(i64 value)
        void Collect(i64 value, ui32 count)
        IHistogramSnapshotPtr Snapshot() const

    ctypedef THolder[IHistogramCollector] IHistogramCollectorPtr

    IHistogramCollectorPtr ExponentialHistogram(ui32 bucketsCount, double base, double scale) except +
    IHistogramCollectorPtr ExplicitHistogram(const TVector[double]& buckets) except +
    IHistogramCollectorPtr LinearHistogram(ui32 bucketsCount, i64 startValue, i64 bucketWidth) except +


cdef extern from "library/cpp/monlib/metrics/metric.h" namespace "NMonitoring" nogil:
    cdef cppclass TGauge:
        TGauge(double value) except +

        void Set(double)
        double Get() const
        double Add(double)

    cdef cppclass TIntGauge:
        TIntGauge(ui64 value) except +

        void Set(ui64)
        ui64 Get() const
        ui64 Add(double)
        ui64 Inc()
        ui64 Dec()

    cdef cppclass TCounter:
        TCounter(ui64 value) except +

        void Set(ui64)
        ui64 Get() const
        void Inc()
        void Reset()

    cdef cppclass TRate:
        TRate(ui64 value) except +

        void Add(ui64)
        ui64 Get() const
        void Inc()

    cdef cppclass THistogram:
        THistogram(IHistogramCollectorPtr collector, bool isRate) except +

        void Record(double value)
        void Record(double value, ui32 count)


cdef class Gauge:
    cdef TGauge* __wrapped

    @staticmethod
    cdef Gauge from_ptr(TGauge* native)


cdef class Counter:
    cdef TCounter* __wrapped

    @staticmethod
    cdef Counter from_ptr(TCounter* native)


cdef class Rate:
    cdef TRate* __wrapped

    @staticmethod
    cdef Rate from_ptr(TRate* native)


cdef class IntGauge:
    cdef TIntGauge* __wrapped

    @staticmethod
    cdef IntGauge from_ptr(TIntGauge* native)


cdef class Histogram:
    cdef THistogram* __wrapped
    cdef bool __is_owner

    @staticmethod
    cdef Histogram from_ptr(THistogram* native)
