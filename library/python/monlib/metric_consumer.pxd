from util.generic.ptr cimport TIntrusivePtr


cdef extern from "library/cpp/monlib/metrics/metric_consumer.h" namespace "NMonitoring" nogil:
    cdef cppclass IMetricConsumer:
        pass

    ctypedef TIntrusivePtr[IMetricConsumer] IMetricConsumerPtr
