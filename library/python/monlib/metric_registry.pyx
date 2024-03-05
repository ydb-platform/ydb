from library.python.monlib.encoder cimport Encoder
from library.python.monlib.labels cimport TLabels
from library.python.monlib.metric cimport (
        Gauge, IntGauge, Counter, Rate, Histogram, IHistogramCollectorPtr,
        ExponentialHistogram, ExplicitHistogram, LinearHistogram)
from library.python.monlib.metric_consumer cimport IMetricConsumer
from library.python.monlib.metric_registry cimport TMetricRegistry

from util.generic.ptr cimport THolder
from util.generic.string cimport TString
from util.datetime.base cimport TInstant
from util.system.types cimport ui32
from util.generic.vector cimport TVector

from libcpp.string cimport string

from cython.operator cimport address, dereference as deref

import datetime as dt
import sys


cdef extern from "<utility>" namespace "std" nogil:
    cdef IHistogramCollectorPtr&& move(IHistogramCollectorPtr t)


def get_or_raise(kwargs, key):
    value = kwargs.get(key)
    if value is None:
        raise ValueError(key + ' argument is required but not specified')

    return value


class HistogramType(object):
    Exponential = 0
    Explicit = 1
    Linear = 2


cdef class MetricRegistry:
    """
    Represents an entity holding a set of counters of different types identified by labels

    Example usage:
    .. ::
        registry = MetricRegistry()

        response_times = registry.histogram_rate(
            {'path': 'ping', 'sensor': 'responseTimeMillis'},
            HistogramType.Explicit, buckets=[10, 20, 50, 200, 500])

        requests = registry.rate({'path': 'ping', 'sensor': 'requestRate'})
        uptime = registry.gauge({'sensor': 'serverUptimeSeconds'})

        # ...
        requests.inc()
        uptime.set(time.time() - start_time)

        # ...
        dumps(registry)
    """
    cdef THolder[TMetricRegistry] __wrapped

    def __cinit__(self, labels=None):
        cdef TLabels native_labels = MetricRegistry._py_to_native_labels(labels)
        self.__wrapped.Reset(new TMetricRegistry(native_labels))

    @staticmethod
    cdef TLabels _py_to_native_labels(dict labels):
        cdef TLabels native_labels = TLabels()

        if labels is not None:
            for name, value in labels.items():
                native_labels.Add(TString(<string>name.encode('utf-8')), TString(<string>value.encode('utf-8')))

        return native_labels

    @staticmethod
    cdef _native_to_py_labels(const TLabels& native_labels):
        result = dict()

        cdef TLabels.const_iterator it = native_labels.begin()
        while it != native_labels.end():
            name = TString(deref(it).Name())
            value = TString(deref(it).Value())
            if (isinstance(name, bytes)):
                name = name.decode('utf-8')

            if (isinstance(value, bytes)):
                value = value.decode('utf-8')

            result[name] = value
            it += 1

        return result

    def _histogram(self, labels, is_rate, hist_type, **kwargs):
        cdef TLabels native_labels = MetricRegistry._py_to_native_labels(labels)
        cdef IHistogramCollectorPtr collector
        cdef TVector[double] native_buckets

        if hist_type == HistogramType.Exponential:
            buckets = int(get_or_raise(kwargs, 'bucket_count'))
            base = float(get_or_raise(kwargs, 'base'))
            scale = float(kwargs.get('scale', 1.))
            collector = move(ExponentialHistogram(buckets, base, scale))
        elif hist_type == HistogramType.Explicit:
            buckets = get_or_raise(kwargs, 'buckets')
            native_buckets = buckets
            collector = move(ExplicitHistogram(native_buckets))
        elif hist_type == HistogramType.Linear:
            buckets = get_or_raise(kwargs, 'bucket_count')
            start_value = get_or_raise(kwargs, 'start_value')
            bucket_width = get_or_raise(kwargs, 'bucket_width')
            collector = move(LinearHistogram(buckets, start_value, bucket_width))
        else:
            # XXX: string representation
            raise ValueError('histogram type {} is not supported'.format(str(hist_type)))

        cdef THistogram* native_hist
        if is_rate:
            native_hist = self.__wrapped.Get().HistogramRate(native_labels, move(collector))
        else:
            native_hist = self.__wrapped.Get().HistogramCounter(native_labels, move(collector))

        return Histogram.from_ptr(native_hist)

    @property
    def common_labels(self):
        """
        Gets labels that are common among all the counters in this registry

        :returns: Common labels as a dict
        """
        cdef const TLabels* native = address(self.__wrapped.Get().CommonLabels())
        labels = MetricRegistry._native_to_py_labels(deref(native))

        return labels

    def gauge(self, labels):
        """
        Gets a gauge counter or creates a new one in case counter with the specified labels
        does not exist

        :param labels: A dict of labels which identifies counter
        :returns: Gauge counter
        """
        cdef TLabels native_labels = MetricRegistry._py_to_native_labels(labels)
        native_gauge = self.__wrapped.Get().Gauge(native_labels)
        return Gauge.from_ptr(native_gauge)

    def int_gauge(self, labels):
        """
        Gets a gauge counter or creates a new one in case counter with the specified labels
        does not exist

        :param labels: A dict of labels which identifies counter
        :returns: IntGauge counter
        """
        cdef TLabels native_labels = MetricRegistry._py_to_native_labels(labels)
        native_gauge = self.__wrapped.Get().IntGauge(native_labels)
        return IntGauge.from_ptr(native_gauge)

    def counter(self, labels):
        """
        Gets a counter or creates a new one in case counter with the specified labels
        does not exist

        :param labels: A dict of labels which identifies counter
        :returns: Counter counter
        """
        cdef TLabels native_labels = MetricRegistry._py_to_native_labels(labels)
        native_counter = self.__wrapped.Get().Counter(native_labels)
        return Counter.from_ptr(native_counter)

    def rate(self, labels):
        """
        Gets a rate counter or creates a new one in case counter with the specified labels
        does not exist

        :param labels: A dict of labels which identifies counter
        :returns: Rate counter
        """
        cdef TLabels native_labels = MetricRegistry._py_to_native_labels(labels)
        native_rate = self.__wrapped.Get().Rate(native_labels)
        return Rate.from_ptr(native_rate)

    def histogram_counter(self, labels, hist_type, **kwargs):
        """
        Gets a histogram counter or creates a new one in case counter with the specified labels
        does not exist

        :param labels: A dict of labels which identifies counter
        :param hist_type: Specifies the way histogram buckets are defined (allowed values: explicit, exponential, linear)

        Keyword arguments:
        :param buckets: A list of bucket upper bounds (explicit)
        :param bucket_count: Number of buckets (linear, exponential)
        :param base: the exponential growth factor for buckets' width (exponential)
        :param scale: linear scale for the buckets. Must be >= 1.0 (exponential)
        :param start_value: the upper bound of the first bucket (linear)

        :returns: Histogram counter

        Example usage:
        .. ::
            my_histogram = registry.histogram_counter(
                {'path': 'ping', 'sensor': 'responseTimeMillis'},
                HistogramType.Explicit, buckets=[10, 20, 50, 200, 500])
            # (-inf; 10] (10; 20] (20; 50] (200; 500] (500; +inf)

            # or:
            my_histogram = registry.histogram_counter(
                {'path': 'ping', 'sensor': 'responseTimeMillis'},
                HistogramType.Linear, bucket_count=4, bucket_width=10, start_value=0)
            # (-inf; 0] (0; 10] (10; 20] (20; +inf)

            # or:
            my_histogram = registry.histogram_counter(
                {'path': 'ping', 'sensor': 'responseTimeMillis'},
                HistogramType.Exponential, bucket_count=6, base=2, scale=3)
            # (-inf; 3] (3; 6] (6; 12] (12; 24] (24; 48] (48; +inf)
        ::
        """
        return self._histogram(labels, False, hist_type, **kwargs)

    def histogram_rate(self, labels, hist_type, **kwargs):
        """
        Gets a histogram rate counter or creates a new one in case counter with the specified labels
        does not exist

        :param labels: A dict of labels which identifies counter
        :param hist_type: Specifies the way histogram buckets are defined (allowed values: explicit, exponential, linear)

        Keyword arguments:
        :param buckets: A list of bucket upper bounds (explicit)
        :param bucket_count: Number of buckets (linear, exponential)
        :param base: the exponential growth factor for buckets' width (exponential)
        :param scale: linear scale for the buckets. Must be >= 1.0 (exponential)
        :param start_value: the upper bound of the first bucket (linear)

        :returns: Histogram counter

        Example usage:
        .. ::
            my_histogram = registry.histogram_counter(
                {'path': 'ping', 'sensor': 'responseTimeMillis'},
                HistogramType.Explicit, buckets=[10, 20, 50, 200, 500])
            # (-inf; 10] (10; 20] (20; 50] (200; 500] (500; +inf)

            # or:
            my_histogram = registry.histogram_counter(
                {'path': 'ping', 'sensor': 'responseTimeMillis'},
                HistogramType.Linear, bucket_count=4, bucket_width=10, start_value=0)
            # (-inf; 0] (0; 10] (10; 20] (20; +inf)

            # or:
            my_histogram = registry.histogram_counter(
                {'path': 'ping', 'sensor': 'responseTimeMillis'},
                HistogramType.Exponential, bucket_count=6, base=2, scale=3)
            # (-inf; 3] (3; 6] (6; 12] (12; 24] (24; 48] (48; +inf)
        ::
        """
        return self._histogram(labels, True, hist_type, **kwargs)

    def reset(self):
        self.__wrapped.Get().Reset()

    def accept(self, time, Encoder encoder):
        cdef IMetricConsumer* ptr = <IMetricConsumer*>encoder.native()
        timestamp = int((time - dt.datetime(1970, 1, 1)).total_seconds())
        self.__wrapped.Get().Accept(TInstant.Seconds(timestamp), ptr)

    def remove_metric(self, labels):
        cdef TLabels native_labels = MetricRegistry._py_to_native_labels(labels)
        self.__wrapped.Get().RemoveMetric(native_labels)
