from libcpp cimport bool

from util.system.types cimport ui32, ui64, i64
from library.python.monlib.metric cimport (
        TGauge, TCounter, TRate, TIntGauge, THistogram,
        IHistogramCollectorPtr)

cdef class Gauge:
    """
    Represents a floating point absolute value
    """
    @staticmethod
    cdef Gauge from_ptr(TGauge* native):
        cdef Gauge wrapper = Gauge.__new__(Gauge)
        wrapper.__wrapped = native

        return wrapper

    def set(self, double value):
        """
        Set metric to the specified value
        :param value: metric value
        """
        self.__wrapped.Set(value)

    def get(self):
        """
        Get metric value.
        :param value: metric value
        """
        return self.__wrapped.Get()

    def add(self, double value):
        """
        Add value to metric.
        :param value: metric value
        """
        return self.__wrapped.Add(value)


cdef class IntGauge:
    """
    Represents an integer absolute value
    """
    @staticmethod
    cdef IntGauge from_ptr(TIntGauge* native):
        cdef IntGauge wrapper = IntGauge.__new__(IntGauge)
        wrapper.__wrapped = native

        return wrapper

    def set(self, i64 value):
        """
        Set metric to the specified value
        :param value: metric value
        """
        self.__wrapped.Set(value)

    def get(self):
        """
        Get metric value
        :param value: metric value
        """
        return self.__wrapped.Get()

    def add(self, i64 value):
        """
        Add value to metric.
        :param value: metric value
        """
        return self.__wrapped.Add(value)

    def inc(self):
        """
        Add 1 to metric.
        """
        return self.__wrapped.Inc()

    def dec(self):
        """
        Add -1 to metric.
        """
        return self.__wrapped.Dec()


cdef class Counter:
    """
    Represents a counter value
    """
    @staticmethod
    cdef Counter from_ptr(TCounter* native):
        cdef Counter wrapper = Counter.__new__(Counter)
        wrapper.__wrapped = native

        return wrapper

    def get(self):
        return self.__wrapped.Get()

    def inc(self):
        """
        Increment metric value
        """
        return self.__wrapped.Inc()

    def reset(self):
        """
        Reset metric value to zero
        """
        return self.__wrapped.Reset()


cdef class Rate:
    """
    Represents a time derivative
    """
    @staticmethod
    cdef Rate from_ptr(TRate* native):
        cdef Rate wrapper = Rate.__new__(Rate)
        wrapper.__wrapped = native

        return wrapper

    def get(self):
        return self.__wrapped.Get()

    def inc(self):
        """
        Increment metric value
        """
        return self.__wrapped.Inc()

    def add(self, ui64 value):
        """
        Add the value to metric
        :param value: value to add to metric
        """
        return self.__wrapped.Add(value)

cdef class Histogram:
    """
    Represents some value distribution
    """
    @staticmethod
    cdef Histogram from_ptr(THistogram* native):
        cdef Histogram wrapper = Histogram.__new__(Histogram, 0)
        wrapper.__is_owner = False
        wrapper.__wrapped = native

        return wrapper

    def __dealloc__(self):
        if self.__is_owner:
            del self.__wrapped

    def collect(self, double value, ui32 count=1):
        """
        Add a few points with same value to the distribution
        :param value: points' value
        :param value: point count
        """
        return self.__wrapped.Record(value, count)
