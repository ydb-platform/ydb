from util.generic.string cimport TStringBuf, TString
from util.generic.ptr cimport THolder
from util.stream.output cimport IOutputStream

from library.python.monlib.metric_consumer cimport IMetricConsumer


cdef extern from "util/stream/input.h" nogil:
    cdef cppclass IInputStream:
        pass


cdef extern from "util/system/file.h" nogil:
    cdef cppclass TFile:
        TFile()
        TFile(TFile)
        pass

    cdef TFile Duplicate(int)


cdef extern from "library/cpp/monlib/encode/encoder.h" namespace "NMonitoring" nogil:
    cdef cppclass IMetricEncoder:
        void Close()

    cdef cppclass ECompression:
        pass

    ctypedef THolder[IMetricEncoder] IMetricEncoderPtr


cdef extern from "library/cpp/monlib/encode/unistat/unistat.h" namespace "NMonitoring" nogil:
    cdef void DecodeUnistat(TStringBuf data, IMetricConsumer* c)


cdef extern from "library/cpp/monlib/encode/json/json.h" namespace "NMonitoring" nogil:
    cdef IMetricEncoderPtr EncoderJson(IOutputStream* out, int indentation)
    cdef IMetricEncoderPtr BufferedEncoderJson(IOutputStream* out, int indentation)

    cdef void DecodeJson(TStringBuf data, IMetricConsumer* c)


cdef extern from "library/cpp/monlib/encode/prometheus/prometheus.h" namespace "NMonitoring" nogil:
    cdef IMetricEncoderPtr EncoderPrometheus(IOutputStream* out, TStringBuf metricNameLabel)


cdef extern from "library/cpp/monlib/encode/spack/spack_v1.h" namespace "NMonitoring" nogil:
    cdef IMetricEncoderPtr EncoderSpackV1(IOutputStream* out, ETimePrecision, ECompression)

    cdef void DecodeSpackV1(IInputStream* input, IMetricConsumer* c) except +
    cdef cppclass ETimePrecision:
        pass

    cdef cppclass EValueType:
        pass


cdef extern from "library/cpp/monlib/encode/spack/spack_v1.h" namespace "NMonitoring::ETimePrecision" nogil:
    cdef ETimePrecision SECONDS "NMonitoring::ETimePrecision::SECONDS"
    cdef ETimePrecision MILLIS "NMonitoring::ETimePrecision::MILLIS"


cdef extern from "library/cpp/monlib/encode/encoder.h" namespace "NMonitoring::ECompression" nogil:
    cdef ECompression UNKNOWN "NMonitoring::ECompression::UNKNOWN"
    cdef ECompression IDENTITY "NMonitoring::ECompression::IDENTITY"
    cdef ECompression ZLIB "NMonitoring::ECompression::ZLIB"
    cdef ECompression LZ4 "NMonitoring::ECompression::LZ4"
    cdef ECompression ZSTD "NMonitoring::ECompression::ZSTD"


cdef class Encoder:
    cdef IMetricEncoderPtr __wrapped
    cdef THolder[TFile] __file
    cdef THolder[IOutputStream] __stream

    cdef IMetricEncoder* native(self)

    cdef _make_stream(self, py_stream)

    @staticmethod
    cdef Encoder create_spack(object stream, ETimePrecision timePrecision, ECompression compression)
    @staticmethod
    cdef Encoder create_json(object stream, int indent)
    @staticmethod
    cdef Encoder create_prometheus(object stream, bytes metricNameLabel)
