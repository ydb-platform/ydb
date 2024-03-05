from util.generic.string cimport TString, TStringBuf
from util.generic.ptr cimport THolder

from cython.operator cimport dereference as deref

import sys

from datetime import datetime
from os import dup


cdef extern from "util/stream/fwd.h" nogil:
    cdef cppclass TAdaptivelyBuffered[T]:
        TAdaptivelyBuffered(TFile) except +

    ctypedef TAdaptivelyBuffered[TUnbufferedFileOutput] TFileOutput

cdef extern from "util/stream/mem.h" nogil:
    cdef cppclass TMemoryInput:
        TMemoryInput(const TStringBuf buf)


cdef extern from "util/stream/file.h" nogil:
    cdef cppclass TUnbufferedFileOutput:
        TUnbufferedFileOutput(TFile)

    cdef cppclass TFileInput:
        TFileInput(TFile) except +


cdef extern from "util/stream/str.h" nogil:
    cdef cppclass TStringStream:
        const TString& Str() const


cdef class Encoder:
    cdef IMetricEncoder* native(self):
        return self.__wrapped.Get()

    def close(self):
        deref(self.__wrapped.Get()).Close()

    def dumps(self):
        return (<TStringStream&?>deref(self.__stream.Get())).Str()

    cdef _make_stream(self, py_stream):
        if py_stream is not None:
            fd = Duplicate(py_stream.fileno())

            self.__file.Reset(new TFile(fd))
            f = self.__file.Get()
            self.__stream.Reset(<IOutputStream*>(new TFileOutput(deref(f))))
        else:
            self.__stream.Reset(<IOutputStream*>(new TStringStream()))

    @staticmethod
    cdef Encoder create_spack(object stream, ETimePrecision precision, ECompression compression):
        cdef Encoder wrapper = Encoder.__new__(Encoder)
        wrapper._make_stream(stream)

        wrapper.__wrapped = EncoderSpackV1(wrapper.__stream.Get(),
            precision,
            compression)

        return wrapper

    @staticmethod
    cdef Encoder create_json(object stream, int indent):
        cdef Encoder wrapper = Encoder.__new__(Encoder)
        wrapper._make_stream(stream)

        wrapper.__wrapped = EncoderJson(wrapper.__stream.Get(), indent)

        return wrapper


cpdef Encoder create_json_encoder(object stream, int indent):
    return Encoder.create_json(stream, indent)


cdef class TimePrecision:
    Millis = <int>MILLIS
    Seconds = <int>SECONDS

    @staticmethod
    cdef ETimePrecision to_native(int p) except *:
        if p == TimePrecision.Millis:
            return MILLIS
        elif p == TimePrecision.Seconds:
            return SECONDS

        raise ValueError('Unsupported TimePrecision value')

cdef class Compression:
    Identity = <int>IDENTITY
    Lz4 = <int>LZ4
    Zlib = <int>ZLIB
    Zstd = <int>ZSTD

    @staticmethod
    cdef ECompression to_native(int p) except *:
        if p == Compression.Identity:
            return IDENTITY
        elif p == Compression.Lz4:
            return LZ4
        elif p == Compression.Zlib:
            return ZLIB
        elif p == Compression.Zstd:
            return ZSTD

        raise ValueError('Unsupported Compression value')


# XXX: timestamps
def dump(registry, fp, format='spack', **kwargs):
    """
    Dumps metrics held by the metric registry to a file. Output can be additionally
    adjusted using kwargs, which may differ depending on the selected format.

    :param registry: Metric registry object
    :param fp: File descriptor to serialize to
    :param format: Format to serialize to (allowed values: spack). Default: json

    Keyword arguments:
    :param time_precision: Time precision (spack)
    :param compression: Compression codec (spack)
    :param indent: Pretty-print indentation for object members and arrays (json)
    :param timestamp: Metric timestamp datetime
    :returns: Nothing
    """
    if not hasattr(fp, 'fileno'):
        raise TypeError('Expected a file-like object, but got ' + str(type(fp)))

    if format == 'spack':
        time_precision = TimePrecision.to_native(kwargs.get('time_precision', TimePrecision.Seconds))
        compression = Compression.to_native(kwargs.get('compression', Compression.Identity))
        encoder = Encoder.create_spack(fp, time_precision, compression)
    elif format == 'json':
        indent = int(kwargs.get('indent', 0))
        encoder = Encoder.create_json(fp, indent)
    timestamp = kwargs.get('timestamp', datetime.utcfromtimestamp(0))

    registry.accept(timestamp, encoder)
    encoder.close()


def dumps(registry, format='spack', **kwargs):
    """
    Dumps metrics held by the metric registry to a string. Output can be additionally
    adjusted using kwargs, which may differ depending on the selected format.

    :param registry: Metric registry object
    :param format: Format to serialize to (allowed values: spack). Default: json

    Keyword arguments:
    :param time_precision: Time precision (spack)
    :param compression: Compression codec (spack)
    :param indent: Pretty-print indentation for object members and arrays (json)
    :param timestamp: Metric timestamp datetime
    :returns: A string of the specified format
    """
    if format == 'spack':
        time_precision = TimePrecision.to_native(kwargs.get('time_precision', TimePrecision.Seconds))
        compression = Compression.to_native(kwargs.get('compression', Compression.Identity))
        encoder = Encoder.create_spack(None, time_precision, compression)
    elif format == 'json':
        indent = int(kwargs.get('indent', 0))
        encoder = Encoder.create_json(None, indent)
    timestamp = kwargs.get('timestamp', datetime.utcfromtimestamp(0))

    registry.accept(timestamp, encoder)
    encoder.close()

    s = encoder.dumps()

    return s


def load(fp, from_format='spack', to_format='json'):
    """
    Converts metrics from one format to another.

    :param fp: File to load data from
    :param from_format: Source string format (allowed values: json, spack, unistat). Default: spack
    :param to_format: Target format (allowed values: json, spack). Default: json
    :returns: a string containing metrics in the specified format
    """
    if from_format == to_format:
        return fp.read()

    cdef THolder[TFile] file
    file.Reset(new TFile(Duplicate(fp.fileno())))

    cdef THolder[TFileInput] input
    input.Reset(new TFileInput(deref(file.Get())))

    if to_format == 'json':
        encoder = Encoder.create_json(None, 0)
    elif to_format == 'spack':
        encoder = Encoder.create_spack(None, SECONDS, IDENTITY)
    else:
        raise ValueError('Unsupported format ' + to_format)

    if from_format == 'spack':
        DecodeSpackV1(<IInputStream*>(input.Get()), <IMetricConsumer*?>encoder.native())
    elif from_format == 'json':
        s = open(fp, 'r').read()
        DecodeJson(TStringBuf(s), <IMetricConsumer*?>encoder.native())
    elif from_format == 'unistat':
        s = open(fp, 'r').read()
        DecodeJson(TStringBuf(s), <IMetricConsumer*?>encoder.native())

    else:
        raise ValueError('Unsupported format ' + from_format)

    encoder.close()
    s = encoder.dumps()

    return s


def loads(s, from_format='spack', to_format='json', compression=Compression.Identity):
    """
    Converts metrics from one format to another.

    :param s: String to load from
    :param from_format: Source string format (allowed values: json, spack, unistat). Default: spack
    :param to_format: Target format (allowed values: json, spack). Default: json
    :returns: a string containing metrics in the specified format
    """
    if from_format == to_format:
        return s

    if sys.version_info[0] >= 3 and not isinstance(s, bytes):
        s = s.encode('iso-8859-15')

    cdef THolder[TMemoryInput] input

    if to_format == 'json':
        encoder = Encoder.create_json(None, 0)
    elif to_format == 'spack':
        comp = Compression.to_native(compression)
        encoder = Encoder.create_spack(None, SECONDS, comp)
    else:
        raise ValueError('Unsupported format ' + to_format)

    if from_format == 'spack':
        input.Reset(new TMemoryInput(s))
        DecodeSpackV1(<IInputStream*>(input.Get()), <IMetricConsumer*?>encoder.native())
    elif from_format == 'json':
        DecodeJson(TStringBuf(s), <IMetricConsumer*?>encoder.native())
    elif from_format == 'unistat':
        DecodeUnistat(TStringBuf(s), <IMetricConsumer*?>encoder.native())
    else:
        raise ValueError('Unsupported format ' + from_format)

    encoder.close()
    s = encoder.dumps()

    return s
