import numpy as np

from .base import NumpyColumn


class NumpyInt8Column(NumpyColumn):
    dtype = np.dtype(np.int8)
    ch_type = 'Int8'


class NumpyUInt8Column(NumpyColumn):
    dtype = np.dtype(np.uint8)
    ch_type = 'UInt8'


class NumpyInt16Column(NumpyColumn):
    dtype = np.dtype(np.int16)
    ch_type = 'Int16'


class NumpyUInt16Column(NumpyColumn):
    dtype = np.dtype(np.uint16)
    ch_type = 'UInt16'


class NumpyInt32Column(NumpyColumn):
    dtype = np.dtype(np.int32)
    ch_type = 'Int32'


class NumpyUInt32Column(NumpyColumn):
    dtype = np.dtype(np.uint32)
    ch_type = 'UInt32'


class NumpyInt64Column(NumpyColumn):
    dtype = np.dtype(np.int64)
    ch_type = 'Int64'


class NumpyUInt64Column(NumpyColumn):
    dtype = np.dtype(np.uint64)
    ch_type = 'UInt64'
