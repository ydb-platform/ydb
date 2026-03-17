import numpy as np

from .base import NumpyColumn


class NumpyFloat32Column(NumpyColumn):
    dtype = np.dtype(np.float32)
    ch_type = 'Float32'


class NumpyFloat64Column(NumpyColumn):
    dtype = np.dtype(np.float64)
    ch_type = 'Float64'
