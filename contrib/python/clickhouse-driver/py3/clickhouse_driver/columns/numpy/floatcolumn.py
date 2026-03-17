import numpy as np

from .base import NumpyColumn

# normalize_null_value = False due to float('nan')
# With normalization pandas.isnull will threat float('nan') as NULL value.


class NumpyFloat32Column(NumpyColumn):
    dtype = np.dtype(np.float32)
    ch_type = 'Float32'
    normalize_null_value = False

    def _get_nulls_map(self, items):
        return [x is None for x in items]


class NumpyFloat64Column(NumpyColumn):
    dtype = np.dtype(np.float64)
    ch_type = 'Float64'
    normalize_null_value = False

    def _get_nulls_map(self, items):
        return [x is None for x in items]
