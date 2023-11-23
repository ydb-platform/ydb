import cython

import numpy as np

from .buffer cimport ResponseBuffer

@cython.boundscheck(False)
@cython.wraparound(False)
def read_numpy_array(ResponseBuffer buffer, np_type: str, unsigned long long num_rows):
    dtype = np.dtype(np_type)
    cdef sz = dtype.itemsize * num_rows
    cdef char * source = buffer.read_bytes_c(dtype.itemsize * num_rows)
    return np.frombuffer(source[:sz], dtype, num_rows)
