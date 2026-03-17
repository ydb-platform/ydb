# cython: embedsignature=True
# cython: profile=False
# cython: linetrace=False
# cython: binding=False
# cython: language_level=3

from cpython.buffer cimport PyBuffer_IsContiguous
from cpython.memoryview cimport PyMemoryView_GET_BUFFER


cpdef memoryview ensure_continguous_memoryview(obj):
    cdef memoryview mv
    if type(obj) is memoryview:
        mv = <memoryview>obj
    else:
        mv = memoryview(obj)
    if not PyBuffer_IsContiguous(PyMemoryView_GET_BUFFER(mv), b'A'):
        raise BufferError("Expected contiguous memory")
    return mv
