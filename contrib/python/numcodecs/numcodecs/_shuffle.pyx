# cython: embedsignature=True
# cython: profile=False
# cython: linetrace=False
# cython: binding=False
# cython: language_level=3

cimport cython

@cython.boundscheck(False)
@cython.wraparound(False)
cpdef void _doShuffle(const unsigned char[::1] src, unsigned char[::1] des, Py_ssize_t element_size) noexcept nogil:
    cdef Py_ssize_t count, i, j, offset, byte_index
    count = len(src) // element_size
    for i in range(count):
        offset = i*element_size
        for byte_index in range(element_size):
            j = byte_index*count + i
            des[j] = src[offset + byte_index]


@cython.boundscheck(False)
@cython.wraparound(False)
cpdef void _doUnshuffle(const unsigned char[::1] src, unsigned char[::1] des, Py_ssize_t element_size) noexcept nogil:
    cdef Py_ssize_t count, i, j, offset, byte_index
    count = len(src) // element_size
    for i in range(element_size):
        offset = i*count
        for byte_index in range(count):
            j = byte_index*element_size + i
            des[j] = src[offset+byte_index]
