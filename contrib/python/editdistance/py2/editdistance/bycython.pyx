# distutils: language = c++
# distutils: sources = editdistance/_editdistance.cpp

from libc.stdlib cimport malloc, free
# from libc.stdint cimport int64_t

cdef extern from "./_editdistance.h":
    ctypedef int int64_t
    unsigned int edit_distance(const int64_t *a, const unsigned int asize, const int64_t *b, const unsigned int bsize)

cpdef unsigned int eval(object a, object b) except 0xffffffffffffffff:
    cdef unsigned int i, dist
    cdef int64_t *al = <int64_t *>malloc(len(a) * sizeof(int64_t))
    for i in range(len(a)):
        al[i] = hash(a[i])
    cdef int64_t *bl = <int64_t *>malloc(len(b) * sizeof(int64_t))
    for i in range(len(b)):
        bl[i] = hash(b[i])
    dist = edit_distance(al, len(a), bl, len(b))
    free(al)
    free(bl)
    return dist
