# distutils: language = c++
# distutils: sources = src/editdistance/_editdistance.cpp

from libc.stdlib cimport malloc, free
from libcpp cimport bool
# from libc.stdint cimport int64_t

cdef extern from "./_editdistance.h":
    ctypedef int int64_t
    unsigned int edit_distance(const int64_t *a, const unsigned int asize, const int64_t *b, const unsigned int bsize)
    bool edit_distance_criterion(const int64_t *a, const unsigned int asize, const int64_t *b, const unsigned int bsize, const unsigned int thr)
    unsigned int edit_distance_dp(const int64_t *str1, const size_t size1, const int64_t *str2, const size_t size2)


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

cpdef bint eval_criterion(object a, object b, const unsigned int thr) except 0xffffffffffffffff:
    cdef unsigned int i
    cdef bint ret
    cdef int64_t *al = <int64_t *>malloc(len(a) * sizeof(int64_t))
    for i in range(len(a)):
        al[i] = hash(a[i])
    cdef int64_t *bl = <int64_t *>malloc(len(b) * sizeof(int64_t))
    for i in range(len(b)):
        bl[i] = hash(b[i])
    ret = edit_distance_criterion(al, len(a), bl, len(b), thr)
    free(al)
    free(bl)
    return ret

cpdef unsigned int eval_dp(object a, object b) except 0xffffffffffffffff:
    cdef unsigned int i, dist
    cdef int64_t *al = <int64_t *>malloc(len(a) * sizeof(int64_t))
    for i in range(len(a)):
        al[i] = hash(a[i])
    cdef int64_t *bl = <int64_t *>malloc(len(b) * sizeof(int64_t))
    for i in range(len(b)):
        bl[i] = hash(b[i])
    dist = edit_distance_dp(al, len(a), bl, len(b))
    free(al)
    free(bl)
    return dist
