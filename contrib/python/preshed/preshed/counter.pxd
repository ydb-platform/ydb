from libc.stdint cimport int64_t

from cymem.cymem cimport Pool

from .maps cimport MapStruct
from .maps cimport map_init, map_get, map_set, map_iter
from .maps cimport key_t


ctypedef int64_t count_t


cdef class PreshCounter:
    cdef Pool mem
    cdef MapStruct* c_map
    cdef public object smoother
    cdef readonly count_t total

    cpdef int inc(self, key_t key, count_t inc) except -1
