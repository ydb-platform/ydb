from libc.stdint cimport uint64_t
from cymem.cymem cimport Pool


ctypedef uint64_t key_t


cdef struct Cell:
    key_t key
    void* value


cdef struct Result:
    int found
    void* value


cdef struct MapStruct:
    Cell* cells
    void* value_for_empty_key
    void* value_for_del_key
    key_t length
    key_t filled
    bint is_empty_key_set
    bint is_del_key_set


cdef void* map_bulk_get(const MapStruct* map_, const key_t* keys, void** values,
                        int n) nogil


cdef Result map_get_unless_missing(const MapStruct* map_, const key_t key) nogil

cdef void* map_get(const MapStruct* map_, const key_t key) nogil

cdef void map_set(Pool mem, MapStruct* map_, key_t key, void* value) except *

cdef void map_init(Pool mem, MapStruct* pmap, size_t length) except *

cdef bint map_iter(const MapStruct* map_, int* i, key_t* key, void** value) nogil

cdef void* map_clear(MapStruct* map_, const key_t key) nogil


cdef class PreshMap:
    cdef MapStruct* c_map
    cdef Pool mem

    cdef inline void* get(self, key_t key) nogil
    cdef void set(self, key_t key, void* value) except *


cdef class PreshMapArray:
    cdef Pool mem
    cdef MapStruct* maps
    cdef size_t length

    cdef inline void* get(self, size_t i, key_t key) nogil
    cdef void set(self, size_t i, key_t key, void* value) except *
