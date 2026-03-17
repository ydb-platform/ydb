from libc.stdint cimport uint64_t, uint32_t
from cymem.cymem cimport Pool

ctypedef uint64_t key_t

cdef struct BloomStruct:
    key_t* bitfield
    key_t hcount # hash count, number of hash functions
    key_t length
    uint32_t seed


cdef class BloomFilter:
    cdef Pool mem
    cdef BloomStruct* c_bloom
    cdef inline bint contains(self, key_t item) nogil


cdef void bloom_init(Pool mem, BloomStruct* bloom, key_t hcount, key_t length, uint32_t seed) except *

cdef void bloom_add(BloomStruct* bloom, key_t item) nogil

cdef bint bloom_contains(const BloomStruct* bloom, key_t item) nogil

cdef void bloom_add(BloomStruct* bloom, key_t item) nogil
