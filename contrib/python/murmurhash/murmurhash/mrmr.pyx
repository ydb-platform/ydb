# cython: freethreading_compatible=True
from libc.stdint cimport uint64_t, int64_t, int32_t


cdef extern from "MurmurHash3.h":
    void MurmurHash3_x86_32(void * key, uint64_t len, uint64_t seed, void* out) noexcept nogil
    void MurmurHash3_x86_128(void * key, int len, uint32_t seed, void* out) noexcept nogil
    void MurmurHash3_x64_128(void * key, int len, uint32_t seed, void* out) noexcept nogil

cdef extern from "MurmurHash2.h":
    uint64_t MurmurHash64A(void * key, int length, uint32_t seed) noexcept nogil
    uint64_t MurmurHash64B(void * key, int length, uint32_t seed) noexcept nogil


cdef uint32_t hash32(void* key, int length, uint32_t seed) noexcept nogil:
    cdef int32_t out
    MurmurHash3_x86_32(key, length, seed, &out)
    return out


cdef uint64_t hash64(void* key, int length, uint64_t seed) noexcept nogil:
    return MurmurHash64A(key, length, seed)

cdef uint64_t real_hash64(void* key, int length, uint64_t seed) noexcept nogil:
    cdef uint64_t[2] out
    MurmurHash3_x86_128(key, length, seed, &out)
    return out[1]


cdef void hash128_x86(const void* key, int length, uint32_t seed, void* out) noexcept nogil:
    MurmurHash3_x86_128(key, length, seed, out)


cdef void hash128_x64(const void* key, int length, uint32_t seed, void* out) noexcept nogil:
    MurmurHash3_x64_128(key, length, seed, out)


cpdef int32_t hash(value, uint32_t seed=0):
    if isinstance(value, unicode):
        return hash_unicode(value, seed=seed)
    else:
        return hash_bytes(value, seed=seed)


cpdef int32_t hash_unicode(unicode value, uint32_t seed=0):
    return hash_bytes(value.encode('utf8'), seed=seed)


cpdef int32_t hash_bytes(bytes value, uint32_t seed=0):
    cdef char* chars = <char*>value
    return hash32(chars, len(value), seed)
