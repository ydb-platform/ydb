from libc.stdint cimport uint64_t, int64_t, uint32_t


cdef uint32_t hash32(void* key, int length, uint32_t seed) noexcept nogil
cdef uint64_t hash64(void* key, int length, uint64_t seed) noexcept nogil
cdef uint64_t real_hash64(void* key, int length, uint64_t seed) noexcept nogil
cdef void hash128_x86(const void* key, int len, uint32_t seed, void* out) noexcept nogil
cdef void hash128_x64(const void* key, int len, uint32_t seed, void* out) noexcept nogil
