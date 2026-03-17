# cython: embedsignature=True
# cython: profile=False
# cython: linetrace=False
# cython: binding=False
# cython: language_level=3


from libc.stdint cimport uint8_t, uint32_t


cdef inline void store_le32(uint8_t c[4], uint32_t i) noexcept nogil:
    c[0] = i & 0xFF
    c[1] = (i >> 8) & 0xFF
    c[2] = (i >> 16) & 0xFF
    c[3] = (i >> 24) & 0xFF


cdef inline uint32_t load_le32(const uint8_t c[4]) noexcept nogil:
    return (
        c[0] |
        (c[1] << 8) |
        (c[2] << 16) |
        (c[3] << 24)
    )
