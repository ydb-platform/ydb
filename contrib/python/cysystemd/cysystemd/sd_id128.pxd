from libc.stdint cimport uint8_t, uint64_t


cdef extern from "<systemd/sd-id128.h>" nogil:
    cdef union sd_id128:
        uint8_t bytes[16]
        uint64_t qwords[2]

    ctypedef sd_id128 sd_id128_t
