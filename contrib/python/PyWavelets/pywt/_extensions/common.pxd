cdef extern from "c/common.h":
    ctypedef int pywt_index_t

    cdef void* wtmalloc(long size)
    cdef void* wtcalloc(long len, long size)
    cdef void wtfree(void* ptr)

    ctypedef struct ArrayInfo:
        size_t * shape
        pywt_index_t * strides
        size_t ndim

    ctypedef enum Coefficient:
        COEF_APPROX = 0
        COEF_DETAIL = 1

    ctypedef enum DiscreteTransformType:
        DWT_TRANSFORM = 0
        SWT_TRANSFORM = 1

    ctypedef enum MODE:
        MODE_INVALID = -1
        MODE_ZEROPAD = 0
        MODE_SYMMETRIC
        MODE_CONSTANT_EDGE
        MODE_SMOOTH
        MODE_PERIODIC
        MODE_PERIODIZATION
        MODE_REFLECT
        MODE_ANTISYMMETRIC
        MODE_ANTIREFLECT
        MODE_MAX

    # buffers lengths
    cdef size_t dwt_buffer_length(size_t input_len, size_t filter_len, MODE mode)
    cdef size_t upsampling_buffer_length(size_t coeffs_len, size_t filter_len,
                                         MODE mode)
    cdef size_t reconstruction_buffer_length(size_t coeffs_len, size_t filter_len)
    cdef size_t idwt_buffer_length(size_t coeffs_len, size_t filter_len, MODE mode)
    cdef size_t swt_buffer_length(size_t coeffs_len)

    # max dec levels
    cdef unsigned char dwt_max_level(size_t input_len, size_t filter_len)
    cdef unsigned char swt_max_level(size_t input_len)
