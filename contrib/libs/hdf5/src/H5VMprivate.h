/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright by The HDF Group.                                               *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of HDF5.  The full HDF5 copyright notice, including     *
 * terms governing use, modification, and redistribution, is contained in    *
 * the COPYING file, which can be found at the root of the source code       *
 * distribution tree, or in https://www.hdfgroup.org/licenses.               *
 * If you do not have access to either file, you may request a copy from     *
 * help@hdfgroup.org.                                                        *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

#ifndef H5VMprivate_H
#define H5VMprivate_H

/* Private headers needed by this file */
#include "H5private.h"   /* Generic Functions            */
#include "H5Eprivate.h"  /* Error handling              */
#include "H5MMprivate.h" /* Memory management            */

/* Vector-Vector sequence operation callback */
typedef herr_t (*H5VM_opvv_func_t)(hsize_t dst_off, hsize_t src_off, size_t len, void *udata);

/* Vector comparison functions like Fortran66 comparison operators */
#define H5VM_vector_eq_s(N, V1, V2) (H5VM_vector_cmp_s(N, V1, V2) == 0)
#define H5VM_vector_lt_s(N, V1, V2) (H5VM_vector_cmp_s(N, V1, V2) < 0)
#define H5VM_vector_gt_s(N, V1, V2) (H5VM_vector_cmp_s(N, V1, V2) > 0)
#define H5VM_vector_le_s(N, V1, V2) (H5VM_vector_cmp_s(N, V1, V2) <= 0)
#define H5VM_vector_ge_s(N, V1, V2) (H5VM_vector_cmp_s(N, V1, V2) >= 0)
#define H5VM_vector_eq_u(N, V1, V2) (H5VM_vector_cmp_u(N, V1, V2) == 0)
#define H5VM_vector_lt_u(N, V1, V2) (H5VM_vector_cmp_u(N, V1, V2) < 0)
#define H5VM_vector_gt_u(N, V1, V2) (H5VM_vector_cmp_u(N, V1, V2) > 0)
#define H5VM_vector_le_u(N, V1, V2) (H5VM_vector_cmp_u(N, V1, V2) <= 0)
#define H5VM_vector_ge_u(N, V1, V2) (H5VM_vector_cmp_u(N, V1, V2) >= 0)

/* Other functions */
#define H5VM_vector_cpy(N, DST, SRC)                                                                         \
    do {                                                                                                     \
        assert(sizeof(*(DST)) == sizeof(*(SRC)));                                                            \
        if (SRC)                                                                                             \
            H5MM_memcpy(DST, SRC, (N) * sizeof(*(DST)));                                                     \
        else                                                                                                 \
            memset(DST, 0, (N) * sizeof(*(DST)));                                                            \
    } while (0)

#define H5VM_vector_zero(N, DST) memset(DST, 0, (N) * sizeof(*(DST)))

/* Given a coordinate offset array (COORDS) of type TYPE, move the unlimited
 * dimension (UNLIM_DIM) value to offset 0, sliding any intermediate values down
 * one position. */
#define H5VM_swizzle_coords(TYPE, COORDS, UNLIM_DIM)                                                         \
    do {                                                                                                     \
        /* COORDS must be an array of type TYPE */                                                           \
        assert(sizeof(COORDS[0]) == sizeof(TYPE));                                                           \
                                                                                                             \
        /* Nothing to do when unlimited dimension is at position 0 */                                        \
        if (0 != (UNLIM_DIM)) {                                                                              \
            TYPE _tmp = (COORDS)[UNLIM_DIM];                                                                 \
                                                                                                             \
            memmove(&(COORDS)[1], &(COORDS)[0], sizeof(TYPE) * (UNLIM_DIM));                                 \
            (COORDS)[0] = _tmp;                                                                              \
        } /* end if */                                                                                       \
    } while (0)

/* Given a coordinate offset array (COORDS) of type TYPE, move the value at
 * offset 0 to offset of the unlimied dimension (UNLIM_DIM), sliding any
 * intermediate values up one position.  Undoes the "swizzle_coords" operation.
 */
#define H5VM_unswizzle_coords(TYPE, COORDS, UNLIM_DIM)                                                       \
    do {                                                                                                     \
        /* COORDS must be an array of type TYPE */                                                           \
        assert(sizeof(COORDS[0]) == sizeof(TYPE));                                                           \
                                                                                                             \
        /* Nothing to do when unlimited dimension is at position 0 */                                        \
        if (0 != (UNLIM_DIM)) {                                                                              \
            TYPE _tmp = (COORDS)[0];                                                                         \
                                                                                                             \
            memmove(&(COORDS)[0], &(COORDS)[1], sizeof(TYPE) * (UNLIM_DIM));                                 \
            (COORDS)[UNLIM_DIM] = _tmp;                                                                      \
        } /* end if */                                                                                       \
    } while (0)

/* A null pointer is equivalent to a zero vector */
#define H5VM_ZERO NULL

H5_DLL hsize_t H5VM_hyper_stride(unsigned n, const hsize_t *size, const hsize_t *total_size,
                                 const hsize_t *offset, hsize_t *stride);
H5_DLL htri_t  H5VM_hyper_eq(unsigned n, const hsize_t *offset1, const hsize_t *size1, const hsize_t *offset2,
                             const hsize_t *size2);
H5_DLL herr_t  H5VM_hyper_fill(unsigned n, const hsize_t *_size, const hsize_t *total_size,
                               const hsize_t *offset, void *_dst, unsigned fill_value);
H5_DLL herr_t  H5VM_hyper_copy(unsigned n, const hsize_t *size, const hsize_t *dst_total_size,
                               const hsize_t *dst_offset, void *_dst, const hsize_t *src_total_size,
                               const hsize_t *src_offset, const void *_src);
H5_DLL herr_t  H5VM_stride_fill(unsigned n, hsize_t elmt_size, const hsize_t *size, const hsize_t *stride,
                                void *_dst, unsigned fill_value);
H5_DLL herr_t H5VM_stride_copy(unsigned n, hsize_t elmt_size, const hsize_t *_size, const hsize_t *dst_stride,
                               void *_dst, const hsize_t *src_stride, const void *_src);
H5_DLL herr_t H5VM_stride_copy_s(unsigned n, hsize_t elmt_size, const hsize_t *_size,
                                 const hssize_t *dst_stride, void *_dst, const hssize_t *src_stride,
                                 const void *_src);
H5_DLL herr_t H5VM_array_fill(void *_dst, const void *src, size_t size, size_t count);
H5_DLL void   H5VM_array_down(unsigned n, const hsize_t *total_size, hsize_t *down);
H5_DLL hsize_t H5VM_array_offset_pre(unsigned n, const hsize_t *acc, const hsize_t *offset);
H5_DLL hsize_t H5VM_array_offset(unsigned n, const hsize_t *total_size, const hsize_t *offset);
H5_DLL herr_t  H5VM_array_calc_pre(hsize_t offset, unsigned n, const hsize_t *down, hsize_t *coords);
H5_DLL herr_t  H5VM_array_calc(hsize_t offset, unsigned n, const hsize_t *total_size, hsize_t *coords);
H5_DLL hsize_t H5VM_chunk_index(unsigned ndims, const hsize_t *coord, const uint32_t *chunk,
                                const hsize_t *down_nchunks);
H5_DLL void H5VM_chunk_scaled(unsigned ndims, const hsize_t *coord, const uint32_t *chunk, hsize_t *scaled);
H5_DLL hsize_t H5VM_chunk_index_scaled(unsigned ndims, const hsize_t *coord, const uint32_t *chunk,
                                       const hsize_t *down_nchunks, hsize_t *scaled);
H5_DLL ssize_t H5VM_opvv(size_t dst_max_nseq, size_t *dst_curr_seq, size_t dst_len_arr[],
                         hsize_t dst_off_arr[], size_t src_max_nseq, size_t *src_curr_seq,
                         size_t src_len_arr[], hsize_t src_off_arr[], H5VM_opvv_func_t op, void *op_data);
H5_DLL ssize_t H5VM_memcpyvv(void *_dst, size_t dst_max_nseq, size_t *dst_curr_seq, size_t dst_len_arr[],
                             hsize_t dst_off_arr[], const void *_src, size_t src_max_nseq,
                             size_t *src_curr_seq, size_t src_len_arr[], hsize_t src_off_arr[]);

/*-------------------------------------------------------------------------
 * Function:    H5VM_vector_reduce_product
 *
 * Purpose:     Product reduction of a vector.  Vector elements and return
 *              value are size_t because we usually want the number of
 *              elements in an array and array dimensions are always of type
 *              size_t.
 *
 * Note:        Although this routine is 'static' in this file, that's intended
 *              only as an optimization and the naming (with a single underscore)
 *              reflects its inclusion in a "private" header file.
 *
 * Return:      Success:    Product of elements
 *              Failure:    1 if N is zero
 *-------------------------------------------------------------------------
 */
static inline hsize_t H5_ATTR_UNUSED
H5VM_vector_reduce_product(unsigned n, const hsize_t *v)
{
    hsize_t ret_value = 1;

    /* Use FUNC_ENTER_NOAPI_NOINIT_NOERR here to avoid performance issues */
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    if (n && !v)
        HGOTO_DONE(0);
    while (n--)
        ret_value *= *v++;

done:
    FUNC_LEAVE_NOAPI(ret_value)
}

/*-------------------------------------------------------------------------
 * Function:    H5VM_vector_zerop_u
 *
 * Purpose:     Determines if all elements of a vector are zero.
 *
 * Note:        Although this routine is 'static' in this file, that's intended
 *              only as an optimization and the naming (with a single underscore)
 *              reflects its inclusion in a "private" header file.
 *
 * Return:      Success:    true if all elements are zero,
 *              Failure:    true if N is zero
 *-------------------------------------------------------------------------
 */
static inline htri_t H5_ATTR_UNUSED
H5VM_vector_zerop_u(int n, const hsize_t *v)
{
    htri_t ret_value = true; /* Return value */

    /* Use FUNC_ENTER_NOAPI_NOINIT_NOERR here to avoid performance issues */
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    if (!v)
        HGOTO_DONE(true);
    while (n--)
        if (*v++)
            HGOTO_DONE(false);

done:
    FUNC_LEAVE_NOAPI(ret_value)
}

/*-------------------------------------------------------------------------
 * Function:    H5VM_vector_zerop_s
 *
 * Purpose:     Determines if all elements of a vector are zero.
 *
 * Note:        Although this routine is 'static' in this file, that's intended
 *              only as an optimization and the naming (with a single underscore)
 *              reflects its inclusion in a "private" header file.
 *
 * Return:      Success:    true if all elements are zero,
 *                          false otherwise
 *              Failure:    true if N is zero
 *-------------------------------------------------------------------------
 */
static inline htri_t H5_ATTR_UNUSED
H5VM_vector_zerop_s(int n, const hssize_t *v)
{
    htri_t ret_value = true; /* Return value */

    /* Use FUNC_ENTER_NOAPI_NOINIT_NOERR here to avoid performance issues */
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    if (!v)
        HGOTO_DONE(true);
    while (n--)
        if (*v++)
            HGOTO_DONE(false);

done:
    FUNC_LEAVE_NOAPI(ret_value)
}

/*-------------------------------------------------------------------------
 * Function:    H5VM_vector_cmp_u
 *
 * Purpose:     Compares two vectors of the same size and determines if V1 is
 *              lexicographically less than, equal, or greater than V2.
 *
 * Note:        Although this routine is 'static' in this file, that's intended
 *              only as an optimization and the naming (with a single underscore)
 *              reflects its inclusion in a "private" header file.
 *
 * Return:      Success:    -1 if V1 is less than V2
 *                          0 if they are equal
 *                          1 if V1 is greater than V2
 *
 *              Failure:    0 if N is zero
 *-------------------------------------------------------------------------
 */
static inline int H5_ATTR_UNUSED
H5VM_vector_cmp_u(unsigned n, const hsize_t *v1, const hsize_t *v2)
{
    int ret_value = 0; /* Return value */

    /* Use FUNC_ENTER_NOAPI_NOINIT_NOERR here to avoid performance issues */
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    if (v1 == v2)
        HGOTO_DONE(0);
    if (v1 == NULL)
        HGOTO_DONE(-1);
    if (v2 == NULL)
        HGOTO_DONE(1);
    while (n--) {
        if (*v1 < *v2)
            HGOTO_DONE(-1);
        if (*v1 > *v2)
            HGOTO_DONE(1);
        v1++;
        v2++;
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)
}

/*-------------------------------------------------------------------------
 * Function:    H5VM_vector_cmp_s
 *
 * Purpose:     Compares two vectors of the same size and determines if V1 is
 *              lexicographically less than, equal, or greater than V2.
 *
 * Note:        Although this routine is 'static' in this file, that's intended
 *              only as an optimization and the naming (with a single underscore)
 *              reflects its inclusion in a "private" header file.
 *
 * Return:      Success:    -1 if V1 is less than V2
 *                          0 if they are equal
 *                          1 if V1 is greater than V2
 *
 *              Failure:    0 if N is zero
 *-------------------------------------------------------------------------
 */
static inline int H5_ATTR_UNUSED
H5VM_vector_cmp_s(unsigned n, const hssize_t *v1, const hssize_t *v2)
{
    int ret_value = 0; /* Return value */

    /* Use FUNC_ENTER_NOAPI_NOINIT_NOERR here to avoid performance issues */
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    if (v1 == v2)
        HGOTO_DONE(0);
    if (v1 == NULL)
        HGOTO_DONE(-1);
    if (v2 == NULL)
        HGOTO_DONE(1);
    while (n--) {
        if (*v1 < *v2)
            HGOTO_DONE(-1);
        if (*v1 > *v2)
            HGOTO_DONE(1);
        v1++;
        v2++;
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)
}

/*-------------------------------------------------------------------------
 * Function:    H5VM_vector_inc
 *
 * Purpose:     Increments V1 by V2
 *
 * Note:        Although this routine is 'static' in this file, that's intended
 *              only as an optimization and the naming (with a single underscore)
 *              reflects its inclusion in a "private" header file.
 *
 * Return:      void
 *-------------------------------------------------------------------------
 */
static inline void H5_ATTR_UNUSED
H5VM_vector_inc(int n, hsize_t *v1, const hsize_t *v2)
{
    while (n--)
        *v1++ += *v2++;
}

/* Lookup table for general log2(n) routine */
static const unsigned char LogTable256[] = {
    /* clang-clang-format off */
    0, 0, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3, 3, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 5,
    5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 6, 6,
    6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
    6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 7, 7, 7, 7,
    7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
    7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
    7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
    7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7
    /* clang-clang-format on */
};

/*-------------------------------------------------------------------------
 * Function:    H5VM_log2_gen
 *
 * Purpose:     Determines the log base two of a number (i.e. log2(n)).
 *              (i.e. the highest bit set in a number)
 *
 * Note:        This is from the "Bit Twiddling Hacks" at:
 *                  http://graphics.stanford.edu/~seander/bithacks.html#IntegerLogLookup
 *
 *              The version on the web-site is for 32-bit quantities and this
 *              version has been extended for 64-bit quantities.
 *
 * Note:        Although this routine is 'static' in this file, that's intended
 *              only as an optimization and the naming (with a single underscore)
 *              reflects its inclusion in a "private" header file.
 *
 * Return:      log2(n) (always - no failure condition)
 *-------------------------------------------------------------------------
 */
static inline unsigned H5_ATTR_UNUSED
H5VM_log2_gen(uint64_t n)
{
    unsigned     r;          /* r will be log2(n) */
    unsigned int t, tt, ttt; /* temporaries */

    if ((ttt = (unsigned)(n >> 32)))
        if ((tt = (unsigned)(n >> 48)))
            r = (t = (unsigned)(n >> 56)) ? 56 + (unsigned)LogTable256[t]
                                          : 48 + (unsigned)LogTable256[tt & 0xFF];
        else
            r = (t = (unsigned)(n >> 40)) ? 40 + (unsigned)LogTable256[t]
                                          : 32 + (unsigned)LogTable256[ttt & 0xFF];
    else if ((tt = (unsigned)(n >> 16)))
        r = (t = (unsigned)(n >> 24)) ? 24 + (unsigned)LogTable256[t] : 16 + (unsigned)LogTable256[tt & 0xFF];
    else
        /* Added 'uint8_t' cast to pacify PGCC compiler */
        r = (t = (unsigned)(n >> 8)) ? 8 + (unsigned)LogTable256[t] : (unsigned)LogTable256[(uint8_t)n];

    return (r);
} /* H5VM_log2_gen() */

/* Lookup table for specialized log2(n) of power of two routine */
static const unsigned MultiplyDeBruijnBitPosition[32] = {0,  1,  28, 2,  29, 14, 24, 3,  30, 22, 20,
                                                         15, 25, 17, 4,  8,  31, 27, 13, 23, 21, 19,
                                                         16, 7,  26, 12, 18, 6,  11, 5,  10, 9};

/*-------------------------------------------------------------------------
 * Function:    H5VM_log2_of2
 *
 * Purpose:     Determines the log base two of a number (i.e. log2(n)).
 *              (i.e. the highest bit set in a number)
 *
 * Note:        **N must be a power of two** and is limited to 32-bit quantities.
 *
 *              This is from the "Bit Twiddling Hacks" at:
 *                  http://graphics.stanford.edu/~seander/bithacks.html#IntegerLogDeBruijn
 *
 * Note:        Although this routine is 'static' in this file, that's intended
 *              only as an optimization and the naming (with a single underscore)
 *              reflects its inclusion in a "private" header file.
 *
 * Return:      log2(n) (always - no failure condition)
 *-------------------------------------------------------------------------
 */
static inline H5_ATTR_PURE unsigned
H5VM_log2_of2(uint32_t n)
{
#ifndef NDEBUG
    assert(POWER_OF_TWO(n));
#endif /* NDEBUG */
    return (MultiplyDeBruijnBitPosition[(n * (uint32_t)0x077CB531UL) >> 27]);
} /* H5VM_log2_of2() */

/*-------------------------------------------------------------------------
 * Function:    H5VM_power2up
 *
 * Purpose:     Round up a number to the next power of 2
 *
 * Note:        Although this routine is 'static' in this file, that's intended
 *              only as an optimization and the naming (with a single underscore)
 *              reflects its inclusion in a "private" header file.
 *
 * Return:      Return the number which is a power of 2
 *-------------------------------------------------------------------------
 */
static inline H5_ATTR_CONST hsize_t
H5VM_power2up(hsize_t n)
{
    hsize_t ret_value = 1; /* Return value */

    /* Returns 0 when n exceeds 2^63 */
    if (n >= (hsize_t)1 << ((sizeof(hsize_t) * CHAR_BIT) - 1))
        ret_value = 0;

    while (ret_value && ret_value < n)
        ret_value <<= 1;

    return (ret_value);
} /* H5VM_power2up */

/*-------------------------------------------------------------------------
 * Function:    H5VM_limit_enc_size
 *
 * Purpose:     Determine the # of bytes needed to encode values within a
 *              range from 0 to a given limit
 *
 * Note:        Although this routine is 'static' in this file, that's intended
 *              only as an optimization and the naming (with a single underscore)
 *              reflects its inclusion in a "private" header file.
 *
 * Return:      Number of bytes needed
 *-------------------------------------------------------------------------
 */
static inline unsigned H5_ATTR_UNUSED
H5VM_limit_enc_size(uint64_t limit)
{
    return (H5VM_log2_gen(limit) / 8) + 1;
} /* end H5VM_limit_enc_size() */

static const unsigned char H5VM_bit_set_g[8]   = {0x80, 0x40, 0x20, 0x10, 0x08, 0x04, 0x02, 0x01};
static const unsigned char H5VM_bit_clear_g[8] = {0x7F, 0xBF, 0xDF, 0xEF, 0xF7, 0xFB, 0xFD, 0xFE};

/*-------------------------------------------------------------------------
 * Function:    H5VM_bit_get
 *
 * Purpose:     Determine the value of the n'th bit in a buffer.
 *
 * Note:        No range checking on <offset> is performed!
 *
 * Note #2:     Bits are sequentially stored in the buffer, starting with bit
 *              offset 0 in the first byte's high-bit position, proceeding down
 *              to bit offset 7 in the first byte's low-bit position, then to
 *              bit offset 8 in the second byte's high-bit position, etc.
 *
 * Note:        Although this routine is 'static' in this file, that's intended
 *              only as an optimization and the naming (with a single underscore)
 *              reflects its inclusion in a "private" header file.
 *
 * Return:      true/false
 *-------------------------------------------------------------------------
 */
static inline bool H5_ATTR_UNUSED
H5VM_bit_get(const unsigned char *buf, size_t offset)
{
    /* Test the appropriate bit in the buffer */
    return (bool)((buf[offset / 8] & (H5VM_bit_set_g[offset % 8])) ? true : false);
} /* end H5VM_bit_get() */

/*-------------------------------------------------------------------------
 * Function:    H5VM_bit_set
 *
 * Purpose:     Set/reset the n'th bit in a buffer.
 *
 * Note:        No range checking on <offset> is performed!
 *
 * Note #2:     Bits are sequentially stored in the buffer, starting with bit
 *              offset 0 in the first byte's high-bit position, proceeding down
 *              to bit offset 7 in the first byte's low-bit position, then to
 *              bit offset 8 in the second byte's high-bit position, etc.
 *
 * Note:        Although this routine is 'static' in this file, that's intended
 *              only as an optimization and the naming (with a single underscore)
 *              reflects its inclusion in a "private" header file.
 *
 * Return:      void
 *-------------------------------------------------------------------------
 */
static inline void H5_ATTR_UNUSED
H5VM_bit_set(unsigned char *buf, size_t offset, bool val)
{
    /* Set/reset the appropriate bit in the buffer */
    if (val)
        buf[offset / 8] |= H5VM_bit_set_g[offset % 8];
    else
        buf[offset / 8] &= H5VM_bit_clear_g[offset % 8];
} /* end H5VM_bit_set() */

#endif /* H5VMprivate_H */
