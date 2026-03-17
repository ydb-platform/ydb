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

/*
 * File-independent encode/decode routines
 */

#ifndef H5encode_H
#define H5encode_H

/***********/
/* Headers */
/***********/
#include "H5MMprivate.h" /* Memory management                        */

/**************************/
/* Library Private Macros */
/**************************/

/*
 * Encode and decode macros for file meta-data.
 * Currently, all file meta-data is little-endian.
 */

#define INT16ENCODE(p, i)                                                                                    \
    do {                                                                                                     \
        *(p) = (uint8_t)((unsigned)(i)&0xff);                                                                \
        (p)++;                                                                                               \
        *(p) = (uint8_t)(((unsigned)(i) >> 8) & 0xff);                                                       \
        (p)++;                                                                                               \
    } while (0)

#define UINT16ENCODE(p, i)                                                                                   \
    do {                                                                                                     \
        *(p) = (uint8_t)((unsigned)(i)&0xff);                                                                \
        (p)++;                                                                                               \
        *(p) = (uint8_t)(((unsigned)(i) >> 8) & 0xff);                                                       \
        (p)++;                                                                                               \
    } while (0)

#define INT32ENCODE(p, i)                                                                                    \
    do {                                                                                                     \
        *(p) = (uint8_t)((uint32_t)(i)&0xff);                                                                \
        (p)++;                                                                                               \
        *(p) = (uint8_t)(((uint32_t)(i) >> 8) & 0xff);                                                       \
        (p)++;                                                                                               \
        *(p) = (uint8_t)(((uint32_t)(i) >> 16) & 0xff);                                                      \
        (p)++;                                                                                               \
        *(p) = (uint8_t)(((uint32_t)(i) >> 24) & 0xff);                                                      \
        (p)++;                                                                                               \
    } while (0)

#define UINT32ENCODE(p, i)                                                                                   \
    do {                                                                                                     \
        *(p) = (uint8_t)((i)&0xff);                                                                          \
        (p)++;                                                                                               \
        *(p) = (uint8_t)(((i) >> 8) & 0xff);                                                                 \
        (p)++;                                                                                               \
        *(p) = (uint8_t)(((i) >> 16) & 0xff);                                                                \
        (p)++;                                                                                               \
        *(p) = (uint8_t)(((i) >> 24) & 0xff);                                                                \
        (p)++;                                                                                               \
    } while (0)

/* Encode an unsigned integer into a variable-sized buffer */
/* (Assumes that the high bits of the integer are zero) */
#define ENCODE_VAR(p, typ, n, l)                                                                             \
    do {                                                                                                     \
        typ      _n = (n);                                                                                   \
        size_t   _i;                                                                                         \
        uint8_t *_p = (uint8_t *)(p);                                                                        \
                                                                                                             \
        for (_i = 0; _i < l; _i++, _n >>= 8)                                                                 \
            *_p++ = (uint8_t)(_n & 0xff);                                                                    \
        (p) = (uint8_t *)(p) + l;                                                                            \
    } while (0)

/* Encode a 32-bit unsigned integer into a variable-sized buffer */
/* (Assumes that the high bits of the integer are zero) */
#define UINT32ENCODE_VAR(p, n, l) ENCODE_VAR(p, uint32_t, n, l)

#define INT64ENCODE(p, n)                                                                                    \
    do {                                                                                                     \
        int64_t  _n = (n);                                                                                   \
        size_t   _i;                                                                                         \
        uint8_t *_p = (uint8_t *)(p);                                                                        \
                                                                                                             \
        for (_i = 0; _i < sizeof(int64_t); _i++, _n >>= 8)                                                   \
            *_p++ = (uint8_t)(_n & 0xff);                                                                    \
        for (/*void*/; _i < 8; _i++)                                                                         \
            *_p++ = (uint8_t)((n) < 0 ? 0xff : 0);                                                           \
        (p) = (uint8_t *)(p) + 8;                                                                            \
    } while (0)

#define UINT64ENCODE(p, n)                                                                                   \
    do {                                                                                                     \
        uint64_t _n = (n);                                                                                   \
        size_t   _i;                                                                                         \
        uint8_t *_p = (uint8_t *)(p);                                                                        \
                                                                                                             \
        for (_i = 0; _i < sizeof(uint64_t); _i++, _n >>= 8)                                                  \
            *_p++ = (uint8_t)(_n & 0xff);                                                                    \
        for (/*void*/; _i < 8; _i++)                                                                         \
            *_p++ = 0;                                                                                       \
        (p) = (uint8_t *)(p) + 8;                                                                            \
    } while (0)

/* Encode a 64-bit unsigned integer into a variable-sized buffer */
/* (Assumes that the high bits of the integer are zero) */
#define UINT64ENCODE_VAR(p, n, l) ENCODE_VAR(p, uint64_t, n, l)

#define H5_ENCODE_UNSIGNED(p, n)                                                                             \
    do {                                                                                                     \
        HDcompile_assert(sizeof(unsigned) == sizeof(uint32_t));                                              \
        UINT32ENCODE(p, n);                                                                                  \
    } while (0)

/* Assumes the endianness of uint64_t is the same as double */
#define H5_ENCODE_DOUBLE(p, n)                                                                               \
    do {                                                                                                     \
        uint64_t _n;                                                                                         \
        size_t   _u;                                                                                         \
        uint8_t *_p = (uint8_t *)(p);                                                                        \
                                                                                                             \
        HDcompile_assert(sizeof(double) == 8);                                                               \
        HDcompile_assert(sizeof(double) == sizeof(uint64_t));                                                \
        H5MM_memcpy(&_n, &n, sizeof(double));                                                                \
        for (_u = 0; _u < sizeof(uint64_t); _u++, _n >>= 8)                                                  \
            *_p++ = (uint8_t)(_n & 0xff);                                                                    \
        (p) = (uint8_t *)(p) + 8;                                                                            \
    } while (0)

/* DECODE converts little endian bytes pointed by p to integer values and store
 * it in i.  For signed values, need to do sign-extension when converting
 * the last byte which carries the sign bit.
 * The macros does not require i be of a certain byte sizes.  It just requires
 * i be big enough to hold the intended value range.  E.g. INT16DECODE works
 * correctly even if i is actually a 64bit int like in a Cray.
 */

#define INT16DECODE(p, i)                                                                                    \
    do {                                                                                                     \
        (i) = (int16_t)((*(p)&0xff));                                                                        \
        (p)++;                                                                                               \
        (i) |= (int16_t)(((*(p)&0xff) << 8) | ((*(p)&0x80) ? ~0xffff : 0x0));                                \
        (p)++;                                                                                               \
    } while (0)

#define UINT16DECODE(p, i)                                                                                   \
    do {                                                                                                     \
        (i) = (uint16_t)(*(p)&0xff);                                                                         \
        (p)++;                                                                                               \
        (i) |= (uint16_t)((*(p)&0xff) << 8);                                                                 \
        (p)++;                                                                                               \
    } while (0)

#define INT32DECODE(p, i)                                                                                    \
    do {                                                                                                     \
        (i) = ((int32_t)(*(p)&0xff));                                                                        \
        (p)++;                                                                                               \
        (i) |= ((int32_t)(*(p)&0xff) << 8);                                                                  \
        (p)++;                                                                                               \
        (i) |= ((int32_t)(*(p)&0xff) << 16);                                                                 \
        (p)++;                                                                                               \
        (i) |= ((int32_t)(((*(p) & (unsigned)0xff) << 24) | ((*(p)&0x80) ? ~0xffffffffULL : 0x0ULL)));       \
        (p)++;                                                                                               \
    } while (0)

#define UINT32DECODE(p, i)                                                                                   \
    do {                                                                                                     \
        (i) = (uint32_t)(*(p)&0xff);                                                                         \
        (p)++;                                                                                               \
        (i) |= ((uint32_t)(*(p)&0xff) << 8);                                                                 \
        (p)++;                                                                                               \
        (i) |= ((uint32_t)(*(p)&0xff) << 16);                                                                \
        (p)++;                                                                                               \
        (i) |= ((uint32_t)(*(p)&0xff) << 24);                                                                \
        (p)++;                                                                                               \
    } while (0)

/* Decode a variable-sized buffer */
/* (Assumes that the high bits of the integer will be zero) */
#define DECODE_VAR(p, n, l)                                                                                  \
    do {                                                                                                     \
        size_t _i;                                                                                           \
                                                                                                             \
        n = 0;                                                                                               \
        (p) += l;                                                                                            \
        for (_i = 0; _i < l; _i++)                                                                           \
            n = (n << 8) | *(--p);                                                                           \
        (p) += l;                                                                                            \
    } while (0)

/* Decode a variable-sized buffer into a 32-bit unsigned integer */
/* (Assumes that the high bits of the integer will be zero) */
#define UINT32DECODE_VAR(p, n, l) DECODE_VAR(p, n, l)

#define INT64DECODE(p, n)                                                                                    \
    do {                                                                                                     \
        /* WE DON'T CHECK FOR OVERFLOW! */                                                                   \
        size_t _i;                                                                                           \
                                                                                                             \
        n = 0;                                                                                               \
        (p) += 8;                                                                                            \
        for (_i = 0; _i < sizeof(int64_t); _i++)                                                             \
            n = (n << 8) | *(--p);                                                                           \
        (p) += 8;                                                                                            \
    } while (0)

#define UINT64DECODE(p, n)                                                                                   \
    do {                                                                                                     \
        /* WE DON'T CHECK FOR OVERFLOW! */                                                                   \
        size_t _i;                                                                                           \
                                                                                                             \
        n = 0;                                                                                               \
        (p) += 8;                                                                                            \
        for (_i = 0; _i < sizeof(uint64_t); _i++)                                                            \
            n = (n << 8) | *(--p);                                                                           \
        (p) += 8;                                                                                            \
    } while (0)

/* Decode a variable-sized buffer into a 64-bit unsigned integer */
/* (Assumes that the high bits of the integer will be zero) */
#define UINT64DECODE_VAR(p, n, l) DECODE_VAR(p, n, l)

#define H5_DECODE_UNSIGNED(p, n)                                                                             \
    do {                                                                                                     \
        HDcompile_assert(sizeof(unsigned) == sizeof(uint32_t));                                              \
        UINT32DECODE(p, n);                                                                                  \
    } while (0)

/* Assumes the endianness of uint64_t is the same as double */
#define H5_DECODE_DOUBLE(p, n)                                                                               \
    do {                                                                                                     \
        uint64_t _n;                                                                                         \
        size_t   _u;                                                                                         \
                                                                                                             \
        HDcompile_assert(sizeof(double) == 8);                                                               \
        HDcompile_assert(sizeof(double) == sizeof(uint64_t));                                                \
        _n = 0;                                                                                              \
        (p) += 8;                                                                                            \
        for (_u = 0; _u < sizeof(uint64_t); _u++)                                                            \
            _n = (_n << 8) | *(--p);                                                                         \
        H5MM_memcpy(&(n), &_n, sizeof(double));                                                              \
        (p) += 8;                                                                                            \
    } while (0)

/* Macros to encode/decode offset/length's for storing in the file */
#define H5_ENCODE_LENGTH_LEN(p, l, s)                                                                        \
    do {                                                                                                     \
        switch (s) {                                                                                         \
            case 4:                                                                                          \
                UINT32ENCODE(p, l);                                                                          \
                break;                                                                                       \
            case 8:                                                                                          \
                UINT64ENCODE(p, l);                                                                          \
                break;                                                                                       \
            case 2:                                                                                          \
                UINT16ENCODE(p, l);                                                                          \
                break;                                                                                       \
            default:                                                                                         \
                assert("bad sizeof size" && 0);                                                              \
        }                                                                                                    \
    } while (0)

#define H5_DECODE_LENGTH_LEN(p, l, s)                                                                        \
    do {                                                                                                     \
        switch (s) {                                                                                         \
            case 4:                                                                                          \
                UINT32DECODE(p, l);                                                                          \
                break;                                                                                       \
            case 8:                                                                                          \
                UINT64DECODE(p, l);                                                                          \
                break;                                                                                       \
            case 2:                                                                                          \
                UINT16DECODE(p, l);                                                                          \
                break;                                                                                       \
            default:                                                                                         \
                assert("bad sizeof size" && 0);                                                              \
        }                                                                                                    \
    } while (0)

#endif /* H5encode_H */
