/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 * Copyright (c) 2004-2015 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2009      Oak Ridge National Labs.  All rights reserved.
 * Copyright (c) 2015-2018 Research Organization for Information Science
 *                         and Technology (RIST).  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"

#ifdef HAVE_IEEE754_H
#include <ieee754.h>
#endif

#include <stddef.h>
#include <stdint.h>

#include "opal/util/arch.h"

#include "opal/types.h"
#include "opal/datatype/opal_datatype.h"
#include "opal/datatype/opal_convertor.h"
#include "opal/datatype/opal_datatype_internal.h"
#include "opal/datatype/opal_datatype_checksum.h"
#include "opal/datatype/opal_convertor_internal.h"


/*
 * Long-Term TODO:
 * We need a better way for the upper layer to convert
 * multiple, consecutive struct-types, e.g. float_int.
 * In the current design, the copy_float_heterogeneous and copy_float_heterogeneous
 * functions would be called 2*count times in total.
 * This is a big performance hit for a structure types.
 *
 * A better way would be to have a conversion registration functionality.
 */

static inline void
opal_dt_swap_bytes(void *to_p, const void *from_p, const size_t size, size_t count)
{
    size_t i;
    size_t back_i = size - 1;
    uint8_t *to = (uint8_t*) to_p;
    uint8_t *from = (uint8_t*) from_p;

    /* Do the first element */
    for (i = 0 ; i < size ; i++, back_i--) {
        to[back_i] = from[i];
    }
    /* Do all the others if any */
    while(count > 1) {
        to += size;
        from += size;
        count--;
        for (i = 0, back_i = size - 1 ; i < size ; i++, back_i--) {
            to[back_i] = from[i];
        }
    }
}

#ifdef HAVE_IEEE754_H
struct bit128 {
    unsigned int mantissa3:32;
    unsigned int mantissa2:32;
    unsigned int mantissa1:32;
    unsigned int mantissa0:16;
    unsigned int exponent:15;
    unsigned int negative:1;
};

struct bit80 {
    unsigned int pad:32;
    unsigned int empty:16;
    unsigned int negative:1;
    unsigned int exponent:15;
    unsigned int mantissa0:32;
    unsigned int mantissa1:32;
};

static inline void
opal_dt_swap_long_double(void *to_p, const void *from_p, const size_t size, size_t count, uint32_t remoteArch)
{
#ifdef HAVE_IEEE754_H
    size_t i;
    long double*to = (long double *) to_p;

    if ((opal_local_arch&OPAL_ARCH_LDISINTEL) && !(remoteArch&OPAL_ARCH_LDISINTEL)) {
#ifdef __x86_64
        for (i=0; i<count; i++, to++) {
            union ieee854_long_double ld;
            struct bit128 * b = (struct bit128 *)to;
            ld.ieee.empty = 0;
            ld.ieee.mantissa0 = 0x80000000 | (((unsigned int)b->mantissa0 << 15) & 0x7FFF8000) | ((b->mantissa1 >> 17) & 0x00007FFF);
            ld.ieee.mantissa1 = ((b->mantissa1 << 15) & 0xFFFF8000) | ((b->mantissa2 << 17) & 0x000007FFF);
            ld.ieee.exponent = b->exponent;
            ld.ieee.negative = b->negative;
            MEMCPY( to, &ld, sizeof(long double));
        }
#endif
    } else if (!(opal_local_arch&OPAL_ARCH_LDISINTEL) && (remoteArch&OPAL_ARCH_LDISINTEL)) {
#ifdef __sparcv9
        for (i=0; i<count; i++, to++) {
            union ieee854_long_double ld;
            struct bit80 * b = (struct bit80 *)to;
            ld.ieee.mantissa3 = 0;
            ld.ieee.mantissa2 = 0;
            ld.ieee.mantissa0 = (b->mantissa0 << 1) | (b->mantissa1 & 0x80000000);
            ld.ieee.mantissa1 = (b->mantissa1 << 1) & 0xFFFFFFFE;
            ld.ieee.exponent = b->exponent;
            ld.ieee.negative = b->negative;
            MEMCPY( to, &ld, sizeof(long double));
        }
#endif
    }
#else
    assert(0);
#endif
}
#else
#define opal_dt_swap_long_double(to_p, from_p, size, count, remoteArch)
#endif

/**
 * BEWARE: Do not use the following macro with composed types such as
 * complex. As the swap is done using the entire type sizeof, the
 * wrong endianess translation will be done.  Instead, use the
 * COPY_2SAMETYPE_HETEROGENEOUS.
 */
#define COPY_TYPE_HETEROGENEOUS( TYPENAME, TYPE )                                         \
            COPY_TYPE_HETEROGENEOUS_INTERNAL( TYPENAME, TYPE, 0 )

#define COPY_TYPE_HETEROGENEOUS_INTERNAL( TYPENAME, TYPE, LONG_DOUBLE )                   \
static int32_t                                                                            \
copy_##TYPENAME##_heterogeneous(opal_convertor_t *pConvertor, size_t count,               \
                                const char* from, size_t from_len, ptrdiff_t from_extent, \
                                char* to, size_t to_length, ptrdiff_t to_extent,          \
                                ptrdiff_t *advance)             \
{                                                                       \
    size_t i;                                                           \
                                                                        \
    datatype_check( #TYPE, sizeof(TYPE), sizeof(TYPE), &count,          \
                   from, from_len, from_extent,                         \
                   to, to_length, to_extent);                           \
                                                                        \
    if ((pConvertor->remoteArch & OPAL_ARCH_ISBIGENDIAN) !=             \
        (opal_local_arch & OPAL_ARCH_ISBIGENDIAN)) {                    \
        if( (to_extent == from_extent) && (to_extent == sizeof(TYPE)) ) { \
            opal_dt_swap_bytes(to, from, sizeof(TYPE), count);          \
            if (LONG_DOUBLE) {                                          \
                opal_dt_swap_long_double(to, from, sizeof(TYPE), count, pConvertor->remoteArch);\
            }                                                           \
        } else {                                                        \
            for( i = 0; i < count; i++ ) {                              \
                opal_dt_swap_bytes(to, from, sizeof(TYPE), 1);          \
                if (LONG_DOUBLE) {                                      \
                    opal_dt_swap_long_double(to, from, sizeof(TYPE), 1, pConvertor->remoteArch);\
                }                                                       \
                to += to_extent;                                        \
                from += from_extent;                                    \
            }                                                           \
        }                                                               \
    } else if ((ptrdiff_t)sizeof(TYPE) == to_extent &&          \
               (ptrdiff_t)sizeof(TYPE) == from_extent) {        \
         MEMCPY( to, from, count * sizeof(TYPE) );                      \
    } else {                                                            \
         /* source or destination are non-contigous */                  \
         for( i = 0; i < count; i++ ) {                                 \
             MEMCPY( to, from, sizeof(TYPE) );                          \
             to += to_extent;                                           \
             from += from_extent;                                       \
         }                                                              \
    }                                                                   \
    *advance = count * from_extent;                                     \
    return count;                                                       \
}

#define COPY_2SAMETYPE_HETEROGENEOUS( TYPENAME, TYPE )                                         \
            COPY_2SAMETYPE_HETEROGENEOUS_INTERNAL( TYPENAME, TYPE, 0)

#define COPY_2SAMETYPE_HETEROGENEOUS_INTERNAL( TYPENAME, TYPE, LONG_DOUBLE)                 \
static int32_t                                                                            \
copy_##TYPENAME##_heterogeneous(opal_convertor_t *pConvertor, size_t count,               \
                                const char* from, size_t from_len, ptrdiff_t from_extent, \
                                char* to, size_t to_length, ptrdiff_t to_extent,          \
                                ptrdiff_t *advance)             \
{                                                                       \
    size_t i;                                                           \
                                                                        \
    datatype_check( #TYPE, sizeof(TYPE), sizeof(TYPE), &count,          \
                   from, from_len, from_extent,                         \
                   to, to_length, to_extent);                           \
                                                                        \
    if ((pConvertor->remoteArch & OPAL_ARCH_ISBIGENDIAN) !=             \
        (opal_local_arch & OPAL_ARCH_ISBIGENDIAN)) {                    \
        if( (to_extent == from_extent) && (to_extent == (2 * sizeof(TYPE))) ) { \
            opal_dt_swap_bytes(to, from, sizeof(TYPE), 2 * count);      \
            if (LONG_DOUBLE) {                                          \
                opal_dt_swap_long_double(to, from, sizeof(TYPE), 2*count, pConvertor->remoteArch);\
            }                                                           \
        } else {                                                        \
            for( i = 0; i < count; i++ ) {                              \
                opal_dt_swap_bytes(to, from, sizeof(TYPE), 2);          \
                if (LONG_DOUBLE) {                                      \
                    opal_dt_swap_long_double(to, from, sizeof(TYPE), 2, pConvertor->remoteArch);\
                }                                                       \
                to += to_extent;                                        \
                from += from_extent;                                    \
            }                                                           \
        }                                                               \
    } else if ((ptrdiff_t)sizeof(TYPE) == to_extent &&          \
               (ptrdiff_t)sizeof(TYPE) == from_extent) {        \
         MEMCPY( to, from, count * sizeof(TYPE) );                      \
    } else {                                                            \
         /* source or destination are non-contigous */                  \
         for( i = 0; i < count; i++ ) {                                 \
             MEMCPY( to, from, sizeof(TYPE) );                          \
             to += to_extent;                                           \
             from += from_extent;                                       \
         }                                                              \
    }                                                                   \
    *advance = count * from_extent;                                     \
    return count;                                                       \
}

#define COPY_2TYPE_HETEROGENEOUS( TYPENAME, TYPE1, TYPE2 )              \
static int32_t                                                          \
copy_##TYPENAME##_heterogeneous(opal_convertor_t *pConvertor, size_t count, \
                                const char* from, size_t from_len, ptrdiff_t from_extent, \
                                char* to, size_t to_length, ptrdiff_t to_extent, \
                                ptrdiff_t *advance)             \
{                                                                       \
    size_t i;                                                           \
                                                                        \
    datatype_check( #TYPENAME, sizeof(TYPE1) + sizeof(TYPE2),           \
                   sizeof(TYPE1) + sizeof(TYPE2), &count,               \
                   from, from_len, from_extent,                         \
                   to, to_length, to_extent);                           \
                                                                        \
    if ((pConvertor->remoteArch & OPAL_ARCH_ISBIGENDIAN) !=             \
        (opal_local_arch & OPAL_ARCH_ISBIGENDIAN)) {                    \
        /* source and destination are different endianness */           \
        for( i = 0; i < count; i++ ) {                                  \
            TYPE1* to_1, *from_1;                                       \
            TYPE2* to_2, *from_2;                                       \
            to_1 = (TYPE1*) to; from_1 = (TYPE1*) from;                 \
            opal_dt_swap_bytes(to_1, from_1, sizeof(TYPE1), 1);         \
            to_2 = (TYPE2*) (to_1 + 1); from_2 = (TYPE2*) (from_1 + 1); \
            opal_dt_swap_bytes(to_2, from_2, sizeof(TYPE2), 1);         \
            to += to_extent;                                            \
            from += from_extent;                                        \
        }                                                               \
    } else if ((ptrdiff_t)(sizeof(TYPE1) + sizeof(TYPE2)) == to_extent &&   \
               (ptrdiff_t)(sizeof(TYPE1) + sizeof(TYPE2)) == from_extent) { \
        /* source and destination are contigous */                      \
        MEMCPY( to, from, count * (sizeof(TYPE1) + sizeof(TYPE2)) );    \
    } else {                                                            \
        /* source or destination are non-contigous */                   \
        for( i = 0; i < count; i++ ) {                                  \
            MEMCPY( to, from, sizeof(TYPE1) + sizeof(TYPE2) );          \
            to += to_extent;                                            \
            from += from_extent;                                        \
        }                                                               \
    }                                                                   \
    *advance = count * from_extent;                                     \
    return count;                                                       \
}


static inline void
datatype_check(char *type, size_t local_size, size_t remote_size, size_t *count,
               const char* from, size_t from_len, ptrdiff_t from_extent,
               char* to, size_t to_len, ptrdiff_t to_extent)
{
    /* make sure the remote buffer is large enough to hold the data */
    if( (remote_size * *count) > from_len ) {
        *count = from_len / remote_size;
        if( (*count * remote_size) != from_len ) {
            DUMP( "oops should I keep this data somewhere (excedent %d bytes)?\n",
                  from_len - (*count * remote_size) );
        }
        DUMP( "correct: copy %s count %d from buffer %p with length %d to %p space %d\n",
              "char", *count, from, from_len, to, to_len );
    } else {
        DUMP( "         copy %s count %d from buffer %p with length %d to %p space %d\n",
              "char", *count, from, from_len, to, to_len );
    }
}

#define CXX_BOOL_COPY_LOOP(TYPE)                        \
    for(size_t i = 0; i < count; i++ ) {                \
        bool *to_real = (bool*) to;                     \
        *to_real = *((TYPE*) from) == 0 ? false : true; \
        to += to_extent;                                \
        from += from_extent;                            \
    }
static int32_t
copy_cxx_bool_heterogeneous(opal_convertor_t *pConvertor, size_t count,
                            const char* from, size_t from_len, ptrdiff_t from_extent,
                            char* to, size_t to_length, ptrdiff_t to_extent,
                            ptrdiff_t *advance)
{
    /* fix up the from extent */
    if ((pConvertor->remoteArch & OPAL_ARCH_BOOLISxx) !=
        (opal_local_arch & OPAL_ARCH_BOOLISxx)) {
        switch (pConvertor->remoteArch & OPAL_ARCH_BOOLISxx) {
        case OPAL_ARCH_BOOLIS8:
            from_extent = 1;
            break;
        case OPAL_ARCH_BOOLIS16:
            from_extent = 2;
            break;
        case OPAL_ARCH_BOOLIS32:
            from_extent = 4;
            break;
        }
    }

    datatype_check( "bool", sizeof(bool), sizeof(bool), &count,
                   from, from_len, from_extent,
                   to, to_length, to_extent);

    if ((to_extent != sizeof(bool) || from_extent != sizeof(bool)) ||
        ((pConvertor->remoteArch & OPAL_ARCH_BOOLISxx) !=
         (opal_local_arch & OPAL_ARCH_BOOLISxx))) {
        switch (pConvertor->remoteArch & OPAL_ARCH_BOOLISxx) {
        case OPAL_ARCH_BOOLIS8:
            CXX_BOOL_COPY_LOOP(int8_t);
            break;
        case OPAL_ARCH_BOOLIS16:
            CXX_BOOL_COPY_LOOP(int16_t);
            break;
        case OPAL_ARCH_BOOLIS32:
            CXX_BOOL_COPY_LOOP(int32_t);
            break;
        }
    } else {
        MEMCPY( to, from, count * sizeof(bool) );
    }

    *advance = count * from_extent;
    return count;
}


COPY_TYPE_HETEROGENEOUS(int1, int8_t)
COPY_TYPE_HETEROGENEOUS(int2, int16_t)
COPY_TYPE_HETEROGENEOUS(int4, int32_t)
#ifdef HAVE_INT64_T
COPY_TYPE_HETEROGENEOUS(int8, int64_t)
#else
#define copy_int8_heterogeneous NULL
#endif

#ifdef HAVE_INT128_T
COPY_TYPE_HETEROGENEOUS(int16, int128_t)
#else
#define copy_int16_heterogeneous NULL
#endif


#if SIZEOF_FLOAT == 2
COPY_TYPE_HETEROGENEOUS( float2, float )
#elif SIZEOF_DOUBLE == 2
COPY_TYPE_HETEROGENEOUS( float2, double )
#elif HAVE_LONG_DOUBLE && SIZEOF_LONG_DOUBLE == 2
COPY_TYPE_HETEROGENEOUS( float2, long double )
#else
/* #error No basic type for copy function for opal_datatype_float2 found */
#define copy_float2_heterogeneous NULL
#endif

#if SIZEOF_FLOAT == 4
COPY_TYPE_HETEROGENEOUS( float4, float )
#elif SIZEOF_DOUBLE == 4
COPY_TYPE_HETEROGENEOUS( float4, double )
#elif HAVE_LONG_DOUBLE && SIZEOF_LONG_DOUBLE == 4
COPY_TYPE_HETEROGENEOUS( float4, long double )
#else
/* #error No basic type for copy function for opal_datatype_float4 found */
#define copy_float4_heterogeneous NULL
#endif

#if SIZEOF_FLOAT == 8
COPY_TYPE_HETEROGENEOUS( float8, float )
#elif SIZEOF_DOUBLE == 8
COPY_TYPE_HETEROGENEOUS( float8, double )
#elif HAVE_LONG_DOUBLE && SIZEOF_LONG_DOUBLE == 8
COPY_TYPE_HETEROGENEOUS( float8, long double )
#else
/* #error No basic type for copy function for opal_datatype_float8 found */
#define copy_float8_heterogeneous NULL
#endif

#if SIZEOF_FLOAT == 12
COPY_TYPE_HETEROGENEOUS( float12, float )
#elif SIZEOF_DOUBLE == 12
COPY_TYPE_HETEROGENEOUS( float12, double )
#elif HAVE_LONG_DOUBLE && SIZEOF_LONG_DOUBLE == 12
COPY_TYPE_HETEROGENEOUS( float12, long double )
#else
/* #error No basic type for copy function for opal_datatype_float12 found */
#define copy_float12_heterogeneous NULL
#endif

#if SIZEOF_FLOAT == 16
COPY_TYPE_HETEROGENEOUS( float16, float )
#elif SIZEOF_DOUBLE == 16
COPY_TYPE_HETEROGENEOUS( float16, double )
#elif HAVE_LONG_DOUBLE && SIZEOF_LONG_DOUBLE == 16
COPY_TYPE_HETEROGENEOUS_INTERNAL( float16, long double, 1)
#else
/* #error No basic type for copy function for opal_datatype_float16 found */
#define copy_float16_heterogeneous NULL
#endif

#if HAVE_FLOAT__COMPLEX
COPY_2SAMETYPE_HETEROGENEOUS( float_complex, float )
#else
/* #error No basic type for copy function for opal_datatype_float_complex found */
#define copy_float_complex_heterogeneous NULL
#endif

#if HAVE_DOUBLE__COMPLEX
COPY_2SAMETYPE_HETEROGENEOUS( double_complex, double )
#else
/* #error No basic type for copy function for opal_datatype_double_complex found */
#define copy_double_complex_heterogeneous NULL
#endif

#if HAVE_LONG_DOUBLE__COMPLEX
COPY_2SAMETYPE_HETEROGENEOUS_INTERNAL( long_double_complex, long double, 1)
#else
/* #error No basic type for copy function for opal_datatype_long_double_complex found */
#define copy_long_double_complex_heterogeneous NULL
#endif

COPY_TYPE_HETEROGENEOUS (wchar, wchar_t)

/* table of predefined copy functions - one for each MPI type */
conversion_fct_t opal_datatype_heterogeneous_copy_functions[OPAL_DATATYPE_MAX_PREDEFINED] = {
   [OPAL_DATATYPE_LOOP]                = NULL,
   [OPAL_DATATYPE_END_LOOP]            = NULL,
   [OPAL_DATATYPE_LB]                  = NULL,
   [OPAL_DATATYPE_UB]                  = NULL,
   [OPAL_DATATYPE_INT1]                = (conversion_fct_t) copy_int1_heterogeneous,
   [OPAL_DATATYPE_INT2]                = (conversion_fct_t) copy_int2_heterogeneous,
   [OPAL_DATATYPE_INT4]                = (conversion_fct_t) copy_int4_heterogeneous,
   [OPAL_DATATYPE_INT8]                = (conversion_fct_t) copy_int8_heterogeneous,
   [OPAL_DATATYPE_INT16]               = (conversion_fct_t) copy_int16_heterogeneous,
   [OPAL_DATATYPE_UINT1]               = (conversion_fct_t) copy_int1_heterogeneous,
   [OPAL_DATATYPE_UINT2]               = (conversion_fct_t) copy_int2_heterogeneous,
   [OPAL_DATATYPE_UINT4]               = (conversion_fct_t) copy_int4_heterogeneous,
   [OPAL_DATATYPE_UINT8]               = (conversion_fct_t) copy_int8_heterogeneous,
   [OPAL_DATATYPE_UINT16]              = (conversion_fct_t) copy_int16_heterogeneous,
   [OPAL_DATATYPE_FLOAT2]              = (conversion_fct_t) copy_float2_heterogeneous,
   [OPAL_DATATYPE_FLOAT4]              = (conversion_fct_t) copy_float4_heterogeneous,
   [OPAL_DATATYPE_FLOAT8]              = (conversion_fct_t) copy_float8_heterogeneous,
   [OPAL_DATATYPE_FLOAT12]             = (conversion_fct_t) copy_float12_heterogeneous,
   [OPAL_DATATYPE_FLOAT16]             = (conversion_fct_t) copy_float16_heterogeneous,
   [OPAL_DATATYPE_FLOAT_COMPLEX]       = (conversion_fct_t) copy_float_complex_heterogeneous,
   [OPAL_DATATYPE_DOUBLE_COMPLEX]      = (conversion_fct_t) copy_double_complex_heterogeneous,
   [OPAL_DATATYPE_LONG_DOUBLE_COMPLEX] = (conversion_fct_t) copy_long_double_complex_heterogeneous,
   [OPAL_DATATYPE_BOOL]                = (conversion_fct_t) copy_cxx_bool_heterogeneous,
   [OPAL_DATATYPE_WCHAR]               = (conversion_fct_t) copy_wchar_heterogeneous,
   [OPAL_DATATYPE_UNAVAILABLE]         = NULL,
};
