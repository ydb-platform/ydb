/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil  -*- */
/*
 * Copyright (c) 2009-2017 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2009      Oak Ridge National Labs.  All rights reserved.
 * Copyright (c) 2010-2012 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2013      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2015-2018 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2016      FUJITSU LIMITED.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/**
 * ompi_datatype_t interface for OMPI internal data type representation
 *
 * ompi_datatype_t is a class which represents contiguous or
 * non-contiguous data together with constituent type-related
 * information.
 */

#ifndef OMPI_DATATYPE_INTERNAL_H
#define OMPI_DATATYPE_INTERNAL_H

#include "opal/datatype/opal_datatype_internal.h"

/*
 * This is the OMPI-layered numbering of ALL supported MPI types
 * (derived from the old DT_ names).
 */
#define OMPI_DATATYPE_MPI_EMPTY                   0x00
#define OMPI_DATATYPE_MPI_INT8_T                  0x01
#define OMPI_DATATYPE_MPI_UINT8_T                 0x02
#define OMPI_DATATYPE_MPI_INT16_T                 0x03
#define OMPI_DATATYPE_MPI_UINT16_T                0x04
#define OMPI_DATATYPE_MPI_INT32_T                 0x05
#define OMPI_DATATYPE_MPI_UINT32_T                0x06
#define OMPI_DATATYPE_MPI_INT64_T                 0x07
#define OMPI_DATATYPE_MPI_UINT64_T                0x08
#define OMPI_DATATYPE_MPI_FLOAT                   0x09
#define OMPI_DATATYPE_MPI_DOUBLE                  0x0A
#define OMPI_DATATYPE_MPI_LONG_DOUBLE             0x0B
#define OMPI_DATATYPE_MPI_COMPLEX8                0x0C
#define OMPI_DATATYPE_MPI_COMPLEX16               0x0D
#define OMPI_DATATYPE_MPI_COMPLEX32               0x0E
#define OMPI_DATATYPE_MPI_WCHAR                   0x0F
#define OMPI_DATATYPE_MPI_PACKED                  0x10

#define OMPI_DATATYPE_MPI_BOOL                    0x11

#define OMPI_DATATYPE_MPI_LOGICAL                 0x12
#define OMPI_DATATYPE_MPI_CHARACTER               0x13
#define OMPI_DATATYPE_MPI_INTEGER                 0x14
#define OMPI_DATATYPE_MPI_REAL                    0x15
#define OMPI_DATATYPE_MPI_DOUBLE_PRECISION        0x16

/*
 * Derived datatypes supposely contiguous
 */
#define OMPI_DATATYPE_MPI_COMPLEX                 0x17   /* Was COMPLEX_FLOAT */
#define OMPI_DATATYPE_MPI_DOUBLE_COMPLEX          0x18   /* Was COMPLEX_DOUBLE */
#define OMPI_DATATYPE_MPI_LONG_DOUBLE_COMPLEX     0x19   /* Was COMPLEX_LONG_DOUBLE */
#define OMPI_DATATYPE_MPI_2INT                    0x1A
#define OMPI_DATATYPE_MPI_2INTEGER                0x1B
#define OMPI_DATATYPE_MPI_2REAL                   0x1C
#define OMPI_DATATYPE_MPI_2DBLPREC                0x1D
#define OMPI_DATATYPE_MPI_2COMPLEX                0x1E
#define OMPI_DATATYPE_MPI_2DOUBLE_COMPLEX         0x1F
/*
 * Derived datatypes which will definitively be non contiguous on some architectures.
 */
#define OMPI_DATATYPE_MPI_FLOAT_INT               0x20
#define OMPI_DATATYPE_MPI_DOUBLE_INT              0x21
#define OMPI_DATATYPE_MPI_LONG_DOUBLE_INT         0x22
#define OMPI_DATATYPE_MPI_LONG_INT                0x23
#define OMPI_DATATYPE_MPI_SHORT_INT               0x24
/*
 * Datatypes from the MPI 2.2 standard
 */
#define OMPI_DATATYPE_MPI_AINT                    0x25
#define OMPI_DATATYPE_MPI_OFFSET                  0x26
#define OMPI_DATATYPE_MPI_C_BOOL                  0x27
#define OMPI_DATATYPE_MPI_C_COMPLEX               0x28
#define OMPI_DATATYPE_MPI_C_FLOAT_COMPLEX         0x29
#define OMPI_DATATYPE_MPI_C_DOUBLE_COMPLEX        0x2A
#define OMPI_DATATYPE_MPI_C_LONG_DOUBLE_COMPLEX   0x2B

#define OMPI_DATATYPE_MPI_LB                      0x2C
#define OMPI_DATATYPE_MPI_UB                      0x2D

/*
 * Datatypes from the MPI 3.0 standard
 */
#define OMPI_DATATYPE_MPI_COUNT                   0x2E

/* This should __ALWAYS__ stay last  */
#define OMPI_DATATYPE_MPI_UNAVAILABLE             0x2F


#define OMPI_DATATYPE_MPI_MAX_PREDEFINED          (OMPI_DATATYPE_MPI_UNAVAILABLE+1)

/*
 * Ensure we can support the predefined datatypes.
 */
#if OMPI_DATATYPE_MAX_PREDEFINED < OMPI_DATATYPE_MPI_UNAVAILABLE
#error OMPI_DATATYPE_MAX_PREDEFINED should be updated to the value of OMPI_DATATYPE_MPI_UNAVAILABLE
#endif

/*
 * Mapped types. The following types have basic equivalents in OPAL. Instead
 * of being redefined as independent types, they will be made synonyms to
 * the most basic type.
 */
#if SIZEOF_CHAR == 1
#define OMPI_DATATYPE_MPI_CHAR                    OMPI_DATATYPE_MPI_INT8_T
#define OMPI_DATATYPE_MPI_SIGNED_CHAR             OMPI_DATATYPE_MPI_INT8_T
#define OMPI_DATATYPE_MPI_UNSIGNED_CHAR           OMPI_DATATYPE_MPI_UINT8_T
#define OMPI_DATATYPE_MPI_BYTE                    OMPI_DATATYPE_MPI_UINT8_T
#elif SIZEOF_CHAR == 2
#define OMPI_DATATYPE_MPI_CHAR                    OMPI_DATATYPE_MPI_INT16_T
#define OMPI_DATATYPE_MPI_SIGNED_CHAR             OMPI_DATATYPE_MPI_INT16_T
#define OMPI_DATATYPE_MPI_UNSIGNED_CHAR           OMPI_DATATYPE_MPI_UINT16_T
#define OMPI_DATATYPE_MPI_BYTE                    OMPI_DATATYPE_MPI_UINT16_T
#elif SIZEOF_CHAR == 4
#define OMPI_DATATYPE_MPI_CHAR                    OMPI_DATATYPE_MPI_INT32_T
#define OMPI_DATATYPE_MPI_SIGNED_CHAR             OMPI_DATATYPE_MPI_INT32_T
#define OMPI_DATATYPE_MPI_UNSIGNED_CHAR           OMPI_DATATYPE_MPI_UINT32_T
#define OMPI_DATATYPE_MPI_BYTE                    OMPI_DATATYPE_MPI_UINT32_T
#elif SIZEOF_CHAR == 8
#define OMPI_DATATYPE_MPI_CHAR                    OMPI_DATATYPE_MPI_INT64_T
#define OMPI_DATATYPE_MPI_SIGNED_CHAR             OMPI_DATATYPE_MPI_INT64_T
#define OMPI_DATATYPE_MPI_UNSIGNED_CHAR           OMPI_DATATYPE_MPI_UINT64_T
#define OMPI_DATATYPE_MPI_BYTE                    OMPI_DATATYPE_MPI_UINT64_T
#endif

#if SIZEOF_SHORT == 1
#define OMPI_DATATYPE_MPI_SHORT                   OMPI_DATATYPE_MPI_INT8_T
#define OMPI_DATATYPE_MPI_UNSIGNED_SHORT          OMPI_DATATYPE_MPI_UINT8_T
#elif SIZEOF_SHORT == 2
#define OMPI_DATATYPE_MPI_SHORT                   OMPI_DATATYPE_MPI_INT16_T
#define OMPI_DATATYPE_MPI_UNSIGNED_SHORT          OMPI_DATATYPE_MPI_UINT16_T
#elif SIZEOF_SHORT == 4
#define OMPI_DATATYPE_MPI_SHORT                   OMPI_DATATYPE_MPI_INT32_T
#define OMPI_DATATYPE_MPI_UNSIGNED_SHORT          OMPI_DATATYPE_MPI_UINT32_T
#elif SIZEOF_SHORT == 8
#define OMPI_DATATYPE_MPI_SHORT                   OMPI_DATATYPE_MPI_INT64_T
#define OMPI_DATATYPE_MPI_UNSIGNED_SHORT          OMPI_DATATYPE_MPI_UINT64_T
#endif

#if SIZEOF_INT == 1
#define OMPI_DATATYPE_MPI_INT                     OMPI_DATATYPE_MPI_INT8_T
#define OMPI_DATATYPE_MPI_UNSIGNED                OMPI_DATATYPE_MPI_UINT8_T
#elif SIZEOF_INT == 2
#define OMPI_DATATYPE_MPI_INT                     OMPI_DATATYPE_MPI_INT16_T
#define OMPI_DATATYPE_MPI_UNSIGNED                OMPI_DATATYPE_MPI_UINT16_T
#elif SIZEOF_INT == 4
#define OMPI_DATATYPE_MPI_INT                     OMPI_DATATYPE_MPI_INT32_T
#define OMPI_DATATYPE_MPI_UNSIGNED                OMPI_DATATYPE_MPI_UINT32_T
#elif SIZEOF_INT == 8
#define OMPI_DATATYPE_MPI_INT                     OMPI_DATATYPE_MPI_INT64_T
#define OMPI_DATATYPE_MPI_UNSIGNED                OMPI_DATATYPE_MPI_UINT64_T
#endif

#if SIZEOF_LONG == 1
#define OMPI_DATATYPE_MPI_LONG                    OMPI_DATATYPE_MPI_INT8_T
#define OMPI_DATATYPE_MPI_UNSIGNED_LONG           OMPI_DATATYPE_MPI_UINT8_T
#elif SIZEOF_LONG == 2
#define OMPI_DATATYPE_MPI_LONG                    OMPI_DATATYPE_MPI_INT16_T
#define OMPI_DATATYPE_MPI_UNSIGNED_LONG           OMPI_DATATYPE_MPI_UINT16_T
#elif SIZEOF_LONG == 4
#define OMPI_DATATYPE_MPI_LONG                    OMPI_DATATYPE_MPI_INT32_T
#define OMPI_DATATYPE_MPI_UNSIGNED_LONG           OMPI_DATATYPE_MPI_UINT32_T
#elif SIZEOF_LONG == 8
#define OMPI_DATATYPE_MPI_LONG                    OMPI_DATATYPE_MPI_INT64_T
#define OMPI_DATATYPE_MPI_UNSIGNED_LONG           OMPI_DATATYPE_MPI_UINT64_T
#endif

#if SIZEOF_LONG_LONG == 1
#define OMPI_DATATYPE_MPI_LONG_LONG_INT           OMPI_DATATYPE_MPI_INT8_T
#define OMPI_DATATYPE_MPI_UNSIGNED_LONG_LONG      OMPI_DATATYPE_MPI_UINT8_T
#elif SIZEOF_LONG_LONG == 2
#define OMPI_DATATYPE_MPI_LONG_LONG_INT           OMPI_DATATYPE_MPI_INT16_T
#define OMPI_DATATYPE_MPI_UNSIGNED_LONG_LONG      OMPI_DATATYPE_MPI_UINT16_T
#elif SIZEOF_LONG_LONG == 4
#define OMPI_DATATYPE_MPI_LONG_LONG_INT           OMPI_DATATYPE_MPI_INT32_T
#define OMPI_DATATYPE_MPI_UNSIGNED_LONG_LONG      OMPI_DATATYPE_MPI_UINT32_T
#elif SIZEOF_LONG_LONG == 8
#define OMPI_DATATYPE_MPI_LONG_LONG_INT           OMPI_DATATYPE_MPI_INT64_T
#define OMPI_DATATYPE_MPI_UNSIGNED_LONG_LONG      OMPI_DATATYPE_MPI_UINT64_T
#endif

/*
 * Optional Fortran datatypes, these map to representable types
 * in the lower layer, aka as other Fortran types have to map to C types,
 * additionally, if the type has the same size as the mandatory
 * Fortran type, map to this one.
 */
/* LOGICAL */
#if OMPI_SIZEOF_FORTRAN_LOGICAL1 == OMPI_SIZEOF_FORTRAN_LOGICAL
#  define OMPI_DATATYPE_MPI_LOGICAL1              OMPI_DATATYPE_MPI_LOGICAL
#elif OMPI_SIZEOF_FORTRAN_LOGICAL1 == 1
#  define OMPI_DATATYPE_MPI_LOGICAL1              OMPI_DATATYPE_MPI_INT8_T
#elif OMPI_SIZEOF_FORTRAN_LOGICAL1 == 2
#  define OMPI_DATATYPE_MPI_LOGICAL1              OMPI_DATATYPE_MPI_INT16_T
#elif OMPI_SIZEOF_FORTRAN_LOGICAL1 == 4
#  define OMPI_DATATYPE_MPI_LOGICAL1              OMPI_DATATYPE_MPI_INT32_T
#elif OMPI_SIZEOF_FORTRAN_LOGICAL1 == 8
#  define OMPI_DATATYPE_MPI_LOGICAL1              OMPI_DATATYPE_MPI_INT64_T
#else
#  define OMPI_DATATYPE_MPI_LOGICAL1              OMPI_DATATYPE_MPI_UNAVAILABLE
#endif

#if OMPI_SIZEOF_FORTRAN_LOGICAL2 == OMPI_SIZEOF_FORTRAN_LOGICAL
#  define OMPI_DATATYPE_MPI_LOGICAL2              OMPI_DATATYPE_MPI_LOGICAL
#elif OMPI_SIZEOF_FORTRAN_LOGICAL2 == 1
#  define OMPI_DATATYPE_MPI_LOGICAL2              OMPI_DATATYPE_MPI_INT8_T
#elif OMPI_SIZEOF_FORTRAN_LOGICAL2 == 2
#  define OMPI_DATATYPE_MPI_LOGICAL2              OMPI_DATATYPE_MPI_INT16_T
#elif OMPI_SIZEOF_FORTRAN_LOGICAL2 == 4
#  define OMPI_DATATYPE_MPI_LOGICAL2              OMPI_DATATYPE_MPI_INT32_T
#elif OMPI_SIZEOF_FORTRAN_LOGICAL2 == 8
#  define OMPI_DATATYPE_MPI_LOGICAL2              OMPI_DATATYPE_MPI_INT64_T
#else
#  define OMPI_DATATYPE_MPI_LOGICAL2              OMPI_DATATYPE_MPI_UNAVAILABLE
#endif

#if OMPI_SIZEOF_FORTRAN_LOGICAL4 == OMPI_SIZEOF_FORTRAN_LOGICAL
#  define OMPI_DATATYPE_MPI_LOGICAL4              OMPI_DATATYPE_MPI_LOGICAL
#elif OMPI_SIZEOF_FORTRAN_LOGICAL4 == 1
#  define OMPI_DATATYPE_MPI_LOGICAL4              OMPI_DATATYPE_MPI_INT8_T
#elif OMPI_SIZEOF_FORTRAN_LOGICAL4 == 2
#  define OMPI_DATATYPE_MPI_LOGICAL4              OMPI_DATATYPE_MPI_INT16_T
#elif OMPI_SIZEOF_FORTRAN_LOGICAL4 == 4
#  define OMPI_DATATYPE_MPI_LOGICAL4              OMPI_DATATYPE_MPI_INT32_T
#elif OMPI_SIZEOF_FORTRAN_LOGICAL4 == 8
#  define OMPI_DATATYPE_MPI_LOGICAL4              OMPI_DATATYPE_MPI_INT64_T
#else
#  define OMPI_DATATYPE_MPI_LOGICAL4              OMPI_DATATYPE_MPI_UNAVAILABLE
#endif

#if OMPI_SIZEOF_FORTRAN_LOGICAL8 == OMPI_SIZEOF_FORTRAN_LOGICAL
#  define OMPI_DATATYPE_MPI_LOGICAL8              OMPI_DATATYPE_MPI_LOGICAL
#elif OMPI_SIZEOF_FORTRAN_LOGICAL8 == 1
#  define OMPI_DATATYPE_MPI_LOGICAL8              OMPI_DATATYPE_MPI_INT8_T
#elif OMPI_SIZEOF_FORTRAN_LOGICAL8 == 2
#  define OMPI_DATATYPE_MPI_LOGICAL8              OMPI_DATATYPE_MPI_INT16_T
#elif OMPI_SIZEOF_FORTRAN_LOGICAL8 == 4
#  define OMPI_DATATYPE_MPI_LOGICAL8              OMPI_DATATYPE_MPI_INT32_T
#elif OMPI_SIZEOF_FORTRAN_LOGICAL8 == 8
#  define OMPI_DATATYPE_MPI_LOGICAL8              OMPI_DATATYPE_MPI_INT64_T
#else
#  define OMPI_DATATYPE_MPI_LOGICAL8              OMPI_DATATYPE_MPI_UNAVAILABLE
#endif

/* INTEGER */
#if OMPI_SIZEOF_FORTRAN_INTEGER1 == OMPI_SIZEOF_FORTRAN_INTEGER
#  define OMPI_DATATYPE_MPI_INTEGER1              OMPI_DATATYPE_MPI_INTEGER
#elif OMPI_SIZEOF_FORTRAN_INTEGER1 == 1
#  define OMPI_DATATYPE_MPI_INTEGER1              OMPI_DATATYPE_MPI_INT8_T
#elif OMPI_SIZEOF_FORTRAN_INTEGER1 == 2
#  define OMPI_DATATYPE_MPI_INTEGER1              OMPI_DATATYPE_MPI_INT16_T
#elif OMPI_SIZEOF_FORTRAN_INTEGER1 == 4
#  define OMPI_DATATYPE_MPI_INTEGER1              OMPI_DATATYPE_MPI_INT32_T
#elif OMPI_SIZEOF_FORTRAN_INTEGER1 == 8
#  define OMPI_DATATYPE_MPI_INTEGER1              OMPI_DATATYPE_MPI_INT64_T
#else
#  define OMPI_DATATYPE_MPI_INTEGER1              OMPI_DATATYPE_MPI_UNAVAILABLE
#endif

#if OMPI_SIZEOF_FORTRAN_INTEGER2 == OMPI_SIZEOF_FORTRAN_INTEGER
#  define OMPI_DATATYPE_MPI_INTEGER2              OMPI_DATATYPE_MPI_INTEGER
#elif OMPI_SIZEOF_FORTRAN_INTEGER2 == 1
#  define OMPI_DATATYPE_MPI_INTEGER2              OMPI_DATATYPE_MPI_INT8_T
#elif OMPI_SIZEOF_FORTRAN_INTEGER2 == 2
#  define OMPI_DATATYPE_MPI_INTEGER2              OMPI_DATATYPE_MPI_INT16_T
#elif OMPI_SIZEOF_FORTRAN_INTEGER2 == 4
#  define OMPI_DATATYPE_MPI_INTEGER2              OMPI_DATATYPE_MPI_INT32_T
#elif OMPI_SIZEOF_FORTRAN_INTEGER2 == 8
#  define OMPI_DATATYPE_MPI_INTEGER2              OMPI_DATATYPE_MPI_INT64_T
#else
#  define OMPI_DATATYPE_MPI_INTEGER2              OMPI_DATATYPE_MPI_UNAVAILABLE
#endif

#if OMPI_SIZEOF_FORTRAN_INTEGER4 == OMPI_SIZEOF_FORTRAN_INTEGER
#  define OMPI_DATATYPE_MPI_INTEGER4              OMPI_DATATYPE_MPI_INTEGER
#elif OMPI_SIZEOF_FORTRAN_INTEGER4 == 1
#  define OMPI_DATATYPE_MPI_INTEGER4              OMPI_DATATYPE_MPI_INT8_T
#elif OMPI_SIZEOF_FORTRAN_INTEGER4 == 2
#  define OMPI_DATATYPE_MPI_INTEGER4              OMPI_DATATYPE_MPI_INT16_T
#elif OMPI_SIZEOF_FORTRAN_INTEGER4 == 4
#  define OMPI_DATATYPE_MPI_INTEGER4              OMPI_DATATYPE_MPI_INT32_T
#elif OMPI_SIZEOF_FORTRAN_INTEGER4 == 8
#  define OMPI_DATATYPE_MPI_INTEGER4              OMPI_DATATYPE_MPI_INT64_T
#else
#  define OMPI_DATATYPE_MPI_INTEGER4              OMPI_DATATYPE_MPI_UNAVAILABLE
#endif

#if OMPI_SIZEOF_FORTRAN_INTEGER8 == OMPI_SIZEOF_FORTRAN_INTEGER
#  define OMPI_DATATYPE_MPI_INTEGER8              OMPI_DATATYPE_MPI_INTEGER
#elif OMPI_SIZEOF_FORTRAN_INTEGER8 == 1
#  define OMPI_DATATYPE_MPI_INTEGER8              OMPI_DATATYPE_MPI_INT8_T
#elif OMPI_SIZEOF_FORTRAN_INTEGER8 == 2
#  define OMPI_DATATYPE_MPI_INTEGER8              OMPI_DATATYPE_MPI_INT16_T
#elif OMPI_SIZEOF_FORTRAN_INTEGER8 == 4
#  define OMPI_DATATYPE_MPI_INTEGER8              OMPI_DATATYPE_MPI_INT32_T
#elif OMPI_SIZEOF_FORTRAN_INTEGER8 == 8
#  define OMPI_DATATYPE_MPI_INTEGER8              OMPI_DATATYPE_MPI_INT64_T
#else
#  define OMPI_DATATYPE_MPI_INTEGER8              OMPI_DATATYPE_MPI_UNAVAILABLE
#endif

#if OMPI_SIZEOF_FORTRAN_INTEGER16 == OMPI_SIZEOF_FORTRAN_INTEGER
#  define OMPI_DATATYPE_MPI_INTEGER16             OMPI_DATATYPE_MPI_INTEGER
#elif OMPI_SIZEOF_FORTRAN_INTEGER16 == 1
#  define OMPI_DATATYPE_MPI_INTEGER16             OMPI_DATATYPE_MPI_INT8_T
#elif OMPI_SIZEOF_FORTRAN_INTEGER16 == 2
#  define OMPI_DATATYPE_MPI_INTEGER16             OMPI_DATATYPE_MPI_INT16_T
#elif OMPI_SIZEOF_FORTRAN_INTEGER16 == 4
#  define OMPI_DATATYPE_MPI_INTEGER16             OMPI_DATATYPE_MPI_INT32_T
#elif OMPI_SIZEOF_FORTRAN_INTEGER16 == 8
#  define OMPI_DATATYPE_MPI_INTEGER16             OMPI_DATATYPE_MPI_INT64_T
#else
#  define OMPI_DATATYPE_MPI_INTEGER16             OMPI_DATATYPE_MPI_UNAVAILABLE
#endif

/* REAL */
#if OMPI_SIZEOF_FORTRAN_REAL2 == OMPI_SIZEOF_FORTRAN_REAL
#  define OMPI_DATATYPE_MPI_REAL2                 OMPI_DATATYPE_MPI_REAL
#elif OMPI_SIZEOF_FORTRAN_REAL2 == SIZEOF_FLOAT
#  define OMPI_DATATYPE_MPI_REAL2                 OMPI_DATATYPE_MPI_FLOAT
#elif OMPI_SIZEOF_FORTRAN_REAL2 == SIZEOF_DOUBLE
#  define OMPI_DATATYPE_MPI_REAL2                 OMPI_DATATYPE_MPI_DOUBLE
#elif OMPI_SIZEOF_FORTRAN_REAL2 == SIZEOF_LONG_DOUBLE
#  define OMPI_DATATYPE_MPI_REAL2                 OMPI_DATATYPE_MPI_LONG_DOUBLE
#else
#  define OMPI_DATATYPE_MPI_REAL2                 OMPI_DATATYPE_MPI_UNAVAILABLE
#endif

#if OMPI_SIZEOF_FORTRAN_REAL4 == OMPI_SIZEOF_FORTRAN_REAL
#  define OMPI_DATATYPE_MPI_REAL4                 OMPI_DATATYPE_MPI_REAL
#elif OMPI_SIZEOF_FORTRAN_REAL4 == SIZEOF_FLOAT
#  define OMPI_DATATYPE_MPI_REAL4                 OMPI_DATATYPE_MPI_FLOAT
#elif OMPI_SIZEOF_FORTRAN_REAL4 == SIZEOF_DOUBLE
#  define OMPI_DATATYPE_MPI_REAL4                 OMPI_DATATYPE_MPI_DOUBLE
#elif OMPI_SIZEOF_FORTRAN_REAL4 == SIZEOF_LONG_DOUBLE
#  define OMPI_DATATYPE_MPI_REAL4                 OMPI_DATATYPE_MPI_LONG_DOUBLE
#else
#  define OMPI_DATATYPE_MPI_REAL4                 OMPI_DATATYPE_MPI_UNAVAILABLE
#endif

#if OMPI_SIZEOF_FORTRAN_REAL8 == OMPI_SIZEOF_FORTRAN_REAL
#  define OMPI_DATATYPE_MPI_REAL8                 OMPI_DATATYPE_MPI_REAL
#elif OMPI_SIZEOF_FORTRAN_REAL8 == SIZEOF_FLOAT
#  define OMPI_DATATYPE_MPI_REAL8                 OMPI_DATATYPE_MPI_FLOAT
#elif OMPI_SIZEOF_FORTRAN_REAL8 == SIZEOF_DOUBLE
#  define OMPI_DATATYPE_MPI_REAL8                 OMPI_DATATYPE_MPI_DOUBLE
#elif OMPI_SIZEOF_FORTRAN_REAL8 == SIZEOF_LONG_DOUBLE
#  define OMPI_DATATYPE_MPI_REAL8                 OMPI_DATATYPE_MPI_LONG_DOUBLE
#else
#  define OMPI_DATATYPE_MPI_REAL8                 OMPI_DATATYPE_MPI_UNAVAILABLE
#endif

#if OMPI_SIZEOF_FORTRAN_REAL16 == OMPI_SIZEOF_FORTRAN_REAL
#  define OMPI_DATATYPE_MPI_REAL16                OMPI_DATATYPE_MPI_REAL
#elif OMPI_SIZEOF_FORTRAN_REAL16 == SIZEOF_FLOAT
#  define OMPI_DATATYPE_MPI_REAL16                OMPI_DATATYPE_MPI_FLOAT
#elif OMPI_SIZEOF_FORTRAN_REAL16 == SIZEOF_DOUBLE
#  define OMPI_DATATYPE_MPI_REAL16                OMPI_DATATYPE_MPI_DOUBLE
#elif OMPI_SIZEOF_FORTRAN_REAL16 == SIZEOF_LONG_DOUBLE
#  define OMPI_DATATYPE_MPI_REAL16                OMPI_DATATYPE_MPI_LONG_DOUBLE
#else
#  define OMPI_DATATYPE_MPI_REAL16                OMPI_DATATYPE_MPI_UNAVAILABLE
#endif

/*
 * C++ datatypes, these map to C datatypes.
 */
#define OMPI_DATATYPE_MPI_CXX_BOOL                OMPI_DATATYPE_MPI_C_BOOL
#define OMPI_DATATYPE_MPI_CXX_FLOAT_COMPLEX       OMPI_DATATYPE_MPI_C_FLOAT_COMPLEX
#define OMPI_DATATYPE_MPI_CXX_DOUBLE_COMPLEX      OMPI_DATATYPE_MPI_C_DOUBLE_COMPLEX
#define OMPI_DATATYPE_MPI_CXX_LONG_DOUBLE_COMPLEX OMPI_DATATYPE_MPI_C_LONG_DOUBLE_COMPLEX

extern const ompi_datatype_t* ompi_datatype_basicDatatypes[OMPI_DATATYPE_MPI_MAX_PREDEFINED];

/* There 3 types of predefined data types.
 * - the basic one composed by just one basic datatype which are
 *   definitively contiguous
 * - the derived ones where the same basic type is used multiple times.
 *   They should be most of the time contiguous.
 * - and finally the derived one where multiple basic types are used.
 *   Depending on the architecture they can be contiguous or not.
 *
 * At this level we do not care from which language the datatype came from
 * (C, C++ or FORTRAN), we only focus on their internal representation in
 * the host memory.
 */

#define OMPI_DATATYPE_EMPTY_DATA(NAME)                                               \
    .id = OMPI_DATATYPE_MPI_ ## NAME,                                                \
    .d_f_to_c_index = -1,                                                            \
    .d_keyhash = NULL,                                                               \
    .args = NULL,                                                                    \
    .packed_description = NULL,                                                      \
    .name = "MPI_" # NAME

#define OMPI_DATATYPE_INITIALIZER_UNAVAILABLE(FLAGS)                                 \
    OPAL_DATATYPE_INITIALIZER_UNAVAILABLE(FLAGS)

#define OMPI_DATATYPE_INIT_PREDEFINED_BASIC_TYPE_X( TYPE, NAME, FLAGS )              \
    { /*ompi_predefined_datatype_t*/                                                 \
        { /* ompi_datatype_t */                                                      \
            OMPI_DATATYPE_INITIALIZER_ ## TYPE (OMPI_DATATYPE_FLAG_PREDEFINED |      \
                                                OMPI_DATATYPE_FLAG_ANALYZED   |      \
                                                OMPI_DATATYPE_FLAG_MONOTONIC  |      \
                                                (FLAGS)) /*super*/,                  \
            OMPI_DATATYPE_EMPTY_DATA(NAME) /*id,d_f_to_c_index,d_keyhash,args,packed_description,name*/ \
        },                                                                           \
        {0, } /* padding */                                                          \
    }

/* Preprocessor hack to make sure TYPE gets expanded properly */
#define OMPI_DATATYPE_INIT_PREDEFINED_BASIC_TYPE(TYPE, NAME, FLAGS) OMPI_DATATYPE_INIT_PREDEFINED_BASIC_TYPE_X(TYPE, NAME, FLAGS)

/*
 * Two macros for convenience
 */
#define OMPI_DATATYPE_INIT_PREDEFINED( NAME, FLAGS )                                 \
    OMPI_DATATYPE_INIT_PREDEFINED_BASIC_TYPE( NAME, NAME, FLAGS )
#define OMPI_DATATYPE_INIT_UNAVAILABLE( NAME, FLAGS )                                \
    OMPI_DATATYPE_INIT_PREDEFINED_BASIC_TYPE( UNAVAILABLE, NAME, FLAGS )
#define OMPI_DATATYPE_INIT_UNAVAILABLE_BASIC_TYPE(TYPE, NAME, FLAGS)                 \
    OMPI_DATATYPE_INIT_PREDEFINED_BASIC_TYPE( UNAVAILABLE, NAME, FLAGS )

/*
 * Initilization for these types is deferred until runtime.
 *
 * Using this macro implies that at this point not all informations needed
 * to fill up the datatype are known. We fill them with zeros and then later
 * when the datatype engine will be initialized we complete with the
 * correct information. This macro should be used for all composed types.
 */
#define OMPI_DATATYPE_INIT_DEFER(NAME, FLAGS)                                        \
    OMPI_DATATYPE_INIT_UNAVAILABLE(NAME, FLAGS)


#if OMPI_BUILD_FORTRAN_BINDINGS
/*
 * Fortran types are based on the underlying OPAL types: They share the ID -- however,
 * the alignment is overwritten.
 */
#define OMPI_DATATYPE_INITIALIZER_FORTRAN( TYPE, NAME, SIZE, ALIGN, FLAGS )          \
    {                                                                                \
        .super = OPAL_OBJ_STATIC_INIT(opal_datatype_t),                              \
        .flags = OPAL_DATATYPE_FLAG_BASIC |                                          \
            OMPI_DATATYPE_FLAG_PREDEFINED |                                          \
            OMPI_DATATYPE_FLAG_ANALYZED   |                                          \
            OMPI_DATATYPE_FLAG_MONOTONIC  |                                          \
            OMPI_DATATYPE_FLAG_DATA_FORTRAN | (FLAGS),                               \
        .id = OPAL_DATATYPE_ ## TYPE ## SIZE,                                        \
        .bdt_used = (((uint32_t)1)<<(OPAL_DATATYPE_ ## TYPE ## SIZE)),               \
        .size = SIZE,                                                                \
        .true_lb = 0, .true_ub = SIZE, .lb = 0, .ub = SIZE,                          \
        .align = (ALIGN),                                                            \
        .nbElems = 1,                                                                \
        .name = OPAL_DATATYPE_INIT_NAME(TYPE ## SIZE),                               \
        .desc = OPAL_DATATYPE_INIT_DESC_PREDEFINED(TYPE ## SIZE),                    \
        .opt_desc = OPAL_DATATYPE_INIT_DESC_PREDEFINED(TYPE ## SIZE),                \
        .ptypes = OPAL_DATATYPE_INIT_PTYPES_ARRAY(TYPE ## SIZE)                      \
    }

#define OMPI_DATATYPE_INIT_PREDEFINED_BASIC_TYPE_FORTRAN( TYPE, NAME, SIZE, ALIGN, FLAGS ) \
    { /*ompi_predefined_datatype_t*/                                                 \
        .dt = {                                                                      \
            .super = OMPI_DATATYPE_INITIALIZER_FORTRAN( TYPE, NAME, SIZE, ALIGN, FLAGS), \
            OMPI_DATATYPE_EMPTY_DATA(NAME) /*id,d_f_to_c_index,d_keyhash,args,packed_description,name*/ \
        },                                                                           \
        .padding = {0, }                                                             \
    }
#else
#define OMPI_DATATYPE_INIT_PREDEFINED_BASIC_TYPE_FORTRAN( TYPE, NAME, SIZE, ALIGN, FLAGS ) \
    OMPI_DATATYPE_INIT_PREDEFINED_BASIC_TYPE( UNAVAILABLE, NAME, FLAGS )
#endif


/*
 * OMPI-Versions of Initializer mapped onto OPAL-Types
 */

#define OMPI_DATATYPE_INITIALIZER_LB                  OPAL_DATATYPE_INITIALIZER_LB
#define OMPI_DATATYPE_INITIALIZER_UB                  OPAL_DATATYPE_INITIALIZER_UB

#define OMPI_DATATYPE_INITIALIZER_INT8_T              OPAL_DATATYPE_INITIALIZER_INT1
#define OMPI_DATATYPE_INITIALIZER_UINT8_T             OPAL_DATATYPE_INITIALIZER_UINT1
#define OMPI_DATATYPE_INITIALIZER_INT16_T             OPAL_DATATYPE_INITIALIZER_INT2
#define OMPI_DATATYPE_INITIALIZER_UINT16_T            OPAL_DATATYPE_INITIALIZER_UINT2
#define OMPI_DATATYPE_INITIALIZER_INT32_T             OPAL_DATATYPE_INITIALIZER_INT4
#define OMPI_DATATYPE_INITIALIZER_UINT32_T            OPAL_DATATYPE_INITIALIZER_UINT4
#define OMPI_DATATYPE_INITIALIZER_INT64_T             OPAL_DATATYPE_INITIALIZER_INT8
#define OMPI_DATATYPE_INITIALIZER_UINT64_T            OPAL_DATATYPE_INITIALIZER_UINT8

#if SIZEOF_CHAR == 1
#define OMPI_DATATYPE_INITIALIZER_CHAR                OPAL_DATATYPE_INITIALIZER_INT1
#define OMPI_DATATYPE_INITIALIZER_UNSIGNED_CHAR       OPAL_DATATYPE_INITIALIZER_UINT1
#define OMPI_DATATYPE_INITIALIZER_SIGNED_CHAR         OPAL_DATATYPE_INITIALIZER_INT1
#define OMPI_DATATYPE_INITIALIZER_BYTE                OPAL_DATATYPE_INITIALIZER_UINT1
#elif SIZEOF_CHAR == 2
#define OMPI_DATATYPE_INITIALIZER_CHAR                OPAL_DATATYPE_INITIALIZER_INT2
#define OMPI_DATATYPE_INITIALIZER_UNSIGNED_CHAR       OPAL_DATATYPE_INITIALIZER_UINT2
#define OMPI_DATATYPE_INITIALIZER_SIGNED_CHAR         OPAL_DATATYPE_INITIALIZER_INT2
#define OMPI_DATATYPE_INITIALIZER_BYTE                OPAL_DATATYPE_INITIALIZER_UINT2
#elif SIZEOF_CHAR == 4
#define OMPI_DATATYPE_INITIALIZER_CHAR                OPAL_DATATYPE_INITIALIZER_INT4
#define OMPI_DATATYPE_INITIALIZER_UNSIGNED_CHAR       OPAL_DATATYPE_INITIALIZER_UINT4
#define OMPI_DATATYPE_INITIALIZER_SIGNED_CHAR         OPAL_DATATYPE_INITIALIZER_INT4
#define OMPI_DATATYPE_INITIALIZER_BYTE                OPAL_DATATYPE_INITIALIZER_UINT4
#elif SIZEOF_CHAR == 8
#define OMPI_DATATYPE_INITIALIZER_CHAR                OPAL_DATATYPE_INITIALIZER_INT8
#define OMPI_DATATYPE_INITIALIZER_UNSIGNED_CHAR       OPAL_DATATYPE_INITIALIZER_UINT8
#define OMPI_DATATYPE_INITIALIZER_SIGNED_CHAR         OPAL_DATATYPE_INITIALIZER_INT8
#define OMPI_DATATYPE_INITIALIZER_BYTE                OPAL_DATATYPE_INITIALIZER_UINT8
#endif

#if SIZEOF_SHORT == 2
#define OMPI_DATATYPE_INITIALIZER_SHORT               OPAL_DATATYPE_INITIALIZER_INT2
#define OMPI_DATATYPE_INITIALIZER_UNSIGNED_SHORT      OPAL_DATATYPE_INITIALIZER_UINT2
#elif SIZEOF_SHORT == 4
#define OMPI_DATATYPE_INITIALIZER_SHORT               OPAL_DATATYPE_INITIALIZER_INT4
#define OMPI_DATATYPE_INITIALIZER_UNSIGNED_SHORT      OPAL_DATATYPE_INITIALIZER_UINT4
#elif SIZEOF_SHORT == 8
#define OMPI_DATATYPE_INITIALIZER_SHORT               OPAL_DATATYPE_INITIALIZER_INT8
#define OMPI_DATATYPE_INITIALIZER_UNSIGNED_SHORT      OPAL_DATATYPE_INITIALIZER_UINT8
#endif

#if SIZEOF_INT == 2
#define OMPI_DATATYPE_INITIALIZER_INT                 OPAL_DATATYPE_INITIALIZER_INT2
#define OMPI_DATATYPE_INITIALIZER_UNSIGNED            OPAL_DATATYPE_INITIALIZER_UINT2
#elif SIZEOF_INT == 4
#define OMPI_DATATYPE_INITIALIZER_INT                 OPAL_DATATYPE_INITIALIZER_INT4
#define OMPI_DATATYPE_INITIALIZER_UNSIGNED            OPAL_DATATYPE_INITIALIZER_UINT4
#elif SIZEOF_INT == 8
#define OMPI_DATATYPE_INITIALIZER_INT                 OPAL_DATATYPE_INITIALIZER_INT8
#define OMPI_DATATYPE_INITIALIZER_UNSIGNED            OPAL_DATATYPE_INITIALIZER_UINT8
#endif

#if SIZEOF_LONG == 4
#define OMPI_DATATYPE_INITIALIZER_LONG                OPAL_DATATYPE_INITIALIZER_INT4
#define OMPI_DATATYPE_INITIALIZER_UNSIGNED_LONG       OPAL_DATATYPE_INITIALIZER_UINT4
#elif SIZEOF_LONG == 8
#define OMPI_DATATYPE_INITIALIZER_LONG                OPAL_DATATYPE_INITIALIZER_INT8
#define OMPI_DATATYPE_INITIALIZER_UNSIGNED_LONG       OPAL_DATATYPE_INITIALIZER_UINT8
#elif SIZEOF_LONG == 16
#define OMPI_DATATYPE_INITIALIZER_LONG                OPAL_DATATYPE_INITIALIZER_INT16
#define OMPI_DATATYPE_INITIALIZER_UNSIGNED_LONG       OPAL_DATATYPE_INITIALIZER_UINT16
#endif


#if HAVE_LONG_LONG
#if SIZEOF_LONG_LONG == 4
#define OMPI_DATATYPE_INITIALIZER_LONG_LONG_INT       OPAL_DATATYPE_INITIALIZER_INT4
#define OMPI_DATATYPE_INITIALIZER_UNSIGNED_LONG_LONG  OPAL_DATATYPE_INITIALIZER_UINT4
#elif SIZEOF_LONG_LONG == 8
#define OMPI_DATATYPE_INITIALIZER_LONG_LONG_INT       OPAL_DATATYPE_INITIALIZER_INT8
#define OMPI_DATATYPE_INITIALIZER_UNSIGNED_LONG_LONG  OPAL_DATATYPE_INITIALIZER_UINT8
#elif SIZEOF_LONG_LONG == 16
#define OMPI_DATATYPE_INITIALIZER_LONG_LONG_INT       OPAL_DATATYPE_INITIALIZER_INT16
#define OMPI_DATATYPE_INITIALIZER_UNSIGNED_LONG_LONG  OPAL_DATATYPE_INITIALIZER_UINT16
#endif

#else /* HAVE_LONG_LONG */

#define OMPI_DATATYPE_INITIALIZER_LONG_LONG_INT       OPAL_DATATYPE_INIT_UNAVAILABLE (LONG_LONG_INT, OMPI_DATATYPE_FLAG_DATA_C)
#define OMPI_DATATYPE_INITIALIZER_UNSIGNED_LONG_LONG  OPAL_DATATYPE_INIT_UNAVAILABLE (UNSIGNED_LONG_LONG, OMPI_DATATYPE_FLAG_DATA_C)

#endif /* HAVE_LONG_LONG */

#if SIZEOF_FLOAT == 2
#define OMPI_DATATYPE_INITIALIZER_FLOAT               OPAL_DATATYPE_INITIALIZER_FLOAT2
#elif SIZEOF_FLOAT == 4
#define OMPI_DATATYPE_INITIALIZER_FLOAT               OPAL_DATATYPE_INITIALIZER_FLOAT4
#elif SIZEOF_FLOAT == 8
#define OMPI_DATATYPE_INITIALIZER_FLOAT               OPAL_DATATYPE_INITIALIZER_FLOAT8
#endif


#if SIZEOF_DOUBLE == 4
#define OMPI_DATATYPE_INITIALIZER_DOUBLE              OPAL_DATATYPE_INITIALIZER_FLOAT4
#elif SIZEOF_DOUBLE == 8
#define OMPI_DATATYPE_INITIALIZER_DOUBLE              OPAL_DATATYPE_INITIALIZER_FLOAT8
#elif SIZEOF_DOUBLE == 12
#define OMPI_DATATYPE_INITIALIZER_DOUBLE              OPAL_DATATYPE_INITIALIZER_FLOAT12
#elif SIZEOF_DOUBLE == 16
#define OMPI_DATATYPE_INITIALIZER_DOUBLE              OPAL_DATATYPE_INITIALIZER_FLOAT16
#endif


#if HAVE_LONG_DOUBLE
#if SIZEOF_LONG_DOUBLE == 4
#define OMPI_DATATYPE_INITIALIZER_LONG_DOUBLE         OPAL_DATATYPE_INITIALIZER_FLOAT4
#elif SIZEOF_LONG_DOUBLE == 8
#define OMPI_DATATYPE_INITIALIZER_LONG_DOUBLE         OPAL_DATATYPE_INITIALIZER_FLOAT8
#elif SIZEOF_LONG_DOUBLE == 12
#define OMPI_DATATYPE_INITIALIZER_LONG_DOUBLE         OPAL_DATATYPE_INITIALIZER_FLOAT12
#elif SIZEOF_LONG_DOUBLE == 16
#define OMPI_DATATYPE_INITIALIZER_LONG_DOUBLE         OPAL_DATATYPE_INITIALIZER_FLOAT16
#endif

#else /* HAVE_LONG_DOUBLE */

#define OMPI_DATATYPE_INITIALIZER_LONG_DOUBLE         OMPI_DATATYPE_INIT_UNAVAILABLE(LONG_DOUBLE, OMPI_DATATYPE_FLAG_DATA_C)

#endif  /* HAVE_LONG_DOUBLE */

#define OMPI_DATATYPE_INITIALIZER_PACKED              OPAL_DATATYPE_INITIALIZER_UINT1

#define OMPI_DATATYPE_INITIALIZER_BOOL                OPAL_DATATYPE_INITIALIZER_BOOL

#define OMPI_DATATYPE_INITIALIZER_WCHAR               OPAL_DATATYPE_INITIALIZER_WCHAR

#define OMPI_DATATYPE_INITIALIZER_C_FLOAT_COMPLEX       OPAL_DATATYPE_INITIALIZER_FLOAT_COMPLEX
#define OMPI_DATATYPE_INITIALIZER_C_DOUBLE_COMPLEX      OPAL_DATATYPE_INITIALIZER_DOUBLE_COMPLEX
#define OMPI_DATATYPE_INITIALIZER_C_LONG_DOUBLE_COMPLEX OPAL_DATATYPE_INITIALIZER_LONG_DOUBLE_COMPLEX

/*
 * Following are the structured types, that cannot be represented
 * by one single OPAL basic type
 */
#define OMPI_DATATYPE_FIRST_TYPE                      OPAL_DATATYPE_MAX_PREDEFINED

/*
 * Derived datatypes supposely contiguous
 */
#define OMPI_DATATYPE_2INT                            (OMPI_DATATYPE_FIRST_TYPE+6)
#define OMPI_DATATYPE_2INTEGER                        (OMPI_DATATYPE_FIRST_TYPE+7)
#define OMPI_DATATYPE_2REAL                           (OMPI_DATATYPE_FIRST_TYPE+8)
#define OMPI_DATATYPE_2DBLPREC                        (OMPI_DATATYPE_FIRST_TYPE+9)
#define OMPI_DATATYPE_2COMPLEX                        (OMPI_DATATYPE_FIRST_TYPE+10)
#define OMPI_DATATYPE_2DOUBLE_COMPLEX                 (OMPI_DATATYPE_FIRST_TYPE+11)
/*
 * Derived datatypes which will definitively be non contiguous on some architectures.
 */
#define OMPI_DATATYPE_FLOAT_INT                       (OMPI_DATATYPE_FIRST_TYPE+12)
#define OMPI_DATATYPE_DOUBLE_INT                      (OMPI_DATATYPE_FIRST_TYPE+13)
#define OMPI_DATATYPE_LONG_DOUBLE_INT                 (OMPI_DATATYPE_FIRST_TYPE+14)
#define OMPI_DATATYPE_SHORT_INT                       (OMPI_DATATYPE_FIRST_TYPE+15)
#define OMPI_DATATYPE_LONG_INT                        (OMPI_DATATYPE_FIRST_TYPE+16)

/* Used locally as a nice marker */
#define OMPI_DATATYPE_UNAVAILABLE                     (OMPI_DATATYPE_FIRST_TYPE+17)

/* If the number of basic datatype should change update OMPI_DATATYPE_MAX_PREDEFINED in ompi_datatype.h */
#if OMPI_DATATYPE_MAX_PREDEFINED <= OMPI_DATATYPE_UNAVAILABLE
#error OMPI_DATATYPE_MAX_PREDEFINED should be updated to the next value after the OMPI_DATATYPE_UNAVAILABLE define
#endif  /* safe check for max predefined datatypes. */

#endif /* OMPI_DATATYPE_INTERNAL_H */
