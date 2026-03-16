/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2006 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2010 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2007 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2006-2014 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2013      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif

#include "ompi/mca/op/op.h"


/*
 * Since all the functions in this file are essentially identical, we
 * use a macro to substitute in names and types.  The core operation
 * in all functions that use this macro is the same.
 *
 * This macro is for (out op in).
 */
#define OP_FUNC(name, type_name, type, op) \
  static void ompi_op_base_2buff_##name##_##type_name(void *in, void *out, int *count, \
                                                      struct ompi_datatype_t **dtype, \
                                                      struct ompi_op_base_module_1_0_0_t *module) \
  {                                                                      \
      int i;                                                             \
      type *a = (type *) in;                                             \
      type *b = (type *) out;                                            \
      for (i = 0; i < *count; ++i) {                                     \
          *(b++) op *(a++);                                              \
      }                                                                  \
  }

/*
 * Since all the functions in this file are essentially identical, we
 * use a macro to substitute in names and types.  The core operation
 * in all functions that use this macro is the same.
 *
 * This macro is for (out = op(out, in))
 */
#define FUNC_FUNC(name, type_name, type) \
  static void ompi_op_base_2buff_##name##_##type_name(void *in, void *out, int *count, \
                                                struct ompi_datatype_t **dtype, \
                                                struct ompi_op_base_module_1_0_0_t *module) \
  {                                                                      \
      int i;                                                             \
      type *a = (type *) in;                                             \
      type *b = (type *) out;                                            \
      for (i = 0; i < *count; ++i) {                                     \
          *(b) = current_func(*(b), *(a));                               \
          ++b;                                                           \
          ++a;                                                           \
      }                                                                  \
  }

/*
 * Since all the functions in this file are essentially identical, we
 * use a macro to substitute in names and types.  The core operation
 * in all functions that use this macro is the same.
 *
 * This macro is for minloc and maxloc
 */
#define LOC_STRUCT(type_name, type1, type2) \
  typedef struct { \
      type1 v; \
      type2 k; \
  } ompi_op_predefined_##type_name##_t;

#define LOC_FUNC(name, type_name, op) \
    static void ompi_op_base_2buff_##name##_##type_name(void *in, void *out, int *count, \
                                                        struct ompi_datatype_t **dtype, \
                                                        struct ompi_op_base_module_1_0_0_t *module) \
    {                                                                   \
        int i;                                                          \
        ompi_op_predefined_##type_name##_t *a = (ompi_op_predefined_##type_name##_t*) in; \
        ompi_op_predefined_##type_name##_t *b = (ompi_op_predefined_##type_name##_t*) out; \
        for (i = 0; i < *count; ++i, ++a, ++b) {                        \
            if (a->v op b->v) {                                         \
                b->v = a->v;                                            \
                b->k = a->k;                                            \
            } else if (a->v == b->v) {                                  \
                b->k = (b->k < a->k ? b->k : a->k);                     \
            }                                                           \
        }                                                               \
    }

/*************************************************************************
 * Max
 *************************************************************************/

#undef current_func
#define current_func(a, b) ((a) > (b) ? (a) : (b))
/* C integer */
FUNC_FUNC(max,   int8_t,   int8_t)
FUNC_FUNC(max,  uint8_t,  uint8_t)
FUNC_FUNC(max,  int16_t,  int16_t)
FUNC_FUNC(max, uint16_t, uint16_t)
FUNC_FUNC(max,  int32_t,  int32_t)
FUNC_FUNC(max, uint32_t, uint32_t)
FUNC_FUNC(max,  int64_t,  int64_t)
FUNC_FUNC(max, uint64_t, uint64_t)
/* Fortran integer */
#if OMPI_HAVE_FORTRAN_INTEGER
FUNC_FUNC(max, fortran_integer, ompi_fortran_integer_t)
#endif
#if OMPI_HAVE_FORTRAN_INTEGER1
FUNC_FUNC(max, fortran_integer1, ompi_fortran_integer1_t)
#endif
#if OMPI_HAVE_FORTRAN_INTEGER2
FUNC_FUNC(max, fortran_integer2, ompi_fortran_integer2_t)
#endif
#if OMPI_HAVE_FORTRAN_INTEGER4
FUNC_FUNC(max, fortran_integer4, ompi_fortran_integer4_t)
#endif
#if OMPI_HAVE_FORTRAN_INTEGER8
FUNC_FUNC(max, fortran_integer8, ompi_fortran_integer8_t)
#endif
#if OMPI_HAVE_FORTRAN_INTEGER16
FUNC_FUNC(max, fortran_integer16, ompi_fortran_integer16_t)
#endif
/* Floating point */
FUNC_FUNC(max, float, float)
FUNC_FUNC(max, double, double)
#if HAVE_LONG_DOUBLE
FUNC_FUNC(max, long_double, long double)
#endif
#if OMPI_HAVE_FORTRAN_REAL
FUNC_FUNC(max, fortran_real, ompi_fortran_real_t)
#endif
#if OMPI_HAVE_FORTRAN_DOUBLE_PRECISION
FUNC_FUNC(max, fortran_double_precision, ompi_fortran_double_precision_t)
#endif
#if OMPI_HAVE_FORTRAN_REAL2
FUNC_FUNC(max, fortran_real2, ompi_fortran_real2_t)
#endif
#if OMPI_HAVE_FORTRAN_REAL4
FUNC_FUNC(max, fortran_real4, ompi_fortran_real4_t)
#endif
#if OMPI_HAVE_FORTRAN_REAL8
FUNC_FUNC(max, fortran_real8, ompi_fortran_real8_t)
#endif
#if OMPI_HAVE_FORTRAN_REAL16 && OMPI_REAL16_MATCHES_C
FUNC_FUNC(max, fortran_real16, ompi_fortran_real16_t)
#endif


/*************************************************************************
 * Min
 *************************************************************************/

#undef current_func
#define current_func(a, b) ((a) < (b) ? (a) : (b))
/* C integer */
FUNC_FUNC(min,   int8_t,   int8_t)
FUNC_FUNC(min,  uint8_t,  uint8_t)
FUNC_FUNC(min,  int16_t,  int16_t)
FUNC_FUNC(min, uint16_t, uint16_t)
FUNC_FUNC(min,  int32_t,  int32_t)
FUNC_FUNC(min, uint32_t, uint32_t)
FUNC_FUNC(min,  int64_t,  int64_t)
FUNC_FUNC(min, uint64_t, uint64_t)
/* Fortran integer */
#if OMPI_HAVE_FORTRAN_INTEGER
FUNC_FUNC(min, fortran_integer, ompi_fortran_integer_t)
#endif
#if OMPI_HAVE_FORTRAN_INTEGER1
FUNC_FUNC(min, fortran_integer1, ompi_fortran_integer1_t)
#endif
#if OMPI_HAVE_FORTRAN_INTEGER2
FUNC_FUNC(min, fortran_integer2, ompi_fortran_integer2_t)
#endif
#if OMPI_HAVE_FORTRAN_INTEGER4
FUNC_FUNC(min, fortran_integer4, ompi_fortran_integer4_t)
#endif
#if OMPI_HAVE_FORTRAN_INTEGER8
FUNC_FUNC(min, fortran_integer8, ompi_fortran_integer8_t)
#endif
#if OMPI_HAVE_FORTRAN_INTEGER16
FUNC_FUNC(min, fortran_integer16, ompi_fortran_integer16_t)
#endif
/* Floating point */
FUNC_FUNC(min, float, float)
FUNC_FUNC(min, double, double)
#if HAVE_LONG_DOUBLE
FUNC_FUNC(min, long_double, long double)
#endif
#if OMPI_HAVE_FORTRAN_REAL
FUNC_FUNC(min, fortran_real, ompi_fortran_real_t)
#endif
#if OMPI_HAVE_FORTRAN_DOUBLE_PRECISION
FUNC_FUNC(min, fortran_double_precision, ompi_fortran_double_precision_t)
#endif
#if OMPI_HAVE_FORTRAN_REAL2
FUNC_FUNC(min, fortran_real2, ompi_fortran_real2_t)
#endif
#if OMPI_HAVE_FORTRAN_REAL4
FUNC_FUNC(min, fortran_real4, ompi_fortran_real4_t)
#endif
#if OMPI_HAVE_FORTRAN_REAL8
FUNC_FUNC(min, fortran_real8, ompi_fortran_real8_t)
#endif
#if OMPI_HAVE_FORTRAN_REAL16 && OMPI_REAL16_MATCHES_C
FUNC_FUNC(min, fortran_real16, ompi_fortran_real16_t)
#endif

/*************************************************************************
 * Sum
 *************************************************************************/

/* C integer */
OP_FUNC(sum,   int8_t,   int8_t, +=)
OP_FUNC(sum,  uint8_t,  uint8_t, +=)
OP_FUNC(sum,  int16_t,  int16_t, +=)
OP_FUNC(sum, uint16_t, uint16_t, +=)
OP_FUNC(sum,  int32_t,  int32_t, +=)
OP_FUNC(sum, uint32_t, uint32_t, +=)
OP_FUNC(sum,  int64_t,  int64_t, +=)
OP_FUNC(sum, uint64_t, uint64_t, +=)
/* Fortran integer */
#if OMPI_HAVE_FORTRAN_INTEGER
OP_FUNC(sum, fortran_integer, ompi_fortran_integer_t, +=)
#endif
#if OMPI_HAVE_FORTRAN_INTEGER1
OP_FUNC(sum, fortran_integer1, ompi_fortran_integer1_t, +=)
#endif
#if OMPI_HAVE_FORTRAN_INTEGER2
OP_FUNC(sum, fortran_integer2, ompi_fortran_integer2_t, +=)
#endif
#if OMPI_HAVE_FORTRAN_INTEGER4
OP_FUNC(sum, fortran_integer4, ompi_fortran_integer4_t, +=)
#endif
#if OMPI_HAVE_FORTRAN_INTEGER8
OP_FUNC(sum, fortran_integer8, ompi_fortran_integer8_t, +=)
#endif
#if OMPI_HAVE_FORTRAN_INTEGER16
OP_FUNC(sum, fortran_integer16, ompi_fortran_integer16_t, +=)
#endif
/* Floating point */
OP_FUNC(sum, float, float, +=)
OP_FUNC(sum, double, double, +=)
#if HAVE_LONG_DOUBLE
OP_FUNC(sum, long_double, long double, +=)
#endif
#if OMPI_HAVE_FORTRAN_REAL
OP_FUNC(sum, fortran_real, ompi_fortran_real_t, +=)
#endif
#if OMPI_HAVE_FORTRAN_DOUBLE_PRECISION
OP_FUNC(sum, fortran_double_precision, ompi_fortran_double_precision_t, +=)
#endif
#if OMPI_HAVE_FORTRAN_REAL2
OP_FUNC(sum, fortran_real2, ompi_fortran_real2_t, +=)
#endif
#if OMPI_HAVE_FORTRAN_REAL4
OP_FUNC(sum, fortran_real4, ompi_fortran_real4_t, +=)
#endif
#if OMPI_HAVE_FORTRAN_REAL8
OP_FUNC(sum, fortran_real8, ompi_fortran_real8_t, +=)
#endif
#if OMPI_HAVE_FORTRAN_REAL16 && OMPI_REAL16_MATCHES_C
OP_FUNC(sum, fortran_real16, ompi_fortran_real16_t, +=)
#endif
/* Complex */
#if HAVE_FLOAT__COMPLEX
OP_FUNC(sum, c_float_complex, float _Complex, +=)
#endif
#if HAVE_DOUBLE__COMPLEX
OP_FUNC(sum, c_double_complex, double _Complex, +=)
#endif
#if HAVE_LONG_DOUBLE__COMPLEX
OP_FUNC(sum, c_long_double_complex, long double _Complex, +=)
#endif

/*************************************************************************
 * Product
 *************************************************************************/

/* C integer */
OP_FUNC(prod,   int8_t,   int8_t, *=)
OP_FUNC(prod,  uint8_t,  uint8_t, *=)
OP_FUNC(prod,  int16_t,  int16_t, *=)
OP_FUNC(prod, uint16_t, uint16_t, *=)
OP_FUNC(prod,  int32_t,  int32_t, *=)
OP_FUNC(prod, uint32_t, uint32_t, *=)
OP_FUNC(prod,  int64_t,  int64_t, *=)
OP_FUNC(prod, uint64_t, uint64_t, *=)
/* Fortran integer */
#if OMPI_HAVE_FORTRAN_INTEGER
OP_FUNC(prod, fortran_integer, ompi_fortran_integer_t, *=)
#endif
#if OMPI_HAVE_FORTRAN_INTEGER1
OP_FUNC(prod, fortran_integer1, ompi_fortran_integer1_t, *=)
#endif
#if OMPI_HAVE_FORTRAN_INTEGER2
OP_FUNC(prod, fortran_integer2, ompi_fortran_integer2_t, *=)
#endif
#if OMPI_HAVE_FORTRAN_INTEGER4
OP_FUNC(prod, fortran_integer4, ompi_fortran_integer4_t, *=)
#endif
#if OMPI_HAVE_FORTRAN_INTEGER8
OP_FUNC(prod, fortran_integer8, ompi_fortran_integer8_t, *=)
#endif
#if OMPI_HAVE_FORTRAN_INTEGER16
OP_FUNC(prod, fortran_integer16, ompi_fortran_integer16_t, *=)
#endif
/* Floating point */
OP_FUNC(prod, float, float, *=)
OP_FUNC(prod, double, double, *=)
#if HAVE_LONG_DOUBLE
OP_FUNC(prod, long_double, long double, *=)
#endif
#if OMPI_HAVE_FORTRAN_REAL
OP_FUNC(prod, fortran_real, ompi_fortran_real_t, *=)
#endif
#if OMPI_HAVE_FORTRAN_DOUBLE_PRECISION
OP_FUNC(prod, fortran_double_precision, ompi_fortran_double_precision_t, *=)
#endif
#if OMPI_HAVE_FORTRAN_REAL2
OP_FUNC(prod, fortran_real2, ompi_fortran_real2_t, *=)
#endif
#if OMPI_HAVE_FORTRAN_REAL4
OP_FUNC(prod, fortran_real4, ompi_fortran_real4_t, *=)
#endif
#if OMPI_HAVE_FORTRAN_REAL8
OP_FUNC(prod, fortran_real8, ompi_fortran_real8_t, *=)
#endif
#if OMPI_HAVE_FORTRAN_REAL16 && OMPI_REAL16_MATCHES_C
OP_FUNC(prod, fortran_real16, ompi_fortran_real16_t, *=)
#endif
/* Complex */
#if HAVE_FLOAT__COMPLEX
OP_FUNC(prod, c_float_complex, float _Complex, *=)
#endif
#if HAVE_DOUBLE__COMPLEX
OP_FUNC(prod, c_double_complex, double _Complex, *=)
#endif
#if HAVE_LONG_DOUBLE__COMPLEX
OP_FUNC(prod, c_long_double_complex, long double _Complex, *=)
#endif

/*************************************************************************
 * Logical AND
 *************************************************************************/

#undef current_func
#define current_func(a, b) ((a) && (b))
/* C integer */
FUNC_FUNC(land,   int8_t,   int8_t)
FUNC_FUNC(land,  uint8_t,  uint8_t)
FUNC_FUNC(land,  int16_t,  int16_t)
FUNC_FUNC(land, uint16_t, uint16_t)
FUNC_FUNC(land,  int32_t,  int32_t)
FUNC_FUNC(land, uint32_t, uint32_t)
FUNC_FUNC(land,  int64_t,  int64_t)
FUNC_FUNC(land, uint64_t, uint64_t)
/* Logical */
#if OMPI_HAVE_FORTRAN_LOGICAL
FUNC_FUNC(land, fortran_logical, ompi_fortran_logical_t)
#endif
/* C++ bool */
FUNC_FUNC(land, bool, bool)

/*************************************************************************
 * Logical OR
 *************************************************************************/

#undef current_func
#define current_func(a, b) ((a) || (b))
/* C integer */
FUNC_FUNC(lor,   int8_t,   int8_t)
FUNC_FUNC(lor,  uint8_t,  uint8_t)
FUNC_FUNC(lor,  int16_t,  int16_t)
FUNC_FUNC(lor, uint16_t, uint16_t)
FUNC_FUNC(lor,  int32_t,  int32_t)
FUNC_FUNC(lor, uint32_t, uint32_t)
FUNC_FUNC(lor,  int64_t,  int64_t)
FUNC_FUNC(lor, uint64_t, uint64_t)
/* Logical */
#if OMPI_HAVE_FORTRAN_LOGICAL
FUNC_FUNC(lor, fortran_logical, ompi_fortran_logical_t)
#endif
/* C++ bool */
FUNC_FUNC(lor, bool, bool)

/*************************************************************************
 * Logical XOR
 *************************************************************************/

#undef current_func
#define current_func(a, b) ((a ? 1 : 0) ^ (b ? 1: 0))
/* C integer */
FUNC_FUNC(lxor,   int8_t,   int8_t)
FUNC_FUNC(lxor,  uint8_t,  uint8_t)
FUNC_FUNC(lxor,  int16_t,  int16_t)
FUNC_FUNC(lxor, uint16_t, uint16_t)
FUNC_FUNC(lxor,  int32_t,  int32_t)
FUNC_FUNC(lxor, uint32_t, uint32_t)
FUNC_FUNC(lxor,  int64_t,  int64_t)
FUNC_FUNC(lxor, uint64_t, uint64_t)
/* Logical */
#if OMPI_HAVE_FORTRAN_LOGICAL
FUNC_FUNC(lxor, fortran_logical, ompi_fortran_logical_t)
#endif
/* C++ bool */
FUNC_FUNC(lxor, bool, bool)

/*************************************************************************
 * Bitwise AND
 *************************************************************************/

#undef current_func
#define current_func(a, b) ((a) & (b))
/* C integer */
FUNC_FUNC(band,   int8_t,   int8_t)
FUNC_FUNC(band,  uint8_t,  uint8_t)
FUNC_FUNC(band,  int16_t,  int16_t)
FUNC_FUNC(band, uint16_t, uint16_t)
FUNC_FUNC(band,  int32_t,  int32_t)
FUNC_FUNC(band, uint32_t, uint32_t)
FUNC_FUNC(band,  int64_t,  int64_t)
FUNC_FUNC(band, uint64_t, uint64_t)
/* Fortran integer */
#if OMPI_HAVE_FORTRAN_INTEGER
FUNC_FUNC(band, fortran_integer, ompi_fortran_integer_t)
#endif
#if OMPI_HAVE_FORTRAN_INTEGER1
FUNC_FUNC(band, fortran_integer1, ompi_fortran_integer1_t)
#endif
#if OMPI_HAVE_FORTRAN_INTEGER2
FUNC_FUNC(band, fortran_integer2, ompi_fortran_integer2_t)
#endif
#if OMPI_HAVE_FORTRAN_INTEGER4
FUNC_FUNC(band, fortran_integer4, ompi_fortran_integer4_t)
#endif
#if OMPI_HAVE_FORTRAN_INTEGER8
FUNC_FUNC(band, fortran_integer8, ompi_fortran_integer8_t)
#endif
#if OMPI_HAVE_FORTRAN_INTEGER16
FUNC_FUNC(band, fortran_integer16, ompi_fortran_integer16_t)
#endif
/* Byte */
FUNC_FUNC(band, byte, char)

/*************************************************************************
 * Bitwise OR
 *************************************************************************/

#undef current_func
#define current_func(a, b) ((a) | (b))
/* C integer */
FUNC_FUNC(bor,   int8_t,   int8_t)
FUNC_FUNC(bor,  uint8_t,  uint8_t)
FUNC_FUNC(bor,  int16_t,  int16_t)
FUNC_FUNC(bor, uint16_t, uint16_t)
FUNC_FUNC(bor,  int32_t,  int32_t)
FUNC_FUNC(bor, uint32_t, uint32_t)
FUNC_FUNC(bor,  int64_t,  int64_t)
FUNC_FUNC(bor, uint64_t, uint64_t)
/* Fortran integer */
#if OMPI_HAVE_FORTRAN_INTEGER
FUNC_FUNC(bor, fortran_integer, ompi_fortran_integer_t)
#endif
#if OMPI_HAVE_FORTRAN_INTEGER1
FUNC_FUNC(bor, fortran_integer1, ompi_fortran_integer1_t)
#endif
#if OMPI_HAVE_FORTRAN_INTEGER2
FUNC_FUNC(bor, fortran_integer2, ompi_fortran_integer2_t)
#endif
#if OMPI_HAVE_FORTRAN_INTEGER4
FUNC_FUNC(bor, fortran_integer4, ompi_fortran_integer4_t)
#endif
#if OMPI_HAVE_FORTRAN_INTEGER8
FUNC_FUNC(bor, fortran_integer8, ompi_fortran_integer8_t)
#endif
#if OMPI_HAVE_FORTRAN_INTEGER16
FUNC_FUNC(bor, fortran_integer16, ompi_fortran_integer16_t)
#endif
/* Byte */
FUNC_FUNC(bor, byte, char)

/*************************************************************************
 * Bitwise XOR
 *************************************************************************/

#undef current_func
#define current_func(a, b) ((a) ^ (b))
/* C integer */
FUNC_FUNC(bxor,   int8_t,   int8_t)
FUNC_FUNC(bxor,  uint8_t,  uint8_t)
FUNC_FUNC(bxor,  int16_t,  int16_t)
FUNC_FUNC(bxor, uint16_t, uint16_t)
FUNC_FUNC(bxor,  int32_t,  int32_t)
FUNC_FUNC(bxor, uint32_t, uint32_t)
FUNC_FUNC(bxor,  int64_t,  int64_t)
FUNC_FUNC(bxor, uint64_t, uint64_t)
/* Fortran integer */
#if OMPI_HAVE_FORTRAN_INTEGER
FUNC_FUNC(bxor, fortran_integer, ompi_fortran_integer_t)
#endif
#if OMPI_HAVE_FORTRAN_INTEGER1
FUNC_FUNC(bxor, fortran_integer1, ompi_fortran_integer1_t)
#endif
#if OMPI_HAVE_FORTRAN_INTEGER2
FUNC_FUNC(bxor, fortran_integer2, ompi_fortran_integer2_t)
#endif
#if OMPI_HAVE_FORTRAN_INTEGER4
FUNC_FUNC(bxor, fortran_integer4, ompi_fortran_integer4_t)
#endif
#if OMPI_HAVE_FORTRAN_INTEGER8
FUNC_FUNC(bxor, fortran_integer8, ompi_fortran_integer8_t)
#endif
#if OMPI_HAVE_FORTRAN_INTEGER16
FUNC_FUNC(bxor, fortran_integer16, ompi_fortran_integer16_t)
#endif
/* Byte */
FUNC_FUNC(bxor, byte, char)

/*************************************************************************
 * Min and max location "pair" datatypes
 *************************************************************************/

#if OMPI_HAVE_FORTRAN_REAL
LOC_STRUCT(2real, ompi_fortran_real_t, ompi_fortran_real_t)
#endif
#if OMPI_HAVE_FORTRAN_DOUBLE_PRECISION
LOC_STRUCT(2double_precision, ompi_fortran_double_precision_t, ompi_fortran_double_precision_t)
#endif
#if OMPI_HAVE_FORTRAN_INTEGER
LOC_STRUCT(2integer, ompi_fortran_integer_t, ompi_fortran_integer_t)
#endif
LOC_STRUCT(float_int, float, int)
LOC_STRUCT(double_int, double, int)
LOC_STRUCT(long_int, long, int)
LOC_STRUCT(2int, int, int)
LOC_STRUCT(short_int, short, int)
#if HAVE_LONG_DOUBLE
LOC_STRUCT(long_double_int, long double, int)
#endif

/*************************************************************************
 * Max location
 *************************************************************************/

#if OMPI_HAVE_FORTRAN_REAL
LOC_FUNC(maxloc, 2real, >)
#endif
#if OMPI_HAVE_FORTRAN_DOUBLE_PRECISION
LOC_FUNC(maxloc, 2double_precision, >)
#endif
#if OMPI_HAVE_FORTRAN_INTEGER
LOC_FUNC(maxloc, 2integer, >)
#endif
LOC_FUNC(maxloc, float_int, >)
LOC_FUNC(maxloc, double_int, >)
LOC_FUNC(maxloc, long_int, >)
LOC_FUNC(maxloc, 2int, >)
LOC_FUNC(maxloc, short_int, >)
#if HAVE_LONG_DOUBLE
LOC_FUNC(maxloc, long_double_int, >)
#endif

/*************************************************************************
 * Min location
 *************************************************************************/

#if OMPI_HAVE_FORTRAN_REAL
LOC_FUNC(minloc, 2real, <)
#endif
#if OMPI_HAVE_FORTRAN_DOUBLE_PRECISION
LOC_FUNC(minloc, 2double_precision, <)
#endif
#if OMPI_HAVE_FORTRAN_INTEGER
LOC_FUNC(minloc, 2integer, <)
#endif
LOC_FUNC(minloc, float_int, <)
LOC_FUNC(minloc, double_int, <)
LOC_FUNC(minloc, long_int, <)
LOC_FUNC(minloc, 2int, <)
LOC_FUNC(minloc, short_int, <)
#if HAVE_LONG_DOUBLE
LOC_FUNC(minloc, long_double_int, <)
#endif


/*
 *  This is a three buffer (2 input and 1 output) version of the reduction
 *    routines, needed for some optimizations.
 */
#define OP_FUNC_3BUF(name, type_name, type, op) \
    static void ompi_op_base_3buff_##name##_##type_name(void * restrict in1,   \
                                                        void * restrict in2, void * restrict out, int *count, \
                                                        struct ompi_datatype_t **dtype, \
                                                        struct ompi_op_base_module_1_0_0_t *module) \
    {                                                                   \
        int i;                                                          \
        type *a1 = (type *) in1;                                        \
        type *a2 = (type *) in2;                                        \
        type *b = (type *) out;                                         \
        for (i = 0; i < *count; ++i) {                                  \
            *(b++) =  *(a1++) op *(a2++);                               \
        }                                                               \
    }

/*
 * Since all the functions in this file are essentially identical, we
 * use a macro to substitute in names and types.  The core operation
 * in all functions that use this macro is the same.
 *
 * This macro is for (out = op(in1, in2))
 */
#define FUNC_FUNC_3BUF(name, type_name, type)                           \
    static void ompi_op_base_3buff_##name##_##type_name(void * restrict in1, \
                                                        void * restrict in2, void * restrict out, int *count, \
                                                        struct ompi_datatype_t **dtype, \
                                                        struct ompi_op_base_module_1_0_0_t *module) \
    {                                                                   \
        int i;                                                          \
        type *a1 = (type *) in1;                                        \
        type *a2 = (type *) in2;                                        \
        type *b = (type *) out;                                         \
        for (i = 0; i < *count; ++i) {                                  \
            *(b) = current_func(*(a1), *(a2));                          \
            ++b;                                                        \
            ++a1;                                                       \
            ++a2;                                                       \
        }                                                               \
    }

/*
 * Since all the functions in this file are essentially identical, we
 * use a macro to substitute in names and types.  The core operation
 * in all functions that use this macro is the same.
 *
 * This macro is for minloc and maxloc
 */
/*
#define LOC_STRUCT(type_name, type1, type2) \
  typedef struct { \
      type1 v; \
      type2 k; \
  } ompi_op_predefined_##type_name##_t;
*/

#define LOC_FUNC_3BUF(name, type_name, op) \
  static void ompi_op_base_3buff_##name##_##type_name(void * restrict in1,      \
                                                      void * restrict in2, void * restrict out, int *count, \
                                                      struct ompi_datatype_t **dtype, \
                                                      struct ompi_op_base_module_1_0_0_t *module) \
  {                                                                     \
      int i;                                                            \
      ompi_op_predefined_##type_name##_t *a1 = (ompi_op_predefined_##type_name##_t*) in1; \
      ompi_op_predefined_##type_name##_t *a2 = (ompi_op_predefined_##type_name##_t*) in2; \
      ompi_op_predefined_##type_name##_t *b = (ompi_op_predefined_##type_name##_t*) out; \
      for (i = 0; i < *count; ++i, ++a1, ++a2, ++b ) {                  \
          if (a1->v op a2->v) {                                         \
              b->v = a1->v;                                             \
              b->k = a1->k;                                             \
          } else if (a1->v == a2->v) {                                  \
              b->v = a1->v;                                             \
              b->k = (a2->k < a1->k ? a2->k : a1->k);                   \
          } else {                                                      \
              b->v = a2->v;                                             \
              b->k = a2->k;                                             \
          }                                                             \
      }                                                                 \
  }

/*************************************************************************
 * Max
 *************************************************************************/

#undef current_func
#define current_func(a, b) ((a) > (b) ? (a) : (b))
/* C integer */
FUNC_FUNC_3BUF(max,   int8_t,   int8_t)
FUNC_FUNC_3BUF(max,  uint8_t,  uint8_t)
FUNC_FUNC_3BUF(max,  int16_t,  int16_t)
FUNC_FUNC_3BUF(max, uint16_t, uint16_t)
FUNC_FUNC_3BUF(max,  int32_t,  int32_t)
FUNC_FUNC_3BUF(max, uint32_t, uint32_t)
FUNC_FUNC_3BUF(max,  int64_t,  int64_t)
FUNC_FUNC_3BUF(max, uint64_t, uint64_t)
/* Fortran integer */
#if OMPI_HAVE_FORTRAN_INTEGER
FUNC_FUNC_3BUF(max, fortran_integer, ompi_fortran_integer_t)
#endif
#if OMPI_HAVE_FORTRAN_INTEGER1
FUNC_FUNC_3BUF(max, fortran_integer1, ompi_fortran_integer1_t)
#endif
#if OMPI_HAVE_FORTRAN_INTEGER2
FUNC_FUNC_3BUF(max, fortran_integer2, ompi_fortran_integer2_t)
#endif
#if OMPI_HAVE_FORTRAN_INTEGER4
FUNC_FUNC_3BUF(max, fortran_integer4, ompi_fortran_integer4_t)
#endif
#if OMPI_HAVE_FORTRAN_INTEGER8
FUNC_FUNC_3BUF(max, fortran_integer8, ompi_fortran_integer8_t)
#endif
#if OMPI_HAVE_FORTRAN_INTEGER16
FUNC_FUNC_3BUF(max, fortran_integer16, ompi_fortran_integer16_t)
#endif
/* Floating point */
FUNC_FUNC_3BUF(max, float, float)
FUNC_FUNC_3BUF(max, double, double)
#if HAVE_LONG_DOUBLE
FUNC_FUNC_3BUF(max, long_double, long double)
#endif
#if OMPI_HAVE_FORTRAN_REAL
FUNC_FUNC_3BUF(max, fortran_real, ompi_fortran_real_t)
#endif
#if OMPI_HAVE_FORTRAN_DOUBLE_PRECISION
FUNC_FUNC_3BUF(max, fortran_double_precision, ompi_fortran_double_precision_t)
#endif
#if OMPI_HAVE_FORTRAN_REAL2
FUNC_FUNC_3BUF(max, fortran_real2, ompi_fortran_real2_t)
#endif
#if OMPI_HAVE_FORTRAN_REAL4
FUNC_FUNC_3BUF(max, fortran_real4, ompi_fortran_real4_t)
#endif
#if OMPI_HAVE_FORTRAN_REAL8
FUNC_FUNC_3BUF(max, fortran_real8, ompi_fortran_real8_t)
#endif
#if OMPI_HAVE_FORTRAN_REAL16 && OMPI_REAL16_MATCHES_C
FUNC_FUNC_3BUF(max, fortran_real16, ompi_fortran_real16_t)
#endif


/*************************************************************************
 * Min
 *************************************************************************/

#undef current_func
#define current_func(a, b) ((a) < (b) ? (a) : (b))
/* C integer */
FUNC_FUNC_3BUF(min,   int8_t,   int8_t)
FUNC_FUNC_3BUF(min,  uint8_t,  uint8_t)
FUNC_FUNC_3BUF(min,  int16_t,  int16_t)
FUNC_FUNC_3BUF(min, uint16_t, uint16_t)
FUNC_FUNC_3BUF(min,  int32_t,  int32_t)
FUNC_FUNC_3BUF(min, uint32_t, uint32_t)
FUNC_FUNC_3BUF(min,  int64_t,  int64_t)
FUNC_FUNC_3BUF(min, uint64_t, uint64_t)
/* Fortran integer */
#if OMPI_HAVE_FORTRAN_INTEGER
FUNC_FUNC_3BUF(min, fortran_integer, ompi_fortran_integer_t)
#endif
#if OMPI_HAVE_FORTRAN_INTEGER1
FUNC_FUNC_3BUF(min, fortran_integer1, ompi_fortran_integer1_t)
#endif
#if OMPI_HAVE_FORTRAN_INTEGER2
FUNC_FUNC_3BUF(min, fortran_integer2, ompi_fortran_integer2_t)
#endif
#if OMPI_HAVE_FORTRAN_INTEGER4
FUNC_FUNC_3BUF(min, fortran_integer4, ompi_fortran_integer4_t)
#endif
#if OMPI_HAVE_FORTRAN_INTEGER8
FUNC_FUNC_3BUF(min, fortran_integer8, ompi_fortran_integer8_t)
#endif
#if OMPI_HAVE_FORTRAN_INTEGER16
FUNC_FUNC_3BUF(min, fortran_integer16, ompi_fortran_integer16_t)
#endif
/* Floating point */
FUNC_FUNC_3BUF(min, float, float)
FUNC_FUNC_3BUF(min, double, double)
#if HAVE_LONG_DOUBLE
FUNC_FUNC_3BUF(min, long_double, long double)
#endif
#if OMPI_HAVE_FORTRAN_REAL
FUNC_FUNC_3BUF(min, fortran_real, ompi_fortran_real_t)
#endif
#if OMPI_HAVE_FORTRAN_DOUBLE_PRECISION
FUNC_FUNC_3BUF(min, fortran_double_precision, ompi_fortran_double_precision_t)
#endif
#if OMPI_HAVE_FORTRAN_REAL2
FUNC_FUNC_3BUF(min, fortran_real2, ompi_fortran_real2_t)
#endif
#if OMPI_HAVE_FORTRAN_REAL4
FUNC_FUNC_3BUF(min, fortran_real4, ompi_fortran_real4_t)
#endif
#if OMPI_HAVE_FORTRAN_REAL8
FUNC_FUNC_3BUF(min, fortran_real8, ompi_fortran_real8_t)
#endif
#if OMPI_HAVE_FORTRAN_REAL16 && OMPI_REAL16_MATCHES_C
FUNC_FUNC_3BUF(min, fortran_real16, ompi_fortran_real16_t)
#endif

/*************************************************************************
 * Sum
 *************************************************************************/

/* C integer */
OP_FUNC_3BUF(sum,   int8_t,   int8_t, +)
OP_FUNC_3BUF(sum,  uint8_t,  uint8_t, +)
OP_FUNC_3BUF(sum,  int16_t,  int16_t, +)
OP_FUNC_3BUF(sum, uint16_t, uint16_t, +)
OP_FUNC_3BUF(sum,  int32_t,  int32_t, +)
OP_FUNC_3BUF(sum, uint32_t, uint32_t, +)
OP_FUNC_3BUF(sum,  int64_t,  int64_t, +)
OP_FUNC_3BUF(sum, uint64_t, uint64_t, +)
/* Fortran integer */
#if OMPI_HAVE_FORTRAN_INTEGER
OP_FUNC_3BUF(sum, fortran_integer, ompi_fortran_integer_t, +)
#endif
#if OMPI_HAVE_FORTRAN_INTEGER1
OP_FUNC_3BUF(sum, fortran_integer1, ompi_fortran_integer1_t, +)
#endif
#if OMPI_HAVE_FORTRAN_INTEGER2
OP_FUNC_3BUF(sum, fortran_integer2, ompi_fortran_integer2_t, +)
#endif
#if OMPI_HAVE_FORTRAN_INTEGER4
OP_FUNC_3BUF(sum, fortran_integer4, ompi_fortran_integer4_t, +)
#endif
#if OMPI_HAVE_FORTRAN_INTEGER8
OP_FUNC_3BUF(sum, fortran_integer8, ompi_fortran_integer8_t, +)
#endif
#if OMPI_HAVE_FORTRAN_INTEGER16
OP_FUNC_3BUF(sum, fortran_integer16, ompi_fortran_integer16_t, +)
#endif
/* Floating point */
OP_FUNC_3BUF(sum, float, float, +)
OP_FUNC_3BUF(sum, double, double, +)
#if HAVE_LONG_DOUBLE
OP_FUNC_3BUF(sum, long_double, long double, +)
#endif
#if OMPI_HAVE_FORTRAN_REAL
OP_FUNC_3BUF(sum, fortran_real, ompi_fortran_real_t, +)
#endif
#if OMPI_HAVE_FORTRAN_DOUBLE_PRECISION
OP_FUNC_3BUF(sum, fortran_double_precision, ompi_fortran_double_precision_t, +)
#endif
#if OMPI_HAVE_FORTRAN_REAL2
OP_FUNC_3BUF(sum, fortran_real2, ompi_fortran_real2_t, +)
#endif
#if OMPI_HAVE_FORTRAN_REAL4
OP_FUNC_3BUF(sum, fortran_real4, ompi_fortran_real4_t, +)
#endif
#if OMPI_HAVE_FORTRAN_REAL8
OP_FUNC_3BUF(sum, fortran_real8, ompi_fortran_real8_t, +)
#endif
#if OMPI_HAVE_FORTRAN_REAL16 && OMPI_REAL16_MATCHES_C
OP_FUNC_3BUF(sum, fortran_real16, ompi_fortran_real16_t, +)
#endif
/* Complex */
#if HAVE_FLOAT__COMPLEX
OP_FUNC_3BUF(sum, c_float_complex, float _Complex, +)
#endif
#if HAVE_DOUBLE__COMPLEX
OP_FUNC_3BUF(sum, c_double_complex, double _Complex, +)
#endif
#if HAVE_LONG_DOUBLE__COMPLEX
OP_FUNC_3BUF(sum, c_long_double_complex, long double _Complex, +)
#endif

/*************************************************************************
 * Product
 *************************************************************************/

/* C integer */
OP_FUNC_3BUF(prod,   int8_t,   int8_t, *)
OP_FUNC_3BUF(prod,  uint8_t,  uint8_t, *)
OP_FUNC_3BUF(prod,  int16_t,  int16_t, *)
OP_FUNC_3BUF(prod, uint16_t, uint16_t, *)
OP_FUNC_3BUF(prod,  int32_t,  int32_t, *)
OP_FUNC_3BUF(prod, uint32_t, uint32_t, *)
OP_FUNC_3BUF(prod,  int64_t,  int64_t, *)
OP_FUNC_3BUF(prod, uint64_t, uint64_t, *)
/* Fortran integer */
#if OMPI_HAVE_FORTRAN_INTEGER
OP_FUNC_3BUF(prod, fortran_integer, ompi_fortran_integer_t, *)
#endif
#if OMPI_HAVE_FORTRAN_INTEGER1
OP_FUNC_3BUF(prod, fortran_integer1, ompi_fortran_integer1_t, *)
#endif
#if OMPI_HAVE_FORTRAN_INTEGER2
OP_FUNC_3BUF(prod, fortran_integer2, ompi_fortran_integer2_t, *)
#endif
#if OMPI_HAVE_FORTRAN_INTEGER4
OP_FUNC_3BUF(prod, fortran_integer4, ompi_fortran_integer4_t, *)
#endif
#if OMPI_HAVE_FORTRAN_INTEGER8
OP_FUNC_3BUF(prod, fortran_integer8, ompi_fortran_integer8_t, *)
#endif
#if OMPI_HAVE_FORTRAN_INTEGER16
OP_FUNC_3BUF(prod, fortran_integer16, ompi_fortran_integer16_t, *)
#endif
/* Floating point */
OP_FUNC_3BUF(prod, float, float, *)
OP_FUNC_3BUF(prod, double, double, *)
#if HAVE_LONG_DOUBLE
OP_FUNC_3BUF(prod, long_double, long double, *)
#endif
#if OMPI_HAVE_FORTRAN_REAL
OP_FUNC_3BUF(prod, fortran_real, ompi_fortran_real_t, *)
#endif
#if OMPI_HAVE_FORTRAN_DOUBLE_PRECISION
OP_FUNC_3BUF(prod, fortran_double_precision, ompi_fortran_double_precision_t, *)
#endif
#if OMPI_HAVE_FORTRAN_REAL2
OP_FUNC_3BUF(prod, fortran_real2, ompi_fortran_real2_t, *)
#endif
#if OMPI_HAVE_FORTRAN_REAL4
OP_FUNC_3BUF(prod, fortran_real4, ompi_fortran_real4_t, *)
#endif
#if OMPI_HAVE_FORTRAN_REAL8
OP_FUNC_3BUF(prod, fortran_real8, ompi_fortran_real8_t, *)
#endif
#if OMPI_HAVE_FORTRAN_REAL16 && OMPI_REAL16_MATCHES_C
OP_FUNC_3BUF(prod, fortran_real16, ompi_fortran_real16_t, *)
#endif
/* Complex */
#if HAVE_FLOAT__COMPLEX
OP_FUNC_3BUF(prod, c_float_complex, float _Complex, *)
#endif
#if HAVE_DOUBLE__COMPLEX
OP_FUNC_3BUF(prod, c_double_complex, double _Complex, *)
#endif
#if HAVE_LONG_DOUBLE__COMPLEX
OP_FUNC_3BUF(prod, c_long_double_complex, long double _Complex, *)
#endif

/*************************************************************************
 * Logical AND
 *************************************************************************/

#undef current_func
#define current_func(a, b) ((a) && (b))
/* C integer */
FUNC_FUNC_3BUF(land,   int8_t,   int8_t)
FUNC_FUNC_3BUF(land,  uint8_t,  uint8_t)
FUNC_FUNC_3BUF(land,  int16_t,  int16_t)
FUNC_FUNC_3BUF(land, uint16_t, uint16_t)
FUNC_FUNC_3BUF(land,  int32_t,  int32_t)
FUNC_FUNC_3BUF(land, uint32_t, uint32_t)
FUNC_FUNC_3BUF(land,  int64_t,  int64_t)
FUNC_FUNC_3BUF(land, uint64_t, uint64_t)
/* Logical */
#if OMPI_HAVE_FORTRAN_LOGICAL
FUNC_FUNC_3BUF(land, fortran_logical, ompi_fortran_logical_t)
#endif
/* C++ bool */
FUNC_FUNC_3BUF(land, bool, bool)

/*************************************************************************
 * Logical OR
 *************************************************************************/

#undef current_func
#define current_func(a, b) ((a) || (b))
/* C integer */
FUNC_FUNC_3BUF(lor,   int8_t,   int8_t)
FUNC_FUNC_3BUF(lor,  uint8_t,  uint8_t)
FUNC_FUNC_3BUF(lor,  int16_t,  int16_t)
FUNC_FUNC_3BUF(lor, uint16_t, uint16_t)
FUNC_FUNC_3BUF(lor,  int32_t,  int32_t)
FUNC_FUNC_3BUF(lor, uint32_t, uint32_t)
FUNC_FUNC_3BUF(lor,  int64_t,  int64_t)
FUNC_FUNC_3BUF(lor, uint64_t, uint64_t)
/* Logical */
#if OMPI_HAVE_FORTRAN_LOGICAL
FUNC_FUNC_3BUF(lor, fortran_logical, ompi_fortran_logical_t)
#endif
/* C++ bool */
FUNC_FUNC_3BUF(lor, bool, bool)

/*************************************************************************
 * Logical XOR
 *************************************************************************/

#undef current_func
#define current_func(a, b) ((a ? 1 : 0) ^ (b ? 1: 0))
/* C integer */
FUNC_FUNC_3BUF(lxor,   int8_t,   int8_t)
FUNC_FUNC_3BUF(lxor,  uint8_t,  uint8_t)
FUNC_FUNC_3BUF(lxor,  int16_t,  int16_t)
FUNC_FUNC_3BUF(lxor, uint16_t, uint16_t)
FUNC_FUNC_3BUF(lxor,  int32_t,  int32_t)
FUNC_FUNC_3BUF(lxor, uint32_t, uint32_t)
FUNC_FUNC_3BUF(lxor,  int64_t,  int64_t)
FUNC_FUNC_3BUF(lxor, uint64_t, uint64_t)
/* Logical */
#if OMPI_HAVE_FORTRAN_LOGICAL
FUNC_FUNC_3BUF(lxor, fortran_logical, ompi_fortran_logical_t)
#endif
/* C++ bool */
FUNC_FUNC_3BUF(lxor, bool, bool)

/*************************************************************************
 * Bitwise AND
 *************************************************************************/

#undef current_func
#define current_func(a, b) ((a) & (b))
/* C integer */
FUNC_FUNC_3BUF(band,   int8_t,   int8_t)
FUNC_FUNC_3BUF(band,  uint8_t,  uint8_t)
FUNC_FUNC_3BUF(band,  int16_t,  int16_t)
FUNC_FUNC_3BUF(band, uint16_t, uint16_t)
FUNC_FUNC_3BUF(band,  int32_t,  int32_t)
FUNC_FUNC_3BUF(band, uint32_t, uint32_t)
FUNC_FUNC_3BUF(band,  int64_t,  int64_t)
FUNC_FUNC_3BUF(band, uint64_t, uint64_t)
/* Fortran integer */
#if OMPI_HAVE_FORTRAN_INTEGER
FUNC_FUNC_3BUF(band, fortran_integer, ompi_fortran_integer_t)
#endif
#if OMPI_HAVE_FORTRAN_INTEGER1
FUNC_FUNC_3BUF(band, fortran_integer1, ompi_fortran_integer1_t)
#endif
#if OMPI_HAVE_FORTRAN_INTEGER2
FUNC_FUNC_3BUF(band, fortran_integer2, ompi_fortran_integer2_t)
#endif
#if OMPI_HAVE_FORTRAN_INTEGER4
FUNC_FUNC_3BUF(band, fortran_integer4, ompi_fortran_integer4_t)
#endif
#if OMPI_HAVE_FORTRAN_INTEGER8
FUNC_FUNC_3BUF(band, fortran_integer8, ompi_fortran_integer8_t)
#endif
#if OMPI_HAVE_FORTRAN_INTEGER16
FUNC_FUNC_3BUF(band, fortran_integer16, ompi_fortran_integer16_t)
#endif
/* Byte */
FUNC_FUNC_3BUF(band, byte, char)

/*************************************************************************
 * Bitwise OR
 *************************************************************************/

#undef current_func
#define current_func(a, b) ((a) | (b))
/* C integer */
FUNC_FUNC_3BUF(bor,   int8_t,   int8_t)
FUNC_FUNC_3BUF(bor,  uint8_t,  uint8_t)
FUNC_FUNC_3BUF(bor,  int16_t,  int16_t)
FUNC_FUNC_3BUF(bor, uint16_t, uint16_t)
FUNC_FUNC_3BUF(bor,  int32_t,  int32_t)
FUNC_FUNC_3BUF(bor, uint32_t, uint32_t)
FUNC_FUNC_3BUF(bor,  int64_t,  int64_t)
FUNC_FUNC_3BUF(bor, uint64_t, uint64_t)
/* Fortran integer */
#if OMPI_HAVE_FORTRAN_INTEGER
FUNC_FUNC_3BUF(bor, fortran_integer, ompi_fortran_integer_t)
#endif
#if OMPI_HAVE_FORTRAN_INTEGER1
FUNC_FUNC_3BUF(bor, fortran_integer1, ompi_fortran_integer1_t)
#endif
#if OMPI_HAVE_FORTRAN_INTEGER2
FUNC_FUNC_3BUF(bor, fortran_integer2, ompi_fortran_integer2_t)
#endif
#if OMPI_HAVE_FORTRAN_INTEGER4
FUNC_FUNC_3BUF(bor, fortran_integer4, ompi_fortran_integer4_t)
#endif
#if OMPI_HAVE_FORTRAN_INTEGER8
FUNC_FUNC_3BUF(bor, fortran_integer8, ompi_fortran_integer8_t)
#endif
#if OMPI_HAVE_FORTRAN_INTEGER16
FUNC_FUNC_3BUF(bor, fortran_integer16, ompi_fortran_integer16_t)
#endif
/* Byte */
FUNC_FUNC_3BUF(bor, byte, char)

/*************************************************************************
 * Bitwise XOR
 *************************************************************************/

#undef current_func
#define current_func(a, b) ((a) ^ (b))
/* C integer */
FUNC_FUNC_3BUF(bxor,   int8_t,   int8_t)
FUNC_FUNC_3BUF(bxor,  uint8_t,  uint8_t)
FUNC_FUNC_3BUF(bxor,  int16_t,  int16_t)
FUNC_FUNC_3BUF(bxor, uint16_t, uint16_t)
FUNC_FUNC_3BUF(bxor,  int32_t,  int32_t)
FUNC_FUNC_3BUF(bxor, uint32_t, uint32_t)
FUNC_FUNC_3BUF(bxor,  int64_t,  int64_t)
FUNC_FUNC_3BUF(bxor, uint64_t, uint64_t)
/* Fortran integer */
#if OMPI_HAVE_FORTRAN_INTEGER
FUNC_FUNC_3BUF(bxor, fortran_integer, ompi_fortran_integer_t)
#endif
#if OMPI_HAVE_FORTRAN_INTEGER1
FUNC_FUNC_3BUF(bxor, fortran_integer1, ompi_fortran_integer1_t)
#endif
#if OMPI_HAVE_FORTRAN_INTEGER2
FUNC_FUNC_3BUF(bxor, fortran_integer2, ompi_fortran_integer2_t)
#endif
#if OMPI_HAVE_FORTRAN_INTEGER4
FUNC_FUNC_3BUF(bxor, fortran_integer4, ompi_fortran_integer4_t)
#endif
#if OMPI_HAVE_FORTRAN_INTEGER8
FUNC_FUNC_3BUF(bxor, fortran_integer8, ompi_fortran_integer8_t)
#endif
#if OMPI_HAVE_FORTRAN_INTEGER16
FUNC_FUNC_3BUF(bxor, fortran_integer16, ompi_fortran_integer16_t)
#endif
/* Byte */
FUNC_FUNC_3BUF(bxor, byte, char)

/*************************************************************************
 * Min and max location "pair" datatypes
 *************************************************************************/

/*
#if OMPI_HAVE_FORTRAN_REAL
LOC_STRUCT_3BUF(2real, ompi_fortran_real_t, ompi_fortran_real_t)
#endif
#if OMPI_HAVE_FORTRAN_DOUBLE_PRECISION
LOC_STRUCT_3BUF(2double_precision, ompi_fortran_double_precision_t, ompi_fortran_double_precision_t)
#endif
#if OMPI_HAVE_FORTRAN_INTEGER
LOC_STRUCT_3BUF(2integer, ompi_fortran_integer_t, ompi_fortran_integer_t)
#endif
LOC_STRUCT_3BUF(float_int, float, int)
LOC_STRUCT_3BUF(double_int, double, int)
LOC_STRUCT_3BUF(long_int, long, int)
LOC_STRUCT_3BUF(2int, int, int)
LOC_STRUCT_3BUF(short_int, short, int)
#if HAVE_LONG_DOUBLE
LOC_STRUCT_3BUF(long_double_int, long double, int)
#endif
*/

/*************************************************************************
 * Max location
 *************************************************************************/

#if OMPI_HAVE_FORTRAN_REAL
LOC_FUNC_3BUF(maxloc, 2real, >)
#endif
#if OMPI_HAVE_FORTRAN_DOUBLE_PRECISION
LOC_FUNC_3BUF(maxloc, 2double_precision, >)
#endif
#if OMPI_HAVE_FORTRAN_INTEGER
LOC_FUNC_3BUF(maxloc, 2integer, >)
#endif
LOC_FUNC_3BUF(maxloc, float_int, >)
LOC_FUNC_3BUF(maxloc, double_int, >)
LOC_FUNC_3BUF(maxloc, long_int, >)
LOC_FUNC_3BUF(maxloc, 2int, >)
LOC_FUNC_3BUF(maxloc, short_int, >)
#if HAVE_LONG_DOUBLE
LOC_FUNC_3BUF(maxloc, long_double_int, >)
#endif

/*************************************************************************
 * Min location
 *************************************************************************/

#if OMPI_HAVE_FORTRAN_REAL
LOC_FUNC_3BUF(minloc, 2real, <)
#endif
#if OMPI_HAVE_FORTRAN_DOUBLE_PRECISION
LOC_FUNC_3BUF(minloc, 2double_precision, <)
#endif
#if OMPI_HAVE_FORTRAN_INTEGER
LOC_FUNC_3BUF(minloc, 2integer, <)
#endif
LOC_FUNC_3BUF(minloc, float_int, <)
LOC_FUNC_3BUF(minloc, double_int, <)
LOC_FUNC_3BUF(minloc, long_int, <)
LOC_FUNC_3BUF(minloc, 2int, <)
LOC_FUNC_3BUF(minloc, short_int, <)
#if HAVE_LONG_DOUBLE
LOC_FUNC_3BUF(minloc, long_double_int, <)
#endif

/*
 * Helpful defines, because there's soooo many names!
 *
 * **NOTE** These #define's used to be strictly ordered but the use of
 * designated initializers removed this restrictions. When adding new
 * operators ALWAYS use a designated initalizer!
 */

/** C integer ***********************************************************/
#define C_INTEGER(name, ftype)                                              \
  [OMPI_OP_BASE_TYPE_INT8_T] = ompi_op_base_##ftype##_##name##_int8_t,     \
  [OMPI_OP_BASE_TYPE_UINT8_T] = ompi_op_base_##ftype##_##name##_uint8_t,   \
  [OMPI_OP_BASE_TYPE_INT16_T] = ompi_op_base_##ftype##_##name##_int16_t,   \
  [OMPI_OP_BASE_TYPE_UINT16_T] = ompi_op_base_##ftype##_##name##_uint16_t, \
  [OMPI_OP_BASE_TYPE_INT32_T] = ompi_op_base_##ftype##_##name##_int32_t,   \
  [OMPI_OP_BASE_TYPE_UINT32_T] = ompi_op_base_##ftype##_##name##_uint32_t, \
  [OMPI_OP_BASE_TYPE_INT64_T] = ompi_op_base_##ftype##_##name##_int64_t,   \
  [OMPI_OP_BASE_TYPE_UINT64_T] = ompi_op_base_##ftype##_##name##_uint64_t

/** All the Fortran integers ********************************************/

#if OMPI_HAVE_FORTRAN_INTEGER
#define FORTRAN_INTEGER_PLAIN(name, ftype) ompi_op_base_##ftype##_##name##_fortran_integer
#else
#define FORTRAN_INTEGER_PLAIN(name, ftype) NULL
#endif
#if OMPI_HAVE_FORTRAN_INTEGER1
#define FORTRAN_INTEGER1(name, ftype) ompi_op_base_##ftype##_##name##_fortran_integer1
#else
#define FORTRAN_INTEGER1(name, ftype) NULL
#endif
#if OMPI_HAVE_FORTRAN_INTEGER2
#define FORTRAN_INTEGER2(name, ftype) ompi_op_base_##ftype##_##name##_fortran_integer2
#else
#define FORTRAN_INTEGER2(name, ftype) NULL
#endif
#if OMPI_HAVE_FORTRAN_INTEGER4
#define FORTRAN_INTEGER4(name, ftype) ompi_op_base_##ftype##_##name##_fortran_integer4
#else
#define FORTRAN_INTEGER4(name, ftype) NULL
#endif
#if OMPI_HAVE_FORTRAN_INTEGER8
#define FORTRAN_INTEGER8(name, ftype) ompi_op_base_##ftype##_##name##_fortran_integer8
#else
#define FORTRAN_INTEGER8(name, ftype) NULL
#endif
#if OMPI_HAVE_FORTRAN_INTEGER16
#define FORTRAN_INTEGER16(name, ftype) ompi_op_base_##ftype##_##name##_fortran_integer16
#else
#define FORTRAN_INTEGER16(name, ftype) NULL
#endif

#define FORTRAN_INTEGER(name, ftype)                                  \
    [OMPI_OP_BASE_TYPE_INTEGER] = FORTRAN_INTEGER_PLAIN(name, ftype), \
    [OMPI_OP_BASE_TYPE_INTEGER1] = FORTRAN_INTEGER1(name, ftype),     \
    [OMPI_OP_BASE_TYPE_INTEGER2] = FORTRAN_INTEGER2(name, ftype),     \
    [OMPI_OP_BASE_TYPE_INTEGER4] = FORTRAN_INTEGER4(name, ftype),     \
    [OMPI_OP_BASE_TYPE_INTEGER8] = FORTRAN_INTEGER8(name, ftype),     \
    [OMPI_OP_BASE_TYPE_INTEGER16] = FORTRAN_INTEGER16(name, ftype)

/** All the Fortran reals ***********************************************/

#if OMPI_HAVE_FORTRAN_REAL
#define FLOATING_POINT_FORTRAN_REAL_PLAIN(name, ftype) ompi_op_base_##ftype##_##name##_fortran_real
#else
#define FLOATING_POINT_FORTRAN_REAL_PLAIN(name, ftype) NULL
#endif
#if OMPI_HAVE_FORTRAN_REAL2
#define FLOATING_POINT_FORTRAN_REAL2(name, ftype) ompi_op_base_##ftype##_##name##_fortran_real2
#else
#define FLOATING_POINT_FORTRAN_REAL2(name, ftype) NULL
#endif
#if OMPI_HAVE_FORTRAN_REAL4
#define FLOATING_POINT_FORTRAN_REAL4(name, ftype) ompi_op_base_##ftype##_##name##_fortran_real4
#else
#define FLOATING_POINT_FORTRAN_REAL4(name, ftype) NULL
#endif
#if OMPI_HAVE_FORTRAN_REAL8
#define FLOATING_POINT_FORTRAN_REAL8(name, ftype) ompi_op_base_##ftype##_##name##_fortran_real8
#else
#define FLOATING_POINT_FORTRAN_REAL8(name, ftype) NULL
#endif
/* If:
   - we have fortran REAL*16, *and*
   - fortran REAL*16 matches the bit representation of the
     corresponding C type
   Only then do we put in function pointers for REAL*16 reductions.
   Otherwise, just put in NULL. */
#if OMPI_HAVE_FORTRAN_REAL16 && OMPI_REAL16_MATCHES_C
#define FLOATING_POINT_FORTRAN_REAL16(name, ftype) ompi_op_base_##ftype##_##name##_fortran_real16
#else
#define FLOATING_POINT_FORTRAN_REAL16(name, ftype) NULL
#endif

#define FLOATING_POINT_FORTRAN_REAL(name, ftype)                               \
    [OMPI_OP_BASE_TYPE_REAL] = FLOATING_POINT_FORTRAN_REAL_PLAIN(name, ftype), \
    [OMPI_OP_BASE_TYPE_REAL2] = FLOATING_POINT_FORTRAN_REAL2(name, ftype),     \
    [OMPI_OP_BASE_TYPE_REAL4] = FLOATING_POINT_FORTRAN_REAL4(name, ftype),     \
    [OMPI_OP_BASE_TYPE_REAL8] = FLOATING_POINT_FORTRAN_REAL8(name, ftype),     \
    [OMPI_OP_BASE_TYPE_REAL16] = FLOATING_POINT_FORTRAN_REAL16(name, ftype)

/** Fortran double precision ********************************************/

#if OMPI_HAVE_FORTRAN_DOUBLE_PRECISION
#define FLOATING_POINT_FORTRAN_DOUBLE_PRECISION(name, ftype)  \
    ompi_op_base_##ftype##_##name##_fortran_double_precision
#else
#define FLOATING_POINT_FORTRAN_DOUBLE_PRECISION(name, ftype) NULL
#endif

/** Floating point, including all the Fortran reals *********************/

#define FLOATING_POINT(name, ftype)                                                            \
  [OMPI_OP_BASE_TYPE_FLOAT] = ompi_op_base_##ftype##_##name##_float,                           \
  [OMPI_OP_BASE_TYPE_DOUBLE] = ompi_op_base_##ftype##_##name##_double,                         \
  FLOATING_POINT_FORTRAN_REAL(name, ftype),                                                    \
  [OMPI_OP_BASE_TYPE_DOUBLE_PRECISION] = FLOATING_POINT_FORTRAN_DOUBLE_PRECISION(name, ftype), \
  [OMPI_OP_BASE_TYPE_LONG_DOUBLE] = ompi_op_base_##ftype##_##name##_long_double

/** Fortran logical *****************************************************/

#if OMPI_HAVE_FORTRAN_LOGICAL
#define FORTRAN_LOGICAL(name, ftype)                                          \
  ompi_op_base_##ftype##_##name##_fortran_logical  /* OMPI_OP_BASE_TYPE_LOGICAL */
#else
#define FORTRAN_LOGICAL(name, ftype) NULL
#endif

#define LOGICAL(name, ftype)                                    \
    [OMPI_OP_BASE_TYPE_LOGICAL] = FORTRAN_LOGICAL(name, ftype), \
    [OMPI_OP_BASE_TYPE_BOOL] = ompi_op_base_##ftype##_##name##_bool

/** Complex *****************************************************/

#define FLOAT_COMPLEX(name, ftype) ompi_op_base_##ftype##_##name##_c_float_complex
#define DOUBLE_COMPLEX(name, ftype) ompi_op_base_##ftype##_##name##_c_double_complex
#define LONG_DOUBLE_COMPLEX(name, ftype) ompi_op_base_##ftype##_##name##_c_long_double_complex

#define COMPLEX(name, ftype)                                                  \
    [OMPI_OP_BASE_TYPE_C_FLOAT_COMPLEX] = FLOAT_COMPLEX(name, ftype),         \
    [OMPI_OP_BASE_TYPE_C_DOUBLE_COMPLEX] = DOUBLE_COMPLEX(name, ftype),       \
    [OMPI_OP_BASE_TYPE_C_LONG_DOUBLE_COMPLEX] = LONG_DOUBLE_COMPLEX(name, ftype)

/** Byte ****************************************************************/

#define BYTE(name, ftype)                                     \
  [OMPI_OP_BASE_TYPE_BYTE] = ompi_op_base_##ftype##_##name##_byte

/** Fortran complex *****************************************************/
/** Fortran "2" types ***************************************************/

#if OMPI_HAVE_FORTRAN_REAL
#define TWOLOC_FORTRAN_2REAL(name, ftype) ompi_op_base_##ftype##_##name##_2real
#else
#define TWOLOC_FORTRAN_2REAL(name, ftype) NULL
#endif
#if OMPI_HAVE_FORTRAN_DOUBLE_PRECISION
#define TWOLOC_FORTRAN_2DOUBLE_PRECISION(name, ftype) ompi_op_base_##ftype##_##name##_2double_precision
#else
#define TWOLOC_FORTRAN_2DOUBLE_PRECISION(name, ftype) NULL
#endif
#if OMPI_HAVE_FORTRAN_INTEGER
#define TWOLOC_FORTRAN_2INTEGER(name, ftype) ompi_op_base_##ftype##_##name##_2integer
#else
#define TWOLOC_FORTRAN_2INTEGER(name, ftype) NULL
#endif

/** All "2" types *******************************************************/

#define TWOLOC(name, ftype)                                                                   \
    [OMPI_OP_BASE_TYPE_2REAL] = TWOLOC_FORTRAN_2REAL(name, ftype),                            \
    [OMPI_OP_BASE_TYPE_2DOUBLE_PRECISION] = TWOLOC_FORTRAN_2DOUBLE_PRECISION(name, ftype),    \
    [OMPI_OP_BASE_TYPE_2INTEGER] = TWOLOC_FORTRAN_2INTEGER(name, ftype),                      \
    [OMPI_OP_BASE_TYPE_FLOAT_INT] = ompi_op_base_##ftype##_##name##_float_int,                \
    [OMPI_OP_BASE_TYPE_DOUBLE_INT] = ompi_op_base_##ftype##_##name##_double_int,              \
    [OMPI_OP_BASE_TYPE_LONG_INT] = ompi_op_base_##ftype##_##name##_long_int,                  \
    [OMPI_OP_BASE_TYPE_2INT] = ompi_op_base_##ftype##_##name##_2int,                          \
    [OMPI_OP_BASE_TYPE_SHORT_INT] = ompi_op_base_##ftype##_##name##_short_int,                \
    [OMPI_OP_BASE_TYPE_LONG_DOUBLE_INT] = ompi_op_base_##ftype##_##name##_long_double_int

/*
 * MPI_OP_NULL
 * All types
 */
#define FLAGS_NO_FLOAT \
    (OMPI_OP_FLAGS_INTRINSIC | OMPI_OP_FLAGS_ASSOC | OMPI_OP_FLAGS_COMMUTE)
#define FLAGS \
    (OMPI_OP_FLAGS_INTRINSIC | OMPI_OP_FLAGS_ASSOC | \
     OMPI_OP_FLAGS_FLOAT_ASSOC | OMPI_OP_FLAGS_COMMUTE)

ompi_op_base_handler_fn_t ompi_op_base_functions[OMPI_OP_BASE_FORTRAN_OP_MAX][OMPI_OP_BASE_TYPE_MAX] =
    {
        /* Corresponds to MPI_OP_NULL */
        [OMPI_OP_BASE_FORTRAN_NULL] = {
            /* Leaving this empty puts in NULL for all entries */
            NULL,
        },
        /* Corresponds to MPI_MAX */
        [OMPI_OP_BASE_FORTRAN_MAX] = {
            C_INTEGER(max, 2buff),
            FORTRAN_INTEGER(max, 2buff),
            FLOATING_POINT(max, 2buff),
        },
        /* Corresponds to MPI_MIN */
        [OMPI_OP_BASE_FORTRAN_MIN] = {
            C_INTEGER(min, 2buff),
            FORTRAN_INTEGER(min, 2buff),
            FLOATING_POINT(min, 2buff),
        },
        /* Corresponds to MPI_SUM */
        [OMPI_OP_BASE_FORTRAN_SUM] = {
            C_INTEGER(sum, 2buff),
            FORTRAN_INTEGER(sum, 2buff),
            FLOATING_POINT(sum, 2buff),
            COMPLEX(sum, 2buff),
        },
        /* Corresponds to MPI_PROD */
        [OMPI_OP_BASE_FORTRAN_PROD] = {
            C_INTEGER(prod, 2buff),
            FORTRAN_INTEGER(prod, 2buff),
            FLOATING_POINT(prod, 2buff),
            COMPLEX(prod, 2buff),
        },
        /* Corresponds to MPI_LAND */
        [OMPI_OP_BASE_FORTRAN_LAND] = {
            C_INTEGER(land, 2buff),
            LOGICAL(land, 2buff),
        },
        /* Corresponds to MPI_BAND */
        [OMPI_OP_BASE_FORTRAN_BAND] = {
            C_INTEGER(band, 2buff),
            FORTRAN_INTEGER(band, 2buff),
            BYTE(band, 2buff),
        },
        /* Corresponds to MPI_LOR */
        [OMPI_OP_BASE_FORTRAN_LOR] = {
            C_INTEGER(lor, 2buff),
            LOGICAL(lor, 2buff),
        },
        /* Corresponds to MPI_BOR */
        [OMPI_OP_BASE_FORTRAN_BOR] = {
            C_INTEGER(bor, 2buff),
            FORTRAN_INTEGER(bor, 2buff),
            BYTE(bor, 2buff),
        },
        /* Corresponds to MPI_LXOR */
        [OMPI_OP_BASE_FORTRAN_LXOR] = {
            C_INTEGER(lxor, 2buff),
            LOGICAL(lxor, 2buff),
        },
        /* Corresponds to MPI_BXOR */
        [OMPI_OP_BASE_FORTRAN_BXOR] = {
            C_INTEGER(bxor, 2buff),
            FORTRAN_INTEGER(bxor, 2buff),
            BYTE(bxor, 2buff),
        },
        /* Corresponds to MPI_MAXLOC */
        [OMPI_OP_BASE_FORTRAN_MAXLOC] = {
            TWOLOC(maxloc, 2buff),
        },
        /* Corresponds to MPI_MINLOC */
        [OMPI_OP_BASE_FORTRAN_MINLOC] = {
            TWOLOC(minloc, 2buff),
        },
        /* Corresponds to MPI_REPLACE */
        [OMPI_OP_BASE_FORTRAN_REPLACE] = {
            /* (MPI_ACCUMULATE is handled differently than the other
               reductions, so just zero out its function
               impementations here to ensure that users don't invoke
               MPI_REPLACE with any reduction operations other than
               ACCUMULATE) */
            NULL,
        },

    };


ompi_op_base_3buff_handler_fn_t ompi_op_base_3buff_functions[OMPI_OP_BASE_FORTRAN_OP_MAX][OMPI_OP_BASE_TYPE_MAX] =
    {
        /* Corresponds to MPI_OP_NULL */
        [OMPI_OP_BASE_FORTRAN_NULL] = {
            /* Leaving this empty puts in NULL for all entries */
            NULL,
        },
        /* Corresponds to MPI_MAX */
        [OMPI_OP_BASE_FORTRAN_MAX] = {
            C_INTEGER(max, 3buff),
            FORTRAN_INTEGER(max, 3buff),
            FLOATING_POINT(max, 3buff),
        },
        /* Corresponds to MPI_MIN */
        [OMPI_OP_BASE_FORTRAN_MIN] = {
            C_INTEGER(min, 3buff),
            FORTRAN_INTEGER(min, 3buff),
            FLOATING_POINT(min, 3buff),
        },
        /* Corresponds to MPI_SUM */
        [OMPI_OP_BASE_FORTRAN_SUM] = {
            C_INTEGER(sum, 3buff),
            FORTRAN_INTEGER(sum, 3buff),
            FLOATING_POINT(sum, 3buff),
            COMPLEX(sum, 3buff),
        },
        /* Corresponds to MPI_PROD */
        [OMPI_OP_BASE_FORTRAN_PROD] = {
            C_INTEGER(prod, 3buff),
            FORTRAN_INTEGER(prod, 3buff),
            FLOATING_POINT(prod, 3buff),
            COMPLEX(prod, 3buff),
        },
        /* Corresponds to MPI_LAND */
        [OMPI_OP_BASE_FORTRAN_LAND] ={
            C_INTEGER(land, 3buff),
            LOGICAL(land, 3buff),
        },
        /* Corresponds to MPI_BAND */
        [OMPI_OP_BASE_FORTRAN_BAND] = {
            C_INTEGER(band, 3buff),
            FORTRAN_INTEGER(band, 3buff),
            BYTE(band, 3buff),
        },
        /* Corresponds to MPI_LOR */
        [OMPI_OP_BASE_FORTRAN_LOR] = {
            C_INTEGER(lor, 3buff),
            LOGICAL(lor, 3buff),
        },
        /* Corresponds to MPI_BOR */
        [OMPI_OP_BASE_FORTRAN_BOR] = {
            C_INTEGER(bor, 3buff),
            FORTRAN_INTEGER(bor, 3buff),
            BYTE(bor, 3buff),
        },
        /* Corresponds to MPI_LXOR */
        [OMPI_OP_BASE_FORTRAN_LXOR] = {
            C_INTEGER(lxor, 3buff),
            LOGICAL(lxor, 3buff),
        },
        /* Corresponds to MPI_BXOR */
        [OMPI_OP_BASE_FORTRAN_BXOR] = {
            C_INTEGER(bxor, 3buff),
            FORTRAN_INTEGER(bxor, 3buff),
            BYTE(bxor, 3buff),
        },
        /* Corresponds to MPI_MAXLOC */
        [OMPI_OP_BASE_FORTRAN_MAXLOC] = {
            TWOLOC(maxloc, 3buff),
        },
        /* Corresponds to MPI_MINLOC */
        [OMPI_OP_BASE_FORTRAN_MINLOC] = {
            TWOLOC(minloc, 3buff),
        },
        /* Corresponds to MPI_REPLACE */
        [OMPI_OP_BASE_FORTRAN_REPLACE] = {
            /* MPI_ACCUMULATE is handled differently than the other
               reductions, so just zero out its function
               impementations here to ensure that users don't invoke
               MPI_REPLACE with any reduction operations other than
               ACCUMULATE */
            NULL,
        },
    };

