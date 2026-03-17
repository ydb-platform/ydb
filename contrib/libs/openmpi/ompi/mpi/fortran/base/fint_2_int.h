/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2009 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2011-2012 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2012      Oracle and/or its affiliates.  All rights reserved.
 * Copyright (c) 2014      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef OMPI_FORTRAN_BASE_FINT_2_INT_H
#define OMPI_FORTRAN_BASE_FINT_2_INT_H

#include "ompi_config.h"

#include <stdlib.h>

/*
 * Define MACROS to take account of different size of MPI_Fint from int
 */

#if OMPI_SIZEOF_FORTRAN_INTEGER == SIZEOF_INT
  #define OMPI_ARRAY_NAME_DECL(a)
  #define OMPI_2_DIM_ARRAY_NAME_DECL(a, dim2)
  #define OMPI_SINGLE_NAME_DECL(a)
  #define OMPI_ARRAY_NAME_CONVERT(a) a
  #define OMPI_SINGLE_NAME_CONVERT(a) a
  #define OMPI_INT_2_FINT(a) a
  #define OMPI_FINT_2_INT(a) a
  #define OMPI_PFINT_2_PINT(a) a
  #define OMPI_ARRAY_FINT_2_INT_ALLOC(in, n)
  #define OMPI_ARRAY_FINT_2_INT(in, n)
  #define OMPI_2_DIM_ARRAY_FINT_2_INT(in, n, dim2)
  #define OMPI_ARRAY_FINT_2_INT_CLEANUP(in)
  #define OMPI_SINGLE_FINT_2_INT(in)
  #define OMPI_SINGLE_INT_2_FINT(in)
  #define OMPI_ARRAY_INT_2_FINT(in, n)

#elif OMPI_SIZEOF_FORTRAN_INTEGER > SIZEOF_INT
  #define OMPI_ARRAY_NAME_DECL(a) int *c_##a
  #define OMPI_2_DIM_ARRAY_NAME_DECL(a, dim2) int (*c_##a)[dim2], dim2_index
  #define OMPI_SINGLE_NAME_DECL(a) int c_##a
  #define OMPI_ARRAY_NAME_CONVERT(a) c_##a
  #define OMPI_SINGLE_NAME_CONVERT(a) &c_##a
  #define OMPI_INT_2_FINT(a) a
  #define OMPI_FINT_2_INT(a) (int) (a)
  #define OMPI_PFINT_2_PINT(a) (int *) (a)

  /* This is for OUT parameters. Does only alloc */
  #define OMPI_ARRAY_FINT_2_INT_ALLOC(in, n) \
    OMPI_ARRAY_NAME_CONVERT(in) = malloc(n * sizeof(int))

  /* This is for IN/IN-OUT parameters. Does alloc and assignment */
  #define OMPI_ARRAY_FINT_2_INT(in, n) \
    do { \
      int converted_n = (int)(n); \
      OMPI_ARRAY_NAME_CONVERT(in) = malloc(converted_n * sizeof(int)); \
      while(--converted_n >= 0) { \
        OMPI_ARRAY_NAME_CONVERT(in)[converted_n] = (int) in[converted_n]; \
      } \
    } while (0)

  /* This is for 2-dim arrays */
  #define OMPI_2_DIM_ARRAY_FINT_2_INT(in, n, dim2) \
    do { \
      int converted_n = (int)(n); \
      OMPI_ARRAY_NAME_CONVERT(in) = (int (*)[dim2]) malloc(converted_n * sizeof(*OMPI_ARRAY_NAME_CONVERT(in))); \
      while(--converted_n >= 0) { \
        for(dim2_index = 0; dim2_index < dim2; ++dim2_index) { \
          OMPI_ARRAY_NAME_CONVERT(in)[converted_n][dim2_index] = (int)in[converted_n][dim2_index]; \
        } \
      } \
    } while (0)

  /* This is for IN parameters. Does only free */
  #define OMPI_ARRAY_FINT_2_INT_CLEANUP(in) \
    free(OMPI_ARRAY_NAME_CONVERT(in))

  /* This is for single IN parameter */
  #define OMPI_SINGLE_FINT_2_INT(in) \
    OMPI_ARRAY_NAME_CONVERT(in) = (int) *(in)

  /* This is for single OUT parameter */
  #define OMPI_SINGLE_INT_2_FINT(in) \
    *(in) = OMPI_ARRAY_NAME_CONVERT(in)

  /* This is for OUT/IN-OUT parametes. Does back assignment and free */
  #define OMPI_ARRAY_INT_2_FINT(in, n) \
    do { \
      int converted_n = (int)(n); \
      while(--converted_n >= 0) { \
        in[converted_n] = OMPI_ARRAY_NAME_CONVERT(in)[converted_n]; \
      } \
      free(OMPI_ARRAY_NAME_CONVERT(in)); \
    } while (0)
#else /* int > MPI_Fint  */
  #define OMPI_ARRAY_NAME_DECL(a) int *c_##a
  #define OMPI_2_DIM_ARRAY_NAME_DECL(a, dim2) int (*c_##a)[dim2], dim2_index
  #define OMPI_SINGLE_NAME_DECL(a) int c_##a
  #define OMPI_ARRAY_NAME_CONVERT(a) c_##a
  #define OMPI_SINGLE_NAME_CONVERT(a) &c_##a
  #define OMPI_INT_2_FINT(a) (MPI_Fint)(a)
  #define OMPI_FINT_2_INT(a) (a)
  #define OMPI_PFINT_2_PINT(a) a

  /* This is for OUT parameters. Does only alloc */
  #define OMPI_ARRAY_FINT_2_INT_ALLOC(in, n) \
    OMPI_ARRAY_NAME_CONVERT(in) = malloc(n * sizeof(int))

  #define OMPI_ARRAY_FINT_2_INT(in, n) \
    do { \
      int converted_n = (int)(n); \
      OMPI_ARRAY_NAME_CONVERT(in) = malloc(converted_n * sizeof(int)); \
      while(--converted_n >= 0) { \
        OMPI_ARRAY_NAME_CONVERT(in)[converted_n] = in[converted_n]; \
      } \
    } while (0)

  #define OMPI_2_DIM_ARRAY_FINT_2_INT(in, n, dim2) \
    do { \
      int converted_n = (int)(n); \
      OMPI_ARRAY_NAME_CONVERT(in) = (int (*)[dim2]) malloc(converted_n * sizeof(*OMPI_ARRAY_NAME_CONVERT(in))); \
      while(--converted_n >= 0) { \
        for(dim2_index = 0; dim2_index < dim2; ++dim2_index) { \
          OMPI_ARRAY_NAME_CONVERT(in)[converted_n][dim2_index] = in[converted_n][dim2_index]; \
        } \
      } \
    } while (0)

  #define OMPI_ARRAY_FINT_2_INT_CLEANUP(in) \
    free(OMPI_ARRAY_NAME_CONVERT(in))

  #define OMPI_SINGLE_FINT_2_INT(in) \
     OMPI_ARRAY_NAME_CONVERT(in) = *(in)

  #define OMPI_SINGLE_INT_2_FINT(in) \
    *in = (MPI_Fint) OMPI_ARRAY_NAME_CONVERT(in)

  #define OMPI_ARRAY_INT_2_FINT(in, n) \
    do { \
      int converted_n = (int)(n); \
      while(--converted_n >= 0) { \
        in[converted_n] = OMPI_ARRAY_NAME_CONVERT(in)[converted_n]; \
      } \
      free(OMPI_ARRAY_NAME_CONVERT(in)); \
    } while (0)

#endif

/*
 * Define MACROS to take account of different size of logical from int
 */

#if OMPI_SIZEOF_FORTRAN_LOGICAL == SIZEOF_INT
#  define OMPI_LOGICAL_NAME_DECL(in)               /* Not needed for int==logical */
#  define OMPI_LOGICAL_NAME_CONVERT(in)        in  /* Not needed for int==logical */
#  define OMPI_LOGICAL_SINGLE_NAME_CONVERT(in) in /* Not needed for int==logical */
#  define OMPI_LOGICAL_ARRAY_NAME_DECL(in)         /* Not needed for int==logical */
#  define OMPI_LOGICAL_ARRAY_NAME_CONVERT(in)  in  /* Not needed for int==logical */
#  define OMPI_ARRAY_LOGICAL_2_INT_ALLOC(in,n)     /* Not needed for int==logical */
#  define OMPI_ARRAY_LOGICAL_2_INT_CLEANUP(in)     /* Not needed for int==logical */

#  if OMPI_FORTRAN_VALUE_TRUE == 1
#    define OMPI_FORTRAN_MUST_CONVERT_LOGICAL_2_INT    0
#    define OMPI_LOGICAL_2_INT(a) a
#    define OMPI_INT_2_LOGICAL(a) a
#    define OMPI_ARRAY_LOGICAL_2_INT(in, n)
#    define OMPI_ARRAY_INT_2_LOGICAL(in, n)
#    define OMPI_SINGLE_INT_2_LOGICAL(a)            /* Single-OUT variable -- Not needed for int==logical, true=1 */
#  else
#    define OMPI_FORTRAN_MUST_CONVERT_LOGICAL_2_INT    1
#    define OMPI_LOGICAL_2_INT(a) ((a)==0? 0 : 1)
#    define OMPI_INT_2_LOGICAL(a) ((a)==0? 0 : OMPI_FORTRAN_VALUE_TRUE)
#    define OMPI_SINGLE_INT_2_LOGICAL(a) *a=OMPI_INT_2_LOGICAL(OMPI_LOGICAL_NAME_CONVERT(*a))
#    define OMPI_ARRAY_LOGICAL_2_INT(in, n) do { \
       int converted_n = (int)(n); \
       OMPI_ARRAY_LOGICAL_2_INT_ALLOC(in, converted_n + 1); \
       while (--converted_n >= 0) { \
         OMPI_LOGICAL_ARRAY_NAME_CONVERT(in)[converted_n]=OMPI_LOGICAL_2_INT(in[converted_n]); \
       } \
     } while (0)
#    define OMPI_ARRAY_INT_2_LOGICAL(in, n) do { \
       int converted_n = (int)(n); \
       while (--converted_n >= 0) { \
         in[converted_n]=OMPI_INT_2_LOGICAL(OMPI_LOGICAL_ARRAY_NAME_CONVERT(in)[converted_n]); \
       } \
       OMPI_ARRAY_LOGICAL_2_INT_CLEANUP(in); \
     }  while (0)

#  endif
#else
/*
 * For anything other than Fortran-logical == C-int, we have to convert
 */
#  define OMPI_FORTRAN_MUST_CONVERT_LOGICAL_2_INT    1
#  define OMPI_LOGICAL_NAME_DECL(in)           int c_##in
#  define OMPI_LOGICAL_NAME_CONVERT(in)        c_##in
#  define OMPI_LOGICAL_SINGLE_NAME_CONVERT(in) &c_##in
#  define OMPI_LOGICAL_ARRAY_NAME_DECL(in)     int * c_##in
#  define OMPI_LOGICAL_ARRAY_NAME_CONVERT(in)  c_##in
#  define OMPI_ARRAY_LOGICAL_2_INT_ALLOC(in,n) \
      OMPI_LOGICAL_ARRAY_NAME_CONVERT(in) = malloc(n * sizeof(int))
#  define OMPI_ARRAY_LOGICAL_2_INT_CLEANUP(in) \
      free(OMPI_LOGICAL_ARRAY_NAME_CONVERT(in))

#  if OMPI_FORTRAN_VALUE_TRUE == 1
#    define OMPI_LOGICAL_2_INT(a) (int)a
#    define OMPI_INT_2_LOGICAL(a) (ompi_fortran_logical_t)a
#    define OMPI_SINGLE_INT_2_LOGICAL(a) *a=(OMPI_INT_2_LOGICAL(OMPI_LOGICAL_NAME_CONVERT(a)))
#  else
#    define OMPI_LOGICAL_2_INT(a) ((a)==0? 0 : 1)
#    define OMPI_INT_2_LOGICAL(a) ((a)==0? 0 : OMPI_FORTRAN_VALUE_TRUE)
#    define OMPI_SINGLE_INT_2_LOGICAL(a) *a=(OMPI_INT_2_LOGICAL(OMPI_LOGICAL_NAME_CONVERT(a)))
#  endif
#  define OMPI_ARRAY_LOGICAL_2_INT(in, n) do { \
       int converted_n = (int)(n); \
       OMPI_ARRAY_LOGICAL_2_INT_ALLOC(in, converted_n + 1); \
       while (--converted_n >= 0) { \
         OMPI_LOGICAL_ARRAY_NAME_CONVERT(in)[converted_n]=OMPI_LOGICAL_2_INT(in[converted_n]); \
       } \
     } while (0)
#  define OMPI_ARRAY_INT_2_LOGICAL(in, n) do { \
       int converted_n = (int)(n); \
       while (--converted_n >= 0) { \
         in[converted_n]=OMPI_INT_2_LOGICAL(OMPI_LOGICAL_ARRAY_NAME_CONVERT(in)[converted_n]); \
       } \
       OMPI_ARRAY_LOGICAL_2_INT_CLEANUP(in); \
     }  while (0)
#endif /* OMPI_SIZEOF_FORTRAN_LOGICAL */


#endif /* OMPI_FORTRAN_BASE_FINT_2_INT_H */
