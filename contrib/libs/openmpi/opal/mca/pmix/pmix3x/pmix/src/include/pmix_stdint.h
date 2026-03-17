/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2016      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2018      Intel, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 * This file includes the C99 stdint.h file if available, and otherwise
 * defines fixed-width types according to the SIZEOF information
 * gathered by configure.
 */

#ifndef PMIX_STDINT_H
#define PMIX_STDINT_H 1

#include "pmix_config.h"

/*
 * Include what we can and define what is missing.
 */
#include <limits.h>
#include <stdint.h>

#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif

/* 128-bit */

#ifdef HAVE_INT128_T

typedef int128_t pmix_int128_t;
typedef uint128_t pmix_uint128_t;

#define HAVE_PMIX_INT128_T 1

#elif defined(HAVE___INT128)

/* suppress warning about __int128 type */
#pragma GCC diagnostic push
/* Clang won't quietly accept "-pedantic", but GCC versions older than ~4.8
 * won't quietly accept "-Wpedanic".  The whole "#pragma GCC diagnostic ..."
 * facility only was added to GCC as of version 4.6. */
#if defined(__clang__) || (defined(__GNUC__) && __GNUC__ >= 6)
#pragma GCC diagnostic ignored "-Wpedantic"
#else
#pragma GCC diagnostic ignored "-pedantic"
#endif
typedef __int128 pmix_int128_t;
typedef unsigned __int128 pmix_uint128_t;
#pragma GCC diagnostic pop

#define HAVE_PMIX_INT128_T 1

#else

#define HAVE_PMIX_INT128_T 0

#endif

/* Pointers */

#if SIZEOF_VOID_P == SIZEOF_INT

#ifndef HAVE_INTPTR_T
typedef signed int intptr_t;
#endif

#ifndef HAVE_UINTPTR_T
typedef unsigned int uintptr_t;
#endif

#elif SIZEOF_VOID_P == SIZEOF_LONG

#ifndef HAVE_INTPTR_T
typedef signed long intptr_t;
#endif

#ifndef HAVE_UINTPTR_T
typedef unsigned long uintptr_t;
#endif

#elif HAVE_LONG_LONG && SIZEOF_VOID_P == SIZEOF_LONG_LONG

#ifndef HAVE_INTPTR_T
typedef signed long long intptr_t;
#endif
#ifndef HAVE_UINTPTR_T
typedef unsigned long long uintptr_t;
#endif

#else

#error Failed to define pointer-sized integer types

#endif

/* inttypes.h printf specifiers */
# include <inttypes.h>

#ifndef PRIsize_t
# if defined(ACCEPT_C99)
#   define PRIsize_t "zu"
# elif SIZEOF_SIZE_T == SIZEOF_LONG
#   define PRIsize_t "lu"
# elif SIZEOF_SIZE_T == SIZEOF_LONG_LONG
#   define PRIsize_t "llu"
# else
#   define PRIsize_t "u"
# endif
#endif

#endif /* PMIX_STDINT_H */

