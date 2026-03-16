/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2007 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2006 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007-2014 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2014-2016 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2015-2017 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2018      Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#if !defined(PMIX_THREAD_USAGE_H)
#define PMIX_THREAD_USAGE_H

#include "pmix_config.h"

#include "src/atomics/sys/atomic.h"
#include "src/include/prefetch.h"


/**
 * Use an atomic operation for increment/decrement
 */

#define PMIX_THREAD_DEFINE_ATOMIC_OP(type, name, operator, suffix)      \
static inline type pmix_thread_ ## name ## _fetch_ ## suffix (pmix_atomic_ ## type *addr, type delta) \
{                                                                       \
    return pmix_atomic_ ## name ## _fetch_ ## suffix (addr, delta);     \
}                                                                       \
                                                                        \
static inline type pmix_thread_fetch_ ## name ## _ ## suffix (pmix_atomic_ ## type *addr, type delta) \
{                                                                       \
    return pmix_atomic_fetch_ ## name ## _ ## suffix (addr, delta);     \
}

#define PMIX_THREAD_DEFINE_ATOMIC_COMPARE_EXCHANGE(type, addr_type, suffix)       \
static inline bool pmix_thread_compare_exchange_strong_ ## suffix (pmix_atomic_ ## addr_type *addr, type *compare, type value) \
{                                                                       \
    return pmix_atomic_compare_exchange_strong_ ## suffix (addr, (addr_type *) compare, (addr_type) value); \
}

#define PMIX_THREAD_DEFINE_ATOMIC_SWAP(type, addr_type, suffix)         \
static inline type pmix_thread_swap_ ## suffix (pmix_atomic_ ## addr_type *ptr, type newvalue) \
{                                                                       \
    return (type) pmix_atomic_swap_ ## suffix (ptr, (addr_type) newvalue); \
}

PMIX_THREAD_DEFINE_ATOMIC_OP(int32_t, add, +, 32)
PMIX_THREAD_DEFINE_ATOMIC_OP(size_t, add, +, size_t)
PMIX_THREAD_DEFINE_ATOMIC_OP(int32_t, and, &, 32)
PMIX_THREAD_DEFINE_ATOMIC_OP(int32_t, or, |, 32)
PMIX_THREAD_DEFINE_ATOMIC_OP(int32_t, xor, ^, 32)
PMIX_THREAD_DEFINE_ATOMIC_OP(int32_t, sub, -, 32)
PMIX_THREAD_DEFINE_ATOMIC_OP(size_t, sub, -, size_t)

PMIX_THREAD_DEFINE_ATOMIC_COMPARE_EXCHANGE(int32_t, int32_t, 32)
PMIX_THREAD_DEFINE_ATOMIC_COMPARE_EXCHANGE(void *, intptr_t, ptr)
PMIX_THREAD_DEFINE_ATOMIC_SWAP(int32_t, int32_t, 32)
PMIX_THREAD_DEFINE_ATOMIC_SWAP(void *, intptr_t, ptr)

#define PMIX_THREAD_ADD_FETCH32 pmix_thread_add_fetch_32
#define PMIX_ATOMIC_ADD_FETCH32 pmix_thread_add_fetch_32

#define PMIX_THREAD_AND_FETCH32 pmix_thread_and_fetch_32
#define PMIX_ATOMIC_AND_FETCH32 pmix_thread_and_fetch_32

#define PMIX_THREAD_OR_FETCH32 pmix_thread_or_fetch_32
#define PMIX_ATOMIC_OR_FETCH32 pmix_thread_or_fetch_32

#define PMIX_THREAD_XOR_FETCH32 pmix_thread_xor_fetch_32
#define PMIX_ATOMIC_XOR_FETCH32 pmix_thread_xor_fetch_32

#define PMIX_THREAD_ADD_FETCH_SIZE_T pmix_thread_add_fetch_size_t
#define PMIX_ATOMIC_ADD_FETCH_SIZE_T pmix_thread_add_fetch_size_t

#define PMIX_THREAD_SUB_FETCH_SIZE_T pmix_thread_sub_fetch_size_t
#define PMIX_ATOMIC_SUB_FETCH_SIZE_T pmix_thread_sub_fetch_size_t

#define PMIX_THREAD_FETCH_ADD32 pmix_thread_fetch_add_32
#define PMIX_ATOMIC_FETCH_ADD32 pmix_thread_fetch_add_32

#define PMIX_THREAD_FETCH_AND32 pmix_thread_fetch_and_32
#define PMIX_ATOMIC_FETCH_AND32 pmix_thread_fetch_and_32

#define PMIX_THREAD_FETCH_OR32 pmix_thread_fetch_or_32
#define PMIX_ATOMIC_FETCH_OR32 pmix_thread_fetch_or_32

#define PMIX_THREAD_FETCH_XOR32 pmix_thread_fetch_xor_32
#define PMIX_ATOMIC_FETCH_XOR32 pmix_thread_fetch_xor_32

#define PMIX_THREAD_FETCH_ADD_SIZE_T pmix_thread_fetch_add_size_t
#define PMIX_ATOMIC_FETCH_ADD_SIZE_T pmix_thread_fetch_add_size_t

#define PMIX_THREAD_FETCH_SUB_SIZE_T pmix_thread_fetch_sub_size_t
#define PMIX_ATOMIC_FETCH_SUB_SIZE_T pmix_thread_fetch_sub_size_t

#define PMIX_THREAD_COMPARE_EXCHANGE_STRONG_32 pmix_thread_compare_exchange_strong_32
#define PMIX_ATOMIC_COMPARE_EXCHANGE_STRONG_32 pmix_thread_compare_exchange_strong_32

#define PMIX_THREAD_COMPARE_EXCHANGE_STRONG_PTR(x, y, z) pmix_thread_compare_exchange_strong_ptr ((pmix_atomic_intptr_t *) x, (intptr_t *) y, (intptr_t) z)
#define PMIX_ATOMIC_COMPARE_EXCHANGE_STRONG_PTR PMIX_THREAD_COMPARE_EXCHANGE_STRONG_PTR

#define PMIX_THREAD_SWAP_32 pmix_thread_swap_32
#define PMIX_ATOMIC_SWAP_32 pmix_thread_swap_32

#define PMIX_THREAD_SWAP_PTR(x, y) pmix_thread_swap_ptr ((pmix_atomic_intptr_t *) x, (intptr_t) y)
#define PMIX_ATOMIC_SWAP_PTR PMIX_THREAD_SWAP_PTR

/* define 64-bit macros is 64-bit atomic math is available */
#if PMIX_HAVE_ATOMIC_MATH_64

PMIX_THREAD_DEFINE_ATOMIC_OP(int64_t, add, +, 64)
PMIX_THREAD_DEFINE_ATOMIC_OP(int64_t, and, &, 64)
PMIX_THREAD_DEFINE_ATOMIC_OP(int64_t, or, |, 64)
PMIX_THREAD_DEFINE_ATOMIC_OP(int64_t, xor, ^, 64)
PMIX_THREAD_DEFINE_ATOMIC_OP(int64_t, sub, -, 64)
PMIX_THREAD_DEFINE_ATOMIC_COMPARE_EXCHANGE(int64_t, int64_t, 64)
PMIX_THREAD_DEFINE_ATOMIC_SWAP(int64_t, int64_t, 64)

#define PMIX_THREAD_ADD_FETCH64 pmix_thread_add_fetch_64
#define PMIX_ATOMIC_ADD_FETCH64 pmix_thread_add_fetch_64

#define PMIX_THREAD_AND_FETCH64 pmix_thread_and_fetch_64
#define PMIX_ATOMIC_AND_FETCH64 pmix_thread_and_fetch_64

#define PMIX_THREAD_OR_FETCH64 pmix_thread_or_fetch_64
#define PMIX_ATOMIC_OR_FETCH64 pmix_thread_or_fetch_64

#define PMIX_THREAD_XOR_FETCH64 pmix_thread_xor_fetch_64
#define PMIX_ATOMIC_XOR_FETCH64 pmix_thread_xor_fetch_64

#define PMIX_THREAD_FETCH_ADD64 pmix_thread_fetch_add_64
#define PMIX_ATOMIC_FETCH_ADD64 pmix_thread_fetch_add_64

#define PMIX_THREAD_FETCH_AND64 pmix_thread_fetch_and_64
#define PMIX_ATOMIC_FETCH_AND64 pmix_thread_fetch_and_64

#define PMIX_THREAD_FETCH_OR64 pmix_thread_fetch_or_64
#define PMIX_ATOMIC_FETCH_OR64 pmix_thread_fetch_or_64

#define PMIX_THREAD_FETCH_XOR64 pmix_thread_fetch_xor_64
#define PMIX_ATOMIC_FETCH_XOR64 pmix_thread_fetch_xor_64

#define PMIX_THREAD_COMPARE_EXCHANGE_STRONG_64 pmix_thread_compare_exchange_strong_64
#define PMIX_ATOMIC_COMPARE_EXCHANGE_STRONG_64 pmix_thread_compare_exchange_strong_64

#define PMIX_THREAD_SWAP_64 pmix_thread_swap_64
#define PMIX_ATOMIC_SWAP_64 pmix_thread_swap_64

#endif

/* thread local storage */
#if PMIX_C_HAVE__THREAD_LOCAL
#define pmix_thread_local _Thread_local
#define PMIX_HAVE_THREAD_LOCAL 1

#elif PMIX_C_HAVE___THREAD /* PMIX_C_HAVE__THREAD_LOCAL */
#define pmix_thread_local __thread
#define PMIX_HAVE_THREAD_LOCAL 1
#endif /* PMIX_C_HAVE___THREAD */

#if !defined(PMIX_HAVE_THREAD_LOCAL)
#define PMIX_HAVE_THREAD_LOCAL 0
#endif /* !defined(PMIX_HAVE_THREAD_LOCAL) */

#endif /* !defined(PMIX_THREAD_USAGE_H) */
