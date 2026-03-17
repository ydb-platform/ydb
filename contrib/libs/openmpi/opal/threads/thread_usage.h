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
 * $COPYRIGHT$
 * 
 * Additional copyrights may follow
 * 
 * $HEADER$
 */

#if !defined(OPAL_THREAD_USAGE_H)
#define OPAL_THREAD_USAGE_H

#include "opal_config.h"

#include "opal/sys/atomic.h"
#include "opal/prefetch.h"

OPAL_DECLSPEC extern bool opal_uses_threads;

/**
 * Check and see if the process is using multiple threads.
 *
 * @retval true If the process may have more than one thread.
 * @retval false If the process only has a single thread.
 *
 * The value that this function returns is influenced by:
 *
 * - how MPI_INIT or MPI_INIT_THREAD was invoked,
 * - what the final MPI thread level was determined to be,
 * - whether the OMPI or MPI libraries are multi-threaded
 *
 * MPI_INIT and MPI_INIT_THREAD (specifically, back-end OMPI startup
 * functions) invoke opal_set_using_threads() to influence the value of
 * this function, depending on their situation. Some examples:
 *
 * - if MPI_INIT is invoked, and the ompi components in use are
 * single-threaded, this value will be false.
 *
 * - if MPI_INIT_THREAD is invoked with MPI_THREAD_MULTIPLE, we have
 * thread support, and the final thread level is determined to be
 * MPI_THREAD_MULTIPLE, this value will be true.
 *
 * - if the process is a single-threaded OMPI executable (e.g., mpicc),
 * this value will be false.
 *
 * Hence, this function will return false if there is guaranteed to
 * only be one thread in the process.  If there is even the
 * possibility that we may have multiple threads, true will be
 * returned.
 */
#define opal_using_threads()  opal_uses_threads

/**
 * Set whether the process is using multiple threads or not.
 *
 * @param have Boolean indicating whether the process is using
 * multiple threads or not.
 *
 * @retval opal_using_threads The new return value from
 * opal_using_threads().
 *
 * This function is used to influence the return value of
 * opal_using_threads().  If configure detected that we have thread
 * support, the return value of future invocations of
 * opal_using_threads() will be the parameter's value.  If configure
 * detected that we have no thread support, then the retuen from
 * opal_using_threads() will always be false.
 */
static inline bool opal_set_using_threads(bool have)
{
    opal_uses_threads = have;
    return opal_using_threads();
}


/**
 * Use an atomic operation for increment/decrement if opal_using_threads()
 * indicates that threads are in use by the application or library.
 */

#define OPAL_THREAD_DEFINE_ATOMIC_OP(type, name, operator, suffix)      \
static inline type opal_thread_ ## name ## _fetch_ ## suffix (volatile type *addr, type delta) \
{                                                                       \
    if (OPAL_UNLIKELY(opal_using_threads())) {                          \
        return opal_atomic_ ## name ## _fetch_ ## suffix (addr, delta); \
    }                                                                   \
                                                                        \
    *addr = *addr operator delta;                                       \
    return *addr;                                                       \
}                                                                       \
                                                                        \
static inline type opal_thread_fetch_ ## name ## _ ## suffix (volatile type *addr, type delta) \
{                                                                       \
    if (OPAL_UNLIKELY(opal_using_threads())) {                          \
        return opal_atomic_fetch_ ## name ## _ ## suffix (addr, delta); \
    }                                                                   \
                                                                        \
    type old = *addr;                                                   \
    *addr = old operator delta;                                         \
    return old;                                                         \
}

#define OPAL_THREAD_DEFINE_ATOMIC_COMPARE_EXCHANGE(type, addr_type, suffix)       \
static inline bool opal_thread_compare_exchange_strong_ ## suffix (volatile addr_type *addr, type *compare, type value) \
{                                                                       \
    if (OPAL_UNLIKELY(opal_using_threads())) {                          \
        return opal_atomic_compare_exchange_strong_ ## suffix ((volatile type *) addr, compare, value); \
    }                                                                   \
                                                                        \
    if ((type) *addr == *compare) {                                     \
        ((type *) addr)[0] = value;                                     \
        return true;                                                    \
    }                                                                   \
                                                                        \
    *compare = ((type *) addr)[0];                                      \
                                                                        \
    return false;                                                       \
}

#define OPAL_THREAD_DEFINE_ATOMIC_SWAP(type, addr_type, suffix)         \
static inline type opal_thread_swap_ ## suffix (volatile addr_type *ptr, type newvalue) \
{                                                                       \
    if (opal_using_threads ()) {                                        \
        return opal_atomic_swap_ ## suffix ((volatile type *) ptr, newvalue); \
    }                                                                   \
                                                                        \
    type old = ((type *) ptr)[0];                                       \
    ((type *) ptr)[0] = newvalue;                                       \
                                                                        \
    return old;                                                         \
}

OPAL_THREAD_DEFINE_ATOMIC_OP(int32_t, add, +, 32)
OPAL_THREAD_DEFINE_ATOMIC_OP(size_t, add, +, size_t)
OPAL_THREAD_DEFINE_ATOMIC_OP(int32_t, and, &, 32)
OPAL_THREAD_DEFINE_ATOMIC_OP(int32_t, or, |, 32)
OPAL_THREAD_DEFINE_ATOMIC_OP(int32_t, xor, ^, 32)
OPAL_THREAD_DEFINE_ATOMIC_OP(int32_t, sub, -, 32)
OPAL_THREAD_DEFINE_ATOMIC_OP(size_t, sub, -, size_t)

OPAL_THREAD_DEFINE_ATOMIC_COMPARE_EXCHANGE(int32_t, int32_t, 32)
OPAL_THREAD_DEFINE_ATOMIC_COMPARE_EXCHANGE(void *, intptr_t, ptr)
OPAL_THREAD_DEFINE_ATOMIC_SWAP(int32_t, int32_t, 32)
OPAL_THREAD_DEFINE_ATOMIC_SWAP(void *, intptr_t, ptr)

#define OPAL_THREAD_ADD_FETCH32 opal_thread_add_fetch_32
#define OPAL_ATOMIC_ADD_FETCH32 opal_thread_add_fetch_32

#define OPAL_THREAD_AND_FETCH32 opal_thread_and_fetch_32
#define OPAL_ATOMIC_AND_FETCH32 opal_thread_and_fetch_32

#define OPAL_THREAD_OR_FETCH32 opal_thread_or_fetch_32
#define OPAL_ATOMIC_OR_FETCH32 opal_thread_or_fetch_32

#define OPAL_THREAD_XOR_FETCH32 opal_thread_xor_fetch_32
#define OPAL_ATOMIC_XOR_FETCH32 opal_thread_xor_fetch_32

#define OPAL_THREAD_ADD_FETCH_SIZE_T opal_thread_add_fetch_size_t
#define OPAL_ATOMIC_ADD_FETCH_SIZE_T opal_thread_add_fetch_size_t

#define OPAL_THREAD_SUB_FETCH_SIZE_T opal_thread_sub_fetch_size_t
#define OPAL_ATOMIC_SUB_FETCH_SIZE_T opal_thread_sub_fetch_size_t

#define OPAL_THREAD_FETCH_ADD32 opal_thread_fetch_add_32
#define OPAL_ATOMIC_FETCH_ADD32 opal_thread_fetch_add_32

#define OPAL_THREAD_FETCH_AND32 opal_thread_fetch_and_32
#define OPAL_ATOMIC_FETCH_AND32 opal_thread_fetch_and_32

#define OPAL_THREAD_FETCH_OR32 opal_thread_fetch_or_32
#define OPAL_ATOMIC_FETCH_OR32 opal_thread_fetch_or_32

#define OPAL_THREAD_FETCH_XOR32 opal_thread_fetch_xor_32
#define OPAL_ATOMIC_FETCH_XOR32 opal_thread_fetch_xor_32

#define OPAL_THREAD_FETCH_ADD_SIZE_T opal_thread_fetch_add_size_t
#define OPAL_ATOMIC_FETCH_ADD_SIZE_T opal_thread_fetch_add_size_t

#define OPAL_THREAD_FETCH_SUB_SIZE_T opal_thread_fetch_sub_size_t
#define OPAL_ATOMIC_FETCH_SUB_SIZE_T opal_thread_fetch_sub_size_t

#define OPAL_THREAD_COMPARE_EXCHANGE_STRONG_32 opal_thread_compare_exchange_strong_32
#define OPAL_ATOMIC_COMPARE_EXCHANGE_STRONG_32 opal_thread_compare_exchange_strong_32

#define OPAL_THREAD_COMPARE_EXCHANGE_STRONG_PTR(x, y, z) opal_thread_compare_exchange_strong_ptr ((volatile intptr_t *) x, (void *) y, (void *) z)
#define OPAL_ATOMIC_COMPARE_EXCHANGE_STRONG_PTR OPAL_THREAD_COMPARE_EXCHANGE_STRONG_PTR

#define OPAL_THREAD_SWAP_32 opal_thread_swap_32
#define OPAL_ATOMIC_SWAP_32 opal_thread_swap_32

#define OPAL_THREAD_SWAP_PTR(x, y) opal_thread_swap_ptr ((volatile intptr_t *) x, (void *) y)
#define OPAL_ATOMIC_SWAP_PTR OPAL_THREAD_SWAP_PTR

/* define 64-bit macros is 64-bit atomic math is available */
#if OPAL_HAVE_ATOMIC_MATH_64

OPAL_THREAD_DEFINE_ATOMIC_OP(int64_t, add, +, 64)
OPAL_THREAD_DEFINE_ATOMIC_OP(int64_t, and, &, 64)
OPAL_THREAD_DEFINE_ATOMIC_OP(int64_t, or, |, 64)
OPAL_THREAD_DEFINE_ATOMIC_OP(int64_t, xor, ^, 64)
OPAL_THREAD_DEFINE_ATOMIC_OP(int64_t, sub, -, 64)
OPAL_THREAD_DEFINE_ATOMIC_COMPARE_EXCHANGE(int64_t, int64_t, 64)
OPAL_THREAD_DEFINE_ATOMIC_SWAP(int64_t, int64_t, 64)

#define OPAL_THREAD_ADD_FETCH64 opal_thread_add_fetch_64
#define OPAL_ATOMIC_ADD_FETCH64 opal_thread_add_fetch_64

#define OPAL_THREAD_AND_FETCH64 opal_thread_and_fetch_64
#define OPAL_ATOMIC_AND_FETCH64 opal_thread_and_fetch_64

#define OPAL_THREAD_OR_FETCH64 opal_thread_or_fetch_64
#define OPAL_ATOMIC_OR_FETCH64 opal_thread_or_fetch_64

#define OPAL_THREAD_XOR_FETCH64 opal_thread_xor_fetch_64
#define OPAL_ATOMIC_XOR_FETCH64 opal_thread_xor_fetch_64

#define OPAL_THREAD_FETCH_ADD64 opal_thread_fetch_add_64
#define OPAL_ATOMIC_FETCH_ADD64 opal_thread_fetch_add_64

#define OPAL_THREAD_FETCH_AND64 opal_thread_fetch_and_64
#define OPAL_ATOMIC_FETCH_AND64 opal_thread_fetch_and_64

#define OPAL_THREAD_FETCH_OR64 opal_thread_fetch_or_64
#define OPAL_ATOMIC_FETCH_OR64 opal_thread_fetch_or_64

#define OPAL_THREAD_FETCH_XOR64 opal_thread_fetch_xor_64
#define OPAL_ATOMIC_FETCH_XOR64 opal_thread_fetch_xor_64

#define OPAL_THREAD_COMPARE_EXCHANGE_STRONG_64 opal_thread_compare_exchange_strong_64
#define OPAL_ATOMIC_COMPARE_EXCHANGE_STRONG_64 opal_thread_compare_exchange_strong_64

#define OPAL_THREAD_SWAP_64 opal_thread_swap_64
#define OPAL_ATOMIC_SWAP_64 opal_thread_swap_64

#endif

/* thread local storage */
#if OPAL_C_HAVE__THREAD_LOCAL
#define opal_thread_local _Thread_local
#define OPAL_HAVE_THREAD_LOCAL 1

#elif OPAL_C_HAVE___THREAD /* OPAL_C_HAVE__THREAD_LOCAL */
#define opal_thread_local __thread
#define OPAL_HAVE_THREAD_LOCAL 1
#endif /* OPAL_C_HAVE___THREAD */

#if !defined(OPAL_HAVE_THREAD_LOCAL)
#define OPAL_HAVE_THREAD_LOCAL 0
#endif /* !defined(OPAL_HAVE_THREAD_LOCAL) */

#endif /* !defined(OPAL_THREAD_USAGE_H) */
