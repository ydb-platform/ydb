/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2013 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2011      Sandia National Laboratories. All rights reserved.
 * Copyright (c) 2014-2018 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2016-2017 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2018      Triad National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2018      Intel, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef PMIX_SYS_ARCH_ATOMIC_H
#define PMIX_SYS_ARCH_ATOMIC_H 1

/**********************************************************************
 *
 * Memory Barriers
 *
 *********************************************************************/
#define PMIX_HAVE_ATOMIC_MEM_BARRIER 1

#define PMIX_HAVE_ATOMIC_MATH_32 1
#define PMIX_HAVE_ATOMIC_COMPARE_EXCHANGE_32 1
#define PMIX_HAVE_ATOMIC_ADD_32 1
#define PMIX_HAVE_ATOMIC_AND_32 1
#define PMIX_HAVE_ATOMIC_OR_32 1
#define PMIX_HAVE_ATOMIC_XOR_32 1
#define PMIX_HAVE_ATOMIC_SUB_32 1
#define PMIX_HAVE_ATOMIC_SWAP_32 1
#define PMIX_HAVE_ATOMIC_MATH_64 1
#define PMIX_HAVE_ATOMIC_COMPARE_EXCHANGE_64 1
#define PMIX_HAVE_ATOMIC_ADD_64 1
#define PMIX_HAVE_ATOMIC_AND_64 1
#define PMIX_HAVE_ATOMIC_OR_64 1
#define PMIX_HAVE_ATOMIC_XOR_64 1
#define PMIX_HAVE_ATOMIC_SUB_64 1
#define PMIX_HAVE_ATOMIC_SWAP_64 1


static inline void pmix_atomic_mb(void)
{
    __atomic_thread_fence (__ATOMIC_SEQ_CST);
}

static inline void pmix_atomic_rmb(void)
{
#if PMIX_ASSEMBLY_ARCH == PMIX_X86_64
    /* work around a bug in older gcc versions where ACQUIRE seems to get
     * treated as a no-op instead of being equivalent to
     * __asm__ __volatile__("": : :"memory") */
    __atomic_thread_fence (__ATOMIC_SEQ_CST);
#else
    __atomic_thread_fence (__ATOMIC_ACQUIRE);
#endif
}

static inline void pmix_atomic_wmb(void)
{
    __atomic_thread_fence (__ATOMIC_RELEASE);
}

#define PMIXMB() pmix_atomic_mb()

/**********************************************************************
 *
 * Atomic math operations
 *
 *********************************************************************/

/*
 * Suppress numerous (spurious ?) warnings from Oracle Studio compilers
 * see https://community.oracle.com/thread/3968347
 */ 
#if defined(__SUNPRO_C) || defined(__SUNPRO_CC)
#pragma error_messages(off, E_ARG_INCOMPATIBLE_WITH_ARG_L)
#endif

static inline bool pmix_atomic_compare_exchange_strong_acq_32 (pmix_atomic_int32_t *addr, int32_t *oldval, int32_t newval)
{
    return __atomic_compare_exchange_n (addr, oldval, newval, false, __ATOMIC_ACQUIRE, __ATOMIC_RELAXED);
}


static inline bool pmix_atomic_compare_exchange_strong_rel_32 (pmix_atomic_int32_t *addr, int32_t *oldval, int32_t newval)
{
    return __atomic_compare_exchange_n (addr, oldval, newval, false, __ATOMIC_RELEASE, __ATOMIC_RELAXED);
}

static inline bool pmix_atomic_compare_exchange_strong_32 (pmix_atomic_int32_t *addr, int32_t *oldval, int32_t newval)
{
    return __atomic_compare_exchange_n (addr, oldval, newval, false, __ATOMIC_ACQUIRE, __ATOMIC_RELAXED);
}

static inline int32_t pmix_atomic_swap_32 (pmix_atomic_int32_t *addr, int32_t newval)
{
    int32_t oldval;
    __atomic_exchange (addr, &newval, &oldval, __ATOMIC_RELAXED);
    return oldval;
}

static inline int32_t pmix_atomic_fetch_add_32(pmix_atomic_int32_t *addr, int32_t delta)
{
    return __atomic_fetch_add (addr, delta, __ATOMIC_RELAXED);
}

static inline int32_t pmix_atomic_fetch_and_32(pmix_atomic_int32_t *addr, int32_t value)
{
    return __atomic_fetch_and (addr, value, __ATOMIC_RELAXED);
}

static inline int32_t pmix_atomic_fetch_or_32(pmix_atomic_int32_t *addr, int32_t value)
{
    return __atomic_fetch_or (addr, value, __ATOMIC_RELAXED);
}

static inline int32_t pmix_atomic_fetch_xor_32(pmix_atomic_int32_t *addr, int32_t value)
{
    return __atomic_fetch_xor (addr, value, __ATOMIC_RELAXED);
}

static inline int32_t pmix_atomic_fetch_sub_32(pmix_atomic_int32_t *addr, int32_t delta)
{
    return __atomic_fetch_sub (addr, delta, __ATOMIC_RELAXED);
}

static inline bool pmix_atomic_compare_exchange_strong_acq_64 (pmix_atomic_int64_t *addr, int64_t *oldval, int64_t newval)
{
    return __atomic_compare_exchange_n (addr, oldval, newval, false, __ATOMIC_ACQUIRE, __ATOMIC_RELAXED);
}

static inline bool pmix_atomic_compare_exchange_strong_rel_64 (pmix_atomic_int64_t *addr, int64_t *oldval, int64_t newval)
{
    return __atomic_compare_exchange_n (addr, oldval, newval, false, __ATOMIC_RELEASE, __ATOMIC_RELAXED);
}


static inline bool pmix_atomic_compare_exchange_strong_64 (pmix_atomic_int64_t *addr, int64_t *oldval, int64_t newval)
{
    return __atomic_compare_exchange_n (addr, oldval, newval, false, __ATOMIC_ACQUIRE, __ATOMIC_RELAXED);
}

static inline int64_t pmix_atomic_swap_64 (pmix_atomic_int64_t *addr, int64_t newval)
{
    int64_t oldval;
    __atomic_exchange (addr, &newval, &oldval, __ATOMIC_RELAXED);
    return oldval;
}

static inline int64_t pmix_atomic_fetch_add_64(pmix_atomic_int64_t *addr, int64_t delta)
{
    return __atomic_fetch_add (addr, delta, __ATOMIC_RELAXED);
}

static inline int64_t pmix_atomic_fetch_and_64(pmix_atomic_int64_t *addr, int64_t value)
{
    return __atomic_fetch_and (addr, value, __ATOMIC_RELAXED);
}

static inline int64_t pmix_atomic_fetch_or_64(pmix_atomic_int64_t *addr, int64_t value)
{
    return __atomic_fetch_or (addr, value, __ATOMIC_RELAXED);
}

static inline int64_t pmix_atomic_fetch_xor_64(pmix_atomic_int64_t *addr, int64_t value)
{
    return __atomic_fetch_xor (addr, value, __ATOMIC_RELAXED);
}

static inline int64_t pmix_atomic_fetch_sub_64(pmix_atomic_int64_t *addr, int64_t delta)
{
    return __atomic_fetch_sub (addr, delta, __ATOMIC_RELAXED);
}

#if PMIX_HAVE_GCC_BUILTIN_CSWAP_INT128

#define PMIX_HAVE_ATOMIC_COMPARE_EXCHANGE_128 1

static inline bool pmix_atomic_compare_exchange_strong_128 (pmix_atomic_int128_t *addr,
                                                            pmix_int128_t *oldval, pmix_int128_t newval)
{
    return __atomic_compare_exchange_n (addr, oldval, newval, false,
                                        __ATOMIC_ACQUIRE, __ATOMIC_RELAXED);
}

#elif defined(PMIX_HAVE_SYNC_BUILTIN_CSWAP_INT128) && PMIX_HAVE_SYNC_BUILTIN_CSWAP_INT128

#define PMIX_HAVE_ATOMIC_COMPARE_EXCHANGE_128 1

/* __atomic version is not lock-free so use legacy __sync version */

static inline bool pmix_atomic_compare_exchange_strong_128 (pmix_atomic_pmix_int128_t *addr,
                                                            pmix_int128_t *oldval, pmix_int128_t newval)
{
    pmix_int128_t prev = __sync_val_compare_and_swap (addr, *oldval, newval);
    bool ret = prev == *oldval;
    *oldval = prev;
    return ret;
}

#endif

#if defined(__HLE__)

#include <immintrin.h>

#define PMIX_HAVE_ATOMIC_SPINLOCKS 1

static inline void pmix_atomic_lock_init (pmix_atomic_lock_t* lock, int32_t value)
{
   lock->u.lock = value;
}

static inline int pmix_atomic_trylock(pmix_atomic_lock_t *lock)
{
    int ret = __atomic_exchange_n (&lock->u.lock, PMIX_ATOMIC_LOCK_LOCKED,
                                   __ATOMIC_ACQUIRE | __ATOMIC_HLE_ACQUIRE);
    if (PMIX_ATOMIC_LOCK_LOCKED == ret) {
        /* abort the transaction */
        _mm_pause ();
        return 1;
    }

    return 0;
}

static inline void pmix_atomic_lock (pmix_atomic_lock_t *lock)
{
    while (PMIX_ATOMIC_LOCK_LOCKED == __atomic_exchange_n (&lock->u.lock, PMIX_ATOMIC_LOCK_LOCKED,
                                                      __ATOMIC_ACQUIRE | __ATOMIC_HLE_ACQUIRE)) {
        /* abort the transaction */
        _mm_pause ();
    }
}

static inline void pmix_atomic_unlock (pmix_atomic_lock_t *lock)
{
    __atomic_store_n (&lock->u.lock, PMIX_ATOMIC_LOCK_UNLOCKED,
                       __ATOMIC_RELEASE | __ATOMIC_HLE_RELEASE);
}

#endif

#if defined(__SUNPRO_C) || defined(__SUNPRO_CC)
#pragma error_messages(default, E_ARG_INCOMPATIBLE_WITH_ARG_L)
#endif

#endif /* ! PMIX_SYS_ARCH_ATOMIC_H */
