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
 * Copyright (c) 2017      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
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

static inline void pmix_atomic_mb(void)
{
    __sync_synchronize();
}

static inline void pmix_atomic_rmb(void)
{
    __sync_synchronize();
}

static inline void pmix_atomic_wmb(void)
{
    __sync_synchronize();
}

#define PMIXMB() pmix_atomic_mb()

/**********************************************************************
 *
 * Atomic math operations
 *
 *********************************************************************/

#define PMIX_HAVE_ATOMIC_COMPARE_EXCHANGE_32 1

static inline bool pmix_atomic_compare_exchange_strong_32 (pmix_atomic_int32_t *addr, int32_t *oldval, int32_t newval)
{
    int32_t prev = __sync_val_compare_and_swap (addr, *oldval, newval);
    bool ret = prev == *oldval;
    *oldval = prev;
    return ret;
}

#define pmix_atomic_compare_exchange_strong_acq_32 pmix_atomic_compare_exchange_strong_32
#define pmix_atomic_compare_exchange_strong_rel_32 pmix_atomic_compare_exchange_strong_32

#define PMIX_HAVE_ATOMIC_MATH_32 1

#define PMIX_HAVE_ATOMIC_ADD_32 1
static inline int32_t pmix_atomic_fetch_add_32(pmix_atomic_int32_t *addr, int32_t delta)
{
    return __sync_fetch_and_add(addr, delta);
}

#define PMIX_HAVE_ATOMIC_AND_32 1
static inline int32_t pmix_atomic_fetch_and_32(pmix_atomic_int32_t *addr, int32_t value)
{
    return __sync_fetch_and_and(addr, value);
}

#define PMIX_HAVE_ATOMIC_OR_32 1
static inline int32_t pmix_atomic_fetch_or_32(pmix_atomic_int32_t *addr, int32_t value)
{
    return __sync_fetch_and_or(addr, value);
}

#define PMIX_HAVE_ATOMIC_XOR_32 1
static inline int32_t pmix_atomic_fetch_xor_32(pmix_atomic_int32_t *addr, int32_t value)
{
    return __sync_fetch_and_xor(addr, value);
}

#define PMIX_HAVE_ATOMIC_SUB_32 1
static inline int32_t pmix_atomic_fetch_sub_32(pmix_atomic_int32_t *addr, int32_t delta)
{
    return __sync_fetch_and_sub(addr, delta);
}

#if PMIX_ASM_SYNC_HAVE_64BIT

#define PMIX_HAVE_ATOMIC_COMPARE_EXCHANGE_64 1

static inline bool pmix_atomic_compare_exchange_strong_64 (pmix_atomic_int64_t *addr, int64_t *oldval, int64_t newval)
{
    int64_t prev = __sync_val_compare_and_swap (addr, *oldval, newval);
    bool ret = prev == *oldval;
    *oldval = prev;
    return ret;
}

#define pmix_atomic_compare_exchange_strong_acq_64 pmix_atomic_compare_exchange_strong_64
#define pmix_atomic_compare_exchange_strong_rel_64 pmix_atomic_compare_exchange_strong_64

#define PMIX_HAVE_ATOMIC_MATH_64 1
#define PMIX_HAVE_ATOMIC_ADD_64 1
static inline int64_t pmix_atomic_fetch_add_64(pmix_atomic_int64_t *addr, int64_t delta)
{
    return __sync_fetch_and_add(addr, delta);
}

#define PMIX_HAVE_ATOMIC_AND_64 1
static inline int64_t pmix_atomic_fetch_and_64(pmix_atomic_int64_t *addr, int64_t value)
{
    return __sync_fetch_and_and(addr, value);
}

#define PMIX_HAVE_ATOMIC_OR_64 1
static inline int64_t pmix_atomic_fetch_or_64(pmix_atomic_int64_t *addr, int64_t value)
{
    return __sync_fetch_and_or(addr, value);
}

#define PMIX_HAVE_ATOMIC_XOR_64 1
static inline int64_t pmix_atomic_fetch_xor_64(pmix_atomic_int64_t *addr, int64_t value)
{
    return __sync_fetch_and_xor(addr, value);
}

#define PMIX_HAVE_ATOMIC_SUB_64 1
static inline int64_t pmix_atomic_fetch_sub_64(pmix_atomic_int64_t *addr, int64_t delta)
{
    return __sync_fetch_and_sub(addr, delta);
}

#endif

#if PMIX_HAVE_SYNC_BUILTIN_CSWAP_INT128
static inline bool pmix_atomic_compare_exchange_strong_128 (pmix_atomic_int128_t *addr,
                                                            pmix_int128_t *oldval, pmix_int128_t newval)
{
    pmix_int128_t prev = __sync_val_compare_and_swap (addr, *oldval, newval);
    bool ret = prev == *oldval;
    *oldval = prev;
    return ret;
}

#define PMIX_HAVE_ATOMIC_COMPARE_EXCHANGE_128 1

#endif

#endif /* ! PMIX_SYS_ARCH_ATOMIC_H */
