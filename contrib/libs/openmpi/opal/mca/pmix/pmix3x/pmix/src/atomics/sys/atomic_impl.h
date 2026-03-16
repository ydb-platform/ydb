/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2014 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2010-2014 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2012-2018 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2018      Intel, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/* Inline C implementation of the functions defined in atomic.h */

#include <stdlib.h>

/**********************************************************************
 *
 * Atomic math operations
 *
 * All the architectures provide a compare_and_set atomic operations. If
 * they dont provide atomic additions and/or substractions then we can
 * define these operations using the atomic compare_and_set.
 *
 * Some architectures do not provide support for the 64 bits
 * atomic operations. Until we find a better solution let's just
 * undefine all those functions if there is no 64 bit compare-exchange
 *
 *********************************************************************/
#if PMIX_HAVE_ATOMIC_COMPARE_EXCHANGE_32

#if !defined(PMIX_HAVE_ATOMIC_MIN_32)
static inline int32_t pmix_atomic_fetch_min_32 (pmix_atomic_int32_t *addr, int32_t value)
{
    int32_t old = *addr;
    do {
        if (old <= value) {
            break;
        }
    } while (!pmix_atomic_compare_exchange_strong_32 (addr, &old, value));

    return old;
}

#define PMIX_HAVE_ATOMIC_MIN_32 1

#endif /* PMIX_HAVE_ATOMIC_MIN_32 */

#if !defined(PMIX_HAVE_ATOMIC_MAX_32)
static inline int32_t pmix_atomic_fetch_max_32 (pmix_atomic_int32_t *addr, int32_t value)
{
    int32_t old = *addr;
    do {
        if (old >= value) {
            break;
        }
    } while (!pmix_atomic_compare_exchange_strong_32 (addr, &old, value));

    return old;
}

#define PMIX_HAVE_ATOMIC_MAX_32 1
#endif /* PMIX_HAVE_ATOMIC_MAX_32 */

#define PMIX_ATOMIC_DEFINE_CMPXCG_OP(type, bits, operation, name)  \
    static inline type pmix_atomic_fetch_ ## name ## _ ## bits (pmix_atomic_ ## type *addr, type value) \
    {                                                                   \
        type oldval;                                                    \
        do {                                                            \
            oldval = *addr;                                             \
        } while (!pmix_atomic_compare_exchange_strong_ ## bits (addr, &oldval, oldval operation value)); \
                                                                        \
        return oldval;                                                  \
    }

#if !defined(PMIX_HAVE_ATOMIC_SWAP_32)
#define PMIX_HAVE_ATOMIC_SWAP_32 1
static inline int32_t pmix_atomic_swap_32(pmix_atomic_int32_t *addr,
                                          int32_t newval)
{
    int32_t old = *addr;
    do {
    } while (!pmix_atomic_compare_exchange_strong_32 (addr, &old, newval));

    return old;
}
#endif /* PMIX_HAVE_ATOMIC_SWAP_32 */

#if !defined(PMIX_HAVE_ATOMIC_ADD_32)
#define PMIX_HAVE_ATOMIC_ADD_32 1

PMIX_ATOMIC_DEFINE_CMPXCG_OP(int32_t, 32, +, add)

#endif  /* PMIX_HAVE_ATOMIC_ADD_32 */

#if !defined(PMIX_HAVE_ATOMIC_AND_32)
#define PMIX_HAVE_ATOMIC_AND_32 1

PMIX_ATOMIC_DEFINE_CMPXCG_OP(int32_t, 32, &, and)

#endif  /* PMIX_HAVE_ATOMIC_AND_32 */

#if !defined(PMIX_HAVE_ATOMIC_OR_32)
#define PMIX_HAVE_ATOMIC_OR_32 1

PMIX_ATOMIC_DEFINE_CMPXCG_OP(int32_t, 32, |, or)

#endif  /* PMIX_HAVE_ATOMIC_OR_32 */

#if !defined(PMIX_HAVE_ATOMIC_XOR_32)
#define PMIX_HAVE_ATOMIC_XOR_32 1

PMIX_ATOMIC_DEFINE_CMPXCG_OP(int32_t, 32, ^, xor)

#endif  /* PMIX_HAVE_ATOMIC_XOR_32 */


#if !defined(PMIX_HAVE_ATOMIC_SUB_32)
#define PMIX_HAVE_ATOMIC_SUB_32 1

PMIX_ATOMIC_DEFINE_CMPXCG_OP(int32_t, 32, -, sub)

#endif  /* PMIX_HAVE_ATOMIC_SUB_32 */

#endif /* PMIX_HAVE_ATOMIC_COMPARE_EXCHANGE_32 */


#if PMIX_HAVE_ATOMIC_COMPARE_EXCHANGE_64

#if !defined(PMIX_HAVE_ATOMIC_MIN_64)
static inline int64_t pmix_atomic_fetch_min_64 (pmix_atomic_int64_t *addr, int64_t value)
{
    int64_t old = *addr;
    do {
        if (old <= value) {
            break;
        }
    } while (!pmix_atomic_compare_exchange_strong_64 (addr, &old, value));

    return old;
}

#define PMIX_HAVE_ATOMIC_MIN_64 1

#endif /* PMIX_HAVE_ATOMIC_MIN_64 */

#if !defined(PMIX_HAVE_ATOMIC_MAX_64)
static inline int64_t pmix_atomic_fetch_max_64 (pmix_atomic_int64_t *addr, int64_t value)
{
    int64_t old = *addr;
    do {
        if (old >= value) {
            break;
        }
    } while (!pmix_atomic_compare_exchange_strong_64 (addr, &old, value));

    return old;
}

#define PMIX_HAVE_ATOMIC_MAX_64 1
#endif /* PMIX_HAVE_ATOMIC_MAX_64 */

#if !defined(PMIX_HAVE_ATOMIC_SWAP_64)
#define PMIX_HAVE_ATOMIC_SWAP_64 1
static inline int64_t pmix_atomic_swap_64(pmix_atomic_int64_t *addr,
                                          int64_t newval)
{
    int64_t old = *addr;
    do {
    } while (!pmix_atomic_compare_exchange_strong_64 (addr, &old, newval));

    return old;
}
#endif /* PMIX_HAVE_ATOMIC_SWAP_64 */

#if !defined(PMIX_HAVE_ATOMIC_ADD_64)
#define PMIX_HAVE_ATOMIC_ADD_64 1

PMIX_ATOMIC_DEFINE_CMPXCG_OP(int64_t, 64, +, add)

#endif  /* PMIX_HAVE_ATOMIC_ADD_64 */

#if !defined(PMIX_HAVE_ATOMIC_AND_64)
#define PMIX_HAVE_ATOMIC_AND_64 1

PMIX_ATOMIC_DEFINE_CMPXCG_OP(int64_t, 64, &, and)

#endif  /* PMIX_HAVE_ATOMIC_AND_64 */

#if !defined(PMIX_HAVE_ATOMIC_OR_64)
#define PMIX_HAVE_ATOMIC_OR_64 1

PMIX_ATOMIC_DEFINE_CMPXCG_OP(int64_t, 64, |, or)

#endif  /* PMIX_HAVE_ATOMIC_OR_64 */

#if !defined(PMIX_HAVE_ATOMIC_XOR_64)
#define PMIX_HAVE_ATOMIC_XOR_64 1

PMIX_ATOMIC_DEFINE_CMPXCG_OP(int64_t, 64, ^, xor)

#endif  /* PMIX_HAVE_ATOMIC_XOR_64 */

#if !defined(PMIX_HAVE_ATOMIC_SUB_64)
#define PMIX_HAVE_ATOMIC_SUB_64 1

PMIX_ATOMIC_DEFINE_CMPXCG_OP(int64_t, 64, -, sub)

#endif  /* PMIX_HAVE_ATOMIC_SUB_64 */

#else

#if !defined(PMIX_HAVE_ATOMIC_ADD_64)
#define PMIX_HAVE_ATOMIC_ADD_64 0
#endif

#if !defined(PMIX_HAVE_ATOMIC_SUB_64)
#define PMIX_HAVE_ATOMIC_SUB_64 0
#endif

#endif  /* PMIX_HAVE_ATOMIC_COMPARE_EXCHANGE_64 */

#if (PMIX_HAVE_ATOMIC_COMPARE_EXCHANGE_32 || PMIX_HAVE_ATOMIC_COMPARE_EXCHANGE_64)

#if PMIX_HAVE_ATOMIC_COMPARE_EXCHANGE_32 && PMIX_HAVE_ATOMIC_COMPARE_EXCHANGE_64
#define PMIX_ATOMIC_DEFINE_CMPXCG_XX(semantics)                         \
    static inline bool                                                  \
    pmix_atomic_compare_exchange_strong ## semantics ## xx (pmix_atomic_intptr_t* addr, intptr_t *oldval, \
                                                            int64_t newval, const size_t length) \
    {                                                                   \
        switch (length) {                                               \
        case 4:                                                         \
            return pmix_atomic_compare_exchange_strong_32 ((pmix_atomic_int32_t *) addr, \
                                                           (int32_t *) oldval, (int32_t) newval); \
        case 8:                                                         \
            return pmix_atomic_compare_exchange_strong_64 ((pmix_atomic_int64_t *) addr, \
                                                           (int64_t *) oldval, (int64_t) newval); \
        }                                                               \
        abort();                                                        \
    }
#elif PMIX_HAVE_ATOMIC_COMPARE_EXCHANGE_32
#define PMIX_ATOMIC_DEFINE_CMPXCG_XX(semantics)                         \
    static inline bool                                                  \
    pmix_atomic_compare_exchange_strong ## semantics ## xx (pmix_atomic_intptr_t* addr, intptr_t *oldval, \
                                                            int64_t newval, const size_t length) \
    {                                                                   \
        switch (length) {                                               \
        case 4:                                                         \
            return pmix_atomic_compare_exchange_strong_32 ((pmix_atomic_int32_t *) addr, \
                                                           (int32_t *) oldval, (int32_t) newval); \
        }                                                               \
        abort();                                                        \
    }
#else
#error "Platform does not have required atomic compare-and-swap functionality"
#endif

PMIX_ATOMIC_DEFINE_CMPXCG_XX(_)
PMIX_ATOMIC_DEFINE_CMPXCG_XX(_acq_)
PMIX_ATOMIC_DEFINE_CMPXCG_XX(_rel_)

#if SIZEOF_VOID_P == 4 && PMIX_HAVE_ATOMIC_COMPARE_EXCHANGE_32
#define PMIX_ATOMIC_DEFINE_CMPXCG_PTR_XX(semantics)                     \
    static inline bool                                                  \
        pmix_atomic_compare_exchange_strong ## semantics ## ptr (pmix_atomic_intptr_t* addr, intptr_t *oldval, intptr_t newval) \
    {                                                                   \
        return pmix_atomic_compare_exchange_strong_32 ((pmix_atomic_int32_t *) addr, (int32_t *) oldval, (int32_t) newval); \
    }
#elif SIZEOF_VOID_P == 8 && PMIX_HAVE_ATOMIC_COMPARE_EXCHANGE_64
#define PMIX_ATOMIC_DEFINE_CMPXCG_PTR_XX(semantics)                     \
    static inline bool                                                  \
        pmix_atomic_compare_exchange_strong ## semantics ## ptr (pmix_atomic_intptr_t* addr, intptr_t *oldval, intptr_t newval) \
    {                                                                   \
        return pmix_atomic_compare_exchange_strong_64 ((pmix_atomic_int64_t *) addr, (int64_t *) oldval, (int64_t) newval); \
    }
#else
#error "Can not define pmix_atomic_compare_exchange_strong_ptr with existing atomics"
#endif

PMIX_ATOMIC_DEFINE_CMPXCG_PTR_XX(_)
PMIX_ATOMIC_DEFINE_CMPXCG_PTR_XX(_acq_)
PMIX_ATOMIC_DEFINE_CMPXCG_PTR_XX(_rel_)

#endif /* (PMIX_HAVE_ATOMIC_COMPARE_EXCHANGE_32 || PMIX_HAVE_ATOMIC_COMPARE_EXCHANGE_64) */


#if (PMIX_HAVE_ATOMIC_SWAP_32 || PMIX_HAVE_ATOMIC_SWAP_64)

#if SIZEOF_VOID_P == 4 && PMIX_HAVE_ATOMIC_SWAP_32
#define pmix_atomic_swap_ptr(addr, value) (intptr_t) pmix_atomic_swap_32((pmix_atomic_int32_t *) addr, (int32_t) value)
#elif SIZEOF_VOID_P == 8 && PMIX_HAVE_ATOMIC_SWAP_64
#define pmix_atomic_swap_ptr(addr, value) (intptr_t) pmix_atomic_swap_64((pmix_atomic_int64_t *) addr, (int64_t) value)
#endif

#endif /* (PMIX_HAVE_ATOMIC_SWAP_32 || PMIX_HAVE_ATOMIC_SWAP_64) */

#if (PMIX_HAVE_ATOMIC_LLSC_32 || PMIX_HAVE_ATOMIC_LLSC_64)

#if SIZEOF_VOID_P == 4 && PMIX_HAVE_ATOMIC_LLSC_32

#define pmix_atomic_ll_ptr(addr, ret) pmix_atomic_ll_32((pmix_atomic_int32_t *) (addr), ret)
#define pmix_atomic_sc_ptr(addr, value, ret) pmix_atomic_sc_32((pmix_atomic_int32_t *) (addr), (intptr_t) (value), ret)

#define PMIX_HAVE_ATOMIC_LLSC_PTR 1

#elif SIZEOF_VOID_P == 8 && PMIX_HAVE_ATOMIC_LLSC_64

#define pmix_atomic_ll_ptr(addr, ret) pmix_atomic_ll_64((pmix_atomic_int64_t *) (addr), ret)
#define pmix_atomic_sc_ptr(addr, value, ret) pmix_atomic_sc_64((pmix_atomic_int64_t *) (addr), (intptr_t) (value), ret)

#define PMIX_HAVE_ATOMIC_LLSC_PTR 1

#endif

#endif /* (PMIX_HAVE_ATOMIC_LLSC_32 || PMIX_HAVE_ATOMIC_LLSC_64)*/

#if !defined(PMIX_HAVE_ATOMIC_LLSC_PTR)
#define PMIX_HAVE_ATOMIC_LLSC_PTR 0
#endif

#if PMIX_HAVE_ATOMIC_MATH_32 || PMIX_HAVE_ATOMIC_MATH_64

static inline void
    pmix_atomic_add_xx(pmix_atomic_intptr_t* addr, int32_t value, size_t length)
{
   switch( length ) {
#if PMIX_HAVE_ATOMIC_ADD_32
   case 4:
       (void) pmix_atomic_fetch_add_32( (pmix_atomic_int32_t*)addr, (int32_t)value );
      break;
#endif  /* PMIX_HAVE_ATOMIC_COMPARE_EXCHANGE_32 */

#if PMIX_HAVE_ATOMIC_ADD_64
   case 8:
       (void) pmix_atomic_fetch_add_64( (pmix_atomic_int64_t*)addr, (int64_t)value );
      break;
#endif  /* PMIX_HAVE_ATOMIC_ADD_64 */
   default:
       /* This should never happen, so deliberately abort (hopefully
          leaving a corefile for analysis) */
       abort();
   }
}


static inline void
pmix_atomic_sub_xx(pmix_atomic_intptr_t* addr, int32_t value, size_t length)
{
   switch( length ) {
#if PMIX_HAVE_ATOMIC_SUB_32
   case 4:
       (void) pmix_atomic_fetch_sub_32( (pmix_atomic_int32_t*)addr, (int32_t)value );
      break;
#endif  /* PMIX_HAVE_ATOMIC_SUB_32 */

#if PMIX_HAVE_ATOMIC_SUB_64
   case 8:
       (void) pmix_atomic_fetch_sub_64( (pmix_atomic_int64_t*)addr, (int64_t)value );
      break;
#endif  /* PMIX_HAVE_ATOMIC_SUB_64 */
   default:
       /* This should never happen, so deliberately abort (hopefully
          leaving a corefile for analysis) */
       abort();
   }
}

#define PMIX_ATOMIC_DEFINE_OP_FETCH(op, operation, type, ptr_type, suffix) \
    static inline type pmix_atomic_ ## op ## _fetch_ ## suffix (pmix_atomic_ ## ptr_type *addr, type value) \
    {                                                                   \
        return pmix_atomic_fetch_ ## op ## _ ## suffix (addr, value) operation value; \
    }

PMIX_ATOMIC_DEFINE_OP_FETCH(add, +, int32_t, int32_t, 32)
PMIX_ATOMIC_DEFINE_OP_FETCH(and, &, int32_t, int32_t, 32)
PMIX_ATOMIC_DEFINE_OP_FETCH(or, |, int32_t, int32_t, 32)
PMIX_ATOMIC_DEFINE_OP_FETCH(xor, ^, int32_t, int32_t, 32)
PMIX_ATOMIC_DEFINE_OP_FETCH(sub, -, int32_t, int32_t, 32)

static inline int32_t pmix_atomic_min_fetch_32 (pmix_atomic_int32_t *addr, int32_t value)
{
    int32_t old = pmix_atomic_fetch_min_32 (addr, value);
    return old <= value ? old : value;
}

static inline int32_t pmix_atomic_max_fetch_32 (pmix_atomic_int32_t *addr, int32_t value)
{
    int32_t old = pmix_atomic_fetch_max_32 (addr, value);
    return old >= value ? old : value;
}

#if PMIX_HAVE_ATOMIC_MATH_64
PMIX_ATOMIC_DEFINE_OP_FETCH(add, +, int64_t, int64_t, 64)
PMIX_ATOMIC_DEFINE_OP_FETCH(and, &, int64_t, int64_t, 64)
PMIX_ATOMIC_DEFINE_OP_FETCH(or, |, int64_t, int64_t, 64)
PMIX_ATOMIC_DEFINE_OP_FETCH(xor, ^, int64_t, int64_t, 64)
PMIX_ATOMIC_DEFINE_OP_FETCH(sub, -, int64_t, int64_t, 64)

static inline int64_t pmix_atomic_min_fetch_64 (pmix_atomic_int64_t *addr, int64_t value)
{
    int64_t old = pmix_atomic_fetch_min_64 (addr, value);
    return old <= value ? old : value;
}

static inline int64_t pmix_atomic_max_fetch_64 (pmix_atomic_int64_t *addr, int64_t value)
{
    int64_t old = pmix_atomic_fetch_max_64 (addr, value);
    return old >= value ? old : value;
}

#endif

static inline intptr_t pmix_atomic_fetch_add_ptr( pmix_atomic_intptr_t* addr,
                                           void* delta )
{
#if SIZEOF_VOID_P == 4 && PMIX_HAVE_ATOMIC_ADD_32
    return pmix_atomic_fetch_add_32((pmix_atomic_int32_t*) addr, (unsigned long) delta);
#elif SIZEOF_VOID_P == 8 && PMIX_HAVE_ATOMIC_ADD_64
    return pmix_atomic_fetch_add_64((pmix_atomic_int64_t*) addr, (unsigned long) delta);
#else
    abort ();
    return 0;
#endif
}

static inline intptr_t pmix_atomic_add_fetch_ptr( pmix_atomic_intptr_t* addr,
                                           void* delta )
{
#if SIZEOF_VOID_P == 4 && PMIX_HAVE_ATOMIC_ADD_32
    return pmix_atomic_add_fetch_32((pmix_atomic_int32_t*) addr, (unsigned long) delta);
#elif SIZEOF_VOID_P == 8 && PMIX_HAVE_ATOMIC_ADD_64
    return pmix_atomic_add_fetch_64((pmix_atomic_int64_t*) addr, (unsigned long) delta);
#else
    abort ();
    return 0;
#endif
}

static inline intptr_t pmix_atomic_fetch_sub_ptr( pmix_atomic_intptr_t* addr,
                                           void* delta )
{
#if SIZEOF_VOID_P == 4 && PMIX_HAVE_ATOMIC_SUB_32
    return pmix_atomic_fetch_sub_32((pmix_atomic_int32_t*) addr, (unsigned long) delta);
#elif SIZEOF_VOID_P == 8 && PMIX_HAVE_ATOMIC_SUB_32
    return pmix_atomic_fetch_sub_64((pmix_atomic_int64_t*) addr, (unsigned long) delta);
#else
    abort();
    return 0;
#endif
}

static inline intptr_t pmix_atomic_sub_fetch_ptr( pmix_atomic_intptr_t* addr,
                                           void* delta )
{
#if SIZEOF_VOID_P == 4 && PMIX_HAVE_ATOMIC_SUB_32
    return pmix_atomic_sub_fetch_32((pmix_atomic_int32_t*) addr, (unsigned long) delta);
#elif SIZEOF_VOID_P == 8 && PMIX_HAVE_ATOMIC_SUB_32
    return pmix_atomic_sub_fetch_64((pmix_atomic_int64_t*) addr, (unsigned long) delta);
#else
    abort();
    return 0;
#endif
}

#endif /* PMIX_HAVE_ATOMIC_MATH_32 || PMIX_HAVE_ATOMIC_MATH_64 */

/**********************************************************************
 *
 * Atomic spinlocks
 *
 *********************************************************************/
#ifdef PMIX_NEED_INLINE_ATOMIC_SPINLOCKS

/*
 * Lock initialization function. It set the lock to UNLOCKED.
 */
static inline void
pmix_atomic_lock_init( pmix_atomic_lock_t* lock, int32_t value )
{
   lock->u.lock = value;
}


static inline int
pmix_atomic_trylock(pmix_atomic_lock_t *lock)
{
    int32_t unlocked = PMIX_ATOMIC_LOCK_UNLOCKED;
    bool ret = pmix_atomic_compare_exchange_strong_32 (&lock->u.lock, &unlocked, PMIX_ATOMIC_LOCK_LOCKED);
    return (ret == false) ? 1 : 0;
}


static inline void
pmix_atomic_lock(pmix_atomic_lock_t *lock)
{
    while (pmix_atomic_trylock (lock)) {
        while (lock->u.lock == PMIX_ATOMIC_LOCK_LOCKED) {
            /* spin */ ;
        }
    }
}


static inline void
pmix_atomic_unlock(pmix_atomic_lock_t *lock)
{
   pmix_atomic_wmb();
   lock->u.lock=PMIX_ATOMIC_LOCK_UNLOCKED;
}

#endif /* PMIX_HAVE_ATOMIC_SPINLOCKS */
