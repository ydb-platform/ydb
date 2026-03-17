/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2010 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007      Sun Microsystems, Inc.  All rights reserverd.
 * Copyright (c) 2012-2018 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2016-2017 Research Organization for Information Science
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

/*
 * On x86_64, we use cmpxchg.
 */


#define SMPLOCK "lock; "
#define PMIXMB() __asm__ __volatile__("": : :"memory")


/**********************************************************************
 *
 * Define constants for AMD64 / x86_64 / EM64T / ...
 *
 *********************************************************************/
#define PMIX_HAVE_ATOMIC_MEM_BARRIER 1

#define PMIX_HAVE_ATOMIC_COMPARE_EXCHANGE_32 1

#define PMIX_HAVE_ATOMIC_COMPARE_EXCHANGE_64 1

/**********************************************************************
 *
 * Memory Barriers
 *
 *********************************************************************/
#if PMIX_GCC_INLINE_ASSEMBLY

static inline void pmix_atomic_mb(void)
{
    PMIXMB();
}


static inline void pmix_atomic_rmb(void)
{
    PMIXMB();
}


static inline void pmix_atomic_wmb(void)
{
    PMIXMB();
}

static inline void pmix_atomic_isync(void)
{
}

#endif /* PMIX_GCC_INLINE_ASSEMBLY */


/**********************************************************************
 *
 * Atomic math operations
 *
 *********************************************************************/
#if PMIX_GCC_INLINE_ASSEMBLY

static inline bool pmix_atomic_compare_exchange_strong_32 (pmix_atomic_int32_t *addr, int32_t *oldval, int32_t newval)
{
   unsigned char ret;
   __asm__ __volatile__ (
                       SMPLOCK "cmpxchgl %3,%2   \n\t"
                               "sete     %0      \n\t"
                       : "=qm" (ret), "+a" (*oldval), "+m" (*addr)
                       : "q"(newval)
                       : "memory", "cc");

   return (bool) ret;
}

#endif /* PMIX_GCC_INLINE_ASSEMBLY */

#define pmix_atomic_compare_exchange_strong_acq_32 pmix_atomic_compare_exchange_strong_32
#define pmix_atomic_compare_exchange_strong_rel_32 pmix_atomic_compare_exchange_strong_32

#if PMIX_GCC_INLINE_ASSEMBLY

static inline bool pmix_atomic_compare_exchange_strong_64 (pmix_atomic_int64_t *addr, int64_t *oldval, int64_t newval)
{
   unsigned char ret;
   __asm__ __volatile__ (
                       SMPLOCK "cmpxchgq %3,%2   \n\t"
                               "sete     %0      \n\t"
                       : "=qm" (ret), "+a" (*oldval), "+m" (*((pmix_atomic_long_t *)addr))
                       : "q"(newval)
                       : "memory", "cc"
                       );

   return (bool) ret;
}

#endif /* PMIX_GCC_INLINE_ASSEMBLY */

#define pmix_atomic_compare_exchange_strong_acq_64 pmix_atomic_compare_exchange_strong_64
#define pmix_atomic_compare_exchange_strong_rel_64 pmix_atomic_compare_exchange_strong_64

#if PMIX_GCC_INLINE_ASSEMBLY && PMIX_HAVE_CMPXCHG16B && HAVE_PMIX_INT128_T

static inline bool pmix_atomic_compare_exchange_strong_128 (pmix_atomic_int128_t *addr, pmix_int128_t *oldval, pmix_int128_t newval)
{
    unsigned char ret;

    /* cmpxchg16b compares the value at the address with eax:edx (low:high). if the values are
     * the same the contents of ebx:ecx are stores at the address. in all cases the value stored
     * at the address is returned in eax:edx. */
    __asm__ __volatile__ (SMPLOCK "cmpxchg16b (%%rsi)   \n\t"
                                  "sete     %0      \n\t"
                          : "=qm" (ret), "+a" (((int64_t *)oldval)[0]), "+d" (((int64_t *)oldval)[1])
                          : "S" (addr), "b" (((int64_t *)&newval)[0]), "c" (((int64_t *)&newval)[1])
                          : "memory", "cc");

    return (bool) ret;
}

#define PMIX_HAVE_ATOMIC_COMPARE_EXCHANGE_128 1

#endif /* PMIX_GCC_INLINE_ASSEMBLY */


#if PMIX_GCC_INLINE_ASSEMBLY

#define PMIX_HAVE_ATOMIC_SWAP_32 1

#define PMIX_HAVE_ATOMIC_SWAP_64 1

static inline int32_t pmix_atomic_swap_32( pmix_atomic_int32_t *addr,
					   int32_t newval)
{
    int32_t oldval;

    __asm__ __volatile__("xchg %1, %0" :
			 "=r" (oldval), "+m" (*addr) :
			 "0" (newval) :
			 "memory");
    return oldval;
}

#endif /* PMIX_GCC_INLINE_ASSEMBLY */

#if PMIX_GCC_INLINE_ASSEMBLY

static inline int64_t pmix_atomic_swap_64( pmix_atomic_int64_t *addr,
                                           int64_t newval)
{
    int64_t oldval;

    __asm__ __volatile__("xchgq %1, %0" :
			 "=r" (oldval), "+m" (*addr) :
			 "0" (newval) :
			 "memory");
    return oldval;
}

#endif /* PMIX_GCC_INLINE_ASSEMBLY */



#if PMIX_GCC_INLINE_ASSEMBLY

#define PMIX_HAVE_ATOMIC_MATH_32 1
#define PMIX_HAVE_ATOMIC_MATH_64 1

#define PMIX_HAVE_ATOMIC_ADD_32 1

/**
 * atomic_add - add integer to atomic variable
 * @i: integer value to add
 * @v: pointer of type int
 *
 * Atomically adds @i to @v.
 */
static inline int32_t pmix_atomic_fetch_add_32(pmix_atomic_int32_t* v, int i)
{
    int ret = i;
   __asm__ __volatile__(
                        SMPLOCK "xaddl %1,%0"
                        :"+m" (*v), "+r" (ret)
                        :
                        :"memory", "cc"
                        );
   return ret;
}

#define PMIX_HAVE_ATOMIC_ADD_64 1

/**
 * atomic_add - add integer to atomic variable
 * @i: integer value to add
 * @v: pointer of type int
 *
 * Atomically adds @i to @v.
 */
static inline int64_t pmix_atomic_fetch_add_64(pmix_atomic_int64_t* v, int64_t i)
{
    int64_t ret = i;
   __asm__ __volatile__(
                        SMPLOCK "xaddq %1,%0"
                        :"+m" (*v), "+r" (ret)
                        :
                        :"memory", "cc"
                        );
   return ret;
}

#define PMIX_HAVE_ATOMIC_SUB_32 1

/**
 * atomic_sub - subtract the atomic variable
 * @i: integer value to subtract
 * @v: pointer of type int
 *
 * Atomically subtracts @i from @v.
 */
static inline int32_t pmix_atomic_fetch_sub_32(pmix_atomic_int32_t* v, int i)
{
    int ret = -i;
   __asm__ __volatile__(
                        SMPLOCK "xaddl %1,%0"
                        :"+m" (*v), "+r" (ret)
                        :
                        :"memory", "cc"
                        );
   return ret;
}

#define PMIX_HAVE_ATOMIC_SUB_64 1

/**
 * atomic_sub - subtract the atomic variable
 * @i: integer value to subtract
 * @v: pointer of type int
 *
 * Atomically subtracts @i from @v.
 */
static inline int64_t pmix_atomic_fetch_sub_64(pmix_atomic_int64_t* v, int64_t i)
{
    int64_t ret = -i;
   __asm__ __volatile__(
                        SMPLOCK "xaddq %1,%0"
                        :"+m" (*v), "+r" (ret)
                        :
                        :"memory", "cc"
                        );
   return ret;
}

#endif /* PMIX_GCC_INLINE_ASSEMBLY */

#endif /* ! PMIX_SYS_ARCH_ATOMIC_H */
