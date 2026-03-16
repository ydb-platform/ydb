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
 * Copyright (c) 2012-2017 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2016-2017 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
#ifndef OPAL_SYS_ARCH_ATOMIC_H
#define OPAL_SYS_ARCH_ATOMIC_H 1

/*
 * On x86_64, we use cmpxchg.
 */


#define SMPLOCK "lock; "
#define MB() __asm__ __volatile__("": : :"memory")


/**********************************************************************
 *
 * Define constants for AMD64 / x86_64 / EM64T / ...
 *
 *********************************************************************/
#define OPAL_HAVE_ATOMIC_MEM_BARRIER 1

#define OPAL_HAVE_ATOMIC_COMPARE_EXCHANGE_32 1

#define OPAL_HAVE_ATOMIC_COMPARE_EXCHANGE_64 1

/**********************************************************************
 *
 * Memory Barriers
 *
 *********************************************************************/
#if OPAL_GCC_INLINE_ASSEMBLY

static inline void opal_atomic_mb(void)
{
    MB();
}


static inline void opal_atomic_rmb(void)
{
    MB();
}


static inline void opal_atomic_wmb(void)
{
    MB();
}

static inline void opal_atomic_isync(void)
{
}

#endif /* OPAL_GCC_INLINE_ASSEMBLY */


/**********************************************************************
 *
 * Atomic math operations
 *
 *********************************************************************/
#if OPAL_GCC_INLINE_ASSEMBLY

static inline bool opal_atomic_compare_exchange_strong_32 (volatile int32_t *addr, int32_t *oldval, int32_t newval)
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

#endif /* OPAL_GCC_INLINE_ASSEMBLY */

#define opal_atomic_compare_exchange_strong_acq_32 opal_atomic_compare_exchange_strong_32
#define opal_atomic_compare_exchange_strong_rel_32 opal_atomic_compare_exchange_strong_32

#if OPAL_GCC_INLINE_ASSEMBLY

static inline bool opal_atomic_compare_exchange_strong_64 (volatile int64_t *addr, int64_t *oldval, int64_t newval)
{
   unsigned char ret;
   __asm__ __volatile__ (
                       SMPLOCK "cmpxchgq %3,%2   \n\t"
                               "sete     %0      \n\t"
                       : "=qm" (ret), "+a" (*oldval), "+m" (*((volatile long*)addr))
                       : "q"(newval)
                       : "memory", "cc"
                       );

   return (bool) ret;
}

#endif /* OPAL_GCC_INLINE_ASSEMBLY */

#define opal_atomic_compare_exchange_strong_acq_64 opal_atomic_compare_exchange_strong_64
#define opal_atomic_compare_exchange_strong_rel_64 opal_atomic_compare_exchange_strong_64

#if OPAL_GCC_INLINE_ASSEMBLY && OPAL_HAVE_CMPXCHG16B && HAVE_OPAL_INT128_T

static inline bool opal_atomic_compare_exchange_strong_128 (volatile opal_int128_t *addr, opal_int128_t *oldval, opal_int128_t newval)
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

#define OPAL_HAVE_ATOMIC_COMPARE_EXCHANGE_128 1

#endif /* OPAL_GCC_INLINE_ASSEMBLY */


#if OPAL_GCC_INLINE_ASSEMBLY

#define OPAL_HAVE_ATOMIC_SWAP_32 1

#define OPAL_HAVE_ATOMIC_SWAP_64 1

static inline int32_t opal_atomic_swap_32( volatile int32_t *addr,
					   int32_t newval)
{
    int32_t oldval;

    __asm__ __volatile__("xchg %1, %0" :
			 "=r" (oldval), "+m" (*addr) :
			 "0" (newval) :
			 "memory");
    return oldval;
}

#endif /* OPAL_GCC_INLINE_ASSEMBLY */

#if OPAL_GCC_INLINE_ASSEMBLY

static inline int64_t opal_atomic_swap_64( volatile int64_t *addr,
                                           int64_t newval)
{
    int64_t oldval;

    __asm__ __volatile__("xchgq %1, %0" :
			 "=r" (oldval), "+m" (*addr) :
			 "0" (newval) :
			 "memory");
    return oldval;
}

#endif /* OPAL_GCC_INLINE_ASSEMBLY */



#if OPAL_GCC_INLINE_ASSEMBLY

#define OPAL_HAVE_ATOMIC_MATH_32 1
#define OPAL_HAVE_ATOMIC_MATH_64 1

#define OPAL_HAVE_ATOMIC_ADD_32 1

/**
 * atomic_add - add integer to atomic variable
 * @i: integer value to add
 * @v: pointer of type int
 *
 * Atomically adds @i to @v.
 */
static inline int32_t opal_atomic_fetch_add_32(volatile int32_t* v, int i)
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

#define OPAL_HAVE_ATOMIC_ADD_64 1

/**
 * atomic_add - add integer to atomic variable
 * @i: integer value to add
 * @v: pointer of type int
 *
 * Atomically adds @i to @v.
 */
static inline int64_t opal_atomic_fetch_add_64(volatile int64_t* v, int64_t i)
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

#define OPAL_HAVE_ATOMIC_SUB_32 1

/**
 * atomic_sub - subtract the atomic variable
 * @i: integer value to subtract
 * @v: pointer of type int
 *
 * Atomically subtracts @i from @v.
 */
static inline int32_t opal_atomic_fetch_sub_32(volatile int32_t* v, int i)
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

#define OPAL_HAVE_ATOMIC_SUB_64 1

/**
 * atomic_sub - subtract the atomic variable
 * @i: integer value to subtract
 * @v: pointer of type int
 *
 * Atomically subtracts @i from @v.
 */
static inline int64_t opal_atomic_fetch_sub_64(volatile int64_t* v, int64_t i)
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

#endif /* OPAL_GCC_INLINE_ASSEMBLY */

#endif /* ! OPAL_SYS_ARCH_ATOMIC_H */
