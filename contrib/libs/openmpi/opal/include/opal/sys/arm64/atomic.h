/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2010      IBM Corporation.  All rights reserved.
 * Copyright (c) 2010      ARM ltd.  All rights reserved.
 * Copyright (c) 2016-2017 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#if !defined(OPAL_SYS_ARCH_ATOMIC_H)

#define OPAL_SYS_ARCH_ATOMIC_H 1

#if OPAL_GCC_INLINE_ASSEMBLY

#define OPAL_HAVE_ATOMIC_MEM_BARRIER 1
#define OPAL_HAVE_ATOMIC_LLSC_32 1
#define OPAL_HAVE_ATOMIC_COMPARE_EXCHANGE_32 1
#define OPAL_HAVE_ATOMIC_SWAP_32 1
#define OPAL_HAVE_ATOMIC_MATH_32 1
#define OPAL_HAVE_ATOMIC_COMPARE_EXCHANGE_64 1
#define OPAL_HAVE_ATOMIC_SWAP_64 1
#define OPAL_HAVE_ATOMIC_LLSC_64 1
#define OPAL_HAVE_ATOMIC_ADD_32 1
#define OPAL_HAVE_ATOMIC_AND_32 1
#define OPAL_HAVE_ATOMIC_OR_32 1
#define OPAL_HAVE_ATOMIC_XOR_32 1
#define OPAL_HAVE_ATOMIC_SUB_32 1
#define OPAL_HAVE_ATOMIC_ADD_64 1
#define OPAL_HAVE_ATOMIC_AND_64 1
#define OPAL_HAVE_ATOMIC_OR_64 1
#define OPAL_HAVE_ATOMIC_XOR_64 1
#define OPAL_HAVE_ATOMIC_SUB_64 1

#define MB()  __asm__ __volatile__ ("dmb sy" : : : "memory")
#define RMB() __asm__ __volatile__ ("dmb ld" : : : "memory")
#define WMB() __asm__ __volatile__ ("dmb st" : : : "memory")

/**********************************************************************
 *
 * Memory Barriers
 *
 *********************************************************************/

static inline void opal_atomic_mb (void)
{
    MB();
}

static inline void opal_atomic_rmb (void)
{
    RMB();
}

static inline void opal_atomic_wmb (void)
{
    WMB();
}

static inline void opal_atomic_isync (void)
{
    __asm__ __volatile__ ("isb");
}

/**********************************************************************
 *
 * Atomic math operations
 *
 *********************************************************************/

static inline bool opal_atomic_compare_exchange_strong_32 (volatile int32_t *addr, int32_t *oldval, int32_t newval)
{
    int32_t prev, tmp;
    bool ret;

    __asm__ __volatile__ ("1:  ldaxr    %w0, [%2]      \n"
                          "    cmp     %w0, %w3        \n"
                          "    bne     2f              \n"
                          "    stxr    %w1, %w4, [%2]  \n"
                          "    cbnz    %w1, 1b         \n"
                          "2:                          \n"
                          : "=&r" (prev), "=&r" (tmp)
                          : "r" (addr), "r" (*oldval), "r" (newval)
                          : "cc", "memory");

    ret = (prev == *oldval);
    *oldval = prev;
    return ret;
}

static inline int32_t opal_atomic_swap_32(volatile int32_t *addr, int32_t newval)
{
    int32_t ret, tmp;

    __asm__ __volatile__ ("1:  ldaxr   %w0, [%2]       \n"
                          "    stlxr   %w1, %w3, [%2]  \n"
                          "    cbnz    %w1, 1b         \n"
                          : "=&r" (ret), "=&r" (tmp)
                          : "r" (addr), "r" (newval)
                          : "cc", "memory");

    return ret;
}

/* these two functions aren't inlined in the non-gcc case because then
   there would be two function calls (since neither cmpset_32 nor
   atomic_?mb can be inlined).  Instead, we "inline" them by hand in
   the assembly, meaning there is one function call overhead instead
   of two */
static inline bool opal_atomic_compare_exchange_strong_acq_32 (volatile int32_t *addr, int32_t *oldval, int32_t newval)
{
    int32_t prev, tmp;
    bool ret;

    __asm__ __volatile__ ("1:  ldaxr   %w0, [%2]       \n"
                          "    cmp     %w0, %w3        \n"
                          "    bne     2f              \n"
                          "    stxr    %w1, %w4, [%2]  \n"
                          "    cbnz    %w1, 1b         \n"
                          "2:                          \n"
                          : "=&r" (prev), "=&r" (tmp)
                          : "r" (addr), "r" (*oldval), "r" (newval)
                          : "cc", "memory");

    ret = (prev == *oldval);
    *oldval = prev;
    return ret;
}


static inline bool opal_atomic_compare_exchange_strong_rel_32 (volatile int32_t *addr, int32_t *oldval, int32_t newval)
{
    int32_t prev, tmp;
    bool ret;

    __asm__ __volatile__ ("1:  ldxr    %w0, [%2]       \n"
                          "    cmp     %w0, %w3        \n"
                          "    bne     2f              \n"
                          "    stlxr   %w1, %w4, [%2]  \n"
                          "    cbnz    %w1, 1b         \n"
                          "2:                          \n"
                          : "=&r" (prev), "=&r" (tmp)
                          : "r" (addr), "r" (*oldval), "r" (newval)
                          : "cc", "memory");

    ret = (prev == *oldval);
    *oldval = prev;
    return ret;
}

#define opal_atomic_ll_32(addr, ret)                                    \
    do {                                                                \
        volatile int32_t *_addr = (addr);                               \
        int32_t _ret;                                                   \
                                                                        \
        __asm__ __volatile__ ("ldaxr    %w0, [%1]          \n"          \
                              : "=&r" (_ret)                            \
                              : "r" (_addr));                           \
                                                                        \
        ret = (typeof(ret)) _ret;                                       \
    } while (0)

#define opal_atomic_sc_32(addr, newval, ret)                            \
    do {                                                                \
        volatile int32_t *_addr = (addr);                               \
        int32_t _newval = (int32_t) newval;                             \
        int _ret;                                                       \
                                                                        \
        __asm__ __volatile__ ("stlxr    %w0, %w2, [%1]     \n"          \
                              : "=&r" (_ret)                            \
                              : "r" (_addr), "r" (_newval)              \
                              : "cc", "memory");                        \
                                                                        \
        ret = (_ret == 0);                                              \
    } while (0)

static inline bool opal_atomic_compare_exchange_strong_64 (volatile int64_t *addr, int64_t *oldval, int64_t newval)
{
    int64_t prev;
    int tmp;
    bool ret;

    __asm__ __volatile__ ("1:  ldaxr    %0, [%2]       \n"
                          "    cmp     %0, %3          \n"
                          "    bne     2f              \n"
                          "    stxr    %w1, %4, [%2]   \n"
                          "    cbnz    %w1, 1b         \n"
                          "2:                          \n"
                          : "=&r" (prev), "=&r" (tmp)
                          : "r" (addr), "r" (*oldval), "r" (newval)
                          : "cc", "memory");

    ret = (prev == *oldval);
    *oldval = prev;
    return ret;
}

static inline int64_t opal_atomic_swap_64 (volatile int64_t *addr, int64_t newval)
{
    int64_t ret;
    int tmp;

    __asm__ __volatile__ ("1:  ldaxr   %0, [%2]        \n"
                          "    stlxr   %w1, %3, [%2]   \n"
                          "    cbnz    %w1, 1b         \n"
                          : "=&r" (ret), "=&r" (tmp)
                          : "r" (addr), "r" (newval)
                          : "cc", "memory");

    return ret;
}

/* these two functions aren't inlined in the non-gcc case because then
   there would be two function calls (since neither cmpset_64 nor
   atomic_?mb can be inlined).  Instead, we "inline" them by hand in
   the assembly, meaning there is one function call overhead instead
   of two */
static inline bool opal_atomic_compare_exchange_strong_acq_64 (volatile int64_t *addr, int64_t *oldval, int64_t newval)
{
    int64_t prev;
    int tmp;
    bool ret;

    __asm__ __volatile__ ("1:  ldaxr   %0, [%2]        \n"
                          "    cmp     %0, %3          \n"
                          "    bne     2f              \n"
                          "    stxr    %w1, %4, [%2]   \n"
                          "    cbnz    %w1, 1b         \n"
                          "2:                          \n"
                          : "=&r" (prev), "=&r" (tmp)
                          : "r" (addr), "r" (*oldval), "r" (newval)
                          : "cc", "memory");

    ret = (prev == *oldval);
    *oldval = prev;
    return ret;
}


static inline bool opal_atomic_compare_exchange_strong_rel_64 (volatile int64_t *addr, int64_t *oldval, int64_t newval)
{
    int64_t prev;
    int tmp;
    bool ret;

    __asm__ __volatile__ ("1:  ldxr    %0, [%2]        \n"
                          "    cmp     %0, %3          \n"
                          "    bne     2f              \n"
                          "    stlxr   %w1, %4, [%2]   \n"
                          "    cbnz    %w1, 1b         \n"
                          "2:                          \n"
                          : "=&r" (prev), "=&r" (tmp)
                          : "r" (addr), "r" (*oldval), "r" (newval)
                          : "cc", "memory");

    ret = (prev == *oldval);
    *oldval = prev;
    return ret;
}

#define opal_atomic_ll_64(addr, ret)                            \
    do {                                                        \
        volatile int64_t *_addr = (addr);                       \
        int64_t _ret;                                           \
                                                                \
        __asm__ __volatile__ ("ldaxr    %0, [%1]          \n"   \
                              : "=&r" (_ret)                    \
                              : "r" (_addr));                   \
                                                                \
        ret = (typeof(ret)) _ret;                               \
    } while (0)

#define opal_atomic_sc_64(addr, newval, ret)                            \
    do {                                                                \
        volatile int64_t *_addr = (addr);                               \
        int64_t _newval = (int64_t) newval;                             \
        int _ret;                                                       \
                                                                        \
        __asm__ __volatile__ ("stlxr    %w0, %2, [%1]     \n"           \
                              : "=&r" (_ret)                            \
                              : "r" (_addr), "r" (_newval)              \
                              : "cc", "memory");                        \
                                                                        \
        ret = (_ret == 0);                                              \
    } while (0)

#define OPAL_ASM_MAKE_ATOMIC(type, bits, name, inst, reg)                   \
    static inline type opal_atomic_fetch_ ## name ## _ ## bits (volatile type *addr, type value) \
    {                                                                   \
        type newval, old;                                               \
        int32_t tmp;                                                    \
                                                                        \
        __asm__ __volatile__("1:  ldxr   %" reg "1, [%3]        \n"     \
                             "    " inst "   %" reg "0, %" reg "1, %" reg "4 \n" \
                             "    stxr   %w2, %" reg "0, [%3]   \n"     \
                             "    cbnz   %w2, 1b         \n"            \
                             : "=&r" (newval), "=&r" (old), "=&r" (tmp) \
                             : "r" (addr), "r" (value)                  \
                             : "cc", "memory");                         \
                                                                        \
        return old;                                                     \
    }

OPAL_ASM_MAKE_ATOMIC(int32_t, 32, add, "add", "w")
OPAL_ASM_MAKE_ATOMIC(int32_t, 32, and, "and", "w")
OPAL_ASM_MAKE_ATOMIC(int32_t, 32, or, "orr", "w")
OPAL_ASM_MAKE_ATOMIC(int32_t, 32, xor, "eor", "w")
OPAL_ASM_MAKE_ATOMIC(int32_t, 32, sub, "sub", "w")
OPAL_ASM_MAKE_ATOMIC(int64_t, 64, add, "add", "")
OPAL_ASM_MAKE_ATOMIC(int64_t, 64, and, "and", "")
OPAL_ASM_MAKE_ATOMIC(int64_t, 64, or, "orr", "")
OPAL_ASM_MAKE_ATOMIC(int64_t, 64, xor, "eor", "")
OPAL_ASM_MAKE_ATOMIC(int64_t, 64, sub, "sub", "")

#endif /* OPAL_GCC_INLINE_ASSEMBLY */

#endif /* ! OPAL_SYS_ARCH_ATOMIC_H */
