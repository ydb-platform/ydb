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
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef PMIX_SYS_ARCH_TIMER_H
#define PMIX_SYS_ARCH_TIMER_H 1

typedef uint64_t pmix_timer_t;

#if PMIX_GCC_INLINE_ASSEMBLY


#if PMIX_ASSEMBLY_ARCH == PMIX_SPARCV9_64

static inline pmix_timer_t
pmix_sys_timer_get_cycles(void)
{
    pmix_timer_t ret;

    __asm__ __volatile__("rd %%tick, %0" : "=r"(ret));

    return ret;
}

#else /* PMIX_SPARCV9_32 */

static inline pmix_timer_t
pmix_sys_timer_get_cycles(void)
{
    pmix_timer_t ret;
    int a, b;

    __asm__ __volatile__("rd %%tick, %0    \n"
                         "srlx %0, 32, %1 " :
                         "=r"(a), "=r"(b)
                         );

    ret = (0x00000000FFFFFFFF & a) | (((pmix_timer_t) b) << 32);

    return ret;
}

#endif

#define PMIX_HAVE_SYS_TIMER_GET_CYCLES 1

#else

#define PMIX_HAVE_SYS_TIMER_GET_CYCLES 0

#endif /* PMIX_GCC_INLINE_ASSEMBLY */

#endif /* ! PMIX_SYS_ARCH_TIMER_H */
