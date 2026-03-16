/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2008      The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2016      Broadcom Limited. All rights reserved.
 * Copyright (c) 2016      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef OPAL_SYS_ARCH_TIMER_H
#define OPAL_SYS_ARCH_TIMER_H 1

#include <sys/times.h>

typedef uint64_t opal_timer_t;

static inline opal_timer_t
opal_sys_timer_get_cycles(void)
{
    opal_timer_t ret;

    __asm__ __volatile__ ("isb" ::: "memory");
    __asm__ __volatile__ ("mrs %0,  CNTVCT_EL0" : "=r" (ret));

    return ret;
}


static inline opal_timer_t
opal_sys_timer_freq(void)
{
    opal_timer_t freq;
    __asm__ __volatile__ ("mrs %0,  CNTFRQ_EL0" : "=r" (freq));
    return (opal_timer_t)(freq);
}

#define OPAL_HAVE_SYS_TIMER_GET_CYCLES 1

#endif /* ! OPAL_SYS_ARCH_TIMER_H */
