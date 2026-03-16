/*
 * Copyright (c) 2008      The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef PMIX_SYS_ARCH_TIMER_H
#define PMIX_SYS_ARCH_TIMER_H 1

#include <sys/times.h>

typedef uint64_t pmix_timer_t;

static inline pmix_timer_t
pmix_sys_timer_get_cycles(void)
{
    pmix_timer_t ret;
    struct tms accurate_clock;

    times(&accurate_clock);
    ret = accurate_clock.tms_utime + accurate_clock.tms_stime;

    return ret;
}

#define PMIX_HAVE_SYS_TIMER_GET_CYCLES 1

#endif /* ! PMIX_SYS_ARCH_TIMER_H */
