/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2016 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2015-2017 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2015-2017 Cisco Systems, Inc.  All rights reserved
 * Copyright (c) 2016 Broadcom Limited. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"

#include <string.h>

#include "opal/mca/timer/timer.h"
#include "opal/mca/timer/base/base.h"
#include "opal/mca/timer/linux/timer_linux.h"
#include "opal/constants.h"
#include "opal/util/show_help.h"

static opal_timer_t opal_timer_linux_get_cycles_sys_timer(void);
static opal_timer_t opal_timer_linux_get_usec_sys_timer(void);

/**
 * Define some sane defaults until we call the _init function.
 */
#if OPAL_HAVE_CLOCK_GETTIME
static opal_timer_t opal_timer_linux_get_cycles_clock_gettime(void);
static opal_timer_t opal_timer_linux_get_usec_clock_gettime(void);

opal_timer_t (*opal_timer_base_get_cycles)(void) =
    opal_timer_linux_get_cycles_clock_gettime;
opal_timer_t (*opal_timer_base_get_usec)(void) =
    opal_timer_linux_get_usec_clock_gettime;
#else
opal_timer_t (*opal_timer_base_get_cycles)(void) =
    opal_timer_linux_get_cycles_sys_timer;
opal_timer_t (*opal_timer_base_get_usec)(void) =
    opal_timer_linux_get_usec_sys_timer;
#endif  /* OPAL_HAVE_CLOCK_GETTIME */

static opal_timer_t opal_timer_linux_freq = {0};

static int opal_timer_linux_open(void);

const opal_timer_base_component_2_0_0_t mca_timer_linux_component = {
    /* First, the mca_component_t struct containing meta information
       about the component itself */
    .timerc_version = {
        OPAL_TIMER_BASE_VERSION_2_0_0,

        /* Component name and version */
        .mca_component_name = "linux",
        MCA_BASE_MAKE_VERSION(component, OPAL_MAJOR_VERSION, OPAL_MINOR_VERSION,
                              OPAL_RELEASE_VERSION),

        /* Component open and close functions */
        .mca_open_component = opal_timer_linux_open,
    },
    .timerc_data = {
        /* The component is checkpoint ready */
        MCA_BASE_METADATA_PARAM_CHECKPOINT
    },
};

static char *
find_info(FILE* fp, char *str, char *buf, size_t buflen)
{
    char *tmp;

    rewind(fp);
    while (NULL != fgets(buf, buflen, fp)) {
        if (strncmp(buf, str, strlen(str)) == 0) {
            /* we found the line.  Now eat everything up to,
               including, and one past the : */
            for (tmp = buf ; (*tmp != '\0') && (*tmp != ':') ; ++tmp) ;
            if (*tmp == '\0') {
                continue;
            }
            for ( ++tmp ; *tmp == ' ' ; ++tmp);
            if ('\0' != *tmp) {
                return tmp;
            }
        }
    }

    return NULL;
}

static int opal_timer_linux_find_freq(void)
{
    FILE *fp;
    char *loc;
    float cpu_f;
    int ret;
    char buf[1024];

    fp = fopen("/proc/cpuinfo", "r");
    if (NULL == fp) {
        return OPAL_ERR_IN_ERRNO;
    }

    opal_timer_linux_freq = 0;

#if OPAL_ASSEMBLY_ARCH == OPAL_ARM64
	opal_timer_linux_freq = opal_sys_timer_freq();
#endif

    if (0 == opal_timer_linux_freq) {
        /* first, look for a timebase field.  probably only on PPC,
           but one never knows */
        loc = find_info(fp, "timebase", buf, 1024);
        if (NULL != loc) {
            int freq;
            ret = sscanf(loc, "%d", &freq);
            if (1 == ret) {
                opal_timer_linux_freq = freq;
            }
        }
    }

#if ((OPAL_ASSEMBLY_ARCH == OPAL_IA32) || (OPAL_ASSEMBLY_ARCH == OPAL_X86_64))
    if (0 == opal_timer_linux_freq && opal_sys_timer_is_monotonic()) {
        /* tsc is exposed through bogomips ~> loops_per_jiffy ~> tsc_khz */
        loc = find_info(fp, "bogomips", buf, 1024);
        if (NULL != loc) {
            ret = sscanf(loc, "%f", &cpu_f);
            if (1 == ret) {
                /* number is in MHz * 2 and has 2 decimal digits
                   convert to Hz and make an integer */
                opal_timer_linux_freq = (opal_timer_t) (cpu_f * 100.0f) * 5000;
            }
        }
    }
#endif

    if (0 == opal_timer_linux_freq) {
        /* find the CPU speed - most timers are 1:1 with CPU speed */
        loc = find_info(fp, "cpu MHz", buf, 1024);
        if (NULL != loc) {
            ret = sscanf(loc, "%f", &cpu_f);
            if (1 == ret) {
                /* numer is in MHz - convert to Hz and make an integer */
                opal_timer_linux_freq = (opal_timer_t) (cpu_f * 1000000);
            }
        }
    }

    if (0 == opal_timer_linux_freq) {
        /* look for the sparc way of getting cpu frequency */
        loc = find_info(fp, "Cpu0ClkTck", buf, 1024);
        if (NULL != loc) {
            unsigned int freq;
            ret = sscanf(loc, "%x", &freq);
            if (1 == ret) {
                opal_timer_linux_freq = freq;
            }
        }
    }

    fclose(fp);

    /* convert the timer frequency to MHz to avoid an extra operation when
     * converting from cycles to usec */
    opal_timer_linux_freq /= 1000000;

    return OPAL_SUCCESS;
}

int opal_timer_linux_open(void)
{
    int ret = OPAL_SUCCESS;

    if (mca_timer_base_monotonic && !opal_sys_timer_is_monotonic ()) {
#if OPAL_HAVE_CLOCK_GETTIME && (0 == OPAL_TIMER_MONOTONIC)
        struct timespec res;
        if( 0 == clock_getres(CLOCK_MONOTONIC, &res)) {
            opal_timer_linux_freq = 1.e3;
            opal_timer_base_get_cycles = opal_timer_linux_get_cycles_clock_gettime;
            opal_timer_base_get_usec = opal_timer_linux_get_usec_clock_gettime;
            return ret;
        }
#else
        /* Monotonic time requested but cannot be found. Complain! */
        opal_show_help("help-opal-timer-linux.txt", "monotonic not supported", true);
#endif  /* OPAL_HAVE_CLOCK_GETTIME && (0 == OPAL_TIMER_MONOTONIC) */
    }
    ret = opal_timer_linux_find_freq();
    opal_timer_base_get_cycles = opal_timer_linux_get_cycles_sys_timer;
    opal_timer_base_get_usec = opal_timer_linux_get_usec_sys_timer;
    return ret;
}

#if OPAL_HAVE_CLOCK_GETTIME
opal_timer_t opal_timer_linux_get_usec_clock_gettime(void)
{
    struct timespec tp = {.tv_sec = 0, .tv_nsec = 0};

    (void) clock_gettime (CLOCK_MONOTONIC, &tp);

    return (tp.tv_sec * 1e6 + tp.tv_nsec/1000);
}

opal_timer_t opal_timer_linux_get_cycles_clock_gettime(void)
{
    struct timespec tp = {.tv_sec = 0, .tv_nsec = 0};

    (void) clock_gettime(CLOCK_MONOTONIC, &tp);

    return (tp.tv_sec * 1e9 + tp.tv_nsec);
}
#endif  /* OPAL_HAVE_CLOCK_GETTIME */

opal_timer_t opal_timer_linux_get_cycles_sys_timer(void)
{
#if OPAL_HAVE_SYS_TIMER_GET_CYCLES
    return opal_sys_timer_get_cycles();
#else
    return 0;
#endif
}


opal_timer_t opal_timer_linux_get_usec_sys_timer(void)
{
#if OPAL_HAVE_SYS_TIMER_GET_CYCLES
    /* freq is in MHz, so this gives usec */
    return opal_sys_timer_get_cycles()  / opal_timer_linux_freq;
#else
    return 0;
#endif
}

opal_timer_t opal_timer_base_get_freq(void)
{
    return opal_timer_linux_freq * 1000000;
}
