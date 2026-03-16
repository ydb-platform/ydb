/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007-2014 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2015-2016 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2017      IBM Corporation.  All rights reserved.
 * Copyright (c) 2017      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
#include "ompi_config.h"

#ifdef HAVE_SYS_TIME_H
#include <sys/time.h>
#endif
#include <stdio.h>
#ifdef HAVE_TIME_H
#include <time.h>
#endif

#include MCA_timer_IMPLEMENTATION_HEADER
#include "ompi/mpi/c/bindings.h"
#include "ompi/runtime/mpiruntime.h"

#if OMPI_BUILD_MPI_PROFILING
#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak MPI_Wtick = PMPI_Wtick
#endif
#define MPI_Wtick PMPI_Wtick
#endif

double MPI_Wtick(void)
{
    OPAL_CR_NOOP_PROGRESS();

    /*
     * See https://github.com/open-mpi/ompi/issues/3003
     * to get an idea what's going on here.
     */
#if 0
#if OPAL_TIMER_CYCLE_NATIVE
    {
        opal_timer_t freq = opal_timer_base_get_freq();
        if (0 == freq) {
            /* That should never happen, but if it does, return a bogus value
             * rather than crashing with a division by zero */
            return (double)0.0;
        }
        return (double)1.0 / (double)freq;
    }
#elif OPAL_TIMER_USEC_NATIVE
    return 0.000001;
#endif
#else
#if defined(__linux__) && OPAL_HAVE_CLOCK_GETTIME
    struct timespec spec;
    double wtick = 0.0;
    if (0 == clock_getres(CLOCK_MONOTONIC, &spec)){
        wtick =  spec.tv_sec + spec.tv_nsec * 1.0e-09;
    } else {
        /* guess */
        wtick = 1.0e-09;
    }
    return wtick;
#else
    /* Otherwise, we already return usec precision. */
    return 0.000001;
#endif
#endif
}
