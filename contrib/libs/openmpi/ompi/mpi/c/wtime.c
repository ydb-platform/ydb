/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2018 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2006-2014 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
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
#endif  /* HAVE_TIME_H */

#include MCA_timer_IMPLEMENTATION_HEADER
#include "ompi/mpi/c/bindings.h"
#include "ompi/runtime/mpiruntime.h"
#include "ompi/runtime/ompi_spc.h"

#if OMPI_BUILD_MPI_PROFILING
#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak MPI_Wtime = PMPI_Wtime
#endif
#define MPI_Wtime PMPI_Wtime
/**
 * Have a base time set on the first call to wtime, to improve the range
 * and accuracy of the user visible timer.
 * More info: https://github.com/mpi-forum/mpi-issues/issues/77#issuecomment-369663119
 */
#if defined(__linux__) && OPAL_HAVE_CLOCK_GETTIME
struct timespec ompi_wtime_time_origin = {.tv_sec = 0};
#else
struct timeval ompi_wtime_time_origin = {.tv_sec = 0};
#endif
#else  /* OMPI_BUILD_MPI_PROFILING */
#if defined(__linux__) && OPAL_HAVE_CLOCK_GETTIME
extern struct timespec ompi_wtime_time_origin;
#else
extern struct timeval ompi_wtime_time_origin;
#endif
#endif

double MPI_Wtime(void)
{
    double wtime;

    SPC_RECORD(OMPI_SPC_WTIME, 1);

    /*
     * See https://github.com/open-mpi/ompi/issues/3003 to find out
     * what's happening here.
     */
#if 0
#if OPAL_TIMER_CYCLE_NATIVE
    wtime = ((double) opal_timer_base_get_cycles()) / opal_timer_base_get_freq();
#elif OPAL_TIMER_USEC_NATIVE
    wtime = ((double) opal_timer_base_get_usec()) / 1000000.0;
#endif
#else
#if defined(__linux__) && OPAL_HAVE_CLOCK_GETTIME
    struct timespec tp;
    (void) clock_gettime(CLOCK_MONOTONIC, &tp);
    if( OPAL_UNLIKELY(0 == ompi_wtime_time_origin.tv_sec) ) {
        ompi_wtime_time_origin = tp;
    }
    wtime  = (double)(tp.tv_nsec - ompi_wtime_time_origin.tv_nsec)/1.0e+9;
    wtime += (tp.tv_sec - ompi_wtime_time_origin.tv_sec);
#else
    /* Fall back to gettimeofday() if we have nothing else */
    struct timeval tv;
    gettimeofday(&tv, NULL);
    if( OPAL_UNLIKELY(0 == ompi_wtime_time_origin.tv_sec) ) {
        ompi_wtime_time_origin = tv;
    }
    wtime  = (double)(tv.tv_usec - ompi_wtime_time_origin.tv_usec) / 1.0e+6;
    wtime += (tv.tv_sec - ompi_wtime_time_origin.tv_sec);
#endif
#endif

    OPAL_CR_NOOP_PROGRESS();

    return wtime;
}
