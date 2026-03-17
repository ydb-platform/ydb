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
 * Copyright (c) 2006-2008 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
#include "ompi_config.h"
#include <stdio.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#include <string.h>

#include "ompi/mpi/c/bindings.h"
#include "ompi/runtime/params.h"
#include "ompi/communicator/communicator.h"
#include "ompi/errhandler/errhandler.h"

#if OMPI_BUILD_MPI_PROFILING
#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak MPI_Get_processor_name = PMPI_Get_processor_name
#endif
#define MPI_Get_processor_name PMPI_Get_processor_name
#endif

static const char FUNC_NAME[] = "MPI_Get_processor_name";


int MPI_Get_processor_name(char *name, int *resultlen)
{
    OPAL_CR_NOOP_PROGRESS();

    if ( MPI_PARAM_CHECK) {
        OMPI_ERR_INIT_FINALIZE(FUNC_NAME);
        if ( NULL == name  ) {
            return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_ARG,
                                          FUNC_NAME);
        }
        if ( NULL == resultlen  ) {
            return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_ARG,
                                          FUNC_NAME);
        }
    }

    /* A simple implementation of this function using gethostname.

       Note that MPI-2.1 requires:
       - terminating the string with a \0
       - name[*resultlen] == '\0'
       - and therefore (*resultlen) cannot be > (MPI_MAX_PROCESSOR_NAME-1)

       Guard against gethostname() returning a *really long* hostname
       and not null-terminating the string.  The Fortran API version
       will pad to the right if necessary. */
    gethostname(name, (MPI_MAX_PROCESSOR_NAME - 1));
    name[MPI_MAX_PROCESSOR_NAME - 1] = '\0';
    *resultlen = (int) strlen(name);

    return MPI_SUCCESS;
}
