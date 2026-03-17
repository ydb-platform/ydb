/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2008 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2006-2012 Cisco Systems, Inc.  All rights reserved.
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

#include "ompi/mpi/c/bindings.h"
#include "ompi/runtime/params.h"
#include "ompi/communicator/communicator.h"
#include "ompi/errhandler/errhandler.h"
#include "ompi/mpi/fortran/base/fint_2_int.h"
#include "ompi/memchecker.h"

#if OMPI_BUILD_MPI_PROFILING
#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak MPI_Comm_c2f = PMPI_Comm_c2f
#endif
#define MPI_Comm_c2f PMPI_Comm_c2f
#endif

static const char FUNC_NAME[] = "MPI_Comm_c2f";


MPI_Fint MPI_Comm_c2f(MPI_Comm comm)
{
    MEMCHECKER(
        memchecker_comm(comm);
    );

    OPAL_CR_NOOP_PROGRESS();

    if ( MPI_PARAM_CHECK) {
        OMPI_ERR_INIT_FINALIZE(FUNC_NAME);

        /* Note that ompi_comm_invalid() explicitly checks for
           MPI_COMM_NULL, but MPI_COMM_C2F is supposed to treat
           MPI_COMM_NULL as a valid communicator (and therefore return
           a valid Fortran handle for it).  Hence, this function
           should not return an error if MPI_COMM_NULL is passed in.

           See a big comment in ompi/communicator/communicator.h about
           this. */
        if (ompi_comm_invalid (comm) && MPI_COMM_NULL != comm) {
            return OMPI_INT_2_FINT(-1);
        }
    }

    return OMPI_INT_2_FINT(comm->c_f_to_c_index);
}
