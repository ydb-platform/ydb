/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2017 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2008 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2006      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2015      Intel, Inc. All rights reserved.
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
#include "ompi/memchecker.h"

#if OMPI_BUILD_MPI_PROFILING
#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak MPI_Comm_disconnect = PMPI_Comm_disconnect
#endif
#define MPI_Comm_disconnect PMPI_Comm_disconnect
#endif

#include "ompi/dpm/dpm.h"


static const char FUNC_NAME[] = "MPI_Comm_disconnect";


int MPI_Comm_disconnect(MPI_Comm *comm)
{
    int ret = MPI_SUCCESS;

    MEMCHECKER(
        memchecker_comm(*comm);
    );

    if ( MPI_PARAM_CHECK ) {
        OMPI_ERR_INIT_FINALIZE(FUNC_NAME);

        if ( ompi_comm_invalid (*comm))
            return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_COMM,
                                          FUNC_NAME);
    }

    if (MPI_COMM_WORLD == *comm || MPI_COMM_SELF == *comm ) {
        return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_COMM, FUNC_NAME);
    }

    OPAL_CR_ENTER_LIBRARY();

    if ( OMPI_COMM_IS_DYNAMIC(*comm)) {
        if (OMPI_SUCCESS != ompi_dpm_disconnect (*comm)) {
            ret = OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_COMM, FUNC_NAME);
        }
    }
    else {
        (*comm)->c_coll->coll_barrier(*comm, (*comm)->c_coll->coll_barrier_module);
    }

    ompi_comm_free(comm);

    OPAL_CR_EXIT_LIBRARY();
    return ret;
}
