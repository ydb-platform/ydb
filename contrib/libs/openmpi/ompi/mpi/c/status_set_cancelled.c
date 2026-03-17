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
#pragma weak MPI_Status_set_cancelled = PMPI_Status_set_cancelled
#endif
#define MPI_Status_set_cancelled PMPI_Status_set_cancelled
#endif

static const char FUNC_NAME[] = "MPI_Status_set_cancelled";


int MPI_Status_set_cancelled(MPI_Status *status, int flag)
{
    MEMCHECKER(
        if(status != MPI_STATUSES_IGNORE) {
            /*
             * Before checking the complete status, we need to reset the definedness
             * of the MPI_ERROR-field (single-completion calls wait/test).
             */
            opal_memchecker_base_mem_defined(&status->MPI_ERROR, sizeof(int));
            memchecker_status(status);
        }
    );

    OPAL_CR_NOOP_PROGRESS();

    if (MPI_PARAM_CHECK) {
        int rc = MPI_SUCCESS;
        OMPI_ERR_INIT_FINALIZE(FUNC_NAME);
        if (NULL == status ||
            MPI_STATUS_IGNORE == status ||
            MPI_STATUSES_IGNORE == status) {
            rc = MPI_ERR_ARG;
        }
        OMPI_ERRHANDLER_CHECK(rc, MPI_COMM_WORLD, rc, FUNC_NAME);
    }

    status->_cancelled = flag;
    return MPI_SUCCESS;
}
