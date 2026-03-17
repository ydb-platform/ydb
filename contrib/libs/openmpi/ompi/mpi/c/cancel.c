/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2018 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2008 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2009      Cisco Systems, Inc.  All rights reserved.
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
#include "ompi/request/request.h"
#include "ompi/memchecker.h"
#include "ompi/runtime/ompi_spc.h"

#if OMPI_BUILD_MPI_PROFILING
#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak MPI_Cancel = PMPI_Cancel
#endif
#define MPI_Cancel PMPI_Cancel
#endif

static const char FUNC_NAME[] = "MPI_Cancel";


int MPI_Cancel(MPI_Request *request)
{
    int rc;

    SPC_RECORD(OMPI_SPC_CANCEL, 1);

    MEMCHECKER(
        memchecker_request(request);
    );

    if ( MPI_PARAM_CHECK ) {
        rc = MPI_SUCCESS;
        OMPI_ERR_INIT_FINALIZE(FUNC_NAME);
        if (NULL == request || NULL == *request ||
            MPI_REQUEST_NULL == *request) {
            OMPI_ERRHANDLER_RETURN(MPI_ERR_REQUEST, MPI_COMM_WORLD,
                                   MPI_ERR_REQUEST, FUNC_NAME);
        }
    }

    if (MPI_REQUEST_NULL == *request) {
        return MPI_SUCCESS;
    }

    OPAL_CR_ENTER_LIBRARY();
    rc = ompi_request_cancel(*request);
    OMPI_ERRHANDLER_RETURN(rc, MPI_COMM_WORLD, rc, FUNC_NAME);
}

