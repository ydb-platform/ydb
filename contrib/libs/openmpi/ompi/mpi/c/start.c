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
 * Copyright (c) 2006      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2015-2017 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2017      FUJITSU LIMITED.  All rights reserved.
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
#include "ompi/mca/pml/pml.h"
#include "ompi/request/request.h"
#include "ompi/errhandler/errhandler.h"
#include "ompi/memchecker.h"

#if OMPI_BUILD_MPI_PROFILING
#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak MPI_Start = PMPI_Start
#endif
#define MPI_Start PMPI_Start
#endif

static const char FUNC_NAME[] = "MPI_Start";


int MPI_Start(MPI_Request *request)
{
    int ret = OMPI_SUCCESS;

    MEMCHECKER(
        memchecker_request(request);
    );

    if ( MPI_PARAM_CHECK ) {
        int rc = MPI_SUCCESS;
        OMPI_ERR_INIT_FINALIZE(FUNC_NAME);
        if (request == NULL) {
            rc = MPI_ERR_REQUEST;
        }
        OMPI_ERRHANDLER_CHECK(rc, MPI_COMM_WORLD, rc, FUNC_NAME);
    }
    /**
     * Per definition of the handling of persistent request in the
     * MPI standard 3.1 page 78 line 19: we must have the following
     * sequence CREATE (START COMPLETE)* FREE. The upper level is
     * responsible for handling any concurency. The PML must handle
     * this case, as it is the only one knowing if the request can
     * be reused or not (it is PML completed or not?).
     */

    switch((*request)->req_type) {
    case OMPI_REQUEST_PML:
    case OMPI_REQUEST_COLL:
        if ( MPI_PARAM_CHECK && !(*request)->req_persistent) {
            return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_REQUEST, FUNC_NAME);
        }
        OPAL_CR_ENTER_LIBRARY();

        ret = (*request)->req_start(1, request);

        OPAL_CR_EXIT_LIBRARY();
        return ret;

    case OMPI_REQUEST_NOOP:
        /**
         * We deal with a MPI_PROC_NULL request. If the request is
         * already active, fall back to the error case in the default.
         * Otherwise, mark it active so we can correctly handle it in
         * the wait*.
         */
        if( OMPI_REQUEST_INACTIVE == (*request)->req_state ) {
            (*request)->req_state = OMPI_REQUEST_ACTIVE;
            return MPI_SUCCESS;
        }

    default:
        return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_REQUEST, FUNC_NAME);
    }
}

