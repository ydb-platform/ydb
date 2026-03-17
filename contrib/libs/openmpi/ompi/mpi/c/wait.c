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
 * Copyright (c) 2006      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2015-2017 Research Organization for Information Science
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
#pragma weak MPI_Wait = PMPI_Wait
#endif
#define MPI_Wait PMPI_Wait
#endif

static const char FUNC_NAME[] = "MPI_Wait";


int MPI_Wait(MPI_Request *request, MPI_Status *status)
{
    SPC_RECORD(OMPI_SPC_WAIT, 1);

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

    if (MPI_REQUEST_NULL == *request) {
        if (MPI_STATUS_IGNORE != status) {
            *status = ompi_status_empty;
            /*
             * Per MPI-1, the MPI_ERROR field is not defined for single-completion calls
             */
            MEMCHECKER(
                opal_memchecker_base_mem_undefined(&status->MPI_ERROR, sizeof(int));
            );
        }
        return MPI_SUCCESS;
    }

    OPAL_CR_ENTER_LIBRARY();

    if (OMPI_SUCCESS == ompi_request_wait(request, status)) {
        /*
         * Per MPI-1, the MPI_ERROR field is not defined for single-completion calls
         */
        MEMCHECKER(
            if (MPI_STATUS_IGNORE != status) {
                opal_memchecker_base_mem_undefined(&status->MPI_ERROR, sizeof(int));
            }
        );
        OPAL_CR_EXIT_LIBRARY();
        return MPI_SUCCESS;
    }

    MEMCHECKER(
        if (MPI_STATUS_IGNORE != status) {
            opal_memchecker_base_mem_undefined(&status->MPI_ERROR, sizeof(int));
        }
    );
    OPAL_CR_EXIT_LIBRARY();
    return ompi_errhandler_request_invoke(1, request, FUNC_NAME);
}
