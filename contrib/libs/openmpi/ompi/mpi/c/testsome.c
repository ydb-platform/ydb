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
 * Copyright (c) 2006-2012 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2012      Oracle and/or its affiliates.  All rights reserved.
 * Copyright (c) 2012      Los Alamos National Security, LLC.  All rights
 *                         reserved.
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
#pragma weak MPI_Testsome = PMPI_Testsome
#endif
#define MPI_Testsome PMPI_Testsome
#endif

static const char FUNC_NAME[] = "MPI_Testsome";


int MPI_Testsome(int incount, MPI_Request requests[],
                 int *outcount, int indices[],
                 MPI_Status statuses[])
{
    SPC_RECORD(OMPI_SPC_TESTSOME, 1);

    MEMCHECKER(
        int j;
        for (j = 0; j < incount; j++){
            memchecker_request(&requests[j]);
        }
    );

    if ( MPI_PARAM_CHECK ) {
        int indx, rc = MPI_SUCCESS;
        OMPI_ERR_INIT_FINALIZE(FUNC_NAME);
        if ((NULL == requests) && (0 != incount)) {
            rc = MPI_ERR_REQUEST;
        } else {
            for (indx = 0; indx < incount; ++indx) {
                if (NULL == requests[indx]) {
                    rc = MPI_ERR_REQUEST;
                    break;
                }
            }
        }
        if (((NULL == outcount || NULL == indices) && incount > 0) ||
            incount < 0) {
            return MPI_ERR_ARG;
        }
        OMPI_ERRHANDLER_CHECK(rc, MPI_COMM_WORLD, rc, FUNC_NAME);
    }

    if (OPAL_UNLIKELY(0 == incount)) {
        *outcount = MPI_UNDEFINED;
        return OMPI_SUCCESS;
    }

    OPAL_CR_ENTER_LIBRARY();

    if (OMPI_SUCCESS == ompi_request_test_some(incount, requests, outcount,
                                               indices, statuses)) {
        OPAL_CR_EXIT_LIBRARY();
        return MPI_SUCCESS;
    }

    if (MPI_SUCCESS !=
        ompi_errhandler_request_invoke(incount, requests, FUNC_NAME)) {
        OPAL_CR_EXIT_LIBRARY();
        return MPI_ERR_IN_STATUS;
    }

    OPAL_CR_EXIT_LIBRARY();
    return MPI_SUCCESS;
}
