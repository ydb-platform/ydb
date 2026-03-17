/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
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
 * Copyright (c) 2012-2016 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2014-2017 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2017-2018 FUJITSU LIMITED.  All rights reserved.
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
#include "ompi/mca/pml/pml.h"
#include "ompi/request/request.h"
#include "ompi/memchecker.h"

#if OMPI_BUILD_MPI_PROFILING
#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak MPI_Startall = PMPI_Startall
#endif
#define MPI_Startall PMPI_Startall
#endif

static const char FUNC_NAME[] = "MPI_Startall";


int MPI_Startall(int count, MPI_Request requests[])
{
    int i, j;
    int ret = OMPI_SUCCESS;
    ompi_request_start_fn_t start_fn = NULL;

    MEMCHECKER(
        for (j = 0; j < count; j++){
            memchecker_request(&requests[j]);
        }
    );

    if ( MPI_PARAM_CHECK ) {
        int rc = MPI_SUCCESS;
        OMPI_ERR_INIT_FINALIZE(FUNC_NAME);
        if (NULL == requests) {
            rc = MPI_ERR_REQUEST;
        } else if (count < 0) {
            rc = MPI_ERR_ARG;
        } else {
            for (i = 0; i < count; ++i) {
                if (NULL == requests[i] ||
                    ! requests[i]->req_persistent ||
                    (OMPI_REQUEST_PML  != requests[i]->req_type &&
                     OMPI_REQUEST_COLL != requests[i]->req_type &&
                     OMPI_REQUEST_NOOP != requests[i]->req_type)) {
                    rc = MPI_ERR_REQUEST;
                    break;
                }
            }
        }
        OMPI_ERRHANDLER_CHECK(rc, MPI_COMM_WORLD, rc, FUNC_NAME);
    }

    OPAL_CR_ENTER_LIBRARY();

    for (i = 0, j = -1; i < count; ++i) {
        /* Per MPI it is invalid to start an active request */
        if (OMPI_REQUEST_INACTIVE != requests[i]->req_state) {
            return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_REQUEST, FUNC_NAME);
        }

        if (OMPI_REQUEST_NOOP == requests[i]->req_type) {
            /**
             * We deal with a MPI_PROC_NULL request. If the request is
             * already active, fall back to the error case in the default.
             * Otherwise, mark it active so we can correctly handle it in
             * the wait*.
             */
            requests[i]->req_state = OMPI_REQUEST_ACTIVE;
        }

        /* Call a req_start callback function per requests which have the
         * same req_start value. */
        if (requests[i]->req_start != start_fn) {
            if (NULL != start_fn && i != 0) {
                start_fn(i - j, requests + j);
            }
            start_fn = requests[i]->req_start;
            j = i;
        }
    }

    if (NULL != start_fn) {
        start_fn(i - j, requests + j);
    }

    OPAL_CR_EXIT_LIBRARY();
    return ret;
}

