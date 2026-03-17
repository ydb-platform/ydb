/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2010 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2008 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2013      Los Alamos National Security, LLC. All rights
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
#include <limits.h>

#include "ompi/mpi/c/bindings.h"
#include "ompi/runtime/params.h"
#include "ompi/communicator/communicator.h"
#include "ompi/errhandler/errhandler.h"
#include "ompi/datatype/ompi_datatype.h"
#include "ompi/memchecker.h"

#if OMPI_BUILD_MPI_PROFILING
#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak MPI_Get_elements = PMPI_Get_elements
#endif
#define MPI_Get_elements PMPI_Get_elements
#endif

static const char FUNC_NAME[] = "MPI_Get_elements";

int MPI_Get_elements(const MPI_Status *status, MPI_Datatype datatype, int *count)
{
    size_t internal_count;
    int ret;

    OPAL_CR_NOOP_PROGRESS();

    MEMCHECKER(
               if (status != MPI_STATUSES_IGNORE) {
                   /*
                    * Before checking the complete status, we need to reset the definedness
                    * of the MPI_ERROR-field (single-completion calls wait/test).
                    */
                   opal_memchecker_base_mem_defined((void*)&status->MPI_ERROR, sizeof(int));
                   memchecker_status(status);
                   memchecker_datatype(datatype);
               }
               );

    if (MPI_PARAM_CHECK) {
        int err = MPI_SUCCESS;
        OMPI_ERR_INIT_FINALIZE(FUNC_NAME);
        if (NULL == status || MPI_STATUSES_IGNORE == status ||
            MPI_STATUS_IGNORE == status || NULL == count) {
            err = MPI_ERR_ARG;
        } else if (NULL == datatype || MPI_DATATYPE_NULL == datatype) {
            err = MPI_ERR_TYPE;
        } else {
            OMPI_CHECK_DATATYPE_FOR_RECV(err, datatype, 1);
        }
        OMPI_ERRHANDLER_CHECK(err, MPI_COMM_WORLD, err, FUNC_NAME);
    }

    ret = ompi_datatype_get_elements (datatype, status->_ucount, &internal_count);
    if (OMPI_SUCCESS == ret || OMPI_ERR_VALUE_OUT_OF_BOUNDS == ret) {
        if (OMPI_SUCCESS == ret && internal_count <= INT_MAX) {
            *count = internal_count;
        } else {
            /* If we have more elements that we can represent with a signed int then we must
             * set count to MPI_UNDEFINED (MPI 3.0).
             */
            *count = MPI_UNDEFINED;
        }

        return MPI_SUCCESS;
    }

    return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_ARG, FUNC_NAME);
}
