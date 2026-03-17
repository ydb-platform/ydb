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
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2015-2018 Cisco Systems, Inc.  All rights reserved
 * Copyright (c) 2015      Intel, Inc. All rights reserved
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include "ompi/mpi/c/bindings.h"
#include "ompi/runtime/params.h"
#include "ompi/communicator/communicator.h"
#include "ompi/errhandler/errhandler.h"
#include "ompi/mca/hook/base/base.h"

#if OMPI_BUILD_MPI_PROFILING
#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak MPI_Finalized = PMPI_Finalized
#endif
#define MPI_Finalized PMPI_Finalized
#endif

static const char FUNC_NAME[] = "MPI_Finalized";


int MPI_Finalized(int *flag)
{
    OPAL_CR_NOOP_PROGRESS();

    ompi_hook_base_mpi_finalized_top(flag);

    int32_t state = ompi_mpi_state;

    if (MPI_PARAM_CHECK) {
        if (NULL == flag) {

            /* If we have an error, the action that we take depends on
               whether we're currently (after MPI_Init and before
               MPI_Finalize) or not */

            if (state >= OMPI_MPI_STATE_INIT_COMPLETED &&
                state < OMPI_MPI_STATE_FINALIZE_PAST_COMM_SELF_DESTRUCT) {
                return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_ARG,
                                              FUNC_NAME);
            } else {
                /* We have no MPI object here so call ompi_errhandle_invoke
                 * directly */
                return ompi_errhandler_invoke(NULL, NULL, -1,
                                              ompi_errcode_get_mpi_code(MPI_ERR_ARG),
                                              FUNC_NAME);
            }
        }
    }

    *flag = (state >= OMPI_MPI_STATE_FINALIZE_PAST_COMM_SELF_DESTRUCT);

    ompi_hook_base_mpi_finalized_bottom(flag);

    return MPI_SUCCESS;
}
