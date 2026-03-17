/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2014 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2016-2017 IBM Corporation. All rights reserved.
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
#include "ompi/info/info.h"
#include <stdlib.h>
#include <string.h>

#if OMPI_BUILD_MPI_PROFILING
#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak MPI_Comm_get_info = PMPI_Comm_get_info
#endif
#define MPI_Comm_get_info PMPI_Comm_get_info
#endif

static const char FUNC_NAME[] = "MPI_Comm_get_info";


int MPI_Comm_get_info(MPI_Comm comm, MPI_Info *info_used)
{
    OPAL_CR_NOOP_PROGRESS();

    if (MPI_PARAM_CHECK) {
        OMPI_ERR_INIT_FINALIZE(FUNC_NAME);
        if (NULL == info_used) {
            return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_INFO,
                                          FUNC_NAME);
        }
        if (ompi_comm_invalid(comm)) {
            return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_COMM,
                                          FUNC_NAME);
        }
    }

    if (NULL == comm->super.s_info) {
/*
 * Setup any defaults if MPI_Win_set_info was never called
 */
        opal_infosubscribe_change_info(&comm->super, &MPI_INFO_NULL->super);
    }


    (*info_used) = OBJ_NEW(ompi_info_t);
    if (NULL == (*info_used)) {
       return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_NO_MEM,
                                      FUNC_NAME);
    }
    opal_info_t *opal_info_used = &(*info_used)->super;

    opal_info_dup_mpistandard(comm->super.s_info, &opal_info_used);

    return MPI_SUCCESS;
}
