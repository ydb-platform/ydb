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
 * Copyright (c) 2012      Oak Rigde National Laboratory. All rights reserved.
 * Copyright (c) 2015-2018 Research Organization for Information Science
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
#include "ompi/datatype/ompi_datatype.h"
#include "ompi/memchecker.h"
#include "ompi/mpiext/pcollreq/c/mpiext_pcollreq_c.h"
#include "ompi/runtime/ompi_spc.h"

#if OMPI_BUILD_MPI_PROFILING
#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak MPIX_Barrier_init = PMPIX_Barrier_init
#endif
#define MPIX_Barrier_init PMPIX_Barrier_init
#endif

static const char FUNC_NAME[] = "MPIX_Barrier_init";


int MPIX_Barrier_init(MPI_Comm comm, MPI_Info info, MPI_Request *request)
{
    int err = MPI_SUCCESS;

    SPC_RECORD(OMPI_SPC_BARRIER_INIT, 1);

    MEMCHECKER(
            memchecker_comm(comm);
            );

    /* Error checking */

    if (MPI_PARAM_CHECK) {
        OMPI_ERR_INIT_FINALIZE(FUNC_NAME);
        if (ompi_comm_invalid(comm)) {
            return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_COMM, FUNC_NAME);
        }
    }

    OPAL_CR_ENTER_LIBRARY();

    err = comm->c_coll->coll_barrier_init(comm, info, request, comm->c_coll->coll_barrier_init_module);

    /* All done */

    OMPI_ERRHANDLER_RETURN(err, comm, err, FUNC_NAME);
}
