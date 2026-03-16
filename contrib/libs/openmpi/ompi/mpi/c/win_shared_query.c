/*
 * Copyright (c) 2012-2013 Sandia National Laboratories. All rights reserved.
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
#include "ompi/win/win.h"
#include "ompi/mca/osc/osc.h"

#if OMPI_BUILD_MPI_PROFILING
#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak MPI_Win_shared_query = PMPI_Win_shared_query
#endif
#define MPI_Win_shared_query PMPI_Win_shared_query
#endif

static const char FUNC_NAME[] = "MPI_Win_shared_query";


int MPI_Win_shared_query(MPI_Win win, int rank, MPI_Aint *size, int *disp_unit, void *baseptr)
{
    int rc;
    size_t tsize;

    if (MPI_PARAM_CHECK) {
        OMPI_ERR_INIT_FINALIZE(FUNC_NAME);

        if (ompi_win_invalid(win)) {
            return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_WIN, FUNC_NAME);
         } else if (MPI_PROC_NULL != rank && ompi_win_peer_invalid(win, rank)) {
            return OMPI_ERRHANDLER_INVOKE(win, MPI_ERR_RANK, FUNC_NAME);
         }
    }

    OPAL_CR_ENTER_LIBRARY();

    if (NULL != win->w_osc_module->osc_win_shared_query) {
        rc = win->w_osc_module->osc_win_shared_query(win, rank, &tsize, disp_unit, baseptr);
        *size = tsize;
    } else {
        rc = MPI_ERR_RMA_FLAVOR;
    }
    OMPI_ERRHANDLER_RETURN(rc, win, rc, FUNC_NAME);
}
