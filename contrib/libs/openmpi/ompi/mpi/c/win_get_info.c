/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2013      Sandia National Laboratories.  All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC. All rights
 *                         reserved.
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
#include "ompi/errhandler/errhandler.h"
#include "ompi/win/win.h"
#include "opal/util/info.h"
#include "opal/util/info_subscriber.h"

#if OMPI_BUILD_MPI_PROFILING
#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak MPI_Win_get_info = PMPI_Win_get_info
#endif
#define MPI_Win_get_info PMPI_Win_get_info
#endif

static const char FUNC_NAME[] = "MPI_Win_get_info";

int MPI_Win_get_info(MPI_Win win, MPI_Info *info_used)
{
    int ret;

    OPAL_CR_NOOP_PROGRESS();

    if (MPI_PARAM_CHECK) {
        OMPI_ERR_INIT_FINALIZE(FUNC_NAME);

        if (ompi_win_invalid(win)) {
            return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_WIN, FUNC_NAME);
        }

        if (NULL == info_used) {
            return OMPI_ERRHANDLER_INVOKE(win, MPI_ERR_ARG, FUNC_NAME);
        }
    }

    if (NULL == win->super.s_info) {
/*
 * Setup any defaults if MPI_Win_set_info was never called
 */
	opal_infosubscribe_change_info(&win->super, &MPI_INFO_NULL->super); 	
    }

    (*info_used) = OBJ_NEW(ompi_info_t);
    if (NULL == (*info_used)) {
       return OMPI_ERRHANDLER_INVOKE(win, MPI_ERR_NO_MEM, FUNC_NAME);
    }
    opal_info_t *opal_info_used = &(*info_used)->super;

    ret = opal_info_dup_mpistandard(win->super.s_info, &opal_info_used);

    OMPI_ERRHANDLER_RETURN(ret, win, ret, FUNC_NAME);
}
