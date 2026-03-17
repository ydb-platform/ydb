/*
 * Copyright (c) 2013      Sandia National Laboratories.  All rights reserved.
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
#include "ompi/communicator/communicator.h"
#include "opal/util/info_subscriber.h"

#if OMPI_BUILD_MPI_PROFILING
#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak MPI_Win_set_info = PMPI_Win_set_info
#endif
#define MPI_Win_set_info PMPI_Win_set_info
#endif

static const char FUNC_NAME[] = "MPI_Win_set_info";


int MPI_Win_set_info(MPI_Win win, MPI_Info info)
{
    int ret;

    if (MPI_PARAM_CHECK) {
        OMPI_ERR_INIT_FINALIZE(FUNC_NAME);

        if (ompi_win_invalid(win)) {
            return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_WIN, FUNC_NAME);
        }

        if (NULL == info || MPI_INFO_NULL == info ||
            ompi_info_is_freed(info)) {
            return OMPI_ERRHANDLER_INVOKE(win, MPI_ERR_INFO, FUNC_NAME);
        }
    }

    OPAL_CR_ENTER_LIBRARY();

    ret = opal_infosubscribe_change_info(&(win->super), &(info->super));

    OMPI_ERRHANDLER_RETURN(ret, win, ret, FUNC_NAME);
}
