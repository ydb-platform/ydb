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
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2017      IBM Corporation. All rights reserved.
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
#include "ompi/info/info.h"
#include "ompi/win/win.h"
#include "ompi/memchecker.h"

#if OMPI_BUILD_MPI_PROFILING
#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak MPI_Win_create_dynamic = PMPI_Win_create_dynamic
#endif
#define MPI_Win_create_dynamic PMPI_Win_create_dynamic
#endif

static const char FUNC_NAME[] = "MPI_Win_create_dynamic";


int MPI_Win_create_dynamic(MPI_Info info, MPI_Comm comm, MPI_Win *win)
{
    int ret = MPI_SUCCESS;

    MEMCHECKER(
        memchecker_comm(comm);
    );
    /* argument checking */
    if (MPI_PARAM_CHECK) {
        OMPI_ERR_INIT_FINALIZE(FUNC_NAME);

        if (ompi_comm_invalid (comm)) {
            return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_COMM,
                                          FUNC_NAME);

        } else if (NULL == info || ompi_info_is_freed(info)) {
            return OMPI_ERRHANDLER_INVOKE(comm, MPI_ERR_INFO,
                                          FUNC_NAME);

        } else if (NULL == win) {
            return OMPI_ERRHANDLER_INVOKE(comm, MPI_ERR_WIN, FUNC_NAME);
        }
    }

    /* communicator must be an intracommunicator */
    if (OMPI_COMM_IS_INTER(comm)) {
        return OMPI_ERRHANDLER_INVOKE(comm, MPI_ERR_COMM, FUNC_NAME);
    }

    OPAL_CR_ENTER_LIBRARY();

    /* create_dynamic window and return */
    ret = ompi_win_create_dynamic(&(info->super), comm, win);
    if (OMPI_SUCCESS != ret) {
        *win = MPI_WIN_NULL;
        OPAL_CR_EXIT_LIBRARY();
        return OMPI_ERRHANDLER_INVOKE(comm, MPI_ERR_WIN, FUNC_NAME);
    }

    OPAL_CR_EXIT_LIBRARY();
    return MPI_SUCCESS;
}
