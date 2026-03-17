/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
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
 * Copyright (c) 2008-2009 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2015-2017 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2016      Los Alamos National Security, LLC. All rights
 *                         reserved.
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

#if OMPI_BUILD_MPI_PROFILING
#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak MPI_Win_get_errhandler = PMPI_Win_get_errhandler
#endif
#define MPI_Win_get_errhandler PMPI_Win_get_errhandler
#endif

static const char FUNC_NAME[] = "MPI_Win_get_errhandler";


int MPI_Win_get_errhandler(MPI_Win win, MPI_Errhandler *errhandler)
{
    OPAL_CR_NOOP_PROGRESS();

    if (MPI_PARAM_CHECK) {
        OMPI_ERR_INIT_FINALIZE(FUNC_NAME);
        if (ompi_win_invalid(win)) {
            return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_WIN,
                                          FUNC_NAME);
        } else if (NULL == errhandler) {
            return OMPI_ERRHANDLER_INVOKE(win, MPI_ERR_ARG,
                                          FUNC_NAME);
        }
    }

    OPAL_THREAD_LOCK(&win->w_lock);
    /* Retain the errhandler, corresponding to object refcount
       decrease in errhandler_free.c. */
    OBJ_RETAIN(win->error_handler);
    *errhandler = win->error_handler;
    OPAL_THREAD_UNLOCK(&win->w_lock);

    /* All done */
    return MPI_SUCCESS;
}
