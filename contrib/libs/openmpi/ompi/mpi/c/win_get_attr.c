/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2006 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC. All rights
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

#include "ompi/mpi/c/bindings.h"
#include "ompi/runtime/params.h"
#include "ompi/communicator/communicator.h"
#include "ompi/errhandler/errhandler.h"
#include "ompi/attribute/attribute.h"
#include "ompi/win/win.h"

#if OMPI_BUILD_MPI_PROFILING
#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak MPI_Win_get_attr = PMPI_Win_get_attr
#endif
#define MPI_Win_get_attr PMPI_Win_get_attr
#endif

static const char FUNC_NAME[] = "MPI_Win_get_attr";


int MPI_Win_get_attr(MPI_Win win, int win_keyval,
                     void *attribute_val, int *flag)
{
    int ret;

    if (MPI_PARAM_CHECK) {
       OMPI_ERR_INIT_FINALIZE(FUNC_NAME);

       if (ompi_win_invalid(win)) {
           return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_WIN, FUNC_NAME);
       } else if ((NULL == attribute_val) || (NULL == flag)) {
           return OMPI_ERRHANDLER_INVOKE(win, MPI_ERR_ARG, FUNC_NAME);
        } else if (MPI_KEYVAL_INVALID == win_keyval) {
            return OMPI_ERRHANDLER_INVOKE(win, MPI_ERR_KEYVAL, FUNC_NAME);
       }
    }

    OPAL_CR_ENTER_LIBRARY();

    /* This stuff is very confusing.  Be sure to see
       src/attribute/attribute.c for a lengthy comment explaining Open
       MPI attribute behavior. */

    ret = ompi_attr_get_c(win->w_keyhash, win_keyval,
                          (void**)attribute_val, flag);

    OMPI_ERRHANDLER_RETURN(ret, win, MPI_ERR_OTHER, FUNC_NAME);
}
