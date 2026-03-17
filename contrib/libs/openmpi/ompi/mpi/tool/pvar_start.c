/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2012-2013 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2014 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2017      IBM Corporation. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi/mpi/tool/mpit-internal.h"

#if OPAL_HAVE_WEAK_SYMBOLS && OMPI_PROFILING_DEFINES
#pragma weak MPI_T_pvar_start = PMPI_T_pvar_start
#endif

#if OMPI_PROFILING_DEFINES
#include "ompi/mpi/tool/profile/defines.h"
#endif


static int pvar_handle_start (mca_base_pvar_handle_t *handle)
{
    if (OPAL_SUCCESS != mca_base_pvar_handle_start (handle)) {
        return MPI_T_ERR_PVAR_NO_STARTSTOP;
    }

    return MPI_SUCCESS;
}

int MPI_T_pvar_start(MPI_T_pvar_session session, MPI_T_pvar_handle handle)
{
    int ret = MPI_SUCCESS;

    if (!mpit_is_initialized ()) {
        return MPI_T_ERR_NOT_INITIALIZED;
    }

    ompi_mpit_lock ();

    if (MPI_T_PVAR_ALL_HANDLES == handle) {
        OPAL_LIST_FOREACH(handle, &session->handles, mca_base_pvar_handle_t) {
            /* Per MPI 3.0: ignore continuous and started variables when starting
               all variable handles. */
            if (!mca_base_pvar_handle_is_running (handle) &&
                OMPI_SUCCESS != pvar_handle_start (handle)) {
                ret = MPI_T_ERR_PVAR_NO_STARTSTOP;
            }
        }
    } else {
        ret = pvar_handle_start (handle);
    }

    ompi_mpit_unlock ();

    return ompit_opal_to_mpit_error (ret);
}
