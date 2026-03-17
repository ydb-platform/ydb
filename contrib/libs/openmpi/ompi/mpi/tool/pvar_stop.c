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
#pragma weak MPI_T_pvar_stop = PMPI_T_pvar_stop
#endif

#if OMPI_PROFILING_DEFINES
#include "ompi/mpi/tool/profile/defines.h"
#endif


static int pvar_handle_stop (mca_base_pvar_handle_t *handle)
{
    if (OPAL_SUCCESS != mca_base_pvar_handle_stop (handle)) {
        return MPI_T_ERR_PVAR_NO_STARTSTOP;
    }

    return MPI_SUCCESS;
}

int MPI_T_pvar_stop(MPI_T_pvar_session session, MPI_T_pvar_handle handle)
{
    int ret = MPI_SUCCESS;

    if (!mpit_is_initialized ()) {
        return MPI_T_ERR_NOT_INITIALIZED;
    }

    ompi_mpit_lock ();

    if (MPI_T_PVAR_ALL_HANDLES == handle) {
        OPAL_LIST_FOREACH(handle, &session->handles, mca_base_pvar_handle_t) {
            /* Per MPI 3.0: ignore continuous and stopped variables when stopping
               all variable handles. */
            if (mca_base_pvar_handle_is_running (handle) && !mca_base_pvar_is_continuous (handle->pvar) &&
                MPI_SUCCESS != pvar_handle_stop (handle)) {
                /* If we failed to stop any variable we need to return
                   an error. */
                ret = MPI_T_ERR_PVAR_NO_STARTSTOP;
            }
        }
    } else {
        ret = pvar_handle_stop (handle);
    }

    ompi_mpit_unlock ();

    return ompit_opal_to_mpit_error (ret);
}
