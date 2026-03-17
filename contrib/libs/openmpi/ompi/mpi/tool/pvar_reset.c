/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2012-2015 Los Alamos National Security, LLC. All rights
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
#pragma weak MPI_T_pvar_reset = PMPI_T_pvar_reset
#endif

#if OMPI_PROFILING_DEFINES
#include "ompi/mpi/tool/profile/defines.h"
#endif


int MPI_T_pvar_reset(MPI_T_pvar_session session, MPI_T_pvar_handle handle)
{
    int ret = MPI_SUCCESS;

    if (!mpit_is_initialized ()) {
        return MPI_T_ERR_NOT_INITIALIZED;
    }

    ompi_mpit_lock ();

    if (MPI_T_PVAR_ALL_HANDLES == handle) {
        OPAL_LIST_FOREACH(handle, &session->handles, mca_base_pvar_handle_t) {
            /* Per MPI 3.0: ignore read-only variables when resetting all
               handles. */
            if (!mca_base_pvar_is_readonly (handle->pvar) &&
                MPI_SUCCESS != mca_base_pvar_handle_reset (handle)) {
                ret = MPI_T_ERR_PVAR_NO_WRITE;
            }
        }
    } else {
        ret = mca_base_pvar_handle_reset (handle);
    }

    ompi_mpit_unlock ();

    return ompit_opal_to_mpit_error (ret);
}
