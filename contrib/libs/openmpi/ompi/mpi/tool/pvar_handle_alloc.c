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
#pragma weak MPI_T_pvar_handle_alloc = PMPI_T_pvar_handle_alloc
#endif

#if OMPI_PROFILING_DEFINES
#include "ompi/mpi/tool/profile/defines.h"
#endif


int MPI_T_pvar_handle_alloc(MPI_T_pvar_session session, int pvar_index,
                            void *obj_handle, MPI_T_pvar_handle *handle, int *count)
{
    const mca_base_pvar_t *pvar;
    int ret;

    if (!mpit_is_initialized ()) {
        return MPI_T_ERR_NOT_INITIALIZED;
    }

    ompi_mpit_lock ();

    do {
        /* Find the performance variable. mca_base_pvar_get() handles the
           bounds checking. */
        ret = mca_base_pvar_get (pvar_index, &pvar);
        if (OMPI_SUCCESS != ret) {
            break;
        }

        /* Check the variable binding is something sane */
        if (pvar->bind > MPI_T_BIND_MPI_INFO || pvar->bind < MPI_T_BIND_NO_OBJECT) {
            /* This variable specified an invalid binding (not an MPI object). */
            ret = MPI_T_ERR_INVALID_INDEX;
            break;
        }

        ret = mca_base_pvar_handle_alloc (session, pvar_index, obj_handle,
                                          handle, count);
    } while (0);

    ompi_mpit_unlock ();

    return ompit_opal_to_mpit_error(ret);
}
