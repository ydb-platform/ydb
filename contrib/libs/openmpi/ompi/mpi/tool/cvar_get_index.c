/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2012-2014 Los Alamos National Security, LLC. All rights
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
#pragma weak MPI_T_cvar_get_index = PMPI_T_cvar_get_index
#endif

#if OMPI_PROFILING_DEFINES
#include "ompi/mpi/tool/profile/defines.h"
#endif


int MPI_T_cvar_get_index (const char *name, int *cvar_index)
{
    int ret;

    if (!mpit_is_initialized ()) {
        return MPI_T_ERR_NOT_INITIALIZED;
    }

    if (MPI_PARAM_CHECK && (NULL == cvar_index || NULL == name)) {
        return MPI_ERR_ARG;
    }

    ompi_mpit_lock ();
    ret = mca_base_var_find_by_name (name, cvar_index);
    ompi_mpit_unlock ();
    if (OPAL_SUCCESS != ret) {
        return MPI_T_ERR_INVALID_NAME;
    }

    return MPI_SUCCESS;
}
