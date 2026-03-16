/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2012-213 Los Alamos National Security, LLC. All rights
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
#pragma weak MPI_T_category_get_pvars = PMPI_T_category_get_pvars
#endif

#if OMPI_PROFILING_DEFINES
#include "ompi/mpi/tool/profile/defines.h"
#endif


int MPI_T_category_get_pvars(int cat_index, int len, int indices[])
{
    const mca_base_var_group_t *group;
    int rc = MPI_SUCCESS;
    const int *vars;
    int i, size;

    if (!mpit_is_initialized ()) {
        return MPI_T_ERR_NOT_INITIALIZED;
    }

    ompi_mpit_lock ();

    do {
        rc = mca_base_var_group_get (cat_index, &group);
        if (0 > rc) {
            rc = (OPAL_ERR_NOT_FOUND == rc) ? MPI_T_ERR_INVALID_INDEX : MPI_ERR_OTHER;
            break;
        }

        size = opal_value_array_get_size((opal_value_array_t *) &group->group_pvars);
        vars = OPAL_VALUE_ARRAY_GET_BASE(&group->group_pvars, int);

        for (i = 0 ; i < len && i < size ; ++i) {
            indices[i] = vars[i];
        }
    } while (0);

    ompi_mpit_unlock ();

    return rc;
}
