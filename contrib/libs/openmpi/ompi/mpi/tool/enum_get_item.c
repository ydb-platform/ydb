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
#pragma weak MPI_T_enum_get_item = PMPI_T_enum_get_item
#endif

#if OMPI_PROFILING_DEFINES
#include "ompi/mpi/tool/profile/defines.h"
#endif


int MPI_T_enum_get_item(MPI_T_enum enumtype, int index, int *value, char *name,
                        int *name_len)
{
    const char *tmp;
    int rc, count;

    if (!mpit_is_initialized ()) {
        return MPI_T_ERR_NOT_INITIALIZED;
    }

    ompi_mpit_lock ();

    do {
        rc = enumtype->get_count (enumtype, &count);
        if (OPAL_SUCCESS != rc) {
            rc = MPI_ERR_OTHER;
            break;
        }

        if (index >= count) {
            rc = MPI_T_ERR_INVALID_INDEX;
            break;
        }

        rc = enumtype->get_value(enumtype, index, value, &tmp);
        if (OPAL_SUCCESS != rc) {
            rc = MPI_ERR_OTHER;
            break;
        }

        mpit_copy_string(name, name_len, tmp);
    } while (0);

    ompi_mpit_unlock ();

    return rc;
}
