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
#pragma weak MPI_T_enum_get_info = PMPI_T_enum_get_info
#endif

#if OMPI_PROFILING_DEFINES
#include "ompi/mpi/tool/profile/defines.h"
#endif


int MPI_T_enum_get_info(MPI_T_enum enumtype, int *num, char *name, int *name_len)
{
    int rc = MPI_SUCCESS;

    if (!mpit_is_initialized ()) {
        return MPI_T_ERR_NOT_INITIALIZED;
    }

    ompi_mpit_lock ();

    do {
        if (num) {
            rc = enumtype->get_count (enumtype, num);
            if (OPAL_SUCCESS != rc) {
                rc = MPI_ERR_OTHER;
                break;
            }
        }

        mpit_copy_string (name, name_len, enumtype->enum_name);
    } while (0);

    ompi_mpit_unlock ();

    return rc;
}
