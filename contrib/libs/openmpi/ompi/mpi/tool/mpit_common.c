/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2012-2018 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2017      IBM Corporation. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi/mpi/tool/mpit-internal.h"

opal_mutex_t ompi_mpit_big_lock = OPAL_MUTEX_STATIC_INIT;

volatile uint32_t ompi_mpit_init_count = 0;

void ompi_mpit_lock (void)
{
    opal_mutex_lock (&ompi_mpit_big_lock);
}

void ompi_mpit_unlock (void)
{
    opal_mutex_unlock (&ompi_mpit_big_lock);
}

static MPI_Datatype mca_to_mpi_datatypes[MCA_BASE_VAR_TYPE_MAX] = {
    [MCA_BASE_VAR_TYPE_INT] = MPI_INT,
    [MCA_BASE_VAR_TYPE_UNSIGNED_INT] = MPI_UNSIGNED,
    [MCA_BASE_VAR_TYPE_UNSIGNED_LONG] = MPI_UNSIGNED_LONG,
    [MCA_BASE_VAR_TYPE_UNSIGNED_LONG_LONG] = MPI_UNSIGNED_LONG_LONG,

#if SIZEOF_SIZE_T == SIZEOF_UNSIGNED_INT
    [MCA_BASE_VAR_TYPE_SIZE_T] = MPI_UNSIGNED,
#elif SIZEOF_SIZE_T == SIZEOF_UNSIGNED_LONG
    [MCA_BASE_VAR_TYPE_SIZE_T] = MPI_UNSIGNED_LONG,
#elif SIZEOF_SIZE_T == SIZEOF_LONG_LONG
    [MCA_BASE_VAR_TYPE_SIZE_T] = MPI_UNSIGNED_LONG_LONG,
#else
    [MCA_BASE_VAR_TYPE_SIZE_T] = NULL,
#endif

    [MCA_BASE_VAR_TYPE_STRING] = MPI_CHAR,
    [MCA_BASE_VAR_TYPE_VERSION_STRING] = MPI_CHAR,
    [MCA_BASE_VAR_TYPE_BOOL] = MPI_C_BOOL,
    [MCA_BASE_VAR_TYPE_DOUBLE] = MPI_DOUBLE,
    [MCA_BASE_VAR_TYPE_LONG] = MPI_LONG,
    [MCA_BASE_VAR_TYPE_INT32_T] = MPI_INT32_T,
    [MCA_BASE_VAR_TYPE_UINT32_T] = MPI_UINT32_T,
    [MCA_BASE_VAR_TYPE_INT64_T] = MPI_INT64_T,
    [MCA_BASE_VAR_TYPE_UINT64_T] = MPI_UINT64_T,
};

int ompit_var_type_to_datatype (mca_base_var_type_t type, MPI_Datatype *datatype)
{
    if (!datatype) {
        return OMPI_SUCCESS;
    }

    *datatype = mca_to_mpi_datatypes[type];
    assert (*datatype);

    return OMPI_SUCCESS;
}

int ompit_opal_to_mpit_error (int rc)
{
    if (rc >= 0) {
        /* Already an MPI error (always >= 0) */
        return rc;
    }

    switch (rc) {
    case OPAL_ERR_OUT_OF_RESOURCE:
        return MPI_T_ERR_MEMORY;
    case OPAL_ERR_VALUE_OUT_OF_BOUNDS:
    case OPAL_ERR_NOT_BOUND:
        return MPI_T_ERR_INVALID_HANDLE;
    default:
        return MPI_ERR_UNKNOWN;
    }
}
