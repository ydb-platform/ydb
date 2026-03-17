/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2012-2018 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2014 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2016      Intel, Inc.  All rights reserved.
 * Copyright (c) 2017      IBM Corporation. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi/mpi/tool/mpit-internal.h"

#if OPAL_HAVE_WEAK_SYMBOLS && OMPI_PROFILING_DEFINES
#pragma weak MPI_T_cvar_read = PMPI_T_cvar_read
#endif

#if OMPI_PROFILING_DEFINES
#include "ompi/mpi/tool/profile/defines.h"
#endif


int MPI_T_cvar_read (MPI_T_cvar_handle handle, void *buf)
{
    const mca_base_var_storage_t *value = NULL;
    int rc = MPI_SUCCESS;

    if (!mpit_is_initialized ()) {
        return MPI_T_ERR_NOT_INITIALIZED;
    }

    if (MPI_PARAM_CHECK && NULL == buf) {
        return MPI_ERR_ARG;
    }

    ompi_mpit_lock ();

    do {
        rc = mca_base_var_get_value(handle->var->mbv_index, &value, NULL, NULL);
        if (OPAL_SUCCESS != rc || NULL == value) {
            /* shouldn't happen */
            rc = MPI_ERR_OTHER;
            break;
        }

        switch (handle->var->mbv_type) {
        case MCA_BASE_VAR_TYPE_INT:
        case MCA_BASE_VAR_TYPE_UNSIGNED_INT:
            ((int *) buf)[0] = value->intval;
            break;
        case MCA_BASE_VAR_TYPE_INT32_T:
        case MCA_BASE_VAR_TYPE_UINT32_T:
            ((int32_t *) buf)[0] = value->int32tval;
            break;
        case MCA_BASE_VAR_TYPE_INT64_T:
        case MCA_BASE_VAR_TYPE_UINT64_T:
            ((int64_t *) buf)[0] = value->int64tval;
            break;
        case MCA_BASE_VAR_TYPE_LONG:
        case MCA_BASE_VAR_TYPE_UNSIGNED_LONG:
            ((unsigned long *) buf)[0] = value->ulval;
            break;
        case MCA_BASE_VAR_TYPE_UNSIGNED_LONG_LONG:
            ((unsigned long long *) buf)[0] = value->ullval;
            break;
        case MCA_BASE_VAR_TYPE_SIZE_T:
            ((size_t *) buf)[0] = value->sizetval;
            break;
        case MCA_BASE_VAR_TYPE_BOOL:
            ((bool *) buf)[0] = value->boolval;
            break;
        case MCA_BASE_VAR_TYPE_DOUBLE:
            ((double *) buf)[0] = value->lfval;
            break;
        case MCA_BASE_VAR_TYPE_STRING:
            if (NULL == value->stringval) {
                ((char *)buf)[0] = '\0';
            } else {
                strcpy ((char *) buf, value->stringval);
            }

            break;
        default:
            rc = MPI_ERR_OTHER;
        }
    } while (0);

    ompi_mpit_unlock ();

    return rc;
}
