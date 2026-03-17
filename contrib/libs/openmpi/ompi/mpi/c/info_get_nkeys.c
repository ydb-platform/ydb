/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include "ompi/mpi/c/bindings.h"
#include "ompi/runtime/params.h"
#include "ompi/communicator/communicator.h"
#include "ompi/errhandler/errhandler.h"
#include "ompi/info/info.h"

#if OMPI_BUILD_MPI_PROFILING
#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak MPI_Info_get_nkeys = PMPI_Info_get_nkeys
#endif
#define MPI_Info_get_nkeys PMPI_Info_get_nkeys
#endif

static const char FUNC_NAME[] = "MPI_Info_get_nkeys";


/**
 * MPI_Info_get_nkeys - Returns the number of keys defined on an
 * 'MPI_Info' object
 *
 * @param info info object (handle)
 * @param nkeys number of keys defined on 'info' (integer)
 *
 * @retval MPI_SUCCESS
 * @retval MPI_ERR_ARG
 * @retval MPI_ERR_INFO
 *
 * This function returns the number of elements in the list
 * containing the key-value pairs
 */
int MPI_Info_get_nkeys(MPI_Info info, int *nkeys)
{
    int err;

    if (MPI_PARAM_CHECK) {
        OMPI_ERR_INIT_FINALIZE(FUNC_NAME);
        if (NULL == info || MPI_INFO_NULL == info ||
            ompi_info_is_freed(info)) {
            return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_INFO,
                                          FUNC_NAME);
        }
        if (NULL == nkeys) {
            return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_ARG,
                                          FUNC_NAME);
        }
    }

    OPAL_CR_ENTER_LIBRARY();

    err = ompi_info_get_nkeys(info, nkeys);
    OMPI_ERRHANDLER_RETURN(err, MPI_COMM_WORLD, err, FUNC_NAME);
}
