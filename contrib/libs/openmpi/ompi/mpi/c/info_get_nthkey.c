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
#include <string.h>

#if OMPI_BUILD_MPI_PROFILING
#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak MPI_Info_get_nthkey = PMPI_Info_get_nthkey
#endif
#define MPI_Info_get_nthkey PMPI_Info_get_nthkey
#endif

static const char FUNC_NAME[] = "MPI_Info_get_nthkey";


/**
 *   MPI_Info_get_nthkey - Get a key indexed by integer from an 'MPI_Info' obje
 *
 *   @param info info object (handle)
 *   @param n index of key to retrieve (integer)
 *   @param key character string of at least 'MPI_MAX_INFO_KEY' characters
 *
 *   @retval MPI_SUCCESS
 *   @retval MPI_ERR_ARG
 *   @retval MPI_ERR_INFO
 *   @retval MPI_ERR_INFO_KEY
 */
int MPI_Info_get_nthkey(MPI_Info info, int n, char *key)
{
    int nkeys;
    int err;

    /*
     * 1. Check if info is a valid handle
     * 2. Check if there are at least (n+1) elements
     * 3. If so, give the nth defined key
     */
    if (MPI_PARAM_CHECK) {
        OMPI_ERR_INIT_FINALIZE(FUNC_NAME);
        if (NULL == info || MPI_INFO_NULL == info ||
            ompi_info_is_freed(info)) {
            return OMPI_ERRHANDLER_INVOKE (MPI_COMM_WORLD, MPI_ERR_INFO,
                                           FUNC_NAME);
        }
        if (0 > n) {
            return OMPI_ERRHANDLER_INVOKE (MPI_COMM_WORLD, MPI_ERR_ARG,
                                           FUNC_NAME);
        }
        if (NULL == key) {
            return OMPI_ERRHANDLER_INVOKE (MPI_COMM_WORLD, MPI_ERR_INFO_KEY,
                                           FUNC_NAME);
        }
    }

    OPAL_CR_ENTER_LIBRARY();

    /* Keys are indexed on 0, which makes the "n" parameter offset by
       1 from the value returned by get_nkeys().  So be sure to
       compare appropriately. */

    err = ompi_info_get_nkeys(info, &nkeys);
    OMPI_ERRHANDLER_CHECK(err, MPI_COMM_WORLD, err, FUNC_NAME);
    if (n > (nkeys - 1)) {
        OPAL_CR_EXIT_LIBRARY();
        return OMPI_ERRHANDLER_INVOKE (MPI_COMM_WORLD, MPI_ERR_INFO_KEY,
                                       FUNC_NAME);
    }

    /* Everything seems alright. Call the back end key copy */

    err = ompi_info_get_nthkey (info, n, key);
    OMPI_ERRHANDLER_RETURN(err, MPI_COMM_WORLD, err, FUNC_NAME);
}
