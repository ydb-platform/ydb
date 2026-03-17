/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
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
 * Copyright (c) 2013      Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2017      Cisco Systems, Inc.  All rights reserved
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
#include <stdlib.h>
#include <string.h>

#if OMPI_BUILD_MPI_PROFILING
#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak MPI_Info_delete = PMPI_Info_delete
#endif
#define MPI_Info_delete PMPI_Info_delete
#endif

static const char FUNC_NAME[] = "MPI_Info_delete";


/**
 * Delete a (key,value) pair from "info"
 *
 * @param info MPI_Info handle on which we need to operate
 * @param key The key portion of the (key,value) pair that
 *            needs to be deleted
 *
 * @retval MPI_SUCCESS If the (key,val) pair was deleted
 * @retval MPI_ERR_INFO
 * @retval MPI_ERR_INFO_KEYY
 */
int MPI_Info_delete(MPI_Info info, const char *key) {
    int key_length;
    int err;

    /**
     * This function merely deletes the (key,val) pair in info
     */
    if (MPI_PARAM_CHECK) {
        OMPI_ERR_INIT_FINALIZE(FUNC_NAME);
        if (NULL == info || MPI_INFO_NULL == info ||
            ompi_info_is_freed(info)) {
            return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_INFO,
                                          FUNC_NAME);
        }

        key_length = (key) ? (int)strlen (key) : 0;
        if ((NULL == key) || (0 == key_length) ||
            (MPI_MAX_INFO_KEY <= key_length)) {
          return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_INFO_KEY,
                                        FUNC_NAME);
        }
    }

    OPAL_CR_ENTER_LIBRARY();

    err = ompi_info_delete (info, key);

    // Note that ompi_info_delete() (i.e., opal_info_delete()) will
    // return OPAL_ERR_NOT_FOUND if there was no corresponding key to
    // delete.  Per MPI-3.1, we need to convert that to
    // MPI_ERR_INFO_NOKEY.
    if (OPAL_ERR_NOT_FOUND == err) {
        err = MPI_ERR_INFO_NOKEY;
    }

    OMPI_ERRHANDLER_RETURN(err, MPI_COMM_WORLD, err, FUNC_NAME);
}
