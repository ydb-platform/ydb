/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2013 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2012-2013 Inria.  All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2018 IBM Corporation. All rights reserved.
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
#include "opal/util/show_help.h"
#include <stdlib.h>
#include <string.h>

#if OMPI_BUILD_MPI_PROFILING
#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak MPI_Info_set = PMPI_Info_set
#endif
#define MPI_Info_set PMPI_Info_set
#endif

static const char FUNC_NAME[] = "MPI_Info_set";


/**
 *   MPI_Info_set - Set a (key, value) pair in an 'MPI_Info' object
 *
 *   @param key null-terminated character string of the index key
 *   @param value null-terminated character string of the value
 *   @param info info object (handle)
 *
 *   @retval MPI_SUCCESS
 *   @retval MPI_ERR_ARG
 *   @retval MPI_ERR_INFO_KEY
 *   @retval MPI_ERR_INFO_VAL
 *   @retval MPI_ERR_INFO_NOKEY
 *   @retval MPI_ERR_NO_MEM
 *
 *   MPI_Info_set adds the (key,value) pair to info, and overrides
 *   the value if for the same key a previsou value was set. key and
 *   value must be NULL terminated strings in C. In Fortan, leading
 *   and trailing spaces in key and value are stripped. If either
 *   key or value is greater than the allowed maxima, MPI_ERR_INFO_KEY
 *   and MPI_ERR_INFO_VALUE are raised
 */
int MPI_Info_set(MPI_Info info, const char *key, const char *value)
{
    int err;
    int key_length;
    int value_length;

    /*
     * Error conditions are
     *   - info is NULL
     *   - No storage space available for the new value
     *   - Key length exceeded MPI_MAX_KEY_VAL
     *   - value length exceeded MPI_MAX_KEY_VAL
     */

    if (MPI_PARAM_CHECK) {
        OMPI_ERR_INIT_FINALIZE(FUNC_NAME);
        if (NULL == info || MPI_INFO_NULL == info ||
            ompi_info_is_freed(info)) {
            return OMPI_ERRHANDLER_INVOKE (MPI_COMM_WORLD, MPI_ERR_INFO,
                                           FUNC_NAME);
        }

        key_length = (key) ? (int)strlen (key) : 0;
        if ((NULL == key) || (0 == key_length) ||
            (MPI_MAX_INFO_KEY <= key_length)) {
            return OMPI_ERRHANDLER_INVOKE (MPI_COMM_WORLD, MPI_ERR_INFO_KEY,
                                           FUNC_NAME);
        }

        value_length = (value) ? (int)strlen (value) : 0;
        if ((NULL == value) || (0 == value_length) ||
            (MPI_MAX_INFO_VAL <= value_length)) {
            return OMPI_ERRHANDLER_INVOKE (MPI_COMM_WORLD, MPI_ERR_INFO_VALUE,
                                           FUNC_NAME);
        }
    }

// An extra warning condition is a key that uses our reserved prefix "__IN_".
// That one is used internally to deal with the dynamic nature the key/val
// pairs where we have callbacks that modify the val, and the MPI standard
// wants the get_info call to give back the original setting rather than
// the callback-modified setting. So if a user directly used a key __IN_foo
// it would confuse our accounting slightly.
    if (0 == strncmp(key, OPAL_INFO_SAVE_PREFIX, strlen(OPAL_INFO_SAVE_PREFIX))) {
        opal_show_help("help-mpi-api.txt", "info-set-with-reserved-prefix", true,
            key, OPAL_INFO_SAVE_PREFIX);
    }

    OPAL_CR_ENTER_LIBRARY();

    /*
     * If all is right with the arguments, then call the back-end
     * allocator.
     */

    err = ompi_info_set (info, key, value);
    OMPI_ERRHANDLER_RETURN(err, MPI_COMM_WORLD, err, FUNC_NAME);
}
