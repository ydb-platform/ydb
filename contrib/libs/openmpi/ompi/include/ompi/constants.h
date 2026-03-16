/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2011 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2016      Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef OMPI_CONSTANTS_H
#define OMPI_CONSTANTS_H

#if defined(OMPI_RTE_ORTE) && OMPI_RTE_ORTE
#include "orte/constants.h"
#define OMPI_ERR_BASE   ORTE_ERR_MAX
#else
#include "opal/constants.h"
#define OMPI_ERR_BASE   OPAL_ERR_MAX
#endif

/* error codes */
enum {
    /* Error codes inherited from ORTE/OPAL.  Still enum values so
       that we might get nice debugger help */
    OMPI_SUCCESS                    = OPAL_SUCCESS,
    OMPI_ERROR                      = OPAL_ERROR,
    OMPI_ERR_OUT_OF_RESOURCE        = OPAL_ERR_OUT_OF_RESOURCE,
    OMPI_ERR_TEMP_OUT_OF_RESOURCE   = OPAL_ERR_TEMP_OUT_OF_RESOURCE,
    OMPI_ERR_RESOURCE_BUSY          = OPAL_ERR_RESOURCE_BUSY,
    OMPI_ERR_BAD_PARAM              = OPAL_ERR_BAD_PARAM,
    OMPI_ERR_FATAL                  = OPAL_ERR_FATAL,
    OMPI_ERR_NOT_IMPLEMENTED        = OPAL_ERR_NOT_IMPLEMENTED,
    OMPI_ERR_NOT_SUPPORTED          = OPAL_ERR_NOT_SUPPORTED,
    OMPI_ERR_INTERUPTED             = OPAL_ERR_INTERRUPTED,
    OMPI_ERR_WOULD_BLOCK            = OPAL_ERR_WOULD_BLOCK,
    OMPI_ERR_IN_ERRNO               = OPAL_ERR_IN_ERRNO,
    OMPI_ERR_UNREACH                = OPAL_ERR_UNREACH,
    OMPI_ERR_NOT_FOUND              = OPAL_ERR_NOT_FOUND,
    OMPI_EXISTS                     = OPAL_EXISTS, /* indicates that the specified object already exists */
    OMPI_ERR_TIMEOUT                = OPAL_ERR_TIMEOUT,
    OMPI_ERR_NOT_AVAILABLE          = OPAL_ERR_NOT_AVAILABLE,
    OMPI_ERR_PERM                   = OPAL_ERR_PERM,
    OMPI_ERR_VALUE_OUT_OF_BOUNDS    = OPAL_ERR_VALUE_OUT_OF_BOUNDS,
    OMPI_ERR_FILE_READ_FAILURE      = OPAL_ERR_FILE_READ_FAILURE,
    OMPI_ERR_FILE_WRITE_FAILURE     = OPAL_ERR_FILE_WRITE_FAILURE,
    OMPI_ERR_FILE_OPEN_FAILURE      = OPAL_ERR_FILE_OPEN_FAILURE,
    OMPI_ERR_PACK_MISMATCH          = OPAL_ERR_PACK_MISMATCH,
    OMPI_ERR_PACK_FAILURE           = OPAL_ERR_PACK_FAILURE,
    OMPI_ERR_UNPACK_FAILURE         = OPAL_ERR_UNPACK_FAILURE,
    OMPI_ERR_TYPE_MISMATCH          = OPAL_ERR_TYPE_MISMATCH,
    OMPI_ERR_UNKNOWN_DATA_TYPE      = OPAL_ERR_UNKNOWN_DATA_TYPE,
    OMPI_ERR_DATA_TYPE_REDEF        = OPAL_ERR_DATA_TYPE_REDEF,
    OMPI_ERR_DATA_OVERWRITE_ATTEMPT = OPAL_ERR_DATA_OVERWRITE_ATTEMPT,

    OMPI_ERR_BUFFER                 = OPAL_ERR_BUFFER,
    OMPI_ERR_SILENT                 = OPAL_ERR_SILENT,
    OMPI_ERR_HANDLERS_COMPLETE      = OPAL_ERR_HANDLERS_COMPLETE,

    OMPI_ERR_REQUEST                = OMPI_ERR_BASE - 1,
    OMPI_ERR_RMA_SYNC               = OMPI_ERR_BASE - 2,
    OMPI_ERR_RMA_SHARED             = OMPI_ERR_BASE - 3,
    OMPI_ERR_RMA_ATTACH             = OMPI_ERR_BASE - 4,
    OMPI_ERR_RMA_RANGE              = OMPI_ERR_BASE - 5,
    OMPI_ERR_RMA_CONFLICT           = OMPI_ERR_BASE - 6,
    OMPI_ERR_WIN                    = OMPI_ERR_BASE - 7,
    OMPI_ERR_RMA_FLAVOR             = OMPI_ERR_BASE - 8,
};

#define OMPI_ERR_MAX                    (OMPI_ERR_BASE - 100)

#endif /* OMPI_CONSTANTS_H */

