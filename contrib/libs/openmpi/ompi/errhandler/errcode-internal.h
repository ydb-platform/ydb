/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2007 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2010      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2010      Oracle and/or its affiliates.  All rights reserved.
 * Copyright (c) 2012      Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/** @file **/

#ifndef OMPI_ERRCODE_INTERN_H
#define OMPI_ERRCODE_INTERN_H

#include "ompi_config.h"

#include "mpi.h"
#include "ompi/constants.h"
#include "opal/class/opal_object.h"
#include "opal/class/opal_pointer_array.h"

#define OMPI_MAX_ERROR_STRING 64

BEGIN_C_DECLS

/**
 * Back-end type for MPI error codes
 */
struct ompi_errcode_intern_t {
    opal_object_t                       super;
    int                                  code;
    int                              mpi_code;
    int                                 index;
    char      errstring[OMPI_MAX_ERROR_STRING];
};
typedef struct ompi_errcode_intern_t ompi_errcode_intern_t;

OMPI_DECLSPEC extern opal_pointer_array_t ompi_errcodes_intern;
OMPI_DECLSPEC extern int ompi_errcode_intern_lastused;

/**
 * Return the MPI errcode for a given internal error code. */
static inline int ompi_errcode_get_mpi_code(int errcode)
{
    int ret = MPI_ERR_UNKNOWN;
    int i;
    ompi_errcode_intern_t *errc;

    /* If the errcode is >= 0, then it's already an MPI error code, so
       just return it. */
    if (errcode >= 0) {
        return errcode;
    }

    /* Otherwise, it's an internal OMPI code and we need to translate
       it */
    for (i = 0; i < ompi_errcode_intern_lastused; i++) {
        errc = (ompi_errcode_intern_t *)opal_pointer_array_get_item(&ompi_errcodes_intern, i);
        if (errc->code == errcode) {
            ret = errc->mpi_code;
            break;
        }
    }
    return ret;
}

/**
 * Initialize the error codes
 *
 * @returns OMPI_SUCCESS Upon success
 * @returns OMPI_ERROR Otherwise
 *
 * Invoked from ompi_mpi_init(); sets up all static MPI error codes,
 */
int ompi_errcode_intern_init(void);

/**
 * Finalize the error codes.
 *
 * @returns OMPI_SUCCESS Always
 *
 * Invokes from ompi_mpi_finalize(); tears down the error code array.
 */
int ompi_errcode_intern_finalize(void);

END_C_DECLS

#endif /* OMPI_ERRCODE_INTERNAL_H */
