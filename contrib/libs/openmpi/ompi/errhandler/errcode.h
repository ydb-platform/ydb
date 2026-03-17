/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
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
 * Copyright (c) 2006      University of Houston. All rights reserved.
 * Copyright (c) 2007-2015 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/** @file **/

#ifndef OMPI_MPI_ERRCODE_H
#define OMPI_MPI_ERRCODE_H

#include "ompi_config.h"

#include "mpi.h"
#include "opal/class/opal_object.h"
#include "opal/class/opal_pointer_array.h"

BEGIN_C_DECLS

/**
 * Back-end type for MPI error codes.
 * Please note:
 *   if code == MPI_UNDEFINED, than the according structure
 *                             represents an error class.
 *   For the predefined error codes and classes, code and
 *   cls are both set to the according value.
 */
struct ompi_mpi_errcode_t {
    opal_object_t                      super;
    int                                 code;
    int                                  cls;
    char     errstring[MPI_MAX_ERROR_STRING];
};
typedef struct ompi_mpi_errcode_t ompi_mpi_errcode_t;

OMPI_DECLSPEC extern opal_pointer_array_t ompi_mpi_errcodes;
OMPI_DECLSPEC extern int ompi_mpi_errcode_lastused;
OMPI_DECLSPEC extern int ompi_mpi_errcode_lastpredefined;

OMPI_DECLSPEC extern ompi_mpi_errcode_t ompi_err_unknown;

/**
 * Check for a valid error code
 */
static inline bool ompi_mpi_errcode_is_invalid(int errcode)
{
    if ( errcode >= 0 && errcode <= ompi_mpi_errcode_lastused )
        return 0;
    else
        return 1;
}

/**
 * Return the error class
 */
static inline int ompi_mpi_errcode_get_class (int errcode)
{
    ompi_mpi_errcode_t *err = NULL;

    if (errcode >= 0) {
        err = (ompi_mpi_errcode_t *)opal_pointer_array_get_item(&ompi_mpi_errcodes, errcode);
        /* If we get a bogus errcode, return MPI_ERR_UNKNOWN */
    }

    if (NULL != err) {
	if ( err->code != MPI_UNDEFINED ) {
	    return err->cls;
	}
    }
    return ompi_err_unknown.cls;
}

static inline int ompi_mpi_errcode_is_predefined ( int errcode )
{
    if ( errcode >= 0 && errcode <= ompi_mpi_errcode_lastpredefined )
	return true;

    return false;
}

static inline int ompi_mpi_errnum_is_class ( int errnum )
{
    ompi_mpi_errcode_t *err;

    if (errnum < 0) {
        return false;
    }

    if ( errnum <= ompi_mpi_errcode_lastpredefined ) {
	/* Predefined error values represent an error code and
	   an error class at the same time */
	return true;
    }

    err = (ompi_mpi_errcode_t *)opal_pointer_array_get_item(&ompi_mpi_errcodes, errnum);
    if (NULL != err) {
	if ( MPI_UNDEFINED == err->code) {
	    /* Distinction between error class and error code is that for the
	       first one the code section is set to MPI_UNDEFINED  */
	    return true;
	}
    }

    return false;
}


/**
 * Return the error string
 */
static inline char* ompi_mpi_errnum_get_string (int errnum)
{
    ompi_mpi_errcode_t *err = NULL;

    if (errnum >= 0) {
        err = (ompi_mpi_errcode_t *)opal_pointer_array_get_item(&ompi_mpi_errcodes, errnum);
        /* If we get a bogus errcode, return a string indicating that this
           truly should not happen */
    }

    if (NULL != err) {
        return err->errstring;
    } else {
        return "Unknown error (this should not happen!)";
    }
}


/**
 * Initialize the error codes
 *
 * @returns OMPI_SUCCESS Upon success
 * @returns OMPI_ERROR Otherwise
 *
 * Invoked from ompi_mpi_init(); sets up all static MPI error codes,
 */
int ompi_mpi_errcode_init(void);

/**
 * Finalize the error codes.
 *
 * @returns OMPI_SUCCESS Always
 *
 * Invokes from ompi_mpi_finalize(); tears down the error code array.
 */
int ompi_mpi_errcode_finalize(void);

/**
 * Add an error code
 *
 * @param: error class to which this new error code belongs to
 *
 * @returns the new error code on SUCCESS (>0)
 * @returns OMPI_ERROR otherwise
 *
 */
int ompi_mpi_errcode_add (int errclass);

/**
 * Add an error class
 *
 * @param: none
 *
 * @returns the new error class on SUCCESS (>0)
 * @returns OMPI_ERROR otherwise
 *
 */
int ompi_mpi_errclass_add (void);

/**
 * Add an error string to an error code
 *
 * @param: error code for which the string is defined
 * @param: error string to add
 * @param: length of the string
 *
 * @returns OMPI_SUCCESS on success
 * @returns OMPI_ERROR on error
 */
int ompi_mpi_errnum_add_string (int errnum, const char* string, int len);

END_C_DECLS

#endif /* OMPI_MPI_ERRCODE_H */
