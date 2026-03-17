/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2007 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2008	   Sun Microsystems, Inc.  All rights reserved.
 * Copyright (c) 2010-2016 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2014      Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/** @file **/

#ifndef OPAL_H
#define OPAL_H

#include "opal_config.h"
#include "opal/types.h"

BEGIN_C_DECLS

/** version string of opal */
OPAL_DECLSPEC extern const char opal_version_string[];

/* Size of a cache line.  Initialized to a fixed value (see
   opal_init.c) until hwloc data is available, at which time it is
   filled with the smallest size of the lowest cache line (e.g., the
   smallest line size from all L2 caches found on the current system).
   If the hwloc data is available, opal_cache_line_size will be set to
   its final value by the end of orte_init(). */
OPAL_DECLSPEC extern int opal_cache_line_size;

/** Do we want to be warned on fork or not? */
OPAL_DECLSPEC extern bool opal_warn_on_fork;

/**
 * Initialize the OPAL layer, including the MCA system.
 *
 * @retval OPAL_SUCCESS Upon success.
 * @retval OPAL_ERROR Upon failure.
 *
 * \note If this function is called, opal_init_util() should *not* be
 * called.
 */
OPAL_DECLSPEC int opal_init(int* pargc, char*** pargv);

/**
 * Finalize the OPAL layer, including the MCA system.
 *
 * @retval OPAL_SUCCESS Upon success.
 * @retval OPAL_ERROR Upon failure.
 *
 * \note If this function is called, opal_finalize_util() should *not*
 * be called.
 */
OPAL_DECLSPEC int opal_finalize(void);

/**
 * Initialize the OPAL layer, excluding the MCA system.
 *
 * @retval OPAL_SUCCESS Upon success.
 * @retval OPAL_ERROR Upon failure.
 *
 * \note If this function is called, opal_init() should *not*
 * be called.
 */
OPAL_DECLSPEC int opal_init_util(int* pargc, char*** pargv);

/**
 * Disable PSM/PSM2 signal hijacking.
 *
 * See comment in the function for more detail.
 */
OPAL_DECLSPEC int opal_init_psm(void);

/**
 * Finalize the OPAL layer, excluding the MCA system.
 *
 * @retval OPAL_SUCCESS Upon success.
 * @retval OPAL_ERROR Upon failure.
 *
 * \note If this function is called, opal_finalize() should *not*
 * be called.
 */
OPAL_DECLSPEC int opal_finalize_util(void);

/**
 * Initialize a very thin OPAL layer solely for use
 * by unit tests. The purpose of this function is to
 * provide the absolute bare minimum support required
 * to open, select, and close a framework. This is
 * maintained separately from the other OPAL runtime
 * APIs to avoid conflicts when new frameworks are
 * added to the normal OPAL init sequence. It has no
 * other purpose and should not be used outside of
 * unit tests.
 *
 * @retval OPAL_SUCCESS Upon success.
 * @retval OPAL_ERROR Upon failure.
 */
OPAL_DECLSPEC int opal_init_test(void);

/**
 * Finalize the very thin OPAL layer used solely
 * by unit tests. The purpose of this function is to
 * finalize the absolute bare minimum support opened
 * by its companion opal_init_test API. It has no
 * other purpose and should not be used outside of
 * unit tests.
 *
 * @retval OPAL_SUCCESS Upon success.
 * @retval OPAL_ERROR Upon failure.
 */
OPAL_DECLSPEC void opal_finalize_test(void);

OPAL_DECLSPEC void opal_warn_fork(void);

/**
 * Internal function.  Do not call.
 */
OPAL_DECLSPEC int opal_register_params(void);
OPAL_DECLSPEC int opal_deregister_params(void);

END_C_DECLS

#endif
