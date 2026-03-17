/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2006 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007-2008 Sun Microsystems, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/**
 * @file
 *
 * Interface into the Open MPI Run Time Environment
 */
#ifndef ORTE_RUNTIME_H
#define ORTE_RUNTIME_H

#include "orte_config.h"

#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif

#include "orte/util/proc_info.h"

BEGIN_C_DECLS

/** version string of ompi */
ORTE_DECLSPEC extern const char orte_version_string[];

/**
 * Whether ORTE is initialized or we are in orte_finalize
 */
ORTE_DECLSPEC extern int orte_initialized;
ORTE_DECLSPEC extern bool orte_finalizing;
ORTE_DECLSPEC extern int orte_debug_output;
ORTE_DECLSPEC extern bool orte_debug_flag;

    /**
     * Initialize the Open Run Time Environment
     *
     * Initlize the Open Run Time Environment, including process
     * control, malloc debugging and threads, and out of band messaging.
     * This function should be called exactly once.  This function should
     * be called by every application using the RTE interface, including
     * MPI applications and mpirun.
     *
     * @param pargc  Pointer to the number of arguments in the pargv array
     * @param pargv  The list of arguments.
     * @param flags  Whether we are ORTE tool or not
     */
ORTE_DECLSPEC    int orte_init(int*pargc, char*** pargv, orte_proc_type_t flags);

    /**
     * Initialize parameters for ORTE.
     *
     * @retval ORTE_SUCCESS Upon success.
     * @retval ORTE_ERROR Upon failure.
     */
ORTE_DECLSPEC    int orte_register_params(void);

    /**
     * Finalize the Open run time environment. Any function calling \code
     * orte_init should call \code orte_finalize.
     *
     */
ORTE_DECLSPEC int orte_finalize(void);

END_C_DECLS

#endif /* RUNTIME_H */
