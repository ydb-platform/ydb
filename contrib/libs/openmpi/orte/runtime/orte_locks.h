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
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/**
 * @file
 *
 * Locks to prevent loops inside ORTE
 */
#ifndef ORTE_LOCKS_H
#define ORTE_LOCKS_H

#include "orte_config.h"

#include "opal/sys/atomic.h"

BEGIN_C_DECLS

/* for everyone */
ORTE_DECLSPEC extern opal_atomic_lock_t orte_finalize_lock;

/* for HNPs */
ORTE_DECLSPEC extern opal_atomic_lock_t orte_abort_inprogress_lock;
ORTE_DECLSPEC extern opal_atomic_lock_t orte_jobs_complete_lock;
ORTE_DECLSPEC extern opal_atomic_lock_t orte_quit_lock;

/**
 * Initialize the locks
 */
ORTE_DECLSPEC int orte_locks_init(void);

END_C_DECLS

#endif /* #ifndef ORTE_LOCKS_H */
