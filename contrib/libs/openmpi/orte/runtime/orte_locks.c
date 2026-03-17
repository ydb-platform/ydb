/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
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
 *
 */

#include "orte_config.h"
#include "orte/constants.h"

#include "orte/runtime/orte_locks.h"

/* for everyone */
opal_atomic_lock_t orte_finalize_lock = {{0}};

/* for HNPs */
opal_atomic_lock_t orte_abort_inprogress_lock = {{0}};
opal_atomic_lock_t orte_jobs_complete_lock = {{0}};
opal_atomic_lock_t orte_quit_lock = {{0}};

int orte_locks_init(void)
{
    /* for everyone */
    opal_atomic_lock_init(&orte_finalize_lock, OPAL_ATOMIC_LOCK_UNLOCKED);

    /* for HNPs */
    opal_atomic_lock_init(&orte_abort_inprogress_lock, OPAL_ATOMIC_LOCK_UNLOCKED);
    opal_atomic_lock_init(&orte_jobs_complete_lock, OPAL_ATOMIC_LOCK_UNLOCKED);
    opal_atomic_lock_init(&orte_quit_lock, OPAL_ATOMIC_LOCK_UNLOCKED);

    return ORTE_SUCCESS;
}
