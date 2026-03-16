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
 * Copyright (c) 2011-2013 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2016      Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/** @file:
 */

#ifndef ORTE_MCA_RAS_BASE_H
#define ORTE_MCA_RAS_BASE_H

/*
 * includes
 */
#include "orte_config.h"

#include "orte/mca/ras/ras.h"
/*
 * Global functions for MCA overall collective open and close
 */

BEGIN_C_DECLS

/*
 * MCA Framework
 */
ORTE_DECLSPEC extern mca_base_framework_t orte_ras_base_framework;
/* select a component */
ORTE_DECLSPEC    int orte_ras_base_select(void);

/*
 * globals that might be needed
 */
typedef struct orte_ras_base_t {
    bool allocation_read;
    orte_ras_base_module_t *active_module;
    int total_slots_alloc;
    int multiplier;
    bool launch_orted_on_hn;
} orte_ras_base_t;

ORTE_DECLSPEC extern orte_ras_base_t orte_ras_base;

ORTE_DECLSPEC void orte_ras_base_display_alloc(void);

ORTE_DECLSPEC void orte_ras_base_allocate(int fd, short args, void *cbdata);

ORTE_DECLSPEC int orte_ras_base_add_hosts(orte_job_t *jdata);

END_C_DECLS

#endif
