/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2008 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2011 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2010 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2011-2015 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2012 Cisco Systems, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/** @file:
 *
 * The OpenRTE Environment-Specific Services
 *
 */

#ifndef ORTE_ESS_H
#define ORTE_ESS_H

#include "orte_config.h"
#include "orte/types.h"

#include "orte/mca/mca.h"
#include "opal/mca/hwloc/base/base.h"

#include "orte/util/proc_info.h"
#include "orte/runtime/runtime.h"

BEGIN_C_DECLS

/*
 * API functions
 */

/*
 * Initialize the RTE for this environment
 */
typedef int (*orte_ess_base_module_init_fn_t)(void);

/*
 * Finalize the RTE for this environment
 */
typedef int (*orte_ess_base_module_finalize_fn_t)(void);

/**
 * Abort the current application
 *
 * Aborts currently running application, NOTE: We do NOT call the
 * regular C-library "abort" function, even
 * though that would have alerted us to the fact that this is
 * an abnormal termination, because it would automatically cause
 * a core file to be generated. The "report" flag indicates if the
 * function should create an appropriate file to alert the local
 * orted that termination was abnormal.
 */
typedef void (*orte_ess_base_module_abort_fn_t)(int status, bool report);

/**
 * Handle fault tolerance updates
 *
 * @param[in] state Fault tolerance state update
 *
 * @retval ORTE_SUCCESS The operation completed successfully
 * @retval ORTE_ERROR   An unspecifed error occurred
 */
typedef int  (*orte_ess_base_module_ft_event_fn_t)(int state);

/*
 * the standard module data structure
 */
struct orte_ess_base_module_3_0_0_t {
    orte_ess_base_module_init_fn_t                  init;
    orte_ess_base_module_finalize_fn_t              finalize;
    orte_ess_base_module_abort_fn_t                 abort;
    orte_ess_base_module_ft_event_fn_t              ft_event;
};
typedef struct orte_ess_base_module_3_0_0_t orte_ess_base_module_3_0_0_t;
typedef struct orte_ess_base_module_3_0_0_t orte_ess_base_module_t;

/*
 * the standard component data structure
 */
struct orte_ess_base_component_2_0_0_t {
    mca_base_component_t base_version;
    mca_base_component_data_t base_data;
};
typedef struct orte_ess_base_component_2_0_0_t orte_ess_base_component_2_0_0_t;
typedef struct orte_ess_base_component_2_0_0_t orte_ess_base_component_t;

/*
 * Macro for use in components that are of type ess
 */
#define ORTE_ESS_BASE_VERSION_3_0_0 \
    ORTE_MCA_BASE_VERSION_2_1_0("ess", 3, 0, 0)

/* Global structure for accessing ESS functions */
ORTE_DECLSPEC extern orte_ess_base_module_t orte_ess;  /* holds selected module's function pointers */

END_C_DECLS

#endif
