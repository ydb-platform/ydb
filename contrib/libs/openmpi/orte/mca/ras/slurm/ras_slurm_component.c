/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2008 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2012-2015 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2015      Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "orte_config.h"
#include "orte/constants.h"

#include "opal/mca/base/base.h"
#include "opal/util/net.h"
#include "opal/opal_socket_errno.h"

#include "orte/util/name_fns.h"
#include "orte/mca/errmgr/errmgr.h"
#include "orte/runtime/orte_globals.h"

#include "orte/mca/ras/base/ras_private.h"
#include "ras_slurm.h"


/*
 * Local functions
 */
static int ras_slurm_register(void);
static int ras_slurm_open(void);
static int ras_slurm_close(void);
static int orte_ras_slurm_component_query(mca_base_module_t **module, int *priority);


orte_ras_slurm_component_t mca_ras_slurm_component = {
    {
        /* First, the mca_base_component_t struct containing meta
           information about the component itself */

        .base_version = {
            ORTE_RAS_BASE_VERSION_2_0_0,

            /* Component name and version */
            .mca_component_name = "slurm",
            MCA_BASE_MAKE_VERSION(component, ORTE_MAJOR_VERSION, ORTE_MINOR_VERSION,
                                  ORTE_RELEASE_VERSION),

            /* Component open and close functions */
            .mca_open_component = ras_slurm_open,
            .mca_close_component = ras_slurm_close,
            .mca_query_component = orte_ras_slurm_component_query,
            .mca_register_component_params = ras_slurm_register
        },
        .base_data = {
            /* The component is checkpoint ready */
            MCA_BASE_METADATA_PARAM_CHECKPOINT
        },
    }
};


static int ras_slurm_register(void)
{
    mca_base_component_t *component = &mca_ras_slurm_component.super.base_version;

    mca_ras_slurm_component.timeout = 30;
    (void) mca_base_component_var_register (component, "dyn_allocate_timeout",
                                            "Number of seconds to wait for Slurm dynamic allocation",
                                            MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                            OPAL_INFO_LVL_9,
                                            MCA_BASE_VAR_SCOPE_READONLY,
                                            &mca_ras_slurm_component.timeout);

    mca_ras_slurm_component.dyn_alloc_enabled = false;
    (void) mca_base_component_var_register (component, "enable_dyn_alloc",
                                            "Whether or not dynamic allocations are enabled",
                                            MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                            OPAL_INFO_LVL_9,
                                            MCA_BASE_VAR_SCOPE_READONLY,
                                            &mca_ras_slurm_component.dyn_alloc_enabled);

    mca_ras_slurm_component.config_file = NULL;
    (void) mca_base_component_var_register (component, "config_file",
                                            "Path to Slurm configuration file",
                                            MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0,
                                            OPAL_INFO_LVL_9,
                                            MCA_BASE_VAR_SCOPE_READONLY,
                                            &mca_ras_slurm_component.config_file);

    mca_ras_slurm_component.rolling_alloc = false;
    (void) mca_base_component_var_register (component, "enable_rolling_alloc",
                                            "Enable partial dynamic allocations",
                                            MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                            OPAL_INFO_LVL_9,
                                            MCA_BASE_VAR_SCOPE_READONLY,
                                            &mca_ras_slurm_component.rolling_alloc);

    mca_ras_slurm_component.use_all = false;
    (void) mca_base_component_var_register (component, "use_entire_allocation",
                                            "Use entire allocation (not just job step nodes) for this application",
                                            MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                            OPAL_INFO_LVL_5,
                                            MCA_BASE_VAR_SCOPE_READONLY,
                                            &mca_ras_slurm_component.use_all);

    return ORTE_SUCCESS;
}

static int ras_slurm_open(void)
{
    return ORTE_SUCCESS;
}

static int ras_slurm_close(void)
{
    return ORTE_SUCCESS;
}

static int orte_ras_slurm_component_query(mca_base_module_t **module, int *priority)
{
    /* if I built, then slurm support is available. If
     * I am not in a Slurm allocation, and dynamic alloc
     * is not enabled, then disqualify myself
     */
    if (NULL == getenv("SLURM_JOBID") &&
        !mca_ras_slurm_component.dyn_alloc_enabled) {
        /* disqualify ourselves */
        *priority = 0;
        *module = NULL;
        return ORTE_ERROR;
    }

    OPAL_OUTPUT_VERBOSE((2, orte_ras_base_framework.framework_output,
                         "%s ras:slurm: available for selection",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));
    /* since only one RM can exist on a cluster, just set
     * my priority to something - the other components won't
     * be responding anyway
     */
    *priority = 50;
    *module = (mca_base_module_t *) &orte_ras_slurm_module;
    return ORTE_SUCCESS;
}
