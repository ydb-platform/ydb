/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2011      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "orte_config.h"
#include "orte/constants.h"

#include "opal/mca/base/base.h"

#include "orte/util/show_help.h"

#include "orte/mca/rmaps/base/base.h"
#include "rmaps_ppr.h"

/*
 * Local functions
 */

static int orte_rmaps_ppr_open(void);
static int orte_rmaps_ppr_close(void);
static int orte_rmaps_ppr_query(mca_base_module_t **module, int *priority);
static int orte_rmaps_ppr_register(void);

orte_rmaps_base_component_t mca_rmaps_ppr_component = {
    .base_version = {
        ORTE_RMAPS_BASE_VERSION_2_0_0,

        .mca_component_name = "ppr",
        MCA_BASE_MAKE_VERSION(component, ORTE_MAJOR_VERSION, ORTE_MINOR_VERSION,
                              ORTE_RELEASE_VERSION),
        .mca_open_component = orte_rmaps_ppr_open,
        .mca_close_component = orte_rmaps_ppr_close,
        .mca_query_component = orte_rmaps_ppr_query,
        .mca_register_component_params = orte_rmaps_ppr_register,
    },
    .base_data = {
        /* The component is checkpoint ready */
        MCA_BASE_METADATA_PARAM_CHECKPOINT
    },
};

static int my_priority;

static int orte_rmaps_ppr_open(void)
{
    return ORTE_SUCCESS;
}


static int orte_rmaps_ppr_query(mca_base_module_t **module, int *priority)
{
    *priority = my_priority;
    *module = (mca_base_module_t *)&orte_rmaps_ppr_module;
    return ORTE_SUCCESS;
}

/**
 *  Close all subsystems.
 */

static int orte_rmaps_ppr_close(void)
{
    return ORTE_SUCCESS;
}


static int orte_rmaps_ppr_register(void)
{
    my_priority = 90;
    (void) mca_base_component_var_register(&mca_rmaps_ppr_component.base_version,
                                           "priority", "Priority of the ppr rmaps component",
                                           MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                           OPAL_INFO_LVL_9,
                                           MCA_BASE_VAR_SCOPE_READONLY, &my_priority);

    return ORTE_SUCCESS;
}
