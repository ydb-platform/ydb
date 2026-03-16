/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2011      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2011-2016 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2014      Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "orte_config.h"
#include "orte/constants.h"

#include "orte/mca/mca.h"
#include "opal/runtime/opal_params.h"

#include "orte/util/proc_info.h"

#include "grpcomm_direct.h"

static int my_priority=5;  /* must be below "bad" module */
static int direct_open(void);
static int direct_close(void);
static int direct_query(mca_base_module_t **module, int *priority);
static int direct_register(void);

/*
 * Struct of function pointers that need to be initialized
 */
orte_grpcomm_base_component_t mca_grpcomm_direct_component = {
    .base_version = {
        ORTE_GRPCOMM_BASE_VERSION_3_0_0,

        .mca_component_name = "direct",
        MCA_BASE_MAKE_VERSION(component, ORTE_MAJOR_VERSION, ORTE_MINOR_VERSION,
                              ORTE_RELEASE_VERSION),
        .mca_open_component = direct_open,
        .mca_close_component = direct_close,
        .mca_query_component = direct_query,
        .mca_register_component_params = direct_register,
    },
    .base_data = {
        /* The component is checkpoint ready */
        MCA_BASE_METADATA_PARAM_CHECKPOINT
    },
};

static int direct_register(void)
{
    mca_base_component_t *c = &mca_grpcomm_direct_component.base_version;

    /* make the priority adjustable so users can select
     * direct for use by apps without affecting daemons
     */
    my_priority = 85;
    (void) mca_base_component_var_register(c, "priority",
                                           "Priority of the grpcomm direct component",
                                           MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                           OPAL_INFO_LVL_9,
                                           MCA_BASE_VAR_SCOPE_READONLY,
                                           &my_priority);
    return ORTE_SUCCESS;
}

/* Open the component */
static int direct_open(void)
{
    return ORTE_SUCCESS;
}

static int direct_close(void)
{
    return ORTE_SUCCESS;
}

static int direct_query(mca_base_module_t **module, int *priority)
{
    /* we are always available */
    *priority = my_priority;
    *module = (mca_base_module_t *)&orte_grpcomm_direct_module;
    return ORTE_SUCCESS;
}
