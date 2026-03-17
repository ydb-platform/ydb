/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2011-2015 Los Alamos National Security, LLC. All rights
 *                         reserved.
 *
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "orte_config.h"
#include "opal/util/output.h"

#include "orte/mca/state/state.h"
#include "orte/mca/state/base/base.h"
#include "state_app.h"

/*
 * Public string for version number
 */
const char *orte_state_app_component_version_string =
    "ORTE STATE app MCA component version " ORTE_VERSION;

/*
 * Local functionality
 */
static int state_app_open(void);
static int state_app_close(void);
static int state_app_component_query(mca_base_module_t **module, int *priority);

/*
 * Instantiate the public struct with all of our public information
 * and pointer to our public functions in it
 */
orte_state_base_component_t mca_state_app_component =
{
    /* Handle the general mca_component_t struct containing
     *  meta information about the component
     */
    .base_version = {
        ORTE_STATE_BASE_VERSION_1_0_0,
        /* Component name and version */
        .mca_component_name = "app",
        MCA_BASE_MAKE_VERSION(component, ORTE_MAJOR_VERSION, ORTE_MINOR_VERSION,
                              ORTE_RELEASE_VERSION),

        /* Component open and close functions */
        .mca_open_component = state_app_open,
        .mca_close_component = state_app_close,
        .mca_query_component = state_app_component_query,
    },
    .base_data = {
        /* The component is checkpoint ready */
        MCA_BASE_METADATA_PARAM_CHECKPOINT
    },
};

static int my_priority=1000;

static int state_app_open(void)
{
    return ORTE_SUCCESS;
}

static int state_app_close(void)
{
    return ORTE_SUCCESS;
}

static int state_app_component_query(mca_base_module_t **module, int *priority)
{
    if (ORTE_PROC_IS_APP) {
        /* set our priority high as we are the default for apps */
        *priority = my_priority;
        *module = (mca_base_module_t *)&orte_state_app_module;
        return ORTE_SUCCESS;
    }

    *priority = -1;
    *module = NULL;
    return ORTE_ERROR;
}
