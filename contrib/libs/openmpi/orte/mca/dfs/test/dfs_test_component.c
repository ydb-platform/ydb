/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2012-2015 Los Alamos National Security, LLC. All rights
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

#include "orte/runtime/orte_globals.h"

#include "orte/mca/dfs/dfs.h"
#include "orte/mca/dfs/base/base.h"
#include "dfs_test.h"

/*
 * Public string for version number
 */
const char *orte_dfs_test_component_version_string =
    "ORTE DFS test MCA component version " ORTE_VERSION;

/*
 * Local functionality
 */
static int dfs_test_register(void);
static int dfs_test_open(void);
static int dfs_test_close(void);
static int dfs_test_component_query(mca_base_module_t **module, int *priority);

/*
 * Instantiate the public struct with all of our public information
 * and pointer to our public functions in it
 */
orte_dfs_base_component_t mca_dfs_test_component =
{
    /* Handle the general mca_component_t struct containing
     *  meta information about the component
     */
    .base_version = {
        ORTE_DFS_BASE_VERSION_1_0_0,
        /* Component name and version */
        .mca_component_name = "test",
        MCA_BASE_MAKE_VERSION(component, ORTE_MAJOR_VERSION, ORTE_MINOR_VERSION,
                              ORTE_RELEASE_VERSION),

        /* Component open and close functions */
        .mca_open_component = dfs_test_open,
        .mca_close_component = dfs_test_close,
        .mca_query_component = dfs_test_component_query,
        .mca_register_component_params = dfs_test_register,
    },
    .base_data = {
        /* The component is checkpoint ready */
        MCA_BASE_METADATA_PARAM_CHECKPOINT
    },
};

static bool select_me = false;

static int dfs_test_register(void)
{
    select_me = false;
    (void) mca_base_component_var_register(&mca_dfs_test_component.base_version, "select",
                                           "Apps select the test plug-in for the DFS framework",
                                           MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                           OPAL_INFO_LVL_9,
                                           MCA_BASE_VAR_SCOPE_ALL_EQ, &select_me);

    return ORTE_SUCCESS;
}

static int dfs_test_open(void)
{
    return ORTE_SUCCESS;
}

static int dfs_test_close(void)
{
    return ORTE_SUCCESS;
}

static int dfs_test_component_query(mca_base_module_t **module, int *priority)
{
    if (ORTE_PROC_IS_APP && select_me) {
        /* set our priority high so apps use us */
        *priority = 10000;
        *module = (mca_base_module_t *)&orte_dfs_test_module;
        return ORTE_SUCCESS;
    }

    *priority = -1;
    *module = NULL;
    return ORTE_ERROR;
}
