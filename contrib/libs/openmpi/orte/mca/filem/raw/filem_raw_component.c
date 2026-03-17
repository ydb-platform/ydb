/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2012-2015 Los Alamos National Security, LLC. All rights
 *                         reserved
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "orte_config.h"
#include "opal/util/output.h"
#include "orte/constants.h"



#include "orte/mca/filem/filem.h"
#include "orte/mca/filem/base/base.h"
#include "filem_raw.h"

/*
 * Public string for version number
 */
const char *orte_filem_raw_component_version_string =
"ORTE FILEM raw MCA component version " ORTE_VERSION;

/*
 * Local functionality
 */
static int filem_raw_register(void);
static int filem_raw_open(void);
static int filem_raw_close(void);
static int filem_raw_query(mca_base_module_t **module, int *priority);

bool orte_filem_raw_flatten_trees=false;

orte_filem_base_component_t mca_filem_raw_component = {
    .base_version = {
        ORTE_FILEM_BASE_VERSION_2_0_0,
        /* Component name and version */
        .mca_component_name = "raw",
        MCA_BASE_MAKE_VERSION(component, ORTE_MAJOR_VERSION, ORTE_MINOR_VERSION,
                              ORTE_RELEASE_VERSION),

        /* Component open and close functions */
        .mca_open_component = filem_raw_open,
        .mca_close_component = filem_raw_close,
        .mca_query_component = filem_raw_query,
        .mca_register_component_params = filem_raw_register,
    },
    .base_data = {
        /* The component is checkpoint ready */
        MCA_BASE_METADATA_PARAM_CHECKPOINT
    },
};

static int filem_raw_register(void)
{
    mca_base_component_t *c = &mca_filem_raw_component.base_version;

    orte_filem_raw_flatten_trees = false;
    (void) mca_base_component_var_register(c, "flatten_directory_trees",
                                           "Put all files in the working directory instead of creating their respective directory trees",
                                           MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                           OPAL_INFO_LVL_9,
                                           MCA_BASE_VAR_SCOPE_READONLY,
                                           &orte_filem_raw_flatten_trees);

    return ORTE_SUCCESS;
}

static int filem_raw_open(void)
{
    return ORTE_SUCCESS;
}

static int filem_raw_close(void)
{
    return ORTE_SUCCESS;
}

static int filem_raw_query(mca_base_module_t **module, int *priority)
{
    *priority = 0;

    /* never for an APP */
    if (ORTE_PROC_IS_APP) {
        *module = NULL;
        return ORTE_ERROR;
    }

    /* otherwise, use if selected */
    *module = (mca_base_module_t*) &mca_filem_raw_module;
    return ORTE_SUCCESS;
}
