/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2009 The Trustees of Indiana University.
 *                         All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"

#include "opal/constants.h"
#include "opal/mca/crs/crs.h"
#include "opal/mca/crs/base/base.h"
#include "crs_none.h"

/*
 * Public string for version number
 */
const char *opal_crs_none_component_version_string =
"OPAL CRS none MCA component version " OPAL_VERSION;

/*
 * Local functionality
 */
static int crs_none_register (void);
static int crs_none_open(void);
static int crs_none_close(void);

/*
 * Instantiate the public struct with all of our public information
 * and pointer to our public functions in it
 */
opal_crs_none_component_t mca_crs_none_component = {
    /* First do the base component stuff */
    {
        /* Handle the general mca_component_t struct containing
         *  meta information about the component itnone
         */
        .base_version = {
            OPAL_CRS_BASE_VERSION_2_0_0,

            /* Component name and version */
            .mca_component_name = "none",
            MCA_BASE_MAKE_VERSION(component, OPAL_MAJOR_VERSION, OPAL_MINOR_VERSION,
                                  OPAL_RELEASE_VERSION),

            /* Component open and close functions */
            .mca_open_component = crs_none_open,
            .mca_close_component = crs_none_close,
            .mca_query_component = opal_crs_none_component_query,
            .mca_register_component_params = crs_none_register,
        },
        .base_data = {
            /* The component is checkpoint ready */
            MCA_BASE_METADATA_PARAM_CHECKPOINT
        },

        .verbose = 0,
        .output_handle = -1,
        .priority = 1,
    }
};

/*
 * None module
 */
static opal_crs_base_module_t loc_module = {
    /** Initialization Function */
    opal_crs_none_module_init,
    /** Finalization Function */
    opal_crs_none_module_finalize,

    /** Checkpoint interface */
    opal_crs_none_checkpoint,

    /** Restart Command Access */
    opal_crs_none_restart,

    /** Disable checkpoints */
    opal_crs_none_disable_checkpoint,
    /** Enable checkpoints */
    opal_crs_none_enable_checkpoint,

    /** Prelaunch */
    opal_crs_none_prelaunch,

    /** Register Thread */
    opal_crs_none_reg_thread
};

bool opal_crs_none_select_warning = false;

static int crs_none_register (void)
{
    int ret;

    (void) mca_base_component_var_register (&mca_crs_none_component.super.base_version,
                                            "priority", "Priority of the crs none "
                                            "component", MCA_BASE_VAR_TYPE_INT, NULL,
                                            0, MCA_BASE_VAR_FLAG_DEFAULT_ONLY,
                                            OPAL_INFO_LVL_3,
                                            MCA_BASE_VAR_SCOPE_CONSTANT,
                                            &mca_crs_none_component.super.priority);

    opal_crs_none_select_warning = false;
    ret = mca_base_component_var_register (&mca_crs_none_component.super.base_version,
                                           "select_warning",
                                           "Enable warning when the 'none' component is selected when checkpoint/restart functionality is requested."
                                           "[Default = disabled/no-warning]",
                                           MCA_BASE_VAR_TYPE_BOOL, NULL, 0, MCA_BASE_VAR_FLAG_SETTABLE,
                                           OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_ALL,
                                           &opal_crs_none_select_warning);
    return (0 > ret) ? ret : OPAL_SUCCESS;
}

static int crs_none_open(void)
{
    return OPAL_SUCCESS;
}

static int crs_none_close(void)
{
    return OPAL_SUCCESS;
}

int opal_crs_none_component_query(mca_base_module_t **module, int *priority)
{
    *module   = (mca_base_module_t *)&loc_module;
    *priority = mca_crs_none_component.super.priority;

    return OPAL_SUCCESS;
}

