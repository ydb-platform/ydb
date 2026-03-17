/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2016      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "patcher_overwrite.h"
#include "opal/mca/mca.h"
#include "opal/mca/base/base.h"

static int mca_patcher_overwrite_priority;

static int mca_patcher_overwrite_register (void)
{
    mca_patcher_overwrite_priority = 37;
    mca_base_component_var_register (&mca_patcher_overwrite_component.patcherc_version,
                                     "priority", "Priority of the overwrite binary patcher component",
                                     MCA_BASE_VAR_TYPE_INT, NULL, 0, 0, OPAL_INFO_LVL_5,
                                     MCA_BASE_VAR_SCOPE_CONSTANT, &mca_patcher_overwrite_priority);

    return OPAL_SUCCESS;
}

static int mca_patcher_overwrite_query (mca_base_module_t **module, int *priority)
{
    *module = &mca_patcher_overwrite_module.super;
    *priority = mca_patcher_overwrite_priority;
    return OPAL_SUCCESS;
}

mca_patcher_base_component_t mca_patcher_overwrite_component = {
    .patcherc_version = {
        OPAL_PATCHER_BASE_VERSION_1_0_0,
        .mca_component_name = "overwrite",
        MCA_BASE_MAKE_VERSION(component, OPAL_MAJOR_VERSION, OPAL_MINOR_VERSION,
                              OPAL_RELEASE_VERSION),
        .mca_query_component = mca_patcher_overwrite_query,
        .mca_register_component_params = mca_patcher_overwrite_register,
    },
};
