/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2016-2018 Intel, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "orte_config.h"
#include "orte/types.h"
#include "opal/types.h"

#include "opal/util/show_help.h"

#include "orte/mca/regx/regx.h"
#include "regx_fwd.h"

static int component_query(mca_base_module_t **module, int *priority);

/*
 * Struct of function pointers and all that to let us be initialized
 */
orte_regx_base_component_t mca_regx_fwd_component = {
    .base_version = {
        MCA_REGX_BASE_VERSION_1_0_0,
        .mca_component_name = "fwd",
        MCA_BASE_MAKE_VERSION(component, ORTE_MAJOR_VERSION, ORTE_MINOR_VERSION,
                              ORTE_RELEASE_VERSION),
        .mca_query_component = component_query,
    },
    .base_data = {
        /* The component is checkpoint ready */
        MCA_BASE_METADATA_PARAM_CHECKPOINT
    },
};

static int component_query(mca_base_module_t **module, int *priority)
{
    *module = (mca_base_module_t*)&orte_regx_fwd_module;
    *priority = 10;
    return ORTE_SUCCESS;
}
