/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2007-2015 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2004-2008 The Trustees of Indiana University.
 *                         All rights reserved.
 * Copyright (c) 2013      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2013-2017 Intel, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "orte_config.h"
#include "orte/constants.h"

#include "opal/mca/base/base.h"

#include "orte/mca/routed/base/base.h"
#include "routed_radix.h"

static int orte_routed_radix_component_register(void);
static int orte_routed_radix_component_query(mca_base_module_t **module, int *priority);

/**
 * component definition
 */
orte_routed_radix_component_t mca_routed_radix_component = {
    {
        /* First, the mca_base_component_t struct containing meta
        information about the component itself */

        .base_version = {
            ORTE_ROUTED_BASE_VERSION_3_0_0,

            .mca_component_name = "radix",
            MCA_BASE_MAKE_VERSION(component, ORTE_MAJOR_VERSION, ORTE_MINOR_VERSION,
                                  ORTE_RELEASE_VERSION),
            .mca_query_component = orte_routed_radix_component_query,
            .mca_register_component_params = orte_routed_radix_component_register,
        },
        .base_data = {
            /* This component can be checkpointed */
            MCA_BASE_METADATA_PARAM_CHECKPOINT
        },
    }
};

static int orte_routed_radix_component_register(void)
{
    mca_base_component_t *c = &mca_routed_radix_component.super.base_version;

    mca_routed_radix_component.radix = 64;
    (void) mca_base_component_var_register(c, NULL,
                                           "Radix to be used for routed radix tree",
                                           MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                           OPAL_INFO_LVL_9,
                                           MCA_BASE_VAR_SCOPE_READONLY,
                                           &mca_routed_radix_component.radix);

    return ORTE_SUCCESS;
}

static int orte_routed_radix_component_query(mca_base_module_t **module, int *priority)
{
    if (0 > mca_routed_radix_component.radix) {
        return ORTE_ERR_BAD_PARAM;
    }

    *priority = 70;
    *module = (mca_base_module_t *) &orte_routed_radix_module;
    return ORTE_SUCCESS;
}
