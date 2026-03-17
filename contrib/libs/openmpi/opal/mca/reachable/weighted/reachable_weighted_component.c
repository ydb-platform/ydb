/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2014      Intel, Inc.  All rights reserved.
 * Copyright (c) 2014      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2017      Amazon.com, Inc. or its affiliates.
 *                         All Rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 * These symbols are in a file by themselves to provide nice linker
 * semantics.  Since linkers generally pull in symbols by object
 * files, keeping these symbols as the only symbols in this file
 * prevents utility programs such as "ompi_info" from having to import
 * entire components just to query their version and parameters.
 */

#include "opal_config.h"

#include "opal/constants.h"
#include "opal/util/proc.h"
#include "opal/mca/reachable/reachable.h"
#include "reachable_weighted.h"

/*
 * Public string showing the reachable weighted component version number
 */
const char *opal_reachable_weighted_component_version_string =
    "OPAL weighted reachable MCA component version " OPAL_VERSION;

/*
 * Local function
 */
static int reachable_weighted_open(void);
static int reachable_weighted_close(void);
static int reachable_weighted_component_query(mca_base_module_t **module, int *priority);
static int component_register(void);


/*
 * Instantiate the public struct with all of our public information
 * and pointers to our public functions in it
 */

opal_reachable_weighted_component_t mca_reachable_weighted_component = {
    {

        /* First, the mca_component_t struct containing meta information
           about the component itself */

        .base_version = {
            /* Indicate that we are a reachable v1.1.0 component (which also
               implies a specific MCA version) */

            OPAL_REACHABLE_BASE_VERSION_2_0_0,

            /* Component name and version */

            .mca_component_name = "weighted",
            MCA_BASE_MAKE_VERSION(component, OPAL_MAJOR_VERSION, OPAL_MINOR_VERSION,
                                  OPAL_RELEASE_VERSION),

            /* Component open and close functions */

            .mca_open_component = reachable_weighted_open,
            .mca_close_component = reachable_weighted_close,
            .mca_query_component = reachable_weighted_component_query,
            .mca_register_component_params = component_register,
        },
        /* Next the MCA v1.0.0 component meta data */
        .base_data = {
            /* The component is checkpoint ready */
            MCA_BASE_METADATA_PARAM_CHECKPOINT
        },
    }
};

static int reachable_weighted_open(void)
{
    /* construct the component fields */

    return OPAL_SUCCESS;
}

static int reachable_weighted_close(void)
{
    return OPAL_SUCCESS;
}

static int component_register(void)
{
    return OPAL_SUCCESS;
}

static int reachable_weighted_component_query(mca_base_module_t **module, int *priority)
{
    *priority = 1;
    *module = (mca_base_module_t *)&opal_reachable_weighted_module;
    return OPAL_SUCCESS;
}
