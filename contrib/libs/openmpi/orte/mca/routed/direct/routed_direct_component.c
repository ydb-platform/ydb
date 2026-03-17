/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2007-2015 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2004-2008 The Trustees of Indiana University.
 *                         All rights reserved.
 * Copyright (c) 2015-2016 Intel, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "orte_config.h"
#include "orte/constants.h"

#include "opal/mca/base/base.h"

#include "orte/runtime/orte_globals.h"
#include "orte/util/proc_info.h"

#include "orte/mca/routed/base/base.h"
#include "routed_direct.h"

static int orte_routed_direct_component_query(mca_base_module_t **module, int *priority);

/**
 * component definition
 */
orte_routed_component_t mca_routed_direct_component = {
      /* First, the mca_base_component_t struct containing meta
         information about the component itself */

    .base_version = {
        ORTE_ROUTED_BASE_VERSION_3_0_0,

        .mca_component_name = "direct",
        MCA_BASE_MAKE_VERSION(component, ORTE_MAJOR_VERSION, ORTE_MINOR_VERSION,
                              ORTE_RELEASE_VERSION),
        .mca_query_component = orte_routed_direct_component_query
    },
    .base_data = {
        /* This component can be checkpointed */
        MCA_BASE_METADATA_PARAM_CHECKPOINT
    },
};

static int orte_routed_direct_component_query(mca_base_module_t **module, int *priority)
{
    /* if we are an app and no daemon URI has been provided, or
     * we are a singleton, then we must be chosen */
    if (ORTE_PROC_IS_APP && NULL == orte_process_info.my_daemon_uri) {
        /* we are direct launched, so set some arbitrary value
         * for the daemon name */
        ORTE_PROC_MY_DAEMON->jobid = 0;
        ORTE_PROC_MY_DAEMON->vpid = 0;
        *priority = 100;
    } else if (ORTE_PROC_IS_SINGLETON) {
        *priority = 100;
    } else {
        /* allow selection only when specifically requested */
        *priority = 0;
    }
    *module = (mca_base_module_t *) &orte_routed_direct_module;
    return ORTE_SUCCESS;
}
