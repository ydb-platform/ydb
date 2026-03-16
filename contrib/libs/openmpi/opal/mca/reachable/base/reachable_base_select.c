/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2014      Intel, Inc. All rights reserved.
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
#include "opal/mca/mca.h"
#include "opal/mca/base/base.h"
#include "opal/mca/reachable/reachable.h"
#include "opal/mca/reachable/base/base.h"

/*
 * Globals
 */

int opal_reachable_base_select(void)
{
    int ret;
    opal_reachable_base_component_t *best_component = NULL;
    opal_reachable_base_module_t *best_module = NULL;

    /*
     * Select the best component
     */
    if( OPAL_SUCCESS != mca_base_select("reachable", opal_reachable_base_framework.framework_output,
                                        &opal_reachable_base_framework.framework_components,
                                        (mca_base_module_t **) &best_module,
                                        (mca_base_component_t **) &best_component, NULL) ) {
        /* notify caller that no available component found */
        return OPAL_ERR_NOT_FOUND;
    }

    /* Save the winner */
    opal_reachable = *best_module;

    /* Initialize the winner */
    ret = opal_reachable.init();

    return ret;
}
