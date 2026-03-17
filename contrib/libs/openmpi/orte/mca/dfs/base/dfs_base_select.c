/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2012-2015 Los Alamos National Security, Inc.  All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */


#include "orte_config.h"
#include "orte/constants.h"

#include <string.h>

#include "orte/mca/mca.h"
#include "opal/mca/base/base.h"
#include "opal/util/output.h"

#include "orte/mca/dfs/base/base.h"

int orte_dfs_base_select(void)
{
    int exit_status = ORTE_SUCCESS;
    orte_dfs_base_component_t *best_component = NULL;
    orte_dfs_base_module_t *best_module = NULL;

    /*
     * Select the best component
     */
    if (OPAL_SUCCESS != mca_base_select("dfs", orte_dfs_base_framework.framework_output,
                                        &orte_dfs_base_framework.framework_components,
                                        (mca_base_module_t **) &best_module,
                                        (mca_base_component_t **) &best_component, NULL)) {
        /* This will only happen if no component was selected, which
         * is okay - we don't have to select anything
         */
        return ORTE_SUCCESS;
    }

    /* Save the winner */
    orte_dfs = *best_module;

    /* Initialize the winner */
    if (NULL != best_module && NULL != orte_dfs.init) {
        if (ORTE_SUCCESS != orte_dfs.init()) {
            exit_status = ORTE_ERROR;
            goto cleanup;
        }
    }

 cleanup:
    return exit_status;
}
