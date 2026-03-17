/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2010 The Trustees of Indiana University.
 *                         All rights reserved.
 * Copyright (c) 2004-2005 The Trustees of the University of Tennessee.
 *                         All rights reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2013-2015 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "orte_config.h"

#include <string.h>

#include "orte/constants.h"

#include "orte/mca/mca.h"
#include "opal/util/output.h"
#include "opal/mca/base/base.h"

#include "orte/mca/sstore/sstore.h"
#include "orte/mca/sstore/base/base.h"

#include "orte/mca/snapc/snapc.h"
#include "orte/mca/snapc/base/base.h"


static orte_snapc_base_module_t none_module = {
    /** Initialization Function */
    orte_snapc_base_module_init,
    /** Finalization Function */
    orte_snapc_base_module_finalize,
    orte_snapc_base_none_setup_job,
    orte_snapc_base_none_release_job,
    orte_snapc_base_none_ft_event,
    orte_snapc_base_none_start_ckpt,
    orte_snapc_base_none_end_ckpt
};

int orte_snapc_base_select(bool seed, bool app)
{
    int exit_status = OPAL_SUCCESS;
    orte_snapc_base_component_t *best_component = NULL;
    orte_snapc_base_module_t *best_module = NULL;
    const char **include_list = NULL;
    int var_id;

    /*
     * Register the framework MCA param and look up include list
     */
    /* XXX -- TODO -- framework_subsytem -- this shouldn't be necessary once the framework system is in place */
    var_id = mca_base_var_find(NULL, "snapc", NULL, NULL);
    mca_base_var_get_value(var_id, &include_list, NULL, NULL);

    if(NULL != include_list && NULL != include_list[0] &&
       0 == strncmp(include_list[0], "none", strlen("none")) ){
        opal_output_verbose(10, orte_snapc_base_framework.framework_output,
                            "snapc:select: Using %s component",
                            include_list[0]);
        best_module    = &none_module;
        /* Close all components since none will be used */
        mca_base_components_close(0, /* Pass 0 to keep this from closing the output handle */
                                  &orte_snapc_base_framework.framework_components,
                                  NULL);
        /* JJH: Todo: Check if none is in the list */
        goto skip_select;
    }

    /*
     * Select the best component
     */
    if( OPAL_SUCCESS != mca_base_select("snapc", orte_snapc_base_framework.framework_output,
                                        &orte_snapc_base_framework.framework_components,
                                        (mca_base_module_t **) &best_module,
                                        (mca_base_component_t **) &best_component, NULL) ) {
        /* This will only happen if no component was selected */
        exit_status = ORTE_ERROR;
        goto cleanup;
    }

 skip_select:
    /* Save the winner */
    orte_snapc = *best_module;

    /* Initialize the winner */
    if (NULL != best_module) {
        if (OPAL_SUCCESS != orte_snapc.snapc_init(seed, app)) {
            exit_status = OPAL_ERROR;
            goto cleanup;
        }
    }

 cleanup:

    return exit_status;
}
