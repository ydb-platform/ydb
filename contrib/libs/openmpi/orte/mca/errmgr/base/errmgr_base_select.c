/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2010 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2010      Cisco Systems, Inc.  All rights reserved.
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

#include "orte/mca/errmgr/base/base.h"
#include "orte/mca/errmgr/base/errmgr_private.h"

int orte_errmgr_base_select(void)
{
    int exit_status = OPAL_SUCCESS;
    orte_errmgr_base_component_t *best_component = NULL;
    orte_errmgr_base_module_t *best_module = NULL;

    /*
     * Select the best component
     */
    if( OPAL_SUCCESS != mca_base_select("errmgr", orte_errmgr_base_framework.framework_output,
                                        &orte_errmgr_base_framework.framework_components,
                                        (mca_base_module_t **) &best_module,
                                        (mca_base_component_t **) &best_component, NULL) ) {
        /* This will only happen if no component was selected */
        exit_status = ORTE_ERROR;
        goto cleanup;
    }

    /* Save the winner */
    orte_errmgr = *best_module;

    /* Initialize the winner */
    if (NULL != best_module) {
        if (OPAL_SUCCESS != orte_errmgr.init()) {
            exit_status = OPAL_ERROR;
            goto cleanup;
        }
    }

 cleanup:
    return exit_status;
}
