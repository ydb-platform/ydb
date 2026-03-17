/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2008 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2018      Intel, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */


#include "orte_config.h"
#include "orte/constants.h"

#include "orte/mca/mca.h"
#include "opal/mca/base/base.h"

#include "orte/mca/regx/base/base.h"


/**
 * Function for selecting one component from all those that are
 * available.
 */
int orte_regx_base_select(void)
{
    orte_regx_base_component_t *best_component = NULL;
    orte_regx_base_module_t *best_module = NULL;
    int rc = ORTE_SUCCESS;

    /*
     * Select the best component
     */
    if (OPAL_SUCCESS != mca_base_select("regx", orte_regx_base_framework.framework_output,
                                        &orte_regx_base_framework.framework_components,
                                        (mca_base_module_t **) &best_module,
                                        (mca_base_component_t **) &best_component, NULL)) {
        /* This will only happen if no component was selected */
        return ORTE_ERR_NOT_FOUND;
    }

    /* Save the winner */
    orte_regx = *best_module;
    /* give it a chance to init */
    if (NULL != orte_regx.init) {
        rc = orte_regx.init();
    }
    return rc;
}
