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
 * Copyright (c) 2011-2015 Los Alamos National Security, LLC.  All rights
 *                         reserved.
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


#include "orte/mca/ras/base/ras_private.h"
#include "orte/mca/ras/base/base.h"


/*
 * Select one RAS component from all those that are available.
 */
int orte_ras_base_select(void)
{
    /* For all other systems, provide the following support */

    orte_ras_base_component_t *best_component = NULL;
    orte_ras_base_module_t *best_module = NULL;

    /*
     * Select the best component
     */
    if( OPAL_SUCCESS != mca_base_select("ras", orte_ras_base_framework.framework_output,
                                        &orte_ras_base_framework.framework_components,
                                        (mca_base_module_t **) &best_module,
                                        (mca_base_component_t **) &best_component, NULL) ) {
        /* This will only happen if no component was selected */
        /* If we didn't find one to select, that is okay */
        return ORTE_SUCCESS;
    }

    /* Save the winner */
    /* No component saved */
    orte_ras_base.active_module = best_module;
    if (NULL != orte_ras_base.active_module->init) {
        return orte_ras_base.active_module->init();
    }

    return ORTE_SUCCESS;
}
