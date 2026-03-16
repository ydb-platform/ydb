/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2008 The Trustees of Indiana University.
 *                         All rights reserved.
 * Copyright (c) 2004-2005 The Trustees of the University of Tennessee.
 *                         All rights reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2012-2015 Los Alamos National Security, LLC.
 *                         All rights reserved
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

#include "orte/mca/filem/filem.h"
#include "orte/mca/filem/base/base.h"


int orte_filem_base_select(void)
{
    int exit_status = ORTE_SUCCESS;
    orte_filem_base_component_t *best_component = NULL;
    orte_filem_base_module_t *best_module = NULL;

    /*
     * Select the best component
     */
    if( OPAL_SUCCESS != mca_base_select("filem", orte_filem_base_framework.framework_output,
                                        &orte_filem_base_framework.framework_components,
                                        (mca_base_module_t **) &best_module,
                                        (mca_base_component_t **) &best_component, NULL) ) {
        /* It is okay to not select anything - we'll just retain
         * the default none module
         */
        return ORTE_SUCCESS;
    }

    /* Save the winner */
    orte_filem = *best_module;

    /* Initialize the winner */
    if (NULL != orte_filem.filem_init) {
        if (ORTE_SUCCESS != orte_filem.filem_init()) {
            exit_status = ORTE_ERROR;
        }
    }

    return exit_status;
}
