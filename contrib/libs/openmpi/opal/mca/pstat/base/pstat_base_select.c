/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2008 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2006 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007      Cisco Systems, Inc. All rights reserved.
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
#include "opal/mca/pstat/pstat.h"
#include "opal/mca/pstat/base/base.h"

/*
 * Globals
 */

int opal_pstat_base_select(void)
{
    int ret, exit_status = OPAL_SUCCESS;
    opal_pstat_base_component_t *best_component = NULL;
    opal_pstat_base_module_t *best_module = NULL;

    /*
     * Select the best component
     */
    if( OPAL_SUCCESS != mca_base_select("pstat", opal_pstat_base_framework.framework_output,
                                        &opal_pstat_base_framework.framework_components,
                                        (mca_base_module_t **) &best_module,
                                        (mca_base_component_t **) &best_component, NULL) ) {
        /* It is okay if we don't find a runnable component - default
         * to the unsupported default.
         */
        goto cleanup;
    }

    /* Save the winner */
    opal_pstat_base_component = best_component;
    opal_pstat                = *best_module;

    /* Initialize the winner */
    if (OPAL_SUCCESS != (ret = opal_pstat.init()) ) {
        exit_status = ret;
        goto cleanup;
    }

 cleanup:
    return exit_status;
}
