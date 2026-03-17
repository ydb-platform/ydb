/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2007 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2008 The Trustees of Indiana University.
 *                         All rights reserved.
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
#include "opal/mca/memchecker/memchecker.h"
#include "opal/mca/memchecker/base/base.h"

/*
 * Globals
 */
bool opal_memchecker_base_selected = false;
const opal_memchecker_base_component_2_0_0_t *opal_memchecker_base_component = NULL;
const opal_memchecker_base_module_1_0_0_t *opal_memchecker_base_module = NULL;


int opal_memchecker_base_select(void)
{
#if OPAL_WANT_MEMCHECKER
    int ret, exit_status = OPAL_SUCCESS;
    opal_memchecker_base_component_2_0_0_t *best_component = NULL;
    opal_memchecker_base_module_1_0_0_t *best_module = NULL;

    /*
     * Select the best component
     */
    if( OPAL_SUCCESS != mca_base_select("memchecker", opal_memchecker_base_framework.framework_output,
                                        &opal_memchecker_base_framework.framework_components,
                                        (mca_base_module_t **) &best_module,
                                        (mca_base_component_t **) &best_component, NULL) ) {
        /* This will only happen if no component was selected */
        exit_status = OPAL_ERR_NOT_FOUND;
        goto cleanup;
    }

    /* Save the winner */
    opal_memchecker_base_component = best_component;
    opal_memchecker_base_module    = best_module;
    opal_memchecker_base_selected  = true;

    /* Initialize the winner */
    if (NULL != opal_memchecker_base_module) {
        if (OPAL_SUCCESS != (ret = opal_memchecker_base_module->init()) ) {
            exit_status = ret;
            goto cleanup;
        }
    }

 cleanup:
    return exit_status;
#else
    return OPAL_SUCCESS;
#endif /* OPAL_WANT_MEMCHECKER */
}

