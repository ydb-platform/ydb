/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2010 The Trustees of Indiana University.
 *                         All rights reserved.
 *
 * Copyright (c) 2015 Cisco Systems, Inc.  All rights reserved.
 * $COPYRIGHT$
 * Copyright (c) 2015      Los Alamos National Security, LLC. All rights
 *                         reserved.
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"

#ifdef HAVE_UNISTD_H
#include "unistd.h"
#endif

#include "opal/include/opal/constants.h"
#include "opal/util/output.h"
#include "opal/mca/mca.h"
#include "opal/mca/base/base.h"
#include "opal/mca/dl/dl.h"
#include "opal/mca/dl/base/base.h"


int opal_dl_base_select(void)
{
    int exit_status = OPAL_SUCCESS;
    opal_dl_base_component_t *best_component = NULL;
    opal_dl_base_module_t *best_module = NULL;

    /*
     * Select the best component
     */
    if (OPAL_SUCCESS != mca_base_select("dl",
                                        opal_dl_base_framework.framework_output,
                                        &opal_dl_base_framework.framework_components,
                                        (mca_base_module_t **) &best_module,
                                        (mca_base_component_t **) &best_component, NULL) ) {
        /* This will only happen if no component was selected */
        exit_status = OPAL_ERROR;
        goto cleanup;
    }

    /* Save the winner */
    opal_dl_base_selected_component = best_component;
    opal_dl = best_module;

 cleanup:
    return exit_status;
}
