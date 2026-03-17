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

#include <src/include/pmix_config.h>

#ifdef HAVE_UNISTD_H
#include "unistd.h"
#endif

#include "pmix_common.h"
#include "src/util/output.h"
#include "src/mca/mca.h"
#include "src/mca/base/base.h"
#include "src/mca/pdl/pdl.h"
#include "src/mca/pdl/base/base.h"


int pmix_pdl_base_select(void)
{
    int exit_status = PMIX_SUCCESS;
    pmix_pdl_base_component_t *best_component = NULL;
    pmix_pdl_base_module_t *best_module = NULL;

    /*
     * Select the best component
     */
    if (PMIX_SUCCESS != pmix_mca_base_select("pdl",
                                             pmix_pdl_base_framework.framework_output,
                                             &pmix_pdl_base_framework.framework_components,
                                             (pmix_mca_base_module_t **) &best_module,
                                             (pmix_mca_base_component_t **) &best_component, NULL) ) {
        /* This will only happen if no component was selected */
        exit_status = PMIX_ERROR;
        goto cleanup;
    }

    /* Save the winner */
    pmix_pdl_base_selected_component = best_component;
    pmix_pdl = best_module;

 cleanup:
    return exit_status;
}
