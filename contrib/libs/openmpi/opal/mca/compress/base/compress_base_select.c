/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2010 The Trustees of Indiana University.
 *                         All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 *
 * Copyright (c) 2015      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * $COPYRIGHT$
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
#include "opal/mca/compress/compress.h"
#include "opal/mca/compress/base/base.h"

int opal_compress_base_select(void)
{
    int ret, exit_status = OPAL_SUCCESS;
    opal_compress_base_component_t *best_component = NULL;
    opal_compress_base_module_t *best_module = NULL;

    /* Compression currently only used with C/R */
    if( !opal_cr_is_enabled ) {
        opal_output_verbose(10, opal_compress_base_framework.framework_output,
                            "compress:open: FT is not enabled, skipping!");
        return OPAL_SUCCESS;
    }

    /*
     * Select the best component
     */
    if( OPAL_SUCCESS != mca_base_select("compress", opal_compress_base_framework.framework_output,
                                        &opal_compress_base_framework.framework_components,
                                        (mca_base_module_t **) &best_module,
                                        (mca_base_component_t **) &best_component, NULL) ) {
        /* This will only happen if no component was selected */
        exit_status = OPAL_ERROR;
        goto cleanup;
    }

    /* Save the winner */
    opal_compress_base_selected_component = *best_component;

    /* Initialize the winner */
    if (NULL != best_module) {
        if (OPAL_SUCCESS != (ret = best_module->init()) ) {
            exit_status = ret;
            goto cleanup;
        }
        opal_compress = *best_module;
    }

 cleanup:
    return exit_status;
}
