/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2010 The Trustees of Indiana University.
 *                         All rights reserved.
 * Copyright (c) 2011-2013 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"

#include "opal/mca/base/base.h"
#include "opal/mca/compress/base/base.h"

#include "opal/mca/compress/base/static-components.h"

/*
 * Globals
 */
opal_compress_base_module_t opal_compress = {
    NULL, /* init             */
    NULL, /* finalize         */
    NULL, /* compress         */
    NULL, /* compress_nb      */
    NULL, /* decompress       */
    NULL  /* decompress_nb    */
};

opal_compress_base_component_t opal_compress_base_selected_component = {{0}};

static int opal_compress_base_register(mca_base_register_flag_t flags);

MCA_BASE_FRAMEWORK_DECLARE(opal, compress, "COMPRESS MCA",
                           opal_compress_base_register, opal_compress_base_open,
                           opal_compress_base_close, mca_compress_base_static_components, 0);

static int opal_compress_base_register(mca_base_register_flag_t flags)
{
    return OPAL_SUCCESS;
}

/**
 * Function for finding and opening either all MCA components,
 * or the one that was specifically requested via a MCA parameter.
 */
int opal_compress_base_open(mca_base_open_flag_t flags)
{
    /* Compression currently only used with C/R */
    if(!opal_cr_is_enabled) {
        opal_output_verbose(10, opal_compress_base_framework.framework_output,
                            "compress:open: FT is not enabled, skipping!");
        return OPAL_SUCCESS;
    }

    /* Open up all available components */
    return mca_base_framework_components_open(&opal_compress_base_framework, flags);
}
