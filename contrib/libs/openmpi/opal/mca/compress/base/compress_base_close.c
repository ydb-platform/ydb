/*
 * Copyright (c) 2004-2010 The Trustees of Indiana University.
 *                         All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"

#include <string.h>
#include "opal/mca/mca.h"
#include "opal/mca/base/base.h"
#include "opal/include/opal/constants.h"
#include "opal/mca/compress/compress.h"
#include "opal/mca/compress/base/base.h"

int opal_compress_base_close(void)
{
    /* Compression currently only used with C/R */
    if( !opal_cr_is_enabled ) {
        opal_output_verbose(10, opal_compress_base_framework.framework_output,
                            "compress:open: FT is not enabled, skipping!");
        return OPAL_SUCCESS;
    }

    /* Call the component's finalize routine */
    if( NULL != opal_compress.finalize ) {
        opal_compress.finalize();
    }

    /* Close all available modules that are open */
    return mca_base_framework_components_close (&opal_compress_base_framework, NULL);
}
