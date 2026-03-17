/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2010 The Trustees of Indiana University.
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
#include "opal/mca/compress/compress.h"
#include "opal/mca/compress/base/base.h"
#include "compress_gzip.h"

/*
 * Public string for version number
 */
const char *opal_compress_gzip_component_version_string =
"OPAL COMPRESS gzip MCA component version " OPAL_VERSION;

/*
 * Local functionality
 */
static int compress_gzip_register (void);
static int compress_gzip_open(void);
static int compress_gzip_close(void);

/*
 * Instantiate the public struct with all of our public information
 * and pointer to our public functions in it
 */
opal_compress_gzip_component_t mca_compress_gzip_component = {
    /* First do the base component stuff */
    {
        /* Handle the general mca_component_t struct containing
         *  meta information about the component itgzip
         */
        .base_version = {
            OPAL_COMPRESS_BASE_VERSION_2_0_0,

            /* Component name and version */
            .mca_component_name = "gzip",
            MCA_BASE_MAKE_VERSION(component, OPAL_MAJOR_VERSION, OPAL_MINOR_VERSION,
                                  OPAL_RELEASE_VERSION),

            /* Component open and close functions */
            .mca_open_component = compress_gzip_open,
            .mca_close_component = compress_gzip_close,
            .mca_query_component = opal_compress_gzip_component_query,
            .mca_register_component_params = compress_gzip_register
        },
        .base_data = {
            /* The component is checkpoint ready */
            MCA_BASE_METADATA_PARAM_CHECKPOINT
        },

        .verbose = 0,
        .output_handle = -1,
    }
};

/*
 * Gzip module
 */
static opal_compress_base_module_t loc_module = {
    /** Initialization Function */
    opal_compress_gzip_module_init,
    /** Finalization Function */
    opal_compress_gzip_module_finalize,

    /** Compress Function */
    opal_compress_gzip_compress,
    opal_compress_gzip_compress_nb,

    /** Decompress Function */
    opal_compress_gzip_decompress,
    opal_compress_gzip_decompress_nb
};

static int compress_gzip_register (void)
{
    int ret;

    mca_compress_gzip_component.super.priority = 15;
    ret = mca_base_component_var_register (&mca_compress_gzip_component.super.base_version,
                                           "priority", "Priority of the COMPRESS gzip component "
                                           "(default: 15)", MCA_BASE_VAR_TYPE_INT, NULL, 0,
                                           MCA_BASE_VAR_FLAG_SETTABLE,
                                           OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_ALL_EQ,
                                           &mca_compress_gzip_component.super.priority);
    if (0 > ret) {
        return ret;
    }

    mca_compress_gzip_component.super.verbose = 0;
    ret = mca_base_component_var_register (&mca_compress_gzip_component.super.base_version,
                                           "verbose",
                                           "Verbose level for the COMPRESS gzip component",
                                           MCA_BASE_VAR_TYPE_INT, NULL, 0, MCA_BASE_VAR_FLAG_SETTABLE,
                                           OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_LOCAL,
                                           &mca_compress_gzip_component.super.verbose);
    return (0 > ret) ? ret : OPAL_SUCCESS;
}

static int compress_gzip_open(void)
{
    /* If there is a custom verbose level for this component than use it
     * otherwise take our parents level and output channel
     */
    if ( 0 != mca_compress_gzip_component.super.verbose) {
        mca_compress_gzip_component.super.output_handle = opal_output_open(NULL);
        opal_output_set_verbosity(mca_compress_gzip_component.super.output_handle,
                                  mca_compress_gzip_component.super.verbose);
    } else {
        mca_compress_gzip_component.super.output_handle = opal_compress_base_framework.framework_output;
    }

    /*
     * Debug output
     */
    opal_output_verbose(10, mca_compress_gzip_component.super.output_handle,
                        "compress:gzip: open()");
    opal_output_verbose(20, mca_compress_gzip_component.super.output_handle,
                        "compress:gzip: open: priority = %d",
                        mca_compress_gzip_component.super.priority);
    opal_output_verbose(20, mca_compress_gzip_component.super.output_handle,
                        "compress:gzip: open: verbosity = %d",
                        mca_compress_gzip_component.super.verbose);
    return OPAL_SUCCESS;
}

static int compress_gzip_close(void)
{
    return OPAL_SUCCESS;
}

int opal_compress_gzip_component_query(mca_base_module_t **module, int *priority)
{
    *module   = (mca_base_module_t *)&loc_module;
    *priority = mca_compress_gzip_component.super.priority;

    return OPAL_SUCCESS;
}

