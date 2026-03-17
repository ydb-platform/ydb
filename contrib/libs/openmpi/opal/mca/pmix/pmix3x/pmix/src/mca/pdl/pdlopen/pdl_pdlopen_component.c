/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2015 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include <src/include/pmix_config.h>

#include "pmix_common.h"
#include "src/mca/pdl/pdl.h"
#include "src/util/argv.h"

#include "pdl_pdlopen.h"


/*
 * Public string showing the sysinfo ompi_linux component version number
 */
const char *pmix_pdl_pdlopen_component_version_string =
    "PMIX pdl pdlopen MCA component version " PMIX_VERSION;


/*
 * Local functions
 */
static int pdlopen_component_register(void);
static int pdlopen_component_open(void);
static int pdlopen_component_close(void);
static int pdlopen_component_query(pmix_mca_base_module_t **module, int *priority);

/*
 * Instantiate the public struct with all of our public information
 * and pointers to our public functions in it
 */

pmix_pdl_pdlopen_component_t mca_pdl_pdlopen_component = {

    /* Fill in the mca_pdl_base_component_t */
    .base = {

        /* First, the mca_component_t struct containing meta information
           about the component itself */
        .base_version = {
            PMIX_PDL_BASE_VERSION_1_0_0,

            /* Component name and version */
            .pmix_mca_component_name = "pdlopen",
            PMIX_MCA_BASE_MAKE_VERSION(component, PMIX_MAJOR_VERSION, PMIX_MINOR_VERSION,
                                       PMIX_RELEASE_VERSION),

            /* Component functions */
            .pmix_mca_register_component_params = pdlopen_component_register,
            .pmix_mca_open_component = pdlopen_component_open,
            .pmix_mca_close_component = pdlopen_component_close,
            .pmix_mca_query_component = pdlopen_component_query,
        },

        .base_data = {
            /* The component is checkpoint ready */
            PMIX_MCA_BASE_METADATA_PARAM_CHECKPOINT
        },

        /* The pdl framework members */
        .priority = 80
    },
};


static int pdlopen_component_register(void)
{
    int ret;

    mca_pdl_pdlopen_component.filename_suffixes_mca_storage = ".so,.dylib,.dll,.sl";
    ret =
        pmix_mca_base_component_var_register(&mca_pdl_pdlopen_component.base.base_version,
                                             "filename_suffixes",
                                             "Comma-delimited list of filename suffixes that the pdlopen component will try",
                                             PMIX_MCA_BASE_VAR_TYPE_STRING,
                                             NULL,
                                             0,
                                             PMIX_MCA_BASE_VAR_FLAG_SETTABLE,
                                             PMIX_INFO_LVL_5,
                                             PMIX_MCA_BASE_VAR_SCOPE_LOCAL,
                                             &mca_pdl_pdlopen_component.filename_suffixes_mca_storage);
    if (ret < 0) {
        return ret;
    }
    mca_pdl_pdlopen_component.filename_suffixes =
        pmix_argv_split(mca_pdl_pdlopen_component.filename_suffixes_mca_storage,
                        ',');

    return PMIX_SUCCESS;
}

static int pdlopen_component_open(void)
{
    return PMIX_SUCCESS;
}


static int pdlopen_component_close(void)
{
    if (NULL != mca_pdl_pdlopen_component.filename_suffixes) {
        pmix_argv_free(mca_pdl_pdlopen_component.filename_suffixes);
        mca_pdl_pdlopen_component.filename_suffixes = NULL;
    }

    return PMIX_SUCCESS;
}


static int pdlopen_component_query(pmix_mca_base_module_t **module, int *priority)
{
    /* The priority value is somewhat meaningless here; by
       pmix/mca/pdl/configure.m4, there's at most one component
       available. */
    *priority = mca_pdl_pdlopen_component.base.priority;
    *module = &pmix_pdl_pdlopen_module.super;

    return PMIX_SUCCESS;
}
