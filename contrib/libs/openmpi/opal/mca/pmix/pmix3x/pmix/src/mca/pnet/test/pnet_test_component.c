/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2008 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2016-2018 Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 * These symbols are in a file by themselves to provide nice linker
 * semantics.  Since linkers generally pull in symbols by object
 * files, keeping these symbols as the only symbols in this file
 * prevents utility programs such as "ompi_info" from having to import
 * entire components just to query their version and parameters.
 */

#include <src/include/pmix_config.h>
#include "pmix_common.h"

#include "src/util/argv.h"
#include "src/mca/pnet/pnet.h"
#include "pnet_test.h"

static pmix_status_t component_open(void);
static pmix_status_t component_close(void);
static pmix_status_t component_query(pmix_mca_base_module_t **module, int *priority);

/*
 * Instantiate the public struct with all of our public information
 * and pointers to our public functions in it
 */
pmix_pnet_test_component_t mca_pnet_test_component = {
    .super = {
        .base = {
            PMIX_PNET_BASE_VERSION_1_0_0,

            /* Component name and version */
            .pmix_mca_component_name = "test",
            PMIX_MCA_BASE_MAKE_VERSION(component,
                                       PMIX_MAJOR_VERSION,
                                       PMIX_MINOR_VERSION,
                                       PMIX_RELEASE_VERSION),

            /* Component open and close functions */
            .pmix_mca_open_component = component_open,
            .pmix_mca_close_component = component_close,
            .pmix_mca_query_component = component_query,
        },
        .data = {
            /* The component is checkpoint ready */
            PMIX_MCA_BASE_METADATA_PARAM_CHECKPOINT
        }
    },
    .include = NULL,
    .exclude = NULL
};

static pmix_status_t component_open(void)
{
    int index;
    const pmix_mca_base_var_storage_t *value=NULL;

    /* we only allow ourselves to be considered IF the user
     * specifically requested so */
    if (0 > (index = pmix_mca_base_var_find("pmix", "pnet", NULL, NULL))) {
        return PMIX_ERROR;
    }
    pmix_mca_base_var_get_value(index, &value, NULL, NULL);
    if (NULL != value && NULL != value->stringval && '\0' != value->stringval[0]) {
        if (NULL != strstr(value->stringval, "test")) {
            return PMIX_SUCCESS;
        }
    }
    return PMIX_ERROR;
}


static pmix_status_t component_query(pmix_mca_base_module_t **module, int *priority)
{
    *priority = 0;
    *module = (pmix_mca_base_module_t *)&pmix_test_module;
    return PMIX_SUCCESS;
}


static pmix_status_t component_close(void)
{
    return PMIX_SUCCESS;
}
