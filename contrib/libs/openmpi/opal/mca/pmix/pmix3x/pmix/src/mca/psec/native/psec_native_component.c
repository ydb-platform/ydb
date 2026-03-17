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
 * Copyright (c) 2016      Intel, Inc. All rights reserved.
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


#include "src/mca/psec/psec.h"
#include "psec_native.h"

static pmix_status_t component_open(void);
static pmix_status_t component_close(void);
static pmix_status_t component_query(pmix_mca_base_module_t **module, int *priority);
static pmix_psec_module_t* assign_module(void);

/*
 * Instantiate the public struct with all of our public information
 * and pointers to our public functions in it
 */
pmix_psec_base_component_t mca_psec_native_component = {
    .base = {
        PMIX_PSEC_BASE_VERSION_1_0_0,

        /* Component name and version */
        .pmix_mca_component_name = "native",
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
    },
    .assign_module = assign_module
};


static int component_open(void)
{
    return PMIX_SUCCESS;
}


static int component_query(pmix_mca_base_module_t **module, int *priority)
{
    *priority = 10;
    *module = (pmix_mca_base_module_t *)&pmix_native_module;
    return PMIX_SUCCESS;
}


static int component_close(void)
{
    return PMIX_SUCCESS;
}

static pmix_psec_module_t* assign_module(void)
{
    return &pmix_native_module;
}
