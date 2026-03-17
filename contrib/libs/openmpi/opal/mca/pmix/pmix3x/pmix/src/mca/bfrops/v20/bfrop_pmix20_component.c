/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2008 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennbfropsee and The University
 *                         of Tennbfropsee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2016-2017 Intel, Inc.  All rights reserved.
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
#include <pmix_common.h>
#include "src/include/types.h"
#include "src/include/pmix_globals.h"

#include "src/util/error.h"
#include "src/server/pmix_server_ops.h"
#include "src/mca/bfrops/base/base.h"
#include "bfrop_pmix20.h"

extern pmix_bfrops_module_t pmix_bfrops_pmix20_module;

static pmix_status_t component_open(void);
static pmix_status_t component_query(pmix_mca_base_module_t **module, int *priority);
static pmix_status_t component_close(void);
static pmix_bfrops_module_t* assign_module(void);

/*
 * Instantiate the public struct with all of our public information
 * and pointers to our public functions in it
 */
pmix_bfrops_base_component_t mca_bfrops_v20_component = {
    .base = {
        PMIX_BFROPS_BASE_VERSION_1_0_0,

        /* Component name and version */
        .pmix_mca_component_name = "v20",
        PMIX_MCA_BASE_MAKE_VERSION(component, PMIX_MAJOR_VERSION, PMIX_MINOR_VERSION,
                                   PMIX_RELEASE_VERSION),

        /* Component open and close functions */
        .pmix_mca_open_component = component_open,
        .pmix_mca_close_component = component_close,
        .pmix_mca_query_component = component_query,
    },
    .priority = 20,
    .assign_module = assign_module
};


pmix_status_t component_open(void)
{
    /* setup the types array */
    PMIX_CONSTRUCT(&mca_bfrops_v20_component.types, pmix_pointer_array_t);
    pmix_pointer_array_init(&mca_bfrops_v20_component.types, 32, INT_MAX, 16);

    return PMIX_SUCCESS;
}


pmix_status_t component_query(pmix_mca_base_module_t **module, int *priority)
{

    *priority = mca_bfrops_v20_component.priority;
    *module = (pmix_mca_base_module_t *)&pmix_bfrops_pmix20_module;
    return PMIX_SUCCESS;
}


pmix_status_t component_close(void)
{
    PMIX_DESTRUCT(&mca_bfrops_v20_component.types);
    return PMIX_SUCCESS;
}

static pmix_bfrops_module_t* assign_module(void)
{
    pmix_output_verbose(10, pmix_bfrops_base_framework.framework_output,
                        "bfrops:pmix20x assigning module");
    return &pmix_bfrops_pmix20_module;
}
