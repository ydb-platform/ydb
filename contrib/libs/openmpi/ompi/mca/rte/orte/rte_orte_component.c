/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2012      Los Alamos National Security, LLC.  All rights reserved.
 * Copyright (c) 2014-2017 Intel, Inc. All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC. All rights
 *                         reserved.
 *
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

#include "ompi_config.h"
#include "ompi/constants.h"

#include "opal/threads/threads.h"
#include "opal/class/opal_list.h"

#include "orte/mca/rml/rml.h"
#include "orte/mca/grpcomm/base/base.h"

#include "ompi/mca/rte/rte.h"
#include "rte_orte.h"

/*
 * Public string showing the component version number
 */
const char *ompi_rte_orte_component_version_string =
    "OMPI orte rte MCA component version " OMPI_VERSION;

/*
 * Local function
 */
static int rte_orte_open(void);
static int rte_orte_close(void);

/*
 * Instantiate the public struct with all of our public information
 * and pointers to our public functions in it
 */

ompi_rte_component_t mca_rte_orte_component = {
    /* First, the mca_component_t struct containing meta information
       about the component itself */

    .base_version = {
        OMPI_RTE_BASE_VERSION_1_0_0,

        /* Component name and version */
        .mca_component_name = "orte",
        MCA_BASE_MAKE_VERSION(component, OMPI_MAJOR_VERSION, OMPI_MINOR_VERSION,
                              OMPI_RELEASE_VERSION),

        /* Component open and close functions */
        .mca_open_component = rte_orte_open,
        .mca_close_component = rte_orte_close,
    },
    .base_data = {
        /* The component is checkpoint ready */
        MCA_BASE_METADATA_PARAM_CHECKPOINT
    },
};

static int rte_orte_open(void)
{
    return OMPI_SUCCESS;
}

static int rte_orte_close(void)
{
    return OMPI_SUCCESS;
}
