/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2011-2017 Cisco Systems, Inc.  All rights reserved
 * Copyright (c) 2015      Los Alamos National Security, LLC. All rights
 *                         reserved.
 *
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"
#include "opal/constants.h"

#include "opal/mca/hwloc/hwloc-internal.h"

/*
 * Public string showing the sysinfo ompi_linux component version number
 */
const char *opal_hwloc_external_component_version_string =
    "OPAL hwloc_external hwloc MCA component version " OPAL_VERSION;


/*
 * Local function
 */
static int hwloc_external_open(void);


/*
 * Instantiate the public struct with all of our public information
 * and pointers to our public functions in it
 */

const opal_hwloc_component_t mca_hwloc_external_component = {

    /* First, the mca_component_t struct containing meta information
       about the component itself */

    .base_version = {
        OPAL_HWLOC_BASE_VERSION_2_0_0,

        /* Component name and version */
        .mca_component_name = "external",
        MCA_BASE_MAKE_VERSION(component, OPAL_MAJOR_VERSION, OPAL_MINOR_VERSION,
                              OPAL_RELEASE_VERSION),

        /* Component open and close functions */
        .mca_open_component = hwloc_external_open,
    },
    .base_data = {
        /* The component is checkpoint ready */
        MCA_BASE_METADATA_PARAM_CHECKPOINT
    },
};

static int hwloc_external_open(void)
{
    /* Must have some code in this file, or the OS X linker may
       eliminate the whole file */
    return OPAL_SUCCESS;
}
