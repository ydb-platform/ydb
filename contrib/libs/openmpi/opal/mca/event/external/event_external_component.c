/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2011      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2013-2015 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2017      IBM Corporation.  All rights reserved.
 *
 * Copyright (c) 2017      Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"
#include "opal/constants.h"

#include "opal/mca/event/event.h"

#include "event.h"

#include "opal/util/argv.h"

/*
 * Public string showing the sysinfo ompi_linux component version number
 */
const char *opal_event_external_component_version_string =
    "OPAL event_external event MCA component version " OPAL_VERSION;


/*
 * Local function
 */
static int event_external_open(void);
static int event_external_register (void);

char *ompi_event_module_include = NULL;

/*
 * Instantiate the public struct with all of our public information
 * and pointers to our public functions in it
 */

const opal_event_component_t mca_event_external_component = {

    /* First, the mca_component_t struct containing meta information
       about the component itself */

    .base_version = {
        OPAL_EVENT_BASE_VERSION_2_0_0,

        /* Component name and version */
        .mca_component_name = "external",
        MCA_BASE_MAKE_VERSION(component, OPAL_MAJOR_VERSION, OPAL_MINOR_VERSION,
                              OPAL_RELEASE_VERSION),

        /* Component open and close functions */
        .mca_open_component = event_external_open,
        .mca_register_component_params = event_external_register
    },
    .base_data = {
        /* The component is checkpoint ready */
        MCA_BASE_METADATA_PARAM_CHECKPOINT
    }
};

static int event_external_open(void)
{
    /* Must have some code in this file, or the OS X linker may
       eliminate the whole file */
    return OPAL_SUCCESS;
}

static int event_external_register (void) {
    const char **all_available_eventops;
    char *avail = NULL;
    char *help_msg = NULL;
    int ret;

    // Get supported methods
    all_available_eventops = event_get_supported_methods();

#ifdef __APPLE__
    ompi_event_module_include ="select";
#else
    ompi_event_module_include = "poll";
#endif

    avail = opal_argv_join((char**)all_available_eventops, ',');
    asprintf( &help_msg,
              "Comma-delimited list of libevent subsystems "
              "to use (%s -- available on your platform)",
              avail );

    ret = mca_base_component_var_register (&mca_event_external_component.base_version,
                                           "event_include", help_msg,
                                           MCA_BASE_VAR_TYPE_STRING, NULL, 0,
                                           MCA_BASE_VAR_FLAG_SETTABLE,
                                           OPAL_INFO_LVL_3,
                                           MCA_BASE_VAR_SCOPE_LOCAL,
                                           &ompi_event_module_include);
    free(help_msg);  /* release the help message */
    free(avail);
    avail = NULL;

    if (0 > ret) {
        return ret;
    }

    ret = mca_base_var_register_synonym (ret, "opal", "opal", "event", "include", 0);
    if (0 > ret) {
        return ret;
    }

    return OPAL_SUCCESS;
}
