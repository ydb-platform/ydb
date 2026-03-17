/*
 * Copyright (c) 2016      Intel, Inc.  All rights reserved.
 * Copyright (c) 2016 Cisco Systems, Inc.  All rights reserved.
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

#include "opal_config.h"

#include "opal/constants.h"
#include "opal/class/opal_list.h"
#include "opal/util/proc.h"
#include "opal/mca/pmix/pmix.h"
#include "pmix_isolated.h"

/*
 * Public string showing the pmix isolated component version number
 */
const char *opal_pmix_isolated_component_version_string =
    "OPAL isolated pmix MCA component version " OPAL_VERSION;

/*
 * Local function
 */
static int isolated_open(void);
static int isolated_close(void);
static int isolated_component_query(mca_base_module_t **module, int *priority);


/*
 * Instantiate the public struct with all of our public information
 * and pointers to our public functions in it
 */

opal_pmix_base_component_t mca_pmix_isolated_component = {
    .base_version = {
	/* Indicate that we are a pmix v1.1.0 component (which also
	   implies a specific MCA version) */

	OPAL_PMIX_BASE_VERSION_2_0_0,

	/* Component name and version */

	.mca_component_name = "isolated",
	MCA_BASE_MAKE_VERSION(component, OPAL_MAJOR_VERSION, OPAL_MINOR_VERSION,
			      OPAL_RELEASE_VERSION),

	/* Component open and close functions */

	.mca_open_component = isolated_open,
	.mca_close_component = isolated_close,
	.mca_query_component = isolated_component_query,
    },
    /* Next the MCA v1.0.0 component meta data */
    .base_data = {
	/* The component is checkpoint ready */
	MCA_BASE_METADATA_PARAM_CHECKPOINT
    }
};

static int isolated_open(void)
{
    return OPAL_SUCCESS;
}

static int isolated_close(void)
{
    return OPAL_SUCCESS;
}


static int isolated_component_query(mca_base_module_t **module, int *priority)
{
    /* ignore us unless requested */
    *priority = 0;
    *module = (mca_base_module_t *)&opal_pmix_isolated_module;
    return OPAL_SUCCESS;
}
