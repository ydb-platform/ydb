/*
 * Copyright (c) 2014-2018 Intel, Inc. All rights reserved.
 * Copyright (c) 2014-2018 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2016-2018 Cisco Systems, Inc.  All rights reserved
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
#include "opal/util/show_help.h"
#include "opal/mca/pmix/pmix.h"
#include "pmix3x.h"

/*
 * Public string showing the pmix external component version number
 */
const char *opal_pmix_pmix3x_component_version_string =
    "OPAL pmix3x MCA component version " OPAL_VERSION;

/*
 * Local function
 */
static int external_register(void);
static int external_open(void);
static int external_close(void);
static int external_component_query(mca_base_module_t **module, int *priority);

/*
 * Local variable
 */
static char *pmix_library_version = NULL;


/*
 * Instantiate the public struct with all of our public information
 * and pointers to our public functions in it
 */

mca_pmix_pmix3x_component_t mca_pmix_pmix3x_component = {
    {
    /* First, the mca_component_t struct containing meta information
       about the component itself */

        .base_version = {
        /* Indicate that we are a pmix v1.1.0 component (which also
           implies a specific MCA version) */

            OPAL_PMIX_BASE_VERSION_2_0_0,

        /* Component name and version */

            .mca_component_name = "pmix3x",
            MCA_BASE_MAKE_VERSION(component, OPAL_MAJOR_VERSION, OPAL_MINOR_VERSION,
                                  OPAL_RELEASE_VERSION),

        /* Component open and close functions */

            .mca_open_component = external_open,
            .mca_close_component = external_close,
            .mca_query_component = external_component_query,
            .mca_register_component_params = external_register
        },
        /* Next the MCA v1.0.0 component meta data */
        .base_data = {
        /* The component is checkpoint ready */
            MCA_BASE_METADATA_PARAM_CHECKPOINT
        }
    },
    .native_launch = false
};

static int external_register(void)
{
    mca_base_component_t *component = &mca_pmix_pmix3x_component.super.base_version;
    char *tmp = NULL;

    mca_pmix_pmix3x_component.silence_warning = false;
    (void) mca_base_component_var_register (component, "silence_warning",
                                            "Silence warning about PMIX_INSTALL_PREFIX",
                                            MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                            OPAL_INFO_LVL_4,
                                            MCA_BASE_VAR_SCOPE_READONLY,
                                            &mca_pmix_pmix3x_component.silence_warning);

    asprintf(&pmix_library_version,
             "PMIx library version %s (embedded in Open MPI)", PMIx_Get_version());
    tmp = pmix_library_version;
    (void) mca_base_component_var_register(component, "library_version",
                                           "Version of the underlying PMIx library",
                                           MCA_BASE_VAR_TYPE_STRING,
                                           NULL, 0, 0,
                                           OPAL_INFO_LVL_4,
                                           MCA_BASE_VAR_SCOPE_CONSTANT,
                                           &pmix_library_version);
    free(tmp);

    return OPAL_SUCCESS;
}

static int external_open(void)
{
    const char *version;

    mca_pmix_pmix3x_component.evindex = 0;
    OBJ_CONSTRUCT(&mca_pmix_pmix3x_component.jobids, opal_list_t);
    OBJ_CONSTRUCT(&mca_pmix_pmix3x_component.events, opal_list_t);
    OBJ_CONSTRUCT(&mca_pmix_pmix3x_component.dmdx, opal_list_t);

    version = PMIx_Get_version();
    if ('3' > version[0]) {
        opal_show_help("help-pmix-base.txt",
                       "incorrect-pmix", true, version, "v3.x");
        return OPAL_ERROR;
    }
    return OPAL_SUCCESS;
}

static int external_close(void)
{
    OPAL_LIST_DESTRUCT(&mca_pmix_pmix3x_component.jobids);
    OPAL_LIST_DESTRUCT(&mca_pmix_pmix3x_component.events);
    OPAL_LIST_DESTRUCT(&mca_pmix_pmix3x_component.dmdx);

    return OPAL_SUCCESS;
}


static int external_component_query(mca_base_module_t **module, int *priority)
{
    char *t, *id;

    /* see if a PMIx server is present */
    if (NULL != (t = getenv("PMIX_SERVER_URI")) ||
        NULL != (id = getenv("PMIX_ID"))) {
        /* if PMIx is present, then we are a client and need to use it */
        *priority = 100;
    } else {
        /* we could be a server, so we still need to be considered */
        *priority = 5;
    }
    *module = (mca_base_module_t *)&opal_pmix_pmix3x_module;
    return OPAL_SUCCESS;
}
