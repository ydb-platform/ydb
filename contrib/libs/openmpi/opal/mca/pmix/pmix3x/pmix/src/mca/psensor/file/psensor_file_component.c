/*
 * Copyright (c) 2010      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2017      Intel, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include <src/include/pmix_config.h>
#include <pmix_common.h>

#include "src/class/pmix_list.h"

#include "src/mca/psensor/base/base.h"
#include "src/mca/psensor/file/psensor_file.h"

/*
 * Local functions
 */
static int psensor_file_open(void);
static int psensor_file_close(void);
static int psensor_file_query(pmix_mca_base_module_t **module, int *priority);

pmix_psensor_file_component_t mca_psensor_file_component = {
    .super = {
        .base = {
            PMIX_PSENSOR_BASE_VERSION_1_0_0,

            /* Component name and version */
            .pmix_mca_component_name = "file",
            PMIX_MCA_BASE_MAKE_VERSION(component,
                                       PMIX_MAJOR_VERSION,
                                       PMIX_MINOR_VERSION,
                                       PMIX_RELEASE_VERSION),

            /* Component open and close functions */
            psensor_file_open,  /* component open  */
            psensor_file_close, /* component close */
            psensor_file_query  /* component query */
        },
    }
};


static int psensor_file_open(void)
{
    PMIX_CONSTRUCT(&mca_psensor_file_component.trackers, pmix_list_t);
    return PMIX_SUCCESS;
}


static int psensor_file_query(pmix_mca_base_module_t **module, int *priority)
{
    *priority = 20;  /* irrelevant */
    *module = (pmix_mca_base_module_t *)&pmix_psensor_file_module;
    return PMIX_SUCCESS;
}

/**
 *  Close all subsystems.
 */

static int psensor_file_close(void)
{
    PMIX_LIST_DESTRUCT(&mca_psensor_file_component.trackers);
    return PMIX_SUCCESS;
}
