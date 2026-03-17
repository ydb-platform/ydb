/*
 * Copyright (c) 2010      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2012      Los Alamos National Security, Inc. All rights reserved.
 * Copyright (c) 2017-2018 Intel, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include <src/include/pmix_config.h>
#include <pmix_common.h>

#include "src/mca/ptl/ptl.h"
#include "src/mca/psensor/base/base.h"
#include "src/mca/psensor/heartbeat/psensor_heartbeat.h"

/*
 * Local functions
 */

static int heartbeat_open(void);
static int heartbeat_close(void);
static int heartbeat_query(pmix_mca_base_module_t **module, int *priority);

pmix_psensor_heartbeat_component_t mca_psensor_heartbeat_component = {
    .super = {
        .base = {
            PMIX_PSENSOR_BASE_VERSION_1_0_0,

            /* Component name and version */
            .pmix_mca_component_name = "heartbeat",
            PMIX_MCA_BASE_MAKE_VERSION(component,
                                       PMIX_MAJOR_VERSION,
                                       PMIX_MINOR_VERSION,
                                       PMIX_RELEASE_VERSION),

            /* Component open and close functions */
            heartbeat_open,  /* component open  */
            heartbeat_close, /* component close */
            heartbeat_query  /* component query */
        }
    }
};


/**
  * component open/close/init function
  */
static int heartbeat_open(void)
{
    PMIX_CONSTRUCT(&mca_psensor_heartbeat_component.trackers, pmix_list_t);

    return PMIX_SUCCESS;
}


static int heartbeat_query(pmix_mca_base_module_t **module, int *priority)
{
    *priority = 5;  // irrelevant
    *module = (pmix_mca_base_module_t *)&pmix_psensor_heartbeat_module;
    return PMIX_SUCCESS;
}

/**
 *  Close all subsystems.
 */

static int heartbeat_close(void)
{
    PMIX_LIST_DESTRUCT(&mca_psensor_heartbeat_component.trackers);

    return PMIX_SUCCESS;
}
