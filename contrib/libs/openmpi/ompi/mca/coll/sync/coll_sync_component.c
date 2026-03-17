/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2008-2009 Cisco Systems, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include <string.h>

#include "opal/util/output.h"

#include "mpi.h"
#include "ompi/constants.h"
#include "coll_sync.h"

/*
 * Public string showing the coll ompi_sync component version number
 */
const char *mca_coll_sync_component_version_string =
    "Open MPI sync collective MCA component version " OMPI_VERSION;

/*
 * Local function
 */
static int sync_register(void);

/*
 * Instantiate the public struct with all of our public information
 * and pointers to our public functions in it
 */

mca_coll_sync_component_t mca_coll_sync_component = {
    {
        /* First, the mca_component_t struct containing meta information
         * about the component itself */

       .collm_version = {
            MCA_COLL_BASE_VERSION_2_0_0,

            /* Component name and version */
            .mca_component_name = "sync",
            MCA_BASE_MAKE_VERSION(component, OMPI_MAJOR_VERSION, OMPI_MINOR_VERSION,
                                  OMPI_RELEASE_VERSION),

            /* Component open and close functions */
            .mca_register_component_params = sync_register
        },
        .collm_data = {
            /* The component is checkpoint ready */
            MCA_BASE_METADATA_PARAM_CHECKPOINT
        },

        /* Initialization / querying functions */

        .collm_init_query = mca_coll_sync_init_query,
        .collm_comm_query = mca_coll_sync_comm_query
    },
};


static int sync_register(void)
{
    mca_base_component_t *c = &mca_coll_sync_component.super.collm_version;

    mca_coll_sync_component.priority = 50;
    (void) mca_base_component_var_register(c, "priority",
                                           "Priority of the sync coll component; only relevant if barrier_before or barrier_after is > 0",
                                           MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                           OPAL_INFO_LVL_9,
                                           MCA_BASE_VAR_SCOPE_READONLY,
                                           &mca_coll_sync_component.priority);

    mca_coll_sync_component.barrier_before_nops = 0;
    (void) mca_base_component_var_register(c, "barrier_before",
                                           "Do a synchronization before each Nth collective",
                                           MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                           OPAL_INFO_LVL_9,
                                           MCA_BASE_VAR_SCOPE_READONLY,
                                           &mca_coll_sync_component.barrier_before_nops);

    mca_coll_sync_component.barrier_after_nops = 0;
    (void) mca_base_component_var_register(c, "barrier_after",
                                           "Do a synchronization after each Nth collective",
                                           MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                           OPAL_INFO_LVL_9,
                                           MCA_BASE_VAR_SCOPE_READONLY,
                                           &mca_coll_sync_component.barrier_after_nops);

    return OMPI_SUCCESS;
}
