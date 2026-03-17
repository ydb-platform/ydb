/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2016 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2008      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC. All rights
 *                         reserved.
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
#include "coll_basic.h"

#include "mpi.h"
#include "ompi/mca/coll/coll.h"
#include "coll_basic.h"

/*
 * Public string showing the coll ompi_basic component version number
 */
const char *mca_coll_basic_component_version_string =
    "Open MPI basic collective MCA component version " OMPI_VERSION;

/*
 * Global variables
 */
int mca_coll_basic_priority = 10;
int mca_coll_basic_crossover = 4;

/*
 * Local function
 */
static int basic_register(void);

/*
 * Instantiate the public struct with all of our public information
 * and pointers to our public functions in it
 */

const mca_coll_base_component_2_0_0_t mca_coll_basic_component = {

    /* First, the mca_component_t struct containing meta information
     * about the component itself */

    .collm_version = {
        MCA_COLL_BASE_VERSION_2_0_0,

        /* Component name and version */
        .mca_component_name = "basic",
        MCA_BASE_MAKE_VERSION(component, OMPI_MAJOR_VERSION, OMPI_MINOR_VERSION,
                              OMPI_RELEASE_VERSION),

        /* Component open and close functions */
        .mca_register_component_params = basic_register,
    },
    .collm_data = {
        /* The component is checkpoint ready */
        MCA_BASE_METADATA_PARAM_CHECKPOINT
    },

    /* Initialization / querying functions */

    .collm_init_query = mca_coll_basic_init_query,
    .collm_comm_query = mca_coll_basic_comm_query,
};


static int
basic_register(void)
{
    /* Use a low priority, but allow other components to be lower */

    mca_coll_basic_priority = 10;
    (void) mca_base_component_var_register(&mca_coll_basic_component.collm_version, "priority",
                                           "Priority of the basic coll component",
                                           MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                           OPAL_INFO_LVL_9,
                                           MCA_BASE_VAR_SCOPE_READONLY,
                                           &mca_coll_basic_priority);
    mca_coll_basic_crossover = 4;
    (void) mca_base_component_var_register(&mca_coll_basic_component.collm_version, "crossover",
                                           "Minimum number of processes in a communicator before using the logarithmic algorithms",
                                           MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                           OPAL_INFO_LVL_9,
                                           MCA_BASE_VAR_SCOPE_READONLY,
                                           &mca_coll_basic_crossover);

    return OMPI_SUCCESS;
}

OBJ_CLASS_INSTANCE(mca_coll_basic_module_t,
                   mca_coll_base_module_t,
                   NULL,
                   NULL);

