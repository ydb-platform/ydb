/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
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
#include "coll_self.h"

#include "mpi.h"
#include "ompi/mca/coll/coll.h"
#include "coll_self.h"

/*
 * Public string showing the coll ompi_self component version number
 */
const char *mca_coll_self_component_version_string =
  "Open MPI self collective MCA component version " OMPI_VERSION;

/*
 * Global variable
 */
int ompi_coll_self_priority = 0;

/*
 * Local function
 */
static int self_register(void);


/*
 * Instantiate the public struct with all of our public information
 * and pointers to our public functions in it
 */

const mca_coll_base_component_2_0_0_t mca_coll_self_component = {

    /* First, the mca_component_t struct containing meta information
       about the component itself */

    .collm_version = {
        MCA_COLL_BASE_VERSION_2_0_0,

        /* Component name and version */
        .mca_component_name = "self",
        MCA_BASE_MAKE_VERSION(component, OMPI_MAJOR_VERSION, OMPI_MINOR_VERSION,
                              OMPI_RELEASE_VERSION),

        /* Component open and close functions */
        .mca_register_component_params = self_register,
    },
    .collm_data = {
        /* The component is checkpoint ready */
        MCA_BASE_METADATA_PARAM_CHECKPOINT
    },

    /* Initialization / querying functions */

    .collm_init_query = mca_coll_self_init_query,
    .collm_comm_query = mca_coll_self_comm_query,
};

static int self_register(void)
{
    /* We'll always be picked if there's only one process in the
       communicator */
    ompi_coll_self_priority = 75;
    (void) mca_base_component_var_register(&mca_coll_self_component.collm_version,
                                           "priority", NULL, MCA_BASE_VAR_TYPE_INT,
                                           NULL, 0, 0, OPAL_INFO_LVL_9,
                                           MCA_BASE_VAR_SCOPE_READONLY,
                                           &ompi_coll_self_priority);

    return OMPI_SUCCESS;
}


OBJ_CLASS_INSTANCE(mca_coll_self_module_t,
                   mca_coll_base_module_t,
                   NULL, NULL);
