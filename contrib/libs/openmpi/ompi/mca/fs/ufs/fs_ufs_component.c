/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2008-2011 University of Houston. All rights reserved.
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
#include "fs_ufs.h"
#include "mpi.h"

int mca_fs_ufs_priority = 10;
int mca_fs_ufs_lock_algorithm=0; /* auto */
/*
 * Private functions
 */
static int register_component(void);


/*
 * Public string showing the fs ufs component version number
 */
const char *mca_fs_ufs_component_version_string =
  "OMPI/MPI ufs FS MCA component version " OMPI_VERSION;

/*
 * Instantiate the public struct with all of our public information
 * and pointers to our public functions in it
 */
mca_fs_base_component_2_0_0_t mca_fs_ufs_component = {

    /* First, the mca_component_t struct containing meta information
       about the component itself */

    .fsm_version = {
        MCA_FS_BASE_VERSION_2_0_0,

        /* Component name and version */
        .mca_component_name = "ufs",
        MCA_BASE_MAKE_VERSION(component, OMPI_MAJOR_VERSION, OMPI_MINOR_VERSION,
                              OMPI_RELEASE_VERSION),
        .mca_register_component_params = register_component,
    },
    .fsm_data = {
        /* This component is checkpointable */
      MCA_BASE_METADATA_PARAM_CHECKPOINT
    },
    .fsm_init_query = mca_fs_ufs_component_init_query,      /* get thread level */
    .fsm_file_query = mca_fs_ufs_component_file_query,      /* get priority and actions */
    .fsm_file_unquery = mca_fs_ufs_component_file_unquery,  /* undo what was done by previous function */
};

static int register_component(void)
{
    mca_fs_ufs_priority = 10;
    (void) mca_base_component_var_register(&mca_fs_ufs_component.fsm_version,
                                           "priority", "Priority of the fs ufs component",
                                           MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                           OPAL_INFO_LVL_9,
                                           MCA_BASE_VAR_SCOPE_READONLY,
                                           &mca_fs_ufs_priority);

    mca_fs_ufs_lock_algorithm = 0;
    (void) mca_base_component_var_register(&mca_fs_ufs_component.fsm_version,
                                           "lock_algorithm", "Locking algorithm used by the fs ufs component. "
                                           " 0: auto (default), 1: skip locking, 2: always lock entire file, "
                                           "3: lock only specific ranges",
                                           MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                           OPAL_INFO_LVL_9,
                                           MCA_BASE_VAR_SCOPE_READONLY,
                                           &mca_fs_ufs_lock_algorithm );

    return OMPI_SUCCESS;
}
