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
 * Copyright (c) 2008-2009 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2011-2015 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2015      Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/**
 * @file
 *
 * Most of the description of the data layout is in the
 * coll_sm_module.c file.
 */

#include "ompi_config.h"

#include "opal/util/show_help.h"
#include "ompi/constants.h"
#include "ompi/mca/coll/coll.h"
#include "coll_sm.h"


/*
 * Public string showing the coll ompi_sm component version number
 */
const char *mca_coll_sm_component_version_string =
    "Open MPI sm collective MCA component version " OMPI_VERSION;


/*
 * Local functions
 */
static int sm_close(void);
static int sm_register(void);

static int coll_sm_shared_mem_used_data;

/*
 * Instantiate the public struct with all of our public information
 * and pointers to our public functions in it
 */

mca_coll_sm_component_t mca_coll_sm_component = {

    /* First, fill in the super */

    {
        /* First, the mca_component_t struct containing meta
           information about the component itself */

        .collm_version = {
            MCA_COLL_BASE_VERSION_2_0_0,

            /* Component name and version */
            .mca_component_name = "sm",
            MCA_BASE_MAKE_VERSION(component, OMPI_MAJOR_VERSION, OMPI_MINOR_VERSION,
                                  OMPI_RELEASE_VERSION),

            /* Component functions */
            .mca_close_component = sm_close,
            .mca_register_component_params = sm_register,
        },
        .collm_data = {
            /* The component is not checkpoint ready */
            MCA_BASE_METADATA_PARAM_NONE
        },

        /* Initialization / querying functions */

        .collm_init_query = mca_coll_sm_init_query,
        .collm_comm_query = mca_coll_sm_comm_query,
    },

    /* sm-component specifc information */

    /* (default) priority */
    /* JMS temporarily lowered until we can get more testing */
    0,

    /* (default) control size (bytes) */
    4096,

    /* (default) number of "in use" flags for each communicator's area
       in the per-communicator shmem segment */
    2,

    /* (default) number of segments for each communicator in the
       per-communicator shmem segment */
    8,

    /* (default) fragment size */
    8192,

    /* (default) degree of tree for tree-based operations (must be <=
       control unit size) */
    4,

    /* (default) number of processes in coll_sm_shared_mem_size
       information variable */
    4,

    /* default values for non-MCA parameters */
    /* Not specifying values here gives us all 0's */
};


/*
 * Shut down the component
 */
static int sm_close(void)
{
    return OMPI_SUCCESS;
}

static int sm_verify_mca_variables(void)
{
    mca_coll_sm_component_t *cs = &mca_coll_sm_component;

    if (0 != (cs->sm_fragment_size % cs->sm_control_size)) {
        cs->sm_fragment_size += cs->sm_control_size -
            (cs->sm_fragment_size % cs->sm_control_size);
    }

    if (cs->sm_comm_num_in_use_flags < 2) {
        cs->sm_comm_num_in_use_flags = 2;
    }

    if (cs->sm_comm_num_segments < cs->sm_comm_num_in_use_flags) {
        cs->sm_comm_num_segments = cs->sm_comm_num_in_use_flags;
    }
    if (0 != (cs->sm_comm_num_segments % cs->sm_comm_num_in_use_flags)) {
        cs->sm_comm_num_segments += cs->sm_comm_num_in_use_flags -
            (cs->sm_comm_num_segments % cs->sm_comm_num_in_use_flags);
    }
    cs->sm_segs_per_inuse_flag =
        cs->sm_comm_num_segments / cs->sm_comm_num_in_use_flags;

    if (cs->sm_tree_degree > cs->sm_control_size) {
        opal_show_help("help-mpi-coll-sm.txt",
                       "tree-degree-larger-than-control", true,
                       cs->sm_tree_degree, cs->sm_control_size);
        cs->sm_tree_degree = cs->sm_control_size;
    }
    if (cs->sm_tree_degree > 255) {
        opal_show_help("help-mpi-coll-sm.txt",
                       "tree-degree-larger-than-255", true,
                       cs->sm_tree_degree);
        cs->sm_tree_degree = 255;
    }

    coll_sm_shared_mem_used_data = (int)(4 * cs->sm_control_size +
        (cs->sm_comm_num_in_use_flags * cs->sm_control_size) +
        (cs->sm_comm_num_segments * (cs->sm_info_comm_size * cs->sm_control_size * 2)) +
        (cs->sm_comm_num_segments * (cs->sm_info_comm_size * cs->sm_fragment_size)));

    return OMPI_SUCCESS;
}

/*
 * Register MCA params
 */
static int sm_register(void)
{
    mca_base_component_t *c = &mca_coll_sm_component.super.collm_version;
    mca_coll_sm_component_t *cs = &mca_coll_sm_component;

    /* If we want to be selected (i.e., all procs on one node), then
       we should have a high priority */

    cs->sm_priority = 0;
    (void) mca_base_component_var_register(c, "priority", "Priority of the sm coll component",
                                           MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                           OPAL_INFO_LVL_9,
                                           MCA_BASE_VAR_SCOPE_READONLY,
                                           &cs->sm_priority);

    cs->sm_control_size = 4096;
    (void) mca_base_component_var_register(c, "control_size",
                                           "Length of the control data -- should usually be either the length of a cache line on most SMPs, or the size of a page on machines that support direct memory affinity page placement (in bytes)",
                                           MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                           OPAL_INFO_LVL_9,
                                           MCA_BASE_VAR_SCOPE_READONLY,
                                           &cs->sm_control_size);

    cs->sm_fragment_size = 8192;
    (void) mca_base_component_var_register(c, "fragment_size",
                                           "Fragment size (in bytes) used for passing data through shared memory (will be rounded up to the nearest control_size size)",
                                           MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                           OPAL_INFO_LVL_9,
                                           MCA_BASE_VAR_SCOPE_READONLY,
                                           &cs->sm_fragment_size);

    cs->sm_comm_num_in_use_flags = 2;
    (void) mca_base_component_var_register(c, "comm_in_use_flags",
                                           "Number of \"in use\" flags, used to mark a message passing area segment as currently being used or not (must be >= 2 and <= comm_num_segments)",
                                           MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                           OPAL_INFO_LVL_9,
                                           MCA_BASE_VAR_SCOPE_READONLY,
                                           &cs->sm_comm_num_in_use_flags);

    cs->sm_comm_num_segments = 8;
    (void) mca_base_component_var_register(c, "comm_num_segments",
                                           "Number of segments in each communicator's shared memory message passing area (must be >= 2, and must be a multiple of comm_in_use_flags)",
                                           MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                           OPAL_INFO_LVL_9,
                                           MCA_BASE_VAR_SCOPE_READONLY,
                                           &cs->sm_comm_num_segments);

    cs->sm_tree_degree = 4;
    (void) mca_base_component_var_register(c, "tree_degree",
                                           "Degree of the tree for tree-based operations (must be => 1 and <= min(control_size, 255))",
                                           MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                           OPAL_INFO_LVL_9,
                                           MCA_BASE_VAR_SCOPE_READONLY,
                                           &cs->sm_tree_degree);

    /* INFO: Calculate how much space we need in the per-communicator
       shmem data segment.  This formula taken directly from
       coll_sm_module.c. */
    cs->sm_info_comm_size = 4;
    (void) mca_base_component_var_register(c, "info_num_procs",
                                           "Number of processes to use for the calculation of the shared_mem_size MCA information parameter (must be => 2)",
                                           MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                           OPAL_INFO_LVL_9,
                                           MCA_BASE_VAR_SCOPE_READONLY,
                                           &cs->sm_info_comm_size);

    coll_sm_shared_mem_used_data = (int)(4 * cs->sm_control_size +
        (cs->sm_comm_num_in_use_flags * cs->sm_control_size) +
        (cs->sm_comm_num_segments * (cs->sm_info_comm_size * cs->sm_control_size * 2)) +
        (cs->sm_comm_num_segments * (cs->sm_info_comm_size * cs->sm_fragment_size)));

    (void) mca_base_component_var_register(c, "shared_mem_used_data",
                                           "Amount of shared memory used, per communicator, in the shared memory data area for info_num_procs processes (in bytes)",
                                           MCA_BASE_VAR_TYPE_INT, NULL, 0,
                                           MCA_BASE_VAR_FLAG_DEFAULT_ONLY,
                                           OPAL_INFO_LVL_9,
                                           MCA_BASE_VAR_SCOPE_READONLY,
                                           &coll_sm_shared_mem_used_data);

    return sm_verify_mca_variables();
}
