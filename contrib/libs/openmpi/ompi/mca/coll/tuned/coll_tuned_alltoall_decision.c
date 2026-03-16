/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2017 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include "mpi.h"
#include "ompi/constants.h"
#include "ompi/datatype/ompi_datatype.h"
#include "ompi/communicator/communicator.h"
#include "ompi/mca/coll/coll.h"
#include "ompi/mca/coll/base/coll_tags.h"
#include "ompi/mca/pml/pml.h"
#include "coll_tuned.h"
#include "ompi/mca/coll/base/coll_base_topo.h"
#include "ompi/mca/coll/base/coll_base_util.h"

/* alltoall algorithm variables */
static int coll_tuned_alltoall_forced_algorithm = 0;
static int coll_tuned_alltoall_segment_size = 0;
static int coll_tuned_alltoall_max_requests;
static int coll_tuned_alltoall_tree_fanout;
static int coll_tuned_alltoall_chain_fanout;

/* valid values for coll_tuned_alltoall_forced_algorithm */
static mca_base_var_enum_value_t alltoall_algorithms[] = {
    {0, "ignore"},
    {1, "linear"},
    {2, "pairwise"},
    {3, "modified_bruck"},
    {4, "linear_sync"},
    {5, "two_proc"},
    {0, NULL}
};

/* The following are used by dynamic and forced rules */

/* publish details of each algorithm and if its forced/fixed/locked in */
/* as you add methods/algorithms you must update this and the query/map routines */

/* this routine is called by the component only */
/* this makes sure that the mca parameters are set to their initial values and perms */
/* module does not call this they call the forced_getvalues routine instead */

int ompi_coll_tuned_alltoall_intra_check_forced_init (coll_tuned_force_algorithm_mca_param_indices_t *mca_param_indices)
{
    mca_base_var_enum_t*new_enum;
    int cnt;

    for( cnt = 0; NULL != alltoall_algorithms[cnt].string; cnt++ );
    ompi_coll_tuned_forced_max_algorithms[ALLTOALL] = cnt;

    (void) mca_base_component_var_register(&mca_coll_tuned_component.super.collm_version,
                                           "alltoall_algorithm_count",
                                           "Number of alltoall algorithms available",
                                           MCA_BASE_VAR_TYPE_INT, NULL, 0,
                                           MCA_BASE_VAR_FLAG_DEFAULT_ONLY,
                                           OPAL_INFO_LVL_5,
                                           MCA_BASE_VAR_SCOPE_CONSTANT,
                                           &ompi_coll_tuned_forced_max_algorithms[ALLTOALL]);

    /* MPI_T: This variable should eventually be bound to a communicator */
    coll_tuned_alltoall_forced_algorithm = 0;
    (void) mca_base_var_enum_create("coll_tuned_alltoall_algorithms", alltoall_algorithms, &new_enum);
    mca_param_indices->algorithm_param_index =
        mca_base_component_var_register(&mca_coll_tuned_component.super.collm_version,
                                        "alltoall_algorithm",
                                        "Which alltoall algorithm is used. Can be locked down to choice of: 0 ignore, 1 basic linear, 2 pairwise, 3: modified bruck, 4: linear with sync, 5:two proc only.",
                                        MCA_BASE_VAR_TYPE_INT, new_enum, 0, MCA_BASE_VAR_FLAG_SETTABLE,
                                        OPAL_INFO_LVL_5,
                                        MCA_BASE_VAR_SCOPE_ALL,
                                        &coll_tuned_alltoall_forced_algorithm);
    OBJ_RELEASE(new_enum);
    if (mca_param_indices->algorithm_param_index < 0) {
        return mca_param_indices->algorithm_param_index;
    }

    coll_tuned_alltoall_segment_size = 0;
    mca_param_indices->segsize_param_index =
        mca_base_component_var_register(&mca_coll_tuned_component.super.collm_version,
                                        "alltoall_algorithm_segmentsize",
                                        "Segment size in bytes used by default for alltoall algorithms. Only has meaning if algorithm is forced and supports segmenting. 0 bytes means no segmentation.",
                                        MCA_BASE_VAR_TYPE_INT, NULL, 0, MCA_BASE_VAR_FLAG_SETTABLE,
                                        OPAL_INFO_LVL_5,
                                        MCA_BASE_VAR_SCOPE_ALL,
                                        &coll_tuned_alltoall_segment_size);

    coll_tuned_alltoall_tree_fanout = ompi_coll_tuned_init_tree_fanout; /* get system wide default */
    mca_param_indices->tree_fanout_param_index =
        mca_base_component_var_register(&mca_coll_tuned_component.super.collm_version,
                                        "alltoall_algorithm_tree_fanout",
                                        "Fanout for n-tree used for alltoall algorithms. Only has meaning if algorithm is forced and supports n-tree topo based operation.",
                                        MCA_BASE_VAR_TYPE_INT, NULL, 0, MCA_BASE_VAR_FLAG_SETTABLE,
                                        OPAL_INFO_LVL_5,
                                        MCA_BASE_VAR_SCOPE_ALL,
                                        &coll_tuned_alltoall_tree_fanout);

    coll_tuned_alltoall_chain_fanout = ompi_coll_tuned_init_chain_fanout; /* get system wide default */
    mca_param_indices->chain_fanout_param_index =
      mca_base_component_var_register(&mca_coll_tuned_component.super.collm_version,
                                      "alltoall_algorithm_chain_fanout",
                                      "Fanout for chains used for alltoall algorithms. Only has meaning if algorithm is forced and supports chain topo based operation.",
                                      MCA_BASE_VAR_TYPE_INT, NULL, 0, MCA_BASE_VAR_FLAG_SETTABLE,
                                      OPAL_INFO_LVL_5,
                                      MCA_BASE_VAR_SCOPE_ALL,
                                      &coll_tuned_alltoall_chain_fanout);

    coll_tuned_alltoall_max_requests = 0; /* no limit for alltoall by default */
    mca_param_indices->max_requests_param_index =
      mca_base_component_var_register(&mca_coll_tuned_component.super.collm_version,
                                      "alltoall_algorithm_max_requests",
                                      "Maximum number of outstanding send or recv requests.  Only has meaning for synchronized algorithms.",
                                      MCA_BASE_VAR_TYPE_INT, NULL, 0, MCA_BASE_VAR_FLAG_SETTABLE,
                                      OPAL_INFO_LVL_5,
                                      MCA_BASE_VAR_SCOPE_ALL,
                                      &coll_tuned_alltoall_max_requests);
    if (mca_param_indices->max_requests_param_index < 0) {
        return mca_param_indices->max_requests_param_index;
    }

    if (coll_tuned_alltoall_max_requests < 0) {
        if( 0 == ompi_comm_rank( MPI_COMM_WORLD ) ) {
            opal_output( 0, "Maximum outstanding requests must be positive number greater than 1.  Switching to system level default %d \n",
                         ompi_coll_tuned_init_max_requests );
        }
        coll_tuned_alltoall_max_requests = 0;
    }

    return (MPI_SUCCESS);
}

int ompi_coll_tuned_alltoall_intra_do_this(const void *sbuf, int scount,
                                           struct ompi_datatype_t *sdtype,
                                           void* rbuf, int rcount,
                                           struct ompi_datatype_t *rdtype,
                                           struct ompi_communicator_t *comm,
                                           mca_coll_base_module_t *module,
                                           int algorithm, int faninout, int segsize,
                                           int max_requests)
{
    OPAL_OUTPUT((ompi_coll_tuned_stream,"coll:tuned:alltoall_intra_do_this selected algorithm %d topo faninout %d segsize %d",
                 algorithm, faninout, segsize));

    switch (algorithm) {
    case (0):
        return ompi_coll_tuned_alltoall_intra_dec_fixed(sbuf, scount, sdtype, rbuf, rcount, rdtype, comm, module);
    case (1):
        return ompi_coll_base_alltoall_intra_basic_linear(sbuf, scount, sdtype, rbuf, rcount, rdtype, comm, module);
    case (2):
        return ompi_coll_base_alltoall_intra_pairwise(sbuf, scount, sdtype, rbuf, rcount, rdtype, comm, module);
    case (3):
        return ompi_coll_base_alltoall_intra_bruck(sbuf, scount, sdtype, rbuf, rcount, rdtype, comm, module);
    case (4):
        return ompi_coll_base_alltoall_intra_linear_sync(sbuf, scount, sdtype, rbuf, rcount, rdtype, comm, module, max_requests);
    case (5):
        return ompi_coll_base_alltoall_intra_two_procs(sbuf, scount, sdtype, rbuf, rcount, rdtype, comm, module);
    } /* switch */
    OPAL_OUTPUT((ompi_coll_tuned_stream,"coll:tuned:alltoall_intra_do_this attempt to select algorithm %d when only 0-%d is valid?",
                 algorithm, ompi_coll_tuned_forced_max_algorithms[ALLTOALL]));
    return (MPI_ERR_ARG);
}
