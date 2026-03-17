/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2017 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2015-2018 Research Organization for Information Science
 *                         and Technology (RIST).  All rights reserved.
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
#include "ompi/mca/coll/base/coll_base_topo.h"
#include "ompi/mca/coll/base/coll_tags.h"
#include "ompi/mca/pml/pml.h"
#include "ompi/op/op.h"
#include "coll_tuned.h"

/* reduce algorithm variables */
static int coll_tuned_reduce_forced_algorithm = 0;
static int coll_tuned_reduce_segment_size = 0;
static int coll_tuned_reduce_max_requests;
static int coll_tuned_reduce_tree_fanout;
static int coll_tuned_reduce_chain_fanout;

/* valid values for coll_tuned_reduce_forced_algorithm */
static mca_base_var_enum_value_t reduce_algorithms[] = {
    {0, "ignore"},
    {1, "linear"},
    {2, "chain"},
    {3, "pipeline"},
    {4, "binary"},
    {5, "binomial"},
    {6, "in-order_binary"},
    {7, "rabenseifner"},
    {0, NULL}
};

/**
 * The following are used by dynamic and forced rules
 *
 * publish details of each algorithm and if its forced/fixed/locked in
 * as you add methods/algorithms you must update this and the query/map routines
 *
 * this routine is called by the component only
 * this makes sure that the mca parameters are set to their initial values and
 * perms module does not call this they call the forced_getvalues routine
 * instead.
 */

int ompi_coll_tuned_reduce_intra_check_forced_init (coll_tuned_force_algorithm_mca_param_indices_t *mca_param_indices)
{
    mca_base_var_enum_t*new_enum;
    int cnt;

    for( cnt = 0; NULL != reduce_algorithms[cnt].string; cnt++ );
    ompi_coll_tuned_forced_max_algorithms[REDUCE] = cnt;

    (void) mca_base_component_var_register(&mca_coll_tuned_component.super.collm_version,
                                           "reduce_algorithm_count",
                                           "Number of reduce algorithms available",
                                           MCA_BASE_VAR_TYPE_INT, NULL, 0,
                                           MCA_BASE_VAR_FLAG_DEFAULT_ONLY,
                                           OPAL_INFO_LVL_5,
                                           MCA_BASE_VAR_SCOPE_CONSTANT,
                                           &ompi_coll_tuned_forced_max_algorithms[REDUCE]);

    /* MPI_T: This variable should eventually be bound to a communicator */
    coll_tuned_reduce_forced_algorithm = 0;
    (void) mca_base_var_enum_create("coll_tuned_reduce_algorithms", reduce_algorithms, &new_enum);
    mca_param_indices->algorithm_param_index =
        mca_base_component_var_register(&mca_coll_tuned_component.super.collm_version,
                                        "reduce_algorithm",
                                        "Which reduce algorithm is used. Can be locked down to choice of: 0 ignore, 1 linear, 2 chain, 3 pipeline, 4 binary, 5 binomial, 6 in-order binary, 7 rabenseifner",
                                        MCA_BASE_VAR_TYPE_INT, new_enum, 0, MCA_BASE_VAR_FLAG_SETTABLE,
                                        OPAL_INFO_LVL_5,
                                        MCA_BASE_VAR_SCOPE_ALL,
                                        &coll_tuned_reduce_forced_algorithm);
    OBJ_RELEASE(new_enum);
    if (mca_param_indices->algorithm_param_index < 0) {
        return mca_param_indices->algorithm_param_index;
    }

    coll_tuned_reduce_segment_size = 0;
    mca_param_indices->segsize_param_index =
        mca_base_component_var_register(&mca_coll_tuned_component.super.collm_version,
                                        "reduce_algorithm_segmentsize",
                                        "Segment size in bytes used by default for reduce algorithms. Only has meaning if algorithm is forced and supports segmenting. 0 bytes means no segmentation.",
                                        MCA_BASE_VAR_TYPE_INT, NULL, 0, MCA_BASE_VAR_FLAG_SETTABLE,
                                        OPAL_INFO_LVL_5,
                                        MCA_BASE_VAR_SCOPE_ALL,
                                        &coll_tuned_reduce_segment_size);

    coll_tuned_reduce_tree_fanout = ompi_coll_tuned_init_tree_fanout; /* get system wide default */
    mca_param_indices->tree_fanout_param_index =
        mca_base_component_var_register(&mca_coll_tuned_component.super.collm_version,
                                        "reduce_algorithm_tree_fanout",
                                        "Fanout for n-tree used for reduce algorithms. Only has meaning if algorithm is forced and supports n-tree topo based operation.",
                                        MCA_BASE_VAR_TYPE_INT, NULL, 0, MCA_BASE_VAR_FLAG_SETTABLE,
                                        OPAL_INFO_LVL_5,
                                        MCA_BASE_VAR_SCOPE_ALL,
                                        &coll_tuned_reduce_tree_fanout);

    coll_tuned_reduce_chain_fanout = ompi_coll_tuned_init_chain_fanout; /* get system wide default */
    mca_param_indices->chain_fanout_param_index =
      mca_base_component_var_register(&mca_coll_tuned_component.super.collm_version,
                                      "reduce_algorithm_chain_fanout",
                                      "Fanout for chains used for reduce algorithms. Only has meaning if algorithm is forced and supports chain topo based operation.",
                                      MCA_BASE_VAR_TYPE_INT, NULL, 0, MCA_BASE_VAR_FLAG_SETTABLE,
                                      OPAL_INFO_LVL_5,
                                      MCA_BASE_VAR_SCOPE_ALL,
                                      &coll_tuned_reduce_chain_fanout);

    coll_tuned_reduce_max_requests = 0; /* no limit for reduce by default */
    mca_param_indices->max_requests_param_index =
      mca_base_component_var_register(&mca_coll_tuned_component.super.collm_version,
                                      "reduce_algorithm_max_requests",
                                      "Maximum number of outstanding send requests on leaf nodes. 0 means no limit.",
                                      MCA_BASE_VAR_TYPE_INT, NULL, 0, MCA_BASE_VAR_FLAG_SETTABLE,
                                      OPAL_INFO_LVL_5,
                                      MCA_BASE_VAR_SCOPE_ALL,
                                      &coll_tuned_reduce_max_requests);
    if (mca_param_indices->max_requests_param_index < 0) {
        return mca_param_indices->max_requests_param_index;
    }

    if (coll_tuned_reduce_max_requests < 0) {
        if( 0 == ompi_comm_rank( MPI_COMM_WORLD ) ) {
            opal_output( 0, "Maximum outstanding requests must be positive number or 0.  Initializing to 0 (no limit).\n" );
        }
        coll_tuned_reduce_max_requests = 0;
    }

    return (MPI_SUCCESS);
}

int ompi_coll_tuned_reduce_intra_do_this(const void *sbuf, void* rbuf, int count,
                                         struct ompi_datatype_t *dtype,
                                         struct ompi_op_t *op, int root,
                                         struct ompi_communicator_t *comm,
                                         mca_coll_base_module_t *module,
                                         int algorithm, int faninout,
                                         int segsize, int max_requests )
{
    OPAL_OUTPUT((ompi_coll_tuned_stream,"coll:tuned:reduce_intra_do_this selected algorithm %d topo faninout %d segsize %d",
                 algorithm, faninout, segsize));

    switch (algorithm) {
    case (0):  return ompi_coll_tuned_reduce_intra_dec_fixed(sbuf, rbuf, count, dtype,
                                                             op, root, comm, module);
    case (1):  return ompi_coll_base_reduce_intra_basic_linear(sbuf, rbuf, count, dtype,
                                                               op, root, comm, module);
    case (2):  return ompi_coll_base_reduce_intra_chain(sbuf, rbuf, count, dtype,
                                                        op, root, comm, module,
                                                        segsize, faninout, max_requests);
    case (3):  return ompi_coll_base_reduce_intra_pipeline(sbuf, rbuf, count, dtype,
                                                           op, root, comm, module,
                                                           segsize, max_requests);
    case (4):  return ompi_coll_base_reduce_intra_binary(sbuf, rbuf, count, dtype,
                                                         op, root, comm, module,
                                                         segsize, max_requests);
    case (5):  return ompi_coll_base_reduce_intra_binomial(sbuf, rbuf, count, dtype,
                                                           op, root, comm, module,
                                                           segsize, max_requests);
    case (6):  return ompi_coll_base_reduce_intra_in_order_binary(sbuf, rbuf, count, dtype,
                                                                  op, root, comm, module,
                                                                  segsize, max_requests);
    case (7):  return ompi_coll_base_reduce_intra_redscat_gather(sbuf, rbuf, count, dtype,
                                                                  op, root, comm, module);
    } /* switch */
    OPAL_OUTPUT((ompi_coll_tuned_stream,"coll:tuned:reduce_intra_do_this attempt to select algorithm %d when only 0-%d is valid?",
                 algorithm, ompi_coll_tuned_forced_max_algorithms[REDUCE]));
    return (MPI_ERR_ARG);
}
