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

/* bcast algorithm variables */
static int coll_tuned_bcast_forced_algorithm = 0;
static int coll_tuned_bcast_segment_size = 0;
static int coll_tuned_bcast_tree_fanout;
static int coll_tuned_bcast_chain_fanout;
/* k-nomial tree radix for the bcast algorithm (>= 2) */
static int coll_tuned_bcast_knomial_radix = 4;

/* valid values for coll_tuned_bcast_forced_algorithm */
static mca_base_var_enum_value_t bcast_algorithms[] = {
    {0, "ignore"},
    {1, "basic_linear"},
    {2, "chain"},
    {3, "pipeline"},
    {4, "split_binary_tree"},
    {5, "binary_tree"},
    {6, "binomial"},
    {7, "knomial"},
    {8, "scatter_allgather"},
    {9, "scatter_allgather_ring"},
    {0, NULL}
};

/* The following are used by dynamic and forced rules */

/* publish details of each algorithm and if its forced/fixed/locked in */
/* as you add methods/algorithms you must update this and the query/map routines */

/* this routine is called by the component only */
/* this makes sure that the mca parameters are set to their initial values and perms */
/* module does not call this they call the forced_getvalues routine instead */

int ompi_coll_tuned_bcast_intra_check_forced_init (coll_tuned_force_algorithm_mca_param_indices_t *mca_param_indices)
{
    mca_base_var_enum_t *new_enum;
    int cnt;

    for( cnt = 0; NULL != bcast_algorithms[cnt].string; cnt++ );
    ompi_coll_tuned_forced_max_algorithms[BCAST] = cnt;

    (void) mca_base_component_var_register(&mca_coll_tuned_component.super.collm_version,
                                           "bcast_algorithm_count",
                                           "Number of bcast algorithms available",
                                           MCA_BASE_VAR_TYPE_INT, NULL, 0,
                                           MCA_BASE_VAR_FLAG_DEFAULT_ONLY,
                                           OPAL_INFO_LVL_5,
                                           MCA_BASE_VAR_SCOPE_CONSTANT,
                                           &ompi_coll_tuned_forced_max_algorithms[BCAST]);

    /* MPI_T: This variable should eventually be bound to a communicator */
    coll_tuned_bcast_forced_algorithm = 0;
    (void) mca_base_var_enum_create("coll_tuned_bcast_algorithms", bcast_algorithms, &new_enum);
    mca_param_indices->algorithm_param_index =
        mca_base_component_var_register(&mca_coll_tuned_component.super.collm_version,
                                        "bcast_algorithm",
                                        "Which bcast algorithm is used. Can be locked down to choice of: 0 ignore, 1 basic linear, 2 chain, 3: pipeline, 4: split binary tree, 5: binary tree, 6: binomial tree, 7: knomial tree, 8: scatter_allgather, 9: scatter_allgather_ring.",
                                        MCA_BASE_VAR_TYPE_INT, new_enum, 0, MCA_BASE_VAR_FLAG_SETTABLE,
                                        OPAL_INFO_LVL_5,
                                        MCA_BASE_VAR_SCOPE_ALL,
                                        &coll_tuned_bcast_forced_algorithm);
    OBJ_RELEASE(new_enum);
    if (mca_param_indices->algorithm_param_index < 0) {
        return mca_param_indices->algorithm_param_index;
    }

    coll_tuned_bcast_segment_size = 0;
    mca_param_indices->segsize_param_index =
        mca_base_component_var_register(&mca_coll_tuned_component.super.collm_version,
                                        "bcast_algorithm_segmentsize",
                                        "Segment size in bytes used by default for bcast algorithms. Only has meaning if algorithm is forced and supports segmenting. 0 bytes means no segmentation.",
                                        MCA_BASE_VAR_TYPE_INT, NULL, 0, MCA_BASE_VAR_FLAG_SETTABLE,
                                        OPAL_INFO_LVL_5,
                                        MCA_BASE_VAR_SCOPE_ALL,
                                        &coll_tuned_bcast_segment_size);

    coll_tuned_bcast_tree_fanout = ompi_coll_tuned_init_tree_fanout; /* get system wide default */
    mca_param_indices->tree_fanout_param_index =
        mca_base_component_var_register(&mca_coll_tuned_component.super.collm_version,
                                        "bcast_algorithm_tree_fanout",
                                        "Fanout for n-tree used for bcast algorithms. Only has meaning if algorithm is forced and supports n-tree topo based operation.",
                                        MCA_BASE_VAR_TYPE_INT, NULL, 0, MCA_BASE_VAR_FLAG_SETTABLE,
                                        OPAL_INFO_LVL_5,
                                        MCA_BASE_VAR_SCOPE_ALL,
                                        &coll_tuned_bcast_tree_fanout);

    coll_tuned_bcast_chain_fanout = ompi_coll_tuned_init_chain_fanout; /* get system wide default */
    mca_param_indices->chain_fanout_param_index =
      mca_base_component_var_register(&mca_coll_tuned_component.super.collm_version,
                                      "bcast_algorithm_chain_fanout",
                                      "Fanout for chains used for bcast algorithms. Only has meaning if algorithm is forced and supports chain topo based operation.",
                                      MCA_BASE_VAR_TYPE_INT, NULL, 0, MCA_BASE_VAR_FLAG_SETTABLE,
                                      OPAL_INFO_LVL_5,
                                      MCA_BASE_VAR_SCOPE_ALL,
                                      &coll_tuned_bcast_chain_fanout);

    coll_tuned_bcast_knomial_radix = 4;
    mca_base_component_var_register(&mca_coll_tuned_component.super.collm_version,
                                    "bcast_algorithm_knomial_radix",
                                    "k-nomial tree radix for the bcast algorithm (radix > 1).",
                                    MCA_BASE_VAR_TYPE_INT, NULL, 0, MCA_BASE_VAR_FLAG_SETTABLE,
                                    OPAL_INFO_LVL_5, MCA_BASE_VAR_SCOPE_ALL,
                                    &coll_tuned_bcast_knomial_radix);

    return (MPI_SUCCESS);
}

int ompi_coll_tuned_bcast_intra_do_this(void *buf, int count,
                                        struct ompi_datatype_t *dtype,
                                        int root,
                                        struct ompi_communicator_t *comm,
                                        mca_coll_base_module_t *module,
                                        int algorithm, int faninout, int segsize)
{
    OPAL_OUTPUT((ompi_coll_tuned_stream,"coll:tuned:bcast_intra_do_this algorithm %d topo faninout %d segsize %d",
                 algorithm, faninout, segsize));

    switch (algorithm) {
    case (0):
        return ompi_coll_tuned_bcast_intra_dec_fixed( buf, count, dtype, root, comm, module );
    case (1):
        return ompi_coll_base_bcast_intra_basic_linear( buf, count, dtype, root, comm, module );
    case (2):
        return ompi_coll_base_bcast_intra_chain( buf, count, dtype, root, comm, module, segsize, faninout );
    case (3):
        return ompi_coll_base_bcast_intra_pipeline( buf, count, dtype, root, comm, module, segsize );
    case (4):
        return ompi_coll_base_bcast_intra_split_bintree( buf, count, dtype, root, comm, module, segsize );
    case (5):
        return ompi_coll_base_bcast_intra_bintree( buf, count, dtype, root, comm, module, segsize );
    case (6):
        return ompi_coll_base_bcast_intra_binomial( buf, count, dtype, root, comm, module, segsize );
    case (7):
        return ompi_coll_base_bcast_intra_knomial(buf, count, dtype, root, comm, module,
                                                  segsize, coll_tuned_bcast_knomial_radix);
    case (8):
        return ompi_coll_base_bcast_intra_scatter_allgather(buf, count, dtype, root, comm, module, segsize);
    case (9):
        return ompi_coll_base_bcast_intra_scatter_allgather_ring(buf, count, dtype, root, comm, module, segsize);
    } /* switch */
    OPAL_OUTPUT((ompi_coll_tuned_stream,"coll:tuned:bcast_intra_do_this attempt to select algorithm %d when only 0-%d is valid?",
                 algorithm, ompi_coll_tuned_forced_max_algorithms[BCAST]));
    return (MPI_ERR_ARG);
}
