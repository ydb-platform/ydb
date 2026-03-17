/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2017 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2015-2017 Research Organization for Information Science
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

/* gather algorithm variables */
static int coll_tuned_gather_forced_algorithm = 0;
static int coll_tuned_gather_segment_size = 0;
static int coll_tuned_gather_tree_fanout;
static int coll_tuned_gather_chain_fanout;

/* valid values for coll_tuned_gather_forced_algorithm */
static mca_base_var_enum_value_t gather_algorithms[] = {
    {0, "ignore"},
    {1, "basic_linear"},
    {2, "binomial"},
    {3, "linear_sync"},
    {0, NULL}
};

/* The following are used by dynamic and forced rules */

/* publish details of each algorithm and if its forced/fixed/locked in */
/* as you add methods/algorithms you must update this and the query/map
   routines */

/* this routine is called by the component only */
/* this makes sure that the mca parameters are set to their initial values
   and perms */
/* module does not call this they call the forced_getvalues routine instead */

int
ompi_coll_tuned_gather_intra_check_forced_init(coll_tuned_force_algorithm_mca_param_indices_t *mca_param_indices)
{
    mca_base_var_enum_t *new_enum;
    int cnt;

    for( cnt = 0; NULL != gather_algorithms[cnt].string; cnt++ );
    ompi_coll_tuned_forced_max_algorithms[GATHER] = cnt;

    (void) mca_base_component_var_register(&mca_coll_tuned_component.super.collm_version,
                                           "gather_algorithm_count",
                                           "Number of gather algorithms available",
                                           MCA_BASE_VAR_TYPE_INT, NULL, 0,
                                           MCA_BASE_VAR_FLAG_DEFAULT_ONLY,
                                           OPAL_INFO_LVL_5,
                                           MCA_BASE_VAR_SCOPE_CONSTANT,
                                           &ompi_coll_tuned_forced_max_algorithms[GATHER]);

    /* MPI_T: This variable should eventually be bound to a communicator */
    coll_tuned_gather_forced_algorithm = 0;
    (void) mca_base_var_enum_create("coll_tuned_gather_algorithms", gather_algorithms, &new_enum);
    mca_param_indices->algorithm_param_index =
        mca_base_component_var_register(&mca_coll_tuned_component.super.collm_version,
                                        "gather_algorithm",
                                        "Which gather algorithm is used. Can be locked down to choice of: 0 ignore, 1 basic linear, 2 binomial, 3 linear with synchronization.",
                                        MCA_BASE_VAR_TYPE_INT, new_enum, 0, MCA_BASE_VAR_FLAG_SETTABLE,
                                        OPAL_INFO_LVL_5,
                                        MCA_BASE_VAR_SCOPE_ALL,
                                        &coll_tuned_gather_forced_algorithm);
    OBJ_RELEASE(new_enum);
    if (mca_param_indices->algorithm_param_index < 0) {
        return mca_param_indices->algorithm_param_index;
    }

    coll_tuned_gather_segment_size = 0;
    mca_param_indices->segsize_param_index =
        mca_base_component_var_register(&mca_coll_tuned_component.super.collm_version,
                                        "gather_algorithm_segmentsize",
                                        "Segment size in bytes used by default for gather algorithms. Only has meaning if algorithm is forced and supports segmenting. 0 bytes means no segmentation. Currently, available algorithms do not support segmentation.",
                                        MCA_BASE_VAR_TYPE_INT, NULL, 0, MCA_BASE_VAR_FLAG_SETTABLE,
                                        OPAL_INFO_LVL_5,
                                        MCA_BASE_VAR_SCOPE_ALL,
                                        &coll_tuned_gather_segment_size);

    coll_tuned_gather_tree_fanout = ompi_coll_tuned_init_tree_fanout; /* get system wide default */
    mca_param_indices->tree_fanout_param_index =
        mca_base_component_var_register(&mca_coll_tuned_component.super.collm_version,
                                        "gather_algorithm_tree_fanout",
                                        "Fanout for n-tree used for gather algorithms. Only has meaning if algorithm is forced and supports n-tree topo based operation. Currently, available algorithms do not support n-tree topologies.",
                                        MCA_BASE_VAR_TYPE_INT, NULL, 0, MCA_BASE_VAR_FLAG_SETTABLE,
                                        OPAL_INFO_LVL_5,
                                        MCA_BASE_VAR_SCOPE_ALL,
                                        &coll_tuned_gather_tree_fanout);

    coll_tuned_gather_chain_fanout = ompi_coll_tuned_init_chain_fanout; /* get system wide default */
    mca_param_indices->chain_fanout_param_index =
      mca_base_component_var_register(&mca_coll_tuned_component.super.collm_version,
                                      "gather_algorithm_chain_fanout",
                                      "Fanout for chains used for gather algorithms. Only has meaning if algorithm is forced and supports chain topo based operation. Currently, available algorithms do not support chain topologies.",
                                      MCA_BASE_VAR_TYPE_INT, NULL, 0, MCA_BASE_VAR_FLAG_SETTABLE,
                                      OPAL_INFO_LVL_5,
                                      MCA_BASE_VAR_SCOPE_ALL,
                                      &coll_tuned_gather_chain_fanout);

    return (MPI_SUCCESS);
}

int
ompi_coll_tuned_gather_intra_do_this(const void *sbuf, int scount,
                                     struct ompi_datatype_t *sdtype,
                                     void* rbuf, int rcount,
                                     struct ompi_datatype_t *rdtype,
                                     int root,
                                     struct ompi_communicator_t *comm,
                                     mca_coll_base_module_t *module,
                                     int algorithm, int faninout, int segsize)
{
    OPAL_OUTPUT((ompi_coll_tuned_stream,
                 "coll:tuned:gather_intra_do_this selected algorithm %d topo faninout %d segsize %d",
                 algorithm, faninout, segsize));

    switch (algorithm) {
    case (0):
        return ompi_coll_tuned_gather_intra_dec_fixed(sbuf, scount, sdtype,
                                                      rbuf, rcount, rdtype,
                                                      root, comm, module);
    case (1):
        return ompi_coll_base_gather_intra_basic_linear(sbuf, scount, sdtype,
                                                        rbuf, rcount, rdtype,
                                                        root, comm, module);
    case (2):
        return ompi_coll_base_gather_intra_binomial(sbuf, scount, sdtype,
                                                    rbuf, rcount, rdtype,
                                                    root, comm, module);
    case (3):
        return ompi_coll_base_gather_intra_linear_sync(sbuf, scount, sdtype,
                                                       rbuf, rcount, rdtype,
                                                       root, comm, module,
                                                       segsize);
    } /* switch */
    OPAL_OUTPUT((ompi_coll_tuned_stream,
                 "coll:tuned:gather_intra_do_this attempt to select algorithm %d when only 0-%d is valid?",
                 algorithm, ompi_coll_tuned_forced_max_algorithms[GATHER]));
    return (MPI_ERR_ARG);
}
