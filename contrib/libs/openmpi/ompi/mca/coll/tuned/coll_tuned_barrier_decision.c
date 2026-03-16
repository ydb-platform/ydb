/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2015 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include "mpi.h"
#include "opal/util/bit_ops.h"
#include "ompi/constants.h"
#include "ompi/communicator/communicator.h"
#include "ompi/mca/coll/coll.h"
#include "ompi/mca/coll/base/coll_tags.h"
#include "ompi/mca/pml/pml.h"
#include "coll_tuned.h"
#include "ompi/mca/coll/base/coll_base_topo.h"
#include "ompi/mca/coll/base/coll_base_util.h"

/* barrier algorithm variables */
static int coll_tuned_barrier_forced_algorithm = 0;

/* valid values for coll_tuned_barrier_forced_algorithm */
static mca_base_var_enum_value_t barrier_algorithms[] = {
    {0, "ignore"},
    {1, "linear"},
    {2, "double_ring"},
    {3, "recursive_doubling"},
    {4, "bruck"},
    {5, "two_proc"},
    {6, "tree"},
    {0, NULL}
};

/* The following are used by dynamic and forced rules */

/* publish details of each algorithm and if its forced/fixed/locked in */
/* as you add methods/algorithms you must update this and the query/map  */
/* routines */

/* this routine is called by the component only */
/* this makes sure that the mca parameters are set to their initial values */
/* and perms */
/* module does not call this they call the forced_getvalues routine instead */

int ompi_coll_tuned_barrier_intra_check_forced_init (coll_tuned_force_algorithm_mca_param_indices_t *mca_param_indices)
{
    mca_base_var_enum_t *new_enum;
    int cnt;

    for( cnt = 0; NULL != barrier_algorithms[cnt].string; cnt++ );
    ompi_coll_tuned_forced_max_algorithms[BARRIER] = cnt;

    (void) mca_base_component_var_register(&mca_coll_tuned_component.super.collm_version,
                                           "barrier_algorithm_count",
                                           "Number of barrier algorithms available",
                                           MCA_BASE_VAR_TYPE_INT, NULL, 0,
                                           MCA_BASE_VAR_FLAG_DEFAULT_ONLY,
                                           OPAL_INFO_LVL_5,
                                           MCA_BASE_VAR_SCOPE_CONSTANT,
                                           &ompi_coll_tuned_forced_max_algorithms[BARRIER]);

    /* MPI_T: This variable should eventually be bound to a communicator */
    coll_tuned_barrier_forced_algorithm = 0;
    (void) mca_base_var_enum_create("coll_tuned_barrier_algorithms", barrier_algorithms, &new_enum);
    mca_param_indices->algorithm_param_index =
        mca_base_component_var_register(&mca_coll_tuned_component.super.collm_version,
                                        "barrier_algorithm",
                                        "Which barrier algorithm is used. Can be locked down to choice of: 0 ignore, 1 linear, 2 double ring, 3: recursive doubling 4: bruck, 5: two proc only, 6: tree",
                                        MCA_BASE_VAR_TYPE_INT, new_enum, 0, MCA_BASE_VAR_FLAG_SETTABLE,
                                        OPAL_INFO_LVL_5,
                                        MCA_BASE_VAR_SCOPE_ALL,
                                        &coll_tuned_barrier_forced_algorithm);
    OBJ_RELEASE(new_enum);
    if (mca_param_indices->algorithm_param_index < 0) {
        return mca_param_indices->algorithm_param_index;
    }

    return (MPI_SUCCESS);
}

int ompi_coll_tuned_barrier_intra_do_this (struct ompi_communicator_t *comm,
                                           mca_coll_base_module_t *module,
                                           int algorithm, int faninout, int segsize)
{
    OPAL_OUTPUT((ompi_coll_tuned_stream,
                 "coll:tuned:barrier_intra_do_this selected algorithm %d topo fanin/out%d",
                 algorithm, faninout));

    switch (algorithm) {
    case (0):   return ompi_coll_tuned_barrier_intra_dec_fixed(comm, module);
    case (1):   return ompi_coll_base_barrier_intra_basic_linear(comm, module);
    case (2):   return ompi_coll_base_barrier_intra_doublering(comm, module);
    case (3):   return ompi_coll_base_barrier_intra_recursivedoubling(comm, module);
    case (4):   return ompi_coll_base_barrier_intra_bruck(comm, module);
    case (5):   return ompi_coll_base_barrier_intra_two_procs(comm, module);
    case (6):   return ompi_coll_base_barrier_intra_tree(comm, module);
    } /* switch */
    OPAL_OUTPUT((ompi_coll_tuned_stream,"coll:tuned:barrier_intra_do_this attempt to select algorithm %d when only 0-%d is valid?",
                 algorithm, ompi_coll_tuned_forced_max_algorithms[BARRIER]));
    return (MPI_ERR_ARG);
}
