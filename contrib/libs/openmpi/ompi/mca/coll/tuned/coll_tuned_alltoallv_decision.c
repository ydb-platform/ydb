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

/* alltoallv algorithm variables */
static int coll_tuned_alltoallv_forced_algorithm = 0;

/* valid values for coll_tuned_alltoallv_forced_algorithm */
static mca_base_var_enum_value_t alltoallv_algorithms[] = {
    {0, "ignore"},
    {1, "basic_linear"},
    {2, "pairwise"},
    {0, NULL}
};

/*
 * The following are used by dynamic and forced rules.  Publish
 * details of each algorithm and if its forced/fixed/locked in as you add
 * methods/algorithms you must update this and the query/map routines.
 * This routine is called by the component only.  This makes sure that
 * the mca parameters are set to their initial values and perms.
 * Module does not call this.  They call the forced_getvalues routine
 * instead.
 */
int ompi_coll_tuned_alltoallv_intra_check_forced_init(coll_tuned_force_algorithm_mca_param_indices_t
                                                      *mca_param_indices)
{
    mca_base_var_enum_t *new_enum;
    int cnt;

    for( cnt = 0; NULL != alltoallv_algorithms[cnt].string; cnt++ );
    ompi_coll_tuned_forced_max_algorithms[ALLTOALLV] = cnt;

    (void) mca_base_component_var_register(&mca_coll_tuned_component.super.collm_version,
                                           "alltoallv_algorithm_count",
                                           "Number of alltoallv algorithms available",
                                           MCA_BASE_VAR_TYPE_INT, NULL, 0,
                                           MCA_BASE_VAR_FLAG_DEFAULT_ONLY,
                                           OPAL_INFO_LVL_5,
                                           MCA_BASE_VAR_SCOPE_CONSTANT,
                                           &ompi_coll_tuned_forced_max_algorithms[ALLTOALLV]);

    /* MPI_T: This variable should eventually be bound to a communicator */
    coll_tuned_alltoallv_forced_algorithm = 0;
    (void) mca_base_var_enum_create("coll_tuned_alltoallv_algorithms", alltoallv_algorithms, &new_enum);
    mca_param_indices->algorithm_param_index =
        mca_base_component_var_register(&mca_coll_tuned_component.super.collm_version,
                                        "alltoallv_algorithm",
                                        "Which alltoallv algorithm is used. "
                                        "Can be locked down to choice of: 0 ignore, "
                                        "1 basic linear, 2 pairwise.",
                                        MCA_BASE_VAR_TYPE_INT, new_enum, 0, MCA_BASE_VAR_FLAG_SETTABLE,
                                        OPAL_INFO_LVL_5,
                                        MCA_BASE_VAR_SCOPE_ALL,
                                        &coll_tuned_alltoallv_forced_algorithm);

    OBJ_RELEASE(new_enum);
    if (mca_param_indices->algorithm_param_index < 0) {
        return mca_param_indices->algorithm_param_index;
    }

    return (MPI_SUCCESS);
}

/* If the user selects dynamic rules and specifies the algorithm to
 * use, then this function is called.  */
int ompi_coll_tuned_alltoallv_intra_do_this(const void *sbuf, const int *scounts, const int *sdisps,
                                            struct ompi_datatype_t *sdtype,
                                            void* rbuf, const int *rcounts, const int *rdisps,
                                            struct ompi_datatype_t *rdtype,
                                            struct ompi_communicator_t *comm,
                                            mca_coll_base_module_t *module,
                                            int algorithm)
{
    OPAL_OUTPUT((ompi_coll_tuned_stream,
                 "coll:tuned:alltoallv_intra_do_this selected algorithm %d ",
                 algorithm));

    switch (algorithm) {
    case (0):
        return ompi_coll_tuned_alltoallv_intra_dec_fixed(sbuf, scounts, sdisps, sdtype,
                                                         rbuf, rcounts, rdisps, rdtype,
                                                         comm, module);
    case (1):
        return ompi_coll_base_alltoallv_intra_basic_linear(sbuf, scounts, sdisps, sdtype,
                                                           rbuf, rcounts, rdisps, rdtype,
                                                           comm, module);
    case (2):
        return ompi_coll_base_alltoallv_intra_pairwise(sbuf, scounts, sdisps, sdtype,
                                                       rbuf, rcounts, rdisps, rdtype,
                                                       comm, module);
    }  /* switch */
    OPAL_OUTPUT((ompi_coll_tuned_stream,
                 "coll:tuned:alltoall_intra_do_this attempt to select "
                 "algorithm %d when only 0-%d is valid.",
                 algorithm, ompi_coll_tuned_forced_max_algorithms[ALLTOALLV]));
    return (MPI_ERR_ARG);
}
