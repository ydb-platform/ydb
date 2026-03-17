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
 * Copyright (c) 2017      IBM Corporation.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"
#include "coll_self.h"

#include <stdio.h>

#include "mpi.h"
#include "ompi/communicator/communicator.h"
#include "ompi/mca/coll/coll.h"
#include "ompi/mca/coll/base/base.h"
#include "ompi/mca/coll/base/coll_base_functions.h"
#include "coll_self.h"


/*
 * Initial query function that is invoked during MPI_INIT, allowing
 * this module to indicate what level of thread support it provides.
 */
int mca_coll_self_init_query(bool enable_progress_threads,
                             bool enable_mpi_threads)
{
    /* Nothing to do */

    return OMPI_SUCCESS;
}


/*
 * Invoked when there's a new communicator that has been created.
 * Look at the communicator and decide which set of functions and
 * priority we want to return.
 */
mca_coll_base_module_t *
mca_coll_self_comm_query(struct ompi_communicator_t *comm,
                         int *priority)
{
    mca_coll_self_module_t *module;

    /* We only work on intracommunicators of size 1 */

    if (!OMPI_COMM_IS_INTER(comm) && 1 == ompi_comm_size(comm)) {
        *priority = ompi_coll_self_priority;

        module = OBJ_NEW(mca_coll_self_module_t);
        if (NULL == module) return NULL;

        module->super.coll_module_enable = mca_coll_self_module_enable;
        module->super.ft_event        = mca_coll_self_ft_event;
        module->super.coll_allgather  = mca_coll_self_allgather_intra;
        module->super.coll_allgatherv = mca_coll_self_allgatherv_intra;
        module->super.coll_allreduce  = mca_coll_self_allreduce_intra;
        module->super.coll_alltoall   = mca_coll_self_alltoall_intra;
        module->super.coll_alltoallv  = mca_coll_self_alltoallv_intra;
        module->super.coll_alltoallw  = mca_coll_self_alltoallw_intra;
        module->super.coll_barrier    = mca_coll_self_barrier_intra;
        module->super.coll_bcast      = mca_coll_self_bcast_intra;
        module->super.coll_exscan     = mca_coll_self_exscan_intra;
        module->super.coll_gather     = mca_coll_self_gather_intra;
        module->super.coll_gatherv    = mca_coll_self_gatherv_intra;
        module->super.coll_reduce     = mca_coll_self_reduce_intra;
        module->super.coll_reduce_scatter = mca_coll_self_reduce_scatter_intra;
        module->super.coll_scan       = mca_coll_self_scan_intra;
        module->super.coll_scatter    = mca_coll_self_scatter_intra;
        module->super.coll_scatterv   = mca_coll_self_scatterv_intra;

        module->super.coll_reduce_local = mca_coll_base_reduce_local;

        return &(module->super);
    }

    return NULL;
}


/*
 * Init module on the communicator
 */
int
mca_coll_self_module_enable(mca_coll_base_module_t *module,
                            struct ompi_communicator_t *comm)
{
    return OMPI_SUCCESS;
}


int mca_coll_self_ft_event(int state) {
    if(OPAL_CRS_CHECKPOINT == state) {
        ;
    }
    else if(OPAL_CRS_CONTINUE == state) {
        ;
    }
    else if(OPAL_CRS_RESTART == state) {
        ;
    }
    else if(OPAL_CRS_TERM == state ) {
        ;
    }
    else {
        ;
    }

    return OMPI_SUCCESS;
}
