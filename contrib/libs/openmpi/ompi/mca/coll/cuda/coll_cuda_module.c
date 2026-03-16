/*
 * Copyright (c) 2014-2017 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2014      NVIDIA Corporation.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include <string.h>
#include <stdio.h>

#include "coll_cuda.h"

#include "mpi.h"

#include "orte/util/show_help.h"
#include "orte/util/proc_info.h"

#include "ompi/constants.h"
#include "ompi/communicator/communicator.h"
#include "ompi/mca/coll/coll.h"
#include "ompi/mca/coll/base/base.h"
#include "coll_cuda.h"


static void mca_coll_cuda_module_construct(mca_coll_cuda_module_t *module)
{
    memset(&(module->c_coll), 0, sizeof(module->c_coll));
}

static void mca_coll_cuda_module_destruct(mca_coll_cuda_module_t *module)
{
    OBJ_RELEASE(module->c_coll.coll_allreduce_module);
    OBJ_RELEASE(module->c_coll.coll_reduce_module);
    OBJ_RELEASE(module->c_coll.coll_reduce_scatter_block_module);
    OBJ_RELEASE(module->c_coll.coll_scatter_module);
    /* If the exscan module is not NULL, then this was an
       intracommunicator, and therefore scan will have a module as
       well. */
    if (NULL != module->c_coll.coll_exscan_module) {
        OBJ_RELEASE(module->c_coll.coll_exscan_module);
        OBJ_RELEASE(module->c_coll.coll_scan_module);
    }
}

OBJ_CLASS_INSTANCE(mca_coll_cuda_module_t, mca_coll_base_module_t,
                   mca_coll_cuda_module_construct,
                   mca_coll_cuda_module_destruct);


/*
 * Initial query function that is invoked during MPI_INIT, allowing
 * this component to disqualify itself if it doesn't support the
 * required level of thread support.
 */
int mca_coll_cuda_init_query(bool enable_progress_threads,
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
mca_coll_cuda_comm_query(struct ompi_communicator_t *comm,
                         int *priority)
{
    mca_coll_cuda_module_t *cuda_module;

    cuda_module = OBJ_NEW(mca_coll_cuda_module_t);
    if (NULL == cuda_module) {
        return NULL;
    }

    *priority = mca_coll_cuda_component.priority;

    /* Choose whether to use [intra|inter] */
    cuda_module->super.coll_module_enable = mca_coll_cuda_module_enable;
    cuda_module->super.ft_event = NULL;

    cuda_module->super.coll_allgather  = NULL;
    cuda_module->super.coll_allgatherv = NULL;
    cuda_module->super.coll_allreduce  = mca_coll_cuda_allreduce;
    cuda_module->super.coll_alltoall   = NULL;
    cuda_module->super.coll_alltoallv  = NULL;
    cuda_module->super.coll_alltoallw  = NULL;
    cuda_module->super.coll_barrier    = NULL;
    cuda_module->super.coll_bcast      = NULL;
    cuda_module->super.coll_exscan     = mca_coll_cuda_exscan;
    cuda_module->super.coll_gather     = NULL;
    cuda_module->super.coll_gatherv    = NULL;
    cuda_module->super.coll_reduce     = mca_coll_cuda_reduce;
    cuda_module->super.coll_reduce_scatter = NULL;
    cuda_module->super.coll_reduce_scatter_block = mca_coll_cuda_reduce_scatter_block;
    cuda_module->super.coll_scan       = mca_coll_cuda_scan;
    cuda_module->super.coll_scatter    = NULL;
    cuda_module->super.coll_scatterv   = NULL;

    return &(cuda_module->super);
}


/*
 * Init module on the communicator
 */
int mca_coll_cuda_module_enable(mca_coll_base_module_t *module,
                                struct ompi_communicator_t *comm)
{
    bool good = true;
    char *msg = NULL;
    mca_coll_cuda_module_t *s = (mca_coll_cuda_module_t*) module;

#define CHECK_AND_RETAIN(src, dst, name)                                                   \
    if (NULL == (src)->c_coll->coll_ ## name ## _module) {                                 \
        good = false;                                                                      \
        msg = #name;                                                                       \
    } else if (good) {                                                                     \
        (dst)->c_coll.coll_ ## name ## _module = (src)->c_coll->coll_ ## name ## _module;  \
        (dst)->c_coll.coll_ ## name = (src)->c_coll->coll_ ## name;                        \
        OBJ_RETAIN((src)->c_coll->coll_ ## name ## _module);                               \
    }

    CHECK_AND_RETAIN(comm, s, allreduce);
    CHECK_AND_RETAIN(comm, s, reduce);
    CHECK_AND_RETAIN(comm, s, reduce_scatter_block);
    CHECK_AND_RETAIN(comm, s, scatter);
    if (!OMPI_COMM_IS_INTER(comm)) {
        /* MPI does not define scan/exscan on intercommunicators */
        CHECK_AND_RETAIN(comm, s, exscan);
        CHECK_AND_RETAIN(comm, s, scan);
    }

    /* All done */
    if (good) {
        return OMPI_SUCCESS;
    }
    orte_show_help("help-mpi-coll-cuda.txt", "missing collective", true,
                   orte_process_info.nodename,
                   mca_coll_cuda_component.priority, msg);
    return OMPI_ERR_NOT_FOUND;
}

