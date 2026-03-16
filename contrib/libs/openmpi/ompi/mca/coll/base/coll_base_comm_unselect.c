/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2017 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2012      Oak Rigde National Laboratory.
 *                         All rights reserved.
 * Copyright (c) 2013 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2014      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2017      IBM Corporation.  All rights reserved.
 * Copyright (c) 2017      FUJITSU LIMITED.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include "ompi/mca/mca.h"
#include "opal/mca/base/base.h"

#include "ompi/communicator/communicator.h"
#include "ompi/mca/coll/base/base.h"

#define CLOSE(comm, func)                                        \
    do {                                                         \
        if (NULL != comm->c_coll->coll_ ## func ## _module) {    \
            if (NULL != comm->c_coll->coll_ ## func ## _module->coll_module_disable) { \
                comm->c_coll->coll_ ## func ## _module->coll_module_disable(           \
                              comm->c_coll->coll_ ## func ## _module, comm);           \
            }                                                    \
            OBJ_RELEASE(comm->c_coll->coll_ ## func ## _module); \
            comm->c_coll->coll_## func = NULL;                   \
            comm->c_coll->coll_## func ## _module = NULL;        \
        }                                                        \
    } while (0)

int mca_coll_base_comm_unselect(ompi_communicator_t * comm)
{
    CLOSE(comm, allgather);
    CLOSE(comm, allgatherv);
    CLOSE(comm, allreduce);
    CLOSE(comm, alltoall);
    CLOSE(comm, alltoallv);
    CLOSE(comm, alltoallw);
    CLOSE(comm, barrier);
    CLOSE(comm, bcast);
    CLOSE(comm, exscan);
    CLOSE(comm, gather);
    CLOSE(comm, gatherv);
    CLOSE(comm, reduce);
    CLOSE(comm, reduce_scatter_block);
    CLOSE(comm, reduce_scatter);
    CLOSE(comm, scan);
    CLOSE(comm, scatter);
    CLOSE(comm, scatterv);

    CLOSE(comm, iallgather);
    CLOSE(comm, iallgatherv);
    CLOSE(comm, iallreduce);
    CLOSE(comm, ialltoall);
    CLOSE(comm, ialltoallv);
    CLOSE(comm, ialltoallw);
    CLOSE(comm, ibarrier);
    CLOSE(comm, ibcast);
    CLOSE(comm, iexscan);
    CLOSE(comm, igather);
    CLOSE(comm, igatherv);
    CLOSE(comm, ireduce);
    CLOSE(comm, ireduce_scatter_block);
    CLOSE(comm, ireduce_scatter);
    CLOSE(comm, iscan);
    CLOSE(comm, iscatter);
    CLOSE(comm, iscatterv);

    CLOSE(comm, allgather_init);
    CLOSE(comm, allgatherv_init);
    CLOSE(comm, allreduce_init);
    CLOSE(comm, alltoall_init);
    CLOSE(comm, alltoallv_init);
    CLOSE(comm, alltoallw_init);
    CLOSE(comm, barrier_init);
    CLOSE(comm, bcast_init);
    CLOSE(comm, exscan_init);
    CLOSE(comm, gather_init);
    CLOSE(comm, gatherv_init);
    CLOSE(comm, reduce_init);
    CLOSE(comm, reduce_scatter_block_init);
    CLOSE(comm, reduce_scatter_init);
    CLOSE(comm, scan_init);
    CLOSE(comm, scatter_init);
    CLOSE(comm, scatterv_init);

    CLOSE(comm, neighbor_allgather);
    CLOSE(comm, neighbor_allgatherv);
    CLOSE(comm, neighbor_alltoall);
    CLOSE(comm, neighbor_alltoallv);
    CLOSE(comm, neighbor_alltoallw);

    CLOSE(comm, ineighbor_allgather);
    CLOSE(comm, ineighbor_allgatherv);
    CLOSE(comm, ineighbor_alltoall);
    CLOSE(comm, ineighbor_alltoallv);
    CLOSE(comm, ineighbor_alltoallw);

    CLOSE(comm, neighbor_allgather_init);
    CLOSE(comm, neighbor_allgatherv_init);
    CLOSE(comm, neighbor_alltoall_init);
    CLOSE(comm, neighbor_alltoallv_init);
    CLOSE(comm, neighbor_alltoallw_init);

    CLOSE(comm, reduce_local);

    free(comm->c_coll);
    comm->c_coll = NULL;

    /* All done */
    return OMPI_SUCCESS;
}

