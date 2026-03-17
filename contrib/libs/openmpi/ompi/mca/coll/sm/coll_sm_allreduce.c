/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2009      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/** @file */

#include "ompi_config.h"

#include "ompi/constants.h"
#include "ompi/communicator/communicator.h"
#include "coll_sm.h"


/**
 * Shared memory allreduce.
 *
 * For the moment, all we're doing is a reduce to root==0 and then a
 * broadcast.  It is possible that we'll do something better someday.
 */
int mca_coll_sm_allreduce_intra(const void *sbuf, void *rbuf, int count,
                                struct ompi_datatype_t *dtype,
                                struct ompi_op_t *op,
                                struct ompi_communicator_t *comm,
                                mca_coll_base_module_t *module)
{
    int ret;

    /* Note that only the root can pass MPI_IN_PLACE to MPI_REDUCE, so
       have slightly different logic for that case. */

    if (MPI_IN_PLACE == sbuf) {
        int rank = ompi_comm_rank(comm);
        if (0 == rank) {
            ret = mca_coll_sm_reduce_intra(sbuf, rbuf, count, dtype, op, 0,
                                           comm, module);
        } else {
            ret = mca_coll_sm_reduce_intra(rbuf, NULL, count, dtype, op, 0,
                                           comm, module);
        }
    } else {
        ret = mca_coll_sm_reduce_intra(sbuf, rbuf, count, dtype, op, 0,
                                       comm, module);
    }
    return (ret == OMPI_SUCCESS) ?
        mca_coll_sm_bcast_intra(rbuf, count, dtype, 0, comm, module) : ret;
}
