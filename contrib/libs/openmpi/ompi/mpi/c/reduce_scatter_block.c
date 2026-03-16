/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2017 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2008 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2006-2012 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2012      Oak Ridge National Labs. All rights reserved.
 * Copyright (c) 2013      Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2015-2018 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
#include "ompi_config.h"
#include <stdio.h>

#include "ompi/mpi/c/bindings.h"
#include "ompi/runtime/params.h"
#include "ompi/communicator/communicator.h"
#include "ompi/errhandler/errhandler.h"
#include "ompi/datatype/ompi_datatype.h"
#include "ompi/op/op.h"
#include "ompi/memchecker.h"
#include "ompi/runtime/ompi_spc.h"

#if OMPI_BUILD_MPI_PROFILING
#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak MPI_Reduce_scatter_block = PMPI_Reduce_scatter_block
#endif
#define MPI_Reduce_scatter_block PMPI_Reduce_scatter_block
#endif

static const char FUNC_NAME[] = "MPI_Reduce_scatter_block";


int MPI_Reduce_scatter_block(const void *sendbuf, void *recvbuf, int recvcount,
                             MPI_Datatype datatype, MPI_Op op, MPI_Comm comm)
{
    int err;

    SPC_RECORD(OMPI_SPC_REDUCE_SCATTER_BLOCK, 1);

    MEMCHECKER(
        memchecker_comm(comm);
        memchecker_datatype(datatype);

        /* check receive buffer of current proccess, whether it's addressable. */
        memchecker_call(&opal_memchecker_base_isaddressable, recvbuf,
                        recvcount, datatype);

        /* check whether the actual send buffer is defined. */
        if(MPI_IN_PLACE == sendbuf) {
            memchecker_call(&opal_memchecker_base_isdefined, recvbuf, recvcount, datatype);
        } else {
            memchecker_call(&opal_memchecker_base_isdefined, sendbuf, recvcount, datatype);

        }
    );

    if (MPI_PARAM_CHECK) {
        char *msg;
        err = MPI_SUCCESS;
        OMPI_ERR_INIT_FINALIZE(FUNC_NAME);
        if (ompi_comm_invalid(comm)) {
            return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_COMM,
                                          FUNC_NAME);
        }

        /* Unrooted operation; same checks for all ranks on both
           intracommunicators and intercommunicators */

        else if (MPI_OP_NULL == op || NULL == op) {
          err = MPI_ERR_OP;
        } else if (!ompi_op_is_valid(op, datatype, &msg, FUNC_NAME)) {
            int ret = OMPI_ERRHANDLER_INVOKE(comm, MPI_ERR_OP, msg);
            free(msg);
            return ret;
        } else if (MPI_IN_PLACE == recvbuf) {
          err = MPI_ERR_ARG;
        }
        OMPI_ERRHANDLER_CHECK(err, comm, err, FUNC_NAME);

        OMPI_CHECK_DATATYPE_FOR_SEND(err, datatype, recvcount);
        OMPI_ERRHANDLER_CHECK(err, comm, err, FUNC_NAME);
    }

    OPAL_CR_ENTER_LIBRARY();

    /* Invoke the coll component to perform the back-end operation */

    OBJ_RETAIN(op);
    err = comm->c_coll->coll_reduce_scatter_block(sendbuf, recvbuf, recvcount,
                                                 datatype, op, comm,
                                                 comm->c_coll->coll_reduce_scatter_block_module);
    OBJ_RELEASE(op);
    OMPI_ERRHANDLER_RETURN(err, comm, err, FUNC_NAME);
}
