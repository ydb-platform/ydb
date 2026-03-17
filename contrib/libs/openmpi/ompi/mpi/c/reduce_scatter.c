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
 * Copyright (c) 2012-2013 Los Alamos National Security, LLC.  All rights
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
#pragma weak MPI_Reduce_scatter = PMPI_Reduce_scatter
#endif
#define MPI_Reduce_scatter PMPI_Reduce_scatter
#endif

static const char FUNC_NAME[] = "MPI_Reduce_scatter";


int MPI_Reduce_scatter(const void *sendbuf, void *recvbuf, const int recvcounts[],
                       MPI_Datatype datatype, MPI_Op op, MPI_Comm comm)
{
    int i, err, size, count;

    SPC_RECORD(OMPI_SPC_REDUCE_SCATTER, 1);

    MEMCHECKER(
        int rank;

        size = ompi_comm_size(comm);
        rank = ompi_comm_rank(comm);
        for (count = i = 0; i < size; ++i) {
            if (0 == recvcounts[i]) {
                count += recvcounts[i];
            }
        }

        memchecker_comm(comm);
        memchecker_datatype(datatype);

        /* check receive buffer of current proccess, whether it's addressable. */
        memchecker_call(&opal_memchecker_base_isaddressable, recvbuf,
                        recvcounts[rank], datatype);

        /* check whether the actual send buffer is defined. */
        if(MPI_IN_PLACE == sendbuf) {
            memchecker_call(&opal_memchecker_base_isdefined, recvbuf, count, datatype);
        } else {
            memchecker_call(&opal_memchecker_base_isdefined, sendbuf, count, datatype);

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
        } else if (NULL == recvcounts) {
          err = MPI_ERR_COUNT;
        } else if (MPI_IN_PLACE == recvbuf) {
          err = MPI_ERR_ARG;
        }
        OMPI_ERRHANDLER_CHECK(err, comm, err, FUNC_NAME);

        /* Based on the standard each group has to provide the same total
           number of elements, so the size of the recvcounts array depends
           on the number of participants in the local group.  */
        size = ompi_comm_size(comm);
        for (i = 0; i < size; ++i) {
          OMPI_CHECK_DATATYPE_FOR_SEND(err, datatype, recvcounts[i]);
          OMPI_ERRHANDLER_CHECK(err, comm, err, FUNC_NAME);
        }
    }

    /* MPI-1, p114, says that each process must supply at least one
       element.  But at least the Pallas benchmarks call MPI_REDUCE
       with a count of 0.  So be sure to handle it.  Grrr... */

    size = ompi_comm_size(comm);
    for (count = i = 0; i < size; ++i) {
        if (0 == recvcounts[i]) {
            ++count;
        }
    }
    if (size == count) {
        return MPI_SUCCESS;
    }

    OPAL_CR_ENTER_LIBRARY();

    /* Invoke the coll component to perform the back-end operation */

    OBJ_RETAIN(op);
    err = comm->c_coll->coll_reduce_scatter(sendbuf, recvbuf, recvcounts,
                                           datatype, op, comm,
                                           comm->c_coll->coll_reduce_scatter_module);
    OBJ_RELEASE(op);
    OMPI_ERRHANDLER_RETURN(err, comm, err, FUNC_NAME);
}
