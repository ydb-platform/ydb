/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2018 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2008 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2013      Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2015-2018 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2016      IBM Corporation.  All rights reserved.
 * Copyright (c) 2018      FUJITSU LIMITED.  All rights reserved.
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
#include "ompi/mpiext/pcollreq/c/mpiext_pcollreq_c.h"
#include "ompi/runtime/ompi_spc.h"

#if OMPI_BUILD_MPI_PROFILING
#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak MPIX_Allreduce_init = PMPIX_Allreduce_init
#endif
#define MPIX_Allreduce_init PMPIX_Allreduce_init
#endif

static const char FUNC_NAME[] = "MPIX_Allreduce_init";


int MPIX_Allreduce_init(const void *sendbuf, void *recvbuf, int count,
                        MPI_Datatype datatype, MPI_Op op, MPI_Comm comm,
                        MPI_Info info, MPI_Request *request)
{
    int err;

    SPC_RECORD(OMPI_SPC_ALLREDUCE_INIT, 1);

    MEMCHECKER(
        memchecker_datatype(datatype);
        memchecker_comm(comm);

        /* check whether receive buffer is defined. */
        memchecker_call(&opal_memchecker_base_isaddressable, recvbuf, count, datatype);

        /* check whether the actual send buffer is defined. */
        if (MPI_IN_PLACE == sendbuf) {
            memchecker_call(&opal_memchecker_base_isdefined, recvbuf, count, datatype);
        } else {
            memchecker_call(&opal_memchecker_base_isdefined, sendbuf, count, datatype);
        }
    );

    if (MPI_PARAM_CHECK) {
        char *msg;

        /* Unrooted operation -- same checks for all ranks on both
           intracommunicators and intercommunicators */

        err = MPI_SUCCESS;
        OMPI_ERR_INIT_FINALIZE(FUNC_NAME);
        if (ompi_comm_invalid(comm)) {
            return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_COMM,
                                          FUNC_NAME);
        } else if (MPI_OP_NULL == op) {
            err = MPI_ERR_OP;
        } else if (!ompi_op_is_valid(op, datatype, &msg, FUNC_NAME)) {
            int ret = OMPI_ERRHANDLER_INVOKE(comm, MPI_ERR_OP, msg);
            free(msg);
            return ret;
        } else if ((MPI_IN_PLACE == sendbuf && OMPI_COMM_IS_INTER(comm)) ||
                   MPI_IN_PLACE == recvbuf ) {
            return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_BUFFER,
                                          FUNC_NAME);
        } else if( (sendbuf == recvbuf) &&
                   (MPI_BOTTOM != sendbuf) &&
                   (count > 1) ) {
            return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_BUFFER,
                                          FUNC_NAME);
        } else {
            OMPI_CHECK_DATATYPE_FOR_SEND(err, datatype, count);
        }
        OMPI_ERRHANDLER_CHECK(err, comm, err, FUNC_NAME);
    }


    /* MPI standard says that reductions have to have a count of at least 1,
     * but some benchmarks (e.g., IMB) calls this function with a count of 0.
     * So handle that case.
     */
    if (0 == count) {
        err = ompi_request_persistent_noop_create(request);
        OMPI_ERRHANDLER_RETURN(err, comm, err, FUNC_NAME);
    }

    OPAL_CR_ENTER_LIBRARY();

    /* Invoke the coll component to perform the back-end operation */

    OBJ_RETAIN(op);
    err = comm->c_coll->coll_allreduce_init(sendbuf, recvbuf, count, datatype,
                                            op, comm, info, request, comm->c_coll->coll_allreduce_init_module);
    OBJ_RELEASE(op);
    OMPI_ERRHANDLER_RETURN(err, comm, err, FUNC_NAME);
}
