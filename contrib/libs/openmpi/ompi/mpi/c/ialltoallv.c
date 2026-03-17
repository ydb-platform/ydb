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
 * Copyright (c) 2007      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2012-2013 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2014-2016 Research Organization for Information Science
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
#include "ompi/memchecker.h"
#include "ompi/runtime/ompi_spc.h"

#if OMPI_BUILD_MPI_PROFILING
#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak MPI_Ialltoallv = PMPI_Ialltoallv
#endif
#define MPI_Ialltoallv PMPI_Ialltoallv
#endif

static const char FUNC_NAME[] = "MPI_Ialltoallv";


int MPI_Ialltoallv(const void *sendbuf, const int sendcounts[], const int sdispls[],
                   MPI_Datatype sendtype, void *recvbuf, const int recvcounts[],
                   const int rdispls[], MPI_Datatype recvtype, MPI_Comm comm,
                   MPI_Request *request)
{
    int i, size, err;

    SPC_RECORD(OMPI_SPC_IALLTOALLV, 1);

    MEMCHECKER(
        ptrdiff_t recv_ext;
        ptrdiff_t send_ext;

        memchecker_comm(comm);

        if (MPI_IN_PLACE != sendbuf) {
            memchecker_datatype(sendtype);
            ompi_datatype_type_extent(sendtype, &send_ext);
        }

        memchecker_datatype(recvtype);
        ompi_datatype_type_extent(recvtype, &recv_ext);

        size = OMPI_COMM_IS_INTER(comm)?ompi_comm_remote_size(comm):ompi_comm_size(comm);
        for ( i = 0; i < size; i++ ) {
            if (MPI_IN_PLACE != sendbuf) {
                /* check if send chunks are defined. */
                memchecker_call(&opal_memchecker_base_isdefined,
                                (char *)(sendbuf)+sdispls[i]*send_ext,
                                sendcounts[i], sendtype);
            }
            /* check if receive chunks are addressable. */
            memchecker_call(&opal_memchecker_base_isaddressable,
                            (char *)(recvbuf)+rdispls[i]*recv_ext,
                            recvcounts[i], recvtype);
        }
    );

    if (MPI_PARAM_CHECK) {

        /* Unrooted operation -- same checks for all ranks */

        err = MPI_SUCCESS;
        OMPI_ERR_INIT_FINALIZE(FUNC_NAME);
        if (ompi_comm_invalid(comm)) {
            return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_COMM,
                                          FUNC_NAME);
        }

        if (MPI_IN_PLACE == sendbuf) {
            sendcounts = recvcounts;
            sdispls = rdispls;
            sendtype = recvtype;
        }

        if ((NULL == sendcounts) || (NULL == sdispls) ||
            (NULL == recvcounts) || (NULL == rdispls) ||
            (MPI_IN_PLACE == sendbuf && OMPI_COMM_IS_INTER(comm)) ||
            MPI_IN_PLACE == recvbuf) {
            return OMPI_ERRHANDLER_INVOKE(comm, MPI_ERR_ARG, FUNC_NAME);
        }

        size = OMPI_COMM_IS_INTER(comm)?ompi_comm_remote_size(comm):ompi_comm_size(comm);
        for (i = 0; i < size; ++i) {
            OMPI_CHECK_DATATYPE_FOR_SEND(err, sendtype, sendcounts[i]);
            OMPI_ERRHANDLER_CHECK(err, comm, err, FUNC_NAME);
            OMPI_CHECK_DATATYPE_FOR_RECV(err, recvtype, recvcounts[i]);
            OMPI_ERRHANDLER_CHECK(err, comm, err, FUNC_NAME);
        }

        if (MPI_IN_PLACE != sendbuf && !OMPI_COMM_IS_INTER(comm)) {
            int me = ompi_comm_rank(comm);
            size_t sendtype_size, recvtype_size;
            ompi_datatype_type_size(sendtype, &sendtype_size);
            ompi_datatype_type_size(recvtype, &recvtype_size);
            if ((sendtype_size*sendcounts[me]) != (recvtype_size*recvcounts[me])) {
                return OMPI_ERRHANDLER_INVOKE(comm, MPI_ERR_TRUNCATE, FUNC_NAME);
            }
        }
    }

    OPAL_CR_ENTER_LIBRARY();

    /* Invoke the coll component to perform the back-end operation */
    err = comm->c_coll->coll_ialltoallv(sendbuf, sendcounts, sdispls,
                                       sendtype, recvbuf, recvcounts, rdispls,
                                       recvtype, comm, request, comm->c_coll->coll_ialltoallv_module);
    OMPI_ERRHANDLER_RETURN(err, comm, err, FUNC_NAME);
}

