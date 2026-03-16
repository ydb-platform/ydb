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
 * Copyright (c) 2007      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2012-2013 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2014-2018 Research Organization for Information Science
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
#pragma weak MPI_Alltoallw = PMPI_Alltoallw
#endif
#define MPI_Alltoallw PMPI_Alltoallw
#endif

static const char FUNC_NAME[] = "MPI_Alltoallw";


int MPI_Alltoallw(const void *sendbuf, const int sendcounts[],
                  const int sdispls[], const MPI_Datatype sendtypes[],
                  void *recvbuf, const int recvcounts[], const int rdispls[],
                  const MPI_Datatype recvtypes[], MPI_Comm comm)
{
    int i, size, err;

    SPC_RECORD(OMPI_SPC_ALLTOALLW, 1);

    MEMCHECKER(
        ptrdiff_t recv_ext;
        ptrdiff_t send_ext;

        memchecker_comm(comm);

        size = OMPI_COMM_IS_INTER(comm)?ompi_comm_remote_size(comm):ompi_comm_size(comm);
        for ( i = 0; i < size; i++ ) {
            if (MPI_IN_PLACE != sendbuf) {
                memchecker_datatype(sendtypes[i]);
                ompi_datatype_type_extent(sendtypes[i], &send_ext);
                memchecker_call(&opal_memchecker_base_isdefined,
                                (char *)(sendbuf)+sdispls[i]*send_ext,
                                sendcounts[i], sendtypes[i]);
            }
            memchecker_datatype(recvtypes[i]);
            ompi_datatype_type_extent(recvtypes[i], &recv_ext);
            memchecker_call(&opal_memchecker_base_isaddressable,
                            (char *)(recvbuf)+rdispls[i]*recv_ext,
                            recvcounts[i], recvtypes[i]);
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
            sdispls    = rdispls;
            sendtypes  = recvtypes;
        }

        if ((NULL == sendcounts) || (NULL == sdispls) || (NULL == sendtypes) ||
            (NULL == recvcounts) || (NULL == rdispls) || (NULL == recvtypes) ||
            (MPI_IN_PLACE == sendbuf && OMPI_COMM_IS_INTER(comm)) ||
            MPI_IN_PLACE == recvbuf) {
            return OMPI_ERRHANDLER_INVOKE(comm, MPI_ERR_ARG, FUNC_NAME);
        }

        size = OMPI_COMM_IS_INTER(comm)?ompi_comm_remote_size(comm):ompi_comm_size(comm);
        for (i = 0; i < size; ++i) {
            OMPI_CHECK_DATATYPE_FOR_SEND(err, sendtypes[i], sendcounts[i]);
            OMPI_ERRHANDLER_CHECK(err, comm, err, FUNC_NAME);
            OMPI_CHECK_DATATYPE_FOR_RECV(err, recvtypes[i], recvcounts[i]);
            OMPI_ERRHANDLER_CHECK(err, comm, err, FUNC_NAME);
        }

        if (MPI_IN_PLACE != sendbuf && !OMPI_COMM_IS_INTER(comm)) {
            size_t sendtype_size, recvtype_size;
            int me = ompi_comm_rank(comm);
            ompi_datatype_type_size(sendtypes[me], &sendtype_size);
            ompi_datatype_type_size(recvtypes[me], &recvtype_size);
            if ((sendtype_size*sendcounts[me]) != (recvtype_size*recvcounts[me])) {
                return OMPI_ERRHANDLER_INVOKE(comm, MPI_ERR_TRUNCATE, FUNC_NAME);
            }
        }
    }

    OPAL_CR_ENTER_LIBRARY();

    /* Invoke the coll component to perform the back-end operation */
    err = comm->c_coll->coll_alltoallw(sendbuf, sendcounts, sdispls, (ompi_datatype_t **) sendtypes,
                                      recvbuf, recvcounts, rdispls, (ompi_datatype_t **) recvtypes,
                                      comm, comm->c_coll->coll_alltoallw_module);
    OMPI_ERRHANDLER_RETURN(err, comm, err, FUNC_NAME);
}

