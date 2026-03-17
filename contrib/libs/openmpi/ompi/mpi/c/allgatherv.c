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
 * Copyright (c) 2010      University of Houston.  All rights reserved.
 * Copyright (c) 2012 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2012-2013 Los Alamos National Security, LLC.  All rights
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
#pragma weak MPI_Allgatherv = PMPI_Allgatherv
#endif
#define MPI_Allgatherv PMPI_Allgatherv
#endif

static const char FUNC_NAME[] = "MPI_Allgatherv";


int MPI_Allgatherv(const void *sendbuf, int sendcount, MPI_Datatype sendtype,
                   void *recvbuf, const int recvcounts[],
                   const int displs[], MPI_Datatype recvtype, MPI_Comm comm)
{
    int i, size, err;

    SPC_RECORD(OMPI_SPC_ALLGATHERV, 1);

    MEMCHECKER(
        int rank;
        ptrdiff_t ext;

        rank = ompi_comm_rank(comm);
        size = ompi_comm_size(comm);
        ompi_datatype_type_extent(recvtype, &ext);

        memchecker_datatype(recvtype);
        memchecker_comm (comm);
        /* check whether the receive buffer is addressable. */
        for (i = 0; i < size; i++) {
            memchecker_call(&opal_memchecker_base_isaddressable,
                            (char *)(recvbuf)+displs[i]*ext,
                            recvcounts[i], recvtype);
        }

        /* check whether the actual send buffer is defined. */
        if (MPI_IN_PLACE == sendbuf) {
            memchecker_call(&opal_memchecker_base_isdefined,
                            (char *)(recvbuf)+displs[rank]*ext,
                            recvcounts[rank], recvtype);
        } else {
            memchecker_datatype(sendtype);
            memchecker_call(&opal_memchecker_base_isdefined, sendbuf, sendcount, sendtype);
        }
    );

    if (MPI_PARAM_CHECK) {

        /* Unrooted operation -- same checks for all ranks on both
           intracommunicators and intercommunicators */

        err = MPI_SUCCESS;
        OMPI_ERR_INIT_FINALIZE(FUNC_NAME);
        if (ompi_comm_invalid(comm)) {
            return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_COMM,
                                          FUNC_NAME);
        } else if ((MPI_IN_PLACE == sendbuf && OMPI_COMM_IS_INTER(comm)) ||
                   MPI_IN_PLACE == recvbuf) {
            return OMPI_ERRHANDLER_INVOKE(comm, MPI_ERR_ARG, FUNC_NAME);
        } else if (MPI_DATATYPE_NULL == recvtype) {
            return OMPI_ERRHANDLER_INVOKE(comm, MPI_ERR_TYPE, FUNC_NAME);
        }

        if (MPI_IN_PLACE != sendbuf) {
            OMPI_CHECK_DATATYPE_FOR_SEND(err, sendtype, sendcount);
        }
        OMPI_ERRHANDLER_CHECK(err, comm, err, FUNC_NAME);

      /* We always define the remote group to be the same as the local
         group in the case of an intracommunicator, so it's safe to
         get the size of the remote group here for both intra- and
         intercommunicators */

        size = ompi_comm_remote_size(comm);
        for (i = 0; i < size; ++i) {
          if (recvcounts[i] < 0) {
            return OMPI_ERRHANDLER_INVOKE(comm, MPI_ERR_COUNT, FUNC_NAME);
          }
        }

        if (NULL == displs) {
          return OMPI_ERRHANDLER_INVOKE(comm, MPI_ERR_BUFFER, FUNC_NAME);
        }
    }

    /* Do we need to do anything?  Everyone had to give the same
       signature, which means that everyone must have given a
       sum(recvounts) > 0 if there's anything to do. */

    if ( OMPI_COMM_IS_INTRA( comm) ) {
	for (i = 0; i < ompi_comm_size(comm); ++i) {
	    if (0 != recvcounts[i]) {
		break;
	    }
	}
	if (i >= ompi_comm_size(comm)) {
	    return MPI_SUCCESS;
	}
    }
    /* There is no rule that can be applied for inter-communicators, since
       recvcount(s)=0 only indicates that the processes in the other group
       do not send anything, sendcount=0 only indicates that I do not send
       anything. However, other processes in my group might very well send
       something */


    OPAL_CR_ENTER_LIBRARY();

    /* Invoke the coll component to perform the back-end operation */
    err = comm->c_coll->coll_allgatherv(sendbuf, sendcount, sendtype,
                                       recvbuf, (int *) recvcounts,
                                       (int *) displs, recvtype, comm,
                                       comm->c_coll->coll_allgatherv_module);
    OMPI_ERRHANDLER_RETURN(err, comm, err, FUNC_NAME);
}

