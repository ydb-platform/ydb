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
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
#include "ompi_config.h"

#include "ompi/mpi/c/bindings.h"
#include "ompi/runtime/params.h"
#include "ompi/communicator/communicator.h"
#include "ompi/errhandler/errhandler.h"
#include "ompi/mca/pml/pml.h"
#include "ompi/request/request.h"
#include "ompi/memchecker.h"
#include "ompi/runtime/ompi_spc.h"

#if OMPI_BUILD_MPI_PROFILING
#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak MPI_Sendrecv = PMPI_Sendrecv
#endif
#define MPI_Sendrecv PMPI_Sendrecv
#endif

static const char FUNC_NAME[] = "MPI_Sendrecv";


int MPI_Sendrecv(const void *sendbuf, int sendcount, MPI_Datatype sendtype,
                 int dest, int sendtag, void *recvbuf, int recvcount,
                 MPI_Datatype recvtype, int source, int recvtag,
                 MPI_Comm comm,  MPI_Status *status)
{
    ompi_request_t* req;
    int rc = MPI_SUCCESS;

    SPC_RECORD(OMPI_SPC_SENDRECV, 1);

    MEMCHECKER(
        memchecker_datatype(sendtype);
        memchecker_datatype(recvtype);
        memchecker_call(&opal_memchecker_base_isdefined, sendbuf, sendcount, sendtype);
        memchecker_comm(comm);
    );

    if ( MPI_PARAM_CHECK ) {
        OMPI_ERR_INIT_FINALIZE(FUNC_NAME);
        OMPI_CHECK_DATATYPE_FOR_SEND(rc, sendtype, sendcount);
        OMPI_CHECK_DATATYPE_FOR_RECV(rc, recvtype, recvcount);
        OMPI_CHECK_USER_BUFFER(rc, sendbuf, sendtype, sendcount);
        OMPI_CHECK_USER_BUFFER(rc, recvbuf, recvtype, recvcount);

        if (ompi_comm_invalid(comm)) {
            return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_COMM, FUNC_NAME);
        } else if (dest != MPI_PROC_NULL && ompi_comm_peer_invalid(comm, dest)) {
            rc = MPI_ERR_RANK;
        } else if (sendtag < 0 || sendtag > mca_pml.pml_max_tag) {
            rc = MPI_ERR_TAG;
        } else if (source != MPI_PROC_NULL && source != MPI_ANY_SOURCE && ompi_comm_peer_invalid(comm, source)) {
            rc = MPI_ERR_RANK;
        } else if (((recvtag < 0) && (recvtag !=  MPI_ANY_TAG)) || (recvtag > mca_pml.pml_max_tag)) {
            rc = MPI_ERR_TAG;
        }
        OMPI_ERRHANDLER_CHECK(rc, comm, rc, FUNC_NAME);
    }

    OPAL_CR_ENTER_LIBRARY();

    if (source != MPI_PROC_NULL) { /* post recv */
        rc = MCA_PML_CALL(irecv(recvbuf, recvcount, recvtype,
                                source, recvtag, comm, &req));
        OMPI_ERRHANDLER_CHECK(rc, comm, rc, FUNC_NAME);
    }

    if (dest != MPI_PROC_NULL) { /* send */
        rc = MCA_PML_CALL(send(sendbuf, sendcount, sendtype, dest,
                               sendtag, MCA_PML_BASE_SEND_STANDARD, comm));
        OMPI_ERRHANDLER_CHECK(rc, comm, rc, FUNC_NAME);
    }

    if (source != MPI_PROC_NULL) { /* wait for recv */
        rc = ompi_request_wait(&req, status);
    } else {
        if (MPI_STATUS_IGNORE != status) {
            *status = ompi_request_empty.req_status;
        }
        rc = MPI_SUCCESS;
    }
    OMPI_ERRHANDLER_RETURN(rc, comm, rc, FUNC_NAME);
}
