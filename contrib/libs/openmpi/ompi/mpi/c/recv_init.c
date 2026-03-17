/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2016 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2006      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2018      FUJITSU LIMITED.  All rights reserved.
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

#if OMPI_BUILD_MPI_PROFILING
#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak MPI_Recv_init = PMPI_Recv_init
#endif
#define MPI_Recv_init PMPI_Recv_init
#endif

static const char FUNC_NAME[] = "MPI_Recv_init";


int MPI_Recv_init(void *buf, int count, MPI_Datatype type, int source,
                  int tag, MPI_Comm comm, MPI_Request *request)
{
    int rc = MPI_SUCCESS;

    MEMCHECKER(
        memchecker_datatype(type);
        memchecker_call(&opal_memchecker_base_isaddressable, buf, count, type);
        memchecker_comm(comm);
    );

    if ( MPI_PARAM_CHECK ) {
        OMPI_ERR_INIT_FINALIZE(FUNC_NAME);
        OMPI_CHECK_DATATYPE_FOR_RECV(rc, type, count);
        OMPI_CHECK_USER_BUFFER(rc, buf, type, count);

        if (ompi_comm_invalid(comm)) {
            return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_COMM, FUNC_NAME);
        } else if (((tag < 0) && (tag != MPI_ANY_TAG)) || (tag > mca_pml.pml_max_tag)) {
            rc = MPI_ERR_TAG;
        } else if ((source != MPI_ANY_SOURCE) &&
                   (MPI_PROC_NULL != source) &&
                   ompi_comm_peer_invalid(comm, source)) {
            rc = MPI_ERR_RANK;
        } else if (NULL == request) {
            rc = MPI_ERR_REQUEST;
        }

        OMPI_ERRHANDLER_CHECK(rc, comm, rc, FUNC_NAME);
    }

    if (MPI_PROC_NULL == source) {
        rc = ompi_request_persistent_noop_create(request);
        OMPI_ERRHANDLER_RETURN(rc, comm, rc, FUNC_NAME);
    }

    OPAL_CR_ENTER_LIBRARY();

    /*
     * Here, we just initialize the request -- memchecker should set the buffer in MPI_Start.
     */
    rc = MCA_PML_CALL(irecv_init(buf,count,type,source,tag,comm,request));
    OMPI_ERRHANDLER_RETURN(rc, comm, rc, FUNC_NAME);
}
