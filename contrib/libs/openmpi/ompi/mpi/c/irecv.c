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
 * Copyright (c) 2006      Cisco Systems, Inc.  All rights reserved.
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
#pragma weak MPI_Irecv = PMPI_Irecv
#endif
#define MPI_Irecv PMPI_Irecv
#endif

static const char FUNC_NAME[] = "MPI_Irecv";


int MPI_Irecv(void *buf, int count, MPI_Datatype type, int source,
              int tag, MPI_Comm comm, MPI_Request *request)
{
    int rc = MPI_SUCCESS;

    SPC_RECORD(OMPI_SPC_IRECV, 1);

    MEMCHECKER(
        memchecker_datatype(type);
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
        } else if ((MPI_ANY_SOURCE != source) &&
                   (MPI_PROC_NULL != source) &&
                   ompi_comm_peer_invalid(comm, source)) {
            rc = MPI_ERR_RANK;
        } else if (NULL == request) {
            rc = MPI_ERR_REQUEST;
        }
        OMPI_ERRHANDLER_CHECK(rc, comm, rc, FUNC_NAME);
    }

    if (source == MPI_PROC_NULL) {
        *request = &ompi_request_empty;
        return MPI_SUCCESS;
    }

    OPAL_CR_ENTER_LIBRARY();

    MEMCHECKER (
        memchecker_call(&opal_memchecker_base_mem_noaccess, buf, count, type);
    );
    rc = MCA_PML_CALL(irecv(buf,count,type,source,tag,comm,request));
    OMPI_ERRHANDLER_RETURN(rc, comm, rc, FUNC_NAME);
}
