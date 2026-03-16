/*
 * Copyright (c) 2011      Sandia National Laboratories. All rights reserved.
 * Copyright (c) 2012 Cisco Systems, Inc.  All rights reserved.
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
#include "ompi/mca/pml/pml.h"
#include "ompi/memchecker.h"
#include "ompi/request/request.h"
#include "ompi/message/message.h"

#if OMPI_BUILD_MPI_PROFILING
#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak MPI_Imrecv = PMPI_Imrecv
#endif
#define MPI_Imrecv PMPI_Imrecv
#endif

static const char FUNC_NAME[] = "MPI_Imrecv";


int MPI_Imrecv(void *buf, int count, MPI_Datatype type,
               MPI_Message *message, MPI_Request *request)
{
    int rc = MPI_SUCCESS;
    ompi_communicator_t *comm;

    MEMCHECKER(
        memchecker_datatype(type);
        memchecker_message(message);
        memchecker_call(&opal_memchecker_base_isaddressable, buf, count, type);
        memchecker_comm(comm);
    );

    if ( MPI_PARAM_CHECK ) {
        OMPI_ERR_INIT_FINALIZE(FUNC_NAME);
        OMPI_CHECK_DATATYPE_FOR_RECV(rc, type, count);
        OMPI_CHECK_USER_BUFFER(rc, buf, type, count);

        if (NULL == message || MPI_MESSAGE_NULL == *message) {
            rc = MPI_ERR_REQUEST;
            comm = MPI_COMM_NULL;
        } else {
            comm = (*message)->comm;
        }

        OMPI_ERRHANDLER_CHECK(rc, comm, rc, FUNC_NAME);
    } else {
        comm = (*message)->comm;
    }

    if (&ompi_message_no_proc.message == *message) {
        *request = &ompi_request_empty;
        *message = MPI_MESSAGE_NULL;
        return MPI_SUCCESS;
     }

    OPAL_CR_ENTER_LIBRARY();

    rc = MCA_PML_CALL(imrecv(buf, count, type, message, request));
    OMPI_ERRHANDLER_RETURN(rc, comm, rc, FUNC_NAME);
}
