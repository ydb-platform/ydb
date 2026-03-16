/*
 * Copyright (c) 2011      Sandia National Laboratories. All rights reserved.
 * Copyright (c) 2012-2013 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2018      The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
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
#include "ompi/runtime/ompi_spc.h"

#if OMPI_BUILD_MPI_PROFILING
#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak MPI_Mrecv = PMPI_Mrecv
#endif
#define MPI_Mrecv PMPI_Mrecv
#endif

static const char FUNC_NAME[] = "MPI_Mrecv";


int MPI_Mrecv(void *buf, int count, MPI_Datatype type,
              MPI_Message *message, MPI_Status *status)
{
    int rc = MPI_SUCCESS;
    ompi_communicator_t *comm;

    SPC_RECORD(OMPI_SPC_MRECV, 1);

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
        if (MPI_STATUS_IGNORE != status) {
            *status = ompi_request_empty.req_status;
        }
        *message = MPI_MESSAGE_NULL;
        return MPI_SUCCESS;
     }

    OPAL_CR_ENTER_LIBRARY();

    rc = MCA_PML_CALL(mrecv(buf, count, type, message, status));
    /* Per MPI-1, the MPI_ERROR field is not defined for
       single-completion calls */
    MEMCHECKER(
        opal_memchecker_base_mem_undefined(&status->MPI_ERROR, sizeof(int));
    );

    OMPI_ERRHANDLER_RETURN(rc, comm, rc, FUNC_NAME);
}
