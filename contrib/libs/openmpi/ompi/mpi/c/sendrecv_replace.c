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
 * Copyright (c) 2010-2012 Oracle and/or its affiliates.  All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2017      IBM Corporation.  All rights reserved.
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
#include "ompi/datatype/ompi_datatype.h"
#include "opal/datatype/opal_convertor.h"
#include "ompi/mca/pml/pml.h"
#include "ompi/proc/proc.h"
#include "ompi/memchecker.h"
#include "ompi/runtime/ompi_spc.h"

#if OMPI_BUILD_MPI_PROFILING
#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak MPI_Sendrecv_replace = PMPI_Sendrecv_replace
#endif
#define MPI_Sendrecv_replace PMPI_Sendrecv_replace
#endif

static const char FUNC_NAME[] = "MPI_Sendrecv_replace";


int MPI_Sendrecv_replace(void * buf, int count, MPI_Datatype datatype,
                         int dest, int sendtag, int source, int recvtag,
                         MPI_Comm comm, MPI_Status *status)

{
    int rc = MPI_SUCCESS;

    SPC_RECORD(OMPI_SPC_SENDRECV_REPLACE, 1);

    MEMCHECKER(
               memchecker_datatype(datatype);
               memchecker_call(&opal_memchecker_base_isdefined, buf, count, datatype);
               memchecker_comm(comm);
               );

    if ( MPI_PARAM_CHECK ) {
        rc = MPI_SUCCESS;
        OMPI_ERR_INIT_FINALIZE(FUNC_NAME);
        OMPI_CHECK_DATATYPE_FOR_RECV(rc, datatype, count);

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

    /* simple case */
    if ( source == MPI_PROC_NULL || dest == MPI_PROC_NULL || count == 0 ) {
        rc = PMPI_Sendrecv(buf, count, datatype, dest, sendtag, buf, count, datatype, source, recvtag, comm, status);

        OPAL_CR_EXIT_LIBRARY();
        return rc;
    }

    /**
     * If we look for an optimal solution, then we should receive the data into a temporary buffer
     * and once the send completes we would unpack back into the original buffer. However, if the
     * sender is unknown, this approach can only be implementing by receiving with the recv datatype
     * (potentially non-contiguous) and thus the allocated memory will be larger than the size of the
     * datatype. A simpler, but potentially less efficient approach is to work on the data we have
     * control of, aka the sent data, and pack it into a contiguous buffer before posting the receive.
     * Once the send completes, we free it.
     */
    opal_convertor_t convertor;
    unsigned char packed_data[2048];
    struct iovec iov = { .iov_base = packed_data, .iov_len = sizeof(packed_data) };
    size_t packed_size, max_data;
    uint32_t iov_count;
    ompi_status_public_t recv_status;
    ompi_proc_t* proc = ompi_comm_peer_lookup(comm, dest);
    if(proc == NULL) {
        rc = MPI_ERR_RANK;
        OMPI_ERRHANDLER_RETURN(rc, comm, rc, FUNC_NAME);
    }

    /* initialize convertor to unpack recv buffer */
    OBJ_CONSTRUCT(&convertor, opal_convertor_t);
    opal_convertor_copy_and_prepare_for_send( proc->super.proc_convertor, &(datatype->super),
                                              count, buf, 0, &convertor );

    /* setup a buffer for recv */
    opal_convertor_get_packed_size( &convertor, &packed_size );
    if( packed_size > sizeof(packed_data) ) {
        rc = PMPI_Alloc_mem(packed_size, MPI_INFO_NULL, &iov.iov_base);
        if(OMPI_SUCCESS != rc) {
            rc = OMPI_ERR_OUT_OF_RESOURCE;
            goto cleanup_and_return;
        }
        iov.iov_len = packed_size;
    }
    max_data = packed_size;
    iov_count = 1;
    rc = opal_convertor_pack(&convertor, &iov, &iov_count, &max_data);
    
    /* recv into temporary buffer */
    rc = PMPI_Sendrecv( iov.iov_base, packed_size, MPI_PACKED, dest, sendtag, buf, count,
                        datatype, source, recvtag, comm, &recv_status );

 cleanup_and_return:
    /* return status to user */
    if(status != MPI_STATUS_IGNORE) {
        *status = recv_status;
    }

    /* release resources */
    if(packed_size > sizeof(packed_data)) {
        PMPI_Free_mem(iov.iov_base);
    }
    OBJ_DESTRUCT(&convertor);

    OPAL_CR_EXIT_LIBRARY();
    OMPI_ERRHANDLER_RETURN(rc, comm, rc, FUNC_NAME);
}
