/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2007 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2006 The Regents of the University of California.
 *                         All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include "pml_cm.h"
#include "pml_cm_recvreq.h"

static int
mca_pml_cm_recv_request_free(struct ompi_request_t** request)
{
    mca_pml_cm_request_t* recvreq = *(mca_pml_cm_request_t**)request;

    assert( false == recvreq->req_free_called );

    recvreq->req_free_called = true;
    if( true == recvreq->req_pml_complete ) {
        if( MCA_PML_CM_REQUEST_RECV_THIN == recvreq->req_pml_type ) {
            MCA_PML_CM_THIN_RECV_REQUEST_RETURN((mca_pml_cm_hvy_recv_request_t*)recvreq );
        } else {
            MCA_PML_CM_HVY_RECV_REQUEST_RETURN((mca_pml_cm_hvy_recv_request_t*)recvreq );
        }
    }

    *request = MPI_REQUEST_NULL;
    return OMPI_SUCCESS;
}


void mca_pml_cm_recv_request_completion(struct mca_mtl_request_t *mtl_request)
{
    mca_pml_cm_request_t *base_request =
        (mca_pml_cm_request_t*) mtl_request->ompi_req;
    if( MCA_PML_CM_REQUEST_RECV_THIN == base_request->req_pml_type ) {
        MCA_PML_CM_THIN_RECV_REQUEST_PML_COMPLETE(((mca_pml_cm_thin_recv_request_t*)base_request));
    } else {
        MCA_PML_CM_HVY_RECV_REQUEST_PML_COMPLETE(((mca_pml_cm_hvy_recv_request_t*)base_request));
    }
}

static void
mca_pml_cm_recv_request_construct(mca_pml_cm_thin_recv_request_t* recvreq)
{
    recvreq->req_base.req_ompi.req_start = mca_pml_cm_start;
    recvreq->req_base.req_ompi.req_free = mca_pml_cm_recv_request_free;
    recvreq->req_base.req_ompi.req_cancel = mca_pml_cm_cancel;
    OBJ_CONSTRUCT( &(recvreq->req_base.req_convertor), opal_convertor_t );
}


OBJ_CLASS_INSTANCE(mca_pml_cm_thin_recv_request_t,
                   mca_pml_cm_request_t,
                   mca_pml_cm_recv_request_construct,
                   NULL);


OBJ_CLASS_INSTANCE(mca_pml_cm_hvy_recv_request_t,
                   mca_pml_cm_request_t,
                   mca_pml_cm_recv_request_construct,
                   NULL);
