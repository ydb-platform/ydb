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
#include "pml_cm_sendreq.h"


/*
 * The free call mark the final stage in a request
 * life-cycle. Starting from this point the request is completed at
 * both PML and user level, and can be used for others p2p
 * communications. Therefore, in the case of the CM PML it should be
 * added to the free request list.
 */
static int
mca_pml_cm_send_request_free(struct ompi_request_t** request)
{
    mca_pml_cm_send_request_t* sendreq = *(mca_pml_cm_send_request_t**)request;
    assert( false == sendreq->req_base.req_free_called );

    sendreq->req_base.req_free_called = true;
    if( true == sendreq->req_base.req_pml_complete ) {
        if( MCA_PML_CM_REQUEST_SEND_THIN == sendreq->req_base.req_pml_type ) {
            MCA_PML_CM_THIN_SEND_REQUEST_RETURN( ((mca_pml_cm_thin_send_request_t*)  sendreq) );
        } else {
            MCA_PML_CM_HVY_SEND_REQUEST_RETURN( ((mca_pml_cm_hvy_send_request_t*)  sendreq) );
        }
    }

    *request = MPI_REQUEST_NULL;
    return OMPI_SUCCESS;
}

void
mca_pml_cm_send_request_completion(struct mca_mtl_request_t *mtl_request)
{
    mca_pml_cm_send_request_t *base_request =
        (mca_pml_cm_send_request_t*) mtl_request->ompi_req;
    if( MCA_PML_CM_REQUEST_SEND_THIN == base_request->req_base.req_pml_type ) {
        MCA_PML_CM_THIN_SEND_REQUEST_PML_COMPLETE(((mca_pml_cm_thin_send_request_t*) base_request));
    } else {
        MCA_PML_CM_HVY_SEND_REQUEST_PML_COMPLETE(((mca_pml_cm_hvy_send_request_t*) base_request));
    }
}

static void mca_pml_cm_send_request_construct(mca_pml_cm_hvy_send_request_t* sendreq)
{
    /* no need to reinit for every send -- never changes */
    sendreq->req_send.req_base.req_ompi.req_start = mca_pml_cm_start;
    sendreq->req_send.req_base.req_ompi.req_free = mca_pml_cm_send_request_free;
    sendreq->req_send.req_base.req_ompi.req_cancel = mca_pml_cm_cancel;
}

OBJ_CLASS_INSTANCE(mca_pml_cm_send_request_t,
                   mca_pml_cm_request_t,
                   NULL,
                   NULL);

OBJ_CLASS_INSTANCE(mca_pml_cm_thin_send_request_t,
                   mca_pml_cm_send_request_t,
                   mca_pml_cm_send_request_construct,
                   NULL);

OBJ_CLASS_INSTANCE(mca_pml_cm_hvy_send_request_t,
                   mca_pml_cm_send_request_t,
                   mca_pml_cm_send_request_construct,
                   NULL);

