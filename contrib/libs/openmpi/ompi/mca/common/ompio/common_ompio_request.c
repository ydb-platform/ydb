/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2016 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2008-2018 University of Houston. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "common_ompio_request.h"
#if OPAL_CUDA_SUPPORT
#include "common_ompio_cuda.h"
#endif

static void mca_common_ompio_request_construct(mca_ompio_request_t* req);
static void mca_common_ompio_request_destruct(mca_ompio_request_t *req);

bool mca_common_ompio_progress_is_registered=false;
/*
 * Global list of requests for this component
 */
opal_list_t mca_common_ompio_pending_requests = {{0}};



static int mca_common_ompio_request_free ( struct ompi_request_t **req)
{
    mca_ompio_request_t *ompio_req = ( mca_ompio_request_t *)*req;
#if OPAL_CUDA_SUPPORT
    if ( NULL != ompio_req->req_tbuf ) {
        if ( MCA_OMPIO_REQUEST_READ == ompio_req->req_type ){
            struct iovec decoded_iov;
            uint32_t iov_count=1;
            size_t pos=0;

            decoded_iov.iov_base = ompio_req->req_tbuf;
            decoded_iov.iov_len  = ompio_req->req_size;
            opal_convertor_unpack (&ompio_req->req_convertor, &decoded_iov, &iov_count, &pos );
        }
        mca_common_ompio_release_buf ( NULL, ompio_req->req_tbuf );
    }
#endif
    if ( NULL != ompio_req->req_free_fn ) {
        ompio_req->req_free_fn (ompio_req );
    }
    opal_list_remove_item (&mca_common_ompio_pending_requests, &ompio_req->req_item);

    OBJ_RELEASE (*req);
    *req = MPI_REQUEST_NULL;
    return OMPI_SUCCESS;
}

static int mca_common_ompio_request_cancel ( struct ompi_request_t *req, int flag)
{
    return OMPI_SUCCESS;
}

OBJ_CLASS_INSTANCE(mca_ompio_request_t, ompi_request_t,
                   mca_common_ompio_request_construct,
                   mca_common_ompio_request_destruct);

void mca_common_ompio_request_construct(mca_ompio_request_t* req)
{
    OMPI_REQUEST_INIT (&(req->req_ompi), false );
    req->req_ompi.req_free   = mca_common_ompio_request_free;
    req->req_ompi.req_cancel = mca_common_ompio_request_cancel;
    req->req_ompi.req_type   = OMPI_REQUEST_IO;
    req->req_data            = NULL;
#if OPAL_CUDA_SUPPORT
    req->req_tbuf            = NULL;
    req->req_size            = 0;
#endif
    req->req_progress_fn     = NULL;
    req->req_free_fn         = NULL;

    OBJ_CONSTRUCT(&req->req_item, opal_list_item_t);
    opal_list_append (&mca_common_ompio_pending_requests, &req->req_item);
    return;
}
void mca_common_ompio_request_destruct(mca_ompio_request_t* req)
{
    OMPI_REQUEST_FINI ( &(req->req_ompi));
    OBJ_DESTRUCT (&req->req_item);
    if ( NULL != req->req_data ) {
        free (req->req_data);
    }

    return;
}

void mca_common_ompio_request_init ( void ) 
{
    /* Create the list of pending requests */
    OBJ_CONSTRUCT(&mca_common_ompio_pending_requests, opal_list_t);
    return;
}

void mca_common_ompio_request_fini ( void ) 
{
    /* Destroy the list of pending requests */
    /* JMS: Good opprotunity here to list out all the IO requests that
       were not destroyed / completed upon MPI_FINALIZE */

    OBJ_DESTRUCT(&mca_common_ompio_pending_requests);
    return;
}

void mca_common_ompio_request_alloc ( mca_ompio_request_t **req, mca_ompio_request_type_t type )
{
    mca_ompio_request_t *ompio_req = NULL;

    ompio_req = OBJ_NEW(mca_ompio_request_t);
    ompio_req->req_type = type;
    ompio_req->req_ompi.req_state = OMPI_REQUEST_ACTIVE;

    *req=ompio_req;

    return;
}

void mca_common_ompio_register_progress ( void ) 
{
    if ( false == mca_common_ompio_progress_is_registered) {
        opal_progress_register (mca_common_ompio_progress);
        mca_common_ompio_progress_is_registered=true;
    }
    return;
}
int mca_common_ompio_progress ( void )
{
    mca_ompio_request_t *req=NULL;
    opal_list_item_t *litem=NULL;
    int completed=0;

    OPAL_LIST_FOREACH(litem, &mca_common_ompio_pending_requests, opal_list_item_t) {
        req = GET_OMPIO_REQ_FROM_ITEM(litem);
        if( REQUEST_COMPLETE(&req->req_ompi) ) {
            continue;
        }
        if ( NULL != req->req_progress_fn ) {
            if ( req->req_progress_fn(req) ) {
                completed++;
                ompi_request_complete (&req->req_ompi, true);
                /* The fbtl progress function is expected to set the
                 * status elements
                 */
            }
        }

    }

    return completed;
}
