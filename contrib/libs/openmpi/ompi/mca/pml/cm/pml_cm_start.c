/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2006 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2006      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2016      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include "ompi/mca/pml/base/pml_base_request.h"
#include "ompi/mca/pml/base/pml_base_sendreq.h"
#include "ompi/mca/pml/base/pml_base_recvreq.h"

#include "pml_cm.h"
#include "pml_cm_sendreq.h"
#include "pml_cm_recvreq.h"


int
mca_pml_cm_start(size_t count, ompi_request_t** requests)
{
    int rc;

    for (size_t i = 0 ; i < count ; i++) {
        mca_pml_cm_request_t *pml_request = (mca_pml_cm_request_t*)requests[i];
        if (OMPI_REQUEST_PML != requests[i]->req_type || NULL == pml_request) {
            continue;
        }

        /* start the request */
        switch (pml_request->req_pml_type) {
        case MCA_PML_CM_REQUEST_SEND_HEAVY:
            {
                mca_pml_cm_hvy_send_request_t* sendreq =
                    (mca_pml_cm_hvy_send_request_t*)pml_request;
                if (!sendreq->req_send.req_base.req_pml_complete) {
                    ompi_request_t *request;

                    /* buffered sends can be mpi complete and pml incomplete. to support this
                     * case we need to allocate a new request. */
                    rc = mca_pml_cm_isend_init (sendreq->req_addr,
                                                sendreq->req_count,
                                                sendreq->req_send.req_base.req_datatype,
                                                sendreq->req_peer,
                                                sendreq->req_tag,
                                                sendreq->req_send.req_send_mode,
                                                sendreq->req_send.req_base.req_comm,
                                                &request);
                    if (OPAL_UNLIKELY(OMPI_SUCCESS != rc)) {
                        return rc;
                    }

                    /* copy the callback and callback data to the new requests */
                    request->req_complete_cb = pml_request->req_ompi.req_complete_cb;
                    request->req_complete_cb_data = pml_request->req_ompi.req_complete_cb_data;

                    /* ensure the old request gets released */
                    pml_request->req_free_called = true;

                    sendreq = (mca_pml_cm_hvy_send_request_t *) request;
                    requests[i] = request;
                }

                /* reset the completion flag */
                sendreq->req_send.req_base.req_pml_complete = false;

                MCA_PML_CM_HVY_SEND_REQUEST_START(sendreq, rc);
                if(rc != OMPI_SUCCESS)
                    return rc;
                break;
            }
        case MCA_PML_CM_REQUEST_RECV_HEAVY:
            {
                mca_pml_cm_hvy_recv_request_t* recvreq =
                    (mca_pml_cm_hvy_recv_request_t*)pml_request;
                MCA_PML_CM_HVY_RECV_REQUEST_START(recvreq, rc);
                if(rc != OMPI_SUCCESS)
                    return rc;
                break;
            }
        default:
            return OMPI_ERR_REQUEST;
        }
    }
    return OMPI_SUCCESS;
}
