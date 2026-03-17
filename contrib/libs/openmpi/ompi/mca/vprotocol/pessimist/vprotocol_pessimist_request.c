/*
 * Copyright (c) 2004-2007 The Trustees of the University of Tennessee.
 *                         All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"
#include "vprotocol_pessimist_request.h"
#include "vprotocol_pessimist_eventlog.h"
#include "ompi/mca/pml/base/pml_base_request.h"

static void vprotocol_pessimist_request_construct(mca_pml_base_request_t *req);

OBJ_CLASS_INSTANCE(mca_vprotocol_pessimist_recv_request_t, mca_pml_base_request_t,
                   vprotocol_pessimist_request_construct, NULL);
OBJ_CLASS_INSTANCE(mca_vprotocol_pessimist_send_request_t, mca_pml_base_request_t,
                   vprotocol_pessimist_request_construct, NULL);


static void vprotocol_pessimist_request_construct(mca_pml_base_request_t *req)
{
    mca_vprotocol_pessimist_request_t *ftreq;

    ftreq = VPESSIMIST_FTREQ(req);
    V_OUTPUT_VERBOSE(250, "pessimist:\treq\tnew\treq=%p\tPreq=%p (aligned to %p)", (void *) req, (void *) ftreq, (void *) &ftreq->pml_req_free);
    req->req_ompi.req_status.MPI_SOURCE = -1; /* no matching made flag */
    ftreq->pml_req_free = req->req_ompi.req_free;
    ftreq->event = NULL;
    ftreq->sb.bytes_progressed = 0;
    assert(ftreq->pml_req_free == req->req_ompi.req_free); /* detection of aligment issues on different arch */
    req->req_ompi.req_free = mca_vprotocol_pessimist_request_free;
    OBJ_CONSTRUCT(& ftreq->list_item, opal_list_item_t);
}

int mca_vprotocol_pessimist_request_free(ompi_request_t **req)
{
    mca_pml_base_request_t *pmlreq = (mca_pml_base_request_t *) *req;
    V_OUTPUT_VERBOSE(50, "pessimist:\treq\tfree\t%"PRIpclock"\tpeer %d\ttag %d\tsize %lu", VPESSIMIST_FTREQ(pmlreq)->reqid, pmlreq->req_peer, pmlreq->req_tag, (unsigned long) pmlreq->req_count);
    vprotocol_pessimist_matching_log_finish(*req);
    pmlreq->req_ompi.req_status.MPI_SOURCE = -1; /* no matching made flag */
    vprotocol_pessimist_sender_based_flush(*req);
    return VPESSIMIST_FTREQ(pmlreq)->pml_req_free(req);
}
