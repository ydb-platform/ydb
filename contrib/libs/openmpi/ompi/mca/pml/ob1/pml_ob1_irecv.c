/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2015 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007-2016 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2010-2012 Oracle and/or its affiliates.  All rights reserved.
 * Copyright (c) 2011      Sandia National Laboratories. All rights reserved.
 * Copyright (c) 2014      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2016-2017 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"
#include "ompi/request/request.h"
#include "pml_ob1_recvreq.h"
#include "pml_ob1_recvfrag.h"
#include "ompi/peruse/peruse-internal.h"
#include "ompi/message/message.h"
#include "ompi/memchecker.h"

/**
 * Single usage request. As we allow recursive calls to recv
 * (from the request completion callback), we cannot rely on
 * using a global request. Thus, once a recv acquires ownership
 * this global request, it should set it to NULL to prevent
 * the reuse until the first user completes.
 */
mca_pml_ob1_recv_request_t *mca_pml_ob1_recvreq = NULL;

int mca_pml_ob1_irecv_init(void *addr,
                           size_t count,
                           ompi_datatype_t * datatype,
                           int src,
                           int tag,
                           struct ompi_communicator_t *comm,
                           struct ompi_request_t **request)
{
    mca_pml_ob1_recv_request_t *recvreq;
    MCA_PML_OB1_RECV_REQUEST_ALLOC(recvreq);
    if (NULL == recvreq)
        return OMPI_ERR_TEMP_OUT_OF_RESOURCE;

    recvreq->req_recv.req_base.req_type = MCA_PML_REQUEST_RECV;
    MCA_PML_OB1_RECV_REQUEST_INIT(recvreq,
                                   addr,
                                   count, datatype, src, tag, comm, true);

    PERUSE_TRACE_COMM_EVENT (PERUSE_COMM_REQ_ACTIVATE,
                             &((recvreq)->req_recv.req_base),
                             PERUSE_RECV);

    /* Work around a leak in start by marking this request as complete. The
     * problem occured because we do not have a way to differentiate an
     * inital request and an incomplete pml request in start. This line
     * allows us to detect this state. */
    recvreq->req_recv.req_base.req_pml_complete = true;

    *request = (ompi_request_t *) recvreq;
    return OMPI_SUCCESS;
}

int mca_pml_ob1_irecv(void *addr,
                      size_t count,
                      ompi_datatype_t * datatype,
                      int src,
                      int tag,
                      struct ompi_communicator_t *comm,
                      struct ompi_request_t **request)
{
    mca_pml_ob1_recv_request_t *recvreq;
    MCA_PML_OB1_RECV_REQUEST_ALLOC(recvreq);
    if (NULL == recvreq)
        return OMPI_ERR_TEMP_OUT_OF_RESOURCE;

    recvreq->req_recv.req_base.req_type = MCA_PML_REQUEST_RECV;
    MCA_PML_OB1_RECV_REQUEST_INIT(recvreq,
                                   addr,
                                   count, datatype, src, tag, comm, false);

    PERUSE_TRACE_COMM_EVENT (PERUSE_COMM_REQ_ACTIVATE,
                             &((recvreq)->req_recv.req_base),
                             PERUSE_RECV);

    MCA_PML_OB1_RECV_REQUEST_START(recvreq);
    *request = (ompi_request_t *) recvreq;
    return OMPI_SUCCESS;
}


int mca_pml_ob1_recv(void *addr,
                     size_t count,
                     ompi_datatype_t * datatype,
                     int src,
                     int tag,
                     struct ompi_communicator_t *comm,
                     ompi_status_public_t * status)
{
    mca_pml_ob1_recv_request_t *recvreq = NULL;
    int rc;

    if (OPAL_LIKELY(!ompi_mpi_thread_multiple)) {
        recvreq = mca_pml_ob1_recvreq;
        mca_pml_ob1_recvreq = NULL;
    }

    if( OPAL_UNLIKELY(NULL == recvreq) ) {
        MCA_PML_OB1_RECV_REQUEST_ALLOC(recvreq);
        if (NULL == recvreq)
            return OMPI_ERR_TEMP_OUT_OF_RESOURCE;
    }

    recvreq->req_recv.req_base.req_type = MCA_PML_REQUEST_RECV;
    MCA_PML_OB1_RECV_REQUEST_INIT(recvreq, addr, count, datatype,
                                  src, tag, comm, false);

    PERUSE_TRACE_COMM_EVENT (PERUSE_COMM_REQ_ACTIVATE,
                             &(recvreq->req_recv.req_base),
                             PERUSE_RECV);

    MCA_PML_OB1_RECV_REQUEST_START(recvreq);
    ompi_request_wait_completion(&recvreq->req_recv.req_base.req_ompi);

    if( true == recvreq->req_recv.req_base.req_pml_complete ) {
        /* make buffer defined when the request is compeleted */
        MEMCHECKER(
            memchecker_call(&opal_memchecker_base_mem_defined,
                            recvreq->req_recv.req_base.req_addr,
                            recvreq->req_recv.req_base.req_count,
                            recvreq->req_recv.req_base.req_datatype);
        );
    }

    if (NULL != status) {  /* return status */
        *status = recvreq->req_recv.req_base.req_ompi.req_status;
    }

    rc = recvreq->req_recv.req_base.req_ompi.req_status.MPI_ERROR;

    if (recvreq->req_recv.req_base.req_pml_complete) {
        /* make buffer defined when the request is compeleted,
           and before releasing the objects. */
        MEMCHECKER(
            memchecker_call(&opal_memchecker_base_mem_defined,
                            recvreq->req_recv.req_base.req_addr,
                            recvreq->req_recv.req_base.req_count,
                            recvreq->req_recv.req_base.req_datatype);
        );
    }

    if (OPAL_UNLIKELY(ompi_mpi_thread_multiple || NULL != mca_pml_ob1_recvreq)) {
        MCA_PML_OB1_RECV_REQUEST_RETURN(recvreq);
    } else {
        mca_pml_ob1_recv_request_fini (recvreq);
        mca_pml_ob1_recvreq = recvreq;
    }

    return rc;
}


int
mca_pml_ob1_imrecv( void *buf,
                    size_t count,
                    ompi_datatype_t *datatype,
                    struct ompi_message_t **message,
                    struct ompi_request_t **request )
{
    mca_pml_ob1_recv_frag_t* frag;
    mca_pml_ob1_recv_request_t *recvreq;
    mca_pml_ob1_hdr_t *hdr;
    int src, tag;
    ompi_communicator_t *comm;
    mca_pml_ob1_comm_proc_t* proc;
    uint64_t seq;

    /* get the request from the message and the frag from the request
       before we overwrite everything */
    recvreq = (mca_pml_ob1_recv_request_t*) (*message)->req_ptr;
    frag = (mca_pml_ob1_recv_frag_t*) recvreq->req_recv.req_base.req_addr;
    src = recvreq->req_recv.req_base.req_ompi.req_status.MPI_SOURCE;
    tag = recvreq->req_recv.req_base.req_ompi.req_status.MPI_TAG;
    comm = (*message)->comm;
    seq = recvreq->req_recv.req_base.req_sequence;

    /* make the request a recv request again */
    /* The old request kept pointers to comm and the char datatype.
       We're about to release those, but need to make sure comm
       doesn't go out of scope (we don't care about the char datatype
       anymore).  So retain comm, then release the frag, then reinit
       the frag (which will retain comm), then release comm (but the
       frag still has it's ref, so it'll stay in scope).  Make
       sense? */
    OBJ_RETAIN(comm);
    MCA_PML_BASE_RECV_REQUEST_FINI(&recvreq->req_recv);
    recvreq->req_recv.req_base.req_type = MCA_PML_REQUEST_RECV;
    MCA_PML_OB1_RECV_REQUEST_INIT(recvreq,
                                  buf,
                                  count, datatype,
                                  src, tag, comm, false);
    OBJ_RELEASE(comm);

    PERUSE_TRACE_COMM_EVENT (PERUSE_COMM_REQ_ACTIVATE,
                             &((recvreq)->req_recv.req_base),
                             PERUSE_RECV);

    /* init/re-init the request */
    recvreq->req_lock = 0;
    recvreq->req_pipeline_depth  = 0;
    recvreq->req_bytes_received  = 0;
    /* What about req_rdma_cnt ? */
    recvreq->req_rdma_idx = 0;
    recvreq->req_pending = false;
    recvreq->req_ack_sent = false;

    MCA_PML_BASE_RECV_START(&recvreq->req_recv);

    /* Note - sequence number already assigned */
    recvreq->req_recv.req_base.req_sequence = seq;

    proc = mca_pml_ob1_peer_lookup (comm, recvreq->req_recv.req_base.req_peer);
    recvreq->req_recv.req_base.req_proc = proc->ompi_proc;
    prepare_recv_req_converter(recvreq);

    /* we can't go through the match, since we already have the match.
       Cheat and do what REQUEST_START does, but without the frag
       search */
    hdr = (mca_pml_ob1_hdr_t*)frag->segments->seg_addr.pval;
    switch(hdr->hdr_common.hdr_type) {
    case MCA_PML_OB1_HDR_TYPE_MATCH:
        mca_pml_ob1_recv_request_progress_match(recvreq, frag->btl, frag->segments,
                                                frag->num_segments);
        break;
    case MCA_PML_OB1_HDR_TYPE_RNDV:
        mca_pml_ob1_recv_request_progress_rndv(recvreq, frag->btl, frag->segments,
                                               frag->num_segments);
        break;
    case MCA_PML_OB1_HDR_TYPE_RGET:
        mca_pml_ob1_recv_request_progress_rget(recvreq, frag->btl, frag->segments,
                                               frag->num_segments);
        break;
    default:
        assert(0);
    }
    MCA_PML_OB1_RECV_FRAG_RETURN(frag);

    ompi_message_return(*message);
    *message = MPI_MESSAGE_NULL;
    *request = (ompi_request_t *) recvreq;

    return OMPI_SUCCESS;
}


int
mca_pml_ob1_mrecv( void *buf,
                   size_t count,
                   ompi_datatype_t *datatype,
                   struct ompi_message_t **message,
                   ompi_status_public_t* status )
{
    mca_pml_ob1_recv_frag_t* frag;
    mca_pml_ob1_recv_request_t *recvreq;
    mca_pml_ob1_hdr_t *hdr;
    int src, tag, rc;
    ompi_communicator_t *comm;
    mca_pml_ob1_comm_proc_t* proc;
    uint64_t seq;

    /* get the request from the message and the frag from the request
       before we overwrite everything */
    comm = (*message)->comm;
    recvreq = (mca_pml_ob1_recv_request_t*) (*message)->req_ptr;
    frag = (mca_pml_ob1_recv_frag_t*) recvreq->req_recv.req_base.req_addr;
    src = recvreq->req_recv.req_base.req_ompi.req_status.MPI_SOURCE;
    tag = recvreq->req_recv.req_base.req_ompi.req_status.MPI_TAG;
    seq = recvreq->req_recv.req_base.req_sequence;

    /* make the request a recv request again */
    /* The old request kept pointers to comm and the char datatype.
       We're about to release those, but need to make sure comm
       doesn't go out of scope (we don't care about the char datatype
       anymore).  So retain comm, then release the frag, then reinit
       the frag (which will retain comm), then release comm (but the
       frag still has it's ref, so it'll stay in scope).  Make
       sense? */
    OBJ_RETAIN(comm);
    MCA_PML_BASE_RECV_REQUEST_FINI(&recvreq->req_recv);
    recvreq->req_recv.req_base.req_type = MCA_PML_REQUEST_RECV;
    MCA_PML_OB1_RECV_REQUEST_INIT(recvreq,
                                  buf,
                                  count, datatype,
                                  src, tag, comm, false);
    OBJ_RELEASE(comm);

    PERUSE_TRACE_COMM_EVENT (PERUSE_COMM_REQ_ACTIVATE,
                             &((recvreq)->req_recv.req_base),
                             PERUSE_RECV);

    /* init/re-init the request */
    recvreq->req_lock = 0;
    recvreq->req_pipeline_depth  = 0;
    recvreq->req_bytes_received  = 0;
    recvreq->req_rdma_cnt = 0;
    recvreq->req_rdma_idx = 0;
    recvreq->req_pending = false;

    MCA_PML_BASE_RECV_START(&recvreq->req_recv);

    /* Note - sequence number already assigned */
    recvreq->req_recv.req_base.req_sequence = seq;

    proc = mca_pml_ob1_peer_lookup (comm, recvreq->req_recv.req_base.req_peer);
    recvreq->req_recv.req_base.req_proc = proc->ompi_proc;
    prepare_recv_req_converter(recvreq);

    /* we can't go through the match, since we already have the match.
       Cheat and do what REQUEST_START does, but without the frag
       search */
    hdr = (mca_pml_ob1_hdr_t*)frag->segments->seg_addr.pval;
    switch(hdr->hdr_common.hdr_type) {
    case MCA_PML_OB1_HDR_TYPE_MATCH:
        mca_pml_ob1_recv_request_progress_match(recvreq, frag->btl, frag->segments,
                                                frag->num_segments);
        break;
    case MCA_PML_OB1_HDR_TYPE_RNDV:
        mca_pml_ob1_recv_request_progress_rndv(recvreq, frag->btl, frag->segments,
                                               frag->num_segments);
        break;
    case MCA_PML_OB1_HDR_TYPE_RGET:
        mca_pml_ob1_recv_request_progress_rget(recvreq, frag->btl, frag->segments,
                                               frag->num_segments);
        break;
    default:
        assert(0);
    }

    ompi_message_return(*message);
    *message = MPI_MESSAGE_NULL;
    ompi_request_wait_completion(&(recvreq->req_recv.req_base.req_ompi));

    MCA_PML_OB1_RECV_FRAG_RETURN(frag);

    if (NULL != status) {  /* return status */
        *status = recvreq->req_recv.req_base.req_ompi.req_status;
    }
    rc = recvreq->req_recv.req_base.req_ompi.req_status.MPI_ERROR;
    ompi_request_free( (ompi_request_t**)&recvreq );
    return rc;
}

