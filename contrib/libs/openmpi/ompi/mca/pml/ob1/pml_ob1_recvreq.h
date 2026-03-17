/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2016 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2007 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2008      UT-Battelle, LLC. All rights reserved.
 * Copyright (c) 2011-2015 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2014      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 *
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/**
 * @file
 */
#ifndef OMPI_PML_OB1_RECV_REQUEST_H
#define OMPI_PML_OB1_RECV_REQUEST_H

#include "pml_ob1.h"
#include "pml_ob1_rdma.h"
#include "pml_ob1_rdmafrag.h"
#include "ompi/proc/proc.h"
#include "ompi/mca/pml/ob1/pml_ob1_comm.h"
#include "opal/mca/mpool/base/base.h"
#include "ompi/mca/pml/base/pml_base_recvreq.h"

BEGIN_C_DECLS

struct mca_pml_ob1_recv_request_t {
    mca_pml_base_recv_request_t req_recv;
    opal_ptr_t remote_req_send;
    int32_t  req_lock;
    int32_t  req_pipeline_depth;
    size_t   req_bytes_received;  /**< amount of data transferred into the user buffer */
    size_t   req_bytes_expected; /**< local size of the data as suggested by the user */
    size_t   req_rdma_offset;
    size_t   req_send_offset;
    uint32_t req_rdma_cnt;
    uint32_t req_rdma_idx;
    bool req_pending;
    bool req_ack_sent; /**< whether ack was sent to the sender */
    bool req_match_received; /**< Prevent request to be completed prematurely */
    opal_mutex_t lock;
    mca_bml_base_btl_t *rdma_bml;
    mca_btl_base_registration_handle_t *local_handle;
    /** The size of this array is set from mca_pml_ob1.max_rdma_per_request */
    mca_pml_ob1_com_btl_t req_rdma[];
};
typedef struct mca_pml_ob1_recv_request_t mca_pml_ob1_recv_request_t;

OBJ_CLASS_DECLARATION(mca_pml_ob1_recv_request_t);

static inline bool lock_recv_request(mca_pml_ob1_recv_request_t *recvreq)
{
        return OPAL_THREAD_ADD_FETCH32(&recvreq->req_lock,  1) == 1;
}

static inline bool unlock_recv_request(mca_pml_ob1_recv_request_t *recvreq)
{
        return OPAL_THREAD_ADD_FETCH32(&recvreq->req_lock, -1) == 0;
}

/**
 *  Allocate a recv request from the modules free list.
 *
 *  @param rc (OUT)  OMPI_SUCCESS or error status on failure.
 *  @return          Receive request.
 */
#define MCA_PML_OB1_RECV_REQUEST_ALLOC(recvreq)                    \
do {                                                               \
    recvreq = (mca_pml_ob1_recv_request_t *)                          \
        opal_free_list_get (&mca_pml_base_recv_requests);             \
} while(0)


/**
 * Initialize a receive request with call parameters.
 *
 * @param request (IN)       Receive request.
 * @param addr (IN)          User buffer.
 * @param count (IN)         Number of elements of indicated datatype.
 * @param datatype (IN)      User defined datatype.
 * @param src (IN)           Source rank w/in the communicator.
 * @param tag (IN)           User defined tag.
 * @param comm (IN)          Communicator.
 * @param persistent (IN)    Is this a ersistent request.
 */
#define MCA_PML_OB1_RECV_REQUEST_INIT( request,                     \
                                       addr,                        \
                                       count,                       \
                                       datatype,                    \
                                       src,                         \
                                       tag,                         \
                                       comm,                        \
                                       persistent)                  \
do {                                                                \
    MCA_PML_BASE_RECV_REQUEST_INIT( &(request)->req_recv,           \
                                    addr,                           \
                                    count,                          \
                                    datatype,                       \
                                    src,                            \
                                    tag,                            \
                                    comm,                           \
                                    persistent);                    \
} while(0)

/**
 * Mark the request as completed at MPI level for internal purposes.
 *
 *  @param recvreq (IN)  Receive request.
 */
#define MCA_PML_OB1_RECV_REQUEST_MPI_COMPLETE( recvreq )                              \
    do {                                                                              \
        PERUSE_TRACE_COMM_EVENT( PERUSE_COMM_REQ_COMPLETE,                            \
                                 &(recvreq->req_recv.req_base), PERUSE_RECV );        \
        ompi_request_complete( &(recvreq->req_recv.req_base.req_ompi), true );        \
    } while (0)

static inline void mca_pml_ob1_recv_request_fini (mca_pml_ob1_recv_request_t *recvreq)
{
    MCA_PML_BASE_RECV_REQUEST_FINI(&recvreq->req_recv);
    if ((recvreq)->local_handle) {
        mca_bml_base_deregister_mem (recvreq->rdma_bml, recvreq->local_handle);
        recvreq->local_handle = NULL;
    }
}

/*
 *  Free the PML receive request
 */
#define MCA_PML_OB1_RECV_REQUEST_RETURN(recvreq)                        \
    {                                                                   \
        mca_pml_ob1_recv_request_fini (recvreq);                        \
        opal_free_list_return (&mca_pml_base_recv_requests,             \
                               (opal_free_list_item_t*)(recvreq));      \
    }

/**
 * Complete receive request. Request structure cannot be accessed after calling
 * this function any more.
 *
 *  @param recvreq (IN)  Receive request.
 */
static inline void
recv_request_pml_complete(mca_pml_ob1_recv_request_t *recvreq)
{
    size_t i;

    if(false == recvreq->req_recv.req_base.req_pml_complete){

        if(recvreq->req_recv.req_bytes_packed > 0) {
            PERUSE_TRACE_COMM_EVENT( PERUSE_COMM_REQ_XFER_END,
                    &recvreq->req_recv.req_base, PERUSE_RECV );
        }

        for(i = 0; i < recvreq->req_rdma_cnt; i++) {
            struct mca_btl_base_registration_handle_t *handle = recvreq->req_rdma[i].btl_reg;
            mca_bml_base_btl_t *bml_btl = recvreq->req_rdma[i].bml_btl;

            if (NULL != handle) {
                mca_bml_base_deregister_mem (bml_btl, handle);
            }
        }
        recvreq->req_rdma_cnt = 0;


        if(true == recvreq->req_recv.req_base.req_free_called) {
            if( MPI_SUCCESS != recvreq->req_recv.req_base.req_ompi.req_status.MPI_ERROR ) {
                ompi_mpi_abort(&ompi_mpi_comm_world.comm, MPI_ERR_REQUEST);
            }
            MCA_PML_OB1_RECV_REQUEST_RETURN(recvreq);
        } else {
            /* initialize request status */
            recvreq->req_recv.req_base.req_pml_complete = true;
            recvreq->req_recv.req_base.req_ompi.req_status._ucount =
                recvreq->req_bytes_received;
            if (recvreq->req_recv.req_bytes_packed > recvreq->req_bytes_expected) {
                recvreq->req_recv.req_base.req_ompi.req_status._ucount =
                    recvreq->req_recv.req_bytes_packed;
                recvreq->req_recv.req_base.req_ompi.req_status.MPI_ERROR =
                    MPI_ERR_TRUNCATE;
            }
            if (OPAL_UNLIKELY(recvreq->local_handle)) {
                mca_bml_base_deregister_mem (recvreq->rdma_bml, recvreq->local_handle);
                recvreq->local_handle = NULL;
            }
            MCA_PML_OB1_RECV_REQUEST_MPI_COMPLETE(recvreq);
        }

    }
}

static inline bool
recv_request_pml_complete_check(mca_pml_ob1_recv_request_t *recvreq)
{
#if OPAL_ENABLE_MULTI_THREADS
    opal_atomic_rmb();
#endif
    if(recvreq->req_match_received &&
            recvreq->req_bytes_received >= recvreq->req_recv.req_bytes_packed &&
            lock_recv_request(recvreq)) {
        recv_request_pml_complete(recvreq);
        return true;
    }

    return false;
}

extern void mca_pml_ob1_recv_req_start(mca_pml_ob1_recv_request_t *req);
#define MCA_PML_OB1_RECV_REQUEST_START(r) mca_pml_ob1_recv_req_start(r)

static inline void prepare_recv_req_converter(mca_pml_ob1_recv_request_t *req)
{
    if( req->req_recv.req_base.req_datatype->super.size | req->req_recv.req_base.req_count ) {
        opal_convertor_copy_and_prepare_for_recv(
                req->req_recv.req_base.req_proc->super.proc_convertor,
                &(req->req_recv.req_base.req_datatype->super),
                req->req_recv.req_base.req_count,
                req->req_recv.req_base.req_addr,
                0,
                &req->req_recv.req_base.req_convertor);
        opal_convertor_get_unpacked_size(&req->req_recv.req_base.req_convertor,
                                         &req->req_bytes_expected);
    }
}

#define MCA_PML_OB1_RECV_REQUEST_MATCHED(request, hdr) \
    recv_req_matched(request, hdr)

static inline void recv_req_matched(mca_pml_ob1_recv_request_t *req,
                                    mca_pml_ob1_match_hdr_t *hdr)
{
    req->req_recv.req_base.req_ompi.req_status.MPI_SOURCE = hdr->hdr_src;
    req->req_recv.req_base.req_ompi.req_status.MPI_TAG = hdr->hdr_tag;
    req->req_match_received = true;
#if OPAL_ENABLE_MULTI_THREADS
    opal_atomic_wmb();
#endif
    if(req->req_recv.req_bytes_packed > 0) {
#if OPAL_ENABLE_HETEROGENEOUS_SUPPORT
        if(MPI_ANY_SOURCE == req->req_recv.req_base.req_peer) {
            /* non wildcard prepared during post recv */
            prepare_recv_req_converter(req);
        }
#endif  /* OPAL_ENABLE_HETEROGENEOUS_SUPPORT */
        PERUSE_TRACE_COMM_EVENT(PERUSE_COMM_REQ_XFER_BEGIN,
                                &req->req_recv.req_base, PERUSE_RECV);
    }
}


/**
 *
 */

#define MCA_PML_OB1_RECV_REQUEST_UNPACK( request,                                 \
                                         segments,                                \
                                         num_segments,                            \
                                         seg_offset,                              \
                                         data_offset,                             \
                                         bytes_received,                          \
                                         bytes_delivered)                         \
do {                                                                              \
    bytes_delivered = 0;                                                          \
    if(request->req_recv.req_bytes_packed > 0) {                                  \
        struct iovec iov[MCA_BTL_DES_MAX_SEGMENTS];                               \
        uint32_t iov_count = 0;                                                   \
        size_t max_data = bytes_received;                                         \
        size_t n, offset = seg_offset;                                            \
        mca_btl_base_segment_t* segment = segments;                               \
                                                                                  \
        for( n = 0; n < num_segments; n++, segment++ ) {                          \
            if(offset >= segment->seg_len) {                                      \
                offset -= segment->seg_len;                                       \
            } else {                                                              \
                iov[iov_count].iov_len = segment->seg_len - offset;               \
                iov[iov_count].iov_base = (IOVBASE_TYPE*)                         \
                    ((unsigned char*)segment->seg_addr.pval + offset);            \
                iov_count++;                                                      \
                offset = 0;                                                       \
            }                                                                     \
        }                                                                         \
        OPAL_THREAD_LOCK(&request->lock);                                         \
        PERUSE_TRACE_COMM_OMPI_EVENT (PERUSE_COMM_REQ_XFER_CONTINUE,              \
                                      &(request->req_recv.req_base), max_data,    \
                                      PERUSE_RECV);                               \
        opal_convertor_set_position( &(request->req_recv.req_base.req_convertor), \
                                     &data_offset );                              \
        opal_convertor_unpack( &(request)->req_recv.req_base.req_convertor,       \
                               iov,                                               \
                               &iov_count,                                        \
                               &max_data );                                       \
        bytes_delivered = max_data;                                               \
        OPAL_THREAD_UNLOCK(&request->lock);                                       \
    }                                                                             \
} while (0)


/**
 *
 */

void mca_pml_ob1_recv_request_progress_match(
    mca_pml_ob1_recv_request_t* req,
    struct mca_btl_base_module_t* btl,
    mca_btl_base_segment_t* segments,
    size_t num_segments);

/**
 *
 */

void mca_pml_ob1_recv_request_progress_frag(
    mca_pml_ob1_recv_request_t* req,
    struct mca_btl_base_module_t* btl,
    mca_btl_base_segment_t* segments,
    size_t num_segments);

#if OPAL_CUDA_SUPPORT
void mca_pml_ob1_recv_request_frag_copy_start(
    mca_pml_ob1_recv_request_t* req,
    struct mca_btl_base_module_t* btl,
    mca_btl_base_segment_t* segments,
    size_t num_segments,
    mca_btl_base_descriptor_t* des);

void mca_pml_ob1_recv_request_frag_copy_finished(struct mca_btl_base_module_t* btl,
    struct mca_btl_base_endpoint_t* ep,
    struct mca_btl_base_descriptor_t* des,
    int status );
#endif /* OPAL_CUDA_SUPPORT */
/**
 *
 */

void mca_pml_ob1_recv_request_progress_rndv(
    mca_pml_ob1_recv_request_t* req,
    struct mca_btl_base_module_t* btl,
    mca_btl_base_segment_t* segments,
    size_t num_segments);

/**
 *
 */

void mca_pml_ob1_recv_request_progress_rget(
    mca_pml_ob1_recv_request_t* req,
    struct mca_btl_base_module_t* btl,
    mca_btl_base_segment_t* segments,
    size_t num_segments);

/**
 *
 */

void mca_pml_ob1_recv_request_matched_probe(
    mca_pml_ob1_recv_request_t* req,
    struct mca_btl_base_module_t* btl,
    mca_btl_base_segment_t* segments,
    size_t num_segments);

/**
 *
 */

int mca_pml_ob1_recv_request_schedule_once(
    mca_pml_ob1_recv_request_t* req, mca_bml_base_btl_t* start_bml_btl);

static inline int mca_pml_ob1_recv_request_schedule_exclusive(
        mca_pml_ob1_recv_request_t* req,
        mca_bml_base_btl_t* start_bml_btl)
{
    int rc;

    do {
        rc = mca_pml_ob1_recv_request_schedule_once(req, start_bml_btl);
        if(rc == OMPI_ERR_OUT_OF_RESOURCE)
            break;
    } while(!unlock_recv_request(req));

    if(OMPI_SUCCESS == rc)
        recv_request_pml_complete_check(req);

    return rc;
}

static inline void mca_pml_ob1_recv_request_schedule(
        mca_pml_ob1_recv_request_t* req,
        mca_bml_base_btl_t* start_bml_btl)
{
    if(!lock_recv_request(req))
        return;

    (void)mca_pml_ob1_recv_request_schedule_exclusive(req, start_bml_btl);
}

#define MCA_PML_OB1_ADD_ACK_TO_PENDING(P, S, D, O, Sz)                  \
    do {                                                                \
        mca_pml_ob1_pckt_pending_t *_pckt;                              \
                                                                        \
        MCA_PML_OB1_PCKT_PENDING_ALLOC(_pckt);                          \
        _pckt->hdr.hdr_common.hdr_type = MCA_PML_OB1_HDR_TYPE_ACK;      \
        _pckt->hdr.hdr_ack.hdr_src_req.lval = (S);                      \
        _pckt->hdr.hdr_ack.hdr_dst_req.pval = (D);                      \
        _pckt->hdr.hdr_ack.hdr_send_offset = (O);                       \
        _pckt->hdr.hdr_ack.hdr_send_size = (Sz);                        \
        _pckt->proc = (P);                                              \
        _pckt->bml_btl = NULL;                                          \
        OPAL_THREAD_LOCK(&mca_pml_ob1.lock);                            \
        opal_list_append(&mca_pml_ob1.pckt_pending,                     \
                         (opal_list_item_t*)_pckt);                     \
        OPAL_THREAD_UNLOCK(&mca_pml_ob1.lock);                          \
    } while(0)

int mca_pml_ob1_recv_request_ack_send_btl(ompi_proc_t* proc,
        mca_bml_base_btl_t* bml_btl, uint64_t hdr_src_req, void *hdr_dst_req,
        uint64_t hdr_rdma_offset, uint64_t size, bool nordma);

static inline int mca_pml_ob1_recv_request_ack_send(ompi_proc_t* proc,
        uint64_t hdr_src_req, void *hdr_dst_req, uint64_t hdr_send_offset,
        uint64_t size, bool nordma)
{
    size_t i;
    mca_bml_base_btl_t* bml_btl;
    mca_bml_base_endpoint_t* endpoint = mca_bml_base_get_endpoint (proc);

    assert (NULL != endpoint);

    for(i = 0; i < mca_bml_base_btl_array_get_size(&endpoint->btl_eager); i++) {
        bml_btl = mca_bml_base_btl_array_get_next(&endpoint->btl_eager);
        if(mca_pml_ob1_recv_request_ack_send_btl(proc, bml_btl, hdr_src_req,
                    hdr_dst_req, hdr_send_offset, size, nordma) == OMPI_SUCCESS)
            return OMPI_SUCCESS;
    }

    MCA_PML_OB1_ADD_ACK_TO_PENDING(proc, hdr_src_req, hdr_dst_req,
                                   hdr_send_offset, size);

    return OMPI_ERR_OUT_OF_RESOURCE;
}

int mca_pml_ob1_recv_request_get_frag(mca_pml_ob1_rdma_frag_t* frag);

/* This function tries to continue recvreq that stuck due to resource
 * unavailability. Recvreq is added to recv_pending list if scheduling of put
 * operation cannot be accomplished for some reason. */
void mca_pml_ob1_recv_request_process_pending(void);

END_C_DECLS

#endif

