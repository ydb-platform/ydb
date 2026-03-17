/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2013 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2006 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2015-2017 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2017      Intel, Inc. All rights reserved
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef PML_CM_SENDREQ_H
#define PML_CM_SENDREQ_H

#include "pml_cm_request.h"
#include "ompi/mca/pml/base/pml_base_sendreq.h"
#include "ompi/mca/pml/base/pml_base_bsend.h"
#include "ompi/mca/pml/pml.h"
#include "ompi/mca/mtl/mtl.h"
#include "opal/prefetch.h"

struct mca_pml_cm_send_request_t {
    mca_pml_cm_request_t req_base;
    mca_pml_base_send_mode_t req_send_mode;
};
typedef struct mca_pml_cm_send_request_t mca_pml_cm_send_request_t;
OBJ_CLASS_DECLARATION(mca_pml_cm_send_request_t);


struct mca_pml_cm_thin_send_request_t {
    mca_pml_cm_send_request_t req_send;
    mca_mtl_request_t req_mtl;            /**< the mtl specific memory. This field should be the last in the struct */
};
typedef struct mca_pml_cm_thin_send_request_t mca_pml_cm_thin_send_request_t;
OBJ_CLASS_DECLARATION(mca_pml_cm_thin_send_request_t);


struct mca_pml_cm_hvy_send_request_t {
    mca_pml_cm_send_request_t req_send;
    const void *req_addr;                 /**< pointer to application buffer */
    size_t req_count;                     /**< count of user datatype elements */
    int32_t req_peer;                     /**< peer process - rank w/in this communicator */
    int32_t req_tag;                      /**< user defined tag */
    void *req_buff;                       /**< pointer to send buffer - may not be application buffer */
    bool req_blocking;
    mca_mtl_request_t req_mtl;            /**< the mtl specific memory. This field should be the last in the struct */
};
typedef struct mca_pml_cm_hvy_send_request_t mca_pml_cm_hvy_send_request_t;
OBJ_CLASS_DECLARATION(mca_pml_cm_hvy_send_request_t);


#if OPAL_ENABLE_HETEROGENEOUS_SUPPORT
#define MCA_PML_CM_THIN_SEND_REQUEST_ALLOC(sendreq, comm, dst,          \
                                           ompi_proc)                   \
do {                                                                    \
    ompi_proc = ompi_comm_peer_lookup( comm, dst );                     \
                                                                        \
    if(OPAL_UNLIKELY(NULL == ompi_proc)) {                              \
        sendreq = NULL;                                                 \
    } else {                                                            \
        sendreq = (mca_pml_cm_thin_send_request_t*)                     \
          opal_free_list_wait (&mca_pml_base_send_requests);            \
        sendreq->req_send.req_base.req_pml_type = MCA_PML_CM_REQUEST_SEND_THIN; \
        sendreq->req_mtl.ompi_req = (ompi_request_t*) sendreq;          \
        sendreq->req_mtl.completion_callback = mca_pml_cm_send_request_completion; \
    }                                                                   \
} while(0)
#else
#define MCA_PML_CM_THIN_SEND_REQUEST_ALLOC(sendreq, comm, dst,          \
                                           ompi_proc)                   \
do {                                                                    \
    sendreq = (mca_pml_cm_thin_send_request_t*)                         \
        opal_free_list_wait (&mca_pml_base_send_requests);              \
    sendreq->req_send.req_base.req_pml_type = MCA_PML_CM_REQUEST_SEND_THIN; \
    sendreq->req_mtl.ompi_req = (ompi_request_t*) sendreq;              \
    sendreq->req_mtl.completion_callback = mca_pml_cm_send_request_completion; \
} while(0)
#endif


#if (OPAL_ENABLE_HETEROGENEOUS_SUPPORT)
#define MCA_PML_CM_HVY_SEND_REQUEST_ALLOC(sendreq, comm, dst,           \
                                          ompi_proc)                    \
{                                                                       \
    ompi_proc = ompi_comm_peer_lookup( comm, dst );                     \
    if(OPAL_UNLIKELY(NULL == ompi_proc)) {                              \
        sendreq = NULL;                                                 \
    } else {                                                            \
        sendreq = (mca_pml_cm_hvy_send_request_t*)                      \
          opal_free_list_wait (&mca_pml_base_send_requests);            \
        sendreq->req_send.req_base.req_pml_type = MCA_PML_CM_REQUEST_SEND_HEAVY; \
        sendreq->req_mtl.ompi_req = (ompi_request_t*) sendreq;          \
        sendreq->req_mtl.completion_callback = mca_pml_cm_send_request_completion; \
    }                                                                   \
}
#else
#define MCA_PML_CM_HVY_SEND_REQUEST_ALLOC(sendreq, comm, dst,           \
                                          ompi_proc)                    \
{                                                                       \
    sendreq = (mca_pml_cm_hvy_send_request_t*)                          \
        opal_free_list_wait (&mca_pml_base_send_requests);              \
    sendreq->req_send.req_base.req_pml_type = MCA_PML_CM_REQUEST_SEND_HEAVY; \
    sendreq->req_mtl.ompi_req = (ompi_request_t*) sendreq;              \
    sendreq->req_mtl.completion_callback = mca_pml_cm_send_request_completion; \
}
#endif

#if (OPAL_ENABLE_HETEROGENEOUS_SUPPORT)
#define MCA_PML_CM_HVY_SEND_REQUEST_INIT_COMMON(req_send,               \
                                            ompi_proc,                  \
                                            comm,                       \
                                            tag,                        \
                                            datatype,                   \
                                            sendmode,                   \
                                            buf,                        \
                                            count,                      \
                                            flags )                     \
{                                                                       \
    OBJ_RETAIN(comm);                                                   \
    OMPI_DATATYPE_RETAIN(datatype);                                     \
    (req_send)->req_base.req_comm = comm;                               \
    (req_send)->req_base.req_datatype = datatype;                       \
    MCA_PML_CM_SWITCH_CUDA_CONVERTOR_OFF(flags, datatype, count);       \
    opal_convertor_copy_and_prepare_for_send(                           \
                                             ompi_proc->super.proc_convertor, \
                                             &(datatype->super),        \
                                             count,                     \
                                             buf,                       \
                                             flags,                     \
                                             &(req_send)->req_base.req_convertor ); \
    (req_send)->req_base.req_ompi.req_mpi_object.comm = comm;           \
    (req_send)->req_base.req_ompi.req_status.MPI_SOURCE =               \
        comm->c_my_rank;                                                \
    (req_send)->req_base.req_ompi.req_status.MPI_TAG = tag;             \
    (req_send)->req_base.req_ompi.req_status._ucount = count;           \
    (req_send)->req_send_mode = sendmode;                               \
    (req_send)->req_base.req_free_called = false;                       \
}
#else
#define MCA_PML_CM_HVY_SEND_REQUEST_INIT_COMMON(req_send,               \
                                            ompi_proc,                  \
                                            comm,                       \
                                            tag,                        \
                                            datatype,                   \
                                            sendmode,                   \
                                            buf,                        \
                                            count,                      \
                                            flags )                     \
{                                                                       \
    OBJ_RETAIN(comm);                                                   \
    OMPI_DATATYPE_RETAIN(datatype);                                     \
    (req_send)->req_base.req_comm = comm;                               \
    (req_send)->req_base.req_datatype = datatype;                       \
    MCA_PML_CM_SWITCH_CUDA_CONVERTOR_OFF(flags, datatype, count);       \
    opal_convertor_copy_and_prepare_for_send(                           \
        ompi_mpi_local_convertor,                                       \
        &(datatype->super),                                             \
        count,                                                          \
        buf,                                                            \
        flags,                                                          \
        &(req_send)->req_base.req_convertor );                          \
    (req_send)->req_base.req_ompi.req_mpi_object.comm = comm;           \
    (req_send)->req_base.req_ompi.req_status.MPI_SOURCE =               \
        comm->c_my_rank;                                                \
    (req_send)->req_base.req_ompi.req_status.MPI_TAG = tag;             \
    (req_send)->req_base.req_ompi.req_status._ucount = count;           \
    (req_send)->req_send_mode = sendmode;                               \
    (req_send)->req_base.req_free_called = false;                       \
}
#endif

#if (OPAL_ENABLE_HETEROGENEOUS_SUPPORT)
#define MCA_PML_CM_SEND_REQUEST_INIT_COMMON(req_send,                   \
                                            ompi_proc,                  \
                                            comm,                       \
                                            tag,                        \
                                            datatype,                   \
                                            sendmode,                   \
                                            buf,                        \
                                            count,                      \
                                            flags )                     \
{                                                                       \
    OBJ_RETAIN(comm);                                                   \
    OMPI_DATATYPE_RETAIN(datatype);                                     \
    (req_send)->req_base.req_comm = comm;                               \
    (req_send)->req_base.req_datatype = datatype;                       \
    MCA_PML_CM_SWITCH_CUDA_CONVERTOR_OFF(flags, datatype, count);       \
    opal_convertor_copy_and_prepare_for_send(                           \
                                             ompi_proc->super.proc_convertor, \
                                             &(datatype->super),        \
                                             count,                     \
                                             buf,                       \
                                             flags,                     \
                                             &(req_send)->req_base.req_convertor ); \
    (req_send)->req_base.req_ompi.req_mpi_object.comm = comm;           \
    (req_send)->req_base.req_ompi.req_status.MPI_SOURCE =               \
        comm->c_my_rank;                                                \
    (req_send)->req_base.req_ompi.req_status.MPI_TAG = tag;             \
    (req_send)->req_base.req_ompi.req_status._ucount = count;           \
    (req_send)->req_send_mode = sendmode;                               \
    (req_send)->req_base.req_free_called = false;                       \
}

#else
#define MCA_PML_CM_SEND_REQUEST_INIT_COMMON(req_send,                   \
                                            ompi_proc,                  \
                                            comm,                       \
                                            tag,                        \
                                            datatype,                   \
                                            sendmode,                   \
                                            buf,                        \
                                            count,                      \
                                            flags )                     \
{                                                                       \
    OBJ_RETAIN(comm);                                                   \
    OMPI_DATATYPE_RETAIN(datatype);                                     \
    (req_send)->req_base.req_comm = comm;                               \
    (req_send)->req_base.req_datatype = datatype;                       \
    if (opal_datatype_is_contiguous_memory_layout(&datatype->super, count)) { \
        (req_send)->req_base.req_convertor.remoteArch =                 \
            ompi_mpi_local_convertor->remoteArch;                       \
        (req_send)->req_base.req_convertor.flags      =                 \
            ompi_mpi_local_convertor->flags;                            \
        (req_send)->req_base.req_convertor.master     =                 \
            ompi_mpi_local_convertor->master;                           \
        (req_send)->req_base.req_convertor.local_size =                 \
            count * datatype->super.size;                               \
        (req_send)->req_base.req_convertor.pBaseBuf   =                 \
            (unsigned char*)buf + datatype->super.true_lb;              \
        (req_send)->req_base.req_convertor.count      = count;          \
        (req_send)->req_base.req_convertor.pDesc      = &datatype->super; \
    } else {                                                            \
        MCA_PML_CM_SWITCH_CUDA_CONVERTOR_OFF(flags, datatype, count);   \
        opal_convertor_copy_and_prepare_for_send(                       \
            ompi_mpi_local_convertor,                                   \
            &(datatype->super),                                         \
            count,                                                      \
            buf,                                                        \
            flags,                                                      \
            &(req_send)->req_base.req_convertor );                      \
    }                                                                   \
    (req_send)->req_base.req_ompi.req_mpi_object.comm = comm;           \
    (req_send)->req_base.req_ompi.req_status.MPI_SOURCE =               \
        comm->c_my_rank;                                                \
    (req_send)->req_base.req_ompi.req_status.MPI_TAG = tag;             \
    (req_send)->req_base.req_ompi.req_status._ucount = count;           \
    (req_send)->req_send_mode = sendmode;                               \
    (req_send)->req_base.req_free_called = false;                       \
}
#endif

#define MCA_PML_CM_HVY_SEND_REQUEST_INIT( sendreq,                      \
                                          ompi_proc,                    \
                                          comm,                         \
                                          tag,                          \
                                          dst,                          \
                                          datatype,                     \
                                          sendmode,                     \
                                          persistent,                   \
                                          blocking,                     \
                                          buf,                          \
                                          count,                        \
                                          flags )                       \
    do {                                                                \
        OMPI_REQUEST_INIT(&(sendreq->req_send.req_base.req_ompi),       \
                          persistent);                                  \
        sendreq->req_tag = tag;                                         \
        sendreq->req_peer = dst;                                        \
        sendreq->req_addr = buf;                                        \
        sendreq->req_count = count;                                     \
        MCA_PML_CM_HVY_SEND_REQUEST_INIT_COMMON( (&sendreq->req_send),  \
                                             ompi_proc,                 \
                                             comm,                      \
                                             tag,                       \
                                             datatype,                  \
                                             sendmode,                  \
                                             buf,                       \
                                             count,                     \
                                             flags )                    \
        opal_convertor_get_packed_size(                                 \
                                       &sendreq->req_send.req_base.req_convertor, \
                                       &sendreq->req_count );           \
                                                                        \
        sendreq->req_blocking = blocking;                               \
        sendreq->req_send.req_base.req_pml_complete =                   \
            (persistent ? true:false);                                  \
    } while(0)


#define MCA_PML_CM_THIN_SEND_REQUEST_INIT( sendreq,                     \
                                           ompi_proc,                   \
                                           comm,                        \
                                           tag,                         \
                                           dst,                         \
                                           datatype,                    \
                                           sendmode,                    \
                                           buf,                         \
                                           count,                       \
                                           flags )                      \
    do {                                                                \
        OMPI_REQUEST_INIT(&(sendreq->req_send.req_base.req_ompi),       \
                          false);                                       \
        MCA_PML_CM_SEND_REQUEST_INIT_COMMON( (&sendreq->req_send),      \
                                             ompi_proc,                 \
                                             comm,                      \
                                             tag,                       \
                                             datatype,                  \
                                             sendmode,                  \
                                             buf,                       \
                                             count,                     \
                                             flags);                    \
        sendreq->req_send.req_base.req_pml_complete = false;            \
    } while(0)


#define MCA_PML_CM_SEND_REQUEST_START_SETUP(req_send)                   \
    do {                                                                \
        (req_send)->req_base.req_pml_complete = false;                  \
        (req_send)->req_base.req_ompi.req_complete = REQUEST_PENDING;   \
        (req_send)->req_base.req_ompi.req_state =                       \
            OMPI_REQUEST_ACTIVE;                                        \
        (req_send)->req_base.req_ompi.req_status._cancelled = 0;        \
    } while (0)


#define MCA_PML_CM_THIN_SEND_REQUEST_START(sendreq,                     \
                                           comm,                        \
                                           tag,                         \
                                           dst,                         \
                                           sendmode,                    \
                                           blocking,                    \
                                           ret)                         \
do {                                                                    \
    MCA_PML_CM_SEND_REQUEST_START_SETUP(&(sendreq)->req_send);          \
    ret = OMPI_MTL_CALL(isend(ompi_mtl,                                 \
                              comm,                                     \
                              dst,                                      \
                              tag,                                      \
                              &sendreq->req_send.req_base.req_convertor, \
                              sendmode,                                 \
                              blocking,                                 \
                              &sendreq->req_mtl));                      \
 } while (0)

#define MCA_PML_CM_HVY_SEND_REQUEST_BSEND_ALLOC(sendreq, ret)           \
do {                                                                    \
    struct iovec iov;                                                   \
    unsigned int iov_count;                                             \
    size_t max_data;                                                    \
                                                                        \
    if(sendreq->req_count > 0) {                                        \
        sendreq->req_buff =                                             \
            mca_pml_base_bsend_request_alloc_buf(sendreq->req_count);   \
        if (NULL == sendreq->req_buff) {                                \
            ret = MPI_ERR_BUFFER;                                       \
        } else {                                                        \
            iov.iov_base = (IOVBASE_TYPE*)sendreq->req_buff;            \
            max_data = iov.iov_len = sendreq->req_count;                \
            iov_count = 1;                                              \
            opal_convertor_pack( &sendreq->req_send.req_base.req_convertor, \
                                 &iov,                                  \
                                 &iov_count,                            \
                                 &max_data );                           \
            opal_convertor_prepare_for_send( &sendreq->req_send.req_base.req_convertor, \
                                             &(ompi_mpi_packed.dt.super),  \
                                             max_data, sendreq->req_buff ); \
        }                                                               \
    }                                                                   \
 } while(0);


#define MCA_PML_CM_HVY_SEND_REQUEST_START(sendreq, ret)                              \
do {                                                                                 \
    ret = OMPI_SUCCESS;                                                              \
    MCA_PML_CM_SEND_REQUEST_START_SETUP(&(sendreq)->req_send);                       \
    if (sendreq->req_send.req_send_mode == MCA_PML_BASE_SEND_BUFFERED) {             \
        MCA_PML_CM_HVY_SEND_REQUEST_BSEND_ALLOC(sendreq, ret);                       \
    }                                                                                \
    if (OMPI_SUCCESS == ret) {                                                       \
        ret = OMPI_MTL_CALL(isend(ompi_mtl,                                          \
                                  sendreq->req_send.req_base.req_comm,               \
                                  sendreq->req_peer,                                 \
                                  sendreq->req_tag,                                  \
                                  &sendreq->req_send.req_base.req_convertor,         \
                                  sendreq->req_send.req_send_mode,                   \
                                  sendreq->req_blocking,                             \
                                  &sendreq->req_mtl));                               \
        if(OMPI_SUCCESS == ret &&                                                    \
           sendreq->req_send.req_send_mode == MCA_PML_BASE_SEND_BUFFERED) {          \
            sendreq->req_send.req_base.req_ompi.req_status.MPI_ERROR = 0;            \
            if(!REQUEST_COMPLETE(&sendreq->req_send.req_base.req_ompi)) {            \
                /* request may have already been marked complete by the MTL */       \
                ompi_request_complete(&(sendreq)->req_send.req_base.req_ompi, true); \
            }                                                                        \
        }                                                                            \
    }                                                                                \
 } while (0)

/*
 * The PML has completed a send request. Note that this request
 * may have been orphaned by the user or have already completed
 * at the MPI level.
 * This macro will never be called directly from the upper level, as it should
 * only be an internal call to the PML.
 */
#define MCA_PML_CM_HVY_SEND_REQUEST_PML_COMPLETE(sendreq)                          \
do {                                                                               \
    assert( false == sendreq->req_send.req_base.req_pml_complete );                \
                                                                                   \
    if (sendreq->req_send.req_send_mode == MCA_PML_BASE_SEND_BUFFERED &&           \
        sendreq->req_count > 0 ) {                                                 \
        mca_pml_base_bsend_request_free(sendreq->req_buff);                        \
    }                                                                              \
                                                                                   \
    if( !REQUEST_COMPLETE(&sendreq->req_send.req_base.req_ompi)) {                 \
        /* the request may have already been marked complete by the MTL */         \
        ompi_request_complete(&(sendreq->req_send.req_base.req_ompi), true);       \
    }                                                                              \
    sendreq->req_send.req_base.req_pml_complete = true;                            \
                                                                                   \
    if( sendreq->req_send.req_base.req_free_called ) {                             \
        MCA_PML_CM_HVY_SEND_REQUEST_RETURN( sendreq );                             \
    } else {                                                                       \
        if(sendreq->req_send.req_base.req_ompi.req_persistent) {                   \
            /* rewind convertor */                                                 \
            size_t offset = 0;                                                     \
            opal_convertor_set_position(&sendreq->req_send.req_base.req_convertor, \
                                        &offset);                                  \
        }                                                                          \
    }                                                                              \
 } while (0)


/*
 * Release resources associated with a request
 */
#define MCA_PML_CM_HVY_SEND_REQUEST_RETURN(sendreq)                     \
    {                                                                   \
        /*  Let the base handle the reference counts */                 \
        OMPI_DATATYPE_RETAIN(sendreq->req_send.req_base.req_datatype);  \
        OBJ_RELEASE(sendreq->req_send.req_base.req_comm);               \
        OMPI_REQUEST_FINI(&sendreq->req_send.req_base.req_ompi);        \
        opal_convertor_cleanup( &(sendreq->req_send.req_base.req_convertor) ); \
        opal_free_list_return ( &mca_pml_base_send_requests,            \
                               (opal_free_list_item_t*)sendreq);        \
    }

/*
 * The PML has completed a send request. Note that this request
 * may have been orphaned by the user or have already completed
 * at the MPI level.
 * This macro will never be called directly from the upper level, as it should
 * only be an internal call to the PML.
 */
#define MCA_PML_CM_THIN_SEND_REQUEST_PML_COMPLETE(sendreq)                   \
do {                                                                         \
    assert( false == sendreq->req_send.req_base.req_pml_complete );          \
                                                                             \
    if( !REQUEST_COMPLETE(&sendreq->req_send.req_base.req_ompi)) {           \
        /* Should only be called for long messages (maybe synchronous) */    \
        ompi_request_complete(&(sendreq->req_send.req_base.req_ompi), true); \
    }                                                                        \
    sendreq->req_send.req_base.req_pml_complete = true;                      \
                                                                             \
    if( sendreq->req_send.req_base.req_free_called ) {                       \
        MCA_PML_CM_THIN_SEND_REQUEST_RETURN( sendreq );                      \
    }                                                                        \
 } while (0)


/*
 * Release resources associated with a request
 */
#define MCA_PML_CM_THIN_SEND_REQUEST_RETURN(sendreq)                    \
    {                                                                   \
        /*  Let the base handle the reference counts */                 \
        OMPI_DATATYPE_RETAIN(sendreq->req_send.req_base.req_datatype);  \
        OBJ_RELEASE(sendreq->req_send.req_base.req_comm);               \
        OMPI_REQUEST_FINI(&sendreq->req_send.req_base.req_ompi);        \
        opal_convertor_cleanup( &(sendreq->req_send.req_base.req_convertor) ); \
        opal_free_list_return ( &mca_pml_base_send_requests,            \
                               (opal_free_list_item_t*)sendreq);        \
    }

extern void
mca_pml_cm_send_request_completion(struct mca_mtl_request_t *mtl_request);

#endif
