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
 * Copyright (c) 2012      Sandia National Laboratories.  All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2017      Intel, Inc. All rights reserved
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef PML_CM_RECVREQ_H
#define PML_CM_RECVREQ_H

#include "pml_cm_request.h"
#include "ompi/mca/pml/base/pml_base_recvreq.h"
#include "ompi/mca/mtl/mtl.h"

struct mca_pml_cm_thin_recv_request_t {
    mca_pml_cm_request_t req_base;
    mca_mtl_request_t req_mtl;            /**< the mtl specific memory. This field should be the last in the struct */
};
typedef struct mca_pml_cm_thin_recv_request_t mca_pml_cm_thin_recv_request_t;
OBJ_CLASS_DECLARATION(mca_pml_cm_thin_recv_request_t);

struct mca_pml_cm_hvy_recv_request_t {
    mca_pml_cm_request_t req_base;
    void *req_addr;                       /**< pointer to application buffer */
    size_t req_count;                     /**< count of user datatype elements */
    int32_t req_peer;                     /**< peer process - rank w/in this communicator */
    int32_t req_tag;                      /**< user defined tag */
    void *req_buff;                       /**< pointer to send buffer - may not be application buffer */
    size_t req_bytes_packed;              /**< packed size of a message given the datatype and count */
    bool req_blocking;
    mca_mtl_request_t req_mtl;            /**< the mtl specific memory. This field should be the last in the struct */
};
typedef struct mca_pml_cm_hvy_recv_request_t mca_pml_cm_hvy_recv_request_t;

OBJ_CLASS_DECLARATION(mca_pml_cm_hvy_recv_request_t);

/**
 *  Allocate a recv request from the modules free list.
 *
 *  @param rc (OUT)  OMPI_SUCCESS or error status on failure.
 *  @return          Receive request.
 */
#define MCA_PML_CM_THIN_RECV_REQUEST_ALLOC(recvreq)                            \
do {                                                                           \
    recvreq = (mca_pml_cm_thin_recv_request_t*)                                \
      opal_free_list_get (&mca_pml_base_recv_requests);                        \
    recvreq->req_base.req_pml_type = MCA_PML_CM_REQUEST_RECV_THIN;             \
    recvreq->req_mtl.ompi_req = (ompi_request_t*) recvreq;                     \
    recvreq->req_mtl.completion_callback = mca_pml_cm_recv_request_completion; \
 } while (0)

#define MCA_PML_CM_HVY_RECV_REQUEST_ALLOC(recvreq)                             \
do {                                                                           \
    recvreq = (mca_pml_cm_hvy_recv_request_t*)                                 \
      opal_free_list_get (&mca_pml_base_recv_requests);                        \
    recvreq->req_base.req_pml_type = MCA_PML_CM_REQUEST_RECV_HEAVY;            \
    recvreq->req_mtl.ompi_req = (ompi_request_t*) recvreq;                     \
    recvreq->req_mtl.completion_callback = mca_pml_cm_recv_request_completion; \
 } while (0)


/**
 * Initialize a receive request with call parameters.
 *
 * @param request (IN)       Receive request.
 * @param addr (IN)          User buffer.
 * @param count (IN)         Number of elements of indicated datatype.
 * @param datatype (IN)      User defined datatype.
 * @param src (IN)           Source rank w/in the communicator.
 * @param comm (IN)          Communicator.
 * @param persistent (IN)    Is this a ersistent request.
 */
#if OPAL_ENABLE_HETEROGENEOUS_SUPPORT
#define MCA_PML_CM_THIN_RECV_REQUEST_INIT( request,                     \
                                           ompi_proc,                   \
                                           comm,                        \
                                           src,                         \
                                           datatype,                    \
                                           addr,                        \
                                           count,                       \
                                           flags )                      \
do {                                                                    \
    OMPI_REQUEST_INIT(&(request)->req_base.req_ompi, false);            \
    (request)->req_base.req_ompi.req_mpi_object.comm = comm;            \
    (request)->req_base.req_pml_complete = false;                       \
    (request)->req_base.req_free_called = false;                        \
    request->req_base.req_comm = comm;                                  \
    request->req_base.req_datatype = datatype;                          \
    OBJ_RETAIN(comm);                                                   \
    OMPI_DATATYPE_RETAIN(datatype);                                     \
                                                                        \
    if( MPI_ANY_SOURCE == src ) {                                       \
        ompi_proc = ompi_proc_local_proc;                               \
    } else {                                                            \
        ompi_proc = ompi_comm_peer_lookup( comm, src );                 \
    }                                                                   \
    MCA_PML_CM_SWITCH_CUDA_CONVERTOR_OFF(flags, datatype, count);       \
    opal_convertor_copy_and_prepare_for_recv(                           \
                                  ompi_proc->super.proc_convertor,      \
                                  &(datatype->super),                   \
                                  count,                                \
                                  addr,                                 \
                                  flags,                                \
                                  &(request)->req_base.req_convertor ); \
} while(0)
#else
#define MCA_PML_CM_THIN_RECV_REQUEST_INIT( request,                     \
                                           ompi_proc,                   \
                                           comm,                        \
                                           src,                         \
                                           datatype,                    \
                                           addr,                        \
                                           count,                       \
                                           flags )                      \
do {                                                                    \
    OMPI_REQUEST_INIT(&(request)->req_base.req_ompi, false);            \
    (request)->req_base.req_ompi.req_mpi_object.comm = comm;            \
    (request)->req_base.req_pml_complete = false;                       \
    (request)->req_base.req_free_called = false;                        \
    request->req_base.req_comm = comm;                                  \
    request->req_base.req_datatype = datatype;                          \
    OBJ_RETAIN(comm);                                                   \
    OMPI_DATATYPE_RETAIN(datatype);                                     \
                                                                        \
    MCA_PML_CM_SWITCH_CUDA_CONVERTOR_OFF(flags, datatype, count);       \
    opal_convertor_copy_and_prepare_for_recv(                           \
        ompi_mpi_local_convertor,                                       \
        &(datatype->super),                                             \
        count,                                                          \
        addr,                                                           \
        flags,                                                          \
        &(request)->req_base.req_convertor );                           \
} while(0)
#endif

#if OPAL_ENABLE_HETEROGENEOUS_SUPPORT
#define MCA_PML_CM_HVY_RECV_REQUEST_INIT( request,                      \
                                          ompi_proc,                    \
                                          comm,                         \
                                          tag,                          \
                                          src,                          \
                                          datatype,                     \
                                          addr,                         \
                                          count,                        \
                                          flags,                        \
                                          persistent)                   \
do {                                                                    \
    OMPI_REQUEST_INIT(&(request)->req_base.req_ompi, persistent);       \
    (request)->req_base.req_ompi.req_mpi_object.comm = comm;            \
    (request)->req_base.req_pml_complete = OPAL_INT_TO_BOOL(persistent); \
    (request)->req_base.req_free_called = false;                        \
    request->req_base.req_comm = comm;                                  \
    request->req_base.req_datatype = datatype;                          \
    request->req_tag = tag;                                             \
    request->req_peer = src;                                            \
    request->req_addr = addr;                                           \
    request->req_count = count;                                         \
    OBJ_RETAIN(comm);                                                   \
    OMPI_DATATYPE_RETAIN(datatype);                                     \
                                                                        \
    if( MPI_ANY_SOURCE == src ) {                                       \
        ompi_proc = ompi_proc_local_proc;                               \
    } else {                                                            \
        ompi_proc = ompi_comm_peer_lookup( comm, src );                 \
    }                                                                   \
    MCA_PML_CM_SWITCH_CUDA_CONVERTOR_OFF(flags, datatype, count);       \
    opal_convertor_copy_and_prepare_for_recv(                           \
                                  ompi_proc->super.proc_convertor,      \
                                  &(datatype->super),                   \
                                  count,                                \
                                  addr,                                 \
                                  flags,                                \
                                  &(request)->req_base.req_convertor ); \
 } while(0)
#else
#define MCA_PML_CM_HVY_RECV_REQUEST_INIT( request,                      \
                                          ompi_proc,                    \
                                          comm,                         \
                                          tag,                          \
                                          src,                          \
                                          datatype,                     \
                                          addr,                         \
                                          count,                        \
                                          flags,                        \
                                          persistent)                   \
do {                                                                    \
    OMPI_REQUEST_INIT(&(request)->req_base.req_ompi, persistent);       \
    (request)->req_base.req_ompi.req_mpi_object.comm = comm;            \
    (request)->req_base.req_pml_complete = OPAL_INT_TO_BOOL(persistent); \
    (request)->req_base.req_free_called = false;                        \
    request->req_base.req_comm = comm;                                  \
    request->req_base.req_datatype = datatype;                          \
    request->req_tag = tag;                                             \
    request->req_peer = src;                                            \
    request->req_addr = addr;                                           \
    request->req_count = count;                                         \
    OBJ_RETAIN(comm);                                                   \
    OMPI_DATATYPE_RETAIN(datatype);                                     \
                                                                        \
    MCA_PML_CM_SWITCH_CUDA_CONVERTOR_OFF(flags, datatype, count);       \
    opal_convertor_copy_and_prepare_for_recv(                           \
        ompi_mpi_local_convertor,                                       \
        &(datatype->super),                                             \
        count,                                                          \
        addr,                                                           \
        flags,                                                          \
        &(request)->req_base.req_convertor );                           \
 } while(0)
#endif

/**
 * Start an initialized request.
 *
 * @param request  Receive request.
 * @return         OMPI_SUCESS or error status on failure.
 */
#define MCA_PML_CM_THIN_RECV_REQUEST_START(request, comm, tag, src, ret) \
do {                                                                    \
    /* init/re-init the request */                                      \
    request->req_base.req_pml_complete = false;                         \
    request->req_base.req_ompi.req_complete = false;                    \
    request->req_base.req_ompi.req_state = OMPI_REQUEST_ACTIVE;         \
                                                                        \
    /* always set the req_status.MPI_TAG to ANY_TAG before starting the \
     * request. This field is used if cancelled to find out if the request \
     * has been matched or not.                                         \
     */                                                                 \
    request->req_base.req_ompi.req_status.MPI_TAG = OMPI_ANY_TAG;       \
    request->req_base.req_ompi.req_status.MPI_ERROR = OMPI_SUCCESS;     \
    request->req_base.req_ompi.req_status._cancelled = 0;               \
    ret = OMPI_MTL_CALL(irecv(ompi_mtl,                                 \
                              comm,                                     \
                              src,                                      \
                              tag,                                      \
                              &recvreq->req_base.req_convertor,         \
                              &recvreq->req_mtl));                      \
} while (0)

#define MCA_PML_CM_THIN_RECV_REQUEST_MATCHED_START(request, message, ret) \
do {                                                                    \
    /* init/re-init the request */                                      \
    request->req_base.req_pml_complete = false;                         \
    request->req_base.req_ompi.req_complete = false;                    \
    request->req_base.req_ompi.req_state = OMPI_REQUEST_ACTIVE;         \
                                                                        \
    /* always set the req_status.MPI_TAG to ANY_TAG before starting the \
     * request. This field is used if cancelled to find out if the request \
     * has been matched or not.                                         \
     */                                                                 \
    request->req_base.req_ompi.req_status.MPI_TAG = OMPI_ANY_TAG;       \
    request->req_base.req_ompi.req_status.MPI_ERROR = OMPI_SUCCESS;     \
    request->req_base.req_ompi.req_status._cancelled = 0;               \
    ret = OMPI_MTL_CALL(imrecv(ompi_mtl,                                \
                               &recvreq->req_base.req_convertor,        \
                               message,                                 \
                               &recvreq->req_mtl));                     \
} while (0)


#define MCA_PML_CM_HVY_RECV_REQUEST_START(request, ret)                 \
do {                                                                    \
/*     opal_output(0, "posting hvy request %d\n", request);                */ \
    /* init/re-init the request */                                      \
    request->req_base.req_pml_complete = false;                         \
    request->req_base.req_ompi.req_complete = false;                    \
    request->req_base.req_ompi.req_state = OMPI_REQUEST_ACTIVE;         \
                                                                        \
    /* always set the req_status.MPI_TAG to ANY_TAG before starting the \
     * request. This field is used if cancelled to find out if the request \
     * has been matched or not.                                         \
     */                                                                 \
    request->req_base.req_ompi.req_status.MPI_TAG = OMPI_ANY_TAG;       \
    request->req_base.req_ompi.req_status.MPI_ERROR = OMPI_SUCCESS;     \
    request->req_base.req_ompi.req_status._cancelled = 0;               \
    ret = OMPI_MTL_CALL(irecv(ompi_mtl,                                 \
                              request->req_base.req_comm,               \
                              request->req_peer,                        \
                              request->req_tag,                         \
                              &recvreq->req_base.req_convertor,         \
                              &recvreq->req_mtl));                      \
} while (0)


/**
 * Mark the request as completed at MPI level for internal purposes.
 *
 *  @param recvreq (IN)  Receive request.
 */
#define MCA_PML_CM_THIN_RECV_REQUEST_MPI_COMPLETE( recvreq )            \
do {                                                                    \
    ompi_request_complete(  &(recvreq->req_base.req_ompi), true );      \
 } while (0)


/**
 *  Return a recv request to the modules free list.
 *
 *  @param recvreq (IN)  Receive request.
 */
#define MCA_PML_CM_THIN_RECV_REQUEST_PML_COMPLETE(recvreq)              \
do {                                                                    \
    assert( false == recvreq->req_base.req_pml_complete );              \
                                                                        \
    if( true == recvreq->req_base.req_free_called ) {                   \
        MCA_PML_CM_THIN_RECV_REQUEST_RETURN( recvreq );                 \
    } else {                                                            \
        recvreq->req_base.req_pml_complete = true;                      \
        ompi_request_complete( &(recvreq->req_base.req_ompi), true );   \
    }                                                                   \
 } while(0)




/**
 *  Return a recv request to the modules free list.
 *
 *  @param recvreq (IN)  Receive request.
 */
#define MCA_PML_CM_HVY_RECV_REQUEST_PML_COMPLETE(recvreq)               \
do {                                                                    \
    assert( false == recvreq->req_base.req_pml_complete );              \
                                                                        \
    if( true == recvreq->req_base.req_free_called ) {                   \
        MCA_PML_CM_HVY_RECV_REQUEST_RETURN( recvreq );                  \
    } else {                                                            \
        /* initialize request status */                                 \
        if(recvreq->req_base.req_ompi.req_persistent) {                 \
            /* rewind convertor */                                      \
            size_t offset = 0;                                          \
            opal_convertor_set_position(&recvreq->req_base.req_convertor, &offset); \
        }                                                               \
        recvreq->req_base.req_pml_complete = true;                      \
        ompi_request_complete(  &(recvreq->req_base.req_ompi), true );  \
    }                                                                   \
 } while(0)


/**
 *  Free the PML receive request
 */
#define MCA_PML_CM_HVY_RECV_REQUEST_RETURN(recvreq)                     \
{                                                                       \
    OBJ_RELEASE((recvreq)->req_base.req_comm);                          \
    OMPI_DATATYPE_RELEASE((recvreq)->req_base.req_datatype);            \
    OMPI_REQUEST_FINI(&(recvreq)->req_base.req_ompi);                   \
    opal_convertor_cleanup( &((recvreq)->req_base.req_convertor) );     \
    opal_free_list_return ( &mca_pml_base_recv_requests,                \
                           (opal_free_list_item_t*)(recvreq));          \
}

/**
 *  Free the PML receive request
 */
#define MCA_PML_CM_THIN_RECV_REQUEST_RETURN(recvreq)                    \
{                                                                       \
    OBJ_RELEASE((recvreq)->req_base.req_comm);                          \
    OMPI_DATATYPE_RELEASE((recvreq)->req_base.req_datatype);            \
    OMPI_REQUEST_FINI(&(recvreq)->req_base.req_ompi);                   \
    opal_convertor_cleanup( &((recvreq)->req_base.req_convertor) );     \
    opal_free_list_return ( &mca_pml_base_recv_requests,                \
                           (opal_free_list_item_t*)(recvreq));          \
}

extern void mca_pml_cm_recv_request_completion(struct mca_mtl_request_t *mtl_request);

#endif


