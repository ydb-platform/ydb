/*
 * Copyright (c) 2004-2006 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2004-2007 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
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

#ifndef PML_CM_H
#define PML_CM_H

#ifdef HAVE_ALLOCA_H
#include <alloca.h>
#endif

#include "ompi_config.h"
#include "ompi/request/request.h"
#include "ompi/mca/pml/pml.h"
#include "ompi/mca/pml/base/base.h"
#include "ompi/datatype/ompi_datatype.h"
#include "ompi/communicator/communicator.h"
#include "ompi/request/request.h"
#include "ompi/mca/mtl/mtl.h"


#include "pml_cm_request.h"
#include "ompi/mca/pml/base/pml_base_recvreq.h"
#include "ompi/mca/mtl/mtl.h"
#include "pml_cm_recvreq.h"
#include "pml_cm_sendreq.h"
#include "ompi/message/message.h"


BEGIN_C_DECLS

struct mca_mtl_request_t;

/* Array of send completion callback - one per send type
 * These are called internally by the library when the send
 * is completed from its perspective.
 */
extern void (*send_completion_callbacks[])
    (struct mca_mtl_request_t *mtl_request);

struct ompi_pml_cm_t {
    mca_pml_base_module_t super;
    int                   free_list_num;
    int                   free_list_max;
    int                   free_list_inc;
};
typedef struct ompi_pml_cm_t ompi_pml_cm_t;
extern ompi_pml_cm_t ompi_pml_cm;

/* PML interface functions */
OMPI_DECLSPEC extern int mca_pml_cm_add_procs(struct ompi_proc_t **procs, size_t nprocs);
OMPI_DECLSPEC extern int mca_pml_cm_del_procs(struct ompi_proc_t **procs, size_t nprocs);

OMPI_DECLSPEC extern int mca_pml_cm_enable(bool enable);
OMPI_DECLSPEC extern int mca_pml_cm_progress(void);

OMPI_DECLSPEC extern int mca_pml_cm_add_comm(struct ompi_communicator_t* comm);
OMPI_DECLSPEC extern int mca_pml_cm_del_comm(struct ompi_communicator_t* comm);


__opal_attribute_always_inline__ static inline int
mca_pml_cm_irecv_init(void *addr,
                      size_t count,
                      ompi_datatype_t * datatype,
                      int src,
                      int tag,
                      struct ompi_communicator_t *comm,
                      struct ompi_request_t **request)
{
    mca_pml_cm_hvy_recv_request_t *recvreq;
    uint32_t flags = 0;
#if OPAL_ENABLE_HETEROGENEOUS_SUPPORT
    ompi_proc_t* ompi_proc;
#endif

    MCA_PML_CM_HVY_RECV_REQUEST_ALLOC(recvreq);
    if( OPAL_UNLIKELY(NULL == recvreq) ) return OMPI_ERR_OUT_OF_RESOURCE;

    MCA_PML_CM_HVY_RECV_REQUEST_INIT(recvreq, ompi_proc, comm, tag, src,
                                     datatype, addr, count, flags, true);

    *request = (ompi_request_t*) recvreq;

    return OMPI_SUCCESS;
}

__opal_attribute_always_inline__ static inline int
mca_pml_cm_irecv(void *addr,
                 size_t count,
                 ompi_datatype_t * datatype,
                 int src,
                 int tag,
                 struct ompi_communicator_t *comm,
                 struct ompi_request_t **request)
{
    int ret;
    uint32_t flags = 0;
    mca_pml_cm_thin_recv_request_t *recvreq;
#if OPAL_ENABLE_HETEROGENEOUS_SUPPORT
    ompi_proc_t* ompi_proc = NULL;
#endif

    MCA_PML_CM_THIN_RECV_REQUEST_ALLOC(recvreq);
    if( OPAL_UNLIKELY(NULL == recvreq) ) return OMPI_ERR_OUT_OF_RESOURCE;

    MCA_PML_CM_THIN_RECV_REQUEST_INIT(recvreq,
                                      ompi_proc,
                                      comm,
                                      src,
                                      datatype,
                                      addr,
                                      count,
                                      flags);

    MCA_PML_CM_THIN_RECV_REQUEST_START(recvreq, comm, tag, src, ret);

    if( OPAL_LIKELY(OMPI_SUCCESS == ret) ) *request = (ompi_request_t*) recvreq;

    return ret;
}

__opal_attribute_always_inline__ static inline void
mca_pml_cm_recv_fast_completion(struct mca_mtl_request_t *mtl_request)
{
    // Do nothing!
    ompi_request_complete(mtl_request->ompi_req, true);
    return;
}

__opal_attribute_always_inline__ static inline int
mca_pml_cm_recv(void *addr,
                size_t count,
                ompi_datatype_t * datatype,
                int src,
                int tag,
                struct ompi_communicator_t *comm,
                ompi_status_public_t * status)
{
    int ret;
    uint32_t flags = 0;
#if OPAL_ENABLE_HETEROGENEOUS_SUPPORT
    ompi_proc_t *ompi_proc;
#endif
    opal_convertor_t convertor;
    mca_pml_cm_request_t req;
    mca_mtl_request_t *req_mtl =
            alloca(sizeof(mca_mtl_request_t) + ompi_mtl->mtl_request_size);

    OBJ_CONSTRUCT(&convertor, opal_convertor_t);
    req_mtl->ompi_req = &req.req_ompi;
    req_mtl->completion_callback = mca_pml_cm_recv_fast_completion;

    req.req_pml_type = MCA_PML_CM_REQUEST_RECV_THIN;
    req.req_free_called = false;
    req.req_ompi.req_complete = false;
    req.req_ompi.req_complete_cb = NULL;
    req.req_ompi.req_state = OMPI_REQUEST_ACTIVE;
    req.req_ompi.req_status.MPI_TAG = OMPI_ANY_TAG;
    req.req_ompi.req_status.MPI_ERROR = OMPI_SUCCESS;
    req.req_ompi.req_status._cancelled = 0;

#if OPAL_ENABLE_HETEROGENEOUS_SUPPORT
    if( MPI_ANY_SOURCE == src ) {
        ompi_proc = ompi_proc_local_proc;
    } else {
        ompi_proc = ompi_comm_peer_lookup( comm, src );
    }

    MCA_PML_CM_SWITCH_CUDA_CONVERTOR_OFF(flags, datatype, count);

    opal_convertor_copy_and_prepare_for_recv(
	ompi_proc->super.proc_convertor,
		&(datatype->super),
		count,
		addr,
                flags,
		&convertor );
#else
    MCA_PML_CM_SWITCH_CUDA_CONVERTOR_OFF(flags, datatype, count);

    opal_convertor_copy_and_prepare_for_recv(
	ompi_mpi_local_convertor,
		&(datatype->super),
		count,
		addr,
                flags,
		&convertor );
#endif

    ret = OMPI_MTL_CALL(irecv(ompi_mtl,
                              comm,
                              src,
                              tag,
                              &convertor,
                              req_mtl));
    if( OPAL_UNLIKELY(OMPI_SUCCESS != ret) ) {
	OBJ_DESTRUCT(&convertor);
        return ret;
    }

    ompi_request_wait_completion(&req.req_ompi);

    if (NULL != status) {  /* return status */
        *status = req.req_ompi.req_status;
    }
    ret = req.req_ompi.req_status.MPI_ERROR;
    OBJ_DESTRUCT(&convertor);
    return ret;
}

__opal_attribute_always_inline__ static inline int
mca_pml_cm_isend_init(const void* buf,
                        size_t count,
                        ompi_datatype_t* datatype,
                        int dst,
                        int tag,
                        mca_pml_base_send_mode_t sendmode,
                        ompi_communicator_t* comm,
                        ompi_request_t** request)
{
    mca_pml_cm_hvy_send_request_t *sendreq;
    uint32_t flags = 0;
#if OPAL_ENABLE_HETEROGENEOUS_SUPPORT
    ompi_proc_t* ompi_proc;
#endif

    MCA_PML_CM_HVY_SEND_REQUEST_ALLOC(sendreq, comm, dst, ompi_proc);
    if (OPAL_UNLIKELY(NULL == sendreq)) return OMPI_ERR_OUT_OF_RESOURCE;

    MCA_PML_CM_HVY_SEND_REQUEST_INIT(sendreq, ompi_proc, comm, tag, dst,
                                     datatype, sendmode, true, false, buf, count, flags);

    /* Work around a leak in start by marking this request as complete. The
     * problem occured because we do not have a way to differentiate an
     * inital request and an incomplete pml request in start. This line
     * allows us to detect this state. */
    sendreq->req_send.req_base.req_pml_complete = true;

    *request = (ompi_request_t*) sendreq;

    return OMPI_SUCCESS;
}

__opal_attribute_always_inline__ static inline int
mca_pml_cm_isend(const void* buf,
                   size_t count,
                   ompi_datatype_t* datatype,
                   int dst,
                   int tag,
                   mca_pml_base_send_mode_t sendmode,
                   ompi_communicator_t* comm,
                   ompi_request_t** request)
{
    int ret;
    uint32_t flags = 0;

    if(sendmode == MCA_PML_BASE_SEND_BUFFERED ) {
        mca_pml_cm_hvy_send_request_t* sendreq;
#if OPAL_ENABLE_HETEROGENEOUS_SUPPORT
        ompi_proc_t* ompi_proc = NULL;
#endif

        MCA_PML_CM_HVY_SEND_REQUEST_ALLOC(sendreq, comm, dst, ompi_proc);
        if (OPAL_UNLIKELY(NULL == sendreq)) return OMPI_ERR_OUT_OF_RESOURCE;

        MCA_PML_CM_HVY_SEND_REQUEST_INIT(sendreq,
                                         ompi_proc,
                                         comm,
                                         tag,
                                         dst,
                                         datatype,
                                         sendmode,
                                         false,
                                         false,
                                         buf,
                                         count,
                                         flags);

        MCA_PML_CM_HVY_SEND_REQUEST_START( sendreq, ret);

        if (OPAL_LIKELY(OMPI_SUCCESS == ret)) *request = (ompi_request_t*) sendreq;

    } else {
        mca_pml_cm_thin_send_request_t* sendreq;
#if OPAL_ENABLE_HETEROGENEOUS_SUPPORT
        ompi_proc_t* ompi_proc = NULL;
#endif
        MCA_PML_CM_THIN_SEND_REQUEST_ALLOC(sendreq, comm, dst, ompi_proc);
        if (OPAL_UNLIKELY(NULL == sendreq)) return OMPI_ERR_OUT_OF_RESOURCE;

        MCA_PML_CM_THIN_SEND_REQUEST_INIT(sendreq,
                                          ompi_proc,
                                          comm,
                                          tag,
                                          dst,
                                          datatype,
                                          sendmode,
                                          buf,
                                          count,
                                          flags);

        MCA_PML_CM_THIN_SEND_REQUEST_START(
                                           sendreq,
                                           comm,
                                           tag,
                                           dst,
                                           sendmode,
                                           false,
                                           ret);

        if (OPAL_LIKELY(OMPI_SUCCESS == ret)) *request = (ompi_request_t*) sendreq;

    }

    return ret;
}

__opal_attribute_always_inline__ static inline int
mca_pml_cm_send(const void *buf,
                size_t count,
                ompi_datatype_t* datatype,
                int dst,
                int tag,
                mca_pml_base_send_mode_t sendmode,
                ompi_communicator_t* comm)
{
    int ret = OMPI_ERROR;
    uint32_t flags = 0;
    ompi_proc_t * ompi_proc;

    if(sendmode == MCA_PML_BASE_SEND_BUFFERED) {
        mca_pml_cm_hvy_send_request_t *sendreq;

        MCA_PML_CM_HVY_SEND_REQUEST_ALLOC(sendreq, comm, dst, ompi_proc);
        if (OPAL_UNLIKELY(NULL == sendreq)) return OMPI_ERR_OUT_OF_RESOURCE;

        MCA_PML_CM_HVY_SEND_REQUEST_INIT(sendreq,
                                         ompi_proc,
                                         comm,
                                         tag,
                                         dst,
                                         datatype,
                                         sendmode,
                                         false,
                                         false,
                                         buf,
                                         count,
                                         flags);
        MCA_PML_CM_HVY_SEND_REQUEST_START(sendreq, ret);
        if (OPAL_UNLIKELY(OMPI_SUCCESS != ret)) {
            MCA_PML_CM_HVY_SEND_REQUEST_RETURN(sendreq);
            return ret;
        }

        ompi_request_free( (ompi_request_t**)&sendreq );
    } else {
        opal_convertor_t convertor;
	OBJ_CONSTRUCT(&convertor, opal_convertor_t);
#if !(OPAL_ENABLE_HETEROGENEOUS_SUPPORT)
	if (opal_datatype_is_contiguous_memory_layout(&datatype->super, count)) {

		convertor.remoteArch = ompi_mpi_local_convertor->remoteArch;
		convertor.flags      = ompi_mpi_local_convertor->flags;
		convertor.master     = ompi_mpi_local_convertor->master;

		convertor.local_size = count * datatype->super.size;
		convertor.pBaseBuf   = (unsigned char*)buf + datatype->super.true_lb;
		convertor.count      = count;
		convertor.pDesc      = &datatype->super;
	} else
#endif
	{
		ompi_proc = ompi_comm_peer_lookup(comm, dst);

                MCA_PML_CM_SWITCH_CUDA_CONVERTOR_OFF(flags, datatype, count);

		opal_convertor_copy_and_prepare_for_send(
		ompi_proc->super.proc_convertor,
			&datatype->super, count, buf, flags,
			&convertor);
	}

        ret = OMPI_MTL_CALL(send(ompi_mtl,
                                 comm,
                                 dst,
                                 tag,
                                 &convertor,
                                 sendmode));
	OBJ_DESTRUCT(&convertor);
    }

    return ret;
}

__opal_attribute_always_inline__ static inline int
mca_pml_cm_iprobe(int src, int tag,
                   struct ompi_communicator_t *comm,
                   int *matched, ompi_status_public_t * status)
{
    return OMPI_MTL_CALL(iprobe(ompi_mtl,
                                comm, src, tag,
                                matched, status));
}

__opal_attribute_always_inline__ static inline int
mca_pml_cm_probe(int src, int tag,
                  struct ompi_communicator_t *comm,
                  ompi_status_public_t * status)
{
    int ret, matched = 0;

    while (true) {
        ret = OMPI_MTL_CALL(iprobe(ompi_mtl,
                                   comm, src, tag,
                                   &matched, status));
        if (OMPI_SUCCESS != ret) break;
        if (matched) break;
        opal_progress();
    }

    return ret;
}

__opal_attribute_always_inline__ static inline int
mca_pml_cm_improbe(int src,
                   int tag,
                   struct ompi_communicator_t* comm,
                   int *matched,
                   struct ompi_message_t **message,
                   ompi_status_public_t* status)
{
    return OMPI_MTL_CALL(improbe(ompi_mtl,
                                 comm, src, tag,
                                 matched, message,
                                 status));
}

__opal_attribute_always_inline__ static inline int
mca_pml_cm_mprobe(int src,
                  int tag,
                  struct ompi_communicator_t* comm,
                  struct ompi_message_t **message,
                  ompi_status_public_t* status)
{
    int ret, matched = 0;

    while (true) {
        ret = OMPI_MTL_CALL(improbe(ompi_mtl,
                                    comm, src, tag,
                                    &matched, message,
                                    status));
        if (OMPI_SUCCESS != ret) break;
        if (matched) break;
        opal_progress();
    }

    return ret;
}

__opal_attribute_always_inline__ static inline int
mca_pml_cm_imrecv(void *buf,
                  size_t count,
                  ompi_datatype_t *datatype,
                  struct ompi_message_t **message,
                  struct ompi_request_t **request)
{
    int ret;
    uint32_t flags = 0;
    mca_pml_cm_thin_recv_request_t *recvreq;
#if OPAL_ENABLE_HETEROGENEOUS_SUPPORT
    ompi_proc_t* ompi_proc;
#endif
    ompi_communicator_t *comm = (*message)->comm;

    MCA_PML_CM_THIN_RECV_REQUEST_ALLOC(recvreq);
    if( OPAL_UNLIKELY(NULL == recvreq) ) return OMPI_ERR_OUT_OF_RESOURCE;

    MCA_PML_CM_THIN_RECV_REQUEST_INIT(recvreq,
                                      ompi_proc,
                                      comm,
                                      (*message)->peer,
                                      datatype,
                                      buf,
                                      count,
                                      flags);

    MCA_PML_CM_THIN_RECV_REQUEST_MATCHED_START(recvreq, message, ret);

    if( OPAL_LIKELY(OMPI_SUCCESS == ret) ) *request = (ompi_request_t*) recvreq;

    return ret;
}

__opal_attribute_always_inline__ static inline int
mca_pml_cm_mrecv(void *buf,
                 size_t count,
                 ompi_datatype_t *datatype,
                 struct ompi_message_t **message,
                 ompi_status_public_t* status)
{
    int ret;
    uint32_t flags = 0;
    mca_pml_cm_thin_recv_request_t *recvreq;
#if OPAL_ENABLE_HETEROGENEOUS_SUPPORT
    ompi_proc_t* ompi_proc;
#endif
    ompi_communicator_t *comm = (*message)->comm;

    MCA_PML_CM_THIN_RECV_REQUEST_ALLOC(recvreq);
    if( OPAL_UNLIKELY(NULL == recvreq) ) return OMPI_ERR_OUT_OF_RESOURCE;

    MCA_PML_CM_THIN_RECV_REQUEST_INIT(recvreq,
                                      ompi_proc,
                                      comm,
                                      (*message)->peer,
                                      datatype,
                                      buf,
                                      count,
                                      flags);

    MCA_PML_CM_THIN_RECV_REQUEST_MATCHED_START(recvreq,
                                               message, ret);
    if( OPAL_UNLIKELY(OMPI_SUCCESS != ret) ) {
        MCA_PML_CM_THIN_RECV_REQUEST_RETURN(recvreq);
        return ret;
    }

    ompi_request_wait_completion(&recvreq->req_base.req_ompi);

    if (NULL != status) {  /* return status */
        *status = recvreq->req_base.req_ompi.req_status;
    }
    ret = recvreq->req_base.req_ompi.req_status.MPI_ERROR;
    ompi_request_free( (ompi_request_t**)&recvreq );

    return ret;
}

OMPI_DECLSPEC extern int mca_pml_cm_start(size_t count, ompi_request_t** requests);


OMPI_DECLSPEC extern int mca_pml_cm_dump(struct ompi_communicator_t* comm,
                                         int verbose);

OMPI_DECLSPEC extern int mca_pml_cm_cancel(struct ompi_request_t *request, int flag);

END_C_DECLS

#endif  /* PML_CM_H_HAS_BEEN_INCLUDED */
