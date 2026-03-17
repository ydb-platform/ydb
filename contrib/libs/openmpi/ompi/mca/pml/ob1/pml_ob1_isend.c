/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2018 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007-2016 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2014      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include "pml_ob1.h"
#include "pml_ob1_sendreq.h"
#include "pml_ob1_recvreq.h"
#include "ompi/peruse/peruse-internal.h"
#include "ompi/runtime/ompi_spc.h"

/**
 * Single usage request. As we allow recursive calls (as an
 * example from the request completion callback), we cannot rely
 * on using a global request. Thus, once a send acquires ownership
 * of this global request, it should set it to NULL to prevent
 * the reuse until the first user completes.
 */
mca_pml_ob1_send_request_t *mca_pml_ob1_sendreq = NULL;

int mca_pml_ob1_isend_init(const void *buf,
                           size_t count,
                           ompi_datatype_t * datatype,
                           int dst,
                           int tag,
                           mca_pml_base_send_mode_t sendmode,
                           ompi_communicator_t * comm,
                           ompi_request_t ** request)
{
    mca_pml_ob1_send_request_t *sendreq = NULL;
    MCA_PML_OB1_SEND_REQUEST_ALLOC(comm, dst, sendreq);
    if (NULL == sendreq)
        return OMPI_ERR_OUT_OF_RESOURCE;

    MCA_PML_OB1_SEND_REQUEST_INIT(sendreq,
                                  buf,
                                  count,
                                  datatype,
                                  dst, tag,
                                  comm, sendmode, true);

    PERUSE_TRACE_COMM_EVENT (PERUSE_COMM_REQ_ACTIVATE,
                             &(sendreq)->req_send.req_base,
                             PERUSE_SEND);

    /* Work around a leak in start by marking this request as complete. The
     * problem occured because we do not have a way to differentiate an
     * inital request and an incomplete pml request in start. This line
     * allows us to detect this state. */
    sendreq->req_send.req_base.req_pml_complete = true;

    *request = (ompi_request_t *) sendreq;
    return OMPI_SUCCESS;
}

/* try to get a small message out on to the wire quickly */
static inline int mca_pml_ob1_send_inline (const void *buf, size_t count,
                                           ompi_datatype_t * datatype,
                                           int dst, int tag, int16_t seqn,
                                           ompi_proc_t *dst_proc, mca_bml_base_endpoint_t* endpoint,
                                           ompi_communicator_t * comm)
{
    mca_pml_ob1_match_hdr_t match;
    mca_bml_base_btl_t *bml_btl;
    opal_convertor_t convertor;
    size_t size;
    int rc;

    bml_btl = mca_bml_base_btl_array_get_next(&endpoint->btl_eager);
    if( NULL == bml_btl->btl->btl_sendi)
        return OMPI_ERR_NOT_AVAILABLE;

    ompi_datatype_type_size (datatype, &size);
    if ((size * count) > 256) {  /* some random number */
        return OMPI_ERR_NOT_AVAILABLE;
    }

    if (count > 0) {
        /* initialize just enough of the convertor to avoid a SEGV in opal_convertor_cleanup */
        OBJ_CONSTRUCT(&convertor, opal_convertor_t);

        /* We will create a convertor specialized for the        */
        /* remote architecture and prepared with the datatype.   */
        opal_convertor_copy_and_prepare_for_send (dst_proc->super.proc_convertor,
                                                  (const struct opal_datatype_t *) datatype,
                                                  count, buf, 0, &convertor);
        opal_convertor_get_packed_size (&convertor, &size);
    } else {
        size = 0;
    }

    mca_pml_ob1_match_hdr_prepare (&match, MCA_PML_OB1_HDR_TYPE_MATCH, 0,
                                   comm->c_contextid, comm->c_my_rank,
                                   tag, seqn);

    ob1_hdr_hton(&match, MCA_PML_OB1_HDR_TYPE_MATCH, dst_proc);

    /* try to send immediately */
    rc = mca_bml_base_sendi (bml_btl, &convertor, &match, OMPI_PML_OB1_MATCH_HDR_LEN,
                             size, MCA_BTL_NO_ORDER, MCA_BTL_DES_FLAGS_PRIORITY | MCA_BTL_DES_FLAGS_BTL_OWNERSHIP,
                             MCA_PML_OB1_HDR_TYPE_MATCH, NULL);

    /* This #if is required due to an issue that arises with the IBM CI (XL Compiler).
     * The compiler doesn't seem to like having a compiler hint attached to an if
     * statement that only has a no-op inside and has the following error:
     *
     * 1586-494 (U) INTERNAL COMPILER ERROR: Signal 11.
     */
#if SPC_ENABLE == 1
    if(OPAL_LIKELY(rc == OPAL_SUCCESS)) {
        SPC_USER_OR_MPI(tag, (ompi_spc_value_t)size, OMPI_SPC_BYTES_SENT_USER, OMPI_SPC_BYTES_SENT_MPI);
    }
#endif

    if (count > 0) {
        opal_convertor_cleanup (&convertor);
    }

    if (OPAL_UNLIKELY(OMPI_SUCCESS != rc)) {
	return rc;
    }

    return (int) size;
}

int mca_pml_ob1_isend(const void *buf,
                      size_t count,
                      ompi_datatype_t * datatype,
                      int dst,
                      int tag,
                      mca_pml_base_send_mode_t sendmode,
                      ompi_communicator_t * comm,
                      ompi_request_t ** request)
{
    mca_pml_ob1_comm_proc_t *ob1_proc = mca_pml_ob1_peer_lookup (comm, dst);
    mca_pml_ob1_send_request_t *sendreq = NULL;
    ompi_proc_t *dst_proc = ob1_proc->ompi_proc;
    mca_bml_base_endpoint_t* endpoint = mca_bml_base_get_endpoint (dst_proc);
    int16_t seqn = 0;
    int rc;

    if (OPAL_UNLIKELY(NULL == endpoint)) {
        return OMPI_ERR_UNREACH;
    }

    if (!OMPI_COMM_CHECK_ASSERT_ALLOW_OVERTAKE(comm)) {
        seqn = (uint16_t) OPAL_THREAD_ADD_FETCH32(&ob1_proc->send_sequence, 1);
    }

    if (MCA_PML_BASE_SEND_SYNCHRONOUS != sendmode) {
        rc = mca_pml_ob1_send_inline (buf, count, datatype, dst, tag, seqn, dst_proc,
                                      endpoint, comm);
        if (OPAL_LIKELY(0 <= rc)) {
            /* NTH: it is legal to return ompi_request_empty since the only valid
             * field in a send completion status is whether or not the send was
             * cancelled (which it can't be at this point anyway). */
            *request = &ompi_request_empty;
            return OMPI_SUCCESS;
        }
    }

    MCA_PML_OB1_SEND_REQUEST_ALLOC(comm, dst, sendreq);
    if (NULL == sendreq)
        return OMPI_ERR_OUT_OF_RESOURCE;

    MCA_PML_OB1_SEND_REQUEST_INIT(sendreq,
                                  buf,
                                  count,
                                  datatype,
                                  dst, tag,
                                  comm, sendmode, false);

    PERUSE_TRACE_COMM_EVENT (PERUSE_COMM_REQ_ACTIVATE,
                             &(sendreq)->req_send.req_base,
                             PERUSE_SEND);

    MCA_PML_OB1_SEND_REQUEST_START_W_SEQ(sendreq, endpoint, seqn, rc);
    *request = (ompi_request_t *) sendreq;
    return rc;
}

int mca_pml_ob1_send(const void *buf,
                     size_t count,
                     ompi_datatype_t * datatype,
                     int dst,
                     int tag,
                     mca_pml_base_send_mode_t sendmode,
                     ompi_communicator_t * comm)
{
    mca_pml_ob1_comm_proc_t *ob1_proc = mca_pml_ob1_peer_lookup (comm, dst);
    ompi_proc_t *dst_proc = ob1_proc->ompi_proc;
    mca_bml_base_endpoint_t* endpoint = mca_bml_base_get_endpoint (dst_proc);
    mca_pml_ob1_send_request_t *sendreq = NULL;
    int16_t seqn = 0;
    int rc;

    if (OPAL_UNLIKELY(NULL == endpoint)) {
        return OMPI_ERR_UNREACH;
    }

    if (OPAL_UNLIKELY(MCA_PML_BASE_SEND_BUFFERED == sendmode)) {
        /* large buffered sends *need* a real request so use isend instead */
        ompi_request_t *brequest;

        rc = mca_pml_ob1_isend (buf, count, datatype, dst, tag, sendmode, comm, &brequest);
        if (OPAL_UNLIKELY(OMPI_SUCCESS != rc)) {
            return rc;
        }

        ompi_request_wait_completion (brequest);
        ompi_request_free (&brequest);
        return OMPI_SUCCESS;
    }

    if (!OMPI_COMM_CHECK_ASSERT_ALLOW_OVERTAKE(comm)) {
        seqn = (uint16_t) OPAL_THREAD_ADD_FETCH32(&ob1_proc->send_sequence, 1);
    }

    /**
     * The immediate send will not have a request, so they are
     * intracable from the point of view of any debugger attached to
     * the parallel application.
     */
    if (MCA_PML_BASE_SEND_SYNCHRONOUS != sendmode) {
        rc = mca_pml_ob1_send_inline (buf, count, datatype, dst, tag, seqn, dst_proc,
                                      endpoint, comm);
        if (OPAL_LIKELY(0 <= rc)) {
            return OMPI_SUCCESS;
        }
    }

    if (OPAL_LIKELY(!ompi_mpi_thread_multiple)) {
        sendreq = mca_pml_ob1_sendreq;
        mca_pml_ob1_sendreq = NULL;
    }

    if( OPAL_UNLIKELY(NULL == sendreq) ) {
        MCA_PML_OB1_SEND_REQUEST_ALLOC(comm, dst, sendreq);
        if (NULL == sendreq)
            return OMPI_ERR_TEMP_OUT_OF_RESOURCE;
    }

    sendreq->req_send.req_base.req_proc = dst_proc;
    sendreq->rdma_frag = NULL;

    MCA_PML_OB1_SEND_REQUEST_INIT(sendreq,
                                  buf,
                                  count,
                                  datatype,
                                  dst, tag,
                                  comm, sendmode, false);

    PERUSE_TRACE_COMM_EVENT (PERUSE_COMM_REQ_ACTIVATE,
                             &sendreq->req_send.req_base,
                             PERUSE_SEND);

    MCA_PML_OB1_SEND_REQUEST_START_W_SEQ(sendreq, endpoint, seqn, rc);
    if (OPAL_LIKELY(rc == OMPI_SUCCESS)) {
        ompi_request_wait_completion(&sendreq->req_send.req_base.req_ompi);

        rc = sendreq->req_send.req_base.req_ompi.req_status.MPI_ERROR;
    }

    if (OPAL_UNLIKELY(ompi_mpi_thread_multiple || NULL != mca_pml_ob1_sendreq)) {
        MCA_PML_OB1_SEND_REQUEST_RETURN(sendreq);
    } else {
        mca_pml_ob1_send_request_fini (sendreq);
        mca_pml_ob1_sendreq = sendreq;
    }

    return rc;
}
