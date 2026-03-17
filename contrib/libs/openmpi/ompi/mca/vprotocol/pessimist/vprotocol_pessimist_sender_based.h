/*
 * Copyright (c) 2004-2007 The Trustees of the University of Tennessee.
 *                         All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef __VPROTOCOL_PESSIMIST_SENDERBASED_H__
#define __VPROTOCOL_PESSIMIST_SENDERBASED_H__

#include "ompi_config.h"
#include "ompi/mca/pml/base/pml_base_sendreq.h"
#include "ompi/mca/pml/v/pml_v_output.h"
#include "vprotocol_pessimist_sender_based_types.h"
#include "vprotocol_pessimist_request.h"
#include "vprotocol_pessimist.h"

BEGIN_C_DECLS

/** Prepare for using the sender based storage
  */
int vprotocol_pessimist_sender_based_init(const char *mmapfile, size_t size);

/** Cleanup mmap etc
  */
void vprotocol_pessimist_sender_based_finalize(void);

/** Manage mmap floating window, allocating enough memory for the message to be
  * asynchronously copied to disk.
  */
void vprotocol_pessimist_sender_based_alloc(size_t len);


/*******************************************************************************
 * Convertor pack (blocking) method (good latency, bad bandwidth)
 */
#if defined(SB_USE_PACK_METHOD)
static inline void __SENDER_BASED_METHOD_COPY(mca_pml_base_send_request_t *pmlreq)
{
    if(0 != pmlreq->req_bytes_packed)
    {
        opal_convertor_t conv;
        size_t max_data;
        size_t zero = 0;
        unsigned int iov_count = 1;
        struct iovec iov;

        max_data = iov.iov_len = pmlreq->req_bytes_packed;
        iov.iov_base = (IOVBASE_TYPE *) VPESSIMIST_SEND_FTREQ(pmlreq)->sb.cursor;
        opal_convertor_clone_with_position( &pmlreq->req_base.req_convertor,
                                            &conv, 0, &zero );
        opal_convertor_pack(&conv, &iov, &iov_count, &max_data);
    }
}

#define __SENDER_BASED_METHOD_FLUSH(REQ)


/*******************************************************************************
 * Convertor replacement (non blocking) method (under testing)
 */
#elif defined(SB_USE_CONVERTOR_METHOD)
int32_t vprotocol_pessimist_sender_based_convertor_advance(opal_convertor_t*,
                                                            struct iovec*,
                                                            uint32_t*,
                                                            size_t*);

#define __SENDER_BASED_METHOD_COPY(REQ) do {                                  \
    opal_convertor_t *pConv;                                                  \
    mca_vprotocol_pessimist_send_request_t *ftreq;                            \
                                                                              \
    pConv = & (REQ)->req_base.req_convertor;                                  \
    ftreq = VPESSIMIST_SEND_FTREQ(REQ);                                       \
    ftreq->sb.conv_flags = pConv->flags;                                      \
    ftreq->sb.conv_advance = pConv->fAdvance;                                 \
                                                                              \
    pConv->flags &= ~CONVERTOR_NO_OP;                                         \
    pConv->fAdvance = vprotocol_pessimist_sender_based_convertor_advance;     \
} while(0)

#define __SENDER_BASED_METHOD_FLUSH(REQ)

#define VPESSIMIST_CONV_REQ(CONV) ((mca_vprotocol_pessimist_send_request_t *) \
    (mca_vprotocol_pessimist.sender_based.sb_conv_to_pessimist_offset +       \
     (uintptr_t) ((CONV)->clone_of)))


/*******************************************************************************
 * progress method
 */
#elif defined(SB_USE_PROGRESS_METHOD)
static inline void __SENDER_BASED_METHOD_COPY(mca_pml_base_send_request_t *req)
{
    if(req->req_bytes_packed)
    {
        mca_vprotocol_pessimist_send_request_t *ftreq = VPESSIMIST_SEND_FTREQ(req);
        ftreq->sb.bytes_progressed = 0;
        opal_list_append(&mca_vprotocol_pessimist.sender_based.sb_sendreq,
                         &ftreq->list_item);
    }
}

static inline int vprotocol_pessimist_sb_progress_req(mca_pml_base_send_request_t *req)
{
    mca_vprotocol_pessimist_request_t *ftreq = VPESSIMIST_SEND_FTREQ(req);
    size_t max_data = 0;

    if(ftreq->sb.bytes_progressed < req->req_bytes_packed)
    {
        opal_convertor_t conv;
        unsigned int iov_count = 1;
        struct iovec iov;
        uintptr_t position = ftreq->sb.bytes_progressed;
        max_data = req->req_bytes_packed - ftreq->sb.bytes_progressed;
        iov.iov_len = max_data;
        iov.iov_base = (IOVBASE_TYPE *) (ftreq->sb.cursor + position);

        V_OUTPUT_VERBOSE(80, "pessimist:\tsb\tprgress\t%"PRIpclock"\tsize %lu from position %lu", ftreq->reqid, max_data, position);
        opal_convertor_clone_with_position(&req->req_base.req_convertor,
                                           &conv, 0, &position );
        opal_convertor_pack(&conv, &iov, &iov_count, &max_data);
        ftreq->sb.bytes_progressed += max_data;
    }
    return max_data;
}

static inline int vprotocol_pessimist_sb_progress_all_reqs(void)
{
    int ret = 0;

    /* progress any waiting Sender Based copy */
    if(!opal_list_is_empty(&mca_vprotocol_pessimist.sender_based.sb_sendreq))
    {
        mca_vprotocol_pessimist_request_t *ftreq = (mca_vprotocol_pessimist_request_t *)
            opal_list_remove_first(&mca_vprotocol_pessimist.sender_based.sb_sendreq);
        if(vprotocol_pessimist_sb_progress_req(VPROTOCOL_SEND_REQ(ftreq)))
            ret = 1;
        opal_list_append(&mca_vprotocol_pessimist.sender_based.sb_sendreq,
                         &ftreq->list_item);
    }
    return ret;
}

static inline void __SENDER_BASED_METHOD_FLUSH(ompi_request_t *req)
{
    mca_pml_base_send_request_t *pmlreq = (mca_pml_base_send_request_t *) req;

    if((pmlreq->req_base.req_type == MCA_PML_REQUEST_SEND) &&
       pmlreq->req_bytes_packed)
    {
        mca_vprotocol_pessimist_request_t *ftreq = VPESSIMIST_SEND_FTREQ(req);
        assert(!opal_list_is_empty(&mca_vprotocol_pessimist.sender_based.sb_sendreq));
        opal_list_remove_item(&mca_vprotocol_pessimist.sender_based.sb_sendreq,
                              (opal_list_item_t *) ftreq);
        vprotocol_pessimist_sb_progress_req(pmlreq);
        assert(pmlreq->req_bytes_packed == ftreq->sb.bytes_progressed);
    }
}

#endif /* SB_USE_*_METHOD */


/** Copy data associated to a pml_base_send_request_t to the sender based
 * message payload buffer
 */
static inline void vprotocol_pessimist_sender_based_copy_start(ompi_request_t *req)
{
    vprotocol_pessimist_sender_based_header_t *sbhdr;
    mca_vprotocol_pessimist_request_t *ftreq = VPESSIMIST_SEND_FTREQ(req);
    mca_pml_base_send_request_t *pmlreq = (mca_pml_base_send_request_t *) req;

    /* Allocate enough sender-based space to hold the message */
    if(mca_vprotocol_pessimist.sender_based.sb_available <
            pmlreq->req_bytes_packed +
            sizeof(vprotocol_pessimist_sender_based_header_t))
    {
        vprotocol_pessimist_sender_based_alloc(pmlreq->req_bytes_packed);
    }

    /* Copy message header to the sender-based space */
    /* /!\ This is NOT thread safe */
    ftreq->sb.cursor = mca_vprotocol_pessimist.sender_based.sb_cursor;
#if 1
    mca_vprotocol_pessimist.sender_based.sb_cursor +=
        sizeof(vprotocol_pessimist_sender_based_header_t) +
        pmlreq->req_bytes_packed;
    mca_vprotocol_pessimist.sender_based.sb_available -=
        sizeof(vprotocol_pessimist_sender_based_header_t) +
        pmlreq->req_bytes_packed;
#endif
    sbhdr = (vprotocol_pessimist_sender_based_header_t *) ftreq->sb.cursor;
    sbhdr->size = pmlreq->req_bytes_packed;
    sbhdr->dst = pmlreq->req_base.req_peer;
    sbhdr->tag = pmlreq->req_base.req_tag;
    sbhdr->contextid = pmlreq->req_base.req_comm->c_contextid;
    sbhdr->sequence = pmlreq->req_base.req_sequence;
    ftreq->sb.cursor += sizeof(vprotocol_pessimist_sender_based_header_t);
    V_OUTPUT_VERBOSE(70, "pessimist:\tsb\tsend\t%"PRIpclock"\tsize %lu (+%lu header)", VPESSIMIST_FTREQ(req)->reqid, (long unsigned)pmlreq->req_bytes_packed, (long unsigned)sizeof(vprotocol_pessimist_sender_based_header_t));

    /* Use one of the previous data copy method */
    __SENDER_BASED_METHOD_COPY(pmlreq);
}

/** Ensure sender based is finished before allowing user to touch send buffer
 */
#define vprotocol_pessimist_sender_based_flush(REQ) __SENDER_BASED_METHOD_FLUSH(REQ)

END_C_DECLS

#endif

