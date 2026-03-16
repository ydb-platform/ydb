/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2011 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2009 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2007 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2006-2007 Voltaire. All rights reserved.
 * Copyright (c) 2009      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2010-2015 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2015      Mellanox Technologies. All rights reserved.
 *
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"

#include "btl_vader.h"
#include "btl_vader_frag.h"
#include "btl_vader_fifo.h"

#include "btl_vader_fbox.h"

/**
 * Initiate an inline send to the peer.
 *
 * @param btl (IN)      BTL module
 * @param peer (IN)     BTL peer addressing
 */
int mca_btl_vader_sendi (struct mca_btl_base_module_t *btl,
                         struct mca_btl_base_endpoint_t *endpoint,
                         struct opal_convertor_t *convertor,
                         void *header, size_t header_size,
                         size_t payload_size, uint8_t order,
                         uint32_t flags, mca_btl_base_tag_t tag,
                         mca_btl_base_descriptor_t **descriptor)
{
    mca_btl_vader_frag_t *frag;
    void *data_ptr = NULL;
    size_t length;

    /* don't attempt sendi if there are pending fragments on the endpoint */
    if (OPAL_UNLIKELY(opal_list_get_size (&endpoint->pending_frags))) {
        if (descriptor) {
            *descriptor = NULL;
        }

        return OPAL_ERR_OUT_OF_RESOURCE;
    }

    if (payload_size) {
        opal_convertor_get_current_pointer (convertor, &data_ptr);
    }

    if (!(payload_size && opal_convertor_need_buffers (convertor)) &&
        mca_btl_vader_fbox_sendi (endpoint, tag, header, header_size, data_ptr, payload_size)) {
        return OPAL_SUCCESS;
    }

    length = header_size + payload_size;

    /* allocate a fragment, giving up if we can't get one */
    frag = (mca_btl_vader_frag_t *) mca_btl_vader_alloc (btl, endpoint, order, length,
                                                         flags | MCA_BTL_DES_FLAGS_BTL_OWNERSHIP);
    if (OPAL_UNLIKELY(NULL == frag)) {
        if (descriptor) {
            *descriptor = NULL;
        }

        return OPAL_ERR_OUT_OF_RESOURCE;
    }

    /* fill in fragment fields */
    frag->hdr->len = length;
    frag->hdr->tag = tag;

    /* write the match header (with MPI comm/tag/etc. info) */
    memcpy (frag->segments[0].seg_addr.pval, header, header_size);

    /* write the message data if there is any */
    /* we can't use single-copy semantics here since as caller will consider the send
       complete when we return */
    if (payload_size) {
        uint32_t iov_count = 1;
        struct iovec iov;

        /* pack the data into the supplied buffer */
        iov.iov_base = (IOVBASE_TYPE *)((uintptr_t)frag->segments[0].seg_addr.pval + header_size);
        iov.iov_len  = length = payload_size;

        (void) opal_convertor_pack (convertor, &iov, &iov_count, &length);

        assert (length == payload_size);
    }

    /* write the fragment pointer to peer's the FIFO. the progress function will return the fragment */
    if (!vader_fifo_write_ep (frag->hdr, endpoint)) {
        if (descriptor) {
            *descriptor = &frag->base;
        } else {
            mca_btl_vader_free (btl, &frag->base);
        }
        return OPAL_ERR_OUT_OF_RESOURCE;
    }

    return OPAL_SUCCESS;
}
