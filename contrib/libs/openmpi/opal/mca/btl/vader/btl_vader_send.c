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
 * Copyright (c) 2010-2014 Los Alamos National Security, LLC. All rights
 *                         reserved.
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
 * Initiate a send to the peer.
 *
 * @param btl (IN)      BTL module
 * @param peer (IN)     BTL peer addressing
 */
int mca_btl_vader_send (struct mca_btl_base_module_t *btl,
                        struct mca_btl_base_endpoint_t *endpoint,
                        struct mca_btl_base_descriptor_t *descriptor,
                        mca_btl_base_tag_t tag)
{
    mca_btl_vader_frag_t *frag = (mca_btl_vader_frag_t *) descriptor;
    const size_t total_size = frag->segments[0].seg_len;

    /* in order to work around a long standing ob1 bug (see #3845) we have to always
     * make the callback. once this is fixed in ob1 we can restore the code below. */
    frag->base.des_flags |= MCA_BTL_DES_SEND_ALWAYS_CALLBACK;

    /* header (+ optional inline data) */
    frag->hdr->len = total_size;
    /* type of message, pt-2-pt, one-sided, etc */
    frag->hdr->tag = tag;

    /* post the relative address of the descriptor into the peer's fifo */
    if (opal_list_get_size (&endpoint->pending_frags) || !vader_fifo_write_ep (frag->hdr, endpoint)) {
        frag->base.des_flags |= MCA_BTL_DES_SEND_ALWAYS_CALLBACK;
        OPAL_THREAD_LOCK(&endpoint->pending_frags_lock);
        opal_list_append (&endpoint->pending_frags, (opal_list_item_t *) frag);
        if (!endpoint->waiting) {
            OPAL_THREAD_LOCK(&mca_btl_vader_component.lock);
            opal_list_append (&mca_btl_vader_component.pending_endpoints, &endpoint->super);
            OPAL_THREAD_UNLOCK(&mca_btl_vader_component.lock);
            endpoint->waiting = true;
        }
        OPAL_THREAD_UNLOCK(&endpoint->pending_frags_lock);
        return OPAL_SUCCESS;
    }

    return OPAL_SUCCESS;

#if 0
    if ((frag->hdr->flags & MCA_BTL_VADER_FLAG_SINGLE_COPY) ||
        !(frag->base.des_flags & MCA_BTL_DES_FLAGS_BTL_OWNERSHIP)) {
        frag->base.des_flags |= MCA_BTL_DES_SEND_ALWAYS_CALLBACK;

        return OPAL_SUCCESS;
    }

    /* data is gone (from the pml's perspective). frag callback/release will
       happen later */
    return 1;
#endif
}
