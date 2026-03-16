/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2012 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2006-2007 Voltaire. All rights reserved.
 * Copyright (c) 2009-2010 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2010-2015 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2010-2012 IBM Corporation.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
#ifndef MCA_BTL_SMCUDA_FIFO_H
#define MCA_BTL_SMCUDA_FIFO_H

#include "btl_smcuda.h"
#include "btl_smcuda_endpoint.h"

static void
add_pending(struct mca_btl_base_endpoint_t *ep, void *data, bool resend)
{
    btl_smcuda_pending_send_item_t *si;
    opal_free_list_item_t *i;
    i = opal_free_list_get (&mca_btl_smcuda_component.pending_send_fl);

    /* don't handle error for now */
    assert(i != NULL);

    si = (btl_smcuda_pending_send_item_t*)i;
    si->data = data;

    OPAL_THREAD_ADD_FETCH32(&mca_btl_smcuda_component.num_pending_sends, +1);

    /* if data was on pending send list then prepend it to the list to
     * minimize reordering */
    OPAL_THREAD_LOCK(&ep->endpoint_lock);
    if (resend)
        opal_list_prepend(&ep->pending_sends, (opal_list_item_t*)si);
    else
        opal_list_append(&ep->pending_sends, (opal_list_item_t*)si);
    OPAL_THREAD_UNLOCK(&ep->endpoint_lock);
}

/*
 * FIFO_MAP(x) defines which FIFO on the receiver should be used
 * by sender rank x.  The map is some many-to-one hash.
 *
 * FIFO_MAP_NUM(n) defines how many FIFOs the receiver has for
 * n senders.
 *
 * That is,
 *
 *      for all    0 <= x < n:
 *
 *              0 <= FIFO_MAP(x) < FIFO_MAP_NUM(n)
 *
 * For example, using some power-of-two nfifos, we could have
 *
 *    FIFO_MAP(x)     = x & (nfifos-1)
 *    FIFO_MAP_NUM(n) = min(nfifos,n)
 *
 * Interesting limits include:
 *
 *    nfifos very large:  In this case, each sender has its
 *       own dedicated FIFO on each receiver and the receiver
 *       has one FIFO per sender.
 *
 *    nfifos == 1:  In this case, all senders use the same
 *       FIFO and each receiver has just one FIFO for all senders.
 */
#define FIFO_MAP(x)     ((x) & (mca_btl_smcuda_component.nfifos - 1))
#define FIFO_MAP_NUM(n) ( (mca_btl_smcuda_component.nfifos) < (n) ? (mca_btl_smcuda_component.nfifos) : (n) )


#define MCA_BTL_SMCUDA_FIFO_WRITE(endpoint_peer, my_smp_rank,               \
                              peer_smp_rank, hdr, resend, retry_pending_sends, rc)        \
do {                                                                    \
    sm_fifo_t* fifo = &(mca_btl_smcuda_component.fifo[peer_smp_rank][FIFO_MAP(my_smp_rank)]); \
                                                                        \
    if ( retry_pending_sends ) {                                        \
        if ( 0 < opal_list_get_size(&endpoint_peer->pending_sends) ) {  \
            btl_smcuda_process_pending_sends(endpoint_peer);                \
        }                                                               \
    }                                                                   \
                                                                        \
    opal_atomic_lock(&(fifo->head_lock));                               \
    /* post fragment */                                                 \
    if(sm_fifo_write(hdr, fifo) != OPAL_SUCCESS) {                      \
        add_pending(endpoint_peer, hdr, resend);                        \
        rc = OPAL_ERR_RESOURCE_BUSY;                                    \
    } else {                                                            \
        MCA_BTL_SMCUDA_SIGNAL_PEER(endpoint_peer);                          \
        rc = OPAL_SUCCESS;                                              \
    }                                                                   \
    opal_atomic_unlock(&(fifo->head_lock));                             \
} while(0)

#endif
