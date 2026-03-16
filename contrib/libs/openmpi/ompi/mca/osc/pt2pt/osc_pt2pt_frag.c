/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2012-2013 Sandia National Laboratories.  All rights reserved.
 * Copyright (c) 2014-2017 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2017-2018 Cisco Systems, Inc.  All rights reserved
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "osc_pt2pt.h"
#include "osc_pt2pt_frag.h"
#include "osc_pt2pt_data_move.h"

static void ompi_osc_pt2pt_frag_constructor (ompi_osc_pt2pt_frag_t *frag)
{
    frag->buffer = frag->super.ptr;
}

OBJ_CLASS_INSTANCE(ompi_osc_pt2pt_frag_t, opal_free_list_item_t,
                   ompi_osc_pt2pt_frag_constructor, NULL);

static int frag_send_cb (ompi_request_t *request)
{
    ompi_osc_pt2pt_frag_t *frag =
        (ompi_osc_pt2pt_frag_t*) request->req_complete_cb_data;
    ompi_osc_pt2pt_module_t *module = frag->module;

    OPAL_OUTPUT_VERBOSE((50, ompi_osc_base_framework.framework_output,
                         "osc pt2pt: frag_send complete to %d, frag = %p, request = %p",
                         frag->target, (void *) frag, (void *) request));

    mark_outgoing_completion(module);
    opal_free_list_return (&mca_osc_pt2pt_component.frags, &frag->super);

    ompi_request_free (&request);

    return 1;
}

static int frag_send (ompi_osc_pt2pt_module_t *module, ompi_osc_pt2pt_frag_t *frag)
{
    int count;

    count = (int)((uintptr_t) frag->top - (uintptr_t) frag->buffer);

    OPAL_OUTPUT_VERBOSE((50, ompi_osc_base_framework.framework_output,
                         "osc pt2pt: frag_send called to %d, frag = %p, count = %d",
                         frag->target, (void *) frag, count));

    OSC_PT2PT_HTON(frag->header, module, frag->target);
    return ompi_osc_pt2pt_isend_w_cb (frag->buffer, count, MPI_BYTE, frag->target, OSC_PT2PT_FRAG_TAG,
                                     module->comm, frag_send_cb, frag);
}


int ompi_osc_pt2pt_frag_start (ompi_osc_pt2pt_module_t *module,
                               ompi_osc_pt2pt_frag_t *frag)
{
    ompi_osc_pt2pt_peer_t *peer = ompi_osc_pt2pt_peer_lookup (module, frag->target);
    int ret;

    assert(0 == frag->pending && peer->active_frag != frag);

    /* we need to signal now that a frag is outgoing to ensure the count sent
     * with the unlock message is correct */
    ompi_osc_signal_outgoing (module, frag->target, 1);

    /* if eager sends are not active, can't send yet, so buffer and
       get out... */
    if (!ompi_osc_pt2pt_peer_sends_active (module, frag->target) || opal_list_get_size (&peer->queued_frags)) {
        OPAL_OUTPUT_VERBOSE((50, ompi_osc_base_framework.framework_output, "queuing fragment to peer %d",
                             frag->target));
        OPAL_THREAD_SCOPED_LOCK(&peer->lock,
                                opal_list_append(&peer->queued_frags, (opal_list_item_t *) frag));
        return OMPI_SUCCESS;
    }

    OPAL_OUTPUT_VERBOSE((50, ompi_osc_base_framework.framework_output, "sending fragment to peer %d",
                         frag->target));

    ret = frag_send(module, frag);

    opal_condition_broadcast(&module->cond);

    return ret;
}

static int ompi_osc_pt2pt_flush_active_frag (ompi_osc_pt2pt_module_t *module, ompi_osc_pt2pt_peer_t *peer)
{
    ompi_osc_pt2pt_frag_t *active_frag = peer->active_frag;
    int ret = OMPI_SUCCESS;

    if (NULL == active_frag) {
        /* nothing to do */
        return OMPI_SUCCESS;
    }

    OPAL_OUTPUT_VERBOSE((50, ompi_osc_base_framework.framework_output,
                         "osc pt2pt: flushing active fragment to target %d. pending: %d",
                         active_frag->target, active_frag->pending));

    if (opal_atomic_compare_exchange_strong_ptr (&peer->active_frag, &active_frag, NULL)) {
        if (0 != OPAL_THREAD_ADD_FETCH32(&active_frag->pending, -1)) {
            /* communication going on while synchronizing; this is an rma usage bug */
            return OMPI_ERR_RMA_SYNC;
        }

        ompi_osc_signal_outgoing (module, active_frag->target, 1);
        ret = frag_send (module, active_frag);
    }

    return ret;
}

int ompi_osc_pt2pt_frag_flush_pending (ompi_osc_pt2pt_module_t *module, int target)
{
    ompi_osc_pt2pt_peer_t *peer = ompi_osc_pt2pt_peer_lookup (module, target);
    ompi_osc_pt2pt_frag_t *frag;
    int ret = OMPI_SUCCESS;

    /* walk through the pending list and send */
    OPAL_THREAD_LOCK(&peer->lock);
    while (NULL != (frag = ((ompi_osc_pt2pt_frag_t *) opal_list_remove_first (&peer->queued_frags)))) {
        ret = frag_send(module, frag);
        if (OPAL_UNLIKELY(OMPI_SUCCESS != ret)) {
            break;
        }
    }
    OPAL_THREAD_UNLOCK(&peer->lock);

    return ret;
}

int ompi_osc_pt2pt_frag_flush_pending_all (ompi_osc_pt2pt_module_t *module)
{
    int ret = OPAL_SUCCESS;

    for (int i = 0 ; i < ompi_comm_size (module->comm) ; ++i) {
        ret = ompi_osc_pt2pt_frag_flush_pending (module, i);
        if (OPAL_UNLIKELY(OMPI_SUCCESS != ret)) {
            return ret;
        }
    }

    return ret;
}

int ompi_osc_pt2pt_frag_flush_target (ompi_osc_pt2pt_module_t *module, int target)
{
    ompi_osc_pt2pt_peer_t *peer = ompi_osc_pt2pt_peer_lookup (module, target);
    int ret = OMPI_SUCCESS;

    OPAL_OUTPUT_VERBOSE((50, ompi_osc_base_framework.framework_output,
                         "osc pt2pt: frag flush to target target %d. queue fragments: %lu",
                         target, (unsigned long) opal_list_get_size (&peer->queued_frags)));

    ret = ompi_osc_pt2pt_frag_flush_pending (module, target);
    if (OMPI_SUCCESS != ret) {
        /* XXX -- TODO -- better error handling */
        return ret;
    }


    /* flush the active frag */
    ret = ompi_osc_pt2pt_flush_active_frag (module, peer);

    OPAL_OUTPUT_VERBOSE((50, ompi_osc_base_framework.framework_output,
                         "osc pt2pt: frag flush target %d finished", target));

    return ret;
}

int ompi_osc_pt2pt_frag_flush_all (ompi_osc_pt2pt_module_t *module)
{
    int ret = OMPI_SUCCESS;

    OPAL_OUTPUT_VERBOSE((50, ompi_osc_base_framework.framework_output,
                         "osc pt2pt: frag flush all begin"));

    /* try to start frags queued to all peers */
    for (int i = 0 ; i < ompi_comm_size (module->comm) ; ++i) {
        ret = ompi_osc_pt2pt_frag_flush_target (module, i);
        if (OMPI_SUCCESS != ret) {
            break;
        }
    }

    OPAL_OUTPUT_VERBOSE((50, ompi_osc_base_framework.framework_output,
                         "osc pt2pt: frag flush all done. ret: %d", ret));

    return ret;
}
