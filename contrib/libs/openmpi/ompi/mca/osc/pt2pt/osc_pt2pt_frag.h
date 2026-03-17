/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2012      Sandia National Laboratories.  All rights reserved.
 * Copyright (c) 2014-2017 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef OSC_PT2PT_FRAG_H
#define OSC_PT2PT_FRAG_H

#include "ompi/communicator/communicator.h"

#include "osc_pt2pt_header.h"
#include "osc_pt2pt_request.h"
#include "opal/align.h"

/** Communication buffer for packing messages */
struct ompi_osc_pt2pt_frag_t {
    opal_free_list_item_t super;
    /* target rank of buffer */
    int target;
    unsigned char *buffer;

    /* space remaining in buffer */
    size_t remain_len;

    /* start of unused space */
    char *top;

    /* Number of operations which have started writing into the frag, but not yet completed doing so */
    volatile int32_t pending;
    int32_t pending_long_sends;
    ompi_osc_pt2pt_frag_header_t *header;
    ompi_osc_pt2pt_module_t *module;
};
typedef struct ompi_osc_pt2pt_frag_t ompi_osc_pt2pt_frag_t;
OBJ_CLASS_DECLARATION(ompi_osc_pt2pt_frag_t);

int ompi_osc_pt2pt_frag_start(ompi_osc_pt2pt_module_t *module, ompi_osc_pt2pt_frag_t *buffer);
int ompi_osc_pt2pt_frag_flush_target(ompi_osc_pt2pt_module_t *module, int target);
int ompi_osc_pt2pt_frag_flush_all(ompi_osc_pt2pt_module_t *module);
int ompi_osc_pt2pt_frag_flush_pending (ompi_osc_pt2pt_module_t *module, int target);
int ompi_osc_pt2pt_frag_flush_pending_all (ompi_osc_pt2pt_module_t *module);

static inline int ompi_osc_pt2pt_frag_finish (ompi_osc_pt2pt_module_t *module,
                                              ompi_osc_pt2pt_frag_t* buffer)
{
    opal_atomic_wmb ();
    if (0 == OPAL_THREAD_ADD_FETCH32(&buffer->pending, -1)) {
        opal_atomic_mb ();
        return ompi_osc_pt2pt_frag_start(module, buffer);
    }

    return OMPI_SUCCESS;
}

static inline ompi_osc_pt2pt_frag_t *ompi_osc_pt2pt_frag_alloc_non_buffered (ompi_osc_pt2pt_module_t *module,
                                                                             ompi_osc_pt2pt_peer_t *peer,
                                                                             size_t request_len)
{
    ompi_osc_pt2pt_frag_t *curr;

    /* to ensure ordering flush the buffer on the peer */
    curr = peer->active_frag;
    if (NULL != curr && opal_atomic_compare_exchange_strong_ptr (&peer->active_frag, &curr, NULL)) {
        /* If there's something pending, the pending finish will
           start the buffer.  Otherwise, we need to start it now. */
        int ret = ompi_osc_pt2pt_frag_finish (module, curr);
        if (OPAL_UNLIKELY(OMPI_SUCCESS != ret)) {
            return NULL;
        }
    }

    curr = (ompi_osc_pt2pt_frag_t *) opal_free_list_get (&mca_osc_pt2pt_component.frags);
    if (OPAL_UNLIKELY(NULL == curr)) {
        return NULL;
    }

    curr->target = peer->rank;

    curr->header = (ompi_osc_pt2pt_frag_header_t*) curr->buffer;
    curr->top = (char*) (curr->header + 1);
    curr->remain_len = mca_osc_pt2pt_component.buffer_size;
    curr->module = module;
    curr->pending = 1;

    curr->header->base.type = OMPI_OSC_PT2PT_HDR_TYPE_FRAG;
    curr->header->base.flags = OMPI_OSC_PT2PT_HDR_FLAG_VALID;
    if (module->passive_target_access_epoch) {
        curr->header->base.flags |= OMPI_OSC_PT2PT_HDR_FLAG_PASSIVE_TARGET;
    }
    curr->header->source = ompi_comm_rank(module->comm);
    curr->header->num_ops = 1;

    return curr;
}

/*
 * Note: this function takes the module lock
 *
 * buffered sends will cache the fragment on the peer object associated with the
 * target. unbuffered-sends will cause the target fragment to be flushed and
 * will not be cached on the peer. this causes the fragment to be flushed as
 * soon as it is sent. this allows request-based rma fragments to be completed
 * so MPI_Test/MPI_Wait/etc will work as expected.
 */
static inline int _ompi_osc_pt2pt_frag_alloc (ompi_osc_pt2pt_module_t *module, int target,
                                             size_t request_len, ompi_osc_pt2pt_frag_t **buffer,
                                             char **ptr, bool long_send, bool buffered)
{
    ompi_osc_pt2pt_peer_t *peer = ompi_osc_pt2pt_peer_lookup (module, target);
    ompi_osc_pt2pt_frag_t *curr;

    /* osc pt2pt headers can have 64-bit values. these will need to be aligned
     * on an 8-byte boundary on some architectures so we up align the allocation
     * size here. */
    request_len = OPAL_ALIGN(request_len, 8, size_t);

    if (request_len > mca_osc_pt2pt_component.buffer_size) {
        return OMPI_ERR_OUT_OF_RESOURCE;
    }

    OPAL_OUTPUT_VERBOSE((MCA_BASE_VERBOSE_TRACE, ompi_osc_base_framework.framework_output,
                         "attempting to allocate buffer for %lu bytes to target %d. long send: %d, "
                         "buffered: %d", (unsigned long) request_len, target, long_send, buffered));

    OPAL_THREAD_LOCK(&module->lock);
    if (buffered) {
        curr = peer->active_frag;
        if (NULL == curr || curr->remain_len < request_len || (long_send && curr->pending_long_sends == 32)) {
            curr = ompi_osc_pt2pt_frag_alloc_non_buffered (module, peer, request_len);
            if (OPAL_UNLIKELY(NULL == curr)) {
                OPAL_THREAD_UNLOCK(&module->lock);
                return OMPI_ERR_OUT_OF_RESOURCE;
            }

            curr->pending_long_sends = long_send;
            peer->active_frag = curr;
        } else {
            OPAL_THREAD_ADD_FETCH32(&curr->header->num_ops, 1);
            curr->pending_long_sends += long_send;
        }

        OPAL_THREAD_ADD_FETCH32(&curr->pending, 1);
    } else {
        curr = ompi_osc_pt2pt_frag_alloc_non_buffered (module, peer, request_len);
        if (OPAL_UNLIKELY(NULL == curr)) {
            OPAL_THREAD_UNLOCK(&module->lock);
            return OMPI_ERR_OUT_OF_RESOURCE;
        }
    }

    *ptr = curr->top;
    *buffer = curr;

    curr->top += request_len;
    curr->remain_len -= request_len;

    OPAL_THREAD_UNLOCK(&module->lock);

    return OMPI_SUCCESS;
}

static inline int ompi_osc_pt2pt_frag_alloc (ompi_osc_pt2pt_module_t *module, int target,
                                             size_t request_len, ompi_osc_pt2pt_frag_t **buffer,
                                             char **ptr, bool long_send, bool buffered)
{
    int ret;

    if (request_len > mca_osc_pt2pt_component.buffer_size) {
        return OMPI_ERR_OUT_OF_RESOURCE;
    }

    do {
        ret = _ompi_osc_pt2pt_frag_alloc (module, target, request_len , buffer, ptr, long_send, buffered);
        if (OPAL_LIKELY(OMPI_SUCCESS == ret || OMPI_ERR_OUT_OF_RESOURCE != ret)) {
            break;
        }

        ompi_osc_pt2pt_frag_flush_pending_all (module);
        opal_progress ();
    } while (1);

    return ret;
}

#endif
