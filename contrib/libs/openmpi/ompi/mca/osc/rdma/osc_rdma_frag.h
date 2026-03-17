/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2012      Sandia National Laboratories.  All rights reserved.
 * Copyright (c) 2014-2018 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef OSC_RDMA_FRAG_H
#define OSC_RDMA_FRAG_H

#include "osc_rdma.h"
#include "opal/align.h"

static inline void ompi_osc_rdma_frag_complete (ompi_osc_rdma_frag_t *frag)
{
    OSC_RDMA_VERBOSE(MCA_BASE_VERBOSE_INFO, "returning frag. pending = %d", frag->pending);
    if (0 == OPAL_THREAD_ADD_FETCH32(&frag->pending, -1)) {
        opal_atomic_rmb ();

        (void) opal_atomic_swap_32 (&frag->pending, 1);
#if OPAL_HAVE_ATOMIC_MATH_64
        (void) opal_atomic_swap_64 (&frag->curr_index, 0);
#else
        (void) opal_atomic_swap_32 (&frag->curr_index, 0);
#endif
    }
}

/*
 * Note: module lock must be held during this operation
 */
static inline int ompi_osc_rdma_frag_alloc (ompi_osc_rdma_module_t *module, size_t request_len,
                                            ompi_osc_rdma_frag_t **buffer, char **ptr)
{
    ompi_osc_rdma_frag_t *curr = module->rdma_frag;
    int64_t my_index;
    int ret;

    /* ensure all buffers are 8-byte aligned */
    request_len = OPAL_ALIGN(request_len, 8, size_t);

    if (request_len > (mca_osc_rdma_component.buffer_size >> 1)) {
        return OMPI_ERR_VALUE_OUT_OF_BOUNDS;
    }

    if (NULL == curr) {
        opal_free_list_item_t *item = NULL;

        item = opal_free_list_get (&mca_osc_rdma_component.frags);
        if (OPAL_UNLIKELY(NULL == item)) {
            OPAL_THREAD_UNLOCK(&module->lock);
            return OMPI_ERR_OUT_OF_RESOURCE;
        }

        curr = (ompi_osc_rdma_frag_t *) item;

        curr->handle = NULL;
        curr->pending = 1;
        curr->module = module;
        curr->curr_index = 0;

        if (module->selected_btl->btl_register_mem) {
            ret = ompi_osc_rdma_register (module, MCA_BTL_ENDPOINT_ANY, curr->super.ptr, mca_osc_rdma_component.buffer_size,
                                          MCA_BTL_REG_FLAG_ACCESS_ANY, &curr->handle);
            if (OPAL_UNLIKELY(OMPI_SUCCESS != ret)) {
                return OMPI_ERR_OUT_OF_RESOURCE;
            }
        }

        if (!opal_atomic_compare_exchange_strong_ptr (&module->rdma_frag, &(void *){NULL}, curr)) {
            ompi_osc_rdma_deregister (module, curr->handle);
            curr->handle = NULL;

            opal_free_list_return (&mca_osc_rdma_component.frags, &curr->super);

            curr = module->rdma_frag;
        }
    }

    OSC_RDMA_VERBOSE(MCA_BASE_VERBOSE_INFO, "allocating frag. pending = %d", curr->pending);
    OPAL_THREAD_ADD_FETCH32(&curr->pending, 1);

#if OPAL_HAVE_ATOMIC_MATH_64
    my_index = opal_atomic_fetch_add_64 (&curr->curr_index, request_len);
#else
    my_index = opal_atomic_fetch_add_32 (&curr->curr_index, request_len);
#endif
    if (my_index + request_len > mca_osc_rdma_component.buffer_size) {
        if (my_index <= mca_osc_rdma_component.buffer_size) {
            /* this thread caused the buffer to spill over */
            ompi_osc_rdma_frag_complete (curr);
        }
        ompi_osc_rdma_frag_complete (curr);
        return OPAL_ERR_OUT_OF_RESOURCE;
    }

    *ptr = (void *) ((intptr_t) curr->super.ptr + my_index);
    *buffer = curr;

    return OMPI_SUCCESS;
}

#endif
