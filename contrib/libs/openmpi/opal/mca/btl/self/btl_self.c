/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2013 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2012-2013 Inria.  All rights reserved.
 * Copyright (c) 2014-2016 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2016      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"

#include <string.h>
#include <stdlib.h>

#include "opal/class/opal_bitmap.h"
#include "opal/datatype/opal_convertor.h"
#include "btl_self.h"
#include "btl_self_frag.h"
#include "opal/util/proc.h"

/**
 * PML->BTL notification of change in the process list.
 * PML->BTL Notification that a receive fragment has been matched.
 * Called for message that is send from process with the virtual
 * address of the shared memory segment being different than that of
 * the receiver.
 *
 * @param btl (IN)
 * @param proc (IN)
 * @param peer (OUT)
 * @return     OPAL_SUCCESS or error status on failure.
 *
 */
static int mca_btl_self_add_procs (struct mca_btl_base_module_t *btl, size_t nprocs,
                                   struct opal_proc_t **procs,
                                   struct mca_btl_base_endpoint_t **peers,
                                   opal_bitmap_t* reachability)
{
    for (int i = 0; i < (int)nprocs; i++ ) {
        if( 0 == opal_compare_proc(procs[i]->proc_name, OPAL_PROC_MY_NAME) ) {
            opal_bitmap_set_bit( reachability, i );
            /* need to return something to keep the bml from ignoring us */
            peers[i] = (struct mca_btl_base_endpoint_t *) 1;
            break;  /* there will always be only one ... */
        }
    }

    return OPAL_SUCCESS;
}

/**
 * PML->BTL notification of change in the process list.
 *
 * @param btl (IN)     BTL instance
 * @param proc (IN)    Peer process
 * @param peer (IN)    Peer addressing information.
 * @return             Status indicating if cleanup was successful
 *
 */
static int mca_btl_self_del_procs (struct mca_btl_base_module_t *btl, size_t nprocs,
                                   struct opal_proc_t **procs,
                                   struct mca_btl_base_endpoint_t **peers)
{
    return OPAL_SUCCESS;
}


/**
 * MCA->BTL Clean up any resources held by BTL module
 * before the module is unloaded.
 *
 * @param btl (IN)   BTL module.
 *
 * Prior to unloading a BTL module, the MCA framework will call
 * the BTL finalize method of the module. Any resources held by
 * the BTL should be released and if required the memory corresponding
 * to the BTL module freed.
 *
 */

static int mca_btl_self_finalize(struct mca_btl_base_module_t* btl)
{
    return OPAL_SUCCESS;
}


/**
 * Allocate a segment.
 *
 * @param btl (IN)      BTL module
 * @param size (IN)     Request segment size.
 */
static mca_btl_base_descriptor_t *mca_btl_self_alloc (struct mca_btl_base_module_t *btl,
                                                      struct mca_btl_base_endpoint_t *endpoint,
                                                      uint8_t order, size_t size, uint32_t flags)
{
    mca_btl_self_frag_t *frag = NULL;

    if (size <= MCA_BTL_SELF_MAX_INLINE_SIZE) {
        MCA_BTL_SELF_FRAG_ALLOC_RDMA(frag);
    } else if (size <= mca_btl_self.btl_eager_limit) {
        MCA_BTL_SELF_FRAG_ALLOC_EAGER(frag);
    } else if (size <= btl->btl_max_send_size) {
        MCA_BTL_SELF_FRAG_ALLOC_SEND(frag);
    }

    if( OPAL_UNLIKELY(NULL == frag) ) {
        return NULL;
    }

    frag->segments[0].seg_len = size;
    frag->base.des_segment_count = 1;
    frag->base.des_flags       = flags;

    return &frag->base;
}

/**
 * Return a segment allocated by this BTL.
 *
 * @param btl (IN)      BTL module
 * @param segment (IN)  Allocated segment.
 */
static int mca_btl_self_free (struct mca_btl_base_module_t *btl, mca_btl_base_descriptor_t *des)
{
    MCA_BTL_SELF_FRAG_RETURN((mca_btl_self_frag_t *) des);

    return OPAL_SUCCESS;
}


/**
 * Prepare data for send
 *
 * @param btl (IN)      BTL module
 */
static struct mca_btl_base_descriptor_t *mca_btl_self_prepare_src (struct mca_btl_base_module_t* btl,
                                                                   struct mca_btl_base_endpoint_t *endpoint,
                                                                   struct opal_convertor_t *convertor,
                                                                   uint8_t order, size_t reserve,
                                                                   size_t *size, uint32_t flags)
{
    bool inline_send = !opal_convertor_need_buffers(convertor);
    size_t buffer_len = reserve + (inline_send ? 0 : *size);
    mca_btl_self_frag_t *frag;

    frag = (mca_btl_self_frag_t *) mca_btl_self_alloc (btl, endpoint, order, buffer_len, flags);
    if (OPAL_UNLIKELY(NULL == frag)) {
        return NULL;
    }

    /* non-contigous data */
    if (OPAL_UNLIKELY(!inline_send)) {
        struct iovec iov = {.iov_len = *size, .iov_base = (IOVBASE_TYPE *) ((uintptr_t) frag->data + reserve)};
        size_t max_data = *size;
        uint32_t iov_count = 1;
        int rc;

        rc = opal_convertor_pack (convertor, &iov, &iov_count, &max_data);
        if(rc < 0) {
            mca_btl_self_free (btl, &frag->base);
            return NULL;
        }

        *size = max_data;
        frag->segments[0].seg_len = reserve + max_data;
    } else {
        void *data_ptr;

        opal_convertor_get_current_pointer (convertor, &data_ptr);

        frag->segments[1].seg_addr.pval = data_ptr;
        frag->segments[1].seg_len = *size;
        frag->base.des_segment_count = 2;
    }

    return &frag->base;
}

/**
 * Initiate a send to the peer.
 *
 * @param btl (IN)      BTL module
 * @param peer (IN)     BTL peer addressing
 */

static int mca_btl_self_send (struct mca_btl_base_module_t *btl,
                              struct mca_btl_base_endpoint_t *endpoint,
                              struct mca_btl_base_descriptor_t *des,
                              mca_btl_base_tag_t tag)
{
    mca_btl_active_message_callback_t* reg;
    int btl_ownership = (des->des_flags & MCA_BTL_DES_FLAGS_BTL_OWNERSHIP);

    /* upcall */
    reg = mca_btl_base_active_message_trigger + tag;
    reg->cbfunc( btl, tag, des, reg->cbdata );

    /* send completion */
    if( des->des_flags & MCA_BTL_DES_SEND_ALWAYS_CALLBACK ) {
        des->des_cbfunc( btl, endpoint, des, OPAL_SUCCESS );
    }
    if( btl_ownership ) {
        mca_btl_self_free( btl, des );
    }
    return 1;
}

static int mca_btl_self_sendi (struct mca_btl_base_module_t *btl, struct mca_btl_base_endpoint_t *endpoint,
                               struct opal_convertor_t *convertor, void *header, size_t header_size,
                               size_t payload_size, uint8_t order, uint32_t flags, mca_btl_base_tag_t tag,
                               mca_btl_base_descriptor_t **descriptor)
{
    mca_btl_base_descriptor_t *frag;

    if (!payload_size || !opal_convertor_need_buffers(convertor)) {
        void *data_ptr = NULL;
        if (payload_size) {
            opal_convertor_get_current_pointer (convertor, &data_ptr);
        }

        mca_btl_base_segment_t segments[2] = {{.seg_addr.pval = header, .seg_len = header_size},
                                              {.seg_addr.pval = data_ptr, .seg_len = payload_size}};
        mca_btl_base_descriptor_t des = {.des_segments = segments, .des_segment_count = payload_size ? 2 : 1,
                                         .des_flags = 0};

        (void) mca_btl_self_send (btl, endpoint, &des, tag);
        return OPAL_SUCCESS;
    }

    frag = mca_btl_self_prepare_src (btl, endpoint, convertor, order, header_size, &payload_size,
                                     flags | MCA_BTL_DES_FLAGS_BTL_OWNERSHIP);
    if (NULL == frag) {
        *descriptor = NULL;
        return OPAL_ERR_OUT_OF_RESOURCE;
    }

    memcpy (frag->des_segments[0].seg_addr.pval, header, header_size);
    (void) mca_btl_self_send (btl, endpoint, frag, tag);
    return OPAL_SUCCESS;
}

static int mca_btl_self_put (mca_btl_base_module_t *btl, struct mca_btl_base_endpoint_t *endpoint, void *local_address,
                             uint64_t remote_address, mca_btl_base_registration_handle_t *local_handle,
                             mca_btl_base_registration_handle_t *remote_handle, size_t size, int flags,
                             int order, mca_btl_base_rdma_completion_fn_t cbfunc, void *cbcontext, void *cbdata)
{
    memcpy ((void *)(intptr_t) remote_address, local_address, size);

    cbfunc (btl, endpoint, local_address, NULL, cbcontext, cbdata, OPAL_SUCCESS);

    return OPAL_SUCCESS;
}

static int mca_btl_self_get (mca_btl_base_module_t *btl, struct mca_btl_base_endpoint_t *endpoint, void *local_address,
                             uint64_t remote_address, mca_btl_base_registration_handle_t *local_handle,
                             mca_btl_base_registration_handle_t *remote_handle, size_t size, int flags,
                             int order, mca_btl_base_rdma_completion_fn_t cbfunc, void *cbcontext, void *cbdata)
{
    memcpy (local_address, (void *)(intptr_t) remote_address, size);

    cbfunc (btl, endpoint, local_address, NULL, cbcontext, cbdata, OPAL_SUCCESS);

    return OPAL_SUCCESS;
}

static int mca_btl_self_ft_event(int state) {
    return OPAL_SUCCESS;
}

/* btl self module */
mca_btl_base_module_t mca_btl_self = {
    .btl_component = &mca_btl_self_component.super,
    .btl_add_procs = mca_btl_self_add_procs,
    .btl_del_procs = mca_btl_self_del_procs,
    .btl_finalize = mca_btl_self_finalize,
    .btl_alloc = mca_btl_self_alloc,
    .btl_free = mca_btl_self_free,
    .btl_prepare_src = mca_btl_self_prepare_src,
    .btl_send = mca_btl_self_send,
    .btl_sendi = mca_btl_self_sendi,
    .btl_put = mca_btl_self_put,
    .btl_get = mca_btl_self_get,
    .btl_dump = mca_btl_base_dump,
    .btl_ft_event = mca_btl_self_ft_event,
};
