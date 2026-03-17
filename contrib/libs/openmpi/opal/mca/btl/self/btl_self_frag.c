/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2006 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2016      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"
#include "btl_self_frag.h"

static inline void mca_btl_self_frag_constructor(mca_btl_self_frag_t* frag)
{
    frag->base.des_flags = 0;
    frag->segments[0].seg_addr.pval = (void *) frag->data;
    frag->segments[0].seg_len = (uint32_t) frag->size;
    frag->base.des_segments = frag->segments;
    frag->base.des_segment_count = 1;
}

static void mca_btl_self_frag_eager_constructor(mca_btl_self_frag_t* frag)
{
    frag->list = &mca_btl_self_component.self_frags_eager;
    frag->size = mca_btl_self.btl_eager_limit;
    mca_btl_self_frag_constructor(frag);
}

static void mca_btl_self_frag_send_constructor(mca_btl_self_frag_t* frag)
{
    frag->list = &mca_btl_self_component.self_frags_send;
    frag->size = mca_btl_self.btl_max_send_size;
    mca_btl_self_frag_constructor(frag);
}

static void mca_btl_self_frag_rdma_constructor(mca_btl_self_frag_t* frag)
{
    frag->list = &mca_btl_self_component.self_frags_rdma;
    frag->size = MCA_BTL_SELF_MAX_INLINE_SIZE;
    mca_btl_self_frag_constructor(frag);
}

OBJ_CLASS_INSTANCE( mca_btl_self_frag_eager_t,
                    mca_btl_base_descriptor_t,
                    mca_btl_self_frag_eager_constructor,
                    NULL );

OBJ_CLASS_INSTANCE( mca_btl_self_frag_send_t,
                    mca_btl_base_descriptor_t,
                    mca_btl_self_frag_send_constructor,
                    NULL );

OBJ_CLASS_INSTANCE( mca_btl_self_frag_rdma_t,
                    mca_btl_base_descriptor_t,
                    mca_btl_self_frag_rdma_constructor,
                    NULL );
