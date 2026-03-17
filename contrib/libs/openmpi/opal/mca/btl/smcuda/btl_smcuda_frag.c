/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2009 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2009      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2012      NVIDIA Corporation.  All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
#include "opal_config.h"
#include "btl_smcuda_frag.h"


static inline void mca_btl_smcuda_frag_common_constructor(mca_btl_smcuda_frag_t* frag)
{
    frag->hdr = (mca_btl_smcuda_hdr_t*)frag->base.super.ptr;
    if(frag->hdr != NULL) {
        frag->hdr->frag = (mca_btl_smcuda_frag_t*)((uintptr_t)frag |
            MCA_BTL_SMCUDA_FRAG_ACK);
        frag->segment.seg_addr.pval = ((char*)frag->hdr) +
            sizeof(mca_btl_smcuda_hdr_t);
        frag->hdr->my_smp_rank = mca_btl_smcuda_component.my_smp_rank;
    }
    frag->segment.seg_len = frag->size;
    frag->base.des_segments = &frag->segment;
    frag->base.des_segment_count = 1;
    frag->base.des_flags = 0;
#if OPAL_CUDA_SUPPORT
    frag->registration = NULL;
#endif /* OPAL_CUDA_SUPPORT */
}

static void mca_btl_smcuda_frag1_constructor(mca_btl_smcuda_frag_t* frag)
{
    frag->size = mca_btl_smcuda_component.eager_limit;
    frag->my_list = &mca_btl_smcuda_component.sm_frags_eager;
    mca_btl_smcuda_frag_common_constructor(frag);
}

static void mca_btl_smcuda_frag2_constructor(mca_btl_smcuda_frag_t* frag)
{
    frag->size = mca_btl_smcuda_component.max_frag_size;
    frag->my_list = &mca_btl_smcuda_component.sm_frags_max;
    mca_btl_smcuda_frag_common_constructor(frag);
}

static void mca_btl_smcuda_user_constructor(mca_btl_smcuda_frag_t* frag)
{
	frag->size = 0;
	frag->my_list = &mca_btl_smcuda_component.sm_frags_user;
	mca_btl_smcuda_frag_common_constructor(frag);
}

OBJ_CLASS_INSTANCE(
    mca_btl_smcuda_frag1_t,
    mca_btl_base_descriptor_t,
    mca_btl_smcuda_frag1_constructor,
    NULL);

OBJ_CLASS_INSTANCE(
    mca_btl_smcuda_frag2_t,
    mca_btl_base_descriptor_t,
    mca_btl_smcuda_frag2_constructor,
    NULL);

OBJ_CLASS_INSTANCE(
    mca_btl_smcuda_user_t,
    mca_btl_base_descriptor_t,
    mca_btl_smcuda_user_constructor,
    NULL);
