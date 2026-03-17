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
 * Copyright (c) 2011-2015 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2018      Triad National Security, LLC. All rights
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

static inline void mca_btl_vader_frag_constructor (mca_btl_vader_frag_t *frag)
{
    frag->hdr = (mca_btl_vader_hdr_t*)frag->base.super.ptr;
    if(frag->hdr != NULL) {
        frag->hdr->frag = frag;
        frag->hdr->flags = 0;
        frag->segments[0].seg_addr.pval = (char *)(frag->hdr + 1);
    }

    frag->base.des_segments      = frag->segments;
    frag->base.des_segment_count = 1;
}

int mca_btl_vader_frag_init (opal_free_list_item_t *item, void *ctx)
{
    mca_btl_vader_frag_t *frag = (mca_btl_vader_frag_t *) item;

    /* Set the list element here so we don't have to set it on the critical path */
    frag->my_list = (opal_free_list_t *) ctx;

    return OPAL_SUCCESS;
}

OBJ_CLASS_INSTANCE(mca_btl_vader_frag_t, mca_btl_base_descriptor_t,
                   mca_btl_vader_frag_constructor, NULL);
