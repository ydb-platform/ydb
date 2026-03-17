/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2013 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/**
 * @file
 */
#ifndef MCA_BTL_SELF_SEND_FRAG_H
#define MCA_BTL_SELF_SEND_FRAG_H

#include <sys/types.h>
#include "btl_self.h"


/**
 * shared memory send fragment derived type.
 */
struct mca_btl_self_frag_t {
    mca_btl_base_descriptor_t base;
    mca_btl_base_segment_t segments[2];
    struct mca_btl_base_endpoint_t *endpoint;
    opal_free_list_t *list;
    size_t size;
    unsigned char data[];
};
typedef struct mca_btl_self_frag_t mca_btl_self_frag_t;
typedef struct mca_btl_self_frag_t mca_btl_self_frag_eager_t;
typedef struct mca_btl_self_frag_t mca_btl_self_frag_send_t;
typedef struct mca_btl_self_frag_t mca_btl_self_frag_rdma_t;

OBJ_CLASS_DECLARATION(mca_btl_self_frag_eager_t);
OBJ_CLASS_DECLARATION(mca_btl_self_frag_send_t);
OBJ_CLASS_DECLARATION(mca_btl_self_frag_rdma_t);

#define MCA_BTL_SELF_FRAG_ALLOC_EAGER(frag)                             \
    {                                                                   \
        frag = (mca_btl_self_frag_t *)                                  \
            opal_free_list_get (&mca_btl_self_component.self_frags_eager); \
    }


#define MCA_BTL_SELF_FRAG_ALLOC_RDMA(frag)                              \
    {                                                                   \
        frag = (mca_btl_self_frag_t *)                                  \
            opal_free_list_get (&mca_btl_self_component.self_frags_rdma); \
    }

#define MCA_BTL_SELF_FRAG_ALLOC_SEND(frag)                              \
    {                                                                   \
        frag = (mca_btl_self_frag_t *)                                  \
            opal_free_list_get (&mca_btl_self_component.self_frags_send); \
    }

#define MCA_BTL_SELF_FRAG_RETURN(frag)                                  \
    {                                                                   \
        opal_free_list_return ((frag)->list, (opal_free_list_item_t*)(frag)); \
    }

#endif /* MCA_BTL_SELF_SEND_FRAG_H */
