/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2016 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2014-2015 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef MCA_BTL_TCP_FRAG_H
#define MCA_BTL_TCP_FRAG_H


#include "opal_config.h"

#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
#ifdef HAVE_SYS_UIO_H
#include <sys/uio.h>
#endif
#ifdef HAVE_NET_UIO_H
#include <net/uio.h>
#endif

#include "btl_tcp.h"
#include "btl_tcp_hdr.h"

BEGIN_C_DECLS

#define MCA_BTL_TCP_FRAG_IOVEC_NUMBER  4

/**
 * TCP fragment derived type.
 */
struct mca_btl_tcp_frag_t {
    mca_btl_base_descriptor_t base;
    mca_btl_base_segment_t segments[2];
    struct mca_btl_base_endpoint_t *endpoint;
    struct mca_btl_tcp_module_t* btl;
    mca_btl_tcp_hdr_t hdr;
    struct iovec iov[MCA_BTL_TCP_FRAG_IOVEC_NUMBER + 1];
    struct iovec *iov_ptr;
    uint32_t iov_cnt;
    uint32_t iov_idx;
    size_t size;
    uint16_t next_step;
    int rc;
    opal_free_list_t* my_list;
    /* fake rdma completion */
    struct {
        mca_btl_base_rdma_completion_fn_t func;
        void *data;
        void *context;
    } cb;
};
typedef struct mca_btl_tcp_frag_t mca_btl_tcp_frag_t;
OBJ_CLASS_DECLARATION(mca_btl_tcp_frag_t);

typedef struct mca_btl_tcp_frag_t mca_btl_tcp_frag_eager_t;

OBJ_CLASS_DECLARATION(mca_btl_tcp_frag_eager_t);

typedef struct mca_btl_tcp_frag_t mca_btl_tcp_frag_max_t;

OBJ_CLASS_DECLARATION(mca_btl_tcp_frag_max_t);

typedef struct mca_btl_tcp_frag_t mca_btl_tcp_frag_user_t;

OBJ_CLASS_DECLARATION(mca_btl_tcp_frag_user_t);


/*
 * Macros to allocate/return descriptors from module specific
 * free list(s).
 */

#define MCA_BTL_TCP_FRAG_ALLOC_EAGER(frag)                              \
{                                                                       \
    frag = (mca_btl_tcp_frag_t*)                                        \
        opal_free_list_get (&mca_btl_tcp_component.tcp_frag_eager);     \
}

#define MCA_BTL_TCP_FRAG_ALLOC_MAX(frag)                                \
{                                                                       \
    frag = (mca_btl_tcp_frag_t*)                                        \
        opal_free_list_get (&mca_btl_tcp_component.tcp_frag_max);       \
}

#define MCA_BTL_TCP_FRAG_ALLOC_USER(frag)                               \
{                                                                       \
    frag = (mca_btl_tcp_frag_t*)                                        \
        opal_free_list_get (&mca_btl_tcp_component.tcp_frag_user);      \
}

#define MCA_BTL_TCP_FRAG_RETURN(frag)                                      \
{                                                                          \
    opal_free_list_return (frag->my_list, (opal_free_list_item_t*)(frag)); \
}

#define MCA_BTL_TCP_FRAG_INIT_DST(frag,ep)                                 \
do {                                                                       \
    frag->rc = 0;                                                          \
    frag->btl = ep->endpoint_btl;                                          \
    frag->endpoint = ep;                                                   \
    frag->iov[0].iov_len = sizeof(frag->hdr);                              \
    frag->iov[0].iov_base = (IOVBASE_TYPE*)&frag->hdr;                     \
    frag->iov_cnt = 1;                                                     \
    frag->iov_idx = 0;                                                     \
    frag->iov_ptr = frag->iov;                                             \
    frag->base.des_segments = frag->segments;                                 \
    frag->base.des_segment_count = 1;                                        \
} while(0)


bool mca_btl_tcp_frag_send(mca_btl_tcp_frag_t*, int sd);
bool mca_btl_tcp_frag_recv(mca_btl_tcp_frag_t*, int sd);
size_t mca_btl_tcp_frag_dump(mca_btl_tcp_frag_t* frag, char* msg, char* buf, size_t length);
END_C_DECLS
#endif
