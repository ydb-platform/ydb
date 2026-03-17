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
 * Copyright (c) 2014-2015 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/**
 *  @file
 */

#ifndef MCA_PML_OB1_RDMAFRAG_H
#define MCA_PML_OB1_RDMAFRAG_H

#include "pml_ob1_hdr.h"

BEGIN_C_DECLS

typedef enum {
    MCA_PML_OB1_RDMA_PUT,
    MCA_PML_OB1_RDMA_GET
} mca_pml_ob1_rdma_state_t;

struct mca_pml_ob1_rdma_frag_t;

typedef void (*mca_pml_ob1_rdma_frag_callback_t)(struct mca_pml_ob1_rdma_frag_t *frag, int64_t rdma_length);

/**
 * Used to keep track of local and remote RDMA operations.
 */
struct mca_pml_ob1_rdma_frag_t {
    opal_free_list_item_t super;
    mca_bml_base_btl_t *rdma_bml;
    mca_pml_ob1_hdr_t rdma_hdr;
    mca_pml_ob1_rdma_state_t rdma_state;
    size_t rdma_length;
    void *rdma_req;
    uint32_t retries;
    mca_pml_ob1_rdma_frag_callback_t cbfunc;

    uint64_t rdma_offset;
    void *local_address;
    mca_btl_base_registration_handle_t *local_handle;

    uint64_t remote_address;
    uint8_t remote_handle[MCA_BTL_REG_HANDLE_MAX_SIZE];
};
typedef struct mca_pml_ob1_rdma_frag_t mca_pml_ob1_rdma_frag_t;

OBJ_CLASS_DECLARATION(mca_pml_ob1_rdma_frag_t);


#define MCA_PML_OB1_RDMA_FRAG_ALLOC(frag)                          \
    do {                                                           \
        frag = (mca_pml_ob1_rdma_frag_t *)                         \
            opal_free_list_wait (&mca_pml_ob1.rdma_frags);         \
    } while(0)

#define MCA_PML_OB1_RDMA_FRAG_RETURN(frag)                              \
    do {                                                                \
        /* return fragment */                                           \
        if (frag->local_handle) {                                       \
            mca_bml_base_deregister_mem (frag->rdma_bml, frag->local_handle); \
            frag->local_handle = NULL;                                  \
        }                                                               \
        opal_free_list_return (&mca_pml_ob1.rdma_frags,                 \
                               (opal_free_list_item_t*)frag);           \
    } while (0)

END_C_DECLS

#endif

