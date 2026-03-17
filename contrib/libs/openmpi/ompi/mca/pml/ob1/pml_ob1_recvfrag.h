/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2018 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2008      UT-Battelle, LLC. All rights reserved.
 * Copyright (c) 2011      Sandia National Laboratories. All rights reserved.
 * Copyright (c) 2012-2015 Los Alamos National Security, LLC. All rights
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

#ifndef MCA_PML_OB1_RECVFRAG_H
#define MCA_PML_OB1_RECVFRAG_H

#include "pml_ob1_hdr.h"

BEGIN_C_DECLS

struct mca_pml_ob1_buffer_t {
    size_t len;
    void * addr;
};
typedef struct mca_pml_ob1_buffer_t mca_pml_ob1_buffer_t;


struct mca_pml_ob1_recv_frag_t {
    opal_free_list_item_t super;
    mca_pml_ob1_hdr_t hdr;
    size_t num_segments;
    struct mca_pml_ob1_recv_frag_t* range;
    mca_btl_base_module_t* btl;
    mca_btl_base_segment_t segments[MCA_BTL_DES_MAX_SEGMENTS];
    mca_pml_ob1_buffer_t buffers[MCA_BTL_DES_MAX_SEGMENTS];
    unsigned char addr[1];
};
typedef struct mca_pml_ob1_recv_frag_t mca_pml_ob1_recv_frag_t;

OBJ_CLASS_DECLARATION(mca_pml_ob1_recv_frag_t);


#define MCA_PML_OB1_RECV_FRAG_ALLOC(frag)                       \
do {                                                            \
    frag = (mca_pml_ob1_recv_frag_t *)                          \
        opal_free_list_wait (&mca_pml_ob1.recv_frags);          \
} while(0)


#define MCA_PML_OB1_RECV_FRAG_INIT(frag, hdr, segs, cnt, btl )          \
do {                                                                    \
    size_t i, _size;                                                    \
    mca_btl_base_segment_t* macro_segments = frag->segments;            \
    mca_pml_ob1_buffer_t* buffers = frag->buffers;                      \
    unsigned char* _ptr = (unsigned char*)frag->addr;                   \
    /* init recv_frag */                                                \
    frag->btl = btl;                                                    \
    ob1_hdr_copy( (mca_pml_ob1_hdr_t*)hdr, &frag->hdr );                \
    frag->num_segments = 1;                                             \
    _size = segs[0].seg_len;                                            \
    for( i = 1; i < cnt; i++ ) {                                        \
        _size += segs[i].seg_len;                                       \
    }                                                                   \
    /* copy over data */                                                \
    if(_size <= mca_pml_ob1.unexpected_limit ) {                        \
        macro_segments[0].seg_addr.pval = frag->addr;                   \
    } else {                                                            \
        buffers[0].len = _size;                                         \
        buffers[0].addr = (char*)                                       \
            mca_pml_ob1.allocator->alc_alloc( mca_pml_ob1.allocator,    \
                                              buffers[0].len,           \
                                              0);                       \
        _ptr = (unsigned char*)(buffers[0].addr);                       \
        macro_segments[0].seg_addr.pval = buffers[0].addr;              \
    }                                                                   \
    macro_segments[0].seg_len = _size;                                  \
    for( i = 0; i < cnt; i++ ) {                                        \
        memcpy( _ptr, segs[i].seg_addr.pval, segs[i].seg_len);          \
        _ptr += segs[i].seg_len;                                        \
    }                                                                   \
 } while(0)


#define MCA_PML_OB1_RECV_FRAG_RETURN(frag)                              \
do {                                                                    \
    if( frag->segments[0].seg_len > mca_pml_ob1.unexpected_limit ) {    \
        /* return buffers */                                            \
        mca_pml_ob1.allocator->alc_free( mca_pml_ob1.allocator,         \
                                         frag->buffers[0].addr );       \
    }                                                                   \
    frag->num_segments = 0;                                             \
                                                                        \
    /* return recv_frag */                                              \
    opal_free_list_return (&mca_pml_ob1.recv_frags,                     \
                           (opal_free_list_item_t*)frag);               \
 } while(0)


/**
 *  Callback from BTL on receipt of a recv_frag (match).
 */

extern void mca_pml_ob1_recv_frag_callback_match( mca_btl_base_module_t *btl,
                                                  mca_btl_base_tag_t tag,
                                                  mca_btl_base_descriptor_t* descriptor,
                                                  void* cbdata );

/**
 *  Callback from BTL on receipt of a recv_frag (rndv).
 */

extern void mca_pml_ob1_recv_frag_callback_rndv( mca_btl_base_module_t *btl,
                                                 mca_btl_base_tag_t tag,
                                                 mca_btl_base_descriptor_t* descriptor,
                                                 void* cbdata );
/**
 *  Callback from BTL on receipt of a recv_frag (rget).
 */

extern void mca_pml_ob1_recv_frag_callback_rget( mca_btl_base_module_t *btl,
                                                 mca_btl_base_tag_t tag,
                                                 mca_btl_base_descriptor_t* descriptor,
                                                 void* cbdata );

/**
 *  Callback from BTL on receipt of a recv_frag (ack).
 */

extern void mca_pml_ob1_recv_frag_callback_ack( mca_btl_base_module_t *btl,
                                                mca_btl_base_tag_t tag,
                                                mca_btl_base_descriptor_t* descriptor,
                                                void* cbdata );
/**
 *  Callback from BTL on receipt of a recv_frag (frag).
 */

extern void mca_pml_ob1_recv_frag_callback_frag( mca_btl_base_module_t *btl,
                                                 mca_btl_base_tag_t tag,
                                                 mca_btl_base_descriptor_t* descriptor,
                                                 void* cbdata );
/**
 *  Callback from BTL on receipt of a recv_frag (put).
 */

extern void mca_pml_ob1_recv_frag_callback_put( mca_btl_base_module_t *btl,
                                                mca_btl_base_tag_t tag,
                                                mca_btl_base_descriptor_t* descriptor,
                                                void* cbdata );
/**
 *  Callback from BTL on receipt of a recv_frag (fin).
 */

extern void mca_pml_ob1_recv_frag_callback_fin( mca_btl_base_module_t *btl,
                                                mca_btl_base_tag_t tag,
                                                mca_btl_base_descriptor_t* descriptor,
                                                void* cbdata );

/**
 * Extract the next fragment from the cant_match ordered list. This fragment
 * will be the next in sequence.
 */
extern mca_pml_ob1_recv_frag_t*
check_cantmatch_for_match(mca_pml_ob1_comm_proc_t *proc);

void append_frag_to_ordered_list(mca_pml_ob1_recv_frag_t** queue,
                                 mca_pml_ob1_recv_frag_t* frag,
                                 uint16_t seq);

extern void mca_pml_ob1_dump_cant_match(mca_pml_ob1_recv_frag_t* queue);
END_C_DECLS

#endif

