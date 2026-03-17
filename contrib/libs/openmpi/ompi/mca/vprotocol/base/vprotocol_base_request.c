/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2007 The Trustees of the University of Tennessee.
 *                         All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"
#include "vprotocol_base_request.h"
#include "ompi/mca/pml/v/pml_v_output.h"

int mca_vprotocol_base_request_parasite(void)
{
    int ret;

    if(mca_vprotocol.req_recv_class)
    {
        opal_free_list_t pml_fl_save = mca_pml_base_recv_requests;
        mca_pml_v.host_pml_req_recv_size =
            pml_fl_save.fl_frag_class->cls_sizeof;
        V_OUTPUT_VERBOSE(300, "req_rebuild: recv\tsize %lu+%lu\talignment=%lu", (unsigned long) mca_pml_v.host_pml_req_recv_size, (unsigned long) mca_vprotocol.req_recv_class->cls_sizeof, (unsigned long) pml_fl_save.fl_frag_alignment);
        mca_vprotocol.req_recv_class->cls_parent =
            pml_fl_save.fl_frag_class;
        mca_vprotocol.req_recv_class->cls_sizeof +=
            pml_fl_save.fl_frag_class->cls_sizeof;
        /* rebuild the requests free list with the right size */
        OBJ_DESTRUCT(&mca_pml_base_recv_requests);
        OBJ_CONSTRUCT(&mca_pml_base_recv_requests, opal_free_list_t);
        ret = opal_free_list_init (&mca_pml_base_recv_requests,
                                   mca_vprotocol.req_recv_class->cls_sizeof,
                                   pml_fl_save.fl_frag_alignment,
                                   mca_vprotocol.req_recv_class,
                                   pml_fl_save.fl_payload_buffer_size,
                                   pml_fl_save.fl_payload_buffer_alignment,
                                   pml_fl_save.fl_num_allocated,
                                   pml_fl_save.fl_max_to_alloc,
                                   pml_fl_save.fl_num_per_alloc,
                                   pml_fl_save.fl_mpool,
                                   pml_fl_save.fl_rcache_reg_flags,
                                   pml_fl_save.fl_rcache,
                                   pml_fl_save.item_init,
                                   pml_fl_save.ctx);
        if(OMPI_SUCCESS != ret) return ret;
    }
    if(mca_vprotocol.req_send_class)
    {
        opal_free_list_t pml_fl_save = mca_pml_base_send_requests;
        mca_pml_v.host_pml_req_send_size =
            pml_fl_save.fl_frag_class->cls_sizeof;
        V_OUTPUT_VERBOSE(300, "req_rebuild: send\tsize %lu+%lu\talignment=%lu", (unsigned long) mca_pml_v.host_pml_req_send_size, (unsigned long) mca_vprotocol.req_send_class->cls_sizeof, (unsigned long) pml_fl_save.fl_frag_alignment);
        mca_vprotocol.req_send_class->cls_parent =
            pml_fl_save.fl_frag_class;
        mca_vprotocol.req_send_class->cls_sizeof +=
            pml_fl_save.fl_frag_class->cls_sizeof;
        /* rebuild the requests free list with the right size */
        OBJ_DESTRUCT(&mca_pml_base_send_requests);
        OBJ_CONSTRUCT(&mca_pml_base_send_requests, opal_free_list_t);
        ret = opal_free_list_init (&mca_pml_base_send_requests,
                                   mca_vprotocol.req_send_class->cls_sizeof,
                                   pml_fl_save.fl_frag_alignment,
                                   mca_vprotocol.req_send_class,
                                   pml_fl_save.fl_payload_buffer_size,
                                   pml_fl_save.fl_payload_buffer_alignment,
                                   pml_fl_save.fl_num_allocated,
                                   pml_fl_save.fl_max_to_alloc,
                                   pml_fl_save.fl_num_per_alloc,
                                   pml_fl_save.fl_mpool,
                                   pml_fl_save.fl_rcache_reg_flags,
                                   pml_fl_save.fl_rcache,
                                   pml_fl_save.item_init,
                                   pml_fl_save.ctx);
        if(OMPI_SUCCESS != ret) return ret;
    }
    return OMPI_SUCCESS;

}
