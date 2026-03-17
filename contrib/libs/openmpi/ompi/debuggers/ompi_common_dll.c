/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2007-2014 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2004-2013 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2008-2009 Sun Microsystems, Inc.  All rights reserved.
 * Copyright (c) 2012-2013 Inria.  All rights reserved.
 * Copyright (c) 2014-2016 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2014      Intel, Inc. All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/**********************************************************************
 * Copyright (C) 2000-2004 by Etnus, LLC.
 * Copyright (C) 1999 by Etnus, Inc.
 * Copyright (C) 1997-1998 Dolphin Interconnect Solutions Inc.
 *
 * Permission is hereby granted to use, reproduce, prepare derivative
 * works, and to redistribute to others.
 *
 *                                DISCLAIMER
 *
 * Neither Dolphin Interconnect Solutions, Etnus LLC, nor any of their
 * employees, makes any warranty express or implied, or assumes any
 * legal liability or responsibility for the accuracy, completeness,
 * or usefulness of any information, apparatus, product, or process
 * disclosed, or represents that its use would not infringe privately
 * owned rights.
 *
 * This code was written by
 * James Cownie: Dolphin Interconnect Solutions. <jcownie@dolphinics.com>
 *               Etnus LLC <jcownie@etnus.com>
 **********************************************************************/

#include "ompi_config.h"

#include "ompi_common_dll_defs.h"

#include <string.h>

/* Basic callbacks into the debugger */
const mqs_basic_callbacks *mqs_basic_entrypoints = NULL;

#if defined(WORDS_BIGENDIAN)
static int host_is_big_endian = 1;
#else
static int host_is_big_endian = 0;
#endif

/*
 * For sanity checking to try to help keep the code in this DLL in
 * sync with the real structs out in the main OMPI code base.  If
 * we're not compiling this file inside ompi_debugger_sanity.c, then
 * ompi_field_offset() won't be defined.  So we define it here to be a
 * call to the real function mqs_field_offset.
 */
#ifndef ompi_field_offset
#define ompi_field_offset(out_name, qh_type, struct_name, field_name)   \
    {                                                                   \
        out_name = mqs_field_offset((qh_type), #field_name);            \
        if (out_name < 0) {                                             \
            fprintf(stderr, "WARNING: Open MPI is unable to find "      \
                    "field " #field_name " in the " #struct_name        \
                    " type.  This can happen can if Open MPI is built " \
                    "without debugging information, or is stripped "    \
                    "after building.\n");                               \
        }                                                               \
    }
#endif

/*
 * Open MPI use a bunch of lists in order to keep track of the
 * internal objects. We have to make sure we're able to find all of
 * them in the image and compute their ofset in order to be able to
 * parse them later.  We need to find the opal_list_item_t, the
 * opal_list_t, the opal_free_list_item_t, and the opal_free_list_t.
 *
 * Once we have these offsets, we should make sure that we have access
 * to all requests lists and types. We're looking here only at the
 * basic type for the requests as they hold all the information we
 * need to export to the debugger.
 */
int ompi_fill_in_type_info(mqs_image *image, char **message)
{
    char* missing_in_action;
    mpi_image_info * i_info = (mpi_image_info *)mqs_get_image_info (image);

    {
        mqs_type* qh_type = mqs_find_type( image, "opal_list_item_t", mqs_lang_c );
        if( !qh_type ) {
            missing_in_action = "opal_list_item_t";
            goto type_missing;
        }
        i_info->opal_list_item_t.type = qh_type;
        i_info->opal_list_item_t.size = mqs_sizeof(qh_type);
        ompi_field_offset(i_info->opal_list_item_t.offset.opal_list_next,
                          qh_type, opal_list_item_t, opal_list_next);
    }
    {
        mqs_type* qh_type = mqs_find_type( image, "opal_list_t", mqs_lang_c );
        if( !qh_type ) {
            missing_in_action = "opal_list_t";
            goto type_missing;
        }
        i_info->opal_list_t.type = qh_type;
        i_info->opal_list_t.size = mqs_sizeof(qh_type);
        ompi_field_offset(i_info->opal_list_t.offset.opal_list_sentinel,
                          qh_type, opal_list_t, opal_list_sentinel);
    }
    {
        mqs_type* qh_type = mqs_find_type( image, "opal_free_list_item_t", mqs_lang_c );
        if( !qh_type ) {
            missing_in_action = "opal_free_list_item_t";
            goto type_missing;
        }
        /* This is just an overloaded opal_list_item_t */
        i_info->opal_free_list_item_t.type = qh_type;
        i_info->opal_free_list_item_t.size = mqs_sizeof(qh_type);
    }
    {
        mqs_type* qh_type = mqs_find_type( image, "opal_free_list_t", mqs_lang_c );

        if( !qh_type ) {
            missing_in_action = "opal_free_list_t";
            goto type_missing;
        }

        i_info->opal_free_list_t.type = qh_type;
        i_info->opal_free_list_t.size = mqs_sizeof(qh_type);
        ompi_field_offset(i_info->opal_free_list_t.offset.fl_mpool,
                          qh_type, opal_free_list_t, fl_mpool);
        ompi_field_offset(i_info->opal_free_list_t.offset.fl_allocations,
                          qh_type, opal_free_list_t, fl_allocations);
        ompi_field_offset(i_info->opal_free_list_t.offset.fl_frag_class,
                          qh_type, opal_free_list_t, fl_frag_class);
        ompi_field_offset(i_info->opal_free_list_t.offset.fl_frag_size,
                          qh_type, opal_free_list_t, fl_frag_size);
        ompi_field_offset(i_info->opal_free_list_t.offset.fl_frag_alignment,
                          qh_type, opal_free_list_t, fl_frag_alignment);
        ompi_field_offset(i_info->opal_free_list_t.offset.fl_max_to_alloc,
                          qh_type, opal_free_list_t, fl_max_to_alloc);
        ompi_field_offset(i_info->opal_free_list_t.offset.fl_num_per_alloc,
                          qh_type, opal_free_list_t, fl_num_per_alloc);
        ompi_field_offset(i_info->opal_free_list_t.offset.fl_num_allocated,
                          qh_type, opal_free_list_t, fl_num_allocated);
    }
    {
        mqs_type* qh_type = mqs_find_type( image, "opal_hash_table_t", mqs_lang_c );
        if( !qh_type ) {
            missing_in_action = "opal_hash_table_t";
            goto type_missing;
        }
        i_info->opal_hash_table_t.type = qh_type;
        i_info->opal_hash_table_t.size = mqs_sizeof(qh_type);
        ompi_field_offset(i_info->opal_hash_table_t.offset.ht_table,
                          qh_type, opal_hash_table_t, ht_table);
        ompi_field_offset(i_info->opal_hash_table_t.offset.ht_size,
                          qh_type, opal_hash_table_t, ht_size);
    }
    /*
     * Now let's look for all types required for reading the requests.
     */
    {
        mqs_type* qh_type = mqs_find_type( image, "ompi_request_t", mqs_lang_c );
        if( !qh_type ) {
            missing_in_action = "ompi_request_t";
            goto type_missing;
        }
        i_info->ompi_request_t.type = qh_type;
        i_info->ompi_request_t.size = mqs_sizeof(qh_type);
        ompi_field_offset(i_info->ompi_request_t.offset.req_type,
                          qh_type, ompi_request_t, req_type);
        ompi_field_offset(i_info->ompi_request_t.offset.req_status,
                          qh_type, ompi_request_t, req_status);
        ompi_field_offset(i_info->ompi_request_t.offset.req_complete,
                          qh_type, ompi_request_t, req_complete);
        ompi_field_offset(i_info->ompi_request_t.offset.req_state,
                          qh_type, ompi_request_t, req_state);
        ompi_field_offset(i_info->ompi_request_t.offset.req_f_to_c_index,
                          qh_type, ompi_request_t, req_f_to_c_index);
    }
    {
        mqs_type* qh_type = mqs_find_type( image, "mca_pml_base_request_t", mqs_lang_c );
        if( !qh_type ) {
            missing_in_action = "mca_pml_base_request_t";
            goto type_missing;
        }
        i_info->mca_pml_base_request_t.type = qh_type;
        i_info->mca_pml_base_request_t.size = mqs_sizeof(qh_type);
        ompi_field_offset(i_info->mca_pml_base_request_t.offset.req_addr,
                          qh_type, mca_pml_base_request_t, req_addr);
        ompi_field_offset(i_info->mca_pml_base_request_t.offset.req_count,
                          qh_type, mca_pml_base_request_t, req_count);
        ompi_field_offset(i_info->mca_pml_base_request_t.offset.req_peer,
                          qh_type, mca_pml_base_request_t, req_peer);
        ompi_field_offset(i_info->mca_pml_base_request_t.offset.req_tag,
                          qh_type, mca_pml_base_request_t, req_tag);
        ompi_field_offset(i_info->mca_pml_base_request_t.offset.req_comm,
                          qh_type, mca_pml_base_request_t, req_comm);
        ompi_field_offset(i_info->mca_pml_base_request_t.offset.req_datatype,
                          qh_type, mca_pml_base_request_t, req_datatype);
        ompi_field_offset(i_info->mca_pml_base_request_t.offset.req_proc,
                          qh_type, mca_pml_base_request_t, req_proc);
        ompi_field_offset(i_info->mca_pml_base_request_t.offset.req_sequence,
                          qh_type, mca_pml_base_request_t, req_sequence);
        ompi_field_offset(i_info->mca_pml_base_request_t.offset.req_type,
                          qh_type, mca_pml_base_request_t, req_type);
        ompi_field_offset(i_info->mca_pml_base_request_t.offset.req_pml_complete,
                          qh_type, mca_pml_base_request_t, req_pml_complete);
    }
    {
        mqs_type* qh_type = mqs_find_type( image, "mca_pml_base_send_request_t", mqs_lang_c );
        if( !qh_type ) {
            missing_in_action = "mca_pml_base_send_request_t";
            goto type_missing;
        }
        i_info->mca_pml_base_send_request_t.type = qh_type;
        i_info->mca_pml_base_send_request_t.size = mqs_sizeof(qh_type);
        ompi_field_offset(i_info->mca_pml_base_send_request_t.offset.req_addr,
                          qh_type, mca_pml_base_send_request_t, req_addr);
        ompi_field_offset(i_info->mca_pml_base_send_request_t.offset.req_bytes_packed,
                          qh_type, mca_pml_base_send_request_t, req_bytes_packed);
        ompi_field_offset(i_info->mca_pml_base_send_request_t.offset.req_send_mode,
                          qh_type, mca_pml_base_send_request_t, req_send_mode);
    }
    {
        mqs_type* qh_type = mqs_find_type( image, "mca_pml_base_recv_request_t", mqs_lang_c );
        if( !qh_type ) {
            missing_in_action = "mca_pml_base_recv_request_t";
            goto type_missing;
        }
        i_info->mca_pml_base_recv_request_t.type = qh_type;
        i_info->mca_pml_base_recv_request_t.size = mqs_sizeof(qh_type);
        ompi_field_offset(i_info->mca_pml_base_recv_request_t.offset.req_bytes_packed,
                          qh_type, mca_pml_base_recv_request_t, req_bytes_packed);
    }
    /*
     * Gather information about the received fragments and theirs headers.
     */
#if 0  /* Disabled until I find a better way */
    {
        mqs_type* qh_type = mqs_find_type( image, "mca_pml_ob1_common_hdr_t", mqs_lang_c );
        if( !qh_type ) {
            missing_in_action = "mca_pml_ob1_common_hdr_t";
            goto type_missing;
        }
        i_info->mca_pml_ob1_common_hdr_t.type = qh_type;
        i_info->mca_pml_ob1_common_hdr_t.size = mqs_sizeof(qh_type);
        ompi_field_offset(i_info->mca_pml_ob1_common_hdr_t.offset.hdr_type,
                          qh_type, mca_pml_ob1_common_hdr_t, hdr_type);
        ompi_field_offset(i_info->mca_pml_ob1_common_hdr_t.offset.hdr_flags,
                          qh_type, mca_pml_ob1_common_hdr_t, hdr_flags);
    }
    {
        mqs_type* qh_type = mqs_find_type( image, "mca_pml_ob1_match_hdr_t", mqs_lang_c );
        if( !qh_type ) {
            missing_in_action = "mca_pml_ob1_match_hdr_t";
            goto type_missing;
        }
        i_info->mca_pml_ob1_match_hdr_t.type = qh_type;
        i_info->mca_pml_ob1_match_hdr_t.size = mqs_sizeof(qh_type);
        ompi_field_offset(i_info->mca_pml_ob1_match_hdr_t.offset.hdr_common,
                          qh_type, mca_pml_ob1_match_hdr_t, hdr_common);
        ompi_field_offset(i_info->mca_pml_ob1_match_hdr_t.offset.hdr_ctx,
                          qh_type, mca_pml_ob1_match_hdr_t, hdr_ctx);
        ompi_field_offset(i_info->mca_pml_ob1_match_hdr_t.offset.hdr_src,
                          qh_type, mca_pml_ob1_match_hdr_t, hdr_src);
        ompi_field_offset(i_info->mca_pml_ob1_match_hdr_t.offset.hdr_tag,
                          qh_type, mca_pml_ob1_match_hdr_t, hdr_tag);
        ompi_field_offset(i_info->mca_pml_ob1_match_hdr_t.offset.hdr_seq,
                          qh_type, mca_pml_ob1_match_hdr_t, hdr_seq);
    }
    {
        mqs_type* qh_type = mqs_find_type( image, "mca_pml_ob1_recv_frag_t", mqs_lang_c );
        if( !qh_type ) {
            missing_in_action = "mca_pml_ob1_recv_frag_t";
            goto type_missing;
        }
        i_info->mca_pml_ob1_recv_frag_t.type = qh_type;
        i_info->mca_pml_ob1_recv_frag_t.size = mqs_sizeof(qh_type);
        ompi_field_offset(i_info->mca_pml_ob1_recv_frag_t.offset.hdr,
                          qh_type, mca_pml_ob1_recv_frag_t, hdr);
        ompi_field_offset(i_info->mca_pml_ob1_recv_frag_t.offset.request,
                          qh_type, mca_pml_ob1_recv_frag_t, request);
    }
#endif
    /*
     * And now let's look at the communicator and group structures.
     */
    {
        mqs_type* qh_type = mqs_find_type( image, "opal_pointer_array_t", mqs_lang_c );
        if( !qh_type ) {
            missing_in_action = "opal_pointer_array_t";
            goto type_missing;
        }
        i_info->opal_pointer_array_t.type = qh_type;
        i_info->opal_pointer_array_t.size = mqs_sizeof(qh_type);
        ompi_field_offset(i_info->opal_pointer_array_t.offset.lowest_free,
                          qh_type, opal_pointer_array_t, lowest_free);
        ompi_field_offset(i_info->opal_pointer_array_t.offset.number_free,
                          qh_type, opal_pointer_array_t, number_free);
        ompi_field_offset(i_info->opal_pointer_array_t.offset.size,
                          qh_type, opal_pointer_array_t, size);
        ompi_field_offset(i_info->opal_pointer_array_t.offset.addr,
                          qh_type, opal_pointer_array_t, addr);
    }
    {
        mqs_type* qh_type = mqs_find_type( image, "ompi_communicator_t", mqs_lang_c );
        if( !qh_type ) {
            missing_in_action = "ompi_communicator_t";
            goto type_missing;
        }
        i_info->ompi_communicator_t.type = qh_type;
        i_info->ompi_communicator_t.size = mqs_sizeof(qh_type);
        ompi_field_offset(i_info->ompi_communicator_t.offset.c_name,
                          qh_type, ompi_communicator_t, c_name);
        ompi_field_offset(i_info->ompi_communicator_t.offset.c_contextid,
                          qh_type, ompi_communicator_t, c_contextid);
        ompi_field_offset(i_info->ompi_communicator_t.offset.c_my_rank,
                          qh_type, ompi_communicator_t, c_my_rank);
        ompi_field_offset(i_info->ompi_communicator_t.offset.c_local_group,
                          qh_type, ompi_communicator_t, c_local_group);
        ompi_field_offset(i_info->ompi_communicator_t.offset.c_remote_group,
                          qh_type, ompi_communicator_t, c_remote_group);
        ompi_field_offset(i_info->ompi_communicator_t.offset.c_flags,
                          qh_type, ompi_communicator_t, c_flags);
        ompi_field_offset(i_info->ompi_communicator_t.offset.c_f_to_c_index,
                          qh_type, ompi_communicator_t, c_f_to_c_index);
        ompi_field_offset(i_info->ompi_communicator_t.offset.c_topo,
                          qh_type, ompi_communicator_t, c_topo);
        ompi_field_offset(i_info->ompi_communicator_t.offset.c_keyhash,
                          qh_type, ompi_communicator_t, c_keyhash);
    }
    {
        mqs_type* qh_type, *cart_type, *graph_type, *dist_graph_type;
        int offset = 0;

        missing_in_action = "mca_topo_base_module_t";
        qh_type = mqs_find_type(image, missing_in_action, mqs_lang_c);
        if( !qh_type ) {
            goto type_missing;
        }
        i_info->mca_topo_base_module_t.type = qh_type;
        i_info->mca_topo_base_module_t.size = mqs_sizeof(qh_type);

        /* The topo module contains multiple unions.  These fields are
           outside those unions. */
        ompi_field_offset(i_info->mca_topo_base_module_t.offset.mtc,
                          qh_type, mca_topo_base_module_t, mtc);
        ompi_field_offset(i_info->mca_topo_base_module_t.offset.reorder,
                          qh_type, mca_topo_base_module_t, reorder);

        /* By definition, the base offsets are 0 in the union.
	   Above we've got the base union offset, so now look up the
           individual fields in the cart, graph and dist_graph structs and
	   add them to the base union offset.  Then we have the offset of
           that field from the mca_topo_base_module_2_1_0_t type. */

        /* Cart type */
        missing_in_action = "mca_topo_base_comm_cart_2_2_0_t";
        cart_type = mqs_find_type(image, missing_in_action, mqs_lang_c);
        if (!cart_type) {
            goto type_missing;
        }
        ompi_field_offset(i_info->mca_topo_base_module_t.offset.mtc_cart.ndims,
                          cart_type, mca_topo_base_comm_cart_2_2_0_t,
                          ndims);
        ompi_field_offset(i_info->mca_topo_base_module_t.offset.mtc_cart.dims,
                          cart_type, mca_topo_base_comm_cart_2_2_0_t,
                          dims);
        ompi_field_offset(i_info->mca_topo_base_module_t.offset.mtc_cart.periods,
                          cart_type, mca_topo_base_comm_cart_2_2_0_t,
                          periods);
        ompi_field_offset(i_info->mca_topo_base_module_t.offset.mtc_cart.coords,
                          cart_type, mca_topo_base_comm_cart_2_2_0_t,
                          coords);
        i_info->mca_topo_base_module_t.offset.mtc_cart.ndims   += offset;
        i_info->mca_topo_base_module_t.offset.mtc_cart.dims    += offset;
        i_info->mca_topo_base_module_t.offset.mtc_cart.periods += offset;
        i_info->mca_topo_base_module_t.offset.mtc_cart.coords  += offset;

        /* Graph type */
        missing_in_action = "mca_topo_base_comm_graph_2_2_0_t";
        graph_type = mqs_find_type(image, missing_in_action, mqs_lang_c);
        if (!graph_type) {
            goto type_missing;
        }
        ompi_field_offset(i_info->mca_topo_base_module_t.offset.mtc_graph.nnodes,
                          graph_type, mca_topo_base_comm_graph_2_2_0_t,
                          nnodes);
        ompi_field_offset(i_info->mca_topo_base_module_t.offset.mtc_graph.index,
                          graph_type, mca_topo_base_comm_graph_2_2_0_t,
                          index);
        ompi_field_offset(i_info->mca_topo_base_module_t.offset.mtc_graph.edges,
                          graph_type, mca_topo_base_comm_graph_2_2_0_t,
                          edges);
        i_info->mca_topo_base_module_t.offset.mtc_graph.nnodes += offset;
        i_info->mca_topo_base_module_t.offset.mtc_graph.index  += offset;
        i_info->mca_topo_base_module_t.offset.mtc_graph.edges  += offset;

        /* Distributed Graph type */
        missing_in_action = "mca_topo_base_comm_dist_graph_2_2_0_t";
        dist_graph_type = mqs_find_type(image, missing_in_action, mqs_lang_c);
        if (!dist_graph_type) {
            goto type_missing;
        }
        ompi_field_offset(i_info->mca_topo_base_module_t.offset.mtc_dist_graph.in,
                          dist_graph_type, mca_topo_base_comm_dist_graph_2_2_0_t,
                          in);
        ompi_field_offset(i_info->mca_topo_base_module_t.offset.mtc_dist_graph.inw,
                          dist_graph_type, mca_topo_base_comm_dist_graph_2_2_0_t,
                          inw);
        ompi_field_offset(i_info->mca_topo_base_module_t.offset.mtc_dist_graph.out,
                          dist_graph_type, mca_topo_base_comm_dist_graph_2_2_0_t,
                          out);
        ompi_field_offset(i_info->mca_topo_base_module_t.offset.mtc_dist_graph.outw,
                          dist_graph_type, mca_topo_base_comm_dist_graph_2_2_0_t,
                          outw);
        ompi_field_offset(i_info->mca_topo_base_module_t.offset.mtc_dist_graph.indegree,
                          dist_graph_type, mca_topo_base_comm_dist_graph_2_2_0_t,
                          indegree);
        ompi_field_offset(i_info->mca_topo_base_module_t.offset.mtc_dist_graph.outdegree,
                          dist_graph_type, mca_topo_base_comm_dist_graph_2_2_0_t,
                          outdegree);
        ompi_field_offset(i_info->mca_topo_base_module_t.offset.mtc_dist_graph.weighted,
                          dist_graph_type, mca_topo_base_comm_dist_graph_2_2_0_t,
                          weighted);
    }
    {
        mqs_type* qh_type = mqs_find_type( image, "ompi_group_t", mqs_lang_c );
        if( !qh_type ) {
            missing_in_action = "ompi_group_t";
            goto type_missing;
        }
        i_info->ompi_group_t.type = qh_type;
        i_info->ompi_group_t.size = mqs_sizeof(qh_type);
        ompi_field_offset(i_info->ompi_group_t.offset.grp_proc_count,
                          qh_type, ompi_group_t, grp_proc_count);
        ompi_field_offset(i_info->ompi_group_t.offset.grp_proc_pointers,
                          qh_type, ompi_group_t, grp_proc_pointers);
        ompi_field_offset(i_info->ompi_group_t.offset.grp_my_rank,
                          qh_type, ompi_group_t, grp_my_rank);
        ompi_field_offset(i_info->ompi_group_t.offset.grp_flags,
                          qh_type, ompi_group_t, grp_flags);
    }
    {
        mqs_type* qh_type = mqs_find_type( image, "ompi_status_public_t", mqs_lang_c );
        if( !qh_type ) {
            missing_in_action = "ompi_status_public_t";
            goto type_missing;
        }
        i_info->ompi_status_public_t.type = qh_type;
        i_info->ompi_status_public_t.size = mqs_sizeof(qh_type);
        ompi_field_offset(i_info->ompi_status_public_t.offset.MPI_SOURCE,
                          qh_type, ompi_status_public_t, MPI_SOURCE);
        ompi_field_offset(i_info->ompi_status_public_t.offset.MPI_TAG,
                          qh_type, ompi_status_public_t, MPI_TAG);
        ompi_field_offset(i_info->ompi_status_public_t.offset.MPI_ERROR,
                          qh_type, ompi_status_public_t, MPI_ERROR);
        ompi_field_offset(i_info->ompi_status_public_t.offset._ucount,
                          qh_type, ompi_status_public_t, _ucount);
        ompi_field_offset(i_info->ompi_status_public_t.offset._cancelled,
                          qh_type, ompi_status_public_t, _cancelled);
    }
    {
        mqs_type* qh_type = mqs_find_type( image, "ompi_datatype_t", mqs_lang_c );
        if( !qh_type ) {
            missing_in_action = "ompi_datatype_t";
            goto type_missing;
        }
        i_info->ompi_datatype_t.type = qh_type;
        i_info->ompi_datatype_t.size = mqs_sizeof(qh_type);
        ompi_field_offset(i_info->ompi_datatype_t.offset.name,
                          qh_type, ompi_datatype_t, name);

        /* get ompi_datatype_t super.size which requires the offset
         * of super and then the offset of size in opal_datatype_t.
         */
        {
            int super_offset = 0;

            ompi_field_offset(super_offset,
                              qh_type, ompi_datatype_t, super);

            qh_type = mqs_find_type( image, "opal_datatype_t", mqs_lang_c );
            if( !qh_type ) {
                missing_in_action = "opal_datatype_t";
                goto type_missing;
            }
            ompi_field_offset(i_info->ompi_datatype_t.offset.size,
                              qh_type, opal_datatype_t, size);
            i_info->ompi_datatype_t.offset.size += super_offset;
        }
    }

    /* All the types are here. Let's succesfully return. */
    *message = NULL;
    return mqs_ok;

 type_missing:
    /*
     * One of the required types is missing in the image. We are
     * unable to extract the information we need from the pointers. We
     * did our best but here we're at our limit. Give up!
     */
    *message = missing_in_action;
    fprintf(stderr, "WARNING: Open MPI is unable to find debugging information about the \"%s\" type.  This can happen if Open MPI was built without debugging information, or was stripped after building.\n",
           missing_in_action);
    return err_missing_type;
}

/***********************************************************************
 * Functions to access the image memory. They are specialized based    *
 * on the type we want to access and the debugged process architecture *
 ***********************************************************************/
mqs_taddr_t ompi_fetch_pointer (mqs_process *proc, mqs_taddr_t addr,
                                mpi_process_info *p_info)
{
    int isize = p_info->sizes.pointer_size;
    char buffer[8];                  /* ASSUME the type fits in 8 bytes */
    mqs_taddr_t res = 0;

    if (mqs_ok == mqs_fetch_data (proc, addr, isize, buffer))
        mqs_target_to_host (proc, buffer,
                            ((char *)&res) + (host_is_big_endian ? sizeof(mqs_taddr_t)-isize : 0),
                            isize);

    return res;
} /* fetch_pointer */

/***********************************************************************/
mqs_tword_t ompi_fetch_int (mqs_process *proc, mqs_taddr_t addr,
                            mpi_process_info *p_info)
{
    int isize = p_info->sizes.int_size;
    char buffer[8];                  /* ASSUME the type fits in 8 bytes */
    mqs_tword_t res = 0;

    if (mqs_ok == mqs_fetch_data (proc, addr, isize, buffer)) {
        mqs_target_to_host (proc, buffer,
                            ((char *)&res) + (host_is_big_endian ? sizeof(mqs_tword_t)-isize : 0),
                            isize);
    }
    return res;
} /* fetch_int */

/***********************************************************************/
mqs_tword_t ompi_fetch_bool(mqs_process *proc, mqs_taddr_t addr,
                            mpi_process_info *p_info)
{
    int isize = p_info->sizes.bool_size;
    mqs_tword_t res = 0;

    mqs_fetch_data (proc, addr, isize, &res);
    return (0 == res ? 0 : 1);
} /* fetch_bool */

/***********************************************************************/
mqs_taddr_t ompi_fetch_size_t(mqs_process *proc, mqs_taddr_t addr,
                              mpi_process_info *p_info)
{
    int isize = p_info->sizes.size_t_size;
    char buffer[8];                  /* ASSUME the type fits in 8 bytes */
    mqs_taddr_t res = 0;

    if (mqs_ok == mqs_fetch_data (proc, addr, isize, buffer))
        mqs_target_to_host (proc, buffer,
                            ((char *)&res) + (host_is_big_endian ? sizeof(mqs_taddr_t)-isize : 0),
                            isize);

    return res;
} /* fetch_size_t */

/***********************************************************************/

int ompi_fetch_opal_pointer_array_info(mqs_process *proc, mqs_taddr_t addr,
                                       mpi_process_info *p_info,
                                       int *size, int *lowest_free,
                                       int *number_free)
{
    mqs_image *image = mqs_get_image(proc);
    mpi_image_info *i_info = (mpi_image_info *) mqs_get_image_info(image);

    *size = ompi_fetch_int(proc,
                           addr + i_info->opal_pointer_array_t.offset.size,
                           p_info);
    *lowest_free = ompi_fetch_int(proc,
                                  addr + i_info->opal_pointer_array_t.offset.lowest_free,
                                  p_info);
    *number_free = ompi_fetch_int(proc,
                                  addr + i_info->opal_pointer_array_t.offset.number_free,
                                  p_info);
    return mqs_ok;
}

/***********************************************************************/

int ompi_fetch_opal_pointer_array_item(mqs_process *proc, mqs_taddr_t addr,
                                       mpi_process_info *p_info, int index,
                                       mqs_taddr_t *item)
{
    mqs_image *image = mqs_get_image(proc);
    mpi_image_info *i_info = (mpi_image_info *) mqs_get_image_info(image);
    int size, lowest_free, number_free;
    mqs_taddr_t base;

    if (index < 0) {
        return mqs_no_information;
    }

    ompi_fetch_opal_pointer_array_info(proc, addr, p_info, &size,
                                       &lowest_free, &number_free);
    if (index >= size) {
        return mqs_no_information;
    }

    base = ompi_fetch_pointer(proc,
                              addr + i_info->opal_pointer_array_t.offset.addr,
                              p_info);
    *item = ompi_fetch_pointer(proc,
                               base + index * p_info->sizes.pointer_size,
                               p_info);

    return mqs_ok;
}

int ompi_get_lib_version(char * buf, int size) {
    int ret;
    ret = snprintf(buf, size-1, "Open MPI v%d.%d.%d%s%s%s%s%s%s%s%s%s",
                   OMPI_MAJOR_VERSION, OMPI_MINOR_VERSION, OMPI_RELEASE_VERSION,
                   (strlen(OMPI_GREEK_VERSION) > 0)?OMPI_GREEK_VERSION:"",
                   (strlen(OPAL_PACKAGE_STRING) > 0)?", package: ":"",
                   (strlen(OPAL_PACKAGE_STRING) > 0)?OPAL_PACKAGE_STRING:"",
                   (strlen(OPAL_IDENT_STRING)> 0)?", ident: ":"",
                   (strlen(OPAL_IDENT_STRING)> 0)?OMPI_IDENT_STRING:"",
                   (strlen(OMPI_REPO_REV) > 0)?", repo rev: ":"",
                   (strlen(OMPI_REPO_REV) > 0)?OMPI_REPO_REV:"",
                   (strlen(OMPI_RELEASE_DATE) > 0)?", ":"",
                   (strlen(OMPI_RELEASE_DATE) > 0)?OMPI_RELEASE_DATE:"");
    buf[size-1] = '\0';
    return ret;
}
