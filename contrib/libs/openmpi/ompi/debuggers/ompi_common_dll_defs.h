/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2007-2014 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2004-2013 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2012-2013 Inria.  All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2016      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
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
 *				  DISCLAIMER
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

#ifndef OMPI_COMMON_DLL_DEFS_H
#define OMPI_COMMON_DLL_DEFS_H

#include "msgq_interface.h"

/***********************************************************************
 * Information associated with a specific executable image.  Common
 * across all DLLs.
 */
typedef struct
{
    /* Functions needed here */
    const struct mqs_image_callbacks * image_callbacks;

    /* basic structures */
    struct {
        mqs_type *type;
        int size;
        struct {
            int opal_list_next;
        } offset;
    } opal_list_item_t;
    struct {
        mqs_type *type;
        int size;
        struct {
            int opal_list_sentinel;
        } offset;
    } opal_list_t;
    struct {
        mqs_type *type;
        int size;
    } opal_free_list_item_t;
    struct {
        mqs_type *type;
        int size;
        struct {
            int fl_frag_class;         /* opal_class_t* */
            int fl_mpool;              /* struct mca_mpool_base_module_t* */
            int fl_frag_size;          /* size_t */
            int fl_frag_alignment;     /* size_t */
            int fl_allocations;        /* opal_list_t */
            int fl_max_to_alloc;       /* size_t */
            int fl_num_per_alloc;      /* size_t */
            int fl_num_allocated;      /* size_t */
        } offset;
    } opal_free_list_t;
    struct {
        mqs_type *type;
        int size;
        struct {
            int ht_table;
            int ht_table_size;
            int ht_size;
            int ht_mask;
        } offset;
    } opal_hash_table_t;
    /* requests structures */
    struct {
        mqs_type *type;
        int size;
        struct {
            int req_type;
            int req_status;
            int req_complete;
            int req_state;
            int req_f_to_c_index;
        } offset;
    } ompi_request_t;
    struct {
        mqs_type *type;
        int size;
        struct {
            int req_addr;
            int req_count;
            int req_peer;
            int req_tag;
            int req_comm;
            int req_datatype;
            int req_proc;
            int req_sequence;
            int req_type;
            int req_pml_complete;
        } offset;
    } mca_pml_base_request_t;
    struct {
        mqs_type *type;
        int size;
        struct {
            int req_addr;
            int req_bytes_packed;
            int req_send_mode;
        } offset;
    } mca_pml_base_send_request_t;
    struct {
        mqs_type *type;
        int size;
        struct {
            int req_bytes_packed;
        } offset;
    } mca_pml_base_recv_request_t;
#if 0
    /* fragments for unexpected messages (as well as theirs headers) */
    struct {
        mqs_type *type;
        int size;
        struct {
            int hdr;
            int request;
        } offset;
    } mca_pml_ob1_recv_frag_t;
    struct {
        mqs_type *type;
        int size;
        struct {
            int hdr_type;
            int hdr_flags;
        } offset;
    } mca_pml_ob1_common_hdr_t;
    struct {
        mqs_type *type;
        int size;
        struct {
            int hdr_common;
            int hdr_ctx;
            int hdr_src;
            int hdr_tag;
            int hdr_seq;
        } offset;
    } mca_pml_ob1_match_hdr_t;
#endif
    /* opal_pointer_array structure */
    struct {
        mqs_type *type;
        int size;
        struct {
            int lowest_free;
            int number_free;
            int size;
            int addr;
        } offset;
    } opal_pointer_array_t;
    /* group structure */
    struct {
        mqs_type *type;
        int size;
        struct {
            int grp_proc_count;
            int grp_proc_pointers;
            int grp_my_rank;
            int grp_flags;
        } offset;
    } ompi_group_t;
    /* communicator structure */
    struct {
        mqs_type *type;
        int size;
        struct {
            int c_name;
            int c_contextid;
            int c_my_rank;
            int c_local_group;
            int c_remote_group;
            int c_flags;
            int c_f_to_c_index;
            int c_topo;
            int c_keyhash;
        } offset;
    } ompi_communicator_t;
    /* base topology information in a communicator */
    struct {
        mqs_type *type;
        int size;
        struct {
            int mtc;
            struct {
                int ndims;
                int dims;
                int periods;
                int coords;
            } mtc_cart;
            struct {
                int nnodes;
                int index;
                int edges;
            } mtc_graph;
            struct {
                int in;
                int inw;
                int out;
                int outw;
                int indegree;
                int outdegree;
                int weighted;
            } mtc_dist_graph;
            int reorder;
        } offset;
    } mca_topo_base_module_t;
    /* MPI_Status */
    struct {
        mqs_type *type;
        int size;
        struct {
            int MPI_SOURCE;
            int MPI_TAG;
            int MPI_ERROR;
            int _cancelled;
            size_t _ucount;
        } offset;
    } ompi_status_public_t;
    /* datatype structure */
    struct {
        mqs_type *type;
        int size;
        struct {
            int size;
            int name;
        } offset;
    } ompi_datatype_t;

    /* For the caller to hang their own stuff */
    void *extra;
} mpi_image_info;

/***********************************************************************/
/* Information for a single process.  Common across all DLLs.
 */
typedef struct
{
    const struct mqs_process_callbacks * process_callbacks; /* Functions needed here */

    mqs_target_type_sizes sizes;			/* Process architecture information */

    /* For the caller to hang their own stuff */
    void *extra;
} mpi_process_info;

/**********************************************************************/
/* Macros to make it transparent that we're calling the TV functions
 * through function pointers.
 */
#define mqs_malloc           (mqs_basic_entrypoints->mqs_malloc_fp)
#define mqs_free             (mqs_basic_entrypoints->mqs_free_fp)
#define mqs_prints           (mqs_basic_entrypoints->mqs_dprints_fp)
#define mqs_put_image_info   (mqs_basic_entrypoints->mqs_put_image_info_fp)
#define mqs_get_image_info   (mqs_basic_entrypoints->mqs_get_image_info_fp)
#define mqs_put_process_info (mqs_basic_entrypoints->mqs_put_process_info_fp)
#define mqs_get_process_info (mqs_basic_entrypoints->mqs_get_process_info_fp)

/* These macros *RELY* on the function already having set up the conventional
 * local variables i_info or p_info.
 */
#define mqs_find_type        (i_info->image_callbacks->mqs_find_type_fp)
#define mqs_field_offset     (i_info->image_callbacks->mqs_field_offset_fp)
#define mqs_sizeof           (i_info->image_callbacks->mqs_sizeof_fp)
#define mqs_get_type_sizes   (i_info->image_callbacks->mqs_get_type_sizes_fp)
#define mqs_find_function    (i_info->image_callbacks->mqs_find_function_fp)
#define mqs_find_symbol      (i_info->image_callbacks->mqs_find_symbol_fp)

#define mqs_get_image        (p_info->process_callbacks->mqs_get_image_fp)
#define mqs_get_global_rank  (p_info->process_callbacks->mqs_get_global_rank_fp)
#define mqs_fetch_data       (p_info->process_callbacks->mqs_fetch_data_fp)
#define mqs_target_to_host   (p_info->process_callbacks->mqs_target_to_host_fp)

/* Basic callbacks into the debugger */
extern const mqs_basic_callbacks *mqs_basic_entrypoints;

/* OMPI-specific functions */
int ompi_fill_in_type_info(mqs_image *image, char **message);

/* Fetch a pointer from the process */
mqs_taddr_t ompi_fetch_pointer(mqs_process *proc, mqs_taddr_t addr,
                               mpi_process_info *p_info);

/* Fetch an int from the process */
mqs_tword_t ompi_fetch_int(mqs_process *proc, mqs_taddr_t addr,
                           mpi_process_info *p_info);

/* Fetch a bool from the process */
mqs_tword_t ompi_fetch_bool(mqs_process *proc, mqs_taddr_t addr,
                            mpi_process_info *p_info);

/* Fetch a size_t from the process */
mqs_taddr_t ompi_fetch_size_t(mqs_process *proc, mqs_taddr_t addr,
                              mpi_process_info *p_info);

/* Helpers to fetch stuff from an opal_pointer_array_t */
int ompi_fetch_opal_pointer_array_info(mqs_process *proc, mqs_taddr_t addr,
                                       mpi_process_info *p_info,
                                       int *size, int *lowest_free,
                                       int *number_free);
int ompi_fetch_opal_pointer_array_item(mqs_process *proc, mqs_taddr_t addr,
                                       mpi_process_info *p_info, int index,
                                       mqs_taddr_t *item);
#define OMPI_MAX_VER_SIZE 256
int ompi_get_lib_version(char *buf, int size);
#endif
