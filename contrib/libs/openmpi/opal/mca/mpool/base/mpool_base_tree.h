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
 * Copyright (c) 2004-2006 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef MCA_MPOOL_BASE_TREE_H
#define MCA_MPOOL_BASE_TREE_H

#include "opal_config.h"

#define MCA_MPOOL_BASE_TREE_MAX 8
#include "opal/mca/mca.h"
#include "opal/mca/mpool/mpool.h"
#include "opal/mca/rcache/rcache.h"

BEGIN_C_DECLS

/*
 * Data structures for the tree of allocated memory
 * used for MPI_Alloc_mem and MPI_Free_mem
 */

/**
 * The item in the tree itself
 */
struct mca_mpool_base_tree_item_t
{
    opal_free_list_item_t super;   /**< the parent class */
    void* key; /**< the address this was alloc'd on */
    size_t num_bytes; /**< the number of bytes in this alloc, only for
                           debugging reporting with
                           mpi_show_mpi_alloc_mem_leaks */
    mca_mpool_base_module_t *mpool;
    mca_rcache_base_module_t *rcaches[MCA_MPOOL_BASE_TREE_MAX]; /**< the registration caches */
    mca_rcache_base_registration_t *regs[MCA_MPOOL_BASE_TREE_MAX]; /**< the registrations */
    uint8_t count; /**< length of the mpools/regs array */
};
typedef struct mca_mpool_base_tree_item_t mca_mpool_base_tree_item_t;

OPAL_DECLSPEC OBJ_CLASS_DECLARATION(mca_mpool_base_tree_item_t);

/*
 * initialize/finalize the rb tree
 */
int mca_mpool_base_tree_init(void);
int mca_mpool_base_tree_fini(void);

/*
 * insert an item in the rb tree
 */
int mca_mpool_base_tree_insert(mca_mpool_base_tree_item_t* item);

/*
 * remove an item from the rb tree
 */
int mca_mpool_base_tree_delete(mca_mpool_base_tree_item_t* item);


/**
 *  find the item in the rb tree
 */
mca_mpool_base_tree_item_t* mca_mpool_base_tree_find(void* base);

/*
 * get a tree item from the free list
 */
mca_mpool_base_tree_item_t* mca_mpool_base_tree_item_get(void);

/*
 * put tree item back into the free list
 */
void mca_mpool_base_tree_item_put(mca_mpool_base_tree_item_t* item);

/*
 * For debugging, print a show_help kind of message if there are items
 * left in the tree. The argument is the number of items to be printed
 * before becoming silent.
 */
void mca_mpool_base_tree_print(int);

END_C_DECLS

#endif /* MCA_MPOOL_BASE_TREE_H */
