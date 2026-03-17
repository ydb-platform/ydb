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
 * Copyright (c) 2006      Voltaire. All rights reserved.
 * Copyright (c) 2011-2018 Los Alamos National Security, LLC. All rights
 *                         reserved.
 *
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/**
 * @file
 */
#ifndef MCA_MPOOL_HUGEPAGE_H
#define MCA_MPOOL_HUGEPAGE_H

#include "opal_config.h"
#include "opal/class/opal_list.h"
#include "opal/class/opal_free_list.h"
#include "opal/class/opal_rb_tree.h"
#include "opal/mca/event/event.h"
#include "opal/mca/mpool/mpool.h"
#include "opal/util/proc.h"
#include "opal/mca/allocator/allocator.h"
#include "opal/util/sys_limits.h"

BEGIN_C_DECLS
struct mca_mpool_hugepage_module_t;
typedef struct mca_mpool_hugepage_module_t mca_mpool_hugepage_module_t;

struct mca_mpool_hugepage_component_t {
    mca_mpool_base_component_t super;
    bool print_stats;
    opal_list_t huge_pages;
    mca_mpool_hugepage_module_t *modules;
    int module_count;
    unsigned long bytes_allocated;
};
typedef struct mca_mpool_hugepage_component_t mca_mpool_hugepage_component_t;

OPAL_DECLSPEC extern mca_mpool_hugepage_component_t mca_mpool_hugepage_component;

struct mca_mpool_hugepage_module_t;

struct mca_mpool_hugepage_hugepage_t {
    /** opal list item superclass */
    opal_list_item_t super;
    /** page size in bytes */
    unsigned long    page_size;
    /** path for mmapped files */
    char            *path;
    /** counter to help ensure unique file names for mmaped files */
    volatile int32_t count;
    /** some platforms allow allocation of hugepages through mmap flags */
    int              mmap_flags;
};
typedef struct mca_mpool_hugepage_hugepage_t mca_mpool_hugepage_hugepage_t;

OBJ_CLASS_DECLARATION(mca_mpool_hugepage_hugepage_t);

struct mca_mpool_hugepage_module_t {
    mca_mpool_base_module_t super;
    mca_mpool_hugepage_hugepage_t *huge_page;
    mca_allocator_base_module_t *allocator;
    opal_mutex_t lock;
    opal_rb_tree_t allocation_tree;
};

/*
 *  Initializes the mpool module.
 */
int mca_mpool_hugepage_module_init (mca_mpool_hugepage_module_t *mpool,
                                    mca_mpool_hugepage_hugepage_t *huge_page);

void *mca_mpool_hugepage_seg_alloc (void *ctx, size_t *sizep);
void mca_mpool_hugepage_seg_free (void *ctx, void *addr);

END_C_DECLS
#endif
