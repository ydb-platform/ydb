/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2007 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 *
 * Copyright (c) 2006      Voltaire. All rights reserved.
 * Copyright (c) 2009      IBM Corporation.  All rights reserved.
 * Copyright (c) 2015-2018 Los Alamos National Security, LLC. All rights
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
 * Registration cache VMA lookup
 */

#ifndef MCA_RCACHE_BASE_VMA_H
#define MCA_RCACHE_BASE_VMA_H

#include "opal_config.h"
#include "opal/class/opal_list.h"
#include "opal/class/opal_interval_tree.h"
#include "opal/class/opal_lifo.h"

BEGIN_C_DECLS

struct mca_rcache_base_registration_t;

struct mca_rcache_base_vma_module_t {
    opal_object_t super;
    opal_interval_tree_t tree;
    opal_list_t vma_list;
    opal_lifo_t vma_gc_lifo;
    size_t reg_cur_cache_size;
    opal_mutex_t vma_lock;
};
typedef struct mca_rcache_base_vma_module_t mca_rcache_base_vma_module_t;

OBJ_CLASS_DECLARATION(mca_rcache_base_vma_module_t);

mca_rcache_base_vma_module_t *mca_rcache_base_vma_module_alloc (void);

int mca_rcache_base_vma_find (mca_rcache_base_vma_module_t *vma_module, void *addr,
                              size_t size, struct mca_rcache_base_registration_t **reg);

int mca_rcache_base_vma_find_all (mca_rcache_base_vma_module_t *vma_module, void *addr,
                                  size_t size, struct mca_rcache_base_registration_t **regs,
                                  int reg_cnt);

int mca_rcache_base_vma_insert (mca_rcache_base_vma_module_t *vma_module,
                                struct mca_rcache_base_registration_t *registration,
                                size_t limit);

int mca_rcache_base_vma_delete (mca_rcache_base_vma_module_t *vma_module,
                                struct mca_rcache_base_registration_t *registration);

void mca_rcache_base_vma_dump_range (mca_rcache_base_vma_module_t *vma_module,
                                     unsigned char *base, size_t size, char *msg);

/**
 * Iterate over registrations in the specified range.
 *
 * @param[in] vma_module  vma tree
 * @param[in] base        base address of region
 * @param[in] size        size of region
 * @param[in] partial_ok  partial overlap of range is ok
 * @param[in] callback_fn function to call for each matching registration handle
 * @param[in] ctx         callback context
 *
 * The callback will be made with the vma lock held. This is a recursive lock so
 * it is still safe to call any vma functions on this vma_module. Keep in mind it
 * is only safe to call mca_rcache_base_vma_delete() on the supplied registration
 * from the callback. The iteration will terminate if the callback returns anything
 * other than OPAL_SUCCESS.
 */
int mca_rcache_base_vma_iterate (mca_rcache_base_vma_module_t *vma_module,
                                 unsigned char *base, size_t size, bool partial_ok,
                                 int (*callback_fn) (struct mca_rcache_base_registration_t *, void *),
                                 void *ctx);

size_t mca_rcache_base_vma_size (mca_rcache_base_vma_module_t *vma_module);

END_C_DECLS

#endif /* MCA_RCACHE_BASE_VMA_H */
