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
 * Copyright (c) 2006      Voltaire. All rights reserved.
 * Copyright (c) 2009-2013 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2009      IBM Corporation.  All rights reserved.
 * Copyright (c) 2013      NVIDIA Corporation.  All rights reserved.
 * Copyright (c) 2015-2018 Los Alamos National Security, LLC. All rights
 *                         reserved.
 *
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"

#include MCA_memory_IMPLEMENTATION_HEADER
#include "opal/mca/memory/memory.h"
#include "opal/mca/rcache/rcache.h"
#include "rcache_base_vma.h"
#include "rcache_base_vma_tree.h"
#include "opal/mca/rcache/base/base.h"

/**
 * Initialize the rcache
 */

static void mca_rcache_base_vma_module_construct (mca_rcache_base_vma_module_t *vma_module) {
    OBJ_CONSTRUCT(&vma_module->vma_lock, opal_recursive_mutex_t);
    (void) mca_rcache_base_vma_tree_init (vma_module);
}

static void mca_rcache_base_vma_module_destruct (mca_rcache_base_vma_module_t *vma_module) {
    OBJ_DESTRUCT(&vma_module->vma_lock);
    mca_rcache_base_vma_tree_finalize (vma_module);
}

OBJ_CLASS_INSTANCE(mca_rcache_base_vma_module_t, opal_object_t,
                   mca_rcache_base_vma_module_construct,
                   mca_rcache_base_vma_module_destruct);

mca_rcache_base_vma_module_t *mca_rcache_base_vma_module_alloc (void)
{
    return OBJ_NEW(mca_rcache_base_vma_module_t);
}

int mca_rcache_base_vma_find (mca_rcache_base_vma_module_t *vma_module, void *addr,
                              size_t size, mca_rcache_base_registration_t **reg)
{
    int rc;
    unsigned char *bound_addr;

    if (size == 0) {
        return OPAL_ERROR;
    }

    bound_addr = (unsigned char *) ((intptr_t) addr + size - 1);

    /* Check to ensure that the cache is valid */
    if (OPAL_UNLIKELY(opal_memory_changed() &&
                      NULL != opal_memory->memoryc_process &&
                      OPAL_SUCCESS != (rc = opal_memory->memoryc_process()))) {
        return rc;
    }

    *reg = mca_rcache_base_vma_tree_find (vma_module, (unsigned char *) addr, bound_addr);

    return OPAL_SUCCESS;
}

int mca_rcache_base_vma_find_all (mca_rcache_base_vma_module_t *vma_module, void *addr,
                                  size_t size, mca_rcache_base_registration_t **regs,
                                  int reg_cnt)
{
    int rc;
    unsigned char *bound_addr;

    if(size == 0) {
        return OPAL_ERROR;
    }

    bound_addr = (unsigned char *) ((intptr_t) addr + size - 1);

    /* Check to ensure that the cache is valid */
    if (OPAL_UNLIKELY(opal_memory_changed() &&
                      NULL != opal_memory->memoryc_process &&
                      OPAL_SUCCESS != (rc = opal_memory->memoryc_process()))) {
        return rc;
    }

    return mca_rcache_base_vma_tree_find_all (vma_module, (unsigned char *) addr,
                                              bound_addr, regs, reg_cnt);
}

int mca_rcache_base_vma_insert (mca_rcache_base_vma_module_t *vma_module,
                                mca_rcache_base_registration_t *reg, size_t limit)
{
    size_t reg_size = reg->bound - reg->base + 1;
    int rc;

    if (limit != 0 && reg_size > limit) {
        /* return out of resources if request is bigger than cache size
         * return temp out of resources otherwise */
        return OPAL_ERR_OUT_OF_RESOURCE;
    }

    /* Check to ensure that the cache is valid */
    if (OPAL_UNLIKELY(opal_memory_changed() &&
                      NULL != opal_memory->memoryc_process &&
                      OPAL_SUCCESS != (rc = opal_memory->memoryc_process()))) {
        return rc;
    }

    rc = mca_rcache_base_vma_tree_insert (vma_module, reg, limit);
    if (OPAL_LIKELY(OPAL_SUCCESS == rc)) {
        /* If we successfully registered, then tell the memory manager
           to start monitoring this region */
        opal_memory->memoryc_register (reg->base, (uint64_t) reg_size,
                                       (uint64_t) (uintptr_t) reg);
    }

    return rc;
}

int mca_rcache_base_vma_delete (mca_rcache_base_vma_module_t *vma_module,
                                mca_rcache_base_registration_t *reg)
{
    /* Tell the memory manager that we no longer care about this
       region */
    opal_memory->memoryc_deregister (reg->base,
                                     (uint64_t) (reg->bound - reg->base),
                                     (uint64_t) (uintptr_t) reg);
    return mca_rcache_base_vma_tree_delete (vma_module, reg);
}

int mca_rcache_base_vma_iterate (mca_rcache_base_vma_module_t *vma_module,
                                 unsigned char *base, size_t size, bool partial_ok,
                                 int (*callback_fn) (struct mca_rcache_base_registration_t *, void *),
                                 void *ctx)
{
    return mca_rcache_base_vma_tree_iterate (vma_module, base, size, partial_ok, callback_fn, ctx);
}

void mca_rcache_base_vma_dump_range (mca_rcache_base_vma_module_t *vma_module,
                                     unsigned char *base, size_t size, char *msg)
{
    mca_rcache_base_vma_tree_dump_range (vma_module, base, size, msg);
}

size_t mca_rcache_base_vma_size (mca_rcache_base_vma_module_t *vma_module)
{
    return mca_rcache_base_vma_tree_size (vma_module);
}
