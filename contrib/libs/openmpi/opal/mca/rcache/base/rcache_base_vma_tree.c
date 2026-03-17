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
 *
 * Copyright (c) 2006      Voltaire. All rights reserved.
 * Copyright (c) 2007      Mellanox Technologies. All rights reserved.
 * Copyright (c) 2009      IBM Corporation.  All rights reserved.
 * Copyright (c) 2013      NVIDIA Corporation.  All rights reserved.
 * Copyright (c) 2013-2018 Cisco Systems, Inc.  All rights reserved
 * Copyright (c) 2015-2018 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal/util/output.h"
#include "rcache_base_vma_tree.h"
#include "opal/mca/rcache/base/base.h"

int mca_rcache_base_vma_tree_init (mca_rcache_base_vma_module_t *vma_module)
{
    OBJ_CONSTRUCT(&vma_module->tree, opal_interval_tree_t);
    vma_module->reg_cur_cache_size = 0;
    return opal_interval_tree_init (&vma_module->tree);
}

void mca_rcache_base_vma_tree_finalize (mca_rcache_base_vma_module_t *vma_module)
{
    OBJ_DESTRUCT(&vma_module->tree);
}

mca_rcache_base_registration_t *mca_rcache_base_vma_tree_find (mca_rcache_base_vma_module_t *vma_module,
                                                               unsigned char *base, unsigned char *bound)
{
    return (mca_rcache_base_registration_t *) opal_interval_tree_find_overlapping (&vma_module->tree, (uintptr_t) base,
                                                                                   ((uintptr_t) bound) + 1);
}

struct mca_rcache_base_vma_tree_find_all_helper_args_t {
    mca_rcache_base_registration_t **regs;
    int reg_cnt;
    int reg_max;
};

typedef struct mca_rcache_base_vma_tree_find_all_helper_args_t mca_rcache_base_vma_tree_find_all_helper_args_t;

static int mca_rcache_base_vma_tree_find_all_helper (uint64_t low, uint64_t high, void *data, void *ctx)
{
    mca_rcache_base_vma_tree_find_all_helper_args_t *args = (mca_rcache_base_vma_tree_find_all_helper_args_t *) ctx;
    mca_rcache_base_registration_t *reg = (mca_rcache_base_registration_t *) data;

    if (args->reg_cnt == args->reg_max) {
        return args->reg_max;
    }

    args->regs[args->reg_cnt++] = reg;

    return OPAL_SUCCESS;
}

int mca_rcache_base_vma_tree_find_all (mca_rcache_base_vma_module_t *vma_module, unsigned char *base,
                                       unsigned char *bound, mca_rcache_base_registration_t **regs,
                                       int reg_cnt)
{
    mca_rcache_base_vma_tree_find_all_helper_args_t args = {.regs = regs, .reg_max = reg_cnt, .reg_cnt = 0};

    (void) opal_interval_tree_traverse (&vma_module->tree, (uint64_t) (uintptr_t) base, ((uint64_t) (uintptr_t) bound) + 1,
                                        true, mca_rcache_base_vma_tree_find_all_helper, &args);
    return args.reg_cnt;
}

struct mca_rcache_base_vma_tree_iterate_helper_args_t {
    int (*callback_fn) (struct mca_rcache_base_registration_t *, void *);
    void *ctx;
};
typedef struct mca_rcache_base_vma_tree_iterate_helper_args_t mca_rcache_base_vma_tree_iterate_helper_args_t;

static int mca_rcache_base_vma_tree_iterate_helper (uint64_t low, uint64_t high, void *data, void *ctx)
{
    mca_rcache_base_vma_tree_iterate_helper_args_t *args = (mca_rcache_base_vma_tree_iterate_helper_args_t *) ctx;
    return args->callback_fn ((mca_rcache_base_registration_t *) data, args->ctx);
}

int mca_rcache_base_vma_tree_iterate (mca_rcache_base_vma_module_t *vma_module, unsigned char *base, size_t size,
                                      bool partial_ok, int (*callback_fn) (struct mca_rcache_base_registration_t *, void *),
                                      void *ctx)
{
    mca_rcache_base_vma_tree_iterate_helper_args_t args = {.callback_fn = callback_fn, .ctx = ctx};
    uintptr_t bound = (uintptr_t) base + size;

    return opal_interval_tree_traverse (&vma_module->tree, (uint64_t) (intptr_t) base, bound, partial_ok,
                                        mca_rcache_base_vma_tree_iterate_helper, &args);
}

int mca_rcache_base_vma_tree_insert (mca_rcache_base_vma_module_t *vma_module,
                                     mca_rcache_base_registration_t *reg, size_t limit)
{
    return opal_interval_tree_insert (&vma_module->tree, reg, (uintptr_t) reg->base, (uintptr_t) reg->bound + 1);
}

/**
 * Function to remove previously memory from the tree without freeing it
 *
 * @param base pointer to the memory to free
 *
 * @retval OPAL_SUCCESS
 * @retval OPAL_ERR_BAD_PARAM if the passed base pointer was invalid
 */
int mca_rcache_base_vma_tree_delete (mca_rcache_base_vma_module_t *vma_module,
                                     mca_rcache_base_registration_t *reg)
{
    return opal_interval_tree_delete (&vma_module->tree, (uintptr_t) reg->base, (uintptr_t) reg->bound + 1, reg);
}

static int mca_rcache_base_tree_dump_range_helper (uint64_t low, uint64_t high, void *data, void *ctx)
{
    mca_rcache_base_registration_t *reg = ( mca_rcache_base_registration_t *) data;

    opal_output(0, "    reg: base=%p, bound=%p, ref_count=%d, flags=0x%x",
                (void *) reg->base, (void *) reg->bound, reg->ref_count, reg->flags);

    return OPAL_SUCCESS;
}

/* Dump out rcache entries within a range of memory.  Useful for debugging. */
void mca_rcache_base_vma_tree_dump_range (mca_rcache_base_vma_module_t *vma_module,
                                          unsigned char *base, size_t size, char *msg)
{
    uintptr_t bound = (uintptr_t) base + size;

    opal_output(0, "Dumping rcache entries: %s", msg ? msg : "");

    if (opal_interval_tree_size (&vma_module->tree)) {
        (void) opal_interval_tree_traverse (&vma_module->tree, (uintptr_t) base, bound, false,
                                            mca_rcache_base_tree_dump_range_helper, NULL);
    } else {
        opal_output(0, "  rcache is empty");
    }
}

size_t mca_rcache_base_vma_tree_size (mca_rcache_base_vma_module_t *vma_module)
{
    return opal_interval_tree_size (&vma_module->tree);
}
