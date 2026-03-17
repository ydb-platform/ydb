/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2015      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2016      Intel, Inc. All rights reserved.
 * Copyrigth (c) 2018      Triad National Security, LLC. All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"
#include "opal/align.h"

#include <stdio.h>
#include <stdlib.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif  /* HAVE_UNISTD_H */

#include "opal/mca/mca.h"
#include "opal/mca/base/base.h"
#include "opal/mca/mpool/base/base.h"
#include "opal/constants.h"
#include "opal/util/sys_limits.h"

struct mca_mpool_base_basic_module_t {
    mca_mpool_base_module_t super;
    opal_mutex_t lock;
    uintptr_t ptr;
    size_t size;
    size_t avail;
    unsigned min_align;
};
typedef struct mca_mpool_base_basic_module_t mca_mpool_base_basic_module_t;

static void *mca_mpool_base_basic_alloc (mca_mpool_base_module_t *mpool, size_t size,
                                         size_t align, uint32_t flags)
{
    mca_mpool_base_basic_module_t *basic_module = (mca_mpool_base_basic_module_t *) mpool;
    uintptr_t next_ptr;
    void *ptr;

    opal_mutex_lock (&basic_module->lock);

    align = align > basic_module->min_align ? align : basic_module->min_align;

    next_ptr = OPAL_ALIGN(basic_module->ptr, align, uintptr_t);

    size = OPAL_ALIGN(size, 8, size_t) + next_ptr - basic_module->ptr;

    if (size > basic_module->avail) {
        opal_mutex_unlock (&basic_module->lock);
        return NULL;
    }

    ptr = (void *) next_ptr;
    basic_module->avail -= size;
    basic_module->ptr += size;

    opal_mutex_unlock (&basic_module->lock);
    return ptr;
}

/**
  * free function
  */
static void mca_mpool_base_basic_free (mca_mpool_base_module_t *mpool, void *addr)
{
    /* nothing to do for now */
}

static void mca_mpool_base_basic_finalize (struct mca_mpool_base_module_t *mpool)
{
    mca_mpool_base_basic_module_t *basic_module = (mca_mpool_base_basic_module_t *) mpool;

    OBJ_DESTRUCT(&basic_module->lock);
    free (mpool);
}

static mca_mpool_base_module_t mca_mpool_basic_template = {
    .mpool_alloc = mca_mpool_base_basic_alloc,
    .mpool_free = mca_mpool_base_basic_free,
    .mpool_finalize = mca_mpool_base_basic_finalize,
    .flags = MCA_MPOOL_FLAGS_MPI_ALLOC_MEM,
};

mca_mpool_base_module_t *mca_mpool_basic_create (void *base, size_t size, unsigned min_align)
{
    mca_mpool_base_basic_module_t *basic_module = calloc (1, sizeof (*basic_module));

    if (OPAL_UNLIKELY(NULL == basic_module)) {
        return NULL;
    }

    memcpy (&basic_module->super, &mca_mpool_basic_template, sizeof (mca_mpool_basic_template));

    OBJ_CONSTRUCT(&basic_module->lock, opal_mutex_t);

    basic_module->super.mpool_base = base;
    basic_module->ptr = (uintptr_t) base;
    basic_module->size = basic_module->avail = size;
    basic_module->min_align = min_align;

    return &basic_module->super;
}
