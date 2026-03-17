/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2015      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2016      Intel, Inc. All rights reserved.
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

static void *mca_mpool_default_alloc (mca_mpool_base_module_t *mpool, size_t size,
                                      size_t align, uint32_t flags)
{
#if HAVE_POSIX_MEMALIGN
    void *addr = NULL;

    if (align <= sizeof(void *)) {
        addr = malloc (size);
    } else {
        (void) posix_memalign (&addr, align, size);
    }
    return addr;
#else
    void *addr, *ret;

    addr = malloc (size + align + sizeof (void *));
    ret = OPAL_ALIGN_PTR((intptr_t) addr + 8, align, void *);
    *((void **) ret - 1) = addr;
    return ret;
#endif
}

static void *mca_mpool_default_realloc (mca_mpool_base_module_t *mpool, void *addr, size_t size)
{
#if HAVE_POSIX_MEMALIGN
    return realloc (addr, size);
#else
    if (NULL != addr) {
        void *base = *((void **) addr - 1);
        void *ptr = realloc (base, size + (intptr_t) addr - (intptr_t) - size);
        void *ret = (void *)((intptr_t) ptr + (intptr_t) addr - (intptr_t) - size);
        *((void **) ret - 1) = ptr;
        return ret;
    } else {
        return mca_mpool_default_alloc (mpool, size, 8, 0);
    }
#endif
}

static void mca_mpool_default_free (mca_mpool_base_module_t *mpool, void *addr)
{
#if HAVE_POSIX_MEMALIGN
    free (addr);
#else
    if (NULL != addr) {
        void *base = *((void **) addr - 1);
        free (base);
    }
#endif
}

static void mca_mpool_default_finalize (struct mca_mpool_base_module_t *mpool)
{
}

static mca_mpool_base_module_t mca_mpool_malloc_module = {
    .mpool_alloc = mca_mpool_default_alloc,
    .mpool_realloc = mca_mpool_default_realloc,
    .mpool_free = mca_mpool_default_free,
    .mpool_finalize = mca_mpool_default_finalize,
    .flags = MCA_MPOOL_FLAGS_MPI_ALLOC_MEM,
};

mca_mpool_base_module_t *mca_mpool_base_default_module = &mca_mpool_malloc_module;
