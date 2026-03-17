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
 * Copyright (c) 2006-2009 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2006      Voltaire. All rights reserved.
 * Copyright (c) 2007      Mellanox Technologies. All rights reserved.
 * Copyright (c) 2010      IBM Corporation.  All rights reserved.
 * Copyright (c) 2011-2016 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2017      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 *
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#define OPAL_DISABLE_ENABLE_MEM_DEBUG 1
#include "opal_config.h"
#include "opal/align.h"
#include "mpool_hugepage.h"
#include <errno.h>
#include <string.h>
#ifdef HAVE_MALLOC_H
#include <malloc.h>
#endif
#include "opal/mca/mpool/base/base.h"
#include "opal/runtime/opal_params.h"
#include "opal/include/opal_stdint.h"
#include "opal/mca/allocator/base/base.h"

#include <fcntl.h>
#include <sys/mman.h>


static void *mca_mpool_hugepage_alloc (mca_mpool_base_module_t *mpool, size_t size, size_t align,
                                   uint32_t flags);
static void *mca_mpool_hugepage_realloc (mca_mpool_base_module_t *mpool, void *addr, size_t size);
static void mca_mpool_hugepage_free (mca_mpool_base_module_t *mpool, void *addr);
static void mca_mpool_hugepage_finalize (mca_mpool_base_module_t *mpool);
static int mca_mpool_hugepage_ft_event (int state);

static void mca_mpool_hugepage_hugepage_constructor (mca_mpool_hugepage_hugepage_t *huge_page)
{
    memset ((char *)huge_page + sizeof(huge_page->super), 0, sizeof (*huge_page) - sizeof (huge_page->super));
}

static void mca_mpool_hugepage_hugepage_destructor (mca_mpool_hugepage_hugepage_t *huge_page)
{
    free (huge_page->path);
}

OBJ_CLASS_INSTANCE(mca_mpool_hugepage_hugepage_t, opal_list_item_t,
                   mca_mpool_hugepage_hugepage_constructor,
                   mca_mpool_hugepage_hugepage_destructor);

static int mca_mpool_rb_hugepage_compare (void *key1, void *key2)
{
    if (key1 == key2) {
        return 0;
    }

    return (key1 < key2) ? -1 : 1;
}

/*
 *  Initializes the mpool module.
 */
int mca_mpool_hugepage_module_init(mca_mpool_hugepage_module_t *mpool,
                                  mca_mpool_hugepage_hugepage_t *huge_page)
{
    mca_allocator_base_component_t *allocator_component;
    int rc;

    mpool->super.mpool_component = &mca_mpool_hugepage_component.super;
    mpool->super.mpool_base = NULL; /* no base .. */
    mpool->super.mpool_alloc = mca_mpool_hugepage_alloc;
    mpool->super.mpool_realloc = mca_mpool_hugepage_realloc;
    mpool->super.mpool_free = mca_mpool_hugepage_free;
    mpool->super.mpool_finalize = mca_mpool_hugepage_finalize;
    mpool->super.mpool_ft_event = mca_mpool_hugepage_ft_event;
    mpool->super.flags = MCA_MPOOL_FLAGS_MPI_ALLOC_MEM;

    OBJ_CONSTRUCT(&mpool->lock, opal_mutex_t);

    mpool->huge_page = huge_page;

    /* use an allocator component to reduce waste when making small allocations */
    allocator_component = mca_allocator_component_lookup ("bucket");
    if (NULL == allocator_component) {
        return OPAL_ERR_NOT_AVAILABLE;
    }

    mpool->allocator = allocator_component->allocator_init (true, mca_mpool_hugepage_seg_alloc,
                                                            mca_mpool_hugepage_seg_free, mpool);

    OBJ_CONSTRUCT(&mpool->allocation_tree, opal_rb_tree_t);
    rc = opal_rb_tree_init (&mpool->allocation_tree, mca_mpool_rb_hugepage_compare);
    if (OPAL_SUCCESS != rc) {
        OBJ_DESTRUCT(&mpool->allocation_tree);
        return OPAL_ERR_NOT_AVAILABLE;
    }

    return OPAL_SUCCESS;
}

void *mca_mpool_hugepage_seg_alloc (void *ctx, size_t *sizep)
{
    mca_mpool_hugepage_module_t *hugepage_module = (mca_mpool_hugepage_module_t *) ctx;
    mca_mpool_hugepage_hugepage_t *huge_page = hugepage_module->huge_page;
    size_t size = *sizep;
    void *base = NULL;
    char *path = NULL;
    int flags = MAP_PRIVATE;
    int fd = -1;
    int rc;

    size = OPAL_ALIGN(size, huge_page->page_size, size_t);

    if (huge_page->path) {
        int32_t count;

        count = opal_atomic_add_fetch_32 (&huge_page->count, 1);

        rc = asprintf (&path, "%s/hugepage.openmpi.%d.%d", huge_page->path,
                       getpid (), count);
        if (0 > rc) {
            return NULL;
        }

        fd = open (path, O_RDWR | O_CREAT, 0600);
        if (-1 == fd) {
            free (path);
            return NULL;
        }

        if (0 != ftruncate (fd, size)) {
            close (fd);
            unlink (path);
            free (path);
            return NULL;
        }
    } else {
#if defined(MAP_ANONYMOUS)
        flags |= MAP_ANONYMOUS;
#elif defined(MAP_ANON)
        /* older versions of OS X do not define MAP_ANONYMOUS (10.9.x and older) */
        flags |= MAP_ANON;
#endif
    }

    base = mmap (NULL, size, PROT_READ | PROT_WRITE, flags | huge_page->mmap_flags, fd, 0);
    if (path) {
        unlink (path);
        free (path);
    }

    if (fd >= 0) {
        close (fd);
    }

    if (MAP_FAILED == base) {
        opal_output_verbose (MCA_BASE_VERBOSE_WARN, opal_mpool_base_framework.framework_verbose,
                             "could not allocate huge page(s). falling back on standard pages");
        /* fall back on regular pages */
        base = mmap (NULL, size, PROT_READ | PROT_WRITE, flags, -1, 0);
    }

    if (MAP_FAILED == base) {
        return NULL;
    }

    opal_mutex_lock (&hugepage_module->lock);
    opal_rb_tree_insert (&hugepage_module->allocation_tree, base, (void *) (intptr_t) size);
    opal_atomic_add (&mca_mpool_hugepage_component.bytes_allocated, (int64_t) size);
    opal_mutex_unlock (&hugepage_module->lock);

    OPAL_OUTPUT_VERBOSE((MCA_BASE_VERBOSE_TRACE, opal_mpool_base_framework.framework_verbose,
                         "allocated segment %p of size %lu bytes", base, size));

    *sizep = size;

    return base;
}

void mca_mpool_hugepage_seg_free (void *ctx, void *addr)
{
    mca_mpool_hugepage_module_t *hugepage_module = (mca_mpool_hugepage_module_t *) ctx;
    size_t size;

    opal_mutex_lock (&hugepage_module->lock);

    size = (size_t) (intptr_t) opal_rb_tree_find (&hugepage_module->allocation_tree, addr);
    if (size > 0) {
        opal_rb_tree_delete (&hugepage_module->allocation_tree, addr);
        OPAL_OUTPUT_VERBOSE((MCA_BASE_VERBOSE_TRACE, opal_mpool_base_framework.framework_verbose,
                             "freeing segment %p of size %lu bytes", addr, size));
        munmap (addr, size);
        opal_atomic_add (&mca_mpool_hugepage_component.bytes_allocated, -(int64_t) size);
    }

    opal_mutex_unlock (&hugepage_module->lock);
}

/**
  * allocate function
  */
static void *mca_mpool_hugepage_alloc (mca_mpool_base_module_t *mpool, size_t size,
                                       size_t align, uint32_t flags)
{
    mca_mpool_hugepage_module_t *hugepage_module = (mca_mpool_hugepage_module_t *) mpool;
    return hugepage_module->allocator->alc_alloc (hugepage_module->allocator, size, align);
}

/**
  * allocate function
  */
static void *mca_mpool_hugepage_realloc (mca_mpool_base_module_t *mpool, void *addr, size_t size)
{
    mca_mpool_hugepage_module_t *hugepage_module = (mca_mpool_hugepage_module_t *) mpool;

    return hugepage_module->allocator->alc_realloc (hugepage_module->allocator, addr, size);
}

/**
  * free function
  */
static void mca_mpool_hugepage_free (mca_mpool_base_module_t *mpool, void *addr)
{
    mca_mpool_hugepage_module_t *hugepage_module = (mca_mpool_hugepage_module_t *) mpool;

    hugepage_module->allocator->alc_free (hugepage_module->allocator, addr);
}

static void mca_mpool_hugepage_finalize (struct mca_mpool_base_module_t *mpool)
{
    mca_mpool_hugepage_module_t *hugepage_module = (mca_mpool_hugepage_module_t *) mpool;

    OBJ_DESTRUCT(&hugepage_module->lock);
    OBJ_DESTRUCT(&hugepage_module->allocation_tree);

    if (hugepage_module->allocator) {
        (void) hugepage_module->allocator->alc_finalize (hugepage_module->allocator);
        hugepage_module->allocator = NULL;
    }
}

static int mca_mpool_hugepage_ft_event (int state) {
    return OPAL_SUCCESS;
}
