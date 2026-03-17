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
 * Copyright (c) 2006-2014 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2006      Voltaire. All rights reserved.
 * Copyright (c) 2007      Mellanox Technologies. All rights reserved.
 * Copyright (c) 2010      IBM Corporation.  All rights reserved.
 * Copyright (c) 2011-2018 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2013      NVIDIA Corporation.  All rights reserved.
 * Copyright (c) 2016      Research Organization for Information Science
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

#include <errno.h>
#include <string.h>
#include <stdlib.h>

#include "opal/align.h"

#include "opal/util/proc.h"
#if OPAL_CUDA_GDR_SUPPORT
#include "opal/mca/common/cuda/common_cuda.h"
#endif /* OPAL_CUDA_GDR_SUPPORT */
#include "opal/mca/rcache/rcache.h"
#include "opal/mca/rcache/base/base.h"

#include "opal/util/sys_limits.h"
#include "opal/align.h"
#include "rcache_grdma.h"


static int mca_rcache_grdma_register (mca_rcache_base_module_t *rcache, void *addr,
                                      size_t size, uint32_t flags, int32_t access_flags,
                                      mca_rcache_base_registration_t **reg);
static int mca_rcache_grdma_deregister (mca_rcache_base_module_t *rcache,
                                        mca_rcache_base_registration_t *reg);
static int mca_rcache_grdma_find (mca_rcache_base_module_t *rcache, void *addr,
                                  size_t size, mca_rcache_base_registration_t **reg);
static int mca_rcache_grdma_invalidate_range (mca_rcache_base_module_t *rcache, void *base,
                                              size_t size);
static void mca_rcache_grdma_finalize (mca_rcache_base_module_t *rcache);
static bool mca_rcache_grdma_evict (mca_rcache_base_module_t *rcache);
static int mca_rcache_grdma_add_to_gc (mca_rcache_base_registration_t *grdma_reg);

static inline bool registration_flags_cacheable (uint32_t flags)
{
    return (mca_rcache_grdma_component.leave_pinned &&
            !(flags &
              (MCA_RCACHE_FLAGS_CACHE_BYPASS |
               MCA_RCACHE_FLAGS_PERSIST |
               MCA_RCACHE_FLAGS_INVALID)));
}

static inline bool registration_is_cacheable(mca_rcache_base_registration_t *reg)
{
    return registration_flags_cacheable (reg->flags);
}

#if OPAL_CUDA_GDR_SUPPORT
static int check_for_cuda_freed_memory(mca_rcache_base_module_t *rcache, void *addr, size_t size);
#endif /* OPAL_CUDA_GDR_SUPPORT */
static void mca_rcache_grdma_cache_contructor (mca_rcache_grdma_cache_t *cache)
{
    memset ((void *)((uintptr_t)cache + sizeof (cache->super)), 0, sizeof (*cache) - sizeof (cache->super));

    OBJ_CONSTRUCT(&cache->lru_list, opal_list_t);
    OBJ_CONSTRUCT(&cache->gc_lifo, opal_lifo_t);

    cache->vma_module = mca_rcache_base_vma_module_alloc ();
}

static void mca_rcache_grdma_cache_destructor (mca_rcache_grdma_cache_t *cache)
{
    /* clear the lru before releasing the list */
    while (NULL != opal_list_remove_first (&cache->lru_list));

    OBJ_DESTRUCT(&cache->lru_list);
    OBJ_DESTRUCT(&cache->gc_lifo);
    if (cache->vma_module) {
        OBJ_RELEASE(cache->vma_module);
    }

    free (cache->cache_name);
}

OBJ_CLASS_INSTANCE(mca_rcache_grdma_cache_t, opal_list_item_t,
                   mca_rcache_grdma_cache_contructor,
                   mca_rcache_grdma_cache_destructor);

/*
 *  Initializes the rcache module.
 */
void mca_rcache_grdma_module_init(mca_rcache_grdma_module_t* rcache, mca_rcache_grdma_cache_t *cache)
{
    OBJ_RETAIN(cache);
    rcache->cache = cache;

    rcache->super.rcache_component = &mca_rcache_grdma_component.super;
    rcache->super.rcache_register = mca_rcache_grdma_register;
    rcache->super.rcache_find = mca_rcache_grdma_find;
    rcache->super.rcache_deregister = mca_rcache_grdma_deregister;
    rcache->super.rcache_invalidate_range = mca_rcache_grdma_invalidate_range;
    rcache->super.rcache_finalize = mca_rcache_grdma_finalize;
    rcache->super.rcache_evict = mca_rcache_grdma_evict;

    rcache->stat_cache_hit = rcache->stat_cache_miss = rcache->stat_evicted = 0;
    rcache->stat_cache_found = rcache->stat_cache_notfound = 0;

    OBJ_CONSTRUCT(&rcache->reg_list, opal_free_list_t);
    opal_free_list_init (&rcache->reg_list, rcache->resources.sizeof_reg,
                         opal_cache_line_size,
                         OBJ_CLASS(mca_rcache_base_registration_t),
                         0, opal_cache_line_size, 0, -1, 32, NULL, 0,
                         NULL, NULL, NULL);
}

static inline int dereg_mem(mca_rcache_base_registration_t *reg)
{
    mca_rcache_grdma_module_t *rcache_grdma = (mca_rcache_grdma_module_t *) reg->rcache;
    int rc;

    reg->ref_count = 0;

    if (!(reg->flags & MCA_RCACHE_FLAGS_CACHE_BYPASS)) {
        mca_rcache_base_vma_delete (rcache_grdma->cache->vma_module, reg);
    }

    rc = rcache_grdma->resources.deregister_mem (rcache_grdma->resources.reg_data, reg);
    if (OPAL_LIKELY(OPAL_SUCCESS == rc)) {
        opal_free_list_return_mt (&rcache_grdma->reg_list,
                                  (opal_free_list_item_t *) reg);
    }

    OPAL_OUTPUT_VERBOSE((MCA_BASE_VERBOSE_TRACE, opal_rcache_base_framework.framework_output,
                         "registration %p destroyed", (void *) reg));
    return rc;
}

static inline void do_unregistration_gc (mca_rcache_base_module_t *rcache)
{
    mca_rcache_grdma_module_t *rcache_grdma = (mca_rcache_grdma_module_t *) rcache;
    opal_list_item_t *item;

    /* Remove registration from garbage collection list before deregistering it */
    while (NULL != (item = opal_lifo_pop_atomic (&rcache_grdma->cache->gc_lifo))) {
        OPAL_OUTPUT_VERBOSE((MCA_BASE_VERBOSE_TRACE, opal_rcache_base_framework.framework_output,
                             "deleting stale registration %p", (void *) item));
        dereg_mem ((mca_rcache_base_registration_t *) item);
    }
}
static inline bool mca_rcache_grdma_evict_lru_local (mca_rcache_grdma_cache_t *cache)
{
    mca_rcache_grdma_module_t *rcache_grdma;
    mca_rcache_base_registration_t *old_reg;

    opal_mutex_lock (&cache->vma_module->vma_lock);
    old_reg = (mca_rcache_base_registration_t *)
        opal_list_remove_first (&cache->lru_list);
    if (NULL == old_reg) {
        opal_mutex_unlock (&cache->vma_module->vma_lock);
        return false;
    }

    rcache_grdma = (mca_rcache_grdma_module_t *) old_reg->rcache;

    (void) dereg_mem (old_reg);
    opal_mutex_unlock (&cache->vma_module->vma_lock);

    rcache_grdma->stat_evicted++;

    return true;
}

static bool mca_rcache_grdma_evict (mca_rcache_base_module_t *rcache)
{
    return mca_rcache_grdma_evict_lru_local (((mca_rcache_grdma_module_t *) rcache)->cache);
}

struct mca_rcache_base_find_args_t {
    mca_rcache_base_registration_t *reg;
    mca_rcache_grdma_module_t *rcache_grdma;
    unsigned char *base;
    unsigned char *bound;
    int access_flags;
};

typedef struct mca_rcache_base_find_args_t mca_rcache_base_find_args_t;

static inline void mca_rcache_grdma_add_to_lru (mca_rcache_grdma_module_t *rcache_grdma, mca_rcache_base_registration_t *grdma_reg)
{
    opal_mutex_lock (&rcache_grdma->cache->vma_module->vma_lock);

    opal_list_append(&rcache_grdma->cache->lru_list, (opal_list_item_t *) grdma_reg);

    /* ensure the append is complete before setting the flag */
    opal_atomic_wmb ();

    /* mark this registration as being in the LRU */
    opal_atomic_fetch_or_32 ((volatile int32_t *) &grdma_reg->flags, MCA_RCACHE_GRDMA_REG_FLAG_IN_LRU);

    opal_mutex_unlock (&rcache_grdma->cache->vma_module->vma_lock);
}

static inline void mca_rcache_grdma_remove_from_lru (mca_rcache_grdma_module_t *rcache_grdma, mca_rcache_base_registration_t *grdma_reg)
{
    /* if the reference count was observed to be 0 (which must be the case for this
     * function to be called then some thread deregistered the region. it may be the
     * case that the deregistration is still ongoing so wait until the deregistration
     * thread has marked this registration as being in the lru before continuing */
    while (!(grdma_reg->flags & MCA_RCACHE_GRDMA_REG_FLAG_IN_LRU));

    /* opal lists are not thread safe at this time so we must lock :'( */
    opal_mutex_lock (&rcache_grdma->cache->vma_module->vma_lock);

    opal_list_remove_item (&rcache_grdma->cache->lru_list, (opal_list_item_t *) grdma_reg);
    /* clear the LRU flag */
    grdma_reg->flags &= ~MCA_RCACHE_GRDMA_REG_FLAG_IN_LRU;

    opal_mutex_unlock (&rcache_grdma->cache->vma_module->vma_lock);
}

static int mca_rcache_grdma_check_cached (mca_rcache_base_registration_t *grdma_reg, void *ctx)
{
    mca_rcache_base_find_args_t *args = (mca_rcache_base_find_args_t *) ctx;
    mca_rcache_grdma_module_t *rcache_grdma = args->rcache_grdma;

    if ((grdma_reg->flags & MCA_RCACHE_FLAGS_INVALID) || &rcache_grdma->super != grdma_reg->rcache ||
        grdma_reg->base > args->base || grdma_reg->bound < args->bound) {
        return 0;
    }

    if (OPAL_UNLIKELY((args->access_flags & grdma_reg->access_flags) != args->access_flags)) {
        args->access_flags |= grdma_reg->access_flags;

        /* can't use this registration */
        return mca_rcache_grdma_add_to_gc (grdma_reg);
    }

    int32_t ref_cnt = opal_atomic_fetch_add_32 (&grdma_reg->ref_count, 1);
    args->reg = grdma_reg;

    if (0 == ref_cnt) {
        mca_rcache_grdma_remove_from_lru (rcache_grdma, grdma_reg);
    }

    /* This segment fits fully within an existing segment. */
    (void) opal_atomic_fetch_add_32 ((volatile int32_t *) &rcache_grdma->stat_cache_hit, 1);
    OPAL_OUTPUT_VERBOSE((MCA_BASE_VERBOSE_TRACE, opal_rcache_base_framework.framework_output,
                         "returning existing registration %p. references %d", (void *) grdma_reg, ref_cnt));
    return 1;
}

/*
 * register memory
 */
static int mca_rcache_grdma_register (mca_rcache_base_module_t *rcache, void *addr,
                                      size_t size, uint32_t flags, int32_t access_flags,
                                      mca_rcache_base_registration_t **reg)
{
    mca_rcache_grdma_module_t *rcache_grdma = (mca_rcache_grdma_module_t*)rcache;
    const bool bypass_cache = !!(flags & MCA_RCACHE_FLAGS_CACHE_BYPASS);
    const bool persist = !!(flags & MCA_RCACHE_FLAGS_PERSIST);
    mca_rcache_base_registration_t *grdma_reg;
    opal_free_list_item_t *item;
    unsigned char *base, *bound;
    unsigned int page_size = opal_getpagesize ();
    int rc;

    *reg = NULL;

    /* if cache bypass is requested don't use the cache */
    base = OPAL_DOWN_ALIGN_PTR(addr, page_size, unsigned char *);
    bound = OPAL_ALIGN_PTR((intptr_t) addr + size, page_size, unsigned char *) - 1;

#if OPAL_CUDA_GDR_SUPPORT
    if (flags & MCA_RCACHE_FLAGS_CUDA_GPU_MEM) {
        size_t psize;
        mca_common_cuda_get_address_range(&base, &psize, addr);
        bound = base + psize - 1;
        /* Check to see if this memory is in the cache and if it has been freed. If so,
         * this call will boot it out of the cache. */
        check_for_cuda_freed_memory(rcache, base, psize);
    }
#endif /* OPAL_CUDA_GDR_SUPPORT */

    do_unregistration_gc (rcache);

    /* look through existing regs if not persistent registration requested.
     * Persistent registration are always registered and placed in the cache */
    if (!(bypass_cache || persist)) {
        mca_rcache_base_find_args_t find_args = {.reg = NULL, .rcache_grdma = rcache_grdma,
                                                 .base = base, .bound = bound,
                                                 .access_flags = access_flags};
        /* check to see if memory is registered */
        rc = mca_rcache_base_vma_iterate (rcache_grdma->cache->vma_module, base, size, false,
                                          mca_rcache_grdma_check_cached, (void *) &find_args);
        if (1 == rc) {
            *reg = find_args.reg;
            return OPAL_SUCCESS;
        }

        /* get updated access flags */
        access_flags = find_args.access_flags;

        OPAL_THREAD_ADD_FETCH32((volatile int32_t *) &rcache_grdma->stat_cache_miss, 1);
    }

    item = opal_free_list_get_mt (&rcache_grdma->reg_list);
    if(NULL == item) {
        return OPAL_ERR_OUT_OF_RESOURCE;
    }
    grdma_reg = (mca_rcache_base_registration_t*)item;

    grdma_reg->rcache = rcache;
    grdma_reg->base = base;
    grdma_reg->bound = bound;
    grdma_reg->flags = flags;
    grdma_reg->access_flags = access_flags;
    grdma_reg->ref_count = 1;
#if OPAL_CUDA_GDR_SUPPORT
    if (flags & MCA_RCACHE_FLAGS_CUDA_GPU_MEM) {
        mca_common_cuda_get_buffer_id(grdma_reg);
    }
#endif /* OPAL_CUDA_GDR_SUPPORT */

    while (OPAL_ERR_OUT_OF_RESOURCE ==
           (rc = rcache_grdma->resources.register_mem(rcache_grdma->resources.reg_data,
                                                     base, bound - base + 1, grdma_reg))) {
        /* try to remove one unused reg and retry */
        if (!mca_rcache_grdma_evict (rcache)) {
            break;
        }
    }

    if (OPAL_UNLIKELY(rc != OPAL_SUCCESS)) {
        opal_free_list_return_mt (&rcache_grdma->reg_list, item);
        return rc;
    }

    if (false == bypass_cache) {
        /* Unless explicitly requested by the caller always store the
         * registration in the rcache. This will speed up the case where
         * no leave pinned protocol is in use but the same segment is in
         * use in multiple simultaneous transactions. We used to set bypass_cache
         * here is !mca_rcache_grdma_component.leave_pinned. */
        rc = mca_rcache_base_vma_insert (rcache_grdma->cache->vma_module, grdma_reg, 0);
        if (OPAL_UNLIKELY(rc != OPAL_SUCCESS)) {
            rcache_grdma->resources.deregister_mem (rcache_grdma->resources.reg_data, grdma_reg);
            opal_free_list_return_mt (&rcache_grdma->reg_list, item);
            return rc;
        }
    }

    OPAL_OUTPUT_VERBOSE((MCA_BASE_VERBOSE_TRACE, opal_rcache_base_framework.framework_output,
                         "created new registration %p for region {%p, %p} with flags 0x%x",
                         (void *)grdma_reg, (void*)base, (void*)bound, grdma_reg->flags));

    *reg = grdma_reg;

    return OPAL_SUCCESS;
}

static int mca_rcache_grdma_find (mca_rcache_base_module_t *rcache, void *addr,
                                  size_t size, mca_rcache_base_registration_t **reg)
{
    mca_rcache_grdma_module_t *rcache_grdma = (mca_rcache_grdma_module_t*)rcache;
    unsigned long page_size = opal_getpagesize ();
    unsigned char *base, *bound;
    int rc;

    base = OPAL_DOWN_ALIGN_PTR(addr, page_size, unsigned char *);
    bound = OPAL_ALIGN_PTR((intptr_t) addr + size - 1, page_size, unsigned char *);

    opal_mutex_lock (&rcache_grdma->cache->vma_module->vma_lock);

    rc = mca_rcache_base_vma_find (rcache_grdma->cache->vma_module, base, bound - base + 1, reg);
    if(NULL != *reg &&
            (mca_rcache_grdma_component.leave_pinned ||
             ((*reg)->flags & MCA_RCACHE_FLAGS_PERSIST) ||
             ((*reg)->base == base && (*reg)->bound == bound))) {
        assert(((void*)(*reg)->bound) >= addr);
        if(0 == (*reg)->ref_count &&
                mca_rcache_grdma_component.leave_pinned) {
            opal_list_remove_item(&rcache_grdma->cache->lru_list,
                                  (opal_list_item_t*)(*reg));
        }
        rcache_grdma->stat_cache_found++;
        opal_atomic_add_fetch_32 (&(*reg)->ref_count, 1);
    } else {
        rcache_grdma->stat_cache_notfound++;
    }

    opal_mutex_unlock (&rcache_grdma->cache->vma_module->vma_lock);

    return rc;
}

static int mca_rcache_grdma_deregister (mca_rcache_base_module_t *rcache,
                                        mca_rcache_base_registration_t *reg)
{
    mca_rcache_grdma_module_t *rcache_grdma = (mca_rcache_grdma_module_t *) rcache;
    int32_t ref_count;

    ref_count = opal_atomic_add_fetch_32 (&reg->ref_count, -1);

    OPAL_OUTPUT_VERBOSE((MCA_BASE_VERBOSE_TRACE, opal_rcache_base_framework.framework_output,
                         "returning registration %p, remaining references %d", (void *) reg, ref_count));

    assert (ref_count >= 0);
    if (ref_count > 0) {
        return OPAL_SUCCESS;
    }

    if (registration_is_cacheable(reg)) {
        mca_rcache_grdma_add_to_lru (rcache_grdma, reg);
        return OPAL_SUCCESS;
    }

    return dereg_mem (reg);
}

struct gc_add_args_t {
    void *base;
    size_t size;
};
typedef struct gc_add_args_t gc_add_args_t;

static int mca_rcache_grdma_add_to_gc (mca_rcache_base_registration_t *grdma_reg)
{
    mca_rcache_grdma_module_t *rcache_grdma = (mca_rcache_grdma_module_t *) grdma_reg->rcache;
    uint32_t flags = opal_atomic_fetch_or_32 ((volatile int32_t *) &grdma_reg->flags, MCA_RCACHE_FLAGS_INVALID);

    if ((flags & MCA_RCACHE_FLAGS_INVALID) || 0 != grdma_reg->ref_count) {
        /* nothing to do */
        return OPAL_SUCCESS;
    }

    /* This may be called from free() so avoid recursively calling into free by just
     * shifting this registration into the garbage collection list. The cleanup will
     * be done on the next registration attempt. */
    if (registration_flags_cacheable (flags)) {
        mca_rcache_grdma_remove_from_lru (rcache_grdma, grdma_reg);
    }

    opal_lifo_push_atomic (&rcache_grdma->cache->gc_lifo, (opal_list_item_t *) grdma_reg);

    return OPAL_SUCCESS;
}

static int gc_add (mca_rcache_base_registration_t *grdma_reg, void *ctx)
{
    gc_add_args_t *args = (gc_add_args_t *) ctx;

    if (grdma_reg->flags & MCA_RCACHE_FLAGS_INVALID) {
        /* nothing more to do */
        return OPAL_SUCCESS;
    }

    if (grdma_reg->ref_count && grdma_reg->base == args->base) {
        /* attempted to remove an active registration. to handle cases where part of
         * an active registration has been unmapped we check if the bases match. this
         * *hopefully* will suppress erroneously emitted errors. if we can't suppress
         * the erroneous error in all cases then this check and return should be removed
         * entirely. we are not required to give an error for a user freeing a buffer
         * that is in-use by MPI. Its just a nice to have. */
        return OPAL_ERROR;
    }

    return mca_rcache_grdma_add_to_gc (grdma_reg);
}

static int mca_rcache_grdma_invalidate_range (mca_rcache_base_module_t *rcache,
                                              void *base, size_t size)
{
    mca_rcache_grdma_module_t *rcache_grdma = (mca_rcache_grdma_module_t *) rcache;
    gc_add_args_t args = {.base = base, .size = size};
    return mca_rcache_base_vma_iterate (rcache_grdma->cache->vma_module, base, size, true, gc_add, &args);
}

/* Make sure this registration request is not stale.  In other words, ensure
 * that we do not have a cuMemAlloc, cuMemFree, cuMemAlloc state.  If we do
 * kick out the regisrations and deregister.  This function needs to be called
 * with the rcache->vma_module->vma_lock held. */
#if OPAL_CUDA_GDR_SUPPORT

static int check_for_cuda_freed_memory (mca_rcache_base_module_t *rcache, void *addr, size_t size)
{
    mca_rcache_grdma_module_t *rcache_grdma = (mca_rcache_grdma_module_t *) rcache;
    mca_rcache_base_registration_t *reg;

    mca_rcache_base_vma_find (rcache_grdma->cache->vma_module, addr, size, &reg);
    if (NULL == reg) {
        return OPAL_SUCCESS;
    }

    /* If not previously freed memory, just return 0 */
    if (!(mca_common_cuda_previously_freed_memory(reg))) {
        return OPAL_SUCCESS;
    }

    /* This memory has been freed.  Find all registrations and delete. Ensure they are deregistered
     * now by passing dereg_mem as the delete function. This is safe because the vma lock is
     * recursive and this is only called from register. */
    return mca_rcache_base_vma_iterate (rcache_grdma->cache->vma_module, addr, size, true, gc_add, NULL);
}
#endif /* OPAL_CUDA_GDR_SUPPORT */

static void mca_rcache_grdma_finalize (mca_rcache_base_module_t *rcache)
{
    mca_rcache_grdma_module_t *rcache_grdma = (mca_rcache_grdma_module_t*)rcache;

    /* Statistic */
    if (true == mca_rcache_grdma_component.print_stats) {
        opal_output(0, "%s grdma: stats "
                "(hit/miss/found/not found/evicted/tree size): %d/%d/%d/%d/%d/%ld\n",
                OPAL_NAME_PRINT(OPAL_PROC_MY_NAME),
                rcache_grdma->stat_cache_hit, rcache_grdma->stat_cache_miss,
                rcache_grdma->stat_cache_found, rcache_grdma->stat_cache_notfound,
                    rcache_grdma->stat_evicted, (long) mca_rcache_base_vma_size (rcache_grdma->cache->vma_module));
    }

   do_unregistration_gc (&rcache_grdma->super);

    (void) mca_rcache_base_vma_iterate (rcache_grdma->cache->vma_module, NULL, (size_t) -1, true,
                                        gc_add, (void *) rcache);
    do_unregistration_gc (rcache);

    OBJ_RELEASE(rcache_grdma->cache);

    OBJ_DESTRUCT(&rcache_grdma->reg_list);

    /* this rcache was allocated by grdma_init in rcache_grdma_component.c */
    free(rcache);
}
