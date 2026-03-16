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
 * Copyright (c) 2012-2015 NVIDIA Corporation.  All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC.  All rights
 *                         reserved.
 *
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/**
 * @file:
 *
 * This memory pool is used for getting the memory handle of remote
 * GPU memory when using CUDA.  Hence, the name is "rgpusm" for "remote
 * CUDA" GPU memory.  There is a cache that can be used to store the
 * remote handles in case they are reused to save on the registration
 * cost as that can be expensive, on the order of 100 usecs.  The
 * cache can also be used just to track how many handles are in use at
 * a time.  It is best to look at this with the three different
 * scenarios that are possible.
 * 1. rcache_rgpusm_leave_pinned=0, cache_size=unlimited
 * 2. rcache_rgpusm_leave_pinned=0, cache_size=limited
 * 3. rcache_rgpusm_leave_pinned=1, cache_size=unlimited (default)
 * 4. rcache_rgpusm_leave_pinned=1, cache_size=limited.
 *
 * Case 1: The cache is unused and remote memory is registered and
 * unregistered for each transaction.  The amount of outstanding
 * registered memory is unlimited.
 * Case 2: The cache keeps track of how much memory is registered at a
 * time.  Since leave pinned is 0, any memory that is registered is in
 * use.  If the amount to register exceeds the amount, we will error
 * out.  This could be handled more gracefully, but this is not a
 * common way to run, so we will leave as is.
 * Case 3: The cache is needed to track current and past transactions.
 * However, there is no limit on the number that can be stored.
 * Therefore, once memory enters the cache, and gets registered, it
 * stays that way forever.
 * Case 4: The cache is needed to track current and past transactions.
 * In addition, a list of most recently used (but no longer in use)
 * registrations is stored so that it can be used to evict
 * registrations from the cache.  In addition, these registrations are
 * deregistered.
 *
 * I also want to capture how we can run into the case where we do not
 * find something in the cache, but when we try to register it, we get
 * an error back from the CUDA library saying the memory is in use.
 * This can happen in the following scenario.  The application mallocs
 * a buffer of size 32K.  The library loads this in the cache and
 * registers it.  The application then frees the buffer.  It then
 * mallocs a buffer of size 64K.  This malloc returns the same base
 * address as the first 32K allocation.  The library searches the
 * cache, but since the size is larger than the original allocation it
 * does not find the registration.  It then attempts to register this.
 * The CUDA library returns an error saying it is already mapped.  To
 * handle this, we return an error of OPAL_ERR_WOULD_BLOCK to the
 * memory pool.  The memory pool then looks for the registration based
 * on the base address and a size of 4.  We use the small size to make
 * sure that we find the registration.  This registration is evicted,
 * and we try to register again.
 */

#define OPAL_DISABLE_ENABLE_MEM_DEBUG 1
#include "opal_config.h"
#include "opal/align.h"
#include "opal/mca/rcache/rgpusm/rcache_rgpusm.h"
#include <errno.h>
#include <string.h>
#ifdef HAVE_MALLOC_H
#include <malloc.h>
#endif
#include "opal/util/proc.h"
#include "opal/mca/rcache/rcache.h"
#include "opal/mca/rcache/base/base.h"
#include "opal/mca/rcache/base/base.h"
#include "opal/mca/common/cuda/common_cuda.h"


static int mca_rcache_rgpusm_deregister_no_lock(struct mca_rcache_base_module_t *,
                                               mca_rcache_base_registration_t *);
static inline bool mca_rcache_rgpusm_deregister_lru (mca_rcache_base_module_t *rcache) {
    mca_rcache_rgpusm_module_t *rcache_rgpusm = (mca_rcache_rgpusm_module_t *) rcache;
    mca_rcache_base_registration_t *old_reg;
    int rc;

    /* Remove the registration from the cache and list before
       deregistering the memory */
    old_reg = (mca_rcache_base_registration_t*)
        opal_list_remove_first (&rcache_rgpusm->lru_list);
    if (NULL == old_reg) {
        opal_output_verbose(10, mca_rcache_rgpusm_component.output,
                            "RGPUSM: The LRU list is empty. There is nothing to deregister");
        return false;
    }

    mca_rcache_base_vma_delete (rcache_rgpusm->vma_module, old_reg);

    /* Drop the rcache lock while we deregister the memory */
    OPAL_THREAD_UNLOCK(&rcache->lock);
    assert(old_reg->ref_count == 0);
    rc = cuda_closememhandle (NULL, old_reg);
    OPAL_THREAD_LOCK(&rcache->lock);

    /* This introduces a potential leak of registrations if
       the deregistration fails to occur as we no longer have
       a reference to it. Is this possible? */
    if (OPAL_SUCCESS != rc) {
        opal_output_verbose(10, mca_rcache_rgpusm_component.output,
                            "RGPUSM: Failed to deregister the memory addr=%p, size=%d",
                            old_reg->base, (int)(old_reg->bound - old_reg->base + 1));
        return false;
    }

    opal_free_list_return (&rcache_rgpusm->reg_list,
                           (opal_free_list_item_t*)old_reg);
    rcache_rgpusm->stat_evicted++;

    return true;
}


/*
 *  Initializes the rcache module.
 */
void mca_rcache_rgpusm_module_init(mca_rcache_rgpusm_module_t* rcache)
{
    rcache->super.rcache_component = &mca_rcache_rgpusm_component.super;
    rcache->super.rcache_register = mca_rcache_rgpusm_register;
    rcache->super.rcache_find = mca_rcache_rgpusm_find;
    rcache->super.rcache_deregister = mca_rcache_rgpusm_deregister;
    rcache->super.rcache_finalize = mca_rcache_rgpusm_finalize;
    rcache->vma_module = mca_rcache_base_vma_module_alloc ();

    OBJ_CONSTRUCT(&rcache->reg_list, opal_free_list_t);
    opal_free_list_init (&rcache->reg_list, sizeof(struct mca_rcache_common_cuda_reg_t),
            opal_cache_line_size,
            OBJ_CLASS(mca_rcache_base_registration_t),
            0,opal_cache_line_size,
            0, -1, 32, NULL, 0, NULL, NULL, NULL);
    OBJ_CONSTRUCT(&rcache->lru_list, opal_list_t);
    rcache->stat_cache_hit = rcache->stat_cache_miss = rcache->stat_evicted = 0;
    rcache->stat_cache_found = rcache->stat_cache_notfound = 0;
    rcache->stat_cache_valid = rcache->stat_cache_invalid = 0;

}

/*
 * This function opens and handle using the handle that was received
 * from the remote memory.  It uses the addr and size of the remote
 * memory for caching the registration.
 */
int mca_rcache_rgpusm_register (mca_rcache_base_module_t *rcache, void *addr,
                               size_t size, uint32_t flags, int32_t access_flags,
                               mca_rcache_base_registration_t **reg)
{
    mca_rcache_rgpusm_module_t *rcache_rgpusm = (mca_rcache_rgpusm_module_t*)rcache;
    mca_rcache_common_cuda_reg_t *rgpusm_reg;
    mca_rcache_common_cuda_reg_t *rget_reg;
    opal_free_list_item_t *item;
    int rc;
    int mypeer;  /* just for debugging */

    /* In order to preserve the signature of the mca_rcache_rgpusm_register
     * function, we are using the **reg variable to not only get back the
     * registration information, but to hand in the memory handle received
     * from the remote side. */
    rget_reg = (mca_rcache_common_cuda_reg_t *)*reg;

    mypeer = flags;
    flags = 0;
    /* No need to support MCA_RCACHE_FLAGS_CACHE_BYPASS in here. It is not used. */
    assert(0 == (flags & MCA_RCACHE_FLAGS_CACHE_BYPASS));

    /* This chunk of code handles the case where leave pinned is not
     * set and we do not use the cache.  This is not typically how we
     * will be running.  This means that one can have an unlimited
     * number of registrations occuring at the same time.  Since we
     * are not leaving the registrations pinned, the number of
     * registrations is unlimited and there is no need for a cache. */
    if(!mca_rcache_rgpusm_component.leave_pinned && 0 == mca_rcache_rgpusm_component.rcache_size_limit) {
        item = opal_free_list_get (&rcache_rgpusm->reg_list);
        if(NULL == item) {
            return OPAL_ERR_OUT_OF_RESOURCE;
        }
        rgpusm_reg = (mca_rcache_common_cuda_reg_t*)item;
        rgpusm_reg->base.rcache = rcache;
        rgpusm_reg->base.base = addr;
        rgpusm_reg->base.bound = (unsigned char *)addr + size - 1;;
        rgpusm_reg->base.flags = flags;

        /* Copy the memory handle received into the registration */
        memcpy(rgpusm_reg->data.memHandle, rget_reg->data.memHandle, sizeof(rget_reg->data.memHandle));

        /* The rget_reg registration is holding the memory handle needed
         * to register the remote memory.  This was received from the remote
         * process.  A pointer to the memory is returned in the alloc_base field. */
        rc = cuda_openmemhandle (addr, size, (mca_rcache_base_registration_t *)rgpusm_reg,
                                 (mca_rcache_base_registration_t *)rget_reg);

        /* This error should not happen with no cache in use. */
        assert(OPAL_ERR_WOULD_BLOCK != rc);

        if(rc != OPAL_SUCCESS) {
            opal_free_list_return (&rcache_rgpusm->reg_list, item);
            return rc;
        }
        rgpusm_reg->base.ref_count++;
        *reg = (mca_rcache_base_registration_t *)rgpusm_reg;
        return OPAL_SUCCESS;
    }

    /* Check to see if memory is registered and stored in the cache. */
    OPAL_THREAD_LOCK(&rcache->lock);
    mca_rcache_base_vma_find (rcache_rgpusm->vma_module, addr, size, reg);

    /* If *reg is not NULL, we have a registration.  Let us see if the
     * memory handle matches the one we were looking for.  If not, the
     * registration is invalid and needs to be removed. This happens
     * if memory was allocated, freed, and allocated again and ends up
     * with the same virtual address and within the limits of the
     * previous registration.  The memory handle check will catch that
     * scenario as the handles have unique serial numbers.  */
    if (*reg != NULL) {
        rcache_rgpusm->stat_cache_hit++;
        opal_output_verbose(10, mca_rcache_rgpusm_component.output,
                            "RGPUSM: Found addr=%p,size=%d (base=%p,size=%d) in cache",
                            addr, (int)size, (*reg)->base,
                            (int)((*reg)->bound - (*reg)->base));

        if (mca_common_cuda_memhandle_matches((mca_rcache_common_cuda_reg_t *)*reg, rget_reg)) {
            /* Registration matches what was requested.  All is good. */
            rcache_rgpusm->stat_cache_valid++;
        } else {
            /* This is an old registration.  Need to boot it. */
            opal_output_verbose(10, mca_rcache_rgpusm_component.output,
                                "RGPUSM: Mismatched Handle: Evicting/unregistering "
                                "addr=%p,size=%d (base=%p,size=%d) from cache",
                                addr, (int)size, (*reg)->base,
                                (int)((*reg)->bound - (*reg)->base));

            /* The ref_count has to be zero as this memory cannot possibly
             * be in use.  Assert on that just to make sure. */
            assert(0 == (*reg)->ref_count);
            if (mca_rcache_rgpusm_component.leave_pinned) {
                opal_list_remove_item(&rcache_rgpusm->lru_list,
                                      (opal_list_item_t*)(*reg));
            }

            /* Bump the reference count to keep things copacetic in deregister */
            (*reg)->ref_count++;
            /* Invalidate the registration so it will get booted out. */
            (*reg)->flags |= MCA_RCACHE_FLAGS_INVALID;
            mca_rcache_rgpusm_deregister_no_lock(rcache, *reg);
            *reg = NULL;
            rcache_rgpusm->stat_cache_invalid++;
        }
    } else {
        /* Nothing was found in the cache. */
        rcache_rgpusm->stat_cache_miss++;
    }

    /* If we have a registration here, then we know it is valid. */
    if (*reg != NULL) {
        opal_output_verbose(10, mca_rcache_rgpusm_component.output,
                            "RGPUSM: CACHE HIT is good: ep=%d, addr=%p, size=%d in cache",
                            mypeer, addr, (int)size);

        /* When using leave pinned, we keep an LRU list. */
        if ((0 == (*reg)->ref_count) && mca_rcache_rgpusm_component.leave_pinned) {
            opal_output_verbose(20, mca_rcache_rgpusm_component.output,
                                "RGPUSM: POP OFF LRU: ep=%d, addr=%p, size=%d in cache",
                                mypeer, addr, (int)size);
            opal_list_remove_item(&rcache_rgpusm->lru_list,
                                  (opal_list_item_t*)(*reg));
        }
        (*reg)->ref_count++;
        OPAL_THREAD_UNLOCK(&rcache->lock);
        opal_output(-1, "reg->ref_count=%d", (int)(*reg)->ref_count);
        opal_output_verbose(80, mca_rcache_rgpusm_component.output,
                           "RGPUSM: Found entry in cache addr=%p, size=%d", addr, (int)size);
        return OPAL_SUCCESS;
    }

    /* If we are here, then we did not find a registration, or it was invalid,
     * so this is a new one, and we are going to use the cache. */
    assert(NULL == *reg);
    opal_output_verbose(10, mca_rcache_rgpusm_component.output,
                        "RGPUSM: New registration ep=%d, addr=%p, size=%d. Need to register and insert in cache",
                         mypeer, addr, (int)size);

    item = opal_free_list_get (&rcache_rgpusm->reg_list);
    if(NULL == item) {
        OPAL_THREAD_UNLOCK(&rcache->lock);
        return OPAL_ERR_OUT_OF_RESOURCE;
    }
    rgpusm_reg = (mca_rcache_common_cuda_reg_t*)item;

    rgpusm_reg->base.rcache = rcache;
    rgpusm_reg->base.base = addr;
    rgpusm_reg->base.bound = (unsigned char *)addr + size - 1;
    rgpusm_reg->base.flags = flags;

    /* Need the memory handle saved in the registration */
    memcpy(rgpusm_reg->data.memHandle, rget_reg->data.memHandle, sizeof(rget_reg->data.memHandle));

    /* Actually register the memory, which opens the memory handle.
     * Need to do this prior to putting in the cache as the base and
     * bound values may be changed by the registration.  The memory
     * associated with the handle comes back in the alloc_base
     * value. */
    rc = cuda_openmemhandle (addr, size, (mca_rcache_base_registration_t *)rgpusm_reg,
                             (mca_rcache_base_registration_t *)rget_reg);
    /* There is a chance we can get the OPAL_ERR_WOULD_BLOCK from the
     * CUDA codes attempt to register the memory.  The case that this
     * can happen is as follows.  A block of memory is registered.
     * Then the sending side frees the memory.  The sending side then
     * cuMemAllocs memory again and gets the same base
     * address. However, it cuMemAllocs a block that is larger than
     * the one in the cache.  The cache will return that memory is not
     * registered and call into CUDA to register it.  However, that
     * will fail with CUDA_ERROR_ALREADY_MAPPED.  Therefore we need to
     * boot that previous allocation out and deregister it first.
     */
    if (OPAL_ERR_WOULD_BLOCK == rc) {
        mca_rcache_base_registration_t *oldreg;

        /* Need to make sure it is at least 4 bytes in size  This will
         * ensure we get the hit in the cache. */
        mca_rcache_base_vma_find (rcache_rgpusm->vma_module, addr, 4, &oldreg);

        /* For most cases, we will find a registration that overlaps.
         * Removal of it should allow the registration we are
         * attempting to succeed. */
        if (NULL != oldreg) {
            /* The ref_count has to be zero as this memory cannot
             * possibly be in use.  Assert on that just to make sure. */
            assert(0 == oldreg->ref_count);
            if (mca_rcache_rgpusm_component.leave_pinned) {
                opal_list_remove_item(&rcache_rgpusm->lru_list,
                                      (opal_list_item_t*)oldreg);
            }

            /* Bump the reference count to keep things copacetic in deregister */
            oldreg->ref_count++;
            /* Invalidate the registration so it will get booted out. */
            oldreg->flags |= MCA_RCACHE_FLAGS_INVALID;
            mca_rcache_rgpusm_deregister_no_lock(rcache, oldreg);
            rcache_rgpusm->stat_evicted++;

            /* And try again.  This one usually works. */
            rc = cuda_openmemhandle (addr, size, (mca_rcache_base_registration_t *)rgpusm_reg,
                                     (mca_rcache_base_registration_t *)rget_reg);
        }

        /* There is a chance that another registration is blocking our
         * ability to register.  Check the rc to see if we still need
         * to try and clear out registrations. */
        while (OPAL_SUCCESS != rc) {
            if (true != mca_rcache_rgpusm_deregister_lru(rcache)) {
                rc = OPAL_ERROR;
                break;
            }
            /* Clear out one registration. */
            rc = cuda_openmemhandle (addr, size, (mca_rcache_base_registration_t *)rgpusm_reg,
                                     (mca_rcache_base_registration_t *)rget_reg);
        }
    }

    if(rc != OPAL_SUCCESS) {
        OPAL_THREAD_UNLOCK(&rcache->lock);
        opal_free_list_return (&rcache_rgpusm->reg_list, item);
        return rc;
    }

    opal_output_verbose(80, mca_rcache_rgpusm_component.output,
                        "RGPUSM: About to insert in rgpusm cache addr=%p, size=%d", addr, (int)size);
    rc = mca_rcache_base_vma_insert (rcache_rgpusm->vma_module, (mca_rcache_base_registration_t *)rgpusm_reg,
                                      mca_rcache_rgpusm_component.rcache_size_limit);
    if (OPAL_ERR_TEMP_OUT_OF_RESOURCE == rc) {
        opal_output_verbose(40, mca_rcache_rgpusm_component.output,
                            "RGPUSM: No room in the cache - boot the first one out");
        (void)mca_rcache_rgpusm_deregister_lru(rcache);
        if (mca_rcache_rgpusm_component.empty_cache) {
            int remNum = 1;
            /* Empty out every registration from LRU until it is empty */
            opal_output_verbose(40, mca_rcache_rgpusm_component.output,
                                "RGPUSM: About to delete all the unused entries in the cache");
            while (mca_rcache_rgpusm_deregister_lru(rcache)) {
                remNum++;
            }
            opal_output_verbose(40, mca_rcache_rgpusm_component.output,
                                "RGPUSM: Deleted and deregistered %d entries", remNum);
            rc = mca_rcache_base_vma_insert (rcache_rgpusm->vma_module, (mca_rcache_base_registration_t *)rgpusm_reg,
                                             mca_rcache_rgpusm_component.rcache_size_limit);
        } else {
            /* Check for room after one removal. If not, remove another one until there is space */
            while((rc = mca_rcache_base_vma_insert (rcache_rgpusm->vma_module, (mca_rcache_base_registration_t *)rgpusm_reg,
                                                    mca_rcache_rgpusm_component.rcache_size_limit)) ==
                  OPAL_ERR_TEMP_OUT_OF_RESOURCE) {
                opal_output_verbose(40, mca_rcache_rgpusm_component.output,
                                    "RGPUSM: No room in the cache - boot one out");
                if (!mca_rcache_rgpusm_deregister_lru(rcache)) {
                    break;
                }
            }
        }
    }

    if(rc != OPAL_SUCCESS) {
        OPAL_THREAD_UNLOCK(&rcache->lock);
        opal_free_list_return (&rcache_rgpusm->reg_list, item);
        /* We cannot recover from this.  We can be here if the size of
         * the cache is smaller than the amount of memory we are
         * trying to register in a single transfer.  In that case, rc
         * is MPI_ERR_OUT_OF_RESOURCES, but everything is stuck at
         * that point.  Therefore, just error out completely.
         */
        opal_output_verbose(10, mca_rcache_rgpusm_component.output,
                            "RGPUSM: Failed to register addr=%p, size=%d", addr, (int)size);
        return OPAL_ERROR;
    }

    rgpusm_reg->base.ref_count++;
    *reg = (mca_rcache_base_registration_t *)rgpusm_reg;
    OPAL_THREAD_UNLOCK(&rcache->lock);

    return OPAL_SUCCESS;
}

int mca_rcache_rgpusm_find(struct mca_rcache_base_module_t *rcache, void *addr,
        size_t size, mca_rcache_base_registration_t **reg)
{
    mca_rcache_rgpusm_module_t *rcache_rgpusm = (mca_rcache_rgpusm_module_t*)rcache;
    int rc;
    unsigned char *base, *bound;

    base = addr;
    bound = base + size - 1; /* To keep cache hits working correctly */

    OPAL_THREAD_LOCK(&rcache->lock);
    opal_output(-1, "Looking for addr=%p, size=%d", addr, (int)size);
    rc = mca_rcache_base_vma_find (rcache_rgpusm->vma_module, addr, size, reg);
    if(*reg != NULL && mca_rcache_rgpusm_component.leave_pinned) {
        if(0 == (*reg)->ref_count && mca_rcache_rgpusm_component.leave_pinned) {
            opal_list_remove_item(&rcache_rgpusm->lru_list, (opal_list_item_t*)(*reg));
        }
        rcache_rgpusm->stat_cache_found++;
        (*reg)->ref_count++;
    } else {
        rcache_rgpusm->stat_cache_notfound++;
    }
    OPAL_THREAD_UNLOCK(&rcache->lock);

    return rc;
}

static inline bool registration_is_cachebale(mca_rcache_base_registration_t *reg)
{
     return !(reg->flags &
             (MCA_RCACHE_FLAGS_CACHE_BYPASS |
              MCA_RCACHE_FLAGS_INVALID));
}

int mca_rcache_rgpusm_deregister(struct mca_rcache_base_module_t *rcache,
                            mca_rcache_base_registration_t *reg)
{
    mca_rcache_rgpusm_module_t *rcache_rgpusm = (mca_rcache_rgpusm_module_t*)rcache;
    int rc = OPAL_SUCCESS;
    assert(reg->ref_count > 0);

    OPAL_THREAD_LOCK(&rcache->lock);
    reg->ref_count--;
    opal_output(-1, "Deregister: reg->ref_count=%d", (int)reg->ref_count);
    if(reg->ref_count > 0) {
        OPAL_THREAD_UNLOCK(&rcache->lock);
        return OPAL_SUCCESS;
    }
    if(mca_rcache_rgpusm_component.leave_pinned && registration_is_cachebale(reg))
    {
        /* if leave_pinned is set don't deregister memory, but put it
         * on LRU list for future use */
        opal_output_verbose(20, mca_rcache_rgpusm_component.output,
                            "RGPUSM: Deregister: addr=%p, size=%d: cacheable and pinned, leave in cache, PUSH IN LRU",
                            reg->base, (int)(reg->bound - reg->base + 1));
        opal_list_prepend(&rcache_rgpusm->lru_list, (opal_list_item_t*)reg);
    } else {
        /* Remove from rcache first */
        if(!(reg->flags & MCA_RCACHE_FLAGS_CACHE_BYPASS))
            mca_rcache_base_vma_delete (rcache_rgpusm->vma_module, reg);

        /* Drop the rcache lock before deregistring the memory */
        OPAL_THREAD_UNLOCK(&rcache->lock);

        {
             assert(reg->ref_count == 0);
             rc = cuda_closememhandle (NULL, reg);
         }

        OPAL_THREAD_LOCK(&rcache->lock);

        if(OPAL_SUCCESS == rc) {
            opal_free_list_return (&rcache_rgpusm->reg_list,
                                   (opal_free_list_item_t*)reg);
        }
    }
    OPAL_THREAD_UNLOCK(&rcache->lock);

    return rc;
}

int mca_rcache_rgpusm_deregister_no_lock(struct mca_rcache_base_module_t *rcache,
                            mca_rcache_base_registration_t *reg)
{
    mca_rcache_rgpusm_module_t *rcache_rgpusm = (mca_rcache_rgpusm_module_t*)rcache;
    int rc = OPAL_SUCCESS;
    assert(reg->ref_count > 0);

    reg->ref_count--;
    opal_output(-1, "Deregister: reg->ref_count=%d", (int)reg->ref_count);
    if(reg->ref_count > 0) {
        return OPAL_SUCCESS;
    }
    if(mca_rcache_rgpusm_component.leave_pinned && registration_is_cachebale(reg))
    {
        /* if leave_pinned is set don't deregister memory, but put it
         * on LRU list for future use */
        opal_list_prepend(&rcache_rgpusm->lru_list, (opal_list_item_t*)reg);
    } else {
        /* Remove from rcache first */
        if(!(reg->flags & MCA_RCACHE_FLAGS_CACHE_BYPASS))
            mca_rcache_base_vma_delete (rcache_rgpusm->vma_module, reg);

        assert(reg->ref_count == 0);
        rc = cuda_closememhandle (NULL, reg);

        if(OPAL_SUCCESS == rc) {
            opal_free_list_return (&rcache_rgpusm->reg_list,
                                   (opal_free_list_item_t*)reg);
        }
    }

    return rc;
}

#define RGPUSM_RCACHE_NREGS 100

void mca_rcache_rgpusm_finalize(struct mca_rcache_base_module_t *rcache)
{
    mca_rcache_rgpusm_module_t *rcache_rgpusm = (mca_rcache_rgpusm_module_t*)rcache;
    mca_rcache_base_registration_t *reg;
    mca_rcache_base_registration_t *regs[RGPUSM_RCACHE_NREGS];
    int reg_cnt, i;
    int rc;

    /* Statistic */
    if(true == mca_rcache_rgpusm_component.print_stats) {
        opal_output(0, "%s rgpusm: stats "
                "(hit/valid/invalid/miss/evicted): %d/%d/%d/%d/%d\n",
                OPAL_NAME_PRINT(OPAL_PROC_MY_NAME),
                rcache_rgpusm->stat_cache_hit, rcache_rgpusm->stat_cache_valid,
                rcache_rgpusm->stat_cache_invalid, rcache_rgpusm->stat_cache_miss,
                rcache_rgpusm->stat_evicted);
    }

    OPAL_THREAD_LOCK(&rcache->lock);
    do {
        reg_cnt = mca_rcache_base_vma_find_all (rcache_rgpusm->vma_module, 0, (size_t)-1,
                regs, RGPUSM_RCACHE_NREGS);
        opal_output(-1, "Registration size at finalize = %d", reg_cnt);

        for(i = 0; i < reg_cnt; i++) {
            reg = regs[i];

            if(reg->ref_count) {
                reg->ref_count = 0; /* otherway dereg will fail on assert */
            } else if (mca_rcache_rgpusm_component.leave_pinned) {
                opal_list_remove_item(&rcache_rgpusm->lru_list,
                        (opal_list_item_t*)reg);
            }

            /* Remove from rcache first */
            mca_rcache_base_vma_delete (rcache_rgpusm->vma_module, reg);

            /* Drop lock before deregistering memory */
            OPAL_THREAD_UNLOCK(&rcache->lock);
            assert(reg->ref_count == 0);
            rc = cuda_closememhandle (NULL, reg);
            OPAL_THREAD_LOCK(&rcache->lock);

            if(rc != OPAL_SUCCESS) {
                /* Potentially lose track of registrations
                   do we have to put it back? */
                continue;
            }

            opal_free_list_return (&rcache_rgpusm->reg_list,
                                   (opal_free_list_item_t *) reg);
        }
    } while(reg_cnt == RGPUSM_RCACHE_NREGS);

    OBJ_DESTRUCT(&rcache_rgpusm->lru_list);
    OBJ_DESTRUCT(&rcache_rgpusm->reg_list);
    OPAL_THREAD_UNLOCK(&rcache->lock);
}
