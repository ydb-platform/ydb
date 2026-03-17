/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2009 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2006-2007 Mellanox Technologies. All rights reserved.
 * Copyright (c) 2010-2013 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2011      NVIDIA Corporation.  All rights reserved.
 * Copyright (c) 2012-2018 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"

#include "opal/class/opal_free_list.h"
#include "opal/align.h"
#include "opal/util/output.h"
#include "opal/mca/mpool/mpool.h"
#include "opal/mca/mpool/base/base.h"
#include "opal/mca/rcache/rcache.h"
#include "opal/util/sys_limits.h"

typedef struct opal_free_list_item_t opal_free_list_memory_t;

OBJ_CLASS_INSTANCE(opal_free_list_item_t,
                   opal_list_item_t,
                   NULL, NULL);

static void opal_free_list_construct(opal_free_list_t* fl)
{
    OBJ_CONSTRUCT(&fl->fl_lock, opal_mutex_t);
    OBJ_CONSTRUCT(&fl->fl_condition, opal_condition_t);
    fl->fl_max_to_alloc = 0;
    fl->fl_num_allocated = 0;
    fl->fl_num_per_alloc = 0;
    fl->fl_num_waiting = 0;
    fl->fl_frag_size = sizeof(opal_free_list_item_t);
    fl->fl_frag_alignment = 0;
    fl->fl_payload_buffer_size = 0;
    fl->fl_payload_buffer_alignment = 0;
    fl->fl_frag_class = OBJ_CLASS(opal_free_list_item_t);
    fl->fl_mpool = NULL;
    fl->fl_rcache = NULL;
    /* default flags */
    fl->fl_rcache_reg_flags = MCA_RCACHE_FLAGS_CACHE_BYPASS |
        MCA_RCACHE_FLAGS_CUDA_REGISTER_MEM;
    fl->ctx = NULL;
    OBJ_CONSTRUCT(&(fl->fl_allocations), opal_list_t);
}

static void opal_free_list_allocation_release (opal_free_list_t *fl, opal_free_list_memory_t *fl_mem)
{
    if (NULL != fl->fl_rcache) {
        fl->fl_rcache->rcache_deregister (fl->fl_rcache, fl_mem->registration);
    }

    if (NULL != fl->fl_mpool) {
        fl->fl_mpool->mpool_free (fl->fl_mpool, fl_mem->ptr);
    } else if (fl_mem->ptr) {
        free (fl_mem->ptr);
    }

    /* destruct the item (we constructed it), then free the memory chunk */
    OBJ_DESTRUCT(fl_mem);
    free(fl_mem);
}

static void opal_free_list_destruct(opal_free_list_t *fl)
{
    opal_list_item_t *item;
    opal_free_list_item_t *fl_item;

#if 0 && OPAL_ENABLE_DEBUG
    if(opal_list_get_size(&fl->super) != fl->fl_num_allocated) {
        opal_output(0, "opal_free_list: %d allocated %d returned: %s:%d\n",
            fl->fl_num_allocated, opal_list_get_size(&fl->super),
            fl->super.super.cls_init_file_name, fl->super.super.cls_init_lineno);
    }
#endif

    while(NULL != (item = opal_lifo_pop(&(fl->super)))) {
        fl_item = (opal_free_list_item_t*)item;

        /* destruct the item (we constructed it), the underlying memory will be
         * reclaimed when we free the slab (opal_free_list_memory_t ptr)
         * containing it */
        OBJ_DESTRUCT(fl_item);
    }

    while(NULL != (item = opal_list_remove_first(&fl->fl_allocations))) {
        opal_free_list_allocation_release (fl, (opal_free_list_memory_t *) item);
    }

    OBJ_DESTRUCT(&fl->fl_allocations);
    OBJ_DESTRUCT(&fl->fl_condition);
    OBJ_DESTRUCT(&fl->fl_lock);
}

OBJ_CLASS_INSTANCE(opal_free_list_t, opal_lifo_t, opal_free_list_construct,
                   opal_free_list_destruct);


int opal_free_list_init (opal_free_list_t *flist, size_t frag_size, size_t frag_alignment,
                         opal_class_t *frag_class, size_t payload_buffer_size,
                         size_t payload_buffer_alignment, int num_elements_to_alloc,
                         int max_elements_to_alloc, int num_elements_per_alloc,
                         mca_mpool_base_module_t *mpool, int rcache_reg_flags,
                         mca_rcache_base_module_t *rcache, opal_free_list_item_init_fn_t item_init,
                         void *ctx)
{
    /* alignment must be more than zero and power of two */
    if (frag_alignment <= 1 || (frag_alignment & (frag_alignment - 1))) {
        return OPAL_ERROR;
    }

    if (0 < payload_buffer_size) {
        if (payload_buffer_alignment <= 1 || (payload_buffer_alignment & (payload_buffer_alignment - 1)))
            return OPAL_ERROR;
    }

    if (frag_class && frag_size < frag_class->cls_sizeof) {
        frag_size = frag_class->cls_sizeof;
    }

    if (frag_size > flist->fl_frag_size) {
        flist->fl_frag_size = frag_size;
    }

    if (frag_class) {
        flist->fl_frag_class = frag_class;
    }

    flist->fl_payload_buffer_size = payload_buffer_size;
    flist->fl_max_to_alloc = max_elements_to_alloc;
    flist->fl_num_allocated = 0;
    flist->fl_num_per_alloc = num_elements_per_alloc;
    flist->fl_mpool = mpool ? mpool : mca_mpool_base_default_module;
    flist->fl_rcache = rcache;
    flist->fl_frag_alignment = frag_alignment;
    flist->fl_payload_buffer_alignment = payload_buffer_alignment;
    flist->item_init = item_init;
    flist->fl_rcache_reg_flags |= rcache_reg_flags;
    flist->ctx = ctx;

    if (num_elements_to_alloc) {
        return opal_free_list_grow_st (flist, num_elements_to_alloc, NULL);
    }

    return OPAL_SUCCESS;
}

int opal_free_list_grow_st (opal_free_list_t* flist, size_t num_elements, opal_free_list_item_t **item_out)
{
    unsigned char *ptr, *payload_ptr = NULL;
    opal_free_list_memory_t *alloc_ptr;
    size_t alloc_size, head_size, elem_size = 0, buffer_size = 0, align = 0;
    mca_rcache_base_registration_t *reg = NULL;
    int rc = OPAL_SUCCESS;

    if (flist->fl_max_to_alloc && (flist->fl_num_allocated + num_elements) >
        flist->fl_max_to_alloc) {
        num_elements = flist->fl_max_to_alloc - flist->fl_num_allocated;
    }

    if (num_elements == 0) {
        return OPAL_ERR_TEMP_OUT_OF_RESOURCE;
    }

    head_size = OPAL_ALIGN(flist->fl_frag_size, flist->fl_frag_alignment, size_t);

    /* NTH: calculate allocation alignment first as it might change the number of elements */
    if (0 != flist->fl_payload_buffer_size) {
        elem_size = OPAL_ALIGN(flist->fl_payload_buffer_size,
                               flist->fl_payload_buffer_alignment, size_t);

        /* elem_size should not be 0 here */
        assert (elem_size > 0);

        buffer_size = num_elements * elem_size;
        align = flist->fl_payload_buffer_alignment;

        if (MCA_RCACHE_FLAGS_CUDA_REGISTER_MEM & flist->fl_rcache_reg_flags) {
            size_t pagesize = opal_getpagesize ();
            /* CUDA cannot handle registering overlapping regions, so make
             * sure each region is page sized and page aligned. */
            align = OPAL_ALIGN(align, pagesize, size_t);
            buffer_size = OPAL_ALIGN(buffer_size, pagesize, size_t);

            /* avoid wasting space in the buffer */
            num_elements = buffer_size / elem_size;
        }
    }

    /* calculate head allocation size */
    alloc_size = num_elements * head_size + sizeof(opal_free_list_memory_t) +
        flist->fl_frag_alignment;

    alloc_ptr = (opal_free_list_memory_t *) malloc(alloc_size);
    if (OPAL_UNLIKELY(NULL == alloc_ptr)) {
        return OPAL_ERR_TEMP_OUT_OF_RESOURCE;
    }

    if (0 != flist->fl_payload_buffer_size) {
        /* allocate the rest from the mpool (or use memalign/malloc) */
        payload_ptr = (unsigned char *) flist->fl_mpool->mpool_alloc(flist->fl_mpool, buffer_size, align, 0);
        if (NULL == payload_ptr) {
            free(alloc_ptr);
            return OPAL_ERR_TEMP_OUT_OF_RESOURCE;
        }

        if (flist->fl_rcache) {
            rc = flist->fl_rcache->rcache_register (flist->fl_rcache, payload_ptr, num_elements * elem_size,
                                                    flist->fl_rcache_reg_flags, MCA_RCACHE_ACCESS_ANY, &reg);
            if (OPAL_UNLIKELY(OPAL_SUCCESS != rc)) {
                free (alloc_ptr);
                flist->fl_mpool->mpool_free (flist->fl_mpool, payload_ptr);

                return rc;
            }
        }
    }

    /* make the alloc_ptr a list item, save the chunk in the allocations list,
     * and have ptr point to memory right after the list item structure */
    OBJ_CONSTRUCT(alloc_ptr, opal_free_list_item_t);
    opal_list_append(&(flist->fl_allocations), (opal_list_item_t*)alloc_ptr);

    alloc_ptr->registration = reg;
    alloc_ptr->ptr = payload_ptr;

    ptr = (unsigned char*)alloc_ptr + sizeof(opal_free_list_memory_t);
    ptr = OPAL_ALIGN_PTR(ptr, flist->fl_frag_alignment, unsigned char*);

    for(size_t i = 0; i < num_elements ; ++i) {
        opal_free_list_item_t* item = (opal_free_list_item_t*)ptr;
        item->registration = reg;
        item->ptr = payload_ptr;

        OBJ_CONSTRUCT_INTERNAL(item, flist->fl_frag_class);
        item->super.item_free = 0;

        /* run the initialize function if present */
        if (flist->item_init) {
            if (OPAL_SUCCESS != (rc = flist->item_init(item, flist->ctx))) {
                num_elements = i;
                OBJ_DESTRUCT (item);
                break;
            }
        }

        /* NTH: in case the free list may be accessed from multiple threads
         * use the atomic lifo push. The overhead is small compared to the
         * overall overhead of opal_free_list_grow(). */
        if (item_out && 0 == i) {
            /* ensure the thread that is growing the free list always gets an item
             * if one is available */
            *item_out = item;
        } else {
            opal_lifo_push_atomic (&flist->super, &item->super);
        }

        ptr += head_size;
        payload_ptr += elem_size;
    }

    if (OPAL_SUCCESS != rc && 0 == num_elements) {
        /* couldn't initialize any items */
        opal_list_remove_item (&flist->fl_allocations, (opal_list_item_t *) alloc_ptr);
        opal_free_list_allocation_release (flist, alloc_ptr);
        return OPAL_ERR_OUT_OF_RESOURCE;
    }

    flist->fl_num_allocated += num_elements;
    return OPAL_SUCCESS;
}

/**
 * This function resize the free_list to contain at least the specified
 * number of elements. We do not create all of them in the same memory
 * segment. Instead we will several time the fl_num_per_alloc elements
 * until we reach the required number of the maximum allowed by the
 * initialization.
 */
int opal_free_list_resize_mt(opal_free_list_t *flist, size_t size)
{
    ssize_t inc_num;
    int ret = OPAL_SUCCESS;

    if (flist->fl_num_allocated > size) {
        return OPAL_SUCCESS;
    }

    opal_mutex_lock (&flist->fl_lock);
    do {
        ret = opal_free_list_grow_st (flist, flist->fl_num_per_alloc, NULL);
        if (OPAL_SUCCESS != ret) {
            break;
        }

        inc_num = (ssize_t)size - (ssize_t)flist->fl_num_allocated;
    } while (inc_num > 0);
    opal_mutex_unlock (&flist->fl_lock);

    return ret;
}
