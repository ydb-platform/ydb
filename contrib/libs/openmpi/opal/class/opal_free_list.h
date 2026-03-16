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
 * Copyright (c) 2010      IBM Corporation.  All rights reserved.
 * Copyright (c) 2010-2015 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2014-2018 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef OPAL_FREE_LIST_H
#define OPAL_FREE_LIST_H

#include "opal_config.h"
#include "opal/class/opal_lifo.h"
#include "opal/prefetch.h"
#include "opal/threads/condition.h"
#include "opal/constants.h"
#include "opal/runtime/opal.h"

BEGIN_C_DECLS

struct mca_mem_pool_t;
struct opal_free_list_item_t;

/**
 * Free list item initializtion function.
 *
 * @param item   (IN)  Free list item to initialize
 * @param ctx    (IN)  Free list initialization context
 *
 * @returns OPAL_SUCCESS on success
 * @returns opal error code on failure
 *
 * This function attempts to initialize the free list item
 * specified in item. If the item can be initialized the
 * function should return OPAL_SUCCESS. On any error
 * opal_free_list_grow will stop initializing new items.
 */
typedef int (*opal_free_list_item_init_fn_t) (
        struct opal_free_list_item_t *item, void *ctx);

struct opal_free_list_t {
    /** Items in a free list are stored last-in first-out */
    opal_lifo_t super;
    /** Maximum number of items to allocate in the free list */
    size_t fl_max_to_alloc;
    /** Current number of items allocated */
    size_t fl_num_allocated;
    /** Number of items to allocate when growing the free list */
    size_t fl_num_per_alloc;
    /** Number of threads waiting on free list item availability */
    size_t fl_num_waiting;
    /** Size of each free list item */
    size_t fl_frag_size;
    /** Free list item alignment */
    size_t fl_frag_alignment;
    /** Free list item buffer size */
    size_t fl_payload_buffer_size;
    /** Free list item buffer alignment */
    size_t fl_payload_buffer_alignment;
    /** Class of free list items */
    opal_class_t *fl_frag_class;
    /** mpool to use for free list buffer allocation (posix_memalign/malloc
     * are used if this is NULL) */
    struct mca_mpool_base_module_t *fl_mpool;
    /** registration cache */
    struct mca_rcache_base_module_t *fl_rcache;
    /** Multi-threaded lock. Used when the free list is empty. */
    opal_mutex_t fl_lock;
    /** Multi-threaded condition. Used when threads are waiting on free
     * list item availability. */
    opal_condition_t fl_condition;
    /** List of free list allocation */
    opal_list_t fl_allocations;
    /** Flags to pass to the rcache register function */
    int fl_rcache_reg_flags;
    /** Free list item initialization function */
    opal_free_list_item_init_fn_t item_init;
    /** Initialization function context */
    void *ctx;
};
typedef struct opal_free_list_t opal_free_list_t;
OPAL_DECLSPEC OBJ_CLASS_DECLARATION(opal_free_list_t);

struct mca_mpool_base_registration_t;
struct opal_free_list_item_t
{
    opal_list_item_t super;
    struct mca_rcache_base_registration_t *registration;
    void *ptr;
};
typedef struct opal_free_list_item_t opal_free_list_item_t;
OPAL_DECLSPEC OBJ_CLASS_DECLARATION(opal_free_list_item_t);


/**
 * Initialize a free list.
 *
 * @param free_list                (IN)  Free list.
 * @param frag_size                (IN)  Size of each element - allocated by malloc.
 * @param frag_alignment           (IN)  Fragment alignment.
 * @param frag_class               (IN)  opal_class_t of element - used to initialize allocated elements.
 * @param payload_buffer_size      (IN)  Size of payload buffer - allocated from mpool.
 * @param payload_buffer_alignment (IN)  Payload buffer alignment.
 * @param num_elements_to_alloc    (IN)  Initial number of elements to allocate.
 * @param max_elements_to_alloc    (IN)  Maximum number of elements to allocate.
 * @param num_elements_per_alloc   (IN)  Number of elements to grow by per allocation.
 * @param mpool                    (IN)  Optional memory pool for allocations.
 * @param rcache_reg_flags         (IN)  Flags to pass to rcache registration function.
 * @param rcache                   (IN)  Optional registration cache.
 * @param item_init                (IN)  Optional item initialization function
 * @param ctx                      (IN)  Initialization function context.
 */

OPAL_DECLSPEC int opal_free_list_init (opal_free_list_t *free_list,
                                       size_t frag_size,
                                       size_t frag_alignment,
                                       opal_class_t* frag_class,
                                       size_t payload_buffer_size,
                                       size_t payload_buffer_alignment,
                                       int num_elements_to_alloc,
                                       int max_elements_to_alloc,
                                       int num_elements_per_alloc,
                                       struct mca_mpool_base_module_t *mpool,
                                       int rcache_reg_flags,
                                       struct mca_rcache_base_module_t *rcache,
                                       opal_free_list_item_init_fn_t item_init,
                                       void *ctx);

/**
 * Grow the free list by at most num_elements elements.
 *
 * @param flist         (IN)   Free list to grow
 * @param num_elements  (IN)   Number of elements to add
 * @param item_out      (OUT)  Location to store new free list item (can be NULL)
 *
 * @returns OPAL_SUCCESS if any elements were added
 * @returns OPAL_ERR_OUT_OF_RESOURCE if no elements could be added
 *
 * This function will attempt to grow the free list by num_elements items. The
 * caller must hold the free list lock if calling this function on a free list
 * that may be accessed by multiple threads simultaneously. Note: this is an
 * internal function that will be used when needed by opal_free_list_get* and
 * opal_free_list_wait*.
 *
 * The item_out parameter can be used to ensure that the thread calling this
 * function always gets a free list item if the list is successfully grown.
 * This eliminates a race condition with code that simply calls free_list_get
 * and assumes NULL is an out of memory condition (which it wasn't necessarily
 * before this parameter was added).
 */
OPAL_DECLSPEC int opal_free_list_grow_st (opal_free_list_t *flist, size_t num_elements, opal_free_list_item_t **item_out);

/**
 * Grow the free list to be at least size elements.
 *
 * @param flist    (IN)   Free list to resize.
 * @param size     (IN)   New size
 *
 * @returns OPAL_SUCCESS if the free list was resized
 * @returns OPAL_ERR_OUT_OF_RESOURCE if resources could not be allocated
 *
 * This function will not shrink the list if it is already larger than size
 * and may grow it past size if necessary (it will grow in num_elements_per_alloc
 * chunks). This function is thread-safe and will obtain the free list lock before
 * growing the free list.
 */
OPAL_DECLSPEC int opal_free_list_resize_mt (opal_free_list_t *flist, size_t size);


/**
 * Attemp to obtain an item from a free list.
 *
 * @param fl (IN)        Free list.
 * @param item (OUT)     Allocated item.
 *
 * If the requested item is not available the free list is grown to
 * accomodate the request - unless the max number of allocations has
 * been reached.  If this is the case - a NULL pointer is returned
 * to the caller. This function comes in three flavor: thread safe
 * (opal_free_list_get_mt), single threaded (opal_free_list_get_st),
 * and opal_using_threads conditioned (opal_free_list_get).
 */
static inline opal_free_list_item_t *opal_free_list_get_mt (opal_free_list_t *flist)
{
    opal_free_list_item_t *item =
        (opal_free_list_item_t*) opal_lifo_pop_atomic (&flist->super);

    if (OPAL_UNLIKELY(NULL == item)) {
        opal_mutex_lock (&flist->fl_lock);
        opal_free_list_grow_st (flist, flist->fl_num_per_alloc, &item);
        opal_mutex_unlock (&flist->fl_lock);
    }

    return item;
}

static inline opal_free_list_item_t *opal_free_list_get_st (opal_free_list_t *flist)
{
    opal_free_list_item_t *item =
        (opal_free_list_item_t*) opal_lifo_pop_st (&flist->super);

    if (OPAL_UNLIKELY(NULL == item)) {
        opal_free_list_grow_st (flist, flist->fl_num_per_alloc, &item);
    }

    return item;
}

static inline opal_free_list_item_t *opal_free_list_get (opal_free_list_t *flist)
{
    if (opal_using_threads ()) {
        return opal_free_list_get_mt (flist);
    }

    return opal_free_list_get_st (flist);
}

/** compatibility macro */
#define OPAL_FREE_LIST_GET(fl, item)            \
    (item) = opal_free_list_get (fl)

/**
 * Blocking call to obtain an item from a free list.
 *
 * @param fl (IN)        Free list.
 * @param item (OUT)     Allocated item.
 *
 * If the requested item is not available the free list is grown to
 * accomodate the request - unless the max number of allocations has
 * been reached. In this case the caller is blocked until an item
 * is returned to the list.
 */

/** compatibility macro */
#define OPAL_FREE_LIST_WAIT(fl, item)           \
    (item) = opal_free_list_wait (fl)

static inline opal_free_list_item_t *opal_free_list_wait_mt (opal_free_list_t *fl)
{
    opal_free_list_item_t *item =
        (opal_free_list_item_t *) opal_lifo_pop_atomic (&fl->super);

    while (NULL == item) {
        if (!opal_mutex_trylock (&fl->fl_lock)) {
            if (fl->fl_max_to_alloc <= fl->fl_num_allocated ||
                OPAL_SUCCESS != opal_free_list_grow_st (fl, fl->fl_num_per_alloc, &item)) {
                fl->fl_num_waiting++;
                opal_condition_wait (&fl->fl_condition, &fl->fl_lock);
                fl->fl_num_waiting--;
            } else {
                if (0 < fl->fl_num_waiting) {
                    if (1 == fl->fl_num_waiting) {
                        opal_condition_signal (&fl->fl_condition);
                    } else {
                        opal_condition_broadcast (&fl->fl_condition);
                    }
                }
            }
        } else {
            /* If I wasn't able to get the lock in the begining when I finaly grab it
             * the one holding the lock in the begining already grow the list. I will
             * release the lock and try to get a new element until I succeed.
             */
            opal_mutex_lock (&fl->fl_lock);
        }
        opal_mutex_unlock (&fl->fl_lock);
        if (NULL == item) {
            item = (opal_free_list_item_t *) opal_lifo_pop_atomic (&fl->super);
        }
    }

    return item;
}

static inline opal_free_list_item_t *opal_free_list_wait_st (opal_free_list_t *fl)
{
    opal_free_list_item_t *item =
        (opal_free_list_item_t *) opal_lifo_pop (&fl->super);

    while (NULL == item) {
        if (fl->fl_max_to_alloc <= fl->fl_num_allocated ||
            OPAL_SUCCESS != opal_free_list_grow_st (fl, fl->fl_num_per_alloc, &item)) {
            /* try to make progress */
            opal_progress ();
        }
        if (NULL == item) {
            item = (opal_free_list_item_t *) opal_lifo_pop (&fl->super);
        }
    }

    return item;
}

static inline opal_free_list_item_t *opal_free_list_wait (opal_free_list_t *fl)
{
    if (opal_using_threads ()) {
        return opal_free_list_wait_mt (fl);
    } else {
        return opal_free_list_wait_st (fl);
    }
}

/**
 * Return an item to a free list.
 *
 * @param fl (IN)        Free list.
 * @param item (OUT)     Allocated item.
 *
 */
static inline void opal_free_list_return_mt (opal_free_list_t *flist,
                                             opal_free_list_item_t *item)
{
    opal_list_item_t* original;

    original = opal_lifo_push_atomic (&flist->super, &item->super);
    if (&flist->super.opal_lifo_ghost == original) {
        if (flist->fl_num_waiting > 0) {
            /* one one item is being returned so it doesn't make sense to wake
             * more than a single waiting thread. additionally, posix thread
             * semantics do not require that the lock be held to signal the
             * condition variable. */
            opal_condition_signal (&flist->fl_condition);
        }
    }
}

static inline void opal_free_list_return_st (opal_free_list_t *flist,
                                             opal_free_list_item_t *item)
{
    opal_list_item_t* original;

    original = opal_lifo_push_st (&flist->super, &item->super);
    if (&flist->super.opal_lifo_ghost == original) {
        if (flist->fl_num_waiting > 0) {
            /* one one item is being returned so it doesn't make sense to wake
             * more than a single waiting thread. additionally, posix thread
             * semantics do not require that the lock be held to signal the
             * condition variable. */
            opal_condition_signal (&flist->fl_condition);
        }
    }
}

static inline void opal_free_list_return (opal_free_list_t *flist,
                                          opal_free_list_item_t *item)
{
    if (opal_using_threads ()) {
        opal_free_list_return_mt (flist, item);
    } else {
        opal_free_list_return_st (flist, item);
    }
}

/** compatibility macro */
#define OPAL_FREE_LIST_RETURN(fl, item)         \
    opal_free_list_return (fl, item)

END_C_DECLS
#endif

