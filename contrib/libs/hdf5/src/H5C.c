/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright by The HDF Group.                                               *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of HDF5.  The full HDF5 copyright notice, including     *
 * terms governing use, modification, and redistribution, is contained in    *
 * the COPYING file, which can be found at the root of the source code       *
 * distribution tree, or in https://www.hdfgroup.org/licenses.               *
 * If you do not have access to either file, you may request a copy from     *
 * help@hdfgroup.org.                                                        *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/*-------------------------------------------------------------------------
 *
 * Created:     H5C.c
 *
 * Purpose:     Functions in this file implement a generic cache for
 *              things which exist on disk, and which may be
 *              unambiguously referenced by their disk addresses.
 *
 *		For a detailed overview of the cache, please see the
 *		header comment for H5C_t in H5Cpkg.h.
 *
 *-------------------------------------------------------------------------
 */

/**************************************************************************
 *
 *                To Do:
 *
 *    Code Changes:
 *
 *	 - Change protect/unprotect to lock/unlock.
 *
 *     - Flush entries in increasing address order in
 *       H5C__make_space_in_cache().
 *
 *     - Also in H5C__make_space_in_cache(), use high and low water marks
 *       to reduce the number of I/O calls.
 *
 *     - When flushing, attempt to combine contiguous entries to reduce
 *       I/O overhead.  Can't do this just yet as some entries are not
 *       contiguous.  Do this in parallel only or in serial as well?
 *
 *	 - Fix nodes in memory to point directly to the skip list node from
 *         the LRU list, eliminating skip list lookups when evicting objects
 *         from the cache.
 *
 **************************************************************************/

/****************/
/* Module Setup */
/****************/

#include "H5Cmodule.h" /* This source code file is part of the H5C module */
#define H5F_FRIEND     /* suppress error about including H5Fpkg  */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions            */
#include "H5ACprivate.h" /* Metadata cache               */
#include "H5Cpkg.h"      /* Cache                        */
#include "H5Eprivate.h"  /* Error handling               */
#include "H5Fpkg.h"      /* Files                        */
#include "H5FLprivate.h" /* Free Lists                   */
#include "H5MFprivate.h" /* File memory management       */
#include "H5MMprivate.h" /* Memory management            */

/****************/
/* Local Macros */
/****************/

/******************/
/* Local Typedefs */
/******************/

/********************/
/* Local Prototypes */
/********************/

/*********************/
/* Package Variables */
/*********************/

/* Declare a free list to manage the tag info struct */
H5FL_DEFINE(H5C_tag_info_t);

/*****************************/
/* Library Private Variables */
/*****************************/

/*******************/
/* Local Variables */
/*******************/

/* Declare a free list to manage the H5C_t struct */
H5FL_DEFINE_STATIC(H5C_t);

/*-------------------------------------------------------------------------
 * Function:    H5C_create
 *
 * Purpose:     Allocate, initialize, and return the address of a new
 *        instance of H5C_t.
 *
 *        In general, the max_cache_size parameter must be positive,
 *        and the min_clean_size parameter must lie in the closed
 *        interval [0, max_cache_size].
 *
 *        The check_write_permitted parameter must either be NULL,
 *        or point to a function of type H5C_write_permitted_func_t.
 *        If it is NULL, the cache will use the write_permitted
 *        flag to determine whether writes are permitted.
 *
 * Return:      Success:        Pointer to the new instance.
 *              Failure:        NULL
 *
 *-------------------------------------------------------------------------
 */
H5C_t *
H5C_create(size_t max_cache_size, size_t min_clean_size, int max_type_id,
           const H5C_class_t *const *class_table_ptr, H5C_write_permitted_func_t check_write_permitted,
           bool write_permitted, H5C_log_flush_func_t log_flush, void *aux_ptr)
{
    int    i;
    H5C_t *cache_ptr = NULL;
    H5C_t *ret_value = NULL; /* Return value */

    FUNC_ENTER_NOAPI(NULL)

    assert(max_cache_size >= H5C__MIN_MAX_CACHE_SIZE);
    assert(max_cache_size <= H5C__MAX_MAX_CACHE_SIZE);
    assert(min_clean_size <= max_cache_size);

    assert(max_type_id >= 0);
    assert(max_type_id < H5C__MAX_NUM_TYPE_IDS);
    assert(class_table_ptr);

    for (i = 0; i <= max_type_id; i++) {
        assert((class_table_ptr)[i]);
        assert(strlen((class_table_ptr)[i]->name) > 0);
    } /* end for */

    if (NULL == (cache_ptr = H5FL_CALLOC(H5C_t)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed");

    if (NULL == (cache_ptr->slist_ptr = H5SL_create(H5SL_TYPE_HADDR, NULL)))
        HGOTO_ERROR(H5E_CACHE, H5E_CANTCREATE, NULL, "can't create skip list");

    cache_ptr->tag_list = NULL;

    /* If we get this far, we should succeed.  Go ahead and initialize all
     * the fields.
     */

    cache_ptr->flush_in_progress = false;

    if (NULL == (cache_ptr->log_info = (H5C_log_info_t *)H5MM_calloc(sizeof(H5C_log_info_t))))
        HGOTO_ERROR(H5E_CACHE, H5E_CANTALLOC, NULL, "memory allocation failed");

    cache_ptr->aux_ptr = aux_ptr;

    cache_ptr->max_type_id = max_type_id;

    cache_ptr->class_table_ptr = class_table_ptr;

    cache_ptr->max_cache_size = max_cache_size;
    cache_ptr->min_clean_size = min_clean_size;

    cache_ptr->check_write_permitted = check_write_permitted;
    cache_ptr->write_permitted       = write_permitted;

    cache_ptr->log_flush = log_flush;

    cache_ptr->evictions_enabled      = true;
    cache_ptr->close_warning_received = false;

    cache_ptr->index_len        = 0;
    cache_ptr->index_size       = (size_t)0;
    cache_ptr->clean_index_size = (size_t)0;
    cache_ptr->dirty_index_size = (size_t)0;

    for (i = 0; i < H5C_RING_NTYPES; i++) {
        cache_ptr->index_ring_len[i]        = 0;
        cache_ptr->index_ring_size[i]       = (size_t)0;
        cache_ptr->clean_index_ring_size[i] = (size_t)0;
        cache_ptr->dirty_index_ring_size[i] = (size_t)0;

        cache_ptr->slist_ring_len[i]  = 0;
        cache_ptr->slist_ring_size[i] = (size_t)0;
    } /* end for */

    for (i = 0; i < H5C__HASH_TABLE_LEN; i++)
        (cache_ptr->index)[i] = NULL;

    cache_ptr->il_len  = 0;
    cache_ptr->il_size = (size_t)0;
    cache_ptr->il_head = NULL;
    cache_ptr->il_tail = NULL;

    /* Tagging Field Initializations */
    cache_ptr->ignore_tags     = false;
    cache_ptr->num_objs_corked = 0;

    /* slist field initializations */
    cache_ptr->slist_enabled = false;
    cache_ptr->slist_changed = false;
    cache_ptr->slist_len     = 0;
    cache_ptr->slist_size    = (size_t)0;

    /* slist_ring_len, slist_ring_size, and
     * slist_ptr initialized above.
     */

#ifdef H5C_DO_SANITY_CHECKS
    cache_ptr->slist_len_increase  = 0;
    cache_ptr->slist_size_increase = 0;
#endif /* H5C_DO_SANITY_CHECKS */

    cache_ptr->entries_removed_counter   = 0;
    cache_ptr->last_entry_removed_ptr    = NULL;
    cache_ptr->entry_watched_for_removal = NULL;

    cache_ptr->pl_len      = 0;
    cache_ptr->pl_size     = (size_t)0;
    cache_ptr->pl_head_ptr = NULL;
    cache_ptr->pl_tail_ptr = NULL;

    cache_ptr->pel_len      = 0;
    cache_ptr->pel_size     = (size_t)0;
    cache_ptr->pel_head_ptr = NULL;
    cache_ptr->pel_tail_ptr = NULL;

    cache_ptr->LRU_list_len  = 0;
    cache_ptr->LRU_list_size = (size_t)0;
    cache_ptr->LRU_head_ptr  = NULL;
    cache_ptr->LRU_tail_ptr  = NULL;

#ifdef H5_HAVE_PARALLEL
    cache_ptr->coll_list_len   = 0;
    cache_ptr->coll_list_size  = (size_t)0;
    cache_ptr->coll_head_ptr   = NULL;
    cache_ptr->coll_tail_ptr   = NULL;
    cache_ptr->coll_write_list = NULL;
#endif /* H5_HAVE_PARALLEL */

#if H5C_MAINTAIN_CLEAN_AND_DIRTY_LRU_LISTS
    cache_ptr->cLRU_list_len  = 0;
    cache_ptr->cLRU_list_size = (size_t)0;
    cache_ptr->cLRU_head_ptr  = NULL;
    cache_ptr->cLRU_tail_ptr  = NULL;

    cache_ptr->dLRU_list_len  = 0;
    cache_ptr->dLRU_list_size = (size_t)0;
    cache_ptr->dLRU_head_ptr  = NULL;
    cache_ptr->dLRU_tail_ptr  = NULL;
#endif /* H5C_MAINTAIN_CLEAN_AND_DIRTY_LRU_LISTS */

    cache_ptr->size_increase_possible        = false;
    cache_ptr->flash_size_increase_possible  = false;
    cache_ptr->flash_size_increase_threshold = 0;
    cache_ptr->size_decrease_possible        = false;
    cache_ptr->resize_enabled                = false;
    cache_ptr->cache_full                    = false;
    cache_ptr->size_decreased                = false;
    cache_ptr->resize_in_progress            = false;
    cache_ptr->msic_in_progress              = false;

    cache_ptr->resize_ctl.version            = H5C__CURR_AUTO_SIZE_CTL_VER;
    cache_ptr->resize_ctl.rpt_fcn            = NULL;
    cache_ptr->resize_ctl.set_initial_size   = false;
    cache_ptr->resize_ctl.initial_size       = H5C__DEF_AR_INIT_SIZE;
    cache_ptr->resize_ctl.min_clean_fraction = H5C__DEF_AR_MIN_CLEAN_FRAC;
    cache_ptr->resize_ctl.max_size           = H5C__DEF_AR_MAX_SIZE;
    cache_ptr->resize_ctl.min_size           = H5C__DEF_AR_MIN_SIZE;
    cache_ptr->resize_ctl.epoch_length       = H5C__DEF_AR_EPOCH_LENGTH;

    cache_ptr->resize_ctl.incr_mode           = H5C_incr__off;
    cache_ptr->resize_ctl.lower_hr_threshold  = H5C__DEF_AR_LOWER_THRESHHOLD;
    cache_ptr->resize_ctl.increment           = H5C__DEF_AR_INCREMENT;
    cache_ptr->resize_ctl.apply_max_increment = true;
    cache_ptr->resize_ctl.max_increment       = H5C__DEF_AR_MAX_INCREMENT;

    cache_ptr->resize_ctl.flash_incr_mode = H5C_flash_incr__off;
    cache_ptr->resize_ctl.flash_multiple  = 1.0;
    cache_ptr->resize_ctl.flash_threshold = 0.25;

    cache_ptr->resize_ctl.decr_mode              = H5C_decr__off;
    cache_ptr->resize_ctl.upper_hr_threshold     = H5C__DEF_AR_UPPER_THRESHHOLD;
    cache_ptr->resize_ctl.decrement              = H5C__DEF_AR_DECREMENT;
    cache_ptr->resize_ctl.apply_max_decrement    = true;
    cache_ptr->resize_ctl.max_decrement          = H5C__DEF_AR_MAX_DECREMENT;
    cache_ptr->resize_ctl.epochs_before_eviction = H5C__DEF_AR_EPCHS_B4_EVICT;
    cache_ptr->resize_ctl.apply_empty_reserve    = true;
    cache_ptr->resize_ctl.empty_reserve          = H5C__DEF_AR_EMPTY_RESERVE;

    cache_ptr->epoch_markers_active = 0;

    /* no need to initialize the ring buffer itself */
    cache_ptr->epoch_marker_ringbuf_first = 1;
    cache_ptr->epoch_marker_ringbuf_last  = 0;
    cache_ptr->epoch_marker_ringbuf_size  = 0;

    /* Initialize all epoch marker entries' fields to zero/false/NULL */
    memset(cache_ptr->epoch_markers, 0, sizeof(cache_ptr->epoch_markers));

    /* Set non-zero/false/NULL fields for epoch markers */
    for (i = 0; i < H5C__MAX_EPOCH_MARKERS; i++) {
        ((cache_ptr->epoch_markers)[i]).addr = (haddr_t)i;
        ((cache_ptr->epoch_markers)[i]).type = H5AC_EPOCH_MARKER;
    }

    /* Initialize cache image generation on file close related fields.
     * Initial value of image_ctl must match H5C__DEFAULT_CACHE_IMAGE_CTL
     * in H5Cprivate.h.
     */
    cache_ptr->image_ctl.version            = H5C__CURR_CACHE_IMAGE_CTL_VER;
    cache_ptr->image_ctl.generate_image     = false;
    cache_ptr->image_ctl.save_resize_status = false;
    cache_ptr->image_ctl.entry_ageout       = -1;
    cache_ptr->image_ctl.flags              = H5C_CI__ALL_FLAGS;

    cache_ptr->serialization_in_progress = false;
    cache_ptr->load_image                = false;
    cache_ptr->image_loaded              = false;
    cache_ptr->delete_image              = false;
    cache_ptr->image_addr                = HADDR_UNDEF;
    cache_ptr->image_len                 = 0;
    cache_ptr->image_data_len            = 0;

    cache_ptr->entries_loaded_counter         = 0;
    cache_ptr->entries_inserted_counter       = 0;
    cache_ptr->entries_relocated_counter      = 0;
    cache_ptr->entry_fd_height_change_counter = 0;

    cache_ptr->num_entries_in_image = 0;
    cache_ptr->image_entries        = NULL;
    cache_ptr->image_buffer         = NULL;

    /* initialize free space manager related fields: */
    cache_ptr->rdfsm_settled = false;
    cache_ptr->mdfsm_settled = false;

    if (H5C_reset_cache_hit_rate_stats(cache_ptr) < 0)
        /* this should be impossible... */
        HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, NULL, "H5C_reset_cache_hit_rate_stats failed");

    H5C_stats__reset(cache_ptr);

    cache_ptr->prefix[0] = '\0'; /* empty string */

#ifndef NDEBUG
    cache_ptr->get_entry_ptr_from_addr_counter = 0;
#endif

    /* Set return value */
    ret_value = cache_ptr;

done:
    if (NULL == ret_value) {
        if (cache_ptr != NULL) {
            if (cache_ptr->slist_ptr != NULL)
                H5SL_close(cache_ptr->slist_ptr);

            HASH_CLEAR(hh, cache_ptr->tag_list);
            cache_ptr->tag_list = NULL;

            if (cache_ptr->log_info != NULL)
                H5MM_xfree(cache_ptr->log_info);

            cache_ptr = H5FL_FREE(H5C_t, cache_ptr);
        }
    }

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C_create() */

/*-------------------------------------------------------------------------
 * Function:    H5C_prep_for_file_close
 *
 * Purpose:     This function should be called just prior to the cache
 *        flushes at file close.  There should be no protected
 *        entries in the cache at this point.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C_prep_for_file_close(H5F_t *f)
{
    H5C_t *cache_ptr;
    bool   image_generated = false;   /* Whether a cache image was generated */
    herr_t ret_value       = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity checks */
    assert(f);
    assert(f->shared);
    assert(f->shared->cache);
    cache_ptr = f->shared->cache;
    assert(cache_ptr);

    /* It is possible to receive the close warning more than once */
    if (cache_ptr->close_warning_received)
        HGOTO_DONE(SUCCEED);
    cache_ptr->close_warning_received = true;

    /* Make certain there aren't any protected entries */
    assert(cache_ptr->pl_len == 0);

    /* Prepare cache image */
    if (H5C__prep_image_for_file_close(f, &image_generated) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_CANTCREATE, FAIL, "can't create cache image");

#ifdef H5_HAVE_PARALLEL
    if ((H5F_INTENT(f) & H5F_ACC_RDWR) && !image_generated && cache_ptr->aux_ptr != NULL &&
        f->shared->fs_persist) {
        /* If persistent free space managers are enabled, flushing the
         * metadata cache may result in the deletion, insertion, and/or
         * dirtying of entries.
         *
         * This is a problem in PHDF5, as it breaks two invariants of
         * our management of the metadata cache across all processes:
         *
         * 1) Entries will not be dirtied, deleted, inserted, or moved
         *    during flush in the parallel case.
         *
         * 2) All processes contain the same set of dirty metadata
         *    entries on entry to a sync point.
         *
         * To solve this problem for the persistent free space managers,
         * serialize the metadata cache on all processes prior to the
         * first sync point on file shutdown.  The shutdown warning is
         * a convenient location for this call.
         *
         * This is sufficient since:
         *
         * 1) FSM settle routines are only invoked on file close.  Since
         *    serialization make the same settle calls as flush on file
         *    close, and since the close warning is issued after all
         *    non FSM related space allocations and just before the
         *    first sync point on close, this call will leave the caches
         *    in a consistent state across the processes if they were
         *    consistent before.
         *
         * 2) Since the FSM settle routines are only invoked once during
         *    file close, invoking them now will prevent their invocation
         *    during a flush, and thus avoid any resulting entry dirties,
         *    deletions, insertion, or moves during the flush.
         */
        if (H5C__serialize_cache(f) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_CANTSERIALIZE, FAIL, "serialization of the cache failed");
    }  /* end if */
#endif /* H5_HAVE_PARALLEL */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C_prep_for_file_close() */

/*-------------------------------------------------------------------------
 * Function:    H5C_dest
 *
 * Purpose:     Flush all data to disk and destroy the cache.
 *
 *              This function fails if any object are protected since the
 *              resulting file might not be consistent.
 *
 * Note:        *cache_ptr has been freed upon successful return.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C_dest(H5F_t *f)
{
    H5C_t          *cache_ptr = f->shared->cache;
    H5C_tag_info_t *item      = NULL;
    H5C_tag_info_t *tmp       = NULL;
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity check */
    assert(cache_ptr);
    assert(cache_ptr->close_warning_received);

#if H5AC_DUMP_IMAGE_STATS_ON_CLOSE
    if (H5C__image_stats(cache_ptr, true) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "Can't display cache image stats");
#endif /* H5AC_DUMP_IMAGE_STATS_ON_CLOSE */

    /* Enable the slist, as it is needed in the flush */
    if (H5C_set_slist_enabled(f->shared->cache, true, false) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "set slist enabled failed");

    /* Flush and invalidate all cache entries */
    if (H5C__flush_invalidate_cache(f, H5C__NO_FLAGS_SET) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_CANTFLUSH, FAIL, "unable to flush cache");

    /* Generate & write cache image if requested */
    if (cache_ptr->image_ctl.generate_image)
        if (H5C__generate_cache_image(f, cache_ptr) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_CANTCREATE, FAIL, "Can't generate metadata cache image");

    /* Question: Is it possible for cache_ptr->slist be non-null at this
     *           point?  If no, shouldn't this if statement be an assert?
     */
    if (cache_ptr->slist_ptr != NULL) {
        assert(cache_ptr->slist_len == 0);
        assert(cache_ptr->slist_size == 0);

        H5SL_close(cache_ptr->slist_ptr);
        cache_ptr->slist_ptr = NULL;
    }

    HASH_ITER(hh, cache_ptr->tag_list, item, tmp)
    {
        HASH_DELETE(hh, cache_ptr->tag_list, item);
        item = H5FL_FREE(H5C_tag_info_t, item);
    }

    if (cache_ptr->log_info != NULL)
        H5MM_xfree(cache_ptr->log_info);

#ifdef H5C_DO_SANITY_CHECKS
    if (cache_ptr->get_entry_ptr_from_addr_counter > 0)
        fprintf(stdout, "*** %" PRId64 " calls to H5C_get_entry_ptr_from_add(). ***\n",
                cache_ptr->get_entry_ptr_from_addr_counter);
#endif /* H5C_DO_SANITY_CHECKS */

    cache_ptr = H5FL_FREE(H5C_t, cache_ptr);

done:
    if (ret_value < 0 && cache_ptr && cache_ptr->slist_ptr)
        /* Arguably, it shouldn't be necessary to re-enable the slist after
         * the call to H5C__flush_invalidate_cache(), as the metadata cache
         * should be discarded.  However, in the test code, we make multiple
         * calls to H5C_dest().  Thus we re-enable the slist on failure if it
         * and the cache still exist.  JRM -- 5/15/20
         */
        if (H5C_set_slist_enabled(f->shared->cache, false, false) < 0)
            HDONE_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "disable slist on flush dest failure failed");

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C_dest() */

/*-------------------------------------------------------------------------
 * Function:    H5C_evict
 *
 * Purpose:     Evict all except pinned entries in the cache
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C_evict(H5F_t *f)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity check */
    assert(f);

    /* Enable the slist, as it is needed in the flush */
    if (H5C_set_slist_enabled(f->shared->cache, true, false) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "set slist enabled failed");

    /* Flush and invalidate all cache entries except the pinned entries */
    if (H5C__flush_invalidate_cache(f, H5C__EVICT_ALLOW_LAST_PINS_FLAG) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_CANTFLUSH, FAIL, "unable to evict entries in the cache");

    /* Disable the slist */
    if (H5C_set_slist_enabled(f->shared->cache, false, true) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "set slist disabled failed");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C_evict() */

/*-------------------------------------------------------------------------
 * Function:    H5C_flush_cache
 *
 * Purpose:    Flush (and possibly destroy) the entries contained in the
 *        specified cache.
 *
 *        If the cache contains protected entries, the function will
 *        fail, as protected entries cannot be flushed.  However
 *        all unprotected entries should be flushed before the
 *        function returns failure.
 *
 * Return:      Non-negative on success/Negative on failure or if there was
 *        a request to flush all items and an entry was protected.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C_flush_cache(H5F_t *f, unsigned flags)
{
#ifdef H5C_DO_SANITY_CHECKS
    int      i;
    uint32_t index_len        = 0;
    size_t   index_size       = (size_t)0;
    size_t   clean_index_size = (size_t)0;
    size_t   dirty_index_size = (size_t)0;
    size_t   slist_size       = (size_t)0;
    uint32_t slist_len        = 0;
#endif /* H5C_DO_SANITY_CHECKS */
    H5C_ring_t ring;
    H5C_t     *cache_ptr;
    bool       destroy;
    herr_t     ret_value = SUCCEED;

    FUNC_ENTER_NOAPI(FAIL)

    assert(f);
    assert(f->shared);
    cache_ptr = f->shared->cache;
    assert(cache_ptr);
    assert(cache_ptr->slist_ptr);

#ifdef H5C_DO_SANITY_CHECKS
    assert(cache_ptr->index_ring_len[H5C_RING_UNDEFINED] == 0);
    assert(cache_ptr->index_ring_size[H5C_RING_UNDEFINED] == (size_t)0);
    assert(cache_ptr->clean_index_ring_size[H5C_RING_UNDEFINED] == (size_t)0);
    assert(cache_ptr->dirty_index_ring_size[H5C_RING_UNDEFINED] == (size_t)0);
    assert(cache_ptr->slist_ring_len[H5C_RING_UNDEFINED] == 0);
    assert(cache_ptr->slist_ring_size[H5C_RING_UNDEFINED] == (size_t)0);

    for (i = H5C_RING_USER; i < H5C_RING_NTYPES; i++) {
        index_len += cache_ptr->index_ring_len[i];
        index_size += cache_ptr->index_ring_size[i];
        clean_index_size += cache_ptr->clean_index_ring_size[i];
        dirty_index_size += cache_ptr->dirty_index_ring_size[i];

        slist_len += cache_ptr->slist_ring_len[i];
        slist_size += cache_ptr->slist_ring_size[i];
    } /* end for */

    assert(cache_ptr->index_len == index_len);
    assert(cache_ptr->index_size == index_size);
    assert(cache_ptr->clean_index_size == clean_index_size);
    assert(cache_ptr->dirty_index_size == dirty_index_size);
    assert(cache_ptr->slist_len == slist_len);
    assert(cache_ptr->slist_size == slist_size);
#endif /* H5C_DO_SANITY_CHECKS */

#ifdef H5C_DO_EXTREME_SANITY_CHECKS
    if (H5C__validate_protected_entry_list(cache_ptr) < 0 || H5C__validate_pinned_entry_list(cache_ptr) < 0 ||
        H5C__validate_lru_list(cache_ptr) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "an extreme sanity check failed on entry");
#endif /* H5C_DO_EXTREME_SANITY_CHECKS */

    destroy = ((flags & H5C__FLUSH_INVALIDATE_FLAG) != 0);
    assert(!(destroy && ((flags & H5C__FLUSH_IGNORE_PROTECTED_FLAG) != 0)));
    assert(!(cache_ptr->flush_in_progress));

    cache_ptr->flush_in_progress = true;

    if (destroy) {
        if (H5C__flush_invalidate_cache(f, flags) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_CANTFLUSH, FAIL, "flush invalidate failed");
    } /* end if */
    else {
        /* flush each ring, starting from the outermost ring and
         * working inward.
         */
        ring = H5C_RING_USER;
        while (ring < H5C_RING_NTYPES) {
            /* Only call the free space manager settle routines when close
             * warning has been received.
             */
            if (cache_ptr->close_warning_received) {
                switch (ring) {
                    case H5C_RING_USER:
                        break;

                    case H5C_RING_RDFSM:
                        /* Settle raw data FSM */
                        if (!cache_ptr->rdfsm_settled)
                            if (H5MF_settle_raw_data_fsm(f, &cache_ptr->rdfsm_settled) < 0)
                                HGOTO_ERROR(H5E_CACHE, H5E_CANTFLUSH, FAIL, "RD FSM settle failed");
                        break;

                    case H5C_RING_MDFSM:
                        /* Settle metadata FSM */
                        if (!cache_ptr->mdfsm_settled)
                            if (H5MF_settle_meta_data_fsm(f, &cache_ptr->mdfsm_settled) < 0)
                                HGOTO_ERROR(H5E_CACHE, H5E_CANTFLUSH, FAIL, "MD FSM settle failed");
                        break;

                    case H5C_RING_SBE:
                    case H5C_RING_SB:
                        break;

                    default:
                        HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "Unknown ring?!?!");
                        break;
                } /* end switch */
            }     /* end if */

            if (H5C__flush_ring(f, ring, flags) < 0)
                HGOTO_ERROR(H5E_CACHE, H5E_CANTFLUSH, FAIL, "flush ring failed");
            ring++;
        } /* end while */
    }     /* end else */

done:
    cache_ptr->flush_in_progress = false;

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C_flush_cache() */

/*-------------------------------------------------------------------------
 * Function:    H5C_flush_to_min_clean
 *
 * Purpose:    Flush dirty entries until the caches min clean size is
 *        attained.
 *
 *        This function is used in the implementation of the
 *        metadata cache in PHDF5.  To avoid "messages from the
 *        future", the cache on process 0 can't be allowed to
 *        flush entries until the other processes have reached
 *        the same point in the calculation.  If this constraint
 *        is not met, it is possible that the other processes will
 *        read metadata generated at a future point in the
 *        computation.
 *
 *
 * Return:      Non-negative on success/Negative on failure or if
 *        write is not permitted.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C_flush_to_min_clean(H5F_t *f)
{
    H5C_t *cache_ptr;
    bool   write_permitted;
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_NOAPI(FAIL)

    assert(f);
    assert(f->shared);
    cache_ptr = f->shared->cache;
    assert(cache_ptr);

    if (cache_ptr->check_write_permitted != NULL) {
        if ((cache_ptr->check_write_permitted)(f, &write_permitted) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "can't get write_permitted");
    } /* end if */
    else
        write_permitted = cache_ptr->write_permitted;

    if (!write_permitted)
        HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "cache write is not permitted!?!");

    if (H5C__make_space_in_cache(f, (size_t)0, write_permitted) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "H5C__make_space_in_cache failed");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C_flush_to_min_clean() */

/*-------------------------------------------------------------------------
 * Function:    H5C_reset_cache_hit_rate_stats()
 *
 * Purpose:     Reset the cache hit rate computation fields.
 *
 * Return:      SUCCEED on success, and FAIL on failure.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C_reset_cache_hit_rate_stats(H5C_t *cache_ptr)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    if (cache_ptr == NULL)
        HGOTO_ERROR(H5E_CACHE, H5E_BADVALUE, FAIL, "bad cache_ptr on entry");

    cache_ptr->cache_hits     = 0;
    cache_ptr->cache_accesses = 0;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C_reset_cache_hit_rate_stats() */

/*-------------------------------------------------------------------------
 * Function:    H5C_set_cache_auto_resize_config
 *
 * Purpose:    Set the cache automatic resize configuration to the
 *        provided values if they are in range, and fail if they
 *        are not.
 *
 *        If the new configuration enables automatic cache resizing,
 *        coerce the cache max size and min clean size into agreement
 *        with the new policy and re-set the full cache hit rate
 *        stats.
 *
 * Return:      SUCCEED on success, and FAIL on failure.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C_set_cache_auto_resize_config(H5C_t *cache_ptr, H5C_auto_size_ctl_t *config_ptr)
{
    size_t new_max_cache_size;
    size_t new_min_clean_size;
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    if (cache_ptr == NULL)
        HGOTO_ERROR(H5E_CACHE, H5E_BADVALUE, FAIL, "bad cache_ptr on entry");
    if (config_ptr == NULL)
        HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "NULL config_ptr on entry");
    if (config_ptr->version != H5C__CURR_AUTO_SIZE_CTL_VER)
        HGOTO_ERROR(H5E_CACHE, H5E_BADVALUE, FAIL, "unknown config version");

    /* check general configuration section of the config: */
    if (H5C_validate_resize_config(config_ptr, H5C_RESIZE_CFG__VALIDATE_GENERAL) < 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADRANGE, FAIL, "error in general configuration fields of new config");

    /* check size increase control fields of the config: */
    if (H5C_validate_resize_config(config_ptr, H5C_RESIZE_CFG__VALIDATE_INCREMENT) < 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADRANGE, FAIL, "error in the size increase control fields of new config");

    /* check size decrease control fields of the config: */
    if (H5C_validate_resize_config(config_ptr, H5C_RESIZE_CFG__VALIDATE_DECREMENT) < 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADRANGE, FAIL, "error in the size decrease control fields of new config");

    /* check for conflicts between size increase and size decrease controls: */
    if (H5C_validate_resize_config(config_ptr, H5C_RESIZE_CFG__VALIDATE_INTERACTIONS) < 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADRANGE, FAIL, "conflicting threshold fields in new config");

    /* will set the increase possible fields to false later if needed */
    cache_ptr->size_increase_possible       = true;
    cache_ptr->flash_size_increase_possible = true;
    cache_ptr->size_decrease_possible       = true;

    switch (config_ptr->incr_mode) {
        case H5C_incr__off:
            cache_ptr->size_increase_possible = false;
            break;

        case H5C_incr__threshold:
            if ((config_ptr->lower_hr_threshold <= 0.0) || (config_ptr->increment <= 1.0) ||
                ((config_ptr->apply_max_increment) && (config_ptr->max_increment <= 0)))
                cache_ptr->size_increase_possible = false;
            break;

        default: /* should be unreachable */
            HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "Unknown incr_mode?!?!?");
    } /* end switch */

    /* logically, this is where configuration for flash cache size increases
     * should go.  However, this configuration depends on max_cache_size, so
     * we wait until the end of the function, when this field is set.
     */

    switch (config_ptr->decr_mode) {
        case H5C_decr__off:
            cache_ptr->size_decrease_possible = false;
            break;

        case H5C_decr__threshold:
            if (config_ptr->upper_hr_threshold >= 1.0 || config_ptr->decrement >= 1.0 ||
                (config_ptr->apply_max_decrement && config_ptr->max_decrement <= 0))
                cache_ptr->size_decrease_possible = false;
            break;

        case H5C_decr__age_out:
            if ((config_ptr->apply_empty_reserve && config_ptr->empty_reserve >= 1.0) ||
                (config_ptr->apply_max_decrement && config_ptr->max_decrement <= 0))
                cache_ptr->size_decrease_possible = false;
            break;

        case H5C_decr__age_out_with_threshold:
            if ((config_ptr->apply_empty_reserve && config_ptr->empty_reserve >= 1.0) ||
                (config_ptr->apply_max_decrement && config_ptr->max_decrement <= 0) ||
                config_ptr->upper_hr_threshold >= 1.0)
                cache_ptr->size_decrease_possible = false;
            break;

        default: /* should be unreachable */
            HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "Unknown decr_mode?!?!?");
    } /* end switch */

    if (config_ptr->max_size == config_ptr->min_size) {
        cache_ptr->size_increase_possible       = false;
        cache_ptr->flash_size_increase_possible = false;
        cache_ptr->size_decrease_possible       = false;
    } /* end if */

    /* flash_size_increase_possible is intentionally omitted from the
     * following:
     */
    cache_ptr->resize_enabled = cache_ptr->size_increase_possible || cache_ptr->size_decrease_possible;
    cache_ptr->resize_ctl     = *config_ptr;

    /* Resize the cache to the supplied initial value if requested, or as
     * necessary to force it within the bounds of the current automatic
     * cache resizing configuration.
     *
     * Note that the min_clean_fraction may have changed, so we
     * go through the exercise even if the current size is within
     * range and an initial size has not been provided.
     */
    if (cache_ptr->resize_ctl.set_initial_size)
        new_max_cache_size = cache_ptr->resize_ctl.initial_size;
    else if (cache_ptr->max_cache_size > cache_ptr->resize_ctl.max_size)
        new_max_cache_size = cache_ptr->resize_ctl.max_size;
    else if (cache_ptr->max_cache_size < cache_ptr->resize_ctl.min_size)
        new_max_cache_size = cache_ptr->resize_ctl.min_size;
    else
        new_max_cache_size = cache_ptr->max_cache_size;

    new_min_clean_size = (size_t)((double)new_max_cache_size * (cache_ptr->resize_ctl.min_clean_fraction));

    /* since new_min_clean_size is of type size_t, we have
     *
     *     ( 0 <= new_min_clean_size )
     *
     * by definition.
     */
    assert(new_min_clean_size <= new_max_cache_size);
    assert(cache_ptr->resize_ctl.min_size <= new_max_cache_size);
    assert(new_max_cache_size <= cache_ptr->resize_ctl.max_size);

    if (new_max_cache_size < cache_ptr->max_cache_size)
        cache_ptr->size_decreased = true;

    cache_ptr->max_cache_size = new_max_cache_size;
    cache_ptr->min_clean_size = new_min_clean_size;

    if (H5C_reset_cache_hit_rate_stats(cache_ptr) < 0)
        /* this should be impossible... */
        HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "H5C_reset_cache_hit_rate_stats failed");

    /* remove excess epoch markers if any */
    if ((config_ptr->decr_mode == H5C_decr__age_out_with_threshold) ||
        (config_ptr->decr_mode == H5C_decr__age_out)) {
        if (cache_ptr->epoch_markers_active > cache_ptr->resize_ctl.epochs_before_eviction)
            if (H5C__autoadjust__ageout__remove_excess_markers(cache_ptr) < 0)
                HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "can't remove excess epoch markers");
    } /* end if */
    else if (cache_ptr->epoch_markers_active > 0) {
        if (H5C__autoadjust__ageout__remove_all_markers(cache_ptr) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "error removing all epoch markers");
    }

    /* configure flash size increase facility.  We wait until the
     * end of the function, as we need the max_cache_size set before
     * we start to keep things simple.
     *
     * If we haven't already ruled out flash cache size increases above,
     * go ahead and configure it.
     */
    if (cache_ptr->flash_size_increase_possible) {
        switch (config_ptr->flash_incr_mode) {
            case H5C_flash_incr__off:
                cache_ptr->flash_size_increase_possible = false;
                break;

            case H5C_flash_incr__add_space:
                cache_ptr->flash_size_increase_possible = true;
                cache_ptr->flash_size_increase_threshold =
                    (size_t)(((double)(cache_ptr->max_cache_size)) * (cache_ptr->resize_ctl.flash_threshold));
                break;

            default: /* should be unreachable */
                HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "Unknown flash_incr_mode?!?!?");
                break;
        } /* end switch */
    }     /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C_set_cache_auto_resize_config() */

/*-------------------------------------------------------------------------
 * Function:    H5C_set_evictions_enabled()
 *
 * Purpose:     Set cache_ptr->evictions_enabled to the value of the
 *              evictions enabled parameter.
 *
 * Return:      SUCCEED on success, and FAIL on failure.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C_set_evictions_enabled(H5C_t *cache_ptr, bool evictions_enabled)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    if (cache_ptr == NULL)
        HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "Bad cache_ptr on entry");

    /* There is no fundamental reason why we should not permit
     * evictions to be disabled while automatic resize is enabled.
     * However, allowing it would greatly complicate testing
     * the feature.  Hence the following:
     */
    if ((evictions_enabled != true) && ((cache_ptr->resize_ctl.incr_mode != H5C_incr__off) ||
                                        (cache_ptr->resize_ctl.decr_mode != H5C_decr__off)))
        HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "Can't disable evictions when auto resize enabled");

    cache_ptr->evictions_enabled = evictions_enabled;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C_set_evictions_enabled() */

/*-------------------------------------------------------------------------
 * Function:    H5C_set_slist_enabled()
 *
 * Purpose:     Enable or disable the slist as directed.
 *
 *              The slist (skip list) is an address ordered list of
 *              dirty entries in the metadata cache.  However, this
 *              list is only needed during flush and close, where we
 *              use it to write entries in more or less increasing
 *              address order.
 *
 *              This function sets up and enables further operations
 *              on the slist, or disable the slist.  This in turn
 *              allows us to avoid the overhead of maintaining the
 *              slist when it is not needed.
 *
 *
 *              If the slist_enabled parameter is true, the function
 *
 *              1) Verifies that the slist is empty.
 *
 *              2) Scans the index list, and inserts all dirty entries
 *                 into the slist.
 *
 *              3) Sets cache_ptr->slist_enabled = true.
 *
 *              Note that the clear_slist parameter is ignored if
 *              the slist_enabed parameter is true.
 *
 *
 *              If the slist_enabled_parameter is false, the function
 *              shuts down the slist.
 *
 *              Normally the slist will be empty at this point, however
 *              that need not be the case if H5C_flush_cache() has been
 *              called with the H5C__FLUSH_MARKED_ENTRIES_FLAG.
 *
 *              Thus shutdown proceeds as follows:
 *
 *              1) Test to see if the slist is empty.  If it is, proceed
 *                 to step 3.
 *
 *              2) Test to see if the clear_slist parameter is true.
 *
 *                 If it is, remove all entries from the slist.
 *
 *                 If it isn't, throw an error.
 *
 *              3) set cache_ptr->slist_enabled = false.
 *
 * Return:      SUCCEED on success, and FAIL on failure.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C_set_slist_enabled(H5C_t *cache_ptr, bool slist_enabled, bool clear_slist)
{
    H5C_cache_entry_t *entry_ptr;
    herr_t             ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    if (cache_ptr == NULL)
        HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "Bad cache_ptr on entry");

    if (slist_enabled) {
        if (cache_ptr->slist_enabled)
            HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "slist already enabled?");
        if ((cache_ptr->slist_len != 0) || (cache_ptr->slist_size != 0))
            HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "slist not empty?");

        /* set cache_ptr->slist_enabled to true so that the slist
         * maintenance macros will be enabled.
         */
        cache_ptr->slist_enabled = true;

        /* scan the index list and insert all dirty entries in the slist */
        entry_ptr = cache_ptr->il_head;
        while (entry_ptr != NULL) {
            if (entry_ptr->is_dirty)
                H5C__INSERT_ENTRY_IN_SLIST(cache_ptr, entry_ptr, FAIL);
            entry_ptr = entry_ptr->il_next;
        }

        /* we don't maintain a dirty index len, so we can't do a cross
         * check against it.  Note that there is no point in cross checking
         * against the dirty LRU size, as the dirty LRU may not be maintained,
         * and in any case, there is no requirement that all dirty entries
         * will reside on the dirty LRU.
         */
        assert(cache_ptr->dirty_index_size == cache_ptr->slist_size);
    }
    else { /* take down the skip list */
        if (!cache_ptr->slist_enabled)
            HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "slist already disabled?");

        if ((cache_ptr->slist_len != 0) || (cache_ptr->slist_size != 0)) {
            if (clear_slist) {
                H5SL_node_t *node_ptr;

                node_ptr = H5SL_first(cache_ptr->slist_ptr);
                while (node_ptr != NULL) {
                    entry_ptr = (H5C_cache_entry_t *)H5SL_item(node_ptr);
                    H5C__REMOVE_ENTRY_FROM_SLIST(cache_ptr, entry_ptr, false, FAIL);
                    node_ptr = H5SL_first(cache_ptr->slist_ptr);
                }
            }
            else
                HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "slist not empty?");
        }

        cache_ptr->slist_enabled = false;

        assert(0 == cache_ptr->slist_len);
        assert(0 == cache_ptr->slist_size);
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C_set_slist_enabled() */

/*-------------------------------------------------------------------------
 * Function:    H5C_unsettle_ring()
 *
 * Purpose:     Advise the metadata cache that the specified free space
 *              manager ring is no longer settled (if it was on entry).
 *
 *              If the target free space manager ring is already
 *              unsettled, do nothing, and return SUCCEED.
 *
 *              If the target free space manager ring is settled, and
 *              we are not in the process of a file shutdown, mark
 *              the ring as unsettled, and return SUCCEED.
 *
 *              If the target free space manager is settled, and we
 *              are in the process of a file shutdown, post an error
 *              message, and return FAIL.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C_unsettle_ring(H5F_t *f, H5C_ring_t ring)
{
    H5C_t *cache_ptr;
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity checks */
    assert(f);
    assert(f->shared);
    assert(f->shared->cache);
    assert((H5C_RING_RDFSM == ring) || (H5C_RING_MDFSM == ring));
    cache_ptr = f->shared->cache;

    switch (ring) {
        case H5C_RING_RDFSM:
            if (cache_ptr->rdfsm_settled) {
                if (cache_ptr->close_warning_received)
                    HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "unexpected rdfsm ring unsettle");
                cache_ptr->rdfsm_settled = false;
            } /* end if */
            break;

        case H5C_RING_MDFSM:
            if (cache_ptr->mdfsm_settled) {
                if (cache_ptr->close_warning_received)
                    HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "unexpected mdfsm ring unsettle");
                cache_ptr->mdfsm_settled = false;
            } /* end if */
            break;

        default:
            assert(false); /* this should be un-reachable */
            break;
    } /* end switch */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C_unsettle_ring() */

/*-------------------------------------------------------------------------
 * Function:    H5C_validate_resize_config()
 *
 * Purpose:    Run a sanity check on the specified sections of the
 *             provided instance of struct H5C_auto_size_ctl_t.
 *
 *        Do nothing and return SUCCEED if no errors are detected,
 *        and flag an error and return FAIL otherwise.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C_validate_resize_config(H5C_auto_size_ctl_t *config_ptr, unsigned int tests)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    if (config_ptr == NULL)
        HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "NULL config_ptr on entry");

    if (config_ptr->version != H5C__CURR_AUTO_SIZE_CTL_VER)
        HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "Unknown config version");

    if ((tests & H5C_RESIZE_CFG__VALIDATE_GENERAL) != 0) {
        if (config_ptr->max_size > H5C__MAX_MAX_CACHE_SIZE)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "max_size too big");
        if (config_ptr->min_size < H5C__MIN_MAX_CACHE_SIZE)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "min_size too small");
        if (config_ptr->min_size > config_ptr->max_size)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "min_size > max_size");
        if (config_ptr->set_initial_size && ((config_ptr->initial_size < config_ptr->min_size) ||
                                             (config_ptr->initial_size > config_ptr->max_size)))
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL,
                        "initial_size must be in the interval [min_size, max_size]");
        if ((config_ptr->min_clean_fraction < 0.0) || (config_ptr->min_clean_fraction > 1.0))
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL,
                        "min_clean_fraction must be in the interval [0.0, 1.0]");
        if (config_ptr->epoch_length < H5C__MIN_AR_EPOCH_LENGTH)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "epoch_length too small");
        if (config_ptr->epoch_length > H5C__MAX_AR_EPOCH_LENGTH)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "epoch_length too big");
    } /* H5C_RESIZE_CFG__VALIDATE_GENERAL */

    if ((tests & H5C_RESIZE_CFG__VALIDATE_INCREMENT) != 0) {
        if ((config_ptr->incr_mode != H5C_incr__off) && (config_ptr->incr_mode != H5C_incr__threshold))
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "Invalid incr_mode");

        if (config_ptr->incr_mode == H5C_incr__threshold) {
            if ((config_ptr->lower_hr_threshold < 0.0) || (config_ptr->lower_hr_threshold > 1.0))
                HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL,
                            "lower_hr_threshold must be in the range [0.0, 1.0]");
            if (config_ptr->increment < 1.0)
                HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "increment must be greater than or equal to 1.0");

            /* no need to check max_increment, as it is a size_t,
             * and thus must be non-negative.
             */
        } /* H5C_incr__threshold */

        switch (config_ptr->flash_incr_mode) {
            case H5C_flash_incr__off:
                /* nothing to do here */
                break;

            case H5C_flash_incr__add_space:
                if ((config_ptr->flash_multiple < 0.1) || (config_ptr->flash_multiple > 10.0))
                    HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL,
                                "flash_multiple must be in the range [0.1, 10.0]");
                if ((config_ptr->flash_threshold < 0.1) || (config_ptr->flash_threshold > 1.0))
                    HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL,
                                "flash_threshold must be in the range [0.1, 1.0]");
                break;

            default:
                HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "Invalid flash_incr_mode");
                break;
        } /* end switch */
    }     /* H5C_RESIZE_CFG__VALIDATE_INCREMENT */

    if ((tests & H5C_RESIZE_CFG__VALIDATE_DECREMENT) != 0) {
        if ((config_ptr->decr_mode != H5C_decr__off) && (config_ptr->decr_mode != H5C_decr__threshold) &&
            (config_ptr->decr_mode != H5C_decr__age_out) &&
            (config_ptr->decr_mode != H5C_decr__age_out_with_threshold))
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "Invalid decr_mode");

        if (config_ptr->decr_mode == H5C_decr__threshold) {
            if (config_ptr->upper_hr_threshold > 1.0)
                HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "upper_hr_threshold must be <= 1.0");
            if ((config_ptr->decrement > 1.0) || (config_ptr->decrement < 0.0))
                HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "decrement must be in the interval [0.0, 1.0]");

            /* no need to check max_decrement as it is a size_t
             * and thus must be non-negative.
             */
        } /* H5C_decr__threshold */

        if ((config_ptr->decr_mode == H5C_decr__age_out) ||
            (config_ptr->decr_mode == H5C_decr__age_out_with_threshold)) {
            if (config_ptr->epochs_before_eviction < 1)
                HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "epochs_before_eviction must be positive");
            if (config_ptr->epochs_before_eviction > H5C__MAX_EPOCH_MARKERS)
                HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "epochs_before_eviction too big");
            if (config_ptr->apply_empty_reserve &&
                (config_ptr->empty_reserve > 1.0 || config_ptr->empty_reserve < 0.0))
                HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "empty_reserve must be in the interval [0.0, 1.0]");

            /* no need to check max_decrement as it is a size_t
             * and thus must be non-negative.
             */
        } /* H5C_decr__age_out || H5C_decr__age_out_with_threshold */

        if (config_ptr->decr_mode == H5C_decr__age_out_with_threshold)
            if ((config_ptr->upper_hr_threshold > 1.0) || (config_ptr->upper_hr_threshold < 0.0))
                HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL,
                            "upper_hr_threshold must be in the interval [0.0, 1.0]");
    } /* H5C_RESIZE_CFG__VALIDATE_DECREMENT */

    if ((tests & H5C_RESIZE_CFG__VALIDATE_INTERACTIONS) != 0) {
        if ((config_ptr->incr_mode == H5C_incr__threshold) &&
            ((config_ptr->decr_mode == H5C_decr__threshold) ||
             (config_ptr->decr_mode == H5C_decr__age_out_with_threshold)) &&
            (config_ptr->lower_hr_threshold >= config_ptr->upper_hr_threshold))
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "conflicting threshold fields in config");
    } /* H5C_RESIZE_CFG__VALIDATE_INTERACTIONS */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C_validate_resize_config() */

/*-------------------------------------------------------------------------
 * Function:    H5C_cork
 *
 * Purpose:     To cork/uncork/get cork status of an object depending on "action":
 *        H5C__SET_CORK:
 *            To cork the object
 *            Return error if the object is already corked
 *        H5C__UNCORK:
 *            To uncork the object
 *            Return error if the object is not corked
 *         H5C__GET_CORKED:
 *            To retrieve the cork status of an object in
 *            the parameter "corked"
 *
 * Return:      Success:        Non-negative
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C_cork(H5C_t *cache_ptr, haddr_t obj_addr, unsigned action, bool *corked)
{
    H5C_tag_info_t *tag_info  = NULL;
    herr_t          ret_value = SUCCEED;

    FUNC_ENTER_NOAPI_NOINIT

    /* Assertions */
    assert(cache_ptr != NULL);
    assert(H5_addr_defined(obj_addr));
    assert(action == H5C__SET_CORK || action == H5C__UNCORK || action == H5C__GET_CORKED);

    /* Search the list of corked object addresses in the cache */
    HASH_FIND(hh, cache_ptr->tag_list, &obj_addr, sizeof(haddr_t), tag_info);

    if (H5C__GET_CORKED == action) {
        assert(corked);
        if (tag_info != NULL && tag_info->corked)
            *corked = true;
        else
            *corked = false;
    }
    else {
        /* Sanity check */
        assert(H5C__SET_CORK == action || H5C__UNCORK == action);

        /* Perform appropriate action */
        if (H5C__SET_CORK == action) {
            /* Check if this is the first entry for this tagged object */
            if (NULL == tag_info) {
                /* Allocate new tag info struct */
                if (NULL == (tag_info = H5FL_CALLOC(H5C_tag_info_t)))
                    HGOTO_ERROR(H5E_CACHE, H5E_CANTALLOC, FAIL, "can't allocate tag info for cache entry");

                /* Set the tag for all entries */
                tag_info->tag = obj_addr;

                /* Insert tag info into hash table */
                HASH_ADD(hh, cache_ptr->tag_list, tag, sizeof(haddr_t), tag_info);
            }
            else {
                /* Check for object already corked */
                if (tag_info->corked)
                    HGOTO_ERROR(H5E_CACHE, H5E_CANTCORK, FAIL, "object already corked");
                assert(tag_info->entry_cnt > 0 && tag_info->head);
            }

            /* Set the corked status for the entire object */
            tag_info->corked = true;
            cache_ptr->num_objs_corked++;
        }
        else {
            /* Sanity check */
            if (NULL == tag_info)
                HGOTO_ERROR(H5E_CACHE, H5E_CANTUNCORK, FAIL, "tag info pointer is NULL");

            /* Check for already uncorked */
            if (!tag_info->corked)
                HGOTO_ERROR(H5E_CACHE, H5E_CANTUNCORK, FAIL, "object already uncorked");

            /* Set the corked status for the entire object */
            tag_info->corked = false;
            cache_ptr->num_objs_corked--;

            /* Remove the tag info from the tag list, if there's no more entries with this tag */
            if (0 == tag_info->entry_cnt) {
                /* Sanity check */
                assert(NULL == tag_info->head);

                HASH_DELETE(hh, cache_ptr->tag_list, tag_info);

                /* Release the tag info */
                tag_info = H5FL_FREE(H5C_tag_info_t, tag_info);
            }
            else
                assert(NULL != tag_info->head);
        }
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C_cork() */
