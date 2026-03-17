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
 * Created:     H5Centry.c
 *
 * Purpose:     Routines which operate on cache entries.
 *
 *-------------------------------------------------------------------------
 */

/****************/
/* Module Setup */
/****************/

#include "H5Cmodule.h" /* This source code file is part of the H5C module */
#define H5F_FRIEND     /* suppress error about including H5Fpkg  */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions            */
#include "H5Cpkg.h"      /* Cache                        */
#include "H5Eprivate.h"  /* Error handling               */
#include "H5Fpkg.h"      /* Files                        */
#include "H5MFprivate.h" /* File memory management       */

/****************/
/* Local Macros */
/****************/

/******************/
/* Local Typedefs */
/******************/

/********************/
/* Local Prototypes */
/********************/
static herr_t H5C__autoadjust__ageout(H5F_t *f, double hit_rate, enum H5C_resize_status *status_ptr,
                                      size_t *new_max_cache_size_ptr, bool write_permitted);
static herr_t H5C__autoadjust__ageout__cycle_epoch_marker(H5C_t *cache_ptr);
static herr_t H5C__autoadjust__ageout__evict_aged_out_entries(H5F_t *f, bool write_permitted);
static herr_t H5C__autoadjust__ageout__insert_new_marker(H5C_t *cache_ptr);
static herr_t H5C__flush_invalidate_ring(H5F_t *f, H5C_ring_t ring, unsigned flags);
static herr_t H5C__serialize_ring(H5F_t *f, H5C_ring_t ring);

/*********************/
/* Package Variables */
/*********************/

/*****************************/
/* Library Private Variables */
/*****************************/

/*******************/
/* Local Variables */
/*******************/

/*-------------------------------------------------------------------------
 * Function:    H5C__auto_adjust_cache_size
 *
 * Purpose:        Obtain the current full cache hit rate, and compare it
 *        with the hit rate thresholds for modifying cache size.
 *        If one of the thresholds has been crossed, adjusts the
 *        size of the cache accordingly.
 *
 *        The function then resets the full cache hit rate
 *        statistics, and exits.
 *
 * Return:      Non-negative on success/Negative on failure or if there was
 *        an attempt to flush a protected item.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C__auto_adjust_cache_size(H5F_t *f, bool write_permitted)
{
    H5C_t                 *cache_ptr             = f->shared->cache;
    bool                   reentrant_call        = false;
    bool                   inserted_epoch_marker = false;
    size_t                 new_max_cache_size    = 0;
    size_t                 old_max_cache_size    = 0;
    size_t                 new_min_clean_size    = 0;
    size_t                 old_min_clean_size    = 0;
    double                 hit_rate;
    enum H5C_resize_status status    = in_spec; /* will change if needed */
    herr_t                 ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(f);
    assert(cache_ptr);
    assert(cache_ptr->cache_accesses >= cache_ptr->resize_ctl.epoch_length);
    assert(0.0 <= cache_ptr->resize_ctl.min_clean_fraction);
    assert(cache_ptr->resize_ctl.min_clean_fraction <= 100.0);

    /* check to see if cache_ptr->resize_in_progress is true.  If it, this
     * is a re-entrant call via a client callback called in the resize
     * process.  To avoid an infinite recursion, set reentrant_call to
     * true, and goto done.
     */
    if (cache_ptr->resize_in_progress) {
        reentrant_call = true;
        HGOTO_DONE(SUCCEED);
    } /* end if */

    cache_ptr->resize_in_progress = true;

    if (!cache_ptr->resize_enabled)
        HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "Auto cache resize disabled");

    assert((cache_ptr->resize_ctl.incr_mode != H5C_incr__off) ||
           (cache_ptr->resize_ctl.decr_mode != H5C_decr__off));

    if (H5C_get_cache_hit_rate(cache_ptr, &hit_rate) != SUCCEED)
        HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "Can't get hit rate");

    assert((0.0 <= hit_rate) && (hit_rate <= 1.0));

    switch (cache_ptr->resize_ctl.incr_mode) {
        case H5C_incr__off:
            if (cache_ptr->size_increase_possible)
                HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "size_increase_possible but H5C_incr__off?!?!?");
            break;

        case H5C_incr__threshold:
            if (hit_rate < cache_ptr->resize_ctl.lower_hr_threshold) {
                if (!cache_ptr->size_increase_possible)
                    status = increase_disabled;
                else if (cache_ptr->max_cache_size >= cache_ptr->resize_ctl.max_size) {
                    assert(cache_ptr->max_cache_size == cache_ptr->resize_ctl.max_size);
                    status = at_max_size;
                }
                else if (!cache_ptr->cache_full)
                    status = not_full;
                else {
                    new_max_cache_size =
                        (size_t)(((double)(cache_ptr->max_cache_size)) * cache_ptr->resize_ctl.increment);

                    /* clip to max size if necessary */
                    if (new_max_cache_size > cache_ptr->resize_ctl.max_size)
                        new_max_cache_size = cache_ptr->resize_ctl.max_size;

                    /* clip to max increment if necessary */
                    if (cache_ptr->resize_ctl.apply_max_increment &&
                        ((cache_ptr->max_cache_size + cache_ptr->resize_ctl.max_increment) <
                         new_max_cache_size))
                        new_max_cache_size = cache_ptr->max_cache_size + cache_ptr->resize_ctl.max_increment;

                    status = increase;
                }
            }
            break;

        default:
            HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "unknown incr_mode");
    }

    /* If the decr_mode is either age out or age out with threshold, we
     * must run the marker maintenance code, whether we run the size
     * reduction code or not.  We do this in two places -- here we
     * insert a new marker if the number of active epoch markers is
     * is less than the current epochs before eviction, and after
     * the ageout call, we cycle the markers.
     *
     * However, we can't call the ageout code or cycle the markers
     * unless there was a full complement of markers in place on
     * entry.  The inserted_epoch_marker flag is used to track this.
     */

    if (((cache_ptr->resize_ctl.decr_mode == H5C_decr__age_out) ||
         (cache_ptr->resize_ctl.decr_mode == H5C_decr__age_out_with_threshold)) &&
        (cache_ptr->epoch_markers_active < cache_ptr->resize_ctl.epochs_before_eviction)) {

        if (H5C__autoadjust__ageout__insert_new_marker(cache_ptr) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "can't insert new epoch marker");

        inserted_epoch_marker = true;
    }

    /* don't run the cache size decrease code unless the cache size
     * increase code is disabled, or the size increase code sees no need
     * for action.  In either case, status == in_spec at this point.
     */

    if (status == in_spec) {
        switch (cache_ptr->resize_ctl.decr_mode) {
            case H5C_decr__off:
                break;

            case H5C_decr__threshold:
                if (hit_rate > cache_ptr->resize_ctl.upper_hr_threshold) {
                    if (!cache_ptr->size_decrease_possible)
                        status = decrease_disabled;
                    else if (cache_ptr->max_cache_size <= cache_ptr->resize_ctl.min_size) {
                        assert(cache_ptr->max_cache_size == cache_ptr->resize_ctl.min_size);
                        status = at_min_size;
                    }
                    else {
                        new_max_cache_size =
                            (size_t)(((double)(cache_ptr->max_cache_size)) * cache_ptr->resize_ctl.decrement);

                        /* clip to min size if necessary */
                        if (new_max_cache_size < cache_ptr->resize_ctl.min_size)
                            new_max_cache_size = cache_ptr->resize_ctl.min_size;

                        /* clip to max decrement if necessary */
                        if (cache_ptr->resize_ctl.apply_max_decrement &&
                            ((cache_ptr->resize_ctl.max_decrement + new_max_cache_size) <
                             cache_ptr->max_cache_size))
                            new_max_cache_size =
                                cache_ptr->max_cache_size - cache_ptr->resize_ctl.max_decrement;

                        status = decrease;
                    }
                }
                break;

            case H5C_decr__age_out_with_threshold:
            case H5C_decr__age_out:
                if (!inserted_epoch_marker) {
                    if (!cache_ptr->size_decrease_possible)
                        status = decrease_disabled;
                    else {
                        if (H5C__autoadjust__ageout(f, hit_rate, &status, &new_max_cache_size,
                                                    write_permitted) < 0)
                            HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "ageout code failed");
                    } /* end else */
                }     /* end if */
                break;

            default:
                HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "unknown incr_mode");
        }
    }

    /* cycle the epoch markers here if appropriate */
    if (((cache_ptr->resize_ctl.decr_mode == H5C_decr__age_out) ||
         (cache_ptr->resize_ctl.decr_mode == H5C_decr__age_out_with_threshold)) &&
        !inserted_epoch_marker)
        /* move last epoch marker to the head of the LRU list */
        if (H5C__autoadjust__ageout__cycle_epoch_marker(cache_ptr) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "error cycling epoch marker");

    if ((status == increase) || (status == decrease)) {
        old_max_cache_size = cache_ptr->max_cache_size;
        old_min_clean_size = cache_ptr->min_clean_size;

        new_min_clean_size =
            (size_t)((double)new_max_cache_size * (cache_ptr->resize_ctl.min_clean_fraction));

        /* new_min_clean_size is of size_t, and thus must be non-negative.
         * Hence we have
         *
         *     ( 0 <= new_min_clean_size ).
         *
         * by definition.
         */
        assert(new_min_clean_size <= new_max_cache_size);
        assert(cache_ptr->resize_ctl.min_size <= new_max_cache_size);
        assert(new_max_cache_size <= cache_ptr->resize_ctl.max_size);

        cache_ptr->max_cache_size = new_max_cache_size;
        cache_ptr->min_clean_size = new_min_clean_size;

        if (status == increase)
            cache_ptr->cache_full = false;
        else if (status == decrease)
            cache_ptr->size_decreased = true;

        /* update flash cache size increase fields as appropriate */
        if (cache_ptr->flash_size_increase_possible) {
            switch (cache_ptr->resize_ctl.flash_incr_mode) {
                case H5C_flash_incr__off:
                    HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL,
                                "flash_size_increase_possible but H5C_flash_incr__off?!");
                    break;

                case H5C_flash_incr__add_space:
                    cache_ptr->flash_size_increase_threshold =
                        (size_t)(((double)(cache_ptr->max_cache_size)) *
                                 (cache_ptr->resize_ctl.flash_threshold));
                    break;

                default: /* should be unreachable */
                    HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "Unknown flash_incr_mode?!?!?");
                    break;
            }
        }
    }

    if (cache_ptr->resize_ctl.rpt_fcn != NULL)
        (cache_ptr->resize_ctl.rpt_fcn)(cache_ptr, H5C__CURR_AUTO_RESIZE_RPT_FCN_VER, hit_rate, status,
                                        old_max_cache_size, new_max_cache_size, old_min_clean_size,
                                        new_min_clean_size);

    if (H5C_reset_cache_hit_rate_stats(cache_ptr) < 0)
        /* this should be impossible... */
        HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "H5C_reset_cache_hit_rate_stats failed");

done:
    /* Sanity checks */
    assert(cache_ptr->resize_in_progress);
    if (!reentrant_call)
        cache_ptr->resize_in_progress = false;
    assert((!reentrant_call) || (cache_ptr->resize_in_progress));

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__auto_adjust_cache_size() */

/*-------------------------------------------------------------------------
 * Function:    H5C__autoadjust__ageout
 *
 * Purpose:     Implement the ageout automatic cache size decrement
 *        algorithm.  Note that while this code evicts aged out
 *        entries, the code does not change the maximum cache size.
 *        Instead, the function simply computes the new value (if
 *        any change is indicated) and reports this value in
 *        *new_max_cache_size_ptr.
 *
 * Return:      Non-negative on success/Negative on failure or if there was
 *              an attempt to flush a protected item.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5C__autoadjust__ageout(H5F_t *f, double hit_rate, enum H5C_resize_status *status_ptr,
                        size_t *new_max_cache_size_ptr, bool write_permitted)
{
    H5C_t *cache_ptr = f->shared->cache;
    size_t test_size;
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(f);
    assert(cache_ptr);
    assert((status_ptr) && (*status_ptr == in_spec));
    assert((new_max_cache_size_ptr) && (*new_max_cache_size_ptr == 0));

    /* remove excess epoch markers if any */
    if (cache_ptr->epoch_markers_active > cache_ptr->resize_ctl.epochs_before_eviction)
        if (H5C__autoadjust__ageout__remove_excess_markers(cache_ptr) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "can't remove excess epoch markers");

    if ((cache_ptr->resize_ctl.decr_mode == H5C_decr__age_out) ||
        ((cache_ptr->resize_ctl.decr_mode == H5C_decr__age_out_with_threshold) &&
         (hit_rate >= cache_ptr->resize_ctl.upper_hr_threshold))) {

        if (cache_ptr->max_cache_size > cache_ptr->resize_ctl.min_size) {
            /* evict aged out cache entries if appropriate... */
            if (H5C__autoadjust__ageout__evict_aged_out_entries(f, write_permitted) < 0)
                HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "error flushing aged out entries");

            /* ... and then reduce cache size if appropriate */
            if (cache_ptr->index_size < cache_ptr->max_cache_size) {
                if (cache_ptr->resize_ctl.apply_empty_reserve) {
                    test_size =
                        (size_t)(((double)cache_ptr->index_size) / (1 - cache_ptr->resize_ctl.empty_reserve));
                    if (test_size < cache_ptr->max_cache_size) {
                        *status_ptr             = decrease;
                        *new_max_cache_size_ptr = test_size;
                    }
                }
                else {
                    *status_ptr             = decrease;
                    *new_max_cache_size_ptr = cache_ptr->index_size;
                }

                if (*status_ptr == decrease) {
                    /* clip to min size if necessary */
                    if (*new_max_cache_size_ptr < cache_ptr->resize_ctl.min_size)
                        *new_max_cache_size_ptr = cache_ptr->resize_ctl.min_size;

                    /* clip to max decrement if necessary */
                    if ((cache_ptr->resize_ctl.apply_max_decrement) &&
                        ((cache_ptr->resize_ctl.max_decrement + *new_max_cache_size_ptr) <
                         cache_ptr->max_cache_size))
                        *new_max_cache_size_ptr =
                            cache_ptr->max_cache_size - cache_ptr->resize_ctl.max_decrement;
                }
            }
        }
        else
            *status_ptr = at_min_size;
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__autoadjust__ageout() */

/*-------------------------------------------------------------------------
 * Function:    H5C__autoadjust__ageout__cycle_epoch_marker
 *
 * Purpose:     Remove the oldest epoch marker from the LRU list,
 *        and reinsert it at the head of the LRU list.  Also
 *        remove the epoch marker's index from the head of the
 *        ring buffer, and re-insert it at the tail of the ring
 *        buffer.
 *
 * Return:      SUCCEED on success/FAIL on failure.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5C__autoadjust__ageout__cycle_epoch_marker(H5C_t *cache_ptr)
{
    int    i;
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(cache_ptr);

    if (cache_ptr->epoch_markers_active <= 0)
        HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "No active epoch markers on entry?!?!?");

    /* remove the last marker from both the ring buffer and the LRU list */
    i = cache_ptr->epoch_marker_ringbuf[cache_ptr->epoch_marker_ringbuf_first];
    cache_ptr->epoch_marker_ringbuf_first =
        (cache_ptr->epoch_marker_ringbuf_first + 1) % (H5C__MAX_EPOCH_MARKERS + 1);
    if (cache_ptr->epoch_marker_ringbuf_size <= 0)
        HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "ring buffer underflow");

    cache_ptr->epoch_marker_ringbuf_size -= 1;
    if (cache_ptr->epoch_marker_active[i] != true)
        HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "unused marker in LRU?!?");

    H5C__DLL_REMOVE((&((cache_ptr->epoch_markers)[i])), (cache_ptr)->LRU_head_ptr, (cache_ptr)->LRU_tail_ptr,
                    (cache_ptr)->LRU_list_len, (cache_ptr)->LRU_list_size, (FAIL))

    /* now, re-insert it at the head of the LRU list, and at the tail of
     * the ring buffer.
     */
    assert(cache_ptr->epoch_markers[i].addr == (haddr_t)i);
    assert(cache_ptr->epoch_markers[i].next == NULL);
    assert(cache_ptr->epoch_markers[i].prev == NULL);

    cache_ptr->epoch_marker_ringbuf_last =
        (cache_ptr->epoch_marker_ringbuf_last + 1) % (H5C__MAX_EPOCH_MARKERS + 1);
    cache_ptr->epoch_marker_ringbuf[cache_ptr->epoch_marker_ringbuf_last] = i;
    if (cache_ptr->epoch_marker_ringbuf_size >= H5C__MAX_EPOCH_MARKERS)
        HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "ring buffer overflow");

    cache_ptr->epoch_marker_ringbuf_size += 1;

    H5C__DLL_PREPEND(&(cache_ptr->epoch_markers[i]), cache_ptr->LRU_head_ptr, cache_ptr->LRU_tail_ptr,
                     cache_ptr->LRU_list_len, cache_ptr->LRU_list_size, FAIL)
done:

    FUNC_LEAVE_NOAPI(ret_value)

} /* H5C__autoadjust__ageout__cycle_epoch_marker() */

/*-------------------------------------------------------------------------
 * Function:    H5C__autoadjust__ageout__evict_aged_out_entries
 *
 * Purpose:     Evict clean entries in the cache that haven't
 *        been accessed for at least
 *        cache_ptr->resize_ctl.epochs_before_eviction epochs,
 *        and flush dirty entries that haven't been accessed for
 *        that amount of time.
 *
 *        Depending on configuration, the function will either
 *        flush or evict all such entries, or all such entries it
 *        encounters until it has freed the maximum amount of space
 *        allowed under the maximum decrement.
 *
 *        If we are running in parallel mode, writes may not be
 *        permitted.  If so, the function simply skips any dirty
 *        entries it may encounter.
 *
 *        The function makes no attempt to maintain the minimum
 *        clean size, as there is no guarantee that the cache size
 *        will be changed.
 *
 *        If there is no cache size change, the minimum clean size
 *        constraint will be met through a combination of clean
 *        entries and free space in the cache.
 *
 *        If there is a cache size reduction, the minimum clean size
 *        will be re-calculated, and will be enforced the next time
 *        we have to make space in the cache.
 *
 * Return:      Non-negative on success/Negative on failure.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5C__autoadjust__ageout__evict_aged_out_entries(H5F_t *f, bool write_permitted)
{
    H5C_t             *cache_ptr = f->shared->cache;
    size_t             eviction_size_limit;
    size_t             bytes_evicted = 0;
    bool               prev_is_dirty = false;
    bool               restart_scan;
    H5C_cache_entry_t *entry_ptr;
    H5C_cache_entry_t *next_ptr;
    H5C_cache_entry_t *prev_ptr;
    herr_t             ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(f);
    assert(cache_ptr);

    /* if there is a limit on the amount that the cache size can be decrease
     * in any one round of the cache size reduction algorithm, load that
     * limit into eviction_size_limit.  Otherwise, set eviction_size_limit
     * to the equivalent of infinity.  The current size of the index will
     * do nicely.
     */
    if (cache_ptr->resize_ctl.apply_max_decrement)
        eviction_size_limit = cache_ptr->resize_ctl.max_decrement;
    else
        eviction_size_limit = cache_ptr->index_size; /* i.e. infinity */

    if (write_permitted) {
        restart_scan = false;
        entry_ptr    = cache_ptr->LRU_tail_ptr;
        while (entry_ptr != NULL && entry_ptr->type->id != H5AC_EPOCH_MARKER_ID &&
               bytes_evicted < eviction_size_limit) {
            bool skipping_entry = false;

            assert(!(entry_ptr->is_protected));
            assert(!(entry_ptr->is_read_only));
            assert((entry_ptr->ro_ref_count) == 0);

            next_ptr = entry_ptr->next;
            prev_ptr = entry_ptr->prev;

            if (prev_ptr != NULL)
                prev_is_dirty = prev_ptr->is_dirty;

            if (entry_ptr->is_dirty) {
                assert(!entry_ptr->prefetched_dirty);

                /* dirty corked entry is skipped */
                if (entry_ptr->tag_info && entry_ptr->tag_info->corked)
                    skipping_entry = true;
                else {
                    /* reset entries_removed_counter and
                     * last_entry_removed_ptr prior to the call to
                     * H5C__flush_single_entry() so that we can spot
                     * unexpected removals of entries from the cache,
                     * and set the restart_scan flag if proceeding
                     * would be likely to cause us to scan an entry
                     * that is no longer in the cache.
                     */
                    cache_ptr->entries_removed_counter = 0;
                    cache_ptr->last_entry_removed_ptr  = NULL;

                    if (H5C__flush_single_entry(f, entry_ptr, H5C__NO_FLAGS_SET) < 0)
                        HGOTO_ERROR(H5E_CACHE, H5E_CANTFLUSH, FAIL, "unable to flush entry");

                    if (cache_ptr->entries_removed_counter > 1 ||
                        cache_ptr->last_entry_removed_ptr == prev_ptr)
                        restart_scan = true;
                } /* end else */
            }     /* end if */
            else if (!entry_ptr->prefetched_dirty) {
                bytes_evicted += entry_ptr->size;

                if (H5C__flush_single_entry(
                        f, entry_ptr, H5C__FLUSH_INVALIDATE_FLAG | H5C__DEL_FROM_SLIST_ON_DESTROY_FLAG) < 0)
                    HGOTO_ERROR(H5E_CACHE, H5E_CANTFLUSH, FAIL, "unable to flush entry");
            } /* end else-if */
            else {
                assert(!entry_ptr->is_dirty);
                assert(entry_ptr->prefetched_dirty);

                skipping_entry = true;
            } /* end else */

            if (prev_ptr != NULL) {
                if (skipping_entry)
                    entry_ptr = prev_ptr;
                else if (restart_scan || (prev_ptr->is_dirty != prev_is_dirty) ||
                         (prev_ptr->next != next_ptr) || (prev_ptr->is_protected) || (prev_ptr->is_pinned)) {
                    /* Something has happened to the LRU -- start over
                     * from the tail.
                     */
                    restart_scan = false;
                    entry_ptr    = cache_ptr->LRU_tail_ptr;

                    H5C__UPDATE_STATS_FOR_LRU_SCAN_RESTART(cache_ptr);
                } /* end else-if */
                else
                    entry_ptr = prev_ptr;
            } /* end if */
            else
                entry_ptr = NULL;
        } /* end while */

        /* for now at least, don't bother to maintain the minimum clean size,
         * as the cache should now be less than its maximum size.  Due to
         * the vaguries of the cache size reduction algorithm, we may not
         * reduce the size of the cache.
         *
         * If we do, we will calculate a new minimum clean size, which will
         * be enforced the next time we try to make space in the cache.
         *
         * If we don't, no action is necessary, as we have just evicted and/or
         * or flushed a bunch of entries and therefore the sum of the clean
         * and free space in the cache must be greater than or equal to the
         * min clean space requirement (assuming that requirement was met on
         * entry).
         */
    } /* end if */
    else /* ! write_permitted */ {
        /* Since we are not allowed to write, all we can do is evict
         * any clean entries that we may encounter before we either
         * hit the eviction size limit, or encounter the epoch marker.
         *
         * If we are operating read only, this isn't an issue, as there
         * will not be any dirty entries.
         *
         * If we are operating in R/W mode, all the dirty entries we
         * skip will be flushed the next time we attempt to make space
         * when writes are permitted.  This may have some local
         * performance implications, but it shouldn't cause any net
         * slowdown.
         */
        assert(H5C_MAINTAIN_CLEAN_AND_DIRTY_LRU_LISTS);
        entry_ptr = cache_ptr->LRU_tail_ptr;
        while (entry_ptr != NULL && ((entry_ptr->type)->id != H5AC_EPOCH_MARKER_ID) &&
               (bytes_evicted < eviction_size_limit)) {
            assert(!(entry_ptr->is_protected));

            prev_ptr = entry_ptr->prev;

            if (!(entry_ptr->is_dirty) && !(entry_ptr->prefetched_dirty))
                if (H5C__flush_single_entry(
                        f, entry_ptr, H5C__FLUSH_INVALIDATE_FLAG | H5C__DEL_FROM_SLIST_ON_DESTROY_FLAG) < 0)
                    HGOTO_ERROR(H5E_CACHE, H5E_CANTFLUSH, FAIL, "unable to flush clean entry");

            /* just skip the entry if it is dirty, as we can't do
             * anything with it now since we can't write.
             *
             * Since all entries are clean, serialize() will not be called,
             * and thus we needn't test to see if the LRU has been changed
             * out from under us.
             */
            entry_ptr = prev_ptr;
        } /* end while */
    }     /* end else */

    if (cache_ptr->index_size < cache_ptr->max_cache_size)
        cache_ptr->cache_full = false;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__autoadjust__ageout__evict_aged_out_entries() */

/*-------------------------------------------------------------------------
 * Function:    H5C__autoadjust__ageout__insert_new_marker
 *
 * Purpose:     Find an unused marker cache entry, mark it as used, and
 *        insert it at the head of the LRU list.  Also add the
 *        marker's index in the epoch_markers array.
 *
 * Return:      SUCCEED on success/FAIL on failure.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5C__autoadjust__ageout__insert_new_marker(H5C_t *cache_ptr)
{
    int    i;
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(cache_ptr);

    if (cache_ptr->epoch_markers_active >= cache_ptr->resize_ctl.epochs_before_eviction)
        HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "Already have a full complement of markers");

    /* find an unused marker */
    i = 0;
    while ((cache_ptr->epoch_marker_active)[i] && i < H5C__MAX_EPOCH_MARKERS)
        i++;
    if (i >= H5C__MAX_EPOCH_MARKERS)
        HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "Can't find unused marker");

    assert(((cache_ptr->epoch_markers)[i]).addr == (haddr_t)i);
    assert(((cache_ptr->epoch_markers)[i]).next == NULL);
    assert(((cache_ptr->epoch_markers)[i]).prev == NULL);

    (cache_ptr->epoch_marker_active)[i] = true;

    cache_ptr->epoch_marker_ringbuf_last =
        (cache_ptr->epoch_marker_ringbuf_last + 1) % (H5C__MAX_EPOCH_MARKERS + 1);
    (cache_ptr->epoch_marker_ringbuf)[cache_ptr->epoch_marker_ringbuf_last] = i;
    if (cache_ptr->epoch_marker_ringbuf_size >= H5C__MAX_EPOCH_MARKERS)
        HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "ring buffer overflow");

    cache_ptr->epoch_marker_ringbuf_size += 1;

    H5C__DLL_PREPEND(&(cache_ptr->epoch_markers[i]), cache_ptr->LRU_head_ptr, cache_ptr->LRU_tail_ptr,
                     cache_ptr->LRU_list_len, cache_ptr->LRU_list_size, FAIL)

    cache_ptr->epoch_markers_active += 1;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__autoadjust__ageout__insert_new_marker() */

/*-------------------------------------------------------------------------
 * Function:    H5C__autoadjust__ageout__remove_all_markers
 *
 * Purpose:     Remove all epoch markers from the LRU list and mark them
 *              as inactive.
 *
 * Return:      SUCCEED on success/FAIL on failure.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C__autoadjust__ageout__remove_all_markers(H5C_t *cache_ptr)
{
    int    ring_buf_index;
    int    i;
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(cache_ptr);

    while (cache_ptr->epoch_markers_active > 0) {
        /* get the index of the last epoch marker in the LRU list
         * and remove it from the ring buffer.
         */

        ring_buf_index = cache_ptr->epoch_marker_ringbuf_first;
        i              = (cache_ptr->epoch_marker_ringbuf)[ring_buf_index];

        cache_ptr->epoch_marker_ringbuf_first =
            (cache_ptr->epoch_marker_ringbuf_first + 1) % (H5C__MAX_EPOCH_MARKERS + 1);

        if (cache_ptr->epoch_marker_ringbuf_size <= 0)
            HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "ring buffer underflow");
        cache_ptr->epoch_marker_ringbuf_size -= 1;

        if (cache_ptr->epoch_marker_active[i] != true)
            HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "unused marker in LRU?!?");

        /* remove the epoch marker from the LRU list */
        H5C__DLL_REMOVE(&(cache_ptr->epoch_markers[i]), cache_ptr->LRU_head_ptr, cache_ptr->LRU_tail_ptr,
                        cache_ptr->LRU_list_len, cache_ptr->LRU_list_size, FAIL)

        /* mark the epoch marker as unused. */
        cache_ptr->epoch_marker_active[i] = false;

        assert(cache_ptr->epoch_markers[i].addr == (haddr_t)i);
        assert(cache_ptr->epoch_markers[i].next == NULL);
        assert(cache_ptr->epoch_markers[i].prev == NULL);

        /* decrement the number of active epoch markers */
        cache_ptr->epoch_markers_active -= 1;

        assert(cache_ptr->epoch_markers_active == cache_ptr->epoch_marker_ringbuf_size);
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__autoadjust__ageout__remove_all_markers() */

/*-------------------------------------------------------------------------
 * Function:    H5C__autoadjust__ageout__remove_excess_markers
 *
 * Purpose:     Remove epoch markers from the end of the LRU list and
 *        mark them as inactive until the number of active markers
 *        equals the current value of
 *        cache_ptr->resize_ctl.epochs_before_eviction.
 *
 * Return:      SUCCEED on success/FAIL on failure.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C__autoadjust__ageout__remove_excess_markers(H5C_t *cache_ptr)
{
    int    ring_buf_index;
    int    i;
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(cache_ptr);

    if (cache_ptr->epoch_markers_active <= cache_ptr->resize_ctl.epochs_before_eviction)
        HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "no excess markers on entry");

    while (cache_ptr->epoch_markers_active > cache_ptr->resize_ctl.epochs_before_eviction) {
        /* get the index of the last epoch marker in the LRU list
         * and remove it from the ring buffer.
         */
        ring_buf_index = cache_ptr->epoch_marker_ringbuf_first;
        i              = (cache_ptr->epoch_marker_ringbuf)[ring_buf_index];

        cache_ptr->epoch_marker_ringbuf_first =
            (cache_ptr->epoch_marker_ringbuf_first + 1) % (H5C__MAX_EPOCH_MARKERS + 1);

        if (cache_ptr->epoch_marker_ringbuf_size <= 0)
            HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "ring buffer underflow");
        cache_ptr->epoch_marker_ringbuf_size -= 1;

        if (cache_ptr->epoch_marker_active[i] != true)
            HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "unused marker in LRU?!?");

        /* remove the epoch marker from the LRU list */
        H5C__DLL_REMOVE(&(cache_ptr->epoch_markers[i]), cache_ptr->LRU_head_ptr, cache_ptr->LRU_tail_ptr,
                        cache_ptr->LRU_list_len, cache_ptr->LRU_list_size, FAIL)

        /* mark the epoch marker as unused. */
        cache_ptr->epoch_marker_active[i] = false;

        assert(cache_ptr->epoch_markers[i].addr == (haddr_t)i);
        assert(cache_ptr->epoch_markers[i].next == NULL);
        assert(cache_ptr->epoch_markers[i].prev == NULL);

        /* decrement the number of active epoch markers */
        cache_ptr->epoch_markers_active -= 1;

        assert(cache_ptr->epoch_markers_active == cache_ptr->epoch_marker_ringbuf_size);
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__autoadjust__ageout__remove_excess_markers() */

/*-------------------------------------------------------------------------
 * Function:    H5C__flash_increase_cache_size
 *
 * Purpose:     If there is not at least new_entry_size - old_entry_size
 *              bytes of free space in the cache and the current
 *              max_cache_size is less than cache_ptr->resize_ctl.max_size,
 *              perform a flash increase in the cache size and then reset
 *              the full cache hit rate statistics, and exit.
 *
 * Return:      Non-negative on success/Negative on failure.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C__flash_increase_cache_size(H5C_t *cache_ptr, size_t old_entry_size, size_t new_entry_size)
{
    size_t                 new_max_cache_size = 0;
    size_t                 old_max_cache_size = 0;
    size_t                 new_min_clean_size = 0;
    size_t                 old_min_clean_size = 0;
    size_t                 space_needed;
    enum H5C_resize_status status = flash_increase; /* may change */
    double                 hit_rate;
    herr_t                 ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(cache_ptr);
    assert(cache_ptr->flash_size_increase_possible);
    assert(new_entry_size > cache_ptr->flash_size_increase_threshold);
    assert(old_entry_size < new_entry_size);

    if (old_entry_size >= new_entry_size)
        HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "old_entry_size >= new_entry_size");

    space_needed = new_entry_size - old_entry_size;
    if (((cache_ptr->index_size + space_needed) > cache_ptr->max_cache_size) &&
        (cache_ptr->max_cache_size < cache_ptr->resize_ctl.max_size)) {
        switch (cache_ptr->resize_ctl.flash_incr_mode) {
            case H5C_flash_incr__off:
                HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL,
                            "flash_size_increase_possible but H5C_flash_incr__off?!");
                break;

            case H5C_flash_incr__add_space:
                if (cache_ptr->index_size < cache_ptr->max_cache_size) {
                    assert((cache_ptr->max_cache_size - cache_ptr->index_size) < space_needed);
                    space_needed -= cache_ptr->max_cache_size - cache_ptr->index_size;
                }
                space_needed       = (size_t)(((double)space_needed) * cache_ptr->resize_ctl.flash_multiple);
                new_max_cache_size = cache_ptr->max_cache_size + space_needed;
                break;

            default: /* should be unreachable */
                HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "Unknown flash_incr_mode?!?!?");
                break;
        }

        if (new_max_cache_size > cache_ptr->resize_ctl.max_size)
            new_max_cache_size = cache_ptr->resize_ctl.max_size;
        assert(new_max_cache_size > cache_ptr->max_cache_size);

        new_min_clean_size = (size_t)((double)new_max_cache_size * cache_ptr->resize_ctl.min_clean_fraction);
        assert(new_min_clean_size <= new_max_cache_size);

        old_max_cache_size = cache_ptr->max_cache_size;
        old_min_clean_size = cache_ptr->min_clean_size;

        cache_ptr->max_cache_size = new_max_cache_size;
        cache_ptr->min_clean_size = new_min_clean_size;

        /* update flash cache size increase fields as appropriate */
        assert(cache_ptr->flash_size_increase_possible);

        switch (cache_ptr->resize_ctl.flash_incr_mode) {
            case H5C_flash_incr__off:
                HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL,
                            "flash_size_increase_possible but H5C_flash_incr__off?!");
                break;

            case H5C_flash_incr__add_space:
                cache_ptr->flash_size_increase_threshold =
                    (size_t)((double)cache_ptr->max_cache_size * cache_ptr->resize_ctl.flash_threshold);
                break;

            default: /* should be unreachable */
                HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "Unknown flash_incr_mode?!?!?");
                break;
        }

        /* note that we don't cycle the epoch markers.  We can
         * argue either way as to whether we should, but for now
         * we don't.
         */

        if (cache_ptr->resize_ctl.rpt_fcn != NULL) {
            /* get the hit rate for the reporting function.  Should still
             * be good as we haven't reset the hit rate statistics.
             */
            if (H5C_get_cache_hit_rate(cache_ptr, &hit_rate) != SUCCEED)
                HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "Can't get hit rate");

            (cache_ptr->resize_ctl.rpt_fcn)(cache_ptr, H5C__CURR_AUTO_RESIZE_RPT_FCN_VER, hit_rate, status,
                                            old_max_cache_size, new_max_cache_size, old_min_clean_size,
                                            new_min_clean_size);
        }

        if (H5C_reset_cache_hit_rate_stats(cache_ptr) < 0)
            /* this should be impossible... */
            HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "H5C_reset_cache_hit_rate_stats failed");
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__flash_increase_cache_size() */

/*-------------------------------------------------------------------------
 * Function:    H5C__flush_invalidate_cache
 *
 * Purpose:    Flush and destroy the entries contained in the target
 *        cache.
 *
 *        If the cache contains protected entries, the function will
 *        fail, as protected entries cannot be either flushed or
 *        destroyed.  However all unprotected entries should be
 *        flushed and destroyed before the function returns failure.
 *
 *        While pinned entries can usually be flushed, they cannot
 *        be destroyed.  However, they should be unpinned when all
 *        the entries that reference them have been destroyed (thus
 *        reduding the pinned entry's reference count to 0, allowing
 *        it to be unpinned).
 *
 *        If pinned entries are present, the function makes repeated
 *        passes through the cache, flushing all dirty entries
 *        (including the pinned dirty entries where permitted) and
 *        destroying all unpinned entries.  This process is repeated
 *        until either the cache is empty, or the number of pinned
 *        entries stops decreasing on each pass.
 *
 * Return:      Non-negative on success/Negative on failure or if there was
 *        a request to flush all items and something was protected.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C__flush_invalidate_cache(H5F_t *f, unsigned flags)
{
    H5C_t     *cache_ptr;
    H5C_ring_t ring;
    herr_t     ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(f);
    assert(f->shared);
    cache_ptr = f->shared->cache;
    assert(cache_ptr);
    assert(cache_ptr->slist_ptr);
    assert(cache_ptr->slist_enabled);

#ifdef H5C_DO_SANITY_CHECKS
    {
        int32_t  i;
        uint32_t index_len        = 0;
        uint32_t slist_len        = 0;
        size_t   index_size       = (size_t)0;
        size_t   clean_index_size = (size_t)0;
        size_t   dirty_index_size = (size_t)0;
        size_t   slist_size       = (size_t)0;

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
    }
#endif /* H5C_DO_SANITY_CHECKS */

    /* remove ageout markers if present */
    if (cache_ptr->epoch_markers_active > 0)
        if (H5C__autoadjust__ageout__remove_all_markers(cache_ptr) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "error removing all epoch markers");

    /* flush invalidate each ring, starting from the outermost ring and
     * working inward.
     */
    ring = H5C_RING_USER;
    while (ring < H5C_RING_NTYPES) {
        if (H5C__flush_invalidate_ring(f, ring, flags) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_CANTFLUSH, FAIL, "flush invalidate ring failed");
        ring++;
    } /* end while */

#ifndef NDEBUG
    /* Invariants, after destroying all entries in the hash table */
    if (!(flags & H5C__EVICT_ALLOW_LAST_PINS_FLAG)) {
        assert(cache_ptr->index_size == 0);
        assert(cache_ptr->clean_index_size == 0);
        assert(cache_ptr->pel_len == 0);
        assert(cache_ptr->pel_size == 0);
    } /* end if */
    else {
        H5C_cache_entry_t *entry_ptr; /* Cache entry */
        unsigned           u;         /* Local index variable */

        /* All rings except ring 4 should be empty now */
        /* (Ring 4 has the superblock) */
        for (u = H5C_RING_USER; u < H5C_RING_SB; u++) {
            assert(cache_ptr->index_ring_len[u] == 0);
            assert(cache_ptr->index_ring_size[u] == 0);
            assert(cache_ptr->clean_index_ring_size[u] == 0);
        } /* end for */

        /* Check that any remaining pinned entries are in the superblock ring */
        entry_ptr = cache_ptr->pel_head_ptr;
        while (entry_ptr) {
            /* Check ring */
            assert(entry_ptr->ring == H5C_RING_SB);

            /* Advance to next entry in pinned entry list */
            entry_ptr = entry_ptr->next;
        } /* end while */
    }     /* end else */

    assert(cache_ptr->dirty_index_size == 0);
    assert(cache_ptr->slist_len == 0);
    assert(cache_ptr->slist_size == 0);
    assert(cache_ptr->pl_len == 0);
    assert(cache_ptr->pl_size == 0);
    assert(cache_ptr->LRU_list_len == 0);
    assert(cache_ptr->LRU_list_size == 0);
#endif

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__flush_invalidate_cache() */

/*-------------------------------------------------------------------------
 * Function:    H5C__flush_invalidate_ring
 *
 * Purpose:     Flush and destroy the entries contained in the target
 *              cache and ring.
 *
 *              If the ring contains protected entries, the function will
 *              fail, as protected entries cannot be either flushed or
 *              destroyed.  However all unprotected entries should be
 *              flushed and destroyed before the function returns failure.
 *
 *              While pinned entries can usually be flushed, they cannot
 *              be destroyed.  However, they should be unpinned when all
 *              the entries that reference them have been destroyed (thus
 *              reduding the pinned entry's reference count to 0, allowing
 *              it to be unpinned).
 *
 *              If pinned entries are present, the function makes repeated
 *              passes through the cache, flushing all dirty entries
 *              (including the pinned dirty entries where permitted) and
 *              destroying all unpinned entries.  This process is repeated
 *              until either the cache is empty, or the number of pinned
 *              entries stops decreasing on each pass.
 *
 *              If flush dependencies appear in the target ring, the
 *              function makes repeated passes through the cache flushing
 *              entries in flush dependency order.
 *
 * Return:      Non-negative on success/Negative on failure or if there was
 *              a request to flush all items and something was protected.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5C__flush_invalidate_ring(H5F_t *f, H5C_ring_t ring, unsigned flags)
{
    H5C_t             *cache_ptr;
    bool               restart_slist_scan;
    uint32_t           protected_entries = 0;
    int32_t            i;
    uint32_t           cur_ring_pel_len;
    uint32_t           old_ring_pel_len;
    unsigned           cooked_flags;
    unsigned           evict_flags;
    H5SL_node_t       *node_ptr       = NULL;
    H5C_cache_entry_t *entry_ptr      = NULL;
    H5C_cache_entry_t *next_entry_ptr = NULL;
#ifdef H5C_DO_SANITY_CHECKS
    uint32_t initial_slist_len  = 0;
    size_t   initial_slist_size = 0;
#endif /* H5C_DO_SANITY_CHECKS */
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(f);
    assert(f->shared);
    cache_ptr = f->shared->cache;
    assert(cache_ptr);
    assert(cache_ptr->slist_enabled);
    assert(cache_ptr->slist_ptr);
    assert(ring > H5C_RING_UNDEFINED);
    assert(ring < H5C_RING_NTYPES);

    assert(cache_ptr->epoch_markers_active == 0);

    /* Filter out the flags that are not relevant to the flush/invalidate.
     */
    cooked_flags = flags & H5C__FLUSH_CLEAR_ONLY_FLAG;
    evict_flags  = flags & H5C__EVICT_ALLOW_LAST_PINS_FLAG;

    /* The flush procedure here is a bit strange.
     *
     * In the outer while loop we make at least one pass through the
     * cache, and then repeat until either all the pinned entries in
     * the ring unpin themselves, or until the number of pinned entries
     * in the ring stops declining.  In this later case, we scream and die.
     *
     * Since the fractal heap can dirty, resize, and/or move entries
     * in is flush callback, it is possible that the cache will still
     * contain dirty entries at this point.  If so, we must make more
     * passes through the skip list to allow it to empty.
     *
     * Further, since clean entries can be dirtied, resized, and/or moved
     * as the result of a flush call back (either the entries own, or that
     * for some other cache entry), we can no longer promise to flush
     * the cache entries in increasing address order.
     *
     * Instead, we make a pass through
     * the skip list, and then a pass through the "clean" entries, and
     * then repeating as needed.  Thus it is quite possible that an
     * entry will be evicted from the cache only to be re-loaded later
     * in the flush process.
     *
     * The bottom line is that entries will probably be flushed in close
     * to increasing address order, but there are no guarantees.
     */

    /* compute the number of pinned entries in this ring */
    entry_ptr        = cache_ptr->pel_head_ptr;
    cur_ring_pel_len = 0;
    while (entry_ptr != NULL) {
        assert(entry_ptr->ring >= ring);
        if (entry_ptr->ring == ring)
            cur_ring_pel_len++;

        entry_ptr = entry_ptr->next;
    } /* end while */
    old_ring_pel_len = cur_ring_pel_len;

    while (cache_ptr->index_ring_len[ring] > 0) {
        /* first, try to flush-destroy any dirty entries.   Do this by
         * making a scan through the slist.  Note that new dirty entries
         * may be created by the flush call backs.  Thus it is possible
         * that the slist will not be empty after we finish the scan.
         */

#ifdef H5C_DO_SANITY_CHECKS
        /* Depending on circumstances, H5C__flush_single_entry() will
         * remove dirty entries from the slist as it flushes them.
         * Thus for sanity checks we must make note of the initial
         * slist length and size before we do any flushes.
         */
        initial_slist_len  = cache_ptr->slist_len;
        initial_slist_size = cache_ptr->slist_size;

        /* There is also the possibility that entries will be
         * dirtied, resized, moved, and/or removed from the cache
         * as the result of calls to the flush callbacks.  We use
         * the slist_len_increase and slist_size_increase increase
         * fields in struct H5C_t to track these changes for purpose
         * of sanity checking.
         *
         * To this end, we must zero these fields before we start
         * the pass through the slist.
         */
        cache_ptr->slist_len_increase  = 0;
        cache_ptr->slist_size_increase = 0;
#endif /* H5C_DO_SANITY_CHECKS */

        /* Set the cache_ptr->slist_changed to false.
         *
         * This flag is set to true by H5C__flush_single_entry if the slist
         * is modified by a pre_serialize, serialize, or notify callback.
         *
         * H5C__flush_invalidate_ring() uses this flag to detect any
         * modifications to the slist that might corrupt the scan of
         * the slist -- and restart the scan in this event.
         */
        cache_ptr->slist_changed = false;

        /* this done, start the scan of the slist */
        restart_slist_scan = true;
        while (restart_slist_scan || (node_ptr != NULL)) {
            if (restart_slist_scan) {
                restart_slist_scan = false;

                /* Start at beginning of skip list */
                node_ptr = H5SL_first(cache_ptr->slist_ptr);
                if (node_ptr == NULL)
                    /* the slist is empty -- break out of inner loop */
                    break;

                /* Get cache entry for this node */
                next_entry_ptr = (H5C_cache_entry_t *)H5SL_item(node_ptr);
                if (NULL == next_entry_ptr)
                    HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "next_entry_ptr == NULL ?!?!");

                assert(next_entry_ptr->is_dirty);
                assert(next_entry_ptr->in_slist);
                assert(next_entry_ptr->ring >= ring);
            } /* end if */

            entry_ptr = next_entry_ptr;

            /* It is possible that entries will be dirtied, resized,
             * flushed, or removed from the cache via the take ownership
             * flag as the result of pre_serialize or serialized callbacks.
             *
             * This in turn can corrupt the scan through the slist.
             *
             * We test for slist modifications in the pre_serialize
             * and serialize callbacks, and restart the scan of the
             * slist if we find them.  However, best we do some extra
             * sanity checking just in case.
             */
            assert(entry_ptr != NULL);
            assert(entry_ptr->in_slist);
            assert(entry_ptr->is_dirty);
            assert(entry_ptr->ring >= ring);

            /* increment node pointer now, before we delete its target
             * from the slist.
             */
            node_ptr = H5SL_next(node_ptr);
            if (node_ptr != NULL) {
                next_entry_ptr = (H5C_cache_entry_t *)H5SL_item(node_ptr);
                if (NULL == next_entry_ptr)
                    HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "next_entry_ptr == NULL ?!?!");

                assert(next_entry_ptr->is_dirty);
                assert(next_entry_ptr->in_slist);
                assert(next_entry_ptr->ring >= ring);
                assert(entry_ptr != next_entry_ptr);
            } /* end if */
            else
                next_entry_ptr = NULL;

            /* Note that we now remove nodes from the slist as we flush
             * the associated entries, instead of leaving them there
             * until we are done, and then destroying all nodes in
             * the slist.
             *
             * While this optimization used to be easy, with the possibility
             * of new entries being added to the slist in the midst of the
             * flush, we must keep the slist in canonical form at all
             * times.
             */
            if (((!entry_ptr->flush_me_last) ||
                 ((entry_ptr->flush_me_last) && (cache_ptr->num_last_entries >= cache_ptr->slist_len))) &&
                (entry_ptr->flush_dep_nchildren == 0) && (entry_ptr->ring == ring)) {
                if (entry_ptr->is_protected) {
                    /* We have major problems -- but lets flush
                     * everything we can before we flag an error.
                     */
                    protected_entries++;
                } /* end if */
                else if (entry_ptr->is_pinned) {
                    if (H5C__flush_single_entry(f, entry_ptr, H5C__DURING_FLUSH_FLAG) < 0)
                        HGOTO_ERROR(H5E_CACHE, H5E_CANTFLUSH, FAIL, "dirty pinned entry flush failed");

                    if (cache_ptr->slist_changed) {
                        /* The slist has been modified by something
                         * other than the simple removal of the
                         * of the flushed entry after the flush.
                         *
                         * This has the potential to corrupt the
                         * scan through the slist, so restart it.
                         */
                        restart_slist_scan       = true;
                        cache_ptr->slist_changed = false;
                        H5C__UPDATE_STATS_FOR_SLIST_SCAN_RESTART(cache_ptr);
                    } /* end if */
                }     /* end else-if */
                else {
                    if (H5C__flush_single_entry(f, entry_ptr,
                                                (cooked_flags | H5C__DURING_FLUSH_FLAG |
                                                 H5C__FLUSH_INVALIDATE_FLAG |
                                                 H5C__DEL_FROM_SLIST_ON_DESTROY_FLAG)) < 0)
                        HGOTO_ERROR(H5E_CACHE, H5E_CANTFLUSH, FAIL, "dirty entry flush destroy failed");

                    if (cache_ptr->slist_changed) {
                        /* The slist has been modified by something
                         * other than the simple removal of the
                         * of the flushed entry after the flush.
                         *
                         * This has the potential to corrupt the
                         * scan through the slist, so restart it.
                         */
                        restart_slist_scan       = true;
                        cache_ptr->slist_changed = false;
                        H5C__UPDATE_STATS_FOR_SLIST_SCAN_RESTART(cache_ptr);
                    } /* end if */
                }     /* end else */
            }         /* end if */
        }             /* end while loop scanning skip list */

#ifdef H5C_DO_SANITY_CHECKS
        /* It is possible that entries were added to the slist during
         * the scan, either before or after scan pointer.  The following
         * asserts take this into account.
         *
         * Don't bother with the sanity checks if node_ptr != NULL, as
         * in this case we broke out of the loop because it got changed
         * out from under us.
         */

        if (node_ptr == NULL) {
            assert(cache_ptr->slist_len ==
                   (uint32_t)((int32_t)initial_slist_len + cache_ptr->slist_len_increase));
            assert(cache_ptr->slist_size ==
                   (size_t)((ssize_t)initial_slist_size + cache_ptr->slist_size_increase));
        } /* end if */
#endif    /* H5C_DO_SANITY_CHECKS */

        /* Since we are doing a destroy, we must make a pass through
         * the hash table and try to flush - destroy all entries that
         * remain.
         *
         * It used to be that all entries remaining in the cache at
         * this point had to be clean, but with the fractal heap mods
         * this may not be the case.  If so, we will flush entries out
         * in increasing address order.
         *
         * Writes to disk are possible here.
         */

        /* Reset the counters so that we can detect insertions, loads,
         * and moves caused by the pre_serialize and serialize calls.
         */
        cache_ptr->entries_loaded_counter    = 0;
        cache_ptr->entries_inserted_counter  = 0;
        cache_ptr->entries_relocated_counter = 0;

        next_entry_ptr = cache_ptr->il_head;
        while (next_entry_ptr != NULL) {
            entry_ptr = next_entry_ptr;
            assert(entry_ptr->ring >= ring);

            next_entry_ptr = entry_ptr->il_next;

            if (((!entry_ptr->flush_me_last) ||
                 (entry_ptr->flush_me_last && (cache_ptr->num_last_entries >= cache_ptr->slist_len))) &&
                (entry_ptr->flush_dep_nchildren == 0) && (entry_ptr->ring == ring)) {

                if (entry_ptr->is_protected) {
                    /* we have major problems -- but lets flush and
                     * destroy everything we can before we flag an
                     * error.
                     */
                    protected_entries++;

                    if (!entry_ptr->in_slist)
                        assert(!(entry_ptr->is_dirty));
                } /* end if */
                else if (!entry_ptr->is_pinned) {
                    /* if *entry_ptr is dirty, it is possible
                     * that one or more other entries may be
                     * either removed from the cache, loaded
                     * into the cache, or moved to a new location
                     * in the file as a side effect of the flush.
                     *
                     * It's also possible that removing a clean
                     * entry will remove the last child of a proxy
                     * entry, allowing it to be removed also and
                     * invalidating the next_entry_ptr.
                     *
                     * If either of these happen, and one of the target
                     * or proxy entries happens to be the next entry in
                     * the hash bucket, we could either find ourselves
                     * either scanning a non-existent entry, scanning
                     * through a different bucket, or skipping an entry.
                     *
                     * Neither of these are good, so restart the
                     * the scan at the head of the hash bucket
                     * after the flush if we detect that the next_entry_ptr
                     * becomes invalid.
                     *
                     * This is not as inefficient at it might seem,
                     * as hash buckets typically have at most two
                     * or three entries.
                     */
                    cache_ptr->entry_watched_for_removal = next_entry_ptr;
                    if (H5C__flush_single_entry(f, entry_ptr,
                                                (cooked_flags | H5C__DURING_FLUSH_FLAG |
                                                 H5C__FLUSH_INVALIDATE_FLAG |
                                                 H5C__DEL_FROM_SLIST_ON_DESTROY_FLAG)) < 0)
                        HGOTO_ERROR(H5E_CACHE, H5E_CANTFLUSH, FAIL, "Entry flush destroy failed");

                    /* Restart the index list scan if necessary.  Must
                     * do this if the next entry is evicted, and also if
                     * one or more entries are inserted, loaded, or moved
                     * as these operations can result in part of the scan
                     * being skipped -- which can cause a spurious failure
                     * if this results in the size of the pinned entry
                     * failing to decline during the pass.
                     */
                    if (((NULL != next_entry_ptr) && (NULL == cache_ptr->entry_watched_for_removal)) ||
                        (cache_ptr->entries_loaded_counter > 0) ||
                        (cache_ptr->entries_inserted_counter > 0) ||
                        (cache_ptr->entries_relocated_counter > 0)) {

                        next_entry_ptr = cache_ptr->il_head;

                        cache_ptr->entries_loaded_counter    = 0;
                        cache_ptr->entries_inserted_counter  = 0;
                        cache_ptr->entries_relocated_counter = 0;

                        H5C__UPDATE_STATS_FOR_INDEX_SCAN_RESTART(cache_ptr);
                    } /* end if */
                    else
                        cache_ptr->entry_watched_for_removal = NULL;
                } /* end if */
            }     /* end if */
        }         /* end for loop scanning hash table */

        /* We can't do anything if entries are pinned.  The
         * hope is that the entries will be unpinned as the
         * result of destroys of entries that reference them.
         *
         * We detect this by noting the change in the number
         * of pinned entries from pass to pass.  If it stops
         * shrinking before it hits zero, we scream and die.
         */
        old_ring_pel_len = cur_ring_pel_len;
        entry_ptr        = cache_ptr->pel_head_ptr;
        cur_ring_pel_len = 0;

        while (entry_ptr != NULL) {
            assert(entry_ptr->ring >= ring);

            if (entry_ptr->ring == ring)
                cur_ring_pel_len++;

            entry_ptr = entry_ptr->next;
        } /* end while */

        /* Check if the number of pinned entries in the ring is positive, and
         * it is not declining.  Scream and die if so.
         */
        if ((cur_ring_pel_len > 0) && (cur_ring_pel_len >= old_ring_pel_len)) {
            /* Don't error if allowed to have pinned entries remaining */
            if (evict_flags)
                HGOTO_DONE(true);

            HGOTO_ERROR(
                H5E_CACHE, H5E_CANTFLUSH, FAIL,
                "Pinned entry count not decreasing, cur_ring_pel_len = %d, old_ring_pel_len = %d, ring = %d",
                (int)cur_ring_pel_len, (int)old_ring_pel_len, (int)ring);
        } /* end if */

        assert(protected_entries == cache_ptr->pl_len);

        if ((protected_entries > 0) && (protected_entries == cache_ptr->index_len))
            HGOTO_ERROR(H5E_CACHE, H5E_CANTFLUSH, FAIL,
                        "Only protected entries left in cache, protected_entries = %d",
                        (int)protected_entries);
    } /* main while loop */

    /* Invariants, after destroying all entries in the ring */
    for (i = (int)H5C_RING_UNDEFINED; i <= (int)ring; i++) {
        assert(cache_ptr->index_ring_len[i] == 0);
        assert(cache_ptr->index_ring_size[i] == (size_t)0);
        assert(cache_ptr->clean_index_ring_size[i] == (size_t)0);
        assert(cache_ptr->dirty_index_ring_size[i] == (size_t)0);

        assert(cache_ptr->slist_ring_len[i] == 0);
        assert(cache_ptr->slist_ring_size[i] == (size_t)0);
    } /* end for */

    assert(protected_entries <= cache_ptr->pl_len);

    if (protected_entries > 0)
        HGOTO_ERROR(H5E_CACHE, H5E_CANTFLUSH, FAIL, "Cache has protected entries");
    else if (cur_ring_pel_len > 0)
        HGOTO_ERROR(H5E_CACHE, H5E_CANTFLUSH, FAIL, "Can't unpin all pinned entries in ring");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__flush_invalidate_ring() */

/*-------------------------------------------------------------------------
 * Function:    H5C__flush_ring
 *
 * Purpose:     Flush the entries contained in the specified cache and
 *              ring.  All entries in rings outside the specified ring
 *              must have been flushed on entry.
 *
 *              If the cache contains protected entries in the specified
 *              ring, the function will fail, as protected entries cannot
 *              be flushed.  However all unprotected entries in the target
 *              ring should be flushed before the function returns failure.
 *
 *              If flush dependencies appear in the target ring, the
 *              function makes repeated passes through the slist flushing
 *              entries in flush dependency order.
 *
 * Return:      Non-negative on success/Negative on failure or if there was
 *              a request to flush all items and something was protected.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C__flush_ring(H5F_t *f, H5C_ring_t ring, unsigned flags)
{
    H5C_t             *cache_ptr = f->shared->cache;
    bool               flushed_entries_last_pass;
    bool               flush_marked_entries;
    bool               ignore_protected;
    bool               tried_to_flush_protected_entry = false;
    bool               restart_slist_scan;
    uint32_t           protected_entries = 0;
    H5SL_node_t       *node_ptr          = NULL;
    H5C_cache_entry_t *entry_ptr         = NULL;
    H5C_cache_entry_t *next_entry_ptr    = NULL;
#ifdef H5C_DO_SANITY_CHECKS
    uint32_t initial_slist_len  = 0;
    size_t   initial_slist_size = 0;
#endif /* H5C_DO_SANITY_CHECKS */
    int    i;
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(cache_ptr);
    assert(cache_ptr->slist_enabled);
    assert(cache_ptr->slist_ptr);
    assert((flags & H5C__FLUSH_INVALIDATE_FLAG) == 0);
    assert(ring > H5C_RING_UNDEFINED);
    assert(ring < H5C_RING_NTYPES);

#ifdef H5C_DO_EXTREME_SANITY_CHECKS
    if (H5C__validate_protected_entry_list(cache_ptr) < 0 || H5C__validate_pinned_entry_list(cache_ptr) < 0 ||
        H5C__validate_lru_list(cache_ptr) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "an extreme sanity check failed on entry");
#endif /* H5C_DO_EXTREME_SANITY_CHECKS */

    ignore_protected     = ((flags & H5C__FLUSH_IGNORE_PROTECTED_FLAG) != 0);
    flush_marked_entries = ((flags & H5C__FLUSH_MARKED_ENTRIES_FLAG) != 0);

    if (!flush_marked_entries)
        for (i = (int)H5C_RING_UNDEFINED; i < (int)ring; i++)
            assert(cache_ptr->slist_ring_len[i] == 0);

    assert(cache_ptr->flush_in_progress);

    /* When we are only flushing marked entries, the slist will usually
     * still contain entries when we have flushed everything we should.
     * Thus we track whether we have flushed any entries in the last
     * pass, and terminate if we haven't.
     */
    flushed_entries_last_pass = true;

    /* Set the cache_ptr->slist_changed to false.
     *
     * This flag is set to true by H5C__flush_single_entry if the
     * slist is modified by a pre_serialize, serialize, or notify callback.
     * H5C_flush_cache uses this flag to detect any modifications
     * to the slist that might corrupt the scan of the slist -- and
     * restart the scan in this event.
     */
    cache_ptr->slist_changed = false;

    while ((cache_ptr->slist_ring_len[ring] > 0) && (protected_entries == 0) && (flushed_entries_last_pass)) {
        flushed_entries_last_pass = false;

#ifdef H5C_DO_SANITY_CHECKS
        /* For sanity checking, try to verify that the skip list has
         * the expected size and number of entries at the end of each
         * internal while loop (see below).
         *
         * Doing this get a bit tricky, as depending on flags, we may
         * or may not flush all the entries in the slist.
         *
         * To make things more entertaining, with the advent of the
         * fractal heap, the entry serialize callback can cause entries
         * to be dirtied, resized, and/or moved.  Also, the
         * pre_serialize callback can result in an entry being
         * removed from the cache via the take ownership flag.
         *
         * To deal with this, we first make note of the initial
         * skip list length and size:
         */
        initial_slist_len  = cache_ptr->slist_len;
        initial_slist_size = cache_ptr->slist_size;

        /* As mentioned above, there is the possibility that
         * entries will be dirtied, resized, flushed, or removed
         * from the cache via the take ownership flag  during
         * our pass through the skip list.  To capture the number
         * of entries added, and the skip list size delta,
         * zero the slist_len_increase and slist_size_increase of
         * the cache's instance of H5C_t.  These fields will be
         * updated elsewhere to account for slist insertions and/or
         * dirty entry size changes.
         */
        cache_ptr->slist_len_increase  = 0;
        cache_ptr->slist_size_increase = 0;

        /* at the end of the loop, use these values to compute the
         * expected slist length and size and compare this with the
         * value recorded in the cache's instance of H5C_t.
         */
#endif /* H5C_DO_SANITY_CHECKS */

        restart_slist_scan = true;
        while ((restart_slist_scan) || (node_ptr != NULL)) {
            if (restart_slist_scan) {
                restart_slist_scan = false;

                /* Start at beginning of skip list */
                node_ptr = H5SL_first(cache_ptr->slist_ptr);
                if (node_ptr == NULL)
                    /* the slist is empty -- break out of inner loop */
                    break;

                /* Get cache entry for this node */
                next_entry_ptr = (H5C_cache_entry_t *)H5SL_item(node_ptr);
                if (NULL == next_entry_ptr)
                    HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "next_entry_ptr == NULL ?!?!");

                assert(next_entry_ptr->is_dirty);
                assert(next_entry_ptr->in_slist);
            } /* end if */

            entry_ptr = next_entry_ptr;

            /* With the advent of the fractal heap, the free space
             * manager, and the version 3 cache, it is possible
             * that the pre-serialize or serialize callback will
             * dirty, resize, or take ownership of other entries
             * in the cache.
             *
             * To deal with this, there is code to detect any
             * change in the skip list not directly under the control
             * of this function.  If such modifications are detected,
             * we must re-start the scan of the skip list to avoid
             * the possibility that the target of the next_entry_ptr
             * may have been flushed or deleted from the cache.
             *
             * To verify that all such possibilities have been dealt
             * with, we do a bit of extra sanity checking on
             * entry_ptr.
             */
            assert(entry_ptr->in_slist);
            assert(entry_ptr->is_dirty);

            if (!flush_marked_entries || entry_ptr->flush_marker)
                assert(entry_ptr->ring >= ring);

            /* Advance node pointer now, before we delete its target
             * from the slist.
             */
            node_ptr = H5SL_next(node_ptr);
            if (node_ptr != NULL) {
                next_entry_ptr = (H5C_cache_entry_t *)H5SL_item(node_ptr);
                if (NULL == next_entry_ptr)
                    HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "next_entry_ptr == NULL ?!?!");

                assert(next_entry_ptr->is_dirty);
                assert(next_entry_ptr->in_slist);

                if (!flush_marked_entries || next_entry_ptr->flush_marker)
                    assert(next_entry_ptr->ring >= ring);

                assert(entry_ptr != next_entry_ptr);
            } /* end if */
            else
                next_entry_ptr = NULL;

            if ((!flush_marked_entries || entry_ptr->flush_marker) &&
                ((!entry_ptr->flush_me_last) ||
                 ((entry_ptr->flush_me_last) && ((cache_ptr->num_last_entries >= cache_ptr->slist_len) ||
                                                 (flush_marked_entries && entry_ptr->flush_marker)))) &&
                ((entry_ptr->flush_dep_nchildren == 0) || (entry_ptr->flush_dep_ndirty_children == 0)) &&
                (entry_ptr->ring == ring)) {

                assert(entry_ptr->flush_dep_nunser_children == 0);

                if (entry_ptr->is_protected) {
                    /* we probably have major problems -- but lets
                     * flush everything we can before we decide
                     * whether to flag an error.
                     */
                    tried_to_flush_protected_entry = true;
                    protected_entries++;
                } /* end if */
                else {
                    if (H5C__flush_single_entry(f, entry_ptr, (flags | H5C__DURING_FLUSH_FLAG)) < 0)
                        HGOTO_ERROR(H5E_CACHE, H5E_CANTFLUSH, FAIL, "Can't flush entry");

                    if (cache_ptr->slist_changed) {
                        /* The slist has been modified by something
                         * other than the simple removal of the
                         * of the flushed entry after the flush.
                         *
                         * This has the potential to corrupt the
                         * scan through the slist, so restart it.
                         */
                        restart_slist_scan       = true;
                        cache_ptr->slist_changed = false;
                        H5C__UPDATE_STATS_FOR_SLIST_SCAN_RESTART(cache_ptr);
                    } /* end if */

                    flushed_entries_last_pass = true;
                } /* end else */
            }     /* end if */
        }         /* while ( ( restart_slist_scan ) || ( node_ptr != NULL ) ) */

#ifdef H5C_DO_SANITY_CHECKS
        /* Verify that the slist size and length are as expected. */
        assert((uint32_t)((int32_t)initial_slist_len + cache_ptr->slist_len_increase) ==
               cache_ptr->slist_len);
        assert((size_t)((ssize_t)initial_slist_size + cache_ptr->slist_size_increase) ==
               cache_ptr->slist_size);
#endif /* H5C_DO_SANITY_CHECKS */
    }  /* while */

    assert(protected_entries <= cache_ptr->pl_len);

    if (((cache_ptr->pl_len > 0) && !ignore_protected) || tried_to_flush_protected_entry)
        HGOTO_ERROR(H5E_CACHE, H5E_CANTFLUSH, FAIL, "cache has protected items");

#ifdef H5C_DO_SANITY_CHECKS
    if (!flush_marked_entries) {
        assert(cache_ptr->slist_ring_len[ring] == 0);
        assert(cache_ptr->slist_ring_size[ring] == 0);
    }  /* end if */
#endif /* H5C_DO_SANITY_CHECKS */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__flush_ring() */

/*-------------------------------------------------------------------------
 * Function:    H5C__make_space_in_cache
 *
 * Purpose:     Attempt to evict cache entries until the index_size
 *        is at least needed_space below max_cache_size.
 *
 *        In passing, also attempt to bring cLRU_list_size to a
 *        value greater than min_clean_size.
 *
 *        Depending on circumstances, both of these goals may
 *        be impossible, as in parallel mode, we must avoid generating
 *        a write as part of a read (to avoid deadlock in collective
 *        I/O), and in all cases, it is possible (though hopefully
 *        highly unlikely) that the protected list may exceed the
 *        maximum size of the cache.
 *
 *        Thus the function simply does its best, returning success
 *        unless an error is encountered.
 *
 *        Observe that this function cannot occasion a read.
 *
 * Return:      Non-negative on success/Negative on failure.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C__make_space_in_cache(H5F_t *f, size_t space_needed, bool write_permitted)
{
    H5C_t *cache_ptr = f->shared->cache;
#if H5C_COLLECT_CACHE_STATS
    int32_t clean_entries_skipped    = 0;
    int32_t dirty_pf_entries_skipped = 0;
    int32_t total_entries_scanned    = 0;
#endif /* H5C_COLLECT_CACHE_STATS */
    uint32_t           entries_examined = 0;
    uint32_t           initial_list_len;
    size_t             empty_space;
    bool               reentrant_call    = false;
    bool               prev_is_dirty     = false;
    bool               didnt_flush_entry = false;
    bool               restart_scan;
    H5C_cache_entry_t *entry_ptr;
    H5C_cache_entry_t *prev_ptr;
    H5C_cache_entry_t *next_ptr;
#ifndef NDEBUG
    uint32_t num_corked_entries = 0;
#endif
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(f);
    assert(cache_ptr);
    assert(cache_ptr->index_size == (cache_ptr->clean_index_size + cache_ptr->dirty_index_size));

    /* check to see if cache_ptr->msic_in_progress is true.  If it, this
     * is a re-entrant call via a client callback called in the make
     * space in cache process.  To avoid an infinite recursion, set
     * reentrant_call to true, and goto done.
     */
    if (cache_ptr->msic_in_progress) {
        reentrant_call = true;
        HGOTO_DONE(SUCCEED);
    } /* end if */

    cache_ptr->msic_in_progress = true;

    if (write_permitted) {
        restart_scan     = false;
        initial_list_len = cache_ptr->LRU_list_len;
        entry_ptr        = cache_ptr->LRU_tail_ptr;

        if (cache_ptr->index_size >= cache_ptr->max_cache_size)
            empty_space = 0;
        else
            empty_space = cache_ptr->max_cache_size - cache_ptr->index_size;

        while ((((cache_ptr->index_size + space_needed) > cache_ptr->max_cache_size) ||
                ((empty_space + cache_ptr->clean_index_size) < (cache_ptr->min_clean_size))) &&
               (entries_examined <= (2 * initial_list_len)) && (entry_ptr != NULL)) {
            assert(!(entry_ptr->is_protected));
            assert(!(entry_ptr->is_read_only));
            assert((entry_ptr->ro_ref_count) == 0);

            next_ptr = entry_ptr->next;
            prev_ptr = entry_ptr->prev;

            if (prev_ptr != NULL)
                prev_is_dirty = prev_ptr->is_dirty;

            if (entry_ptr->is_dirty && (entry_ptr->tag_info && entry_ptr->tag_info->corked)) {
                /* Skip "dirty" corked entries.  */
#ifndef NDEBUG
                ++num_corked_entries;
#endif
                didnt_flush_entry = true;
            }
            else if ((entry_ptr->type->id != H5AC_EPOCH_MARKER_ID) && !entry_ptr->flush_in_progress &&
                     !entry_ptr->prefetched_dirty) {
                didnt_flush_entry = false;
                if (entry_ptr->is_dirty) {
#if H5C_COLLECT_CACHE_STATS
                    if ((cache_ptr->index_size + space_needed) > cache_ptr->max_cache_size)
                        cache_ptr->entries_scanned_to_make_space++;
#endif /* H5C_COLLECT_CACHE_STATS */

                    /* reset entries_removed_counter and
                     * last_entry_removed_ptr prior to the call to
                     * H5C__flush_single_entry() so that we can spot
                     * unexpected removals of entries from the cache,
                     * and set the restart_scan flag if proceeding
                     * would be likely to cause us to scan an entry
                     * that is no longer in the cache.
                     */
                    cache_ptr->entries_removed_counter = 0;
                    cache_ptr->last_entry_removed_ptr  = NULL;

                    if (H5C__flush_single_entry(f, entry_ptr, H5C__NO_FLAGS_SET) < 0)
                        HGOTO_ERROR(H5E_CACHE, H5E_CANTFLUSH, FAIL, "unable to flush entry");

                    if ((cache_ptr->entries_removed_counter > 1) ||
                        (cache_ptr->last_entry_removed_ptr == prev_ptr))

                        restart_scan = true;
                }
                else if ((cache_ptr->index_size + space_needed) > cache_ptr->max_cache_size
#ifdef H5_HAVE_PARALLEL
                         && !(entry_ptr->coll_access)
#endif /* H5_HAVE_PARALLEL */
                ) {
#if H5C_COLLECT_CACHE_STATS
                    cache_ptr->entries_scanned_to_make_space++;
#endif /* H5C_COLLECT_CACHE_STATS */

                    if (H5C__flush_single_entry(f, entry_ptr,
                                                H5C__FLUSH_INVALIDATE_FLAG |
                                                    H5C__DEL_FROM_SLIST_ON_DESTROY_FLAG) < 0)
                        HGOTO_ERROR(H5E_CACHE, H5E_CANTFLUSH, FAIL, "unable to flush entry");
                }
                else {
                    /* We have enough space so don't flush clean entry. */
#if H5C_COLLECT_CACHE_STATS
                    clean_entries_skipped++;
#endif /* H5C_COLLECT_CACHE_STATS */
                    didnt_flush_entry = true;
                }

#if H5C_COLLECT_CACHE_STATS
                total_entries_scanned++;
#endif /* H5C_COLLECT_CACHE_STATS */
            }
            else {

                /* Skip epoch markers, entries that are in the process
                 * of being flushed, and entries marked as prefetched_dirty
                 * (occurs in the R/O case only).
                 */
                didnt_flush_entry = true;

#if H5C_COLLECT_CACHE_STATS
                if (entry_ptr->prefetched_dirty)
                    dirty_pf_entries_skipped++;
#endif /* H5C_COLLECT_CACHE_STATS */
            }

            if (prev_ptr != NULL) {
                if (didnt_flush_entry)
                    /* epoch markers don't get flushed, and we don't touch
                     * entries that are in the process of being flushed.
                     * Hence no need for sanity checks, as we haven't
                     * flushed anything.  Thus just set entry_ptr to prev_ptr
                     * and go on.
                     */
                    entry_ptr = prev_ptr;
                else if (restart_scan || prev_ptr->is_dirty != prev_is_dirty || prev_ptr->next != next_ptr ||
                         prev_ptr->is_protected || prev_ptr->is_pinned) {
                    /* something has happened to the LRU -- start over
                     * from the tail.
                     */
                    restart_scan = false;
                    entry_ptr    = cache_ptr->LRU_tail_ptr;
                    H5C__UPDATE_STATS_FOR_LRU_SCAN_RESTART(cache_ptr);
                }
                else
                    entry_ptr = prev_ptr;
            }
            else
                entry_ptr = NULL;

            entries_examined++;

            if (cache_ptr->index_size >= cache_ptr->max_cache_size)
                empty_space = 0;
            else
                empty_space = cache_ptr->max_cache_size - cache_ptr->index_size;

            assert(cache_ptr->index_size == (cache_ptr->clean_index_size + cache_ptr->dirty_index_size));
        }

#if H5C_COLLECT_CACHE_STATS
        cache_ptr->calls_to_msic++;

        cache_ptr->total_entries_skipped_in_msic += clean_entries_skipped;
        cache_ptr->total_dirty_pf_entries_skipped_in_msic += dirty_pf_entries_skipped;
        cache_ptr->total_entries_scanned_in_msic += total_entries_scanned;

        if (clean_entries_skipped > cache_ptr->max_entries_skipped_in_msic)
            cache_ptr->max_entries_skipped_in_msic = clean_entries_skipped;

        if (dirty_pf_entries_skipped > cache_ptr->max_dirty_pf_entries_skipped_in_msic)
            cache_ptr->max_dirty_pf_entries_skipped_in_msic = dirty_pf_entries_skipped;

        if (total_entries_scanned > cache_ptr->max_entries_scanned_in_msic)
            cache_ptr->max_entries_scanned_in_msic = total_entries_scanned;
#endif /* H5C_COLLECT_CACHE_STATS */

        /* NEED: work on a better assert for corked entries */
        assert((entries_examined > (2 * initial_list_len)) ||
               ((cache_ptr->pl_size + cache_ptr->pel_size + cache_ptr->min_clean_size) >
                cache_ptr->max_cache_size) ||
               ((cache_ptr->clean_index_size + empty_space) >= cache_ptr->min_clean_size) ||
               ((num_corked_entries)));
#if H5C_MAINTAIN_CLEAN_AND_DIRTY_LRU_LISTS

        assert((entries_examined > (2 * initial_list_len)) ||
               (cache_ptr->cLRU_list_size <= cache_ptr->clean_index_size));
        assert((entries_examined > (2 * initial_list_len)) ||
               (cache_ptr->dLRU_list_size <= cache_ptr->dirty_index_size));

#endif /* H5C_MAINTAIN_CLEAN_AND_DIRTY_LRU_LISTS */
    }
    else {
        assert(H5C_MAINTAIN_CLEAN_AND_DIRTY_LRU_LISTS);

#if H5C_MAINTAIN_CLEAN_AND_DIRTY_LRU_LISTS
        initial_list_len = cache_ptr->cLRU_list_len;
        entry_ptr        = cache_ptr->cLRU_tail_ptr;

        while (((cache_ptr->index_size + space_needed) > cache_ptr->max_cache_size) &&
               (entries_examined <= initial_list_len) && (entry_ptr != NULL)) {
            assert(!(entry_ptr->is_protected));
            assert(!(entry_ptr->is_read_only));
            assert((entry_ptr->ro_ref_count) == 0);
            assert(!(entry_ptr->is_dirty));

            prev_ptr = entry_ptr->aux_prev;

            if (!entry_ptr->prefetched_dirty
#ifdef H5_HAVE_PARALLEL
                && !entry_ptr->coll_access
#endif /* H5_HAVE_PARALLEL */
            ) {
                if (H5C__flush_single_entry(
                        f, entry_ptr, H5C__FLUSH_INVALIDATE_FLAG | H5C__DEL_FROM_SLIST_ON_DESTROY_FLAG) < 0)
                    HGOTO_ERROR(H5E_CACHE, H5E_CANTFLUSH, FAIL, "unable to flush entry");
            } /* end if */

            /* we are scanning the clean LRU, so the serialize function
             * will not be called on any entry -- thus there is no
             * concern about the list being modified out from under
             * this function.
             */

            entry_ptr = prev_ptr;
            entries_examined++;
        }
#endif /* H5C_MAINTAIN_CLEAN_AND_DIRTY_LRU_LISTS */
    }

done:
    /* Sanity checks */
    assert(cache_ptr->msic_in_progress);
    if (!reentrant_call)
        cache_ptr->msic_in_progress = false;
    assert((!reentrant_call) || (cache_ptr->msic_in_progress));

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__make_space_in_cache() */

/*-------------------------------------------------------------------------
 * Function:    H5C__serialize_cache
 *
 * Purpose:    Serialize (i.e. construct an on disk image) for all entries
 *        in the metadata cache including clean entries.
 *
 *        Note that flush dependencies and "flush me last" flags
 *        must be observed in the serialization process.
 *
 *        Note also that entries may be loaded, flushed, evicted,
 *        expunged, relocated, resized, or removed from the cache
 *        during this process, just as these actions may occur during
 *        a regular flush.
 *
 *        However, we are given that the cache will contain no protected
 *        entries on entry to this routine (although entries may be
 *        briefly protected and then unprotected during the serialize
 *        process).
 *
 *        The objective of this routine is serialize all entries and
 *        to force all entries into their actual locations on disk.
 *
 *        The initial need for this routine is to settle all entries
 *        in the cache prior to construction of the metadata cache
 *        image so that the size of the cache image can be calculated.
 *
 * Return:      Non-negative on success/Negative on failure or if there was
 *        a request to flush all items and something was protected.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C__serialize_cache(H5F_t *f)
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
    herr_t     ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
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

#ifndef NDEBUG
    /* if this is a debug build, set the serialization_count field of
     * each entry in the cache to zero before we start the serialization.
     * This allows us to detect the case in which any entry is serialized
     * more than once (a performance issues), and more importantly, the
     * case is which any flush dependency parent is serializes more than
     * once (a correctness issue).
     */
    {
        H5C_cache_entry_t *scan_ptr = NULL;

        scan_ptr = cache_ptr->il_head;
        while (scan_ptr != NULL) {
            scan_ptr->serialization_count = 0;
            scan_ptr                      = scan_ptr->il_next;
        } /* end while */
    }     /* end block */
#endif

    /* set cache_ptr->serialization_in_progress to true, and back
     * to false at the end of the function.  Must maintain this flag
     * to support H5C_get_serialization_in_progress(), which is in
     * turn required to support sanity checking in some cache
     * clients.
     */
    assert(!cache_ptr->serialization_in_progress);
    cache_ptr->serialization_in_progress = true;

    /* Serialize each ring, starting from the outermost ring and
     * working inward.
     */
    ring = H5C_RING_USER;
    while (ring < H5C_RING_NTYPES) {
        assert(cache_ptr->close_warning_received);
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

        if (H5C__serialize_ring(f, ring) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_CANTSERIALIZE, FAIL, "serialize ring failed");

        ring++;
    } /* end while */

#ifndef NDEBUG
    /* Verify that no entry has been serialized more than once.
     * FD parents with multiple serializations should have been caught
     * elsewhere, so no specific check for them here.
     */
    {
        H5C_cache_entry_t *scan_ptr = NULL;

        scan_ptr = cache_ptr->il_head;
        while (scan_ptr != NULL) {
            assert(scan_ptr->serialization_count <= 1);

            scan_ptr = scan_ptr->il_next;
        } /* end while */
    }     /* end block */
#endif

done:
    cache_ptr->serialization_in_progress = false;
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__serialize_cache() */

/*-------------------------------------------------------------------------
 * Function:    H5C__serialize_ring
 *
 * Purpose:     Serialize the entries contained in the specified cache and
 *              ring.  All entries in rings outside the specified ring
 *              must have been serialized on entry.
 *
 *              If the cache contains protected entries in the specified
 *              ring, the function will fail, as protected entries cannot
 *              be serialized.  However all unprotected entries in the
 *              target ring should be serialized before the function
 *              returns failure.
 *
 *              If flush dependencies appear in the target ring, the
 *              function makes repeated passes through the index list
 *              serializing entries in flush dependency order.
 *
 *              All entries outside the H5C_RING_SBE are marked for
 *              inclusion in the cache image.  Entries in H5C_RING_SBE
 *              and below are marked for exclusion from the image.
 *
 * Return:      Non-negative on success/Negative on failure or if there was
 *              a request to flush all items and something was protected.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5C__serialize_ring(H5F_t *f, H5C_ring_t ring)
{
    bool               done = false;
    H5C_t             *cache_ptr;
    H5C_cache_entry_t *entry_ptr;
    herr_t             ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(f);
    assert(f->shared);
    cache_ptr = f->shared->cache;
    assert(cache_ptr);
    assert(ring > H5C_RING_UNDEFINED);
    assert(ring < H5C_RING_NTYPES);

    assert(cache_ptr->serialization_in_progress);

    /* The objective here is to serialize all entries in the cache ring
     * in flush dependency order.
     *
     * The basic algorithm is to scan the cache index list looking for
     * unserialized entries that are either not in a flush dependency
     * relationship, or which have no unserialized children.  Any such
     * entry is serialized and its flush dependency parents (if any) are
     * informed -- allowing them to decrement their userialized child counts.
     *
     * However, this algorithm is complicated by the ability
     * of client serialization callbacks to perform operations on
     * on the cache which can result in the insertion, deletion,
     * relocation, resize, dirty, flush, eviction, or removal (via the
     * take ownership flag) of entries.  Changes in the flush dependency
     * structure are also possible.
     *
     * On the other hand, the algorithm is simplified by the fact that
     * we are serializing, not flushing.  Thus, as long as all entries
     * are serialized correctly, it doesn't matter if we have to go back
     * and serialize an entry a second time.
     *
     * These possible actions result in the following modifications to
     * the basic algorithm:
     *
     * 1) In the event of an entry expunge, eviction or removal, we must
     *    restart the scan as it is possible that the next entry in our
     *    scan is no longer in the cache.  Were we to examine this entry,
     *    we would be accessing deallocated memory.
     *
     * 2) A resize, dirty, or insertion of an entry may result in the
     *    the increment of a flush dependency parent's dirty and/or
     *    unserialized child count.  In the context of serializing the
     *    the cache, this is a non-issue, as even if we have already
     *    serialized the parent, it will be marked dirty and its image
     *    marked out of date if appropriate when the child is serialized.
     *
     *    However, this is a major issue for a flush, as were this to happen
     *    in a flush, it would violate the invariant that the flush dependency
     *    feature is intended to enforce.  As the metadata cache has no
     *    control over the behavior of cache clients, it has no way of
     *    preventing this behaviour.  However, it should detect it if at all
     *    possible.
     *
     *    Do this by maintaining a count of the number of times each entry is
     *    serialized during a cache serialization.  If any flush dependency
     *    parent is serialized more than once, throw an assertion failure.
     *
     * 3) An entry relocation will typically change the location of the
     *    entry in the index list.  This shouldn't cause problems as we
     *    will scan the index list until we make a complete pass without
     *    finding anything to serialize -- making relocations of either
     *    the current or next entries irrelevant.
     *
     *    Note that since a relocation may result in our skipping part of
     *    the index list, we must always do at least one more pass through
     *    the index list after an entry relocation.
     *
     * 4) Changes in the flush dependency structure are possible on
     *    entry insertion, load, expunge, evict, or remove.  Destruction
     *    of a flush dependency has no effect, as it can only relax the
     *    flush dependencies.  Creation of a flush dependency can create
     *    an unserialized child of a flush dependency parent where all
     *    flush dependency children were previously serialized.  Should
     *    this child dirty the flush dependency parent when it is serialized,
     *    the parent will be re-serialized.
     *
     *    Per the discussion of 2) above, this is a non issue for cache
     *    serialization, and a major problem for cache flush.  Using the
     *    same detection mechanism, throw an assertion failure if this
     *    condition appears.
     *
     * Observe that either eviction or removal of entries as a result of
     * a serialization is not a problem as long as the flush dependency
     * tree does not change beyond the removal of a leaf.
     */
    while (!done) {
        /* Reset the counters so that we can detect insertions, loads,
         * moves, and flush dependency height changes caused by the pre_serialize
         * and serialize callbacks.
         */
        cache_ptr->entries_loaded_counter    = 0;
        cache_ptr->entries_inserted_counter  = 0;
        cache_ptr->entries_relocated_counter = 0;

        done      = true; /* set to false if any activity in inner loop */
        entry_ptr = cache_ptr->il_head;
        while (entry_ptr != NULL) {
            /* Verify that either the entry is already serialized, or
             * that it is assigned to either the target or an inner
             * ring.
             */
            assert((entry_ptr->ring >= ring) || (entry_ptr->image_up_to_date));

            /* Skip flush me last entries or inner ring entries */
            if (!entry_ptr->flush_me_last && entry_ptr->ring == ring) {

                /* if we encounter an unserialized entry in the current
                 * ring that is not marked flush me last, we are not done.
                 */
                if (!entry_ptr->image_up_to_date)
                    done = false;

                /* Serialize the entry if its image is not up to date
                 * and it has no unserialized flush dependency children.
                 */
                if (!entry_ptr->image_up_to_date && entry_ptr->flush_dep_nunser_children == 0) {
                    assert(entry_ptr->serialization_count == 0);

                    /* Serialize the entry */
                    if (H5C__serialize_single_entry(f, cache_ptr, entry_ptr) < 0)
                        HGOTO_ERROR(H5E_CACHE, H5E_CANTSERIALIZE, FAIL, "entry serialization failed");

                    assert(entry_ptr->flush_dep_nunser_children == 0);
                    assert(entry_ptr->serialization_count == 0);

#ifndef NDEBUG
                    /* Increment serialization counter (to detect multiple serializations) */
                    entry_ptr->serialization_count++;
#endif
                } /* end if */
            }     /* end if */

            /* Check for the cache being perturbed during the entry serialize */
            if ((cache_ptr->entries_loaded_counter > 0) || (cache_ptr->entries_inserted_counter > 0) ||
                (cache_ptr->entries_relocated_counter > 0)) {

#if H5C_COLLECT_CACHE_STATS
                H5C__UPDATE_STATS_FOR_INDEX_SCAN_RESTART(cache_ptr);
#endif /* H5C_COLLECT_CACHE_STATS */

                /* Reset the counters */
                cache_ptr->entries_loaded_counter    = 0;
                cache_ptr->entries_inserted_counter  = 0;
                cache_ptr->entries_relocated_counter = 0;

                /* Restart scan */
                entry_ptr = cache_ptr->il_head;
            } /* end if */
            else
                /* Advance to next entry */
                entry_ptr = entry_ptr->il_next;
        } /* while ( entry_ptr != NULL ) */
    }     /* while ( ! done ) */

    /* Reset the counters so that we can detect insertions, loads,
     * moves, and flush dependency height changes caused by the pre_serialize
     * and serialize callbacks.
     */
    cache_ptr->entries_loaded_counter    = 0;
    cache_ptr->entries_inserted_counter  = 0;
    cache_ptr->entries_relocated_counter = 0;

    /* At this point, all entries not marked "flush me last" and in
     * the current ring or outside it should be serialized and have up
     * to date images.  Scan the index list again to serialize the
     * "flush me last" entries (if they are in the current ring) and to
     * verify that all other entries have up to date images.
     */
    entry_ptr = cache_ptr->il_head;
    while (entry_ptr != NULL) {
        assert(entry_ptr->ring > H5C_RING_UNDEFINED);
        assert(entry_ptr->ring < H5C_RING_NTYPES);
        assert((entry_ptr->ring >= ring) || (entry_ptr->image_up_to_date));

        if (entry_ptr->ring == ring) {
            if (entry_ptr->flush_me_last) {
                if (!entry_ptr->image_up_to_date) {
                    assert(entry_ptr->serialization_count == 0);
                    assert(entry_ptr->flush_dep_nunser_children == 0);

                    /* Serialize the entry */
                    if (H5C__serialize_single_entry(f, cache_ptr, entry_ptr) < 0)
                        HGOTO_ERROR(H5E_CACHE, H5E_CANTSERIALIZE, FAIL, "entry serialization failed");

                    /* Check for the cache changing */
                    if ((cache_ptr->entries_loaded_counter > 0) ||
                        (cache_ptr->entries_inserted_counter > 0) ||
                        (cache_ptr->entries_relocated_counter > 0))
                        HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL,
                                    "flush_me_last entry serialization triggered restart");

                    assert(entry_ptr->flush_dep_nunser_children == 0);
                    assert(entry_ptr->serialization_count == 0);
#ifndef NDEBUG
                    /* Increment serialization counter (to detect multiple serializations) */
                    entry_ptr->serialization_count++;
#endif
                } /* end if */
            }     /* end if */
            else {
                assert(entry_ptr->image_up_to_date);
                assert(entry_ptr->serialization_count <= 1);
                assert(entry_ptr->flush_dep_nunser_children == 0);
            } /* end else */
        }     /* if ( entry_ptr->ring == ring ) */

        entry_ptr = entry_ptr->il_next;
    } /* while ( entry_ptr != NULL ) */

done:
    assert(cache_ptr->serialization_in_progress);
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__serialize_ring() */
