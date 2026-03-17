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
#include "H5CXprivate.h" /* API Contexts                 */
#include "H5Eprivate.h"  /* Error handling               */
#include "H5Fpkg.h"      /* Files                        */
#include "H5MFprivate.h" /* File memory management       */
#include "H5MMprivate.h" /* Memory management            */

/****************/
/* Local Macros */
/****************/
#if H5C_DO_MEMORY_SANITY_CHECKS
#define H5C_IMAGE_EXTRA_SPACE  8
#define H5C_IMAGE_SANITY_VALUE "DeadBeef"
#else /* H5C_DO_MEMORY_SANITY_CHECKS */
#define H5C_IMAGE_EXTRA_SPACE 0
#endif /* H5C_DO_MEMORY_SANITY_CHECKS */

/******************/
/* Local Typedefs */
/******************/

/* Alias for pointer to cache entry, for use when allocating sequences of them */
typedef H5C_cache_entry_t *H5C_cache_entry_ptr_t;

/********************/
/* Local Prototypes */
/********************/
static herr_t H5C__pin_entry_from_client(H5C_t *cache_ptr, H5C_cache_entry_t *entry_ptr);
static herr_t H5C__unpin_entry_real(H5C_t *cache_ptr, H5C_cache_entry_t *entry_ptr, bool update_rp);
static herr_t H5C__unpin_entry_from_client(H5C_t *cache_ptr, H5C_cache_entry_t *entry_ptr, bool update_rp);
static herr_t H5C__generate_image(H5F_t *f, H5C_t *cache_ptr, H5C_cache_entry_t *entry_ptr);
static herr_t H5C__verify_len_eoa(H5F_t *f, const H5C_class_t *type, haddr_t addr, size_t *len, bool actual);
static void  *H5C__load_entry(H5F_t *f,
#ifdef H5_HAVE_PARALLEL
                             bool coll_access,
#endif /* H5_HAVE_PARALLEL */
                             const H5C_class_t *type, haddr_t addr, void *udata);
static herr_t H5C__mark_flush_dep_dirty(H5C_cache_entry_t *entry);
static herr_t H5C__mark_flush_dep_clean(H5C_cache_entry_t *entry);
static herr_t H5C__mark_flush_dep_serialized(H5C_cache_entry_t *entry);
static herr_t H5C__mark_flush_dep_unserialized(H5C_cache_entry_t *entry);
#ifndef NDEBUG
static void H5C__assert_flush_dep_nocycle(const H5C_cache_entry_t *entry,
                                          const H5C_cache_entry_t *base_entry);
#endif
static herr_t H5C__destroy_pf_entry_child_flush_deps(H5C_t *cache_ptr, H5C_cache_entry_t *pf_entry_ptr,
                                                     H5C_cache_entry_t **fd_children);
static herr_t H5C__deserialize_prefetched_entry(H5F_t *f, H5C_t *cache_ptr, H5C_cache_entry_t **entry_ptr_ptr,
                                                const H5C_class_t *type, haddr_t addr, void *udata);

/*********************/
/* Package Variables */
/*********************/

/*****************************/
/* Library Private Variables */
/*****************************/

/*******************/
/* Local Variables */
/*******************/

/* Declare a free list to manage arrays of cache entries */
H5FL_SEQ_DEFINE_STATIC(H5C_cache_entry_ptr_t);

/*-------------------------------------------------------------------------
 * Function:    H5C__pin_entry_from_client()
 *
 * Purpose:     Internal routine to pin a cache entry from a client action.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5C__pin_entry_from_client(H5C_t
#if !H5C_COLLECT_CACHE_STATS
                               H5_ATTR_UNUSED
#endif
                                             *cache_ptr,
                           H5C_cache_entry_t *entry_ptr)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(cache_ptr);
    assert(entry_ptr);
    assert(entry_ptr->is_protected);

    /* Check if the entry is already pinned */
    if (entry_ptr->is_pinned) {
        /* Check if the entry was pinned through an explicit pin from a client */
        if (entry_ptr->pinned_from_client)
            HGOTO_ERROR(H5E_CACHE, H5E_CANTPIN, FAIL, "entry is already pinned");
    } /* end if */
    else {
        entry_ptr->is_pinned = true;

        H5C__UPDATE_STATS_FOR_PIN(cache_ptr, entry_ptr);
    } /* end else */

    /* Mark that the entry was pinned through an explicit pin from a client */
    entry_ptr->pinned_from_client = true;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__pin_entry_from_client() */

/*-------------------------------------------------------------------------
 * Function:    H5C__unpin_entry_real()
 *
 * Purpose:     Internal routine to unpin a cache entry.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5C__unpin_entry_real(H5C_t *cache_ptr, H5C_cache_entry_t *entry_ptr, bool update_rp)
{
    herr_t ret_value = SUCCEED; /* Return value */

#ifdef H5C_DO_SANITY_CHECKS
    FUNC_ENTER_PACKAGE
#else
    FUNC_ENTER_PACKAGE_NOERR
#endif

    /* Sanity checking */
    assert(cache_ptr);
    assert(entry_ptr);
    assert(entry_ptr->is_pinned);

    /* If requested, update the replacement policy if the entry is not protected */
    if (update_rp && !entry_ptr->is_protected)
        H5C__UPDATE_RP_FOR_UNPIN(cache_ptr, entry_ptr, FAIL);

    /* Unpin the entry now */
    entry_ptr->is_pinned = false;

    /* Update the stats for an unpin operation */
    H5C__UPDATE_STATS_FOR_UNPIN(cache_ptr, entry_ptr);

#ifdef H5C_DO_SANITY_CHECKS
done:
#endif
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__unpin_entry_real() */

/*-------------------------------------------------------------------------
 * Function:    H5C__unpin_entry_from_client()
 *
 * Purpose:     Internal routine to unpin a cache entry from a client action.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5C__unpin_entry_from_client(H5C_t *cache_ptr, H5C_cache_entry_t *entry_ptr, bool update_rp)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checking */
    assert(cache_ptr);
    assert(entry_ptr);

    /* Error checking (should be sanity checks?) */
    if (!entry_ptr->is_pinned)
        HGOTO_ERROR(H5E_CACHE, H5E_CANTUNPIN, FAIL, "entry isn't pinned");
    if (!entry_ptr->pinned_from_client)
        HGOTO_ERROR(H5E_CACHE, H5E_CANTUNPIN, FAIL, "entry wasn't pinned by cache client");

    /* Check if the entry is not pinned from a flush dependency */
    if (!entry_ptr->pinned_from_cache)
        if (H5C__unpin_entry_real(cache_ptr, entry_ptr, update_rp) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_CANTUNPIN, FAIL, "can't unpin entry");

    /* Mark the entry as explicitly unpinned by the client */
    entry_ptr->pinned_from_client = false;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__unpin_entry_from_client() */

/*-------------------------------------------------------------------------
 * Function:    H5C__generate_image
 *
 * Purpose:     Serialize an entry and generate its image.
 *
 * Note:        This may cause the entry to be re-sized and/or moved in
 *              the cache.
 *
 *              As we will not update the metadata cache's data structures
 *              until we we finish the write, we must touch up these
 *              data structures for size and location changes even if we
 *              are about to delete the entry from the cache (i.e. on a
 *              flush destroy).
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5C__generate_image(H5F_t *f, H5C_t *cache_ptr, H5C_cache_entry_t *entry_ptr)
{
    haddr_t  new_addr        = HADDR_UNDEF;
    haddr_t  old_addr        = HADDR_UNDEF;
    size_t   new_len         = 0;
    unsigned serialize_flags = H5C__SERIALIZE_NO_FLAGS_SET;
    herr_t   ret_value       = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(f);
    assert(cache_ptr);
    assert(entry_ptr);
    assert(!entry_ptr->image_up_to_date);
    assert(entry_ptr->is_dirty);
    assert(!entry_ptr->is_protected);
    assert(entry_ptr->type);

    /* make note of the entry's current address */
    old_addr = entry_ptr->addr;

    /* Call client's pre-serialize callback, if there's one */
    if ((entry_ptr->type->pre_serialize) &&
        ((entry_ptr->type->pre_serialize)(f, (void *)entry_ptr, entry_ptr->addr, entry_ptr->size, &new_addr,
                                          &new_len, &serialize_flags) < 0))
        HGOTO_ERROR(H5E_CACHE, H5E_CANTFLUSH, FAIL, "unable to pre-serialize entry");

    /* Check for any flags set in the pre-serialize callback */
    if (serialize_flags != H5C__SERIALIZE_NO_FLAGS_SET) {
        /* Check for unexpected flags from serialize callback */
        if (serialize_flags & ~(H5C__SERIALIZE_RESIZED_FLAG | H5C__SERIALIZE_MOVED_FLAG))
            HGOTO_ERROR(H5E_CACHE, H5E_CANTFLUSH, FAIL, "unknown serialize flag(s)");

#ifdef H5_HAVE_PARALLEL
        /* In the parallel case, resizes and moves in
         * the serialize operation can cause problems.
         * If they occur, scream and die.
         *
         * At present, in the parallel case, the aux_ptr
         * will only be set if there is more than one
         * process.  Thus we can use this to detect
         * the parallel case.
         *
         * This works for now, but if we start using the
         * aux_ptr for other purposes, we will have to
         * change this test accordingly.
         *
         * NB: While this test detects entryies that attempt
         *     to resize or move themselves during a flush
         *     in the parallel case, it will not detect an
         *     entry that dirties, resizes, and/or moves
         *     other entries during its flush.
         */
        if (cache_ptr->aux_ptr != NULL)
            HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "resize/move in serialize occurred in parallel case");
#endif

        /* If required, resize the buffer and update the entry and the cache
         * data structures
         */
        if (serialize_flags & H5C__SERIALIZE_RESIZED_FLAG) {
            /* Sanity check */
            assert(new_len > 0);

            /* Allocate a new image buffer */
            if (NULL ==
                (entry_ptr->image_ptr = H5MM_realloc(entry_ptr->image_ptr, new_len + H5C_IMAGE_EXTRA_SPACE)))
                HGOTO_ERROR(H5E_CACHE, H5E_CANTALLOC, FAIL,
                            "memory allocation failed for on disk image buffer");

#if H5C_DO_MEMORY_SANITY_CHECKS
            H5MM_memcpy(((uint8_t *)entry_ptr->image_ptr) + new_len, H5C_IMAGE_SANITY_VALUE,
                        H5C_IMAGE_EXTRA_SPACE);
#endif /* H5C_DO_MEMORY_SANITY_CHECKS */

            /* Update statistics for resizing the entry */
            H5C__UPDATE_STATS_FOR_ENTRY_SIZE_CHANGE(cache_ptr, entry_ptr, new_len);

            /* Update the hash table for the size change */
            H5C__UPDATE_INDEX_FOR_SIZE_CHANGE(cache_ptr, entry_ptr->size, new_len, entry_ptr,
                                              !entry_ptr->is_dirty, FAIL);

            /* The entry can't be protected since we are in the process of
             * flushing it.  Thus we must update the replacement policy data
             * structures for the size change.  The macro deals with the pinned
             * case.
             */
            H5C__UPDATE_RP_FOR_SIZE_CHANGE(cache_ptr, entry_ptr, new_len, FAIL);

            /* As we haven't updated the cache data structures for
             * for the flush or flush destroy yet, the entry should
             * be in the slist if the slist is enabled.  Since
             * H5C__UPDATE_SLIST_FOR_SIZE_CHANGE() is a no-op if the
             * slist is enabled, call it un-conditionally.
             */
            assert(entry_ptr->is_dirty);
            assert((entry_ptr->in_slist) || (!cache_ptr->slist_enabled));

            H5C__UPDATE_SLIST_FOR_SIZE_CHANGE(cache_ptr, entry_ptr->size, new_len);

            /* Finally, update the entry for its new size */
            entry_ptr->size = new_len;
        } /* end if */

        /* If required, udate the entry and the cache data structures
         * for a move
         */
        if (serialize_flags & H5C__SERIALIZE_MOVED_FLAG) {
            /* Update stats and entries relocated counter */
            H5C__UPDATE_STATS_FOR_MOVE(cache_ptr, entry_ptr);

            /* We must update cache data structures for the change in address */
            if (entry_ptr->addr == old_addr) {
                /* Delete the entry from the hash table and the slist */
                H5C__DELETE_FROM_INDEX(cache_ptr, entry_ptr, FAIL);
                H5C__REMOVE_ENTRY_FROM_SLIST(cache_ptr, entry_ptr, false, FAIL);

                /* Update the entry for its new address */
                entry_ptr->addr = new_addr;

                /* And then reinsert in the index and slist */
                H5C__INSERT_IN_INDEX(cache_ptr, entry_ptr, FAIL);
                H5C__INSERT_ENTRY_IN_SLIST(cache_ptr, entry_ptr, FAIL);
            }    /* end if */
            else /* move is already done for us -- just do sanity checks */
                assert(entry_ptr->addr == new_addr);
        } /* end if */
    }     /* end if(serialize_flags != H5C__SERIALIZE_NO_FLAGS_SET) */

    /* Serialize object into buffer */
    if (entry_ptr->type->serialize(f, entry_ptr->image_ptr, entry_ptr->size, (void *)entry_ptr) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_CANTFLUSH, FAIL, "unable to serialize entry");

#if H5C_DO_MEMORY_SANITY_CHECKS
    assert(0 == memcmp(((uint8_t *)entry_ptr->image_ptr) + entry_ptr->size, H5C_IMAGE_SANITY_VALUE,
                       H5C_IMAGE_EXTRA_SPACE));
#endif /* H5C_DO_MEMORY_SANITY_CHECKS */

    entry_ptr->image_up_to_date = true;

    /* Propagate the fact that the entry is serialized up the
     * flush dependency chain if appropriate.  Since the image must
     * have been out of date for this function to have been called
     * (see assertion on entry), no need to check that -- only check
     * for flush dependency parents.
     */
    assert(entry_ptr->flush_dep_nunser_children == 0);

    if (entry_ptr->flush_dep_nparents > 0)
        if (H5C__mark_flush_dep_serialized(entry_ptr) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_CANTNOTIFY, FAIL,
                        "Can't propagate serialization status to fd parents");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__generate_image */

/*-------------------------------------------------------------------------
 * Function:    H5C__flush_single_entry
 *
 * Purpose:     Flush or clear (and evict if requested) the cache entry
 *              with the specified address and type.  If the type is NULL,
 *              any unprotected entry at the specified address will be
 *              flushed (and possibly evicted).
 *
 *              Attempts to flush a protected entry will result in an
 *              error.
 *
 *              If the H5C__FLUSH_INVALIDATE_FLAG flag is set, the entry will
 *              be cleared and not flushed, and the call can't be part of a
 *              sequence of flushes.
 *
 *              The function does nothing silently if there is no entry
 *              at the supplied address, or if the entry found has the
 *              wrong type.
 *
 * Return:      Non-negative on success/Negative on failure or if there was
 *              an attempt to flush a protected item.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C__flush_single_entry(H5F_t *f, H5C_cache_entry_t *entry_ptr, unsigned flags)
{
    H5C_t  *cache_ptr;                 /* Cache for file */
    bool    destroy;                   /* external flag */
    bool    clear_only;                /* external flag */
    bool    free_file_space;           /* external flag */
    bool    take_ownership;            /* external flag */
    bool    del_from_slist_on_destroy; /* external flag */
    bool    during_flush;              /* external flag */
    bool    write_entry;               /* internal flag */
    bool    destroy_entry;             /* internal flag */
    bool    generate_image;            /* internal flag */
    bool    update_page_buffer;        /* internal flag */
    bool    was_dirty;
    bool    suppress_image_entry_writes = false;
    bool    suppress_image_entry_frees  = false;
    haddr_t entry_addr                  = HADDR_UNDEF;
    herr_t  ret_value                   = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(f);
    cache_ptr = f->shared->cache;
    assert(cache_ptr);
    assert(entry_ptr);
    assert(entry_ptr->ring != H5C_RING_UNDEFINED);
    assert(entry_ptr->type);

    /* setup external flags from the flags parameter */
    destroy                   = ((flags & H5C__FLUSH_INVALIDATE_FLAG) != 0);
    clear_only                = ((flags & H5C__FLUSH_CLEAR_ONLY_FLAG) != 0);
    free_file_space           = ((flags & H5C__FREE_FILE_SPACE_FLAG) != 0);
    take_ownership            = ((flags & H5C__TAKE_OWNERSHIP_FLAG) != 0);
    del_from_slist_on_destroy = ((flags & H5C__DEL_FROM_SLIST_ON_DESTROY_FLAG) != 0);
    during_flush              = ((flags & H5C__DURING_FLUSH_FLAG) != 0);
    generate_image            = ((flags & H5C__GENERATE_IMAGE_FLAG) != 0);
    update_page_buffer        = ((flags & H5C__UPDATE_PAGE_BUFFER_FLAG) != 0);

    /* Set the flag for destroying the entry, based on the 'take ownership'
     * and 'destroy' flags
     */
    if (take_ownership)
        destroy_entry = false;
    else
        destroy_entry = destroy;

    /* we will write the entry to disk if it exists, is dirty, and if the
     * clear only flag is not set.
     */
    if (entry_ptr->is_dirty && !clear_only)
        write_entry = true;
    else
        write_entry = false;

    /* if we have received close warning, and we have been instructed to
     * generate a metadata cache image, and we have actually constructed
     * the entry images, set suppress_image_entry_frees to true.
     *
     * Set suppress_image_entry_writes to true if indicated by the
     * image_ctl flags.
     */
    if (cache_ptr->close_warning_received && cache_ptr->image_ctl.generate_image &&
        cache_ptr->num_entries_in_image > 0 && cache_ptr->image_entries != NULL) {

        /* Sanity checks */
        assert(entry_ptr->image_up_to_date || !(entry_ptr->include_in_image));
        assert(entry_ptr->image_ptr || !(entry_ptr->include_in_image));
        assert((!clear_only) || !(entry_ptr->include_in_image));
        assert((!take_ownership) || !(entry_ptr->include_in_image));
        assert((!free_file_space) || !(entry_ptr->include_in_image));

        suppress_image_entry_frees = true;

        if (cache_ptr->image_ctl.flags & H5C_CI__SUPRESS_ENTRY_WRITES)
            suppress_image_entry_writes = true;
    } /* end if */

    /* run initial sanity checks */
#ifdef H5C_DO_SANITY_CHECKS
    if (cache_ptr->slist_enabled) {
        if (entry_ptr->in_slist) {
            assert(entry_ptr->is_dirty);
            if (entry_ptr->flush_marker && !entry_ptr->is_dirty)
                HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "entry in slist failed sanity checks");
        } /* end if */
        else {
            assert(!entry_ptr->is_dirty);
            assert(!entry_ptr->flush_marker);
            if (entry_ptr->is_dirty || entry_ptr->flush_marker)
                HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "entry failed sanity checks");
        } /* end else */
    }
    else { /* slist is disabled */
        assert(!entry_ptr->in_slist);
        if (!entry_ptr->is_dirty)
            if (entry_ptr->flush_marker)
                HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "flush marked clean entry?");
    }
#endif /* H5C_DO_SANITY_CHECKS */

    if (entry_ptr->is_protected)
        /* Attempt to flush a protected entry -- scream and die. */
        HGOTO_ERROR(H5E_CACHE, H5E_PROTECT, FAIL, "Attempt to flush a protected entry");

    /* Set entry_ptr->flush_in_progress = true and set
     * entry_ptr->flush_marker = false
     *
     * We will set flush_in_progress back to false at the end if the
     * entry still exists at that point.
     */
    entry_ptr->flush_in_progress = true;
    entry_ptr->flush_marker      = false;

    /* Preserve current dirty state for later */
    was_dirty = entry_ptr->is_dirty;

    /* The entry is dirty, and we are doing a flush, a flush destroy or have
     * been requested to generate an image.  In those cases, serialize the
     * entry.
     */
    if (write_entry || generate_image) {
        assert(entry_ptr->is_dirty);
        if (NULL == entry_ptr->image_ptr) {
            if (NULL == (entry_ptr->image_ptr = H5MM_malloc(entry_ptr->size + H5C_IMAGE_EXTRA_SPACE)))
                HGOTO_ERROR(H5E_CACHE, H5E_CANTALLOC, FAIL,
                            "memory allocation failed for on disk image buffer");

#if H5C_DO_MEMORY_SANITY_CHECKS
            H5MM_memcpy(((uint8_t *)entry_ptr->image_ptr) + entry_ptr->size, H5C_IMAGE_SANITY_VALUE,
                        H5C_IMAGE_EXTRA_SPACE);
#endif /* H5C_DO_MEMORY_SANITY_CHECKS */

        } /* end if */

        if (!entry_ptr->image_up_to_date) {
            /* Sanity check */
            assert(!entry_ptr->prefetched);

            /* Generate the entry's image */
            if (H5C__generate_image(f, cache_ptr, entry_ptr) < 0)
                HGOTO_ERROR(H5E_CACHE, H5E_CANTGET, FAIL, "can't generate entry's image");
        } /* end if ( ! (entry_ptr->image_up_to_date) ) */
    }     /* end if */

    /* Finally, write the image to disk.
     *
     * Note that if the H5AC__CLASS_SKIP_WRITES flag is set in the
     * in the entry's type, we silently skip the write.  This
     * flag should only be used in test code.
     */
    if (write_entry) {
        assert(entry_ptr->is_dirty);

#ifdef H5C_DO_SANITY_CHECKS
        if (cache_ptr->check_write_permitted && !cache_ptr->write_permitted)
            HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "Write when writes are always forbidden!?!?!");
#endif /* H5C_DO_SANITY_CHECKS */

        /* Write the image to disk unless the write is suppressed.
         *
         * This happens if both suppress_image_entry_writes and
         * entry_ptr->include_in_image are true, or if the
         * H5AC__CLASS_SKIP_WRITES is set in the entry's type.  This
         * flag should only be used in test code
         */
        if ((!suppress_image_entry_writes || !entry_ptr->include_in_image) &&
            ((entry_ptr->type->flags & H5C__CLASS_SKIP_WRITES) == 0)) {
            H5FD_mem_t mem_type = H5FD_MEM_DEFAULT;

#ifdef H5_HAVE_PARALLEL
            if (cache_ptr->coll_write_list) {
                if (H5SL_insert(cache_ptr->coll_write_list, entry_ptr, &entry_ptr->addr) < 0)
                    HGOTO_ERROR(H5E_CACHE, H5E_CANTINSERT, FAIL, "unable to insert skip list item");
            } /* end if */
            else {
#endif /* H5_HAVE_PARALLEL */
                if (entry_ptr->prefetched) {
                    assert(entry_ptr->type->id == H5AC_PREFETCHED_ENTRY_ID);
                    mem_type = cache_ptr->class_table_ptr[entry_ptr->prefetch_type_id]->mem_type;
                } /* end if */
                else
                    mem_type = entry_ptr->type->mem_type;

                if (H5F_block_write(f, mem_type, entry_ptr->addr, entry_ptr->size, entry_ptr->image_ptr) < 0)
                    HGOTO_ERROR(H5E_CACHE, H5E_CANTFLUSH, FAIL, "Can't write image to file");
#ifdef H5_HAVE_PARALLEL
            }
#endif    /* H5_HAVE_PARALLEL */
        } /* end if */

        /* if the entry has a notify callback, notify it that we have
         * just flushed the entry.
         */
        if (entry_ptr->type->notify &&
            (entry_ptr->type->notify)(H5C_NOTIFY_ACTION_AFTER_FLUSH, entry_ptr) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_CANTNOTIFY, FAIL, "can't notify client of entry flush");
    } /* if ( write_entry ) */

    /* At this point, all pre-serialize and serialize calls have been
     * made if it was appropriate to make them.  Similarly, the entry
     * has been written to disk if desired.
     *
     * Thus it is now safe to update the cache data structures for the
     * flush.
     */

    /* start by updating the statistics */
    if (clear_only) {
        /* only log a clear if the entry was dirty */
        if (was_dirty)
            H5C__UPDATE_STATS_FOR_CLEAR(cache_ptr, entry_ptr);
    }
    else if (write_entry) {
        assert(was_dirty);

        /* only log a flush if we actually wrote to disk */
        H5C__UPDATE_STATS_FOR_FLUSH(cache_ptr, entry_ptr);
    } /* end else if */

    /* Note that the algorithm below is (very) similar to the set of operations
     * in H5C_remove_entry() and should be kept in sync with changes
     * to that code. - QAK, 2016/11/30
     */

    /* Update the cache internal data structures. */
    if (destroy) {
        /* Sanity checks */
        if (take_ownership)
            assert(!destroy_entry);
        else
            assert(destroy_entry);

        assert(!entry_ptr->is_pinned);

        /* Update stats, while entry is still in the cache */
        H5C__UPDATE_STATS_FOR_EVICTION(cache_ptr, entry_ptr, take_ownership);

        /* If the entry's type has a 'notify' callback and the entry is about
         * to be removed from the cache, send a 'before eviction' notice while
         * the entry is still fully integrated in the cache.
         */
        if (entry_ptr->type->notify &&
            (entry_ptr->type->notify)(H5C_NOTIFY_ACTION_BEFORE_EVICT, entry_ptr) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_CANTNOTIFY, FAIL, "can't notify client about entry to evict");

        /* Update the cache internal data structures as appropriate
         * for a destroy.  Specifically:
         *
         * 1) Delete it from the index
         *
         * 2) Delete it from the skip list if requested.
         *
         * 3) Delete it from the collective read access list.
         *
         * 4) Update the replacement policy for eviction
         *
         * 5) Remove it from the tag list for this object
         *
         * Finally, if the destroy_entry flag is set, discard the
         * entry.
         */
        H5C__DELETE_FROM_INDEX(cache_ptr, entry_ptr, FAIL);

        if (entry_ptr->in_slist && del_from_slist_on_destroy)
            H5C__REMOVE_ENTRY_FROM_SLIST(cache_ptr, entry_ptr, during_flush, FAIL);

#ifdef H5_HAVE_PARALLEL
        /* Check for collective read access flag */
        if (entry_ptr->coll_access) {
            entry_ptr->coll_access = false;
            H5C__REMOVE_FROM_COLL_LIST(cache_ptr, entry_ptr, FAIL);
        } /* end if */
#endif    /* H5_HAVE_PARALLEL */

        H5C__UPDATE_RP_FOR_EVICTION(cache_ptr, entry_ptr, FAIL);

        /* Remove entry from tag list */
        if (H5C__untag_entry(cache_ptr, entry_ptr) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_CANTREMOVE, FAIL, "can't remove entry from tag list");

        /* verify that the entry is no longer part of any flush dependencies */
        assert(entry_ptr->flush_dep_nparents == 0);
        assert(entry_ptr->flush_dep_nchildren == 0);
    } /* end if */
    else {
        assert(clear_only || write_entry);
        assert(entry_ptr->is_dirty);
        assert((!cache_ptr->slist_enabled) || (entry_ptr->in_slist));

        /* We are either doing a flush or a clear.
         *
         * A clear and a flush are the same from the point of
         * view of the replacement policy and the slist.
         * Hence no differentiation between them.
         */
        H5C__UPDATE_RP_FOR_FLUSH(cache_ptr, entry_ptr, FAIL);
        H5C__REMOVE_ENTRY_FROM_SLIST(cache_ptr, entry_ptr, during_flush, FAIL);

        /* mark the entry as clean and update the index for
         * entry clean.  Also, call the clear callback
         * if defined.
         */
        entry_ptr->is_dirty = false;

        H5C__UPDATE_INDEX_FOR_ENTRY_CLEAN(cache_ptr, entry_ptr, FAIL);

        /* Check for entry changing status and do notifications, etc. */
        if (was_dirty) {
            /* If the entry's type has a 'notify' callback send a
             * 'entry cleaned' notice now that the entry is fully
             * integrated into the cache.
             */
            if (entry_ptr->type->notify &&
                (entry_ptr->type->notify)(H5C_NOTIFY_ACTION_ENTRY_CLEANED, entry_ptr) < 0)
                HGOTO_ERROR(H5E_CACHE, H5E_CANTNOTIFY, FAIL,
                            "can't notify client about entry dirty flag cleared");

            /* Propagate the clean flag up the flush dependency chain
             * if appropriate
             */
            if (entry_ptr->flush_dep_ndirty_children != 0)
                assert(entry_ptr->flush_dep_ndirty_children == 0);
            if (entry_ptr->flush_dep_nparents > 0)
                if (H5C__mark_flush_dep_clean(entry_ptr) < 0)
                    HGOTO_ERROR(H5E_CACHE, H5E_CANTMARKCLEAN, FAIL, "Can't propagate flush dep clean flag");
        } /* end if */
    }     /* end else */

    /* reset the flush_in progress flag */
    entry_ptr->flush_in_progress = false;

    /* capture the cache entry address for the log_flush call at the
     * end before the entry_ptr gets freed
     */
    entry_addr = entry_ptr->addr;

    /* Internal cache data structures should now be up to date, and
     * consistent with the status of the entry.
     *
     * Now discard the entry if appropriate.
     */
    if (destroy) {
        /* Sanity check */
        assert(0 == entry_ptr->flush_dep_nparents);

        /* if both suppress_image_entry_frees and entry_ptr->include_in_image
         * are true, simply set entry_ptr->image_ptr to NULL, as we have
         * another pointer to the buffer in an instance of H5C_image_entry_t
         * in cache_ptr->image_entries.
         *
         * Otherwise, free the buffer if it exists.
         */
        if (suppress_image_entry_frees && entry_ptr->include_in_image)
            entry_ptr->image_ptr = NULL;
        else if (entry_ptr->image_ptr != NULL)
            entry_ptr->image_ptr = H5MM_xfree(entry_ptr->image_ptr);

        /* If the entry is not a prefetched entry, verify that the flush
         * dependency parents addresses array has been transferred.
         *
         * If the entry is prefetched, the free_isr routine will dispose of
         * the flush dependency parents addresses array if necessary.
         */
        if (!entry_ptr->prefetched) {
            assert(0 == entry_ptr->fd_parent_count);
            assert(NULL == entry_ptr->fd_parent_addrs);
        } /* end if */

        /* Check whether we should free the space in the file that
         * the entry occupies
         */
        if (free_file_space) {
            hsize_t fsf_size;

            /* Sanity checks */
            assert(H5_addr_defined(entry_ptr->addr));
            assert(!H5F_IS_TMP_ADDR(f, entry_ptr->addr));
#ifndef NDEBUG
            {
                size_t curr_len;

                /* Get the actual image size for the thing again */
                entry_ptr->type->image_len((void *)entry_ptr, &curr_len);
                assert(curr_len == entry_ptr->size);
            }
#endif

            /* If the file space free size callback is defined, use
             * it to get the size of the block of file space to free.
             * Otherwise use entry_ptr->size.
             */
            if (entry_ptr->type->fsf_size) {
                if ((entry_ptr->type->fsf_size)((void *)entry_ptr, &fsf_size) < 0)
                    HGOTO_ERROR(H5E_CACHE, H5E_CANTFREE, FAIL, "unable to get file space free size");
            }    /* end if */
            else /* no file space free size callback -- use entry size */
                fsf_size = entry_ptr->size;

            /* Release the space on disk */
            if (H5MF_xfree(f, entry_ptr->type->mem_type, entry_ptr->addr, fsf_size) < 0)
                HGOTO_ERROR(H5E_CACHE, H5E_CANTFREE, FAIL, "unable to free file space for cache entry");
        } /* end if ( free_file_space ) */

        /* Reset the pointer to the cache the entry is within. -QAK */
        entry_ptr->cache_ptr = NULL;

        /* increment entries_removed_counter and set
         * last_entry_removed_ptr.  As we are likely abuut to
         * free the entry, recall that last_entry_removed_ptr
         * must NEVER be dereferenced.
         *
         * Recall that these fields are maintained to allow functions
         * that perform scans of lists of entries to detect the
         * unexpected removal of entries (via expunge, eviction,
         * or take ownership at present), so that they can re-start
         * their scans if necessary.
         *
         * Also check if the entry we are watching for removal is being
         * removed (usually the 'next' entry for an iteration) and reset
         * it to indicate that it was removed.
         */
        cache_ptr->entries_removed_counter++;
        cache_ptr->last_entry_removed_ptr = entry_ptr;

        if (entry_ptr == cache_ptr->entry_watched_for_removal)
            cache_ptr->entry_watched_for_removal = NULL;

        /* Check for actually destroying the entry in memory */
        /* (As opposed to taking ownership of it) */
        if (destroy_entry) {
            if (entry_ptr->is_dirty) {
                /* Reset dirty flag */
                entry_ptr->is_dirty = false;

                /* If the entry's type has a 'notify' callback send a
                 * 'entry cleaned' notice now that the entry is fully
                 * integrated into the cache.
                 */
                if (entry_ptr->type->notify &&
                    (entry_ptr->type->notify)(H5C_NOTIFY_ACTION_ENTRY_CLEANED, entry_ptr) < 0)
                    HGOTO_ERROR(H5E_CACHE, H5E_CANTNOTIFY, FAIL,
                                "can't notify client about entry dirty flag cleared");
            } /* end if */

            /* verify that the image has been freed */
            assert(entry_ptr->image_ptr == NULL);

            if (entry_ptr->type->free_icr((void *)entry_ptr) < 0)
                HGOTO_ERROR(H5E_CACHE, H5E_CANTFLUSH, FAIL, "free_icr callback failed");
        } /* end if */
        else {
            assert(take_ownership);
        } /* end else */
    }     /* if (destroy) */

    /* Check if we have to update the page buffer with cleared entries
     * so it doesn't go out of date
     */
    if (update_page_buffer) {
        /* Sanity check */
        assert(!destroy);
        assert(entry_ptr->image_ptr);

        if (f->shared->page_buf && (f->shared->page_buf->page_size >= entry_ptr->size))
            if (H5PB_update_entry(f->shared->page_buf, entry_ptr->addr, entry_ptr->size,
                                  entry_ptr->image_ptr) > 0)
                HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "Failed to update PB with metadata cache");
    } /* end if */

    if (cache_ptr->log_flush)
        if ((cache_ptr->log_flush)(cache_ptr, entry_addr, was_dirty, flags) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_CANTFLUSH, FAIL, "log_flush callback failed");

done:
    assert((ret_value != SUCCEED) || (destroy_entry) || (!entry_ptr->flush_in_progress));
    assert((ret_value != SUCCEED) || (destroy_entry) || (take_ownership) || (!entry_ptr->is_dirty));

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__flush_single_entry() */

/*-------------------------------------------------------------------------
 * Function:    H5C__verify_len_eoa
 *
 * Purpose:     Verify that 'len' does not exceed eoa when 'actual' is
 *              false i.e. 'len" is the initial speculative length from
 *              get_load_size callback with null image pointer.
 *              If exceed, adjust 'len' accordingly.
 *
 *              Verify that 'len' should not exceed eoa when 'actual' is
 *              true i.e. 'len' is the actual length from get_load_size
 *              callback with non-null image pointer.
 *              If exceed, return error.
 *
 * Return:      FAIL if error is detected, SUCCEED otherwise.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5C__verify_len_eoa(H5F_t *f, const H5C_class_t *type, haddr_t addr, size_t *len, bool actual)
{
    H5FD_mem_t cooked_type;         /* Modified type, accounting for switching global heaps */
    haddr_t    eoa;                 /* End-of-allocation in the file */
    herr_t     ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* if type == H5FD_MEM_GHEAP, H5F_block_read() forces
     * type to H5FD_MEM_DRAW via its call to H5F__accum_read().
     * Thus we do the same for purposes of computing the EOA
     * for sanity checks.
     */
    cooked_type = (type->mem_type == H5FD_MEM_GHEAP) ? H5FD_MEM_DRAW : type->mem_type;

    /* Get the file's end-of-allocation value */
    eoa = H5F_get_eoa(f, cooked_type);
    if (!H5_addr_defined(eoa))
        HGOTO_ERROR(H5E_CACHE, H5E_BADVALUE, FAIL, "invalid EOA address for file");

    /* Check for bad address in general */
    if (H5_addr_gt(addr, eoa))
        HGOTO_ERROR(H5E_CACHE, H5E_BADVALUE, FAIL, "address of object past end of allocation");

    /* Check if the amount of data to read will be past the EOA */
    if (H5_addr_gt((addr + *len), eoa)) {
        if (actual)
            HGOTO_ERROR(H5E_CACHE, H5E_BADVALUE, FAIL, "actual len exceeds EOA");
        else
            /* Trim down the length of the metadata */
            *len = (size_t)(eoa - addr);
    } /* end if */

    if (*len <= 0)
        HGOTO_ERROR(H5E_CACHE, H5E_BADVALUE, FAIL, "len not positive after adjustment for EOA");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__verify_len_eoa() */

/*-------------------------------------------------------------------------
 * Function:    H5C__load_entry
 *
 * Purpose:     Attempt to load the entry at the specified disk address
 *              and with the specified type into memory.  If successful.
 *              return the in memory address of the entry.  Return NULL
 *              on failure.
 *
 *              Note that this function simply loads the entry into
 *              core.  It does not insert it into the cache.
 *
 * Return:      Non-NULL on success / NULL on failure.
 *
 *-------------------------------------------------------------------------
 */
void *
H5C__load_entry(H5F_t *f,
#ifdef H5_HAVE_PARALLEL
                bool coll_access,
#endif /* H5_HAVE_PARALLEL */
                const H5C_class_t *type, haddr_t addr, void *udata)
{
    bool               dirty = false; /* Flag indicating whether thing was dirtied during deserialize */
    uint8_t           *image = NULL;  /* Buffer for disk image                    */
    void              *thing = NULL;  /* Pointer to thing loaded                  */
    H5C_cache_entry_t *entry = NULL;  /* Alias for thing loaded, as cache entry   */
    size_t             len;           /* Size of image in file                    */
#ifdef H5_HAVE_PARALLEL
    int      mpi_rank = 0;             /* MPI process rank                         */
    MPI_Comm comm     = MPI_COMM_NULL; /* File MPI Communicator                    */
    int      mpi_code;                 /* MPI error code                           */
#endif                                 /* H5_HAVE_PARALLEL */
    void *ret_value = NULL;            /* Return value                             */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(f);
    assert(f->shared);
    assert(f->shared->cache);
    assert(type);
    assert(H5_addr_defined(addr));
    assert(type->get_initial_load_size);
    if (type->flags & H5C__CLASS_SPECULATIVE_LOAD_FLAG)
        assert(type->get_final_load_size);
    else
        assert(NULL == type->get_final_load_size);
    assert(type->deserialize);

    /* Can't see how skip reads could be usefully combined with
     * the speculative read flag.  Hence disallow.
     */
    assert(!((type->flags & H5C__CLASS_SKIP_READS) && (type->flags & H5C__CLASS_SPECULATIVE_LOAD_FLAG)));

    /* Call the get_initial_load_size callback, to retrieve the initial size of image */
    if (type->get_initial_load_size(udata, &len) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_CANTGET, NULL, "can't retrieve image size");
    assert(len > 0);

    /* Check for possible speculative read off the end of the file */
    if (type->flags & H5C__CLASS_SPECULATIVE_LOAD_FLAG)
        if (H5C__verify_len_eoa(f, type, addr, &len, false) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_BADVALUE, NULL, "invalid len with respect to EOA");

    /* Allocate the buffer for reading the on-disk entry image */
    if (NULL == (image = (uint8_t *)H5MM_malloc(len + H5C_IMAGE_EXTRA_SPACE)))
        HGOTO_ERROR(H5E_CACHE, H5E_CANTALLOC, NULL, "memory allocation failed for on disk image buffer");
#if H5C_DO_MEMORY_SANITY_CHECKS
    H5MM_memcpy(image + len, H5C_IMAGE_SANITY_VALUE, H5C_IMAGE_EXTRA_SPACE);
#endif /* H5C_DO_MEMORY_SANITY_CHECKS */

#ifdef H5_HAVE_PARALLEL
    if (H5F_HAS_FEATURE(f, H5FD_FEAT_HAS_MPI)) {
        if ((mpi_rank = H5F_mpi_get_rank(f)) < 0)
            HGOTO_ERROR(H5E_FILE, H5E_CANTGET, NULL, "Can't get MPI rank");
        if ((comm = H5F_mpi_get_comm(f)) == MPI_COMM_NULL)
            HGOTO_ERROR(H5E_FILE, H5E_CANTGET, NULL, "get_comm request failed");
    }  /* end if */
#endif /* H5_HAVE_PARALLEL */

    /* Get the on-disk entry image */
    if (0 == (type->flags & H5C__CLASS_SKIP_READS)) {
        unsigned tries, max_tries;   /* The # of read attempts               */
        unsigned retries;            /* The # of retries                     */
        htri_t   chk_ret;            /* return from verify_chksum callback   */
        size_t   actual_len = len;   /* The actual length, after speculative reads have been resolved */
        uint64_t nanosec    = 1;     /* # of nanoseconds to sleep between retries */
        void    *new_image;          /* Pointer to image                     */
        bool     len_changed = true; /* Whether to re-check speculative entries */

        /* Get the # of read attempts */
        max_tries = tries = H5F_GET_READ_ATTEMPTS(f);

        /*
         * This do/while loop performs the following till the metadata checksum
         * is correct or the file's number of allowed read attempts are reached.
         *   --read the metadata
         *   --determine the actual size of the metadata
         *   --perform checksum verification
         */
        do {
            if (actual_len != len) {
                if (NULL == (new_image = H5MM_realloc(image, len + H5C_IMAGE_EXTRA_SPACE)))
                    HGOTO_ERROR(H5E_CACHE, H5E_CANTALLOC, NULL, "image null after H5MM_realloc()");
                image = (uint8_t *)new_image;
#if H5C_DO_MEMORY_SANITY_CHECKS
                H5MM_memcpy(image + len, H5C_IMAGE_SANITY_VALUE, H5C_IMAGE_EXTRA_SPACE);
#endif        /* H5C_DO_MEMORY_SANITY_CHECKS */
            } /* end if */

#ifdef H5_HAVE_PARALLEL
            if (!coll_access || 0 == mpi_rank) {
#endif /* H5_HAVE_PARALLEL */
                if (H5F_block_read(f, type->mem_type, addr, len, image) < 0) {
#ifdef H5_HAVE_PARALLEL
                    if (coll_access) {
                        /* Push an error, but still participate in following MPI_Bcast */
                        memset(image, 0, len);
                        HDONE_ERROR(H5E_CACHE, H5E_READERROR, NULL, "Can't read image*");
                    }
                    else
#endif
                        HGOTO_ERROR(H5E_CACHE, H5E_READERROR, NULL, "Can't read image*");
                }

#ifdef H5_HAVE_PARALLEL
            } /* end if */
            /* if the collective metadata read optimization is turned on,
             * bcast the metadata read from process 0 to all ranks in the file
             * communicator
             */
            if (coll_access) {
                int buf_size;

                H5_CHECKED_ASSIGN(buf_size, int, len, size_t);
                if (MPI_SUCCESS != (mpi_code = MPI_Bcast(image, buf_size, MPI_BYTE, 0, comm)))
                    HMPI_GOTO_ERROR(NULL, "MPI_Bcast failed", mpi_code)
            } /* end if */
#endif        /* H5_HAVE_PARALLEL */

            /* If the entry could be read speculatively and the length is still
             *  changing, check for updating the actual size
             */
            if ((type->flags & H5C__CLASS_SPECULATIVE_LOAD_FLAG) && len_changed) {
                /* Retrieve the actual length */
                actual_len = len;
                if (type->get_final_load_size(image, len, udata, &actual_len) < 0)
                    continue; /* Transfer control to while() and count towards retries */

                /* Check for the length changing */
                if (actual_len != len) {
                    /* Verify that the length isn't past the EOA for the file */
                    if (H5C__verify_len_eoa(f, type, addr, &actual_len, true) < 0)
                        HGOTO_ERROR(H5E_CACHE, H5E_BADVALUE, NULL, "actual_len exceeds EOA");

                    /* Expand buffer to new size */
                    if (NULL == (new_image = H5MM_realloc(image, actual_len + H5C_IMAGE_EXTRA_SPACE)))
                        HGOTO_ERROR(H5E_CACHE, H5E_CANTALLOC, NULL, "image null after H5MM_realloc()");
                    image = (uint8_t *)new_image;
#if H5C_DO_MEMORY_SANITY_CHECKS
                    H5MM_memcpy(image + actual_len, H5C_IMAGE_SANITY_VALUE, H5C_IMAGE_EXTRA_SPACE);
#endif /* H5C_DO_MEMORY_SANITY_CHECKS */

                    if (actual_len > len) {
#ifdef H5_HAVE_PARALLEL
                        if (!coll_access || 0 == mpi_rank) {
#endif /* H5_HAVE_PARALLEL */
                            /* If the thing's image needs to be bigger for a speculatively
                             * loaded thing, go get the on-disk image again (the extra portion).
                             */
                            if (H5F_block_read(f, type->mem_type, addr + len, actual_len - len, image + len) <
                                0) {
#ifdef H5_HAVE_PARALLEL
                                if (coll_access) {
                                    /* Push an error, but still participate in following MPI_Bcast */
                                    memset(image + len, 0, actual_len - len);
                                    HDONE_ERROR(H5E_CACHE, H5E_CANTLOAD, NULL, "can't read image");
                                }
                                else
#endif
                                    HGOTO_ERROR(H5E_CACHE, H5E_CANTLOAD, NULL, "can't read image");
                            }

#ifdef H5_HAVE_PARALLEL
                        }
                        /* If the collective metadata read optimization is turned on,
                         * Bcast the metadata read from process 0 to all ranks in the file
                         * communicator */
                        if (coll_access) {
                            int buf_size;

                            H5_CHECKED_ASSIGN(buf_size, int, actual_len - len, size_t);
                            if (MPI_SUCCESS !=
                                (mpi_code = MPI_Bcast(image + len, buf_size, MPI_BYTE, 0, comm)))
                                HMPI_GOTO_ERROR(NULL, "MPI_Bcast failed", mpi_code)
                        } /* end if */
#endif                    /* H5_HAVE_PARALLEL */
                    }     /* end if */
                }         /* end if (actual_len != len) */
                else {
                    /* The length has stabilized */
                    len_changed = false;

                    /* Set the final length */
                    len = actual_len;
                } /* else */
            }     /* end if */

            /* If there's no way to verify the checksum for a piece of metadata
             * (usually because there's no checksum in the file), leave now
             */
            if (type->verify_chksum == NULL)
                break;

            /* Verify the checksum for the metadata image */
            if ((chk_ret = type->verify_chksum(image, actual_len, udata)) < 0)
                HGOTO_ERROR(H5E_CACHE, H5E_CANTGET, NULL, "failure from verify_chksum callback");
            if (chk_ret == true)
                break;

            /* Sleep for some time */
            H5_nanosleep(nanosec);
            nanosec *= 2; /* Double the sleep time next time */
        } while (--tries);

        /* Check for too many tries */
        if (tries == 0)
            HGOTO_ERROR(H5E_CACHE, H5E_READERROR, NULL,
                        "incorrect metadata checksum after all read attempts");

        /* Calculate and track the # of retries */
        retries = max_tries - tries;
        if (retries) /* Does not track 0 retry */
            if (H5F_track_metadata_read_retries(f, (unsigned)type->mem_type, retries) < 0)
                HGOTO_ERROR(H5E_CACHE, H5E_BADVALUE, NULL, "cannot track read tries = %u ", retries);

        /* Set the final length (in case it wasn't set earlier) */
        len = actual_len;
    } /* end if !H5C__CLASS_SKIP_READS */

    /* Deserialize the on-disk image into the native memory form */
    if (NULL == (thing = type->deserialize(image, len, udata, &dirty)))
        HGOTO_ERROR(H5E_CACHE, H5E_CANTLOAD, NULL, "Can't deserialize image");

    entry = (H5C_cache_entry_t *)thing;

    /* In general, an entry should be clean just after it is loaded.
     *
     * However, when this code is used in the metadata cache, it is
     * possible that object headers will be dirty at this point, as
     * the deserialize function will alter object headers if necessary to
     * fix an old bug.
     *
     * In the following assert:
     *
     *     assert( ( dirty == false ) || ( type->id == 5 || type->id == 6 ) );
     *
     * note that type ids 5 & 6 are associated with object headers in the
     * metadata cache.
     *
     * When we get to using H5C for other purposes, we may wish to
     * tighten up the assert so that the loophole only applies to the
     * metadata cache.
     */

    assert((dirty == false) || (type->id == 5 || type->id == 6));

    entry->cache_ptr = f->shared->cache;
    entry->addr      = addr;
    entry->size      = len;
    assert(entry->size < H5C_MAX_ENTRY_SIZE);
    entry->image_ptr        = image;
    entry->image_up_to_date = !dirty;
    entry->type             = type;
    entry->is_dirty         = dirty;
    entry->dirtied          = false;
    entry->is_protected     = false;
    entry->is_read_only     = false;
    entry->ro_ref_count     = 0;
    entry->is_pinned        = false;
    entry->in_slist         = false;
    entry->flush_marker     = false;
#ifdef H5_HAVE_PARALLEL
    entry->clear_on_unprotect = false;
    entry->flush_immediately  = false;
    entry->coll_access        = coll_access;
#endif /* H5_HAVE_PARALLEL */
    entry->flush_in_progress   = false;
    entry->destroy_in_progress = false;

    entry->ring = H5C_RING_UNDEFINED;

    /* Initialize flush dependency fields */
    entry->flush_dep_parent          = NULL;
    entry->flush_dep_nparents        = 0;
    entry->flush_dep_parent_nalloc   = 0;
    entry->flush_dep_nchildren       = 0;
    entry->flush_dep_ndirty_children = 0;
    entry->flush_dep_nunser_children = 0;
    entry->ht_next                   = NULL;
    entry->ht_prev                   = NULL;
    entry->il_next                   = NULL;
    entry->il_prev                   = NULL;

    entry->next = NULL;
    entry->prev = NULL;

#if H5C_MAINTAIN_CLEAN_AND_DIRTY_LRU_LISTS
    entry->aux_next = NULL;
    entry->aux_prev = NULL;
#endif /* H5C_MAINTAIN_CLEAN_AND_DIRTY_LRU_LISTS */

#ifdef H5_HAVE_PARALLEL
    entry->coll_next = NULL;
    entry->coll_prev = NULL;
#endif /* H5_HAVE_PARALLEL */

    /* initialize cache image related fields */
    entry->include_in_image     = false;
    entry->lru_rank             = 0;
    entry->image_dirty          = false;
    entry->fd_parent_count      = 0;
    entry->fd_parent_addrs      = NULL;
    entry->fd_child_count       = 0;
    entry->fd_dirty_child_count = 0;
    entry->image_fd_height      = 0;
    entry->prefetched           = false;
    entry->prefetch_type_id     = 0;
    entry->age                  = 0;
    entry->prefetched_dirty     = false;
#ifndef NDEBUG /* debugging field */
    entry->serialization_count = 0;
#endif

    /* initialize tag list fields */
    entry->tl_next  = NULL;
    entry->tl_prev  = NULL;
    entry->tag_info = NULL;

    H5C__RESET_CACHE_ENTRY_STATS(entry);

    ret_value = thing;

done:
    /* Cleanup on error */
    if (NULL == ret_value) {
        /* Release resources */
        if (thing && type->free_icr(thing) < 0)
            HDONE_ERROR(H5E_CACHE, H5E_CANTFLUSH, NULL, "free_icr callback failed");
        if (image)
            image = (uint8_t *)H5MM_xfree(image);
    } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__load_entry() */

/*-------------------------------------------------------------------------
 * Function:    H5C__mark_flush_dep_dirty()
 *
 * Purpose:     Recursively propagate the flush_dep_ndirty_children flag
 *              up the dependency chain in response to entry either
 *              becoming dirty or having its flush_dep_ndirty_children
 *              increased from 0.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5C__mark_flush_dep_dirty(H5C_cache_entry_t *entry)
{
    unsigned u;                   /* Local index variable */
    herr_t   ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(entry);

    /* Iterate over the parent entries, if any */
    for (u = 0; u < entry->flush_dep_nparents; u++) {
        /* Sanity check */
        assert(entry->flush_dep_parent[u]->flush_dep_ndirty_children <
               entry->flush_dep_parent[u]->flush_dep_nchildren);

        /* Adjust the parent's number of dirty children */
        entry->flush_dep_parent[u]->flush_dep_ndirty_children++;

        /* If the parent has a 'notify' callback, send a 'child entry dirtied' notice */
        if (entry->flush_dep_parent[u]->type->notify &&
            (entry->flush_dep_parent[u]->type->notify)(H5C_NOTIFY_ACTION_CHILD_DIRTIED,
                                                       entry->flush_dep_parent[u]) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_CANTNOTIFY, FAIL,
                        "can't notify parent about child entry dirty flag set");
    } /* end for */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__mark_flush_dep_dirty() */

/*-------------------------------------------------------------------------
 * Function:    H5C__mark_flush_dep_clean()
 *
 * Purpose:     Recursively propagate the flush_dep_ndirty_children flag
 *              up the dependency chain in response to entry either
 *              becoming clean or having its flush_dep_ndirty_children
 *              reduced to 0.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5C__mark_flush_dep_clean(H5C_cache_entry_t *entry)
{
    int    i;                   /* Local index variable */
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(entry);

    /* Iterate over the parent entries, if any */
    /* Note reverse iteration order, in case the callback removes the flush
     *  dependency - QAK, 2017/08/12
     */
    for (i = ((int)entry->flush_dep_nparents) - 1; i >= 0; i--) {
        /* Sanity check */
        assert(entry->flush_dep_parent[i]->flush_dep_ndirty_children > 0);

        /* Adjust the parent's number of dirty children */
        entry->flush_dep_parent[i]->flush_dep_ndirty_children--;

        /* If the parent has a 'notify' callback, send a 'child entry cleaned' notice */
        if (entry->flush_dep_parent[i]->type->notify &&
            (entry->flush_dep_parent[i]->type->notify)(H5C_NOTIFY_ACTION_CHILD_CLEANED,
                                                       entry->flush_dep_parent[i]) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_CANTNOTIFY, FAIL,
                        "can't notify parent about child entry dirty flag reset");
    } /* end for */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__mark_flush_dep_clean() */

/*-------------------------------------------------------------------------
 * Function:    H5C__mark_flush_dep_serialized()
 *
 * Purpose:     Decrement the flush_dep_nunser_children fields of all the
 *        target entry's flush dependency parents in response to
 *        the target entry becoming serialized.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5C__mark_flush_dep_serialized(H5C_cache_entry_t *entry_ptr)
{
    int    i;                   /* Local index variable */
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(entry_ptr);

    /* Iterate over the parent entries, if any */
    /* Note reverse iteration order, in case the callback removes the flush
     *  dependency - QAK, 2017/08/12
     */
    for (i = ((int)entry_ptr->flush_dep_nparents) - 1; i >= 0; i--) {
        /* Sanity checks */
        assert(entry_ptr->flush_dep_parent);
        assert(entry_ptr->flush_dep_parent[i]->flush_dep_nunser_children > 0);

        /* decrement the parents number of unserialized children */
        entry_ptr->flush_dep_parent[i]->flush_dep_nunser_children--;

        /* If the parent has a 'notify' callback, send a 'child entry serialized' notice */
        if (entry_ptr->flush_dep_parent[i]->type->notify &&
            (entry_ptr->flush_dep_parent[i]->type->notify)(H5C_NOTIFY_ACTION_CHILD_SERIALIZED,
                                                           entry_ptr->flush_dep_parent[i]) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_CANTNOTIFY, FAIL,
                        "can't notify parent about child entry serialized flag set");
    } /* end for */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__mark_flush_dep_serialized() */

/*-------------------------------------------------------------------------
 * Function:    H5C__mark_flush_dep_unserialized()
 *
 * Purpose:     Increment the flush_dep_nunser_children fields of all the
 *              target entry's flush dependency parents in response to
 *              the target entry becoming unserialized.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5C__mark_flush_dep_unserialized(H5C_cache_entry_t *entry_ptr)
{
    unsigned u;                   /* Local index variable */
    herr_t   ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(entry_ptr);

    /* Iterate over the parent entries, if any */
    for (u = 0; u < entry_ptr->flush_dep_nparents; u++) {
        /* Sanity check */
        assert(entry_ptr->flush_dep_parent);
        assert(entry_ptr->flush_dep_parent[u]->flush_dep_nunser_children <
               entry_ptr->flush_dep_parent[u]->flush_dep_nchildren);

        /* increment parents number of usserialized children */
        entry_ptr->flush_dep_parent[u]->flush_dep_nunser_children++;

        /* If the parent has a 'notify' callback, send a 'child entry unserialized' notice */
        if (entry_ptr->flush_dep_parent[u]->type->notify &&
            (entry_ptr->flush_dep_parent[u]->type->notify)(H5C_NOTIFY_ACTION_CHILD_UNSERIALIZED,
                                                           entry_ptr->flush_dep_parent[u]) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_CANTNOTIFY, FAIL,
                        "can't notify parent about child entry serialized flag reset");
    } /* end for */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__mark_flush_dep_unserialized() */

#ifndef NDEBUG
/*-------------------------------------------------------------------------
 * Function:    H5C__assert_flush_dep_nocycle()
 *
 * Purpose:     Assert recursively that base_entry is not the same as
 *              entry, and perform the same assertion on all of entry's
 *              flush dependency parents.  This is used to detect cycles
 *              created by flush dependencies.
 *
 * Return:      void
 *
 *-------------------------------------------------------------------------
 */
static void
H5C__assert_flush_dep_nocycle(const H5C_cache_entry_t *entry, const H5C_cache_entry_t *base_entry)
{
    unsigned u; /* Local index variable */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(entry);
    assert(base_entry);

    /* Make sure the entries are not the same */
    assert(base_entry != entry);

    /* Iterate over entry's parents (if any) */
    for (u = 0; u < entry->flush_dep_nparents; u++)
        H5C__assert_flush_dep_nocycle(entry->flush_dep_parent[u], base_entry);

    FUNC_LEAVE_NOAPI_VOID
} /* H5C__assert_flush_dep_nocycle() */
#endif

/*-------------------------------------------------------------------------
 * Function:    H5C__serialize_single_entry
 *
 * Purpose:     Serialize the cache entry pointed to by the entry_ptr
 *        parameter.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C__serialize_single_entry(H5F_t *f, H5C_t *cache_ptr, H5C_cache_entry_t *entry_ptr)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(f);
    assert(cache_ptr);
    assert(entry_ptr);
    assert(!entry_ptr->prefetched);
    assert(!entry_ptr->image_up_to_date);
    assert(entry_ptr->is_dirty);
    assert(!entry_ptr->is_protected);
    assert(!entry_ptr->flush_in_progress);
    assert(entry_ptr->type);

    /* Set entry_ptr->flush_in_progress to true so the target entry
     * will not be evicted out from under us.  Must set it back to false
     * when we are done.
     */
    entry_ptr->flush_in_progress = true;

    /* Allocate buffer for the entry image if required. */
    if (NULL == entry_ptr->image_ptr) {
        assert(entry_ptr->size > 0);
        if (NULL == (entry_ptr->image_ptr = H5MM_malloc(entry_ptr->size + H5C_IMAGE_EXTRA_SPACE)))
            HGOTO_ERROR(H5E_CACHE, H5E_CANTALLOC, FAIL, "memory allocation failed for on disk image buffer");
#if H5C_DO_MEMORY_SANITY_CHECKS
        H5MM_memcpy(((uint8_t *)entry_ptr->image_ptr) + entry_ptr->size, H5C_IMAGE_SANITY_VALUE,
                    H5C_IMAGE_EXTRA_SPACE);
#endif /* H5C_DO_MEMORY_SANITY_CHECKS */
    }  /* end if */

    /* Generate image for entry */
    if (H5C__generate_image(f, cache_ptr, entry_ptr) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_CANTSERIALIZE, FAIL, "Can't generate image for cache entry");

    /* Reset the flush_in progress flag */
    entry_ptr->flush_in_progress = false;

done:
    assert((ret_value != SUCCEED) || (!entry_ptr->flush_in_progress));
    assert((ret_value != SUCCEED) || (entry_ptr->image_up_to_date));
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__serialize_single_entry() */

/*-------------------------------------------------------------------------
 * Function:    H5C__destroy_pf_entry_child_flush_deps()
 *
 * Purpose:     Destroy all flush dependencies in this the supplied
 *		prefetched entry is the parent.  Note that the children
 *		in these flush dependencies must be prefetched entries as
 *		well.
 *
 *		As this action is part of the process of transferring all
 *		such flush dependencies to the deserialized version of the
 *		prefetched entry, ensure that the data necessary to complete
 *		the transfer is retained.
 *
 *		Note: The current implementation of this function is
 *		      quite inefficient -- mostly due to the current
 *		      implementation of flush dependencies.  This should
 *		      be fixed at some point.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5C__destroy_pf_entry_child_flush_deps(H5C_t *cache_ptr, H5C_cache_entry_t *pf_entry_ptr,
                                       H5C_cache_entry_t **fd_children)
{
    H5C_cache_entry_t *entry_ptr;
#ifndef NDEBUG
    unsigned entries_visited = 0;
#endif
    int    fd_children_found = 0;
    bool   found;
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(cache_ptr);
    assert(pf_entry_ptr);
    assert(pf_entry_ptr->type);
    assert(pf_entry_ptr->type->id == H5AC_PREFETCHED_ENTRY_ID);
    assert(pf_entry_ptr->prefetched);
    assert(pf_entry_ptr->fd_child_count > 0);
    assert(fd_children);

    /* Scan each entry on the index list */
    entry_ptr = cache_ptr->il_head;
    while (entry_ptr != NULL) {
        /* Here we look at entry_ptr->flush_dep_nparents and not
         * entry_ptr->fd_parent_count as it is possible that some
         * or all of the prefetched flush dependency child relationships
         * have already been destroyed.
         */
        if (entry_ptr->prefetched && (entry_ptr->flush_dep_nparents > 0)) {
            unsigned u; /* Local index variable */

            /* Re-init */
            u     = 0;
            found = false;

            /* Sanity checks */
            assert(entry_ptr->type);
            assert(entry_ptr->type->id == H5AC_PREFETCHED_ENTRY_ID);
            assert(entry_ptr->fd_parent_count >= entry_ptr->flush_dep_nparents);
            assert(entry_ptr->fd_parent_addrs);
            assert(entry_ptr->flush_dep_parent);

            /* Look for correct entry */
            while (!found && (u < entry_ptr->fd_parent_count)) {
                /* Sanity check entry */
                assert(entry_ptr->flush_dep_parent[u]);

                /* Correct entry? */
                if (pf_entry_ptr == entry_ptr->flush_dep_parent[u])
                    found = true;

                u++;
            } /* end while */

            if (found) {
                assert(NULL == fd_children[fd_children_found]);

                /* Remove flush dependency */
                fd_children[fd_children_found] = entry_ptr;
                fd_children_found++;
                if (H5C_destroy_flush_dependency(pf_entry_ptr, entry_ptr) < 0)
                    HGOTO_ERROR(H5E_CACHE, H5E_CANTUNDEPEND, FAIL,
                                "can't destroy pf entry child flush dependency");

#ifndef NDEBUG
                /* Sanity check -- verify that the address of the parent
                 * appears in entry_ptr->fd_parent_addrs.  Must do a search,
                 * as with flush dependency creates and destroys,
                 * entry_ptr->fd_parent_addrs and entry_ptr->flush_dep_parent
                 * can list parents in different order.
                 */
                found = false;
                u     = 0;
                while (!found && u < entry_ptr->fd_parent_count) {
                    if (pf_entry_ptr->addr == entry_ptr->fd_parent_addrs[u])
                        found = true;
                    u++;
                } /* end while */
                assert(found);
#endif
            } /* end if */
        }     /* end if */

#ifndef NDEBUG
        entries_visited++;
#endif
        entry_ptr = entry_ptr->il_next;
    } /* end while */

    /* Post-op sanity checks */
    assert(NULL == fd_children[fd_children_found]);
    assert((unsigned)fd_children_found == pf_entry_ptr->fd_child_count);
    assert(entries_visited == cache_ptr->index_len);
    assert(!pf_entry_ptr->is_pinned);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__destroy_pf_entry_child_flush_deps() */

/*-------------------------------------------------------------------------
 * Function:    H5C__deserialize_prefetched_entry()
 *
 * Purpose:     Deserialize the supplied prefetched entry entry, and return
 *              a pointer to the deserialized entry in *entry_ptr_ptr.
 *              If successful, remove the prefetched entry from the cache,
 *              and free it.  Insert the deserialized entry into the cache.
 *
 *              Note that the on disk image of the entry is not freed --
 *              a pointer to it is stored in the deserialized entries'
 *              image_ptr field, and its image_up_to_date field is set to
 *              true unless the entry is dirtied by the deserialize call.
 *
 *              If the prefetched entry is a flush dependency child,
 *              destroy that flush dependency prior to calling the
 *              deserialize callback.  If appropriate, the flush dependency
 *              relationship will be recreated by the cache client.
 *
 *              If the prefetched entry is a flush dependency parent,
 *              destroy the flush dependency relationship with all its
 *              children.  As all these children must be prefetched entries,
 *              recreate these flush dependency relationships with
 *              deserialized entry after it is inserted in the cache.
 *
 *              Since deserializing a prefetched entry is semantically
 *              equivalent to a load, issue an entry loaded notification
 *              if the notify callback is defined.
 *
 * Return:      SUCCEED on success, and FAIL on failure.
 *
 *              Note that *entry_ptr_ptr is undefined on failure.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5C__deserialize_prefetched_entry(H5F_t *f, H5C_t *cache_ptr, H5C_cache_entry_t **entry_ptr_ptr,
                                  const H5C_class_t *type, haddr_t addr, void *udata)
{
    bool dirty = false;                     /* Flag indicating whether thing was
                                             * dirtied during deserialize
                                             */
    size_t             len;                 /* Size of image in file */
    void              *thing = NULL;        /* Pointer to thing loaded */
    H5C_cache_entry_t *pf_entry_ptr;        /* pointer to the prefetched entry   */
                                            /* supplied in *entry_ptr_ptr.       */
    H5C_cache_entry_t *ds_entry_ptr;        /* Alias for thing loaded, as cache
                                             * entry
                                             */
    H5C_cache_entry_t **fd_children = NULL; /* Pointer to a dynamically      */
                                            /* allocated array of pointers to    */
                                            /* the flush dependency children of  */
                                            /* the prefetched entry, or NULL if  */
                                            /* that array does not exist.        */
    unsigned flush_flags = (H5C__FLUSH_INVALIDATE_FLAG | H5C__FLUSH_CLEAR_ONLY_FLAG);
    int      i;
    herr_t   ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* sanity checks */
    assert(f);
    assert(f->shared);
    assert(f->shared->cache);
    assert(f->shared->cache == cache_ptr);
    assert(entry_ptr_ptr);
    assert(*entry_ptr_ptr);
    pf_entry_ptr = *entry_ptr_ptr;
    assert(pf_entry_ptr->type);
    assert(pf_entry_ptr->type->id == H5AC_PREFETCHED_ENTRY_ID);
    assert(pf_entry_ptr->prefetched);
    assert(pf_entry_ptr->image_up_to_date);
    assert(pf_entry_ptr->image_ptr);
    assert(pf_entry_ptr->size > 0);
    assert(pf_entry_ptr->addr == addr);
    assert(type);
    assert(type->id == pf_entry_ptr->prefetch_type_id);
    assert(type->mem_type == cache_ptr->class_table_ptr[type->id]->mem_type);

    /* verify absence of prohibited or unsupported type flag combinations */
    assert(!(type->flags & H5C__CLASS_SKIP_READS));

    /* Can't see how skip reads could be usefully combined with
     * either the speculative read flag.  Hence disallow.
     */
    assert(!((type->flags & H5C__CLASS_SKIP_READS) && (type->flags & H5C__CLASS_SPECULATIVE_LOAD_FLAG)));
    assert(H5_addr_defined(addr));
    assert(type->get_initial_load_size);
    assert(type->deserialize);

    /* if *pf_entry_ptr is a flush dependency child, destroy all such
     * relationships now.  The client will restore the relationship(s) with
     * the deserialized entry if appropriate.
     */
    assert(pf_entry_ptr->fd_parent_count == pf_entry_ptr->flush_dep_nparents);
    for (i = (int)(pf_entry_ptr->fd_parent_count) - 1; i >= 0; i--) {
        assert(pf_entry_ptr->flush_dep_parent);
        assert(pf_entry_ptr->flush_dep_parent[i]);
        assert(pf_entry_ptr->flush_dep_parent[i]->flush_dep_nchildren > 0);
        assert(pf_entry_ptr->fd_parent_addrs);
        assert(pf_entry_ptr->flush_dep_parent[i]->addr == pf_entry_ptr->fd_parent_addrs[i]);

        if (H5C_destroy_flush_dependency(pf_entry_ptr->flush_dep_parent[i], pf_entry_ptr) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_CANTUNDEPEND, FAIL, "can't destroy pf entry parent flush dependency");

        pf_entry_ptr->fd_parent_addrs[i] = HADDR_UNDEF;
    } /* end for */
    assert(pf_entry_ptr->flush_dep_nparents == 0);

    /* If *pf_entry_ptr is a flush dependency parent, destroy its flush
     * dependency relationships with all its children (which must be
     * prefetched entries as well).
     *
     * These flush dependency relationships will have to be restored
     * after the deserialized entry is inserted into the cache in order
     * to transfer these relationships to the new entry.  Hence save the
     * pointers to the flush dependency children of *pf_enty_ptr for later
     * use.
     */
    if (pf_entry_ptr->fd_child_count > 0) {
        if (NULL == (fd_children = (H5C_cache_entry_t **)H5MM_calloc(
                         sizeof(H5C_cache_entry_t **) * (size_t)(pf_entry_ptr->fd_child_count + 1))))
            HGOTO_ERROR(H5E_CACHE, H5E_CANTALLOC, FAIL, "memory allocation failed for fd child ptr array");

        if (H5C__destroy_pf_entry_child_flush_deps(cache_ptr, pf_entry_ptr, fd_children) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_CANTUNDEPEND, FAIL,
                        "can't destroy pf entry child flush dependency(s).");
    } /* end if */

    /* Since the size of the on disk image is known exactly, there is
     * no need for either a call to the get_initial_load_size() callback,
     * or retries if the H5C__CLASS_SPECULATIVE_LOAD_FLAG flag is set.
     * Similarly, there is no need to clamp possible reads beyond
     * EOF.
     */
    len = pf_entry_ptr->size;

    /* Deserialize the prefetched on-disk image of the entry into the
     * native memory form
     */
    if (NULL == (thing = type->deserialize(pf_entry_ptr->image_ptr, len, udata, &dirty)))
        HGOTO_ERROR(H5E_CACHE, H5E_CANTLOAD, FAIL, "Can't deserialize image");
    ds_entry_ptr = (H5C_cache_entry_t *)thing;

    /* In general, an entry should be clean just after it is loaded.
     *
     * However, when this code is used in the metadata cache, it is
     * possible that object headers will be dirty at this point, as
     * the deserialize function will alter object headers if necessary to
     * fix an old bug.
     *
     * In the following assert:
     *
     * 	assert( ( dirty == false ) || ( type->id == 5 || type->id == 6 ) );
     *
     * note that type ids 5 & 6 are associated with object headers in the
     * metadata cache.
     *
     * When we get to using H5C for other purposes, we may wish to
     * tighten up the assert so that the loophole only applies to the
     * metadata cache.
     *
     * Note that at present, dirty can't be set to true with prefetched
     * entries.  However this may change, so include this functionality
     * against that possibility.
     *
     * Also, note that it is possible for a prefetched entry to be dirty --
     * hence the value assigned to ds_entry_ptr->is_dirty below.
     */

    assert((dirty == false) || (type->id == 5 || type->id == 6));

    ds_entry_ptr->cache_ptr = f->shared->cache;
    ds_entry_ptr->addr      = addr;
    ds_entry_ptr->size      = len;
    assert(ds_entry_ptr->size < H5C_MAX_ENTRY_SIZE);
    ds_entry_ptr->image_ptr        = pf_entry_ptr->image_ptr;
    ds_entry_ptr->image_up_to_date = !dirty;
    ds_entry_ptr->type             = type;
    ds_entry_ptr->is_dirty         = dirty | pf_entry_ptr->is_dirty;
    ds_entry_ptr->dirtied          = false;
    ds_entry_ptr->is_protected     = false;
    ds_entry_ptr->is_read_only     = false;
    ds_entry_ptr->ro_ref_count     = 0;
    ds_entry_ptr->is_pinned        = false;
    ds_entry_ptr->in_slist         = false;
    ds_entry_ptr->flush_marker     = false;
#ifdef H5_HAVE_PARALLEL
    ds_entry_ptr->clear_on_unprotect = false;
    ds_entry_ptr->flush_immediately  = false;
    ds_entry_ptr->coll_access        = false;
#endif /* H5_HAVE_PARALLEL */
    ds_entry_ptr->flush_in_progress   = false;
    ds_entry_ptr->destroy_in_progress = false;

    ds_entry_ptr->ring = pf_entry_ptr->ring;

    /* Initialize flush dependency height fields */
    ds_entry_ptr->flush_dep_parent          = NULL;
    ds_entry_ptr->flush_dep_nparents        = 0;
    ds_entry_ptr->flush_dep_parent_nalloc   = 0;
    ds_entry_ptr->flush_dep_nchildren       = 0;
    ds_entry_ptr->flush_dep_ndirty_children = 0;
    ds_entry_ptr->flush_dep_nunser_children = 0;

    /* Initialize fields supporting the hash table: */
    ds_entry_ptr->ht_next = NULL;
    ds_entry_ptr->ht_prev = NULL;
    ds_entry_ptr->il_next = NULL;
    ds_entry_ptr->il_prev = NULL;

    /* Initialize fields supporting replacement policies: */
    ds_entry_ptr->next = NULL;
    ds_entry_ptr->prev = NULL;
#if H5C_MAINTAIN_CLEAN_AND_DIRTY_LRU_LISTS
    ds_entry_ptr->aux_next = NULL;
    ds_entry_ptr->aux_prev = NULL;
#endif /* H5C_MAINTAIN_CLEAN_AND_DIRTY_LRU_LISTS */
#ifdef H5_HAVE_PARALLEL
    pf_entry_ptr->coll_next = NULL;
    pf_entry_ptr->coll_prev = NULL;
#endif /* H5_HAVE_PARALLEL */

    /* Initialize cache image related fields */
    ds_entry_ptr->include_in_image     = false;
    ds_entry_ptr->lru_rank             = 0;
    ds_entry_ptr->image_dirty          = false;
    ds_entry_ptr->fd_parent_count      = 0;
    ds_entry_ptr->fd_parent_addrs      = NULL;
    ds_entry_ptr->fd_child_count       = pf_entry_ptr->fd_child_count;
    ds_entry_ptr->fd_dirty_child_count = 0;
    ds_entry_ptr->image_fd_height      = 0;
    ds_entry_ptr->prefetched           = false;
    ds_entry_ptr->prefetch_type_id     = 0;
    ds_entry_ptr->age                  = 0;
    ds_entry_ptr->prefetched_dirty     = pf_entry_ptr->prefetched_dirty;
#ifndef NDEBUG /* debugging field */
    ds_entry_ptr->serialization_count = 0;
#endif

    H5C__RESET_CACHE_ENTRY_STATS(ds_entry_ptr);

    /* Apply to to the newly deserialized entry */
    if (H5C__tag_entry(cache_ptr, ds_entry_ptr) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_CANTTAG, FAIL, "Cannot tag metadata entry");

    /* We have successfully deserialized the prefetched entry.
     *
     * Before we return a pointer to the deserialized entry, we must remove
     * the prefetched entry from the cache, discard it, and replace it with
     * the deserialized entry.  Note that we do not free the prefetched
     * entries image, as that has been transferred to the deserialized
     * entry.
     *
     * Also note that we have not yet restored any flush dependencies.  This
     * must wait until the deserialized entry is inserted in the cache.
     *
     * To delete the prefetched entry from the cache:
     *
     *  1) Set pf_entry_ptr->image_ptr to NULL.  Since we have already
     *     transferred the buffer containing the image to *ds_entry_ptr,
     *     this is not a memory leak.
     *
     *  2) Call H5C__flush_single_entry() with the H5C__FLUSH_INVALIDATE_FLAG
     *     and H5C__FLUSH_CLEAR_ONLY_FLAG flags set.
     */
    pf_entry_ptr->image_ptr = NULL;

    if (pf_entry_ptr->is_dirty) {
        assert(((cache_ptr->slist_enabled) && (pf_entry_ptr->in_slist)) ||
               ((!cache_ptr->slist_enabled) && (!pf_entry_ptr->in_slist)));

        flush_flags |= H5C__DEL_FROM_SLIST_ON_DESTROY_FLAG;
    } /* end if */

    if (H5C__flush_single_entry(f, pf_entry_ptr, flush_flags) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_CANTEXPUNGE, FAIL, "can't expunge prefetched entry");

#ifndef NDEGUG /* verify deletion */
    H5C__SEARCH_INDEX(cache_ptr, addr, pf_entry_ptr, FAIL);

    assert(NULL == pf_entry_ptr);
#endif

    /* Insert the deserialized entry into the cache.  */
    H5C__INSERT_IN_INDEX(cache_ptr, ds_entry_ptr, FAIL);

    assert(!ds_entry_ptr->in_slist);
    if (ds_entry_ptr->is_dirty)
        H5C__INSERT_ENTRY_IN_SLIST(cache_ptr, ds_entry_ptr, FAIL);

    H5C__UPDATE_RP_FOR_INSERTION(cache_ptr, ds_entry_ptr, FAIL);

    /* Deserializing a prefetched entry is the conceptual equivalent of
     * loading it from file.  If the deserialized entry has a notify callback,
     * send an "after load" notice now that the deserialized entry is fully
     * integrated into the cache.
     */
    if (ds_entry_ptr->type->notify &&
        (ds_entry_ptr->type->notify)(H5C_NOTIFY_ACTION_AFTER_LOAD, ds_entry_ptr) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_CANTNOTIFY, FAIL, "can't notify client about entry loaded into cache");

    /* Restore flush dependencies with the flush dependency children of
     * of the prefetched entry.  Note that we must protect *ds_entry_ptr
     * before the call to avoid triggering sanity check failures, and
     * then unprotect it afterwards.
     */
    i = 0;
    if (fd_children != NULL) {
        H5C__UPDATE_RP_FOR_PROTECT(cache_ptr, ds_entry_ptr, FAIL);
        ds_entry_ptr->is_protected = true;
        while (fd_children[i] != NULL) {
            /* Sanity checks */
            assert((fd_children[i])->prefetched);
            assert((fd_children[i])->fd_parent_count > 0);
            assert((fd_children[i])->fd_parent_addrs);

#ifndef NDEBUG
            {
                int  j;
                bool found;

                j     = 0;
                found = false;
                while ((j < (int)((fd_children[i])->fd_parent_count)) && (!found)) {
                    if ((fd_children[i])->fd_parent_addrs[j] == ds_entry_ptr->addr)
                        found = true;

                    j++;
                } /* end while */
                assert(found);
            }
#endif

            if (H5C_create_flush_dependency(ds_entry_ptr, fd_children[i]) < 0)
                HGOTO_ERROR(H5E_CACHE, H5E_CANTDEPEND, FAIL, "Can't restore child flush dependency");

            i++;
        } /* end while */

        H5C__UPDATE_RP_FOR_UNPROTECT(cache_ptr, ds_entry_ptr, FAIL);
        ds_entry_ptr->is_protected = false;
    } /* end if ( fd_children != NULL ) */
    assert((unsigned)i == ds_entry_ptr->fd_child_count);

    ds_entry_ptr->fd_child_count = 0;
    H5C__UPDATE_STATS_FOR_PREFETCH_HIT(cache_ptr);

    /* finally, pass ds_entry_ptr back to the caller */
    *entry_ptr_ptr = ds_entry_ptr;

done:
    if (fd_children)
        fd_children = (H5C_cache_entry_t **)H5MM_xfree((void *)fd_children);

    /* Release resources on error */
    if (FAIL == ret_value)
        if (thing && type->free_icr(thing) < 0)
            HDONE_ERROR(H5E_CACHE, H5E_CANTFLUSH, FAIL, "free_icr callback failed");

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__deserialize_prefetched_entry() */

/*-------------------------------------------------------------------------
 * Function:    H5C_insert_entry
 *
 * Purpose:     Adds the specified thing to the cache.  The thing need not
 *              exist on disk yet, but it must have an address and disk
 *              space reserved.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C_insert_entry(H5F_t *f, const H5C_class_t *type, haddr_t addr, void *thing, unsigned int flags)
{
    H5C_t      *cache_ptr;
    H5AC_ring_t ring = H5C_RING_UNDEFINED;
    bool        insert_pinned;
    bool        flush_last;
#ifdef H5_HAVE_PARALLEL
    bool coll_access = false; /* whether access to the cache entry is done collectively */
#endif                        /* H5_HAVE_PARALLEL */
    bool               set_flush_marker;
    bool               write_permitted = true;
    size_t             empty_space;
    H5C_cache_entry_t *entry_ptr = NULL;
    H5C_cache_entry_t *test_entry_ptr;
    bool               entry_tagged = false;
    herr_t             ret_value    = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    assert(f);
    assert(f->shared);

    cache_ptr = f->shared->cache;

    assert(cache_ptr);
    assert(type);
    assert(type->mem_type == cache_ptr->class_table_ptr[type->id]->mem_type);
    assert(type->image_len);
    assert(H5_addr_defined(addr));
    assert(thing);

#ifdef H5C_DO_EXTREME_SANITY_CHECKS
    /* no need to verify that entry is not already in the index as */
    /* we already make that check below.                           */
    if (H5C__validate_protected_entry_list(cache_ptr) < 0 || H5C__validate_pinned_entry_list(cache_ptr) < 0 ||
        H5C__validate_lru_list(cache_ptr) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "an extreme sanity check failed on entry");
#endif /* H5C_DO_EXTREME_SANITY_CHECKS */

    set_flush_marker = ((flags & H5C__SET_FLUSH_MARKER_FLAG) != 0);
    insert_pinned    = ((flags & H5C__PIN_ENTRY_FLAG) != 0);
    flush_last       = ((flags & H5C__FLUSH_LAST_FLAG) != 0);

    /* Get the ring type from the API context */
    ring = H5CX_get_ring();

    entry_ptr = (H5C_cache_entry_t *)thing;

    /* verify that the new entry isn't already in the hash table -- scream
     * and die if it is.
     */

    H5C__SEARCH_INDEX(cache_ptr, addr, test_entry_ptr, FAIL);

    if (test_entry_ptr != NULL) {
        if (test_entry_ptr == entry_ptr)
            HGOTO_ERROR(H5E_CACHE, H5E_CANTINS, FAIL, "entry already in cache");
        else
            HGOTO_ERROR(H5E_CACHE, H5E_CANTINS, FAIL, "duplicate entry in cache");
    } /* end if */

    entry_ptr->cache_ptr = cache_ptr;
    entry_ptr->addr      = addr;
    entry_ptr->type      = type;

    entry_ptr->image_ptr        = NULL;
    entry_ptr->image_up_to_date = false;

    entry_ptr->is_protected = false;
    entry_ptr->is_read_only = false;
    entry_ptr->ro_ref_count = 0;

    entry_ptr->is_pinned          = insert_pinned;
    entry_ptr->pinned_from_client = insert_pinned;
    entry_ptr->pinned_from_cache  = false;
    entry_ptr->flush_me_last      = flush_last;

    /* newly inserted entries are assumed to be dirty */
    entry_ptr->is_dirty = true;

    /* not protected, so can't be dirtied */
    entry_ptr->dirtied = false;

    /* Retrieve the size of the thing */
    if ((type->image_len)(thing, &(entry_ptr->size)) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_CANTGETSIZE, FAIL, "can't get size of thing");
    assert(entry_ptr->size > 0 && entry_ptr->size < H5C_MAX_ENTRY_SIZE);

    entry_ptr->in_slist = false;

#ifdef H5_HAVE_PARALLEL
    entry_ptr->clear_on_unprotect = false;
    entry_ptr->flush_immediately  = false;
#endif /* H5_HAVE_PARALLEL */

    entry_ptr->flush_in_progress   = false;
    entry_ptr->destroy_in_progress = false;

    entry_ptr->ring = ring;

    /* Initialize flush dependency fields */
    entry_ptr->flush_dep_parent          = NULL;
    entry_ptr->flush_dep_nparents        = 0;
    entry_ptr->flush_dep_parent_nalloc   = 0;
    entry_ptr->flush_dep_nchildren       = 0;
    entry_ptr->flush_dep_ndirty_children = 0;
    entry_ptr->flush_dep_nunser_children = 0;

    entry_ptr->ht_next = NULL;
    entry_ptr->ht_prev = NULL;
    entry_ptr->il_next = NULL;
    entry_ptr->il_prev = NULL;

    entry_ptr->next = NULL;
    entry_ptr->prev = NULL;

#if H5C_MAINTAIN_CLEAN_AND_DIRTY_LRU_LISTS
    entry_ptr->aux_next = NULL;
    entry_ptr->aux_prev = NULL;
#endif /* H5C_MAINTAIN_CLEAN_AND_DIRTY_LRU_LISTS */

#ifdef H5_HAVE_PARALLEL
    entry_ptr->coll_next = NULL;
    entry_ptr->coll_prev = NULL;
#endif /* H5_HAVE_PARALLEL */

    /* initialize cache image related fields */
    entry_ptr->include_in_image     = false;
    entry_ptr->lru_rank             = 0;
    entry_ptr->image_dirty          = false;
    entry_ptr->fd_parent_count      = 0;
    entry_ptr->fd_parent_addrs      = NULL;
    entry_ptr->fd_child_count       = 0;
    entry_ptr->fd_dirty_child_count = 0;
    entry_ptr->image_fd_height      = 0;
    entry_ptr->prefetched           = false;
    entry_ptr->prefetch_type_id     = 0;
    entry_ptr->age                  = 0;
    entry_ptr->prefetched_dirty     = false;
#ifndef NDEBUG /* debugging field */
    entry_ptr->serialization_count = 0;
#endif

    /* initialize tag list fields */
    entry_ptr->tl_next  = NULL;
    entry_ptr->tl_prev  = NULL;
    entry_ptr->tag_info = NULL;

    /* Apply tag to newly inserted entry */
    if (H5C__tag_entry(cache_ptr, entry_ptr) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_CANTTAG, FAIL, "Cannot tag metadata entry");
    entry_tagged = true;

    H5C__RESET_CACHE_ENTRY_STATS(entry_ptr);

    if (cache_ptr->flash_size_increase_possible &&
        (entry_ptr->size > cache_ptr->flash_size_increase_threshold))
        if (H5C__flash_increase_cache_size(cache_ptr, 0, entry_ptr->size) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_CANTINS, FAIL, "H5C__flash_increase_cache_size failed");

    if (cache_ptr->index_size >= cache_ptr->max_cache_size)
        empty_space = 0;
    else
        empty_space = cache_ptr->max_cache_size - cache_ptr->index_size;

    if (cache_ptr->evictions_enabled &&
        (((cache_ptr->index_size + entry_ptr->size) > cache_ptr->max_cache_size) ||
         (((empty_space + cache_ptr->clean_index_size) < cache_ptr->min_clean_size)))) {
        size_t space_needed;

        if (empty_space <= entry_ptr->size)
            cache_ptr->cache_full = true;

        if (cache_ptr->check_write_permitted != NULL) {
            if ((cache_ptr->check_write_permitted)(f, &write_permitted) < 0)
                HGOTO_ERROR(H5E_CACHE, H5E_CANTINS, FAIL, "Can't get write_permitted");
        } /* end if */
        else
            write_permitted = cache_ptr->write_permitted;

        assert(entry_ptr->size <= H5C_MAX_ENTRY_SIZE);
        space_needed = entry_ptr->size;
        if (space_needed > cache_ptr->max_cache_size)
            space_needed = cache_ptr->max_cache_size;

        /* Note that space_needed is just the amount of space that
         * needed to insert the new entry without exceeding the cache
         * size limit.  The subsequent call to H5C__make_space_in_cache()
         * may evict the entries required to free more or less space
         * depending on conditions.  It MAY be less if the cache is
         * currently undersized, or more if the cache is oversized.
         *
         * The cache can exceed its maximum size limit via the following
         * mechanisms:
         *
         * First, it is possible for the cache to grow without
         * bound as long as entries are protected and not unprotected.
         *
         * Second, when writes are not permitted it is also possible
         * for the cache to grow without bound.
         *
         * Finally, we usually don't check to see if the cache is
         * oversized at the end of an unprotect.  As a result, it is
         * possible to have a vastly oversized cache with no protected
         * entries as long as all the protects precede the unprotects.
         */

        if (H5C__make_space_in_cache(f, space_needed, write_permitted) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_CANTINS, FAIL, "H5C__make_space_in_cache failed");
    } /* end if */

    H5C__INSERT_IN_INDEX(cache_ptr, entry_ptr, FAIL);

    /* New entries are presumed to be dirty */
    assert(entry_ptr->is_dirty);
    entry_ptr->flush_marker = set_flush_marker;
    H5C__INSERT_ENTRY_IN_SLIST(cache_ptr, entry_ptr, FAIL);
    H5C__UPDATE_RP_FOR_INSERTION(cache_ptr, entry_ptr, FAIL);

#ifdef H5C_DO_EXTREME_SANITY_CHECKS
    if (H5C__validate_protected_entry_list(cache_ptr) < 0 || H5C__validate_pinned_entry_list(cache_ptr) < 0 ||
        H5C__validate_lru_list(cache_ptr) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "an extreme sanity check failed just before done");
#endif /* H5C_DO_EXTREME_SANITY_CHECKS */

    /* If the entry's type has a 'notify' callback send a 'after insertion'
     * notice now that the entry is fully integrated into the cache.
     */
    if (entry_ptr->type->notify && (entry_ptr->type->notify)(H5C_NOTIFY_ACTION_AFTER_INSERT, entry_ptr) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_CANTNOTIFY, FAIL, "can't notify client about entry inserted into cache");

    H5C__UPDATE_STATS_FOR_INSERTION(cache_ptr, entry_ptr);

#ifdef H5_HAVE_PARALLEL
    if (H5F_HAS_FEATURE(f, H5FD_FEAT_HAS_MPI))
        coll_access = H5F_get_coll_metadata_reads(f);

    entry_ptr->coll_access = coll_access;
    if (coll_access) {
        H5C__INSERT_IN_COLL_LIST(cache_ptr, entry_ptr, FAIL);

        /* Make sure the size of the collective entries in the cache remain in check */
        if (H5P_USER_TRUE == H5F_COLL_MD_READ(f)) {
            if (cache_ptr->max_cache_size * 80 < cache_ptr->coll_list_size * 100) {
                if (H5C_clear_coll_entries(cache_ptr, true) < 0)
                    HGOTO_ERROR(H5E_CACHE, H5E_CANTFLUSH, FAIL, "can't clear collective metadata entries");
            } /* end if */
        }     /* end if */
        else {
            if (cache_ptr->max_cache_size * 40 < cache_ptr->coll_list_size * 100) {
                if (H5C_clear_coll_entries(cache_ptr, true) < 0)
                    HGOTO_ERROR(H5E_CACHE, H5E_CANTFLUSH, FAIL, "can't clear collective metadata entries");
            } /* end if */
        }     /* end else */
    }         /* end if */
#endif

done:
#ifdef H5C_DO_EXTREME_SANITY_CHECKS
    if (H5C__validate_protected_entry_list(cache_ptr) < 0 || H5C__validate_pinned_entry_list(cache_ptr) < 0 ||
        H5C__validate_lru_list(cache_ptr) < 0)
        HDONE_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "an extreme sanity check failed on exit");
#endif /* H5C_DO_EXTREME_SANITY_CHECKS */

    if (ret_value < 0 && entry_tagged)
        if (H5C__untag_entry(cache_ptr, entry_ptr) < 0)
            HDONE_ERROR(H5E_CACHE, H5E_CANTREMOVE, FAIL, "can't remove entry from tag list");

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C_insert_entry() */

/*-------------------------------------------------------------------------
 * Function:    H5C_mark_entry_dirty
 *
 * Purpose:    Mark a pinned or protected entry as dirty.  The target entry
 *         MUST be either pinned or protected, and MAY be both.
 *
 *         In the protected case, this call is the functional
 *         equivalent of setting the H5C__DIRTIED_FLAG on an unprotect
 *         call.
 *
 *         In the pinned but not protected case, if the entry is not
 *         already dirty, the function places function marks the entry
 *         dirty and places it on the skip list.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C_mark_entry_dirty(void *thing)
{
    H5C_t             *cache_ptr;
    H5C_cache_entry_t *entry_ptr = (H5C_cache_entry_t *)thing;
    herr_t             ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity checks */
    assert(entry_ptr);
    assert(H5_addr_defined(entry_ptr->addr));
    cache_ptr = entry_ptr->cache_ptr;
    assert(cache_ptr);

    if (entry_ptr->is_protected) {
        assert(!((entry_ptr)->is_read_only));

        /* set the dirtied flag */
        entry_ptr->dirtied = true;

        /* reset image_up_to_date */
        if (entry_ptr->image_up_to_date) {
            entry_ptr->image_up_to_date = false;

            if (entry_ptr->flush_dep_nparents > 0)
                if (H5C__mark_flush_dep_unserialized(entry_ptr) < 0)
                    HGOTO_ERROR(H5E_CACHE, H5E_CANTNOTIFY, FAIL,
                                "Can't propagate serialization status to fd parents");
        } /* end if */
    }     /* end if */
    else if (entry_ptr->is_pinned) {
        bool was_clean; /* Whether the entry was previously clean */
        bool image_was_up_to_date;

        /* Remember previous dirty status */
        was_clean = !entry_ptr->is_dirty;

        /* Check if image is up to date */
        image_was_up_to_date = entry_ptr->image_up_to_date;

        /* Mark the entry as dirty if it isn't already */
        entry_ptr->is_dirty         = true;
        entry_ptr->image_up_to_date = false;

        /* Modify cache data structures */
        if (was_clean)
            H5C__UPDATE_INDEX_FOR_ENTRY_DIRTY(cache_ptr, entry_ptr, FAIL);
        if (!entry_ptr->in_slist)
            H5C__INSERT_ENTRY_IN_SLIST(cache_ptr, entry_ptr, FAIL);

        /* Update stats for entry being marked dirty */
        H5C__UPDATE_STATS_FOR_DIRTY_PIN(cache_ptr, entry_ptr);

        /* Check for entry changing status and do notifications, etc. */
        if (was_clean) {
            /* If the entry's type has a 'notify' callback send a 'entry dirtied'
             * notice now that the entry is fully integrated into the cache.
             */
            if (entry_ptr->type->notify &&
                (entry_ptr->type->notify)(H5C_NOTIFY_ACTION_ENTRY_DIRTIED, entry_ptr) < 0)
                HGOTO_ERROR(H5E_CACHE, H5E_CANTNOTIFY, FAIL,
                            "can't notify client about entry dirty flag set");

            /* Propagate the dirty flag up the flush dependency chain if appropriate */
            if (entry_ptr->flush_dep_nparents > 0)
                if (H5C__mark_flush_dep_dirty(entry_ptr) < 0)
                    HGOTO_ERROR(H5E_CACHE, H5E_CANTMARKDIRTY, FAIL, "Can't propagate flush dep dirty flag");
        } /* end if */
        if (image_was_up_to_date)
            if (entry_ptr->flush_dep_nparents > 0)
                if (H5C__mark_flush_dep_unserialized(entry_ptr) < 0)
                    HGOTO_ERROR(H5E_CACHE, H5E_CANTNOTIFY, FAIL,
                                "Can't propagate serialization status to fd parents");
    } /* end if */
    else
        HGOTO_ERROR(H5E_CACHE, H5E_CANTMARKDIRTY, FAIL, "Entry is neither pinned nor protected??");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C_mark_entry_dirty() */

/*-------------------------------------------------------------------------
 * Function:    H5C_mark_entry_clean
 *
 * Purpose:    Mark a pinned entry as clean.  The target entry MUST be pinned.
 *
 *         If the entry is not
 *         already clean, the function places function marks the entry
 *         clean and removes it from the skip list.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C_mark_entry_clean(void *_thing)
{
    H5C_t             *cache_ptr;
    H5C_cache_entry_t *entry_ptr = (H5C_cache_entry_t *)_thing;
    herr_t             ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity checks */
    assert(entry_ptr);
    assert(H5_addr_defined(entry_ptr->addr));
    cache_ptr = entry_ptr->cache_ptr;
    assert(cache_ptr);

    /* Operate on pinned entry */
    if (entry_ptr->is_protected)
        HGOTO_ERROR(H5E_CACHE, H5E_CANTMARKCLEAN, FAIL, "entry is protected");
    else if (entry_ptr->is_pinned) {
        bool was_dirty; /* Whether the entry was previously dirty */

        /* Remember previous dirty status */
        was_dirty = entry_ptr->is_dirty;

        /* Mark the entry as clean if it isn't already */
        entry_ptr->is_dirty = false;

        /* Also reset the 'flush_marker' flag, since the entry shouldn't be flushed now */
        entry_ptr->flush_marker = false;

        /* Modify cache data structures */
        if (was_dirty)
            H5C__UPDATE_INDEX_FOR_ENTRY_CLEAN(cache_ptr, entry_ptr, FAIL);
        if (entry_ptr->in_slist)
            H5C__REMOVE_ENTRY_FROM_SLIST(cache_ptr, entry_ptr, false, FAIL);

        /* Update stats for entry being marked clean */
        H5C__UPDATE_STATS_FOR_CLEAR(cache_ptr, entry_ptr);

        /* Check for entry changing status and do notifications, etc. */
        if (was_dirty) {
            /* If the entry's type has a 'notify' callback send a 'entry cleaned'
             * notice now that the entry is fully integrated into the cache.
             */
            if (entry_ptr->type->notify &&
                (entry_ptr->type->notify)(H5C_NOTIFY_ACTION_ENTRY_CLEANED, entry_ptr) < 0)
                HGOTO_ERROR(H5E_CACHE, H5E_CANTNOTIFY, FAIL,
                            "can't notify client about entry dirty flag cleared");

            /* Propagate the clean up the flush dependency chain, if appropriate */
            if (entry_ptr->flush_dep_nparents > 0)
                if (H5C__mark_flush_dep_clean(entry_ptr) < 0)
                    HGOTO_ERROR(H5E_CACHE, H5E_CANTMARKCLEAN, FAIL, "Can't propagate flush dep clean");
        } /* end if */
    }     /* end if */
    else
        HGOTO_ERROR(H5E_CACHE, H5E_CANTMARKCLEAN, FAIL, "Entry is not pinned??");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C_mark_entry_clean() */

/*-------------------------------------------------------------------------
 * Function:    H5C_mark_entry_unserialized
 *
 * Purpose:    Mark a pinned or protected entry as unserialized.  The target
 *             entry MUST be either pinned or protected, and MAY be both.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C_mark_entry_unserialized(void *thing)
{
    H5C_cache_entry_t *entry     = (H5C_cache_entry_t *)thing;
    herr_t             ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity checks */
    assert(entry);
    assert(H5_addr_defined(entry->addr));

    if (entry->is_protected || entry->is_pinned) {
        assert(!entry->is_read_only);

        /* Reset image_up_to_date */
        if (entry->image_up_to_date) {
            entry->image_up_to_date = false;

            if (entry->flush_dep_nparents > 0)
                if (H5C__mark_flush_dep_unserialized(entry) < 0)
                    HGOTO_ERROR(H5E_CACHE, H5E_CANTSET, FAIL,
                                "Can't propagate serialization status to fd parents");
        } /* end if */
    }     /* end if */
    else
        HGOTO_ERROR(H5E_CACHE, H5E_CANTMARKUNSERIALIZED, FAIL,
                    "Entry to unserialize is neither pinned nor protected??");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C_mark_entry_unserialized() */

/*-------------------------------------------------------------------------
 * Function:    H5C_mark_entry_serialized
 *
 * Purpose:    Mark a pinned entry as serialized.  The target entry MUST be
 *             pinned.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C_mark_entry_serialized(void *_thing)
{
    H5C_cache_entry_t *entry     = (H5C_cache_entry_t *)_thing;
    herr_t             ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity checks */
    assert(entry);
    assert(H5_addr_defined(entry->addr));

    /* Operate on pinned entry */
    if (entry->is_protected)
        HGOTO_ERROR(H5E_CACHE, H5E_CANTMARKSERIALIZED, FAIL, "entry is protected");
    else if (entry->is_pinned) {
        /* Check for entry changing status and do notifications, etc. */
        if (!entry->image_up_to_date) {
            /* Set the image_up_to_date flag */
            entry->image_up_to_date = true;

            /* Propagate the serialize up the flush dependency chain, if appropriate */
            if (entry->flush_dep_nparents > 0)
                if (H5C__mark_flush_dep_serialized(entry) < 0)
                    HGOTO_ERROR(H5E_CACHE, H5E_CANTMARKSERIALIZED, FAIL,
                                "Can't propagate flush dep serialize");
        } /* end if */
    }     /* end if */
    else
        HGOTO_ERROR(H5E_CACHE, H5E_CANTMARKSERIALIZED, FAIL, "Entry is not pinned??");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C_mark_entry_serialized() */

/*-------------------------------------------------------------------------
 * Function:    H5C_move_entry
 *
 * Purpose:     Use this function to notify the cache that an entry's
 *              file address changed.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C_move_entry(H5C_t *cache_ptr, const H5C_class_t *type, haddr_t old_addr, haddr_t new_addr)
{
    H5C_cache_entry_t *entry_ptr      = NULL;
    H5C_cache_entry_t *test_entry_ptr = NULL;
    herr_t             ret_value      = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    assert(cache_ptr);
    assert(type);
    assert(H5_addr_defined(old_addr));
    assert(H5_addr_defined(new_addr));
    assert(H5_addr_ne(old_addr, new_addr));

#ifdef H5C_DO_EXTREME_SANITY_CHECKS
    if (H5C__validate_protected_entry_list(cache_ptr) < 0 || H5C__validate_pinned_entry_list(cache_ptr) < 0 ||
        H5C__validate_lru_list(cache_ptr) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "an extreme sanity check failed on entry");
#endif /* H5C_DO_EXTREME_SANITY_CHECKS */

    H5C__SEARCH_INDEX(cache_ptr, old_addr, entry_ptr, FAIL);

    if (entry_ptr == NULL || entry_ptr->type != type)
        /* the old item doesn't exist in the cache, so we are done. */
        HGOTO_DONE(SUCCEED);

    assert(entry_ptr->addr == old_addr);
    assert(entry_ptr->type == type);

    /* Check for R/W status, otherwise error */
    /* (Moving a R/O entry would mark it dirty, which shouldn't
     *  happen. QAK - 2016/12/02)
     */
    if (entry_ptr->is_read_only)
        HGOTO_ERROR(H5E_CACHE, H5E_CANTMOVE, FAIL, "can't move R/O entry");

    H5C__SEARCH_INDEX(cache_ptr, new_addr, test_entry_ptr, FAIL);

    if (test_entry_ptr != NULL) { /* we are hosed */
        if (test_entry_ptr->type == type)
            HGOTO_ERROR(H5E_CACHE, H5E_CANTMOVE, FAIL, "target already moved & reinserted???");
        else
            HGOTO_ERROR(H5E_CACHE, H5E_CANTMOVE, FAIL, "new address already in use?");
    } /* end if */

    /* If we get this far we have work to do.  Remove *entry_ptr from
     * the hash table (and skip list if necessary), change its address to the
     * new address, mark it as dirty (if it isn't already) and then re-insert.
     *
     * Update the replacement policy for a hit to avoid an eviction before
     * the moved entry is touched.  Update stats for a move.
     *
     * Note that we do not check the size of the cache, or evict anything.
     * Since this is a simple re-name, cache size should be unaffected.
     *
     * Check to see if the target entry is in the process of being destroyed
     * before we delete from the index, etc.  If it is, all we do is
     * change the addr.  If the entry is only in the process of being flushed,
     * don't mark it as dirty either, lest we confuse the flush call back.
     */
    if (!entry_ptr->destroy_in_progress) {
        H5C__DELETE_FROM_INDEX(cache_ptr, entry_ptr, FAIL);

        if (entry_ptr->in_slist) {
            assert(cache_ptr->slist_ptr);
            H5C__REMOVE_ENTRY_FROM_SLIST(cache_ptr, entry_ptr, false, FAIL);
        } /* end if */
    }     /* end if */

    entry_ptr->addr = new_addr;

    if (!entry_ptr->destroy_in_progress) {
        bool was_dirty; /* Whether the entry was previously dirty */

        /* Remember previous dirty status */
        was_dirty = entry_ptr->is_dirty;

        /* Mark the entry as dirty if it isn't already */
        entry_ptr->is_dirty = true;

        /* This shouldn't be needed, but it keeps the test code happy */
        if (entry_ptr->image_up_to_date) {
            entry_ptr->image_up_to_date = false;
            if (entry_ptr->flush_dep_nparents > 0)
                if (H5C__mark_flush_dep_unserialized(entry_ptr) < 0)
                    HGOTO_ERROR(H5E_CACHE, H5E_CANTNOTIFY, FAIL,
                                "Can't propagate serialization status to fd parents");
        } /* end if */

        /* Modify cache data structures */
        H5C__INSERT_IN_INDEX(cache_ptr, entry_ptr, FAIL);
        H5C__INSERT_ENTRY_IN_SLIST(cache_ptr, entry_ptr, FAIL);

        /* Skip some actions if we're in the middle of flushing the entry */
        if (!entry_ptr->flush_in_progress) {
            /* Update the replacement policy for the entry */
            H5C__UPDATE_RP_FOR_MOVE(cache_ptr, entry_ptr, was_dirty, FAIL);

            /* Check for entry changing status and do notifications, etc. */
            if (!was_dirty) {
                /* If the entry's type has a 'notify' callback send a 'entry dirtied'
                 * notice now that the entry is fully integrated into the cache.
                 */
                if (entry_ptr->type->notify &&
                    (entry_ptr->type->notify)(H5C_NOTIFY_ACTION_ENTRY_DIRTIED, entry_ptr) < 0)
                    HGOTO_ERROR(H5E_CACHE, H5E_CANTNOTIFY, FAIL,
                                "can't notify client about entry dirty flag set");

                /* Propagate the dirty flag up the flush dependency chain if appropriate */
                if (entry_ptr->flush_dep_nparents > 0)
                    if (H5C__mark_flush_dep_dirty(entry_ptr) < 0)
                        HGOTO_ERROR(H5E_CACHE, H5E_CANTMARKDIRTY, FAIL,
                                    "Can't propagate flush dep dirty flag");
            } /* end if */
        }     /* end if */
    }         /* end if */

    H5C__UPDATE_STATS_FOR_MOVE(cache_ptr, entry_ptr);

done:
#ifdef H5C_DO_EXTREME_SANITY_CHECKS
    if (H5C__validate_protected_entry_list(cache_ptr) < 0 || H5C__validate_pinned_entry_list(cache_ptr) < 0 ||
        H5C__validate_lru_list(cache_ptr) < 0)
        HDONE_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "an extreme sanity check failed on exit");
#endif /* H5C_DO_EXTREME_SANITY_CHECKS */

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C_move_entry() */

/*-------------------------------------------------------------------------
 * Function:    H5C_resize_entry
 *
 * Purpose:    Resize a pinned or protected entry.
 *
 *         Resizing an entry dirties it, so if the entry is not
 *         already dirty, the function places the entry on the
 *         skip list.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C_resize_entry(void *thing, size_t new_size)
{
    H5C_t             *cache_ptr;
    H5C_cache_entry_t *entry_ptr = (H5C_cache_entry_t *)thing;
    herr_t             ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity checks */
    assert(entry_ptr);
    assert(H5_addr_defined(entry_ptr->addr));
    cache_ptr = entry_ptr->cache_ptr;
    assert(cache_ptr);

    /* Check for usage errors */
    if (new_size <= 0)
        HGOTO_ERROR(H5E_CACHE, H5E_BADVALUE, FAIL, "New size is non-positive");
    if (!(entry_ptr->is_pinned || entry_ptr->is_protected))
        HGOTO_ERROR(H5E_CACHE, H5E_BADTYPE, FAIL, "Entry isn't pinned or protected??");

#ifdef H5C_DO_EXTREME_SANITY_CHECKS
    if (H5C__validate_protected_entry_list(cache_ptr) < 0 || H5C__validate_pinned_entry_list(cache_ptr) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "an extreme sanity check failed on entry");
#endif /* H5C_DO_EXTREME_SANITY_CHECKS */

    /* update for change in entry size if necessary */
    if (entry_ptr->size != new_size) {
        bool was_clean;

        /* make note of whether the entry was clean to begin with */
        was_clean = !entry_ptr->is_dirty;

        /* mark the entry as dirty if it isn't already */
        entry_ptr->is_dirty = true;

        /* Reset the image up-to-date status */
        if (entry_ptr->image_up_to_date) {
            entry_ptr->image_up_to_date = false;
            if (entry_ptr->flush_dep_nparents > 0)
                if (H5C__mark_flush_dep_unserialized(entry_ptr) < 0)
                    HGOTO_ERROR(H5E_CACHE, H5E_CANTNOTIFY, FAIL,
                                "Can't propagate serialization status to fd parents");
        } /* end if */

        /* Release the current image */
        if (entry_ptr->image_ptr)
            entry_ptr->image_ptr = H5MM_xfree(entry_ptr->image_ptr);

        /* do a flash cache size increase if appropriate */
        if (cache_ptr->flash_size_increase_possible) {
            if (new_size > entry_ptr->size) {
                size_t size_increase;

                size_increase = new_size - entry_ptr->size;
                if (size_increase >= cache_ptr->flash_size_increase_threshold)
                    if (H5C__flash_increase_cache_size(cache_ptr, entry_ptr->size, new_size) < 0)
                        HGOTO_ERROR(H5E_CACHE, H5E_CANTRESIZE, FAIL, "flash cache increase failed");
            }
        }

        /* update the pinned and/or protected entry list */
        if (entry_ptr->is_pinned)
            H5C__DLL_UPDATE_FOR_SIZE_CHANGE(cache_ptr->pel_len, cache_ptr->pel_size, entry_ptr->size,
                                            new_size, FAIL)
        if (entry_ptr->is_protected)
            H5C__DLL_UPDATE_FOR_SIZE_CHANGE(cache_ptr->pl_len, cache_ptr->pl_size, entry_ptr->size, new_size,
                                            FAIL)

#ifdef H5_HAVE_PARALLEL
        if (entry_ptr->coll_access)
            H5C__DLL_UPDATE_FOR_SIZE_CHANGE(cache_ptr->coll_list_len, cache_ptr->coll_list_size,
                                            entry_ptr->size, new_size, FAIL)
#endif /* H5_HAVE_PARALLEL */

        /* update statistics just before changing the entry size */
        H5C__UPDATE_STATS_FOR_ENTRY_SIZE_CHANGE(cache_ptr, entry_ptr, new_size);

        /* update the hash table */
        H5C__UPDATE_INDEX_FOR_SIZE_CHANGE(cache_ptr, entry_ptr->size, new_size, entry_ptr, was_clean, FAIL);

        /* if the entry is in the skip list, update that too */
        if (entry_ptr->in_slist)
            H5C__UPDATE_SLIST_FOR_SIZE_CHANGE(cache_ptr, entry_ptr->size, new_size);

        /* finally, update the entry size proper */
        entry_ptr->size = new_size;

        if (!entry_ptr->in_slist)
            H5C__INSERT_ENTRY_IN_SLIST(cache_ptr, entry_ptr, FAIL);

        if (entry_ptr->is_pinned)
            H5C__UPDATE_STATS_FOR_DIRTY_PIN(cache_ptr, entry_ptr);

        /* Check for entry changing status and do notifications, etc. */
        if (was_clean) {
            /* If the entry's type has a 'notify' callback send a 'entry dirtied'
             * notice now that the entry is fully integrated into the cache.
             */
            if (entry_ptr->type->notify &&
                (entry_ptr->type->notify)(H5C_NOTIFY_ACTION_ENTRY_DIRTIED, entry_ptr) < 0)
                HGOTO_ERROR(H5E_CACHE, H5E_CANTNOTIFY, FAIL,
                            "can't notify client about entry dirty flag set");

            /* Propagate the dirty flag up the flush dependency chain if appropriate */
            if (entry_ptr->flush_dep_nparents > 0)
                if (H5C__mark_flush_dep_dirty(entry_ptr) < 0)
                    HGOTO_ERROR(H5E_CACHE, H5E_CANTMARKDIRTY, FAIL, "Can't propagate flush dep dirty flag");
        } /* end if */
    }     /* end if */

done:
#ifdef H5C_DO_EXTREME_SANITY_CHECKS
    if (H5C__validate_protected_entry_list(cache_ptr) < 0 || H5C__validate_pinned_entry_list(cache_ptr) < 0)
        HDONE_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "an extreme sanity check failed on exit");
#endif /* H5C_DO_EXTREME_SANITY_CHECKS */

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C_resize_entry() */

/*-------------------------------------------------------------------------
 * Function:    H5C_pin_protected_entry()
 *
 * Purpose:    Pin a protected cache entry.  The entry must be protected
 *             at the time of call, and must be unpinned.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C_pin_protected_entry(void *thing)
{
    H5C_t             *cache_ptr;
    H5C_cache_entry_t *entry_ptr = (H5C_cache_entry_t *)thing; /* Pointer to entry to pin */
    herr_t             ret_value = SUCCEED;                    /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity checks */
    assert(entry_ptr);
    assert(H5_addr_defined(entry_ptr->addr));
    cache_ptr = entry_ptr->cache_ptr;
    assert(cache_ptr);

#ifdef H5C_DO_EXTREME_SANITY_CHECKS
    if (H5C__validate_protected_entry_list(cache_ptr) < 0 || H5C__validate_pinned_entry_list(cache_ptr) < 0 ||
        H5C__validate_lru_list(cache_ptr) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "an extreme sanity check failed on entry");
#endif /* H5C_DO_EXTREME_SANITY_CHECKS */

    /* Only protected entries can be pinned */
    if (!entry_ptr->is_protected)
        HGOTO_ERROR(H5E_CACHE, H5E_CANTPIN, FAIL, "Entry isn't protected");

    /* Pin the entry from a client */
    if (H5C__pin_entry_from_client(cache_ptr, entry_ptr) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_CANTPIN, FAIL, "Can't pin entry by client");

done:
#ifdef H5C_DO_EXTREME_SANITY_CHECKS
    if (H5C__validate_protected_entry_list(cache_ptr) < 0 || H5C__validate_pinned_entry_list(cache_ptr) < 0 ||
        H5C__validate_lru_list(cache_ptr) < 0)
        HDONE_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "an extreme sanity check failed on exit");
#endif /* H5C_DO_EXTREME_SANITY_CHECKS */

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C_pin_protected_entry() */

/*-------------------------------------------------------------------------
 * Function:    H5C_protect
 *
 * Purpose:     If the target entry is not in the cache, load it.  If
 *        necessary, attempt to evict one or more entries to keep
 *        the cache within its maximum size.
 *
 *        Mark the target entry as protected, and return its address
 *        to the caller.  The caller must call H5C_unprotect() when
 *        finished with the entry.
 *
 *        While it is protected, the entry may not be either evicted
 *        or flushed -- nor may it be accessed by another call to
 *        H5C_protect.  Any attempt to do so will result in a failure.
 *
 * Return:      Success:        Ptr to the desired entry
 *              Failure:        NULL
 *
 *-------------------------------------------------------------------------
 */
void *
H5C_protect(H5F_t *f, const H5C_class_t *type, haddr_t addr, void *udata, unsigned flags)
{
    H5C_t      *cache_ptr;
    H5AC_ring_t ring = H5C_RING_UNDEFINED;
    bool        hit;
    bool        have_write_permitted = false;
    bool        read_only            = false;
    bool        flush_last;
#ifdef H5_HAVE_PARALLEL
    bool coll_access = false; /* whether access to the cache entry is done collectively */
#endif                        /* H5_HAVE_PARALLEL */
    bool               write_permitted = false;
    bool               was_loaded      = false; /* Whether the entry was loaded as a result of the protect */
    size_t             empty_space;
    void              *thing;
    H5C_cache_entry_t *entry_ptr;
    void              *ret_value = NULL; /* Return value */

    FUNC_ENTER_NOAPI(NULL)

    /* check args */
    assert(f);
    assert(f->shared);
    cache_ptr = f->shared->cache;
    assert(cache_ptr);
    assert(type);
    assert(type->mem_type == cache_ptr->class_table_ptr[type->id]->mem_type);
    assert(H5_addr_defined(addr));

#ifdef H5C_DO_EXTREME_SANITY_CHECKS
    if (H5C__validate_protected_entry_list(cache_ptr) < 0 || H5C__validate_pinned_entry_list(cache_ptr) < 0 ||
        H5C__validate_lru_list(cache_ptr) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, NULL, "an extreme sanity check failed on entry");
#endif /* H5C_DO_EXTREME_SANITY_CHECKS */

    /* Load the cache image, if requested */
    if (cache_ptr->load_image) {
        cache_ptr->load_image = false;
        if (H5C__load_cache_image(f) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_CANTLOAD, NULL, "Can't load cache image");
    } /* end if */

    read_only  = ((flags & H5C__READ_ONLY_FLAG) != 0);
    flush_last = ((flags & H5C__FLUSH_LAST_FLAG) != 0);

    /* Get the ring type from the API context */
    ring = H5CX_get_ring();

#ifdef H5_HAVE_PARALLEL
    if (H5F_HAS_FEATURE(f, H5FD_FEAT_HAS_MPI))
        coll_access = H5F_get_coll_metadata_reads(f);
#endif /* H5_HAVE_PARALLEL */

    /* first check to see if the target is in cache */
    H5C__SEARCH_INDEX(cache_ptr, addr, entry_ptr, NULL);

    if (entry_ptr != NULL) {
        if (entry_ptr->ring != ring)
            HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, NULL, "ring type mismatch occurred for cache entry");

        if (entry_ptr->prefetched) {
            /* This call removes the prefetched entry from the cache,
             * and replaces it with an entry deserialized from the
             * image of the prefetched entry.
             */
            if (H5C__deserialize_prefetched_entry(f, cache_ptr, &entry_ptr, type, addr, udata) < 0)
                HGOTO_ERROR(H5E_CACHE, H5E_CANTLOAD, NULL, "can't deserialize prefetched entry");

            assert(!entry_ptr->prefetched);
            assert(entry_ptr->addr == addr);
        } /* end if */

        /* Check for trying to load the wrong type of entry from an address */
        if (entry_ptr->type != type)
            HGOTO_ERROR(H5E_CACHE, H5E_BADTYPE, NULL, "incorrect cache entry type");

#ifdef H5_HAVE_PARALLEL
        /* If this is a collective metadata read, the entry is not marked as
         * collective, and is clean, it is possible that other processes will
         * not have it in its cache and will expect a bcast of the entry from
         * process 0. So process 0 will bcast the entry to all other ranks.
         * Ranks that _do_ have the entry in their cache still have to
         * participate in the bcast.
         */
        if (coll_access) {
            if (!entry_ptr->is_dirty && !entry_ptr->coll_access) {
                MPI_Comm comm;     /* File MPI Communicator */
                int      mpi_code; /* MPI error code */
                int      buf_size;

                if (MPI_COMM_NULL == (comm = H5F_mpi_get_comm(f)))
                    HGOTO_ERROR(H5E_FILE, H5E_CANTGET, NULL, "get_comm request failed");

                if (entry_ptr->image_ptr == NULL) {
                    int mpi_rank;

                    if ((mpi_rank = H5F_mpi_get_rank(f)) < 0)
                        HGOTO_ERROR(H5E_FILE, H5E_CANTGET, NULL, "Can't get MPI rank");

                    if (NULL == (entry_ptr->image_ptr = H5MM_malloc(entry_ptr->size + H5C_IMAGE_EXTRA_SPACE)))
                        HGOTO_ERROR(H5E_CACHE, H5E_CANTALLOC, NULL,
                                    "memory allocation failed for on disk image buffer");
#if H5C_DO_MEMORY_SANITY_CHECKS
                    H5MM_memcpy(((uint8_t *)entry_ptr->image_ptr) + entry_ptr->size, H5C_IMAGE_SANITY_VALUE,
                                H5C_IMAGE_EXTRA_SPACE);
#endif /* H5C_DO_MEMORY_SANITY_CHECKS */
                    if (0 == mpi_rank && H5C__generate_image(f, cache_ptr, entry_ptr) < 0)
                        /* If image generation fails, push an error but
                         * still participate in the following MPI_Bcast
                         */
                        HDONE_ERROR(H5E_CACHE, H5E_CANTGET, NULL, "can't generate entry's image");
                } /* end if */
                assert(entry_ptr->image_ptr);

                H5_CHECKED_ASSIGN(buf_size, int, entry_ptr->size, size_t);
                if (MPI_SUCCESS != (mpi_code = MPI_Bcast(entry_ptr->image_ptr, buf_size, MPI_BYTE, 0, comm)))
                    HMPI_GOTO_ERROR(NULL, "MPI_Bcast failed", mpi_code)

                /* Mark the entry as collective and insert into the collective list */
                entry_ptr->coll_access = true;
                H5C__INSERT_IN_COLL_LIST(cache_ptr, entry_ptr, NULL);
            } /* end if */
            else if (entry_ptr->coll_access)
                H5C__MOVE_TO_TOP_IN_COLL_LIST(cache_ptr, entry_ptr, NULL);
        } /* end if */
#endif    /* H5_HAVE_PARALLEL */

#ifdef H5C_DO_TAGGING_SANITY_CHECKS
        {
            /* Verify tag value */
            if (cache_ptr->ignore_tags != true) {
                haddr_t tag; /* Tag value */

                /* The entry is already in the cache, but make sure that the tag value
                 * is still legal. This will ensure that had the entry NOT been in the
                 * cache, tagging was still set up correctly and it would have received
                 * a legal tag value after getting loaded from disk.
                 */

                /* Get the tag */
                tag = H5CX_get_tag();

                if (H5C_verify_tag(entry_ptr->type->id, tag) < 0)
                    HGOTO_ERROR(H5E_CACHE, H5E_CANTGET, NULL, "tag verification failed");
            } /* end if */
        }
#endif

        hit   = true;
        thing = (void *)entry_ptr;
    }
    else {
        /* must try to load the entry from disk. */
        hit = false;
        if (NULL == (thing = H5C__load_entry(f,
#ifdef H5_HAVE_PARALLEL
                                             coll_access,
#endif /* H5_HAVE_PARALLEL */
                                             type, addr, udata)))
            HGOTO_ERROR(H5E_CACHE, H5E_CANTLOAD, NULL, "can't load entry");

        entry_ptr = (H5C_cache_entry_t *)thing;
        cache_ptr->entries_loaded_counter++;

        entry_ptr->ring = ring;
#ifdef H5_HAVE_PARALLEL
        if (H5F_HAS_FEATURE(f, H5FD_FEAT_HAS_MPI) && entry_ptr->coll_access)
            H5C__INSERT_IN_COLL_LIST(cache_ptr, entry_ptr, NULL);
#endif /* H5_HAVE_PARALLEL */

        /* Apply tag to newly protected entry */
        if (H5C__tag_entry(cache_ptr, entry_ptr) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_CANTTAG, NULL, "Cannot tag metadata entry");

        /* If the entry is very large, and we are configured to allow it,
         * we may wish to perform a flash cache size increase.
         */
        if (cache_ptr->flash_size_increase_possible &&
            (entry_ptr->size > cache_ptr->flash_size_increase_threshold))
            if (H5C__flash_increase_cache_size(cache_ptr, 0, entry_ptr->size) < 0)
                HGOTO_ERROR(H5E_CACHE, H5E_CANTPROTECT, NULL, "H5C__flash_increase_cache_size failed");

        if (cache_ptr->index_size >= cache_ptr->max_cache_size)
            empty_space = 0;
        else
            empty_space = cache_ptr->max_cache_size - cache_ptr->index_size;

        /* try to free up if necceary and if evictions are permitted.  Note
         * that if evictions are enabled, we will call H5C__make_space_in_cache()
         * regardless if the min_free_space requirement is not met.
         */
        if (cache_ptr->evictions_enabled &&
            (((cache_ptr->index_size + entry_ptr->size) > cache_ptr->max_cache_size) ||
             ((empty_space + cache_ptr->clean_index_size) < cache_ptr->min_clean_size))) {

            size_t space_needed;

            if (empty_space <= entry_ptr->size)
                cache_ptr->cache_full = true;

            if (cache_ptr->check_write_permitted != NULL) {
                if ((cache_ptr->check_write_permitted)(f, &write_permitted) < 0)
                    HGOTO_ERROR(H5E_CACHE, H5E_CANTPROTECT, NULL, "Can't get write_permitted 1");
                else
                    have_write_permitted = true;
            } /* end if */
            else {
                write_permitted      = cache_ptr->write_permitted;
                have_write_permitted = true;
            } /* end else */

            assert(entry_ptr->size <= H5C_MAX_ENTRY_SIZE);
            space_needed = entry_ptr->size;
            if (space_needed > cache_ptr->max_cache_size)
                space_needed = cache_ptr->max_cache_size;

            /* Note that space_needed is just the amount of space that
             * needed to insert the new entry without exceeding the cache
             * size limit.  The subsequent call to H5C__make_space_in_cache()
             * may evict the entries required to free more or less space
             * depending on conditions.  It MAY be less if the cache is
             * currently undersized, or more if the cache is oversized.
             *
             * The cache can exceed its maximum size limit via the following
             * mechanisms:
             *
             * First, it is possible for the cache to grow without
             * bound as long as entries are protected and not unprotected.
             *
             * Second, when writes are not permitted it is also possible
             * for the cache to grow without bound.
             *
             * Third, the user may choose to disable evictions -- causing
             * the cache to grow without bound until evictions are
             * re-enabled.
             *
             * Finally, we usually don't check to see if the cache is
             * oversized at the end of an unprotect.  As a result, it is
             * possible to have a vastly oversized cache with no protected
             * entries as long as all the protects precede the unprotects.
             */
            if (H5C__make_space_in_cache(f, space_needed, write_permitted) < 0)
                HGOTO_ERROR(H5E_CACHE, H5E_CANTPROTECT, NULL, "H5C__make_space_in_cache failed");
        } /* end if */

        /* Insert the entry in the hash table.
         *
         *   *******************************************
         *
         * Set the flush_me_last field
         * of the newly loaded entry before inserting it into the
         * index.  Must do this, as the index tracked the number of
         * entries with the flush_last field set, but assumes that
         * the field will not change after insertion into the index.
         *
         * Note that this means that the H5C__FLUSH_LAST_FLAG flag
         * is ignored if the entry is already in cache.
         */
        entry_ptr->flush_me_last = flush_last;

        H5C__INSERT_IN_INDEX(cache_ptr, entry_ptr, NULL);
        if (entry_ptr->is_dirty && !entry_ptr->in_slist)
            H5C__INSERT_ENTRY_IN_SLIST(cache_ptr, entry_ptr, NULL);

        /* insert the entry in the data structures used by the replacement
         * policy.  We are just going to take it out again when we update
         * the replacement policy for a protect, but this simplifies the
         * code.  If we do this often enough, we may want to optimize this.
         */
        H5C__UPDATE_RP_FOR_INSERTION(cache_ptr, entry_ptr, NULL);

        /* Record that the entry was loaded, to trigger a notify callback later */
        /* (After the entry is fully added to the cache) */
        was_loaded = true;
    } /* end else */

    assert(entry_ptr->addr == addr);
    assert(entry_ptr->type == type);

    if (entry_ptr->is_protected) {
        if (read_only && entry_ptr->is_read_only) {
            assert(entry_ptr->ro_ref_count > 0);
            (entry_ptr->ro_ref_count)++;
        } /* end if */
        else
            HGOTO_ERROR(H5E_CACHE, H5E_CANTPROTECT, NULL, "Target already protected & not read only?!?");
    } /* end if */
    else {
        H5C__UPDATE_RP_FOR_PROTECT(cache_ptr, entry_ptr, NULL);

        entry_ptr->is_protected = true;
        if (read_only) {
            entry_ptr->is_read_only = true;
            entry_ptr->ro_ref_count = 1;
        } /* end if */
        entry_ptr->dirtied = false;
    } /* end else */

    H5C__UPDATE_CACHE_HIT_RATE_STATS(cache_ptr, hit);
    H5C__UPDATE_STATS_FOR_PROTECT(cache_ptr, entry_ptr, hit);

    ret_value = thing;

    if (cache_ptr->evictions_enabled &&
        (cache_ptr->size_decreased ||
         (cache_ptr->resize_enabled && (cache_ptr->cache_accesses >= cache_ptr->resize_ctl.epoch_length)))) {

        if (!have_write_permitted) {
            if (cache_ptr->check_write_permitted != NULL) {
                if ((cache_ptr->check_write_permitted)(f, &write_permitted) < 0)
                    HGOTO_ERROR(H5E_CACHE, H5E_CANTPROTECT, NULL, "Can't get write_permitted");
                else
                    have_write_permitted = true;
            }
            else {
                write_permitted      = cache_ptr->write_permitted;
                have_write_permitted = true;
            }
        }

        if (cache_ptr->resize_enabled && (cache_ptr->cache_accesses >= cache_ptr->resize_ctl.epoch_length))
            if (H5C__auto_adjust_cache_size(f, write_permitted) < 0)
                HGOTO_ERROR(H5E_CACHE, H5E_CANTPROTECT, NULL, "Cache auto-resize failed");

        if (cache_ptr->size_decreased) {
            cache_ptr->size_decreased = false;

            /* check to see if the cache is now oversized due to the cache
             * size reduction.  If it is, try to evict enough entries to
             * bring the cache size down to the current maximum cache size.
             *
             * Also, if the min_clean_size requirement is not met, we
             * should also call H5C__make_space_in_cache() to bring us
             * into compliance.
             */
            if (cache_ptr->index_size >= cache_ptr->max_cache_size)
                empty_space = 0;
            else
                empty_space = cache_ptr->max_cache_size - cache_ptr->index_size;

            if ((cache_ptr->index_size > cache_ptr->max_cache_size) ||
                ((empty_space + cache_ptr->clean_index_size) < cache_ptr->min_clean_size)) {

                if (cache_ptr->index_size > cache_ptr->max_cache_size)
                    cache_ptr->cache_full = true;

                if (H5C__make_space_in_cache(f, (size_t)0, write_permitted) < 0)
                    HGOTO_ERROR(H5E_CACHE, H5E_CANTPROTECT, NULL, "H5C__make_space_in_cache failed");
            }
        } /* end if */
    }

    /* If we loaded the entry and the entry's type has a 'notify' callback, send
     * an 'after load' notice now that the entry is fully integrated into
     * the cache and protected.  We must wait until it is protected so it is not
     * evicted during the notify callback.
     */
    if (was_loaded)
        /* If the entry's type has a 'notify' callback send a 'after load'
         * notice now that the entry is fully integrated into the cache.
         */
        if (entry_ptr->type->notify && (entry_ptr->type->notify)(H5C_NOTIFY_ACTION_AFTER_LOAD, entry_ptr) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_CANTNOTIFY, NULL,
                        "can't notify client about entry inserted into cache");

#ifdef H5_HAVE_PARALLEL
    /* Make sure the size of the collective entries in the cache remain in check */
    if (coll_access) {
        if (H5P_USER_TRUE == H5F_COLL_MD_READ(f)) {
            if (cache_ptr->max_cache_size * 80 < cache_ptr->coll_list_size * 100)
                if (H5C_clear_coll_entries(cache_ptr, true) < 0)
                    HGOTO_ERROR(H5E_CACHE, H5E_CANTFLUSH, NULL, "can't clear collective metadata entries");
        } /* end if */
        else {
            if (cache_ptr->max_cache_size * 40 < cache_ptr->coll_list_size * 100)
                if (H5C_clear_coll_entries(cache_ptr, true) < 0)
                    HGOTO_ERROR(H5E_CACHE, H5E_CANTFLUSH, NULL, "can't clear collective metadata entries");
        } /* end else */
    }     /* end if */
#endif    /* H5_HAVE_PARALLEL */

done:
#ifdef H5C_DO_EXTREME_SANITY_CHECKS
    if (H5C__validate_protected_entry_list(cache_ptr) < 0 || H5C__validate_pinned_entry_list(cache_ptr) < 0 ||
        H5C__validate_lru_list(cache_ptr) < 0)
        HDONE_ERROR(H5E_CACHE, H5E_SYSTEM, NULL, "an extreme sanity check failed on exit");
#endif /* H5C_DO_EXTREME_SANITY_CHECKS */

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C_protect() */

/*-------------------------------------------------------------------------
 * Function:    H5C_unpin_entry()
 *
 * Purpose:    Unpin a cache entry.  The entry can be either protected or
 *             unprotected at the time of call, but must be pinned.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C_unpin_entry(void *_entry_ptr)
{
    H5C_t             *cache_ptr;
    H5C_cache_entry_t *entry_ptr = (H5C_cache_entry_t *)_entry_ptr; /* Pointer to entry to unpin */
    herr_t             ret_value = SUCCEED;                         /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity check */
    assert(entry_ptr);
    cache_ptr = entry_ptr->cache_ptr;
    assert(cache_ptr);

#ifdef H5C_DO_EXTREME_SANITY_CHECKS
    if (H5C__validate_protected_entry_list(cache_ptr) < 0 || H5C__validate_pinned_entry_list(cache_ptr) < 0 ||
        H5C__validate_lru_list(cache_ptr) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "an extreme sanity check failed on entry");
#endif /* H5C_DO_EXTREME_SANITY_CHECKS */

    /* Unpin the entry */
    if (H5C__unpin_entry_from_client(cache_ptr, entry_ptr, true) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_CANTUNPIN, FAIL, "Can't unpin entry from client");

done:
#ifdef H5C_DO_EXTREME_SANITY_CHECKS
    if (H5C__validate_protected_entry_list(cache_ptr) < 0 || H5C__validate_pinned_entry_list(cache_ptr) < 0 ||
        H5C__validate_lru_list(cache_ptr) < 0)
        HDONE_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "an extreme sanity check failed on exit");
#endif /* H5C_DO_EXTREME_SANITY_CHECKS */

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C_unpin_entry() */

/*-------------------------------------------------------------------------
 * Function:    H5C_unprotect
 *
 * Purpose:    Undo an H5C_protect() call -- specifically, mark the
 *        entry as unprotected, remove it from the protected list,
 *        and give it back to the replacement policy.
 *
 *        The TYPE and ADDR arguments must be the same as those in
 *        the corresponding call to H5C_protect() and the THING
 *        argument must be the value returned by that call to
 *        H5C_protect().
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *        If the deleted flag is true, simply remove the target entry
 *        from the cache, clear it, and free it without writing it to
 *        disk.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C_unprotect(H5F_t *f, haddr_t addr, void *thing, unsigned flags)
{
    H5C_t *cache_ptr;
    bool   deleted;
    bool   dirtied;
    bool   set_flush_marker;
    bool   pin_entry;
    bool   unpin_entry;
    bool   free_file_space;
    bool   take_ownership;
    bool   was_clean;
#ifdef H5_HAVE_PARALLEL
    bool clear_entry = false;
#endif /* H5_HAVE_PARALLEL */
    H5C_cache_entry_t *entry_ptr;
    H5C_cache_entry_t *test_entry_ptr;
    herr_t             ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    deleted          = ((flags & H5C__DELETED_FLAG) != 0);
    dirtied          = ((flags & H5C__DIRTIED_FLAG) != 0);
    set_flush_marker = ((flags & H5C__SET_FLUSH_MARKER_FLAG) != 0);
    pin_entry        = ((flags & H5C__PIN_ENTRY_FLAG) != 0);
    unpin_entry      = ((flags & H5C__UNPIN_ENTRY_FLAG) != 0);
    free_file_space  = ((flags & H5C__FREE_FILE_SPACE_FLAG) != 0);
    take_ownership   = ((flags & H5C__TAKE_OWNERSHIP_FLAG) != 0);

    assert(f);
    assert(f->shared);

    cache_ptr = f->shared->cache;

    assert(cache_ptr);
    assert(H5_addr_defined(addr));
    assert(thing);
    assert(!(pin_entry && unpin_entry));

    /* deleted flag must accompany free_file_space */
    assert((!free_file_space) || (deleted));

    /* deleted flag must accompany take_ownership */
    assert((!take_ownership) || (deleted));

    /* can't have both free_file_space & take_ownership */
    assert(!(free_file_space && take_ownership));

    entry_ptr = (H5C_cache_entry_t *)thing;
    assert(entry_ptr->addr == addr);

    /* also set the dirtied variable if the dirtied field is set in
     * the entry.
     */
    dirtied |= entry_ptr->dirtied;
    was_clean = !(entry_ptr->is_dirty);

#ifdef H5C_DO_EXTREME_SANITY_CHECKS
    if (H5C__validate_protected_entry_list(cache_ptr) < 0 || H5C__validate_pinned_entry_list(cache_ptr) < 0 ||
        H5C__validate_lru_list(cache_ptr) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "an extreme sanity check failed on entry");
#endif /* H5C_DO_EXTREME_SANITY_CHECKS */

    /* if the entry has multiple read only protects, just decrement
     * the ro_ref_counter.  Don't actually unprotect until the ref count
     * drops to zero.
     */
    if (entry_ptr->ro_ref_count > 1) {
        /* Sanity check */
        assert(entry_ptr->is_protected);
        assert(entry_ptr->is_read_only);

        if (dirtied)
            HGOTO_ERROR(H5E_CACHE, H5E_CANTUNPROTECT, FAIL, "Read only entry modified??");

        /* Reduce the RO ref count */
        (entry_ptr->ro_ref_count)--;

        /* Pin or unpin the entry as requested. */
        if (pin_entry) {
            /* Pin the entry from a client */
            if (H5C__pin_entry_from_client(cache_ptr, entry_ptr) < 0)
                HGOTO_ERROR(H5E_CACHE, H5E_CANTPIN, FAIL, "Can't pin entry by client");
        }
        else if (unpin_entry) {
            /* Unpin the entry from a client */
            if (H5C__unpin_entry_from_client(cache_ptr, entry_ptr, false) < 0)
                HGOTO_ERROR(H5E_CACHE, H5E_CANTUNPIN, FAIL, "Can't unpin entry by client");
        } /* end if */
    }
    else {
        if (entry_ptr->is_read_only) {
            /* Sanity check */
            assert(entry_ptr->ro_ref_count == 1);

            if (dirtied)
                HGOTO_ERROR(H5E_CACHE, H5E_CANTUNPROTECT, FAIL, "Read only entry modified??");

            entry_ptr->is_read_only = false;
            entry_ptr->ro_ref_count = 0;
        } /* end if */

#ifdef H5_HAVE_PARALLEL
        /* When the H5C code is used to implement the metadata cache in the
         * PHDF5 case, only the cache on process 0 is allowed to write to file.
         * All the other metadata caches must hold dirty entries until they
         * are told that the entries are clean.
         *
         * The clear_on_unprotect flag in the H5C_cache_entry_t structure
         * exists to deal with the case in which an entry is protected when
         * its cache receives word that the entry is now clean.  In this case,
         * the clear_on_unprotect flag is set, and the entry is flushed with
         * the H5C__FLUSH_CLEAR_ONLY_FLAG.
         *
         * All this is a bit awkward, but until the metadata cache entries
         * are contiguous, with only one dirty flag, we have to let the supplied
         * functions deal with the resetting the is_dirty flag.
         */
        if (entry_ptr->clear_on_unprotect) {
            /* Sanity check */
            assert(entry_ptr->is_dirty);

            entry_ptr->clear_on_unprotect = false;
            if (!dirtied)
                clear_entry = true;
        } /* end if */
#endif    /* H5_HAVE_PARALLEL */

        if (!entry_ptr->is_protected)
            HGOTO_ERROR(H5E_CACHE, H5E_CANTUNPROTECT, FAIL, "Entry already unprotected??");

        /* Mark the entry as dirty if appropriate */
        entry_ptr->is_dirty = (entry_ptr->is_dirty || dirtied);
        if (dirtied && entry_ptr->image_up_to_date) {
            entry_ptr->image_up_to_date = false;
            if (entry_ptr->flush_dep_nparents > 0)
                if (H5C__mark_flush_dep_unserialized(entry_ptr) < 0)
                    HGOTO_ERROR(H5E_CACHE, H5E_CANTNOTIFY, FAIL,
                                "Can't propagate serialization status to fd parents");
        } /* end if */

        /* Check for newly dirtied entry */
        if (was_clean && entry_ptr->is_dirty) {
            /* Update index for newly dirtied entry */
            H5C__UPDATE_INDEX_FOR_ENTRY_DIRTY(cache_ptr, entry_ptr, FAIL);

            /* If the entry's type has a 'notify' callback send a
             * 'entry dirtied' notice now that the entry is fully
             * integrated into the cache.
             */
            if (entry_ptr->type->notify &&
                (entry_ptr->type->notify)(H5C_NOTIFY_ACTION_ENTRY_DIRTIED, entry_ptr) < 0)
                HGOTO_ERROR(H5E_CACHE, H5E_CANTNOTIFY, FAIL,
                            "can't notify client about entry dirty flag set");

            /* Propagate the flush dep dirty flag up the flush dependency chain
             * if appropriate
             */
            if (entry_ptr->flush_dep_nparents > 0)
                if (H5C__mark_flush_dep_dirty(entry_ptr) < 0)
                    HGOTO_ERROR(H5E_CACHE, H5E_CANTMARKDIRTY, FAIL, "Can't propagate flush dep dirty flag");
        } /* end if */
        /* Check for newly clean entry */
        else if (!was_clean && !entry_ptr->is_dirty) {

            /* If the entry's type has a 'notify' callback send a
             * 'entry cleaned' notice now that the entry is fully
             * integrated into the cache.
             */
            if (entry_ptr->type->notify &&
                (entry_ptr->type->notify)(H5C_NOTIFY_ACTION_ENTRY_CLEANED, entry_ptr) < 0)
                HGOTO_ERROR(H5E_CACHE, H5E_CANTNOTIFY, FAIL,
                            "can't notify client about entry dirty flag cleared");

            /* Propagate the flush dep clean flag up the flush dependency chain
             * if appropriate
             */
            if (entry_ptr->flush_dep_nparents > 0)
                if (H5C__mark_flush_dep_clean(entry_ptr) < 0)
                    HGOTO_ERROR(H5E_CACHE, H5E_CANTMARKDIRTY, FAIL, "Can't propagate flush dep dirty flag");
        } /* end else-if */

        /* Pin or unpin the entry as requested. */
        if (pin_entry) {
            /* Pin the entry from a client */
            if (H5C__pin_entry_from_client(cache_ptr, entry_ptr) < 0)
                HGOTO_ERROR(H5E_CACHE, H5E_CANTPIN, FAIL, "Can't pin entry by client");
        }
        else if (unpin_entry) {
            /* Unpin the entry from a client */
            if (H5C__unpin_entry_from_client(cache_ptr, entry_ptr, false) < 0)
                HGOTO_ERROR(H5E_CACHE, H5E_CANTUNPIN, FAIL, "Can't unpin entry by client");
        } /* end if */

        /* H5C__UPDATE_RP_FOR_UNPROTECT will place the unprotected entry on
         * the pinned entry list if entry_ptr->is_pinned is true.
         */
        H5C__UPDATE_RP_FOR_UNPROTECT(cache_ptr, entry_ptr, FAIL);

        entry_ptr->is_protected = false;

        /* if the entry is dirty, 'or' its flush_marker with the set flush flag,
         * and then add it to the skip list if it isn't there already.
         */
        if (entry_ptr->is_dirty) {
            entry_ptr->flush_marker |= set_flush_marker;
            if (!entry_ptr->in_slist)
                /* this is a no-op if cache_ptr->slist_enabled is false */
                H5C__INSERT_ENTRY_IN_SLIST(cache_ptr, entry_ptr, FAIL);
        } /* end if */

        /* This implementation of the "deleted" option is a bit inefficient, as
         * we re-insert the entry to be deleted into the replacement policy
         * data structures, only to remove them again.  Depending on how often
         * we do this, we may want to optimize a bit.
         */
        if (deleted) {
            unsigned flush_flags = (H5C__FLUSH_CLEAR_ONLY_FLAG | H5C__FLUSH_INVALIDATE_FLAG);

            /* verify that the target entry is in the cache. */
            H5C__SEARCH_INDEX(cache_ptr, addr, test_entry_ptr, FAIL);

            if (test_entry_ptr == NULL)
                HGOTO_ERROR(H5E_CACHE, H5E_CANTUNPROTECT, FAIL, "entry not in hash table?!?");
            else if (test_entry_ptr != entry_ptr)
                HGOTO_ERROR(H5E_CACHE, H5E_CANTUNPROTECT, FAIL,
                            "hash table contains multiple entries for addr?!?");

            /* Set the 'free file space' flag for the flush, if needed */
            if (free_file_space)
                flush_flags |= H5C__FREE_FILE_SPACE_FLAG;

            /* Set the "take ownership" flag for the flush, if needed */
            if (take_ownership)
                flush_flags |= H5C__TAKE_OWNERSHIP_FLAG;

            /* Delete the entry from the skip list on destroy */
            flush_flags |= H5C__DEL_FROM_SLIST_ON_DESTROY_FLAG;

            assert((!cache_ptr->slist_enabled) || (((!was_clean) || dirtied) == (entry_ptr->in_slist)));

            if (H5C__flush_single_entry(f, entry_ptr, flush_flags) < 0)
                HGOTO_ERROR(H5E_CACHE, H5E_CANTUNPROTECT, FAIL, "Can't flush entry");
        } /* end if */
#ifdef H5_HAVE_PARALLEL
        else if (clear_entry) {
            /* Verify that the target entry is in the cache. */
            H5C__SEARCH_INDEX(cache_ptr, addr, test_entry_ptr, FAIL);

            if (test_entry_ptr == NULL)
                HGOTO_ERROR(H5E_CACHE, H5E_CANTUNPROTECT, FAIL, "entry not in hash table?!?");
            else if (test_entry_ptr != entry_ptr)
                HGOTO_ERROR(H5E_CACHE, H5E_CANTUNPROTECT, FAIL,
                            "hash table contains multiple entries for addr?!?");

            if (H5C__flush_single_entry(f, entry_ptr,
                                        H5C__FLUSH_CLEAR_ONLY_FLAG | H5C__DEL_FROM_SLIST_ON_DESTROY_FLAG) < 0)
                HGOTO_ERROR(H5E_CACHE, H5E_CANTUNPROTECT, FAIL, "Can't clear entry");
        } /* end else if */
#endif    /* H5_HAVE_PARALLEL */
    }

    H5C__UPDATE_STATS_FOR_UNPROTECT(cache_ptr);

done:
#ifdef H5C_DO_EXTREME_SANITY_CHECKS
    if (H5C__validate_protected_entry_list(cache_ptr) < 0 || H5C__validate_pinned_entry_list(cache_ptr) < 0 ||
        H5C__validate_lru_list(cache_ptr) < 0)
        HDONE_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "an extreme sanity check failed on exit");
#endif /* H5C_DO_EXTREME_SANITY_CHECKS */

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C_unprotect() */

/*-------------------------------------------------------------------------
 * Function:    H5C_unsettle_entry_ring
 *
 * Purpose:     Advise the metadata cache that the specified entry's free space
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
H5C_unsettle_entry_ring(void *_entry)
{
    H5C_cache_entry_t *entry = (H5C_cache_entry_t *)_entry; /* Entry whose ring to unsettle */
    H5C_t             *cache;                               /* Cache for file */
    herr_t             ret_value = SUCCEED;                 /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity checks */
    assert(entry);
    assert(entry->ring != H5C_RING_UNDEFINED);
    assert((H5C_RING_USER == entry->ring) || (H5C_RING_RDFSM == entry->ring) ||
           (H5C_RING_MDFSM == entry->ring));
    cache = entry->cache_ptr;
    assert(cache);

    switch (entry->ring) {
        case H5C_RING_USER:
            /* Do nothing */
            break;

        case H5C_RING_RDFSM:
            if (cache->rdfsm_settled) {
                if (cache->flush_in_progress || cache->close_warning_received)
                    HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "unexpected rdfsm ring unsettle");
                cache->rdfsm_settled = false;
            } /* end if */
            break;

        case H5C_RING_MDFSM:
            if (cache->mdfsm_settled) {
                if (cache->flush_in_progress || cache->close_warning_received)
                    HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "unexpected mdfsm ring unsettle");
                cache->mdfsm_settled = false;
            } /* end if */
            break;

        default:
            assert(false); /* this should be un-reachable */
            break;
    } /* end switch */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C_unsettle_entry_ring() */

/*-------------------------------------------------------------------------
 * Function:    H5C_create_flush_dependency()
 *
 * Purpose:     Initiates a parent<->child entry flush dependency.  The parent
 *              entry must be pinned or protected at the time of call, and must
 *              have all dependencies removed before the cache can shut down.
 *
 * Note:        Flush dependencies in the cache indicate that a child entry
 *              must be flushed to the file before its parent.  (This is
 *              currently used to implement Single-Writer/Multiple-Reader (SWMR)
 *              I/O access for data structures in the file).
 *
 *              Creating a flush dependency between two entries will also pin
 *              the parent entry.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C_create_flush_dependency(void *parent_thing, void *child_thing)
{
    H5C_t             *cache_ptr;
    H5C_cache_entry_t *parent_entry = (H5C_cache_entry_t *)parent_thing; /* Ptr to parent thing's entry */
    H5C_cache_entry_t *child_entry  = (H5C_cache_entry_t *)child_thing;  /* Ptr to child thing's entry */
    herr_t             ret_value    = SUCCEED;                           /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity checks */
    assert(parent_entry);
    assert(H5_addr_defined(parent_entry->addr));
    assert(child_entry);
    assert(H5_addr_defined(child_entry->addr));
    cache_ptr = parent_entry->cache_ptr;
    assert(cache_ptr);
    assert(cache_ptr == child_entry->cache_ptr);
#ifndef NDEBUG
    /* Make sure the parent is not already a parent */
    {
        unsigned u;

        for (u = 0; u < child_entry->flush_dep_nparents; u++)
            assert(child_entry->flush_dep_parent[u] != parent_entry);
    } /* end block */
#endif

    /* More sanity checks */
    if (child_entry == parent_entry)
        HGOTO_ERROR(H5E_CACHE, H5E_CANTDEPEND, FAIL, "Child entry flush dependency parent can't be itself");
    if (!(parent_entry->is_protected || parent_entry->is_pinned))
        HGOTO_ERROR(H5E_CACHE, H5E_CANTDEPEND, FAIL, "Parent entry isn't pinned or protected");

    /* Check for parent not pinned */
    if (!parent_entry->is_pinned) {
        /* Sanity check */
        assert(parent_entry->flush_dep_nchildren == 0);
        assert(!parent_entry->pinned_from_client);
        assert(!parent_entry->pinned_from_cache);

        /* Pin the parent entry */
        parent_entry->is_pinned = true;
        H5C__UPDATE_STATS_FOR_PIN(cache_ptr, parent_entry);
    } /* end else */

    /* Mark the entry as pinned from the cache's action (possibly redundantly) */
    parent_entry->pinned_from_cache = true;

    /* Check if we need to resize the child's parent array */
    if (child_entry->flush_dep_nparents >= child_entry->flush_dep_parent_nalloc) {
        if (child_entry->flush_dep_parent_nalloc == 0) {
            /* Array does not exist yet, allocate it */
            assert(!child_entry->flush_dep_parent);

            if (NULL == (child_entry->flush_dep_parent =
                             H5FL_SEQ_MALLOC(H5C_cache_entry_ptr_t, H5C_FLUSH_DEP_PARENT_INIT)))
                HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL,
                            "memory allocation failed for flush dependency parent list");
            child_entry->flush_dep_parent_nalloc = H5C_FLUSH_DEP_PARENT_INIT;
        } /* end if */
        else {
            /* Resize existing array */
            assert(child_entry->flush_dep_parent);

            if (NULL == (child_entry->flush_dep_parent =
                             H5FL_SEQ_REALLOC(H5C_cache_entry_ptr_t, child_entry->flush_dep_parent,
                                              2 * child_entry->flush_dep_parent_nalloc)))
                HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL,
                            "memory allocation failed for flush dependency parent list");
            child_entry->flush_dep_parent_nalloc *= 2;
        } /* end else */
        cache_ptr->entry_fd_height_change_counter++;
    } /* end if */

    /* Add the dependency to the child's parent array */
    child_entry->flush_dep_parent[child_entry->flush_dep_nparents] = parent_entry;
    child_entry->flush_dep_nparents++;

    /* Increment parent's number of children */
    parent_entry->flush_dep_nchildren++;

    /* Adjust the number of dirty children */
    if (child_entry->is_dirty) {
        /* Sanity check */
        assert(parent_entry->flush_dep_ndirty_children < parent_entry->flush_dep_nchildren);

        parent_entry->flush_dep_ndirty_children++;

        /* If the parent has a 'notify' callback, send a 'child entry dirtied' notice */
        if (parent_entry->type->notify &&
            (parent_entry->type->notify)(H5C_NOTIFY_ACTION_CHILD_DIRTIED, parent_entry) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_CANTNOTIFY, FAIL,
                        "can't notify parent about child entry dirty flag set");
    } /* end if */

    /* adjust the parent's number of unserialized children.  Note
     * that it is possible for and entry to be clean and unserialized.
     */
    if (!child_entry->image_up_to_date) {
        assert(parent_entry->flush_dep_nunser_children < parent_entry->flush_dep_nchildren);

        parent_entry->flush_dep_nunser_children++;

        /* If the parent has a 'notify' callback, send a 'child entry unserialized' notice */
        if (parent_entry->type->notify &&
            (parent_entry->type->notify)(H5C_NOTIFY_ACTION_CHILD_UNSERIALIZED, parent_entry) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_CANTNOTIFY, FAIL,
                        "can't notify parent about child entry serialized flag reset");
    } /* end if */

    /* Post-conditions, for successful operation */
    assert(parent_entry->is_pinned);
    assert(parent_entry->flush_dep_nchildren > 0);
    assert(child_entry->flush_dep_parent);
    assert(child_entry->flush_dep_nparents > 0);
    assert(child_entry->flush_dep_parent_nalloc > 0);
#ifndef NDEBUG
    H5C__assert_flush_dep_nocycle(parent_entry, child_entry);
#endif

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C_create_flush_dependency() */

/*-------------------------------------------------------------------------
 * Function:    H5C_destroy_flush_dependency()
 *
 * Purpose:     Terminates a parent<-> child entry flush dependency.  The
 *              parent entry must be pinned.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C_destroy_flush_dependency(void *parent_thing, void *child_thing)
{
    H5C_t             *cache_ptr;
    H5C_cache_entry_t *parent_entry = (H5C_cache_entry_t *)parent_thing; /* Ptr to parent entry */
    H5C_cache_entry_t *child_entry  = (H5C_cache_entry_t *)child_thing;  /* Ptr to child entry */
    unsigned           u;                                                /* Local index variable */
    herr_t             ret_value = SUCCEED;                              /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity checks */
    assert(parent_entry);
    assert(H5_addr_defined(parent_entry->addr));
    assert(child_entry);
    assert(H5_addr_defined(child_entry->addr));
    cache_ptr = parent_entry->cache_ptr;
    assert(cache_ptr);
    assert(cache_ptr == child_entry->cache_ptr);

    /* Usage checks */
    if (!parent_entry->is_pinned)
        HGOTO_ERROR(H5E_CACHE, H5E_CANTUNDEPEND, FAIL, "Parent entry isn't pinned");
    if (NULL == child_entry->flush_dep_parent)
        HGOTO_ERROR(H5E_CACHE, H5E_CANTUNDEPEND, FAIL,
                    "Child entry doesn't have a flush dependency parent array");
    if (0 == parent_entry->flush_dep_nchildren)
        HGOTO_ERROR(H5E_CACHE, H5E_CANTUNDEPEND, FAIL,
                    "Parent entry flush dependency ref. count has no child dependencies");

    /* Search for parent in child's parent array.  This is a linear search
     * because we do not expect large numbers of parents.  If this changes, we
     * may wish to change the parent array to a skip list */
    for (u = 0; u < child_entry->flush_dep_nparents; u++)
        if (child_entry->flush_dep_parent[u] == parent_entry)
            break;
    if (u == child_entry->flush_dep_nparents)
        HGOTO_ERROR(H5E_CACHE, H5E_CANTUNDEPEND, FAIL,
                    "Parent entry isn't a flush dependency parent for child entry");

    /* Remove parent entry from child's parent array */
    if (u < (child_entry->flush_dep_nparents - 1))
        memmove(&child_entry->flush_dep_parent[u], &child_entry->flush_dep_parent[u + 1],
                (child_entry->flush_dep_nparents - u - 1) * sizeof(child_entry->flush_dep_parent[0]));
    child_entry->flush_dep_nparents--;

    /* Adjust parent entry's nchildren and unpin parent if it goes to zero */
    parent_entry->flush_dep_nchildren--;
    if (0 == parent_entry->flush_dep_nchildren) {
        /* Sanity check */
        assert(parent_entry->pinned_from_cache);

        /* Check if we should unpin parent entry now */
        if (!parent_entry->pinned_from_client)
            if (H5C__unpin_entry_real(cache_ptr, parent_entry, true) < 0)
                HGOTO_ERROR(H5E_CACHE, H5E_CANTUNPIN, FAIL, "Can't unpin entry");

        /* Mark the entry as unpinned from the cache's action */
        parent_entry->pinned_from_cache = false;
    } /* end if */

    /* Adjust parent entry's ndirty_children */
    if (child_entry->is_dirty) {
        /* Sanity check */
        assert(parent_entry->flush_dep_ndirty_children > 0);

        parent_entry->flush_dep_ndirty_children--;

        /* If the parent has a 'notify' callback, send a 'child entry cleaned' notice */
        if (parent_entry->type->notify &&
            (parent_entry->type->notify)(H5C_NOTIFY_ACTION_CHILD_CLEANED, parent_entry) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_CANTNOTIFY, FAIL,
                        "can't notify parent about child entry dirty flag reset");
    } /* end if */

    /* adjust parent entry's number of unserialized children */
    if (!child_entry->image_up_to_date) {
        assert(parent_entry->flush_dep_nunser_children > 0);

        parent_entry->flush_dep_nunser_children--;

        /* If the parent has a 'notify' callback, send a 'child entry serialized' notice */
        if (parent_entry->type->notify &&
            (parent_entry->type->notify)(H5C_NOTIFY_ACTION_CHILD_SERIALIZED, parent_entry) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_CANTNOTIFY, FAIL,
                        "can't notify parent about child entry serialized flag set");
    } /* end if */

    /* Shrink or free the parent array if appropriate */
    if (child_entry->flush_dep_nparents == 0) {
        child_entry->flush_dep_parent = H5FL_SEQ_FREE(H5C_cache_entry_ptr_t, child_entry->flush_dep_parent);
        child_entry->flush_dep_parent_nalloc = 0;
    } /* end if */
    else if (child_entry->flush_dep_parent_nalloc > H5C_FLUSH_DEP_PARENT_INIT &&
             child_entry->flush_dep_nparents <= (child_entry->flush_dep_parent_nalloc / 4)) {
        if (NULL == (child_entry->flush_dep_parent =
                         H5FL_SEQ_REALLOC(H5C_cache_entry_ptr_t, child_entry->flush_dep_parent,
                                          child_entry->flush_dep_parent_nalloc / 4)))
            HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL,
                        "memory allocation failed for flush dependency parent list");
        child_entry->flush_dep_parent_nalloc /= 4;
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C_destroy_flush_dependency() */

/*-------------------------------------------------------------------------
 * Function:    H5C_expunge_entry
 *
 * Purpose:     Expunge an entry from the cache without writing it to disk
 *              even if it is dirty.  The entry may not be either pinned or
 *              protected.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C_expunge_entry(H5F_t *f, const H5C_class_t *type, haddr_t addr, unsigned flags)
{
    H5C_t             *cache_ptr;
    H5C_cache_entry_t *entry_ptr   = NULL;
    unsigned           flush_flags = (H5C__FLUSH_INVALIDATE_FLAG | H5C__FLUSH_CLEAR_ONLY_FLAG);
    herr_t             ret_value   = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    assert(f);
    assert(f->shared);
    cache_ptr = f->shared->cache;
    assert(cache_ptr);
    assert(type);
    assert(H5_addr_defined(addr));

#ifdef H5C_DO_EXTREME_SANITY_CHECKS
    if (H5C__validate_lru_list(cache_ptr) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "LRU extreme sanity check failed on entry");
#endif /* H5C_DO_EXTREME_SANITY_CHECKS */

    /* Look for entry in cache */
    H5C__SEARCH_INDEX(cache_ptr, addr, entry_ptr, FAIL);
    if ((entry_ptr == NULL) || (entry_ptr->type != type))
        /* the target doesn't exist in the cache, so we are done. */
        HGOTO_DONE(SUCCEED);

    assert(entry_ptr->addr == addr);
    assert(entry_ptr->type == type);

    /* Check for entry being pinned or protected */
    if (entry_ptr->is_protected)
        HGOTO_ERROR(H5E_CACHE, H5E_CANTEXPUNGE, FAIL, "Target entry is protected");
    if (entry_ptr->is_pinned)
        HGOTO_ERROR(H5E_CACHE, H5E_CANTEXPUNGE, FAIL, "Target entry is pinned");

    /* If we get this far, call H5C__flush_single_entry() with the
     * H5C__FLUSH_INVALIDATE_FLAG and the H5C__FLUSH_CLEAR_ONLY_FLAG.
     * This will clear the entry, and then delete it from the cache.
     */

    /* Pass along 'free file space' flag */
    flush_flags |= (flags & H5C__FREE_FILE_SPACE_FLAG);

    /* Delete the entry from the skip list on destroy */
    flush_flags |= H5C__DEL_FROM_SLIST_ON_DESTROY_FLAG;

    if (H5C__flush_single_entry(f, entry_ptr, flush_flags) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_CANTEXPUNGE, FAIL, "can't flush entry");

done:
#ifdef H5C_DO_EXTREME_SANITY_CHECKS
    if (H5C__validate_lru_list(cache_ptr) < 0)
        HDONE_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "LRU extreme sanity check failed on exit");
#endif /* H5C_DO_EXTREME_SANITY_CHECKS */

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C_expunge_entry() */

/*-------------------------------------------------------------------------
 * Function:    H5C_remove_entry
 *
 * Purpose:     Remove an entry from the cache.  Must be not protected, pinned,
 *        dirty, involved in flush dependencies, etc.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C_remove_entry(void *_entry)
{
    H5C_cache_entry_t *entry = (H5C_cache_entry_t *)_entry; /* Entry to remove */
    H5C_t             *cache;                               /* Cache for file */
    herr_t             ret_value = SUCCEED;                 /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity checks */
    assert(entry);
    assert(entry->ring != H5C_RING_UNDEFINED);
    cache = entry->cache_ptr;
    assert(cache);

    /* Check for error conditions */
    if (entry->is_dirty)
        HGOTO_ERROR(H5E_CACHE, H5E_CANTREMOVE, FAIL, "can't remove dirty entry from cache");
    if (entry->is_protected)
        HGOTO_ERROR(H5E_CACHE, H5E_CANTREMOVE, FAIL, "can't remove protected entry from cache");
    if (entry->is_pinned)
        HGOTO_ERROR(H5E_CACHE, H5E_CANTREMOVE, FAIL, "can't remove pinned entry from cache");
    /* NOTE: If these two errors are getting tripped because the entry is
     *          in a flush dependency with a freedspace entry, move the checks
     *          after the "before evict" message is sent, and add the
     *          "child being evicted" message to the "before evict" notify
     *          section below.  QAK - 2017/08/03
     */
    if (entry->flush_dep_nparents > 0)
        HGOTO_ERROR(H5E_CACHE, H5E_CANTREMOVE, FAIL,
                    "can't remove entry with flush dependency parents from cache");
    if (entry->flush_dep_nchildren > 0)
        HGOTO_ERROR(H5E_CACHE, H5E_CANTREMOVE, FAIL,
                    "can't remove entry with flush dependency children from cache");

    /* Additional internal cache consistency checks */
    assert(!entry->in_slist);
    assert(!entry->flush_marker);
    assert(!entry->flush_in_progress);

    /* Note that the algorithm below is (very) similar to the set of operations
     * in H5C__flush_single_entry() and should be kept in sync with changes
     * to that code. - QAK, 2016/11/30
     */

    /* Update stats, as if we are "destroying" and taking ownership of the entry */
    H5C__UPDATE_STATS_FOR_EVICTION(cache, entry, true);

    /* If the entry's type has a 'notify' callback, send a 'before eviction'
     * notice while the entry is still fully integrated in the cache.
     */
    if (entry->type->notify && (entry->type->notify)(H5C_NOTIFY_ACTION_BEFORE_EVICT, entry) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_CANTNOTIFY, FAIL, "can't notify client about entry to evict");

    /* Update the cache internal data structures as appropriate for a destroy.
     * Specifically:
     *    1) Delete it from the index
     *    2) Delete it from the collective read access list
     *    3) Update the replacement policy for eviction
     *    4) Remove it from the tag list for this object
     */

    H5C__DELETE_FROM_INDEX(cache, entry, FAIL);

#ifdef H5_HAVE_PARALLEL
    /* Check for collective read access flag */
    if (entry->coll_access) {
        entry->coll_access = false;
        H5C__REMOVE_FROM_COLL_LIST(cache, entry, FAIL);
    }  /* end if */
#endif /* H5_HAVE_PARALLEL */

    H5C__UPDATE_RP_FOR_EVICTION(cache, entry, FAIL);

    /* Remove entry from tag list */
    if (H5C__untag_entry(cache, entry) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_CANTREMOVE, FAIL, "can't remove entry from tag list");

    /* Increment entries_removed_counter and set last_entry_removed_ptr.
     * As we me be about to free the entry, recall that last_entry_removed_ptr
     * must NEVER be dereferenced.
     *
     * Recall that these fields are maintained to allow functions that perform
     * scans of lists of entries to detect the unexpected removal of entries
     * (via expunge, eviction, or take ownership at present), so that they can
     * re-start their scans if necessary.
     *
     * Also check if the entry we are watching for removal is being
     * removed (usually the 'next' entry for an iteration) and reset
     * it to indicate that it was removed.
     */
    cache->entries_removed_counter++;
    cache->last_entry_removed_ptr = entry;
    if (entry == cache->entry_watched_for_removal)
        cache->entry_watched_for_removal = NULL;

    /* Internal cache data structures should now be up to date, and
     * consistent with the status of the entry.
     *
     * Now clean up internal cache fields if appropriate.
     */

    /* Free the buffer for the on disk image */
    if (entry->image_ptr != NULL)
        entry->image_ptr = H5MM_xfree(entry->image_ptr);

    /* Reset the pointer to the cache the entry is within */
    entry->cache_ptr = NULL;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__remove_entry() */
