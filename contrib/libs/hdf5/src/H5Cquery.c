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
 * Created:     H5Cquery.c
 *
 * Purpose:     Routines which query different components of the generic
 *              cache structure or entries.
 *
 *-------------------------------------------------------------------------
 */

/****************/
/* Module Setup */
/****************/

#include "H5Cmodule.h" /* This source code file is part of the H5C module */
#define H5F_FRIEND     /*suppress error about including H5Fpkg	  */

/***********/
/* Headers */
/***********/
#include "H5private.h"  /* Generic Functions			*/
#include "H5Cpkg.h"     /* Cache				*/
#include "H5Eprivate.h" /* Error handling		  	*/
#include "H5Fpkg.h"     /* Files				*/

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

/*****************************/
/* Library Private Variables */
/*****************************/

/*******************/
/* Local Variables */
/*******************/

/*-------------------------------------------------------------------------
 * Function:    H5C_get_cache_auto_resize_config
 *
 * Purpose:	Copy the current configuration of the cache automatic
 *		re-sizing function into the instance of H5C_auto_size_ctl_t
 *		pointed to by config_ptr.
 *
 * Return:      SUCCEED on success, and FAIL on failure.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C_get_cache_auto_resize_config(const H5C_t *cache_ptr, H5C_auto_size_ctl_t *config_ptr)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    if (cache_ptr == NULL)
        HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "Bad cache_ptr on entry.");
    if (config_ptr == NULL)
        HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "Bad config_ptr on entry.");

    *config_ptr = cache_ptr->resize_ctl;

    config_ptr->set_initial_size = false;
    config_ptr->initial_size     = cache_ptr->max_cache_size;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C_get_cache_auto_resize_config() */

/*-------------------------------------------------------------------------
 * Function:    H5C_get_cache_size
 *
 * Purpose:	Return the cache maximum size, the minimum clean size, the
 *		current size, and the current number of entries in
 *              *max_size_ptr, *min_clean_size_ptr, *cur_size_ptr, and
 *		*cur_num_entries_ptr respectively.  If any of these
 *		parameters are NULL, skip that value.
 *
 * Return:      SUCCEED on success, and FAIL on failure.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C_get_cache_size(const H5C_t *cache_ptr, size_t *max_size_ptr, size_t *min_clean_size_ptr,
                   size_t *cur_size_ptr, uint32_t *cur_num_entries_ptr)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    if (cache_ptr == NULL)
        HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "Bad cache_ptr on entry.");

    if (max_size_ptr != NULL)
        *max_size_ptr = cache_ptr->max_cache_size;

    if (min_clean_size_ptr != NULL)
        *min_clean_size_ptr = cache_ptr->min_clean_size;

    if (cur_size_ptr != NULL)
        *cur_size_ptr = cache_ptr->index_size;

    if (cur_num_entries_ptr != NULL)
        *cur_num_entries_ptr = cache_ptr->index_len;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C_get_cache_size() */

/*-------------------------------------------------------------------------
 * Function:    H5C_get_cache_flush_in_progress
 *
 * Purpose:	Return flush_in_progress in *flush_in_progress_ptr
 *		    If the parameter is NULL, skip that value.
 *
 * Return:  SUCCEED on success, and FAIL on failure.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C_get_cache_flush_in_progress(const H5C_t *cache_ptr, bool *flush_in_progress_ptr)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    if (cache_ptr == NULL)
        HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "Bad cache_ptr on entry.");

    if (flush_in_progress_ptr != NULL)
        *flush_in_progress_ptr = cache_ptr->flush_in_progress;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C_get_cache_flush_in_progress() */

/*-------------------------------------------------------------------------
 * Function:    H5C_get_cache_hit_rate
 *
 * Purpose:	Compute and return the current cache hit rate in
 *              *hit_rate_ptr.  If there have been no accesses since the
 *              last time the cache hit rate stats were reset, set
 *		*hit_rate_ptr to 0.0.  On error, *hit_rate_ptr is
 *		undefined.
 *
 * Return:      SUCCEED on success, and FAIL on failure.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C_get_cache_hit_rate(const H5C_t *cache_ptr, double *hit_rate_ptr)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    if (cache_ptr == NULL)
        HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "Bad cache_ptr on entry.");
    if (hit_rate_ptr == NULL)
        HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "Bad hit_rate_ptr on entry.");

    assert(cache_ptr->cache_hits >= 0);
    assert(cache_ptr->cache_accesses >= cache_ptr->cache_hits);

    if (cache_ptr->cache_accesses > 0)
        *hit_rate_ptr = ((double)(cache_ptr->cache_hits)) / ((double)(cache_ptr->cache_accesses));
    else
        *hit_rate_ptr = 0.0;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C_get_cache_hit_rate() */

/*-------------------------------------------------------------------------
 * Function:    H5C_get_entry_status
 *
 * Purpose:     This function is used to determine whether the cache
 *		contains an entry with the specified base address.  If
 *		the entry exists, it also reports some status information
 *		on the entry.
 *
 *		Status information is reported in the locations pointed
 *		to by the size_ptr, in_cache_ptr, is_dirty_ptr, and
 *		is_protected_ptr.  While in_cache_ptr must be defined,
 *		the remaining pointers may be NULL, in which case the
 *		associated data is not reported.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C_get_entry_status(const H5F_t *f, haddr_t addr, size_t *size_ptr, bool *in_cache_ptr, bool *is_dirty_ptr,
                     bool *is_protected_ptr, bool *is_pinned_ptr, bool *is_corked_ptr,
                     bool *is_flush_dep_parent_ptr, bool *is_flush_dep_child_ptr, bool *image_up_to_date_ptr)
{
    H5C_t             *cache_ptr;
    H5C_cache_entry_t *entry_ptr = NULL;
    herr_t             ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity checks */
    assert(f);
    assert(f->shared);
    cache_ptr = f->shared->cache;
    assert(cache_ptr != NULL);
    assert(H5_addr_defined(addr));
    assert(in_cache_ptr != NULL);

    if (cache_ptr == NULL)
        HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "Bad cache_ptr on entry.");

    H5C__SEARCH_INDEX(cache_ptr, addr, entry_ptr, FAIL);

    if (entry_ptr == NULL) {
        /* the entry doesn't exist in the cache -- report this
         * and quit.
         */
        *in_cache_ptr = false;
    } /* end if */
    else {
        *in_cache_ptr = true;
        if (size_ptr != NULL)
            *size_ptr = entry_ptr->size;
        if (is_dirty_ptr != NULL)
            *is_dirty_ptr = entry_ptr->is_dirty;
        if (is_protected_ptr != NULL)
            *is_protected_ptr = entry_ptr->is_protected;
        if (is_pinned_ptr != NULL)
            *is_pinned_ptr = entry_ptr->is_pinned;
        if (is_corked_ptr != NULL)
            *is_corked_ptr = entry_ptr->tag_info ? entry_ptr->tag_info->corked : false;
        if (is_flush_dep_parent_ptr != NULL)
            *is_flush_dep_parent_ptr = (entry_ptr->flush_dep_nchildren > 0);
        if (is_flush_dep_child_ptr != NULL)
            *is_flush_dep_child_ptr = (entry_ptr->flush_dep_nparents > 0);
        if (image_up_to_date_ptr != NULL)
            *image_up_to_date_ptr = entry_ptr->image_up_to_date;
    } /* end else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C_get_entry_status() */

/*-------------------------------------------------------------------------
 * Function:    H5C_get_evictions_enabled()
 *
 * Purpose:     Copy the current value of cache_ptr->evictions_enabled into
 *              *evictions_enabled_ptr.
 *
 * Return:      SUCCEED on success, and FAIL on failure.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C_get_evictions_enabled(const H5C_t *cache_ptr, bool *evictions_enabled_ptr)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    if (cache_ptr == NULL)
        HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "Bad cache_ptr on entry.");

    if (evictions_enabled_ptr == NULL)
        HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "Bad evictions_enabled_ptr on entry.");

    *evictions_enabled_ptr = cache_ptr->evictions_enabled;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C_get_evictions_enabled() */

/*-------------------------------------------------------------------------
 * Function:    H5C_get_aux_ptr
 *
 * Purpose:     Get the aux_ptr field from the cache.
 *
 *              This field will either be NULL (when accessing a file serially)
 *              or contains a pointer to the auxiliary info for parallel I/O.
 *
 * Return:      NULL/non-NULL (can't fail)
 *
 *-------------------------------------------------------------------------
 */
void *
H5C_get_aux_ptr(const H5C_t *cache_ptr)
{
    FUNC_ENTER_NOAPI_NOERR

    /* Check arguments */
    assert(cache_ptr);

    FUNC_LEAVE_NOAPI(cache_ptr->aux_ptr)
} /* H5C_get_aux_ptr() */

/*-------------------------------------------------------------------------
 * Function:    H5C_get_entry_ring
 *
 * Purpose:     Given a file address, retrieve the ring for an entry at that
 *              address.
 *
 * 		On error, the value of *ring is not modified.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C_get_entry_ring(const H5F_t *f, haddr_t addr, H5C_ring_t *ring)
{
    H5C_t             *cache_ptr;           /* Pointer to cache */
    H5C_cache_entry_t *entry_ptr;           /* Pointer to cache entry at address */
    herr_t             ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity checks */
    assert(f);
    assert(f->shared);
    cache_ptr = f->shared->cache;
    assert(cache_ptr);
    assert(H5_addr_defined(addr));

    /* Locate the entry at the address */
    H5C__SEARCH_INDEX(cache_ptr, addr, entry_ptr, FAIL);
    if (entry_ptr == NULL)
        HGOTO_ERROR(H5E_CACHE, H5E_NOTFOUND, FAIL, "can't find entry in index");

    /* Return the ring value */
    *ring = entry_ptr->ring;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C_get_entry_ring() */

/*-------------------------------------------------------------------------
 * Function:    H5C_get_mdc_image_info
 *
 * Purpose:	    To retrieve the address and size of the cache image in the file.
 *
 * Return:      SUCCEED on success, and FAIL on failure.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C_get_mdc_image_info(const H5C_t *cache_ptr, haddr_t *image_addr, hsize_t *image_len)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    if (cache_ptr == NULL)
        HGOTO_ERROR(H5E_CACHE, H5E_BADVALUE, FAIL, "bad cache_ptr on entry");

    if (image_addr)
        *image_addr = cache_ptr->image_addr;
    if (image_len)
        *image_len = cache_ptr->image_len;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C_get_mdc_image_info() */
