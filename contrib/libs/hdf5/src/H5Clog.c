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
 * Created:     H5Clog.c
 *
 * Purpose:     Functions for metadata cache logging
 *
 *-------------------------------------------------------------------------
 */

/****************/
/* Module Setup */
/****************/
#include "H5Cmodule.h" /* This source code file is part of the H5C module */

/***********/
/* Headers */
/***********/
#include "H5private.h"  /* Generic Functions                        */
#define H5AC_FRIEND     /* Suppress error about including H5ACpkg */
#include "H5ACpkg.h"    /* Metadata cache                           */
#include "H5Cpkg.h"     /* Cache                                    */
#include "H5Clog.h"     /* Cache logging                            */
#include "H5Eprivate.h" /* Error handling                           */

/****************/
/* Local Macros */
/****************/

/******************/
/* Local Typedefs */
/******************/

/********************/
/* Package Typedefs */
/********************/

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
 * Function:    H5C_log_set_up
 *
 * Purpose:     Setup for metadata cache logging.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C_log_set_up(H5C_t *cache, const char log_location[], H5C_log_style_t style, bool start_immediately)
{
    int    mpi_rank  = -1;      /* -1 indicates serial (no MPI rank) */
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity checks */
    assert(cache);
    assert(log_location);

    /* Check logging flags */
    if (cache->log_info->enabled)
        HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "logging already set up");

        /* Get the rank when MPI is in use. Logging clients will usually
         * use that to create per-process logs.
         */
#ifdef H5_HAVE_PARALLEL
    if (NULL != cache->aux_ptr)
        mpi_rank = ((H5AC_aux_t *)(cache->aux_ptr))->mpi_rank;
#endif /*H5_HAVE_PARALLEL*/

    /* Set up logging */
    if (H5C_LOG_STYLE_JSON == style) {
        if (H5C__log_json_set_up(cache->log_info, log_location, mpi_rank) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "unable to set up json logging");
    }
    else if (H5C_LOG_STYLE_TRACE == style) {
        if (H5C__log_trace_set_up(cache->log_info, log_location, mpi_rank) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "unable to set up trace logging");
    }
    else
        HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "unknown logging style");

    /* Set logging flags */
    cache->log_info->enabled = true;

    /* Start logging if requested */
    if (start_immediately)
        if (H5C_start_logging(cache) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "unable to start logging");

done:

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C_log_set_up() */

/*-------------------------------------------------------------------------
 * Function:    H5C_log_tear_down
 *
 * Purpose:     Tear-down for metadata cache logging.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C_log_tear_down(H5C_t *cache)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity checks */
    assert(cache);

    /* Check logging flags */
    if (false == cache->log_info->enabled)
        HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "logging not enabled");

    /* Stop logging if that's going on */
    if (cache->log_info->logging)
        if (H5C_stop_logging(cache) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "unable to stop logging");

    /* Tear down logging */
    if (cache->log_info->cls->tear_down_logging)
        if (cache->log_info->cls->tear_down_logging(cache->log_info) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "log-specific tear down call failed");

    /* Unset logging flags */
    cache->log_info->enabled = false;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C_log_tear_down() */

/*-------------------------------------------------------------------------
 * Function:    H5C_start_logging
 *
 * Purpose:     Start logging metadata cache operations.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C_start_logging(H5C_t *cache)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity checks */
    assert(cache);

    /* Check logging flags */
    if (false == cache->log_info->enabled)
        HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "logging not enabled");

    /* Start logging */
    if (cache->log_info->cls->start_logging)
        if (cache->log_info->cls->start_logging(cache->log_info) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "log-specific start call failed");

    /* Set logging flags */
    cache->log_info->logging = true;

    /* Write a log message */
    if (cache->log_info->cls->write_start_log_msg)
        if (cache->log_info->cls->write_start_log_msg(cache->log_info->udata) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "log-specific write start call failed");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C_start_logging() */

/*-------------------------------------------------------------------------
 * Function:    H5C_stop_logging
 *
 * Purpose:     Stop logging metadata cache operations.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C_stop_logging(H5C_t *cache)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity checks */
    assert(cache);

    /* Check logging flags */
    if (false == cache->log_info->enabled)
        HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "logging not enabled");
    if (false == cache->log_info->logging)
        HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "logging not in progress");

    /* Write a log message */
    if (cache->log_info->cls->write_stop_log_msg)
        if (cache->log_info->cls->write_stop_log_msg(cache->log_info->udata) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "log-specific write stop call failed");

    /* Stop logging */
    if (cache->log_info->cls->stop_logging)
        if (cache->log_info->cls->stop_logging(cache->log_info) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "log-specific stop call failed");

    /* Set logging flags */
    cache->log_info->logging = false;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C_stop_logging() */

/*-------------------------------------------------------------------------
 * Function:    H5C_get_logging_status
 *
 * Purpose:     Determines if the cache is actively logging (via the OUT
 *              parameters).
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C_get_logging_status(const H5C_t *cache, bool *is_enabled, bool *is_currently_logging)
{
    FUNC_ENTER_NOAPI_NOERR

    /* Sanity checks */
    assert(cache);
    assert(is_enabled);
    assert(is_currently_logging);

    /* Get logging flags */
    *is_enabled           = cache->log_info->enabled;
    *is_currently_logging = cache->log_info->logging;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5C_get_logging_status() */

/*-------------------------------------------------------------------------
 * Function:    H5C_log_write_create_cache_msg
 *
 * Purpose:     Write a log message for cache creation.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C_log_write_create_cache_msg(H5C_t *cache, herr_t fxn_ret_value)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity checks */
    assert(cache);

    /* Write a log message */
    if (cache->log_info->cls->write_create_cache_log_msg)
        if (cache->log_info->cls->write_create_cache_log_msg(cache->log_info->udata, fxn_ret_value) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "log-specific write create cache call failed");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C_log_write_create_cache_msg() */

/*-------------------------------------------------------------------------
 * Function:    H5C_log_write_destroy_cache_msg
 *
 * Purpose:     Write a log message for cache destruction.
 *
 * NOTE:        This can't print out the H5AC call return value, since we
 *              won't know that until the cache is destroyed and at that
 *              point we no longer have pointers to the logging information.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C_log_write_destroy_cache_msg(H5C_t *cache)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity checks */
    assert(cache);

    /* Write a log message */
    if (cache->log_info->cls->write_destroy_cache_log_msg)
        if (cache->log_info->cls->write_destroy_cache_log_msg(cache->log_info->udata) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "log-specific write destroy cache call failed");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C_log_write_destroy_cache_msg() */

/*-------------------------------------------------------------------------
 * Function:    H5C_log_write_evict_cache_msg
 *
 * Purpose:     Write a log message for eviction of cache entries.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C_log_write_evict_cache_msg(H5C_t *cache, herr_t fxn_ret_value)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity checks */
    assert(cache);

    /* Write a log message */
    if (cache->log_info->cls->write_evict_cache_log_msg)
        if (cache->log_info->cls->write_evict_cache_log_msg(cache->log_info->udata, fxn_ret_value) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "log-specific write evict cache call failed");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C_log_write_evict_cache_msg() */

/*-------------------------------------------------------------------------
 * Function:    H5C_log_write_expunge_entry_msg
 *
 * Purpose:     Write a log message for expunge of cache entries.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C_log_write_expunge_entry_msg(H5C_t *cache, haddr_t address, int type_id, herr_t fxn_ret_value)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity checks */
    assert(cache);

    /* Write a log message */
    if (cache->log_info->cls->write_expunge_entry_log_msg)
        if (cache->log_info->cls->write_expunge_entry_log_msg(cache->log_info->udata, address, type_id,
                                                              fxn_ret_value) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "log-specific write expunge entry call failed");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C_log_write_expunge_entry_msg() */

/*-------------------------------------------------------------------------
 * Function:    H5C_log_write_flush_cache_msg
 *
 * Purpose:     Write a log message for cache flushes.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C_log_write_flush_cache_msg(H5C_t *cache, herr_t fxn_ret_value)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity checks */
    assert(cache);

    /* Write a log message */
    if (cache->log_info->cls->write_flush_cache_log_msg)
        if (cache->log_info->cls->write_flush_cache_log_msg(cache->log_info->udata, fxn_ret_value) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "log-specific flush cache call failed");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C_log_write_flush_cache_msg() */

/*-------------------------------------------------------------------------
 * Function:    H5C_log_write_insert_entry_msg
 *
 * Purpose:     Write a log message for insertion of cache entries.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C_log_write_insert_entry_msg(H5C_t *cache, haddr_t address, int type_id, unsigned flags, size_t size,
                               herr_t fxn_ret_value)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity checks */
    assert(cache);

    /* Write a log message */
    if (cache->log_info->cls->write_insert_entry_log_msg)
        if (cache->log_info->cls->write_insert_entry_log_msg(cache->log_info->udata, address, type_id, flags,
                                                             size, fxn_ret_value) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "log-specific insert entry call failed");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C_log_write_insert_entry_msg() */

/*-------------------------------------------------------------------------
 * Function:    H5C_log_write_mark_entry_dirty_msg
 *
 * Purpose:     Write a log message for marking cache entries as dirty.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C_log_write_mark_entry_dirty_msg(H5C_t *cache, const H5C_cache_entry_t *entry, herr_t fxn_ret_value)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity checks */
    assert(cache);

    /* Write a log message */
    assert(entry);
    if (cache->log_info->cls->write_mark_entry_dirty_log_msg)
        if (cache->log_info->cls->write_mark_entry_dirty_log_msg(cache->log_info->udata, entry,
                                                                 fxn_ret_value) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "log-specific mark dirty entry call failed");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C_log_write_mark_entry_dirty_msg() */

/*-------------------------------------------------------------------------
 * Function:    H5C_log_write_mark_entry_clean_msg
 *
 * Purpose:     Write a log message for marking cache entries as clean.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C_log_write_mark_entry_clean_msg(H5C_t *cache, const H5C_cache_entry_t *entry, herr_t fxn_ret_value)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity checks */
    assert(cache);

    /* Write a log message */
    assert(entry);
    if (cache->log_info->cls->write_mark_entry_clean_log_msg)
        if (cache->log_info->cls->write_mark_entry_clean_log_msg(cache->log_info->udata, entry,
                                                                 fxn_ret_value) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "log-specific mark clean entry call failed");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C_log_write_mark_entry_clean_msg() */

/*-------------------------------------------------------------------------
 * Function:    H5C_log_write_mark_unserialized_entry_msg
 *
 * Purpose:     Write a log message for marking cache entries as unserialized.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C_log_write_mark_unserialized_entry_msg(H5C_t *cache, const H5C_cache_entry_t *entry, herr_t fxn_ret_value)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity checks */
    assert(cache);

    /* Write a log message */
    assert(entry);
    if (cache->log_info->cls->write_mark_unserialized_entry_log_msg)
        if (cache->log_info->cls->write_mark_unserialized_entry_log_msg(cache->log_info->udata, entry,
                                                                        fxn_ret_value) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "log-specific mark unserialized entry call failed");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C_log_write_mark_unserialized_entry_msg() */

/*-------------------------------------------------------------------------
 * Function:    H5C_log_write_mark_serialized_entry_msg
 *
 * Purpose:     Write a log message for marking cache entries as serialize.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C_log_write_mark_serialized_entry_msg(H5C_t *cache, const H5C_cache_entry_t *entry, herr_t fxn_ret_value)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity checks */
    assert(cache);

    /* Write a log message */
    assert(entry);
    if (cache->log_info->cls->write_mark_serialized_entry_log_msg)
        if (cache->log_info->cls->write_mark_serialized_entry_log_msg(cache->log_info->udata, entry,
                                                                      fxn_ret_value) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "log-specific mark serialized entry call failed");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C_log_write_mark_serialized_entry_msg() */

/*-------------------------------------------------------------------------
 * Function:    H5C_log_write_move_entry_msg
 *
 * Purpose:     Write a log message for moving a cache entry.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C_log_write_move_entry_msg(H5C_t *cache, haddr_t old_addr, haddr_t new_addr, int type_id,
                             herr_t fxn_ret_value)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity checks */
    assert(cache);

    /* Write a log message */
    if (cache->log_info->cls->write_move_entry_log_msg)
        if (cache->log_info->cls->write_move_entry_log_msg(cache->log_info->udata, old_addr, new_addr,
                                                           type_id, fxn_ret_value) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "log-specific move entry call failed");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C_log_write_move_entry_msg() */

/*-------------------------------------------------------------------------
 * Function:    H5C_log_write_pin_entry_msg
 *
 * Purpose:     Write a log message for pinning a cache entry.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C_log_write_pin_entry_msg(H5C_t *cache, const H5C_cache_entry_t *entry, herr_t fxn_ret_value)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity checks */
    assert(cache);

    /* Write a log message */
    assert(entry);
    if (cache->log_info->cls->write_pin_entry_log_msg)
        if (cache->log_info->cls->write_pin_entry_log_msg(cache->log_info->udata, entry, fxn_ret_value) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "log-specific pin entry call failed");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C_log_write_pin_entry_msg() */

/*-------------------------------------------------------------------------
 * Function:    H5C_log_write_create_fd_msg
 *
 * Purpose:     Write a log message for creating a flush dependency between
 *              two cache entries.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C_log_write_create_fd_msg(H5C_t *cache, const H5C_cache_entry_t *parent, const H5C_cache_entry_t *child,
                            herr_t fxn_ret_value)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity checks */
    assert(cache);

    /* Write a log message */
    assert(parent);
    assert(child);
    if (cache->log_info->cls->write_create_fd_log_msg)
        if (cache->log_info->cls->write_create_fd_log_msg(cache->log_info->udata, parent, child,
                                                          fxn_ret_value) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "log-specific create fd call failed");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C_log_write_create_fd_msg() */

/*-------------------------------------------------------------------------
 * Function:    H5C_log_write_protect_entry_msg
 *
 * Purpose:     Write a log message for protecting a cache entry.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C_log_write_protect_entry_msg(H5C_t *cache, const H5C_cache_entry_t *entry, int type_id, unsigned flags,
                                herr_t fxn_ret_value)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity checks */
    assert(cache);

    /* Write a log message */
    assert(entry);
    if (cache->log_info->cls->write_protect_entry_log_msg)
        if (cache->log_info->cls->write_protect_entry_log_msg(cache->log_info->udata, entry, type_id, flags,
                                                              fxn_ret_value) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "log-specific protect entry call failed");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C_log_write_protect_entry_msg() */

/*-------------------------------------------------------------------------
 * Function:    H5C_log_write_resize_entry_msg
 *
 * Purpose:     Write a log message for resizing a cache entry.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C_log_write_resize_entry_msg(H5C_t *cache, const H5C_cache_entry_t *entry, size_t new_size,
                               herr_t fxn_ret_value)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity checks */
    assert(cache);

    /* Write a log message */
    assert(entry);
    if (cache->log_info->cls->write_resize_entry_log_msg)
        if (cache->log_info->cls->write_resize_entry_log_msg(cache->log_info->udata, entry, new_size,
                                                             fxn_ret_value) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "log-specific resize entry call failed");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C_log_write_resize_entry_msg() */

/*-------------------------------------------------------------------------
 * Function:    H5C_log_write_unpin_entry_msg
 *
 * Purpose:     Write a log message for unpinning a cache entry.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C_log_write_unpin_entry_msg(H5C_t *cache, const H5C_cache_entry_t *entry, herr_t fxn_ret_value)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity checks */
    assert(cache);

    /* Write a log message */
    assert(entry);
    if (cache->log_info->cls->write_unpin_entry_log_msg)
        if (cache->log_info->cls->write_unpin_entry_log_msg(cache->log_info->udata, entry, fxn_ret_value) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "log-specific unpin entry call failed");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C_log_write_unpin_entry_msg() */

/*-------------------------------------------------------------------------
 * Function:    H5C_log_write_destroy_fd_msg
 *
 * Purpose:     Write a log message for destroying a flush dependency
 *              between two cache entries.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C_log_write_destroy_fd_msg(H5C_t *cache, const H5C_cache_entry_t *parent, const H5C_cache_entry_t *child,
                             herr_t fxn_ret_value)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity checks */
    assert(cache);

    /* Write a log message */
    assert(parent);
    assert(child);
    if (cache->log_info->cls->write_destroy_fd_log_msg)
        if (cache->log_info->cls->write_destroy_fd_log_msg(cache->log_info->udata, parent, child,
                                                           fxn_ret_value) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "log-specific destroy fd call failed");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C_log_write_destroy_fd_msg() */

/*-------------------------------------------------------------------------
 * Function:    H5C_log_write_unprotect_entry_msg
 *
 * Purpose:     Write a log message for unprotecting a cache entry.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C_log_write_unprotect_entry_msg(H5C_t *cache, haddr_t address, int type_id, unsigned flags,
                                  herr_t fxn_ret_value)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity checks */
    assert(cache);

    /* Write a log message */
    if (cache->log_info->cls->write_unprotect_entry_log_msg)
        if (cache->log_info->cls->write_unprotect_entry_log_msg(cache->log_info->udata, address, type_id,
                                                                flags, fxn_ret_value) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "log-specific unprotect entry call failed");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C_log_write_unprotect_entry_msg() */

/*-------------------------------------------------------------------------
 * Function:    H5C_log_write_set_cache_config_msg
 *
 * Purpose:     Write a log message for setting the cache configuration.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C_log_write_set_cache_config_msg(H5C_t *cache, const H5AC_cache_config_t *config, herr_t fxn_ret_value)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity checks */
    assert(cache);

    /* Write a log message */
    assert(config);
    if (cache->log_info->cls->write_set_cache_config_log_msg)
        if (cache->log_info->cls->write_set_cache_config_log_msg(cache->log_info->udata, config,
                                                                 fxn_ret_value) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "log-specific set cache config call failed");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C_log_write_set_cache_config_msg() */

/*-------------------------------------------------------------------------
 * Function:    H5C_log_write_remove_entry_msg
 *
 * Purpose:     Write a log message for removing a cache entry.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C_log_write_remove_entry_msg(H5C_t *cache, const H5C_cache_entry_t *entry, herr_t fxn_ret_value)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity checks */
    assert(cache);

    /* Write a log message */
    assert(entry);
    if (cache->log_info->cls->write_remove_entry_log_msg)
        if (cache->log_info->cls->write_remove_entry_log_msg(cache->log_info->udata, entry, fxn_ret_value) <
            0)
            HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "log-specific remove entry call failed");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C_log_write_remove_entry_msg() */
