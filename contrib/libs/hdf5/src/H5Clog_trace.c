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
 * Created:     H5Clog_trace.c
 *
 * Purpose:     Cache log implementation that emits trace entries intended
 *              for consumption by a future 'cache replay' feature.
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
#include "H5private.h"   /* Generic Functions                        */
#include "H5Cpkg.h"      /* Cache                                    */
#include "H5Clog.h"      /* Cache logging                            */
#include "H5Eprivate.h"  /* Error handling                           */
#include "H5MMprivate.h" /* Memory management                        */

/****************/
/* Local Macros */
/****************/

/* Max log message size */
#define H5C_MAX_TRACE_LOG_MSG_SIZE 4096

/******************/
/* Local Typedefs */
/******************/

/********************/
/* Package Typedefs */
/********************/

typedef struct H5C_log_trace_udata_t {
    FILE *outfile;
    char *message;
} H5C_log_trace_udata_t;

/********************/
/* Local Prototypes */
/********************/

/* Internal message handling calls */
static herr_t H5C__trace_write_log_message(H5C_log_trace_udata_t *trace_udata);

/* Log message callbacks */
static herr_t H5C__trace_tear_down_logging(H5C_log_info_t *log_info);
static herr_t H5C__trace_write_expunge_entry_log_msg(void *udata, haddr_t address, int type_id,
                                                     herr_t fxn_ret_value);
static herr_t H5C__trace_write_flush_cache_log_msg(void *udata, herr_t fxn_ret_value);
static herr_t H5C__trace_write_insert_entry_log_msg(void *udata, haddr_t address, int type_id, unsigned flags,
                                                    size_t size, herr_t fxn_ret_value);
static herr_t H5C__trace_write_mark_entry_dirty_log_msg(void *udata, const H5C_cache_entry_t *entry,
                                                        herr_t fxn_ret_value);
static herr_t H5C__trace_write_mark_entry_clean_log_msg(void *udata, const H5C_cache_entry_t *entry,
                                                        herr_t fxn_ret_value);
static herr_t H5C__trace_write_mark_unserialized_entry_log_msg(void *udata, const H5C_cache_entry_t *entry,
                                                               herr_t fxn_ret_value);
static herr_t H5C__trace_write_mark_serialized_entry_log_msg(void *udata, const H5C_cache_entry_t *entry,
                                                             herr_t fxn_ret_value);
static herr_t H5C__trace_write_move_entry_log_msg(void *udata, haddr_t old_addr, haddr_t new_addr,
                                                  int type_id, herr_t fxn_ret_value);
static herr_t H5C__trace_write_pin_entry_log_msg(void *udata, const H5C_cache_entry_t *entry,
                                                 herr_t fxn_ret_value);
static herr_t H5C__trace_write_create_fd_log_msg(void *udata, const H5C_cache_entry_t *parent,
                                                 const H5C_cache_entry_t *child, herr_t fxn_ret_value);
static herr_t H5C__trace_write_protect_entry_log_msg(void *udata, const H5C_cache_entry_t *entry, int type_id,
                                                     unsigned flags, herr_t fxn_ret_value);
static herr_t H5C__trace_write_resize_entry_log_msg(void *udata, const H5C_cache_entry_t *entry,
                                                    size_t new_size, herr_t fxn_ret_value);
static herr_t H5C__trace_write_unpin_entry_log_msg(void *udata, const H5C_cache_entry_t *entry,
                                                   herr_t fxn_ret_value);
static herr_t H5C__trace_write_destroy_fd_log_msg(void *udata, const H5C_cache_entry_t *parent,
                                                  const H5C_cache_entry_t *child, herr_t fxn_ret_value);
static herr_t H5C__trace_write_unprotect_entry_log_msg(void *udata, haddr_t address, int type_id,
                                                       unsigned flags, herr_t fxn_ret_value);
static herr_t H5C__trace_write_set_cache_config_log_msg(void *udata, const H5AC_cache_config_t *config,
                                                        herr_t fxn_ret_value);
static herr_t H5C__trace_write_remove_entry_log_msg(void *udata, const H5C_cache_entry_t *entry,
                                                    herr_t fxn_ret_value);

/*********************/
/* Package Variables */
/*********************/

/*****************************/
/* Library Private Variables */
/*****************************/

/*******************/
/* Local Variables */
/*******************/

/* Note that there's no cache set up call since that's the
 * place where this struct is wired into the cache.
 */
static const H5C_log_class_t H5C_trace_log_class_g = {"trace",
                                                      H5C__trace_tear_down_logging,
                                                      NULL, /* start logging */
                                                      NULL, /* stop logging */
                                                      NULL, /* write start message */
                                                      NULL, /* write stop message */
                                                      NULL, /* write create cache message */
                                                      NULL, /* write destroy cache message */
                                                      NULL, /* write evict cache message */
                                                      H5C__trace_write_expunge_entry_log_msg,
                                                      H5C__trace_write_flush_cache_log_msg,
                                                      H5C__trace_write_insert_entry_log_msg,
                                                      H5C__trace_write_mark_entry_dirty_log_msg,
                                                      H5C__trace_write_mark_entry_clean_log_msg,
                                                      H5C__trace_write_mark_unserialized_entry_log_msg,
                                                      H5C__trace_write_mark_serialized_entry_log_msg,
                                                      H5C__trace_write_move_entry_log_msg,
                                                      H5C__trace_write_pin_entry_log_msg,
                                                      H5C__trace_write_create_fd_log_msg,
                                                      H5C__trace_write_protect_entry_log_msg,
                                                      H5C__trace_write_resize_entry_log_msg,
                                                      H5C__trace_write_unpin_entry_log_msg,
                                                      H5C__trace_write_destroy_fd_log_msg,
                                                      H5C__trace_write_unprotect_entry_log_msg,
                                                      H5C__trace_write_set_cache_config_log_msg,
                                                      H5C__trace_write_remove_entry_log_msg};

/*-------------------------------------------------------------------------
 * Function:    H5C__trace_write_log_message
 *
 * Purpose:     Write a message to the log file and flush the file.
 *              The message string is neither modified nor freed.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5C__trace_write_log_message(H5C_log_trace_udata_t *trace_udata)
{
    size_t n_chars;
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(trace_udata);
    assert(trace_udata->outfile);
    assert(trace_udata->message);

    /* Write the log message and flush */
    n_chars = strlen(trace_udata->message);
    if ((int)n_chars != fprintf(trace_udata->outfile, "%s", trace_udata->message))
        HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "error writing log message");
    memset((void *)(trace_udata->message), 0, (size_t)(n_chars * sizeof(char)));

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__trace_write_log_message() */

/*-------------------------------------------------------------------------
 * Function:    H5C__log_trace_set_up
 *
 * Purpose:     Setup for metadata cache logging.
 *
 *              Metadata logging is enabled and disabled at two levels. This
 *              function and the associated tear_down function open and close
 *              the log file. the start_ and stop_logging functions are then
 *              used to switch logging on/off. Optionally, logging can begin
 *              as soon as the log file is opened (set via the start_immediately
 *              parameter to this function).
 *
 *              The log functionality is split between the H5C and H5AC
 *              packages. Log state and direct log manipulation resides in
 *              H5C. Log messages are generated in H5AC and sent to
 *              the H5C__trace_write_log_message function.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C__log_trace_set_up(H5C_log_info_t *log_info, const char log_location[], int mpi_rank)
{
    H5C_log_trace_udata_t *trace_udata = NULL;
    char                  *file_name   = NULL;
    size_t                 n_chars;
    herr_t                 ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(log_info);
    assert(log_location);

    /* Set up the class struct */
    log_info->cls = &H5C_trace_log_class_g;

    /* Allocate memory for the JSON-specific data */
    if (NULL == (log_info->udata = H5MM_calloc(sizeof(H5C_log_trace_udata_t))))
        HGOTO_ERROR(H5E_CACHE, H5E_CANTALLOC, FAIL, "memory allocation failed");
    trace_udata = (H5C_log_trace_udata_t *)(log_info->udata);

    /* Allocate memory for the message buffer */
    if (NULL == (trace_udata->message = (char *)H5MM_calloc(H5C_MAX_TRACE_LOG_MSG_SIZE * sizeof(char))))
        HGOTO_ERROR(H5E_CACHE, H5E_CANTALLOC, FAIL, "memory allocation failed");

    /* Possibly fix up the log file name.
     * The extra 39 characters are for adding the rank to the file name
     * under parallel HDF5. 39 characters allows > 2^127 processes which
     * should be enough for anybody.
     *
     * allocation size = <path length> + dot + <rank # length> + \0
     */
    n_chars = strlen(log_location) + 1 + 39 + 1;
    if (NULL == (file_name = (char *)H5MM_calloc(n_chars * sizeof(char))))
        HGOTO_ERROR(H5E_CACHE, H5E_CANTALLOC, FAIL,
                    "can't allocate memory for mdc log file name manipulation");

    /* Add the rank to the log file name when MPI is in use */
    if (-1 == mpi_rank)
        snprintf(file_name, n_chars, "%s", log_location);
    else
        snprintf(file_name, n_chars, "%s.%d", log_location, mpi_rank);

    /* Open log file and set it to be unbuffered */
    if (NULL == (trace_udata->outfile = fopen(file_name, "w")))
        HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "can't create mdc log file");
    HDsetbuf(trace_udata->outfile, NULL);

    /* Write the header */
    fprintf(trace_udata->outfile, "### HDF5 metadata cache trace file version 1 ###\n");

done:
    if (file_name)
        H5MM_xfree(file_name);

    /* Free and reset the log info struct on errors */
    if (FAIL == ret_value) {
        /* Free */
        if (trace_udata && trace_udata->message)
            H5MM_xfree(trace_udata->message);
        if (trace_udata)
            H5MM_xfree(trace_udata);

        /* Reset */
        log_info->udata = NULL;
        log_info->cls   = NULL;
    }

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__log_trace_set_up() */

/*-------------------------------------------------------------------------
 * Function:    H5C__trace_tear_down_logging
 *
 * Purpose:     Tear-down for metadata cache logging.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5C__trace_tear_down_logging(H5C_log_info_t *log_info)
{
    H5C_log_trace_udata_t *trace_udata = NULL;
    herr_t                 ret_value   = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(log_info);

    /* Alias */
    trace_udata = (H5C_log_trace_udata_t *)(log_info->udata);

    /* Free the message buffer */
    H5MM_xfree(trace_udata->message);

    /* Close log file */
    if (EOF == fclose(trace_udata->outfile))
        HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "problem closing mdc log file");
    trace_udata->outfile = NULL;

    /* Fre the udata */
    H5MM_xfree(trace_udata);

    /* Reset the log class info and udata */
    log_info->cls   = NULL;
    log_info->udata = NULL;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__trace_tear_down_logging() */

/*-------------------------------------------------------------------------
 * Function:    H5C__trace_write_expunge_entry_log_msg
 *
 * Purpose:     Write a log message for expunge of cache entries.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5C__trace_write_expunge_entry_log_msg(void *udata, haddr_t address, int type_id, herr_t fxn_ret_value)
{
    H5C_log_trace_udata_t *trace_udata = (H5C_log_trace_udata_t *)(udata);
    herr_t                 ret_value   = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(trace_udata);
    assert(trace_udata->message);

    /* Create the log message string */
    snprintf(trace_udata->message, H5C_MAX_TRACE_LOG_MSG_SIZE, "H5AC_expunge_entry 0x%lx %d %d\n",
             (unsigned long)address, type_id, (int)fxn_ret_value);

    /* Write the log message to the file */
    if (H5C__trace_write_log_message(trace_udata) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "unable to emit log message");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__trace_write_expunge_entry_log_msg() */

/*-------------------------------------------------------------------------
 * Function:    H5C__trace_write_flush_cache_log_msg
 *
 * Purpose:     Write a log message for cache flushes.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5C__trace_write_flush_cache_log_msg(void *udata, herr_t fxn_ret_value)
{
    H5C_log_trace_udata_t *trace_udata = (H5C_log_trace_udata_t *)(udata);
    herr_t                 ret_value   = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(trace_udata);
    assert(trace_udata->message);

    /* Create the log message string */
    snprintf(trace_udata->message, H5C_MAX_TRACE_LOG_MSG_SIZE, "H5AC_flush %d\n", (int)fxn_ret_value);

    /* Write the log message to the file */
    if (H5C__trace_write_log_message(trace_udata) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "unable to emit log message");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__trace_write_flush_cache_log_msg() */

/*-------------------------------------------------------------------------
 * Function:    H5C__trace_write_insert_entry_log_msg
 *
 * Purpose:     Write a log message for insertion of cache entries.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5C__trace_write_insert_entry_log_msg(void *udata, haddr_t address, int type_id, unsigned flags, size_t size,
                                      herr_t fxn_ret_value)
{
    H5C_log_trace_udata_t *trace_udata = (H5C_log_trace_udata_t *)(udata);
    herr_t                 ret_value   = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(trace_udata);
    assert(trace_udata->message);

    /* Create the log message string */
    snprintf(trace_udata->message, H5C_MAX_TRACE_LOG_MSG_SIZE, "H5AC_insert_entry 0x%lx %d 0x%x %d %d\n",
             (unsigned long)address, type_id, flags, (int)size, (int)fxn_ret_value);

    /* Write the log message to the file */
    if (H5C__trace_write_log_message(trace_udata) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "unable to emit log message");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__trace_write_insert_entry_log_msg() */

/*-------------------------------------------------------------------------
 * Function:    H5C__trace_write_mark_entry_dirty_log_msg
 *
 * Purpose:     Write a log message for marking cache entries as dirty.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5C__trace_write_mark_entry_dirty_log_msg(void *udata, const H5C_cache_entry_t *entry, herr_t fxn_ret_value)
{
    H5C_log_trace_udata_t *trace_udata = (H5C_log_trace_udata_t *)(udata);
    herr_t                 ret_value   = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(trace_udata);
    assert(trace_udata->message);
    assert(entry);

    /* Create the log message string */
    snprintf(trace_udata->message, H5C_MAX_TRACE_LOG_MSG_SIZE, "H5AC_mark_entry_dirty 0x%lx %d\n",
             (unsigned long)(entry->addr), (int)fxn_ret_value);

    /* Write the log message to the file */
    if (H5C__trace_write_log_message(trace_udata) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "unable to emit log message");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__trace_write_mark_entry_dirty_log_msg() */

/*-------------------------------------------------------------------------
 * Function:    H5C__trace_write_mark_entry_clean_log_msg
 *
 * Purpose:     Write a log message for marking cache entries as clean.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5C__trace_write_mark_entry_clean_log_msg(void *udata, const H5C_cache_entry_t *entry, herr_t fxn_ret_value)
{
    H5C_log_trace_udata_t *trace_udata = (H5C_log_trace_udata_t *)(udata);
    herr_t                 ret_value   = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(trace_udata);
    assert(trace_udata->message);
    assert(entry);

    /* Create the log message string */
    snprintf(trace_udata->message, H5C_MAX_TRACE_LOG_MSG_SIZE, "H5AC_mark_entry_clean 0x%lx %d\n",
             (unsigned long)(entry->addr), (int)fxn_ret_value);

    /* Write the log message to the file */
    if (H5C__trace_write_log_message(trace_udata) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "unable to emit log message");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__trace_write_mark_entry_clean_log_msg() */

/*-------------------------------------------------------------------------
 * Function:    H5C__trace_write_mark_unserialized_entry_log_msg
 *
 * Purpose:     Write a log message for marking cache entries as unserialized.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5C__trace_write_mark_unserialized_entry_log_msg(void *udata, const H5C_cache_entry_t *entry,
                                                 herr_t fxn_ret_value)
{
    H5C_log_trace_udata_t *trace_udata = (H5C_log_trace_udata_t *)(udata);
    herr_t                 ret_value   = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(trace_udata);
    assert(trace_udata->message);
    assert(entry);

    /* Create the log message string */
    snprintf(trace_udata->message, H5C_MAX_TRACE_LOG_MSG_SIZE, "H5AC_mark_entry_unserialized 0x%lx %d\n",
             (unsigned long)(entry->addr), (int)fxn_ret_value);

    /* Write the log message to the file */
    if (H5C__trace_write_log_message(trace_udata) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "unable to emit log message");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__trace_write_mark_unserialized_entry_log_msg() */

/*-------------------------------------------------------------------------
 * Function:    H5C__trace_write_mark_serialized_entry_log_msg
 *
 * Purpose:     Write a log message for marking cache entries as serialize.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5C__trace_write_mark_serialized_entry_log_msg(void *udata, const H5C_cache_entry_t *entry,
                                               herr_t fxn_ret_value)
{
    H5C_log_trace_udata_t *trace_udata = (H5C_log_trace_udata_t *)(udata);
    herr_t                 ret_value   = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(trace_udata);
    assert(trace_udata->message);
    assert(entry);

    /* Create the log message string */
    snprintf(trace_udata->message, H5C_MAX_TRACE_LOG_MSG_SIZE, "H5AC_mark_entry_serialized 0x%lx %d\n",
             (unsigned long)(entry->addr), (int)fxn_ret_value);

    /* Write the log message to the file */
    if (H5C__trace_write_log_message(trace_udata) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "unable to emit log message");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__trace_write_mark_serialized_entry_log_msg() */

/*-------------------------------------------------------------------------
 * Function:    H5C__trace_write_move_entry_log_msg
 *
 * Purpose:     Write a log message for moving a cache entry.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5C__trace_write_move_entry_log_msg(void *udata, haddr_t old_addr, haddr_t new_addr, int type_id,
                                    herr_t fxn_ret_value)
{
    H5C_log_trace_udata_t *trace_udata = (H5C_log_trace_udata_t *)(udata);
    herr_t                 ret_value   = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(trace_udata);
    assert(trace_udata->message);

    /* Create the log message string */
    snprintf(trace_udata->message, H5C_MAX_TRACE_LOG_MSG_SIZE, "H5AC_move_entry 0x%lx 0x%lx %d %d\n",
             (unsigned long)old_addr, (unsigned long)new_addr, type_id, (int)fxn_ret_value);

    /* Write the log message to the file */
    if (H5C__trace_write_log_message(trace_udata) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "unable to emit log message");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__trace_write_move_entry_log_msg() */

/*-------------------------------------------------------------------------
 * Function:    H5C__trace_write_pin_entry_log_msg
 *
 * Purpose:     Write a log message for pinning a cache entry.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5C__trace_write_pin_entry_log_msg(void *udata, const H5C_cache_entry_t *entry, herr_t fxn_ret_value)
{
    H5C_log_trace_udata_t *trace_udata = (H5C_log_trace_udata_t *)(udata);
    herr_t                 ret_value   = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(trace_udata);
    assert(trace_udata->message);
    assert(entry);

    /* Create the log message string */
    snprintf(trace_udata->message, H5C_MAX_TRACE_LOG_MSG_SIZE, "H5AC_pin_protected_entry 0x%lx %d\n",
             (unsigned long)(entry->addr), (int)fxn_ret_value);

    /* Write the log message to the file */
    if (H5C__trace_write_log_message(trace_udata) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "unable to emit log message");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__trace_write_pin_entry_log_msg() */

/*-------------------------------------------------------------------------
 * Function:    H5C__trace_write_create_fd_log_msg
 *
 * Purpose:     Write a log message for creating a flush dependency between
 *              two cache entries.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5C__trace_write_create_fd_log_msg(void *udata, const H5C_cache_entry_t *parent,
                                   const H5C_cache_entry_t *child, herr_t fxn_ret_value)
{
    H5C_log_trace_udata_t *trace_udata = (H5C_log_trace_udata_t *)(udata);
    herr_t                 ret_value   = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(trace_udata);
    assert(trace_udata->message);
    assert(parent);
    assert(child);

    /* Create the log message string */
    snprintf(trace_udata->message, H5C_MAX_TRACE_LOG_MSG_SIZE,
             "H5AC_create_flush_dependency 0x%lx 0x%lx %d\n", (unsigned long)(parent->addr),
             (unsigned long)(child->addr), (int)fxn_ret_value);

    /* Write the log message to the file */
    if (H5C__trace_write_log_message(trace_udata) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "unable to emit log message");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__trace_write_create_fd_log_msg() */

/*-------------------------------------------------------------------------
 * Function:    H5C__trace_write_protect_entry_log_msg
 *
 * Purpose:     Write a log message for protecting a cache entry.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5C__trace_write_protect_entry_log_msg(void *udata, const H5C_cache_entry_t *entry, int type_id,
                                       unsigned flags, herr_t fxn_ret_value)
{
    H5C_log_trace_udata_t *trace_udata = (H5C_log_trace_udata_t *)(udata);
    herr_t                 ret_value   = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(trace_udata);
    assert(trace_udata->message);
    assert(entry);

    /* Create the log message string */
    snprintf(trace_udata->message, H5C_MAX_TRACE_LOG_MSG_SIZE, "H5AC_protect 0x%lx %d 0x%x %d %d\n",
             (unsigned long)(entry->addr), type_id, flags, (int)(entry->size), (int)fxn_ret_value);

    /* Write the log message to the file */
    if (H5C__trace_write_log_message(trace_udata) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "unable to emit log message");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__trace_write_protect_entry_log_msg() */

/*-------------------------------------------------------------------------
 * Function:    H5C__trace_write_resize_entry_log_msg
 *
 * Purpose:     Write a log message for resizing a cache entry.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5C__trace_write_resize_entry_log_msg(void *udata, const H5C_cache_entry_t *entry, size_t new_size,
                                      herr_t fxn_ret_value)
{
    H5C_log_trace_udata_t *trace_udata = (H5C_log_trace_udata_t *)(udata);
    herr_t                 ret_value   = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(trace_udata);
    assert(trace_udata->message);
    assert(entry);

    /* Create the log message string */
    snprintf(trace_udata->message, H5C_MAX_TRACE_LOG_MSG_SIZE, "H5AC_resize_entry 0x%lx %d %d\n",
             (unsigned long)(entry->addr), (int)new_size, (int)fxn_ret_value);

    /* Write the log message to the file */
    if (H5C__trace_write_log_message(trace_udata) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "unable to emit log message");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__trace_write_resize_entry_log_msg() */

/*-------------------------------------------------------------------------
 * Function:    H5C__trace_write_unpin_entry_log_msg
 *
 * Purpose:     Write a log message for unpinning a cache entry.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5C__trace_write_unpin_entry_log_msg(void *udata, const H5C_cache_entry_t *entry, herr_t fxn_ret_value)
{
    H5C_log_trace_udata_t *trace_udata = (H5C_log_trace_udata_t *)(udata);
    herr_t                 ret_value   = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(trace_udata);
    assert(trace_udata->message);
    assert(entry);

    /* Create the log message string */
    snprintf(trace_udata->message, H5C_MAX_TRACE_LOG_MSG_SIZE, "H5AC_unpin_entry 0x%lx %d\n",
             (unsigned long)(entry->addr), (int)fxn_ret_value);

    /* Write the log message to the file */
    if (H5C__trace_write_log_message(trace_udata) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "unable to emit log message");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__trace_write_unpin_entry_log_msg() */

/*-------------------------------------------------------------------------
 * Function:    H5C__trace_write_destroy_fd_log_msg
 *
 * Purpose:     Write a log message for destroying a flush dependency
 *              between two cache entries.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5C__trace_write_destroy_fd_log_msg(void *udata, const H5C_cache_entry_t *parent,
                                    const H5C_cache_entry_t *child, herr_t fxn_ret_value)
{
    H5C_log_trace_udata_t *trace_udata = (H5C_log_trace_udata_t *)(udata);
    herr_t                 ret_value   = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(trace_udata);
    assert(trace_udata->message);
    assert(parent);
    assert(child);

    /* Create the log message string */
    snprintf(trace_udata->message, H5C_MAX_TRACE_LOG_MSG_SIZE,
             "H5AC_destroy_flush_dependency 0x%lx 0x%lx %d\n", (unsigned long)(parent->addr),
             (unsigned long)(child->addr), (int)fxn_ret_value);

    /* Write the log message to the file */
    if (H5C__trace_write_log_message(trace_udata) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "unable to emit log message");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__trace_write_destroy_fd_log_msg() */

/*-------------------------------------------------------------------------
 * Function:    H5C__trace_write_unprotect_entry_log_msg
 *
 * Purpose:     Write a log message for unprotecting a cache entry.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5C__trace_write_unprotect_entry_log_msg(void *udata, haddr_t address, int type_id, unsigned flags,
                                         herr_t fxn_ret_value)
{
    H5C_log_trace_udata_t *trace_udata = (H5C_log_trace_udata_t *)(udata);
    herr_t                 ret_value   = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(trace_udata);
    assert(trace_udata->message);

    /* Create the log message string */
    snprintf(trace_udata->message, H5C_MAX_TRACE_LOG_MSG_SIZE, "H5AC_unprotect 0x%lx %d 0x%x %d\n",
             (unsigned long)(address), type_id, flags, (int)fxn_ret_value);

    /* Write the log message to the file */
    if (H5C__trace_write_log_message(trace_udata) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "unable to emit log message");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__trace_write_unprotect_entry_log_msg() */

/*-------------------------------------------------------------------------
 * Function:    H5C__trace_write_set_cache_config_log_msg
 *
 * Purpose:     Write a log message for setting the cache configuration.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5C__trace_write_set_cache_config_log_msg(void *udata, const H5AC_cache_config_t *config,
                                          herr_t fxn_ret_value)
{
    H5C_log_trace_udata_t *trace_udata = (H5C_log_trace_udata_t *)(udata);
    herr_t                 ret_value   = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(trace_udata);
    assert(trace_udata->message);
    assert(config);

    /* Create the log message string */
    snprintf(trace_udata->message, H5C_MAX_TRACE_LOG_MSG_SIZE,
             "H5AC_set_cache_auto_resize_config %d %d %d %d \"%s\" %d %d %d %f %d %d %ld %d %f %f %d %f %f "
             "%d %d %d %f %f %d %d %d %d %f %zu %d %d\n",
             config->version, (int)(config->rpt_fcn_enabled), (int)(config->open_trace_file),
             (int)(config->close_trace_file), config->trace_file_name, (int)(config->evictions_enabled),
             (int)(config->set_initial_size), (int)(config->initial_size), config->min_clean_fraction,
             (int)(config->max_size), (int)(config->min_size), config->epoch_length, (int)(config->incr_mode),
             config->lower_hr_threshold, config->increment, (int)(config->flash_incr_mode),
             config->flash_multiple, config->flash_threshold, (int)(config->apply_max_increment),
             (int)(config->max_increment), (int)(config->decr_mode), config->upper_hr_threshold,
             config->decrement, (int)(config->apply_max_decrement), (int)(config->max_decrement),
             config->epochs_before_eviction, (int)(config->apply_empty_reserve), config->empty_reserve,
             config->dirty_bytes_threshold, config->metadata_write_strategy, (int)fxn_ret_value);

    /* Write the log message to the file */
    if (H5C__trace_write_log_message(trace_udata) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "unable to emit log message");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__trace_write_set_cache_config_log_msg() */

/*-------------------------------------------------------------------------
 * Function:    H5C__trace_write_remove_entry_log_msg
 *
 * Purpose:     Write a log message for removing a cache entry.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5C__trace_write_remove_entry_log_msg(void *udata, const H5C_cache_entry_t *entry, herr_t fxn_ret_value)
{
    H5C_log_trace_udata_t *trace_udata = (H5C_log_trace_udata_t *)(udata);
    herr_t                 ret_value   = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(trace_udata);
    assert(trace_udata->message);
    assert(entry);

    /* Create the log message string */
    snprintf(trace_udata->message, H5C_MAX_TRACE_LOG_MSG_SIZE, "H5AC_remove_entry 0x%lx %d\n",
             (unsigned long)(entry->addr), (int)fxn_ret_value);

    /* Write the log message to the file */
    if (H5C__trace_write_log_message(trace_udata) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "unable to emit log message");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__trace_write_remove_entry_log_msg() */
