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
 * Created:     H5Clog_json.c
 *
 * Purpose:     Cache log implementation that emits JSON-formatted log
 *              entries for consumption by new-fangled data analysis tools.
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
#define H5C_MAX_JSON_LOG_MSG_SIZE 1024

/******************/
/* Local Typedefs */
/******************/

/********************/
/* Package Typedefs */
/********************/

typedef struct H5C_log_json_udata_t {
    FILE *outfile;
    char *message;
} H5C_log_json_udata_t;

/********************/
/* Local Prototypes */
/********************/

/* Internal message handling calls */
static herr_t H5C__json_write_log_message(H5C_log_json_udata_t *json_udata);

/* Log message callbacks */
static herr_t H5C__json_tear_down_logging(H5C_log_info_t *log_info);
static herr_t H5C__json_write_start_log_msg(void *udata);
static herr_t H5C__json_write_stop_log_msg(void *udata);
static herr_t H5C__json_write_create_cache_log_msg(void *udata, herr_t fxn_ret_value);
static herr_t H5C__json_write_destroy_cache_log_msg(void *udata);
static herr_t H5C__json_write_evict_cache_log_msg(void *udata, herr_t fxn_ret_value);
static herr_t H5C__json_write_expunge_entry_log_msg(void *udata, haddr_t address, int type_id,
                                                    herr_t fxn_ret_value);
static herr_t H5C__json_write_flush_cache_log_msg(void *udata, herr_t fxn_ret_value);
static herr_t H5C__json_write_insert_entry_log_msg(void *udata, haddr_t address, int type_id, unsigned flags,
                                                   size_t size, herr_t fxn_ret_value);
static herr_t H5C__json_write_mark_entry_dirty_log_msg(void *udata, const H5C_cache_entry_t *entry,
                                                       herr_t fxn_ret_value);
static herr_t H5C__json_write_mark_entry_clean_log_msg(void *udata, const H5C_cache_entry_t *entry,
                                                       herr_t fxn_ret_value);
static herr_t H5C__json_write_mark_unserialized_entry_log_msg(void *udata, const H5C_cache_entry_t *entry,
                                                              herr_t fxn_ret_value);
static herr_t H5C__json_write_mark_serialized_entry_log_msg(void *udata, const H5C_cache_entry_t *entry,
                                                            herr_t fxn_ret_value);
static herr_t H5C__json_write_move_entry_log_msg(void *udata, haddr_t old_addr, haddr_t new_addr, int type_id,
                                                 herr_t fxn_ret_value);
static herr_t H5C__json_write_pin_entry_log_msg(void *udata, const H5C_cache_entry_t *entry,
                                                herr_t fxn_ret_value);
static herr_t H5C__json_write_create_fd_log_msg(void *udata, const H5C_cache_entry_t *parent,
                                                const H5C_cache_entry_t *child, herr_t fxn_ret_value);
static herr_t H5C__json_write_protect_entry_log_msg(void *udata, const H5C_cache_entry_t *entry, int type_id,
                                                    unsigned flags, herr_t fxn_ret_value);
static herr_t H5C__json_write_resize_entry_log_msg(void *udata, const H5C_cache_entry_t *entry,
                                                   size_t new_size, herr_t fxn_ret_value);
static herr_t H5C__json_write_unpin_entry_log_msg(void *udata, const H5C_cache_entry_t *entry,
                                                  herr_t fxn_ret_value);
static herr_t H5C__json_write_destroy_fd_log_msg(void *udata, const H5C_cache_entry_t *parent,
                                                 const H5C_cache_entry_t *child, herr_t fxn_ret_value);
static herr_t H5C__json_write_unprotect_entry_log_msg(void *udata, haddr_t address, int type_id,
                                                      unsigned flags, herr_t fxn_ret_value);
static herr_t H5C__json_write_set_cache_config_log_msg(void *udata, const H5AC_cache_config_t *config,
                                                       herr_t fxn_ret_value);
static herr_t H5C__json_write_remove_entry_log_msg(void *udata, const H5C_cache_entry_t *entry,
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
static const H5C_log_class_t H5C_json_log_class_g = {"json",
                                                     H5C__json_tear_down_logging,
                                                     NULL, /* start logging */
                                                     NULL, /* stop logging */
                                                     H5C__json_write_start_log_msg,
                                                     H5C__json_write_stop_log_msg,
                                                     H5C__json_write_create_cache_log_msg,
                                                     H5C__json_write_destroy_cache_log_msg,
                                                     H5C__json_write_evict_cache_log_msg,
                                                     H5C__json_write_expunge_entry_log_msg,
                                                     H5C__json_write_flush_cache_log_msg,
                                                     H5C__json_write_insert_entry_log_msg,
                                                     H5C__json_write_mark_entry_dirty_log_msg,
                                                     H5C__json_write_mark_entry_clean_log_msg,
                                                     H5C__json_write_mark_unserialized_entry_log_msg,
                                                     H5C__json_write_mark_serialized_entry_log_msg,
                                                     H5C__json_write_move_entry_log_msg,
                                                     H5C__json_write_pin_entry_log_msg,
                                                     H5C__json_write_create_fd_log_msg,
                                                     H5C__json_write_protect_entry_log_msg,
                                                     H5C__json_write_resize_entry_log_msg,
                                                     H5C__json_write_unpin_entry_log_msg,
                                                     H5C__json_write_destroy_fd_log_msg,
                                                     H5C__json_write_unprotect_entry_log_msg,
                                                     H5C__json_write_set_cache_config_log_msg,
                                                     H5C__json_write_remove_entry_log_msg};

/*-------------------------------------------------------------------------
 * Function:    H5C__json_write_log_message
 *
 * Purpose:     Write a message to the log file and flush the file.
 *              The message string is neither modified nor freed.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5C__json_write_log_message(H5C_log_json_udata_t *json_udata)
{
    size_t n_chars;
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(json_udata);
    assert(json_udata->outfile);
    assert(json_udata->message);

    /* Write the log message and flush */
    n_chars = strlen(json_udata->message);
    if ((int)n_chars != fprintf(json_udata->outfile, "%s", json_udata->message))
        HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "error writing log message");
    memset((void *)(json_udata->message), 0, (size_t)(n_chars * sizeof(char)));

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__json_write_log_message() */

/*-------------------------------------------------------------------------
 * Function:    H5C__log_json_set_up
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
 *              the H5C__json_write_log_message function.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C__log_json_set_up(H5C_log_info_t *log_info, const char log_location[], int mpi_rank)
{
    H5C_log_json_udata_t *json_udata = NULL;
    char                 *file_name  = NULL;
    size_t                n_chars;
    herr_t                ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(log_info);
    assert(log_location);

    /* Set up the class struct */
    log_info->cls = &H5C_json_log_class_g;

    /* Allocate memory for the JSON-specific data */
    if (NULL == (log_info->udata = H5MM_calloc(sizeof(H5C_log_json_udata_t))))
        HGOTO_ERROR(H5E_CACHE, H5E_CANTALLOC, FAIL, "memory allocation failed");
    json_udata = (H5C_log_json_udata_t *)(log_info->udata);

    /* Allocate memory for the message buffer */
    if (NULL == (json_udata->message = (char *)H5MM_calloc(H5C_MAX_JSON_LOG_MSG_SIZE * sizeof(char))))
        HGOTO_ERROR(H5E_CACHE, H5E_CANTALLOC, FAIL, "memory allocation failed");

    /* Possibly fix up the log file name.
     * The extra 39 characters are for adding the rank to the file name
     * under parallel HDF5. 39 characters allows > 2^127 processes which
     * should be enough for anybody.
     *
     * allocation size = "RANK_" + <rank # length> + dot + <path length> + \0
     */
    n_chars = 5 + 39 + 1 + strlen(log_location) + 1;
    if (NULL == (file_name = (char *)H5MM_calloc(n_chars * sizeof(char))))
        HGOTO_ERROR(H5E_CACHE, H5E_CANTALLOC, FAIL,
                    "can't allocate memory for mdc log file name manipulation");

    /* Add the rank to the log file name when MPI is in use */
    if (-1 == mpi_rank)
        snprintf(file_name, n_chars, "%s", log_location);
    else
        snprintf(file_name, n_chars, "RANK_%d.%s", mpi_rank, log_location);

    /* Open log file and set it to be unbuffered */
    if (NULL == (json_udata->outfile = fopen(file_name, "w")))
        HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "can't create mdc log file");
    HDsetbuf(json_udata->outfile, NULL);

done:
    if (file_name)
        H5MM_xfree(file_name);

    /* Free and reset the log info struct on errors */
    if (FAIL == ret_value) {
        /* Free */
        if (json_udata && json_udata->message)
            H5MM_xfree(json_udata->message);
        if (json_udata)
            H5MM_xfree(json_udata);

        /* Reset */
        log_info->udata = NULL;
        log_info->cls   = NULL;
    }

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__log_json_set_up() */

/*-------------------------------------------------------------------------
 * Function:    H5C__json_tear_down_logging
 *
 * Purpose:     Tear-down for metadata cache logging.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5C__json_tear_down_logging(H5C_log_info_t *log_info)
{
    H5C_log_json_udata_t *json_udata = NULL;
    herr_t                ret_value  = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(log_info);

    /* Alias */
    json_udata = (H5C_log_json_udata_t *)(log_info->udata);

    /* Free the message buffer */
    H5MM_xfree(json_udata->message);

    /* Close log file */
    if (EOF == fclose(json_udata->outfile))
        HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "problem closing mdc log file");
    json_udata->outfile = NULL;

    /* Fre the udata */
    H5MM_xfree(json_udata);

    /* Reset the log class info and udata */
    log_info->cls   = NULL;
    log_info->udata = NULL;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__json_tear_down_logging() */

/*-------------------------------------------------------------------------
 * Function:    H5C__json_write_start_log_msg
 *
 * Purpose:     Write a log message when logging starts.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5C__json_write_start_log_msg(void *udata)
{
    H5C_log_json_udata_t *json_udata = (H5C_log_json_udata_t *)(udata);
    herr_t                ret_value  = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(json_udata);
    assert(json_udata->message);

    /* Create the log message string (opens the JSON array) */
    snprintf(json_udata->message, H5C_MAX_JSON_LOG_MSG_SIZE, "\
{\n\
\"HDF5 metadata cache log messages\" : [\n\
{\
\"timestamp\":%lld,\
\"action\":\"logging start\"\
},\n\
",
             (long long)HDtime(NULL));

    /* Write the log message to the file */
    if (H5C__json_write_log_message(json_udata) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "unable to emit log message");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__json_write_start_log_msg() */

/*-------------------------------------------------------------------------
 * Function:    H5C__json_write_stop_log_msg
 *
 * Purpose:     Write a log message when logging ends.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5C__json_write_stop_log_msg(void *udata)
{
    H5C_log_json_udata_t *json_udata = (H5C_log_json_udata_t *)(udata);
    herr_t                ret_value  = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(json_udata);
    assert(json_udata->message);

    /* Create the log message string (closes the JSON array) */
    snprintf(json_udata->message, H5C_MAX_JSON_LOG_MSG_SIZE, "\
{\
\"timestamp\":%lld,\
\"action\":\"logging stop\"\
}\n\
]}\n\
",
             (long long)HDtime(NULL));

    /* Write the log message to the file */
    if (H5C__json_write_log_message(json_udata) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "unable to emit log message");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__json_write_stop_log_msg() */

/*-------------------------------------------------------------------------
 * Function:    H5C__json_write_create_cache_log_msg
 *
 * Purpose:     Write a log message for cache creation.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5C__json_write_create_cache_log_msg(void *udata, herr_t fxn_ret_value)
{
    H5C_log_json_udata_t *json_udata = (H5C_log_json_udata_t *)(udata);
    herr_t                ret_value  = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(json_udata);
    assert(json_udata->message);

    /* Create the log message string */
    snprintf(json_udata->message, H5C_MAX_JSON_LOG_MSG_SIZE, "\
{\
\"timestamp\":%lld,\
\"action\":\"create\",\
\"returned\":%d\
},\n\
",
             (long long)HDtime(NULL), (int)fxn_ret_value);

    /* Write the log message to the file */
    if (H5C__json_write_log_message(json_udata) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "unable to emit log message");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__json_write_create_cache_log_msg() */

/*-------------------------------------------------------------------------
 * Function:    H5C__json_write_destroy_cache_log_msg
 *
 * Purpose:     Write a log message for cache destruction.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5C__json_write_destroy_cache_log_msg(void *udata)
{
    H5C_log_json_udata_t *json_udata = (H5C_log_json_udata_t *)(udata);
    herr_t                ret_value  = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(json_udata);
    assert(json_udata->message);

    /* Create the log message string */
    snprintf(json_udata->message, H5C_MAX_JSON_LOG_MSG_SIZE, "\
{\
\"timestamp\":%lld,\
\"action\":\"destroy\"\
},\n\
",
             (long long)HDtime(NULL));

    /* Write the log message to the file */
    if (H5C__json_write_log_message(json_udata) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "unable to emit log message");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__json_write_destroy_cache_log_msg() */

/*-------------------------------------------------------------------------
 * Function:    H5C__json_write_evict_cache_log_msg
 *
 * Purpose:     Write a log message for eviction of cache entries.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5C__json_write_evict_cache_log_msg(void *udata, herr_t fxn_ret_value)
{
    H5C_log_json_udata_t *json_udata = (H5C_log_json_udata_t *)(udata);
    herr_t                ret_value  = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(json_udata);
    assert(json_udata->message);

    /* Create the log message string */
    snprintf(json_udata->message, H5C_MAX_JSON_LOG_MSG_SIZE, "\
{\
\"timestamp\":%lld,\
\"action\":\"evict\",\
\"returned\":%d\
},\n\
",
             (long long)HDtime(NULL), (int)fxn_ret_value);

    /* Write the log message to the file */
    if (H5C__json_write_log_message(json_udata) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "unable to emit log message");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__json_write_evict_cache_log_msg() */

/*-------------------------------------------------------------------------
 * Function:    H5C__json_write_expunge_entry_log_msg
 *
 * Purpose:     Write a log message for expunge of cache entries.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5C__json_write_expunge_entry_log_msg(void *udata, haddr_t address, int type_id, herr_t fxn_ret_value)
{
    H5C_log_json_udata_t *json_udata = (H5C_log_json_udata_t *)(udata);
    herr_t                ret_value  = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(json_udata);
    assert(json_udata->message);

    /* Create the log message string */
    snprintf(json_udata->message, H5C_MAX_JSON_LOG_MSG_SIZE, "\
{\
\"timestamp\":%lld,\
\"action\":\"expunge\",\
\"address\":0x%lx,\
\"type_id\":%d,\
\"returned\":%d\
},\n\
",
             (long long)HDtime(NULL), (unsigned long)address, (int)type_id, (int)fxn_ret_value);

    /* Write the log message to the file */
    if (H5C__json_write_log_message(json_udata) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "unable to emit log message");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__json_write_expunge_entry_log_msg() */

/*-------------------------------------------------------------------------
 * Function:    H5C__json_write_flush_cache_log_msg
 *
 * Purpose:     Write a log message for cache flushes.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5C__json_write_flush_cache_log_msg(void *udata, herr_t fxn_ret_value)
{
    H5C_log_json_udata_t *json_udata = (H5C_log_json_udata_t *)(udata);
    herr_t                ret_value  = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(json_udata);
    assert(json_udata->message);

    /* Create the log message string */
    snprintf(json_udata->message, H5C_MAX_JSON_LOG_MSG_SIZE, "\
{\
\"timestamp\":%lld,\
\"action\":\"flush\",\
\"returned\":%d\
},\n\
",
             (long long)HDtime(NULL), (int)fxn_ret_value);

    /* Write the log message to the file */
    if (H5C__json_write_log_message(json_udata) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "unable to emit log message");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__json_write_flush_cache_log_msg() */

/*-------------------------------------------------------------------------
 * Function:    H5C__json_write_insert_entry_log_msg
 *
 * Purpose:     Write a log message for insertion of cache entries.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5C__json_write_insert_entry_log_msg(void *udata, haddr_t address, int type_id, unsigned flags, size_t size,
                                     herr_t fxn_ret_value)
{
    H5C_log_json_udata_t *json_udata = (H5C_log_json_udata_t *)(udata);
    herr_t                ret_value  = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(json_udata);
    assert(json_udata->message);

    /* Create the log message string */
    snprintf(json_udata->message, H5C_MAX_JSON_LOG_MSG_SIZE, "\
{\
\"timestamp\":%lld,\
\"action\":\"insert\",\
\"address\":0x%lx,\
\"type_id\":%d,\
\"flags\":0x%x,\
\"size\":%d,\
\"returned\":%d\
},\n\
",
             (long long)HDtime(NULL), (unsigned long)address, type_id, flags, (int)size, (int)fxn_ret_value);

    /* Write the log message to the file */
    if (H5C__json_write_log_message(json_udata) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "unable to emit log message");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__json_write_insert_entry_log_msg() */

/*-------------------------------------------------------------------------
 * Function:    H5C__json_write_mark_entry_dirty_log_msg
 *
 * Purpose:     Write a log message for marking cache entries as dirty.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5C__json_write_mark_entry_dirty_log_msg(void *udata, const H5C_cache_entry_t *entry, herr_t fxn_ret_value)
{
    H5C_log_json_udata_t *json_udata = (H5C_log_json_udata_t *)(udata);
    herr_t                ret_value  = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(json_udata);
    assert(json_udata->message);
    assert(entry);

    /* Create the log message string */
    snprintf(json_udata->message, H5C_MAX_JSON_LOG_MSG_SIZE, "\
{\
\"timestamp\":%lld,\
\"action\":\"dirty\",\
\"address\":0x%lx,\
\"returned\":%d\
},\n\
",
             (long long)HDtime(NULL), (unsigned long)entry->addr, (int)fxn_ret_value);

    /* Write the log message to the file */
    if (H5C__json_write_log_message(json_udata) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "unable to emit log message");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__json_write_mark_entry_dirty_log_msg() */

/*-------------------------------------------------------------------------
 * Function:    H5C__json_write_mark_entry_clean_log_msg
 *
 * Purpose:     Write a log message for marking cache entries as clean.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5C__json_write_mark_entry_clean_log_msg(void *udata, const H5C_cache_entry_t *entry, herr_t fxn_ret_value)
{
    H5C_log_json_udata_t *json_udata = (H5C_log_json_udata_t *)(udata);
    herr_t                ret_value  = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(json_udata);
    assert(json_udata->message);
    assert(entry);

    /* Create the log message string */
    snprintf(json_udata->message, H5C_MAX_JSON_LOG_MSG_SIZE, "\
{\
\"timestamp\":%lld,\
\"action\":\"clean\",\
\"address\":0x%lx,\
\"returned\":%d\
},\n\
",
             (long long)HDtime(NULL), (unsigned long)entry->addr, (int)fxn_ret_value);

    /* Write the log message to the file */
    if (H5C__json_write_log_message(json_udata) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "unable to emit log message");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__json_write_mark_entry_clean_log_msg() */

/*-------------------------------------------------------------------------
 * Function:    H5C__json_write_mark_unserialized_entry_log_msg
 *
 * Purpose:     Write a log message for marking cache entries as unserialized.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5C__json_write_mark_unserialized_entry_log_msg(void *udata, const H5C_cache_entry_t *entry,
                                                herr_t fxn_ret_value)
{
    H5C_log_json_udata_t *json_udata = (H5C_log_json_udata_t *)(udata);
    herr_t                ret_value  = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(json_udata);
    assert(json_udata->message);
    assert(entry);

    /* Create the log message string */
    snprintf(json_udata->message, H5C_MAX_JSON_LOG_MSG_SIZE, "\
{\
\"timestamp\":%lld,\
\"action\":\"unserialized\",\
\"address\":0x%lx,\
\"returned\":%d\
},\n\
",
             (long long)HDtime(NULL), (unsigned long)entry->addr, (int)fxn_ret_value);

    /* Write the log message to the file */
    if (H5C__json_write_log_message(json_udata) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "unable to emit log message");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__json_write_mark_unserialized_entry_log_msg() */

/*-------------------------------------------------------------------------
 * Function:    H5C__json_write_mark_serialize_entry_log_msg
 *
 * Purpose:     Write a log message for marking cache entries as serialize.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5C__json_write_mark_serialized_entry_log_msg(void *udata, const H5C_cache_entry_t *entry,
                                              herr_t fxn_ret_value)
{
    H5C_log_json_udata_t *json_udata = (H5C_log_json_udata_t *)(udata);
    herr_t                ret_value  = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(json_udata);
    assert(json_udata->message);
    assert(entry);

    /* Create the log message string */
    snprintf(json_udata->message, H5C_MAX_JSON_LOG_MSG_SIZE, "\
{\
\"timestamp\":%lld,\
\"action\":\"serialized\",\
\"address\":0x%lx,\
\"returned\":%d\
},\n\
",
             (long long)HDtime(NULL), (unsigned long)entry->addr, (int)fxn_ret_value);

    /* Write the log message to the file */
    if (H5C__json_write_log_message(json_udata) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "unable to emit log message");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__json_write_mark_serialized_entry_log_msg() */

/*-------------------------------------------------------------------------
 * Function:    H5C__json_write_move_entry_log_msg
 *
 * Purpose:     Write a log message for moving a cache entry.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5C__json_write_move_entry_log_msg(void *udata, haddr_t old_addr, haddr_t new_addr, int type_id,
                                   herr_t fxn_ret_value)
{
    H5C_log_json_udata_t *json_udata = (H5C_log_json_udata_t *)(udata);
    herr_t                ret_value  = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(json_udata);
    assert(json_udata->message);

    /* Create the log message string */
    snprintf(json_udata->message, H5C_MAX_JSON_LOG_MSG_SIZE, "\
{\
\"timestamp\":%lld,\
\"action\":\"move\",\
\"old_address\":0x%lx,\
\"new_address\":0x%lx,\
\"type_id\":%d,\
\"returned\":%d\
},\n\
",
             (long long)HDtime(NULL), (unsigned long)old_addr, (unsigned long)new_addr, type_id,
             (int)fxn_ret_value);

    /* Write the log message to the file */
    if (H5C__json_write_log_message(json_udata) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "unable to emit log message");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__json_write_move_entry_log_msg() */

/*-------------------------------------------------------------------------
 * Function:    H5C__json_write_pin_entry_log_msg
 *
 * Purpose:     Write a log message for pinning a cache entry.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5C__json_write_pin_entry_log_msg(void *udata, const H5C_cache_entry_t *entry, herr_t fxn_ret_value)
{
    H5C_log_json_udata_t *json_udata = (H5C_log_json_udata_t *)(udata);
    herr_t                ret_value  = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(json_udata);
    assert(json_udata->message);
    assert(entry);

    /* Create the log message string */
    snprintf(json_udata->message, H5C_MAX_JSON_LOG_MSG_SIZE, "\
{\
\"timestamp\":%lld,\
\"action\":\"pin\",\
\"address\":0x%lx,\
\"returned\":%d\
},\n\
",
             (long long)HDtime(NULL), (unsigned long)entry->addr, (int)fxn_ret_value);

    /* Write the log message to the file */
    if (H5C__json_write_log_message(json_udata) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "unable to emit log message");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__json_write_pin_entry_log_msg() */

/*-------------------------------------------------------------------------
 * Function:    H5C__json_write_create_fd_log_msg
 *
 * Purpose:     Write a log message for creating a flush dependency between
 *              two cache entries.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5C__json_write_create_fd_log_msg(void *udata, const H5C_cache_entry_t *parent,
                                  const H5C_cache_entry_t *child, herr_t fxn_ret_value)
{
    H5C_log_json_udata_t *json_udata = (H5C_log_json_udata_t *)(udata);
    herr_t                ret_value  = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(json_udata);
    assert(json_udata->message);
    assert(parent);
    assert(child);

    /* Create the log message string */
    snprintf(json_udata->message, H5C_MAX_JSON_LOG_MSG_SIZE, "\
{\
\"timestamp\":%lld,\
\"action\":\"create_fd\",\
\"parent_addr\":0x%lx,\
\"child_addr\":0x%lx,\
\"returned\":%d\
},\n\
",
             (long long)HDtime(NULL), (unsigned long)parent->addr, (unsigned long)child->addr,
             (int)fxn_ret_value);

    /* Write the log message to the file */
    if (H5C__json_write_log_message(json_udata) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "unable to emit log message");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__json_write_create_fd_log_msg() */

/*-------------------------------------------------------------------------
 * Function:    H5C__json_write_protect_entry_log_msg
 *
 * Purpose:     Write a log message for protecting a cache entry.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5C__json_write_protect_entry_log_msg(void *udata, const H5C_cache_entry_t *entry, int type_id,
                                      unsigned flags, herr_t fxn_ret_value)
{
    H5C_log_json_udata_t *json_udata = (H5C_log_json_udata_t *)(udata);
    char                  rw_s[16];
    herr_t                ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(json_udata);
    assert(json_udata->message);
    assert(entry);

    if (H5C__READ_ONLY_FLAG == flags)
        strcpy(rw_s, "READ");
    else
        strcpy(rw_s, "WRITE");

    /* Create the log message string */
    snprintf(json_udata->message, H5C_MAX_JSON_LOG_MSG_SIZE, "\
{\
\"timestamp\":%lld,\
\"action\":\"protect\",\
\"address\":0x%lx,\
\"type_id\":%d,\
\"readwrite\":\"%s\",\
\"size\":%d,\
\"returned\":%d\
},\n\
",
             (long long)HDtime(NULL), (unsigned long)entry->addr, type_id, rw_s, (int)entry->size,
             (int)fxn_ret_value);

    /* Write the log message to the file */
    if (H5C__json_write_log_message(json_udata) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "unable to emit log message");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__json_write_protect_entry_log_msg() */

/*-------------------------------------------------------------------------
 * Function:    H5C__json_write_resize_entry_log_msg
 *
 * Purpose:     Write a log message for resizing a cache entry.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5C__json_write_resize_entry_log_msg(void *udata, const H5C_cache_entry_t *entry, size_t new_size,
                                     herr_t fxn_ret_value)
{
    H5C_log_json_udata_t *json_udata = (H5C_log_json_udata_t *)(udata);
    herr_t                ret_value  = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(json_udata);
    assert(json_udata->message);
    assert(entry);

    /* Create the log message string */
    snprintf(json_udata->message, H5C_MAX_JSON_LOG_MSG_SIZE, "\
{\
\"timestamp\":%lld,\
\"action\":\"resize\",\
\"address\":0x%lx,\
\"new_size\":%d,\
\"returned\":%d\
},\n\
",
             (long long)HDtime(NULL), (unsigned long)entry->addr, (int)new_size, (int)fxn_ret_value);

    /* Write the log message to the file */
    if (H5C__json_write_log_message(json_udata) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "unable to emit log message");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__json_write_resize_entry_log_msg() */

/*-------------------------------------------------------------------------
 * Function:    H5C__json_write_unpin_entry_log_msg
 *
 * Purpose:     Write a log message for unpinning a cache entry.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5C__json_write_unpin_entry_log_msg(void *udata, const H5C_cache_entry_t *entry, herr_t fxn_ret_value)
{
    H5C_log_json_udata_t *json_udata = (H5C_log_json_udata_t *)(udata);
    herr_t                ret_value  = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(json_udata);
    assert(json_udata->message);
    assert(entry);

    /* Create the log message string */
    snprintf(json_udata->message, H5C_MAX_JSON_LOG_MSG_SIZE, "\
{\
\"timestamp\":%lld,\
\"action\":\"unpin\",\
\"address\":0x%lx,\
\"returned\":%d\
},\n\
",
             (long long)HDtime(NULL), (unsigned long)entry->addr, (int)fxn_ret_value);

    /* Write the log message to the file */
    if (H5C__json_write_log_message(json_udata) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "unable to emit log message");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__json_write_unpin_entry_log_msg() */

/*-------------------------------------------------------------------------
 * Function:    H5C__json_write_destroy_fd_log_msg
 *
 * Purpose:     Write a log message for destroying a flush dependency
 *              between two cache entries.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5C__json_write_destroy_fd_log_msg(void *udata, const H5C_cache_entry_t *parent,
                                   const H5C_cache_entry_t *child, herr_t fxn_ret_value)
{
    H5C_log_json_udata_t *json_udata = (H5C_log_json_udata_t *)(udata);
    herr_t                ret_value  = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(json_udata);
    assert(json_udata->message);
    assert(parent);
    assert(child);

    /* Create the log message string */
    snprintf(json_udata->message, H5C_MAX_JSON_LOG_MSG_SIZE, "\
{\
\"timestamp\":%lld,\
\"action\":\"destroy_fd\",\
\"parent_addr\":0x%lx,\
\"child_addr\":0x%lx,\
\"returned\":%d\
},\n\
",
             (long long)HDtime(NULL), (unsigned long)parent->addr, (unsigned long)child->addr,
             (int)fxn_ret_value);

    /* Write the log message to the file */
    if (H5C__json_write_log_message(json_udata) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "unable to emit log message");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__json_write_destroy_fd_log_msg() */

/*-------------------------------------------------------------------------
 * Function:    H5C__json_write_unprotect_entry_log_msg
 *
 * Purpose:     Write a log message for unprotecting a cache entry.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5C__json_write_unprotect_entry_log_msg(void *udata, haddr_t address, int type_id, unsigned flags,
                                        herr_t fxn_ret_value)
{
    H5C_log_json_udata_t *json_udata = (H5C_log_json_udata_t *)(udata);
    herr_t                ret_value  = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(json_udata);
    assert(json_udata->message);

    /* Create the log message string */
    snprintf(json_udata->message, H5C_MAX_JSON_LOG_MSG_SIZE, "\
{\
\"timestamp\":%lld,\
\"action\":\"unprotect\",\
\"address\":0x%lx,\
\"id\":%d,\
\"flags\":%x,\
\"returned\":%d\
},\n\
",
             (long long)HDtime(NULL), (unsigned long)address, type_id, flags, (int)fxn_ret_value);

    /* Write the log message to the file */
    if (H5C__json_write_log_message(json_udata) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "unable to emit log message");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__json_write_unprotect_entry_log_msg() */

/*-------------------------------------------------------------------------
 * Function:    H5C__json_write_set_cache_config_log_msg
 *
 * Purpose:     Write a log message for setting the cache configuration.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5C__json_write_set_cache_config_log_msg(void *udata, const H5AC_cache_config_t H5_ATTR_NDEBUG_UNUSED *config,
                                         herr_t fxn_ret_value)
{
    H5C_log_json_udata_t *json_udata = (H5C_log_json_udata_t *)(udata);
    herr_t                ret_value  = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(json_udata);
    assert(json_udata->message);
    assert(config);

    /* Create the log message string */
    snprintf(json_udata->message, H5C_MAX_JSON_LOG_MSG_SIZE, "\
{\
\"timestamp\":%lld,\
\"action\":\"set_config\",\
\"returned\":%d\
},\n\
",
             (long long)HDtime(NULL), (int)fxn_ret_value);

    /* Write the log message to the file */
    if (H5C__json_write_log_message(json_udata) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "unable to emit log message");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__json_write_set_cache_config_log_msg() */

/*-------------------------------------------------------------------------
 * Function:    H5C__json_write_remove_entry_log_msg
 *
 * Purpose:     Write a log message for removing a cache entry.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5C__json_write_remove_entry_log_msg(void *udata, const H5C_cache_entry_t *entry, herr_t fxn_ret_value)
{
    H5C_log_json_udata_t *json_udata = (H5C_log_json_udata_t *)(udata);
    herr_t                ret_value  = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(json_udata);
    assert(json_udata->message);
    assert(entry);

    /* Create the log message string */
    snprintf(json_udata->message, H5C_MAX_JSON_LOG_MSG_SIZE, "\
{\
\"timestamp\":%lld,\
\"action\":\"remove\",\
\"address\":0x%lx,\
\"returned\":%d\
},\n\
",
             (long long)HDtime(NULL), (unsigned long)entry->addr, (int)fxn_ret_value);

    /* Write the log message to the file */
    if (H5C__json_write_log_message(json_udata) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_LOGGING, FAIL, "unable to emit log message");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__json_write_remove_entry_log_msg() */
