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

/*
 * Purpose:     Cache logging header file
 */

#ifndef H5Clog_H
#define H5Clog_H

/* Get package's private header */
#include "H5Cprivate.h" /* Cache                                    */

/**************************/
/* Package Private Macros */
/**************************/

/****************************/
/* Package Private Typedefs */
/****************************/

/* Forward declaration for class struct */
typedef struct H5C_log_info_t H5C_log_info_t;

/* Class for generating logging messages */
typedef struct H5C_log_class_t {
    const char *name; /* String for debugging */

    /* Callbacks for writing log messages */
    herr_t (*tear_down_logging)(H5C_log_info_t *log_info);
    herr_t (*start_logging)(H5C_log_info_t *log_info);
    herr_t (*stop_logging)(H5C_log_info_t *log_info);
    herr_t (*write_start_log_msg)(void *udata);
    herr_t (*write_stop_log_msg)(void *udata);
    herr_t (*write_create_cache_log_msg)(void *udata, herr_t fxn_ret_value);
    herr_t (*write_destroy_cache_log_msg)(void *udata);
    herr_t (*write_evict_cache_log_msg)(void *udata, herr_t fxn_ret_value);
    herr_t (*write_expunge_entry_log_msg)(void *udata, haddr_t address, int type_id, herr_t fxn_ret_value);
    herr_t (*write_flush_cache_log_msg)(void *udata, herr_t fxn_ret_value);
    herr_t (*write_insert_entry_log_msg)(void *udata, haddr_t address, int type_id, unsigned flags,
                                         size_t size, herr_t fxn_ret_value);
    herr_t (*write_mark_entry_dirty_log_msg)(void *udata, const H5C_cache_entry_t *entry,
                                             herr_t fxn_ret_value);
    herr_t (*write_mark_entry_clean_log_msg)(void *udata, const H5C_cache_entry_t *entry,
                                             herr_t fxn_ret_value);
    herr_t (*write_mark_unserialized_entry_log_msg)(void *udata, const H5C_cache_entry_t *entry,
                                                    herr_t fxn_ret_value);
    herr_t (*write_mark_serialized_entry_log_msg)(void *udata, const H5C_cache_entry_t *entry,
                                                  herr_t fxn_ret_value);
    herr_t (*write_move_entry_log_msg)(void *udata, haddr_t old_addr, haddr_t new_addr, int type_id,
                                       herr_t fxn_ret_value);
    herr_t (*write_pin_entry_log_msg)(void *udata, const H5C_cache_entry_t *entry, herr_t fxn_ret_value);
    herr_t (*write_create_fd_log_msg)(void *udata, const H5C_cache_entry_t *parent,
                                      const H5C_cache_entry_t *child, herr_t fxn_ret_value);
    herr_t (*write_protect_entry_log_msg)(void *udata, const H5C_cache_entry_t *entry, int type_id,
                                          unsigned flags, herr_t fxn_ret_value);
    herr_t (*write_resize_entry_log_msg)(void *udata, const H5C_cache_entry_t *entry, size_t new_size,
                                         herr_t fxn_ret_value);
    herr_t (*write_unpin_entry_log_msg)(void *udata, const H5C_cache_entry_t *entry, herr_t fxn_ret_value);
    herr_t (*write_destroy_fd_log_msg)(void *udata, const H5C_cache_entry_t *parent,
                                       const H5C_cache_entry_t *child, herr_t fxn_ret_value);
    herr_t (*write_unprotect_entry_log_msg)(void *udata, haddr_t address, int type_id, unsigned flags,
                                            herr_t fxn_ret_value);
    herr_t (*write_set_cache_config_log_msg)(void *udata, const H5AC_cache_config_t *config,
                                             herr_t fxn_ret_value);
    herr_t (*write_remove_entry_log_msg)(void *udata, const H5C_cache_entry_t *entry, herr_t fxn_ret_value);

} H5C_log_class_t;

/* Logging information */
struct H5C_log_info_t {
    bool                   enabled; /* Was the logging set up? */
    bool                   logging; /* Are we currently logging? */
    const H5C_log_class_t *cls;     /* Callbacks for writing log messages */
    void                  *udata;   /* Log-specific data */
};

/*****************************/
/* Package Private Variables */
/*****************************/

/******************************/
/* Package Private Prototypes */
/******************************/
H5_DLL herr_t H5C_log_set_up(H5C_t *cache, const char log_location[], H5C_log_style_t style,
                             bool start_immediately);
H5_DLL herr_t H5C_log_tear_down(H5C_t *cache);

H5_DLL herr_t H5C_log_write_create_cache_msg(H5C_t *cache, herr_t fxn_ret_value);
H5_DLL herr_t H5C_log_write_destroy_cache_msg(H5C_t *cache);
H5_DLL herr_t H5C_log_write_evict_cache_msg(H5C_t *cache, herr_t fxn_ret_value);
H5_DLL herr_t H5C_log_write_expunge_entry_msg(H5C_t *cache, haddr_t address, int type_id,
                                              herr_t fxn_ret_value);
H5_DLL herr_t H5C_log_write_flush_cache_msg(H5C_t *cache, herr_t fxn_ret_value);
H5_DLL herr_t H5C_log_write_insert_entry_msg(H5C_t *cache, haddr_t address, int type_id, unsigned flags,
                                             size_t size, herr_t fxn_ret_value);
H5_DLL herr_t H5C_log_write_mark_entry_dirty_msg(H5C_t *cache, const H5C_cache_entry_t *entry,
                                                 herr_t fxn_ret_value);
H5_DLL herr_t H5C_log_write_mark_entry_clean_msg(H5C_t *cache, const H5C_cache_entry_t *entry,
                                                 herr_t fxn_ret_value);
H5_DLL herr_t H5C_log_write_mark_unserialized_entry_msg(H5C_t *cache, const H5C_cache_entry_t *entry,
                                                        herr_t fxn_ret_value);
H5_DLL herr_t H5C_log_write_mark_serialized_entry_msg(H5C_t *cache, const H5C_cache_entry_t *entry,
                                                      herr_t fxn_ret_value);
H5_DLL herr_t H5C_log_write_move_entry_msg(H5C_t *cache, haddr_t old_addr, haddr_t new_addr, int type_id,
                                           herr_t fxn_ret_value);
H5_DLL herr_t H5C_log_write_pin_entry_msg(H5C_t *cache, const H5C_cache_entry_t *entry, herr_t fxn_ret_value);
H5_DLL herr_t H5C_log_write_create_fd_msg(H5C_t *cache, const H5C_cache_entry_t *parent,
                                          const H5C_cache_entry_t *child, herr_t fxn_ret_value);
H5_DLL herr_t H5C_log_write_protect_entry_msg(H5C_t *cache, const H5C_cache_entry_t *entry, int type_id,
                                              unsigned flags, herr_t fxn_ret_value);
H5_DLL herr_t H5C_log_write_resize_entry_msg(H5C_t *cache, const H5C_cache_entry_t *entry, size_t new_size,
                                             herr_t fxn_ret_value);
H5_DLL herr_t H5C_log_write_unpin_entry_msg(H5C_t *cache, const H5C_cache_entry_t *entry,
                                            herr_t fxn_ret_value);
H5_DLL herr_t H5C_log_write_destroy_fd_msg(H5C_t *cache, const H5C_cache_entry_t *parent,
                                           const H5C_cache_entry_t *child, herr_t fxn_ret_value);
H5_DLL herr_t H5C_log_write_unprotect_entry_msg(H5C_t *cache, haddr_t address, int type_id, unsigned flags,
                                                herr_t fxn_ret_value);
H5_DLL herr_t H5C_log_write_set_cache_config_msg(H5C_t *cache, const H5AC_cache_config_t *config,
                                                 herr_t fxn_ret_value);
H5_DLL herr_t H5C_log_write_remove_entry_msg(H5C_t *cache, const H5C_cache_entry_t *entry,
                                             herr_t fxn_ret_value);

/* Logging-specific setup functions */
H5_DLL herr_t H5C__log_json_set_up(H5C_log_info_t *log_info, const char log_location[], int mpi_rank);
H5_DLL herr_t H5C__log_trace_set_up(H5C_log_info_t *log_info, const char log_location[], int mpi_rank);

#endif /* H5Clog_H */
