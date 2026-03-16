/*
 * Copyright (c) 2012-2013 Los Alamos National Security, Inc.  All rights reserved.
 * Copyright (c) 2017      Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/** @file:
 */

#ifndef ORTE_MCA_DFS_BASE_H
#define ORTE_MCA_DFS_BASE_H

/*
 * includes
 */
#include "orte_config.h"
#include "orte/types.h"
#include "orte/constants.h"

#include "opal/class/opal_list.h"
#include "opal/mca/event/event.h"

#include "orte/mca/mca.h"
#include "orte/mca/dfs/dfs.h"


BEGIN_C_DECLS

/*
 * MCA Framework
 */
ORTE_DECLSPEC extern mca_base_framework_t orte_dfs_base_framework;
/* select a component */
ORTE_DECLSPEC    int orte_dfs_base_select(void);

/* tracker for active files */
typedef struct {
    opal_list_item_t super;
    orte_process_name_t requestor;
    orte_process_name_t host_daemon;
    char *uri;
    char *scheme;
    char *filename;
    int local_fd;
    int remote_fd;
    size_t location;
} orte_dfs_tracker_t;
OBJ_CLASS_DECLARATION(orte_dfs_tracker_t);

/* requests */
typedef struct {
    opal_list_item_t super;
    opal_event_t ev;
    uint64_t id;
    orte_dfs_cmd_t cmd;
    orte_process_name_t target;
    char *uri;
    int local_fd;
    int remote_fd;
    uint8_t *read_buffer;
    long read_length;
    opal_buffer_t *bptr;
    opal_buffer_t bucket;
    orte_dfs_open_callback_fn_t  open_cbfunc;
    orte_dfs_close_callback_fn_t close_cbfunc;
    orte_dfs_size_callback_fn_t  size_cbfunc;
    orte_dfs_seek_callback_fn_t  seek_cbfunc;
    orte_dfs_read_callback_fn_t  read_cbfunc;
    orte_dfs_post_callback_fn_t  post_cbfunc;
    orte_dfs_fm_callback_fn_t    fm_cbfunc;
    orte_dfs_load_callback_fn_t  load_cbfunc;
    orte_dfs_purge_callback_fn_t purge_cbfunc;
    void *cbdata;
} orte_dfs_request_t;
OBJ_CLASS_DECLARATION(orte_dfs_request_t);

END_C_DECLS

#endif
