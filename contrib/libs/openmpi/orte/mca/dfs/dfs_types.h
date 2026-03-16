/*
 * Copyright (c) 2012      Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2014      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef ORTE_MCA_DFS_TYPES_H
#define ORTE_MCA_DFS_TYPES_H

#include "orte_config.h"

#include "opal/class/opal_list.h"
#include "opal/dss/dss_types.h"
#include "opal/util/proc.h"

BEGIN_C_DECLS

typedef uint8_t orte_dfs_cmd_t;
#define ORTE_DFS_CMD_T OPAL_UINT8

#define ORTE_DFS_OPEN_CMD          1
#define ORTE_DFS_CLOSE_CMD         2
#define ORTE_DFS_SIZE_CMD          3
#define ORTE_DFS_SEEK_CMD          4
#define ORTE_DFS_READ_CMD          5
#define ORTE_DFS_POST_CMD          6
#define ORTE_DFS_GETFM_CMD         7
#define ORTE_DFS_LOAD_CMD          8
#define ORTE_DFS_PURGE_CMD         9
#define ORTE_DFS_RELAY_POSTS_CMD  10

/* file maps */
typedef struct {
    opal_list_item_t super;
    orte_jobid_t jobid;
    opal_list_t maps;
} orte_dfs_jobfm_t;
ORTE_DECLSPEC OBJ_CLASS_DECLARATION(orte_dfs_jobfm_t);

typedef struct {
    opal_list_item_t super;
    orte_vpid_t vpid;
    int num_entries;
    opal_buffer_t data;
} orte_dfs_vpidfm_t;
ORTE_DECLSPEC OBJ_CLASS_DECLARATION(orte_dfs_vpidfm_t);

typedef void (*orte_dfs_open_callback_fn_t)(int fd, void *cbdata);

typedef void (*orte_dfs_close_callback_fn_t)(int fd, void *cbdata);

typedef void (*orte_dfs_size_callback_fn_t)(long size, void *cbdata);

typedef void (*orte_dfs_seek_callback_fn_t)(long offset, void *cbdata);

typedef void (*orte_dfs_read_callback_fn_t)(long status,
                                            uint8_t *buffer,
                                            void *cbdata);

typedef void (*orte_dfs_post_callback_fn_t)(void *cbdata);

typedef void (*orte_dfs_fm_callback_fn_t)(opal_buffer_t *fmaps, void *cbdata);

typedef void (*orte_dfs_load_callback_fn_t)(void *cbdata);

typedef void (*orte_dfs_purge_callback_fn_t)(void *cbdata);

END_C_DECLS

#endif
