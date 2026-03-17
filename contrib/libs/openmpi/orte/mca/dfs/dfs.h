/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2012-2015 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef ORTE_MCA_DFS_H
#define ORTE_MCA_DFS_H

#include "orte_config.h"
#include "orte/types.h"

#ifdef HAVE_FCNTL_H
#include <fcntl.h>
#endif

#include "orte/mca/mca.h"
#include "opal/mca/base/base.h"

#include "orte/mca/dfs/dfs_types.h"

BEGIN_C_DECLS

/*
 * Framework Interfaces
 */
/**
 * Module initialization function.
 *
 * @retval ORTE_SUCCESS The operation completed successfully
 * @retval ORTE_ERROR   An unspecifed error occurred
 */
typedef int (*orte_dfs_base_module_init_fn_t)(void);

/**
 * Module finalization function.
 *
 * @retval ORTE_SUCCESS The operation completed successfully
 * @retval ORTE_ERROR   An unspecifed error occurred
 */
typedef int (*orte_dfs_base_module_finalize_fn_t)(void);

/* Open a file
 *
 * Open a possibly remote file for reading. The uri can include file
 * system descriptions (e.g., file:///, nfs:///, or hdfs:///). Note
 * that this is a full uri - i.e., it may include a hostname to
 * indicate where the file is located
 *
 * The file descriptor will be returned in the cbfunc. It
 * represents the number by which the file can be referenced,
 * and will be an ORTE error code upon failure
 */
typedef void (*orte_dfs_base_module_open_fn_t)(char *uri,
                                               orte_dfs_open_callback_fn_t cbfunc,
                                               void *cbdata);

/* Close a file
 *
 * Closes and invalidates the file descriptor
 */
typedef void (*orte_dfs_base_module_close_fn_t)(int fd,
                                                orte_dfs_close_callback_fn_t cbfunc,
                                                void *cbdata);

/* Get the size of a file
 *
 */
typedef void (*orte_dfs_base_module_get_file_size_fn_t)(int fd,
                                                        orte_dfs_size_callback_fn_t cbfunc,
                                                        void *cbdata);

/* Position a file
 *
 * Move the read position in the file to the specified byte number
 * relative to the location specified by whence:
 *     SEEK_SET => from beginning of file
 *     SEEK_CUR => from current location
 *
 * The callback will return the offset, or a negative value if
 * the requested seek would take the pointer past the end of the
 * file. This is contrary to standard lseek behavior, but is consistent
 * with the read-only nature of this framework
 */
typedef void (*orte_dfs_base_module_seek_fn_t)(int fd, long offset, int whence,
                                               orte_dfs_seek_callback_fn_t cbfunc,
                                               void *cbdata);

/* Read bytes from a possibly remote file
 *
 * Read the specified number of bytes from the given file, using the
 * specified offset (in bytes). The status returned in cbfunc is the actual number
 * of bytes read, which should match the request unless the requested
 * length/offset would read past the end of file. An ORTE error code
 * will be returned upon error
 *
 * Note: the caller is responsible for ensuring the buffer is at least
 *       length bytes in size
 */
typedef void (*orte_dfs_base_module_read_fn_t)(int fd, uint8_t *buffer,
                                               long length,
                                               orte_dfs_read_callback_fn_t cbfunc,
                                               void *cbdata);


/* Post a file map so others may access it */
typedef void (*orte_dfs_base_module_post_file_map_fn_t)(opal_buffer_t *buf,
                                                        orte_dfs_post_callback_fn_t cbfunc,
                                                        void *cbdata);

/* Get the file map for a process
 *
 * Returns the file map associated with the specified process name. If
 * NULL is provided, then all known process maps will be returned in the
 * byte object. It is the responsibility of the caller to unpack it, so
 * applications are free to specify whatever constitutes a "file map" that
 * suits their purposes
 */
typedef void (*orte_dfs_base_module_get_file_map_fn_t)(orte_process_name_t *target,
                                                       orte_dfs_fm_callback_fn_t cbfunc,
                                                       void *cbdata);


/* Load file maps for a job
 */
typedef void (*orte_dfs_base_module_load_file_maps_fn_t)(orte_jobid_t jobid,
                                                         opal_buffer_t *buf,
                                                         orte_dfs_load_callback_fn_t cbfunc,
                                                         void *cbdata);

/* Purge file maps for a job */
typedef void (*orte_dfs_base_module_purge_file_maps_fn_t)(orte_jobid_t jobid,
                                                          orte_dfs_purge_callback_fn_t cbfunc,
                                                          void *cbdata);

/*
 * Module Structure
 */
struct orte_dfs_base_module_1_0_0_t {
    /** Initialization Function */
    orte_dfs_base_module_init_fn_t             init;
    /** Finalization Function */
    orte_dfs_base_module_finalize_fn_t         finalize;

    orte_dfs_base_module_open_fn_t             open;
    orte_dfs_base_module_close_fn_t            close;
    orte_dfs_base_module_get_file_size_fn_t    get_file_size;
    orte_dfs_base_module_seek_fn_t             seek;
    orte_dfs_base_module_read_fn_t             read;
    orte_dfs_base_module_post_file_map_fn_t    post_file_map;
    orte_dfs_base_module_get_file_map_fn_t     get_file_map;
    orte_dfs_base_module_load_file_maps_fn_t   load_file_maps;
    orte_dfs_base_module_purge_file_maps_fn_t  purge_file_maps;
};
typedef struct orte_dfs_base_module_1_0_0_t orte_dfs_base_module_1_0_0_t;
typedef orte_dfs_base_module_1_0_0_t orte_dfs_base_module_t;
ORTE_DECLSPEC extern orte_dfs_base_module_t orte_dfs;

/*
 * DFS Component
 */
struct orte_dfs_base_component_1_0_0_t {
    /** MCA base component */
    mca_base_component_t base_version;
    /** MCA base data */
    mca_base_component_data_t base_data;
};
typedef struct orte_dfs_base_component_1_0_0_t orte_dfs_base_component_1_0_0_t;
typedef orte_dfs_base_component_1_0_0_t orte_dfs_base_component_t;

/*
 * Macro for use in components that are of type errmgr
 */
#define ORTE_DFS_BASE_VERSION_1_0_0 \
    ORTE_MCA_BASE_VERSION_2_1_0("dfs", 1, 0, 0)

END_C_DECLS

#endif
