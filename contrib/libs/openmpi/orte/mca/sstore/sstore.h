/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c)      2010 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 * Copyright (c) 2015      Los Alamos National Security, LLC. All rights
 *                         reserved.
 *                         Corporation.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/**
 * @file
 *
 * Distributed Stable Storage (SStore) Interface
 *
 */

#ifndef MCA_SSTORE_H
#define MCA_SSTORE_H

#include "orte_config.h"
#include "orte/constants.h"
#include "orte/types.h"
#include "orte/runtime/orte_globals.h"

#include "orte/mca/mca.h"
#include "opal/mca/base/base.h"

#include "opal/class/opal_object.h"

BEGIN_C_DECLS

/**
 * Keys accepted as metadata
 */
typedef uint32_t orte_sstore_base_key_t;
/** CRS Component */
#define SSTORE_METADATA_LOCAL_CRS_COMP          0
/** Compress Component */
#define SSTORE_METADATA_LOCAL_COMPRESS_COMP     1
/** Compress Component Postfix */
#define SSTORE_METADATA_LOCAL_COMPRESS_POSTFIX  2
/** Process PID */
#define SSTORE_METADATA_LOCAL_PID               3
/** Checkpoint Context File */
#define SSTORE_METADATA_LOCAL_CONTEXT           4
/** Directory to make on restart */
#define SSTORE_METADATA_LOCAL_MKDIR             5
/** File to touch on restart */
#define SSTORE_METADATA_LOCAL_TOUCH             6

/** Local snapshot reference (e.g., opal_snapshot_0.ckpt) */
#define SSTORE_METADATA_LOCAL_SNAP_REF          7
/** Local snapshot reference format string (e.g., opal_snapshot_%d.ckpt) passed vpid */
#define SSTORE_METADATA_LOCAL_SNAP_REF_FMT      8
/** Local snapshot directory (Full Path excluding reference) */
#define SSTORE_METADATA_LOCAL_SNAP_LOC          9
/** Local snapshot reference directory (Full Path) */
#define SSTORE_METADATA_LOCAL_SNAP_REF_LOC_FMT 10
/** Local snapshot metadata file (Full Path) */
#define SSTORE_METADATA_LOCAL_SNAP_META        11

/** Global snapshot reference (e.g., ompi_global_snapshot_1234.ckpt) */
#define SSTORE_METADATA_GLOBAL_SNAP_REF        12
/** Global snapshot location (Relative Path from base) */
#define SSTORE_METADATA_GLOBAL_SNAP_LOC        13
/** Global snapshot location (Full path) */
#define SSTORE_METADATA_GLOBAL_SNAP_LOC_ABS    14
/** Global snapshot metadata file (Full path) */
#define SSTORE_METADATA_GLOBAL_SNAP_META       15
/** Global snapshot sequence number */
#define SSTORE_METADATA_GLOBAL_SNAP_SEQ        16
/** AMCA Parameter to be preserved for ompi-restart */
#define SSTORE_METADATA_GLOBAL_AMCA_PARAM      17

/** Total number of sequence numbers for this snapshot */
#define SSTORE_METADATA_GLOBAL_SNAP_NUM_SEQ    18
/** Comma separated list of all sequence numbers for this snapshot */
#define SSTORE_METADATA_GLOBAL_SNAP_ALL_SEQ    19

/** Access the current default base directory (Full Path) */
#define SSTORE_METADATA_BASE_LOC               20

/** The local process is skipping the checkpoint
 * Usually this is because there is a migration, and it is not participating
 */
#define SSTORE_METADATA_LOCAL_SKIP_CKPT        21

/** A Migration checkpoint does not necessarily contain all of the processes
 * in the job, so it is not a checkpoint that can be restarted from normally.
 * Therefore, it needs to be marked specially. */
#define SSTORE_METADATA_GLOBAL_MIGRATING       22

/** TUNE Parameter to be preserved for ompi-restart */
#define SSTORE_METADATA_GLOBAL_TUNE_PARAM      23

/** */
#define SSTORE_METADATA_MAX                    24

/**
 * Storage handle
 */
#define ORTE_SSTORE_HANDLE OPAL_UINT32
typedef uint32_t orte_sstore_base_handle_t;
ORTE_DECLSPEC extern orte_sstore_base_handle_t orte_sstore_handle_current;
ORTE_DECLSPEC extern orte_sstore_base_handle_t orte_sstore_handle_last_stable;
#define ORTE_SSTORE_HANDLE_INVALID 0

/**
 * Local and Global snapshot information structure
 * Primarily used by orte-restart as an abstract way to handle metadata
 */
struct orte_sstore_base_local_snapshot_info_1_0_0_t {
    /** List super object */
    opal_list_item_t super;

    /** Stable Storage Handle */
    orte_sstore_base_handle_t ss_handle;

    /** ORTE Process name */
    orte_process_name_t process_name;

    /** CRS Component */
    char *crs_comp;

    /** Compress Component */
    char *compress_comp;

    /** Compress Component Postfix */
    char *compress_postfix;

    /** Start/End Timestamps */
    char *start_time;
    char *end_time;
};
typedef struct orte_sstore_base_local_snapshot_info_1_0_0_t orte_sstore_base_local_snapshot_info_1_0_0_t;
typedef struct orte_sstore_base_local_snapshot_info_1_0_0_t orte_sstore_base_local_snapshot_info_t;

ORTE_DECLSPEC OBJ_CLASS_DECLARATION(orte_sstore_base_local_snapshot_info_t);

struct orte_sstore_base_global_snapshot_info_1_0_0_t {
    /** List super object */
    opal_list_item_t super;

    /** A list of orte_sstore_base_local_snapshot_info_t's */
    opal_list_t local_snapshots;

    /** Stable Storage Handle */
    orte_sstore_base_handle_t ss_handle;

    /** Start Timestamp */
    char * start_time;

    /** End Timestamp */
    char * end_time;

    /** Sequence number */
    int seq_num;

    /** Reference */
    char *reference;

    /** AMCA parameter used */
    char *amca_param;

    /** TUNE parameter used */
    char *tune_param;

    /** Internal use only: Cache some information on the structure */
    int num_seqs;
    char ** all_seqs;
    char *basedir;
    char *metadata_filename;
};
typedef struct orte_sstore_base_global_snapshot_info_1_0_0_t orte_sstore_base_global_snapshot_info_1_0_0_t;
typedef struct orte_sstore_base_global_snapshot_info_1_0_0_t orte_sstore_base_global_snapshot_info_t;

ORTE_DECLSPEC OBJ_CLASS_DECLARATION(orte_sstore_base_global_snapshot_info_t);

/**
 * Module initialization function.
 * Returns ORTE_SUCCESS
 */
typedef int (*orte_sstore_base_module_init_fn_t)
     (void);

/**
 * Module finalization function.
 * Returns ORTE_SUCCESS
 */
typedef int (*orte_sstore_base_module_finalize_fn_t)
     (void);

/**
 * Request a checkpoint storage handle from stable storage
 *
 * @param handle Checkpoint storage handle
 * @param key Key to use as an identifier
 * @param value Value of the key specified
 *
 * @return ORTE_SUCCESS on success
 * @return ORTE_ERROR on failure
 */
typedef int (*orte_sstore_base_request_checkpoint_handle_fn_t)
    (orte_sstore_base_handle_t *handle, int seq, orte_jobid_t jobid);

/**
 * Request a restart storage handle from stable storage
 * This function will fail if the key cannot be matched.
 * If multiple matches exist, it will return the latest one.
 * If they key is NULL, then the latest entry will be used.
 *
 * @param handle Restart storage handle
 *
 * @return ORTE_SUCCESS on success
 * @return ORTE_ERROR on failure
 */
typedef int (*orte_sstore_base_request_restart_handle_fn_t)
    (orte_sstore_base_handle_t *handle,
     char *basedir, char *ref, int seq,
     orte_sstore_base_global_snapshot_info_t *snapshot);

/**
 * Request snapshot info from a given handle.
 * If they key is NULL, then the latest entry will be used.
 *
 * @param handle Restart storage handle
 *
 * @return ORTE_SUCCESS on success
 * @return ORTE_ERROR on failure
 */
typedef int (*orte_sstore_base_request_global_snapshot_data_fn_t)
    (orte_sstore_base_handle_t *handle,
     orte_sstore_base_global_snapshot_info_t *snapshot);

/**
 * Register access to a handle.
 *
 * @param handle Storage handle
 *
 * @return ORTE_SUCCESS on success
 * @return ORTE_ERROR on failure
 */
typedef int (*orte_sstore_base_register_handle_fn_t)
    (orte_sstore_base_handle_t handle);

/**
 * Get attribute on the storage handle
 *
 * @param handle Storage handle
 * @param key Key to access
 * @param value Value of the key. NULL if not avaialble
 *
 * @return ORTE_SUCCESS on success
 * @return ORTE_ERROR on failure
 */
typedef int (*orte_sstore_base_get_attribute_fn_t)
    (orte_sstore_base_handle_t handle, orte_sstore_base_key_t key, char **value);

/**
 * Set attribute on the storage handle
 *
 * @param handle Storage handle
 * @param key Key to set
 * @param value Value of the key.
 *
 * @return ORTE_SUCCESS on success
 * @return ORTE_ERROR on failure
 */
typedef int (*orte_sstore_base_set_attribute_fn_t)
    (orte_sstore_base_handle_t handle, orte_sstore_base_key_t key, char *value);

/**
 * Synchronize the handle
 *
 * @param handle Storage handle
 *
 * @return ORTE_SUCCESS on success
 * @return ORTE_ERROR on failure
 */
typedef int (*orte_sstore_base_sync_fn_t)
    (orte_sstore_base_handle_t handle);

/**
 * Remove data associated with the handle
 *
 * @param handle Storage handle
 *
 * @return ORTE_SUCCESS on success
 * @return ORTE_ERROR on failure
 */
typedef int (*orte_sstore_base_remove_fn_t)
    (orte_sstore_base_handle_t handle);

/**
 * Pack a handle into a buffer
 * Only called between the HNP and ORTED (or Global and Local SnapC coordinators)
 *
 * @param peer Peer to which this is being sent (or NULL if to all peers)
 * @param buffer Buffer to pack the data into
 * @param handle Storage handle
 *
 * @return ORTE_SUCCESS on success
 * @return ORTE_ERROR on failure
 */
typedef int (*orte_sstore_base_pack_fn_t)
    (orte_process_name_t* peer, opal_buffer_t* buffer, orte_sstore_base_handle_t handle);

/**
 * Unack a handle from a buffer
 * Only called between the HNP and ORTED (or Global and Local SnapC coordinators)
 *
 * @param peer Peer from which this was received
 * @param buffer Buffer to unpack the data
 * @param handle Storage handle
 *
 * @return ORTE_SUCCESS on success
 * @return ORTE_ERROR on failure
 */
typedef int (*orte_sstore_base_unpack_fn_t)
    (orte_process_name_t* peer, opal_buffer_t* buffer, orte_sstore_base_handle_t *handle);

/**
 * Fetch application context dependencies before local launch
 *
 * @param app Application context
 *
 * @return ORTE_SUCCESS on success
 * @return ORTE_ERROR on failure
 */
typedef int (*orte_sstore_base_fetch_app_deps_fn_t)
    (orte_app_context_t *app);

/**
 * Wait for all application context dependencies to be fetched
 *
 * @return ORTE_SUCCESS on success
 * @return ORTE_ERROR on failure
 */
typedef int (*orte_sstore_base_wait_all_deps_fn_t)
    (void);

/**
 * Structure for SSTORE components.
 */
struct orte_sstore_base_component_2_0_0_t {
    /** MCA base component */
    mca_base_component_t base_version;
    /** MCA base data */
    mca_base_component_data_t base_data;

    /** Verbosity Level */
    int verbose;
    /** Output Handle for opal_output */
    int output_handle;
    /** Default Priority */
    int priority;
};
typedef struct orte_sstore_base_component_2_0_0_t orte_sstore_base_component_2_0_0_t;
typedef struct orte_sstore_base_component_2_0_0_t orte_sstore_base_component_t;

/**
 * Structure for SSTORE  modules
 */
struct orte_sstore_base_module_1_0_0_t {
    /** Initialization Function */
    orte_sstore_base_module_init_fn_t           sstore_init;
    /** Finalization Function */
    orte_sstore_base_module_finalize_fn_t       sstore_finalize;

    /** Request handle */
    orte_sstore_base_request_checkpoint_handle_fn_t    request_checkpoint_handle;
    orte_sstore_base_request_restart_handle_fn_t       request_restart_handle;
    orte_sstore_base_request_global_snapshot_data_fn_t request_global_snapshot_data;
    orte_sstore_base_register_handle_fn_t              register_handle;

    /** Get/Set Attributes */
    orte_sstore_base_get_attribute_fn_t         get_attr;
    orte_sstore_base_set_attribute_fn_t         set_attr;

    /** Sync */
    orte_sstore_base_sync_fn_t                  sync;

    /** Remove */
    orte_sstore_base_remove_fn_t                remove;

    /** Pack/Unpack Handle */
    orte_sstore_base_pack_fn_t                  pack_handle;
    orte_sstore_base_unpack_fn_t                unpack_handle;

    /** Launch Helpers */
    orte_sstore_base_fetch_app_deps_fn_t        fetch_app_deps;
    orte_sstore_base_wait_all_deps_fn_t         wait_all_deps;
};
typedef struct orte_sstore_base_module_1_0_0_t orte_sstore_base_module_1_0_0_t;
typedef struct orte_sstore_base_module_1_0_0_t orte_sstore_base_module_t;

ORTE_DECLSPEC extern orte_sstore_base_module_t orte_sstore;

/**
 * Macro for use in components that are of type SSTORE
 */
#define ORTE_SSTORE_BASE_VERSION_2_0_0 \
    ORTE_MCA_BASE_VERSION_2_1_0("sstore", 2, 0, 0)

END_C_DECLS

#endif /* ORTE_SSTORE_H */

