/*
 * Copyright (c)      2010 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2013      Los Alamos National Security, LLC.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
#ifndef ORTE_SSTORE_BASE_H
#define ORTE_SSTORE_BASE_H

#include "orte_config.h"

#include "orte/mca/sstore/sstore.h"

/*
 * Global functions for MCA overall SStore
 */

BEGIN_C_DECLS

/*
 * MCA Framework
 */
ORTE_DECLSPEC extern mca_base_framework_t orte_sstore_base_framework;
/* select a component */
ORTE_DECLSPEC    int orte_sstore_base_select(void);

/**
 * Object stuff
 */
void orte_sstore_base_local_snapshot_info_construct(orte_sstore_base_local_snapshot_info_t *snapshot);
void orte_sstore_base_local_snapshot_info_destruct( orte_sstore_base_local_snapshot_info_t *snapshot);

void orte_sstore_base_global_snapshot_info_construct(orte_sstore_base_global_snapshot_info_t *snapshot);
void orte_sstore_base_global_snapshot_info_destruct( orte_sstore_base_global_snapshot_info_t *snapshot);

/**
 * Globals
 */
ORTE_DECLSPEC extern orte_sstore_base_module_t orte_sstore;

/*
 * Context of this module
 */
#define ORTE_SSTORE_UNASSIGN_TYPE 0
#define ORTE_SSTORE_GLOBAL_TYPE   1
#define ORTE_SSTORE_LOCAL_TYPE    2
#define ORTE_SSTORE_TOOL_TYPE     4
#define ORTE_SSTORE_APP_TYPE      8
ORTE_DECLSPEC extern int orte_sstore_context;

/**
 * Snapshot metadata
 */
#define SSTORE_METADATA_LOCAL_CRS_COMP_STR  CRS_METADATA_COMP
#define SSTORE_METADATA_LOCAL_PID_STR       CRS_METADATA_PID
#define SSTORE_METADATA_LOCAL_CONTEXT_STR   CRS_METADATA_CONTEXT
#define SSTORE_METADATA_LOCAL_MKDIR_STR     CRS_METADATA_MKDIR
#define SSTORE_METADATA_LOCAL_TOUCH_STR     CRS_METADATA_TOUCH

#define SSTORE_METADATA_LOCAL_COMPRESS_COMP_STR    ("# OPAL Compress Component: ")
#define SSTORE_METADATA_LOCAL_COMPRESS_POSTFIX_STR ("# OPAL Compress Postfix: ")

#define SSTORE_METADATA_LOCAL_SNAP_REF_FMT_STR ("# Local Snapshot Format Reference: ")
#define SSTORE_METADATA_GLOBAL_SNAP_SEQ_STR    ("# Seq: ")
#define SSTORE_METADATA_GLOBAL_AMCA_PARAM_STR  ("# AMCA: ")
#define SSTORE_METADATA_GLOBAL_TUNE_PARAM_STR  ("# TUNE: ")

#define SSTORE_METADATA_INTERNAL_DONE_SEQ_STR  ("# Finished Seq: ")
#define SSTORE_METADATA_INTERNAL_TIME_STR      ("# Timestamp: ")
#define SSTORE_METADATA_INTERNAL_PROCESS_STR   ("# Process: ")

#define SSTORE_METADATA_INTERNAL_MIG_SEQ_STR       ("# Migrate Seq: ")
#define SSTORE_METADATA_INTERNAL_DONE_MIG_SEQ_STR  ("# Finished Migrate Seq: ")


/**
 * Some utility functions
 */
ORTE_DECLSPEC extern bool   orte_sstore_base_is_checkpoint_available;
ORTE_DECLSPEC extern char * orte_sstore_base_local_metadata_filename;
ORTE_DECLSPEC extern char * orte_sstore_base_global_metadata_filename;
ORTE_DECLSPEC extern char * orte_sstore_base_local_snapshot_fmt;
ORTE_DECLSPEC extern char * orte_sstore_base_global_snapshot_dir;
ORTE_DECLSPEC extern char * orte_sstore_base_global_snapshot_ref;
ORTE_DECLSPEC extern char * orte_sstore_base_prelaunch_location;

ORTE_DECLSPEC int orte_sstore_base_get_global_snapshot_ref(char **name_str, pid_t pid);

ORTE_DECLSPEC int orte_sstore_base_convert_key_to_string(orte_sstore_base_key_t key, char **key_str);
ORTE_DECLSPEC int orte_sstore_base_convert_string_to_key(char *key_str, orte_sstore_base_key_t *key);

ORTE_DECLSPEC int orte_sstore_base_metadata_read_next_seq_num(FILE *file);
ORTE_DECLSPEC int orte_sstore_base_metadata_read_next_token(FILE *file, char **token, char **value);
ORTE_DECLSPEC int orte_sstore_base_metadata_seek_to_seq_num(FILE *file, int seq_num);

ORTE_DECLSPEC int orte_sstore_base_extract_global_metadata(orte_sstore_base_global_snapshot_info_t *global_snapshot);
ORTE_DECLSPEC int orte_sstore_base_get_all_snapshots(opal_list_t *all_snapshots, char *basedir);
ORTE_DECLSPEC int orte_sstore_base_find_largest_seq_num(orte_sstore_base_global_snapshot_info_t *global_snapshot, int *seq_num);
ORTE_DECLSPEC int orte_sstore_base_find_all_seq_nums(orte_sstore_base_global_snapshot_info_t *global_snapshot, int *num_seq, char ***seq_list);

/*
 * Common Tool functionality for interfacing with orte-restart/checkpoint
 */
ORTE_DECLSPEC int orte_sstore_base_tool_request_restart_handle(orte_sstore_base_handle_t *handle,
                                                               char *basedir, char *ref, int seq,
                                                               orte_sstore_base_global_snapshot_info_t *snapshot);
ORTE_DECLSPEC int orte_sstore_base_tool_get_attr(orte_sstore_base_handle_t handle, orte_sstore_base_key_t key, char **value);

END_C_DECLS

#endif /* ORTE_SSTORE_BASE_H */
