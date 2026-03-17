/*
 * Copyright (c)      2010 The Trustees of Indiana University.
 *                         All rights reserved.
 * Copyright (c) 2004-2011 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "orte_config.h"

#include <string.h>
#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef HAVE_SYS_STAT_H
#include <sys/stat.h>
#endif /* HAVE_SYS_STAT_H */
#ifdef HAVE_DIRENT_H
#include <dirent.h>
#endif /* HAVE_DIRENT_H */
#include <time.h>

#include "orte/constants.h"

#include "orte/mca/mca.h"
#include "opal/mca/base/base.h"
#include "opal/util/argv.h"
#include "opal/mca/crs/base/base.h"

#include "orte/mca/errmgr/errmgr.h"
#include "orte/runtime/orte_globals.h"
#include "orte/util/proc_info.h"

#include "orte/mca/sstore/sstore.h"
#include "orte/mca/sstore/base/base.h"

/******************
 * Local Functions
 ******************/

/******************
 * Object Stuff
 ******************/
OBJ_CLASS_INSTANCE(orte_sstore_base_local_snapshot_info_t,
                   opal_list_item_t,
                   orte_sstore_base_local_snapshot_info_construct,
                   orte_sstore_base_local_snapshot_info_destruct);

void orte_sstore_base_local_snapshot_info_construct(orte_sstore_base_local_snapshot_info_t *snapshot)
{
    snapshot->process_name.jobid  = 0;
    snapshot->process_name.vpid   = 0;

    snapshot->crs_comp = NULL;
    snapshot->compress_comp    = NULL;
    snapshot->compress_postfix = NULL;

    snapshot->start_time = NULL;
    snapshot->end_time   = NULL;
}

void orte_sstore_base_local_snapshot_info_destruct( orte_sstore_base_local_snapshot_info_t *snapshot)
{
    snapshot->process_name.jobid  = 0;
    snapshot->process_name.vpid   = 0;

    if( NULL != snapshot->crs_comp ) {
        free(snapshot->crs_comp);
        snapshot->crs_comp = NULL;
    }

    if( NULL != snapshot->compress_comp ) {
        free(snapshot->compress_comp);
        snapshot->compress_comp = NULL;
    }

    if( NULL != snapshot->compress_postfix ) {
        free(snapshot->compress_postfix);
        snapshot->compress_postfix = NULL;
    }

    if( NULL != snapshot->start_time ) {
        free(snapshot->start_time);
        snapshot->start_time = NULL;
    }

    if( NULL != snapshot->end_time ) {
        free(snapshot->end_time);
        snapshot->end_time = NULL;
    }
}

OBJ_CLASS_INSTANCE(orte_sstore_base_global_snapshot_info_t,
                   opal_list_item_t,
                   orte_sstore_base_global_snapshot_info_construct,
                   orte_sstore_base_global_snapshot_info_destruct);

void orte_sstore_base_global_snapshot_info_construct(orte_sstore_base_global_snapshot_info_t *snapshot)
{
    OBJ_CONSTRUCT(&(snapshot->local_snapshots), opal_list_t);

    snapshot->ss_handle  = ORTE_SSTORE_HANDLE_INVALID;

    snapshot->start_time = NULL;
    snapshot->end_time   = NULL;

    snapshot->seq_num = -1;

    snapshot->num_seqs = 0;
    snapshot->all_seqs = NULL;
    snapshot->basedir = NULL;
    snapshot->reference = NULL;
    snapshot->amca_param = NULL;
    snapshot->tune_param = NULL;
    snapshot->metadata_filename = NULL;
}

void orte_sstore_base_global_snapshot_info_destruct( orte_sstore_base_global_snapshot_info_t *snapshot)
{
    opal_list_item_t* item = NULL;

    while (NULL != (item = opal_list_remove_first(&snapshot->local_snapshots))) {
        OBJ_RELEASE(item);
    }
    OBJ_DESTRUCT(&(snapshot->local_snapshots));

    snapshot->ss_handle  = ORTE_SSTORE_HANDLE_INVALID;

    if( NULL != snapshot->start_time ) {
        free(snapshot->start_time);
        snapshot->start_time = NULL;
    }

    if( NULL != snapshot->end_time ) {
        free(snapshot->end_time);
        snapshot->end_time = NULL;
    }

    snapshot->seq_num = -1;

    snapshot->num_seqs = 0;

    if( NULL != snapshot->all_seqs ) {
        opal_argv_free(snapshot->all_seqs);
        snapshot->all_seqs = NULL;
    }

    if( NULL != snapshot->basedir ) {
        free(snapshot->basedir);
        snapshot->basedir = NULL;
    }

    if( NULL != snapshot->reference ) {
        free(snapshot->reference);
        snapshot->reference = NULL;
    }

    if( NULL != snapshot->amca_param ) {
        free(snapshot->amca_param);
        snapshot->amca_param = NULL;
    }

    if( NULL != snapshot->tune_param ) {
        free(snapshot->tune_param);
        snapshot->tune_param = NULL;
    }

    if( NULL != snapshot->metadata_filename ) {
        free(snapshot->metadata_filename);
        snapshot->metadata_filename = NULL;
    }
}

/***************
 * Tool interface functionality
 ***************/
static orte_sstore_base_global_snapshot_info_t *tool_global_snapshot = NULL;

int orte_sstore_base_tool_request_restart_handle(orte_sstore_base_handle_t *handle,
                                                 char *basedir, char *ref, int seq,
                                                 orte_sstore_base_global_snapshot_info_t *snapshot)
{
    int ret, exit_status = ORTE_SUCCESS;
    char * tmp_str = NULL;

    if( NULL != tool_global_snapshot ) {
        OBJ_RELEASE(tool_global_snapshot);
    }
    tool_global_snapshot = snapshot;
    OBJ_RETAIN(tool_global_snapshot);

    snapshot->reference = strdup(ref);
    if( NULL == basedir ) {
        snapshot->basedir = strdup(orte_sstore_base_global_snapshot_dir);
    } else {
        snapshot->basedir = strdup(basedir);
    }
    asprintf(&(snapshot->metadata_filename),
             "%s/%s/%s",
             snapshot->basedir,
             snapshot->reference,
             orte_sstore_base_global_metadata_filename);

    /*
     * Check the checkpoint location
     */
    asprintf(&tmp_str, "%s/%s",
             snapshot->basedir,
             snapshot->reference);
    if (0 >  (ret = access(tmp_str, F_OK)) ) {
        opal_output(0, ("Error: The snapshot requested does not exist!\n"
                        "Check the path (%s)!"),
                    tmp_str);
        exit_status = ORTE_ERROR;
        goto cleanup;
    }
    if(NULL != tmp_str ) {
        free(tmp_str);
        tmp_str = NULL;
    }

    /*
     * If we were asked to find the largest seq num
     */
    if( seq < 0 ) {
        if( ORTE_SUCCESS != (ret = orte_sstore_base_find_largest_seq_num(snapshot, &seq)) ) {
            opal_output(0, ("Error: Failed to find a valid sequence number in snapshot metadata!\n"
                            "Check the metadata file (%s)!"),
                        snapshot->metadata_filename);
            exit_status = ORTE_ERROR;
            goto cleanup;
        }
        snapshot->seq_num = seq;
    } else {
        snapshot->seq_num = seq;
    }

    /*
     * Check the checkpoint sequence location
     */
    asprintf(&tmp_str, "%s/%s/%d",
             snapshot->basedir,
             snapshot->reference,
             snapshot->seq_num);
    if (0 >  (ret = access(tmp_str, F_OK)) ) {
        opal_output(0, ("Error: The snapshot sequence requested does not exist!\n"
                        "Check the path (%s)!"),
                    tmp_str);
        exit_status = ORTE_ERROR;
        goto cleanup;
    }
    if(NULL != tmp_str ) {
        free(tmp_str);
        tmp_str = NULL;
    }

    /*
     * Build the list of processes attached to the snapshot
     */
    if( ORTE_SUCCESS != (ret = orte_sstore_base_extract_global_metadata(snapshot)) ) {
        opal_output(0, "Error: Failed to extract process information! Check the metadata file in (%s)!",
                    tmp_str);
        exit_status = ORTE_ERROR;
        goto cleanup;
    }

    /*
     * Save some basic infomation
     */
    snapshot->ss_handle = 1;
    *handle = 1;

 cleanup:
    if( NULL != tmp_str ) {
        free(tmp_str);
        tmp_str = NULL;
    }

    return exit_status;
}

int orte_sstore_base_tool_get_attr(orte_sstore_base_handle_t handle, orte_sstore_base_key_t key, char **value)
{
    int ret, exit_status = ORTE_SUCCESS;

    if( SSTORE_METADATA_GLOBAL_SNAP_LOC_ABS == key ) {
        asprintf(value, "%s/%s",
                 tool_global_snapshot->basedir,
                 tool_global_snapshot->reference);
    }
    else if( SSTORE_METADATA_LOCAL_SNAP_REF_FMT == key ) {
        *value = strdup(orte_sstore_base_local_snapshot_fmt);
    }
    else if( SSTORE_METADATA_LOCAL_SNAP_LOC == key ) {
        asprintf(value, "%s/%s/%d",
                 tool_global_snapshot->basedir,
                 tool_global_snapshot->reference,
                 tool_global_snapshot->seq_num);
    }
    else if( SSTORE_METADATA_LOCAL_SNAP_REF_LOC_FMT == key ) {
        asprintf(value, "%s/%s/%d/%s",
                 tool_global_snapshot->basedir,
                 tool_global_snapshot->reference,
                 tool_global_snapshot->seq_num,
                 orte_sstore_base_local_snapshot_fmt);
    }
    else if( SSTORE_METADATA_GLOBAL_SNAP_NUM_SEQ == key ) {
        if( NULL == tool_global_snapshot->all_seqs ) {
            if( ORTE_SUCCESS != (ret = orte_sstore_base_find_all_seq_nums(tool_global_snapshot,
                                                                          &(tool_global_snapshot->num_seqs),
                                                                          &(tool_global_snapshot->all_seqs)))) {
                ORTE_ERROR_LOG(ORTE_ERROR);
                exit_status = ORTE_ERROR;
                goto cleanup;
            }
        }
        asprintf(value, "%d", tool_global_snapshot->num_seqs);
    }
    else if( SSTORE_METADATA_GLOBAL_SNAP_ALL_SEQ == key ) {
        if( NULL == tool_global_snapshot->all_seqs ) {
            if( ORTE_SUCCESS != (ret = orte_sstore_base_find_all_seq_nums(tool_global_snapshot,
                                                                          &(tool_global_snapshot->num_seqs),
                                                                          &(tool_global_snapshot->all_seqs)))) {
                ORTE_ERROR_LOG(ORTE_ERROR);
                exit_status = ORTE_ERROR;
                goto cleanup;
            }
        }
        *value = opal_argv_join(tool_global_snapshot->all_seqs, ',');
    }
    else if( SSTORE_METADATA_GLOBAL_AMCA_PARAM == key ) {
        *value = strdup(tool_global_snapshot->amca_param);
    }
    else if( SSTORE_METADATA_GLOBAL_TUNE_PARAM == key ) {
        *value = strdup(tool_global_snapshot->tune_param);
    }
    else {
        return ORTE_ERR_NOT_SUPPORTED;
    }

 cleanup:
    return exit_status;
}

/********************
 * Utility functions
 ********************/
int orte_sstore_base_get_global_snapshot_ref(char **name_str, pid_t pid)
{
    if( NULL == orte_sstore_base_global_snapshot_ref ) {
        asprintf(name_str, "ompi_global_snapshot_%d.ckpt", pid);
    }
    else {
        *name_str = strdup(orte_sstore_base_global_snapshot_ref);
    }

    return ORTE_SUCCESS;
}

int orte_sstore_base_convert_key_to_string(orte_sstore_base_key_t key, char **key_str)
{
    switch(key) {
    case SSTORE_METADATA_LOCAL_CRS_COMP:
        *key_str = strdup(SSTORE_METADATA_LOCAL_CRS_COMP_STR);
        break;
    case SSTORE_METADATA_LOCAL_COMPRESS_COMP:
        *key_str = strdup(SSTORE_METADATA_LOCAL_COMPRESS_COMP_STR);
        break;
    case SSTORE_METADATA_LOCAL_COMPRESS_POSTFIX:
        *key_str = strdup(SSTORE_METADATA_LOCAL_COMPRESS_POSTFIX_STR);
        break;
    case SSTORE_METADATA_LOCAL_PID:
        *key_str = strdup(SSTORE_METADATA_LOCAL_PID_STR);
        break;
    case SSTORE_METADATA_LOCAL_CONTEXT:
        *key_str = strdup(SSTORE_METADATA_LOCAL_CONTEXT_STR);
        break;
    case SSTORE_METADATA_LOCAL_MKDIR:
        *key_str = strdup(SSTORE_METADATA_LOCAL_MKDIR_STR);
        break;
    case SSTORE_METADATA_LOCAL_TOUCH:
        *key_str = strdup(SSTORE_METADATA_LOCAL_TOUCH_STR);
        break;
    case SSTORE_METADATA_LOCAL_SNAP_REF:
        *key_str = NULL;
        break;
    case SSTORE_METADATA_LOCAL_SNAP_REF_FMT:
        *key_str = strdup(SSTORE_METADATA_LOCAL_SNAP_REF_FMT_STR);
        break;
    case SSTORE_METADATA_LOCAL_SNAP_LOC:
        *key_str = NULL;
        break;
    case SSTORE_METADATA_LOCAL_SNAP_META:
        *key_str = NULL;
        break;
    case SSTORE_METADATA_GLOBAL_SNAP_REF:
        *key_str = NULL;
        break;
    case SSTORE_METADATA_GLOBAL_SNAP_LOC:
        *key_str = NULL;
        break;
    case SSTORE_METADATA_GLOBAL_SNAP_LOC_ABS:
        *key_str = NULL;
        break;
    case SSTORE_METADATA_GLOBAL_SNAP_META:
        *key_str = NULL;
        break;
    case SSTORE_METADATA_GLOBAL_SNAP_SEQ:
        *key_str = strdup(SSTORE_METADATA_GLOBAL_SNAP_SEQ_STR);
        break;
    case SSTORE_METADATA_GLOBAL_AMCA_PARAM:
        *key_str = strdup(SSTORE_METADATA_GLOBAL_AMCA_PARAM_STR);
        break;
    case SSTORE_METADATA_GLOBAL_TUNE_PARAM:
        *key_str = strdup(SSTORE_METADATA_GLOBAL_TUNE_PARAM_STR);
        break;
    default:
        *key_str = NULL;
        break;
    }

    return ORTE_SUCCESS;
}

int orte_sstore_base_convert_string_to_key(char *key_str, orte_sstore_base_key_t *key)
{
    if( 0 == strncmp(key_str, SSTORE_METADATA_LOCAL_CRS_COMP_STR, strlen(SSTORE_METADATA_LOCAL_CRS_COMP_STR))) {
        *key = SSTORE_METADATA_LOCAL_CRS_COMP;
    }
    else if( 0 == strncmp(key_str, SSTORE_METADATA_LOCAL_COMPRESS_COMP_STR, strlen(SSTORE_METADATA_LOCAL_COMPRESS_COMP_STR))) {
        *key = SSTORE_METADATA_LOCAL_COMPRESS_COMP;
    }
    else if( 0 == strncmp(key_str, SSTORE_METADATA_LOCAL_COMPRESS_POSTFIX_STR, strlen(SSTORE_METADATA_LOCAL_COMPRESS_POSTFIX_STR))) {
        *key = SSTORE_METADATA_LOCAL_COMPRESS_POSTFIX;
    }
    else if( 0 == strncmp(key_str, SSTORE_METADATA_LOCAL_PID_STR, strlen(SSTORE_METADATA_LOCAL_PID_STR))) {
        *key = SSTORE_METADATA_LOCAL_PID;
    }
    else if( 0 == strncmp(key_str, SSTORE_METADATA_LOCAL_CONTEXT_STR, strlen(SSTORE_METADATA_LOCAL_CONTEXT_STR))) {
        *key = SSTORE_METADATA_LOCAL_CONTEXT;
    }
    else if( 0 == strncmp(key_str, SSTORE_METADATA_LOCAL_MKDIR_STR, strlen(SSTORE_METADATA_LOCAL_MKDIR_STR))) {
        *key = SSTORE_METADATA_LOCAL_MKDIR;
    }
    else if( 0 == strncmp(key_str, SSTORE_METADATA_LOCAL_TOUCH_STR, strlen(SSTORE_METADATA_LOCAL_TOUCH_STR))) {
        *key = SSTORE_METADATA_LOCAL_TOUCH;
    }
    else if( 0 == strncmp(key_str, SSTORE_METADATA_LOCAL_SNAP_REF_FMT_STR, strlen(SSTORE_METADATA_LOCAL_SNAP_REF_FMT_STR))) {
        *key = SSTORE_METADATA_LOCAL_SNAP_REF_FMT;
    }
    else if( 0 == strncmp(key_str, SSTORE_METADATA_GLOBAL_SNAP_SEQ_STR, strlen(SSTORE_METADATA_GLOBAL_SNAP_SEQ_STR))) {
        *key = SSTORE_METADATA_GLOBAL_SNAP_SEQ;
    }
    else if( 0 == strncmp(key_str, SSTORE_METADATA_GLOBAL_AMCA_PARAM_STR, strlen(SSTORE_METADATA_GLOBAL_AMCA_PARAM_STR))) {
        *key = SSTORE_METADATA_GLOBAL_AMCA_PARAM;
    }
    else if( 0 == strncmp(key_str, SSTORE_METADATA_GLOBAL_TUNE_PARAM_STR, strlen(SSTORE_METADATA_GLOBAL_TUNE_PARAM_STR))) {
        *key = SSTORE_METADATA_GLOBAL_TUNE_PARAM;
    }
    else {
        *key = SSTORE_METADATA_MAX;
    }

    return ORTE_SUCCESS;
}

int orte_sstore_base_get_all_snapshots(opal_list_t *all_snapshots, char *basedir)
{
#ifndef HAVE_DIRENT_H
    return ORTE_ERR_NOT_SUPPORTED;
#else
    int ret, exit_status = ORTE_SUCCESS;
    char *loc_basedir = NULL;
    char * tmp_str = NULL, * metadata_file = NULL;
    DIR *dirp = NULL;
    struct dirent *dir_entp = NULL;
    struct stat file_status;
    orte_sstore_base_global_snapshot_info_t *global_snapshot = NULL;

    /* Sanity check */
    if( NULL == all_snapshots ||
        (NULL == orte_sstore_base_global_snapshot_dir && NULL == basedir)) {
        ORTE_ERROR_LOG(ORTE_ERROR);
        exit_status = ORTE_ERROR;
        goto cleanup;
    }

    if( NULL == basedir ) {
        loc_basedir = strdup(orte_sstore_base_global_snapshot_dir);
    } else {
        loc_basedir = strdup(basedir);
    }

    /*
     * Get all subdirectories under the base directory
     */
    dirp = opendir(loc_basedir);
    while( NULL != (dir_entp = readdir(dirp))) {
        /* Skip "." and ".." if they are in the list */
        if( 0 == strncmp("..", dir_entp->d_name, strlen("..") ) ||
            0 == strncmp(".",  dir_entp->d_name, strlen(".")  ) ) {
            continue;
        }

        /* Add the full path */
        asprintf(&tmp_str, "%s/%s", loc_basedir, dir_entp->d_name);
        if(0 != (ret = stat(tmp_str, &file_status) ) ){
            free( tmp_str);
            tmp_str = NULL;
            continue;
        } else {
            /* Is it a directory? */
            if(S_ISDIR(file_status.st_mode) ) {
                asprintf(&metadata_file, "%s/%s",
                         tmp_str,
                         orte_sstore_base_global_metadata_filename);
                if(0 != (ret = stat(metadata_file, &file_status) ) ){
                    free( tmp_str);
                    tmp_str = NULL;
                    free( metadata_file);
                    metadata_file = NULL;
                    continue;
                } else {
                    if(S_ISREG(file_status.st_mode) ) {
                        global_snapshot = OBJ_NEW(orte_sstore_base_global_snapshot_info_t);

                        global_snapshot->ss_handle = 1;
                        global_snapshot->basedir = strdup(loc_basedir);
                        asprintf(&(global_snapshot->reference),
                                 "%s",
                                 dir_entp->d_name);
                        asprintf(&(global_snapshot->metadata_filename),
                                 "%s/%s/%s",
                                 global_snapshot->basedir,
                                 global_snapshot->reference,
                                 orte_sstore_base_global_metadata_filename);

                        opal_list_append(all_snapshots, &(global_snapshot->super));
                    }
                }
                free( metadata_file);
                metadata_file = NULL;
            }
        }

        free( tmp_str);
        tmp_str = NULL;
    }

    closedir(dirp);

 cleanup:
    if( NULL != loc_basedir ) {
        free(loc_basedir);
        loc_basedir = NULL;
    }

    if( NULL != tmp_str) {
        free( tmp_str);
        tmp_str = NULL;
    }

    return exit_status;
#endif /* HAVE_DIRENT_H */
}

int orte_sstore_base_extract_global_metadata(orte_sstore_base_global_snapshot_info_t *global_snapshot)
{
    int ret, exit_status = ORTE_SUCCESS;
    FILE *metadata = NULL;
    char * token = NULL;
    char * value = NULL;
    orte_process_name_t proc;
    opal_list_item_t* item = NULL;
    orte_sstore_base_local_snapshot_info_t *vpid_snapshot = NULL;

    /*
     * Cleanup the structure a bit, so we can refresh it below
     */
    while (NULL != (item = opal_list_remove_first(&global_snapshot->local_snapshots))) {
        OBJ_RELEASE(item);
    }

    if( NULL != global_snapshot->start_time ) {
        free( global_snapshot->start_time );
        global_snapshot->start_time = NULL;
    }

    if( NULL != global_snapshot->end_time ) {
        free( global_snapshot->end_time );
        global_snapshot->end_time = NULL;
    }

    /*
     * Open the metadata file
     */
    if (NULL == (metadata = fopen(global_snapshot->metadata_filename, "r")) ) {
        opal_output(orte_sstore_base_framework.framework_output,
                    "sstore:base:extract_global_metadata() Unable to open the file (%s)\n",
                    global_snapshot->metadata_filename);
        ORTE_ERROR_LOG(ORTE_ERROR);
        exit_status = ORTE_ERROR;
        goto cleanup;
    }

    /*
     * Seek to the sequence number requested
     */
    if( ORTE_SUCCESS != (ret = orte_sstore_base_metadata_seek_to_seq_num(metadata, global_snapshot->seq_num))) {
        ORTE_ERROR_LOG(ORTE_ERROR);
        exit_status = ORTE_ERROR;
        goto cleanup;
    }

    /*
     * Extract each token and make the records
     */
    do {
        if( ORTE_SUCCESS != orte_sstore_base_metadata_read_next_token(metadata, &token, &value) ) {
            break;
        }

        if(0 == strncmp(token, SSTORE_METADATA_GLOBAL_SNAP_SEQ_STR,  strlen(SSTORE_METADATA_GLOBAL_SNAP_SEQ_STR)) ||
           0 == strncmp(token, SSTORE_METADATA_INTERNAL_MIG_SEQ_STR, strlen(SSTORE_METADATA_INTERNAL_MIG_SEQ_STR)) ) {
            break;
        }

        if( 0 == strncmp(token, SSTORE_METADATA_INTERNAL_PROCESS_STR, strlen(SSTORE_METADATA_INTERNAL_PROCESS_STR)) ) {
            orte_util_convert_string_to_process_name(&proc, value);

            /* Not the first process, so append it to the list */
            if( NULL != vpid_snapshot) {
                opal_list_append(&global_snapshot->local_snapshots, &(vpid_snapshot->super));
            }

            vpid_snapshot = OBJ_NEW(orte_sstore_base_local_snapshot_info_t);
            vpid_snapshot->ss_handle = global_snapshot->ss_handle;

            vpid_snapshot->process_name.jobid  = proc.jobid;
            vpid_snapshot->process_name.vpid   = proc.vpid;
        }
        else if(0 == strncmp(token, SSTORE_METADATA_LOCAL_CRS_COMP_STR, strlen(SSTORE_METADATA_LOCAL_CRS_COMP_STR))) {
            vpid_snapshot->crs_comp = strdup(value);
        }
        else if(0 == strncmp(token, SSTORE_METADATA_LOCAL_COMPRESS_COMP_STR, strlen(SSTORE_METADATA_LOCAL_COMPRESS_COMP_STR))) {
            vpid_snapshot->compress_comp = strdup(value);
        }
        else if(0 == strncmp(token, SSTORE_METADATA_LOCAL_COMPRESS_POSTFIX_STR, strlen(SSTORE_METADATA_LOCAL_COMPRESS_POSTFIX_STR))) {
            vpid_snapshot->compress_postfix = strdup(value);
        }
        else if(0 == strncmp(token, SSTORE_METADATA_INTERNAL_TIME_STR, strlen(SSTORE_METADATA_INTERNAL_TIME_STR)) ) {
            if( NULL == global_snapshot->start_time) {
                global_snapshot->start_time = strdup(value);
            }
            else {
                global_snapshot->end_time   = strdup(value);
            }
        }
        else if(0 == strncmp(token, SSTORE_METADATA_GLOBAL_AMCA_PARAM_STR, strlen(SSTORE_METADATA_GLOBAL_AMCA_PARAM_STR))) {
            global_snapshot->amca_param  = strdup(value);
        }
        else if(0 == strncmp(token, SSTORE_METADATA_GLOBAL_TUNE_PARAM_STR, strlen(SSTORE_METADATA_GLOBAL_TUNE_PARAM_STR))) {
            global_snapshot->tune_param  = strdup(value);
        }
    } while(0 == feof(metadata) );

    /* Append the last item */
    if( NULL != vpid_snapshot) {
        opal_list_append(&global_snapshot->local_snapshots, &(vpid_snapshot->super));
    }

 cleanup:
    if( NULL != metadata ) {
        fclose(metadata);
        metadata = NULL;
    }
    if( NULL != value ) {
        free(value);
        value = NULL;
    }
    if( NULL != token ) {
        free(token);
        token = NULL;
    }

    return exit_status;
}

int orte_sstore_base_find_largest_seq_num(orte_sstore_base_global_snapshot_info_t *global_snapshot, int *seq_num)
{
    int exit_status = ORTE_SUCCESS;
    FILE *metadata = NULL;
    int tmp_seq_num = -1;

    *seq_num = -1;

    /*
     * Open the metadata file
     */
    if (NULL == (metadata = fopen(global_snapshot->metadata_filename, "r")) ) {
        opal_output(orte_sstore_base_framework.framework_output,
                    "sstore:base:find_largest_seq_num() Unable to open the file (%s)\n",
                    global_snapshot->metadata_filename);
        ORTE_ERROR_LOG(ORTE_ERROR);
        exit_status = ORTE_ERROR;
        goto cleanup;
    }

    while(0 <= (tmp_seq_num = orte_sstore_base_metadata_read_next_seq_num(metadata)) ) {
        if( tmp_seq_num > *seq_num ) {
            *seq_num = tmp_seq_num;
        }
    }

    if( *seq_num < 0 ) {
        exit_status = ORTE_ERROR;
    }

 cleanup:
    if( NULL != metadata ) {
        fclose(metadata);
        metadata = NULL;
    }

    return exit_status;
}

int orte_sstore_base_find_all_seq_nums(orte_sstore_base_global_snapshot_info_t *global_snapshot, int *num_seq, char ***seq_list)
{
    int exit_status = ORTE_SUCCESS;
    FILE *metadata = NULL;
    int tmp_seq_num = -1;
    char * tmp_str = NULL;

    *num_seq = 0;
    *seq_list = NULL;

    /*
     * Open the metadata file
     */
    if (NULL == (metadata = fopen(global_snapshot->metadata_filename, "r")) ) {
        opal_output(orte_sstore_base_framework.framework_output,
                    "sstore:base:find_all_seq_nums() Unable to open the file (%s)\n",
                    global_snapshot->metadata_filename);
        ORTE_ERROR_LOG(ORTE_ERROR);
        exit_status = ORTE_ERROR;
        goto cleanup;
    }

    while(0 <= (tmp_seq_num = orte_sstore_base_metadata_read_next_seq_num(metadata)) ) {
        asprintf(&tmp_str, "%d", tmp_seq_num);

        if( NULL != tmp_str ) {
            opal_argv_append(num_seq, seq_list, tmp_str);
            free(tmp_str);
            tmp_str = NULL;
        }
    }

 cleanup:
    if( NULL != metadata ) {
        fclose(metadata);
        metadata = NULL;
    }

    if( NULL != tmp_str ) {
        free(tmp_str);
        tmp_str = NULL;
    }

    return exit_status;
}


/*
 * Extract the next sequence number from the file
 */
int orte_sstore_base_metadata_read_next_seq_num(FILE *file)
{
    char *token = NULL;
    char *value = NULL;
    int seq_int = -1;

    do {
        if( ORTE_SUCCESS != orte_sstore_base_metadata_read_next_token(file, &token, &value) ) {
            seq_int = -1;
            goto cleanup;
        }
    } while(0 != strncmp(token, SSTORE_METADATA_INTERNAL_DONE_SEQ_STR, strlen(SSTORE_METADATA_INTERNAL_DONE_SEQ_STR)));

    seq_int = atoi(value);

 cleanup:
    if( NULL != token) {
        free(token);
        token = NULL;
    }

    if( NULL != value) {
        free(value);
        value = NULL;
    }

    return seq_int;
}

int orte_sstore_base_metadata_seek_to_seq_num(FILE *file, int seq_num)
{
    char *token = NULL;
    char *value = NULL;
    int seq_int = -1;

    rewind(file);

    do {
        do {
            if( ORTE_SUCCESS != orte_sstore_base_metadata_read_next_token(file, &token, &value) ) {
                seq_int = -1;
                goto cleanup;
            }
        } while(0 != strncmp(token, SSTORE_METADATA_GLOBAL_SNAP_SEQ_STR, strlen(SSTORE_METADATA_GLOBAL_SNAP_SEQ_STR)) );
        seq_int = atoi(value);
    } while(seq_num != seq_int );

 cleanup:
    if( NULL != token) {
        free(token);
        token = NULL;
    }

    if( NULL != value) {
        free(value);
        value = NULL;
    }

    if( seq_num != seq_int ) {
        return ORTE_ERROR;
    } else {
        return ORTE_SUCCESS;
    }
}

int orte_sstore_base_metadata_read_next_token(FILE *file, char **token, char **value)
{
    int exit_status = ORTE_SUCCESS;
    int max_len = 256;
    char * line = NULL;
    int line_len = 0;
    int c = 0, s = 0, v = 0;
    char *local_token = NULL;
    char *local_value = NULL;
    bool end_of_line = false;

    line = (char *) malloc(sizeof(char) * max_len);

 try_again:
    /*
     * If we are at the end of the file, then just return
     */
    if(0 != feof(file) ) {
        exit_status = ORTE_ERROR;
        goto cleanup;
    }

    /*
     * Other wise grab the next token/value pair
     */
    if (NULL == fgets(line, max_len, file) ) {
        exit_status = ORTE_ERROR;
        goto cleanup;
    }
    line_len = strlen(line);
    /* Strip off the new line if it it there */
    if('\n' == line[line_len-1]) {
        line[line_len-1] = '\0';
        line_len--;
        end_of_line = true;
    }
    else {
        end_of_line = false;
    }

    /* Ignore lines with just '#' too */
    if(2 >= line_len)
        goto try_again;

    /*
     * Extract the token from the set
     */
    for(c = 0;
        line[c] != ':' &&
            c < line_len;
        ++c) {
        ;
    }
    c += 2; /* For the ' ' and the '\0' */
    local_token = (char *)malloc(sizeof(char) * (c + 1));

    for(s = 0; s < c; ++s) {
        local_token[s] = line[s];
    }

    local_token[s] = '\0';
    *token = strdup(local_token);

    if( NULL != local_token) {
        free(local_token);
        local_token = NULL;
    }

    /*
     * Extract the value from the set
     */
    local_value = (char *)malloc(sizeof(char) * (line_len - c + 1));
    for(v = 0, s = c;
        s < line_len;
        ++s, ++v) {
        local_value[v] = line[s];
    }

    while(!end_of_line) {
        if (NULL == fgets(line, max_len, file) ) {
            exit_status = ORTE_ERROR;
            goto cleanup;
        }
        line_len = strlen(line);
        /* Strip off the new line if it it there */
        if('\n' == line[line_len-1]) {
            line[line_len-1] = '\0';
            line_len--;
            end_of_line = true;
        }
        else {
            end_of_line = false;
        }

        local_value = (char *)realloc(local_value, sizeof(char) * line_len);
        for(s = 0;
            s < line_len;
            ++s, ++v) {
            local_value[v] = line[s];
        }
    }

    local_value[v] = '\0';
    *value = strdup(local_value);

 cleanup:
    if( NULL != local_token) {
        free(local_token);
        local_token = NULL;
    }

    if( NULL != local_value) {
        free(local_value);
        local_value = NULL;
    }

    if( NULL != line) {
        free(line);
        line = NULL;
    }

    return exit_status;
}
