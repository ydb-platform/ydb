/*
 * Copyright (c) 2004-2010 The Trustees of Indiana University.
 *                         All rights reserved.
 * Copyright (c) 2004-2011 The Trustees of the University of Tennessee.
 *                         All rights reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007      Evergrid, Inc. All rights reserved.
 * Copyright (c) 2013      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2014-2016 Intel, Inc.  All rights reserved.
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
#endif  /* HAVE_SYS_TYPES_H */
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif  /* HAVE_UNISTD_H */
#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif /* HAVE_SYS_TYPES_H */
#ifdef HAVE_SYS_STAT_H
#include <sys/stat.h>
#endif /* HAVE_SYS_STAT_H */
#ifdef HAVE_DIRENT_H
#include <dirent.h>
#endif /* HAVE_DIRENT_H */
#include <time.h>

#include "orte/mca/mca.h"
#include "opal/mca/base/base.h"

#include "opal/util/os_dirpath.h"
#include "opal/util/output.h"
#include "opal/util/show_help.h"
#include "opal/util/basename.h"
#include "opal/util/argv.h"
#include "opal/mca/crs/crs.h"
#include "opal/mca/crs/base/base.h"
#include "opal/dss/dss.h"

#include "orte/mca/rml/rml.h"
#include "orte/mca/rml/rml_types.h"
#include "orte/mca/errmgr/errmgr.h"
#include "orte/runtime/orte_globals.h"
#include "orte/util/name_fns.h"

#include "orte/mca/sstore/sstore.h"
#include "orte/mca/sstore/base/base.h"

#include "orte/mca/snapc/snapc.h"
#include "orte/mca/snapc/base/base.h"

/******************
 * Local Functions
 ******************/
size_t orte_snapc_base_snapshot_seq_number = 0;

/******************
 * Object stuff
 ******************/
OBJ_CLASS_INSTANCE(orte_snapc_base_local_snapshot_t,
                   opal_list_item_t,
                   orte_snapc_base_local_snapshot_construct,
                   orte_snapc_base_local_snapshot_destruct);

void orte_snapc_base_local_snapshot_construct(orte_snapc_base_local_snapshot_t *snapshot)
{
    snapshot->process_name.jobid  = 0;
    snapshot->process_name.vpid   = 0;

    snapshot->state = ORTE_SNAPC_CKPT_STATE_NONE;

    snapshot->ss_handle  = ORTE_SSTORE_HANDLE_INVALID;
}

void orte_snapc_base_local_snapshot_destruct( orte_snapc_base_local_snapshot_t *snapshot)
{
    snapshot->process_name.jobid  = 0;
    snapshot->process_name.vpid   = 0;

    snapshot->state = ORTE_SNAPC_CKPT_STATE_NONE;

    snapshot->ss_handle  = ORTE_SSTORE_HANDLE_INVALID;
}

/****/
OBJ_CLASS_INSTANCE(orte_snapc_base_global_snapshot_t,
                   opal_list_item_t,
                   orte_snapc_base_global_snapshot_construct,
                   orte_snapc_base_global_snapshot_destruct);

void orte_snapc_base_global_snapshot_construct(orte_snapc_base_global_snapshot_t *snapshot)
{
    OBJ_CONSTRUCT(&(snapshot->local_snapshots), opal_list_t);

    snapshot->options = OBJ_NEW(opal_crs_base_ckpt_options_t);

    snapshot->ss_handle  = ORTE_SSTORE_HANDLE_INVALID;
}

void orte_snapc_base_global_snapshot_destruct( orte_snapc_base_global_snapshot_t *snapshot)
{
    opal_list_item_t* item = NULL;

    while (NULL != (item = opal_list_remove_first(&snapshot->local_snapshots))) {
        OBJ_RELEASE(item);
    }
    OBJ_DESTRUCT(&(snapshot->local_snapshots));

    if( NULL != snapshot->options ) {
        OBJ_RELEASE(snapshot->options);
        snapshot->options = NULL;
    }

    snapshot->ss_handle  = ORTE_SSTORE_HANDLE_INVALID;
}

OBJ_CLASS_INSTANCE(orte_snapc_base_quiesce_t,
                   opal_object_t,
                   orte_snapc_base_quiesce_construct,
                   orte_snapc_base_quiesce_destruct);

void orte_snapc_base_quiesce_construct(orte_snapc_base_quiesce_t *quiesce)
{
    quiesce->epoch         = -1;
    quiesce->snapshot      = NULL;
    quiesce->ss_handle     = ORTE_SSTORE_HANDLE_INVALID;
    quiesce->ss_snapshot   = NULL;
    quiesce->handle        = NULL;
    quiesce->target_dir    = NULL;
    quiesce->crs_name      = NULL;
    quiesce->cmdline       = NULL;
    quiesce->cr_state      = OPAL_CRS_NONE;
    quiesce->checkpointing = false;
    quiesce->restarting    = false;

    quiesce->migrating     = false;
    quiesce->num_migrating = 0;
    OBJ_CONSTRUCT(&(quiesce->migrating_procs), opal_pointer_array_t);
    opal_pointer_array_init(&(quiesce->migrating_procs), 8, INT32_MAX, 8);
}

void orte_snapc_base_quiesce_destruct( orte_snapc_base_quiesce_t *quiesce)
{
    int i;
    void *item = NULL;

    quiesce->epoch = -1;

    if( NULL != quiesce->snapshot ) {
        OBJ_RELEASE(quiesce->snapshot);
        quiesce->snapshot      = NULL;
    }

    quiesce->ss_handle     = ORTE_SSTORE_HANDLE_INVALID;
    if( NULL != quiesce->ss_snapshot ) {
        OBJ_RELEASE(quiesce->ss_snapshot);
        quiesce->ss_snapshot   = NULL;
    }

    if( NULL != quiesce->handle ) {
        free(quiesce->handle);
        quiesce->handle = NULL;
    }
    if( NULL != quiesce->target_dir ) {
        free(quiesce->target_dir);
        quiesce->target_dir = NULL;
    }
    if( NULL != quiesce->crs_name ) {
        free(quiesce->crs_name);
        quiesce->crs_name = NULL;
    }
    if( NULL != quiesce->cmdline ) {
        free(quiesce->cmdline);
        quiesce->cmdline = NULL;
    }

    quiesce->cr_state      = OPAL_CRS_NONE;
    quiesce->checkpointing = false;
    quiesce->restarting    = false;

    quiesce->migrating     = false;
    quiesce->num_migrating = 0;
    for( i = 0; i < quiesce->migrating_procs.size; ++i) {
        item = opal_pointer_array_get_item(&(quiesce->migrating_procs), i);
        if( NULL != item ) {
            OBJ_RELEASE(item);
        }
    }
    OBJ_DESTRUCT(&(quiesce->migrating_procs));
}

OBJ_CLASS_INSTANCE(orte_snapc_base_request_op_t,
                   opal_object_t,
                   orte_snapc_base_request_op_construct,
                   orte_snapc_base_request_op_destruct);

void orte_snapc_base_request_op_construct(orte_snapc_base_request_op_t *op)
{
    op->event     = ORTE_SNAPC_OP_NONE;
    op->is_active = false;
    op->leader    = -1;

    op->seq_num       = -1;
    op->global_handle = NULL;
    op->ss_handle     = ORTE_SSTORE_HANDLE_INVALID;

    op->mig_num       = -1;
    op->mig_vpids     = NULL;
    /*op->mig_host_pref = NULL;*/
    op->mig_vpid_pref = NULL;
    op->mig_off_node  = NULL;
}

void orte_snapc_base_request_op_destruct( orte_snapc_base_request_op_t *op)
{
    op->event     = ORTE_SNAPC_OP_NONE;
    op->is_active = false;
    op->leader    = -1;

    op->seq_num       = -1;
    if(NULL != op->global_handle ) {
        free(op->global_handle);
        op->global_handle = NULL;
    }

    op->ss_handle     = ORTE_SSTORE_HANDLE_INVALID;

    op->mig_num       = -1;
    /*
    if( NULL != op->mig_vpids ) {
        free( op->mig_vpids );
        op->mig_vpids = NULL;
    }

    if( NULL != op->mig_host_pref ) {
        free( op->mig_host_pref );
        op->mig_host_pref = NULL;
    }

    if( NULL != op->mig_vpid_pref ) {
        free( op->mig_vpid_pref );
        op->mig_vpid_pref = NULL;
    }

    if( NULL != op->mig_off_node ) {
        free( op->mig_off_node );
        op->mig_off_node = NULL;
    }
    */
}


/***********************
 * None component stuff
 ************************/
int orte_snapc_base_none_open(void)
{
    return ORTE_SUCCESS;
}

int orte_snapc_base_none_close(void)
{
    return ORTE_SUCCESS;
}

int orte_snapc_base_none_query(mca_base_module_t **module, int *priority)
{
    *module = NULL;
    *priority = 0;

    return OPAL_SUCCESS;
}

int orte_snapc_base_module_init(bool seed, bool app)
{
    return ORTE_SUCCESS;
}

int orte_snapc_base_module_finalize(void)
{
    return ORTE_SUCCESS;
}

/* None RML command line response callback */
static void snapc_none_global_cmdline_request(int status,
                                              orte_process_name_t* sender,
                                              opal_buffer_t *buffer,
                                              orte_rml_tag_t tag,
                                              void* cbdata);
int orte_snapc_base_none_setup_job(orte_jobid_t jobid)
{

    /*
     * Coordinator command listener
     */
    orte_snapc_base_snapshot_seq_number = -1;
    orte_rml.recv_buffer_nb(ORTE_NAME_WILDCARD,
                            ORTE_RML_TAG_CKPT,
                            ORTE_RML_PERSISTENT,
                            snapc_none_global_cmdline_request,
                            NULL);

    return ORTE_SUCCESS;
}

int orte_snapc_base_none_release_job(orte_jobid_t jobid)
{
    /*
     * Remove the checkpoint request callback
     */

    return ORTE_SUCCESS;
}

int orte_snapc_base_none_ft_event(int state)
{
    return ORTE_SUCCESS;
}

int orte_snapc_base_none_start_ckpt(orte_snapc_base_quiesce_t *datum)
{
    return ORTE_SUCCESS;
}

int orte_snapc_base_none_end_ckpt(orte_snapc_base_quiesce_t *datum)
{
    return ORTE_SUCCESS;
}


/********************
 * Local Functions
 ********************/
/* None RML response callback */
static void snapc_none_global_cmdline_request(int status,
                                              orte_process_name_t* sender,
                                              opal_buffer_t *buffer,
                                              orte_rml_tag_t tag,
                                              void* cbdata)
{
    int ret;
    orte_snapc_cmd_flag_t command;
    orte_std_cntr_t n = 1;
    opal_crs_base_ckpt_options_t *options = NULL;
    orte_jobid_t jobid;

    options = OBJ_NEW(opal_crs_base_ckpt_options_t);

    n = 1;
    if (ORTE_SUCCESS != (ret = opal_dss.unpack(buffer, &command, &n, ORTE_SNAPC_CMD))) {
        ORTE_ERROR_LOG(ret);
        goto cleanup;
    }

    /*
     * orte_checkpoint has requested that a checkpoint be taken
     * Respond that a checkpoint cannot be taken at this time
     */
    if (ORTE_SNAPC_GLOBAL_INIT_CMD == command) {
        /*
         * Do the basic handshake with the orte_checkpoint command
         */
        if( ORTE_SUCCESS != (ret = orte_snapc_base_global_coord_ckpt_init_cmd(sender, buffer, options, &jobid)) ) {
            ORTE_ERROR_LOG(ret);
            goto cleanup;
        }

        /*
         * Respond with an invalid response
         */
        if( ORTE_SUCCESS != (ret = orte_snapc_base_global_coord_ckpt_update_cmd(sender, 0, ORTE_SNAPC_CKPT_STATE_NO_CKPT)) ) {
            ORTE_ERROR_LOG(ret);
            goto cleanup;
        }
    }
    /*
     * Unknown command
     */
    else {
        ORTE_ERROR_LOG(ret);
        goto cleanup;
    }

 cleanup:
    if( NULL != options ) {
        OBJ_RELEASE(options);
        options = NULL;
    }

    return;
}

/********************
 * Utility functions
 ********************/

/* Report the checkpoint status */
void orte_snapc_ckpt_state_notify(int state)
{
    switch(state) {
    case ORTE_SNAPC_CKPT_STATE_ESTABLISHED:
        opal_output(0, "%d: Checkpoint established for process %s.",
                    orte_process_info.pid, ORTE_JOBID_PRINT(ORTE_PROC_MY_NAME->jobid));
        break;
    case ORTE_SNAPC_CKPT_STATE_NO_CKPT:
        opal_output(0, "%d: Process %s is not checkpointable.",
                    orte_process_info.pid, ORTE_JOBID_PRINT(ORTE_PROC_MY_NAME->jobid));
        break;
    case ORTE_SNAPC_CKPT_STATE_ERROR:
        opal_output(0, "%d: Failed to checkpoint process %s.",
                    orte_process_info.pid, ORTE_JOBID_PRINT(ORTE_PROC_MY_NAME->jobid));
        break;
    case ORTE_SNAPC_CKPT_STATE_RECOVERED:
        opal_output(0, "%d: Successfully restarted process %s.",
                    orte_process_info.pid, ORTE_JOBID_PRINT(ORTE_PROC_MY_NAME->jobid));
        break;
    case ORTE_SNAPC_CKPT_STATE_NO_RESTART:
        opal_output(0, "%d: Failed to restart process %s.",
                    orte_process_info.pid, ORTE_JOBID_PRINT(ORTE_PROC_MY_NAME->jobid));
        break;
    /* ADK: We currently do not notify for these states, but good to
     * have them around anyways. */
    case ORTE_SNAPC_CKPT_STATE_NONE:
    case ORTE_SNAPC_CKPT_STATE_REQUEST:
    case ORTE_SNAPC_CKPT_STATE_PENDING:
    case ORTE_SNAPC_CKPT_STATE_RUNNING:
    case ORTE_SNAPC_CKPT_STATE_STOPPED:
    case ORTE_SNAPC_CKPT_STATE_MIGRATING:
    case ORTE_SNAPC_CKPT_STATE_FINISHED_LOCAL:
    default:
        break;
    }
}

int orte_snapc_base_global_coord_ckpt_init_cmd(orte_process_name_t* peer,
                                               opal_buffer_t* buffer,
                                               opal_crs_base_ckpt_options_t *options,
                                               orte_jobid_t *jobid)
{
    int ret, exit_status = ORTE_SUCCESS;
    orte_std_cntr_t count = 1;
    orte_ns_cmp_bitmask_t mask;

    mask = ORTE_NS_CMP_ALL;

    /*
     * Do not send to self, as that is silly.
     */
    if (OPAL_EQUAL ==
            orte_util_compare_name_fields(mask, peer, ORTE_PROC_MY_HNP)) {
        OPAL_OUTPUT_VERBOSE((10, orte_snapc_base_framework.framework_output,
                             "%s) base:ckpt_init_cmd: Error: Do not send to self!\n",
                             ORTE_SNAPC_COORD_NAME_PRINT(orte_snapc_coord_type)));
        return ORTE_SUCCESS;
    }

    OPAL_OUTPUT_VERBOSE((10, orte_snapc_base_framework.framework_output,
                         "%s) base:ckpt_init_cmd: Receiving commands\n",
                         ORTE_SNAPC_COORD_NAME_PRINT(orte_snapc_coord_type)));

    /********************
     * Receive command line checkpoint request:
     * - Command (already received)
     * - options
     * - jobid
     ********************/
    if( ORTE_SUCCESS != (ret = orte_snapc_base_unpack_options(buffer, options)) ) {
        opal_output(orte_snapc_base_framework.framework_output,
                    "%s) base:ckpt_init_cmd: Error: Unpack (options) Failure (ret = %d)\n",
                    ORTE_SNAPC_COORD_NAME_PRINT(orte_snapc_coord_type), ret );
        ORTE_ERROR_LOG(ret);
        exit_status = ret;
        goto cleanup;
    }

    count = 1;
    if ( ORTE_SUCCESS != (ret = opal_dss.unpack(buffer, jobid, &count, ORTE_JOBID)) ) {
        opal_output(orte_snapc_base_framework.framework_output,
                    "%s) base:ckpt_init_cmd: Error: DSS Unpack (jobid) Failure (ret = %d) (LINE = %d)\n",
                    ORTE_SNAPC_COORD_NAME_PRINT(orte_snapc_coord_type),
                    ret, __LINE__);
        ORTE_ERROR_LOG(ret);
        exit_status = ret;
        goto cleanup;
    }

    OPAL_OUTPUT_VERBOSE((10, orte_snapc_base_framework.framework_output,
                         "%s) base:ckpt_init_cmd: Received [%d, %d, %s]\n",
                         ORTE_SNAPC_COORD_NAME_PRINT(orte_snapc_coord_type),
                         (int)(options->term),
                         (int)(options->stop),
                         ORTE_JOBID_PRINT(*jobid)));

 cleanup:
    return exit_status;
}

int orte_snapc_base_unpack_options(opal_buffer_t* buffer,
                                   opal_crs_base_ckpt_options_t *options)
{
    int ret, exit_status = ORTE_SUCCESS;
    orte_std_cntr_t count = 1;

    count = 1;
    if ( ORTE_SUCCESS != (ret = opal_dss.unpack(buffer, &(options->term), &count, OPAL_BOOL)) ) {
        opal_output(orte_snapc_base_framework.framework_output,
                    "snapc:base:unpack_options: Error: Unpack (term) Failure (ret = %d)\n",
                    ret);
        ORTE_ERROR_LOG(ret);
        exit_status = ret;
        goto cleanup;
    }

    count = 1;
    if ( ORTE_SUCCESS != (ret = opal_dss.unpack(buffer, &(options->stop), &count, OPAL_BOOL)) ) {
        opal_output(orte_snapc_base_framework.framework_output,
                    "snapc:base:unpack_options: Error: Unpack (stop) Failure (ret = %d)\n",
                    ret);
        ORTE_ERROR_LOG(ret);
        exit_status = ret;
        goto cleanup;
    }

    count = 1;
    if ( ORTE_SUCCESS != (ret = opal_dss.unpack(buffer, &(options->inc_prep_only), &count, OPAL_BOOL)) ) {
        opal_output(orte_snapc_base_framework.framework_output,
                    "snapc:base:unpack_options: Error: Unpack (inc_prep_only) Failure (ret = %d)\n",
                    ret);
        ORTE_ERROR_LOG(ret);
        exit_status = ret;
        goto cleanup;
    }

    count = 1;
    if ( ORTE_SUCCESS != (ret = opal_dss.unpack(buffer, &(options->inc_recover_only), &count, OPAL_BOOL)) ) {
        opal_output(orte_snapc_base_framework.framework_output,
                    "snapc:base:unpack_options: Error: Unpack (inc_recover_only) Failure (ret = %d)\n",
                    ret);
        ORTE_ERROR_LOG(ret);
        exit_status = ret;
        goto cleanup;
    }

#if OPAL_ENABLE_CRDEBUG == 1
    count = 1;
    if ( ORTE_SUCCESS != (ret = opal_dss.unpack(buffer, &(options->attach_debugger), &count, OPAL_BOOL)) ) {
        opal_output(orte_snapc_base_framework.framework_output,
                    "snapc:base:unpack_options: Error: Unpack (attach_debugger) Failure (ret = %d)\n",
                    ret);
        ORTE_ERROR_LOG(ret);
        exit_status = ret;
        goto cleanup;
    }

    count = 1;
    if ( ORTE_SUCCESS != (ret = opal_dss.unpack(buffer, &(options->detach_debugger), &count, OPAL_BOOL)) ) {
        opal_output(orte_snapc_base_framework.framework_output,
                    "snapc:base:unpack_options: Error: Unpack (detach_debugger) Failure (ret = %d)\n",
                    ret);
        ORTE_ERROR_LOG(ret);
        exit_status = ret;
        goto cleanup;
    }
#endif

 cleanup:
    return exit_status;
}

int orte_snapc_base_pack_options(opal_buffer_t* buffer,
                                 opal_crs_base_ckpt_options_t *options)
{
    int ret, exit_status = ORTE_SUCCESS;

    if (ORTE_SUCCESS != (ret = opal_dss.pack(buffer, &(options->term), 1, OPAL_BOOL))) {
        ORTE_ERROR_LOG(ret);
        exit_status = ret;
        goto cleanup;
    }

    if (ORTE_SUCCESS != (ret = opal_dss.pack(buffer, &(options->stop), 1, OPAL_BOOL))) {
        ORTE_ERROR_LOG(ret);
        exit_status = ret;
        goto cleanup;
    }

    if (ORTE_SUCCESS != (ret = opal_dss.pack(buffer, &(options->inc_prep_only), 1, OPAL_BOOL))) {
        ORTE_ERROR_LOG(ret);
        exit_status = ret;
        goto cleanup;
    }

    if (ORTE_SUCCESS != (ret = opal_dss.pack(buffer, &(options->inc_recover_only), 1, OPAL_BOOL))) {
        ORTE_ERROR_LOG(ret);
        exit_status = ret;
        goto cleanup;
    }

#if OPAL_ENABLE_CRDEBUG == 1
    if (ORTE_SUCCESS != (ret = opal_dss.pack(buffer, &(options->attach_debugger), 1, OPAL_BOOL))) {
        ORTE_ERROR_LOG(ret);
        exit_status = ret;
        goto cleanup;
    }

    if (ORTE_SUCCESS != (ret = opal_dss.pack(buffer, &(options->detach_debugger), 1, OPAL_BOOL))) {
        ORTE_ERROR_LOG(ret);
        exit_status = ret;
        goto cleanup;
    }
#endif

 cleanup:
    return exit_status;
}

int orte_snapc_base_global_coord_ckpt_update_cmd(orte_process_name_t* peer,
                                                 orte_sstore_base_handle_t ss_handle,
                                                 int ckpt_status)
{
    int ret, exit_status = ORTE_SUCCESS;
    opal_buffer_t *loc_buffer = NULL;
    orte_snapc_cmd_flag_t command = ORTE_SNAPC_GLOBAL_UPDATE_CMD;
    char *global_snapshot_handle = NULL;
    char *tmp_str = NULL;
    int seq_num;
    orte_ns_cmp_bitmask_t mask;

    /*
     * Noop if invalid peer, or peer not specified (JJH Double check this)
     */
    if( NULL == peer ||
        OPAL_EQUAL == orte_util_compare_name_fields(ORTE_NS_CMP_ALL, ORTE_NAME_INVALID, peer) ) {
        /*return ORTE_ERR_BAD_PARAM;*/
        return ORTE_SUCCESS;
    }

    mask = ORTE_NS_CMP_ALL;

    /*
     * Do not send to self, as that is silly.
     */
    if (OPAL_EQUAL == orte_util_compare_name_fields(mask, peer, ORTE_PROC_MY_HNP)) {
        OPAL_OUTPUT_VERBOSE((10, orte_snapc_base_framework.framework_output,
                             "%s) base:ckpt_update_cmd: Error: Do not send to self!\n",
                             ORTE_SNAPC_COORD_NAME_PRINT(orte_snapc_coord_type)));
        return ORTE_SUCCESS;
    }

    /*
     * Pass on the checkpoint state.
     */
    orte_snapc_ckpt_state_notify(ckpt_status);

    OPAL_OUTPUT_VERBOSE((10, orte_snapc_base_framework.framework_output,
                         "%s) base:ckpt_update_cmd: Sending update command <status %d>\n",
                         ORTE_SNAPC_COORD_NAME_PRINT(orte_snapc_coord_type),
                         ckpt_status));

    /********************
     * Send over the status of the checkpoint
     * - ckpt_state
     * - global snapshot handle (upon finish only)
     * - sequence number        (upon finish only)
     ********************/
    if (NULL == (loc_buffer = OBJ_NEW(opal_buffer_t))) {
        exit_status = ORTE_ERROR;
        goto cleanup;
    }

    if (ORTE_SUCCESS != (ret = opal_dss.pack(loc_buffer, &command, 1, ORTE_SNAPC_CMD)) ) {
        ORTE_ERROR_LOG(ret);
        exit_status = ret;
        OBJ_RELEASE(loc_buffer);
        goto cleanup;
    }

    if (ORTE_SUCCESS != (ret = opal_dss.pack(loc_buffer, &ckpt_status, 1, OPAL_INT))) {
        opal_output(orte_snapc_base_framework.framework_output,
                    "%s) base:ckpt_update_cmd: Error: DSS Pack (ckpt_status) Failure (ret = %d) (LINE = %d)\n",
                    ORTE_SNAPC_COORD_NAME_PRINT(orte_snapc_coord_type),
                    ret, __LINE__);
        ORTE_ERROR_LOG(ret);
        exit_status = ret;
        OBJ_RELEASE(loc_buffer);
        goto cleanup;
    }

    if( ORTE_SNAPC_CKPT_STATE_RECOVERED == ckpt_status ||
        ORTE_SNAPC_CKPT_STATE_ESTABLISHED  == ckpt_status ||
        ORTE_SNAPC_CKPT_STATE_STOPPED   == ckpt_status ||
        ORTE_SNAPC_CKPT_STATE_ERROR     == ckpt_status ) {

        if( ORTE_SNAPC_CKPT_STATE_ERROR != ckpt_status ) {
            if( ORTE_SUCCESS != (ret = orte_sstore.get_attr(ss_handle,
                                                            SSTORE_METADATA_GLOBAL_SNAP_REF,
                                                            &global_snapshot_handle)) ) {
                opal_output(orte_snapc_base_framework.framework_output,
                            "%s) base:ckpt_update_cmd: Error: SStore get_attr failed (ret = %d)\n",
                            ORTE_SNAPC_COORD_NAME_PRINT(orte_snapc_coord_type), ret );
                ORTE_ERROR_LOG(ret);
                /* Do not exit here, continue so that we can inform the tool
                 * that the checkpoint has failed
                 */
            }

            if( ORTE_SUCCESS != (ret = orte_sstore.get_attr(ss_handle,
                                                            SSTORE_METADATA_GLOBAL_SNAP_SEQ,
                                                            &tmp_str)) ) {
                opal_output(orte_snapc_base_framework.framework_output,
                            "%s) base:ckpt_update_cmd: Error: SStore get_attr failed (ret = %d)\n",
                            ORTE_SNAPC_COORD_NAME_PRINT(orte_snapc_coord_type), ret );
                ORTE_ERROR_LOG(ret);
                /* Do not exit here, continue so that we can inform the tool
                 * that the checkpoint has failed
                 */
            }

            if( NULL != tmp_str ) {
                seq_num = atoi(tmp_str);
            } else {
                seq_num = -1;
            }
        } else {
            /* Checkpoint Error Case */
            global_snapshot_handle = NULL;
            seq_num = -1;
        }

        OPAL_OUTPUT_VERBOSE((10, orte_snapc_base_framework.framework_output,
                             "%s) base:ckpt_update_cmd: Sending update command <status %d> + <ref %s> <seq %d>\n",
                             ORTE_SNAPC_COORD_NAME_PRINT(orte_snapc_coord_type),
                             ckpt_status, global_snapshot_handle, seq_num));

        if (ORTE_SUCCESS != (ret = opal_dss.pack(loc_buffer, &global_snapshot_handle, 1, OPAL_STRING))) {
            opal_output(orte_snapc_base_framework.framework_output,
                        "%s) base:ckpt_update_cmd: Error: DSS Pack (snapshot handle) Failure (ret = %d) (LINE = %d)\n",
                        ORTE_SNAPC_COORD_NAME_PRINT(orte_snapc_coord_type),
                        ret, __LINE__);
            ORTE_ERROR_LOG(ret);
            exit_status = ret;
            OBJ_RELEASE(loc_buffer);
            goto cleanup;
        }

        if (ORTE_SUCCESS != (ret = opal_dss.pack(loc_buffer, &seq_num, 1, OPAL_INT))) {
            opal_output(orte_snapc_base_framework.framework_output,
                        "%s) base:ckpt_update_cmd: Error: DSS Pack (seq number) Failure (ret = %d) (LINE = %d)\n",
                        ORTE_SNAPC_COORD_NAME_PRINT(orte_snapc_coord_type),
                        ret, __LINE__);
            ORTE_ERROR_LOG(ret);
            exit_status = ret;
            OBJ_RELEASE(loc_buffer);
            goto cleanup;
        }
    }

    if (0 > (ret = orte_rml.send_buffer_nb(orte_mgmt_conduit,
                                           peer, loc_buffer,
                                           ORTE_RML_TAG_CKPT,
                                           orte_rml_send_callback, NULL))) {
        opal_output(orte_snapc_base_framework.framework_output,
                    "%s) base:ckpt_update_cmd: Error: Send (ckpt_status) Failure (ret = %d) (LINE = %d)\n",
                    ORTE_SNAPC_COORD_NAME_PRINT(orte_snapc_coord_type),
                    ret, __LINE__);
        ORTE_ERROR_LOG(ret);
        exit_status = ret;
        OBJ_RELEASE(loc_buffer);
        goto cleanup;
    }

 cleanup:
    if( NULL != global_snapshot_handle ){
        free(global_snapshot_handle);
        global_snapshot_handle = NULL;
    }
    if( NULL != tmp_str ) {
        free(tmp_str);
        tmp_str = NULL;
    }

    return exit_status;
}

/****************************
 * Command line tool request functions
 ****************************/
/* JJH TODO - Move the command line functions here ? */

/*****************************
 * Snapshot metadata functions
 *****************************/
int orte_snapc_ckpt_state_str(char ** state_str, int state)
{
    switch(state) {
    case ORTE_SNAPC_CKPT_STATE_NONE:
        *state_str = strdup(" -- ");
        break;
    case ORTE_SNAPC_CKPT_STATE_REQUEST:
        *state_str = strdup("Requested");
        break;
    case ORTE_SNAPC_CKPT_STATE_PENDING:
        *state_str = strdup("Pending");
        break;
    case ORTE_SNAPC_CKPT_STATE_RUNNING:
        *state_str = strdup("Running");
        break;
    case ORTE_SNAPC_CKPT_STATE_STOPPED:
        *state_str = strdup("Stopped");
        break;
    case ORTE_SNAPC_CKPT_STATE_MIGRATING:
        *state_str = strdup("Migrating");
        break;
    case ORTE_SNAPC_CKPT_STATE_ESTABLISHED:
        *state_str = strdup("Checkpoint Established");
        break;
    case ORTE_SNAPC_CKPT_STATE_RECOVERED:
        *state_str = strdup("Continuing/Recovered");
        break;
    case ORTE_SNAPC_CKPT_STATE_FINISHED_LOCAL:
        *state_str = strdup("Locally Finished");
        break;
    case ORTE_SNAPC_CKPT_STATE_ERROR:
        *state_str = strdup("Error");
        break;
    default:
        asprintf(state_str, "Unknown %d", state);
        break;
    }

    return ORTE_SUCCESS;
}
