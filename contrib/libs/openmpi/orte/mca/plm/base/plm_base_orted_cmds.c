/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2011 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2011-2012 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2014      Intel, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 */

#include "orte_config.h"
#include "orte/constants.h"

#include <string.h>
#ifdef HAVE_SYS_TIME_H
#include <sys/time.h>
#endif


#include "opal/dss/dss.h"
#include "opal/mca/event/event.h"

#include "orte/mca/odls/odls_types.h"
#include "orte/mca/grpcomm/base/base.h"
#include "orte/mca/errmgr/errmgr.h"
#include "orte/mca/ess/ess.h"
#include "orte/mca/rml/rml.h"
#include "orte/mca/rml/rml_types.h"
#include "orte/runtime/orte_globals.h"
#include "orte/runtime/orte_wait.h"
#include "orte/util/name_fns.h"
#include "orte/util/proc_info.h"
#include "orte/mca/state/state.h"
#include "orte/runtime/orte_wait.h"
#include "orte/orted/orted.h"

#include "orte/mca/plm/base/base.h"
#include "orte/mca/plm/base/plm_private.h"

#if 0
static void failed_cmd(int fd, short event, void *cbdata)
{
    orte_timer_t *tm = (orte_timer_t*)cbdata;

    /* we get called if an abnormal term
     * don't complete in time - just force exit
     */
    OPAL_OUTPUT_VERBOSE((5, orte_plm_base_framework.framework_output,
                         "%s plm:base:orted_cmd command timed out",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));
    OBJ_RELEASE(tm);
/*
    ORTE_FORCED_TERMINATE(ORTE_ERROR_DEFAULT_EXIT_CODE);
*/
}
#endif

int orte_plm_base_orted_exit(orte_daemon_cmd_flag_t command)
{
    int rc;
    opal_buffer_t *cmd;
    orte_daemon_cmd_flag_t cmmnd;
    orte_grpcomm_signature_t *sig;

    OPAL_OUTPUT_VERBOSE((5, orte_plm_base_framework.framework_output,
                         "%s plm:base:orted_cmd sending orted_exit commands",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));

    /* flag that orteds are being terminated */
    orte_orteds_term_ordered = true;
    cmmnd = command;

    /* if we are terminating before launch, or abnormally
     * terminating, then the daemons may not be wired up
     * and therefore cannot depend on detecting their
     * routed children to determine termination
     */
    if (orte_abnormal_term_ordered ||
        orte_never_launched ||
        !orte_routing_is_enabled) {
        cmmnd = ORTE_DAEMON_HALT_VM_CMD;
    }

    /* send it express delivery! */
    cmd = OBJ_NEW(opal_buffer_t);
    /* pack the command */
    if (ORTE_SUCCESS != (rc = opal_dss.pack(cmd, &cmmnd, 1, ORTE_DAEMON_CMD))) {
        ORTE_ERROR_LOG(rc);
        OBJ_RELEASE(cmd);
        return rc;
    }
    /* goes to all daemons */
    sig = OBJ_NEW(orte_grpcomm_signature_t);
    sig->signature = (orte_process_name_t*)malloc(sizeof(orte_process_name_t));
    sig->signature[0].jobid = ORTE_PROC_MY_NAME->jobid;
    sig->signature[0].vpid = ORTE_VPID_WILDCARD;
    if (ORTE_SUCCESS != (rc = orte_grpcomm.xcast(sig, ORTE_RML_TAG_DAEMON, cmd))) {
        ORTE_ERROR_LOG(rc);
    }
    OBJ_RELEASE(cmd);
    OBJ_RELEASE(sig);

#if 0
    /* if we are abnormally ordering the termination, then
     * set a timeout in case it never finishes
     */
    if (orte_abnormal_term_ordered) {
        ORTE_DETECT_TIMEOUT(orte_process_info.num_procs, 100, 3, failed_cmd, NULL);
    }
#endif
    return rc;
}


int orte_plm_base_orted_terminate_job(orte_jobid_t jobid)
{
    opal_pointer_array_t procs;
    orte_proc_t proc;
    int rc;

    OPAL_OUTPUT_VERBOSE((5, orte_plm_base_framework.framework_output,
                         "%s plm:base:orted_terminate job %s",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         ORTE_JOBID_PRINT(jobid)));

    OBJ_CONSTRUCT(&procs, opal_pointer_array_t);
    opal_pointer_array_init(&procs, 1, 1, 1);
    OBJ_CONSTRUCT(&proc, orte_proc_t);
    proc.name.jobid = jobid;
    proc.name.vpid = ORTE_VPID_WILDCARD;
    opal_pointer_array_add(&procs, &proc);
    if (ORTE_SUCCESS != (rc = orte_plm_base_orted_kill_local_procs(&procs))) {
        ORTE_ERROR_LOG(rc);
    }
    OBJ_DESTRUCT(&procs);
    OBJ_DESTRUCT(&proc);
    return rc;
}

int orte_plm_base_orted_kill_local_procs(opal_pointer_array_t *procs)
{
    int rc;
    opal_buffer_t *cmd;
    orte_daemon_cmd_flag_t command=ORTE_DAEMON_KILL_LOCAL_PROCS;
    int v;
    orte_proc_t *proc;
    orte_grpcomm_signature_t *sig;

    OPAL_OUTPUT_VERBOSE((5, orte_plm_base_framework.framework_output,
                         "%s plm:base:orted_cmd sending kill_local_procs cmds",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));

    cmd = OBJ_NEW(opal_buffer_t);
    /* pack the command */
    if (ORTE_SUCCESS != (rc = opal_dss.pack(cmd, &command, 1, ORTE_DAEMON_CMD))) {
        ORTE_ERROR_LOG(rc);
        OBJ_RELEASE(cmd);
        return rc;
    }

    /* pack the proc names */
    if (NULL != procs) {
        for (v=0; v < procs->size; v++) {
            if (NULL == (proc = (orte_proc_t*)opal_pointer_array_get_item(procs, v))) {
                continue;
            }
            if (ORTE_SUCCESS != (rc = opal_dss.pack(cmd, &(proc->name), 1, ORTE_NAME))) {
                ORTE_ERROR_LOG(rc);
                OBJ_RELEASE(cmd);
                return rc;
            }
        }
    }
    /* goes to all daemons */
    sig = OBJ_NEW(orte_grpcomm_signature_t);
    sig->signature = (orte_process_name_t*)malloc(sizeof(orte_process_name_t));
    sig->signature[0].jobid = ORTE_PROC_MY_NAME->jobid;
    sig->signature[0].vpid = ORTE_VPID_WILDCARD;
    if (ORTE_SUCCESS != (rc = orte_grpcomm.xcast(sig, ORTE_RML_TAG_DAEMON, cmd))) {
        ORTE_ERROR_LOG(rc);
    }
    OBJ_RELEASE(cmd);
    OBJ_RELEASE(sig);

    /* we're done! */
    return rc;
}


int orte_plm_base_orted_signal_local_procs(orte_jobid_t job, int32_t signal)
{
    int rc;
    opal_buffer_t cmd;
    orte_daemon_cmd_flag_t command=ORTE_DAEMON_SIGNAL_LOCAL_PROCS;
    orte_grpcomm_signature_t *sig;

    OPAL_OUTPUT_VERBOSE((5, orte_plm_base_framework.framework_output,
                         "%s plm:base:orted_cmd sending signal_local_procs cmds",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));

    OBJ_CONSTRUCT(&cmd, opal_buffer_t);

    /* pack the command */
    if (ORTE_SUCCESS != (rc = opal_dss.pack(&cmd, &command, 1, ORTE_DAEMON_CMD))) {
        ORTE_ERROR_LOG(rc);
        OBJ_DESTRUCT(&cmd);
        return rc;
    }

    /* pack the jobid */
    if (ORTE_SUCCESS != (rc = opal_dss.pack(&cmd, &job, 1, ORTE_JOBID))) {
        ORTE_ERROR_LOG(rc);
        OBJ_DESTRUCT(&cmd);
        return rc;
    }

    /* pack the signal */
    if (ORTE_SUCCESS != (rc = opal_dss.pack(&cmd, &signal, 1, OPAL_INT32))) {
        ORTE_ERROR_LOG(rc);
        OBJ_DESTRUCT(&cmd);
        return rc;
    }

    /* goes to all daemons */
    sig = OBJ_NEW(orte_grpcomm_signature_t);
    sig->signature = (orte_process_name_t*)malloc(sizeof(orte_process_name_t));
    sig->signature[0].jobid = ORTE_PROC_MY_NAME->jobid;
    sig->signature[0].vpid = ORTE_VPID_WILDCARD;
    if (ORTE_SUCCESS != (rc = orte_grpcomm.xcast(sig, ORTE_RML_TAG_DAEMON, &cmd))) {
        ORTE_ERROR_LOG(rc);
    }
    OBJ_DESTRUCT(&cmd);
    OBJ_RELEASE(sig);

    /* we're done! */
    return ORTE_SUCCESS;
}
