/*
 * Copyright (c) 2009-2010 The Trustees of Indiana University.
 *                         All rights reserved.
 * Copyright (c) 2010-2013 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2010-2011 Oak Ridge National Labs.  All rights reserved.
 * Copyright (c) 2004-2011 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2011-2013 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2014-2018 Intel, Inc.  All rights reserved.
 * Copyright (c) 2017      IBM Corporation. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "orte_config.h"

#include <sys/types.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif  /* HAVE_UNISTD_H */
#include <string.h>

#include "opal/util/output.h"
#include "opal/dss/dss.h"

#include "orte/util/error_strings.h"
#include "orte/util/name_fns.h"
#include "orte/util/proc_info.h"
#include "orte/util/session_dir.h"
#include "orte/util/show_help.h"
#include "orte/util/threads.h"

#include "orte/mca/iof/base/base.h"
#include "orte/mca/rml/rml.h"
#include "orte/mca/odls/odls.h"
#include "orte/mca/odls/base/base.h"
#include "orte/mca/odls/base/odls_private.h"
#include "orte/mca/plm/plm_types.h"
#include "orte/mca/routed/routed.h"
#include "orte/mca/ess/ess.h"
#include "orte/mca/state/state.h"

#include "orte/runtime/orte_wait.h"
#include "orte/runtime/orte_quit.h"
#include "orte/runtime/orte_globals.h"
#include "orte/runtime/data_type_support/orte_dt_support.h"

#include "orte/mca/errmgr/errmgr.h"
#include "orte/mca/errmgr/base/base.h"
#include "orte/mca/errmgr/base/errmgr_private.h"

#include "errmgr_default_orted.h"

/*
 * Module functions: Global
 */
static int init(void);
static int finalize(void);
static void orted_abort(int error_code, char *fmt, ...);

/******************
 * default_orted module
 ******************/
orte_errmgr_base_module_t orte_errmgr_default_orted_module = {
    .init = init,
    .finalize = finalize,
    .logfn = orte_errmgr_base_log,
    .abort = orted_abort,
    .abort_peers = orte_errmgr_base_abort_peers
};

/* Local functions */
static bool any_live_children(orte_jobid_t job);
static int pack_state_update(opal_buffer_t *alert, orte_job_t *jobdat);
static int pack_state_for_proc(opal_buffer_t *alert, orte_proc_t *child);
static void failed_start(orte_job_t *jobdat);
static void killprocs(orte_jobid_t job, orte_vpid_t vpid);

static void job_errors(int fd, short args, void *cbdata);
static void proc_errors(int fd, short args, void *cbdata);

/************************
 * API Definitions
 ************************/
static int init(void)
{
    /* setup state machine to trap job errors */
    orte_state.add_job_state(ORTE_JOB_STATE_ERROR, job_errors, ORTE_ERROR_PRI);

    /* set the lost connection state to run at MSG priority so
     * we can process any last messages from the proc
     */
    orte_state.add_proc_state(ORTE_PROC_STATE_COMM_FAILED, proc_errors, ORTE_MSG_PRI);

    /* setup state machine to trap proc errors */
    orte_state.add_proc_state(ORTE_PROC_STATE_ERROR, proc_errors, ORTE_ERROR_PRI);

    return ORTE_SUCCESS;
}

static int finalize(void)
{
    return ORTE_SUCCESS;
}

static void wakeup(int sd, short args, void *cbdata)
{
    /* nothing more we can do */
    ORTE_ACQUIRE_OBJECT(cbdata);
    orte_quit(0, 0, NULL);
}

/* this function only gets called when FORCED_TERMINATE
 * has been invoked, which means that there is some
 * internal failure (e.g., to pack/unpack a correct value).
 * We could just exit, but that doesn't result in any
 * meaningful error message to the user. Likewise, just
 * printing something to stdout/stderr won't necessarily
 * get back to the user. Instead, we will send an error
 * report to mpirun and give it a chance to order our
 * termination. In order to ensure we _do_ terminate,
 * we set a timer - if it fires before we receive the
 * termination command, then we will exit on our own. This
 * protects us in the case that the failure is in the
 * messaging system itself */
static void orted_abort(int error_code, char *fmt, ...)
{
    va_list arglist;
    char *outmsg = NULL;
    orte_plm_cmd_flag_t cmd;
    opal_buffer_t *alert;
    orte_vpid_t null=ORTE_VPID_INVALID;
    orte_proc_state_t state = ORTE_PROC_STATE_CALLED_ABORT;
    orte_timer_t *timer;
    int rc;

    /* only do this once */
    if (orte_abnormal_term_ordered) {
        return;
    }

    /* set the aborting flag */
    orte_abnormal_term_ordered = true;

    /* If there was a message, construct it */
    va_start(arglist, fmt);
    if (NULL != fmt) {
        vasprintf(&outmsg, fmt, arglist);
    }
    va_end(arglist);

    /* use the show-help system to get the message out */
    orte_show_help("help-errmgr-base.txt", "simple-message", true, outmsg);

    /* tell the HNP we are in distress */
    alert = OBJ_NEW(opal_buffer_t);
    /* pack update state command */
    cmd = ORTE_PLM_UPDATE_PROC_STATE;
    if (ORTE_SUCCESS != (rc = opal_dss.pack(alert, &cmd, 1, ORTE_PLM_CMD))) {
        ORTE_ERROR_LOG(rc);
        OBJ_RELEASE(alert);
        goto cleanup;
    }
    /* pack the jobid */
    if (ORTE_SUCCESS != (rc = opal_dss.pack(alert, &ORTE_PROC_MY_NAME->jobid, 1, ORTE_JOBID))) {
        ORTE_ERROR_LOG(rc);
        OBJ_RELEASE(alert);
        goto cleanup;
    }
    /* pack our vpid */
    if (ORTE_SUCCESS != (rc = opal_dss.pack(alert, &ORTE_PROC_MY_NAME->vpid, 1, ORTE_VPID))) {
        ORTE_ERROR_LOG(rc);
        OBJ_RELEASE(alert);
        goto cleanup;
    }
    /* pack our pid */
    if (ORTE_SUCCESS != (rc = opal_dss.pack(alert, &orte_process_info.pid, 1, OPAL_PID))) {
        ORTE_ERROR_LOG(rc);
        OBJ_RELEASE(alert);
        goto cleanup;
    }
    /* pack our state */
    if (ORTE_SUCCESS != (rc = opal_dss.pack(alert, &state, 1, ORTE_PROC_STATE))) {
        ORTE_ERROR_LOG(rc);
        OBJ_RELEASE(alert);
        goto cleanup;
    }
    /* pack our exit code */
    if (ORTE_SUCCESS != (rc = opal_dss.pack(alert, &error_code, 1, ORTE_EXIT_CODE))) {
        ORTE_ERROR_LOG(rc);
        OBJ_RELEASE(alert);
        goto cleanup;
    }
    /* flag that this job is complete so the receiver can know */
    if (ORTE_SUCCESS != (rc = opal_dss.pack(alert, &null, 1, ORTE_VPID))) {
        ORTE_ERROR_LOG(rc);
        OBJ_RELEASE(alert);
        goto cleanup;
    }

    /* send it */
    if (0 > (rc = orte_rml.send_buffer_nb(orte_mgmt_conduit,
                                          ORTE_PROC_MY_HNP, alert,
                                          ORTE_RML_TAG_PLM,
                                          orte_rml_send_callback, NULL))) {
        ORTE_ERROR_LOG(rc);
        OBJ_RELEASE(alert);
        /* we can't communicate, so give up */
        orte_quit(0, 0, NULL);
        return;
    }

  cleanup:
    /* set a timer for exiting - this also gives the message a chance
     * to get out! */
    if (NULL == (timer = OBJ_NEW(orte_timer_t))) {
        ORTE_ERROR_LOG(ORTE_ERR_OUT_OF_RESOURCE);
        return;
    }
    timer->tv.tv_sec = 5;
    timer->tv.tv_usec = 0;
    opal_event_evtimer_set(orte_event_base, timer->ev, wakeup, NULL);
    opal_event_set_priority(timer->ev, ORTE_ERROR_PRI);
    ORTE_POST_OBJECT(timer);
    opal_event_evtimer_add(timer->ev, &timer->tv);

}

static void job_errors(int fd, short args, void *cbdata)
{
    orte_state_caddy_t *caddy = (orte_state_caddy_t*)cbdata;
    orte_job_t *jdata;
    orte_job_state_t jobstate;
    int rc;
    orte_plm_cmd_flag_t cmd;
    opal_buffer_t *alert;

    ORTE_ACQUIRE_OBJECT(caddy);

    /*
     * if orte is trying to shutdown, just let it
     */
    if (orte_finalizing) {
        return;
    }

    /* if the jdata is NULL, then we abort as this
     * is reporting an unrecoverable error
     */
    if (NULL == caddy->jdata) {
        ORTE_ACTIVATE_JOB_STATE(NULL, ORTE_JOB_STATE_FORCED_EXIT);
        OBJ_RELEASE(caddy);
        return;
    }

    /* update the state */
    jdata = caddy->jdata;
    jobstate = caddy->job_state;
    jdata->state = jobstate;

    OPAL_OUTPUT_VERBOSE((1, orte_errmgr_base_framework.framework_output,
                         "%s errmgr:default_orted: job %s reported error state %s",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         ORTE_JOBID_PRINT(jdata->jobid),
                         orte_job_state_to_str(jobstate)));

    switch (jobstate) {
    case ORTE_JOB_STATE_FAILED_TO_START:
        failed_start(jdata);
        break;
    case ORTE_JOB_STATE_COMM_FAILED:
        /* kill all local procs */
        killprocs(ORTE_JOBID_WILDCARD, ORTE_VPID_WILDCARD);
        /* order termination */
        ORTE_FORCED_TERMINATE(ORTE_ERROR_DEFAULT_EXIT_CODE);
        goto cleanup;
        break;
    case ORTE_JOB_STATE_HEARTBEAT_FAILED:
        /* let the HNP handle this */
        goto cleanup;
        break;

    default:
        break;
    }
    alert = OBJ_NEW(opal_buffer_t);
    /* pack update state command */
    cmd = ORTE_PLM_UPDATE_PROC_STATE;
    if (ORTE_SUCCESS != (rc = opal_dss.pack(alert, &cmd, 1, ORTE_PLM_CMD))) {
        ORTE_ERROR_LOG(rc);
        OBJ_RELEASE(alert);
        goto cleanup;
    }
    /* pack the job info */
    if (ORTE_SUCCESS != (rc = pack_state_update(alert, jdata))) {
        ORTE_ERROR_LOG(rc);
        OBJ_RELEASE(alert);
        goto cleanup;
    }
    /* send it */
    if (0 > (rc = orte_rml.send_buffer_nb(orte_mgmt_conduit,
                                          ORTE_PROC_MY_HNP, alert,
                                          ORTE_RML_TAG_PLM,
                                          orte_rml_send_callback, NULL))) {
        ORTE_ERROR_LOG(rc);
        OBJ_RELEASE(alert);
    }

 cleanup:
    OBJ_RELEASE(caddy);
}

static void proc_errors(int fd, short args, void *cbdata)
{
    orte_state_caddy_t *caddy = (orte_state_caddy_t*)cbdata;
    orte_job_t *jdata;
    orte_process_name_t *proc = &caddy->name;
    orte_proc_state_t state = caddy->proc_state;
    char *rtmod;
    orte_proc_t *child, *ptr;
    opal_buffer_t *alert;
    orte_plm_cmd_flag_t cmd;
    int rc=ORTE_SUCCESS;
    int i;
    orte_wait_tracker_t *t2;

    ORTE_ACQUIRE_OBJECT(caddy);

    OPAL_OUTPUT_VERBOSE((2, orte_errmgr_base_framework.framework_output,
                         "%s errmgr:default_orted:proc_errors process %s error state %s",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         ORTE_NAME_PRINT(proc),
                         orte_proc_state_to_str(state)));

    /*
     * if orte is trying to shutdown, just let it
     */
    if (orte_finalizing) {
        OPAL_OUTPUT_VERBOSE((2, orte_errmgr_base_framework.framework_output,
                             "%s errmgr:default_orted:proc_errors finalizing - ignoring error",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));
        goto cleanup;
    }

    /* if this is a heartbeat failure, let the HNP handle it */
    if (ORTE_PROC_STATE_HEARTBEAT_FAILED == state) {
        OPAL_OUTPUT_VERBOSE((2, orte_errmgr_base_framework.framework_output,
                             "%s errmgr:default_orted:proc_errors heartbeat failed - ignoring error",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));
        goto cleanup;
    }

    /* if this was a failed comm, then see if it was to our
     * lifeline
     */
    if (ORTE_PROC_STATE_LIFELINE_LOST == state ||
        ORTE_PROC_STATE_UNABLE_TO_SEND_MSG == state ||
        ORTE_PROC_STATE_NO_PATH_TO_TARGET == state ||
        ORTE_PROC_STATE_PEER_UNKNOWN == state ||
        ORTE_PROC_STATE_FAILED_TO_CONNECT == state) {
        OPAL_OUTPUT_VERBOSE((2, orte_errmgr_base_framework.framework_output,
                             "%s errmgr:orted lifeline lost or unable to communicate - exiting",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));
        /* set our exit status */
        ORTE_UPDATE_EXIT_STATUS(ORTE_ERROR_DEFAULT_EXIT_CODE);
        /* kill our children */
        killprocs(ORTE_JOBID_WILDCARD, ORTE_VPID_WILDCARD);
        /* terminate - our routed children will see
         * us leave and automatically die
         */
        orte_quit(0, 0, NULL);
        goto cleanup;
    }

    /* get the job object */
    if (NULL == (jdata = orte_get_job_data_object(proc->jobid))) {
        /* must already be complete */
        OPAL_OUTPUT_VERBOSE((2, orte_errmgr_base_framework.framework_output,
                             "%s errmgr:default_orted:proc_errors NULL jdata - ignoring error",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));
        goto cleanup;
    }

    /* get our management conduit's routed module name */
    rtmod = orte_rml.get_routed(orte_mgmt_conduit);

    if (ORTE_PROC_STATE_COMM_FAILED == state) {
        /* if it is our own connection, ignore it */
        if (OPAL_EQUAL == orte_util_compare_name_fields(ORTE_NS_CMP_ALL, ORTE_PROC_MY_NAME, proc)) {
            OPAL_OUTPUT_VERBOSE((2, orte_errmgr_base_framework.framework_output,
                                 "%s errmgr:default_orted:proc_errors comm_failed to self - ignoring error",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));
            goto cleanup;
        }
        /* was it a daemon? */
        if (proc->jobid != ORTE_PROC_MY_NAME->jobid) {
            /* nope - we can't seem to trust that we will catch the waitpid
             * in this situation, so push this over to be handled as if
             * it were a waitpid trigger so we don't create a bunch of
             * duplicate code */
            OPAL_OUTPUT_VERBOSE((2, orte_errmgr_base_framework.framework_output,
                                 "%s errmgr:default_orted:proc_errors comm_failed to non-daemon - handling as waitpid",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));
            /* get the proc_t */
            if (NULL == (child = (orte_proc_t*)opal_pointer_array_get_item(jdata->procs, proc->vpid))) {
                ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
                ORTE_FORCED_TERMINATE(ORTE_ERROR_DEFAULT_EXIT_CODE);
                goto cleanup;
            }
            /* leave the exit code alone - process this as a waitpid */
            t2 = OBJ_NEW(orte_wait_tracker_t);
            OBJ_RETAIN(child);  // protect against race conditions
            t2->child = child;
            t2->evb = orte_event_base;
            opal_event_set(t2->evb, &t2->ev, -1,
                           OPAL_EV_WRITE, orte_odls_base_default_wait_local_proc, t2);
            opal_event_set_priority(&t2->ev, ORTE_MSG_PRI);
            opal_event_active(&t2->ev, OPAL_EV_WRITE, 1);
            goto cleanup;
        }
        OPAL_OUTPUT_VERBOSE((2, orte_errmgr_base_framework.framework_output,
                             "%s errmgr:default:orted daemon %s exited",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             ORTE_NAME_PRINT(proc)));
        /* if we are using static ports, then it is possible that the HNP
         * will not see this termination. So if the HNP didn't order us
         * to terminate, then we should ensure it knows */
        if (orte_static_ports && !orte_orteds_term_ordered) {
            /* send an alert to the HNP */
            alert = OBJ_NEW(opal_buffer_t);
            /* pack update state command */
            cmd = ORTE_PLM_UPDATE_PROC_STATE;
            if (ORTE_SUCCESS != (rc = opal_dss.pack(alert, &cmd, 1, ORTE_PLM_CMD))) {
                ORTE_ERROR_LOG(rc);
                return;
            }
            /* get the proc_t */
            if (NULL == (child = (orte_proc_t*)opal_pointer_array_get_item(jdata->procs, proc->vpid))) {
                ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
                ORTE_FORCED_TERMINATE(ORTE_ERROR_DEFAULT_EXIT_CODE);
                goto cleanup;
            }
            /* set the exit code to reflect the problem */
            child->exit_code = ORTE_ERR_COMM_FAILURE;
            /* pack only the data for this daemon - have to start with the jobid
             * so the receiver can unpack it correctly
             */
            if (ORTE_SUCCESS != (rc = opal_dss.pack(alert, &proc->jobid, 1, ORTE_JOBID))) {
                ORTE_ERROR_LOG(rc);
                return;
            }

            /* now pack the daemon's info */
            if (ORTE_SUCCESS != (rc = pack_state_for_proc(alert, child))) {
                ORTE_ERROR_LOG(rc);
                return;
            }
            /* send it */
            OPAL_OUTPUT_VERBOSE((5, orte_errmgr_base_framework.framework_output,
                                 "%s errmgr:default_orted reporting lost connection to daemon %s",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 ORTE_NAME_PRINT(proc)));
            if (0 > (rc = orte_rml.send_buffer_nb(orte_mgmt_conduit,
                                                  ORTE_PROC_MY_HNP, alert,
                                                  ORTE_RML_TAG_PLM,
                                                  orte_rml_send_callback, NULL))) {
                ORTE_ERROR_LOG(rc);
                OBJ_RELEASE(alert);
            }
            /* mark that we notified the HNP for this job so we don't do it again */
            orte_set_attribute(&jdata->attributes, ORTE_JOB_FAIL_NOTIFIED, ORTE_ATTR_LOCAL, NULL, OPAL_BOOL);
            /* continue on */
            goto cleanup;
        }

        if (orte_orteds_term_ordered) {
            /* are any of my children still alive */
            for (i=0; i < orte_local_children->size; i++) {
                if (NULL != (child = (orte_proc_t*)opal_pointer_array_get_item(orte_local_children, i))) {
                    if (ORTE_FLAG_TEST(child, ORTE_PROC_FLAG_ALIVE)) {
                        OPAL_OUTPUT_VERBOSE((5, orte_state_base_framework.framework_output,
                                             "%s errmgr:default:orted[%s(%d)] proc %s is alive",
                                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                             __FILE__, __LINE__,
                                             ORTE_NAME_PRINT(&child->name)));
                        goto cleanup;
                    }
                }
            }
            /* if all my routes and children are gone, then terminate
               ourselves nicely (i.e., this is a normal termination) */
            if (0 == orte_routed.num_routes(rtmod)) {
                OPAL_OUTPUT_VERBOSE((2, orte_errmgr_base_framework.framework_output,
                                     "%s errmgr:default:orted all routes gone - exiting",
                                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));
                ORTE_ACTIVATE_JOB_STATE(NULL, ORTE_JOB_STATE_DAEMONS_TERMINATED);
            } else {
                OPAL_OUTPUT_VERBOSE((2, orte_errmgr_base_framework.framework_output,
                                     "%s errmgr:default:orted not exiting, num_routes() == %d",
                                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                     (int)orte_routed.num_routes(rtmod)));
            }
        }
        /* if not, then we can continue */
        goto cleanup;
    }

    if (NULL == (child = (orte_proc_t*)opal_pointer_array_get_item(jdata->procs, proc->vpid))) {
        ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
        ORTE_FORCED_TERMINATE(ORTE_ERROR_DEFAULT_EXIT_CODE);
        goto cleanup;
    }
    /* if this is not a local proc for this job, we can
     * ignore this call
     */
    if (!ORTE_FLAG_TEST(child, ORTE_PROC_FLAG_LOCAL)) {
        OPAL_OUTPUT_VERBOSE((2, orte_errmgr_base_framework.framework_output,
                             "%s errmgr:default_orted:proc_errors proc is not local - ignoring error",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));
        goto cleanup;
    }

    OPAL_OUTPUT_VERBOSE((2, orte_errmgr_base_framework.framework_output,
                         "%s errmgr:default_orted got state %s for proc %s",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         orte_proc_state_to_str(state),
                         ORTE_NAME_PRINT(proc)));

    if (ORTE_PROC_STATE_TERM_NON_ZERO == state) {
        /* update the state */
        child->state = state;
        /* report this as abnormal termination to the HNP, unless we already have
         * done so for this job */
        if (!orte_get_attribute(&jdata->attributes, ORTE_JOB_FAIL_NOTIFIED, NULL, OPAL_BOOL)) {
            alert = OBJ_NEW(opal_buffer_t);
            /* pack update state command */
            cmd = ORTE_PLM_UPDATE_PROC_STATE;
            if (ORTE_SUCCESS != (rc = opal_dss.pack(alert, &cmd, 1, ORTE_PLM_CMD))) {
                ORTE_ERROR_LOG(rc);
                return;
            }
            /* pack only the data for this proc - have to start with the jobid
             * so the receiver can unpack it correctly
             */
            if (ORTE_SUCCESS != (rc = opal_dss.pack(alert, &proc->jobid, 1, ORTE_JOBID))) {
                ORTE_ERROR_LOG(rc);
                return;
            }

            /* now pack the child's info */
            if (ORTE_SUCCESS != (rc = pack_state_for_proc(alert, child))) {
                ORTE_ERROR_LOG(rc);
                return;
            }
            /* send it */
            OPAL_OUTPUT_VERBOSE((5, orte_errmgr_base_framework.framework_output,
                                 "%s errmgr:default_orted reporting proc %s abnormally terminated with non-zero status (local procs = %d)",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 ORTE_NAME_PRINT(&child->name),
                                 jdata->num_local_procs));
            if (0 > (rc = orte_rml.send_buffer_nb(orte_mgmt_conduit,
                                                  ORTE_PROC_MY_HNP, alert,
                                                  ORTE_RML_TAG_PLM,
                                                  orte_rml_send_callback, NULL))) {
                ORTE_ERROR_LOG(rc);
                OBJ_RELEASE(alert);
            }
            /* mark that we notified the HNP for this job so we don't do it again */
            orte_set_attribute(&jdata->attributes, ORTE_JOB_FAIL_NOTIFIED, ORTE_ATTR_LOCAL, NULL, OPAL_BOOL);
        }
        /* if the proc has terminated, notify the state machine */
        if (ORTE_FLAG_TEST(child, ORTE_PROC_FLAG_IOF_COMPLETE) &&
            ORTE_FLAG_TEST(child, ORTE_PROC_FLAG_WAITPID) &&
            !ORTE_FLAG_TEST(child, ORTE_PROC_FLAG_RECORDED)) {
            ORTE_ACTIVATE_PROC_STATE(proc, ORTE_PROC_STATE_TERMINATED);
        }
        goto cleanup;
    }

    if (ORTE_PROC_STATE_FAILED_TO_START == state ||
        ORTE_PROC_STATE_FAILED_TO_LAUNCH == state) {
        /* update the proc state */
        child->state = state;
        /* count the proc as having "terminated" */
        jdata->num_terminated++;
        /* leave the error report in this case to the
         * state machine, which will receive notice
         * when all local procs have attempted to start
         * so that we send a consolidated error report
         * back to the HNP
         */
        if (jdata->num_local_procs == jdata->num_terminated) {
            /* let the state machine know */
            if (ORTE_PROC_STATE_FAILED_TO_START == state) {
                ORTE_ACTIVATE_JOB_STATE(jdata, ORTE_JOB_STATE_FAILED_TO_START);
            } else {
                ORTE_ACTIVATE_JOB_STATE(jdata, ORTE_JOB_STATE_FAILED_TO_LAUNCH);
            }
        }
        goto cleanup;
    }

    if (ORTE_PROC_STATE_TERMINATED < state) {
        /* if we were ordered to terminate, see if
         * any of our routes or local children remain alive - if not, then
         * terminate ourselves. */
        if (orte_orteds_term_ordered) {
            /* mark the child as no longer alive and update the counters, if necessary.
             * we have to do this here as we aren't going to send this to the state
             * machine, and we want to keep the bookkeeping accurate just in case */
            if (ORTE_FLAG_TEST(child, ORTE_PROC_FLAG_ALIVE)) {
                ORTE_FLAG_UNSET(child, ORTE_PROC_FLAG_ALIVE);
            }
            if (!ORTE_FLAG_TEST(child, ORTE_PROC_FLAG_RECORDED)) {
                ORTE_FLAG_SET(child, ORTE_PROC_FLAG_RECORDED);
                jdata->num_terminated++;
            }
            for (i=0; i < orte_local_children->size; i++) {
                if (NULL != (child = (orte_proc_t*)opal_pointer_array_get_item(orte_local_children, i))) {
                    if (ORTE_FLAG_TEST(child, ORTE_PROC_FLAG_ALIVE)) {
                        goto keep_going;
                    }
                }
            }
            /* if all my routes and children are gone, then terminate
               ourselves nicely (i.e., this is a normal termination) */
            if (0 == orte_routed.num_routes(rtmod)) {
                OPAL_OUTPUT_VERBOSE((2, orte_errmgr_base_framework.framework_output,
                                     "%s errmgr:default:orted all routes gone - exiting",
                                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));
                ORTE_ACTIVATE_JOB_STATE(NULL, ORTE_JOB_STATE_DAEMONS_TERMINATED);
            }
            /* no need to alert the HNP - we are already on our way out */
            goto cleanup;
        }

    keep_going:
        /* if the job hasn't completed and the state is abnormally
         * terminated, then we need to alert the HNP right away - but
         * only do this once!
         */
        if (!orte_get_attribute(&jdata->attributes, ORTE_JOB_FAIL_NOTIFIED, NULL, OPAL_BOOL)) {
            alert = OBJ_NEW(opal_buffer_t);
            /* pack update state command */
            cmd = ORTE_PLM_UPDATE_PROC_STATE;
            if (ORTE_SUCCESS != (rc = opal_dss.pack(alert, &cmd, 1, ORTE_PLM_CMD))) {
                ORTE_ERROR_LOG(rc);
                return;
            }
            /* pack only the data for this proc - have to start with the jobid
             * so the receiver can unpack it correctly
             */
            if (ORTE_SUCCESS != (rc = opal_dss.pack(alert, &proc->jobid, 1, ORTE_JOBID))) {
                ORTE_ERROR_LOG(rc);
                return;
            }
            child->state = state;
            /* now pack the child's info */
            if (ORTE_SUCCESS != (rc = pack_state_for_proc(alert, child))) {
                ORTE_ERROR_LOG(rc);
                return;
            }
            OPAL_OUTPUT_VERBOSE((5, orte_errmgr_base_framework.framework_output,
                                 "%s errmgr:default_orted reporting proc %s aborted to HNP (local procs = %d)",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 ORTE_NAME_PRINT(&child->name),
                                 jdata->num_local_procs));
            /* send it */
            if (0 > (rc = orte_rml.send_buffer_nb(orte_mgmt_conduit,
                                                  ORTE_PROC_MY_HNP, alert,
                                                  ORTE_RML_TAG_PLM,
                                                  orte_rml_send_callback, NULL))) {
                ORTE_ERROR_LOG(rc);
            }
            /* mark that we notified the HNP for this job so we don't do it again */
            orte_set_attribute(&jdata->attributes, ORTE_JOB_FAIL_NOTIFIED, ORTE_ATTR_LOCAL, NULL, OPAL_BOOL);
        }
        /* if the proc has terminated, notify the state machine */
        if (ORTE_FLAG_TEST(child, ORTE_PROC_FLAG_IOF_COMPLETE) &&
            ORTE_FLAG_TEST(child, ORTE_PROC_FLAG_WAITPID) &&
            !ORTE_FLAG_TEST(child, ORTE_PROC_FLAG_RECORDED)) {
            ORTE_ACTIVATE_PROC_STATE(proc, ORTE_PROC_STATE_TERMINATED);
        }
        goto cleanup;
    }

    /* only other state is terminated - see if anyone is left alive */
    if (!any_live_children(proc->jobid)) {
        alert = OBJ_NEW(opal_buffer_t);
        /* pack update state command */
        cmd = ORTE_PLM_UPDATE_PROC_STATE;
        if (ORTE_SUCCESS != (rc = opal_dss.pack(alert, &cmd, 1, ORTE_PLM_CMD))) {
            ORTE_ERROR_LOG(rc);
            return;
        }
        /* pack the data for the job */
        if (ORTE_SUCCESS != (rc = pack_state_update(alert, jdata))) {
            ORTE_ERROR_LOG(rc);
            return;
        }

        OPAL_OUTPUT_VERBOSE((5, orte_errmgr_base_framework.framework_output,
                             "%s errmgr:default_orted reporting all procs in %s terminated",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             ORTE_JOBID_PRINT(jdata->jobid)));

        /* remove all of this job's children from the global list */
        for (i=0; i < orte_local_children->size; i++) {
            if (NULL == (ptr = (orte_proc_t*)opal_pointer_array_get_item(orte_local_children, i))) {
                continue;
            }
            if (jdata->jobid == ptr->name.jobid) {
                opal_pointer_array_set_item(orte_local_children, i, NULL);
                OBJ_RELEASE(ptr);
            }
        }

        /* ensure the job's local session directory tree is removed */
        orte_session_dir_cleanup(jdata->jobid);

        /* remove this job from our local job data since it is complete */
        OBJ_RELEASE(jdata);

        /* send it */
        if (0 > (rc = orte_rml.send_buffer_nb(orte_mgmt_conduit,
                                              ORTE_PROC_MY_HNP, alert,
                                              ORTE_RML_TAG_PLM,
                                              orte_rml_send_callback, NULL))) {
            ORTE_ERROR_LOG(rc);
        }
        return;
    }

  cleanup:
    OBJ_RELEASE(caddy);
}

/*****************
 * Local Functions
 *****************/
static bool any_live_children(orte_jobid_t job)
{
    int i;
    orte_proc_t *child;

    for (i=0; i < orte_local_children->size; i++) {
        if (NULL == (child = (orte_proc_t*)opal_pointer_array_get_item(orte_local_children, i))) {
            continue;
        }
        /* is this child part of the specified job? */
        if ((job == child->name.jobid || ORTE_JOBID_WILDCARD == job) &&
            ORTE_FLAG_TEST(child, ORTE_PROC_FLAG_ALIVE)) {
            return true;
        }
    }

    /* if we get here, then nobody is left alive from that job */
    return false;

}

static int pack_state_for_proc(opal_buffer_t *alert, orte_proc_t *child)
{
    int rc;

    /* pack the child's vpid */
    if (ORTE_SUCCESS != (rc = opal_dss.pack(alert, &(child->name.vpid), 1, ORTE_VPID))) {
        ORTE_ERROR_LOG(rc);
        return rc;
    }
    /* pack the pid */
    if (ORTE_SUCCESS != (rc = opal_dss.pack(alert, &child->pid, 1, OPAL_PID))) {
        ORTE_ERROR_LOG(rc);
        return rc;
    }
    /* pack its state */
    if (ORTE_SUCCESS != (rc = opal_dss.pack(alert, &child->state, 1, ORTE_PROC_STATE))) {
        ORTE_ERROR_LOG(rc);
        return rc;
    }
    /* pack its exit code */
    if (ORTE_SUCCESS != (rc = opal_dss.pack(alert, &child->exit_code, 1, ORTE_EXIT_CODE))) {
        ORTE_ERROR_LOG(rc);
        return rc;
    }

    return ORTE_SUCCESS;
}

static int pack_state_update(opal_buffer_t *alert, orte_job_t *jobdat)
{
    int rc, i;
    orte_proc_t *child;
    orte_vpid_t null=ORTE_VPID_INVALID;

    /* pack the jobid */
    if (ORTE_SUCCESS != (rc = opal_dss.pack(alert, &jobdat->jobid, 1, ORTE_JOBID))) {
        ORTE_ERROR_LOG(rc);
        return rc;
    }
    for (i=0; i < orte_local_children->size; i++) {
        if (NULL == (child = (orte_proc_t*)opal_pointer_array_get_item(orte_local_children, i))) {
            continue;
        }
        /* if this child is part of the job... */
        if (child->name.jobid == jobdat->jobid) {
            if (ORTE_SUCCESS != (rc = pack_state_for_proc(alert, child))) {
                ORTE_ERROR_LOG(rc);
                return rc;
            }
        }
    }
    /* flag that this job is complete so the receiver can know */
    if (ORTE_SUCCESS != (rc = opal_dss.pack(alert, &null, 1, ORTE_VPID))) {
        ORTE_ERROR_LOG(rc);
        return rc;
    }

    return ORTE_SUCCESS;
}

static void failed_start(orte_job_t *jobdat)
{
    int i;
    orte_proc_t *child;

    /* set the state */
    jobdat->state = ORTE_JOB_STATE_FAILED_TO_START;

    for (i=0; i < orte_local_children->size; i++) {
        if (NULL == (child = (orte_proc_t*)opal_pointer_array_get_item(orte_local_children, i))) {
            continue;
        }
        /* is this child part of the specified job? */
        if (child->name.jobid == jobdat->jobid) {
            if (ORTE_PROC_STATE_FAILED_TO_START == child->state) {
                /* this proc never launched - flag that the iof
                 * is complete or else we will hang waiting for
                 * pipes to close that were never opened
                 */
                ORTE_FLAG_SET(child, ORTE_PROC_FLAG_IOF_COMPLETE);
                /* ditto for waitpid */
                ORTE_FLAG_SET(child, ORTE_PROC_FLAG_WAITPID);
            }
        }
    }
    OPAL_OUTPUT_VERBOSE((1, orte_errmgr_base_framework.framework_output,
                         "%s errmgr:hnp: job %s reported incomplete start",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         ORTE_JOBID_PRINT(jobdat->jobid)));
    return;
}

static void killprocs(orte_jobid_t job, orte_vpid_t vpid)
{
    opal_pointer_array_t cmd;
    orte_proc_t proc;
    int rc;

    if (ORTE_JOBID_WILDCARD == job
        && ORTE_VPID_WILDCARD == vpid) {
        if (ORTE_SUCCESS != (rc = orte_odls.kill_local_procs(NULL))) {
            ORTE_ERROR_LOG(rc);
        }
        return;
    }

    OBJ_CONSTRUCT(&cmd, opal_pointer_array_t);
    OBJ_CONSTRUCT(&proc, orte_proc_t);
    proc.name.jobid = job;
    proc.name.vpid = vpid;
    opal_pointer_array_add(&cmd, &proc);
    if (ORTE_SUCCESS != (rc = orte_odls.kill_local_procs(&cmd))) {
        ORTE_ERROR_LOG(rc);
    }
    OBJ_DESTRUCT(&cmd);
    OBJ_DESTRUCT(&proc);
}
