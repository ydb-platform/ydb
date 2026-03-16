/*
 * Copyright (c) 2009-2011 The Trustees of Indiana University.
 *                         All rights reserved.
 * Copyright (c) 2010      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2010-2011 Oak Ridge National Labs.  All rights reserved.
 * Copyright (c) 2004-2011 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2011      Oracle and/or all its affiliates.  All rights reserved.
 * Copyright (c) 2011-2013 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2014-2018 Intel, Inc.  All rights reserved.
 * Copyright (c) 2017      IBM Corporation.  All rights reserved.
 * Copyright (c) 2018      Research Organization for Information Science
 *                         and Technology (RIST).  All rights reserved.
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
#ifdef HAVE_SYS_WAIT_H
#include <sys/wait.h>
#endif

#include "opal/util/output.h"
#include "opal/dss/dss.h"

#include "orte/mca/iof/base/base.h"
#include "orte/mca/rml/rml.h"
#include "orte/mca/odls/odls.h"
#include "orte/mca/odls/base/base.h"
#include "orte/mca/odls/base/odls_private.h"
#include "orte/mca/plm/base/plm_private.h"
#include "orte/mca/plm/plm.h"
#include "orte/mca/rmaps/rmaps_types.h"
#include "orte/mca/routed/routed.h"
#include "orte/mca/grpcomm/grpcomm.h"
#include "orte/mca/ess/ess.h"
#include "orte/mca/state/state.h"

#include "orte/util/error_strings.h"
#include "orte/util/name_fns.h"
#include "orte/util/proc_info.h"
#include "orte/util/show_help.h"
#include "orte/util/threads.h"

#include "orte/runtime/orte_globals.h"
#include "orte/runtime/orte_locks.h"
#include "orte/runtime/orte_quit.h"
#include "orte/runtime/data_type_support/orte_dt_support.h"

#include "orte/mca/errmgr/errmgr.h"
#include "orte/mca/errmgr/base/base.h"
#include "orte/mca/errmgr/base/errmgr_private.h"

#include "errmgr_default_hnp.h"

static int init(void);
static int finalize(void);
static void hnp_abort(int error_code, char *fmt, ...);

/******************
 * default_hnp module
 ******************/
orte_errmgr_base_module_t orte_errmgr_default_hnp_module = {
    .init = init,
    .finalize = finalize,
    .logfn = orte_errmgr_base_log,
    .abort = hnp_abort,
    .abort_peers = orte_errmgr_base_abort_peers
};


/*
 * Local functions
 */
static void default_hnp_abort(orte_job_t *jdata);
static void job_errors(int fd, short args, void *cbdata);
static void proc_errors(int fd, short args, void *cbdata);

/**********************
 * From DEFAULT_HNP
 **********************/
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
static void hnp_abort(int error_code, char *fmt, ...)
{
    va_list arglist;
    char *outmsg = NULL;
    orte_timer_t *timer;

    /* only do this once */
    if (orte_abnormal_term_ordered) {
        return;
    }

    /* ensure we exit with non-zero status */
    ORTE_UPDATE_EXIT_STATUS(error_code);

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

    /* this could have happened very early, so see if it happened
     * before we started anything - if so, we can just finalize */
    if (orte_never_launched) {
        orte_quit(0, 0, NULL);
        return;
    }

    /* tell the daemons to terminate */
    if (ORTE_SUCCESS != orte_plm.terminate_orteds()) {
        orte_quit(0, 0, NULL);
        return;
    }

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
    orte_exit_code_t sts;
    orte_proc_t *aborted_proc;
    opal_buffer_t *answer;
    int32_t rc, ret;
    int room, *rmptr;

    ORTE_ACQUIRE_OBJECT(caddy);

    /*
     * if orte is trying to shutdown, just let it
     */
    if (orte_finalizing) {
        return;
    }

    /* ensure we have an error exit status */
    ORTE_UPDATE_EXIT_STATUS(ORTE_ERROR_DEFAULT_EXIT_CODE);

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
                         "%s errmgr:default_hnp: job %s reported state %s",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         ORTE_JOBID_PRINT(jdata->jobid),
                         orte_job_state_to_str(jobstate)));

    if (ORTE_JOB_STATE_NEVER_LAUNCHED == jobstate ||
        ORTE_JOB_STATE_ALLOC_FAILED == jobstate ||
        ORTE_JOB_STATE_MAP_FAILED == jobstate ||
        ORTE_JOB_STATE_CANNOT_LAUNCH == jobstate) {
        if (1 == ORTE_LOCAL_JOBID(jdata->jobid)) {
            /* this is the primary job */
            orte_never_launched = true;
        }
        /* disable routing as we may not have performed the daemon
         * wireup - e.g., in a managed environment, all the daemons
         * "phone home", but don't actually wireup into the routed
         * network until they receive the launch message
         */
        orte_routing_is_enabled = false;
        jdata->num_terminated = jdata->num_procs;
        /* activate the terminated state so we can exit */
        ORTE_ACTIVATE_JOB_STATE(caddy->jdata, ORTE_JOB_STATE_TERMINATED);
        /* if it was a dynamic spawn, then we better tell them this didn't work */
        if (ORTE_JOBID_INVALID != jdata->originator.jobid) {
            rc = jobstate;
            answer = OBJ_NEW(opal_buffer_t);
            if (ORTE_SUCCESS != (ret = opal_dss.pack(answer, &rc, 1, OPAL_INT32))) {
                ORTE_ERROR_LOG(ret);
                ORTE_FORCED_TERMINATE(ORTE_ERROR_DEFAULT_EXIT_CODE);
                OBJ_RELEASE(caddy);
                return;
            }
            if (ORTE_SUCCESS != (ret = opal_dss.pack(answer, &jdata->jobid, 1, ORTE_JOBID))) {
                ORTE_ERROR_LOG(ret);
                ORTE_FORCED_TERMINATE(ORTE_ERROR_DEFAULT_EXIT_CODE);
                OBJ_RELEASE(caddy);
                return;
            }
            /* pack the room number */
            rmptr = &room;
            if (orte_get_attribute(&jdata->attributes, ORTE_JOB_ROOM_NUM, (void**)&rmptr, OPAL_INT)) {
                if (ORTE_SUCCESS != (ret = opal_dss.pack(answer, &room, 1, OPAL_INT))) {
                    ORTE_ERROR_LOG(ret);
                    ORTE_FORCED_TERMINATE(ORTE_ERROR_DEFAULT_EXIT_CODE);
                    OBJ_RELEASE(caddy);
                    return;
                }
            }
            OPAL_OUTPUT_VERBOSE((5, orte_errmgr_base_framework.framework_output,
                                 "%s errmgr:hnp sending dyn error release of job %s to %s",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 ORTE_JOBID_PRINT(jdata->jobid),
                                 ORTE_NAME_PRINT(&jdata->originator)));
            if (0 > (ret = orte_rml.send_buffer_nb(orte_mgmt_conduit,
                                                   &jdata->originator, answer,
                                                   ORTE_RML_TAG_LAUNCH_RESP,
                                                   orte_rml_send_callback, NULL))) {
                ORTE_ERROR_LOG(ret);
                OBJ_RELEASE(answer);
                ORTE_FORCED_TERMINATE(ORTE_ERROR_DEFAULT_EXIT_CODE);
            }
        }
        OBJ_RELEASE(caddy);
        return;
    }

    if (ORTE_JOB_STATE_FAILED_TO_START == jobstate ||
        ORTE_JOB_STATE_FAILED_TO_LAUNCH == jobstate) {
        /* the job object for this job will have been NULL'd
         * in the array if the job was solely local. If it isn't
         * NULL, then we need to tell everyone else to die
         */
        aborted_proc = NULL;
        if (orte_get_attribute(&jdata->attributes, ORTE_JOB_ABORTED_PROC, (void**)&aborted_proc, OPAL_PTR)) {
            sts = aborted_proc->exit_code;
            if (ORTE_PROC_MY_NAME->jobid == jdata->jobid) {
                if (WIFSIGNALED(sts)) { /* died on signal */
#ifdef WCOREDUMP
                    if (WCOREDUMP(sts)) {
                        orte_show_help("help-plm-base.txt", "daemon-died-signal-core", true,
                                       WTERMSIG(sts));
                        sts = WTERMSIG(sts);
                    } else {
                        orte_show_help("help-plm-base.txt", "daemon-died-signal", true,
                                       WTERMSIG(sts));
                        sts = WTERMSIG(sts);
                    }
#else
                    orte_show_help("help-plm-base.txt", "daemon-died-signal", true,
                                   WTERMSIG(sts));
                    sts = WTERMSIG(sts);
#endif /* WCOREDUMP */
                } else {
                    orte_show_help("help-plm-base.txt", "daemon-died-no-signal", true,
                                   WEXITSTATUS(sts));
                    sts = WEXITSTATUS(sts);
                }
            }
        }
        /* if this is the daemon job, then we need to ensure we
         * output an error message indicating we couldn't launch the
         * daemons */
        if (jdata->jobid == ORTE_PROC_MY_NAME->jobid) {
            orte_show_help("help-errmgr-base.txt", "failed-daemon-launch", true);
        }
    }

    /* if the daemon job aborted and we haven't heard from everyone yet,
     * then this could well have been caused by a daemon not finding
     * a way back to us. In this case, output a message indicating a daemon
     * died without reporting. Otherwise, say nothing as we
     * likely already output an error message */
    if (ORTE_JOB_STATE_ABORTED == jobstate &&
        jdata->jobid == ORTE_PROC_MY_NAME->jobid &&
        jdata->num_procs != jdata->num_reported) {
        orte_show_help("help-errmgr-base.txt", "failed-daemon", true);
    }

    /* abort the job */
    ORTE_ACTIVATE_JOB_STATE(caddy->jdata, ORTE_JOB_STATE_FORCED_EXIT);
    /* set the global abnormal exit flag  */
    orte_abnormal_term_ordered = true;
    OBJ_RELEASE(caddy);
}

static void proc_errors(int fd, short args, void *cbdata)
{
    orte_state_caddy_t *caddy = (orte_state_caddy_t*)cbdata;
    orte_job_t *jdata;
    orte_proc_t *pptr, *proct;
    orte_process_name_t *proc = &caddy->name;
    orte_proc_state_t state = caddy->proc_state;
    int i;
    int32_t i32, *i32ptr;
    char *rtmod;

    ORTE_ACQUIRE_OBJECT(caddy);

    OPAL_OUTPUT_VERBOSE((1, orte_errmgr_base_framework.framework_output,
                         "%s errmgr:default_hnp: for proc %s state %s",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         ORTE_NAME_PRINT(proc),
                         orte_proc_state_to_str(state)));

    /*
     * if orte is trying to shutdown, just let it
     */
    if (orte_finalizing) {
        goto cleanup;
    }

    /* get the job object */
    if (NULL == (jdata = orte_get_job_data_object(proc->jobid))) {
        /* could be a race condition */
        goto cleanup;
    }
    pptr = (orte_proc_t*)opal_pointer_array_get_item(jdata->procs, proc->vpid);
    rtmod = orte_rml.get_routed(orte_mgmt_conduit);

    /* we MUST handle a communication failure before doing anything else
     * as it requires some special care to avoid normal termination issues
     * for local application procs
     */
    if (ORTE_PROC_STATE_COMM_FAILED == state) {
        /* is this to a daemon? */
        if (ORTE_PROC_MY_NAME->jobid != proc->jobid) {
            /* nope - ignore it */
            OPAL_OUTPUT_VERBOSE((5, orte_errmgr_base_framework.framework_output,
                                 "%s Comm failure to non-daemon proc - ignoring it",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));
            goto cleanup;
        }
        /* if this is my own connection, ignore it */
        if (ORTE_PROC_MY_NAME->vpid == proc->vpid) {
            OPAL_OUTPUT_VERBOSE((5, orte_errmgr_base_framework.framework_output,
                                 "%s Comm failure on my own connection - ignoring it",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));
            goto cleanup;
        }
        /* mark the daemon as gone */
        ORTE_FLAG_UNSET(pptr, ORTE_PROC_FLAG_ALIVE);
        /* if we have ordered orteds to terminate or abort
         * is in progress, record it */
        if (orte_orteds_term_ordered || orte_abnormal_term_ordered) {
            OPAL_OUTPUT_VERBOSE((5, orte_errmgr_base_framework.framework_output,
                                 "%s Comm failure: daemons terminating - recording daemon %s as gone",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ORTE_NAME_PRINT(proc)));
            /* remove from dependent routes, if it is one */
            orte_routed.route_lost(rtmod, proc);
            /* if all my routes and local children are gone, then terminate ourselves */
            if (0 == orte_routed.num_routes(rtmod)) {
                for (i=0; i < orte_local_children->size; i++) {
                    if (NULL != (proct = (orte_proc_t*)opal_pointer_array_get_item(orte_local_children, i)) &&
                        ORTE_FLAG_TEST(pptr, ORTE_PROC_FLAG_ALIVE) && proct->state < ORTE_PROC_STATE_UNTERMINATED) {
                        /* at least one is still alive */
                        OPAL_OUTPUT_VERBOSE((5, orte_errmgr_base_framework.framework_output,
                                             "%s Comm failure: at least one proc (%s) still alive",
                                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                             ORTE_NAME_PRINT(&proct->name)));
                        goto cleanup;
                    }
                }
                /* call our appropriate exit procedure */
                OPAL_OUTPUT_VERBOSE((5, orte_errmgr_base_framework.framework_output,
                                     "%s errmgr_hnp: all routes and children gone - ordering exit",
                                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));
                ORTE_ACTIVATE_JOB_STATE(NULL, ORTE_JOB_STATE_DAEMONS_TERMINATED);
            } else {
                OPAL_OUTPUT_VERBOSE((5, orte_errmgr_base_framework.framework_output,
                                     "%s Comm failure: %d routes remain alive",
                                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                     (int)orte_routed.num_routes(rtmod)));
            }
            goto cleanup;
        }
        OPAL_OUTPUT_VERBOSE((5, orte_errmgr_base_framework.framework_output,
                             "%s Comm failure: daemon %s - aborting",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ORTE_NAME_PRINT(proc)));
        /* record the first one to fail */
        if (!ORTE_FLAG_TEST(jdata, ORTE_JOB_FLAG_ABORTED)) {
            /* mark the daemon job as failed */
            jdata->state = ORTE_JOB_STATE_COMM_FAILED;
            /* point to the lowest rank to cause the problem */
            orte_set_attribute(&jdata->attributes, ORTE_JOB_ABORTED_PROC, ORTE_ATTR_LOCAL, pptr, OPAL_PTR);
            /* retain the object so it doesn't get free'd */
            OBJ_RETAIN(pptr);
            ORTE_FLAG_SET(jdata, ORTE_JOB_FLAG_ABORTED);
            if (!orte_enable_recovery) {
                /* output an error message so the user knows what happened */
                orte_show_help("help-errmgr-base.txt", "node-died", true,
                               ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                               orte_process_info.nodename,
                               ORTE_NAME_PRINT(proc),
                               pptr->node->name);
                /* update our exit code */
                ORTE_UPDATE_EXIT_STATUS(pptr->exit_code);
                /* just in case the exit code hadn't been set, do it here - this
                 * won't override any reported exit code */
                ORTE_UPDATE_EXIT_STATUS(ORTE_ERR_COMM_FAILURE);
            }
        }
        /* if recovery is enabled, then we are done - otherwise,
         * abort the system */
        if (!orte_enable_recovery) {
            default_hnp_abort(jdata);
        }
        goto cleanup;
    }

    /* update the proc state - can get multiple reports on a proc
     * depending on circumstances, so ensure we only do this once
     */
    if (pptr->state < ORTE_PROC_STATE_TERMINATED) {
        pptr->state = state;
    }

    /* if we were ordered to terminate, mark this proc as dead and see if
     * any of our routes or local children remain alive - if not, then
     * terminate ourselves. */
    if (orte_orteds_term_ordered) {
        for (i=0; i < orte_local_children->size; i++) {
            if (NULL != (proct = (orte_proc_t*)opal_pointer_array_get_item(orte_local_children, i))) {
                if (ORTE_FLAG_TEST(proct, ORTE_PROC_FLAG_ALIVE)) {
                    goto keep_going;
                }
            }
        }
        /* if all my routes and children are gone, then terminate
           ourselves nicely (i.e., this is a normal termination) */
        if (0 == orte_routed.num_routes(rtmod)) {
            OPAL_OUTPUT_VERBOSE((2, orte_errmgr_base_framework.framework_output,
                                 "%s errmgr:default:hnp all routes gone - exiting",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));
            ORTE_ACTIVATE_JOB_STATE(NULL, ORTE_JOB_STATE_DAEMONS_TERMINATED);
        }
    }

  keep_going:
    /* if this is a continuously operating job, then there is nothing more
     * to do - we let the job continue to run */
    if (orte_get_attribute(&jdata->attributes, ORTE_JOB_CONTINUOUS_OP, NULL, OPAL_BOOL) ||
        ORTE_FLAG_TEST(jdata, ORTE_JOB_FLAG_RECOVERABLE)) {
        /* always mark the waitpid as having fired */
        ORTE_ACTIVATE_PROC_STATE(&pptr->name, ORTE_PROC_STATE_WAITPID_FIRED);
        /* if this is a remote proc, we won't hear anything more about it
         * as the default behavior would be to terminate the job. So be sure to
         * mark the IOF as having completed too so we correctly mark this proc
         * as dead and notify everyone as required */
        if (!ORTE_FLAG_TEST(pptr, ORTE_PROC_FLAG_LOCAL)) {
            ORTE_ACTIVATE_PROC_STATE(&pptr->name, ORTE_PROC_STATE_IOF_COMPLETE);
        }
        goto cleanup;
    }

    /* ensure we record the failed proc properly so we can report
     * the error once we terminate
     */
    switch (state) {
    case ORTE_PROC_STATE_KILLED_BY_CMD:
        OPAL_OUTPUT_VERBOSE((5, orte_errmgr_base_framework.framework_output,
                             "%s errmgr:hnp: proc %s killed by cmd",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             ORTE_NAME_PRINT(proc)));
        /* we ordered this proc to die, so it isn't an abnormal termination
         * and we don't flag it as such
         */
        if (jdata->num_terminated >= jdata->num_procs) {
            /* this job has terminated */
            ORTE_ACTIVATE_JOB_STATE(jdata, ORTE_JOB_STATE_TERMINATED);
        }
        /* don't abort the job as this isn't an abnormal termination */
        break;

    case ORTE_PROC_STATE_ABORTED:
        OPAL_OUTPUT_VERBOSE((5, orte_errmgr_base_framework.framework_output,
                             "%s errmgr:hnp: proc %s aborted",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             ORTE_NAME_PRINT(proc)));
        if (!ORTE_FLAG_TEST(jdata, ORTE_JOB_FLAG_ABORTED)) {
            jdata->state = ORTE_JOB_STATE_ABORTED;
            /* point to the first rank to cause the problem */
            orte_set_attribute(&jdata->attributes, ORTE_JOB_ABORTED_PROC, ORTE_ATTR_LOCAL, pptr, OPAL_PTR);
            /* retain the object so it doesn't get free'd */
            OBJ_RETAIN(pptr);
            ORTE_FLAG_SET(jdata, ORTE_JOB_FLAG_ABORTED);
            ORTE_UPDATE_EXIT_STATUS(pptr->exit_code);
            /* abnormal termination - abort, but only do it once
             * to avoid creating a lot of confusion */
            default_hnp_abort(jdata);
        }
        break;

    case ORTE_PROC_STATE_ABORTED_BY_SIG:
        OPAL_OUTPUT_VERBOSE((5, orte_errmgr_base_framework.framework_output,
                             "%s errmgr:hnp: proc %s aborted by signal",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             ORTE_NAME_PRINT(proc)));

        ORTE_UPDATE_EXIT_STATUS(pptr->exit_code);
        /* track the number of non-zero exits */
        i32 = 0;
        i32ptr = &i32;
        orte_get_attribute(&jdata->attributes, ORTE_JOB_NUM_NONZERO_EXIT, (void**)&i32ptr, OPAL_INT32);
        ++i32;
        orte_set_attribute(&jdata->attributes, ORTE_JOB_NUM_NONZERO_EXIT, ORTE_ATTR_LOCAL, i32ptr, OPAL_INT32);
        if (orte_abort_non_zero_exit) {

            if (!ORTE_FLAG_TEST(jdata, ORTE_JOB_FLAG_ABORTED)) {
                jdata->state = ORTE_JOB_STATE_ABORTED_BY_SIG;
                /* point to the first rank to cause the problem */
                orte_set_attribute(&jdata->attributes, ORTE_JOB_ABORTED_PROC, ORTE_ATTR_LOCAL, pptr, OPAL_PTR);
                /* retain the object so it doesn't get free'd */
                OBJ_RETAIN(pptr);
                ORTE_FLAG_SET(jdata, ORTE_JOB_FLAG_ABORTED);
                ORTE_UPDATE_EXIT_STATUS(pptr->exit_code);
                /* abnormal termination - abort, but only do it once
                 * to avoid creating a lot of confusion */
                default_hnp_abort(jdata);
            }
        } else {
            /* user requested we consider this normal termination */
            if (jdata->num_terminated >= jdata->num_procs) {
                /* this job has terminated */
                ORTE_ACTIVATE_JOB_STATE(jdata, ORTE_JOB_STATE_TERMINATED);
            }
        }
        break;

    case ORTE_PROC_STATE_TERM_WO_SYNC:
        OPAL_OUTPUT_VERBOSE((5, orte_errmgr_base_framework.framework_output,
                             "%s errmgr:hnp: proc %s terminated without sync",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             ORTE_NAME_PRINT(proc)));
        if (!ORTE_FLAG_TEST(jdata, ORTE_JOB_FLAG_ABORTED)) {
            jdata->state = ORTE_JOB_STATE_ABORTED_WO_SYNC;
            /* point to the first rank to cause the problem */
            orte_set_attribute(&jdata->attributes, ORTE_JOB_ABORTED_PROC, ORTE_ATTR_LOCAL, pptr, OPAL_PTR);
            /* retain the object so it doesn't get free'd */
            OBJ_RETAIN(pptr);
            ORTE_FLAG_SET(jdata, ORTE_JOB_FLAG_ABORTED);
            ORTE_UPDATE_EXIT_STATUS(pptr->exit_code);
            /* now treat a special case - if the proc exit'd without a required
             * sync, it may have done so with a zero exit code. We want to ensure
             * that the user realizes there was an error, so in this -one- case,
             * we overwrite the process' exit code with the default error code
             */
            ORTE_UPDATE_EXIT_STATUS(ORTE_ERROR_DEFAULT_EXIT_CODE);
            /* abnormal termination - abort, but only do it once
             * to avoid creating a lot of confusion */
            default_hnp_abort(jdata);
        }
        break;

    case ORTE_PROC_STATE_FAILED_TO_START:
    case ORTE_PROC_STATE_FAILED_TO_LAUNCH:
        OPAL_OUTPUT_VERBOSE((5, orte_errmgr_base_framework.framework_output,
                             "%s errmgr:hnp: proc %s %s",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             ORTE_NAME_PRINT(proc),
                             orte_proc_state_to_str(state)));
        if (!ORTE_FLAG_TEST(jdata, ORTE_JOB_FLAG_ABORTED)) {
            if (ORTE_PROC_STATE_FAILED_TO_START) {
                jdata->state = ORTE_JOB_STATE_FAILED_TO_START;
            } else {
                jdata->state = ORTE_JOB_STATE_FAILED_TO_LAUNCH;
            }
            /* point to the first rank to cause the problem */
            orte_set_attribute(&jdata->attributes, ORTE_JOB_ABORTED_PROC, ORTE_ATTR_LOCAL, pptr, OPAL_PTR);
            /* retain the object so it doesn't get free'd */
            OBJ_RETAIN(pptr);
            ORTE_FLAG_SET(jdata, ORTE_JOB_FLAG_ABORTED);
            ORTE_UPDATE_EXIT_STATUS(pptr->exit_code);
            /* abnormal termination - abort, but only do it once
             * to avoid creating a lot of confusion */
            default_hnp_abort(jdata);
        }
        /* if this was a daemon, report it */
        if (jdata->jobid == ORTE_PROC_MY_NAME->jobid) {
            /* output a message indicating we failed to launch a daemon */
            orte_show_help("help-errmgr-base.txt", "failed-daemon-launch", true);
        }
        break;

    case ORTE_PROC_STATE_CALLED_ABORT:
        OPAL_OUTPUT_VERBOSE((5, orte_errmgr_base_framework.framework_output,
                             "%s errmgr:hnp: proc %s called abort with exit code %d",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             ORTE_NAME_PRINT(proc), pptr->exit_code));
        if (!ORTE_FLAG_TEST(jdata, ORTE_JOB_FLAG_ABORTED)) {
            jdata->state = ORTE_JOB_STATE_CALLED_ABORT;
            /* point to the first proc to cause the problem */
            orte_set_attribute(&jdata->attributes, ORTE_JOB_ABORTED_PROC, ORTE_ATTR_LOCAL, pptr, OPAL_PTR);
            /* retain the object so it doesn't get free'd */
            OBJ_RETAIN(pptr);
            ORTE_FLAG_SET(jdata, ORTE_JOB_FLAG_ABORTED);
            ORTE_UPDATE_EXIT_STATUS(pptr->exit_code);
            /* abnormal termination - abort, but only do it once
             * to avoid creating a lot of confusion */
            default_hnp_abort(jdata);
        }
        break;

    case ORTE_PROC_STATE_TERM_NON_ZERO:
        OPAL_OUTPUT_VERBOSE((5, orte_errmgr_base_framework.framework_output,
                             "%s errmgr:hnp: proc %s exited with non-zero status %d",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             ORTE_NAME_PRINT(proc),
                             pptr->exit_code));
        ORTE_UPDATE_EXIT_STATUS(pptr->exit_code);
        /* track the number of non-zero exits */
        i32 = 0;
        i32ptr = &i32;
        orte_get_attribute(&jdata->attributes, ORTE_JOB_NUM_NONZERO_EXIT, (void**)&i32ptr, OPAL_INT32);
        ++i32;
        orte_set_attribute(&jdata->attributes, ORTE_JOB_NUM_NONZERO_EXIT, ORTE_ATTR_LOCAL, i32ptr, OPAL_INT32);
        if (orte_abort_non_zero_exit) {
            if (!ORTE_FLAG_TEST(jdata, ORTE_JOB_FLAG_ABORTED)) {
                jdata->state = ORTE_JOB_STATE_NON_ZERO_TERM;
                /* point to the first rank to cause the problem */
                orte_set_attribute(&jdata->attributes, ORTE_JOB_ABORTED_PROC, ORTE_ATTR_LOCAL, pptr, OPAL_PTR);
                /* retain the object so it doesn't get free'd */
                OBJ_RETAIN(pptr);
                ORTE_FLAG_SET(jdata, ORTE_JOB_FLAG_ABORTED);
                /* abnormal termination - abort, but only do it once
                 * to avoid creating a lot of confusion */
                default_hnp_abort(jdata);
            }
        } else {
            /* user requested we consider this normal termination */
            if (jdata->num_terminated >= jdata->num_procs) {
                /* this job has terminated */
                ORTE_ACTIVATE_JOB_STATE(jdata, ORTE_JOB_STATE_TERMINATED);
            }
        }
        break;

    case ORTE_PROC_STATE_HEARTBEAT_FAILED:
        OPAL_OUTPUT_VERBOSE((5, orte_errmgr_base_framework.framework_output,
                             "%s errmgr:hnp: proc %s heartbeat failed",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             ORTE_NAME_PRINT(proc)));
        if (!ORTE_FLAG_TEST(jdata, ORTE_JOB_FLAG_ABORTED)) {
            jdata->state = ORTE_JOB_STATE_HEARTBEAT_FAILED;
            /* point to the first rank to cause the problem */
            orte_set_attribute(&jdata->attributes, ORTE_JOB_ABORTED_PROC, ORTE_ATTR_LOCAL, pptr, OPAL_PTR);
            /* retain the object so it doesn't get free'd */
            OBJ_RETAIN(pptr);
            ORTE_FLAG_SET(jdata, ORTE_JOB_FLAG_ABORTED);
            ORTE_UPDATE_EXIT_STATUS(pptr->exit_code);
            /* abnormal termination - abort, but only do it once
             * to avoid creating a lot of confusion */
            default_hnp_abort(jdata);
        }
        /* remove from dependent routes, if it is one */
        orte_routed.route_lost(rtmod, proc);
        break;

    case ORTE_PROC_STATE_UNABLE_TO_SEND_MSG:
        OPAL_OUTPUT_VERBOSE((5, orte_errmgr_base_framework.framework_output,
                             "%s errmgr:hnp: unable to send message to proc %s",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             ORTE_NAME_PRINT(proc)));
        /* if this proc is one of my daemons, then we are truly
         * hosed - so just exit out
         */
        if (ORTE_PROC_MY_NAME->jobid == proc->jobid) {
            ORTE_ACTIVATE_JOB_STATE(NULL, ORTE_JOB_STATE_DAEMONS_TERMINATED);
            break;
        }
        if (!ORTE_FLAG_TEST(jdata, ORTE_JOB_FLAG_ABORTED)) {
            /* abnormal termination - abort, but only do it once
             * to avoid creating a lot of confusion */
            default_hnp_abort(jdata);
        }
        break;

    case ORTE_PROC_STATE_NO_PATH_TO_TARGET:
        OPAL_OUTPUT_VERBOSE((5, orte_errmgr_base_framework.framework_output,
                             "%s errmgr:hnp: no message path to proc %s",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             ORTE_NAME_PRINT(proc)));
        orte_show_help("help-errmgr-base.txt", "no-path", true,
                       orte_process_info.nodename, pptr->node->name);
        /* if this proc is one of my daemons, then we are truly
         * hosed - so just exit out
         */
        if (ORTE_PROC_MY_NAME->jobid == proc->jobid) {
            ORTE_ACTIVATE_JOB_STATE(NULL, ORTE_JOB_STATE_DAEMONS_TERMINATED);
            break;
        }
        if (!ORTE_FLAG_TEST(jdata, ORTE_JOB_FLAG_ABORTED)) {
            /* abnormal termination - abort, but only do it once
             * to avoid creating a lot of confusion */
            default_hnp_abort(jdata);
        }
        break;

    case ORTE_PROC_STATE_FAILED_TO_CONNECT:
        OPAL_OUTPUT_VERBOSE((5, orte_errmgr_base_framework.framework_output,
                             "%s errmgr:hnp: cannot connect to proc %s",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             ORTE_NAME_PRINT(proc)));
        orte_show_help("help-errmgr-base.txt", "no-connect", true,
                       orte_process_info.nodename, pptr->node->name);
        /* if this proc is one of my daemons, then we are truly
         * hosed - so just exit out
         */
        if (ORTE_PROC_MY_NAME->jobid == proc->jobid) {
            ORTE_ACTIVATE_JOB_STATE(NULL, ORTE_JOB_STATE_DAEMONS_TERMINATED);
            break;
        }
        if (!ORTE_FLAG_TEST(jdata, ORTE_JOB_FLAG_ABORTED)) {
            /* abnormal termination - abort, but only do it once
             * to avoid creating a lot of confusion */
            default_hnp_abort(jdata);
        }
        break;

    default:
        /* shouldn't get this, but terminate job if required */
        OPAL_OUTPUT_VERBOSE((5, orte_errmgr_base_framework.framework_output,
                             "%s errmgr:hnp: proc %s default error %s",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             ORTE_NAME_PRINT(proc),
                             orte_proc_state_to_str(state)));
        if (jdata->num_terminated == jdata->num_procs) {
            ORTE_ACTIVATE_JOB_STATE(jdata, ORTE_JOB_STATE_TERMINATED);
        }
        break;
    }
    /* if the waitpid fired, be sure to let the state machine know */
    if (ORTE_FLAG_TEST(pptr, ORTE_PROC_FLAG_WAITPID)) {
        ORTE_ACTIVATE_PROC_STATE(&pptr->name, ORTE_PROC_STATE_WAITPID_FIRED);
    }

 cleanup:
    OBJ_RELEASE(caddy);
}

/*****************
 * Local Functions
 *****************/
static void default_hnp_abort(orte_job_t *jdata)
{
    int rc;
    int32_t i32, *i32ptr;

    /* if we are already in progress, then ignore this call */
    if (opal_atomic_trylock(&orte_abort_inprogress_lock)) { /* returns 1 if already locked */
        OPAL_OUTPUT_VERBOSE((1, orte_errmgr_base_framework.framework_output,
                             "%s errmgr:default_hnp: abort in progress, ignoring abort on job %s",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             ORTE_JOBID_PRINT(jdata->jobid)));
        return;
    }

    OPAL_OUTPUT_VERBOSE((1, orte_errmgr_base_framework.framework_output,
                         "%s errmgr:default_hnp: abort called on job %s",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         ORTE_JOBID_PRINT(jdata->jobid)));

    /* set control params to indicate we are terminating */
    orte_job_term_ordered = true;
    orte_enable_recovery = false;

    /* if it is the daemon job that aborted, then we need
     * to flag an abnormal term - otherwise, just abort
     * the job cleanly
     */
    if (ORTE_PROC_MY_NAME->jobid == jdata->jobid) {
        orte_abnormal_term_ordered = true;
    }

    i32 = 0;
    i32ptr = &i32;
    if (orte_get_attribute(&jdata->attributes, ORTE_JOB_NUM_NONZERO_EXIT, (void**)&i32ptr, OPAL_INT32)) {
        /* warn user */
        orte_show_help("help-errmgr-base.txt", "normal-termination-but", true, 
                       (1 == ORTE_LOCAL_JOBID(jdata->jobid)) ? "Primary" : "Child",
                       (1 == ORTE_LOCAL_JOBID(jdata->jobid)) ? "" : ORTE_LOCAL_JOBID_PRINT(jdata->jobid),
                       i32, (1 == i32) ? "process returned\na non-zero exit code" :
                       "processes returned\nnon-zero exit codes");
    }

    OPAL_OUTPUT_VERBOSE((1, orte_errmgr_base_framework.framework_output,
                         "%s errmgr:default_hnp: ordering orted termination",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));

    /* tell the plm to terminate the orteds - they will automatically
     * kill their local procs
     */
    if (ORTE_SUCCESS != (rc = orte_plm.terminate_orteds())) {
        ORTE_ERROR_LOG(rc);
    }
}
