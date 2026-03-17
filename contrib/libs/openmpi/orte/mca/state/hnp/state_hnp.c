/*
 * Copyright (c) 2011-2012 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2014-2018 Intel, Inc. All rights reserved.
 * Copyright (c) 2017      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
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
#include "opal/mca/pmix/pmix.h"

#include "orte/mca/errmgr/errmgr.h"
#include "orte/mca/grpcomm/grpcomm.h"
#include "orte/mca/rml/rml.h"
#include "orte/mca/iof/iof.h"
#include "orte/mca/plm/base/base.h"
#include "orte/mca/ras/base/base.h"
#include "orte/mca/rmaps/base/base.h"
#include "orte/mca/routed/routed.h"
#include "orte/util/session_dir.h"
#include "orte/runtime/orte_quit.h"

#include "orte/mca/state/state.h"
#include "orte/mca/state/base/base.h"
#include "orte/mca/state/base/state_private.h"
#include "state_hnp.h"

/*
 * Module functions: Global
 */
static int init(void);
static int finalize(void);

/******************
 * HNP module - just uses base functions after
 * initializing the proc state machine. Job state
 * machine is unused by hnplication procs at this
 * time.
 ******************/
orte_state_base_module_t orte_state_hnp_module = {
    init,
    finalize,
    orte_state_base_activate_job_state,
    orte_state_base_add_job_state,
    orte_state_base_set_job_state_callback,
    orte_state_base_set_job_state_priority,
    orte_state_base_remove_job_state,
    orte_state_base_activate_proc_state,
    orte_state_base_add_proc_state,
    orte_state_base_set_proc_state_callback,
    orte_state_base_set_proc_state_priority,
    orte_state_base_remove_proc_state
};

static void hnp_notify(int sd, short args, void *cbdata);

/* defined default state machine sequence - individual
 * plm's must add a state for launching daemons
 */
static orte_job_state_t launch_states[] = {
    ORTE_JOB_STATE_INIT,
    ORTE_JOB_STATE_INIT_COMPLETE,
    ORTE_JOB_STATE_ALLOCATE,
    ORTE_JOB_STATE_ALLOCATION_COMPLETE,
    ORTE_JOB_STATE_DAEMONS_LAUNCHED,
    ORTE_JOB_STATE_DAEMONS_REPORTED,
    ORTE_JOB_STATE_VM_READY,
    ORTE_JOB_STATE_MAP,
    ORTE_JOB_STATE_MAP_COMPLETE,
    ORTE_JOB_STATE_SYSTEM_PREP,
    ORTE_JOB_STATE_LAUNCH_APPS,
    ORTE_JOB_STATE_SEND_LAUNCH_MSG,
    ORTE_JOB_STATE_LOCAL_LAUNCH_COMPLETE,
    ORTE_JOB_STATE_RUNNING,
    ORTE_JOB_STATE_REGISTERED,
    /* termination states */
    ORTE_JOB_STATE_TERMINATED,
    ORTE_JOB_STATE_NOTIFY_COMPLETED,
    ORTE_JOB_STATE_NOTIFIED,
    ORTE_JOB_STATE_ALL_JOBS_COMPLETE
};
static orte_state_cbfunc_t launch_callbacks[] = {
    orte_plm_base_setup_job,
    orte_plm_base_setup_job_complete,
    orte_ras_base_allocate,
    orte_plm_base_allocation_complete,
    orte_plm_base_daemons_launched,
    orte_plm_base_daemons_reported,
    orte_plm_base_vm_ready,
    orte_rmaps_base_map_job,
    orte_plm_base_mapping_complete,
    orte_plm_base_complete_setup,
    orte_plm_base_launch_apps,
    orte_plm_base_send_launch_msg,
    orte_state_base_local_launch_complete,
    orte_plm_base_post_launch,
    orte_plm_base_registered,
    orte_state_base_check_all_complete,
    hnp_notify,
    orte_state_base_cleanup_job,
    orte_quit
};

static orte_proc_state_t proc_states[] = {
    ORTE_PROC_STATE_RUNNING,
    ORTE_PROC_STATE_REGISTERED,
    ORTE_PROC_STATE_IOF_COMPLETE,
    ORTE_PROC_STATE_WAITPID_FIRED,
    ORTE_PROC_STATE_TERMINATED
};
static orte_state_cbfunc_t proc_callbacks[] = {
    orte_state_base_track_procs,
    orte_state_base_track_procs,
    orte_state_base_track_procs,
    orte_state_base_track_procs,
    orte_state_base_track_procs
};

static void force_quit(int fd, short args, void *cbdata)
{
    orte_state_caddy_t *caddy = (orte_state_caddy_t*)cbdata;

    /* give us a chance to stop the orteds */
    orte_plm.terminate_orteds();
    OBJ_RELEASE(caddy);
}

/************************
 * API Definitions
 ************************/
static int init(void)
{
    int i, rc;
    int num_states;

    /* setup the state machines */
    OBJ_CONSTRUCT(&orte_job_states, opal_list_t);
    OBJ_CONSTRUCT(&orte_proc_states, opal_list_t);

    /* setup the job state machine */
    num_states = sizeof(launch_states) / sizeof(orte_job_state_t);
    for (i=0; i < num_states; i++) {
        if (ORTE_SUCCESS != (rc = orte_state.add_job_state(launch_states[i],
                                                           launch_callbacks[i],
                                                           ORTE_SYS_PRI))) {
            ORTE_ERROR_LOG(rc);
        }
    }
    /* add the termination response */
    if (ORTE_SUCCESS != (rc = orte_state.add_job_state(ORTE_JOB_STATE_DAEMONS_TERMINATED,
                                                       orte_quit, ORTE_SYS_PRI))) {
        ORTE_ERROR_LOG(rc);
    }
    /* add a default error response */
    if (ORTE_SUCCESS != (rc = orte_state.add_job_state(ORTE_JOB_STATE_FORCED_EXIT,
                                                       force_quit, ORTE_ERROR_PRI))) {
        ORTE_ERROR_LOG(rc);
    }
    /* add callback to report progress, if requested */
    if (ORTE_SUCCESS != (rc = orte_state.add_job_state(ORTE_JOB_STATE_REPORT_PROGRESS,
                                                       orte_state_base_report_progress, ORTE_ERROR_PRI))) {
        ORTE_ERROR_LOG(rc);
    }
    if (5 < opal_output_get_verbosity(orte_state_base_framework.framework_output)) {
        orte_state_base_print_job_state_machine();
    }

    /* populate the proc state machine to allow us to
     * track proc lifecycle changes
     */
    num_states = sizeof(proc_states) / sizeof(orte_proc_state_t);
    for (i=0; i < num_states; i++) {
        if (ORTE_SUCCESS != (rc = orte_state.add_proc_state(proc_states[i],
                                                            proc_callbacks[i],
                                                            ORTE_SYS_PRI))) {
            ORTE_ERROR_LOG(rc);
        }
    }
    if (5 < opal_output_get_verbosity(orte_state_base_framework.framework_output)) {
        orte_state_base_print_proc_state_machine();
    }

    return ORTE_SUCCESS;
}

static int finalize(void)
{
    /* cleanup the proc state machine */
    OPAL_LIST_DESTRUCT(&orte_proc_states);
    /* cleanup the job state machine */
    OPAL_LIST_DESTRUCT(&orte_job_states);

    return ORTE_SUCCESS;
}

static void _send_notification(int status,
                               orte_proc_state_t state,
                               orte_process_name_t *proc,
                               orte_process_name_t *target)
{
    opal_buffer_t *buf;
    orte_grpcomm_signature_t sig;
    int rc;
    opal_value_t kv, *kvptr;
    orte_process_name_t daemon;

    buf = OBJ_NEW(opal_buffer_t);

    opal_output_verbose(5, orte_state_base_framework.framework_output,
                        "%s state:hnp:sending notification %s proc %s target %s",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                        ORTE_ERROR_NAME(status),
                        ORTE_NAME_PRINT(proc),
                        ORTE_NAME_PRINT(target));

    /* pack the status */
    if (ORTE_SUCCESS != (rc = opal_dss.pack(buf, &status, 1, OPAL_INT))) {
        ORTE_ERROR_LOG(rc);
        OBJ_RELEASE(buf);
        return;
    }

    /* the source is the proc */
    if (ORTE_SUCCESS != (rc = opal_dss.pack(buf, proc, 1, ORTE_NAME))) {
        ORTE_ERROR_LOG(rc);
        OBJ_RELEASE(buf);
        return;
    }

    if (OPAL_ERR_PROC_ABORTED == status) {
        /* we will pass three opal_value_t's */
        rc = 3;
        if (ORTE_SUCCESS != (rc = opal_dss.pack(buf, &rc, 1, OPAL_INT))) {
            ORTE_ERROR_LOG(rc);
            OBJ_RELEASE(buf);
            return;
        }
        /* pass along the affected proc(s) */
        OBJ_CONSTRUCT(&kv, opal_value_t);
        kv.key = strdup(OPAL_PMIX_EVENT_AFFECTED_PROC);
        kv.type = OPAL_NAME;
        kv.data.name.jobid = proc->jobid;
        kv.data.name.vpid = proc->vpid;
        kvptr = &kv;
        if (ORTE_SUCCESS != (rc = opal_dss.pack(buf, &kvptr, 1, OPAL_VALUE))) {
            ORTE_ERROR_LOG(rc);
            OBJ_DESTRUCT(&kv);
            OBJ_RELEASE(buf);
            return;
        }
        OBJ_DESTRUCT(&kv);
    } else {
        /* we are going to pass two opal_value_t's */
        rc = 2;
        if (ORTE_SUCCESS != (rc = opal_dss.pack(buf, &rc, 1, OPAL_INT))) {
            ORTE_ERROR_LOG(rc);
            OBJ_RELEASE(buf);
            return;
        }
    }

    /* pass along the affected proc(s) */
    OBJ_CONSTRUCT(&kv, opal_value_t);
    kv.key = strdup(OPAL_PMIX_EVENT_AFFECTED_PROC);
    kv.type = OPAL_NAME;
    kv.data.name.jobid = proc->jobid;
    kv.data.name.vpid = proc->vpid;
    kvptr = &kv;
    if (ORTE_SUCCESS != (rc = opal_dss.pack(buf, &kvptr, 1, OPAL_VALUE))) {
        ORTE_ERROR_LOG(rc);
        OBJ_DESTRUCT(&kv);
        OBJ_RELEASE(buf);
        return;
    }
    OBJ_DESTRUCT(&kv);

    /* pass along the proc(s) to be notified */
    OBJ_CONSTRUCT(&kv, opal_value_t);
    kv.key = strdup(OPAL_PMIX_EVENT_CUSTOM_RANGE);
    kv.type = OPAL_NAME;
    kv.data.name.jobid = target->jobid;
    kv.data.name.vpid = target->vpid;
    kvptr = &kv;
    if (ORTE_SUCCESS != (rc = opal_dss.pack(buf, &kvptr, 1, OPAL_VALUE))) {
        ORTE_ERROR_LOG(rc);
        OBJ_DESTRUCT(&kv);
        OBJ_RELEASE(buf);
        return;
    }
    OBJ_DESTRUCT(&kv);

    /* if the targets are a wildcard, then xcast it to everyone */
    if (ORTE_VPID_WILDCARD == target->vpid) {
        OBJ_CONSTRUCT(&sig, orte_grpcomm_signature_t);
        sig.signature = (orte_process_name_t*)malloc(sizeof(orte_process_name_t));
        sig.signature[0].jobid = ORTE_PROC_MY_NAME->jobid;
        sig.signature[0].vpid = ORTE_VPID_WILDCARD;
        sig.sz = 1;

        if (ORTE_SUCCESS != (rc = orte_grpcomm.xcast(&sig, ORTE_RML_TAG_NOTIFICATION, buf))) {
            ORTE_ERROR_LOG(rc);
        }
        OBJ_DESTRUCT(&sig);
        OBJ_RELEASE(buf);
    } else {
        /* get the daemon hosting the proc to be notified */
        daemon.jobid = ORTE_PROC_MY_NAME->jobid;
        daemon.vpid = orte_get_proc_daemon_vpid(target);
        /* send the notification to that daemon */
        opal_output_verbose(5, orte_state_base_framework.framework_output,
                            "%s state:base:sending notification %s to proc %s at daemon %s",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                            ORTE_ERROR_NAME(status),
                            ORTE_NAME_PRINT(target),
                            ORTE_NAME_PRINT(&daemon));
        if (ORTE_SUCCESS != (rc = orte_rml.send_buffer_nb(orte_mgmt_conduit,
                                                          &daemon, buf,
                                                          ORTE_RML_TAG_NOTIFICATION,
                                                          orte_rml_send_callback, NULL))) {
            ORTE_ERROR_LOG(rc);
            OBJ_RELEASE(buf);
        }
    }
}

static void hnp_notify(int sd, short args, void *cbdata)
{
    orte_state_caddy_t *caddy = (orte_state_caddy_t*)cbdata;
    orte_job_t *jdata = caddy->jdata;
    orte_process_name_t parent, target, *npptr;

    /* if they requested notification upon completion, provide it */
    if (orte_get_attribute(&jdata->attributes, ORTE_JOB_NOTIFY_COMPLETION, NULL, OPAL_BOOL)) {
        /* notify_completion => notify the parent of the termination
         * of this child job. So get the parent jobid info */
        npptr = &parent;
        if (!orte_get_attribute(&jdata->attributes, ORTE_JOB_LAUNCH_PROXY, (void**)&npptr, OPAL_NAME)) {
            /* notify everyone who asked for it */
            target.jobid = jdata->jobid;
            target.vpid = ORTE_VPID_WILDCARD;
            _send_notification(OPAL_ERR_JOB_TERMINATED, caddy->proc_state, &target, ORTE_NAME_WILDCARD);
        } else {
            target.jobid = jdata->jobid;
            target.vpid = ORTE_VPID_WILDCARD;
            _send_notification(OPAL_ERR_JOB_TERMINATED, caddy->proc_state, &target, &parent);
        }
    }
    ORTE_ACTIVATE_JOB_STATE(jdata, ORTE_JOB_STATE_NOTIFIED);

    OBJ_RELEASE(caddy);
}
