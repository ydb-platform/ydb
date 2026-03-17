/* -*- C -*-
 *
 * Copyright (c) 2004-2010 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2011 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2011      Los Alamos National Security, LLC.
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
/** @file:
 *
 */

/*
 * includes
 */
#include "orte_config.h"

#include <string.h>
#ifdef HAVE_SYS_TIME_H
#include <sys/time.h>
#endif

#include "orte/mca/mca.h"
#include "opal/dss/dss.h"
#include "opal/threads/threads.h"
#include "opal/util/argv.h"
#include "opal/util/opal_environ.h"

#include "orte/constants.h"
#include "orte/types.h"
#include "orte/util/proc_info.h"
#include "orte/util/error_strings.h"
#include "orte/mca/errmgr/errmgr.h"
#include "orte/mca/ess/ess.h"
#include "orte/mca/rml/rml.h"
#include "orte/mca/rml/rml_types.h"
#include "orte/mca/routed/routed.h"
#include "orte/mca/ras/base/base.h"
#include "orte/util/name_fns.h"
#include "orte/mca/state/state.h"
#include "orte/runtime/orte_globals.h"
#include "orte/runtime/orte_quit.h"

#include "orte/mca/plm/plm_types.h"
#include "orte/mca/plm/plm.h"
#include "orte/mca/plm/base/plm_private.h"
#include "orte/mca/plm/base/base.h"

static bool recv_issued=false;

int orte_plm_base_comm_start(void)
{
    if (recv_issued) {
        return ORTE_SUCCESS;
    }

    OPAL_OUTPUT_VERBOSE((5, orte_plm_base_framework.framework_output,
                         "%s plm:base:receive start comm",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));

    orte_rml.recv_buffer_nb(ORTE_NAME_WILDCARD,
                            ORTE_RML_TAG_PLM,
                            ORTE_RML_PERSISTENT,
                            orte_plm_base_recv,
                            NULL);
    if (ORTE_PROC_IS_HNP) {
        orte_rml.recv_buffer_nb(ORTE_NAME_WILDCARD,
                                ORTE_RML_TAG_ORTED_CALLBACK,
                                ORTE_RML_PERSISTENT,
                                orte_plm_base_daemon_callback, NULL);
        orte_rml.recv_buffer_nb(ORTE_NAME_WILDCARD,
                                ORTE_RML_TAG_REPORT_REMOTE_LAUNCH,
                                ORTE_RML_PERSISTENT,
                                orte_plm_base_daemon_failed, NULL);
        orte_rml.recv_buffer_nb(ORTE_NAME_WILDCARD,
                                ORTE_RML_TAG_TOPOLOGY_REPORT,
                                ORTE_RML_PERSISTENT,
                                orte_plm_base_daemon_topology, NULL);
    }
    recv_issued = true;

    return ORTE_SUCCESS;
}


int orte_plm_base_comm_stop(void)
{
    if (!recv_issued) {
        return ORTE_SUCCESS;
    }

    OPAL_OUTPUT_VERBOSE((5, orte_plm_base_framework.framework_output,
                         "%s plm:base:receive stop comm",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));

    orte_rml.recv_cancel(ORTE_NAME_WILDCARD, ORTE_RML_TAG_PLM);
    if (ORTE_PROC_IS_HNP) {
        orte_rml.recv_cancel(ORTE_NAME_WILDCARD, ORTE_RML_TAG_ORTED_CALLBACK);
        orte_rml.recv_cancel(ORTE_NAME_WILDCARD, ORTE_RML_TAG_REPORT_REMOTE_LAUNCH);
        orte_rml.recv_cancel(ORTE_NAME_WILDCARD, ORTE_RML_TAG_TOPOLOGY_REPORT);
    }
    recv_issued = false;

    return ORTE_SUCCESS;
}


/* process incoming messages in order of receipt */
void orte_plm_base_recv(int status, orte_process_name_t* sender,
                        opal_buffer_t* buffer, orte_rml_tag_t tag,
                        void* cbdata)
{
    orte_plm_cmd_flag_t command;
    orte_std_cntr_t count;
    orte_jobid_t job;
    orte_job_t *jdata, *parent;
    opal_buffer_t *answer;
    orte_vpid_t vpid;
    orte_proc_t *proc;
    orte_proc_state_t state;
    orte_exit_code_t exit_code;
    int32_t rc=ORTE_SUCCESS, ret;
    orte_app_context_t *app, *child_app;
    orte_process_name_t name, *nptr;
    pid_t pid;
    bool running;
    int i, room;
    char **env;
    char *prefix_dir;

    OPAL_OUTPUT_VERBOSE((5, orte_plm_base_framework.framework_output,
                         "%s plm:base:receive processing msg",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));

    count = 1;
    if (ORTE_SUCCESS != (rc = opal_dss.unpack(buffer, &command, &count, ORTE_PLM_CMD))) {
        ORTE_ERROR_LOG(rc);
        goto CLEANUP;
    }

    switch (command) {
    case ORTE_PLM_LAUNCH_JOB_CMD:
        OPAL_OUTPUT_VERBOSE((5, orte_plm_base_framework.framework_output,
                             "%s plm:base:receive job launch command from %s",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             ORTE_NAME_PRINT(sender)));

        /* unpack the job object */
        count = 1;
        if (ORTE_SUCCESS != (rc = opal_dss.unpack(buffer, &jdata, &count, ORTE_JOB))) {
            ORTE_ERROR_LOG(rc);
            goto ANSWER_LAUNCH;
        }

        /* record the sender so we know who to respond to */
        jdata->originator.jobid = sender->jobid;
        jdata->originator.vpid = sender->vpid;

        /* get the name of the actual spawn parent - i.e., the proc that actually
         * requested the spawn */
        nptr = &name;
        if (!orte_get_attribute(&jdata->attributes, ORTE_JOB_LAUNCH_PROXY, (void**)&nptr, OPAL_NAME)) {
            ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
            rc = ORTE_ERR_NOT_FOUND;
            goto ANSWER_LAUNCH;
        }

        /* get the parent's job object */
        if (NULL != (parent = orte_get_job_data_object(name.jobid))) {
            /* if the prefix was set in the parent's job, we need to transfer
             * that prefix to the child's app_context so any further launch of
             * orteds can find the correct binary. There always has to be at
             * least one app_context in both parent and child, so we don't
             * need to check that here. However, be sure not to overwrite
             * the prefix if the user already provided it!
             */
            app = (orte_app_context_t*)opal_pointer_array_get_item(parent->apps, 0);
            child_app = (orte_app_context_t*)opal_pointer_array_get_item(jdata->apps, 0);
            prefix_dir = NULL;
            if (orte_get_attribute(&app->attributes, ORTE_APP_PREFIX_DIR, (void**)&prefix_dir, OPAL_STRING) &&
                !orte_get_attribute(&child_app->attributes, ORTE_APP_PREFIX_DIR, NULL, OPAL_STRING)) {
                orte_set_attribute(&child_app->attributes, ORTE_APP_PREFIX_DIR, ORTE_ATTR_GLOBAL, prefix_dir, OPAL_STRING);
            }
            if (NULL != prefix_dir) {
                free(prefix_dir);
            }
        }

        /* if the user asked to forward any envars, cycle through the app contexts
         * in the comm_spawn request and add them
         */
        if (NULL != orte_forwarded_envars) {
            for (i=0; i < jdata->apps->size; i++) {
                if (NULL == (app = (orte_app_context_t*)opal_pointer_array_get_item(jdata->apps, i))) {
                    continue;
                }
                env = opal_environ_merge(orte_forwarded_envars, app->env);
                opal_argv_free(app->env);
                app->env = env;
            }
        }

        OPAL_OUTPUT_VERBOSE((5, orte_plm_base_framework.framework_output,
                             "%s plm:base:receive adding hosts",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));

        /* process any add-hostfile and add-host options that were provided */
        if (ORTE_SUCCESS != (rc = orte_ras_base_add_hosts(jdata))) {
            ORTE_ERROR_LOG(rc);
            goto ANSWER_LAUNCH;
        }

        if (NULL != parent) {
            if (NULL == parent->bookmark) {
                /* find the sender's node in the job map */
                if (NULL != (proc = (orte_proc_t*)opal_pointer_array_get_item(parent->procs, sender->vpid))) {
                    /* set the bookmark so the child starts from that place - this means
                     * that the first child process could be co-located with the proc
                     * that called comm_spawn, assuming slots remain on that node. Otherwise,
                     * the procs will start on the next available node
                     */
                    jdata->bookmark = proc->node;
                }
            } else {
                jdata->bookmark = parent->bookmark;
            }
            /* provide the parent's last object */
            jdata->bkmark_obj = parent->bkmark_obj;
        }

        /* launch it */
        OPAL_OUTPUT_VERBOSE((5, orte_plm_base_framework.framework_output,
                             "%s plm:base:receive calling spawn",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));
        if (ORTE_SUCCESS != (rc = orte_plm.spawn(jdata))) {
            ORTE_ERROR_LOG(rc);
            goto ANSWER_LAUNCH;
        }
        break;
    ANSWER_LAUNCH:
        OPAL_OUTPUT_VERBOSE((5, orte_plm_base_framework.framework_output,
                             "%s plm:base:receive - error on launch: %d",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), rc));

        /* setup the response */
        answer = OBJ_NEW(opal_buffer_t);

        /* pack the error code to be returned */
        if (ORTE_SUCCESS != (ret = opal_dss.pack(answer, &rc, 1, OPAL_INT32))) {
            ORTE_ERROR_LOG(ret);
        }

        /* pack an invalid jobid */
        job = ORTE_JOBID_INVALID;
        if (ORTE_SUCCESS != (ret = opal_dss.pack(answer, &job, 1, ORTE_JOBID))) {
            ORTE_ERROR_LOG(ret);
        }
        /* pack the room number of the request */
        if (orte_get_attribute(&jdata->attributes, ORTE_JOB_ROOM_NUM, (void**)&room, OPAL_INT)) {
            if (ORTE_SUCCESS != (ret = opal_dss.pack(answer, &room, 1, OPAL_INT))) {
                ORTE_ERROR_LOG(ret);
            }
        }

        /* send the response back to the sender */
        if (0 > (ret = orte_rml.send_buffer_nb(orte_mgmt_conduit,
                                               sender, answer, ORTE_RML_TAG_LAUNCH_RESP,
                                               orte_rml_send_callback, NULL))) {
            ORTE_ERROR_LOG(ret);
            OBJ_RELEASE(answer);
        }
        break;

    case ORTE_PLM_UPDATE_PROC_STATE:
        opal_output_verbose(5, orte_plm_base_framework.framework_output,
                            "%s plm:base:receive update proc state command from %s",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                            ORTE_NAME_PRINT(sender));
        count = 1;
        while (ORTE_SUCCESS == (rc = opal_dss.unpack(buffer, &job, &count, ORTE_JOBID))) {

            opal_output_verbose(5, orte_plm_base_framework.framework_output,
                                "%s plm:base:receive got update_proc_state for job %s",
                                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                ORTE_JOBID_PRINT(job));

            name.jobid = job;
            running = false;
            /* get the job object */
            jdata = orte_get_job_data_object(job);
            count = 1;
            while (ORTE_SUCCESS == (rc = opal_dss.unpack(buffer, &vpid, &count, ORTE_VPID))) {
                if (ORTE_VPID_INVALID == vpid) {
                    /* flag indicates that this job is complete - move on */
                    break;
                }
                name.vpid = vpid;
                /* unpack the pid */
                count = 1;
                if (ORTE_SUCCESS != (rc = opal_dss.unpack(buffer, &pid, &count, OPAL_PID))) {
                    ORTE_ERROR_LOG(rc);
                    goto CLEANUP;
                }
                /* unpack the state */
                count = 1;
                if (ORTE_SUCCESS != (rc = opal_dss.unpack(buffer, &state, &count, ORTE_PROC_STATE))) {
                    ORTE_ERROR_LOG(rc);
                    goto CLEANUP;
                }
                if (ORTE_PROC_STATE_RUNNING == state) {
                    running = true;
                }
                /* unpack the exit code */
                count = 1;
                if (ORTE_SUCCESS != (rc = opal_dss.unpack(buffer, &exit_code, &count, ORTE_EXIT_CODE))) {
                    ORTE_ERROR_LOG(rc);
                    goto CLEANUP;
                }

                OPAL_OUTPUT_VERBOSE((5, orte_plm_base_framework.framework_output,
                                     "%s plm:base:receive got update_proc_state for vpid %lu state %s exit_code %d",
                                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                     (unsigned long)vpid, orte_proc_state_to_str(state), (int)exit_code));

                if (NULL != jdata) {
                    /* get the proc data object */
                    if (NULL == (proc = (orte_proc_t*)opal_pointer_array_get_item(jdata->procs, vpid))) {
                        ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
                        ORTE_FORCED_TERMINATE(ORTE_ERROR_DEFAULT_EXIT_CODE);
                        goto CLEANUP;
                    }
                    /* NEVER update the proc state before activating the state machine - let
                     * the state cbfunc update it as it may need to compare this
                     * state against the prior proc state */
                    proc->pid = pid;
                    proc->exit_code = exit_code;
                    ORTE_ACTIVATE_PROC_STATE(&name, state);
                }
            }
            /* record that we heard back from a daemon during app launch */
            if (running && NULL != jdata) {
                jdata->num_daemons_reported++;
                if (orte_report_launch_progress) {
                    if (0 == jdata->num_daemons_reported % 100 ||
                        jdata->num_daemons_reported == orte_process_info.num_procs) {
                        ORTE_ACTIVATE_JOB_STATE(jdata, ORTE_JOB_STATE_REPORT_PROGRESS);
                    }
                }
            }
            /* prepare for next job */
            count = 1;
        }
        if (ORTE_ERR_UNPACK_READ_PAST_END_OF_BUFFER != rc) {
            ORTE_ERROR_LOG(rc);
        } else {
            rc = ORTE_SUCCESS;
        }
        break;

    case ORTE_PLM_REGISTERED_CMD:
        count=1;
        if (ORTE_SUCCESS != (rc = opal_dss.unpack(buffer, &job, &count, ORTE_JOBID))) {
            ORTE_ERROR_LOG(rc);
            goto CLEANUP;
        }
        name.jobid = job;
        /* get the job object */
        if (NULL == (jdata = orte_get_job_data_object(job))) {
            ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
            rc = ORTE_ERR_NOT_FOUND;
            goto CLEANUP;
        }
        count=1;
        while (ORTE_SUCCESS == opal_dss.unpack(buffer, &vpid, &count, ORTE_VPID)) {
            name.vpid = vpid;
            ORTE_ACTIVATE_PROC_STATE(&name, ORTE_PROC_STATE_REGISTERED);
            count=1;
        }
        break;

    default:
        ORTE_ERROR_LOG(ORTE_ERR_VALUE_OUT_OF_BOUNDS);
        rc = ORTE_ERR_VALUE_OUT_OF_BOUNDS;
        break;
    }

  CLEANUP:
    /* see if an error occurred - if so, wakeup the HNP so we can exit */
    if (ORTE_PROC_IS_HNP && ORTE_SUCCESS != rc) {
        jdata = NULL;
        ORTE_FORCED_TERMINATE(ORTE_ERROR_DEFAULT_EXIT_CODE);
    }

    OPAL_OUTPUT_VERBOSE((5, orte_plm_base_framework.framework_output,
                         "%s plm:base:receive done processing commands",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));
}

/* where HNP messages come */
void orte_plm_base_receive_process_msg(int fd, short event, void *data)
{
    assert(0);
}
