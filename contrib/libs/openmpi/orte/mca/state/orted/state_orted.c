/*
 * Copyright (c) 2011-2017 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2014-2018 Intel, Inc. All rights reserved.
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
#include "opal/mca/pmix/pmix.h"

#include "orte/mca/errmgr/errmgr.h"
#include "orte/mca/iof/base/base.h"
#include "orte/mca/odls/base/base.h"
#include "orte/mca/rmaps/rmaps_types.h"
#include "orte/mca/rml/rml.h"
#include "orte/mca/routed/routed.h"
#include "orte/util/session_dir.h"
#include "orte/util/threads.h"
#include "orte/orted/pmix/pmix_server_internal.h"
#include "orte/runtime/orte_data_server.h"
#include "orte/runtime/orte_quit.h"

#include "orte/mca/state/state.h"
#include "orte/mca/state/base/base.h"
#include "orte/mca/state/base/state_private.h"
#include "state_orted.h"

/*
 * Module functions: Global
 */
static int init(void);
static int finalize(void);

/******************
 * ORTED module
 ******************/
orte_state_base_module_t orte_state_orted_module = {
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

/* Local functions */
static void track_jobs(int fd, short argc, void *cbdata);
static void track_procs(int fd, short argc, void *cbdata);
static int pack_state_update(opal_buffer_t *buf, orte_job_t *jdata);

/* defined default state machines */
static orte_job_state_t job_states[] = {
    ORTE_JOB_STATE_LOCAL_LAUNCH_COMPLETE,
};
static orte_state_cbfunc_t job_callbacks[] = {
    track_jobs
};

static orte_proc_state_t proc_states[] = {
    ORTE_PROC_STATE_RUNNING,
    ORTE_PROC_STATE_REGISTERED,
    ORTE_PROC_STATE_IOF_COMPLETE,
    ORTE_PROC_STATE_WAITPID_FIRED,
    ORTE_PROC_STATE_TERMINATED
};
static orte_state_cbfunc_t proc_callbacks[] = {
    track_procs,
    track_procs,
    track_procs,
    track_procs,
    track_procs
};

/************************
 * API Definitions
 ************************/
static int init(void)
{
    int num_states, i, rc;

    /* setup the state machine */
    OBJ_CONSTRUCT(&orte_job_states, opal_list_t);
    OBJ_CONSTRUCT(&orte_proc_states, opal_list_t);

    num_states = sizeof(job_states) / sizeof(orte_job_state_t);
    for (i=0; i < num_states; i++) {
        if (ORTE_SUCCESS != (rc = orte_state.add_job_state(job_states[i],
                                                           job_callbacks[i],
                                                           ORTE_SYS_PRI))) {
            ORTE_ERROR_LOG(rc);
        }
    }
    /* add a default error response */
    if (ORTE_SUCCESS != (rc = orte_state.add_job_state(ORTE_JOB_STATE_FORCED_EXIT,
                                                       orte_quit, ORTE_ERROR_PRI))) {
        ORTE_ERROR_LOG(rc);
    }
    /* add a state for when we are ordered to terminate */
    if (ORTE_SUCCESS != (rc = orte_state.add_job_state(ORTE_JOB_STATE_DAEMONS_TERMINATED,
                                                       orte_quit, ORTE_SYS_PRI))) {
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
    opal_list_item_t *item;

    /* cleanup the state machines */
    while (NULL != (item = opal_list_remove_first(&orte_job_states))) {
        OBJ_RELEASE(item);
    }
    OBJ_DESTRUCT(&orte_job_states);
    while (NULL != (item = opal_list_remove_first(&orte_proc_states))) {
        OBJ_RELEASE(item);
    }
    OBJ_DESTRUCT(&orte_proc_states);

    return ORTE_SUCCESS;
}

static void track_jobs(int fd, short argc, void *cbdata)
{
    orte_state_caddy_t *caddy = (orte_state_caddy_t*)cbdata;
    opal_buffer_t *alert;
    orte_plm_cmd_flag_t cmd;
    int rc, i;
    orte_proc_state_t running = ORTE_PROC_STATE_RUNNING;
    orte_proc_t *child;
    orte_vpid_t null=ORTE_VPID_INVALID;

    ORTE_ACQUIRE_OBJECT(caddy);

    if (ORTE_JOB_STATE_LOCAL_LAUNCH_COMPLETE == caddy->job_state) {
        OPAL_OUTPUT_VERBOSE((5, orte_state_base_framework.framework_output,
                            "%s state:orted:track_jobs sending local launch complete for job %s",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                            ORTE_JOBID_PRINT(caddy->jdata->jobid)));
        /* update the HNP with all proc states for this job */
       alert = OBJ_NEW(opal_buffer_t);
         /* pack update state command */
        cmd = ORTE_PLM_UPDATE_PROC_STATE;
        if (ORTE_SUCCESS != (rc = opal_dss.pack(alert, &cmd, 1, ORTE_PLM_CMD))) {
            ORTE_ERROR_LOG(rc);
            OBJ_RELEASE(alert);
            goto cleanup;
        }
        /* pack the jobid */
        if (ORTE_SUCCESS != (rc = opal_dss.pack(alert, &caddy->jdata->jobid, 1, ORTE_JOBID))) {
            ORTE_ERROR_LOG(rc);
            OBJ_RELEASE(alert);
            goto cleanup;
        }
        for (i=0; i < orte_local_children->size; i++) {
            if (NULL == (child = (orte_proc_t*)opal_pointer_array_get_item(orte_local_children, i))) {
                continue;
            }
            /* if this child is part of the job... */
            if (child->name.jobid == caddy->jdata->jobid) {
                /* pack the child's vpid */
                if (ORTE_SUCCESS != (rc = opal_dss.pack(alert, &(child->name.vpid), 1, ORTE_VPID))) {
                    ORTE_ERROR_LOG(rc);
                    OBJ_RELEASE(alert);
                    goto cleanup;
                }
                /* pack the pid */
                if (ORTE_SUCCESS != (rc = opal_dss.pack(alert, &child->pid, 1, OPAL_PID))) {
                    ORTE_ERROR_LOG(rc);
                    OBJ_RELEASE(alert);
                    goto cleanup;
                }
                /* if this proc failed to start, then send that info */
                if (ORTE_PROC_STATE_UNTERMINATED < child->state) {
                    if (ORTE_SUCCESS != (rc = opal_dss.pack(alert, &child->state, 1, ORTE_PROC_STATE))) {
                        ORTE_ERROR_LOG(rc);
                        OBJ_RELEASE(alert);
                        goto cleanup;
                    }
                } else {
                    /* pack the RUNNING state to avoid any race conditions */
                    if (ORTE_SUCCESS != (rc = opal_dss.pack(alert, &running, 1, ORTE_PROC_STATE))) {
                        ORTE_ERROR_LOG(rc);
                        OBJ_RELEASE(alert);
                        goto cleanup;
                    }
                }
                /* pack its exit code */
                if (ORTE_SUCCESS != (rc = opal_dss.pack(alert, &child->exit_code, 1, ORTE_EXIT_CODE))) {
                    ORTE_ERROR_LOG(rc);
                    OBJ_RELEASE(alert);
                    goto cleanup;
                }
            }
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
        }
    }

  cleanup:
    OBJ_RELEASE(caddy);
}

static void track_procs(int fd, short argc, void *cbdata)
{
    orte_state_caddy_t *caddy = (orte_state_caddy_t*)cbdata;
    orte_process_name_t *proc;
    orte_proc_state_t state;
    orte_job_t *jdata;
    orte_proc_t *pdata, *pptr;
    opal_buffer_t *alert;
    int rc, i;
    orte_plm_cmd_flag_t cmd;
    char *rtmod;
    orte_std_cntr_t index;
    orte_job_map_t *map;
    orte_node_t *node;
    orte_process_name_t target;

    ORTE_ACQUIRE_OBJECT(caddy);
    proc = &caddy->name;
    state = caddy->proc_state;

    OPAL_OUTPUT_VERBOSE((5, orte_state_base_framework.framework_output,
                         "%s state:orted:track_procs called for proc %s state %s",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         ORTE_NAME_PRINT(proc),
                         orte_proc_state_to_str(state)));

    /* get the job object for this proc */
    if (NULL == (jdata = orte_get_job_data_object(proc->jobid))) {
        ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
        goto cleanup;
    }
    pdata = (orte_proc_t*)opal_pointer_array_get_item(jdata->procs, proc->vpid);

    if (ORTE_PROC_STATE_RUNNING == state) {
        /* update the proc state */
        pdata->state = state;
        jdata->num_launched++;
        if (jdata->num_launched == jdata->num_local_procs) {
            /* tell the state machine that all local procs for this job
             * were launched so that it can do whatever it needs to do,
             * like send a state update message for all procs to the HNP
             */
            ORTE_ACTIVATE_JOB_STATE(jdata, ORTE_JOB_STATE_LOCAL_LAUNCH_COMPLETE);
        }
        /* don't update until we are told that all are done */
    } else if (ORTE_PROC_STATE_REGISTERED == state) {
        /* update the proc state */
        pdata->state = state;
        jdata->num_reported++;
        if (jdata->num_reported == jdata->num_local_procs) {
            /* once everyone registers, notify the HNP */

            OPAL_OUTPUT_VERBOSE((5, orte_state_base_framework.framework_output,
                                 "%s state:orted: notifying HNP all local registered",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));

            alert = OBJ_NEW(opal_buffer_t);
            /* pack registered command */
            cmd = ORTE_PLM_REGISTERED_CMD;
            if (ORTE_SUCCESS != (rc = opal_dss.pack(alert, &cmd, 1, ORTE_PLM_CMD))) {
                ORTE_ERROR_LOG(rc);
                goto cleanup;
            }
            /* pack the jobid */
            if (ORTE_SUCCESS != (rc = opal_dss.pack(alert, &proc->jobid, 1, ORTE_JOBID))) {
                ORTE_ERROR_LOG(rc);
                goto cleanup;
            }
            /* pack all the local child vpids */
            for (i=0; i < orte_local_children->size; i++) {
                if (NULL == (pptr = (orte_proc_t*)opal_pointer_array_get_item(orte_local_children, i))) {
                    continue;
                }
                if (pptr->name.jobid == proc->jobid) {
                    if (ORTE_SUCCESS != (rc = opal_dss.pack(alert, &pptr->name.vpid, 1, ORTE_VPID))) {
                        ORTE_ERROR_LOG(rc);
                        goto cleanup;
                    }
                }
            }
            /* send it */
            if (0 > (rc = orte_rml.send_buffer_nb(orte_mgmt_conduit,
                                                  ORTE_PROC_MY_HNP, alert,
                                                  ORTE_RML_TAG_PLM,
                                                  orte_rml_send_callback, NULL))) {
                ORTE_ERROR_LOG(rc);
            } else {
                rc = ORTE_SUCCESS;
            }
        }
    } else if (ORTE_PROC_STATE_IOF_COMPLETE == state) {
        /* do NOT update the proc state as this can hit
         * while we are still trying to notify the HNP of
         * successful launch for short-lived procs
         */
        ORTE_FLAG_SET(pdata, ORTE_PROC_FLAG_IOF_COMPLETE);
        /* Release the stdin IOF file descriptor for this child, if one
         * was defined. File descriptors for the other IOF channels - stdout,
         * stderr, and stddiag - were released when their associated pipes
         * were cleared and closed due to termination of the process
         * Do this after we handle termination in case the IOF needs
         * to check to see if all procs from the job are actually terminated
         */
        if (NULL != orte_iof.close) {
            orte_iof.close(proc, ORTE_IOF_STDALL);
        }
        if (ORTE_FLAG_TEST(pdata, ORTE_PROC_FLAG_WAITPID) &&
            !ORTE_FLAG_TEST(pdata, ORTE_PROC_FLAG_RECORDED)) {
            ORTE_ACTIVATE_PROC_STATE(proc, ORTE_PROC_STATE_TERMINATED);
        }
    } else if (ORTE_PROC_STATE_WAITPID_FIRED == state) {
        /* do NOT update the proc state as this can hit
         * while we are still trying to notify the HNP of
         * successful launch for short-lived procs
         */
        ORTE_FLAG_SET(pdata, ORTE_PROC_FLAG_WAITPID);
        if (ORTE_FLAG_TEST(pdata, ORTE_PROC_FLAG_IOF_COMPLETE) &&
            !ORTE_FLAG_TEST(pdata, ORTE_PROC_FLAG_RECORDED)) {
            ORTE_ACTIVATE_PROC_STATE(proc, ORTE_PROC_STATE_TERMINATED);
        }
    } else if (ORTE_PROC_STATE_TERMINATED == state) {
        /* if this proc has not already recorded as terminated, then
         * update the accounting here */
        if (!ORTE_FLAG_TEST(pdata, ORTE_PROC_FLAG_RECORDED)) {
            jdata->num_terminated++;
        }
        /* update the proc state */
        ORTE_FLAG_SET(pdata, ORTE_PROC_FLAG_RECORDED);
        ORTE_FLAG_UNSET(pdata, ORTE_PROC_FLAG_ALIVE);
        pdata->state = state;
        /* Clean up the session directory as if we were the process
         * itself.  This covers the case where the process died abnormally
         * and didn't cleanup its own session directory.
         */
        orte_session_dir_finalize(proc);
        /* if we are trying to terminate and our routes are
         * gone, then terminate ourselves IF no local procs
         * remain (might be some from another job)
         */
        rtmod = orte_rml.get_routed(orte_mgmt_conduit);
        if (orte_orteds_term_ordered &&
            0 == orte_routed.num_routes(rtmod)) {
            for (i=0; i < orte_local_children->size; i++) {
                if (NULL != (pdata = (orte_proc_t*)opal_pointer_array_get_item(orte_local_children, i)) &&
                    ORTE_FLAG_TEST(pdata, ORTE_PROC_FLAG_ALIVE)) {
                    /* at least one is still alive */
                    OPAL_OUTPUT_VERBOSE((5, orte_state_base_framework.framework_output,
                                         "%s state:orted all routes gone but proc %s still alive",
                                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                         ORTE_NAME_PRINT(&pdata->name)));
                    goto cleanup;
                }
            }
            /* call our appropriate exit procedure */
            OPAL_OUTPUT_VERBOSE((5, orte_state_base_framework.framework_output,
                                 "%s state:orted all routes and children gone - exiting",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));
            ORTE_ACTIVATE_JOB_STATE(NULL, ORTE_JOB_STATE_DAEMONS_TERMINATED);
            goto cleanup;
        }
        /* track job status */
        if (jdata->num_terminated == jdata->num_local_procs &&
            !orte_get_attribute(&jdata->attributes, ORTE_JOB_TERM_NOTIFIED, NULL, OPAL_BOOL)) {
            /* pack update state command */
            cmd = ORTE_PLM_UPDATE_PROC_STATE;
            alert = OBJ_NEW(opal_buffer_t);
            if (ORTE_SUCCESS != (rc = opal_dss.pack(alert, &cmd, 1, ORTE_PLM_CMD))) {
                ORTE_ERROR_LOG(rc);
                goto cleanup;
            }
            /* pack the job info */
            if (ORTE_SUCCESS != (rc = pack_state_update(alert, jdata))) {
                ORTE_ERROR_LOG(rc);
            }
            /* send it */
            OPAL_OUTPUT_VERBOSE((5, orte_state_base_framework.framework_output,
                                 "%s state:orted: SENDING JOB LOCAL TERMINATION UPDATE FOR JOB %s",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 ORTE_JOBID_PRINT(jdata->jobid)));
            if (0 > (rc = orte_rml.send_buffer_nb(orte_mgmt_conduit,
                                                  ORTE_PROC_MY_HNP, alert,
                                                  ORTE_RML_TAG_PLM,
                                                  orte_rml_send_callback, NULL))) {
                ORTE_ERROR_LOG(rc);
            }
            /* mark that we sent it so we ensure we don't do it again */
            orte_set_attribute(&jdata->attributes, ORTE_JOB_TERM_NOTIFIED, ORTE_ATTR_LOCAL, NULL, OPAL_BOOL);
            /* cleanup the procs as these are gone */
            for (i=0; i < orte_local_children->size; i++) {
                if (NULL == (pptr = (orte_proc_t*)opal_pointer_array_get_item(orte_local_children, i))) {
                    continue;
                }
                /* if this child is part of the job... */
                if (pptr->name.jobid == jdata->jobid) {
                    /* clear the entry in the local children */
                    opal_pointer_array_set_item(orte_local_children, i, NULL);
                    OBJ_RELEASE(pptr);  // maintain accounting
                }
            }
            /* tell the IOF that the job is complete */
            if (NULL != orte_iof.complete) {
                orte_iof.complete(jdata);
            }

            /* tell the PMIx subsystem the job is complete */
            if (NULL != opal_pmix.server_deregister_nspace) {
                opal_pmix.server_deregister_nspace(jdata->jobid, NULL, NULL);
            }

            /* release the resources */
            if (NULL != jdata->map) {
                map = jdata->map;
                for (index = 0; index < map->nodes->size; index++) {
                    if (NULL == (node = (orte_node_t*)opal_pointer_array_get_item(map->nodes, index))) {
                        continue;
                    }
                    OPAL_OUTPUT_VERBOSE((2, orte_state_base_framework.framework_output,
                                         "%s state:orted releasing procs from node %s",
                                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                         node->name));
                    for (i = 0; i < node->procs->size; i++) {
                        if (NULL == (pptr = (orte_proc_t*)opal_pointer_array_get_item(node->procs, i))) {
                            continue;
                        }
                        if (pptr->name.jobid != jdata->jobid) {
                            /* skip procs from another job */
                            continue;
                        }
                        if (!ORTE_FLAG_TEST(pptr, ORTE_PROC_FLAG_TOOL)) {
                            node->slots_inuse--;
                            node->num_procs--;
                        }
                        OPAL_OUTPUT_VERBOSE((2, orte_state_base_framework.framework_output,
                                             "%s state:orted releasing proc %s from node %s",
                                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                             ORTE_NAME_PRINT(&pptr->name), node->name));
                        /* set the entry in the node array to NULL */
                        opal_pointer_array_set_item(node->procs, i, NULL);
                        /* release the proc once for the map entry */
                        OBJ_RELEASE(pptr);
                    }
                    /* set the node location to NULL */
                    opal_pointer_array_set_item(map->nodes, index, NULL);
                    /* maintain accounting */
                    OBJ_RELEASE(node);
                    /* flag that the node is no longer in a map */
                    ORTE_FLAG_UNSET(node, ORTE_NODE_FLAG_MAPPED);
                }
                OBJ_RELEASE(map);
                jdata->map = NULL;
            }

            /* if requested, check fd status for leaks */
            if (orte_state_base_run_fdcheck) {
                orte_state_base_check_fds(jdata);
            }

            /* if ompi-server is around, then notify it to purge
             * any session-related info */
            if (NULL != orte_data_server_uri) {
                target.jobid = jdata->jobid;
                target.vpid = ORTE_VPID_WILDCARD;
                orte_state_base_notify_data_server(&target);
            }

            /* cleanup the job info */
            opal_hash_table_set_value_uint32(orte_job_data, jdata->jobid, NULL);
            OBJ_RELEASE(jdata);
        }
    }

  cleanup:
    OBJ_RELEASE(caddy);
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

static int pack_state_update(opal_buffer_t *alert, orte_job_t *jdata)
{
    int i, rc;
    orte_proc_t *child;
    orte_vpid_t null=ORTE_VPID_INVALID;

    /* pack the jobid */
    if (ORTE_SUCCESS != (rc = opal_dss.pack(alert, &jdata->jobid, 1, ORTE_JOBID))) {
        ORTE_ERROR_LOG(rc);
        return rc;
    }
    for (i=0; i < orte_local_children->size; i++) {
        if (NULL == (child = (orte_proc_t*)opal_pointer_array_get_item(orte_local_children, i))) {
            continue;
        }
        /* if this child is part of the job... */
        if (child->name.jobid == jdata->jobid) {
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
