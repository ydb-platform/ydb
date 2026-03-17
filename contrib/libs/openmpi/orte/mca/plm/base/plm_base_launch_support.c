/*
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
 * Copyright (c) 2007-2017 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2009      Institut National de Recherche en Informatique
 *                         et Automatique. All rights reserved.
 * Copyright (c) 2011-2012 Los Alamos National Security, LLC.
 * Copyright (c) 2013-2018 Intel, Inc.  All rights reserved.
 * Copyright (c) 2014-2018 Research Organization for Information Science
 *                         and Technology (RIST).  All rights reserved.
 * Copyright (c) 2016      IBM Corporation.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 */

#include "orte_config.h"
#include "orte/constants.h"

#ifdef HAVE_SYS_WAIT_H
#include <sys/wait.h>
#endif
#ifdef HAVE_SYS_TIME_H
#include <sys/time.h>
#endif  /* HAVE_SYS_TIME_H */
#include <ctype.h>

#include "opal/hash_string.h"
#include "opal/util/argv.h"
#include "opal/util/opal_environ.h"
#include "opal/class/opal_pointer_array.h"
#include "opal/dss/dss.h"
#include "opal/mca/hwloc/hwloc-internal.h"
#include "opal/mca/pmix/pmix.h"

#include "orte/util/dash_host/dash_host.h"
#include "orte/util/session_dir.h"
#include "orte/util/show_help.h"
#include "orte/mca/errmgr/errmgr.h"
#include "orte/mca/ess/ess.h"
#include "orte/mca/iof/base/base.h"
#include "orte/mca/odls/base/base.h"
#include "orte/mca/ras/base/base.h"
#include "orte/mca/regx/regx.h"
#include "orte/mca/rmaps/rmaps.h"
#include "orte/mca/rmaps/base/base.h"
#include "orte/mca/rml/rml.h"
#include "orte/mca/rml/rml_types.h"
#include "orte/mca/routed/routed.h"
#include "orte/mca/grpcomm/base/base.h"
#if OPAL_ENABLE_FT_CR == 1
#include "orte/mca/snapc/base/base.h"
#endif
#include "orte/mca/filem/filem.h"
#include "orte/mca/filem/base/base.h"
#include "orte/mca/grpcomm/base/base.h"
#include "orte/mca/rml/base/rml_contact.h"
#include "orte/mca/rtc/rtc.h"
#include "orte/runtime/orte_globals.h"
#include "orte/runtime/runtime.h"
#include "orte/runtime/orte_locks.h"
#include "orte/runtime/orte_quit.h"
#include "orte/util/compress.h"
#include "orte/util/name_fns.h"
#include "orte/util/pre_condition_transports.h"
#include "orte/util/proc_info.h"
#include "orte/util/threads.h"
#include "orte/mca/state/state.h"
#include "orte/mca/state/base/base.h"
#include "orte/util/hostfile/hostfile.h"
#include "orte/mca/odls/odls_types.h"

#include "orte/mca/plm/base/plm_private.h"
#include "orte/mca/plm/base/base.h"

void orte_plm_base_set_slots(orte_node_t *node)
{
    if (0 == strncmp(orte_set_slots, "cores", strlen(orte_set_slots))) {
        if (NULL != node->topology && NULL != node->topology->topo) {
            node->slots = opal_hwloc_base_get_nbobjs_by_type(node->topology->topo,
                                                             HWLOC_OBJ_CORE, 0,
                                                             OPAL_HWLOC_LOGICAL);
        }
    } else if (0 == strncmp(orte_set_slots, "sockets", strlen(orte_set_slots))) {
        if (NULL != node->topology && NULL != node->topology->topo) {
            if (0 == (node->slots = opal_hwloc_base_get_nbobjs_by_type(node->topology->topo,
                                                                       HWLOC_OBJ_SOCKET, 0,
                                                                       OPAL_HWLOC_LOGICAL))) {
                /* some systems don't report sockets - in this case,
                 * use numanodes */
                node->slots = opal_hwloc_base_get_nbobjs_by_type(node->topology->topo,
                                                                 HWLOC_OBJ_NODE, 0,
                                                                 OPAL_HWLOC_LOGICAL);
            }
        }
    } else if (0 == strncmp(orte_set_slots, "numas", strlen(orte_set_slots))) {
        if (NULL != node->topology && NULL != node->topology->topo) {
            node->slots = opal_hwloc_base_get_nbobjs_by_type(node->topology->topo,
                                                             HWLOC_OBJ_NODE, 0,
                                                             OPAL_HWLOC_LOGICAL);
        }
    } else if (0 == strncmp(orte_set_slots, "hwthreads", strlen(orte_set_slots))) {
        if (NULL != node->topology && NULL != node->topology->topo) {
            node->slots = opal_hwloc_base_get_nbobjs_by_type(node->topology->topo,
                                                             HWLOC_OBJ_PU, 0,
                                                             OPAL_HWLOC_LOGICAL);
        }
    } else {
        /* must be a number */
        node->slots = strtol(orte_set_slots, NULL, 10);
    }
    /* mark the node as having its slots "given" */
    ORTE_FLAG_SET(node, ORTE_NODE_FLAG_SLOTS_GIVEN);
}

void orte_plm_base_daemons_reported(int fd, short args, void *cbdata)
{
    orte_state_caddy_t *caddy = (orte_state_caddy_t*)cbdata;
    orte_topology_t *t;
    orte_node_t *node;
    int i;

    ORTE_ACQUIRE_OBJECT(caddy);

    /* if we are not launching, then we just assume that all
     * daemons share our topology */
    if (orte_do_not_launch) {
        node = (orte_node_t*)opal_pointer_array_get_item(orte_node_pool, 0);
        t = node->topology;
        for (i=1; i < orte_node_pool->size; i++) {
            if (NULL == (node = (orte_node_t*)opal_pointer_array_get_item(orte_node_pool, i))) {
                continue;
            }
            if (NULL == node->topology) {
                node->topology = t;
            }
        }
    }

    /* if this is an unmanaged allocation, then set the default
     * slots on each node as directed or using default
     */
    if (!orte_managed_allocation) {
        if (NULL != orte_set_slots &&
            0 != strncmp(orte_set_slots, "none", strlen(orte_set_slots))) {
            caddy->jdata->total_slots_alloc = 0;
            for (i=0; i < orte_node_pool->size; i++) {
                if (NULL == (node = (orte_node_t*)opal_pointer_array_get_item(orte_node_pool, i))) {
                    continue;
                }
                if (!ORTE_FLAG_TEST(node, ORTE_NODE_FLAG_SLOTS_GIVEN)) {
                    OPAL_OUTPUT_VERBOSE((5, orte_plm_base_framework.framework_output,
                                         "%s plm:base:setting slots for node %s by %s",
                                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), node->name, orte_set_slots));
                    orte_plm_base_set_slots(node);
                }
                caddy->jdata->total_slots_alloc += node->slots;
            }
        }
    }

    if (orte_display_allocation) {
        orte_ras_base_display_alloc();
    }
    /* ensure we update the routing plan */
    orte_routed.update_routing_plan(NULL);

    /* progress the job */
    caddy->jdata->state = ORTE_JOB_STATE_DAEMONS_REPORTED;
    ORTE_ACTIVATE_JOB_STATE(caddy->jdata, ORTE_JOB_STATE_VM_READY);

    /* cleanup */
    OBJ_RELEASE(caddy);
}

void orte_plm_base_allocation_complete(int fd, short args, void *cbdata)
{
    orte_state_caddy_t *caddy = (orte_state_caddy_t*)cbdata;

    ORTE_ACQUIRE_OBJECT(caddy);

    /* if we don't want to launch, then we at least want
     * to map so we can see where the procs would have
     * gone - so skip to the mapping state */
    if (orte_do_not_launch) {
        caddy->jdata->state = ORTE_JOB_STATE_ALLOCATION_COMPLETE;
        ORTE_ACTIVATE_JOB_STATE(caddy->jdata, ORTE_JOB_STATE_MAP);
    } else {
        /* move the state machine along */
        caddy->jdata->state = ORTE_JOB_STATE_ALLOCATION_COMPLETE;
        ORTE_ACTIVATE_JOB_STATE(caddy->jdata, ORTE_JOB_STATE_LAUNCH_DAEMONS);
    }

    /* cleanup */
    OBJ_RELEASE(caddy);
}

void orte_plm_base_daemons_launched(int fd, short args, void *cbdata)
{
    orte_state_caddy_t *caddy = (orte_state_caddy_t*)cbdata;

    ORTE_ACQUIRE_OBJECT(caddy);

    /* do NOT increment the state - we wait for the
     * daemons to report that they have actually
     * started before moving to the right state
     */
    /* cleanup */
    OBJ_RELEASE(caddy);
}

static void files_ready(int status, void *cbdata)
{
    orte_job_t *jdata = (orte_job_t*)cbdata;

    if (ORTE_SUCCESS != status) {
        ORTE_FORCED_TERMINATE(status);
    } else {
        ORTE_ACTIVATE_JOB_STATE(jdata, ORTE_JOB_STATE_MAP);
    }
}

void orte_plm_base_vm_ready(int fd, short args, void *cbdata)
{
    orte_state_caddy_t *caddy = (orte_state_caddy_t*)cbdata;

    ORTE_ACQUIRE_OBJECT(caddy);

    /* progress the job */
    caddy->jdata->state = ORTE_JOB_STATE_VM_READY;

    /* position any required files */
    if (ORTE_SUCCESS != orte_filem.preposition_files(caddy->jdata, files_ready, caddy->jdata)) {
        ORTE_FORCED_TERMINATE(ORTE_ERROR_DEFAULT_EXIT_CODE);
    }

    /* cleanup */
    OBJ_RELEASE(caddy);
}

void orte_plm_base_mapping_complete(int fd, short args, void *cbdata)
{
    orte_state_caddy_t *caddy = (orte_state_caddy_t*)cbdata;

    ORTE_ACQUIRE_OBJECT(caddy);

    /* move the state machine along */
    caddy->jdata->state = ORTE_JOB_STATE_MAP_COMPLETE;
    ORTE_ACTIVATE_JOB_STATE(caddy->jdata, ORTE_JOB_STATE_SYSTEM_PREP);

    /* cleanup */
    OBJ_RELEASE(caddy);
}


void orte_plm_base_setup_job(int fd, short args, void *cbdata)
{
    int rc;
    int i;
    orte_app_context_t *app;
    orte_state_caddy_t *caddy = (orte_state_caddy_t*)cbdata;
    char *key;
    orte_job_t *parent;
    orte_process_name_t name, *nptr;

    ORTE_ACQUIRE_OBJECT(caddy);

    OPAL_OUTPUT_VERBOSE((5, orte_plm_base_framework.framework_output,
                         "%s plm:base:setup_job",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));

    if (ORTE_JOB_STATE_INIT != caddy->job_state) {
        ORTE_FORCED_TERMINATE(ORTE_ERROR_DEFAULT_EXIT_CODE);
        OBJ_RELEASE(caddy);
        return;
    }
    /* update job state */
    caddy->jdata->state = caddy->job_state;

    /* start by getting a jobid */
    if (ORTE_JOBID_INVALID == caddy->jdata->jobid) {
        if (ORTE_SUCCESS != (rc = orte_plm_base_create_jobid(caddy->jdata))) {
            ORTE_ERROR_LOG(rc);
            ORTE_FORCED_TERMINATE(ORTE_ERROR_DEFAULT_EXIT_CODE);
            OBJ_RELEASE(caddy);
            return;
        }

        /* store it on the global job data pool - this is the key
         * step required before we launch the daemons. It allows
         * the orte_rmaps_base_setup_virtual_machine routine to
         * search all apps for any hosts to be used by the vm
         */
        opal_hash_table_set_value_uint32(orte_job_data, caddy->jdata->jobid, caddy->jdata);
    }

    /* if job recovery is not enabled, set it to default */
    if (!ORTE_FLAG_TEST(caddy->jdata, ORTE_JOB_FLAG_RECOVERABLE) &&
        orte_enable_recovery) {
        ORTE_FLAG_SET(caddy->jdata, ORTE_JOB_FLAG_RECOVERABLE);
    }

    /* setup transport keys in case the MPI layer needs them. If
     * this is a dynamic spawn, then use the same keys as the
     * parent process had so the new/old procs can communicate.
     * Otherwise we can use the jobfam and stepid as unique keys
     * because they are unique values assigned by the RM
     */
     nptr = &name;
     if (orte_get_attribute(&caddy->jdata->attributes, ORTE_JOB_LAUNCH_PROXY, (void**)&nptr, OPAL_NAME)) {
        /* get the parent jdata */
        if (NULL == (parent = orte_get_job_data_object(name.jobid))) {
            ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
            ORTE_FORCED_TERMINATE(ORTE_ERROR_DEFAULT_EXIT_CODE);
            OBJ_RELEASE(caddy);
            return;
        }
        /* a tool might be the parent calling spawn, so cannot require that
         * a job transport key has been assigned to it */
        key = NULL;
        if (orte_get_attribute(&parent->attributes, ORTE_JOB_TRANSPORT_KEY, (void**)&key, OPAL_STRING) &&
            NULL != key) {
            /* record it */
            orte_set_attribute(&caddy->jdata->attributes, ORTE_JOB_TRANSPORT_KEY, ORTE_ATTR_LOCAL, key, OPAL_STRING);
            /* add the transport key envar to each app */
            for (i=0; i < caddy->jdata->apps->size; i++) {
                if (NULL == (app = (orte_app_context_t*)opal_pointer_array_get_item(caddy->jdata->apps, i))) {
                    continue;
                }
                opal_setenv(OPAL_MCA_PREFIX"orte_precondition_transports", key, true, &app->env);
            }
            free(key);
        } else {
            if (ORTE_SUCCESS != (rc = orte_pre_condition_transports(caddy->jdata, NULL))) {
                ORTE_ERROR_LOG(rc);
                ORTE_FORCED_TERMINATE(ORTE_ERROR_DEFAULT_EXIT_CODE);
                OBJ_RELEASE(caddy);
                return;
            }
        }
    } else {
        /* this will also record the transport key attribute in the job object, and
         * adds the key envar to each app */
        if (ORTE_SUCCESS != (rc = orte_pre_condition_transports(caddy->jdata, NULL))) {
            ORTE_ERROR_LOG(rc);
            ORTE_FORCED_TERMINATE(ORTE_ERROR_DEFAULT_EXIT_CODE);
            OBJ_RELEASE(caddy);
            return;
        }
    }

    /* if app recovery is not defined, set apps to defaults */
    for (i=0; i < caddy->jdata->apps->size; i++) {
        if (NULL == (app = (orte_app_context_t*)opal_pointer_array_get_item(caddy->jdata->apps, i))) {
            continue;
        }
        if (!orte_get_attribute(&app->attributes, ORTE_APP_RECOV_DEF, NULL, OPAL_BOOL)) {
            orte_set_attribute(&app->attributes, ORTE_APP_MAX_RESTARTS, ORTE_ATTR_LOCAL, &orte_max_restarts, OPAL_INT32);
        }
    }

    /* set the job state to the next position */
    ORTE_ACTIVATE_JOB_STATE(caddy->jdata, ORTE_JOB_STATE_INIT_COMPLETE);

    /* cleanup */
    OBJ_RELEASE(caddy);
}

void orte_plm_base_setup_job_complete(int fd, short args, void *cbdata)
{
    orte_state_caddy_t *caddy = (orte_state_caddy_t*)cbdata;

    ORTE_ACQUIRE_OBJECT(caddy);

    /* nothing to do here but move along */
    ORTE_ACTIVATE_JOB_STATE(caddy->jdata, ORTE_JOB_STATE_ALLOCATE);
    OBJ_RELEASE(caddy);
}

void orte_plm_base_complete_setup(int fd, short args, void *cbdata)
{
    orte_job_t *jdata, *jdatorted;
    orte_state_caddy_t *caddy = (orte_state_caddy_t*)cbdata;
    orte_node_t *node;
    uint32_t h;
    orte_vpid_t *vptr;
    int i, rc;
    char *serial_number;
    orte_process_name_t requestor, *rptr;

    ORTE_ACQUIRE_OBJECT(caddy);

    opal_output_verbose(5, orte_plm_base_framework.framework_output,
                        "%s complete_setup on job %s",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                        ORTE_JOBID_PRINT(caddy->jdata->jobid));

    /* bozo check */
    if (ORTE_JOB_STATE_SYSTEM_PREP != caddy->job_state) {
        ORTE_FORCED_TERMINATE(ORTE_ERROR_DEFAULT_EXIT_CODE);
        OBJ_RELEASE(caddy);
        return;
    }
    /* update job state */
    caddy->jdata->state = caddy->job_state;

    /* get the orted job data object */
    if (NULL == (jdatorted = orte_get_job_data_object(ORTE_PROC_MY_NAME->jobid))) {
        ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
        ORTE_FORCED_TERMINATE(ORTE_ERROR_DEFAULT_EXIT_CODE);
        OBJ_RELEASE(caddy);
        return;
    }

    /* convenience */
    jdata = caddy->jdata;

    /* If this job is being started by me, then there is nothing
     * further we need to do as any user directives (e.g., to tie
     * off IO to /dev/null) will have been included in the launch
     * message and the IOF knows how to handle any default situation.
     * However, if this is a proxy spawn request, then the spawner
     * might be a tool that wants IO forwarded to it. If that's the
     * situation, then the job object will contain an attribute
     * indicating that request */
    if (orte_get_attribute(&jdata->attributes, ORTE_JOB_FWDIO_TO_TOOL, NULL, OPAL_BOOL)) {
        /* send a message to our IOF containing the requested pull */
        rptr = &requestor;
        if (orte_get_attribute(&jdata->attributes, ORTE_JOB_LAUNCH_PROXY, (void**)&rptr, OPAL_NAME)) {
            ORTE_IOF_PROXY_PULL(jdata, rptr);
        } else {
            ORTE_IOF_PROXY_PULL(jdata, &jdata->originator);
        }
        /* the tool will PUSH its stdin, so nothing we need to do here
         * about stdin */
    }

    /* if coprocessors were detected, now is the time to
     * identify who is attached to what host - this info
     * will be shipped to the daemons in the nidmap. Someday,
     * there may be a direct way for daemons on coprocessors
     * to detect their hosts - but not today.
     */
    if (orte_coprocessors_detected) {
        /* cycle thru the nodes looking for coprocessors */
        for (i=0; i < orte_node_pool->size; i++) {
            if (NULL == (node = (orte_node_t*)opal_pointer_array_get_item(orte_node_pool, i))) {
                continue;
            }
            /* if we don't have a serial number, then we are not a coprocessor */
            serial_number = NULL;
            if (!orte_get_attribute(&node->attributes, ORTE_NODE_SERIAL_NUMBER, (void**)&serial_number, OPAL_STRING)) {
                continue;
            }
            if (NULL != serial_number) {
                /* if we have a serial number, then we are a coprocessor - so
                 * compute our hash and lookup our hostid
                 */
                OPAL_HASH_STR(serial_number, h);
                free(serial_number);
                if (OPAL_SUCCESS != (rc = opal_hash_table_get_value_uint32(orte_coprocessors, h,
                                                                           (void**)&vptr))) {
                    ORTE_ERROR_LOG(rc);
                    break;
                }
                orte_set_attribute(&node->attributes, ORTE_NODE_HOSTID, ORTE_ATTR_LOCAL, vptr, ORTE_VPID);
            }
        }
    }
    /* done with the coprocessor mapping at this time */
    if (NULL != orte_coprocessors) {
        OBJ_RELEASE(orte_coprocessors);
    }

    /* set the job state to the next position */
    ORTE_ACTIVATE_JOB_STATE(jdata, ORTE_JOB_STATE_LAUNCH_APPS);

    /* cleanup */
    OBJ_RELEASE(caddy);
}

/* catch timeout to allow cmds to progress */
static void timer_cb(int fd, short event, void *cbdata)
{
    orte_job_t *jdata = (orte_job_t*)cbdata;
    orte_timer_t *timer=NULL;

    ORTE_ACQUIRE_OBJECT(jdata);

    /* declare launch failed */
    ORTE_ACTIVATE_JOB_STATE(jdata, ORTE_JOB_STATE_FAILED_TO_START);

    /* free event */
    if (orte_get_attribute(&jdata->attributes, ORTE_JOB_FAILURE_TIMER_EVENT, (void**)&timer, OPAL_PTR)) {
        /* timer is an orte_timer_t object */
        OBJ_RELEASE(timer);
        orte_remove_attribute(&jdata->attributes, ORTE_JOB_FAILURE_TIMER_EVENT);
    }
}

void orte_plm_base_launch_apps(int fd, short args, void *cbdata)
{
    orte_state_caddy_t *caddy = (orte_state_caddy_t*)cbdata;
    orte_job_t *jdata;
    orte_daemon_cmd_flag_t command;
    int rc;

    ORTE_ACQUIRE_OBJECT(caddy);

    /* convenience */
    jdata = caddy->jdata;

    if (ORTE_JOB_STATE_LAUNCH_APPS != caddy->job_state) {
        ORTE_FORCED_TERMINATE(ORTE_ERROR_DEFAULT_EXIT_CODE);
        OBJ_RELEASE(caddy);
        return;
    }
    /* update job state */
    caddy->jdata->state = caddy->job_state;

    OPAL_OUTPUT_VERBOSE((5, orte_plm_base_framework.framework_output,
                         "%s plm:base:launch_apps for job %s",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         ORTE_JOBID_PRINT(jdata->jobid)));

    /* pack the appropriate add_local_procs command */
    if (orte_get_attribute(&jdata->attributes, ORTE_JOB_FIXED_DVM, NULL, OPAL_BOOL)) {
        command = ORTE_DAEMON_DVM_ADD_PROCS;
    } else {
        command = ORTE_DAEMON_ADD_LOCAL_PROCS;
    }
    if (ORTE_SUCCESS != (rc = opal_dss.pack(&jdata->launch_msg, &command, 1, ORTE_DAEMON_CMD))) {
        ORTE_ERROR_LOG(rc);
        ORTE_FORCED_TERMINATE(ORTE_ERROR_DEFAULT_EXIT_CODE);
        OBJ_RELEASE(caddy);
        return;
    }

    /* get the local launcher's required data */
    if (ORTE_SUCCESS != (rc = orte_odls.get_add_procs_data(&jdata->launch_msg, jdata->jobid))) {
        ORTE_ERROR_LOG(rc);
        ORTE_FORCED_TERMINATE(ORTE_ERROR_DEFAULT_EXIT_CODE);
    }

    OBJ_RELEASE(caddy);
    return;
}

void orte_plm_base_send_launch_msg(int fd, short args, void *cbdata)
{
    orte_state_caddy_t *caddy = (orte_state_caddy_t*)cbdata;
    orte_timer_t *timer;
    orte_grpcomm_signature_t *sig;
    orte_job_t *jdata;
    int rc;

    /* convenience */
    jdata = caddy->jdata;

    OPAL_OUTPUT_VERBOSE((5, orte_plm_base_framework.framework_output,
                         "%s plm:base:send launch msg for job %s",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         ORTE_JOBID_PRINT(jdata->jobid)));

    /* if we don't want to launch the apps, now is the time to leave */
    if (orte_do_not_launch) {
        bool compressed;
        uint8_t *cmpdata;
        size_t cmplen;
        /* report the size of the launch message */
        compressed = orte_util_compress_block((uint8_t*)jdata->launch_msg.base_ptr,
                                              jdata->launch_msg.bytes_used,
                                              &cmpdata, &cmplen);
        if (compressed) {
            opal_output(0, "LAUNCH MSG RAW SIZE: %d COMPRESSED SIZE: %d",
                        (int)jdata->launch_msg.bytes_used, (int)cmplen);
            free(cmpdata);
        } else {
            opal_output(0, "LAUNCH MSG RAW SIZE: %d", (int)jdata->launch_msg.bytes_used);
        }
        orte_never_launched = true;
        ORTE_FORCED_TERMINATE(0);
        OBJ_RELEASE(caddy);
        return;
    }

    /* goes to all daemons */
    sig = OBJ_NEW(orte_grpcomm_signature_t);
    sig->signature = (orte_process_name_t*)malloc(sizeof(orte_process_name_t));
    sig->signature[0].jobid = ORTE_PROC_MY_NAME->jobid;
    sig->signature[0].vpid = ORTE_VPID_WILDCARD;
    sig->sz = 1;
    if (ORTE_SUCCESS != (rc = orte_grpcomm.xcast(sig, ORTE_RML_TAG_DAEMON, &jdata->launch_msg))) {
        ORTE_ERROR_LOG(rc);
        OBJ_RELEASE(sig);
        ORTE_FORCED_TERMINATE(ORTE_ERROR_DEFAULT_EXIT_CODE);
        OBJ_RELEASE(caddy);
        return;
    }
    OBJ_DESTRUCT(&jdata->launch_msg);
    OBJ_CONSTRUCT(&jdata->launch_msg, opal_buffer_t);
    /* maintain accounting */
    OBJ_RELEASE(sig);

    /* track that we automatically are considered to have reported - used
     * only to report launch progress
     */
    caddy->jdata->num_daemons_reported++;

    /* if requested, setup a timer - if we don't launch within the
     * defined time, then we know things have failed
     */
    if (0 < orte_startup_timeout) {
        OPAL_OUTPUT_VERBOSE((5, orte_plm_base_framework.framework_output,
                             "%s plm:base:launch defining timeout for job %s",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             ORTE_JOBID_PRINT(jdata->jobid)));
        timer = OBJ_NEW(orte_timer_t);
        timer->payload = jdata;
        opal_event_evtimer_set(orte_event_base,
                               timer->ev, timer_cb, jdata);
        opal_event_set_priority(timer->ev, ORTE_ERROR_PRI);
        timer->tv.tv_sec = orte_startup_timeout;
        timer->tv.tv_usec = 0;
        orte_set_attribute(&jdata->attributes, ORTE_JOB_FAILURE_TIMER_EVENT, ORTE_ATTR_LOCAL, timer, OPAL_PTR);
        ORTE_POST_OBJECT(timer);
        opal_event_evtimer_add(timer->ev, &timer->tv);
    }

    /* cleanup */
    OBJ_RELEASE(caddy);
}

void orte_plm_base_post_launch(int fd, short args, void *cbdata)
{
    int32_t rc;
    orte_job_t *jdata;
    orte_state_caddy_t *caddy = (orte_state_caddy_t*)cbdata;
    orte_process_name_t name;
    orte_timer_t *timer=NULL;
    int ret;
    opal_buffer_t *answer;
    int room, *rmptr;

    ORTE_ACQUIRE_OBJECT(caddy);

    /* convenience */
    jdata = caddy->jdata;

    /* if a timer was defined, cancel it */
    if (orte_get_attribute(&jdata->attributes, ORTE_JOB_FAILURE_TIMER_EVENT, (void**)&timer, OPAL_PTR)) {
        opal_event_evtimer_del(timer->ev);
        OPAL_OUTPUT_VERBOSE((5, orte_plm_base_framework.framework_output,
                             "%s plm:base:launch deleting timeout for job %s",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             ORTE_JOBID_PRINT(jdata->jobid)));
        OBJ_RELEASE(timer);
        orte_remove_attribute(&jdata->attributes, ORTE_JOB_FAILURE_TIMER_EVENT);
    }

    if (ORTE_JOB_STATE_RUNNING != caddy->job_state) {
        ORTE_FORCED_TERMINATE(ORTE_ERROR_DEFAULT_EXIT_CODE);
        OBJ_RELEASE(caddy);
        return;
    }
    /* update job state */
    caddy->jdata->state = caddy->job_state;

    /* complete wiring up the iof */
    OPAL_OUTPUT_VERBOSE((5, orte_plm_base_framework.framework_output,
                         "%s plm:base:launch wiring up iof for job %s",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         ORTE_JOBID_PRINT(jdata->jobid)));

    /* push stdin - the IOF will know what to do with the specified target */
    name.jobid = jdata->jobid;
    name.vpid = jdata->stdin_target;

    if (ORTE_SUCCESS != (rc = orte_iof.push(&name, ORTE_IOF_STDIN, 0))) {
        ORTE_ERROR_LOG(rc);
        ORTE_FORCED_TERMINATE(ORTE_ERROR_DEFAULT_EXIT_CODE);
        OBJ_RELEASE(caddy);
        return;
    }

    /* if this isn't a dynamic spawn, just cleanup */
    if (ORTE_JOBID_INVALID == jdata->originator.jobid) {
        OPAL_OUTPUT_VERBOSE((5, orte_plm_base_framework.framework_output,
                             "%s plm:base:launch job %s is not a dynamic spawn",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             ORTE_JOBID_PRINT(jdata->jobid)));
        goto cleanup;
    }

    /* prep the response */
    rc = ORTE_SUCCESS;
    answer = OBJ_NEW(opal_buffer_t);
    /* pack the status */
    if (ORTE_SUCCESS != (ret = opal_dss.pack(answer, &rc, 1, OPAL_INT32))) {
        ORTE_ERROR_LOG(ret);
        ORTE_FORCED_TERMINATE(ORTE_ERROR_DEFAULT_EXIT_CODE);
        OBJ_RELEASE(caddy);
        return;
    }
    /* pack the jobid */
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
    OPAL_OUTPUT_VERBOSE((5, orte_plm_base_framework.framework_output,
                         "%s plm:base:launch sending dyn release of job %s to %s",
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
        OBJ_RELEASE(caddy);
        return;
    }

  cleanup:
    /* cleanup */
    OBJ_RELEASE(caddy);
}

void orte_plm_base_registered(int fd, short args, void *cbdata)
{
    orte_job_t *jdata;
    orte_state_caddy_t *caddy = (orte_state_caddy_t*)cbdata;

    ORTE_ACQUIRE_OBJECT(caddy);

    /* convenience */
    jdata = caddy->jdata;

    OPAL_OUTPUT_VERBOSE((5, orte_plm_base_framework.framework_output,
                         "%s plm:base:launch %s registered",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         ORTE_JOBID_PRINT(jdata->jobid)));

    if (ORTE_JOB_STATE_REGISTERED != caddy->job_state) {
        OPAL_OUTPUT_VERBOSE((5, orte_plm_base_framework.framework_output,
                             "%s plm:base:launch job %s not registered - state %s",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             ORTE_JOBID_PRINT(jdata->jobid),
                             orte_job_state_to_str(caddy->job_state)));
        ORTE_FORCED_TERMINATE(ORTE_ERROR_DEFAULT_EXIT_CODE);
        OBJ_RELEASE(caddy);
        return;
    }
    /* update job state */
    jdata->state = caddy->job_state;

   /* if this wasn't a debugger job, then need to init_after_spawn for debuggers */
    if (!ORTE_FLAG_TEST(jdata, ORTE_JOB_FLAG_DEBUGGER_DAEMON)) {
        ORTE_ACTIVATE_JOB_STATE(jdata, ORTE_JOB_STATE_READY_FOR_DEBUGGERS);
    }

    OBJ_RELEASE(caddy);
}

/* daemons callback when they start - need to listen for them */
static bool orted_failed_launch;
static orte_job_t *jdatorted=NULL;

/* callback for topology reports */
void orte_plm_base_daemon_topology(int status, orte_process_name_t* sender,
                                   opal_buffer_t *buffer,
                                   orte_rml_tag_t tag, void *cbdata)
{
    hwloc_topology_t topo;
    int rc, idx;
    char *sig, *coprocessors, **sns;
    orte_proc_t *daemon=NULL;
    orte_topology_t *t, *t2;
    int i;
    uint32_t h;
    orte_job_t *jdata;
    uint8_t flag;
    size_t inlen, cmplen;
    uint8_t *packed_data, *cmpdata;
    opal_buffer_t datbuf, *data;

    OPAL_OUTPUT_VERBOSE((5, orte_plm_base_framework.framework_output,
                         "%s plm:base:daemon_topology recvd for daemon %s",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         ORTE_NAME_PRINT(sender)));

    /* get the daemon job, if necessary */
    if (NULL == jdatorted) {
        jdatorted = orte_get_job_data_object(ORTE_PROC_MY_NAME->jobid);
    }
    if (NULL == (daemon = (orte_proc_t*)opal_pointer_array_get_item(jdatorted->procs, sender->vpid))) {
        ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
        orted_failed_launch = true;
        goto CLEANUP;
    }
    OBJ_CONSTRUCT(&datbuf, opal_buffer_t);
    /* unpack the flag to see if this payload is compressed */
    idx=1;
    if (ORTE_SUCCESS != (rc = opal_dss.unpack(buffer, &flag, &idx, OPAL_INT8))) {
        ORTE_ERROR_LOG(rc);
        orted_failed_launch = true;
        goto CLEANUP;
    }
    if (flag) {
        /* unpack the data size */
        idx=1;
        if (ORTE_SUCCESS != (rc = opal_dss.unpack(buffer, &inlen, &idx, OPAL_SIZE))) {
            ORTE_ERROR_LOG(rc);
            orted_failed_launch = true;
            goto CLEANUP;
        }
        /* unpack the unpacked data size */
        idx=1;
        if (ORTE_SUCCESS != (rc = opal_dss.unpack(buffer, &cmplen, &idx, OPAL_SIZE))) {
            ORTE_ERROR_LOG(rc);
            orted_failed_launch = true;
            goto CLEANUP;
        }
        /* allocate the space */
        packed_data = (uint8_t*)malloc(inlen);
        /* unpack the data blob */
        idx = inlen;
        if (ORTE_SUCCESS != (rc = opal_dss.unpack(buffer, packed_data, &idx, OPAL_UINT8))) {
            ORTE_ERROR_LOG(rc);
            orted_failed_launch = true;
            goto CLEANUP;
        }
        /* decompress the data */
        if (orte_util_uncompress_block(&cmpdata, cmplen,
                                       packed_data, inlen)) {
            /* the data has been uncompressed */
            opal_dss.load(&datbuf, cmpdata, cmplen);
            data = &datbuf;
        } else {
            data = buffer;
        }
        free(packed_data);
    } else {
        data = buffer;
    }

    /* unpack the topology signature for this node */
    idx=1;
    if (OPAL_SUCCESS != (rc = opal_dss.unpack(data, &sig, &idx, OPAL_STRING))) {
        ORTE_ERROR_LOG(rc);
        orted_failed_launch = true;
        goto CLEANUP;
    }
    /* find it in the array */
    t = NULL;
    for (i=0; i < orte_node_topologies->size; i++) {
        if (NULL == (t2 = (orte_topology_t*)opal_pointer_array_get_item(orte_node_topologies, i))) {
            continue;
        }
        /* just check the signature */
        if (0 == strcmp(sig, t2->sig)) {
            t = t2;
            break;
        }
    }
    if (NULL == t) {
        /* should never happen */
        ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
        orted_failed_launch = true;
        goto CLEANUP;
    }

    /* unpack the topology */
    idx=1;
    if (OPAL_SUCCESS != (rc = opal_dss.unpack(data, &topo, &idx, OPAL_HWLOC_TOPO))) {
        ORTE_ERROR_LOG(rc);
        orted_failed_launch = true;
        goto CLEANUP;
    }
    /* record the final topology */
    t->topo = topo;

    /* unpack any coprocessors */
    idx=1;
    if (OPAL_SUCCESS != (rc = opal_dss.unpack(data, &coprocessors, &idx, OPAL_STRING))) {
        ORTE_ERROR_LOG(rc);
        orted_failed_launch = true;
        goto CLEANUP;
    }
    if (NULL != coprocessors) {
        /* init the hash table, if necessary */
        if (NULL == orte_coprocessors) {
            orte_coprocessors = OBJ_NEW(opal_hash_table_t);
            opal_hash_table_init(orte_coprocessors, orte_process_info.num_procs);
        }
        /* separate the serial numbers of the coprocessors
         * on this host
         */
        sns = opal_argv_split(coprocessors, ',');
        for (idx=0; NULL != sns[idx]; idx++) {
            /* compute the hash */
            OPAL_HASH_STR(sns[idx], h);
            /* mark that this coprocessor is hosted by this node */
            opal_hash_table_set_value_uint32(orte_coprocessors, h, (void*)&daemon->name.vpid);
        }
        opal_argv_free(sns);
        free(coprocessors);
        orte_coprocessors_detected = true;
    }
    /* see if this daemon is on a coprocessor */
    idx=1;
    if (OPAL_SUCCESS != (rc = opal_dss.unpack(data, &coprocessors, &idx, OPAL_STRING))) {
        ORTE_ERROR_LOG(rc);
        orted_failed_launch = true;
        goto CLEANUP;
    }
    if (NULL != coprocessors) {
        if (orte_get_attribute(&daemon->node->attributes, ORTE_NODE_SERIAL_NUMBER, NULL, OPAL_STRING)) {
            /* this is not allowed - a coprocessor cannot be host
             * to another coprocessor at this time
             */
            ORTE_ERROR_LOG(ORTE_ERR_NOT_SUPPORTED);
            orted_failed_launch = true;
            free(coprocessors);
            goto CLEANUP;
        }
        orte_set_attribute(&daemon->node->attributes, ORTE_NODE_SERIAL_NUMBER, ORTE_ATTR_LOCAL, coprocessors, OPAL_STRING);
        free(coprocessors);
        orte_coprocessors_detected = true;
    }

  CLEANUP:
    OPAL_OUTPUT_VERBOSE((5, orte_plm_base_framework.framework_output,
                         "%s plm:base:orted:report_topo launch %s for daemon %s",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         orted_failed_launch ? "failed" : "completed",
                         ORTE_NAME_PRINT(sender)));

    if (orted_failed_launch) {
        ORTE_ACTIVATE_JOB_STATE(jdatorted, ORTE_JOB_STATE_FAILED_TO_START);
        return;
    } else {
        jdatorted->num_reported++;
        OPAL_OUTPUT_VERBOSE((5, orte_plm_base_framework.framework_output,
                             "%s plm:base:orted_report_launch recvd %d of %d reported daemons",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             jdatorted->num_reported, jdatorted->num_procs));
        if (jdatorted->num_procs == jdatorted->num_reported) {
            bool dvm = true;
            uint32_t key;
            void *nptr;
            jdatorted->state = ORTE_JOB_STATE_DAEMONS_REPORTED;
            /* activate the daemons_reported state for all jobs
             * whose daemons were launched
             */
            rc = opal_hash_table_get_first_key_uint32(orte_job_data, &key, (void **)&jdata, &nptr);
            while (OPAL_SUCCESS == rc) {
                if (ORTE_PROC_MY_NAME->jobid != jdata->jobid) {
                    dvm = false;
                    if (ORTE_JOB_STATE_DAEMONS_LAUNCHED == jdata->state) {
                        ORTE_ACTIVATE_JOB_STATE(jdata, ORTE_JOB_STATE_DAEMONS_REPORTED);
                    }
                }
                rc = opal_hash_table_get_next_key_uint32(orte_job_data, &key, (void **)&jdata, nptr, &nptr);
            }
            if (dvm) {
                /* must be launching a DVM - activate the state */
                ORTE_ACTIVATE_JOB_STATE(jdatorted, ORTE_JOB_STATE_DAEMONS_REPORTED);
            }
        }
    }
}

void orte_plm_base_daemon_callback(int status, orte_process_name_t* sender,
                                   opal_buffer_t *buffer,
                                   orte_rml_tag_t tag, void *cbdata)
{
    char *ptr;
    int rc, idx;
    orte_proc_t *daemon=NULL;
    orte_job_t *jdata;
    orte_process_name_t dname;
    opal_buffer_t *relay;
    char *sig;
    orte_topology_t *t;
    hwloc_topology_t topo;
    int i;
    bool found;
    orte_daemon_cmd_flag_t cmd;
    int32_t flag;
    opal_value_t *kv;
    char *myendian;

    /* get the daemon job, if necessary */
    if (NULL == jdatorted) {
        jdatorted = orte_get_job_data_object(ORTE_PROC_MY_NAME->jobid);
    }

    /* get my endianness */
    t = (orte_topology_t*)opal_pointer_array_get_item(orte_node_topologies, 0);
    if (NULL == t) {
        /* should never happen */
        myendian = "unknown";
    } else {
        myendian = strrchr(t->sig, ':');
        ++myendian;
    }

    /* multiple daemons could be in this buffer, so unpack until we exhaust the data */
    idx = 1;
    while (OPAL_SUCCESS == (rc = opal_dss.unpack(buffer, &dname, &idx, ORTE_NAME))) {
        char *nodename = NULL;

        OPAL_OUTPUT_VERBOSE((5, orte_plm_base_framework.framework_output,
                             "%s plm:base:orted_report_launch from daemon %s",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             ORTE_NAME_PRINT(&dname)));

        /* update state and record for this daemon contact info */
        if (NULL == (daemon = (orte_proc_t*)opal_pointer_array_get_item(jdatorted->procs, dname.vpid))) {
            ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
            orted_failed_launch = true;
            goto CLEANUP;
        }
        daemon->state = ORTE_PROC_STATE_RUNNING;
        /* record that this daemon is alive */
        ORTE_FLAG_SET(daemon, ORTE_PROC_FLAG_ALIVE);

        /* unpack the flag indicating the number of connection blobs
         * in the report */
        idx = 1;
        if (ORTE_SUCCESS != (rc = opal_dss.unpack(buffer, &flag, &idx, OPAL_INT32))) {
            ORTE_ERROR_LOG(rc);
            orted_failed_launch = true;
            goto CLEANUP;
        }
        for (i=0; i < flag; i++) {
            idx = 1;
            if (ORTE_SUCCESS != (rc = opal_dss.unpack(buffer, &kv, &idx, OPAL_VALUE))) {
                ORTE_ERROR_LOG(rc);
                orted_failed_launch = true;
                goto CLEANUP;
            }
            /* store this in a daemon wireup buffer for later distribution */
            opal_pmix.store_local(&dname, kv);
            OBJ_RELEASE(kv);
        }

        /* unpack the node name */
        idx = 1;
        if (ORTE_SUCCESS != (rc = opal_dss.unpack(buffer, &nodename, &idx, OPAL_STRING))) {
            ORTE_ERROR_LOG(rc);
            orted_failed_launch = true;
            goto CLEANUP;
        }
        if (!orte_have_fqdn_allocation) {
            /* remove any domain info */
            if (NULL != (ptr = strchr(nodename, '.'))) {
                *ptr = '\0';
                ptr = strdup(nodename);
                free(nodename);
                nodename = ptr;
            }
        }

        OPAL_OUTPUT_VERBOSE((5, orte_plm_base_framework.framework_output,
                             "%s plm:base:orted_report_launch from daemon %s on node %s",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             ORTE_NAME_PRINT(&daemon->name), nodename));

        /* mark the daemon as launched */
        ORTE_FLAG_SET(daemon->node, ORTE_NODE_FLAG_DAEMON_LAUNCHED);

        if (orte_retain_aliases) {
            char *alias, **atmp=NULL;
            uint8_t naliases, ni;
            /* first, store the nodename itself as an alias. We do
             * this in case the nodename isn't the same as what we
             * were given by the allocation. For example, a hostfile
             * might contain an IP address instead of the value returned
             * by gethostname, yet the daemon will have returned the latter
             * and apps may refer to the host by that name
             */
            opal_argv_append_nosize(&atmp, nodename);
            /* unpack and store the provided aliases */
            idx = 1;
            if (ORTE_SUCCESS != (rc = opal_dss.unpack(buffer, &naliases, &idx, OPAL_UINT8))) {
                ORTE_ERROR_LOG(rc);
                orted_failed_launch = true;
                goto CLEANUP;
            }
            for (ni=0; ni < naliases; ni++) {
                idx = 1;
                if (ORTE_SUCCESS != (rc = opal_dss.unpack(buffer, &alias, &idx, OPAL_STRING))) {
                    ORTE_ERROR_LOG(rc);
                    orted_failed_launch = true;
                    goto CLEANUP;
                }
                opal_argv_append_nosize(&atmp, alias);
                free(alias);
            }
            if (0 < naliases) {
                alias = opal_argv_join(atmp, ',');
                orte_set_attribute(&daemon->node->attributes, ORTE_NODE_ALIAS, ORTE_ATTR_LOCAL, alias, OPAL_STRING);
                free(alias);
            }
            opal_argv_free(atmp);
        }

        /* unpack the topology signature for that node */
        idx=1;
        if (OPAL_SUCCESS != (rc = opal_dss.unpack(buffer, &sig, &idx, OPAL_STRING))) {
            ORTE_ERROR_LOG(rc);
            orted_failed_launch = true;
            goto CLEANUP;
        }
        OPAL_OUTPUT_VERBOSE((5, orte_plm_base_framework.framework_output,
                             "%s RECEIVED TOPOLOGY SIG %s FROM NODE %s",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), sig, nodename));

        /* rank=1 always sends its topology back */
        topo = NULL;
        if (1 == dname.vpid) {
            uint8_t flag;
            size_t inlen, cmplen;
            uint8_t *packed_data, *cmpdata;
            opal_buffer_t datbuf, *data;
            OBJ_CONSTRUCT(&datbuf, opal_buffer_t);
            /* unpack the flag to see if this payload is compressed */
            idx=1;
            if (ORTE_SUCCESS != (rc = opal_dss.unpack(buffer, &flag, &idx, OPAL_INT8))) {
                ORTE_ERROR_LOG(rc);
                orted_failed_launch = true;
                goto CLEANUP;
            }
            if (flag) {
                /* unpack the data size */
                idx=1;
                if (ORTE_SUCCESS != (rc = opal_dss.unpack(buffer, &inlen, &idx, OPAL_SIZE))) {
                    ORTE_ERROR_LOG(rc);
                    orted_failed_launch = true;
                    goto CLEANUP;
                }
                /* unpack the unpacked data size */
                idx=1;
                if (ORTE_SUCCESS != (rc = opal_dss.unpack(buffer, &cmplen, &idx, OPAL_SIZE))) {
                    ORTE_ERROR_LOG(rc);
                    orted_failed_launch = true;
                    goto CLEANUP;
                }
                /* allocate the space */
                packed_data = (uint8_t*)malloc(inlen);
                /* unpack the data blob */
                idx = inlen;
                if (ORTE_SUCCESS != (rc = opal_dss.unpack(buffer, packed_data, &idx, OPAL_UINT8))) {
                    ORTE_ERROR_LOG(rc);
                    orted_failed_launch = true;
                    goto CLEANUP;
                }
                /* decompress the data */
                if (orte_util_uncompress_block(&cmpdata, cmplen,
                                               packed_data, inlen)) {
                    /* the data has been uncompressed */
                    opal_dss.load(&datbuf, cmpdata, cmplen);
                    data = &datbuf;
                } else {
                    data = buffer;
                }
                free(packed_data);
            } else {
                data = buffer;
            }
            idx=1;
            if (OPAL_SUCCESS != (rc = opal_dss.unpack(data, &topo, &idx, OPAL_HWLOC_TOPO))) {
                ORTE_ERROR_LOG(rc);
                orted_failed_launch = true;
                goto CLEANUP;
            }
        }

        /* do we already have this topology from some other node? */
        found = false;
        for (i=0; i < orte_node_topologies->size; i++) {
            if (NULL == (t = (orte_topology_t*)opal_pointer_array_get_item(orte_node_topologies, i))) {
                continue;
            }
            /* just check the signature */
            if (0 == strcmp(sig, t->sig)) {
                OPAL_OUTPUT_VERBOSE((5, orte_plm_base_framework.framework_output,
                                     "%s TOPOLOGY ALREADY RECORDED",
                                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));
                found = true;
                daemon->node->topology = t;
                if (NULL != topo) {
                    hwloc_topology_destroy(topo);
                }
                free(sig);
                break;
            }
#if !OPAL_ENABLE_HETEROGENEOUS_SUPPORT
              else {
                /* check if the difference is due to the endianness */
                ptr = strrchr(sig, ':');
                ++ptr;
                if (0 != strcmp(ptr, myendian)) {
                    /* we don't currently handle multi-endian operations in the
                     * MPI support */
                    orte_show_help("help-plm-base", "multi-endian", true,
                                   nodename, ptr, myendian);
                    orted_failed_launch = true;
                    if (NULL != topo) {
                        hwloc_topology_destroy(topo);
                    }
                    goto CLEANUP;
                }
            }
#endif
        }

        if (!found) {
            /* nope - save the signature and request the complete topology from that node */
            OPAL_OUTPUT_VERBOSE((5, orte_plm_base_framework.framework_output,
                                 "%s NEW TOPOLOGY - ADDING",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));
            t = OBJ_NEW(orte_topology_t);
            t->sig = sig;
            opal_pointer_array_add(orte_node_topologies, t);
            daemon->node->topology = t;
            if (NULL != topo) {
                t->topo = topo;
            } else {
                /* nope - save the signature and request the complete topology from that node */
                OPAL_OUTPUT_VERBOSE((5, orte_plm_base_framework.framework_output,
                                     "%s REQUESTING TOPOLOGY FROM %s",
                                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                     ORTE_NAME_PRINT(&dname)));
                /* construct the request */
                relay = OBJ_NEW(opal_buffer_t);
                cmd = ORTE_DAEMON_REPORT_TOPOLOGY_CMD;
                if (OPAL_SUCCESS != (rc = opal_dss.pack(relay, &cmd, 1, ORTE_DAEMON_CMD))) {
                    ORTE_ERROR_LOG(rc);
                    OBJ_RELEASE(relay);
                    orted_failed_launch = true;
                    goto CLEANUP;
                }
                /* send it */
                orte_rml.send_buffer_nb(orte_mgmt_conduit,
                                        &dname, relay,
                                        ORTE_RML_TAG_DAEMON,
                                        orte_rml_send_callback, NULL);
                /* we will count this node as completed
                 * when we get the full topology back */
                if (NULL != nodename) {
                    free(nodename);
                    nodename = NULL;
                }
                idx = 1;
                continue;
            }
        }

      CLEANUP:
        OPAL_OUTPUT_VERBOSE((5, orte_plm_base_framework.framework_output,
                             "%s plm:base:orted_report_launch %s for daemon %s at contact %s",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             orted_failed_launch ? "failed" : "completed",
                             ORTE_NAME_PRINT(&dname),
                             (NULL == daemon) ? "UNKNOWN" : daemon->rml_uri));

        if (NULL != nodename) {
            free(nodename);
            nodename = NULL;
        }

        if (orted_failed_launch) {
            ORTE_ACTIVATE_JOB_STATE(jdatorted, ORTE_JOB_STATE_FAILED_TO_START);
            return;
        } else {
            jdatorted->num_reported++;
            OPAL_OUTPUT_VERBOSE((5, orte_plm_base_framework.framework_output,
                                 "%s plm:base:orted_report_launch job %s recvd %d of %d reported daemons",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 ORTE_JOBID_PRINT(jdatorted->jobid),
                                 jdatorted->num_reported, jdatorted->num_procs));
            if (jdatorted->num_procs == jdatorted->num_reported) {
                bool dvm = true;
                uint32_t key;
                void *nptr;
                jdatorted->state = ORTE_JOB_STATE_DAEMONS_REPORTED;
                /* activate the daemons_reported state for all jobs
                 * whose daemons were launched
                 */
                rc = opal_hash_table_get_first_key_uint32(orte_job_data, &key, (void **)&jdata, &nptr);
                while (OPAL_SUCCESS == rc) {
                    if (ORTE_PROC_MY_NAME->jobid == jdata->jobid) {
                        goto next;
                    }
                    dvm = false;
                    if (ORTE_JOB_STATE_DAEMONS_LAUNCHED == jdata->state) {
                        ORTE_ACTIVATE_JOB_STATE(jdata, ORTE_JOB_STATE_DAEMONS_REPORTED);
                    }
                  next:
                    rc = opal_hash_table_get_next_key_uint32(orte_job_data, &key, (void **)&jdata, nptr, &nptr);
                }
                if (dvm) {
                    /* must be launching a DVM - activate the state */
                    ORTE_ACTIVATE_JOB_STATE(jdatorted, ORTE_JOB_STATE_DAEMONS_REPORTED);
                }
            }
        }
        idx = 1;
    }
    if (ORTE_ERR_UNPACK_READ_PAST_END_OF_BUFFER != rc) {
        ORTE_ERROR_LOG(rc);
        ORTE_ACTIVATE_JOB_STATE(jdatorted, ORTE_JOB_STATE_FAILED_TO_START);
    }
}

void orte_plm_base_daemon_failed(int st, orte_process_name_t* sender,
                                 opal_buffer_t *buffer,
                                 orte_rml_tag_t tag, void *cbdata)
{
    int status, rc;
    int32_t n;
    orte_vpid_t vpid;
    orte_proc_t *daemon=NULL;

    /* get the daemon job, if necessary */
    if (NULL == jdatorted) {
        jdatorted = orte_get_job_data_object(ORTE_PROC_MY_NAME->jobid);
    }

    /* unpack the daemon that failed */
    n=1;
    if (OPAL_SUCCESS != (rc = opal_dss.unpack(buffer, &vpid, &n, ORTE_VPID))) {
        ORTE_ERROR_LOG(rc);
        ORTE_UPDATE_EXIT_STATUS(ORTE_ERROR_DEFAULT_EXIT_CODE);
        goto finish;
    }

    /* unpack the exit status */
    n=1;
    if (OPAL_SUCCESS != (rc = opal_dss.unpack(buffer, &status, &n, OPAL_INT))) {
        ORTE_ERROR_LOG(rc);
        status = ORTE_ERROR_DEFAULT_EXIT_CODE;
        ORTE_UPDATE_EXIT_STATUS(ORTE_ERROR_DEFAULT_EXIT_CODE);
    } else {
        ORTE_UPDATE_EXIT_STATUS(WEXITSTATUS(status));
    }

    /* find the daemon and update its state/status */
    if (NULL == (daemon = (orte_proc_t*)opal_pointer_array_get_item(jdatorted->procs, vpid))) {
        ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
        goto finish;
    }
    daemon->state = ORTE_PROC_STATE_FAILED_TO_START;
    daemon->exit_code = status;

  finish:
    if (NULL == daemon) {
        ORTE_FORCED_TERMINATE(ORTE_ERROR_DEFAULT_EXIT_CODE);
        return;
    }
    ORTE_ACTIVATE_PROC_STATE(&daemon->name, ORTE_PROC_STATE_FAILED_TO_START);
}

int orte_plm_base_setup_orted_cmd(int *argc, char ***argv)
{
    int i, loc;
    char **tmpv;

    /* set default location to be 0, indicating that
     * only a single word is in the cmd
     */
    loc = 0;
    /* split the command apart in case it is multi-word */
    tmpv = opal_argv_split(orte_launch_agent, ' ');
    for (i = 0; NULL != tmpv && NULL != tmpv[i]; ++i) {
        if (0 == strcmp(tmpv[i], "orted")) {
            loc = i;
        }
        opal_argv_append(argc, argv, tmpv[i]);
    }
    opal_argv_free(tmpv);

    return loc;
}


/* pass all options as MCA params so anything we pickup
 * from the environment can be checked for duplicates
 */
int orte_plm_base_orted_append_basic_args(int *argc, char ***argv,
                                          char *ess,
                                          int *proc_vpid_index)
{
    char *param = NULL;
    const char **tmp_value, **tmp_value2;
    int loc_id;
    char *tmp_force = NULL;
    int i, j, cnt, rc;
    orte_job_t *jdata;
    unsigned long num_procs;
    bool ignore;

    /* check for debug flags */
    if (orte_debug_flag) {
        opal_argv_append(argc, argv, "-"OPAL_MCA_CMD_LINE_ID);
        opal_argv_append(argc, argv, "orte_debug");
        opal_argv_append(argc, argv, "1");
    }
    if (orte_debug_daemons_flag) {
        opal_argv_append(argc, argv, "-"OPAL_MCA_CMD_LINE_ID);
        opal_argv_append(argc, argv, "orte_debug_daemons");
        opal_argv_append(argc, argv, "1");
    }
    if (orte_debug_daemons_file_flag) {
        opal_argv_append(argc, argv, "-"OPAL_MCA_CMD_LINE_ID);
        opal_argv_append(argc, argv, "orte_debug_daemons_file");
        opal_argv_append(argc, argv, "1");
    }
    if (orte_leave_session_attached) {
        opal_argv_append(argc, argv, "-"OPAL_MCA_CMD_LINE_ID);
        opal_argv_append(argc, argv, "orte_leave_session_attached");
        opal_argv_append(argc, argv, "1");
    }

    if (orted_spin_flag) {
        opal_argv_append(argc, argv, "--spin");
    }

    if (opal_hwloc_report_bindings) {
        opal_argv_append(argc, argv, "-"OPAL_MCA_CMD_LINE_ID);
        opal_argv_append(argc, argv, "orte_report_bindings");
        opal_argv_append(argc, argv, "1");
    }

    if (orte_map_stddiag_to_stderr) {
        opal_argv_append(argc, argv, "-"OPAL_MCA_CMD_LINE_ID);
        opal_argv_append(argc, argv, "orte_map_stddiag_to_stderr");
        opal_argv_append(argc, argv, "1");
    }
    else if (orte_map_stddiag_to_stdout) {
        opal_argv_append(argc, argv, "-"OPAL_MCA_CMD_LINE_ID);
        opal_argv_append(argc, argv, "orte_map_stddiag_to_stdout");
        opal_argv_append(argc, argv, "1");
    }

    /* the following is not an mca param */
    if (NULL != getenv("ORTE_TEST_ORTED_SUICIDE")) {
        opal_argv_append(argc, argv, "--test-suicide");
    }

    /* tell the orted what ESS component to use */
    if (NULL != ess) {
        opal_argv_append(argc, argv, "-"OPAL_MCA_CMD_LINE_ID);
        opal_argv_append(argc, argv, "ess");
        opal_argv_append(argc, argv, ess);
    }

    /* pass the daemon jobid */
    opal_argv_append(argc, argv, "-"OPAL_MCA_CMD_LINE_ID);
    opal_argv_append(argc, argv, "ess_base_jobid");
    if (ORTE_SUCCESS != (rc = orte_util_convert_jobid_to_string(&param, ORTE_PROC_MY_NAME->jobid))) {
        ORTE_ERROR_LOG(rc);
        return rc;
    }
    opal_argv_append(argc, argv, param);
    free(param);

    /* setup to pass the vpid */
    if (NULL != proc_vpid_index) {
        opal_argv_append(argc, argv, "-"OPAL_MCA_CMD_LINE_ID);
        opal_argv_append(argc, argv, "ess_base_vpid");
        *proc_vpid_index = *argc;
        opal_argv_append(argc, argv, "<template>");
    }

    /* pass the total number of daemons that will be in the system */
    if (ORTE_PROC_IS_HNP) {
        jdata = orte_get_job_data_object(ORTE_PROC_MY_NAME->jobid);
        num_procs = jdata->num_procs;
    } else {
        num_procs = orte_process_info.num_procs;
    }
    opal_argv_append(argc, argv, "-"OPAL_MCA_CMD_LINE_ID);
    opal_argv_append(argc, argv, "ess_base_num_procs");
    asprintf(&param, "%lu", num_procs);
    opal_argv_append(argc, argv, param);
    free(param);

    /* convert the nodes with daemons to a regex */
    param = NULL;
    if (ORTE_SUCCESS != (rc = orte_regx.nidmap_create(orte_node_pool, &param))) {
        ORTE_ERROR_LOG(rc);
        return rc;
    }
    if (NULL != orte_node_regex) {
        free(orte_node_regex);
    }
    orte_node_regex = param;
    /* if this is too long, then we'll have to do it with
     * a phone home operation instead */
    if (strlen(param) < orte_plm_globals.node_regex_threshold) {
        opal_argv_append(argc, argv, "-"OPAL_MCA_CMD_LINE_ID);
        opal_argv_append(argc, argv, "orte_node_regex");
        opal_argv_append(argc, argv, orte_node_regex);
        /* mark that the nidmap has been communicated */
        orte_nidmap_communicated = true;
    }

    if (!orte_static_ports && !orte_fwd_mpirun_port) {
        /* if we are using static ports, or we are forwarding
         * mpirun's port, then we would have built all the
         * connection info and so there is nothing to be passed.
         * Otherwise, we have to pass the HNP uri so we can
         * phone home */
        opal_argv_append(argc, argv, "-"OPAL_MCA_CMD_LINE_ID);
        opal_argv_append(argc, argv, "orte_hnp_uri");
        opal_argv_append(argc, argv, orte_process_info.my_hnp_uri);
    }

    /* if requested, pass our port */
    if (orte_fwd_mpirun_port) {
        asprintf(&param, "%d", orte_process_info.my_port);
        opal_argv_append(argc, argv, "-"OPAL_MCA_CMD_LINE_ID);
        opal_argv_append(argc, argv, "oob_tcp_static_ipv4_ports");
        opal_argv_append(argc, argv, param);
        free(param);
    }

    /* if --xterm was specified, pass that along */
    if (NULL != orte_xterm) {
        opal_argv_append(argc, argv, "-"OPAL_MCA_CMD_LINE_ID);
        opal_argv_append(argc, argv, "orte_xterm");
        opal_argv_append(argc, argv, orte_xterm);
    }

    loc_id = mca_base_var_find("opal", "mca", "base", "param_files");
    if (loc_id < 0) {
        rc = OPAL_ERR_NOT_FOUND;
        ORTE_ERROR_LOG(rc);
        return rc;
    }
    tmp_value = NULL;
    rc = mca_base_var_get_value(loc_id, &tmp_value, NULL, NULL);
    if (ORTE_SUCCESS != rc) {
        ORTE_ERROR_LOG(rc);
        return rc;
    }
    if (NULL != tmp_value && NULL != tmp_value[0]) {
        rc = strcmp(tmp_value[0], "none");
    } else {
        rc = 1;
    }

    if (0 != rc) {
        /*
         * Pass along the Aggregate MCA Parameter Sets
         */
        /* Add the 'prefix' param */
        tmp_value = NULL;

        loc_id = mca_base_var_find("opal", "mca", "base", "envar_file_prefix");
        if (loc_id < 0) {
            rc = OPAL_ERR_NOT_FOUND;
            ORTE_ERROR_LOG(rc);
            return rc;
        }
        rc = mca_base_var_get_value(loc_id, &tmp_value, NULL, NULL);
        if (ORTE_SUCCESS != rc) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }
        if( NULL != tmp_value && NULL != tmp_value[0] ) {
            /* Could also use the short version '-tune'
             * but being verbose has some value
             */
            opal_argv_append(argc, argv, "-mca");
            opal_argv_append(argc, argv, "mca_base_envar_file_prefix");
            opal_argv_append(argc, argv, tmp_value[0]);
        }

        tmp_value2 = NULL;
        loc_id = mca_base_var_find("opal", "mca", "base", "param_file_prefix");
        mca_base_var_get_value(loc_id, &tmp_value2, NULL, NULL);
        if( NULL != tmp_value2 && NULL != tmp_value2[0] ) {
            /* Could also use the short version '-am'
             * but being verbose has some value
             */
            opal_argv_append(argc, argv, "-"OPAL_MCA_CMD_LINE_ID);
            opal_argv_append(argc, argv, "mca_base_param_file_prefix");
            opal_argv_append(argc, argv, tmp_value2[0]);
            orte_show_help("help-plm-base.txt", "deprecated-amca", true);
        }

        if ((NULL != tmp_value && NULL != tmp_value[0])
            || (NULL != tmp_value2 && NULL != tmp_value2[0])) {
            /* Add the 'path' param */
            tmp_value = NULL;
            loc_id = mca_base_var_find("opal", "mca", "base", "param_file_path");
            if (loc_id < 0) {
                ORTE_ERROR_LOG(rc);
                return rc;
            }
            rc = mca_base_var_get_value(loc_id, &tmp_value, NULL, NULL);
            if (ORTE_SUCCESS != rc) {
                ORTE_ERROR_LOG(rc);
                return rc;
            }
            if( NULL != tmp_value && NULL != tmp_value[0] ) {
                opal_argv_append(argc, argv, "-"OPAL_MCA_CMD_LINE_ID);
                opal_argv_append(argc, argv, "mca_base_param_file_path");
                opal_argv_append(argc, argv, tmp_value[0]);
            }

            /* Add the 'path' param */
            opal_argv_append(argc, argv, "-"OPAL_MCA_CMD_LINE_ID);
            opal_argv_append(argc, argv, "mca_base_param_file_path_force");

            tmp_value = NULL;
            loc_id = mca_base_var_find("opal", "mca", "base", "param_file_path_force");
            if (loc_id < 0) {
                rc = OPAL_ERR_NOT_FOUND;
                ORTE_ERROR_LOG(rc);
                return rc;
            }
            rc = mca_base_var_get_value(loc_id, &tmp_value, NULL, NULL);
            if (OPAL_SUCCESS != rc) {
                ORTE_ERROR_LOG(rc);
                return rc;
            }
            if( NULL == tmp_value || NULL == tmp_value[0] ) {
                /* Get the current working directory */
                tmp_force = (char *) malloc(sizeof(char) * OPAL_PATH_MAX);
                if (NULL == getcwd(tmp_force, OPAL_PATH_MAX)) {
                    free(tmp_force);
                    tmp_force = strdup("");
                }

                opal_argv_append(argc, argv, tmp_force);
                free(tmp_force);
            } else {
                opal_argv_append(argc, argv, tmp_value[0]);
            }
        }
    }

    /* pass along any cmd line MCA params provided to mpirun,
     * being sure to "purge" any that would cause problems
     * on backend nodes and ignoring all duplicates
     */
    if (ORTE_PROC_IS_HNP || ORTE_PROC_IS_DAEMON) {
        cnt = opal_argv_count(orted_cmd_line);
        for (i=0; i < cnt; i+=3) {
            /* if the specified option is more than one word, we don't
             * have a generic way of passing it as some environments ignore
             * any quotes we add, while others don't - so we ignore any
             * such options. In most cases, this won't be a problem as
             * they typically only apply to things of interest to the HNP.
             * Individual environments can add these back into the cmd line
             * as they know if it can be supported
             */
            if (NULL != strchr(orted_cmd_line[i+2], ' ')) {
                continue;
            }
            /* The daemon will attempt to open the PLM on the remote
             * end. Only a few environments allow this, so the daemon
             * only opens the PLM -if- it is specifically told to do
             * so by giving it a specific PLM module. To ensure we avoid
             * confusion, do not include any directives here
             */
            if (0 == strcmp(orted_cmd_line[i+1], "plm")) {
                continue;
            }
            /* check for duplicate */
            ignore = false;
            for (j=0; j < *argc; j++) {
                if (0 == strcmp((*argv)[j], orted_cmd_line[i+1])) {
                    ignore = true;
                    break;
                }
            }
            if (!ignore) {
                /* pass it along */
                opal_argv_append(argc, argv, orted_cmd_line[i]);
                opal_argv_append(argc, argv, orted_cmd_line[i+1]);
                opal_argv_append(argc, argv, orted_cmd_line[i+2]);
            }
        }
    }

    return ORTE_SUCCESS;
}

int orte_plm_base_setup_virtual_machine(orte_job_t *jdata)
{
    orte_node_t *node, *nptr;
    orte_proc_t *proc, *pptr;
    orte_job_map_t *map=NULL;
    int rc, i;
    orte_job_t *daemons;
    opal_list_t nodes, tnodes;
    opal_list_item_t *item, *next;
    orte_app_context_t *app;
    bool one_filter = false;
    int num_nodes;
    bool default_hostfile_used;
    char *hosts = NULL;
    bool singleton=false;
    bool multi_sim = false;

    OPAL_OUTPUT_VERBOSE((5, orte_plm_base_framework.framework_output,
                         "%s plm:base:setup_vm",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));

    if (NULL == (daemons = orte_get_job_data_object(ORTE_PROC_MY_NAME->jobid))) {
        ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
        return ORTE_ERR_NOT_FOUND;
    }
    if (NULL == daemons->map) {
        daemons->map = OBJ_NEW(orte_job_map_t);
    }
    map = daemons->map;

    /* if this job is being launched against a fixed DVM, then there is
     * nothing for us to do - the DVM will stand as is */
    if (orte_get_attribute(&jdata->attributes, ORTE_JOB_FIXED_DVM, NULL, OPAL_BOOL)) {
        /* mark that the daemons have reported so we can proceed */
        daemons->state = ORTE_JOB_STATE_DAEMONS_REPORTED;
        map->num_new_daemons = 0;
        return ORTE_SUCCESS;
    }

    /* if this is a dynamic spawn, then we don't make any changes to
     * the virtual machine unless specifically requested to do so
     */
    if (ORTE_JOBID_INVALID != jdata->originator.jobid) {
        if (0 == map->num_nodes) {
            OPAL_OUTPUT_VERBOSE((5, orte_plm_base_framework.framework_output,
                                 "%s plm:base:setup_vm creating map",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));
            /* this is the first time thru, so the vm is just getting
             * defined - create a map for it and put us in as we
             * are obviously already here! The ess will already
             * have assigned our node to us.
             */
            node = (orte_node_t*)opal_pointer_array_get_item(orte_node_pool, 0);
            opal_pointer_array_add(map->nodes, (void*)node);
            ++(map->num_nodes);
            /* maintain accounting */
            OBJ_RETAIN(node);
            /* mark that this is from a singleton */
            singleton = true;
        }
        OBJ_CONSTRUCT(&nodes, opal_list_t);
        for (i=1; i < orte_node_pool->size; i++) {
            if (NULL == (node = (orte_node_t*)opal_pointer_array_get_item(orte_node_pool, i))) {
                continue;
            }
            /* only add in nodes marked as "added" */
            if (!singleton && ORTE_NODE_STATE_ADDED != node->state) {
                OPAL_OUTPUT_VERBOSE((10, orte_plm_base_framework.framework_output,
                                     "%s plm_base:setup_vm NODE %s WAS NOT ADDED",
                                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), node->name));
                continue;
            }
            OPAL_OUTPUT_VERBOSE((10, orte_plm_base_framework.framework_output,
                                 "%s plm_base:setup_vm ADDING NODE %s",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), node->name));
            /* retain a copy for our use in case the item gets
             * destructed along the way
             */
            OBJ_RETAIN(node);
            opal_list_append(&nodes, &node->super);
            /* reset the state so it can be used for mapping */
            node->state = ORTE_NODE_STATE_UP;
        }
        map->num_new_daemons = 0;
        /* if we didn't get anything, then there is nothing else to
         * do as no other daemons are to be launched
         */
        if (0 == opal_list_get_size(&nodes)) {
            OPAL_OUTPUT_VERBOSE((5, orte_plm_base_framework.framework_output,
                                 "%s plm:base:setup_vm no new daemons required",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));
            OBJ_DESTRUCT(&nodes);
            /* mark that the daemons have reported so we can proceed */
            daemons->state = ORTE_JOB_STATE_DAEMONS_REPORTED;
            ORTE_FLAG_UNSET(daemons, ORTE_JOB_FLAG_UPDATED);
            return ORTE_SUCCESS;
        }
        /* if we got some new nodes to launch, we need to handle it */
        goto process;
    }

    /* if we are not working with a virtual machine, then we
     * look across all jobs and ensure that the "VM" contains
     * all nodes with application procs on them
     */
    multi_sim = orte_get_attribute(&jdata->attributes, ORTE_JOB_MULTI_DAEMON_SIM, NULL, OPAL_BOOL);
    if (orte_get_attribute(&daemons->attributes, ORTE_JOB_NO_VM, NULL, OPAL_BOOL) || multi_sim) {
        OBJ_CONSTRUCT(&nodes, opal_list_t);
        /* loop across all nodes and include those that have
         * num_procs > 0 && no daemon already on them
         */
        for (i=1; i < orte_node_pool->size; i++) {
            if (NULL == (node = (orte_node_t*)opal_pointer_array_get_item(orte_node_pool, i))) {
                continue;
            }
            /* ignore nodes that are marked as do-not-use for this mapping */
            if (ORTE_NODE_STATE_DO_NOT_USE == node->state) {
                OPAL_OUTPUT_VERBOSE((10, orte_plm_base_framework.framework_output,
                                     "NODE %s IS MARKED NO_USE", node->name));
                /* reset the state so it can be used another time */
                node->state = ORTE_NODE_STATE_UP;
                continue;
            }
            if (ORTE_NODE_STATE_DOWN == node->state) {
                OPAL_OUTPUT_VERBOSE((10, orte_plm_base_framework.framework_output,
                                     "NODE %s IS MARKED DOWN", node->name));
                continue;
            }
            if (ORTE_NODE_STATE_NOT_INCLUDED == node->state) {
                OPAL_OUTPUT_VERBOSE((10, orte_plm_base_framework.framework_output,
                                     "NODE %s IS MARKED NO_INCLUDE", node->name));
                /* not to be used */
                continue;
            }
            if (0 < node->num_procs || multi_sim) {
                /* retain a copy for our use in case the item gets
                 * destructed along the way
                 */
                OBJ_RETAIN(node);
                opal_list_append(&nodes, &node->super);
            }
        }
        if (multi_sim) {
            goto process;
        }
        /* see if anybody had procs */
        if (0 == opal_list_get_size(&nodes)) {
            /* if the HNP has some procs, then we are still good */
            node = (orte_node_t*)opal_pointer_array_get_item(orte_node_pool, 0);
            if (0 < node->num_procs) {
                OPAL_OUTPUT_VERBOSE((5, orte_plm_base_framework.framework_output,
                                     "%s plm:base:setup_vm only HNP in use",
                                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));
                OBJ_DESTRUCT(&nodes);
                map->num_nodes = 1;
                /* mark that the daemons have reported so we can proceed */
                daemons->state = ORTE_JOB_STATE_DAEMONS_REPORTED;
                return ORTE_SUCCESS;
            }
            /* well, if the HNP doesn't have any procs, and neither did
             * anyone else...then we have a big problem
             */
            ORTE_FORCED_TERMINATE(ORTE_ERROR_DEFAULT_EXIT_CODE);
            return ORTE_ERR_FATAL;
        }
        goto process;
    }

    if (0 == map->num_nodes) {
        OPAL_OUTPUT_VERBOSE((5, orte_plm_base_framework.framework_output,
                             "%s plm:base:setup_vm creating map",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));
        /* this is the first time thru, so the vm is just getting
         * defined - put us in as we
         * are obviously already here! The ess will already
         * have assigned our node to us.
         */
        node = (orte_node_t*)opal_pointer_array_get_item(orte_node_pool, 0);
        opal_pointer_array_add(map->nodes, (void*)node);
        ++(map->num_nodes);
        /* maintain accounting */
        OBJ_RETAIN(node);
    }

    /* zero-out the number of new daemons as we will compute this
     * each time we are called
     */
    map->num_new_daemons = 0;

    /* setup the list of nodes */
    OBJ_CONSTRUCT(&nodes, opal_list_t);

    /* if this is an unmanaged allocation, then we use
     * the nodes that were specified for the union of
     * all apps - there is no need to collect all
     * available nodes and "filter" them
     */
    if (!orte_managed_allocation) {
        OPAL_OUTPUT_VERBOSE((5, orte_plm_base_framework.framework_output,
                             "%s setup:vm: working unmanaged allocation",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));
        default_hostfile_used = false;
        OBJ_CONSTRUCT(&tnodes, opal_list_t);
        for (i=0; i < jdata->apps->size; i++) {
            if (NULL == (app = (orte_app_context_t*)opal_pointer_array_get_item(jdata->apps, i))) {
                continue;
            }
            /* if the app provided a dash-host, and we are not treating
             * them as requested or "soft" locations, then use those nodes
             */
            hosts = NULL;
            if (!orte_soft_locations &&
                orte_get_attribute(&app->attributes, ORTE_APP_DASH_HOST, (void**)&hosts, OPAL_STRING)) {
                OPAL_OUTPUT_VERBOSE((5, orte_plm_base_framework.framework_output,
                                     "%s using dash_host",
                                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));
                if (ORTE_SUCCESS != (rc = orte_util_add_dash_host_nodes(&tnodes, hosts, false))) {
                    ORTE_ERROR_LOG(rc);
                    free(hosts);
                    return rc;
                }
                free(hosts);
            } else if (orte_get_attribute(&app->attributes, ORTE_APP_HOSTFILE, (void**)&hosts, OPAL_STRING)) {
                /* otherwise, if the app provided a hostfile, then use that */
                OPAL_OUTPUT_VERBOSE((5, orte_plm_base_framework.framework_output,
                                     "%s using hostfile %s",
                                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), hosts));
                if (ORTE_SUCCESS != (rc = orte_util_add_hostfile_nodes(&tnodes, hosts))) {
                    ORTE_ERROR_LOG(rc);
                    free(hosts);
                    return rc;
                }
                free(hosts);
            } else if (NULL != orte_rankfile) {
                /* use the rankfile, if provided */
                OPAL_OUTPUT_VERBOSE((5, orte_plm_base_framework.framework_output,
                                     "%s using rankfile %s",
                                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                     orte_rankfile));
                if (ORTE_SUCCESS != (rc = orte_util_add_hostfile_nodes(&tnodes,
                                                                       orte_rankfile))) {
                    ORTE_ERROR_LOG(rc);
                    return rc;
                }
            } else if (NULL != orte_default_hostfile) {
                if (!default_hostfile_used) {
                    /* fall back to the default hostfile, if provided */
                    OPAL_OUTPUT_VERBOSE((5, orte_plm_base_framework.framework_output,
                                         "%s using default hostfile %s",
                                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                         orte_default_hostfile));
                    if (ORTE_SUCCESS != (rc = orte_util_add_hostfile_nodes(&tnodes,
                                                                           orte_default_hostfile))) {
                        ORTE_ERROR_LOG(rc);
                        return rc;
                    }
                    /* only include it once */
                    default_hostfile_used = true;
                }
            }
        }
        /* cycle thru the resulting list, finding the nodes on
         * the node pool array while removing ourselves
         * and all nodes that are down or otherwise unusable
         */
        while (NULL != (item = opal_list_remove_first(&tnodes))) {
            nptr = (orte_node_t*)item;
            OPAL_OUTPUT_VERBOSE((5, orte_plm_base_framework.framework_output,
                                 "%s checking node %s",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 nptr->name));
            for (i=0; i < orte_node_pool->size; i++) {
                if (NULL == (node = (orte_node_t*)opal_pointer_array_get_item(orte_node_pool, i))) {
                    continue;
                }
                if (0 != strcmp(node->name, nptr->name)) {
                    continue;
                }
                /* have a match - now see if we want this node */
                /* ignore nodes that are marked as do-not-use for this mapping */
                if (ORTE_NODE_STATE_DO_NOT_USE == node->state) {
                    OPAL_OUTPUT_VERBOSE((10, orte_plm_base_framework.framework_output,
                                         "NODE %s IS MARKED NO_USE", node->name));
                    /* reset the state so it can be used another time */
                    node->state = ORTE_NODE_STATE_UP;
                    break;
                }
                if (ORTE_NODE_STATE_DOWN == node->state) {
                    OPAL_OUTPUT_VERBOSE((10, orte_plm_base_framework.framework_output,
                                         "NODE %s IS MARKED DOWN", node->name));
                    break;
                }
                if (ORTE_NODE_STATE_NOT_INCLUDED == node->state) {
                    OPAL_OUTPUT_VERBOSE((10, orte_plm_base_framework.framework_output,
                                         "NODE %s IS MARKED NO_INCLUDE", node->name));
                    break;
                }
                /* if this node is us, ignore it */
                if (0 == node->index) {
                    OPAL_OUTPUT_VERBOSE((5, orte_plm_base_framework.framework_output,
                                         "%s ignoring myself",
                                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));
                    break;
                }
                /* we want it - add it to list */
                OBJ_RETAIN(node);
                opal_list_append(&nodes, &node->super);
            }
            OBJ_RELEASE(nptr);
        }
        OPAL_LIST_DESTRUCT(&tnodes);
        /* if we didn't get anything, then we are the only node in the
         * allocation - so there is nothing else to do as no other
         * daemons are to be launched
         */
        if (0 == opal_list_get_size(&nodes)) {
            OPAL_OUTPUT_VERBOSE((5, orte_plm_base_framework.framework_output,
                                 "%s plm:base:setup_vm only HNP in allocation",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));
            OBJ_DESTRUCT(&nodes);
            /* mark that the daemons have reported so we can proceed */
            daemons->state = ORTE_JOB_STATE_DAEMONS_REPORTED;
            ORTE_FLAG_UNSET(daemons, ORTE_JOB_FLAG_UPDATED);
            return ORTE_SUCCESS;
        }
        /* continue processing */
        goto process;
    }

    /* construct a list of available nodes */
    for (i=1; i < orte_node_pool->size; i++) {
        if (NULL != (node = (orte_node_t*)opal_pointer_array_get_item(orte_node_pool, i))) {
            /* ignore nodes that are marked as do-not-use for this mapping */
            if (ORTE_NODE_STATE_DO_NOT_USE == node->state) {
                OPAL_OUTPUT_VERBOSE((10, orte_plm_base_framework.framework_output,
                                     "NODE %s IS MARKED NO_USE", node->name));
                /* reset the state so it can be used another time */
                node->state = ORTE_NODE_STATE_UP;
                continue;
            }
            if (ORTE_NODE_STATE_DOWN == node->state) {
                OPAL_OUTPUT_VERBOSE((10, orte_plm_base_framework.framework_output,
                                     "NODE %s IS MARKED DOWN", node->name));
                continue;
            }
            if (ORTE_NODE_STATE_NOT_INCLUDED == node->state) {
                OPAL_OUTPUT_VERBOSE((10, orte_plm_base_framework.framework_output,
                                     "NODE %s IS MARKED NO_INCLUDE", node->name));
                /* not to be used */
                continue;
            }
            /* retain a copy for our use in case the item gets
             * destructed along the way
             */
            OBJ_RETAIN(node);
            opal_list_append(&nodes, &node->super);
            /* by default, mark these as not to be included
             * so the filtering logic works correctly
             */
            ORTE_FLAG_UNSET(node, ORTE_NODE_FLAG_MAPPED);
        }
    }

    /* if we didn't get anything, then we are the only node in the
     * system - so there is nothing else to do as no other
     * daemons are to be launched
     */
    if (0 == opal_list_get_size(&nodes)) {
        OPAL_OUTPUT_VERBOSE((5, orte_plm_base_framework.framework_output,
                             "%s plm:base:setup_vm only HNP in allocation",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));
        /* cleanup */
        OBJ_DESTRUCT(&nodes);
        /* mark that the daemons have reported so we can proceed */
        daemons->state = ORTE_JOB_STATE_DAEMONS_REPORTED;
        ORTE_FLAG_UNSET(daemons, ORTE_JOB_FLAG_UPDATED);
        return ORTE_SUCCESS;
    }

    /* filter across the union of all app_context specs - if the HNP
     * was allocated, then we have to include
     * ourselves in case someone has specified a -host or hostfile
     * that includes the head node. We will remove ourselves later
     * as we clearly already exist
     */
    if (orte_hnp_is_allocated) {
        node = (orte_node_t*)opal_pointer_array_get_item(orte_node_pool, 0);
        OBJ_RETAIN(node);
        opal_list_prepend(&nodes, &node->super);
    }
    for (i=0; i < jdata->apps->size; i++) {
        if (NULL == (app = (orte_app_context_t*)opal_pointer_array_get_item(jdata->apps, i))) {
            continue;
        }
        if (ORTE_SUCCESS != (rc = orte_rmaps_base_filter_nodes(app, &nodes, false)) &&
            rc != ORTE_ERR_TAKE_NEXT_OPTION) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }
        if (ORTE_SUCCESS == rc) {
            /* we filtered something */
            one_filter = true;
        }
    }

    if (one_filter) {
        /* at least one filtering option was executed, so
         * remove all nodes that were not mapped
         */
        item = opal_list_get_first(&nodes);
        while (item != opal_list_get_end(&nodes)) {
            next = opal_list_get_next(item);
            node = (orte_node_t*)item;
            if (!ORTE_FLAG_TEST(node, ORTE_NODE_FLAG_MAPPED)) {
                opal_list_remove_item(&nodes, item);
                OBJ_RELEASE(item);
            } else {
                /* The filtering logic sets this flag only for nodes which 
                 * are kept after filtering. This flag will be subsequently
                 * used in rmaps components and must be reset here */
                ORTE_FLAG_UNSET(node, ORTE_NODE_FLAG_MAPPED);
            }
            item = next;
        }
    }

    /* ensure we are not on the list */
    if (0 < opal_list_get_size(&nodes)) {
        item = opal_list_get_first(&nodes);
        node = (orte_node_t*)item;
        if (0 == node->index) {
            opal_list_remove_item(&nodes, item);
            OBJ_RELEASE(item);
        }
    }

    /* if we didn't get anything, then we are the only node in the
     * allocation - so there is nothing else to do as no other
     * daemons are to be launched
     */
    if (0 == opal_list_get_size(&nodes)) {
        OPAL_OUTPUT_VERBOSE((5, orte_plm_base_framework.framework_output,
                             "%s plm:base:setup_vm only HNP left",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));
        OBJ_DESTRUCT(&nodes);
        /* mark that the daemons have reported so we can proceed */
        daemons->state = ORTE_JOB_STATE_DAEMONS_REPORTED;
        ORTE_FLAG_UNSET(daemons, ORTE_JOB_FLAG_UPDATED);
        return ORTE_SUCCESS;
    }

 process:
    /* cycle thru all available nodes and find those that do not already
     * have a daemon on them - no need to include our own as we are
     * obviously already here! If a max vm size was given, then limit
     * the overall number of active nodes to the given number. Only
     * count the HNP's node if it was included in the allocation
     */
    if (orte_hnp_is_allocated) {
        num_nodes = 1;
    } else {
        num_nodes = 0;
    }
    while (NULL != (item = opal_list_remove_first(&nodes))) {
        /* if a max size was given and we are there, then exit the loop */
        if (0 < orte_max_vm_size && num_nodes == orte_max_vm_size) {
            /* maintain accounting */
            OBJ_RELEASE(item);
            break;
        }
        node = (orte_node_t*)item;
        /* if this node is already in the map, skip it */
        if (NULL != node->daemon) {
            num_nodes++;
            /* maintain accounting */
            OBJ_RELEASE(item);
            continue;
        }
        /* add the node to the map - we retained it
         * when adding it to the list, so we don't need
         * to retain it again
         */
        opal_pointer_array_add(map->nodes, (void*)node);
        ++(map->num_nodes);
        num_nodes++;
        /* create a new daemon object for this node */
        proc = OBJ_NEW(orte_proc_t);
        if (NULL == proc) {
            ORTE_ERROR_LOG(ORTE_ERR_OUT_OF_RESOURCE);
            return ORTE_ERR_OUT_OF_RESOURCE;
        }
        proc->name.jobid = ORTE_PROC_MY_NAME->jobid;
        if (ORTE_VPID_MAX-1 <= daemons->num_procs) {
            /* no more daemons available */
            orte_show_help("help-orte-rmaps-base.txt", "out-of-vpids", true);
            OBJ_RELEASE(proc);
            return ORTE_ERR_OUT_OF_RESOURCE;
        }
        proc->name.vpid = daemons->num_procs;  /* take the next available vpid */
        OPAL_OUTPUT_VERBOSE((5, orte_plm_base_framework.framework_output,
                             "%s plm:base:setup_vm add new daemon %s",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             ORTE_NAME_PRINT(&proc->name)));
        /* add the daemon to the daemon job object */
        if (0 > (rc = opal_pointer_array_set_item(daemons->procs, proc->name.vpid, (void*)proc))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }
        ++daemons->num_procs;
        OPAL_OUTPUT_VERBOSE((5, orte_plm_base_framework.framework_output,
                             "%s plm:base:setup_vm assigning new daemon %s to node %s",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             ORTE_NAME_PRINT(&proc->name),
                             node->name));
        /* point the node to the daemon */
        node->daemon = proc;
        OBJ_RETAIN(proc);  /* maintain accounting */
        /* point the proc to the node and maintain accounting */
        proc->node = node;
        OBJ_RETAIN(node);
        if (orte_plm_globals.daemon_nodes_assigned_at_launch) {
            ORTE_FLAG_SET(node, ORTE_NODE_FLAG_LOC_VERIFIED);
        } else {
            ORTE_FLAG_UNSET(node, ORTE_NODE_FLAG_LOC_VERIFIED);
        }
        /* track number of daemons to be launched */
        ++map->num_new_daemons;
        /* and their starting vpid */
        if (ORTE_VPID_INVALID == map->daemon_vpid_start) {
            map->daemon_vpid_start = proc->name.vpid;
        }
        /* loop across all app procs on this node and update their parent */
        for (i=0; i < node->procs->size; i++) {
            if (NULL != (pptr = (orte_proc_t*)opal_pointer_array_get_item(node->procs, i))) {
                pptr->parent = proc->name.vpid;
            }
        }
    }

    if (orte_process_info.num_procs != daemons->num_procs) {
        /* more daemons are being launched - update the routing tree to
         * ensure that the HNP knows how to route messages via
         * the daemon routing tree - this needs to be done
         * here to avoid potential race conditions where the HNP
         * hasn't unpacked its launch message prior to being
         * asked to communicate.
         */
        orte_process_info.num_procs = daemons->num_procs;

        if (orte_process_info.max_procs < orte_process_info.num_procs) {
            orte_process_info.max_procs = orte_process_info.num_procs;
        }

        /* ensure all routing plans are up-to-date - we need this
         * so we know how to tree-spawn and/or xcast info */
        orte_routed.update_routing_plan(NULL);
    }

    /* mark that the daemon job changed */
    ORTE_FLAG_SET(daemons, ORTE_JOB_FLAG_UPDATED);

    /* if new daemons are being launched, mark that this job
     * caused it to happen */
    if (0 < map->num_new_daemons) {
        if (ORTE_SUCCESS != (rc = orte_set_attribute(&jdata->attributes, ORTE_JOB_LAUNCHED_DAEMONS,
                                                     true, NULL, OPAL_BOOL))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }
    }

    return ORTE_SUCCESS;
}
