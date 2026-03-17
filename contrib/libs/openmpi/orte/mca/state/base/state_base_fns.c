/*
 * Copyright (c) 2011-2012 Los Alamos National Security, LLC.
 * Copyright (c) 2014-2018 Intel, Inc. All rights reserved.
 * Copyright (c) 2018      Research Organization for Information Science
 *                         and Technology (RIST).  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/** @file **/

#include "orte_config.h"
#include "orte/constants.h"

#if HAVE_UNISTD_H
#include <unistd.h>
#endif
#if HAVE_FCNTL_H
#include <fcntl.h>
#endif

#include "opal/class/opal_list.h"
#include "opal/mca/event/event.h"
#include "opal/mca/pmix/pmix.h"
#include "opal/util/argv.h"

#include "orte/orted/pmix/pmix_server_internal.h"
#include "orte/runtime/orte_data_server.h"
#include "orte/runtime/orte_globals.h"
#include "orte/runtime/orte_wait.h"
#include "orte/mca/errmgr/errmgr.h"
#include "orte/mca/grpcomm/grpcomm.h"
#include "orte/mca/iof/base/base.h"
#include "orte/mca/rmaps/rmaps_types.h"
#include "orte/mca/plm/plm.h"
#include "orte/mca/rml/rml.h"
#include "orte/mca/routed/routed.h"
#include "orte/util/session_dir.h"
#include "orte/util/threads.h"
#include "orte/util/show_help.h"

#include "orte/mca/state/base/base.h"
#include "orte/mca/state/base/state_private.h"

void orte_state_base_activate_job_state(orte_job_t *jdata,
                                        orte_job_state_t state)
{
    opal_list_item_t *itm, *any=NULL, *error=NULL;
    orte_state_t *s;
    orte_state_caddy_t *caddy;

    for (itm = opal_list_get_first(&orte_job_states);
         itm != opal_list_get_end(&orte_job_states);
         itm = opal_list_get_next(itm)) {
        s = (orte_state_t*)itm;
        if (s->job_state == ORTE_JOB_STATE_ANY) {
            /* save this place */
            any = itm;
        }
        if (s->job_state == ORTE_JOB_STATE_ERROR) {
            error = itm;
        }
        if (s->job_state == state) {
            OPAL_OUTPUT_VERBOSE((1, orte_state_base_framework.framework_output,
                                 "%s ACTIVATING JOB %s STATE %s PRI %d",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 (NULL == jdata) ? "NULL" : ORTE_JOBID_PRINT(jdata->jobid),
                                 orte_job_state_to_str(state), s->priority));
            if (NULL == s->cbfunc) {
                OPAL_OUTPUT_VERBOSE((1, orte_state_base_framework.framework_output,
                                     "%s NULL CBFUNC FOR JOB %s STATE %s",
                                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                     (NULL == jdata) ? "ALL" : ORTE_JOBID_PRINT(jdata->jobid),
                                     orte_job_state_to_str(state)));
                return;
            }
            caddy = OBJ_NEW(orte_state_caddy_t);
            if (NULL != jdata) {
                caddy->jdata = jdata;
                caddy->job_state = state;
                OBJ_RETAIN(jdata);
            }
            ORTE_THREADSHIFT(caddy, orte_event_base, s->cbfunc, s->priority);
            return;
        }
    }
    /* if we get here, then the state wasn't found, so execute
     * the default handler if it is defined
     */
    if (ORTE_JOB_STATE_ERROR < state && NULL != error) {
        s = (orte_state_t*)error;
    } else if (NULL != any) {
        s = (orte_state_t*)any;
    } else {
        OPAL_OUTPUT_VERBOSE((1, orte_state_base_framework.framework_output,
                             "ACTIVATE: ANY STATE NOT FOUND"));
        return;
    }
    if (NULL == s->cbfunc) {
        OPAL_OUTPUT_VERBOSE((1, orte_state_base_framework.framework_output,
                             "ACTIVATE: ANY STATE HANDLER NOT DEFINED"));
        return;
    }
    caddy = OBJ_NEW(orte_state_caddy_t);
    if (NULL != jdata) {
        caddy->jdata = jdata;
        caddy->job_state = state;
        OBJ_RETAIN(jdata);
    }
    OPAL_OUTPUT_VERBOSE((1, orte_state_base_framework.framework_output,
                         "%s ACTIVATING JOB %s STATE %s PRI %d",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         (NULL == jdata) ? "NULL" : ORTE_JOBID_PRINT(jdata->jobid),
                         orte_job_state_to_str(state), s->priority));
    ORTE_THREADSHIFT(caddy, orte_event_base, s->cbfunc, s->priority);
}


int orte_state_base_add_job_state(orte_job_state_t state,
                                  orte_state_cbfunc_t cbfunc,
                                  int priority)
{
    opal_list_item_t *item;
    orte_state_t *st;

    /* check for uniqueness */
    for (item = opal_list_get_first(&orte_job_states);
         item != opal_list_get_end(&orte_job_states);
         item = opal_list_get_next(item)) {
        st = (orte_state_t*)item;
        if (st->job_state == state) {
            OPAL_OUTPUT_VERBOSE((1, orte_state_base_framework.framework_output,
                                 "DUPLICATE STATE DEFINED: %s",
                                 orte_job_state_to_str(state)));
            return ORTE_ERR_BAD_PARAM;
        }
    }

    st = OBJ_NEW(orte_state_t);
    st->job_state = state;
    st->cbfunc = cbfunc;
    st->priority = priority;
    opal_list_append(&orte_job_states, &(st->super));

    return ORTE_SUCCESS;
}

int orte_state_base_set_job_state_callback(orte_job_state_t state,
                                           orte_state_cbfunc_t cbfunc)
{
    opal_list_item_t *item;
    orte_state_t *st;

    for (item = opal_list_get_first(&orte_job_states);
         item != opal_list_get_end(&orte_job_states);
         item = opal_list_get_next(item)) {
        st = (orte_state_t*)item;
        if (st->job_state == state) {
            st->cbfunc = cbfunc;
            return ORTE_SUCCESS;
        }
    }

    /* if not found, assume SYS priority and install it */
    st = OBJ_NEW(orte_state_t);
    st->job_state = state;
    st->cbfunc = cbfunc;
    st->priority = ORTE_SYS_PRI;
    opal_list_append(&orte_job_states, &(st->super));

    return ORTE_SUCCESS;
}

int orte_state_base_set_job_state_priority(orte_job_state_t state,
                                           int priority)
{
    opal_list_item_t *item;
    orte_state_t *st;

    for (item = opal_list_get_first(&orte_job_states);
         item != opal_list_get_end(&orte_job_states);
         item = opal_list_get_next(item)) {
        st = (orte_state_t*)item;
        if (st->job_state == state) {
            st->priority = priority;
            return ORTE_SUCCESS;
        }
    }
    return ORTE_ERR_NOT_FOUND;
}

int orte_state_base_remove_job_state(orte_job_state_t state)
{
    opal_list_item_t *item;
    orte_state_t *st;

    for (item = opal_list_get_first(&orte_job_states);
         item != opal_list_get_end(&orte_job_states);
         item = opal_list_get_next(item)) {
        st = (orte_state_t*)item;
        if (st->job_state == state) {
            opal_list_remove_item(&orte_job_states, item);
            OBJ_RELEASE(item);
            return ORTE_SUCCESS;
        }
    }
    return ORTE_ERR_NOT_FOUND;
}

void orte_state_base_print_job_state_machine(void)
{
    opal_list_item_t *item;
    orte_state_t *st;

    opal_output(0, "ORTE_JOB_STATE_MACHINE:");
    for (item = opal_list_get_first(&orte_job_states);
         item != opal_list_get_end(&orte_job_states);
         item = opal_list_get_next(item)) {
        st = (orte_state_t*)item;
        opal_output(0, "\tState: %s cbfunc: %s",
                    orte_job_state_to_str(st->job_state),
                    (NULL == st->cbfunc) ? "NULL" : "DEFINED");
    }
}


/****    PROC STATE MACHINE    ****/
void orte_state_base_activate_proc_state(orte_process_name_t *proc,
                                         orte_proc_state_t state)
{
    opal_list_item_t *itm, *any=NULL, *error=NULL;
    orte_state_t *s;
    orte_state_caddy_t *caddy;

    for (itm = opal_list_get_first(&orte_proc_states);
         itm != opal_list_get_end(&orte_proc_states);
         itm = opal_list_get_next(itm)) {
        s = (orte_state_t*)itm;
        if (s->proc_state == ORTE_PROC_STATE_ANY) {
            /* save this place */
            any = itm;
        }
        if (s->proc_state == ORTE_PROC_STATE_ERROR) {
            error = itm;
        }
        if (s->proc_state == state) {
            OPAL_OUTPUT_VERBOSE((1, orte_state_base_framework.framework_output,
                                 "%s ACTIVATING PROC %s STATE %s PRI %d",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 ORTE_NAME_PRINT(proc),
                                 orte_proc_state_to_str(state), s->priority));
            if (NULL == s->cbfunc) {
                OPAL_OUTPUT_VERBOSE((1, orte_state_base_framework.framework_output,
                                     "%s NULL CBFUNC FOR PROC %s STATE %s",
                                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                     ORTE_NAME_PRINT(proc),
                                     orte_proc_state_to_str(state)));
                return;
            }
            caddy = OBJ_NEW(orte_state_caddy_t);
            caddy->name = *proc;
            caddy->proc_state = state;
            ORTE_THREADSHIFT(caddy, orte_event_base, s->cbfunc, s->priority);
            return;
        }
    }
    /* if we get here, then the state wasn't found, so execute
     * the default handler if it is defined
     */
    if (ORTE_PROC_STATE_ERROR < state && NULL != error) {
        s = (orte_state_t*)error;
    } else if (NULL != any) {
        s = (orte_state_t*)any;
    } else {
        OPAL_OUTPUT_VERBOSE((1, orte_state_base_framework.framework_output,
                             "INCREMENT: ANY STATE NOT FOUND"));
        return;
    }
    if (NULL == s->cbfunc) {
        OPAL_OUTPUT_VERBOSE((1, orte_state_base_framework.framework_output,
                             "ACTIVATE: ANY STATE HANDLER NOT DEFINED"));
        return;
    }
    caddy = OBJ_NEW(orte_state_caddy_t);
    caddy->name = *proc;
    caddy->proc_state = state;
    OPAL_OUTPUT_VERBOSE((1, orte_state_base_framework.framework_output,
                         "%s ACTIVATING PROC %s STATE %s PRI %d",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         ORTE_NAME_PRINT(proc),
                         orte_proc_state_to_str(state), s->priority));
     ORTE_THREADSHIFT(caddy, orte_event_base, s->cbfunc, s->priority);
}

int orte_state_base_add_proc_state(orte_proc_state_t state,
                                   orte_state_cbfunc_t cbfunc,
                                   int priority)
{
    opal_list_item_t *item;
    orte_state_t *st;

    /* check for uniqueness */
    for (item = opal_list_get_first(&orte_proc_states);
         item != opal_list_get_end(&orte_proc_states);
         item = opal_list_get_next(item)) {
        st = (orte_state_t*)item;
        if (st->proc_state == state) {
            OPAL_OUTPUT_VERBOSE((1, orte_state_base_framework.framework_output,
                                 "DUPLICATE STATE DEFINED: %s",
                                 orte_proc_state_to_str(state)));
            return ORTE_ERR_BAD_PARAM;
        }
    }

    st = OBJ_NEW(orte_state_t);
    st->proc_state = state;
    st->cbfunc = cbfunc;
    st->priority = priority;
    opal_list_append(&orte_proc_states, &(st->super));

    return ORTE_SUCCESS;
}

int orte_state_base_set_proc_state_callback(orte_proc_state_t state,
                                            orte_state_cbfunc_t cbfunc)
{
    opal_list_item_t *item;
    orte_state_t *st;

    for (item = opal_list_get_first(&orte_proc_states);
         item != opal_list_get_end(&orte_proc_states);
         item = opal_list_get_next(item)) {
        st = (orte_state_t*)item;
        if (st->proc_state == state) {
            st->cbfunc = cbfunc;
            return ORTE_SUCCESS;
        }
    }
    return ORTE_ERR_NOT_FOUND;
}

int orte_state_base_set_proc_state_priority(orte_proc_state_t state,
                                            int priority)
{
    opal_list_item_t *item;
    orte_state_t *st;

    for (item = opal_list_get_first(&orte_proc_states);
         item != opal_list_get_end(&orte_proc_states);
         item = opal_list_get_next(item)) {
        st = (orte_state_t*)item;
        if (st->proc_state == state) {
            st->priority = priority;
            return ORTE_SUCCESS;
        }
    }
    return ORTE_ERR_NOT_FOUND;
}

int orte_state_base_remove_proc_state(orte_proc_state_t state)
{
    opal_list_item_t *item;
    orte_state_t *st;

    for (item = opal_list_get_first(&orte_proc_states);
         item != opal_list_get_end(&orte_proc_states);
         item = opal_list_get_next(item)) {
        st = (orte_state_t*)item;
        if (st->proc_state == state) {
            opal_list_remove_item(&orte_proc_states, item);
            OBJ_RELEASE(item);
            return ORTE_SUCCESS;
        }
    }
    return ORTE_ERR_NOT_FOUND;
}

void orte_state_base_print_proc_state_machine(void)
{
    opal_list_item_t *item;
    orte_state_t *st;

    opal_output(0, "ORTE_PROC_STATE_MACHINE:");
    for (item = opal_list_get_first(&orte_proc_states);
         item != opal_list_get_end(&orte_proc_states);
         item = opal_list_get_next(item)) {
        st = (orte_state_t*)item;
        opal_output(0, "\tState: %s cbfunc: %s",
                    orte_proc_state_to_str(st->proc_state),
                    (NULL == st->cbfunc) ? "NULL" : "DEFINED");
    }
}

static void cleanup_node(orte_proc_t *proc)
{
    orte_node_t *node;
    orte_proc_t *p;
    int i;

    OPAL_OUTPUT_VERBOSE((2, orte_state_base_framework.framework_output,
                         "%s state:base:cleanup_node on proc %s",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         ORTE_NAME_PRINT(&proc->name)));

    if (NULL == (node = proc->node)) {
        return;
    }
    if (!ORTE_FLAG_TEST(proc, ORTE_PROC_FLAG_TOOL)) {
        node->num_procs--;
        node->slots_inuse--;
    }
    for (i=0; i < node->procs->size; i++) {
        if (NULL == (p = (orte_proc_t*)opal_pointer_array_get_item(node->procs, i))) {
            continue;
        }
        if (p->name.jobid == proc->name.jobid &&
            p->name.vpid == proc->name.vpid) {
            opal_pointer_array_set_item(node->procs, i, NULL);
            OBJ_RELEASE(p);
            break;
        }
    }
}

void orte_state_base_local_launch_complete(int fd, short argc, void *cbdata)
{
    orte_state_caddy_t *state = (orte_state_caddy_t*)cbdata;
    orte_job_t *jdata = state->jdata;

    if (orte_report_launch_progress) {
        if (0 == jdata->num_daemons_reported % 100 ||
            jdata->num_daemons_reported == orte_process_info.num_procs) {
            ORTE_ACTIVATE_JOB_STATE(jdata, ORTE_JOB_STATE_REPORT_PROGRESS);
        }
    }
    OBJ_RELEASE(state);
}

void orte_state_base_cleanup_job(int fd, short argc, void *cbdata)
{
    orte_state_caddy_t *caddy = (orte_state_caddy_t*)cbdata;
    orte_job_t *jdata;

    ORTE_ACQUIRE_OBJECT(caddy);
    jdata = caddy->jdata;

    OPAL_OUTPUT_VERBOSE((2, orte_state_base_framework.framework_output,
                         "%s state:base:cleanup on job %s",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         (NULL == jdata) ? "NULL" : ORTE_JOBID_PRINT(jdata->jobid)));

    /* flag that we were notified */
    jdata->state = ORTE_JOB_STATE_NOTIFIED;
    /* send us back thru job complete */
    ORTE_ACTIVATE_JOB_STATE(jdata, ORTE_JOB_STATE_TERMINATED);
    OBJ_RELEASE(caddy);
}

void orte_state_base_report_progress(int fd, short argc, void *cbdata)
{
    orte_state_caddy_t *caddy = (orte_state_caddy_t*)cbdata;
    orte_job_t *jdata;

     ORTE_ACQUIRE_OBJECT(caddy);
    jdata = caddy->jdata;

   opal_output(orte_clean_output, "App launch reported: %d (out of %d) daemons - %d (out of %d) procs",
                (int)jdata->num_daemons_reported, (int)orte_process_info.num_procs,
                (int)jdata->num_launched, (int)jdata->num_procs);
    OBJ_RELEASE(caddy);
}

void orte_state_base_notify_data_server(orte_process_name_t *target)
{
    opal_buffer_t *buf;
    int rc, room = -1;
    uint8_t cmd = ORTE_PMIX_PURGE_PROC_CMD;

    /* if nobody local to us published anything, then we can ignore this */
    if (ORTE_JOBID_INVALID == orte_pmix_server_globals.server.jobid) {
        return;
    }

    buf = OBJ_NEW(opal_buffer_t);

    /* pack the room number */
    if (OPAL_SUCCESS != (rc = opal_dss.pack(buf, &room, 1, OPAL_INT))) {
        ORTE_ERROR_LOG(rc);
        OBJ_RELEASE(buf);
        return;
    }

    /* load the command */
    if (OPAL_SUCCESS != (rc = opal_dss.pack(buf, &cmd, 1, OPAL_UINT8))) {
        ORTE_ERROR_LOG(rc);
        OBJ_RELEASE(buf);
        return;
    }

    /* provide the target */
    if (ORTE_SUCCESS != (rc = opal_dss.pack(buf, target, 1, ORTE_NAME))) {
        ORTE_ERROR_LOG(rc);
        OBJ_RELEASE(buf);
        return;
    }

    /* send the request to the server */
    rc = orte_rml.send_buffer_nb(orte_mgmt_conduit,
                                 &orte_pmix_server_globals.server, buf,
                                 ORTE_RML_TAG_DATA_SERVER,
                                 orte_rml_send_callback, NULL);
    if (ORTE_SUCCESS != rc) {
        OBJ_RELEASE(buf);
    }
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
                        "%s state:base:sending notification %s proc %s target %s",
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

void orte_state_base_track_procs(int fd, short argc, void *cbdata)
{
    orte_state_caddy_t *caddy = (orte_state_caddy_t*)cbdata;
    orte_process_name_t *proc;
    orte_proc_state_t state;
    orte_job_t *jdata;
    orte_proc_t *pdata;
    int i;
    char *rtmod;
    orte_process_name_t parent, target;

    ORTE_ACQUIRE_OBJECT(caddy);
    proc = &caddy->name;
    state = caddy->proc_state;

    opal_output_verbose(5, orte_state_base_framework.framework_output,
                        "%s state:base:track_procs called for proc %s state %s",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                        ORTE_NAME_PRINT(proc),
                        orte_proc_state_to_str(state));

    /* get our "lifeline" routed module */
    rtmod = orte_rml.get_routed(orte_mgmt_conduit);

    /* get the job object for this proc */
    if (NULL == (jdata = orte_get_job_data_object(proc->jobid))) {
        ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
        goto cleanup;
    }
    pdata = (orte_proc_t*)opal_pointer_array_get_item(jdata->procs, proc->vpid);

    if (ORTE_PROC_STATE_RUNNING == state) {
        /* update the proc state */
        if (pdata->state < ORTE_PROC_STATE_TERMINATED) {
            pdata->state = state;
        }
        jdata->num_launched++;
        if (jdata->num_launched == jdata->num_procs) {
            if (ORTE_FLAG_TEST(jdata, ORTE_JOB_FLAG_DEBUGGER_DAEMON)) {
                ORTE_ACTIVATE_JOB_STATE(jdata, ORTE_JOB_STATE_READY_FOR_DEBUGGERS);
            } else {
                ORTE_ACTIVATE_JOB_STATE(jdata, ORTE_JOB_STATE_RUNNING);
            }
        }
    } else if (ORTE_PROC_STATE_REGISTERED == state) {
        /* update the proc state */
        if (pdata->state < ORTE_PROC_STATE_TERMINATED) {
            pdata->state = state;
        }
        jdata->num_reported++;
        if (jdata->num_reported == jdata->num_procs) {
            ORTE_ACTIVATE_JOB_STATE(jdata, ORTE_JOB_STATE_REGISTERED);
        }
    } else if (ORTE_PROC_STATE_IOF_COMPLETE == state) {
        /* update the proc state */
        if (pdata->state < ORTE_PROC_STATE_TERMINATED) {
            pdata->state = state;
        }
        /* Release the IOF file descriptors */
        if (NULL != orte_iof.close) {
            orte_iof.close(proc, ORTE_IOF_STDALL);
        }
        ORTE_FLAG_SET(pdata, ORTE_PROC_FLAG_IOF_COMPLETE);
        if (ORTE_FLAG_TEST(pdata, ORTE_PROC_FLAG_WAITPID)) {
            ORTE_ACTIVATE_PROC_STATE(proc, ORTE_PROC_STATE_TERMINATED);
        }
    } else if (ORTE_PROC_STATE_WAITPID_FIRED == state) {
        /* update the proc state */
        if (pdata->state < ORTE_PROC_STATE_TERMINATED) {
            pdata->state = state;
        }
        ORTE_FLAG_SET(pdata, ORTE_PROC_FLAG_WAITPID);
        if (ORTE_FLAG_TEST(pdata, ORTE_PROC_FLAG_IOF_COMPLETE)) {
            ORTE_ACTIVATE_PROC_STATE(proc, ORTE_PROC_STATE_TERMINATED);
        }
    } else if (ORTE_PROC_STATE_TERMINATED == state) {
        /* update the proc state */
        ORTE_FLAG_UNSET(pdata, ORTE_PROC_FLAG_ALIVE);
        if (pdata->state < ORTE_PROC_STATE_TERMINATED) {
            pdata->state = state;
        }
        if (ORTE_FLAG_TEST(pdata, ORTE_PROC_FLAG_LOCAL)) {
            /* tell the PMIx subsystem to cleanup this client */
            opal_pmix.server_deregister_client(proc, NULL, NULL);
            /* Clean up the session directory as if we were the process
             * itself.  This covers the case where the process died abnormally
             * and didn't cleanup its own session directory.
             */
            orte_session_dir_finalize(proc);
        }
        /* if we are trying to terminate and our routes are
         * gone, then terminate ourselves IF no local procs
         * remain (might be some from another job)
         */
        if (orte_orteds_term_ordered &&
            0 == orte_routed.num_routes(rtmod)) {
            for (i=0; i < orte_local_children->size; i++) {
                if (NULL != (pdata = (orte_proc_t*)opal_pointer_array_get_item(orte_local_children, i)) &&
                    ORTE_FLAG_TEST(pdata, ORTE_PROC_FLAG_ALIVE)) {
                    /* at least one is still alive */
                    goto cleanup;
                }
            }
            /* call our appropriate exit procedure */
            OPAL_OUTPUT_VERBOSE((5, orte_state_base_framework.framework_output,
                                 "%s state:base all routes and children gone - exiting",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));
            ORTE_ACTIVATE_JOB_STATE(NULL, ORTE_JOB_STATE_DAEMONS_TERMINATED);
            goto cleanup;
        }
        /* return the allocated slot for reuse */
        cleanup_node(pdata);
        /* track job status */
        jdata->num_terminated++;
        if (jdata->num_terminated == jdata->num_procs) {
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
            ORTE_ACTIVATE_JOB_STATE(jdata, ORTE_JOB_STATE_TERMINATED);
        } else if (ORTE_PROC_STATE_TERMINATED < pdata->state &&
                   !orte_job_term_ordered) {
            /* if this was an abnormal term, notify the other procs of the termination */
            parent.jobid = jdata->jobid;
            parent.vpid = ORTE_VPID_WILDCARD;
            _send_notification(OPAL_ERR_PROC_ABORTED, pdata->state, &pdata->name, &parent);
        }
    }

 cleanup:
    OBJ_RELEASE(caddy);
}

void orte_state_base_check_all_complete(int fd, short args, void *cbdata)
{
    orte_state_caddy_t *caddy = (orte_state_caddy_t*)cbdata;
    orte_job_t *jdata;
    orte_proc_t *proc;
    int i;
    orte_std_cntr_t j;
    orte_job_t *job;
    orte_node_t *node;
    orte_job_map_t *map;
    orte_std_cntr_t index;
    bool one_still_alive;
    orte_vpid_t lowest=0;
    int32_t i32, *i32ptr;
    uint32_t u32;
    void *nptr;
    char *rtmod;

    ORTE_ACQUIRE_OBJECT(caddy);
    jdata = caddy->jdata;

    opal_output_verbose(2, orte_state_base_framework.framework_output,
                        "%s state:base:check_job_complete on job %s",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                        (NULL == jdata) ? "NULL" : ORTE_JOBID_PRINT(jdata->jobid));

    /* get our "lifeline" routed module */
    rtmod = orte_rml.get_routed(orte_mgmt_conduit);


    if (NULL == jdata || jdata->jobid == ORTE_PROC_MY_NAME->jobid) {
        /* just check to see if the daemons are complete */
        OPAL_OUTPUT_VERBOSE((2, orte_state_base_framework.framework_output,
                             "%s state:base:check_job_complete - received NULL job, checking daemons",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));
        goto CHECK_DAEMONS;
    } else {
        /* mark the job as terminated, but don't override any
         * abnormal termination flags
         */
        if (jdata->state < ORTE_JOB_STATE_UNTERMINATED) {
            jdata->state = ORTE_JOB_STATE_TERMINATED;
        }
    }

    /* tell the IOF that the job is complete */
    if (NULL != orte_iof.complete) {
        orte_iof.complete(jdata);
    }

    /* tell the PMIx server to release its data */
    if (NULL != opal_pmix.server_deregister_nspace) {
        opal_pmix.server_deregister_nspace(jdata->jobid, NULL, NULL);
    }

    i32ptr = &i32;
    if (orte_get_attribute(&jdata->attributes, ORTE_JOB_NUM_NONZERO_EXIT, (void**)&i32ptr, OPAL_INT32) && !orte_abort_non_zero_exit) {
        if (!orte_report_child_jobs_separately || 1 == ORTE_LOCAL_JOBID(jdata->jobid)) {
            /* update the exit code */
            ORTE_UPDATE_EXIT_STATUS(lowest);
        }

        /* warn user */
        orte_show_help("help-state-base.txt", "normal-termination-but", true,
                    (1 == ORTE_LOCAL_JOBID(jdata->jobid)) ? "the primary" : "child",
                    (1 == ORTE_LOCAL_JOBID(jdata->jobid)) ? "" : ORTE_LOCAL_JOBID_PRINT(jdata->jobid),
                    i32, (1 == i32) ? "process returned\na non-zero exit code." :
                    "processes returned\nnon-zero exit codes.");
    }

    OPAL_OUTPUT_VERBOSE((2, orte_state_base_framework.framework_output,
                         "%s state:base:check_job_completed declared job %s terminated with state %s - checking all jobs",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         ORTE_JOBID_PRINT(jdata->jobid),
                         orte_job_state_to_str(jdata->state)));

    /* if this job is a continuously operating one, then don't do
     * anything further - just return here
     */
    if (NULL != jdata &&
        (orte_get_attribute(&jdata->attributes, ORTE_JOB_CONTINUOUS_OP, NULL, OPAL_BOOL) ||
         ORTE_FLAG_TEST(jdata, ORTE_JOB_FLAG_RECOVERABLE))) {
        goto CHECK_ALIVE;
    }

    /* if the job that is being checked is the HNP, then we are
     * trying to terminate the orteds. In that situation, we
     * do -not- check all jobs - we simply notify the HNP
     * that the orteds are complete. Also check special case
     * if jdata is NULL - we want
     * to definitely declare the job done if the orteds
     * have completed, no matter what else may be happening.
     * This can happen if a ctrl-c hits in the "wrong" place
     * while launching
     */
 CHECK_DAEMONS:
    if (jdata == NULL || jdata->jobid == ORTE_PROC_MY_NAME->jobid) {
        if (0 == orte_routed.num_routes(rtmod)) {
            /* orteds are done! */
            OPAL_OUTPUT_VERBOSE((2, orte_state_base_framework.framework_output,
                                 "%s orteds complete - exiting",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));
            if (NULL == jdata) {
                jdata = orte_get_job_data_object(ORTE_PROC_MY_NAME->jobid);
            }
            ORTE_ACTIVATE_JOB_STATE(jdata, ORTE_JOB_STATE_DAEMONS_TERMINATED);
            OBJ_RELEASE(caddy);
            return;
        }
        OBJ_RELEASE(caddy);
        return;
    }

    /* Release the resources used by this job. Since some errmgrs may want
     * to continue using resources allocated to the job as part of their
     * fault recovery procedure, we only do this once the job is "complete".
     * Note that an aborted/killed job -is- flagged as complete and will
     * therefore have its resources released. We need to do this after
     * we call the errmgr so that any attempt to restart the job will
     * avoid doing so in the exact same place as the current job
     */
    if (NULL != jdata->map && jdata->state == ORTE_JOB_STATE_TERMINATED) {
        map = jdata->map;
        for (index = 0; index < map->nodes->size; index++) {
            if (NULL == (node = (orte_node_t*)opal_pointer_array_get_item(map->nodes, index))) {
                continue;
            }
            OPAL_OUTPUT_VERBOSE((2, orte_state_base_framework.framework_output,
                                 "%s releasing procs for job %s from node %s",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 ORTE_JOBID_PRINT(jdata->jobid), node->name));
            for (i = 0; i < node->procs->size; i++) {
                if (NULL == (proc = (orte_proc_t*)opal_pointer_array_get_item(node->procs, i))) {
                    continue;
                }
                if (proc->name.jobid != jdata->jobid) {
                    /* skip procs from another job */
                    continue;
                }
                node->slots_inuse--;
                node->num_procs--;
                OPAL_OUTPUT_VERBOSE((2, orte_state_base_framework.framework_output,
                                     "%s releasing proc %s from node %s",
                                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                     ORTE_NAME_PRINT(&proc->name), node->name));
                /* set the entry in the node array to NULL */
                opal_pointer_array_set_item(node->procs, i, NULL);
                /* release the proc once for the map entry */
                OBJ_RELEASE(proc);
            }
            /* set the node location to NULL */
            opal_pointer_array_set_item(map->nodes, index, NULL);
            /* maintain accounting */
            OBJ_RELEASE(node);
        }
        OBJ_RELEASE(map);
        jdata->map = NULL;
    }

 CHECK_ALIVE:
    /* now check to see if all jobs are done - trigger notification of this jdata
     * object when we find it
     */
    one_still_alive = false;
    j = opal_hash_table_get_first_key_uint32(orte_job_data, &u32, (void **)&job, &nptr);
    while (OPAL_SUCCESS == j) {
        /* skip the daemon job and all jobs from other families */
        if (job->jobid == ORTE_PROC_MY_NAME->jobid ||
            ORTE_JOB_FAMILY(job->jobid) != ORTE_JOB_FAMILY(ORTE_PROC_MY_NAME->jobid)) {
            goto next;
        }
        /* if this is the job we are checking AND it normally terminated,
         * then activate the "notify_completed" state - this will release
         * the job state, but is provided so that the HNP main code can
         * take alternative actions if desired. If the state is killed_by_cmd,
         * then go ahead and release it. We cannot release it if it
         * abnormally terminated as mpirun needs the info so it can
         * report appropriately to the user
         *
         * NOTE: do not release the primary job (j=1) so we
         * can pretty-print completion message
         */
        if (NULL != jdata && job->jobid == jdata->jobid) {
            if (jdata->state == ORTE_JOB_STATE_TERMINATED) {
                OPAL_OUTPUT_VERBOSE((2, orte_state_base_framework.framework_output,
                                     "%s state:base:check_job_completed state is terminated - activating notify",
                                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));
                ORTE_ACTIVATE_JOB_STATE(jdata, ORTE_JOB_STATE_NOTIFY_COMPLETED);
                one_still_alive = true;
            } else if (jdata->state == ORTE_JOB_STATE_KILLED_BY_CMD ||
                       jdata->state == ORTE_JOB_STATE_NOTIFIED) {
                OPAL_OUTPUT_VERBOSE((2, orte_state_base_framework.framework_output,
                                     "%s state:base:check_job_completed state is killed or notified - cleaning up",
                                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));
                /* release this object, ensuring that the
                 * pointer array internal accounting
                 * is maintained!
                 */
                if (1 < j) {
                    if (ORTE_FLAG_TEST(jdata, ORTE_JOB_FLAG_DEBUGGER_DAEMON)) {
                        /* this was a debugger daemon. notify that a debugger has detached */
                        ORTE_ACTIVATE_JOB_STATE(jdata, ORTE_JOB_STATE_DEBUGGER_DETACH);
                    }
                    opal_hash_table_set_value_uint32(orte_job_data, jdata->jobid, NULL);
                    OBJ_RELEASE(jdata);
                }
            }
            goto next;
        }
        /* if the job is flagged to not be monitored, skip it */
        if (ORTE_FLAG_TEST(job, ORTE_JOB_FLAG_DO_NOT_MONITOR)) {
            goto next;
        }
        /* when checking for job termination, we must be sure to NOT check
         * our own job as it - rather obviously - has NOT terminated!
         */
        if (ORTE_JOB_STATE_NOTIFIED != job->state) {
            /* we have at least one job that is not done yet - we cannot
             * just return, though, as we need to ensure we cleanout the
             * job data for the job that just completed
             */
            OPAL_OUTPUT_VERBOSE((2, orte_state_base_framework.framework_output,
                                 "%s state:base:check_job_completed job %s is not terminated (%d:%d)",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 ORTE_JOBID_PRINT(job->jobid),
                                 job->num_terminated, job->num_procs));
            one_still_alive = true;
        }
        else {
            OPAL_OUTPUT_VERBOSE((2, orte_state_base_framework.framework_output,
                                 "%s state:base:check_job_completed job %s is terminated (%d vs %d [%s])",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 ORTE_JOBID_PRINT(job->jobid),
                                 job->num_terminated, job->num_procs,
                                 (NULL == jdata) ? "UNKNOWN" : orte_job_state_to_str(jdata->state) ));
        }
      next:
        j = opal_hash_table_get_next_key_uint32(orte_job_data, &u32, (void **)&job, nptr, &nptr);
    }

    /* if a job is still alive, we just return */
    if (one_still_alive) {
        OPAL_OUTPUT_VERBOSE((2, orte_state_base_framework.framework_output,
                             "%s state:base:check_job_completed at least one job is not terminated",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));
        OBJ_RELEASE(caddy);
        return;
    }
    /* if we get here, then all jobs are done, so terminate */
    OPAL_OUTPUT_VERBOSE((2, orte_state_base_framework.framework_output,
                         "%s state:base:check_job_completed all jobs terminated",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));

    /* stop the job timeout event, if set */
    if (NULL != orte_mpiexec_timeout) {
        OBJ_RELEASE(orte_mpiexec_timeout);
        orte_mpiexec_timeout = NULL;
    }

    /* set the exit status to 0 - this will only happen if it
     * wasn't already set by an error condition
     */
    ORTE_UPDATE_EXIT_STATUS(0);

    /* order daemon termination - this tells us to cleanup
     * our local procs as well as telling remote daemons
     * to die
     */
    orte_plm.terminate_orteds();

    OBJ_RELEASE(caddy);
}


void orte_state_base_check_fds(orte_job_t *jdata)
{
    int nfds, i, fdflags, flflags;
    char path[1024], info[256], **list=NULL, *status, *result, *r2;
    ssize_t rc;
    struct flock fl;
    bool flk;
    int cnt = 0;

    /* get the number of available file descriptors
     * for this daemon */
    nfds = getdtablesize();
    result = NULL;
    /* loop over them and get their info */
    for (i=0; i < nfds; i++) {
        fdflags = fcntl(i, F_GETFD);
        if (-1 == fdflags) {
            /* no open fd in that slot */
            continue;
        }
        flflags = fcntl(i, F_GETFL);
        if (-1 == flflags) {
            /* no open fd in that slot */
            continue;
        }
        snprintf(path, 1024, "/proc/self/fd/%d", i);
        memset(info, 0, 256);
        /* read the info about this fd */
        rc = readlink(path, info, 256);
        if (-1 == rc) {
            /* this fd is unavailable */
            continue;
        }
        /* get any file locking status */
        fl.l_type = F_WRLCK;
        fl.l_whence = 0;
        fl.l_start = 0;
        fl.l_len = 0;
        if (-1 == fcntl(i, F_GETLK, &fl)) {
            flk = false;
        } else {
            flk = true;
        }
        /* construct the list of capabilities */
        if (fdflags & FD_CLOEXEC) {
            opal_argv_append_nosize(&list, "cloexec");
        }
        if (flflags & O_APPEND) {
            opal_argv_append_nosize(&list, "append");
        }
        if (flflags & O_NONBLOCK) {
            opal_argv_append_nosize(&list, "nonblock");
        }
        /* from the man page:
         *  Unlike the other values that can be specified in flags,
         * the access mode values O_RDONLY, O_WRONLY, and O_RDWR,
         * do not specify individual bits.  Rather, they define
         * the low order two bits of flags, and defined respectively
         * as 0, 1, and 2. */
        if (O_RDONLY == (flflags & 3)) {
            opal_argv_append_nosize(&list, "rdonly");
        } else if (O_WRONLY == (flflags & 3)) {
            opal_argv_append_nosize(&list, "wronly");
        } else {
            opal_argv_append_nosize(&list, "rdwr");
        }
        if (flk && F_UNLCK != fl.l_type) {
            if (F_WRLCK == fl.l_type) {
                opal_argv_append_nosize(&list, "wrlock");
            } else {
                opal_argv_append_nosize(&list, "rdlock");
            }
        }
        if (NULL != list) {
            status = opal_argv_join(list, ' ');
            opal_argv_free(list);
            list = NULL;
            if (NULL == result) {
                asprintf(&result, "    %d\t(%s)\t%s\n", i, info, status);
            } else {
                asprintf(&r2, "%s    %d\t(%s)\t%s\n", result, i, info, status);
                free(result);
                result = r2;
            }
            free(status);
        }
        ++cnt;
    }
    asprintf(&r2, "%s: %d open file descriptors after job %d completed\n%s",
             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), cnt, ORTE_LOCAL_JOBID(jdata->jobid), result);
    opal_output(0, "%s", r2);
    free(result);
    free(r2);
}
