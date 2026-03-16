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
 * Copyright (c) 2006-2013 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2009      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2011      Oak Ridge National Labs.  All rights reserved.
 * Copyright (c) 2013-2018 Intel, Inc. All rights reserved.
 * Copyright (c) 2014-2017 Mellanox Technologies, Inc.
 *                         All rights reserved.
 * Copyright (c) 2014      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 */

#include "orte_config.h"

#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif

#include "opal/util/argv.h"
#include "opal/util/output.h"
#include "opal/dss/dss.h"
#include "opal/mca/hwloc/hwloc-internal.h"
#include "opal/mca/pstat/pstat.h"

#include "orte/mca/errmgr/errmgr.h"
#include "orte/mca/iof/iof.h"
#include "orte/mca/rmaps/rmaps_types.h"
#include "orte/mca/schizo/schizo.h"
#include "orte/mca/state/state.h"
#include "orte/util/name_fns.h"
#include "orte/util/threads.h"
#include "orte/runtime/orte_globals.h"
#include "orte/mca/rml/rml.h"
#include "orte/mca/plm/plm.h"
#include "orte/mca/plm/base/plm_private.h"

#include "pmix_server_internal.h"

static void _client_conn(int sd, short args, void *cbdata)
{
    orte_pmix_server_op_caddy_t *cd = (orte_pmix_server_op_caddy_t*)cbdata;
    orte_job_t *jdata;
    orte_proc_t *p, *ptr;
    int i;

    ORTE_ACQUIRE_OBJECT(cd);

    if (NULL != cd->server_object) {
        /* we were passed back the orte_proc_t */
        p = (orte_proc_t*)cd->server_object;
    } else {
        /* find the named process */
        p = NULL;
        if (NULL == (jdata = orte_get_job_data_object(cd->proc.jobid))) {
            return;
        }
        for (i=0; i < jdata->procs->size; i++) {
            if (NULL == (ptr = (orte_proc_t*)opal_pointer_array_get_item(jdata->procs, i))) {
                continue;
            }
            if (cd->proc.jobid != ptr->name.jobid) {
                continue;
            }
            if (cd->proc.vpid == ptr->name.vpid) {
                p = ptr;
                break;
            }
        }
    }
    if (NULL != p) {
        ORTE_FLAG_SET(p, ORTE_PROC_FLAG_REG);
        ORTE_ACTIVATE_PROC_STATE(&p->name, ORTE_PROC_STATE_REGISTERED);
    }
    if (NULL != cd->cbfunc) {
        cd->cbfunc(OPAL_SUCCESS, cd->cbdata);
    }
    OBJ_RELEASE(cd);
}

int pmix_server_client_connected_fn(opal_process_name_t *proc, void *server_object,
                                    opal_pmix_op_cbfunc_t cbfunc, void *cbdata)
{
    /* need to thread-shift this request as we are going
     * to access our global list of registered events */
    ORTE_PMIX_THREADSHIFT(proc, server_object, OPAL_SUCCESS, NULL,
                          NULL, _client_conn, cbfunc, cbdata);
    return ORTE_SUCCESS;
}

static void _client_finalized(int sd, short args, void *cbdata)
{
    orte_pmix_server_op_caddy_t *cd = (orte_pmix_server_op_caddy_t*)cbdata;
    orte_job_t *jdata;
    orte_proc_t *p, *ptr;
    int i;

    ORTE_ACQUIRE_OBJECT(cd);

    if (NULL != cd->server_object) {
        /* we were passed back the orte_proc_t */
        p = (orte_proc_t*)cd->server_object;
    } else {
        /* find the named process */
        p = NULL;
        if (NULL == (jdata = orte_get_job_data_object(cd->proc.jobid))) {
            return;
        }
        for (i=0; i < jdata->procs->size; i++) {
            if (NULL == (ptr = (orte_proc_t*)opal_pointer_array_get_item(jdata->procs, i))) {
                continue;
            }
            if (cd->proc.jobid != ptr->name.jobid) {
                continue;
            }
            if (cd->proc.vpid == ptr->name.vpid) {
                p = ptr;
                break;
            }
        }
        /* if we came thru this code path, then this client must be an
         * independent tool that connected to us - i.e., it wasn't
         * something we spawned. For accounting purposes, we have to
         * ensure the job complete procedure is run - otherwise, slots
         * and other resources won't correctly be released */
        ORTE_FLAG_SET(p, ORTE_PROC_FLAG_IOF_COMPLETE);
        ORTE_FLAG_SET(p, ORTE_PROC_FLAG_WAITPID);
        ORTE_ACTIVATE_PROC_STATE(&cd->proc, ORTE_PROC_STATE_TERMINATED);
    }
    if (NULL != p) {
        ORTE_FLAG_SET(p, ORTE_PROC_FLAG_HAS_DEREG);
        /* release the caller */
        if (NULL != cd->cbfunc) {
            cd->cbfunc(ORTE_SUCCESS, cd->cbdata);
        }
    }
    OBJ_RELEASE(cd);
}

int pmix_server_client_finalized_fn(opal_process_name_t *proc, void *server_object,
                                    opal_pmix_op_cbfunc_t cbfunc, void *cbdata)
{
    /* need to thread-shift this request as we are going
     * to access our global list of registered events */
    ORTE_PMIX_THREADSHIFT(proc, server_object, OPAL_SUCCESS, NULL,
                          NULL, _client_finalized, cbfunc, cbdata);
    return ORTE_SUCCESS;

}

static void _client_abort(int sd, short args, void *cbdata)
{
    orte_pmix_server_op_caddy_t *cd = (orte_pmix_server_op_caddy_t*)cbdata;
    orte_job_t *jdata;
    orte_proc_t *p, *ptr;
    int i;

    ORTE_ACQUIRE_OBJECT(cd);

    if (NULL != cd->server_object) {
        p = (orte_proc_t*)cd->server_object;
    } else {
        /* find the named process */
        p = NULL;
        if (NULL == (jdata = orte_get_job_data_object(cd->proc.jobid))) {
            return;
        }
        for (i=0; i < jdata->procs->size; i++) {
            if (NULL == (ptr = (orte_proc_t*)opal_pointer_array_get_item(jdata->procs, i))) {
                continue;
            }
            if (cd->proc.jobid != ptr->name.jobid) {
                continue;
            }
            if (cd->proc.vpid == ptr->name.vpid) {
                p = ptr;
                break;
            }
        }
    }
    if (NULL != p) {
        p->exit_code = cd->status;
        ORTE_ACTIVATE_PROC_STATE(&p->name, ORTE_PROC_STATE_CALLED_ABORT);
    }

    /* release the caller */
    if (NULL != cd->cbfunc) {
        cd->cbfunc(OPAL_SUCCESS, cd->cbdata);
    }
    OBJ_RELEASE(cd);
}

int pmix_server_abort_fn(opal_process_name_t *proc, void *server_object,
                         int status, const char msg[],
                         opal_list_t *procs_to_abort,
                         opal_pmix_op_cbfunc_t cbfunc, void *cbdata)
{
    /* need to thread-shift this request as we are going
     * to access our global list of registered events */
    ORTE_PMIX_THREADSHIFT(proc, server_object, status, msg,
                          procs_to_abort, _client_abort, cbfunc, cbdata);
    return ORTE_SUCCESS;
}

static void _register_events(int sd, short args, void *cbdata)
{
    orte_pmix_server_op_caddy_t *cd = (orte_pmix_server_op_caddy_t*)cbdata;
    opal_value_t *info;

    ORTE_ACQUIRE_OBJECT(cd);

    /* the OPAL layer "owns" the list, but let's deconstruct it
     * here so we don't have to duplicate the data */
    while (NULL != (info = (opal_value_t*)opal_list_remove_first(cd->info))) {
        /* don't worry about duplication as the underlying host
         * server is already protecting us from it */
        opal_list_append(&orte_pmix_server_globals.notifications, &info->super);
    }

    if (NULL != cd->cbfunc) {
        cd->cbfunc(ORTE_SUCCESS, cd->cbdata);
    }
    OBJ_RELEASE(cd);
}

/* hook for the local PMIX server to pass event registrations
 * up to us - we will assume the responsibility for providing
 * notifications for registered events */
int pmix_server_register_events_fn(opal_list_t *info,
                                   opal_pmix_op_cbfunc_t cbfunc,
                                   void *cbdata)
{
    /* need to thread-shift this request as we are going
     * to access our global list of registered events */
    ORTE_PMIX_OPERATION(NULL, info, _register_events, cbfunc, cbdata);
    return ORTE_SUCCESS;
}

static void _deregister_events(int sd, short args, void *cbdata)
{
    orte_pmix_server_op_caddy_t *cd = (orte_pmix_server_op_caddy_t*)cbdata;
    opal_value_t *info, *iptr, *nptr;

    ORTE_ACQUIRE_OBJECT(cd);

    /* the OPAL layer "owns" the list, but let's deconstruct it
     * here for consistency */
    while (NULL != (info = (opal_value_t*)opal_list_remove_first(cd->info))) {
        /* search for matching requests */
        OPAL_LIST_FOREACH_SAFE(iptr, nptr, &orte_pmix_server_globals.notifications, opal_value_t) {
            if (OPAL_EQUAL == opal_dss.compare(iptr, info, OPAL_VALUE)) {
                opal_list_remove_item(&orte_pmix_server_globals.notifications, &iptr->super);
                OBJ_RELEASE(iptr);
                break;
            }
        }
        OBJ_RELEASE(info);
    }

    if (NULL != cd->cbfunc) {
        cd->cbfunc(ORTE_SUCCESS, cd->cbdata);
    }
    OBJ_RELEASE(cd);
}
/* hook for the local PMIX server to pass event deregistrations
 * up to us */
int pmix_server_deregister_events_fn(opal_list_t *info,
                                     opal_pmix_op_cbfunc_t cbfunc,
                                     void *cbdata)
{
    /* need to thread-shift this request as we are going
     * to access our global list of registered events */
    ORTE_PMIX_OPERATION(NULL, info, _deregister_events, cbfunc, cbdata);
    return ORTE_SUCCESS;
}

static void _notify_release(int status, void *cbdata)
{
    orte_pmix_server_op_caddy_t *cd = (orte_pmix_server_op_caddy_t*)cbdata;

    ORTE_ACQUIRE_OBJECT(cd);

    if (NULL != cd->info) {
        OPAL_LIST_RELEASE(cd->info);
    }
    OBJ_RELEASE(cd);
}

/* someone has sent us an event that we need to distribute
 * to our local clients */
void pmix_server_notify(int status, orte_process_name_t* sender,
                        opal_buffer_t *buffer,
                        orte_rml_tag_t tg, void *cbdata)
{
    int cnt, rc, ret, ninfo, n;
    opal_value_t *val;
    orte_pmix_server_op_caddy_t *cd;
    orte_process_name_t source;

    opal_output_verbose(2, orte_pmix_server_globals.output,
                        "%s Notification received from %s",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                        ORTE_NAME_PRINT(sender));

    /* unpack the status */
    cnt = 1;
    if (ORTE_SUCCESS != (rc = opal_dss.unpack(buffer, &ret, &cnt, OPAL_INT))) {
        ORTE_ERROR_LOG(rc);
        return;
    }

    /* unpack the source */
    cnt = 1;
    if (ORTE_SUCCESS != (rc = opal_dss.unpack(buffer, &source, &cnt, ORTE_NAME))) {
        ORTE_ERROR_LOG(rc);
        return;
    }

    /* unpack the infos that were provided */
    cnt = 1;
    if (ORTE_SUCCESS != (rc = opal_dss.unpack(buffer, &ninfo, &cnt, OPAL_INT))) {
        ORTE_ERROR_LOG(rc);
        return;
    }

    /* if any were provided, add them to the list */
    cd = OBJ_NEW(orte_pmix_server_op_caddy_t);

    if (0 < ninfo) {
        cd->info = OBJ_NEW(opal_list_t);
        for (n=0; n < ninfo; n++) {
            val = OBJ_NEW(opal_value_t);
            if (ORTE_SUCCESS != (rc = opal_dss.unpack(buffer, &val, &cnt, OPAL_VALUE))) {
                ORTE_ERROR_LOG(rc);
                OBJ_RELEASE(val);
                OPAL_LIST_RELEASE(cd->info);
                OBJ_RELEASE(cd);
                return;
            }
            opal_list_append(cd->info, &val->super);
        }
    }

    /* protect against infinite loops by marking that this notification was
     * passed down to the server by me */
    if (NULL == cd->info) {
        cd->info = OBJ_NEW(opal_list_t);
    }
    val = OBJ_NEW(opal_value_t);
    val->key = strdup("orte.notify.donotloop");
    val->type = OPAL_BOOL;
    val->data.flag = true;
    opal_list_append(cd->info, &val->super);

    opal_output_verbose(2, orte_pmix_server_globals.output,
                        "%s NOTIFYING PMIX SERVER OF STATUS %d",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ret);
    if (OPAL_SUCCESS != (rc = opal_pmix.server_notify_event(ret, &source, cd->info, _notify_release, cd))) {
        ORTE_ERROR_LOG(rc);
        if (NULL != cd->info) {
            OPAL_LIST_RELEASE(cd->info);
        }
        OBJ_RELEASE(cd);
    }
}

int pmix_server_notify_event(int code, opal_process_name_t *source,
                             opal_list_t *info,
                             opal_pmix_op_cbfunc_t cbfunc, void *cbdata)
{
    opal_buffer_t *buf;
    int rc, ninfo;
    opal_value_t *val;
    orte_grpcomm_signature_t *sig;

    opal_output_verbose(2, orte_pmix_server_globals.output,
                        "%s local process %s generated event code %d",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                        ORTE_NAME_PRINT(source), code);

    /* check to see if this is one we sent down */
    OPAL_LIST_FOREACH(val, info, opal_value_t) {
        if (0 == strcmp(val->key, "orte.notify.donotloop")) {
            /* yep - do not process */
            goto done;
        }
    }

    /* a local process has generated an event - we need to xcast it
     * to all the daemons so it can be passed down to their local
     * procs */
    buf = OBJ_NEW(opal_buffer_t);
    if (NULL == buf) {
        return ORTE_ERR_OUT_OF_RESOURCE;
    }
    /* pack the status code */
    if (OPAL_SUCCESS != (rc = opal_dss.pack(buf, &code, 1, OPAL_INT))) {
        ORTE_ERROR_LOG(rc);
        OBJ_RELEASE(buf);
        return rc;
    }
    /* pack the source */
    if (OPAL_SUCCESS != (rc = opal_dss.pack(buf, source, 1, ORTE_NAME))) {
        ORTE_ERROR_LOG(rc);
        OBJ_RELEASE(buf);
        return rc;
    }

    /* pack the number of infos */
    if (NULL == info) {
        ninfo = 0;
    } else {
        ninfo = opal_list_get_size(info);
    }
    if (OPAL_SUCCESS != (rc = opal_dss.pack(buf, &ninfo, 1, OPAL_INT))) {
        ORTE_ERROR_LOG(rc);
        OBJ_RELEASE(buf);
        return rc;
    }
    if (0 < ninfo) {
        OPAL_LIST_FOREACH(val, info, opal_value_t) {
            if (OPAL_SUCCESS != (rc = opal_dss.pack(buf, &val, 1, OPAL_VALUE))) {
                ORTE_ERROR_LOG(rc);
                OBJ_RELEASE(buf);
                return rc;
            }
        }
    }

    /* goes to all daemons */
    sig = OBJ_NEW(orte_grpcomm_signature_t);
    if (NULL == sig) {
        OBJ_RELEASE(buf);
        return ORTE_ERR_OUT_OF_RESOURCE;
    }
    sig->signature = (orte_process_name_t*)malloc(sizeof(orte_process_name_t));
    if (NULL == sig->signature) {
        OBJ_RELEASE(buf);
        OBJ_RELEASE(sig);
        return ORTE_ERR_OUT_OF_RESOURCE;
    }
    sig->signature[0].jobid = ORTE_PROC_MY_NAME->jobid;
    sig->signature[0].vpid = ORTE_VPID_WILDCARD;
    sig->sz = 1;
    if (ORTE_SUCCESS != (rc = orte_grpcomm.xcast(sig, ORTE_RML_TAG_NOTIFICATION, buf))) {
        ORTE_ERROR_LOG(rc);
        OBJ_RELEASE(buf);
        OBJ_RELEASE(sig);
        return rc;
    }
    OBJ_RELEASE(buf);
    /* maintain accounting */
    OBJ_RELEASE(sig);

  done:
    /* execute the callback */
    if (NULL != cbfunc) {
        cbfunc(ORTE_SUCCESS, cbdata);
    }
    return ORTE_SUCCESS;
}

static void qrel(void *cbdata)
{
    opal_list_t *l = (opal_list_t*)cbdata;
    OPAL_LIST_RELEASE(l);
}
static void _query(int sd, short args, void *cbdata)
{
    orte_pmix_server_op_caddy_t *cd = (orte_pmix_server_op_caddy_t*)cbdata;
    opal_pmix_query_t *q;
    opal_value_t *kv;
    orte_jobid_t jobid;
    orte_job_t *jdata;
    orte_proc_t *proct;
    orte_app_context_t *app;
    int rc = ORTE_SUCCESS, i, k, num_replies;
    opal_list_t *results, targets, *array;
    size_t n;
    uint32_t key;
    void *nptr;
    char **nspaces=NULL, nspace[512];
    char **ans = NULL;
    bool local_only;
    orte_namelist_t *nm;
    opal_pstats_t pstat;
    float pss;

    ORTE_ACQUIRE_OBJECT(cd);

    opal_output_verbose(2, orte_pmix_server_globals.output,
                        "%s processing query",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));

    results = OBJ_NEW(opal_list_t);

    /* see what they wanted */
    OPAL_LIST_FOREACH(q, cd->info, opal_pmix_query_t) {
        for (n=0; NULL != q->keys[n]; n++) {
            opal_output_verbose(2, orte_pmix_server_globals.output,
                                "%s processing key %s",
                                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), q->keys[n]);
            if (0 == strcmp(q->keys[n], OPAL_PMIX_QUERY_NAMESPACES)) {
                /* get the current jobids */
                rc = opal_hash_table_get_first_key_uint32(orte_job_data, &key, (void **)&jdata, &nptr);
                while (OPAL_SUCCESS == rc) {
                    if (ORTE_PROC_MY_NAME->jobid != jdata->jobid) {
                        memset(nspace, 0, 512);
                        (void)opal_snprintf_jobid(nspace, 512, jdata->jobid);
                        opal_argv_append_nosize(&nspaces, nspace);
                    }
                    rc = opal_hash_table_get_next_key_uint32(orte_job_data, &key, (void **)&jdata, nptr, &nptr);
                }
                /* join the results into a single comma-delimited string */
                kv = OBJ_NEW(opal_value_t);
                kv->key = strdup(OPAL_PMIX_QUERY_NAMESPACES);
                kv->type = OPAL_STRING;
                if (NULL != nspaces) {
                    kv->data.string = opal_argv_join(nspaces, ',');
                } else {
                    kv->data.string = NULL;
                }
                opal_list_append(results, &kv->super);
            } else if (0 == strcmp(q->keys[n], OPAL_PMIX_QUERY_SPAWN_SUPPORT)) {
                opal_argv_append_nosize(&ans, OPAL_PMIX_HOST);
                opal_argv_append_nosize(&ans, OPAL_PMIX_HOSTFILE);
                opal_argv_append_nosize(&ans, OPAL_PMIX_ADD_HOST);
                opal_argv_append_nosize(&ans, OPAL_PMIX_ADD_HOSTFILE);
                opal_argv_append_nosize(&ans, OPAL_PMIX_PREFIX);
                opal_argv_append_nosize(&ans, OPAL_PMIX_WDIR);
                opal_argv_append_nosize(&ans, OPAL_PMIX_MAPPER);
                opal_argv_append_nosize(&ans, OPAL_PMIX_PPR);
                opal_argv_append_nosize(&ans, OPAL_PMIX_MAPBY);
                opal_argv_append_nosize(&ans, OPAL_PMIX_RANKBY);
                opal_argv_append_nosize(&ans, OPAL_PMIX_BINDTO);
                /* create the return kv */
                kv = OBJ_NEW(opal_value_t);
                kv->key = strdup(OPAL_PMIX_QUERY_SPAWN_SUPPORT);
                kv->type = OPAL_STRING;
                kv->data.string = opal_argv_join(ans, ',');
                opal_list_append(results, &kv->super);
                opal_argv_free(ans);
                ans = NULL;
            } else if (0 == strcmp(q->keys[n], OPAL_PMIX_QUERY_DEBUG_SUPPORT)) {
                opal_argv_append_nosize(&ans, OPAL_PMIX_DEBUG_STOP_IN_INIT);
                opal_argv_append_nosize(&ans, OPAL_PMIX_DEBUG_JOB);
                opal_argv_append_nosize(&ans, OPAL_PMIX_DEBUG_WAIT_FOR_NOTIFY);
                /* create the return kv */
                kv = OBJ_NEW(opal_value_t);
                kv->key = strdup(OPAL_PMIX_QUERY_DEBUG_SUPPORT);
                kv->type = OPAL_STRING;
                kv->data.string = opal_argv_join(ans, ',');
                opal_list_append(results, &kv->super);
                opal_argv_free(ans);
                ans = NULL;
            } else if (0 == strcmp(q->keys[n], OPAL_PMIX_QUERY_MEMORY_USAGE)) {
                OBJ_CONSTRUCT(&targets, opal_list_t);
                /* check the qualifiers to find the procs they want to
                 * know about - if qualifiers are NULL, then get it for
                 * the daemons + all active jobs */
                if (0 == opal_list_get_size(&q->qualifiers)) {
                    /* create a request tracker */
                    /* xcast a request for all memory usage */
                    /* return success - the callback will be done
                     * once we get the results */
                    OPAL_LIST_DESTRUCT(&targets);
                    return;
                }

                /* scan the qualifiers */
                local_only = false;
                OPAL_LIST_FOREACH(kv, &q->qualifiers, opal_value_t) {
                    if (0 == strcmp(kv->key, OPAL_PMIX_QUERY_LOCAL_ONLY)) {
                        if (OPAL_UNDEF == kv->type || kv->data.flag) {
                            local_only = true;
                        }
                    } else if (0 == strcmp(kv->key, OPAL_PMIX_PROCID)) {
                        /* save this directive on our list of targets */
                        nm = OBJ_NEW(orte_namelist_t);
                        memcpy(&nm->name, &kv->data.name, sizeof(opal_process_name_t));
                        opal_list_append(&targets, &nm->super);
                    }
                }

                /* if they have asked for only our local procs or daemon,
                 * then we can just get the data directly */
                if (local_only) {
                    if (0 == opal_list_get_size(&targets)) {
                        kv = OBJ_NEW(opal_value_t);
                        kv->key = strdup(OPAL_PMIX_QUERY_MEMORY_USAGE);
                        kv->type = OPAL_PTR;
                        array = OBJ_NEW(opal_list_t);
                        kv->data.ptr = array;
                        opal_list_append(results, &kv->super);
                        /* collect my memory usage */
                        OBJ_CONSTRUCT(&pstat, opal_pstats_t);
                        opal_pstat.query(orte_process_info.pid, &pstat, NULL);
                        kv = OBJ_NEW(opal_value_t);
                        kv->key = strdup(OPAL_PMIX_DAEMON_MEMORY);
                        kv->type = OPAL_FLOAT;
                        kv->data.fval = pstat.pss;
                        opal_list_append(array, &kv->super);
                        OBJ_DESTRUCT(&pstat);
                        /* collect the memory usage of all my children */
                        pss = 0.0;
                        num_replies = 0;
                        for (i=0; i < orte_local_children->size; i++) {
                            if (NULL != (proct = (orte_proc_t*)opal_pointer_array_get_item(orte_local_children, i)) &&
                                ORTE_FLAG_TEST(proct, ORTE_PROC_FLAG_ALIVE)) {
                                /* collect the stats on this proc */
                                OBJ_CONSTRUCT(&pstat, opal_pstats_t);
                                if (OPAL_SUCCESS == opal_pstat.query(proct->pid, &pstat, NULL)) {
                                    pss += pstat.pss;
                                    ++num_replies;
                                }
                                OBJ_DESTRUCT(&pstat);
                            }
                        }
                        /* compute the average value */
                        if (0 < num_replies) {
                            pss /= (float)num_replies;
                        }
                        kv = OBJ_NEW(opal_value_t);
                        kv->key = strdup(OPAL_PMIX_CLIENT_AVG_MEMORY);
                        kv->type = OPAL_FLOAT;
                        kv->data.fval = pss;
                        opal_list_append(array, &kv->super);
                    } else {

                    }
                } else {
                    /* if they want it for remote procs, see who is hosting them
                     * and ask directly for the info - if rank=wildcard, then
                     * we need to xcast the request and collect the results */
                }
            } else if (0 == strcmp(q->keys[n], OPAL_PMIX_TIME_REMAINING)) {
                kv = OBJ_NEW(opal_value_t);
                kv->key = strdup(OPAL_PMIX_TIME_REMAINING);
                kv->type = OPAL_UINT32;
                if (ORTE_SUCCESS != orte_schizo.get_remaining_time(&kv->data.uint32)) {
                    OBJ_RELEASE(kv);
                } else {
                    opal_list_append(results, &kv->super);
                }
            } else if (0 == strcmp(q->keys[n], OPAL_PMIX_HWLOC_XML_V1)) {
                if (NULL != opal_hwloc_topology) {
                    char *xmlbuffer=NULL;
                    int len;
                    kv = OBJ_NEW(opal_value_t);
                    kv->key = strdup(OPAL_PMIX_HWLOC_XML_V1);
                    #if HWLOC_API_VERSION < 0x20000
                        /* get this from the v1.x API */
                        if (0 != hwloc_topology_export_xmlbuffer(opal_hwloc_topology, &xmlbuffer, &len)) {
                            OBJ_RELEASE(kv);
                            continue;
                        }
                    #else
                        /* get it from the v2 API */
                        if (0 != hwloc_topology_export_xmlbuffer(opal_hwloc_topology, &xmlbuffer, &len,
                                                                 HWLOC_TOPOLOGY_EXPORT_XML_FLAG_V1)) {
                            OBJ_RELEASE(kv);
                            continue;
                        }
                    #endif
                    kv->data.string = xmlbuffer;
                    kv->type = OPAL_STRING;
                    opal_list_append(results, &kv->super);
                }
            } else if (0 == strcmp(q->keys[n], OPAL_PMIX_HWLOC_XML_V2)) {
                /* we cannot provide it if we are using v1.x */
                #if HWLOC_API_VERSION >= 0x20000
                    if (NULL != opal_hwloc_topology) {
                        char *xmlbuffer=NULL;
                        int len;
                        kv = OBJ_NEW(opal_value_t);
                        kv->key = strdup(OPAL_PMIX_HWLOC_XML_V2);
                        if (0 != hwloc_topology_export_xmlbuffer(opal_hwloc_topology, &xmlbuffer, &len, 0)) {
                            OBJ_RELEASE(kv);
                            continue;
                        }
                        kv->data.string = xmlbuffer;
                        kv->type = OPAL_STRING;
                        opal_list_append(results, &kv->super);
                    }
                #endif
            } else if (0 == strcmp(q->keys[n], OPAL_PMIX_SERVER_URI)) {
                /* they want our URI */
                kv = OBJ_NEW(opal_value_t);
                kv->key = strdup(OPAL_PMIX_SERVER_URI);
                kv->type = OPAL_STRING;
                kv->data.string = strdup(orte_process_info.my_hnp_uri);
                opal_list_append(results, &kv->super);
            } else if (0 == strcmp(q->keys[n], OPAL_PMIX_QUERY_PROC_TABLE)) {
                /* the job they are asking about is in the qualifiers */
                jobid = ORTE_JOBID_INVALID;
                OPAL_LIST_FOREACH(kv, &q->qualifiers, opal_value_t) {
                    if (0 == strcmp(kv->key, OPAL_PMIX_PROCID)) {
                        /* save the id */
                        jobid = kv->data.name.jobid;
                        break;
                    }
                }
                if (ORTE_JOBID_INVALID == jobid) {
                    rc = ORTE_ERR_NOT_FOUND;
                    goto done;
                }
                /* construct a list of values with opal_proc_info_t
                 * entries for each proc in the indicated job */
                jdata = orte_get_job_data_object(jobid);
                if (NULL == jdata) {
                    rc = ORTE_ERR_NOT_FOUND;
                    goto done;
                }
                /* setup the reply */
                kv = OBJ_NEW(opal_value_t);
                kv->key = strdup(OPAL_PMIX_QUERY_PROC_TABLE);
                kv->type = OPAL_PTR;
                array = OBJ_NEW(opal_list_t);
                kv->data.ptr = array;
                opal_list_append(results, &kv->super);
                /* cycle thru the job and create an entry for each proc */
                for (k=0; k < jdata->procs->size; k++) {
                    if (NULL == (proct = (orte_proc_t*)opal_pointer_array_get_item(jdata->procs, k))) {
                        continue;
                    }
                    kv = OBJ_NEW(opal_value_t);
                    kv->type = OPAL_PROC_INFO;
                    kv->data.pinfo.name.jobid = jobid;
                    kv->data.pinfo.name.vpid = proct->name.vpid;
                    if (NULL != proct->node && NULL != proct->node->name) {
                        kv->data.pinfo.hostname = strdup(proct->node->name);
                    }
                    app = (orte_app_context_t*)opal_pointer_array_get_item(jdata->apps, proct->app_idx);
                    if (NULL != app && NULL != app->app) {
                        kv->data.pinfo.executable_name = strdup(app->app);
                    }
                    kv->data.pinfo.pid = proct->pid;
                    kv->data.pinfo.exit_code = proct->exit_code;
                    kv->data.pinfo.state = proct->state;
                    opal_list_append(array, &kv->super);
                }
            } else if (0 == strcmp(q->keys[n], OPAL_PMIX_QUERY_LOCAL_PROC_TABLE)) {
                /* the job they are asking about is in the qualifiers */
                jobid = ORTE_JOBID_INVALID;
                OPAL_LIST_FOREACH(kv, &q->qualifiers, opal_value_t) {
                    if (0 == strcmp(kv->key, OPAL_PMIX_PROCID)) {
                        /* save the id */
                        jobid = kv->data.name.jobid;
                        break;
                    }
                }
                if (ORTE_JOBID_INVALID == jobid) {
                    rc = ORTE_ERR_BAD_PARAM;
                    goto done;
                }
                /* construct a list of values with opal_proc_info_t
                 * entries for each LOCAL proc in the indicated job */
                jdata = orte_get_job_data_object(jobid);
                if (NULL == jdata) {
                    rc = ORTE_ERR_NOT_FOUND;
                    goto done;
                }
                /* setup the reply */
                kv = OBJ_NEW(opal_value_t);
                kv->key = strdup(OPAL_PMIX_QUERY_LOCAL_PROC_TABLE);
                kv->type = OPAL_PTR;
                array = OBJ_NEW(opal_list_t);
                kv->data.ptr = array;
                opal_list_append(results, &kv->super);
                /* cycle thru the job and create an entry for each proc */
                for (k=0; k < jdata->procs->size; k++) {
                    if (NULL == (proct = (orte_proc_t*)opal_pointer_array_get_item(jdata->procs, k))) {
                        continue;
                    }
                    if (ORTE_FLAG_TEST(proct, ORTE_PROC_FLAG_LOCAL)) {
                        kv = OBJ_NEW(opal_value_t);
                        kv->type = OPAL_PROC_INFO;
                        kv->data.pinfo.name.jobid = jobid;
                        kv->data.pinfo.name.vpid = proct->name.vpid;
                        if (NULL != proct->node && NULL != proct->node->name) {
                            kv->data.pinfo.hostname = strdup(proct->node->name);
                        }
                        app = (orte_app_context_t*)opal_pointer_array_get_item(jdata->apps, proct->app_idx);
                        if (NULL != app && NULL != app->app) {
                            kv->data.pinfo.executable_name = strdup(app->app);
                        }
                        kv->data.pinfo.pid = proct->pid;
                        kv->data.pinfo.exit_code = proct->exit_code;
                        kv->data.pinfo.state = proct->state;
                        opal_list_append(array, &kv->super);
                    }
                }
            }
        }
    }

  done:
    if (ORTE_SUCCESS == rc) {
        if (0 == opal_list_get_size(results)) {
            rc = ORTE_ERR_NOT_FOUND;
        } else if (opal_list_get_size(results) < opal_list_get_size(cd->info)) {
            rc = ORTE_ERR_PARTIAL_SUCCESS;
        }
    }
    cd->infocbfunc(rc, results, cd->cbdata, qrel, results);
}

int pmix_server_query_fn(opal_process_name_t *requestor,
                         opal_list_t *queries,
                         opal_pmix_info_cbfunc_t cbfunc, void *cbdata)
{
    orte_pmix_server_op_caddy_t *cd;

    if (NULL == queries || NULL == cbfunc) {
        return OPAL_ERR_BAD_PARAM;
    }

    /* need to threadshift this request */
    cd = OBJ_NEW(orte_pmix_server_op_caddy_t);
    cd->proc = *requestor;
    cd->info = queries;
    cd->infocbfunc = cbfunc;
    cd->cbdata = cbdata;

    opal_event_set(orte_event_base, &(cd->ev), -1,
                   OPAL_EV_WRITE, _query, cd);
    opal_event_set_priority(&(cd->ev), ORTE_MSG_PRI);
    ORTE_POST_OBJECT(cd);
    opal_event_active(&(cd->ev), OPAL_EV_WRITE, 1);

    return ORTE_SUCCESS;
}

static void _toolconn(int sd, short args, void *cbdata)
{
    orte_pmix_server_op_caddy_t *cd = (orte_pmix_server_op_caddy_t*)cbdata;
    orte_job_t *jdata;
    orte_app_context_t *app;
    orte_proc_t *proc;
    orte_node_t *node, *nptr;
    char *hostname = NULL;
    orte_process_name_t tool = {ORTE_JOBID_INVALID, ORTE_VPID_INVALID};
    int rc, i;
    opal_value_t *val;
    bool flag = false, flag_given = false;;

    ORTE_ACQUIRE_OBJECT(cd);

    opal_output_verbose(2, orte_pmix_server_globals.output,
                        "%s TOOL CONNECTION PROCESSING",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));

   /* check for directives */
   if (NULL != cd->info) {
       OPAL_LIST_FOREACH(val, cd->info, opal_value_t) {
            if (0 == strcmp(val->key, OPAL_PMIX_EVENT_SILENT_TERMINATION)) {
                if (OPAL_UNDEF == val->type || val->data.flag) {
                    flag = true;
                    flag_given = true;
                }
            } else if (0 == strcmp(val->key, OPAL_PMIX_NSPACE)) {
                tool.jobid = val->data.name.jobid;
            } else if (0 == strcmp(val->key, OPAL_PMIX_RANK)) {
                tool.vpid = val->data.name.vpid;
            } else if (0 == strcmp(val->key, OPAL_PMIX_HOSTNAME)) {
                if (NULL != hostname) {
                    /* shouldn't happen, but just take the last one */
                    free(hostname);
                }
                hostname = strdup(val->data.string);
            }
        }
    }

    /* if we are not the HNP or master, and the tool doesn't
     * already have a name (i.e., we didn't spawn it), then
     * there is nothing we can currently do.
     * Eventually, when we switch to nspace instead of an
     * integer jobid, we'll just locally assign this value */
    if (ORTE_JOBID_INVALID == tool.jobid ||
        ORTE_VPID_INVALID == tool.vpid) {
        /* if we are the HNP, we can directly assign the jobid */
        if (ORTE_PROC_IS_HNP || ORTE_PROC_IS_MASTER) {
            jdata = OBJ_NEW(orte_job_t);
            rc = orte_plm_base_create_jobid(jdata);
            if (ORTE_SUCCESS != rc) {
                OBJ_RELEASE(jdata);
                if (NULL != cd->toolcbfunc) {
                    cd->toolcbfunc(ORTE_ERROR, tool, cd->cbdata);
                }
                OBJ_RELEASE(cd);
                if (NULL != hostname) {
                    free(hostname);
                }
                return;
            }
            tool.jobid = jdata->jobid;
            tool.vpid = 0;
        } else {
            /* we currently do not support connections to non-HNP/master
             * daemons from tools that were not spawned by a daemon */
            if (NULL != cd->toolcbfunc) {
                cd->toolcbfunc(ORTE_ERR_NOT_SUPPORTED, tool, cd->cbdata);
            }
            OBJ_RELEASE(cd);
            if (NULL != hostname) {
                free(hostname);
            }
            return;
        }
    } else {
        jdata = OBJ_NEW(orte_job_t);
        jdata->jobid = tool.jobid;
    }

    opal_hash_table_set_value_uint32(orte_job_data, jdata->jobid, jdata);
    /* setup some required job-level fields in case this
     * tool calls spawn, or uses some other functions that
     * need them */
    /* must create a map for it (even though it has no
     * info in it) so that the job info will be picked
     * up in subsequent pidmaps or other daemons won't
     * know how to route
     */
    jdata->map = OBJ_NEW(orte_job_map_t);

    /* setup an app_context for the singleton */
    app = OBJ_NEW(orte_app_context_t);
    app->app = strdup("tool");
    app->num_procs = 1;
    opal_pointer_array_add(jdata->apps, app);
    jdata->num_apps = 1;

    /* setup a proc object for the singleton - since we
     * -must- be the HNP, and therefore we stored our
     * node on the global node pool, and since the singleton
     * -must- be on the same node as us, indicate that
     */
    proc = OBJ_NEW(orte_proc_t);
    proc->name.jobid = jdata->jobid;
    proc->name.vpid = tool.vpid;
    proc->parent = ORTE_PROC_MY_NAME->vpid;
    ORTE_FLAG_SET(proc, ORTE_PROC_FLAG_ALIVE);
    ORTE_FLAG_SET(proc, ORTE_PROC_FLAG_TOOL);
    proc->state = ORTE_PROC_STATE_RUNNING;
    /* set the trivial */
    proc->local_rank = 0;
    proc->node_rank = 0;
    proc->app_rank = 0;
    proc->app_idx = 0;
    if (NULL == hostname) {
        /* it is on my node */
        node = (orte_node_t*)opal_pointer_array_get_item(orte_node_pool, 0);
        ORTE_FLAG_SET(proc, ORTE_PROC_FLAG_LOCAL);
    } else {
        /* we need to locate it */
        node = NULL;
        for (i=0; i < orte_node_pool->size; i++) {
            if (NULL == (nptr = (orte_node_t*)opal_pointer_array_get_item(orte_node_pool, i))) {
                continue;
            }
            if (0 == strcmp(hostname, nptr->name)) {
                node = nptr;
                break;
            }
        }
        if (NULL == node) {
            /* not in our allocation - which is still okay */
            node = OBJ_NEW(orte_node_t);
            node->name = strdup(hostname);
            ORTE_FLAG_SET(node, ORTE_NODE_NON_USABLE);
            opal_pointer_array_add(orte_node_pool, node);
        }
        free(hostname);  // no longer needed
    }
    proc->node = node;
    OBJ_RETAIN(node);  /* keep accounting straight */
    opal_pointer_array_add(jdata->procs, proc);
    jdata->num_procs = 1;
    /* add the node to the job map */
    OBJ_RETAIN(node);
    opal_pointer_array_add(jdata->map->nodes, node);
    jdata->map->num_nodes++;
    /* and it obviously is on the node - note that
     * we do _not_ increment the #procs on the node
     * as the tool doesn't count against the slot
     * allocation */
    OBJ_RETAIN(proc);
    opal_pointer_array_add(node->procs, proc);
    /* if they indicated a preference for termination, set it */
    if (flag_given) {
        orte_set_attribute(&jdata->attributes, ORTE_JOB_SILENT_TERMINATION,
                           ORTE_ATTR_GLOBAL, &flag, OPAL_BOOL);
    } else {
        /* we default to silence */
        flag = true;
        orte_set_attribute(&jdata->attributes, ORTE_JOB_SILENT_TERMINATION,
                           ORTE_ATTR_GLOBAL, &flag, OPAL_BOOL);
    }

    if (NULL != cd->toolcbfunc) {
        cd->toolcbfunc(ORTE_ERR_NOT_SUPPORTED, tool, cd->cbdata);
    }
    OBJ_RELEASE(cd);
}

void pmix_tool_connected_fn(opal_list_t *info,
                            opal_pmix_tool_connection_cbfunc_t cbfunc,
                            void *cbdata)
{
    orte_pmix_server_op_caddy_t *cd;

    opal_output_verbose(2, orte_pmix_server_globals.output,
                        "%s TOOL CONNECTION REQUEST RECVD",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));

    /* need to threadshift this request */
    cd = OBJ_NEW(orte_pmix_server_op_caddy_t);
    cd->info = info;
    cd->toolcbfunc = cbfunc;
    cd->cbdata = cbdata;

    opal_event_set(orte_event_base, &(cd->ev), -1,
                   OPAL_EV_WRITE, _toolconn, cd);
    opal_event_set_priority(&(cd->ev), ORTE_MSG_PRI);
    ORTE_POST_OBJECT(cd);
    opal_event_active(&(cd->ev), OPAL_EV_WRITE, 1);

}

static void lgcbfn(int sd, short args, void *cbdata)
{
    orte_pmix_server_op_caddy_t *cd = (orte_pmix_server_op_caddy_t*)cbdata;

    if (NULL != cd->cbfunc) {
        cd->cbfunc(cd->status, cd->cbdata);
    }
    OBJ_RELEASE(cd);
}

void pmix_server_log_fn(opal_process_name_t *requestor,
                        opal_list_t *info,
                        opal_list_t *directives,
                        opal_pmix_op_cbfunc_t cbfunc,
                        void *cbdata)
{
    opal_value_t *val;
    opal_buffer_t *buf;
    int rc;

    opal_output_verbose(2, orte_pmix_server_globals.output,
                        "%s logging info",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));

    OPAL_LIST_FOREACH(val, info, opal_value_t) {
        if (NULL == val->key) {
            ORTE_ERROR_LOG(ORTE_ERR_BAD_PARAM);
            continue;
        }
        if (0 == strcmp(val->key, OPAL_PMIX_LOG_MSG)) {
            /* pull out the blob */
            if (OPAL_BYTE_OBJECT != val->type) {
                continue;
            }
            buf = OBJ_NEW(opal_buffer_t);
            opal_dss.load(buf, val->data.bo.bytes, val->data.bo.size);
            val->data.bo.bytes = NULL;
            if (ORTE_SUCCESS != (rc = orte_rml.send_buffer_nb(orte_mgmt_conduit,
                                                              ORTE_PROC_MY_HNP, buf,
                                                              ORTE_RML_TAG_SHOW_HELP,
                                                              orte_rml_send_callback, NULL))) {
                ORTE_ERROR_LOG(rc);
                OBJ_RELEASE(buf);
            }
        } else if (0 == strcmp(val->key, OPAL_PMIX_LOG_STDERR)) {
            if (ORTE_SUCCESS != (rc = orte_iof.output(requestor, ORTE_IOF_STDERR, val->data.string))) {
                ORTE_ERROR_LOG(rc);
            }
        } else if (0 == strcmp(val->key, OPAL_PMIX_LOG_STDOUT)) {
            if (ORTE_SUCCESS != (rc = orte_iof.output(requestor, ORTE_IOF_STDOUT, val->data.string))) {
                ORTE_ERROR_LOG(rc);
            }
        }
    }

    /* we cannot directly execute the callback here
     * as it would threadlock - so shift to somewhere
     * safe */
    rc = ORTE_SUCCESS;  // unused - silence compiler warning
    ORTE_PMIX_THREADSHIFT(requestor, NULL, rc,
                          NULL, NULL, lgcbfn,
                          cbfunc, cbdata);
}

int pmix_server_job_ctrl_fn(const opal_process_name_t *requestor,
                            opal_list_t *targets,
                            opal_list_t *info,
                            opal_pmix_info_cbfunc_t cbfunc,
                            void *cbdata)
{
    opal_value_t *val;
    int rc, n;
    orte_proc_t *proc;
    opal_pointer_array_t parray, *ptrarray;
    opal_namelist_t *nm;
    opal_buffer_t *cmd;
    orte_daemon_cmd_flag_t cmmnd = ORTE_DAEMON_HALT_VM_CMD;
    orte_grpcomm_signature_t *sig;

    opal_output_verbose(2, orte_pmix_server_globals.output,
                        "%s job control request from %s",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                        ORTE_NAME_PRINT(requestor));

    OPAL_LIST_FOREACH(val, info, opal_value_t) {
        if (NULL == val->key) {
            ORTE_ERROR_LOG(ORTE_ERR_BAD_PARAM);
            continue;
        }
        if (0 == strcmp(val->key, OPAL_PMIX_JOB_CTRL_KILL)) {
            /* convert the list of targets to a pointer array */
            if (0 == opal_list_get_size(targets)) {
                ptrarray = NULL;
            } else {
                OBJ_CONSTRUCT(&parray, opal_pointer_array_t);
                OPAL_LIST_FOREACH(nm, targets, opal_namelist_t) {
                    /* get the proc object for this proc */
                    if (NULL == (proc = orte_get_proc_object(&nm->name))) {
                        ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
                        continue;
                    }
                    OBJ_RETAIN(proc);
                    opal_pointer_array_add(&parray, proc);
                }
                ptrarray = &parray;
            }
            if (ORTE_SUCCESS != (rc = orte_plm.terminate_procs(ptrarray))) {
                ORTE_ERROR_LOG(rc);
            }
            if (NULL != ptrarray) {
                /* cleanup the array */
                for (n=0; n < parray.size; n++) {
                    if (NULL != (proc = (orte_proc_t*)opal_pointer_array_get_item(&parray, n))) {
                        OBJ_RELEASE(proc);
                    }
                }
                OBJ_DESTRUCT(&parray);
            }
            continue;
        } else if (0 == strcmp(val->key, OPAL_PMIX_JOB_CTRL_TERMINATE)) {
            if (0 == opal_list_get_size(targets)) {
                /* terminate the daemons and all running jobs */
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
            }
        }
    }

    return ORTE_OPERATION_SUCCEEDED;
}
