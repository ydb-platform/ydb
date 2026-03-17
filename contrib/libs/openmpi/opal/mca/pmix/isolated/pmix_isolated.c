/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2016-2017 Intel, Inc. All rights reserved.
 * Copyright (c) 2011-2015 Los Alamos National Security, LLC. All
 *                         rights reserved.
 * Copyright (c) 2016 Cisco Systems, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"
#include "opal/constants.h"
#include "opal/types.h"

#ifdef HAVE_STRING_H
#include <string.h>
#endif
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif

#include "opal/dss/dss.h"
#include "opal/mca/event/event.h"
#include "opal/mca/hwloc/base/base.h"
#include "opal/runtime/opal.h"
#include "opal/runtime/opal_progress_threads.h"
#include "opal/util/argv.h"
#include "opal/util/error.h"
#include "opal/util/output.h"
#include "opal/util/proc.h"
#include "opal/util/show_help.h"

#include "pmix_isolated.h"
#include "opal/mca/pmix/base/base.h"
#include "opal/mca/pmix/base/pmix_base_hash.h"


static int isolated_init(opal_list_t *ilist);
static int isolated_fini(void);
static int isolated_initialized(void);
static int isolated_abort(int flat, const char *msg,
                          opal_list_t *procs);
static int isolated_spawn(opal_list_t *jobinfo, opal_list_t *apps, opal_jobid_t *jobid);
static int isolated_spawn_nb(opal_list_t *jobinfo, opal_list_t *apps,
                             opal_pmix_spawn_cbfunc_t cbfunc,
                             void *cbdata);
static int isolated_job_connect(opal_list_t *procs);
static int isolated_job_disconnect(opal_list_t *procs);
static int isolated_job_disconnect_nb(opal_list_t *procs,
                                      opal_pmix_op_cbfunc_t cbfunc,
                                      void *cbdata);
static int isolated_resolve_peers(const char *nodename,
                                  opal_jobid_t jobid,
                                  opal_list_t *procs);
static int isolated_resolve_nodes(opal_jobid_t jobid, char **nodelist);
static int isolated_put(opal_pmix_scope_t scope, opal_value_t *kv);
static int isolated_fence(opal_list_t *procs, int collect_data);
static int isolated_fence_nb(opal_list_t *procs, int collect_data,
                             opal_pmix_op_cbfunc_t cbfunc, void *cbdata);
static int isolated_commit(void);
static int isolated_get(const opal_process_name_t *id,
                        const char *key, opal_list_t *info,
                        opal_value_t **kv);
static int isolated_get_nb(const opal_process_name_t *id, const char *key,
                           opal_list_t *info,
                           opal_pmix_value_cbfunc_t cbfunc, void *cbdata);
static int isolated_publish(opal_list_t *info);
static int isolated_publish_nb(opal_list_t *info,
                               opal_pmix_op_cbfunc_t cbfunc, void *cbdata);
static int isolated_lookup(opal_list_t *data, opal_list_t *info);
static int isolated_lookup_nb(char **keys, opal_list_t *info,
                              opal_pmix_lookup_cbfunc_t cbfunc, void *cbdata);
static int isolated_unpublish(char **keys, opal_list_t *info);
static int isolated_unpublish_nb(char **keys, opal_list_t *info,
                                 opal_pmix_op_cbfunc_t cbfunc, void *cbdata);
static const char *isolated_get_version(void);
static int isolated_store_local(const opal_process_name_t *proc,
                                opal_value_t *val);
static const char *isolated_get_nspace(opal_jobid_t jobid);
static void isolated_register_jobid(opal_jobid_t jobid, const char *nspace);

const opal_pmix_base_module_t opal_pmix_isolated_module = {
    .init = isolated_init,
    .finalize = isolated_fini,
    .initialized = isolated_initialized,
    .abort = isolated_abort,
    .commit = isolated_commit,
    .fence = isolated_fence,
    .fence_nb = isolated_fence_nb,
    .put = isolated_put,
    .get = isolated_get,
    .get_nb = isolated_get_nb,
    .publish = isolated_publish,
    .publish_nb = isolated_publish_nb,
    .lookup = isolated_lookup,
    .lookup_nb = isolated_lookup_nb,
    .unpublish = isolated_unpublish,
    .unpublish_nb = isolated_unpublish_nb,
    .spawn = isolated_spawn,
    .spawn_nb = isolated_spawn_nb,
    .connect = isolated_job_connect,
    .disconnect = isolated_job_disconnect,
    .disconnect_nb = isolated_job_disconnect_nb,
    .resolve_peers = isolated_resolve_peers,
    .resolve_nodes = isolated_resolve_nodes,
    .get_version = isolated_get_version,
    .register_evhandler = opal_pmix_base_register_handler,
    .deregister_evhandler = opal_pmix_base_deregister_handler,
    .notify_event = opal_pmix_base_notify_event,
    .store_local = isolated_store_local,
    .get_nspace = isolated_get_nspace,
    .register_jobid = isolated_register_jobid
};

static int isolated_init_count = 0;
static opal_process_name_t isolated_pname;

static int isolated_init(opal_list_t *ilist)
{
    int rc;
    opal_value_t kv;
    opal_process_name_t wildcard;

    OPAL_PMIX_ACQUIRE_THREAD(&opal_pmix_base.lock);
    ++isolated_init_count;
    if (1 < isolated_init_count) {
        OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);
        return OPAL_SUCCESS;
    }


    wildcard.jobid = 1;
    wildcard.vpid = OPAL_VPID_WILDCARD;

    /* store our name in the opal_proc_t so that
     * debug messages will make sense - an upper
     * layer will eventually overwrite it, but that
     * won't do any harm */
    isolated_pname.jobid = 1;
    isolated_pname.vpid = 0;
    opal_proc_set_name(&isolated_pname);
    opal_output_verbose(10, opal_pmix_base_framework.framework_output,
                        "%s pmix:isolated: assigned tmp name %d %d",
                        OPAL_NAME_PRINT(isolated_pname),isolated_pname.jobid,isolated_pname.vpid);

    // setup hash table
    opal_pmix_base_hash_init();

    /* save the job size */
    OBJ_CONSTRUCT(&kv, opal_value_t);
    kv.key = strdup(OPAL_PMIX_JOB_SIZE);
    kv.type = OPAL_UINT32;
    kv.data.uint32 = 1;
    if (OPAL_SUCCESS != (rc = opal_pmix_base_store(&OPAL_PROC_MY_NAME, &kv))) {
        OPAL_ERROR_LOG(rc);
        OBJ_DESTRUCT(&kv);
        goto err_exit;
    }
    OBJ_DESTRUCT(&kv);

    /* save the appnum */
    OBJ_CONSTRUCT(&kv, opal_value_t);
    kv.key = strdup(OPAL_PMIX_APPNUM);
    kv.type = OPAL_UINT32;
    kv.data.uint32 = 0;
    if (OPAL_SUCCESS != (rc = opal_pmix_base_store(&OPAL_PROC_MY_NAME, &kv))) {
        OPAL_ERROR_LOG(rc);
        OBJ_DESTRUCT(&kv);
        goto err_exit;
    }
    OBJ_DESTRUCT(&kv);

    OBJ_CONSTRUCT(&kv, opal_value_t);
    kv.key = strdup(OPAL_PMIX_UNIV_SIZE);
    kv.type = OPAL_UINT32;
    kv.data.uint32 = 1;
    if (OPAL_SUCCESS != (rc = opal_pmix_base_store(&OPAL_PROC_MY_NAME, &kv))) {
        OPAL_ERROR_LOG(rc);
        OBJ_DESTRUCT(&kv);
        goto err_exit;
    }
    OBJ_DESTRUCT(&kv);

    OBJ_CONSTRUCT(&kv, opal_value_t);
    kv.key = strdup(OPAL_PMIX_MAX_PROCS);
    kv.type = OPAL_UINT32;
    kv.data.uint32 = 1;
    if (OPAL_SUCCESS != (rc = opal_pmix_base_store(&wildcard, &kv))) {
        OPAL_ERROR_LOG(rc);
        OBJ_DESTRUCT(&kv);
        goto err_exit;
    }
    OBJ_DESTRUCT(&kv);

    OBJ_CONSTRUCT(&kv, opal_value_t);
    kv.key = strdup(OPAL_PMIX_JOBID);
    kv.type = OPAL_UINT32;
    kv.data.uint32 = 1;
    if (OPAL_SUCCESS != (rc = opal_pmix_base_store(&OPAL_PROC_MY_NAME, &kv))) {
        OPAL_ERROR_LOG(rc);
        OBJ_DESTRUCT(&kv);
        goto err_exit;
    }
    OBJ_DESTRUCT(&kv);

    /* save the local size */
    OBJ_CONSTRUCT(&kv, opal_value_t);
    kv.key = strdup(OPAL_PMIX_LOCAL_SIZE);
    kv.type = OPAL_UINT32;
    kv.data.uint32 = 1;
    if (OPAL_SUCCESS != (rc = opal_pmix_base_store(&OPAL_PROC_MY_NAME, &kv))) {
        OPAL_ERROR_LOG(rc);
        OBJ_DESTRUCT(&kv);
        goto err_exit;
    }
    OBJ_DESTRUCT(&kv);

    OBJ_CONSTRUCT(&kv, opal_value_t);
    kv.key = strdup(OPAL_PMIX_LOCAL_PEERS);
    kv.type = OPAL_STRING;
    kv.data.string = strdup("0");
    if (OPAL_SUCCESS != (rc = opal_pmix_base_store(&OPAL_PROC_MY_NAME, &kv))) {
        OPAL_ERROR_LOG(rc);
        OBJ_DESTRUCT(&kv);
        goto err_exit;
    }
    OBJ_DESTRUCT(&kv);

    /* save the local leader */
    OBJ_CONSTRUCT(&kv, opal_value_t);
    kv.key = strdup(OPAL_PMIX_LOCALLDR);
    kv.type = OPAL_UINT64;
    kv.data.uint64 = 0;
    if (OPAL_SUCCESS != (rc = opal_pmix_base_store(&OPAL_PROC_MY_NAME, &kv))) {
        OPAL_ERROR_LOG(rc);
        OBJ_DESTRUCT(&kv);
        goto err_exit;
    }

    /* save our local rank */
    OBJ_CONSTRUCT(&kv, opal_value_t);
    kv.key = strdup(OPAL_PMIX_LOCAL_RANK);
    kv.type = OPAL_UINT16;
    kv.data.uint16 = 0;
    if (OPAL_SUCCESS != (rc = opal_pmix_base_store(&OPAL_PROC_MY_NAME, &kv))) {
        OPAL_ERROR_LOG(rc);
        OBJ_DESTRUCT(&kv);
        goto err_exit;
    }

    /* and our node rank */
    OBJ_CONSTRUCT(&kv, opal_value_t);
    kv.key = strdup(OPAL_PMIX_NODE_RANK);
    kv.type = OPAL_UINT16;
    kv.data.uint16 = 0;
    if (OPAL_SUCCESS != (rc = opal_pmix_base_store(&OPAL_PROC_MY_NAME, &kv))) {
        OPAL_ERROR_LOG(rc);
        OBJ_DESTRUCT(&kv);
        goto err_exit;
    }
    OBJ_DESTRUCT(&kv);

    OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);
    return OPAL_SUCCESS;

  err_exit:
    OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);
    return rc;
}

static int isolated_fini(void)
{
    OPAL_PMIX_ACQUIRE_THREAD(&opal_pmix_base.lock);
    --opal_pmix_base.initialized;

    if (0 == isolated_init_count) {
        opal_pmix_base_hash_finalize();
    }

    OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);
    return OPAL_SUCCESS;
}

static int isolated_initialized(void)
{
    OPAL_PMIX_ACQUIRE_THREAD(&opal_pmix_base.lock);
    if (0 < isolated_init_count) {
        OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);
        return 1;
    }
    OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);
    return 0;
}

static int isolated_abort(int flag, const char *msg,
                          opal_list_t *procs)
{
    return OPAL_SUCCESS;
}

static int isolated_spawn(opal_list_t *jobinfo, opal_list_t *apps, opal_jobid_t *jobid)
{
    return OPAL_ERR_NOT_SUPPORTED;
}

static int isolated_spawn_nb(opal_list_t *jobinfo, opal_list_t *apps,
                             opal_pmix_spawn_cbfunc_t cbfunc,
                             void *cbdata)
{
    return OPAL_ERR_NOT_SUPPORTED;
}

static int isolated_job_connect(opal_list_t *procs)
{
    return OPAL_ERR_NOT_SUPPORTED;
}

static int isolated_job_disconnect(opal_list_t *procs)
{
    return OPAL_ERR_NOT_SUPPORTED;
}

static int isolated_job_disconnect_nb(opal_list_t *procs,
                                      opal_pmix_op_cbfunc_t cbfunc,
                                      void *cbdata)
{
    return OPAL_ERR_NOT_SUPPORTED;
}

static int isolated_resolve_peers(const char *nodename,
                                  opal_jobid_t jobid,
                                  opal_list_t *procs)
{
    return OPAL_ERR_NOT_IMPLEMENTED;
}

static int isolated_resolve_nodes(opal_jobid_t jobid, char **nodelist)
{
    return OPAL_ERR_NOT_IMPLEMENTED;
}

static int isolated_put(opal_pmix_scope_t scope,
                        opal_value_t *kv)
{
    int rc;

    opal_output_verbose(2, opal_pmix_base_framework.framework_output,
                        "%s pmix:isolated isolated_put key %s scope %d",
                         OPAL_NAME_PRINT(OPAL_PROC_MY_NAME), kv->key, scope);

    OPAL_PMIX_ACQUIRE_THREAD(&opal_pmix_base.lock);
    if (0 == isolated_init_count) {
        OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);
        return OPAL_ERROR;
    }
    OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);

    rc = opal_pmix_base_store(&isolated_pname, kv);

    return rc;
}

static int isolated_commit(void)
{
    opal_output_verbose(2, opal_pmix_base_framework.framework_output,
                        "%s pmix:isolated isolated commit",
                         OPAL_NAME_PRINT(OPAL_PROC_MY_NAME));

    return OPAL_SUCCESS;
}

static int isolated_fence(opal_list_t *procs, int collect_data)
{
    opal_output_verbose(2, opal_pmix_base_framework.framework_output,
                        "%s pmix:isolated isolated fence",
                         OPAL_NAME_PRINT(OPAL_PROC_MY_NAME));
    return OPAL_SUCCESS;
}

static int isolated_fence_nb(opal_list_t *procs, int collect_data,
                             opal_pmix_op_cbfunc_t cbfunc, void *cbdata)
{
    opal_output_verbose(2, opal_pmix_base_framework.framework_output,
                        "%s pmix:isolated isolated fence_nb",
                         OPAL_NAME_PRINT(OPAL_PROC_MY_NAME));
    if (NULL != cbfunc) {
        cbfunc(OPAL_SUCCESS, cbdata);
    }
    return OPAL_SUCCESS;
}

static int isolated_get(const opal_process_name_t *id,
                        const char *key, opal_list_t *info,
                        opal_value_t **kv)
{
    int rc;
    opal_list_t vals;

    opal_output_verbose(2, opal_pmix_base_framework.framework_output,
                        "%s pmix:isolated getting value for proc %s key %s",
                        OPAL_NAME_PRINT(OPAL_PROC_MY_NAME),
                        OPAL_NAME_PRINT(*id), key);

    OBJ_CONSTRUCT(&vals, opal_list_t);
    rc = opal_pmix_base_fetch(id, key, &vals);
    if (OPAL_SUCCESS == rc) {
        *kv = (opal_value_t*)opal_list_remove_first(&vals);
        return OPAL_SUCCESS;
    } else {
        opal_output_verbose(2, opal_pmix_base_framework.framework_output,
                            "%s pmix:isolated fetch from dstore failed: %d",
                            OPAL_NAME_PRINT(OPAL_PROC_MY_NAME), rc);
    }
    OPAL_LIST_DESTRUCT(&vals);

    return rc;
}
static int isolated_get_nb(const opal_process_name_t *id, const char *key,
                           opal_list_t *info, opal_pmix_value_cbfunc_t cbfunc, void *cbdata)
{
    opal_output_verbose(2, opal_pmix_base_framework.framework_output,
                        "%s pmix:isolated isolated get_nb",
                         OPAL_NAME_PRINT(OPAL_PROC_MY_NAME));
    return OPAL_ERR_NOT_IMPLEMENTED;
}

static int isolated_publish(opal_list_t *info)
{
    opal_output_verbose(2, opal_pmix_base_framework.framework_output,
                        "%s pmix:isolated isolated publish",
                         OPAL_NAME_PRINT(OPAL_PROC_MY_NAME));
    return OPAL_ERR_NOT_SUPPORTED;
}

static int isolated_publish_nb(opal_list_t *info,
                               opal_pmix_op_cbfunc_t cbfunc, void *cbdata)
{
    opal_output_verbose(2, opal_pmix_base_framework.framework_output,
                        "%s pmix:isolated isolated publish_nb",
                         OPAL_NAME_PRINT(OPAL_PROC_MY_NAME));
    return OPAL_ERR_NOT_SUPPORTED;
}

static int isolated_lookup(opal_list_t *data, opal_list_t *info)
{
    opal_output_verbose(2, opal_pmix_base_framework.framework_output,
                        "%s pmix:isolated isolated lookup",
                         OPAL_NAME_PRINT(OPAL_PROC_MY_NAME));
    return OPAL_ERR_NOT_SUPPORTED;
}

static int isolated_lookup_nb(char **keys, opal_list_t *info,
                              opal_pmix_lookup_cbfunc_t cbfunc, void *cbdata)
{
    opal_output_verbose(2, opal_pmix_base_framework.framework_output,
                        "%s pmix:isolated isolated lookup_nb",
                         OPAL_NAME_PRINT(OPAL_PROC_MY_NAME));
    return OPAL_ERR_NOT_SUPPORTED;
}

static int isolated_unpublish(char **keys, opal_list_t *info)
{
    opal_output_verbose(2, opal_pmix_base_framework.framework_output,
                        "%s pmix:isolated isolated unpublish",
                         OPAL_NAME_PRINT(OPAL_PROC_MY_NAME));
    return OPAL_ERR_NOT_SUPPORTED;
}

static int isolated_unpublish_nb(char **keys, opal_list_t *info,
                                 opal_pmix_op_cbfunc_t cbfunc, void *cbdata)
{
    opal_output_verbose(2, opal_pmix_base_framework.framework_output,
                        "%s pmix:isolated isolated unpublish_nb",
                         OPAL_NAME_PRINT(OPAL_PROC_MY_NAME));
    return OPAL_ERR_NOT_SUPPORTED;
}

static const char *isolated_get_version(void)
{
    return "N/A";
}

static int isolated_store_local(const opal_process_name_t *proc,
                                opal_value_t *val)
{
    opal_output_verbose(2, opal_pmix_base_framework.framework_output,
                        "%s pmix:isolated isolated store_local",
                         OPAL_NAME_PRINT(OPAL_PROC_MY_NAME));

    opal_pmix_base_store(proc, val);

    return OPAL_SUCCESS;
}

static const char *isolated_get_nspace(opal_jobid_t jobid)
{
    return "N/A";
}

static void isolated_register_jobid(opal_jobid_t jobid, const char *nspace)
{
    return;
}
