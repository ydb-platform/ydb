/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2014-2018 Intel, Inc. All rights reserved.
 * Copyright (c) 2014-2016 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2014      Artem Y. Polyakov <artpol84@gmail.com>.
 *                         All rights reserved.
 * Copyright (c) 2016-2018 Mellanox Technologies, Inc.
 *                         All rights reserved.
 * Copyright (c) 2016      IBM Corporation.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include <src/include/pmix_config.h>

#include <src/include/types.h>
#include <src/include/pmix_stdint.h>

#include <pmix.h>
#include <pmix_rename.h>

#include "src/include/pmix_globals.h"

#ifdef HAVE_STRING_H
#include <string.h>
#endif
#include <fcntl.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef HAVE_SYS_SOCKET_H
#include <sys/socket.h>
#endif
#ifdef HAVE_SYS_UN_H
#include <sys/un.h>
#endif
#ifdef HAVE_SYS_UIO_H
#include <sys/uio.h>
#endif
#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif

#if PMIX_HAVE_ZLIB
#include <zlib.h>
#endif
#include PMIX_EVENT_HEADER

#include "src/class/pmix_list.h"
#include "src/mca/bfrops/bfrops.h"
#include "src/threads/threads.h"
#include "src/util/argv.h"
#include "src/util/compress.h"
#include "src/util/error.h"
#include "src/util/hash.h"
#include "src/util/output.h"
#include "src/mca/gds/gds.h"
#include "src/mca/ptl/ptl.h"

#include "pmix_client_ops.h"

static pmix_buffer_t* _pack_get(char *nspace, pmix_rank_t rank,
                               const pmix_info_t info[], size_t ninfo,
                               pmix_cmd_t cmd);

static void _getnbfn(int sd, short args, void *cbdata);

static void _getnb_cbfunc(struct pmix_peer_t *pr,
                          pmix_ptl_hdr_t *hdr,
                          pmix_buffer_t *buf, void *cbdata);

static void _value_cbfunc(pmix_status_t status, pmix_value_t *kv, void *cbdata);

static pmix_status_t _getfn_fastpath(const pmix_proc_t *proc, const pmix_key_t key,
                                     const pmix_info_t info[], size_t ninfo,
                                     pmix_value_t **val);

static pmix_status_t process_values(pmix_value_t **v, pmix_cb_t *cb);


PMIX_EXPORT pmix_status_t PMIx_Get(const pmix_proc_t *proc,
                                   const pmix_key_t key,
                                   const pmix_info_t info[], size_t ninfo,
                                   pmix_value_t **val)
{
    pmix_cb_t *cb;
    pmix_status_t rc;

    PMIX_ACQUIRE_THREAD(&pmix_global_lock);

    if (pmix_globals.init_cntr <= 0) {
        PMIX_RELEASE_THREAD(&pmix_global_lock);
        return PMIX_ERR_INIT;
    }
    PMIX_RELEASE_THREAD(&pmix_global_lock);

    pmix_output_verbose(2, pmix_client_globals.get_output,
                        "pmix:client get for %s:%d key %s",
                        (NULL == proc) ? "NULL" : proc->nspace,
                        (NULL == proc) ? PMIX_RANK_UNDEF : proc->rank,
                        (NULL == key) ? "NULL" : key);

    /* try to get data directly, without threadshift */
    if (PMIX_SUCCESS == (rc = _getfn_fastpath(proc, key, info, ninfo, val))) {
        goto done;
    }

    /* create a callback object as we need to pass it to the
     * recv routine so we know which callback to use when
     * the return message is recvd */
    cb = PMIX_NEW(pmix_cb_t);
    if (PMIX_SUCCESS != (rc = PMIx_Get_nb(proc, key, info, ninfo, _value_cbfunc, cb))) {
        PMIX_RELEASE(cb);
        return rc;
    }

    /* wait for the data to return */
    PMIX_WAIT_THREAD(&cb->lock);
    rc = cb->status;
    if (NULL != val) {
        *val = cb->value;
        cb->value = NULL;
    }
    PMIX_RELEASE(cb);

  done:
    pmix_output_verbose(2, pmix_client_globals.get_output,
                        "pmix:client get completed");

    return rc;
}

PMIX_EXPORT pmix_status_t PMIx_Get_nb(const pmix_proc_t *proc, const pmix_key_t key,
                                      const pmix_info_t info[], size_t ninfo,
                                      pmix_value_cbfunc_t cbfunc, void *cbdata)
{
    pmix_cb_t *cb;
    int rank;
    char *nm;

    PMIX_ACQUIRE_THREAD(&pmix_global_lock);

    if (pmix_globals.init_cntr <= 0) {
        PMIX_RELEASE_THREAD(&pmix_global_lock);
        return PMIX_ERR_INIT;
    }
    PMIX_RELEASE_THREAD(&pmix_global_lock);

    /* if the proc is NULL, then the caller is assuming
     * that the key is universally unique within the caller's
     * own nspace. This most likely indicates that the code
     * was originally written for a legacy version of PMI.
     *
     * If the key is NULL, then the caller wants all
     * data from the specified proc. Again, this likely
     * indicates use of a legacy version of PMI.
     *
     * Either case is supported. However, we don't currently
     * support the case where -both- values are NULL */
    if (NULL == proc && NULL == key) {
        pmix_output_verbose(2, pmix_client_globals.get_output,
                            "pmix: get_nb value error - both proc and key are NULL");
        return PMIX_ERR_BAD_PARAM;
    }

    /* if the key is NULL, the rank cannot be WILDCARD as
     * we cannot return all info from every rank */
    if (NULL != proc && PMIX_RANK_WILDCARD == proc->rank && NULL == key) {
        pmix_output_verbose(2, pmix_client_globals.get_output,
                            "pmix: get_nb value error - WILDCARD rank and key is NULL");
        return PMIX_ERR_BAD_PARAM;
    }

    /* if the given proc param is NULL, or the nspace is
     * empty, then the caller is referencing our own nspace */
    if (NULL == proc || 0 == strlen(proc->nspace)) {
        nm = pmix_globals.myid.nspace;
    } else {
        nm = (char*)proc->nspace;
    }

    /* if the proc param is NULL, then we are seeking a key that
     * must be globally unique, so communicate this to the hash
     * functions with the UNDEF rank */
    if (NULL == proc) {
        rank = PMIX_RANK_UNDEF;
    } else {
        rank = proc->rank;
    }

    pmix_output_verbose(2, pmix_client_globals.get_output,
                        "pmix: get_nb value for proc %s:%u key %s",
                        nm, rank, (NULL == key) ? "NULL" : key);

    /* threadshift this request so we can access global structures */
    cb = PMIX_NEW(pmix_cb_t);
    cb->pname.nspace = strdup(nm);
    cb->pname.rank = rank;
    cb->key = (char*)key;
    cb->info = (pmix_info_t*)info;
    cb->ninfo = ninfo;
    cb->cbfunc.valuefn = cbfunc;
    cb->cbdata = cbdata;
    PMIX_THREADSHIFT(cb, _getnbfn);

    return PMIX_SUCCESS;
}

static void _value_cbfunc(pmix_status_t status, pmix_value_t *kv, void *cbdata)
{
    pmix_cb_t *cb = (pmix_cb_t*)cbdata;
    pmix_status_t rc;

    PMIX_ACQUIRE_OBJECT(cb);
    cb->status = status;
    if (PMIX_SUCCESS == status) {
        PMIX_BFROPS_COPY(rc, pmix_client_globals.myserver,
                         (void**)&cb->value, kv, PMIX_VALUE);
        if (PMIX_SUCCESS != rc) {
            PMIX_ERROR_LOG(rc);
        }
    }
    PMIX_POST_OBJECT(cb);
    PMIX_WAKEUP_THREAD(&cb->lock);
}

static pmix_buffer_t* _pack_get(char *nspace, pmix_rank_t rank,
                               const pmix_info_t info[], size_t ninfo,
                               pmix_cmd_t cmd)
{
    pmix_buffer_t *msg;
    pmix_status_t rc;

    /* nope - see if we can get it */
    msg = PMIX_NEW(pmix_buffer_t);
    /* pack the get cmd */
    PMIX_BFROPS_PACK(rc, pmix_client_globals.myserver,
                     msg, &cmd, 1, PMIX_COMMAND);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        PMIX_RELEASE(msg);
        return NULL;
    }
    /* pack the request information - we'll get the entire blob
     * for this proc, so we don't need to pass the key */
    PMIX_BFROPS_PACK(rc, pmix_client_globals.myserver,
                     msg, &nspace, 1, PMIX_STRING);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        PMIX_RELEASE(msg);
        return NULL;
    }
    PMIX_BFROPS_PACK(rc, pmix_client_globals.myserver,
                     msg, &rank, 1, PMIX_PROC_RANK);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        PMIX_RELEASE(msg);
        return NULL;
    }
    /* pack the number of info structs */
    PMIX_BFROPS_PACK(rc, pmix_client_globals.myserver,
                     msg, &ninfo, 1, PMIX_SIZE);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        PMIX_RELEASE(msg);
        return NULL;
    }
    if (0 < ninfo) {
        PMIX_BFROPS_PACK(rc, pmix_client_globals.myserver,
                         msg, info, ninfo, PMIX_INFO);
        if (PMIX_SUCCESS != rc) {
            PMIX_ERROR_LOG(rc);
            PMIX_RELEASE(msg);
            return NULL;
        }
    }
    return msg;
}


/* this callback is coming from the ptl recv, and thus
 * is occurring inside of our progress thread - hence, no
 * need to thread shift */
static void _getnb_cbfunc(struct pmix_peer_t *pr,
                          pmix_ptl_hdr_t *hdr,
                          pmix_buffer_t *buf, void *cbdata)
{
    pmix_cb_t *cb = (pmix_cb_t*)cbdata;
    pmix_cb_t *cb2;
    pmix_status_t rc, ret;
    pmix_value_t *val = NULL;
    int32_t cnt;
    pmix_proc_t proc;
    pmix_kval_t *kv;

    pmix_output_verbose(2, pmix_client_globals.get_output,
                        "pmix: get_nb callback recvd");

    if (NULL == cb) {
        /* nothing we can do */
        PMIX_ERROR_LOG(PMIX_ERR_BAD_PARAM);
        return;
    }

    /* cache the proc id */
    pmix_strncpy(proc.nspace, cb->pname.nspace, PMIX_MAX_NSLEN);
    proc.rank = cb->pname.rank;

    /* a zero-byte buffer indicates that this recv is being
     * completed due to a lost connection */
    if (PMIX_BUFFER_IS_EMPTY(buf)) {
        ret = PMIX_ERR_UNREACH;
        goto done;
    }

    /* unpack the status */
    cnt = 1;
    PMIX_BFROPS_UNPACK(rc, pmix_client_globals.myserver,
                       buf, &ret, &cnt, PMIX_STATUS);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        pmix_list_remove_item(&pmix_client_globals.pending_requests, &cb->super);
        PMIX_RELEASE(cb);
        return;
    }

    if (PMIX_SUCCESS != ret) {
        goto done;
    }
    PMIX_GDS_ACCEPT_KVS_RESP(rc, pmix_client_globals.myserver, buf);
    if (PMIX_SUCCESS != rc) {
        goto done;
    }

  done:
    /* now search any pending requests (including the one this was in
     * response to) to see if they can be met. Note that this function
     * will only be called if the user requested a specific key - we
     * don't support calls to "get" for a NULL key */
    PMIX_LIST_FOREACH_SAFE(cb, cb2, &pmix_client_globals.pending_requests, pmix_cb_t) {
        if (0 == strncmp(proc.nspace, cb->pname.nspace, PMIX_MAX_NSLEN) &&
            cb->pname.rank == proc.rank) {
           /* we have the data for this proc - see if we can find the key */
            cb->proc = &proc;
            cb->scope = PMIX_SCOPE_UNDEF;
            /* fetch the data from server peer module - since it is passing
             * it back to the user, we need a copy of it */
            cb->copy = true;
            PMIX_GDS_FETCH_KV(rc, pmix_client_globals.myserver, cb);
            if (PMIX_SUCCESS == rc) {
                if (1 != pmix_list_get_size(&cb->kvs)) {
                    rc = PMIX_ERR_INVALID_VAL;
                    val = NULL;
                } else {
                    kv = (pmix_kval_t*)pmix_list_remove_first(&cb->kvs);
                    val = kv->value;
                    kv->value = NULL; // protect the value
                    PMIX_RELEASE(kv);
                }
            }
            cb->cbfunc.valuefn(rc, val, cb->cbdata);
            pmix_list_remove_item(&pmix_client_globals.pending_requests, &cb->super);
            PMIX_RELEASE(cb);
        }
    }
}

static void timeout(int fd, short flags, void *cbdata)
{
    pmix_cb_t *cb = (pmix_cb_t*)cbdata;

    /* let them know that we timed out */
    cb->cbfunc.valuefn(PMIX_ERR_TIMEOUT, NULL, cb->cbdata);
    cb->timer_running = false;

    /* remove this request */
    pmix_list_remove_item(&pmix_client_globals.pending_requests, &cb->super);
    PMIX_RELEASE(cb);
}

static pmix_status_t process_values(pmix_value_t **v, pmix_cb_t *cb)
{
    pmix_list_t *kvs = &cb->kvs;
    pmix_kval_t *kv;
    pmix_value_t *val;
    pmix_info_t *info;
    size_t ninfo, n;

    if (NULL != cb->key && 1 == pmix_list_get_size(kvs)) {
        kv = (pmix_kval_t*)pmix_list_get_first(kvs);
        *v = kv->value;
        kv->value = NULL;  // protect the value
        return PMIX_SUCCESS;
    }
    /* we will return the data as an array of pmix_info_t
     * in the kvs pmix_value_t */
    val = (pmix_value_t*)malloc(sizeof(pmix_value_t));
    if (NULL == val) {
        return PMIX_ERR_NOMEM;
    }
    val->type = PMIX_DATA_ARRAY;
    val->data.darray = (pmix_data_array_t*)malloc(sizeof(pmix_data_array_t));
    if (NULL == val->data.darray) {
        PMIX_VALUE_RELEASE(val);
        return PMIX_ERR_NOMEM;
    }
    val->data.darray->type = PMIX_INFO;
    val->data.darray->size = 0;
    val->data.darray->array = NULL;
    ninfo = pmix_list_get_size(kvs);
    PMIX_INFO_CREATE(info, ninfo);
    if (NULL == info) {
        PMIX_VALUE_RELEASE(val);
        return PMIX_ERR_NOMEM;
    }
    /* copy the list elements */
    n=0;
    PMIX_LIST_FOREACH(kv, kvs, pmix_kval_t) {
        pmix_strncpy(info[n].key, kv->key, PMIX_MAX_KEYLEN);
        pmix_value_xfer(&info[n].value, kv->value);
        ++n;
    }
    val->data.darray->size = ninfo;
    val->data.darray->array = info;
    *v = val;
    return PMIX_SUCCESS;
}

static void infocb(pmix_status_t status,
                   pmix_info_t *info, size_t ninfo,
                   void *cbdata,
                   pmix_release_cbfunc_t release_fn,
                   void *release_cbdata)
{
    pmix_query_caddy_t *cd = (pmix_query_caddy_t*)cbdata;
    pmix_value_t *kv = NULL;
    pmix_status_t rc;

    if (PMIX_SUCCESS == status) {
        if (NULL != info) {
            /* there should be only one returned value */
            if (1 != ninfo) {
                rc = PMIX_ERR_INVALID_VAL;
            } else {
                PMIX_VALUE_CREATE(kv, 1);
                if (NULL == kv) {
                    rc = PMIX_ERR_NOMEM;
                } else {
                    /* if this is a compressed string, then uncompress it */
                    if (PMIX_COMPRESSED_STRING == info[0].value.type) {
                        kv->type = PMIX_STRING;
                        pmix_util_uncompress_string(&kv->data.string, (uint8_t*)info[0].value.data.bo.bytes, info[0].value.data.bo.size);
                        if (NULL == kv->data.string) {
                            PMIX_ERROR_LOG(PMIX_ERR_NOMEM);
                            rc = PMIX_ERR_NOMEM;
                            PMIX_VALUE_FREE(kv, 1);
                            kv = NULL;
                        } else {
                            rc = PMIX_SUCCESS;
                        }
                    } else {
                        rc = pmix_value_xfer(kv, &info[0].value);
                    }
                }
            }
        } else {
            rc = PMIX_ERR_NOT_FOUND;
        }
    } else {
        rc = status;
    }
    if (NULL != cd->valcbfunc) {
        cd->valcbfunc(rc, kv, cd->cbdata);
    }
    PMIX_RELEASE(cd);
    if (NULL != kv) {
        PMIX_VALUE_FREE(kv, 1);
    }
    if (NULL != release_fn) {
        release_fn(release_cbdata);
    }
}

static pmix_status_t _getfn_fastpath(const pmix_proc_t *proc, const pmix_key_t key,
                                     const pmix_info_t info[], size_t ninfo,
                                     pmix_value_t **val)
{
    pmix_cb_t *cb = PMIX_NEW(pmix_cb_t);
    pmix_status_t rc = PMIX_SUCCESS;
    size_t n;

    /* scan the incoming directives */
    if (NULL != info) {
        for (n=0; n < ninfo; n++) {
            if (0 == strncmp(info[n].key, PMIX_DATA_SCOPE, PMIX_MAX_KEYLEN)) {
                cb->scope = info[n].value.data.scope;
                break;
            }
        }
    }
    cb->proc = (pmix_proc_t*)proc;
    cb->copy = true;
    cb->key = (char*)key;
    cb->info = (pmix_info_t*)info;
    cb->ninfo = ninfo;

    PMIX_GDS_FETCH_IS_TSAFE(rc, pmix_globals.mypeer);
    if (PMIX_SUCCESS == rc) {
        PMIX_GDS_FETCH_KV(rc, pmix_globals.mypeer, cb);
        if (PMIX_SUCCESS == rc) {
            goto done;
        }
    }
    PMIX_GDS_FETCH_IS_TSAFE(rc, pmix_client_globals.myserver);
    if (PMIX_SUCCESS == rc) {
        PMIX_GDS_FETCH_KV(rc, pmix_client_globals.myserver, cb);
        if (PMIX_SUCCESS == rc) {
            goto done;
        }
    }
    PMIX_RELEASE(cb);
    return rc;

  done:
    rc = process_values(val, cb);
    if (NULL != *val) {
        PMIX_VALUE_COMPRESSED_STRING_UNPACK(*val);
    }
    PMIX_RELEASE(cb);
    return rc;
}

static void _getnbfn(int fd, short flags, void *cbdata)
{
    pmix_cb_t *cb = (pmix_cb_t*)cbdata;
    pmix_cb_t *cbret;
    pmix_buffer_t *msg;
    pmix_value_t *val = NULL;
    pmix_status_t rc;
    size_t n;
    pmix_proc_t proc;
    bool optional = false;
    bool immediate = false;
    struct timeval tv;
    pmix_query_caddy_t *cd;

    /* cb was passed to us from another thread - acquire it */
    PMIX_ACQUIRE_OBJECT(cb);

    pmix_output_verbose(2, pmix_client_globals.get_output,
                        "pmix: getnbfn value for proc %s:%u key %s",
                        cb->pname.nspace, cb->pname.rank,
                        (NULL == cb->key) ? "NULL" : cb->key);

    /* set the proc object identifier */
    pmix_strncpy(proc.nspace, cb->pname.nspace, PMIX_MAX_NSLEN);
    proc.rank = cb->pname.rank;

    /* scan the incoming directives */
    if (NULL != cb->info) {
        for (n=0; n < cb->ninfo; n++) {
            if (0 == strncmp(cb->info[n].key, PMIX_OPTIONAL, PMIX_MAX_KEYLEN)) {
                optional = PMIX_INFO_TRUE(&cb->info[n]);
            } else if (0 == strncmp(cb->info[n].key, PMIX_IMMEDIATE, PMIX_MAX_KEYLEN)) {
                immediate = PMIX_INFO_TRUE(&cb->info[n]);
            } else if (0 == strncmp(cb->info[n].key, PMIX_TIMEOUT, PMIX_MAX_KEYLEN)) {
                /* set a timer to kick us out if we don't
                 * have an answer within their window */
                if (0 < cb->info[n].value.data.integer) {
                    tv.tv_sec = cb->info[n].value.data.integer;
                    tv.tv_usec = 0;
                    pmix_event_evtimer_set(pmix_globals.evbase, &cb->ev,
                                           timeout, cb);
                    pmix_event_evtimer_add(&cb->ev, &tv);
                    cb->timer_running = true;
                }
            } else if (0 == strncmp(cb->info[n].key, PMIX_DATA_SCOPE, PMIX_MAX_KEYLEN)) {
                cb->scope = cb->info[n].value.data.scope;
            }
        }
    }

    /* check the internal storage first */
    cb->proc = &proc;
    cb->copy = true;
    PMIX_GDS_FETCH_KV(rc, pmix_globals.mypeer, cb);
    if (PMIX_SUCCESS == rc) {
        pmix_output_verbose(5, pmix_client_globals.get_output,
                            "pmix:client data found in internal storage");
        rc = process_values(&val, cb);
        goto respond;
    }
    pmix_output_verbose(5, pmix_client_globals.get_output,
                        "pmix:client data NOT found in internal storage");

    /* if the key is NULL or starts with "pmix", then they are looking
     * for data that was provided by the server at startup */
    if (NULL == cb->key || 0 == strncmp(cb->key, "pmix", 4)) {
        cb->proc = &proc;
        /* fetch the data from my server's module - since we are passing
         * it back to the user, we need a copy of it */
        cb->copy = true;
        PMIX_GDS_FETCH_KV(rc, pmix_client_globals.myserver, cb);
        if (PMIX_SUCCESS != rc) {
            pmix_output_verbose(5, pmix_client_globals.get_output,
                                "pmix:client job-level data NOT found");
            if (0 != strncmp(cb->pname.nspace, pmix_globals.myid.nspace, PMIX_MAX_NSLEN)) {
                /* we are asking about the job-level info from another
                 * namespace. It seems that we don't have it - go and
                 * ask server
                 */
                goto request;
            } else if (NULL != cb->key) {
                /* if immediate was given, then we are being directed to
                 * check with the server even though the caller is looking for
                 * job-level info. In some cases, a server may elect not
                 * to provide info at init to save memory */
                if (immediate) {
                    pmix_output_verbose(5, pmix_client_globals.get_output,
                                        "pmix:client IMMEDIATE given - querying data");
                    /* the direct modex request doesn't pass a key as it
                     * was intended to support non-job-level information.
                     * So instead, we will use the PMIx_Query function
                     * to request the information */
                    cd = PMIX_NEW(pmix_query_caddy_t);
                    cd->cbdata = cb->cbdata;
                    cd->valcbfunc = cb->cbfunc.valuefn;
                    PMIX_QUERY_CREATE(cd->queries, 1);
                    cd->nqueries = 1;
                    pmix_argv_append_nosize(&cd->queries[0].keys, cb->key);
                    if (PMIX_SUCCESS != (rc = PMIx_Query_info_nb(cd->queries, 1, infocb, cd))) {
                        PMIX_RELEASE(cd);
                        goto respond;
                    }
                    PMIX_RELEASE(cb);
                    return;
                }
                /* we should have had this info, so respond with the error */
                pmix_output_verbose(5, pmix_client_globals.get_output,
                                    "pmix:client returning NOT FOUND error");
                goto respond;
            } else {
                pmix_output_verbose(5, pmix_client_globals.get_output,
                                    "pmix:client NULL KEY - returning error");
                goto respond;
            }
        }
        pmix_output_verbose(5, pmix_client_globals.get_output,
                            "pmix:client job-level data NOT found");
        rc = process_values(&val, cb);
        goto respond;
    } else {
        cb->proc = &proc;
        cb->copy = true;
        PMIX_GDS_FETCH_KV(rc, pmix_client_globals.myserver, cb);
        if (PMIX_SUCCESS != rc) {
            val = NULL;
            goto request;
        }
        /* return whatever we found */
        rc = process_values(&val, cb);
    }

  respond:
    /* if a callback was provided, execute it */
    if (NULL != cb->cbfunc.valuefn) {
        if (NULL != val)  {
            PMIX_VALUE_COMPRESSED_STRING_UNPACK(val);
        }
        cb->cbfunc.valuefn(rc, val, cb->cbdata);
    }
    if (NULL != val) {
        PMIX_VALUE_RELEASE(val);
    }
    PMIX_RELEASE(cb);
    return;

  request:
    /* if we got here, then we don't have the data for this proc. If we
     * are a server, or we are a client and not connected, then there is
     * nothing more we can do */
    if (PMIX_PROC_IS_SERVER(pmix_globals.mypeer) ||
        (!PMIX_PROC_IS_SERVER(pmix_globals.mypeer) && !pmix_globals.connected)) {
        rc = PMIX_ERR_NOT_FOUND;
        goto respond;
    }

    /* we also have to check the user's directives to see if they do not want
     * us to attempt to retrieve it from the server */
    if (optional) {
        /* they don't want us to try and retrieve it */
        pmix_output_verbose(2, pmix_client_globals.get_output,
                            "PMIx_Get key=%s for rank = %u, namespace = %s was not found - request was optional",
                            cb->key, cb->pname.rank, cb->pname.nspace);
        rc = PMIX_ERR_NOT_FOUND;
        goto respond;
    }

    /* see if we already have a request in place with the server for data from
     * this nspace:rank. If we do, then no need to ask again as the
     * request will return _all_ data from that proc */
    PMIX_LIST_FOREACH(cbret, &pmix_client_globals.pending_requests, pmix_cb_t) {
        if (0 == strncmp(cbret->pname.nspace, cb->pname.nspace, PMIX_MAX_NSLEN) &&
            cbret->pname.rank == cb->pname.rank) {
            /* we do have a pending request, but we still need to track this
             * outstanding request so we can satisfy it once the data is returned */
            pmix_list_append(&pmix_client_globals.pending_requests, &cb->super);
            return;
        }
    }

    /* we don't have a pending request, so let's create one - don't worry
     * about packing the key as we return everything from that proc */
    msg = _pack_get(cb->pname.nspace, cb->pname.rank, cb->info, cb->ninfo, PMIX_GETNB_CMD);
    if (NULL == msg) {
        rc = PMIX_ERROR;
        goto respond;
    }

    pmix_output_verbose(2, pmix_client_globals.get_output,
                        "%s:%d REQUESTING DATA FROM SERVER FOR %s:%d KEY %s",
                        pmix_globals.myid.nspace, pmix_globals.myid.rank,
                        cb->pname.nspace, cb->pname.rank, cb->key);

    /* track the callback object */
    pmix_list_append(&pmix_client_globals.pending_requests, &cb->super);
    /* send to the server */
    PMIX_PTL_SEND_RECV(rc, pmix_client_globals.myserver, msg, _getnb_cbfunc, (void*)cb);
    if (PMIX_SUCCESS != rc) {
        pmix_list_remove_item(&pmix_client_globals.pending_requests, &cb->super);
        rc = PMIX_ERROR;
        goto respond;
    }
    /* we made a lot of changes to cb, so ensure they get
     * written out before we return */
    PMIX_POST_OBJECT(cb);
    return;
}
