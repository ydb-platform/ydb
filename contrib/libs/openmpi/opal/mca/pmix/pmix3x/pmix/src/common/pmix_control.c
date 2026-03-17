/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2014-2018 Intel, Inc. All rights reserved.
 * Copyright (c) 2016      Mellanox Technologies, Inc.
 *                         All rights reserved.
 * Copyright (c) 2016      IBM Corporation.  All rights reserved.
 * Copyright (c) 2019      Research Organization for Information Science
 *                         and Technology (RIST).  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
#include <src/include/pmix_config.h>

#include <src/include/types.h>
#include <src/include/pmix_stdint.h>
#include <src/include/pmix_socket_errno.h>

#include <pmix.h>
#include <pmix_common.h>
#include <pmix_server.h>
#include <pmix_rename.h>

#include "src/threads/threads.h"
#include "src/util/argv.h"
#include "src/util/error.h"
#include "src/util/name_fns.h"
#include "src/util/output.h"
#include "src/mca/bfrops/bfrops.h"
#include "src/mca/ptl/ptl.h"

#include "src/client/pmix_client_ops.h"
#include "src/server/pmix_server_ops.h"
#include "src/include/pmix_globals.h"

static void relcbfunc(void *cbdata)
{
    pmix_shift_caddy_t *cd = (pmix_shift_caddy_t*)cbdata;

    pmix_output_verbose(2, pmix_globals.debug_output,
                        "pmix:job_ctrl release callback");

    if (NULL != cd->info) {
        PMIX_INFO_FREE(cd->info, cd->ninfo);
    }
    PMIX_RELEASE(cd);
}
static void query_cbfunc(struct pmix_peer_t *peer,
                         pmix_ptl_hdr_t *hdr,
                         pmix_buffer_t *buf, void *cbdata)
{
    pmix_query_caddy_t *cd = (pmix_query_caddy_t*)cbdata;
    pmix_status_t rc;
    pmix_shift_caddy_t *results;
    int cnt;

    pmix_output_verbose(2, pmix_globals.debug_output,
                        "pmix:job_ctrl cback from server with %d bytes",
                        (int)buf->bytes_used);

    /* a zero-byte buffer indicates that this recv is being
     * completed due to a lost connection */
    if (PMIX_BUFFER_IS_EMPTY(buf)) {
        /* release the caller */
        if (NULL != cd->cbfunc) {
            cd->cbfunc(PMIX_ERR_COMM_FAILURE, NULL, 0, cd->cbdata, NULL, NULL);
        }
        PMIX_RELEASE(cd);
        return;
    }

    results = PMIX_NEW(pmix_shift_caddy_t);

    /* unpack the status */
    cnt = 1;
    PMIX_BFROPS_UNPACK(rc, peer, buf, &results->status, &cnt, PMIX_STATUS);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        goto complete;
    }
    if (PMIX_SUCCESS != results->status) {
        goto complete;
    }

    /* unpack any returned data */
    cnt = 1;
    PMIX_BFROPS_UNPACK(rc, peer, buf, &results->ninfo, &cnt, PMIX_SIZE);
    if (PMIX_SUCCESS != rc && PMIX_ERR_UNPACK_READ_PAST_END_OF_BUFFER != rc) {
        PMIX_ERROR_LOG(rc);
        goto complete;
    }
    if (0 < results->ninfo) {
        PMIX_INFO_CREATE(results->info, results->ninfo);
        cnt = results->ninfo;
        PMIX_BFROPS_UNPACK(rc, peer, buf, results->info, &cnt, PMIX_INFO);
        if (PMIX_SUCCESS != rc) {
            PMIX_ERROR_LOG(rc);
            goto complete;
        }
    }

  complete:
    pmix_output_verbose(2, pmix_globals.debug_output,
                        "pmix:job_ctrl cback from server releasing");
    /* release the caller */
    if (NULL != cd->cbfunc) {
        cd->cbfunc(results->status, results->info, results->ninfo, cd->cbdata, relcbfunc, results);
    } else {
        PMIX_RELEASE(results);
    }
    PMIX_RELEASE(cd);
}

static void acb(pmix_status_t status,
                pmix_info_t *info, size_t ninfo,
                void *cbdata,
                pmix_release_cbfunc_t release_fn,
                void *release_cbdata)
{
    pmix_cb_t *cb = (pmix_cb_t*)cbdata;
    cb->status = status;
    if (NULL != release_fn) {
        release_fn(release_cbdata);
    }
    PMIX_WAKEUP_THREAD(&cb->lock);
}

PMIX_EXPORT pmix_status_t PMIx_Job_control(const pmix_proc_t targets[], size_t ntargets,
                                           const pmix_info_t directives[], size_t ndirs)
{
    pmix_cb_t cb;
    pmix_status_t rc;

    PMIX_ACQUIRE_THREAD(&pmix_global_lock);

    if (pmix_globals.init_cntr <= 0) {
        PMIX_RELEASE_THREAD(&pmix_global_lock);
        return PMIX_ERR_INIT;
    }
    PMIX_RELEASE_THREAD(&pmix_global_lock);

    pmix_output_verbose(2, pmix_globals.debug_output,
                        "%s pmix:job_ctrl", PMIX_NAME_PRINT(&pmix_globals.myid));

    /* create a callback object as we need to pass it to the
     * recv routine so we know which callback to use when
     * the return message is recvd */
    PMIX_CONSTRUCT(&cb, pmix_cb_t);
    if (PMIX_SUCCESS != (rc = PMIx_Job_control_nb(targets, ntargets,
                                                  directives, ndirs,
                                                  acb, &cb))) {
        PMIX_DESTRUCT(&cb);
        return rc;
    }

    /* wait for the operation to complete */
    PMIX_WAIT_THREAD(&cb.lock);
    rc = cb.status;
    PMIX_DESTRUCT(&cb);

    pmix_output_verbose(2, pmix_globals.debug_output,
                        "pmix:job_ctrl completed");

    return rc;
}

PMIX_EXPORT pmix_status_t PMIx_Job_control_nb(const pmix_proc_t targets[], size_t ntargets,
                                              const pmix_info_t directives[], size_t ndirs,
                                              pmix_info_cbfunc_t cbfunc, void *cbdata)
{
    pmix_buffer_t *msg;
    pmix_cmd_t cmd = PMIX_JOB_CONTROL_CMD;
    pmix_status_t rc;
    pmix_query_caddy_t *cb;

    PMIX_ACQUIRE_THREAD(&pmix_global_lock);

    pmix_output_verbose(2, pmix_globals.debug_output,
                        "pmix: job control called with %d directives", (int)ndirs);

    if (pmix_globals.init_cntr <= 0) {
        PMIX_RELEASE_THREAD(&pmix_global_lock);
        return PMIX_ERR_INIT;
    }

    /* if we are the server, then we just issue the request and
     * return the response */
    if (PMIX_PROC_IS_SERVER(pmix_globals.mypeer) &&
        !PMIX_PROC_IS_LAUNCHER(pmix_globals.mypeer)) {
        PMIX_RELEASE_THREAD(&pmix_global_lock);
        if (NULL == pmix_host_server.job_control) {
            /* nothing we can do */
            return PMIX_ERR_NOT_SUPPORTED;
        }
        pmix_output_verbose(2, pmix_globals.debug_output,
                            "pmix:job_control handed to RM");
        rc = pmix_host_server.job_control(&pmix_globals.myid,
                                          targets, ntargets,
                                          directives, ndirs,
                                          cbfunc, cbdata);
        return rc;
    }

    /* we need to send, so check for connection */
    if (!pmix_globals.connected) {
        PMIX_RELEASE_THREAD(&pmix_global_lock);
        return PMIX_ERR_UNREACH;
    }
    PMIX_RELEASE_THREAD(&pmix_global_lock);

    /* if we are a client, then relay this request to the server */
    msg = PMIX_NEW(pmix_buffer_t);
    /* pack the cmd */
    PMIX_BFROPS_PACK(rc, pmix_client_globals.myserver,
                     msg, &cmd, 1, PMIX_COMMAND);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        PMIX_RELEASE(msg);
        return rc;
    }

    /* pack the number of targets */
    PMIX_BFROPS_PACK(rc, pmix_client_globals.myserver,
                     msg, &ntargets, 1, PMIX_SIZE);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        PMIX_RELEASE(msg);
        return rc;
    }
    /* remember, the targets can be NULL to indicate that the operation
     * is to be done against all members of our nspace */
    if (NULL != targets && 0 < ntargets) {
        /* pack the targets */
        PMIX_BFROPS_PACK(rc, pmix_client_globals.myserver,
                         msg, targets, ntargets, PMIX_PROC);
        if (PMIX_SUCCESS != rc) {
            PMIX_ERROR_LOG(rc);
            PMIX_RELEASE(msg);
            return rc;
        }
    }

    /* pack the directives */
    PMIX_BFROPS_PACK(rc, pmix_client_globals.myserver,
                     msg, &ndirs, 1, PMIX_SIZE);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        PMIX_RELEASE(msg);
        return rc;
    }
    if (NULL != directives && 0 < ndirs) {
        PMIX_BFROPS_PACK(rc, pmix_client_globals.myserver,
                         msg, directives, ndirs, PMIX_INFO);
        if (PMIX_SUCCESS != rc) {
            PMIX_ERROR_LOG(rc);
            PMIX_RELEASE(msg);
            return rc;
        }
    }

    /* create a callback object as we need to pass it to the
     * recv routine so we know which callback to use when
     * the return message is recvd */
    cb = PMIX_NEW(pmix_query_caddy_t);
    cb->cbfunc = cbfunc;
    cb->cbdata = cbdata;

    /* push the message into our event base to send to the server */
    PMIX_PTL_SEND_RECV(rc, pmix_client_globals.myserver,
                       msg, query_cbfunc, (void*)cb);
    if (PMIX_SUCCESS != rc) {
        PMIX_RELEASE(msg);
        PMIX_RELEASE(cb);
    }

    return rc;
}

PMIX_EXPORT pmix_status_t PMIx_Process_monitor(const pmix_info_t *monitor, pmix_status_t error,
                                               const pmix_info_t directives[], size_t ndirs)
{
    pmix_cb_t cb;
    pmix_status_t rc;

    PMIX_ACQUIRE_THREAD(&pmix_global_lock);

    if (pmix_globals.init_cntr <= 0) {
        PMIX_RELEASE_THREAD(&pmix_global_lock);
        return PMIX_ERR_INIT;
    }
    PMIX_RELEASE_THREAD(&pmix_global_lock);

    pmix_output_verbose(2, pmix_globals.debug_output,
                        "%s pmix:monitor", PMIX_NAME_PRINT(&pmix_globals.myid));

    /* create a callback object as we need to pass it to the
     * recv routine so we know which callback to use when
     * the return message is recvd */
    PMIX_CONSTRUCT(&cb, pmix_cb_t);
    if (PMIX_SUCCESS != (rc = PMIx_Process_monitor_nb(monitor, error,
                                                      directives, ndirs,
                                                      acb, &cb))) {
        PMIX_DESTRUCT(&cb);
        return rc;
    }

    /* wait for the operation to complete */
    PMIX_WAIT_THREAD(&cb.lock);
    rc = cb.status;
    PMIX_DESTRUCT(&cb);

    pmix_output_verbose(2, pmix_globals.debug_output,
                        "pmix:monitor completed");

    return rc;
}

PMIX_EXPORT pmix_status_t PMIx_Process_monitor_nb(const pmix_info_t *monitor, pmix_status_t error,
                                                  const pmix_info_t directives[], size_t ndirs,
                                                  pmix_info_cbfunc_t cbfunc, void *cbdata)
{
    pmix_buffer_t *msg;
    pmix_cmd_t cmd = PMIX_MONITOR_CMD;
    pmix_status_t rc;
    pmix_query_caddy_t *cb;

    PMIX_ACQUIRE_THREAD(&pmix_global_lock);

    pmix_output_verbose(2, pmix_globals.debug_output,
                        "pmix: monitor called");

    if (pmix_globals.init_cntr <= 0) {
        PMIX_RELEASE_THREAD(&pmix_global_lock);
        return PMIX_ERR_INIT;
    }

    /* sanity check */
    if (NULL == monitor) {
        PMIX_RELEASE_THREAD(&pmix_global_lock);
        return PMIX_ERR_BAD_PARAM;
    }

    /* if we are the server, then we just issue the request and
     * return the response */
    if (PMIX_PROC_IS_SERVER(pmix_globals.mypeer) &&
        !PMIX_PROC_IS_LAUNCHER(pmix_globals.mypeer)) {
        PMIX_RELEASE_THREAD(&pmix_global_lock);
        if (NULL == pmix_host_server.monitor) {
            /* nothing we can do */
            return PMIX_ERR_NOT_SUPPORTED;
        }
        pmix_output_verbose(2, pmix_globals.debug_output,
                            "pmix:monitor handed to RM");
        rc = pmix_host_server.monitor(&pmix_globals.myid, monitor, error,
                                      directives, ndirs, cbfunc, cbdata);
        return rc;
    }

    /* we need to send, so check for connection */
    if (!pmix_globals.connected) {
        PMIX_RELEASE_THREAD(&pmix_global_lock);
        return PMIX_ERR_UNREACH;
    }
    PMIX_RELEASE_THREAD(&pmix_global_lock);

    /* if the monitor is PMIX_SEND_HEARTBEAT, then send it */
    if (0 == strncmp(monitor->key, PMIX_SEND_HEARTBEAT, PMIX_MAX_KEYLEN)) {
        msg = PMIX_NEW(pmix_buffer_t);
        if (NULL == msg) {
            return PMIX_ERR_NOMEM;
        }
        PMIX_PTL_SEND_ONEWAY(rc, pmix_client_globals.myserver, msg, PMIX_PTL_TAG_HEARTBEAT);
        if (PMIX_SUCCESS != rc) {
            PMIX_RELEASE(msg);
        }
        return rc;
    }

    /* if we are a client, then relay this request to the server */
    msg = PMIX_NEW(pmix_buffer_t);
    /* pack the cmd */
    PMIX_BFROPS_PACK(rc, pmix_client_globals.myserver,
                     msg, &cmd, 1, PMIX_COMMAND);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        PMIX_RELEASE(msg);
        return rc;
    }

    /* pack the monitor */
    PMIX_BFROPS_PACK(rc, pmix_client_globals.myserver,
                     msg, monitor, 1, PMIX_INFO);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        PMIX_RELEASE(msg);
        return rc;
    }

    /* pack the error */
    PMIX_BFROPS_PACK(rc, pmix_client_globals.myserver,
                     msg, &error, 1, PMIX_STATUS);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        PMIX_RELEASE(msg);
        return rc;
    }

    /* pack the directives */
    PMIX_BFROPS_PACK(rc, pmix_client_globals.myserver,
                     msg, &ndirs, 1, PMIX_SIZE);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        PMIX_RELEASE(msg);
        return rc;
    }
    if (0 < ndirs) {
        PMIX_BFROPS_PACK(rc, pmix_client_globals.myserver,
                         msg, directives, ndirs, PMIX_INFO);
        if (PMIX_SUCCESS != rc) {
            PMIX_ERROR_LOG(rc);
            PMIX_RELEASE(msg);
            return rc;
        }
    }

    /* create a callback object as we need to pass it to the
     * recv routine so we know which callback to use when
     * the return message is recvd */
    cb = PMIX_NEW(pmix_query_caddy_t);
    cb->cbfunc = cbfunc;
    cb->cbdata = cbdata;

    /* push the message into our event base to send to the server */
    PMIX_PTL_SEND_RECV(rc, pmix_client_globals.myserver,
                       msg, query_cbfunc, (void*)cb);
    if (PMIX_SUCCESS != rc) {
        PMIX_RELEASE(msg);
        PMIX_RELEASE(cb);
    }

    return rc;
}
