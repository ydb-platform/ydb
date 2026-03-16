/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2014-2018 Intel, Inc. All rights reserved.
 * Copyright (c) 2016      Mellanox Technologies, Inc.
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
#include "src/mca/plog/base/base.h"

#include "src/client/pmix_client_ops.h"
#include "src/server/pmix_server_ops.h"
#include "src/include/pmix_globals.h"

static void opcbfunc(pmix_status_t status, void *cbdata)
{
    pmix_cb_t *cb = (pmix_cb_t*)cbdata;
    cb->status = status;
    PMIX_WAKEUP_THREAD(&cb->lock);
}

static void log_cbfunc(struct pmix_peer_t *peer,
                       pmix_ptl_hdr_t *hdr,
                       pmix_buffer_t *buf, void *cbdata)
{
    pmix_shift_caddy_t *cd = (pmix_shift_caddy_t*)cbdata;
    int32_t m;
    pmix_status_t rc, status;

    /* unpack the return status */
    m=1;
    PMIX_BFROPS_UNPACK(rc, peer, buf, &status, &m, PMIX_STATUS);
    if (PMIX_SUCCESS != rc) {
        status = rc;
    }

    if (NULL != cd->cbfunc.opcbfn) {
        cd->cbfunc.opcbfn(status, cd->cbdata);
    }
    PMIX_RELEASE(cd);
}

PMIX_EXPORT pmix_status_t PMIx_Log(const pmix_info_t data[], size_t ndata,
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

    pmix_output_verbose(2, pmix_plog_base_framework.framework_output,
                        "%s pmix:log", PMIX_NAME_PRINT(&pmix_globals.myid));

    /* create a callback object as we need to pass it to the
     * recv routine so we know which callback to use when
     * the return message is recvd */
    PMIX_CONSTRUCT(&cb, pmix_cb_t);
    if (PMIX_SUCCESS != (rc = PMIx_Log_nb(data, ndata, directives,
                                          ndirs, opcbfunc, &cb))) {
        PMIX_DESTRUCT(&cb);
        return rc;
    }

    /* wait for the operation to complete */
    PMIX_WAIT_THREAD(&cb.lock);
    rc = cb.status;
    PMIX_DESTRUCT(&cb);

    pmix_output_verbose(2, pmix_plog_base_framework.framework_output,
                        "pmix:log completed");

    return rc;
}

static void localcbfunc(pmix_status_t status, void *cbdata)
{
    pmix_shift_caddy_t *cd = (pmix_shift_caddy_t*)cbdata;

    PMIX_INFO_FREE(cd->directives, cd->ndirs);
    if (NULL != cd->cbfunc.opcbfn) {
        cd->cbfunc.opcbfn(status, cd->cbdata);
    }
    PMIX_RELEASE(cd);
}

PMIX_EXPORT pmix_status_t PMIx_Log_nb(const pmix_info_t data[], size_t ndata,
                                      const pmix_info_t directives[], size_t ndirs,
                                      pmix_op_cbfunc_t cbfunc, void *cbdata)

{
    pmix_shift_caddy_t *cd;
    pmix_cmd_t cmd = PMIX_LOG_CMD;
    pmix_buffer_t *msg;
    pmix_status_t rc;
    size_t n;
    time_t timestamp = 0;
    pmix_proc_t *source = NULL;

    PMIX_ACQUIRE_THREAD(&pmix_global_lock);

    pmix_output_verbose(2, pmix_globals.debug_output,
                        "pmix:log non-blocking");

    if (pmix_globals.init_cntr <= 0) {
        PMIX_RELEASE_THREAD(&pmix_global_lock);
        return PMIX_ERR_INIT;
    }

    if (0 == ndata || NULL == data) {
        PMIX_RELEASE_THREAD(&pmix_global_lock);
        return PMIX_ERR_BAD_PARAM;
    }

    /* check the directives - if they requested a timestamp, then
     * get the time, also look for a source */
    if (NULL != directives) {
        for (n=0; n < ndirs; n++) {
            if (0 == strncmp(directives[n].key, PMIX_LOG_GENERATE_TIMESTAMP, PMIX_MAX_KEYLEN)) {
                if (PMIX_INFO_TRUE(&directives[n])) {
                    /* pickup the timestamp */
                    timestamp = time(NULL);
                }
            } else if (0 == strncmp(directives[n].key, PMIX_LOG_SOURCE, PMIX_MAX_KEYLEN)) {
                source = directives[n].value.data.proc;
            }
        }
    }

    /* if we are a client or tool, we never do this ourselves - we
     * always pass this request to our server for execution */
    if (!PMIX_PROC_IS_SERVER(pmix_globals.mypeer) &&
        !PMIX_PROC_IS_LAUNCHER(pmix_globals.mypeer)) {
        /* if we aren't connected, don't attempt to send */
        if (!pmix_globals.connected) {
            PMIX_RELEASE_THREAD(&pmix_global_lock);
            return PMIX_ERR_UNREACH;
        }
        PMIX_RELEASE_THREAD(&pmix_global_lock);

        /* if we are not a server, then relay this request to the server */
        cd = PMIX_NEW(pmix_shift_caddy_t);
        cd->cbfunc.opcbfn = cbfunc;
        cd->cbdata = cbdata;
        msg = PMIX_NEW(pmix_buffer_t);
        PMIX_BFROPS_PACK(rc, pmix_client_globals.myserver,
                         msg, &cmd, 1, PMIX_COMMAND);
        if (PMIX_SUCCESS != rc) {
            PMIX_ERROR_LOG(rc);
            PMIX_RELEASE(msg);
            PMIX_RELEASE(cd);
            return rc;
        }
        /* provide the timestamp - zero will indicate
         * that it wasn't taken */
        PMIX_BFROPS_PACK(rc, pmix_client_globals.myserver,
                         msg, &timestamp, 1, PMIX_TIME);
        if (PMIX_SUCCESS != rc) {
            PMIX_ERROR_LOG(rc);
            PMIX_RELEASE(msg);
            PMIX_RELEASE(cd);
            return rc;
        }
        /* pack the number of data entries */
        PMIX_BFROPS_PACK(rc, pmix_client_globals.myserver,
                         msg, &ndata, 1, PMIX_SIZE);
        if (PMIX_SUCCESS != rc) {
            PMIX_ERROR_LOG(rc);
            PMIX_RELEASE(msg);
            PMIX_RELEASE(cd);
            return rc;
        }
        if (0 < ndata) {
            PMIX_BFROPS_PACK(rc, pmix_client_globals.myserver,
                             msg, data, ndata, PMIX_INFO);
            if (PMIX_SUCCESS != rc) {
                PMIX_ERROR_LOG(rc);
                PMIX_RELEASE(msg);
                PMIX_RELEASE(cd);
                return rc;
            }
        }
        PMIX_BFROPS_PACK(rc, pmix_client_globals.myserver,
                         msg, &ndirs, 1, PMIX_SIZE);
        if (PMIX_SUCCESS != rc) {
            PMIX_ERROR_LOG(rc);
            PMIX_RELEASE(msg);
            PMIX_RELEASE(cd);
            return rc;
        }
        if (0 < ndirs) {
            PMIX_BFROPS_PACK(rc, pmix_client_globals.myserver,
                             msg, directives, ndirs, PMIX_INFO);
            if (PMIX_SUCCESS != rc) {
                PMIX_ERROR_LOG(rc);
                PMIX_RELEASE(msg);
                PMIX_RELEASE(cd);
                return rc;
            }
        }

        pmix_output_verbose(2, pmix_plog_base_framework.framework_output,
                            "pmix:log sending to server");
        PMIX_PTL_SEND_RECV(rc, pmix_client_globals.myserver,
                           msg, log_cbfunc, (void*)cd);
        if (PMIX_SUCCESS != rc) {
            PMIX_ERROR_LOG(rc);
            PMIX_RELEASE(cd);
        }
        return rc;
    }
    PMIX_RELEASE_THREAD(&pmix_global_lock);

    /* if no recorded source was found, then we must be it */
    if (NULL == source) {
        source = &pmix_globals.myid;
        cd = PMIX_NEW(pmix_shift_caddy_t);
        cd->cbfunc.opcbfn = cbfunc;
        cd->cbdata = cbdata;
        cd->ndirs = ndirs + 1;
        PMIX_INFO_CREATE(cd->directives, cd->ndirs);
        for (n=0; n < ndirs; n++) {
            PMIX_INFO_XFER(&cd->directives[n], (pmix_info_t*)&directives[n]);
        }
        PMIX_INFO_LOAD(&cd->directives[ndirs], PMIX_LOG_SOURCE, &source, PMIX_PROC);
        /* call down to process the request - the various components
         * will thread shift as required */
        rc = pmix_plog.log(source, data, ndata, cd->directives, cd->ndirs, localcbfunc, cd);
        if (PMIX_SUCCESS != rc) {
            PMIX_INFO_FREE(cd->directives, cd->ndirs);
            PMIX_RELEASE(cd);
        }
    } else if (0 == strncmp(source->nspace, pmix_globals.myid.nspace, PMIX_MAX_NSLEN) &&
               source->rank == pmix_globals.myid.rank) {
        /* if I am the recorded source, then this is a re-submission of
         * something that got "upcalled" by a prior call. In this case,
         * we return a "not supported" error as clearly we couldn't
         * handle it, and neither could our host */
        rc = PMIX_ERR_NOT_SUPPORTED;
    } else {
        /* call down to process the request - the various components
         * will thread shift as required */
        rc = pmix_plog.log(source, data, ndata, directives, ndirs, cbfunc, cbdata);
    }

    return rc;
}
