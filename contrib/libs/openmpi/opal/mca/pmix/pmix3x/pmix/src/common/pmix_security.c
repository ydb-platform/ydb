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
#include "src/util/output.h"
#include "src/mca/bfrops/bfrops.h"
#include "src/mca/psec/psec.h"
#include "src/mca/ptl/ptl.h"

#include "src/client/pmix_client_ops.h"
#include "src/server/pmix_server_ops.h"
#include "src/include/pmix_globals.h"

static void getcbfunc(struct pmix_peer_t *peer,
                      pmix_ptl_hdr_t *hdr,
                      pmix_buffer_t *buf, void *cbdata)
{
    pmix_query_caddy_t *cd = (pmix_query_caddy_t*)cbdata;
    pmix_status_t rc, status;
    int cnt;
    pmix_byte_object_t cred;
    pmix_info_t *info = NULL;
    size_t ninfo = 0;

    pmix_output_verbose(2, pmix_globals.debug_output,
                        "pmix:security cback from server with %d bytes",
                        (int)buf->bytes_used);

    /* a zero-byte buffer indicates that this recv is being
     * completed due to a lost connection */
    if (PMIX_BUFFER_IS_EMPTY(buf)) {
        /* release the caller */
        if (NULL != cd->credcbfunc) {
            cd->credcbfunc(PMIX_ERR_COMM_FAILURE, NULL, NULL, 0, cd->cbdata);
        }
        PMIX_RELEASE(cd);
        return;
    }

    /* unpack the status */
    cnt = 1;
    PMIX_BFROPS_UNPACK(rc, peer, buf, &status, &cnt, PMIX_STATUS);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        goto complete;
    }
    if (PMIX_SUCCESS != status) {
        goto complete;
    }

    /* unpack the credential */
    cnt = 1;
    PMIX_BFROPS_UNPACK(rc, peer, buf, &cred, &cnt, PMIX_BYTE_OBJECT);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        goto complete;
    }

    /* unpack any returned info */
    cnt = 1;
    PMIX_BFROPS_UNPACK(rc, peer, buf, &ninfo, &cnt, PMIX_SIZE);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        goto complete;
    }
    if (0 < ninfo) {
        PMIX_INFO_CREATE(info, ninfo);
        cnt = ninfo;
        PMIX_BFROPS_UNPACK(rc, peer, buf, info, &cnt, PMIX_INFO);
        if (PMIX_SUCCESS != rc) {
            PMIX_ERROR_LOG(rc);
            goto complete;
        }
    }

  complete:
    pmix_output_verbose(2, pmix_globals.debug_output,
                        "pmix:security cback from server releasing");
    /* release the caller */
    if (NULL != cd->credcbfunc) {
        cd->credcbfunc(status, &cred, info, ninfo, cd->cbdata);
    }
    PMIX_BYTE_OBJECT_DESTRUCT(&cred);
    if (NULL != info) {
        PMIX_INFO_FREE(info, ninfo);
    }
    PMIX_RELEASE(cd);
}

PMIX_EXPORT pmix_status_t PMIx_Get_credential(const pmix_info_t info[], size_t ninfo,
                                              pmix_credential_cbfunc_t cbfunc, void *cbdata)
{
    pmix_buffer_t *msg;
    pmix_cmd_t cmd = PMIX_GET_CREDENTIAL_CMD;
    pmix_status_t rc;
    pmix_query_caddy_t *cb;
    pmix_byte_object_t cred;
    pmix_info_t *results = NULL;
    size_t nresults = 0;

    PMIX_ACQUIRE_THREAD(&pmix_global_lock);

    pmix_output_verbose(2, pmix_globals.debug_output,
                        "pmix: Get_credential called with %d info", (int)ninfo);

    if (pmix_globals.init_cntr <= 0) {
        PMIX_RELEASE_THREAD(&pmix_global_lock);
        return PMIX_ERR_INIT;
    }

    /* if we are the server */
    if (PMIX_PROC_IS_SERVER(pmix_globals.mypeer) &&
        !PMIX_PROC_IS_LAUNCHER(pmix_globals.mypeer)) {
        PMIX_RELEASE_THREAD(&pmix_global_lock);
        /* if the host doesn't support this operation,
         * see if we can generate it ourselves */
        if (NULL == pmix_host_server.get_credential) {
            PMIX_BYTE_OBJECT_CONSTRUCT(&cred);
            PMIX_PSEC_CREATE_CRED(rc, pmix_globals.mypeer, info, ninfo,
                                  &results, &nresults, &cred);
            if (PMIX_SUCCESS == rc) {
                /* pass it back in the callback function */
                if (NULL != cbfunc) {
                    cbfunc(PMIX_SUCCESS, &cred, results, nresults, cbdata);
                    if (NULL != results) {
                        PMIX_INFO_FREE(results, nresults);
                    }
                    PMIX_BYTE_OBJECT_DESTRUCT(&cred);
                }
            }
            return rc;
        }
        /* the host is available, so let them try to create it */
        pmix_output_verbose(2, pmix_globals.debug_output,
                            "pmix:get_credential handed to RM");
        rc = pmix_host_server.get_credential(&pmix_globals.myid,
                                             info, ninfo,
                                             cbfunc, cbdata);
        return rc;
    }

    /* if we are a client or tool and we aren't connected, see
     * if one of our internal plugins is capable of meeting the request */
    if (!pmix_globals.connected) {
        PMIX_RELEASE_THREAD(&pmix_global_lock);
        PMIX_BYTE_OBJECT_CONSTRUCT(&cred);
        PMIX_PSEC_CREATE_CRED(rc, pmix_globals.mypeer, info, ninfo,
                              &results, &nresults, &cred);
        if (PMIX_SUCCESS == rc) {
            /* pass it back in the callback function */
            if (NULL != cbfunc) {
                cbfunc(PMIX_SUCCESS, &cred, results, nresults, cbdata);
                if (NULL != results) {
                    PMIX_INFO_FREE(results, nresults);
                }
                PMIX_BYTE_OBJECT_DESTRUCT(&cred);
            }
        }
        return rc;
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

    /* pack the directives */
    PMIX_BFROPS_PACK(rc, pmix_client_globals.myserver,
                     msg, &ninfo, 1, PMIX_SIZE);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        PMIX_RELEASE(msg);
        return rc;
    }
    if (0 < ninfo) {
        PMIX_BFROPS_PACK(rc, pmix_client_globals.myserver,
                         msg, info, ninfo, PMIX_INFO);
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
    cb->credcbfunc = cbfunc;
    cb->cbdata = cbdata;

    /* push the message into our event base to send to the server */
    PMIX_PTL_SEND_RECV(rc, pmix_client_globals.myserver,
                       msg, getcbfunc, (void*)cb);
    if (PMIX_SUCCESS != rc) {
        PMIX_RELEASE(msg);
        PMIX_RELEASE(cb);
    }

    return rc;
}

static void valid_cbfunc(struct pmix_peer_t *peer,
                         pmix_ptl_hdr_t *hdr,
                         pmix_buffer_t *buf, void *cbdata)
{
    pmix_query_caddy_t *cd = (pmix_query_caddy_t*)cbdata;
    pmix_status_t rc, status;
    int cnt;
    pmix_info_t *info = NULL;
    size_t ninfo = 0;

    pmix_output_verbose(2, pmix_globals.debug_output,
                        "pmix:security cback from server with %d bytes",
                        (int)buf->bytes_used);

    /* a zero-byte buffer indicates that this recv is being
     * completed due to a lost connection */
    if (PMIX_BUFFER_IS_EMPTY(buf)) {
        /* release the caller */
        if (NULL != cd->validcbfunc) {
            cd->validcbfunc(PMIX_ERR_COMM_FAILURE, NULL, 0, cd->cbdata);
        }
        PMIX_RELEASE(cd);
        return;
    }

    /* unpack the status */
    cnt = 1;
    PMIX_BFROPS_UNPACK(rc, peer, buf, &status, &cnt, PMIX_STATUS);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        goto complete;
    }
    if (PMIX_SUCCESS != status) {
        goto complete;
    }

    /* unpack any returned info */
    cnt = 1;
    PMIX_BFROPS_UNPACK(rc, peer, buf, &ninfo, &cnt, PMIX_SIZE);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        goto complete;
    }
    if (0 < ninfo) {
        PMIX_INFO_CREATE(info, ninfo);
        cnt = ninfo;
        PMIX_BFROPS_UNPACK(rc, peer, buf, info, &cnt, PMIX_INFO);
        if (PMIX_SUCCESS != rc) {
            PMIX_ERROR_LOG(rc);
            goto complete;
        }
    }

  complete:
    pmix_output_verbose(2, pmix_globals.debug_output,
                        "pmix:security cback from server releasing");
    /* release the caller */
    if (NULL != cd->validcbfunc) {
        cd->validcbfunc(status, info, ninfo, cd->cbdata);
    }
    if (NULL != info) {
        PMIX_INFO_FREE(info, ninfo);
    }
    PMIX_RELEASE(cd);
}

PMIX_EXPORT pmix_status_t PMIx_Validate_credential(const pmix_byte_object_t *cred,
                                                   const pmix_info_t directives[], size_t ndirs,
                                                   pmix_validation_cbfunc_t cbfunc, void *cbdata)
{
    pmix_buffer_t *msg;
    pmix_cmd_t cmd = PMIX_VALIDATE_CRED_CMD;
    pmix_status_t rc;
    pmix_query_caddy_t *cb;
    pmix_info_t *results = NULL;
    size_t nresults = 0;

    PMIX_ACQUIRE_THREAD(&pmix_global_lock);

    pmix_output_verbose(2, pmix_globals.debug_output,
                        "pmix: monitor called");

    if (pmix_globals.init_cntr <= 0) {
        PMIX_RELEASE_THREAD(&pmix_global_lock);
        return PMIX_ERR_INIT;
    }

    /* if we are the server */
    if (PMIX_PROC_IS_SERVER(pmix_globals.mypeer) &&
        !PMIX_PROC_IS_LAUNCHER(pmix_globals.mypeer)) {
        PMIX_RELEASE_THREAD(&pmix_global_lock);
        /* if the host doesn't support this operation,
         * see if we can validate it ourselves */
        if (NULL == pmix_host_server.validate_credential) {
            PMIX_PSEC_VALIDATE_CRED(rc, pmix_globals.mypeer,
                                    directives, ndirs,
                                    &results, &nresults, cred);
            if (PMIX_SUCCESS == rc) {
                /* pass it back in the callback function */
                if (NULL != cbfunc) {
                    cbfunc(PMIX_SUCCESS, results, nresults, cbdata);
                    if (NULL != results) {
                        PMIX_INFO_FREE(results, nresults);
                    }
                }
            }
            return rc;
        }
        /* the host is available, so let them try to validate it */
        pmix_output_verbose(2, pmix_globals.debug_output,
                            "pmix:get_credential handed to RM");
        rc = pmix_host_server.validate_credential(&pmix_globals.myid, cred,
                                                  directives, ndirs, cbfunc, cbdata);
        return rc;
    }

    /* if we are a client or tool and we aren't connected, see
     * if one of our internal plugins is capable of meeting the request */
    if (!pmix_globals.connected) {
        PMIX_RELEASE_THREAD(&pmix_global_lock);
        PMIX_PSEC_VALIDATE_CRED(rc, pmix_globals.mypeer,
                                directives, ndirs,
                                &results, &nresults, cred);
        if (PMIX_SUCCESS == rc) {
            /* pass it back in the callback function */
            if (NULL != cbfunc) {
                cbfunc(PMIX_SUCCESS, results, nresults, cbdata);
                if (NULL != results) {
                    PMIX_INFO_FREE(results, nresults);
                }
            }
        }
        return rc;
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

    /* pack the credential */
    PMIX_BFROPS_PACK(rc, pmix_client_globals.myserver,
                     msg, cred, 1, PMIX_BYTE_OBJECT);
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
    cb->validcbfunc = cbfunc;
    cb->cbdata = cbdata;

    /* push the message into our event base to send to the server */
    PMIX_PTL_SEND_RECV(rc, pmix_client_globals.myserver,
                       msg, valid_cbfunc, (void*)cb);
    if (PMIX_SUCCESS != rc) {
        PMIX_RELEASE(msg);
        PMIX_RELEASE(cb);
    }

    return rc;
}
