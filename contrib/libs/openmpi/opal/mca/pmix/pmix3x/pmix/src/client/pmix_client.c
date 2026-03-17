/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2014-2018 Intel, Inc.  All rights reserved.
 * Copyright (c) 2014-2018 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2014      Artem Y. Polyakov <artpol84@gmail.com>.
 *                         All rights reserved.
 * Copyright (c) 2016-2017 Mellanox Technologies, Inc.
 *                         All rights reserved.
 * Copyright (c) 2016-2017 IBM Corporation.  All rights reserved.
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
#include PMIX_EVENT2_THREAD_HEADER

static const char pmix_version_string[] = PMIX_VERSION;
static pmix_status_t pmix_init_result = PMIX_ERR_INIT;

#include "src/class/pmix_list.h"
#include "src/event/pmix_event.h"
#include "src/util/argv.h"
#include "src/util/compress.h"
#include "src/util/error.h"
#include "src/util/hash.h"
#include "src/util/name_fns.h"
#include "src/util/output.h"
#include "src/runtime/pmix_progress_threads.h"
#include "src/runtime/pmix_rte.h"
#include "src/threads/threads.h"
#include "src/mca/bfrops/base/base.h"
#include "src/mca/gds/base/base.h"
#include "src/mca/preg/preg.h"
#include "src/mca/ptl/base/base.h"
#include "src/include/pmix_globals.h"
#include "src/common/pmix_iof.h"

#include "pmix_client_ops.h"

#define PMIX_MAX_RETRIES 10

static void _notify_complete(pmix_status_t status, void *cbdata)
{
    pmix_event_chain_t *chain = (pmix_event_chain_t*)cbdata;
    PMIX_ACQUIRE_OBJECT(chain);
    PMIX_RELEASE(chain);
}

static void pmix_client_notify_recv(struct pmix_peer_t *peer,
                                    pmix_ptl_hdr_t *hdr,
                                    pmix_buffer_t *buf, void *cbdata)
{
    pmix_status_t rc;
    int32_t cnt;
    pmix_cmd_t cmd;
    pmix_event_chain_t *chain;
    size_t ninfo;

    pmix_output_verbose(2, pmix_client_globals.base_output,
                        "pmix:client_notify_recv - processing event");

    /* a zero-byte buffer indicates that this recv is being
     * completed due to a lost connection */
    if (PMIX_BUFFER_IS_EMPTY(buf)) {
        return;
    }

    /* start the local notification chain */
    chain = PMIX_NEW(pmix_event_chain_t);
    if (NULL == chain) {
        PMIX_ERROR_LOG(PMIX_ERR_NOMEM);
        return;
    }
    chain->final_cbfunc = _notify_complete;
    chain->final_cbdata = chain;

    cnt=1;
    PMIX_BFROPS_UNPACK(rc, pmix_client_globals.myserver,
                       buf, &cmd, &cnt, PMIX_COMMAND);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        PMIX_RELEASE(chain);
        goto error;
    }
    /* unpack the status */
    cnt=1;
    PMIX_BFROPS_UNPACK(rc, pmix_client_globals.myserver,
                       buf, &chain->status, &cnt, PMIX_STATUS);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        PMIX_RELEASE(chain);
        goto error;
    }

    /* unpack the source of the event */
    cnt=1;
    PMIX_BFROPS_UNPACK(rc, pmix_client_globals.myserver,
                       buf, &chain->source, &cnt, PMIX_PROC);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        PMIX_RELEASE(chain);
        goto error;
    }

    /* unpack the info that might have been provided */
    cnt=1;
    PMIX_BFROPS_UNPACK(rc, pmix_client_globals.myserver,
                       buf, &ninfo, &cnt, PMIX_SIZE);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        PMIX_RELEASE(chain);
        goto error;
    }

    /* we always leave space for event hdlr name and a callback object */
    chain->nallocated = ninfo + 2;
    PMIX_INFO_CREATE(chain->info, chain->nallocated);
    if (NULL == chain->info) {
        PMIX_ERROR_LOG(PMIX_ERR_NOMEM);
        PMIX_RELEASE(chain);
        return;
    }

    if (0 < ninfo) {
        chain->ninfo = ninfo;
        cnt = ninfo;
        PMIX_BFROPS_UNPACK(rc, pmix_client_globals.myserver,
                           buf, chain->info, &cnt, PMIX_INFO);
        if (PMIX_SUCCESS != rc) {
            PMIX_ERROR_LOG(rc);
            PMIX_RELEASE(chain);
            goto error;
        }
    }
    /* prep the chain for processing */
    pmix_prep_event_chain(chain, chain->info, ninfo, false);

    pmix_output_verbose(2, pmix_client_globals.base_output,
                        "[%s:%d] pmix:client_notify_recv - processing event %s, calling errhandler",
                        pmix_globals.myid.nspace, pmix_globals.myid.rank, PMIx_Error_string(chain->status));

    pmix_invoke_local_event_hdlr(chain);
    return;

  error:
    /* we always need to return */
    pmix_output_verbose(2, pmix_client_globals.base_output,
                        "pmix:client_notify_recv - unpack error status =%d, calling def errhandler", rc);
    chain = PMIX_NEW(pmix_event_chain_t);
    if (NULL == chain) {
        PMIX_ERROR_LOG(PMIX_ERR_NOMEM);
        return;
    }
    chain->status = rc;
    pmix_invoke_local_event_hdlr(chain);
}


pmix_client_globals_t pmix_client_globals = {0};

/* callback for wait completion */
static void wait_cbfunc(struct pmix_peer_t *pr,
                        pmix_ptl_hdr_t *hdr,
                        pmix_buffer_t *buf, void *cbdata)
{
    pmix_lock_t *lock = (pmix_lock_t*)cbdata;

    pmix_output_verbose(2, pmix_client_globals.base_output,
                        "pmix:client wait_cbfunc received");
    PMIX_WAKEUP_THREAD(lock);
}

/* callback to receive job info */
static void job_data(struct pmix_peer_t *pr,
                     pmix_ptl_hdr_t *hdr,
                     pmix_buffer_t *buf, void *cbdata)
{
    pmix_status_t rc;
    char *nspace;
    int32_t cnt = 1;
    pmix_cb_t *cb = (pmix_cb_t*)cbdata;

    /* unpack the nspace - should be same as our own */
    PMIX_BFROPS_UNPACK(rc, pmix_client_globals.myserver,
                       buf, &nspace, &cnt, PMIX_STRING);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        cb->status = PMIX_ERROR;
        PMIX_POST_OBJECT(cb);
        PMIX_WAKEUP_THREAD(&cb->lock);
        return;
    }

    /* decode it */
    PMIX_GDS_STORE_JOB_INFO(cb->status,
                            pmix_client_globals.myserver,
                            nspace, buf);
    free(nspace);
    cb->status = PMIX_SUCCESS;
    PMIX_POST_OBJECT(cb);
    PMIX_WAKEUP_THREAD(&cb->lock);
}

PMIX_EXPORT const char* PMIx_Get_version(void)
{
    return pmix_version_string;
}

/* event handler registration callback */
static void evhandler_reg_callbk(pmix_status_t status,
                                 size_t evhandler_ref,
                                 void *cbdata)
{
    pmix_lock_t *lock = (pmix_lock_t*)cbdata;

    lock->status = status;
    PMIX_WAKEUP_THREAD(lock);
}


static void notification_fn(size_t evhdlr_registration_id,
                            pmix_status_t status,
                            const pmix_proc_t *source,
                            pmix_info_t info[], size_t ninfo,
                            pmix_info_t results[], size_t nresults,
                            pmix_event_notification_cbfunc_fn_t cbfunc,
                            void *cbdata)
{
    pmix_lock_t *lock=NULL;
    char *name = NULL;
    size_t n;

    pmix_output_verbose(2, pmix_client_globals.base_output,
                        "[%s:%d] DEBUGGER RELEASE RECVD",
                        pmix_globals.myid.nspace, pmix_globals.myid.rank);
    if (NULL != info) {
        lock = NULL;
        for (n=0; n < ninfo; n++) {
            if (0 == strncmp(info[n].key, PMIX_EVENT_RETURN_OBJECT, PMIX_MAX_KEYLEN)) {
                lock = (pmix_lock_t*)info[n].value.data.ptr;
            } else if (0 == strncmp(info[n].key, PMIX_EVENT_HDLR_NAME, PMIX_MAX_KEYLEN)) {
                name = info[n].value.data.string;
            }
        }
        /* if the object wasn't returned, then that is an error */
        if (NULL == lock) {
            pmix_output_verbose(2, pmix_client_globals.base_output,
                                "event handler %s failed to return object",
                                (NULL == name) ? "NULL" : name);
            /* let the event handler progress */
            if (NULL != cbfunc) {
                cbfunc(PMIX_SUCCESS, NULL, 0, NULL, NULL, cbdata);
            }
            return;
        }
    }
    if (NULL != lock) {
        PMIX_WAKEUP_THREAD(lock);
    }

    if (NULL != cbfunc) {
        cbfunc(PMIX_EVENT_ACTION_COMPLETE, NULL, 0, NULL, NULL, cbdata);
    }
}

typedef struct {
    pmix_info_t *info;
    size_t ninfo;
} mydata_t;

static void release_info(pmix_status_t status, void *cbdata)
{
    mydata_t *cd = (mydata_t*)cbdata;
    PMIX_INFO_FREE(cd->info, cd->ninfo);
    free(cd);
}

static void _check_for_notify(pmix_info_t info[], size_t ninfo)
{
    mydata_t *cd;
    size_t n, m=0;
    pmix_info_t *model=NULL, *library=NULL, *vers=NULL, *tmod=NULL;

    for (n=0; n < ninfo; n++) {
        if (0 == strncmp(info[n].key, PMIX_PROGRAMMING_MODEL, PMIX_MAX_KEYLEN)) {
            /* we need to generate an event indicating that
             * a programming model has been declared */
            model = &info[n];
            ++m;
        } else if (0 == strncmp(info[n].key, PMIX_MODEL_LIBRARY_NAME, PMIX_MAX_KEYLEN)) {
            library = &info[n];
            ++m;
        } else if (0 == strncmp(info[n].key, PMIX_MODEL_LIBRARY_VERSION, PMIX_MAX_KEYLEN)) {
            vers = &info[n];
            ++m;
        } else if (0 == strncmp(info[n].key, PMIX_THREADING_MODEL, PMIX_MAX_KEYLEN)) {
            tmod = &info[n];
            ++m;
        }
    }
    if (0 < m) {
        /* notify anyone listening that a model has been declared */
        cd = (mydata_t*)malloc(sizeof(mydata_t));
        if (NULL == cd) {
            /* nothing we can do */
            return;
        }
        PMIX_INFO_CREATE(cd->info, m+1);
        if (NULL == cd->info) {
            free(cd);
            return;
        }
        cd->ninfo = m+1;
        n = 0;
        if (NULL != model) {
            PMIX_INFO_XFER(&cd->info[n], model);
            ++n;
        }
        if (NULL != library) {
            PMIX_INFO_XFER(&cd->info[n], library);
            ++n;
        }
        if (NULL != vers) {
            PMIX_INFO_XFER(&cd->info[n], vers);
            ++n;
        }
        if (NULL != tmod) {
            PMIX_INFO_XFER(&cd->info[n], tmod);
            ++n;
        }
        /* mark that it is not to go to any default handlers */
        PMIX_INFO_LOAD(&cd->info[n], PMIX_EVENT_NON_DEFAULT, NULL, PMIX_BOOL);
        PMIx_Notify_event(PMIX_MODEL_DECLARED,
                          &pmix_globals.myid, PMIX_RANGE_PROC_LOCAL,
                          cd->info, cd->ninfo, release_info, (void*)cd);
    }
}

static void client_iof_handler(struct pmix_peer_t *pr,
                               pmix_ptl_hdr_t *hdr,
                               pmix_buffer_t *buf, void *cbdata)
{
    pmix_peer_t *peer = (pmix_peer_t*)pr;
    pmix_proc_t source;
    pmix_iof_channel_t channel;
    pmix_byte_object_t bo;
    int32_t cnt;
    pmix_status_t rc;

    pmix_output_verbose(2, pmix_client_globals.iof_output,
                        "recvd IOF");

    /* if the buffer is empty, they are simply closing the channel */
    if (0 == buf->bytes_used) {
        return;
    }

    cnt = 1;
    PMIX_BFROPS_UNPACK(rc, peer, buf, &source, &cnt, PMIX_PROC);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        return;
    }
    cnt = 1;
    PMIX_BFROPS_UNPACK(rc, peer, buf, &channel, &cnt, PMIX_IOF_CHANNEL);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        return;
    }
    cnt = 1;
    PMIX_BFROPS_UNPACK(rc, peer, buf, &bo, &cnt, PMIX_BYTE_OBJECT);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        return;
    }
    if (NULL != bo.bytes && 0 < bo.size) {
        pmix_iof_write_output(&source, channel, &bo, NULL);
    }
    PMIX_BYTE_OBJECT_DESTRUCT(&bo);
}

PMIX_EXPORT pmix_status_t PMIx_Init(pmix_proc_t *proc,
                                    pmix_info_t info[], size_t ninfo)
{
    char *evar;
    pmix_status_t rc;
    pmix_cb_t cb;
    pmix_buffer_t *req;
    pmix_cmd_t cmd = PMIX_REQ_CMD;
    pmix_status_t code = PMIX_ERR_DEBUGGER_RELEASE;
    pmix_proc_t wildcard;
    pmix_info_t ginfo, evinfo[2];
    pmix_value_t *val = NULL;
    pmix_lock_t reglock, releaselock;
    size_t n;
    bool found;
    pmix_ptl_posted_recv_t *rcv;

    PMIX_ACQUIRE_THREAD(&pmix_global_lock);

    if (0 < pmix_globals.init_cntr ||
        (NULL != pmix_globals.mypeer && PMIX_PROC_IS_SERVER(pmix_globals.mypeer))) {
        /* since we have been called before, the nspace and
         * rank should be known. So return them here if
         * requested */
         if (NULL != proc) {
            pmix_strncpy(proc->nspace, pmix_globals.myid.nspace, PMIX_MAX_NSLEN);
            proc->rank = pmix_globals.myid.rank;
        }
        ++pmix_globals.init_cntr;
        /* we also need to check the info keys to see if something need
         * be done with them - e.g., to notify another library that we
         * also have called init */
        PMIX_RELEASE_THREAD(&pmix_global_lock);
        if (NULL != info) {
            _check_for_notify(info, ninfo);
        }
        return pmix_init_result;
    }
    ++pmix_globals.init_cntr;

    /* if we don't see the required info, then we cannot init */
    if (NULL == (evar = getenv("PMIX_NAMESPACE"))) {
        pmix_init_result = PMIX_ERR_INVALID_NAMESPACE;
        PMIX_RELEASE_THREAD(&pmix_global_lock);
        return PMIX_ERR_INVALID_NAMESPACE;
    }

    /* setup the runtime - this init's the globals,
     * opens and initializes the required frameworks */
    if (PMIX_SUCCESS != (rc = pmix_rte_init(PMIX_PROC_CLIENT, info, ninfo,
                                            pmix_client_notify_recv))) {
        PMIX_ERROR_LOG(rc);
        pmix_init_result = rc;
        PMIX_RELEASE_THREAD(&pmix_global_lock);
        return rc;
    }
    /* setup the IO Forwarding recv */
    rcv = PMIX_NEW(pmix_ptl_posted_recv_t);
    rcv->tag = PMIX_PTL_TAG_IOF;
    rcv->cbfunc = client_iof_handler;
    /* add it to the end of the list of recvs */
    pmix_list_append(&pmix_ptl_globals.posted_recvs, &rcv->super);


    /* setup the globals */
    PMIX_CONSTRUCT(&pmix_client_globals.pending_requests, pmix_list_t);
    PMIX_CONSTRUCT(&pmix_client_globals.peers, pmix_pointer_array_t);
    pmix_pointer_array_init(&pmix_client_globals.peers, 1, INT_MAX, 1);
    pmix_client_globals.myserver = PMIX_NEW(pmix_peer_t);
    if (NULL == pmix_client_globals.myserver) {
        pmix_init_result = PMIX_ERR_NOMEM;
        PMIX_RELEASE_THREAD(&pmix_global_lock);
        return PMIX_ERR_NOMEM;
    }
    pmix_client_globals.myserver->nptr = PMIX_NEW(pmix_namespace_t);
    if (NULL == pmix_client_globals.myserver->nptr) {
        PMIX_RELEASE(pmix_client_globals.myserver);
        pmix_init_result = PMIX_ERR_NOMEM;
        PMIX_RELEASE_THREAD(&pmix_global_lock);
        return PMIX_ERR_NOMEM;
    }
    pmix_client_globals.myserver->info = PMIX_NEW(pmix_rank_info_t);
    if (NULL == pmix_client_globals.myserver->info) {
        PMIX_RELEASE(pmix_client_globals.myserver);
        pmix_init_result = PMIX_ERR_NOMEM;
        PMIX_RELEASE_THREAD(&pmix_global_lock);
        return PMIX_ERR_NOMEM;
    }

    /* setup the base verbosity */
    if (0 < pmix_client_globals.base_verbose) {
        /* set default output */
        pmix_client_globals.base_output = pmix_output_open(NULL);
        pmix_output_set_verbosity(pmix_client_globals.base_output,
                                  pmix_client_globals.base_verbose);
    }

    pmix_output_verbose(2, pmix_client_globals.base_output,
                        "pmix: init called");

    /* we require our nspace */
    if (NULL != proc) {
        pmix_strncpy(proc->nspace, evar, PMIX_MAX_NSLEN);
    }
    PMIX_LOAD_NSPACE(pmix_globals.myid.nspace, evar);
    /* set the global pmix_namespace_t object for our peer */
    pmix_globals.mypeer->nptr->nspace = strdup(evar);

    /* we also require our rank */
    if (NULL == (evar = getenv("PMIX_RANK"))) {
        /* let the caller know that the server isn't available yet */
        pmix_init_result = PMIX_ERR_DATA_VALUE_NOT_FOUND;
        PMIX_RELEASE_THREAD(&pmix_global_lock);
        return PMIX_ERR_DATA_VALUE_NOT_FOUND;
    }
    pmix_globals.myid.rank = strtol(evar, NULL, 10);
    if (NULL != proc) {
        proc->rank = pmix_globals.myid.rank;
    }
    pmix_globals.pindex = -1;
    /* setup a rank_info object for us */
    pmix_globals.mypeer->info = PMIX_NEW(pmix_rank_info_t);
    if (NULL == pmix_globals.mypeer->info) {
        pmix_init_result = PMIX_ERR_NOMEM;
        PMIX_RELEASE_THREAD(&pmix_global_lock);
        return PMIX_ERR_NOMEM;
    }
    pmix_globals.mypeer->info->pname.nspace = strdup(proc->nspace);
    pmix_globals.mypeer->info->pname.rank = proc->rank;

    /* select our psec compat module - the selection will be based
     * on the corresponding envars that should have been passed
     * to us at launch */
    evar = getenv("PMIX_SECURITY_MODE");
    pmix_globals.mypeer->nptr->compat.psec = pmix_psec_base_assign_module(evar);
    if (NULL == pmix_globals.mypeer->nptr->compat.psec) {
        pmix_init_result = PMIX_ERR_INIT;
        PMIX_RELEASE_THREAD(&pmix_global_lock);
        return PMIX_ERR_INIT;
    }
    /* the server will be using the same */
    pmix_client_globals.myserver->nptr->compat.psec = pmix_globals.mypeer->nptr->compat.psec;

    /* set the buffer type - the selection will be based
     * on the corresponding envars that should have been passed
     * to us at launch */
    evar = getenv("PMIX_BFROP_BUFFER_TYPE");
    if (NULL == evar) {
        /* just set to our default */
        pmix_globals.mypeer->nptr->compat.type = pmix_bfrops_globals.default_type;
    } else if (0 == strcmp(evar, "PMIX_BFROP_BUFFER_FULLY_DESC")) {
        pmix_globals.mypeer->nptr->compat.type = PMIX_BFROP_BUFFER_FULLY_DESC;
    } else {
        pmix_globals.mypeer->nptr->compat.type = PMIX_BFROP_BUFFER_NON_DESC;
    }
    /* the server will be using the same */
    pmix_client_globals.myserver->nptr->compat.type = pmix_globals.mypeer->nptr->compat.type;

    /* select the gds compat module we will use to interact with
     * our server- the selection will be based
     * on the corresponding envars that should have been passed
     * to us at launch */
    evar = getenv("PMIX_GDS_MODULE");
    if (NULL != evar) {
        PMIX_INFO_LOAD(&ginfo, PMIX_GDS_MODULE, evar, PMIX_STRING);
        pmix_client_globals.myserver->nptr->compat.gds = pmix_gds_base_assign_module(&ginfo, 1);
        PMIX_INFO_DESTRUCT(&ginfo);
    } else {
        pmix_client_globals.myserver->nptr->compat.gds = pmix_gds_base_assign_module(NULL, 0);
    }
    if (NULL == pmix_client_globals.myserver->nptr->compat.gds) {
        pmix_init_result = PMIX_ERR_INIT;
        PMIX_RELEASE_THREAD(&pmix_global_lock);
        return PMIX_ERR_INIT;
    }
    /* now select a GDS module for our own internal use - the user may
     * have passed down a directive for this purpose. If they did, then
     * use it. Otherwise, we want the "hash" module */
    found = false;
    if (info != NULL) {
        for (n=0; n < ninfo; n++) {
            if (0 == strncmp(info[n].key, PMIX_GDS_MODULE, PMIX_MAX_KEYLEN)) {
                PMIX_INFO_LOAD(&ginfo, PMIX_GDS_MODULE, info[n].value.data.string, PMIX_STRING);
                found = true;
                break;
            }
        }
    }
    if (!found) {
        PMIX_INFO_LOAD(&ginfo, PMIX_GDS_MODULE, "hash", PMIX_STRING);
    }
    pmix_globals.mypeer->nptr->compat.gds = pmix_gds_base_assign_module(&ginfo, 1);
    if (NULL == pmix_globals.mypeer->nptr->compat.gds) {
        PMIX_INFO_DESTRUCT(&ginfo);
        pmix_init_result = PMIX_ERR_INIT;
        PMIX_RELEASE_THREAD(&pmix_global_lock);
        return PMIX_ERR_INIT;
    }
    PMIX_INFO_DESTRUCT(&ginfo);

    /* connect to the server */
    rc = pmix_ptl_base_connect_to_peer((struct pmix_peer_t*)pmix_client_globals.myserver, info, ninfo);
    if (PMIX_SUCCESS != rc) {
        pmix_init_result = rc;
        PMIX_RELEASE_THREAD(&pmix_global_lock);
        return rc;
    }
    /* mark that we are using the same module as used for the server */
    pmix_globals.mypeer->nptr->compat.ptl = pmix_client_globals.myserver->nptr->compat.ptl;

    /* send a request for our job info - we do this as a non-blocking
     * transaction because some systems cannot handle very large
     * blocking operations and error out if we try them. */
     req = PMIX_NEW(pmix_buffer_t);
     PMIX_BFROPS_PACK(rc, pmix_client_globals.myserver,
                      req, &cmd, 1, PMIX_COMMAND);
     if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        PMIX_RELEASE(req);
        pmix_init_result = rc;
        PMIX_RELEASE_THREAD(&pmix_global_lock);
        return rc;
    }
    /* send to the server */
    PMIX_CONSTRUCT(&cb, pmix_cb_t);
    PMIX_PTL_SEND_RECV(rc, pmix_client_globals.myserver,
                       req, job_data, (void*)&cb);
    if (PMIX_SUCCESS != rc) {
        pmix_init_result = rc;
        PMIX_RELEASE_THREAD(&pmix_global_lock);
        return rc;
    }
    /* wait for the data to return */
    PMIX_WAIT_THREAD(&cb.lock);
    rc = cb.status;
    PMIX_DESTRUCT(&cb);

    if (PMIX_SUCCESS == rc) {
        pmix_init_result = PMIX_SUCCESS;
    } else {
        pmix_init_result = rc;
        PMIX_RELEASE_THREAD(&pmix_global_lock);
        return rc;
    }
    PMIX_RELEASE_THREAD(&pmix_global_lock);

    /* look for a debugger attach key */
    pmix_strncpy(wildcard.nspace, pmix_globals.myid.nspace, PMIX_MAX_NSLEN);
    wildcard.rank = PMIX_RANK_WILDCARD;
    PMIX_INFO_LOAD(&ginfo, PMIX_OPTIONAL, NULL, PMIX_BOOL);
    if (PMIX_SUCCESS == PMIx_Get(&wildcard, PMIX_DEBUG_STOP_IN_INIT, &ginfo, 1, &val)) {
        PMIX_VALUE_FREE(val, 1); // cleanup memory
        /* if the value was found, then we need to wait for debugger attach here */
        /* register for the debugger release notification */
        PMIX_CONSTRUCT_LOCK(&reglock);
        PMIX_CONSTRUCT_LOCK(&releaselock);
        PMIX_INFO_LOAD(&evinfo[0], PMIX_EVENT_RETURN_OBJECT, &releaselock, PMIX_POINTER);
        PMIX_INFO_LOAD(&evinfo[1], PMIX_EVENT_HDLR_NAME, "WAIT-FOR-DEBUGGER", PMIX_STRING);
        pmix_output_verbose(2, pmix_client_globals.base_output,
                            "[%s:%d] WAITING IN INIT FOR DEBUGGER",
                            pmix_globals.myid.nspace, pmix_globals.myid.rank);
        PMIx_Register_event_handler(&code, 1, evinfo, 2,
                                    notification_fn, evhandler_reg_callbk, (void*)&reglock);
        /* wait for registration to complete */
        PMIX_WAIT_THREAD(&reglock);
        PMIX_DESTRUCT_LOCK(&reglock);
        PMIX_INFO_DESTRUCT(&evinfo[0]);
        PMIX_INFO_DESTRUCT(&evinfo[1]);
        /* wait for release to arrive */
        PMIX_WAIT_THREAD(&releaselock);
        PMIX_DESTRUCT_LOCK(&releaselock);
    }
    PMIX_INFO_DESTRUCT(&ginfo);

    /* check to see if we need to notify anyone */
    if (NULL != info) {
        _check_for_notify(info, ninfo);
    }
    return PMIX_SUCCESS;
}

PMIX_EXPORT int PMIx_Initialized(void)
{
    PMIX_ACQUIRE_THREAD(&pmix_global_lock);

    if (0 < pmix_globals.init_cntr) {
        PMIX_RELEASE_THREAD(&pmix_global_lock);
        return true;
    }
    PMIX_RELEASE_THREAD(&pmix_global_lock);
    return false;
}

typedef struct {
    pmix_lock_t lock;
    pmix_event_t ev;
    bool active;
} pmix_client_timeout_t;

/* timer callback */
static void fin_timeout(int sd, short args, void *cbdata)
{
    pmix_client_timeout_t *tev;
    tev = (pmix_client_timeout_t*)cbdata;

    pmix_output_verbose(2, pmix_client_globals.base_output,
                        "pmix:client finwait timeout fired");
    if (tev->active) {
        tev->active = false;
        PMIX_WAKEUP_THREAD(&tev->lock);
    }
}
/* callback for finalize completion */
static void finwait_cbfunc(struct pmix_peer_t *pr,
                           pmix_ptl_hdr_t *hdr,
                           pmix_buffer_t *buf, void *cbdata)
{
    pmix_client_timeout_t *tev;
    tev = (pmix_client_timeout_t*)cbdata;

    pmix_output_verbose(2, pmix_client_globals.base_output,
                        "pmix:client finwait_cbfunc received");
    if (tev->active) {
        tev->active = false;
        PMIX_WAKEUP_THREAD(&tev->lock);
    }
}

PMIX_EXPORT pmix_status_t PMIx_Finalize(const pmix_info_t info[], size_t ninfo)
{
    pmix_buffer_t *msg;
    pmix_cmd_t cmd = PMIX_FINALIZE_CMD;
    pmix_status_t rc;
    size_t n;
    pmix_client_timeout_t tev;
    struct timeval tv = {2, 0};
    pmix_peer_t *peer;
    int i;

    PMIX_ACQUIRE_THREAD(&pmix_global_lock);
    if (1 != pmix_globals.init_cntr) {
        --pmix_globals.init_cntr;
        PMIX_RELEASE_THREAD(&pmix_global_lock);
        return PMIX_SUCCESS;
    }
    pmix_globals.init_cntr = 0;

    pmix_output_verbose(2, pmix_client_globals.base_output,
                        "%s:%d pmix:client finalize called",
                        pmix_globals.myid.nspace, pmix_globals.myid.rank);

    /* mark that I called finalize */
    pmix_globals.mypeer->finalized = true;

    if ( 0 <= pmix_client_globals.myserver->sd ) {
        /* check to see if we are supposed to execute a
         * blocking fence prior to actually finalizing */
        if (NULL != info && 0 < ninfo) {
            for (n=0; n < ninfo; n++) {
                if (0 == strcmp(PMIX_EMBED_BARRIER, info[n].key)) {
                    if (PMIX_INFO_TRUE(&info[n])) {
                        rc = PMIx_Fence(NULL, 0, NULL, 0);
                        if (PMIX_SUCCESS != rc) {
                            PMIX_ERROR_LOG(rc);
                        }
                    }
                    break;
                }
            }
        }

        /* setup a cmd message to notify the PMIx
         * server that we are normally terminating */
        msg = PMIX_NEW(pmix_buffer_t);
        /* pack the cmd */
        PMIX_BFROPS_PACK(rc, pmix_client_globals.myserver,
                         msg, &cmd, 1, PMIX_COMMAND);
        if (PMIX_SUCCESS != rc) {
            PMIX_ERROR_LOG(rc);
            PMIX_RELEASE(msg);
            PMIX_RELEASE_THREAD(&pmix_global_lock);
            return rc;
        }


        pmix_output_verbose(2, pmix_client_globals.base_output,
                             "%s:%d pmix:client sending finalize sync to server",
                             pmix_globals.myid.nspace, pmix_globals.myid.rank);

        /* setup a timer to protect ourselves should the server be unable
         * to answer for some reason */
        PMIX_CONSTRUCT_LOCK(&tev.lock);
        pmix_event_assign(&tev.ev, pmix_globals.evbase, -1, 0,
                          fin_timeout, &tev);
        tev.active = true;
        PMIX_POST_OBJECT(&tev);
        pmix_event_add(&tev.ev, &tv);
        /* send to the server */
        PMIX_PTL_SEND_RECV(rc, pmix_client_globals.myserver,
                           msg, finwait_cbfunc, (void*)&tev);
        if (PMIX_SUCCESS != rc) {
            PMIX_RELEASE_THREAD(&pmix_global_lock);
            return rc;
        }

        /* wait for the ack to return */
        PMIX_WAIT_THREAD(&tev.lock);
        PMIX_DESTRUCT_LOCK(&tev.lock);
        if (tev.active) {
            pmix_event_del(&tev.ev);
        }

        pmix_output_verbose(2, pmix_client_globals.base_output,
                             "%s:%d pmix:client finalize sync received",
                             pmix_globals.myid.nspace, pmix_globals.myid.rank);
    }

    if (!pmix_globals.external_evbase) {
        /* stop the progress thread, but leave the event base
         * still constructed. This will allow us to safely
         * tear down the infrastructure, including removal
         * of any events objects may be holding */
        (void)pmix_progress_thread_pause(NULL);
    }

    PMIX_LIST_DESTRUCT(&pmix_client_globals.pending_requests);
    for (i=0; i < pmix_client_globals.peers.size; i++) {
        if (NULL != (peer = (pmix_peer_t*)pmix_pointer_array_get_item(&pmix_client_globals.peers, i))) {
            PMIX_RELEASE(peer);
        }
    }
    PMIX_DESTRUCT(&pmix_client_globals.peers);

    if (0 <= pmix_client_globals.myserver->sd) {
        CLOSE_THE_SOCKET(pmix_client_globals.myserver->sd);
    }
    if (NULL != pmix_client_globals.myserver) {
        PMIX_RELEASE(pmix_client_globals.myserver);
    }


    pmix_rte_finalize();
    if (NULL != pmix_globals.mypeer) {
        PMIX_RELEASE(pmix_globals.mypeer);
    }

    PMIX_RELEASE_THREAD(&pmix_global_lock);

    /* finalize the class/object system */
    pmix_class_finalize();

    return PMIX_SUCCESS;
}

PMIX_EXPORT pmix_status_t PMIx_Abort(int flag, const char msg[],
                                     pmix_proc_t procs[], size_t nprocs)
{
    pmix_buffer_t *bfr;
    pmix_cmd_t cmd = PMIX_ABORT_CMD;
    pmix_status_t rc;
    pmix_lock_t reglock;

    pmix_output_verbose(2, pmix_client_globals.base_output,
                        "pmix:client abort called");

    PMIX_ACQUIRE_THREAD(&pmix_global_lock);
    if (pmix_globals.init_cntr <= 0) {
        PMIX_RELEASE_THREAD(&pmix_global_lock);
        return PMIX_ERR_INIT;
    }

    /* if we aren't connected, don't attempt to send */
    if (!pmix_globals.connected) {
        PMIX_RELEASE_THREAD(&pmix_global_lock);
        return PMIX_ERR_UNREACH;
    }
    PMIX_RELEASE_THREAD(&pmix_global_lock);

    /* create a buffer to hold the message */
    bfr = PMIX_NEW(pmix_buffer_t);
    /* pack the cmd */
    PMIX_BFROPS_PACK(rc, pmix_client_globals.myserver,
                     bfr, &cmd, 1, PMIX_COMMAND);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        PMIX_RELEASE(bfr);
        return rc;
    }
    /* pack the status flag */
    PMIX_BFROPS_PACK(rc, pmix_client_globals.myserver,
                     bfr, &flag, 1, PMIX_STATUS);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        PMIX_RELEASE(bfr);
        return rc;
    }
    /* pack the string message - a NULL is okay */
    PMIX_BFROPS_PACK(rc, pmix_client_globals.myserver,
                     bfr, &msg, 1, PMIX_STRING);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        PMIX_RELEASE(bfr);
        return rc;
    }
    /* pack the number of procs */
    PMIX_BFROPS_PACK(rc, pmix_client_globals.myserver,
                     bfr, &nprocs, 1, PMIX_SIZE);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        PMIX_RELEASE(bfr);
        return rc;
    }
    /* pack any provided procs */
    if (0 < nprocs) {
        PMIX_BFROPS_PACK(rc, pmix_client_globals.myserver,
                         bfr, procs, nprocs, PMIX_PROC);
        if (PMIX_SUCCESS != rc) {
            PMIX_ERROR_LOG(rc);
            PMIX_RELEASE(bfr);
            return rc;
        }
    }

    /* send to the server */
    PMIX_CONSTRUCT_LOCK(&reglock);
    PMIX_PTL_SEND_RECV(rc, pmix_client_globals.myserver, bfr,
                       wait_cbfunc, (void*)&reglock);
    if (PMIX_SUCCESS != rc) {
        PMIX_DESTRUCT_LOCK(&reglock);
        return rc;
    }

    /* wait for the release */
     PMIX_WAIT_THREAD(&reglock);
     PMIX_DESTRUCT_LOCK(&reglock);
     return PMIX_SUCCESS;
 }

static void _putfn(int sd, short args, void *cbdata)
{
    pmix_cb_t *cb = (pmix_cb_t*)cbdata;
    pmix_status_t rc;
    pmix_kval_t *kv = NULL;
    uint8_t *tmp;
    size_t len;

    /* need to acquire the cb object from its originating thread */
    PMIX_ACQUIRE_OBJECT(cb);

    /* no need to push info that starts with "pmix" as that is
     * info we would have been provided at startup */
    if (0 == strncmp(cb->key, "pmix", 4)) {
        rc = PMIX_SUCCESS;
        goto done;
    }

    /* setup to xfer the data */
    kv = PMIX_NEW(pmix_kval_t);
    kv->key = strdup(cb->key);  // need to copy as the input belongs to the user
    kv->value = (pmix_value_t*)malloc(sizeof(pmix_value_t));
    if (PMIX_STRING_SIZE_CHECK(cb->value)) {
        /* compress large strings */
        if (pmix_util_compress_string(cb->value->data.string, &tmp, &len)) {
            if (NULL == tmp) {
                PMIX_ERROR_LOG(PMIX_ERR_NOMEM);
                rc = PMIX_ERR_NOMEM;
                PMIX_ERROR_LOG(rc);
                goto done;
            }
            kv->value->type = PMIX_COMPRESSED_STRING;
            kv->value->data.bo.bytes = (char*)tmp;
            kv->value->data.bo.size = len;
            rc = PMIX_SUCCESS;
        } else {
            PMIX_BFROPS_VALUE_XFER(rc, pmix_globals.mypeer,
                                   kv->value, cb->value);
        }
    } else {
        PMIX_BFROPS_VALUE_XFER(rc, pmix_globals.mypeer,
                               kv->value, cb->value);
    }
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        goto done;
    }

    /* store it */
    PMIX_GDS_STORE_KV(rc, pmix_globals.mypeer,
                       &pmix_globals.myid,
                       cb->scope, kv);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
    }

    /* mark that fresh values have been stored so we know
     * to commit them later */
    pmix_globals.commits_pending = true;

  done:
    if (NULL != kv) {
        PMIX_RELEASE(kv);  // maintain accounting
    }
    cb->pstatus = rc;
    /* post the data so the receiving thread can acquire it */
    PMIX_POST_OBJECT(cb);
    PMIX_WAKEUP_THREAD(&cb->lock);
}

PMIX_EXPORT pmix_status_t PMIx_Put(pmix_scope_t scope,
                                   const pmix_key_t key,
                                   pmix_value_t *val)
{
    pmix_cb_t *cb;
    pmix_status_t rc;

    pmix_output_verbose(2, pmix_client_globals.base_output,
                        "pmix: executing put for key %s type %d",
                        key, val->type);

    PMIX_ACQUIRE_THREAD(&pmix_global_lock);
    if (pmix_globals.init_cntr <= 0) {
        PMIX_RELEASE_THREAD(&pmix_global_lock);
        return PMIX_ERR_INIT;
    }
    PMIX_RELEASE_THREAD(&pmix_global_lock);

    /* create a callback object */
    cb = PMIX_NEW(pmix_cb_t);
    cb->scope = scope;
    cb->key = (char*)key;
    cb->value = val;

    /* pass this into the event library for thread protection */
    PMIX_THREADSHIFT(cb, _putfn);

    /* wait for the result */
    PMIX_WAIT_THREAD(&cb->lock);
    rc = cb->pstatus;
    PMIX_RELEASE(cb);

    return rc;
}

static void _commitfn(int sd, short args, void *cbdata)
{
    pmix_cb_t *cb = (pmix_cb_t*)cbdata;
    pmix_status_t rc;
    pmix_scope_t scope;
    pmix_buffer_t *msgout, bkt;
    pmix_cmd_t cmd=PMIX_COMMIT_CMD;
    pmix_kval_t *kv, *kvn;

    /* need to acquire the cb object from its originating thread */
    PMIX_ACQUIRE_OBJECT(cb);

    msgout = PMIX_NEW(pmix_buffer_t);
    /* pack the cmd */
    PMIX_BFROPS_PACK(rc, pmix_client_globals.myserver,
                     msgout, &cmd, 1, PMIX_COMMAND);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        PMIX_RELEASE(msgout);
        goto error;
    }

    /* if we haven't already done it, ensure we have committed our values */
    if (pmix_globals.commits_pending) {
        /* fetch and pack the local values */
        scope = PMIX_LOCAL;
        /* allow the GDS module to pass us this info
         * as a local connection as this data would
         * only go to another local client */
        cb->proc = &pmix_globals.myid;
        cb->scope = scope;
        cb->copy = false;
        PMIX_GDS_FETCH_KV(rc, pmix_globals.mypeer, cb);
        if (PMIX_SUCCESS == rc) {
            PMIX_BFROPS_PACK(rc, pmix_client_globals.myserver,
                             msgout, &scope, 1, PMIX_SCOPE);
            if (PMIX_SUCCESS != rc) {
                PMIX_ERROR_LOG(rc);
                PMIX_RELEASE(msgout);
                goto error;
            }
            PMIX_CONSTRUCT(&bkt, pmix_buffer_t);
            PMIX_LIST_FOREACH_SAFE(kv, kvn, &cb->kvs, pmix_kval_t) {
                PMIX_BFROPS_PACK(rc, pmix_client_globals.myserver,
                                 &bkt, kv, 1, PMIX_KVAL);
                if (PMIX_SUCCESS != rc) {
                    PMIX_ERROR_LOG(rc);
                    PMIX_DESTRUCT(&bkt);
                    PMIX_RELEASE(msgout);
                    goto error;
                }
                pmix_list_remove_item(&cb->kvs, &kv->super);
                PMIX_RELEASE(kv);
            }
            /* now pack the result */
            PMIX_BFROPS_PACK(rc, pmix_client_globals.myserver,
                             msgout, &bkt, 1, PMIX_BUFFER);
            PMIX_DESTRUCT(&bkt);
            if (PMIX_SUCCESS != rc) {
                PMIX_ERROR_LOG(rc);
                PMIX_RELEASE(msgout);
                goto error;
            }
        }

        /* fetch and pack the remote values */
        scope = PMIX_REMOTE;
        /* we need real copies here as this data will
         * go to remote procs - so a connection will
         * not suffice */
        cb->proc = &pmix_globals.myid;
        cb->scope = scope;
        cb->copy = true;
        PMIX_GDS_FETCH_KV(rc, pmix_globals.mypeer, cb);
        if (PMIX_SUCCESS == rc) {
            PMIX_BFROPS_PACK(rc, pmix_client_globals.myserver,
                             msgout, &scope, 1, PMIX_SCOPE);
            if (PMIX_SUCCESS != rc) {
                PMIX_ERROR_LOG(rc);
                PMIX_RELEASE(msgout);
                goto error;
            }
            PMIX_CONSTRUCT(&bkt, pmix_buffer_t);
            PMIX_LIST_FOREACH_SAFE(kv, kvn, &cb->kvs, pmix_kval_t) {
                PMIX_BFROPS_PACK(rc, pmix_client_globals.myserver,
                                 &bkt, kv, 1, PMIX_KVAL);
                if (PMIX_SUCCESS != rc) {
                    PMIX_ERROR_LOG(rc);
                    PMIX_DESTRUCT(&bkt);
                    PMIX_RELEASE(msgout);
                    goto error;
                }
                pmix_list_remove_item(&cb->kvs, &kv->super);
                PMIX_RELEASE(kv);
            }
            /* now pack the result */
            PMIX_BFROPS_PACK(rc, pmix_client_globals.myserver,
                             msgout, &bkt, 1, PMIX_BUFFER);
            PMIX_DESTRUCT(&bkt);
            if (PMIX_SUCCESS != rc) {
                PMIX_ERROR_LOG(rc);
                PMIX_RELEASE(msgout);
                goto error;
            }
        }

        /* record that all committed data to-date has been sent */
        pmix_globals.commits_pending = false;
    }

    /* always send, even if we have nothing to contribute, so the server knows
     * that we contributed whatever we had */
    PMIX_PTL_SEND_RECV(rc, pmix_client_globals.myserver, msgout,
                       wait_cbfunc, (void*)&cb->lock);
    if (PMIX_SUCCESS == rc) {
        /* we should wait for the callback, so don't
         * modify the active flag */
        cb->pstatus = PMIX_SUCCESS;
        return;
    }

  error:
    cb->pstatus = rc;
    /* post the data so the receiving thread can acquire it */
    PMIX_POST_OBJECT(cb);
    PMIX_WAKEUP_THREAD(&cb->lock);
 }

 PMIX_EXPORT pmix_status_t PMIx_Commit(void)
 {
    pmix_cb_t *cb;
    pmix_status_t rc;

    PMIX_ACQUIRE_THREAD(&pmix_global_lock);
    if (pmix_globals.init_cntr <= 0) {
        PMIX_RELEASE_THREAD(&pmix_global_lock);
        return PMIX_ERR_INIT;
    }

    /* if we are a server, or we aren't connected, don't attempt to send */
    if (PMIX_PROC_IS_SERVER(pmix_globals.mypeer)) {
        PMIX_RELEASE_THREAD(&pmix_global_lock);
        return PMIX_SUCCESS;  // not an error
    }
    if (!pmix_globals.connected) {
        PMIX_RELEASE_THREAD(&pmix_global_lock);
        return PMIX_ERR_UNREACH;
    }
    PMIX_RELEASE_THREAD(&pmix_global_lock);

    /* create a callback object */
    cb = PMIX_NEW(pmix_cb_t);
    /* pass this into the event library for thread protection */
    PMIX_THREADSHIFT(cb, _commitfn);

    /* wait for the result */
    PMIX_WAIT_THREAD(&cb->lock);
    rc = cb->pstatus;
    PMIX_RELEASE(cb);

    return rc;
}

static void _resolve_peers(int sd, short args, void *cbdata)
{
    pmix_cb_t *cb = (pmix_cb_t*)cbdata;

    cb->status = pmix_preg.resolve_peers(cb->key, cb->pname.nspace,
                                         &cb->procs, &cb->nprocs);
    /* post the data so the receiving thread can acquire it */
    PMIX_POST_OBJECT(cb);
    PMIX_WAKEUP_THREAD(&cb->lock);
}

/* need to thread-shift this request */
PMIX_EXPORT pmix_status_t PMIx_Resolve_peers(const char *nodename,
                                             const pmix_nspace_t nspace,
                                             pmix_proc_t **procs, size_t *nprocs)
{
    pmix_cb_t *cb;
    pmix_status_t rc;
    pmix_proc_t proc;

    PMIX_ACQUIRE_THREAD(&pmix_global_lock);
    if (pmix_globals.init_cntr <= 0) {
        PMIX_RELEASE_THREAD(&pmix_global_lock);
        return PMIX_ERR_INIT;
    }
    PMIX_RELEASE_THREAD(&pmix_global_lock);


    cb = PMIX_NEW(pmix_cb_t);
    cb->key = (char*)nodename;
    cb->pname.nspace = strdup(nspace);

    PMIX_THREADSHIFT(cb, _resolve_peers);

    /* wait for the result */
    PMIX_WAIT_THREAD(&cb->lock);

    /* if the nspace wasn't found, then we need to
     * ask the server for that info */
    if (PMIX_ERR_INVALID_NAMESPACE == cb->status) {
        pmix_strncpy(proc.nspace, nspace, PMIX_MAX_NSLEN);
        proc.rank = PMIX_RANK_WILDCARD;
        /* any key will suffice as it will bring down
         * the entire data blob */
        rc = PMIx_Get(&proc, PMIX_UNIV_SIZE, NULL, 0, NULL);
        if (PMIX_SUCCESS != rc) {
            PMIX_RELEASE(cb);
            return rc;
        }
        /* retry the fetch */
        cb->lock.active = true;
        PMIX_THREADSHIFT(cb, _resolve_peers);
        PMIX_WAIT_THREAD(&cb->lock);
    }
    *procs = cb->procs;
    *nprocs = cb->nprocs;

    rc = cb->status;
    PMIX_RELEASE(cb);
    return rc;
}

static void _resolve_nodes(int fd, short args, void *cbdata)
{
    pmix_cb_t *cb = (pmix_cb_t*)cbdata;
    char *regex, **names;

    /* get a regular expression describing the PMIX_NODE_MAP */
    cb->status = pmix_preg.resolve_nodes(cb->pname.nspace, &regex);
    if (PMIX_SUCCESS == cb->status) {
        /* parse it into an argv array of names */
        cb->status = pmix_preg.parse_nodes(regex, &names);
        if (PMIX_SUCCESS == cb->status) {
            /* assemble it into a comma-delimited list */
            cb->key = pmix_argv_join(names, ',');
            pmix_argv_free(names);
        } else {
            free(regex);
        }
    }
    /* post the data so the receiving thread can acquire it */
    PMIX_POST_OBJECT(cb);
    PMIX_WAKEUP_THREAD(&cb->lock);
}

/* need to thread-shift this request */
PMIX_EXPORT pmix_status_t PMIx_Resolve_nodes(const pmix_nspace_t nspace, char **nodelist)
{
    pmix_cb_t *cb;
    pmix_status_t rc;
    pmix_proc_t proc;

    PMIX_ACQUIRE_THREAD(&pmix_global_lock);
    if (pmix_globals.init_cntr <= 0) {
        PMIX_RELEASE_THREAD(&pmix_global_lock);
        return PMIX_ERR_INIT;
    }
    PMIX_RELEASE_THREAD(&pmix_global_lock);

    cb = PMIX_NEW(pmix_cb_t);
    cb->pname.nspace = strdup(nspace);

    PMIX_THREADSHIFT(cb, _resolve_nodes);

    /* wait for the result */
    PMIX_WAIT_THREAD(&cb->lock);

    /* if the nspace wasn't found, then we need to
     * ask the server for that info */
    if (PMIX_ERR_INVALID_NAMESPACE == cb->status) {
        pmix_strncpy(proc.nspace, nspace, PMIX_MAX_NSLEN);
        proc.rank = PMIX_RANK_WILDCARD;
        /* any key will suffice as it will bring down
         * the entire data blob */
        rc = PMIx_Get(&proc, PMIX_UNIV_SIZE, NULL, 0, NULL);
        if (PMIX_SUCCESS != rc) {
            PMIX_RELEASE(cb);
            return rc;
        }
        /* retry the fetch */
        cb->lock.active = true;
        PMIX_THREADSHIFT(cb, _resolve_nodes);
        PMIX_WAIT_THREAD(&cb->lock);
    }
    /* the string we want is in the key field */
    *nodelist = cb->key;

    rc = cb->status;
    PMIX_RELEASE(cb);
    return rc;
}
