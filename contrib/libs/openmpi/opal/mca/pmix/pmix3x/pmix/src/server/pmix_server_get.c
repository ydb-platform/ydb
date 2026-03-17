/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2014-2018 Intel, Inc. All rights reserved.
 * Copyright (c) 2014-2015 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2014-2015 Artem Y. Polyakov <artpol84@gmail.com>.
 *                         All rights reserved.
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

#include <pmix_server.h>
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
#include PMIX_EVENT_HEADER

#include "src/class/pmix_list.h"
#include "src/mca/bfrops/bfrops.h"
#include "src/mca/gds/gds.h"
#include "src/util/argv.h"
#include "src/util/error.h"
#include "src/util/output.h"
#include "src/util/pmix_environ.h"

#include "pmix_server_ops.h"

extern pmix_server_module_t pmix_host_server;

typedef struct {
    pmix_object_t super;
    pmix_event_t ev;
    volatile bool active;
    pmix_status_t status;
    const char *data;
    size_t ndata;
    pmix_dmdx_local_t *lcd;
    pmix_release_cbfunc_t relcbfunc;
    void *cbdata;
} pmix_dmdx_reply_caddy_t;
static void dcd_con(pmix_dmdx_reply_caddy_t *p)
{
    p->status = PMIX_ERROR;
    p->ndata = 0;
    p->lcd = NULL;
    p->relcbfunc = NULL;
    p->cbdata = NULL;
}
PMIX_CLASS_INSTANCE(pmix_dmdx_reply_caddy_t,
                   pmix_object_t, dcd_con, NULL);


static void dmdx_cbfunc(pmix_status_t status, const char *data,
                        size_t ndata, void *cbdata,
                        pmix_release_cbfunc_t relfn, void *relcbdata);
static pmix_status_t _satisfy_request(pmix_namespace_t *ns, pmix_rank_t rank,
                                      pmix_server_caddy_t *cd,
                                      pmix_modex_cbfunc_t cbfunc, void *cbdata, bool *scope);
static pmix_status_t create_local_tracker(char nspace[], pmix_rank_t rank,
                                          pmix_info_t info[], size_t ninfo,
                                          pmix_modex_cbfunc_t cbfunc,
                                          void *cbdata,
                                          pmix_dmdx_local_t **lcd,
                                          pmix_dmdx_request_t **rq);

static void get_timeout(int sd, short args, void *cbdata);


/* declare a function whose sole purpose is to
 * free data that we provided to our host server
 * when servicing dmodex requests */
static void relfn(void *cbdata)
{
    char *data = (char*)cbdata;
    if (NULL != data) {
        free(data);
    }
}


pmix_status_t pmix_server_get(pmix_buffer_t *buf,
                              pmix_modex_cbfunc_t cbfunc,
                              void *cbdata)
{
    pmix_server_caddy_t *cd = (pmix_server_caddy_t*)cbdata;
    int32_t cnt;
    pmix_status_t rc;
    pmix_rank_t rank;
    char *cptr;
    char nspace[PMIX_MAX_NSLEN+1];
    pmix_namespace_t *ns, *nptr;
    pmix_info_t *info=NULL;
    size_t ninfo=0;
    pmix_dmdx_local_t *lcd;
    pmix_dmdx_request_t *req;
    bool local;
    bool localonly = false;
    struct timeval tv = {0, 0};
    pmix_buffer_t pbkt, pkt;
    pmix_byte_object_t bo;
    pmix_cb_t cb;
    pmix_proc_t proc;
    char *data;
    size_t sz, n;
    pmix_peer_t *peer;

    pmix_output_verbose(2, pmix_server_globals.get_output,
                        "recvd GET");

    /* setup */
    memset(nspace, 0, sizeof(nspace));

    /* retrieve the nspace and rank of the requested proc */
    cnt = 1;
    PMIX_BFROPS_UNPACK(rc, cd->peer, buf, &cptr, &cnt, PMIX_STRING);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        return rc;
    }
    pmix_strncpy(nspace, cptr, PMIX_MAX_NSLEN);
    free(cptr);
    cnt = 1;
    PMIX_BFROPS_UNPACK(rc, cd->peer, buf, &rank, &cnt, PMIX_PROC_RANK);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        return rc;
    }
    /* retrieve any provided info structs */
    cnt = 1;
    PMIX_BFROPS_UNPACK(rc, cd->peer, buf, &ninfo, &cnt, PMIX_SIZE);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        return rc;
    }
    if (0 < ninfo) {
        PMIX_INFO_CREATE(info, ninfo);
        if (NULL == info) {
            PMIX_ERROR_LOG(PMIX_ERR_NOMEM);
            return PMIX_ERR_NOMEM;
        }
        cnt = ninfo;
        PMIX_BFROPS_UNPACK(rc, cd->peer, buf, info, &cnt, PMIX_INFO);
        if (PMIX_SUCCESS != rc) {
            PMIX_ERROR_LOG(rc);
            PMIX_INFO_FREE(info, ninfo);
            return rc;
        }
    }

    /* search for directives we can deal with here */
    for (n=0; n < ninfo; n++) {
        if (0 == strncmp(info[n].key, PMIX_IMMEDIATE, PMIX_MAX_KEYLEN)) {
            /* just check our own data - don't wait
             * or request it from someone else */
            localonly = PMIX_INFO_TRUE(&info[n]);
        } else if (0 == strncmp(info[n].key, PMIX_TIMEOUT, PMIX_MAX_KEYLEN)) {
            tv.tv_sec = info[n].value.data.uint32;
        }
    }

    /* find the nspace object for this client */
    nptr = NULL;
    PMIX_LIST_FOREACH(ns, &pmix_server_globals.nspaces, pmix_namespace_t) {
        if (0 == strcmp(nspace, ns->nspace)) {
            nptr = ns;
            break;
        }
    }

    pmix_output_verbose(2, pmix_server_globals.get_output,
                        "%s:%d EXECUTE GET FOR %s:%d ON BEHALF OF %s:%d",
                        pmix_globals.myid.nspace,
                        pmix_globals.myid.rank, nspace, rank,
                        cd->peer->info->pname.nspace,
                        cd->peer->info->pname.rank);

    /* This call flows upward from a local client If we don't
     * know about this nspace, then it cannot refer to the
     * nspace of the requestor - i.e., they aren't asking
     * about one of their peers. There are two reasons why we
     * might not know about this nspace at this time:
     *
     * (a) we don't host any local procs from this nspace, and
     *     so the local RM didn't tell us about it. We will have
     *     to request the information from it.
     *
     * (b) a race condition where the other job hasn't registered
     *     its nspace yet. This begs the question as to how the
     *     requestor got the nspace name in the first place!
     *     However, there _may_ be some path whereby that could
     *     happen, so we try to deal with it here.
     *
     * Either way, we are going to have to request the info from
     * the host RM. Since we are hopeful of getting an answer,
     * we add the nspace to our list of known nspaces so the
     * info has a "landing zone" upon return */

    if (NULL == nptr) {
        if (localonly) {
            /* the user doesn't want us to look for the info,
             * so we simply return at this point */
            return PMIX_ERR_NOT_FOUND;
        }
        /* this is for an nspace we don't know about yet, so
         * record the request for data from this process and
         * give the host server a chance to tell us about it.
         * The cbdata passed here is the pmix_server_caddy_t
         * we were passed - it contains the pmix_peer_t of
         * the original requestor so they will get the data
         * back when we receive it */
        rc = create_local_tracker(nspace, rank,
                                  info, ninfo,
                                  cbfunc, cbdata, &lcd, &req);
        if (PMIX_ERR_NOMEM == rc) {
            PMIX_INFO_FREE(info, ninfo);
            return rc;
        }
        if (PMIX_SUCCESS == rc) {
            /* if they specified a timeout for this specific
             * request, set it up now */
            if (0 < tv.tv_sec) {
                pmix_event_evtimer_set(pmix_globals.evbase, &req->ev,
                                       get_timeout, req);
                pmix_event_evtimer_add(&req->ev, &tv);
                req->event_active = true;
            }
            /* we already asked for this info - no need to
             * do it again */
            return PMIX_SUCCESS;
        }
        /* only other return code is NOT_FOUND, indicating that
         * we created a new tracker */

        /* Its possible there will be no local processes on this
         * host, so lets ask for this explicitly.  There can
         * be a race condition here if this information shows
         * up on its own, but at worst the direct modex
         * will simply overwrite the info later */
        if (NULL != pmix_host_server.direct_modex) {
            rc = pmix_host_server.direct_modex(&lcd->proc, info, ninfo, dmdx_cbfunc, lcd);
            if (PMIX_SUCCESS != rc) {
                PMIX_INFO_FREE(info, ninfo);
                pmix_list_remove_item(&pmix_server_globals.local_reqs, &lcd->super);
                PMIX_RELEASE(lcd);
                return rc;
            }
            /* if they specified a timeout for this specific
             * request, set it up now */
            if (0 < tv.tv_sec) {
                pmix_event_evtimer_set(pmix_globals.evbase, &req->ev,
                                       get_timeout, req);
                pmix_event_evtimer_add(&req->ev, &tv);
                req->event_active = true;
            }
        } else {
        /* if we don't have direct modex feature, just respond with "not found" */
            PMIX_INFO_FREE(info, ninfo);
            pmix_list_remove_item(&pmix_server_globals.local_reqs, &lcd->super);
            PMIX_RELEASE(lcd);
            return PMIX_ERR_NOT_FOUND;
        }

        return PMIX_SUCCESS;
    }

    /* this nspace is known, so we can process the request.
     * if the rank is wildcard, then they are asking for the
     * job-level info for this nspace - provide it */
    if (PMIX_RANK_WILDCARD == rank) {
        /* see if we have the job-level info - we won't have it
         * if we have no local procs and haven't already asked
         * for it, so there is no guarantee we have it */
        data = NULL;
        sz = 0;
        pmix_strncpy(proc.nspace, nspace, PMIX_MAX_NSLEN);
        proc.rank = PMIX_RANK_WILDCARD;
        /* if we have local procs for this nspace, then we
         * can retrieve the info from that GDS. Otherwise,
         * we need to retrieve it from our own */
        PMIX_CONSTRUCT(&cb, pmix_cb_t);
        peer = pmix_globals.mypeer;
        /* this data is for a local client, so give the gds the
         * option of returning a complete copy of the data,
         * or returning a pointer to local storage */
        cb.proc = &proc;
        cb.scope = PMIX_SCOPE_UNDEF;
        cb.copy = false;
        PMIX_GDS_FETCH_KV(rc, peer, &cb);
        if (PMIX_SUCCESS != rc) {
            PMIX_DESTRUCT(&cb);
            return rc;
        }
        PMIX_CONSTRUCT(&pkt, pmix_buffer_t);
        /* assemble the provided data into a byte object */
        PMIX_GDS_ASSEMB_KVS_REQ(rc, peer, &proc, &cb.kvs, &pkt, cd);
        if (PMIX_SUCCESS != rc) {
            PMIX_ERROR_LOG(rc);
            PMIX_DESTRUCT(&cb);
            return rc;
        }
        PMIX_UNLOAD_BUFFER(&pkt, bo.bytes, bo.size);
        PMIX_DESTRUCT(&pkt);
        /* pack it into the payload */
        PMIX_CONSTRUCT(&pbkt, pmix_buffer_t);
        PMIX_BFROPS_PACK(rc, cd->peer, &pbkt, &bo, 1, PMIX_BYTE_OBJECT);
        free(bo.bytes);
        if (PMIX_SUCCESS != rc) {
            PMIX_ERROR_LOG(rc);
            PMIX_DESTRUCT(&pbkt);
            PMIX_DESTRUCT(&cb);
            return rc;
        }
        /* unload the resulting payload */
        PMIX_UNLOAD_BUFFER(&pbkt, data, sz);
        PMIX_DESTRUCT(&pbkt);
        /* call the internal callback function - it will
         * release the cbdata */
        cbfunc(PMIX_SUCCESS, data, sz, cbdata, relfn, data);
        /* return success so the server doesn't duplicate
         * the release of cbdata */
        return PMIX_SUCCESS;
    }

    /* We have to wait for all local clients to be registered before
     * we can know whether this request is for data from a local or a
     * remote client because one client might ask for data about another
     * client that the host RM hasn't told us about yet. Fortunately,
     * we do know how many clients to expect, so first check to see if
     * all clients have been registered with us */
     if (!nptr->all_registered) {
        pmix_output_verbose(2, pmix_server_globals.get_output,
                            "%s:%d NSPACE %s not all registered",
                            pmix_globals.myid.nspace,
                            pmix_globals.myid.rank, nspace);

        if (localonly) {
            /* the client asked that we not wait, so return now */
            pmix_output_verbose(2, pmix_server_globals.get_output,
                                "%s:%d CLIENT REQUESTED IMMEDIATE",
                                pmix_globals.myid.nspace,
                                pmix_globals.myid.rank);
            return PMIX_ERR_NOT_FOUND;
        }
        /* we cannot do anything further, so just track this request
         * for now */
        rc = create_local_tracker(nspace, rank, info, ninfo,
                                  cbfunc, cbdata, &lcd, &req);
        if (PMIX_ERR_NOMEM == rc) {
            PMIX_INFO_FREE(info, ninfo);
            return rc;
        }
        pmix_output_verbose(2, pmix_server_globals.get_output,
                            "%s:%d TRACKER CREATED - WAITING",
                            pmix_globals.myid.nspace,
                            pmix_globals.myid.rank);
        /* if they specified a timeout, set it up now */
        if (0 < tv.tv_sec) {
            pmix_event_evtimer_set(pmix_globals.evbase, &req->ev,
                                   get_timeout, req);
            pmix_event_evtimer_add(&req->ev, &tv);
            req->event_active = true;
        }
        /* the peer object has been added to the new lcd tracker,
         * so return success here */
        return PMIX_SUCCESS;
    }

    /* if everyone has registered, see if we already have this data */
    rc = _satisfy_request(nptr, rank, cd, cbfunc, cbdata, &local);
    if( PMIX_SUCCESS == rc ){
        /* request was successfully satisfied */
        PMIX_INFO_FREE(info, ninfo);
        /* return success as the satisfy_request function
         * calls the cbfunc for us, and it will have
         * released the cbdata object */
        return PMIX_SUCCESS;
    }

    pmix_output_verbose(2, pmix_server_globals.get_output,
                        "%s:%d DATA NOT FOUND",
                        pmix_globals.myid.nspace,
                        pmix_globals.myid.rank);

    /* If we get here, then we don't have the data at this time. If
     * the user doesn't want to look for it, then we are done */
    if (localonly) {
        pmix_output_verbose(2, pmix_server_globals.get_output,
                            "%s:%d CLIENT REQUESTED IMMEDIATE",
                            pmix_globals.myid.nspace,
                            pmix_globals.myid.rank);
        return PMIX_ERR_NOT_FOUND;
    }

    /* Check to see if we already have a pending request for the data - if
     * we do, then we can just wait for it to arrive */
    rc = create_local_tracker(nspace, rank, info, ninfo,
                              cbfunc, cbdata, &lcd, &req);
    if (PMIX_ERR_NOMEM == rc || NULL == lcd) {
        /* we have a problem */
        PMIX_INFO_FREE(info, ninfo);
        return PMIX_ERR_NOMEM;
    }
    /* if they specified a timeout, set it up now */
    if (0 < tv.tv_sec) {
        pmix_event_evtimer_set(pmix_globals.evbase, &req->ev,
                               get_timeout, req);
        pmix_event_evtimer_add(&req->ev, &tv);
        req->event_active = true;
    }
    if (PMIX_SUCCESS == rc) {
       /* we are already waiting for the data - nothing more
        * for us to do as the function added the new request
        * to the tracker for us */
       return PMIX_SUCCESS;
    }

    /* Getting here means that we didn't already have a request for
     * for data pending, and so we created a new tracker for this
     * request. We know the identity of all our local clients, so
     * if this is one, then we have nothing further to do - we will
     * fulfill the request once the process commits its data */
    if (local) {
        return PMIX_SUCCESS;
    }

    /* this isn't a local client of ours, so we need to ask the host
     * resource manager server to please get the info for us from
     * whomever is hosting the target process */
    if (NULL != pmix_host_server.direct_modex) {
        rc = pmix_host_server.direct_modex(&lcd->proc, info, ninfo, dmdx_cbfunc, lcd);
        if (PMIX_SUCCESS != rc) {
            /* may have a function entry but not support the request */
            PMIX_INFO_FREE(info, ninfo);
            pmix_list_remove_item(&pmix_server_globals.local_reqs, &lcd->super);
            PMIX_RELEASE(lcd);
        }
    } else {
        pmix_output_verbose(2, pmix_server_globals.get_output,
                            "%s:%d NO SERVER SUPPORT",
                            pmix_globals.myid.nspace,
                            pmix_globals.myid.rank);
        /* if we don't have direct modex feature, just respond with "not found" */
        PMIX_INFO_FREE(info, ninfo);
        pmix_list_remove_item(&pmix_server_globals.local_reqs, &lcd->super);
        PMIX_RELEASE(lcd);
        rc = PMIX_ERR_NOT_FOUND;
    }

    return rc;
}

static pmix_status_t create_local_tracker(char nspace[], pmix_rank_t rank,
                                          pmix_info_t info[], size_t ninfo,
                                          pmix_modex_cbfunc_t cbfunc,
                                          void *cbdata,
                                          pmix_dmdx_local_t **ld,
                                          pmix_dmdx_request_t **rq)
{
    pmix_dmdx_local_t *lcd, *cd;
    pmix_dmdx_request_t *req;
    pmix_status_t rc;

    /* define default */
    *ld = NULL;
    *rq = NULL;

    /* see if we already have an existing request for data
     * from this namespace/rank */
    lcd = NULL;
    PMIX_LIST_FOREACH(cd, &pmix_server_globals.local_reqs, pmix_dmdx_local_t) {
        if (0 != strncmp(nspace, cd->proc.nspace, PMIX_MAX_NSLEN) ||
                rank != cd->proc.rank ) {
            continue;
        }
        lcd = cd;
        break;
    }
    if (NULL != lcd) {
        /* we already have a request, so just track that someone
         * else wants data from the same target */
        rc = PMIX_SUCCESS; // indicates we found an existing request
        goto complete;
    }
    /* we do not have an existing request, so let's create
     * one and add it to our list */
    lcd = PMIX_NEW(pmix_dmdx_local_t);
    if (NULL == lcd){
        return PMIX_ERR_NOMEM;
    }
    pmix_strncpy(lcd->proc.nspace, nspace, PMIX_MAX_NSLEN);
    lcd->proc.rank = rank;
    lcd->info = info;
    lcd->ninfo = ninfo;
    pmix_list_append(&pmix_server_globals.local_reqs, &lcd->super);
    rc = PMIX_ERR_NOT_FOUND;  // indicates that we created a new request tracker

  complete:
    /* track this specific requestor so we return the
     * data to them */
    req = PMIX_NEW(pmix_dmdx_request_t);
    if (NULL == req) {
        *ld = lcd;
        return PMIX_ERR_NOMEM;
    }
    PMIX_RETAIN(lcd);
    req->lcd = lcd;
    req->cbfunc = cbfunc;
    req->cbdata = cbdata;
    pmix_list_append(&lcd->loc_reqs, &req->super);
    *ld = lcd;
    *rq = req;
    return rc;
}

void pmix_pending_nspace_requests(pmix_namespace_t *nptr)
{
    pmix_dmdx_local_t *cd, *cd_next;
    pmix_status_t rc;

    /* Now that we know all local ranks, go along request list and ask for remote data
     * for the non-local ranks, and resolve all pending requests for local procs
     * that were waiting for registration to complete
     */
    PMIX_LIST_FOREACH_SAFE(cd, cd_next, &pmix_server_globals.local_reqs, pmix_dmdx_local_t) {
        pmix_rank_info_t *info;
        bool found = false;

        if (0 != strncmp(nptr->nspace, cd->proc.nspace, PMIX_MAX_NSLEN) ) {
            continue;
        }

        PMIX_LIST_FOREACH(info, &nptr->ranks, pmix_rank_info_t) {
            if (info->pname.rank == cd->proc.rank) {
                found = true;  // we will satisy this request upon commit from new proc
                break;
            }
        }

        /* if not found - this is remote process and we need to send
         * corresponding direct modex request */
        if (!found){
            rc = PMIX_ERR_NOT_SUPPORTED;
            if (NULL != pmix_host_server.direct_modex){
                rc = pmix_host_server.direct_modex(&cd->proc, cd->info, cd->ninfo, dmdx_cbfunc, cd);
            }
            if (PMIX_SUCCESS != rc) {
                pmix_dmdx_request_t *req, *req_next;
                PMIX_LIST_FOREACH_SAFE(req, req_next, &cd->loc_reqs, pmix_dmdx_request_t) {
                    req->cbfunc(PMIX_ERR_NOT_FOUND, NULL, 0, req->cbdata, NULL, NULL);
                    pmix_list_remove_item(&cd->loc_reqs, &req->super);
                    PMIX_RELEASE(req);
                }
                pmix_list_remove_item(&pmix_server_globals.local_reqs, &cd->super);
                PMIX_RELEASE(cd);
            }
        }
    }
}

static pmix_status_t _satisfy_request(pmix_namespace_t *nptr, pmix_rank_t rank,
                                      pmix_server_caddy_t *cd,
                                      pmix_modex_cbfunc_t cbfunc,
                                      void *cbdata, bool *local)
{
    pmix_status_t rc;
    bool found = false;
    pmix_buffer_t pbkt, pkt;
    pmix_rank_info_t *iptr;
    pmix_proc_t proc;
    pmix_cb_t cb;
    pmix_peer_t *peer = NULL;
    pmix_byte_object_t bo;
    char *data = NULL;
    size_t sz = 0;
    pmix_scope_t scope = PMIX_SCOPE_UNDEF;

    pmix_output_verbose(2, pmix_server_globals.get_output,
                        "%s:%d SATISFY REQUEST CALLED",
                        pmix_globals.myid.nspace,
                        pmix_globals.myid.rank);

    /* check to see if this data already has been
     * obtained as a result of a prior direct modex request from
     * a remote peer, or due to data from a local client
     * having been committed */
    PMIX_CONSTRUCT(&pbkt, pmix_buffer_t);
    pmix_strncpy(proc.nspace, nptr->nspace, PMIX_MAX_NSLEN);

    /* if we have local clients of this nspace, then we use
     * the corresponding GDS to retrieve the data. Otherwise,
     * the data will have been stored under our GDS */
    if (0 < nptr->nlocalprocs) {
        if (local) {
            *local = true;
        }
        if (PMIX_RANK_WILDCARD != rank) {
            peer = NULL;
            /* see if the requested rank is local */
            PMIX_LIST_FOREACH(iptr, &nptr->ranks, pmix_rank_info_t) {
                if (rank == iptr->pname.rank) {
                    scope = PMIX_LOCAL;
                    if (0 <= iptr->peerid) {
                        peer = (pmix_peer_t*)pmix_pointer_array_get_item(&pmix_server_globals.clients, iptr->peerid);
                    }
                    if (NULL == peer) {
                        /* this rank has not connected yet, so this request needs to be held */
                        return PMIX_ERR_NOT_FOUND;
                    }
                    break;
                }
            }
            if (PMIX_LOCAL != scope)  {
                /* this must be a remote rank */
                if (local) {
                    *local = false;
                }
                scope = PMIX_REMOTE;
                peer = pmix_globals.mypeer;
            }
        }
    } else {
        if (local) {
            *local = false;
        }
        peer = pmix_globals.mypeer;
        scope = PMIX_REMOTE;
    }

    /* if they are asking about a rank from an nspace different
     * from their own, or they gave a rank of "wildcard", then
     * include a copy of the job-level info */
    if (PMIX_RANK_WILDCARD == rank ||
        0 != strncmp(nptr->nspace, cd->peer->info->pname.nspace, PMIX_MAX_NSLEN)) {
        proc.rank = PMIX_RANK_WILDCARD;
        PMIX_CONSTRUCT(&cb, pmix_cb_t);
        /* this data is requested by a local client, so give the gds the option
         * of returning a copy of the data, or a pointer to
         * local storage */
        cb.proc = &proc;
        cb.scope = PMIX_INTERNAL;
        cb.copy = false;
        PMIX_GDS_FETCH_KV(rc, pmix_globals.mypeer, &cb);
        if (PMIX_SUCCESS == rc) {
            PMIX_CONSTRUCT(&pkt, pmix_buffer_t);
            /* assemble the provided data into a byte object */
            PMIX_GDS_ASSEMB_KVS_REQ(rc, cd->peer, &proc, &cb.kvs, &pkt, cd);
            if (rc != PMIX_SUCCESS) {
                PMIX_ERROR_LOG(rc);
                PMIX_DESTRUCT(&pkt);
                PMIX_DESTRUCT(&pbkt);
                PMIX_DESTRUCT(&cb);
                return rc;
            }
            if (PMIX_PROC_IS_V1(cd->peer)) {
                /* if the client is using v1, then it expects the
                 * data returned to it as the rank followed by abyte object containing
                 * a buffer - so we have to do a little gyration */
                pmix_buffer_t xfer;
                PMIX_CONSTRUCT(&xfer, pmix_buffer_t);
                PMIX_BFROPS_PACK(rc, cd->peer, &xfer, &pkt, 1, PMIX_BUFFER);
                if (PMIX_SUCCESS != rc) {
                    PMIX_ERROR_LOG(rc);
                    PMIX_DESTRUCT(&pkt);
                    PMIX_DESTRUCT(&pbkt);
                    PMIX_DESTRUCT(&xfer);
                    PMIX_DESTRUCT(&cb);
                    return rc;
                }
                PMIX_UNLOAD_BUFFER(&xfer, bo.bytes, bo.size);
                PMIX_DESTRUCT(&xfer);
            } else {
                PMIX_UNLOAD_BUFFER(&pkt, bo.bytes, bo.size);
            }
            PMIX_DESTRUCT(&pkt);
            /* pack it for transmission */
            PMIX_BFROPS_PACK(rc, cd->peer, &pbkt, &bo, 1, PMIX_BYTE_OBJECT);
            if (PMIX_SUCCESS != rc) {
                PMIX_ERROR_LOG(rc);
                PMIX_DESTRUCT(&pbkt);
                PMIX_DESTRUCT(&cb);
                return rc;
            }
        }
        PMIX_DESTRUCT(&cb);
        if (rank == PMIX_RANK_WILDCARD) {
            found = true;
        }
    }

    /* retrieve the data for the specific rank they are asking about */
    if (PMIX_RANK_WILDCARD != rank) {
        if (!PMIX_PROC_IS_SERVER(peer) && !peer->commit_cnt) {
            /* this condition works only for local requests, server does
             * count commits for local ranks, and check this count when
             * local request.
             * if that request performs for remote rank on the remote
             * node (by direct modex) so `peer->commit_cnt` should be ignored,
             * it is can not be counted for the remote side and this condition
             * does not matter for remote case */
            return PMIX_ERR_NOT_FOUND;
        }
        proc.rank = rank;
        PMIX_CONSTRUCT(&cb, pmix_cb_t);
        /* this is a local request, so give the gds the option
         * of returning a copy of the data, or a pointer to
         * local storage */
        cb.proc = &proc;
        cb.scope = scope;
        cb.copy = false;
        PMIX_GDS_FETCH_KV(rc, peer, &cb);
        if (PMIX_SUCCESS == rc) {
            found = true;
            PMIX_CONSTRUCT(&pkt, pmix_buffer_t);
            /* assemble the provided data into a byte object */
            PMIX_GDS_ASSEMB_KVS_REQ(rc, cd->peer, &proc, &cb.kvs, &pkt, cd);
            if (rc != PMIX_SUCCESS) {
                PMIX_ERROR_LOG(rc);
                PMIX_DESTRUCT(&pkt);
                PMIX_DESTRUCT(&pbkt);
                PMIX_DESTRUCT(&cb);
                return rc;
            }
            if (PMIX_PROC_IS_V1(cd->peer)) {
                /* if the client is using v1, then it expects the
                 * data returned to it in a different order than v2
                 * - so we have to do a little gyration */
                /* pack the rank */
                PMIX_BFROPS_PACK(rc, cd->peer, &pbkt, &rank, 1, PMIX_PROC_RANK);
                if (PMIX_SUCCESS != rc) {
                    PMIX_ERROR_LOG(rc);
                    PMIX_DESTRUCT(&pkt);
                    PMIX_DESTRUCT(&pbkt);
                    PMIX_DESTRUCT(&cb);
                    return rc;
                }
                /* now pack the data itself as a buffer */
                PMIX_BFROPS_PACK(rc, cd->peer, &pbkt, &pkt, 1, PMIX_BUFFER);
                if (PMIX_SUCCESS != rc) {
                    PMIX_ERROR_LOG(rc);
                    PMIX_DESTRUCT(&pkt);
                    PMIX_DESTRUCT(&pbkt);
                    PMIX_DESTRUCT(&cb);
                    return rc;
                }
                PMIX_DESTRUCT(&pkt);
            } else {
                PMIX_UNLOAD_BUFFER(&pkt, bo.bytes, bo.size);
                PMIX_DESTRUCT(&pkt);
                /* pack it for transmission */
                PMIX_BFROPS_PACK(rc, cd->peer, &pbkt, &bo, 1, PMIX_BYTE_OBJECT);
                if (PMIX_SUCCESS != rc) {
                    PMIX_ERROR_LOG(rc);
                    PMIX_DESTRUCT(&pbkt);
                    PMIX_DESTRUCT(&cb);
                    return rc;
                }
            }
        }
        PMIX_DESTRUCT(&cb);
    }
    PMIX_UNLOAD_BUFFER(&pbkt, data, sz);
    PMIX_DESTRUCT(&pbkt);

    if (found) {
        /* pass it back */
        cbfunc(rc, data, sz, cbdata, relfn, data);
        return rc;
    }

    return PMIX_ERR_NOT_FOUND;
}

/* Resolve pending requests to this namespace/rank */
pmix_status_t pmix_pending_resolve(pmix_namespace_t *nptr, pmix_rank_t rank,
                                   pmix_status_t status, pmix_dmdx_local_t *lcd)
{
    pmix_dmdx_local_t *cd, *ptr;
    pmix_dmdx_request_t *req;
    pmix_server_caddy_t *scd;

    /* find corresponding request (if exists) */
    if (NULL == lcd) {
        ptr = NULL;
        if (NULL != nptr) {
            PMIX_LIST_FOREACH(cd, &pmix_server_globals.local_reqs, pmix_dmdx_local_t) {
                if (!PMIX_CHECK_NSPACE(nptr->nspace, cd->proc.nspace) ||
                        rank != cd->proc.rank) {
                    continue;
                }
                ptr = cd;
                break;
            }
        }
        if (NULL == ptr) {
            return PMIX_SUCCESS;
        }
    } else {
        ptr = lcd;
    }

    /* if there are no local reqs on this request (e.g., only
     * one proc requested it and that proc has died), then
     * just remove the request */
    if (0 == pmix_list_get_size(&ptr->loc_reqs)) {
        goto cleanup;
    }

    /* somebody was interested in this rank */
    if (PMIX_SUCCESS != status){
        /* if we've got an error for this request - just forward it*/
        PMIX_LIST_FOREACH(req, &ptr->loc_reqs, pmix_dmdx_request_t) {
            req->cbfunc(status, NULL, 0, req->cbdata, NULL, NULL);
        }
    } else if (NULL != nptr) {
        /* if we've got the blob - try to satisfy requests */
        /* run through all the requests to this rank */
        /* this info is going back to one of our peers, so provide a server
         * caddy with our peer in it so the data gets packed correctly */
        scd = PMIX_NEW(pmix_server_caddy_t);
        PMIX_RETAIN(pmix_globals.mypeer);
        scd->peer = pmix_globals.mypeer;
        PMIX_LIST_FOREACH(req, &ptr->loc_reqs, pmix_dmdx_request_t) {
            pmix_status_t rc;
            rc = _satisfy_request(nptr, rank, scd, req->cbfunc, req->cbdata, NULL);
            if( PMIX_SUCCESS != rc ){
                /* if we can't satisfy this particular request (missing key?) */
                req->cbfunc(rc, NULL, 0, req->cbdata, NULL, NULL);
            }
        }
        PMIX_RELEASE(scd);
    }

  cleanup:
    /* remove all requests to this rank and cleanup the corresponding structure */
    pmix_list_remove_item(&pmix_server_globals.local_reqs, &ptr->super);
    PMIX_RELEASE(ptr);

    return PMIX_SUCCESS;
}

/* process the returned data from the host RM server */
static void _process_dmdx_reply(int fd, short args, void *cbdata)
{
    pmix_dmdx_reply_caddy_t *caddy = (pmix_dmdx_reply_caddy_t *)cbdata;
    pmix_server_caddy_t *cd;
    pmix_peer_t *peer;
    pmix_rank_info_t *rinfo;
    int32_t cnt;
    pmix_kval_t *kv;
    pmix_namespace_t *ns, *nptr;
    pmix_status_t rc;
    pmix_list_t nspaces;
    pmix_nspace_caddy_t *nm;
    pmix_dmdx_request_t *dm;
    bool found;
    pmix_buffer_t pbkt;
    pmix_cb_t cb;

    PMIX_ACQUIRE_OBJECT(caddy);

    pmix_output_verbose(2, pmix_server_globals.get_output,
                    "[%s:%d] process dmdx reply from %s:%u",
                    __FILE__, __LINE__,
                    caddy->lcd->proc.nspace, caddy->lcd->proc.rank);

    /* find the nspace object for the proc whose data is being received */
    nptr = NULL;
    PMIX_LIST_FOREACH(ns, &pmix_server_globals.nspaces, pmix_namespace_t) {
        if (0 == strcmp(caddy->lcd->proc.nspace, ns->nspace)) {
            nptr = ns;
            break;
        }
    }

    if (NULL == nptr) {
        /* We may not have this namespace because there are no local
         * processes from it running on this host - so just record it
         * so we know we have the data for any future requests */
        nptr = PMIX_NEW(pmix_namespace_t);
        nptr->nspace = strdup(caddy->lcd->proc.nspace);
        /* add to the list */
        pmix_list_append(&pmix_server_globals.nspaces, &nptr->super);
    }

    /* if the request was successfully satisfied, then store the data.
     * Although we could immediately
     * resolve any outstanding requests on our tracking list, we instead
     * store the data first so we can immediately satisfy any future
     * requests. Then, rather than duplicate the resolve code here, we
     * will let the pmix_pending_resolve function go ahead and retrieve
     * it from the GDS
     *
     * NOTE: if the data returned is NULL, then it has already been
     * stored (e.g., via a register_nspace call in response to a request
     * for job-level data). For now, we will retrieve it so it can
     * be stored for each peer */
    if (PMIX_SUCCESS == caddy->status) {
        /* cycle across all outstanding local requests and collect their
         * unique nspaces so we can store this for each one */
        PMIX_CONSTRUCT(&nspaces, pmix_list_t);
        PMIX_LIST_FOREACH(dm, &caddy->lcd->loc_reqs, pmix_dmdx_request_t) {
            /* this is a local proc that has requested this data - search
             * the list of nspace's and see if we already have it */
            cd = (pmix_server_caddy_t*)dm->cbdata;
            found = false;
            PMIX_LIST_FOREACH(nm, &nspaces, pmix_nspace_caddy_t) {
                if (0 == strcmp(nm->ns->nspace, cd->peer->nptr->nspace)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                /* add it */
                nm = PMIX_NEW(pmix_nspace_caddy_t);
                PMIX_RETAIN(cd->peer->nptr);
                nm->ns = cd->peer->nptr;
                pmix_list_append(&nspaces, &nm->super);
            }
        }
        /* now go thru each unique nspace and store the data using its
         * assigned GDS component */
        PMIX_LIST_FOREACH(nm, &nspaces, pmix_nspace_caddy_t) {
            if (NULL == nm->ns->compat.gds || 0 == nm->ns->nlocalprocs) {
                peer = pmix_globals.mypeer;
            } else {
                /* there must be at least one local proc */
                rinfo = (pmix_rank_info_t*)pmix_list_get_first(&nm->ns->ranks);
                peer = (pmix_peer_t*)pmix_pointer_array_get_item(&pmix_server_globals.clients, rinfo->peerid);
            }
            PMIX_CONSTRUCT(&pbkt, pmix_buffer_t);
            if (NULL == caddy->data) {
                /* we assume that the data was provided via a call to
                 * register_nspace, so what we need to do now is simply
                 * transfer it across to the individual nspace storage
                 * components */
                PMIX_CONSTRUCT(&cb, pmix_cb_t);
                PMIX_PROC_CREATE(cb.proc, 1);
                if (NULL == cb.proc) {
                    PMIX_ERROR_LOG(PMIX_ERR_NOMEM);
                    PMIX_DESTRUCT(&cb);
                    goto complete;
                }
                pmix_strncpy(cb.proc->nspace, nm->ns->nspace, PMIX_MAX_NSLEN);
                cb.proc->rank = PMIX_RANK_WILDCARD;
                cb.scope = PMIX_INTERNAL;
                cb.copy = false;
                PMIX_GDS_FETCH_KV(rc, pmix_globals.mypeer, &cb);
                if (PMIX_SUCCESS != rc) {
                    PMIX_ERROR_LOG(rc);
                    PMIX_DESTRUCT(&cb);
                    goto complete;
                }
                PMIX_LIST_FOREACH(kv, &cb.kvs, pmix_kval_t) {
                    PMIX_GDS_STORE_KV(rc, peer, &caddy->lcd->proc, PMIX_INTERNAL, kv);
                    if (PMIX_SUCCESS != rc) {
                        PMIX_ERROR_LOG(rc);
                        break;
                    }
                }
                PMIX_DESTRUCT(&cb);
            } else {
                PMIX_LOAD_BUFFER(pmix_globals.mypeer, &pbkt, caddy->data, caddy->ndata);
                /* unpack and store it*/
                kv = PMIX_NEW(pmix_kval_t);
                cnt = 1;
                PMIX_BFROPS_UNPACK(rc, pmix_globals.mypeer, &pbkt, kv, &cnt, PMIX_KVAL);
                while (PMIX_SUCCESS == rc) {
                    if (caddy->lcd->proc.rank == PMIX_RANK_WILDCARD) {
                        PMIX_GDS_STORE_KV(rc, peer, &caddy->lcd->proc, PMIX_INTERNAL, kv);
                    } else {
                        PMIX_GDS_STORE_KV(rc, peer, &caddy->lcd->proc, PMIX_REMOTE, kv);
                    }
                    if (PMIX_SUCCESS != rc) {
                        PMIX_ERROR_LOG(rc);
                        caddy->status = rc;
                        goto complete;
                    }
                    PMIX_RELEASE(kv);
                    kv = PMIX_NEW(pmix_kval_t);
                    cnt = 1;
                    PMIX_BFROPS_UNPACK(rc, pmix_globals.mypeer, &pbkt, kv, &cnt, PMIX_KVAL);
                }
                PMIX_RELEASE(kv);
                pbkt.base_ptr = NULL;  // protect the data
                PMIX_DESTRUCT(&pbkt);
                if (PMIX_ERR_UNPACK_READ_PAST_END_OF_BUFFER != rc) {
                    PMIX_ERROR_LOG(rc);
                    caddy->status = rc;
                    goto complete;
                }
            }
        }
        PMIX_LIST_DESTRUCT(&nspaces);
    }

  complete:
    /* always execute the callback to avoid having the client hang */
    pmix_pending_resolve(nptr, caddy->lcd->proc.rank, caddy->status, caddy->lcd);

    /* now call the release function so the host server
     * knows it can release the data */
    if (NULL != caddy->relcbfunc) {
        caddy->relcbfunc(caddy->cbdata);
    }
    PMIX_RELEASE(caddy);
}

/* this is the callback function that the host RM server will call
 * when it gets requested info back from a remote server */
static void dmdx_cbfunc(pmix_status_t status,
                        const char *data, size_t ndata, void *cbdata,
                        pmix_release_cbfunc_t release_fn, void *release_cbdata)
{
    pmix_dmdx_reply_caddy_t *caddy;

    /* because the host RM is calling us from their own thread, we
     * need to thread-shift into our local progress thread before
     * accessing any global info */
    caddy = PMIX_NEW(pmix_dmdx_reply_caddy_t);
    caddy->status = status;
    /* point to the callers cbfunc */
    caddy->relcbfunc = release_fn;
    caddy->cbdata = release_cbdata;

    /* point to the returned data and our own internal
     * tracker */
    caddy->data   = data;
    caddy->ndata  = ndata;
    caddy->lcd    = (pmix_dmdx_local_t *)cbdata;
    pmix_output_verbose(2, pmix_server_globals.get_output,
                        "[%s:%d] queue dmdx reply for %s:%u",
                        __FILE__, __LINE__,
                        caddy->lcd->proc.nspace, caddy->lcd->proc.rank);
    PMIX_THREADSHIFT(caddy, _process_dmdx_reply);
}

static void get_timeout(int sd, short args, void *cbdata)
{
    pmix_dmdx_request_t *req = (pmix_dmdx_request_t*)cbdata;

    pmix_output_verbose(2, pmix_server_globals.get_output,
                        "ALERT: get timeout fired");
    /* execute the provided callback function with the error */
    if (NULL != req->cbfunc) {
        req->cbfunc(PMIX_ERR_TIMEOUT, NULL, 0, req->cbdata, NULL, NULL);
    }
    req->event_active = false;
    pmix_list_remove_item(&req->lcd->loc_reqs, &req->super);
    PMIX_RELEASE(req);
}
