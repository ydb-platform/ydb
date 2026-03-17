/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2014-2019 Intel, Inc.  All rights reserved.
 * Copyright (c) 2014-2018 Research Organization for Information Science
 *                         and Technology (RIST).  All rights reserved.
 * Copyright (c) 2014-2015 Artem Y. Polyakov <artpol84@gmail.com>.
 *                         All rights reserved.
 * Copyright (c) 2016-2017 Mellanox Technologies, Inc.
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
#include "src/include/pmix_globals.h"

#ifdef HAVE_STRING_H
#include <string.h>
#endif
#ifdef HAVE_SYS_STAT_H
#include <sys/stat.h>
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
#ifdef HAVE_TIME_H
#include <time.h>
#endif
#include PMIX_EVENT_HEADER

#include "src/class/pmix_hotel.h"
#include "src/class/pmix_list.h"
#include "src/mca/bfrops/bfrops.h"
#include "src/mca/plog/plog.h"
#include "src/mca/psensor/psensor.h"
#include "src/util/argv.h"
#include "src/util/error.h"
#include "src/util/output.h"
#include "src/util/pmix_environ.h"

#include "pmix_server_ops.h"

pmix_server_module_t pmix_host_server = {0};

pmix_status_t pmix_server_abort(pmix_peer_t *peer, pmix_buffer_t *buf,
                                pmix_op_cbfunc_t cbfunc, void *cbdata)
{
    int32_t cnt;
    pmix_status_t rc;
    int status;
    char *msg;
    size_t nprocs;
    pmix_proc_t *procs = NULL;
    pmix_proc_t proc;

    pmix_output_verbose(2, pmix_server_globals.base_output, "recvd ABORT");

    /* unpack the status */
    cnt = 1;
    PMIX_BFROPS_UNPACK(rc, peer, buf, &status, &cnt, PMIX_STATUS);
    if (PMIX_SUCCESS != rc) {
        return rc;
    }
    /* unpack the message */
    cnt = 1;
    PMIX_BFROPS_UNPACK(rc, peer, buf, &msg, &cnt, PMIX_STRING);
    if (PMIX_SUCCESS != rc) {
        return rc;
    }
    /* unpack the number of procs */
    cnt = 1;
    PMIX_BFROPS_UNPACK(rc, peer, buf, &nprocs, &cnt, PMIX_SIZE);
    if (PMIX_SUCCESS != rc) {
        return rc;
    }

    /* unpack any provided procs - these are the procs the caller
     * wants aborted */
    if (0 < nprocs) {
        PMIX_PROC_CREATE(procs, nprocs);
        if (NULL == procs) {
            if (NULL != msg) {
                free(msg);
            }
            return PMIX_ERR_NOMEM;
        }
        cnt = nprocs;
        PMIX_BFROPS_UNPACK(rc, peer, buf, procs, &cnt, PMIX_PROC);
        if (PMIX_SUCCESS != rc) {
            if (NULL != msg) {
                free(msg);
            }
            return rc;
        }
    }

    /* let the local host's server execute it */
    if (NULL != pmix_host_server.abort) {
        pmix_strncpy(proc.nspace, peer->info->pname.nspace, PMIX_MAX_NSLEN);
        proc.rank = peer->info->pname.rank;
        rc = pmix_host_server.abort(&proc, peer->info->server_object, status, msg,
                                    procs, nprocs, cbfunc, cbdata);
    } else {
        rc = PMIX_ERR_NOT_SUPPORTED;
    }
    PMIX_PROC_FREE(procs, nprocs);

    /* the client passed this msg to us so we could give
     * it to the host server - we are done with it now */
    if (NULL != msg) {
        free(msg);
    }

    return rc;
}

pmix_status_t pmix_server_commit(pmix_peer_t *peer, pmix_buffer_t *buf)
{
    int32_t cnt;
    pmix_status_t rc;
    pmix_buffer_t b2, pbkt;
    pmix_kval_t *kp;
    pmix_scope_t scope;
    pmix_namespace_t *nptr;
    pmix_rank_info_t *info;
    pmix_proc_t proc;
    pmix_dmdx_remote_t *dcd, *dcdnext;
    char *data;
    size_t sz;
    pmix_cb_t cb;

    /* shorthand */
    info = peer->info;
    nptr = peer->nptr;
    pmix_strncpy(proc.nspace, nptr->nspace, PMIX_MAX_NSLEN);
    proc.rank = info->pname.rank;

    pmix_output_verbose(2, pmix_server_globals.base_output,
                        "%s:%d EXECUTE COMMIT FOR %s:%d",
                        pmix_globals.myid.nspace,
                        pmix_globals.myid.rank,
                        nptr->nspace, info->pname.rank);

    /* this buffer will contain one or more buffers, each
     * representing a different scope. These need to be locally
     * stored separately so we can provide required data based
     * on the requestor's location */
    cnt = 1;
    PMIX_BFROPS_UNPACK(rc, peer, buf, &scope, &cnt, PMIX_SCOPE);
    while (PMIX_SUCCESS == rc) {
        /* unpack and store the blob */
        cnt = 1;
        PMIX_CONSTRUCT(&b2, pmix_buffer_t);
        PMIX_BFROPS_ASSIGN_TYPE(peer, &b2);
        PMIX_BFROPS_UNPACK(rc, peer, buf, &b2, &cnt, PMIX_BUFFER);
        if (PMIX_SUCCESS != rc) {
            PMIX_ERROR_LOG(rc);
            return rc;
        }
        /* unpack the buffer and store the values - we store them
         * in this peer's native GDS component so that other local
         * procs from that nspace can access it */
        kp = PMIX_NEW(pmix_kval_t);
        cnt = 1;
        PMIX_BFROPS_UNPACK(rc, peer, &b2, kp, &cnt, PMIX_KVAL);
        while (PMIX_SUCCESS == rc) {
            if( PMIX_LOCAL == scope || PMIX_GLOBAL == scope){
                PMIX_GDS_STORE_KV(rc, peer, &proc, scope, kp);
                if (PMIX_SUCCESS != rc) {
                    PMIX_ERROR_LOG(rc);
                    PMIX_RELEASE(kp);
                    PMIX_DESTRUCT(&b2);
                    return rc;
                }
            }
            if (PMIX_REMOTE == scope || PMIX_GLOBAL == scope) {
                PMIX_GDS_STORE_KV(rc, pmix_globals.mypeer, &proc, scope, kp);
                if (PMIX_SUCCESS != rc) {
                    PMIX_ERROR_LOG(rc);
                    PMIX_RELEASE(kp);
                    PMIX_DESTRUCT(&b2);
                    return rc;
                }
            }
            PMIX_RELEASE(kp);  // maintain accounting
            kp = PMIX_NEW(pmix_kval_t);
            cnt = 1;
            PMIX_BFROPS_UNPACK(rc, peer, &b2, kp, &cnt, PMIX_KVAL);

        }
        PMIX_RELEASE(kp);   // maintain accounting
        PMIX_DESTRUCT(&b2);
        if (PMIX_ERR_UNPACK_READ_PAST_END_OF_BUFFER != rc) {
            PMIX_ERROR_LOG(rc);
            return rc;
        }
        cnt = 1;
        PMIX_BFROPS_UNPACK(rc, peer, buf, &scope, &cnt, PMIX_SCOPE);
    }
    if (PMIX_ERR_UNPACK_READ_PAST_END_OF_BUFFER != rc) {
        PMIX_ERROR_LOG(rc);
        return rc;
    }
    rc = PMIX_SUCCESS;
    /* mark us as having successfully received a blob from this proc */
    info->modex_recvd = true;

    /* update the commit counter */
    peer->commit_cnt++;

    /* see if anyone remote is waiting on this data - could be more than one */
    PMIX_LIST_FOREACH_SAFE(dcd, dcdnext, &pmix_server_globals.remote_pnd, pmix_dmdx_remote_t) {
        if (0 != strncmp(dcd->cd->proc.nspace, nptr->nspace, PMIX_MAX_NSLEN)) {
            continue;
        }
        if (dcd->cd->proc.rank == info->pname.rank) {
           /* we can now fulfill this request - collect the
             * remote/global data from this proc - note that there
             * may not be a contribution */
            data = NULL;
            sz = 0;
            PMIX_CONSTRUCT(&cb, pmix_cb_t);
            cb.proc = &proc;
            cb.scope = PMIX_REMOTE;
            cb.copy = true;
            PMIX_GDS_FETCH_KV(rc, pmix_globals.mypeer, &cb);
            if (PMIX_SUCCESS == rc) {
                /* package it up */
                PMIX_CONSTRUCT(&pbkt, pmix_buffer_t);
                PMIX_LIST_FOREACH(kp, &cb.kvs, pmix_kval_t) {
                    /* we pack this in our native BFROPS form as it
                     * will be sent to another daemon */
                    PMIX_BFROPS_PACK(rc, pmix_globals.mypeer, &pbkt, kp, 1, PMIX_KVAL);
                }
                PMIX_UNLOAD_BUFFER(&pbkt, data, sz);
            }
            PMIX_DESTRUCT(&cb);
            /* execute the callback */
            dcd->cd->cbfunc(rc, data, sz, dcd->cd->cbdata);
            if (NULL != data) {
                free(data);
            }
            /* we have finished this request */
            pmix_list_remove_item(&pmix_server_globals.remote_pnd, &dcd->super);
            PMIX_RELEASE(dcd);
        }
    }
    /* see if anyone local is waiting on this data- could be more than one */
    rc = pmix_pending_resolve(nptr, info->pname.rank, PMIX_SUCCESS, NULL);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
    }
    return rc;
}

/* get an existing object for tracking LOCAL participation in a collective
 * operation such as "fence". The only way this function can be
 * called is if at least one local client process is participating
 * in the operation. Thus, we know that at least one process is
 * involved AND has called the collective operation.
 *
 * NOTE: the host server *cannot* call us with a collective operation
 * as there is no mechanism by which it can do so. We call the host
 * server only after all participating local procs have called us.
 * So it is impossible for us to be called with a collective without
 * us already knowing about all local participants.
 *
 * procs - the array of procs participating in the collective,
 *         regardless of location
 * nprocs - the number of procs in the array
 */
static pmix_server_trkr_t* get_tracker(char *id, pmix_proc_t *procs,
                                       size_t nprocs, pmix_cmd_t type)
{
    pmix_server_trkr_t *trk;
    size_t i, j;
    size_t matches;

    pmix_output_verbose(5, pmix_server_globals.base_output,
                        "get_tracker called with %d procs", (int)nprocs);

    /* bozo check - should never happen outside of programmer error */
    if (NULL == procs && NULL == id) {
        PMIX_ERROR_LOG(PMIX_ERR_BAD_PARAM);
        return NULL;
    }

    /* there is no shortcut way to search the trackers - all
     * we can do is perform a brute-force search. Fortunately,
     * it is highly unlikely that there will be more than one
     * or two active at a time, and they are most likely to
     * involve only a single proc with WILDCARD rank - so this
     * shouldn't take long */
    PMIX_LIST_FOREACH(trk, &pmix_server_globals.collectives, pmix_server_trkr_t) {
        /* Collective operation if unique identified by
         * the set of participating processes and the type of collective,
         * or by the operation ID
         */
        if (NULL != id) {
            if (NULL != trk->id && 0 == strcmp(id, trk->id)) {
                return trk;
            }
        } else {
            if (nprocs != trk->npcs) {
                continue;
            }
            if (type != trk->type) {
                continue;
            }
            matches = 0;
            for (i=0; i < nprocs; i++) {
                /* the procs may be in different order, so we have
                 * to do an exhaustive search */
                for (j=0; j < trk->npcs; j++) {
                    if (0 == strcmp(procs[i].nspace, trk->pcs[j].nspace) &&
                        procs[i].rank == trk->pcs[j].rank) {
                        ++matches;
                        break;
                    }
                }
            }
            if (trk->npcs == matches) {
                return trk;
            }
        }
    }
    /* No tracker was found */
    return NULL;
}

/* create a new object for tracking LOCAL participation in a collective
 * operation such as "fence". The only way this function can be
 * called is if at least one local client process is participating
 * in the operation. Thus, we know that at least one process is
 * involved AND has called the collective operation.
 *
 * NOTE: the host server *cannot* call us with a collective operation
 * as there is no mechanism by which it can do so. We call the host
 * server only after all participating local procs have called us.
 * So it is impossible for us to be called with a collective without
 * us already knowing about all local participants.
 *
 * procs - the array of procs participating in the collective,
 *         regardless of location
 * nprocs - the number of procs in the array
 */
static pmix_server_trkr_t* new_tracker(char *id, pmix_proc_t *procs,
                                       size_t nprocs, pmix_cmd_t type)
{
    pmix_server_trkr_t *trk;
    size_t i;
    bool all_def;
    pmix_namespace_t *nptr, *ns;
    pmix_rank_info_t *info;

    pmix_output_verbose(5, pmix_server_globals.base_output,
                        "new_tracker called with %d procs", (int)nprocs);

    /* bozo check - should never happen outside of programmer error */
    if (NULL == procs) {
        PMIX_ERROR_LOG(PMIX_ERR_BAD_PARAM);
        return NULL;
    }

    pmix_output_verbose(5, pmix_server_globals.base_output,
                        "adding new tracker %s with %d procs",
                        (NULL == id) ? "NO-ID" : id, (int)nprocs);

    /* this tracker is new - create it */
    trk = PMIX_NEW(pmix_server_trkr_t);
    if (NULL == trk) {
        PMIX_ERROR_LOG(PMIX_ERR_NOMEM);
        return NULL;
    }

    if (NULL != id) {
        trk->id = strdup(id);
    }

    if (NULL != procs) {
        /* copy the procs */
        PMIX_PROC_CREATE(trk->pcs, nprocs);
        if (NULL == trk->pcs) {
            PMIX_ERROR_LOG(PMIX_ERR_NOMEM);
            PMIX_RELEASE(trk);
            return NULL;
        }
        memcpy(trk->pcs, procs, nprocs * sizeof(pmix_proc_t));
        trk->npcs = nprocs;
    }
    trk->type = type;

    all_def = true;
    for (i=0; i < nprocs; i++) {
        if (NULL == id) {
            pmix_strncpy(trk->pcs[i].nspace, procs[i].nspace, PMIX_MAX_NSLEN);
            trk->pcs[i].rank = procs[i].rank;
        }
        if (!all_def) {
            continue;
        }
        /* is this nspace known to us? */
        nptr = NULL;
        PMIX_LIST_FOREACH(ns, &pmix_server_globals.nspaces, pmix_namespace_t) {
            if (0 == strcmp(procs[i].nspace, ns->nspace)) {
                nptr = ns;
                break;
            }
        }
        if (NULL == nptr) {
            /* cannot be a local proc */
            pmix_output_verbose(5, pmix_server_globals.base_output,
                                "new_tracker: unknown nspace %s",
                                procs[i].nspace);
            continue;
        }
        /* have all the clients for this nspace been defined? */
        if (!nptr->all_registered) {
            /* nope, so no point in going further on this one - we'll
             * process it once all the procs are known */
            all_def = false;
            pmix_output_verbose(5, pmix_server_globals.base_output,
                                "new_tracker: all clients not registered nspace %s",
                                procs[i].nspace);
            /* we have to continue processing the list of procs
             * to setup the trk->pcs array, so don't break out
             * of the loop */
        }
        /* is this one of my local ranks? */
        PMIX_LIST_FOREACH(info, &nptr->ranks, pmix_rank_info_t) {
            if (procs[i].rank == info->pname.rank ||
                PMIX_RANK_WILDCARD == procs[i].rank) {
                    pmix_output_verbose(5, pmix_server_globals.base_output,
                                        "adding local proc %s.%d to tracker",
                                        info->pname.nspace, info->pname.rank);
                /* track the count */
                ++trk->nlocal;
                if (PMIX_RANK_WILDCARD != procs[i].rank) {
                    break;
                }
            }
        }
    }
    if (all_def) {
        trk->def_complete = true;
    }
    pmix_list_append(&pmix_server_globals.collectives, &trk->super);
    return trk;
}

static void fence_timeout(int sd, short args, void *cbdata)
{
    pmix_server_caddy_t *cd = (pmix_server_caddy_t*)cbdata;

    pmix_output_verbose(2, pmix_server_globals.fence_output,
                        "ALERT: fence timeout fired");

    /* execute the provided callback function with the error */
    if (NULL != cd->trk->modexcbfunc) {
        cd->trk->modexcbfunc(PMIX_ERR_TIMEOUT, NULL, 0, cd->trk, NULL, NULL);
        return;  // the cbfunc will have cleaned up the tracker
    }
    cd->event_active = false;
    /* remove it from the list */
    pmix_list_remove_item(&cd->trk->local_cbs, &cd->super);
    PMIX_RELEASE(cd);
}

pmix_status_t pmix_server_fence(pmix_server_caddy_t *cd,
                                pmix_buffer_t *buf,
                                pmix_modex_cbfunc_t modexcbfunc,
                                pmix_op_cbfunc_t opcbfunc)
{
    int32_t cnt;
    pmix_status_t rc;
    size_t nprocs;
    pmix_proc_t *procs=NULL, pcs;
    bool collect_data = false;
    pmix_server_trkr_t *trk;
    char *data = NULL;
    size_t sz = 0;
    pmix_buffer_t bucket, pbkt;
    pmix_server_caddy_t *scd;
    pmix_cb_t cb;
    pmix_kval_t *kv;
    pmix_byte_object_t bo;
    pmix_info_t *info = NULL;
    size_t ninfo=0, n;
    struct timeval tv = {0, 0};

    pmix_output_verbose(2, pmix_server_globals.fence_output,
                        "recvd FENCE");

    if (NULL == pmix_host_server.fence_nb) {
        PMIX_ERROR_LOG(PMIX_ERR_NOT_SUPPORTED);
        return PMIX_ERR_NOT_SUPPORTED;
    }

    /* unpack the number of procs */
    cnt = 1;
    PMIX_BFROPS_UNPACK(rc, cd->peer, buf, &nprocs, &cnt, PMIX_SIZE);
    if (PMIX_SUCCESS != rc) {
        return rc;
    }
    pmix_output_verbose(2, pmix_server_globals.fence_output,
                        "recvd fence from %s:%u with %d procs",
                        cd->peer->info->pname.nspace, cd->peer->info->pname.rank, (int)nprocs);
    /* there must be at least one as the client has to at least provide
     * their own namespace */
    if (nprocs < 1) {
        return PMIX_ERR_BAD_PARAM;
    }

    /* create space for the procs */
    PMIX_PROC_CREATE(procs, nprocs);
    if (NULL == procs) {
        return PMIX_ERR_NOMEM;
    }
    /* unpack the procs */
    cnt = nprocs;
    PMIX_BFROPS_UNPACK(rc, cd->peer, buf, procs, &cnt, PMIX_PROC);
    if (PMIX_SUCCESS != rc) {
        goto cleanup;
    }

    /* unpack the number of provided info structs */
    cnt = 1;
    PMIX_BFROPS_UNPACK(rc, cd->peer, buf, &ninfo, &cnt, PMIX_SIZE);
    if (PMIX_SUCCESS != rc) {
        return rc;
    }
    if (0 < ninfo) {
        PMIX_INFO_CREATE(info, ninfo);
        if (NULL == info) {
            PMIX_PROC_FREE(procs, nprocs);
            return PMIX_ERR_NOMEM;
        }
        /* unpack the info */
        cnt = ninfo;
        PMIX_BFROPS_UNPACK(rc, cd->peer, buf, info, &cnt, PMIX_INFO);
        if (PMIX_SUCCESS != rc) {
            goto cleanup;
        }
        /* see if we are to collect data or enforce a timeout - we don't internally care
         * about any other directives */
        for (n=0; n < ninfo; n++) {
            if (0 == strcmp(info[n].key, PMIX_COLLECT_DATA)) {
                collect_data = true;
            } else if (0 == strncmp(info[n].key, PMIX_TIMEOUT, PMIX_MAX_KEYLEN)) {
                tv.tv_sec = info[n].value.data.uint32;
            }
        }
    }

    /* find/create the local tracker for this operation */
    if (NULL == (trk = get_tracker(NULL, procs, nprocs, PMIX_FENCENB_CMD))) {
        /* If no tracker was found - create and initialize it once */
        if (NULL == (trk = new_tracker(NULL, procs, nprocs, PMIX_FENCENB_CMD))) {
            /* only if a bozo error occurs */
            PMIX_ERROR_LOG(PMIX_ERROR);
            /* DO NOT HANG */
            if (NULL != opcbfunc) {
                opcbfunc(PMIX_ERROR, cd);
            }
            rc = PMIX_ERROR;
            goto cleanup;
        }
        trk->type = PMIX_FENCENB_CMD;
        trk->modexcbfunc = modexcbfunc;
       /* mark if they want the data back */
        if (collect_data) {
            trk->collect_type = PMIX_COLLECT_YES;
        } else {
            trk->collect_type = PMIX_COLLECT_NO;
        }
    } else {
        switch (trk->collect_type) {
        case PMIX_COLLECT_NO:
            if (collect_data) {
                trk->collect_type = PMIX_COLLECT_INVALID;
            }
            break;
        case PMIX_COLLECT_YES:
            if (!collect_data) {
                trk->collect_type = PMIX_COLLECT_INVALID;
            }
            break;
        default:
            break;
        }
    }
    /* we only save the info structs from the first caller
     * who provides them - it is a user error to provide
     * different values from different participants */
    if (NULL == trk->info) {
        trk->info = info;
        trk->ninfo = ninfo;
    } else {
        /* cleanup */
        PMIX_INFO_FREE(info, ninfo);
        info = NULL;
    }

    /* add this contributor to the tracker so they get
     * notified when we are done */
    pmix_list_append(&trk->local_cbs, &cd->super);
    /* if a timeout was specified, set it */
    if (0 < tv.tv_sec) {
        PMIX_RETAIN(trk);
        cd->trk = trk;
        pmix_event_evtimer_set(pmix_globals.evbase, &cd->ev,
                               fence_timeout, cd);
        pmix_event_evtimer_add(&cd->ev, &tv);
        cd->event_active = true;
    }

    /* if all local contributions have been received,
     * let the local host's server know that we are at the
     * "fence" point - they will callback once the barrier
     * across all participants has been completed */
    if (trk->def_complete &&
        pmix_list_get_size(&trk->local_cbs) == trk->nlocal) {
        pmix_output_verbose(2, pmix_server_globals.base_output,
                            "fence complete");
        /* if the user asked us to collect data, then we have
         * to provide any locally collected data to the host
         * server so they can circulate it - only take data
         * from the specified procs as not everyone is necessarily
         * participating! And only take data intended for remote
         * or global distribution */

        PMIX_CONSTRUCT(&bucket, pmix_buffer_t);

        /* mark the collection type so we can check on the
         * receiving end that all participants did the same */
        unsigned char tmp = (unsigned char)trk->collect_type;
        PMIX_BFROPS_PACK(rc, pmix_globals.mypeer, &bucket,
                         &tmp, 1, PMIX_BYTE);

        if (PMIX_COLLECT_YES == trk->collect_type) {
            pmix_output_verbose(2, pmix_server_globals.fence_output,
                                "fence - assembling data");
            PMIX_LIST_FOREACH(scd, &trk->local_cbs, pmix_server_caddy_t) {
                /* get any remote contribution - note that there
                 * may not be a contribution */
                pmix_strncpy(pcs.nspace, scd->peer->info->pname.nspace, PMIX_MAX_NSLEN);
                pcs.rank = scd->peer->info->pname.rank;
                PMIX_CONSTRUCT(&cb, pmix_cb_t);
                cb.proc = &pcs;
                cb.scope = PMIX_REMOTE;
                cb.copy = true;
                PMIX_GDS_FETCH_KV(rc, pmix_globals.mypeer, &cb);
                if (PMIX_SUCCESS == rc) {
                    PMIX_CONSTRUCT(&pbkt, pmix_buffer_t);
                    /* pack the proc so we know the source */
                    PMIX_BFROPS_PACK(rc, pmix_globals.mypeer, &pbkt,
                                     &pcs, 1, PMIX_PROC);
                    if (PMIX_SUCCESS != rc) {
                        PMIX_ERROR_LOG(rc);
                        PMIX_DESTRUCT(&cb);
                        goto cleanup;
                    }
                    /* pack the returned kval's */
                    PMIX_LIST_FOREACH(kv, &cb.kvs, pmix_kval_t) {
                        PMIX_BFROPS_PACK(rc, pmix_globals.mypeer, &pbkt, kv, 1, PMIX_KVAL);
                        if (PMIX_SUCCESS != rc) {
                            PMIX_ERROR_LOG(rc);
                            PMIX_DESTRUCT(&cb);
                            goto cleanup;
                        }
                    }
                    /* extract the blob */
                    PMIX_UNLOAD_BUFFER(&pbkt, bo.bytes, bo.size);
                    PMIX_DESTRUCT(&pbkt);
                    /* pack the returned blob */
                    PMIX_BFROPS_PACK(rc, pmix_globals.mypeer, &bucket,
                                     &bo, 1, PMIX_BYTE_OBJECT);
                    PMIX_BYTE_OBJECT_DESTRUCT(&bo);
                    if (PMIX_SUCCESS != rc) {
                        PMIX_ERROR_LOG(rc);
                        PMIX_DESTRUCT(&cb);
                        goto cleanup;
                    }
                }
                PMIX_DESTRUCT(&cb);
            }
        }
        /* because the remote servers have to unpack things
         * in chunks, we have to pack the bucket as a single
         * byte object to allow remote unpack */
        PMIX_UNLOAD_BUFFER(&bucket, bo.bytes, bo.size);
        PMIX_DESTRUCT(&bucket);
        PMIX_CONSTRUCT(&bucket, pmix_buffer_t);
        PMIX_BFROPS_PACK(rc, pmix_globals.mypeer, &bucket,
                         &bo, 1, PMIX_BYTE_OBJECT);
        PMIX_BYTE_OBJECT_DESTRUCT(&bo);  // releases the data
        if (PMIX_SUCCESS != rc) {
            PMIX_ERROR_LOG(rc);
            PMIX_DESTRUCT(&cb);
            goto cleanup;
        }
        /* now unload the blob and pass it upstairs */
        PMIX_UNLOAD_BUFFER(&bucket, data, sz);
        PMIX_DESTRUCT(&bucket);
        rc = pmix_host_server.fence_nb(trk->pcs, trk->npcs,
                                       trk->info, trk->ninfo,
                                       data, sz, trk->modexcbfunc, trk);
        if (PMIX_SUCCESS != rc) {
            pmix_list_remove_item(&pmix_server_globals.collectives, &trk->super);
            PMIX_RELEASE(trk);
        }
    }

  cleanup:
    PMIX_PROC_FREE(procs, nprocs);
    return rc;
}

static void opcbfunc(pmix_status_t status, void *cbdata)
{
    pmix_setup_caddy_t *cd = (pmix_setup_caddy_t*)cbdata;

    if (NULL != cd->keys) {
        pmix_argv_free(cd->keys);
    }
    if (NULL != cd->codes) {
        free(cd->codes);
    }
    if (NULL != cd->info) {
        PMIX_INFO_FREE(cd->info, cd->ninfo);
    }
    if (NULL != cd->opcbfunc) {
        cd->opcbfunc(status, cd->cbdata);
    }
    PMIX_RELEASE(cd);
}

pmix_status_t pmix_server_publish(pmix_peer_t *peer,
                                  pmix_buffer_t *buf,
                                  pmix_op_cbfunc_t cbfunc, void *cbdata)
{
    pmix_setup_caddy_t *cd;
    pmix_status_t rc;
    int32_t cnt;
    size_t ninfo;
    pmix_proc_t proc;
    uint32_t uid;

    pmix_output_verbose(2, pmix_server_globals.pub_output,
                        "recvd PUBLISH");

    if (NULL == pmix_host_server.publish) {
        return PMIX_ERR_NOT_SUPPORTED;
    }

    /* unpack the effective user id */
    cnt=1;
    PMIX_BFROPS_UNPACK(rc, peer, buf, &uid, &cnt, PMIX_UINT32);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        return rc;
    }
    /* unpack the number of info objects */
    cnt=1;
    PMIX_BFROPS_UNPACK(rc, peer, buf, &ninfo, &cnt, PMIX_SIZE);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        return rc;
    }
    /* we will be adding one for the user id */
    cd = PMIX_NEW(pmix_setup_caddy_t);
    if (NULL == cd) {
        return PMIX_ERR_NOMEM;
    }
    cd->opcbfunc = cbfunc;
    cd->cbdata = cbdata;
    cd->ninfo = ninfo + 1;
    PMIX_INFO_CREATE(cd->info, cd->ninfo);
    if (NULL == cd->info) {
        rc = PMIX_ERR_NOMEM;
        goto cleanup;
    }
    /* unpack the array of info objects */
    if (0 < cd->ninfo) {
        cnt=cd->ninfo;
        PMIX_BFROPS_UNPACK(rc, peer, buf, cd->info, &cnt, PMIX_INFO);
        if (PMIX_SUCCESS != rc) {
            PMIX_ERROR_LOG(rc);
            goto cleanup;
        }
    }
    pmix_strncpy(cd->info[cd->ninfo-1].key, PMIX_USERID, PMIX_MAX_KEYLEN);
    cd->info[cd->ninfo-1].value.type = PMIX_UINT32;
    cd->info[cd->ninfo-1].value.data.uint32 = uid;

    /* call the local server */
    pmix_strncpy(proc.nspace, peer->info->pname.nspace, PMIX_MAX_NSLEN);
    proc.rank = peer->info->pname.rank;
    rc = pmix_host_server.publish(&proc, cd->info, cd->ninfo, opcbfunc, cd);

  cleanup:
    if (PMIX_SUCCESS != rc) {
        if (NULL != cd->info) {
            PMIX_INFO_FREE(cd->info, cd->ninfo);
        }
        PMIX_RELEASE(cd);
    }
    return rc;
}

static void lkcbfunc(pmix_status_t status,
                     pmix_pdata_t data[], size_t ndata,
                     void *cbdata)
{
    pmix_setup_caddy_t *cd = (pmix_setup_caddy_t*)cbdata;

    /* cleanup the caddy */
    if (NULL != cd->keys) {
        pmix_argv_free(cd->keys);
    }
    if (NULL != cd->info) {
        PMIX_INFO_FREE(cd->info, cd->ninfo);
    }

    /* return the results */
    if (NULL != cd->lkcbfunc) {
        cd->lkcbfunc(status, data, ndata, cd->cbdata);
    }
    PMIX_RELEASE(cd);
}
pmix_status_t pmix_server_lookup(pmix_peer_t *peer,
                                 pmix_buffer_t *buf,
                                 pmix_lookup_cbfunc_t cbfunc, void *cbdata)
{
    pmix_setup_caddy_t *cd;
    int32_t cnt;
    pmix_status_t rc;
    size_t nkeys, i;
    char *sptr;
    size_t ninfo;
    pmix_proc_t proc;
    uint32_t uid;

    pmix_output_verbose(2, pmix_server_globals.pub_output,
                        "recvd LOOKUP");

    if (NULL == pmix_host_server.lookup) {
        return PMIX_ERR_NOT_SUPPORTED;
    }

    /* unpack the effective user id */
    cnt=1;
    PMIX_BFROPS_UNPACK(rc, peer, buf, &uid, &cnt, PMIX_UINT32);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        return rc;
    }
    /* unpack the number of keys */
    cnt=1;
    PMIX_BFROPS_UNPACK(rc, peer, buf, &nkeys, &cnt, PMIX_SIZE);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        return rc;
    }
    /* setup the caddy */
    cd = PMIX_NEW(pmix_setup_caddy_t);
    if (NULL == cd) {
        return PMIX_ERR_NOMEM;
    }
    cd->lkcbfunc = cbfunc;
    cd->cbdata = cbdata;
    /* unpack the array of keys */
    for (i=0; i < nkeys; i++) {
        cnt=1;
        PMIX_BFROPS_UNPACK(rc, peer, buf, &sptr, &cnt, PMIX_STRING);
        if (PMIX_SUCCESS != rc) {
            PMIX_ERROR_LOG(rc);
            goto cleanup;
        }
        pmix_argv_append_nosize(&cd->keys, sptr);
        free(sptr);
    }
    /* unpack the number of info objects */
    cnt=1;
    PMIX_BFROPS_UNPACK(rc, peer, buf, &ninfo, &cnt, PMIX_SIZE);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        goto cleanup;
    }
    /* we will be adding one for the user id */
    cd->ninfo = ninfo + 1;
    PMIX_INFO_CREATE(cd->info, cd->ninfo);
    if (NULL == cd->info) {
        rc = PMIX_ERR_NOMEM;
        goto cleanup;
    }
    /* unpack the array of info objects */
    if (0 < ninfo) {
        cnt=ninfo;
        PMIX_BFROPS_UNPACK(rc, peer, buf, cd->info, &cnt, PMIX_INFO);
        if (PMIX_SUCCESS != rc) {
            PMIX_ERROR_LOG(rc);
            goto cleanup;
        }
    }
    pmix_strncpy(cd->info[cd->ninfo-1].key, PMIX_USERID, PMIX_MAX_KEYLEN);
    cd->info[cd->ninfo-1].value.type = PMIX_UINT32;
    cd->info[cd->ninfo-1].value.data.uint32 = uid;

    /* call the local server */
    pmix_strncpy(proc.nspace, peer->info->pname.nspace, PMIX_MAX_NSLEN);
    proc.rank = peer->info->pname.rank;
    rc = pmix_host_server.lookup(&proc, cd->keys, cd->info, cd->ninfo, lkcbfunc, cd);

  cleanup:
    if (PMIX_SUCCESS != rc) {
        if (NULL != cd->keys) {
            pmix_argv_free(cd->keys);
        }
        if (NULL != cd->info) {
            PMIX_INFO_FREE(cd->info, cd->ninfo);
        }
        PMIX_RELEASE(cd);
    }
    return rc;
}

pmix_status_t pmix_server_unpublish(pmix_peer_t *peer,
                                    pmix_buffer_t *buf,
                                    pmix_op_cbfunc_t cbfunc, void *cbdata)
{
    pmix_setup_caddy_t *cd;
    int32_t cnt;
    pmix_status_t rc;
    size_t i, nkeys, ninfo;
    char *sptr;
    pmix_proc_t proc;
    uint32_t uid;

    pmix_output_verbose(2, pmix_server_globals.pub_output,
                        "recvd UNPUBLISH");

    if (NULL == pmix_host_server.unpublish) {
        return PMIX_ERR_NOT_SUPPORTED;
    }

    /* unpack the effective user id */
    cnt=1;
    PMIX_BFROPS_UNPACK(rc, peer, buf, &uid, &cnt, PMIX_UINT32);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        return rc;
    }
    /* unpack the number of keys */
    cnt=1;
    PMIX_BFROPS_UNPACK(rc, peer, buf, &nkeys, &cnt, PMIX_SIZE);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        return rc;
    }
    /* setup the caddy */
    cd = PMIX_NEW(pmix_setup_caddy_t);
    if (NULL == cd) {
        return PMIX_ERR_NOMEM;
    }
    cd->opcbfunc = cbfunc;
    cd->cbdata = cbdata;
    /* unpack the array of keys */
    for (i=0; i < nkeys; i++) {
        cnt=1;
        PMIX_BFROPS_UNPACK(rc, peer, buf, &sptr, &cnt, PMIX_STRING);
        if (PMIX_SUCCESS != rc) {
            PMIX_ERROR_LOG(rc);
            goto cleanup;
        }
        pmix_argv_append_nosize(&cd->keys, sptr);
        free(sptr);
    }
    /* unpack the number of info objects */
    cnt=1;
    PMIX_BFROPS_UNPACK(rc, peer, buf, &ninfo, &cnt, PMIX_SIZE);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        goto cleanup;
    }
    /* we will be adding one for the user id */
    cd->ninfo = ninfo + 1;
    PMIX_INFO_CREATE(cd->info, cd->ninfo);
    if (NULL == cd->info) {
        rc = PMIX_ERR_NOMEM;
        goto cleanup;
    }
    /* unpack the array of info objects */
    if (0 < ninfo) {
        cnt=ninfo;
        PMIX_BFROPS_UNPACK(rc, peer, buf, cd->info, &cnt, PMIX_INFO);
        if (PMIX_SUCCESS != rc) {
            PMIX_ERROR_LOG(rc);
            goto cleanup;
        }
    }
    pmix_strncpy(cd->info[cd->ninfo-1].key, PMIX_USERID, PMIX_MAX_KEYLEN);
    cd->info[cd->ninfo-1].value.type = PMIX_UINT32;
    cd->info[cd->ninfo-1].value.data.uint32 = uid;

    /* call the local server */
    pmix_strncpy(proc.nspace, peer->info->pname.nspace, PMIX_MAX_NSLEN);
    proc.rank = peer->info->pname.rank;
    rc = pmix_host_server.unpublish(&proc, cd->keys, cd->info, cd->ninfo, opcbfunc, cd);

  cleanup:
    if (PMIX_SUCCESS != rc) {
        if (NULL != cd->keys) {
            pmix_argv_free(cd->keys);
        }
        if (NULL != cd->info) {
            PMIX_INFO_FREE(cd->info, cd->ninfo);
        }
        PMIX_RELEASE(cd);
    }
    return rc;
}

static void spcbfunc(pmix_status_t status,
                     char nspace[], void *cbdata)
{
    pmix_setup_caddy_t *cd = (pmix_setup_caddy_t*)cbdata;
    pmix_iof_req_t *req;
    pmix_buffer_t *msg;
    pmix_status_t rc;
    pmix_iof_cache_t *iof, *ionext;

    /* if it was successful, and there are IOF requests, then
     * register them now */
    if (PMIX_SUCCESS == status && PMIX_FWD_NO_CHANNELS != cd->channels) {
         /* record the request */
        req = PMIX_NEW(pmix_iof_req_t);
        if (NULL == req) {
            status = PMIX_ERR_NOMEM;
            goto cleanup;
        }
        PMIX_RETAIN(cd->peer);
        req->peer = cd->peer;
        req->pname.nspace = strdup(nspace);
        req->pname.rank = PMIX_RANK_WILDCARD;
        req->channels = cd->channels;
        pmix_list_append(&pmix_globals.iof_requests, &req->super);
        /* process any cached IO */
        PMIX_LIST_FOREACH_SAFE(iof, ionext, &pmix_server_globals.iof, pmix_iof_cache_t) {
            /* if the channels don't match, then ignore it */
            if (!(iof->channel & req->channels)) {
                continue;
            }
            /* if the source does not match the request, then ignore it */
            if (!PMIX_CHECK_PROCID(&iof->source, &req->pname)) {
                continue;
            }
            /* never forward back to the source! This can happen if the source
             * is a launcher */
            if (PMIX_CHECK_PROCID(&iof->source, &req->peer->info->pname)) {
                continue;
            }
            pmix_output_verbose(2, pmix_server_globals.iof_output,
                                "PMIX:SERVER:SPAWN delivering cached IOF from %s:%d to %s:%d",
                                iof->source.nspace, iof->source.rank,
                                req->pname.nspace, req->pname.rank);
            /* setup the msg */
            if (NULL == (msg = PMIX_NEW(pmix_buffer_t))) {
                PMIX_ERROR_LOG(PMIX_ERR_OUT_OF_RESOURCE);
                rc = PMIX_ERR_OUT_OF_RESOURCE;
                break;
            }
            /* provide the source */
            PMIX_BFROPS_PACK(rc, req->peer, msg, &iof->source, 1, PMIX_PROC);
            if (PMIX_SUCCESS != rc) {
                PMIX_ERROR_LOG(rc);
                PMIX_RELEASE(msg);
                break;
            }
            /* provide the channel */
            PMIX_BFROPS_PACK(rc, req->peer, msg, &iof->channel, 1, PMIX_IOF_CHANNEL);
            if (PMIX_SUCCESS != rc) {
                PMIX_ERROR_LOG(rc);
                PMIX_RELEASE(msg);
                break;
            }
            /* pack the data */
            PMIX_BFROPS_PACK(rc, req->peer, msg, iof->bo, 1, PMIX_BYTE_OBJECT);
            if (PMIX_SUCCESS != rc) {
                PMIX_ERROR_LOG(rc);
                PMIX_RELEASE(msg);
                break;
            }
            /* send it to the requestor */
            PMIX_PTL_SEND_ONEWAY(rc, req->peer, msg, PMIX_PTL_TAG_IOF);
            if (PMIX_SUCCESS != rc) {
                PMIX_ERROR_LOG(rc);
                PMIX_RELEASE(msg);
            }
            /* remove it from the list since it has now been forwarded */
            pmix_list_remove_item(&pmix_server_globals.iof, &iof->super);
            PMIX_RELEASE(iof);
        }
    }

  cleanup:
    /* cleanup the caddy */
    if (NULL != cd->info) {
        PMIX_INFO_FREE(cd->info, cd->ninfo);
    }
    if (NULL != cd->apps) {
        PMIX_APP_FREE(cd->apps, cd->napps);
    }
    if (NULL != cd->spcbfunc) {
        cd->spcbfunc(status, nspace, cd->cbdata);
    }
    PMIX_RELEASE(cd);
}

pmix_status_t pmix_server_spawn(pmix_peer_t *peer,
                                pmix_buffer_t *buf,
                                pmix_spawn_cbfunc_t cbfunc,
                                void *cbdata)
{
    pmix_setup_caddy_t *cd;
    int32_t cnt;
    pmix_status_t rc;
    pmix_proc_t proc;
    size_t ninfo, n;
    bool stdout_found = false, stderr_found = false, stddiag_found = false;

    pmix_output_verbose(2, pmix_server_globals.spawn_output,
                        "recvd SPAWN from %s:%d", peer->info->pname.nspace, peer->info->pname.rank);

    if (NULL == pmix_host_server.spawn) {
        return PMIX_ERR_NOT_SUPPORTED;
    }

    /* setup */
    cd = PMIX_NEW(pmix_setup_caddy_t);
    if (NULL == cd) {
        return PMIX_ERR_NOMEM;
    }
    PMIX_RETAIN(peer);
    cd->peer = peer;
    cd->spcbfunc = cbfunc;
    cd->cbdata = cbdata;

    /* unpack the number of job-level directives */
    cnt=1;
    PMIX_BFROPS_UNPACK(rc, peer, buf, &ninfo, &cnt, PMIX_SIZE);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        PMIX_RELEASE(cd);
        return rc;
    }
    /* always add one directive that indicates whether the requestor
     * is a tool or client */
    cd->ninfo = ninfo + 1;
    PMIX_INFO_CREATE(cd->info, cd->ninfo);
    if (NULL == cd->info) {
        rc = PMIX_ERR_NOMEM;
        goto cleanup;
    }

    /* unpack the array of directives */
    if (0 < ninfo) {
        cnt = ninfo;
        PMIX_BFROPS_UNPACK(rc, peer, buf, cd->info, &cnt, PMIX_INFO);
        if (PMIX_SUCCESS != rc) {
            PMIX_ERROR_LOG(rc);
            goto cleanup;
        }
        /* run a quick check of the directives to see if any IOF
         * requests were included so we can set that up now - helps
         * to catch any early output - and a request for notification
         * of job termination so we can setup the event registration */
        cd->channels = PMIX_FWD_NO_CHANNELS;
        for (n=0; n < cd->ninfo; n++) {
            if (0 == strncmp(cd->info[n].key, PMIX_FWD_STDIN, PMIX_MAX_KEYLEN)) {
                if (PMIX_INFO_TRUE(&cd->info[n])) {
                    cd->channels |= PMIX_FWD_STDIN_CHANNEL;
                }
            } else if (0 == strncmp(cd->info[n].key, PMIX_FWD_STDOUT, PMIX_MAX_KEYLEN)) {
                stdout_found = true;
                if (PMIX_INFO_TRUE(&cd->info[n])) {
                    cd->channels |= PMIX_FWD_STDOUT_CHANNEL;
                }
            } else if (0 == strncmp(cd->info[n].key, PMIX_FWD_STDERR, PMIX_MAX_KEYLEN)) {
                stderr_found = true;
                if (PMIX_INFO_TRUE(&cd->info[n])) {
                    cd->channels |= PMIX_FWD_STDERR_CHANNEL;
                }
            } else if (0 == strncmp(cd->info[n].key, PMIX_FWD_STDDIAG, PMIX_MAX_KEYLEN)) {
                stddiag_found = true;
                if (PMIX_INFO_TRUE(&cd->info[n])) {
                    cd->channels |= PMIX_FWD_STDDIAG_CHANNEL;
                }
            }
        }
        /* we will construct any required iof request tracker upon completion of the spawn
         * as we need the nspace of the spawned application! */
    }
    /* add the directive to the end */
    if (PMIX_PROC_IS_TOOL(peer)) {
        PMIX_INFO_LOAD(&cd->info[ninfo], PMIX_REQUESTOR_IS_TOOL, NULL, PMIX_BOOL);
        /* if the requestor is a tool, we default to forwarding all
         * output IO channels */
        if (!stdout_found) {
            cd->channels |= PMIX_FWD_STDOUT_CHANNEL;
        }
        if (!stderr_found) {
            cd->channels |= PMIX_FWD_STDERR_CHANNEL;
        }
        if (!stddiag_found) {
            cd->channels |= PMIX_FWD_STDDIAG_CHANNEL;
        }
    } else {
        PMIX_INFO_LOAD(&cd->info[ninfo], PMIX_REQUESTOR_IS_CLIENT, NULL, PMIX_BOOL);
    }

    /* unpack the number of apps */
    cnt=1;
    PMIX_BFROPS_UNPACK(rc, peer, buf, &cd->napps, &cnt, PMIX_SIZE);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        goto cleanup;
    }
    /* unpack the array of apps */
    if (0 < cd->napps) {
        PMIX_APP_CREATE(cd->apps, cd->napps);
        if (NULL == cd->apps) {
            rc = PMIX_ERR_NOMEM;
            goto cleanup;
        }
        cnt = cd->napps;
        PMIX_BFROPS_UNPACK(rc, peer, buf, cd->apps, &cnt, PMIX_APP);
        if (PMIX_SUCCESS != rc) {
            PMIX_ERROR_LOG(rc);
            goto cleanup;
        }
    }
    /* call the local server */
    pmix_strncpy(proc.nspace, peer->info->pname.nspace, PMIX_MAX_NSLEN);
    proc.rank = peer->info->pname.rank;
    rc = pmix_host_server.spawn(&proc, cd->info, cd->ninfo, cd->apps, cd->napps, spcbfunc, cd);

  cleanup:
    if (PMIX_SUCCESS != rc) {
        if (NULL != cd->info) {
            PMIX_INFO_FREE(cd->info, cd->ninfo);
        }
        if (NULL != cd->apps) {
            PMIX_APP_FREE(cd->apps, cd->napps);
        }
        PMIX_RELEASE(cd);
    }
    return rc;
}

pmix_status_t pmix_server_disconnect(pmix_server_caddy_t *cd,
                                     pmix_buffer_t *buf,
                                     pmix_op_cbfunc_t cbfunc)
{
    int32_t cnt;
    pmix_status_t rc;
    pmix_info_t *info = NULL;
    size_t nprocs, ninfo;
    pmix_server_trkr_t *trk;
    pmix_proc_t *procs = NULL;

    if (NULL == pmix_host_server.disconnect) {
        return PMIX_ERR_NOT_SUPPORTED;
    }

    /* unpack the number of procs */
    cnt = 1;
    PMIX_BFROPS_UNPACK(rc, cd->peer, buf, &nprocs, &cnt, PMIX_SIZE);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        goto cleanup;
    }
    /* there must be at least one proc - we do not allow the client
     * to send us NULL proc as the server has no idea what to do
     * with that situation. Instead, the client should at least send
     * us their own namespace for the use-case where the connection
     * spans all procs in that namespace */
    if (nprocs < 1) {
        PMIX_ERROR_LOG(PMIX_ERR_BAD_PARAM);
        rc = PMIX_ERR_BAD_PARAM;
        goto cleanup;
    }

    /* unpack the procs */
    PMIX_PROC_CREATE(procs, nprocs);
    if (NULL == procs) {
        rc = PMIX_ERR_NOMEM;
        goto cleanup;
    }
    cnt = nprocs;
    PMIX_BFROPS_UNPACK(rc, cd->peer, buf, procs, &cnt, PMIX_PROC);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        goto cleanup;
    }

    /* unpack the number of provided info structs */
    cnt = 1;
    PMIX_BFROPS_UNPACK(rc, cd->peer, buf, &ninfo, &cnt, PMIX_SIZE);
    if (PMIX_SUCCESS != rc) {
        return rc;
    }
    if (0 < ninfo) {
        PMIX_INFO_CREATE(info, ninfo);
        if (NULL == info) {
            rc = PMIX_ERR_NOMEM;
            goto cleanup;
        }
        /* unpack the info */
        cnt = ninfo;
        PMIX_BFROPS_UNPACK(rc, cd->peer, buf, info, &cnt, PMIX_INFO);
        if (PMIX_SUCCESS != rc) {
            goto cleanup;
        }
    }

    /* find/create the local tracker for this operation */
    if (NULL == (trk = get_tracker(NULL, procs, nprocs, PMIX_DISCONNECTNB_CMD))) {
        /* we don't have this tracker yet, so get a new one */
        if (NULL == (trk = new_tracker(NULL, procs, nprocs, PMIX_DISCONNECTNB_CMD))) {
            /* only if a bozo error occurs */
            PMIX_ERROR_LOG(PMIX_ERROR);
            rc = PMIX_ERROR;
            goto cleanup;
        }
        trk->op_cbfunc = cbfunc;
    }

    /* if the info keys have not been provided yet, pass
     * them along here */
    if (NULL == trk->info && NULL != info) {
        trk->info = info;
        trk->ninfo = ninfo;
        info = NULL;
        ninfo = 0;
    }

    /* add this contributor to the tracker so they get
     * notified when we are done */
    pmix_list_append(&trk->local_cbs, &cd->super);
    /* if all local contributions have been received,
     * let the local host's server know that we are at the
     * "fence" point - they will callback once the [dis]connect
     * across all participants has been completed */
    if (trk->def_complete &&
        pmix_list_get_size(&trk->local_cbs) == trk->nlocal) {
        rc = pmix_host_server.disconnect(trk->pcs, trk->npcs, trk->info, trk->ninfo, cbfunc, trk);
        if (PMIX_SUCCESS != rc) {
            /* remove this contributor from the list - they will be notified
             * by the switchyard */
            pmix_list_remove_item(&trk->local_cbs, &cd->super);
        }
    } else {
        rc = PMIX_SUCCESS;
    }

  cleanup:
    if (NULL != info) {
        PMIX_INFO_FREE(info, ninfo);
    }
    return rc;
}

static void connect_timeout(int sd, short args, void *cbdata)
{
    pmix_server_caddy_t *cd = (pmix_server_caddy_t*)cbdata;

    pmix_output_verbose(2, pmix_server_globals.connect_output,
                        "ALERT: connect timeout fired");

    /* execute the provided callback function with the error */
    if (NULL != cd->trk->op_cbfunc) {
        cd->trk->op_cbfunc(PMIX_ERR_TIMEOUT, cd->trk);
        return;  // the cbfunc will have cleaned up the tracker
    }
    cd->event_active = false;
    /* remove it from the list */
    pmix_list_remove_item(&cd->trk->local_cbs, &cd->super);
    PMIX_RELEASE(cd);
}

pmix_status_t pmix_server_connect(pmix_server_caddy_t *cd,
                                  pmix_buffer_t *buf,
                                  pmix_op_cbfunc_t cbfunc)
{
    int32_t cnt;
    pmix_status_t rc;
    pmix_proc_t *procs = NULL;
    pmix_info_t *info = NULL;
    size_t nprocs, ninfo, n;
    pmix_server_trkr_t *trk;
    struct timeval tv = {0, 0};

    pmix_output_verbose(2, pmix_server_globals.connect_output,
                        "recvd CONNECT from peer %s:%d",
                        cd->peer->info->pname.nspace,
                        cd->peer->info->pname.rank);

    if (NULL == pmix_host_server.connect) {
        return PMIX_ERR_NOT_SUPPORTED;
    }

    /* unpack the number of procs */
    cnt = 1;
    PMIX_BFROPS_UNPACK(rc, cd->peer, buf, &nprocs, &cnt, PMIX_SIZE);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        goto cleanup;
    }
    /* there must be at least one proc - we do not allow the client
     * to send us NULL proc as the server has no idea what to do
     * with that situation. Instead, the client should at least send
     * us their own namespace for the use-case where the connection
     * spans all procs in that namespace */
    if (nprocs < 1) {
        PMIX_ERROR_LOG(PMIX_ERR_BAD_PARAM);
        rc = PMIX_ERR_BAD_PARAM;
        goto cleanup;
    }

    /* unpack the procs */
    PMIX_PROC_CREATE(procs, nprocs);
    if (NULL == procs) {
        rc = PMIX_ERR_NOMEM;
        goto cleanup;
    }
    cnt = nprocs;
    PMIX_BFROPS_UNPACK(rc, cd->peer, buf, procs, &cnt, PMIX_PROC);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        goto cleanup;
    }

    /* unpack the number of provided info structs */
    cnt = 1;
    PMIX_BFROPS_UNPACK(rc, cd->peer, buf, &ninfo, &cnt, PMIX_SIZE);
    if (PMIX_SUCCESS != rc) {
        return rc;
    }
    if (0 < ninfo) {
        PMIX_INFO_CREATE(info, ninfo);
        if (NULL == info) {
            rc = PMIX_ERR_NOMEM;
            goto cleanup;
        }
        /* unpack the info */
        cnt = ninfo;
        PMIX_BFROPS_UNPACK(rc, cd->peer, buf, info, &cnt, PMIX_INFO);
        if (PMIX_SUCCESS != rc) {
            goto cleanup;
        }
        /* check for a timeout */
        for (n=0; n < ninfo; n++) {
            if (0 == strncmp(info[n].key, PMIX_TIMEOUT, PMIX_MAX_KEYLEN)) {
                tv.tv_sec = info[n].value.data.uint32;
                break;
            }
        }
    }

    /* find/create the local tracker for this operation */
    if (NULL == (trk = get_tracker(NULL, procs, nprocs, PMIX_CONNECTNB_CMD))) {
        /* we don't have this tracker yet, so get a new one */
        if (NULL == (trk = new_tracker(NULL, procs, nprocs, PMIX_CONNECTNB_CMD))) {
            /* only if a bozo error occurs */
            PMIX_ERROR_LOG(PMIX_ERROR);
            /* DO NOT HANG */
            if (NULL != cbfunc) {
                cbfunc(PMIX_ERROR, cd);
            }
            rc = PMIX_ERROR;
            goto cleanup;
        }
        trk->op_cbfunc = cbfunc;
    }

    /* if the info keys have not been provided yet, pass
     * them along here */
    if (NULL == trk->info && NULL != info) {
        trk->info = info;
        trk->ninfo = ninfo;
        info = NULL;
        ninfo = 0;
    }

    /* add this contributor to the tracker so they get
     * notified when we are done */
    pmix_list_append(&trk->local_cbs, &cd->super);

    /* if all local contributions have been received,
     * let the local host's server know that we are at the
     * "fence" point - they will callback once the [dis]connect
     * across all participants has been completed */
    if (trk->def_complete &&
        pmix_list_get_size(&trk->local_cbs) == trk->nlocal) {
        rc = pmix_host_server.connect(trk->pcs, trk->npcs, trk->info, trk->ninfo, cbfunc, trk);
        if (PMIX_SUCCESS != rc) {
            /* remove this contributor from the list - they will be notified
             * by the switchyard */
            pmix_list_remove_item(&trk->local_cbs, &cd->super);
        }
    } else {
        rc = PMIX_SUCCESS;
    }
    /* if a timeout was specified, set it */
    if (PMIX_SUCCESS == rc && 0 < tv.tv_sec) {
        PMIX_RETAIN(trk);
        cd->trk = trk;
        pmix_event_evtimer_set(pmix_globals.evbase, &cd->ev,
                               connect_timeout, cd);
        pmix_event_evtimer_add(&cd->ev, &tv);
        cd->event_active = true;
    }

  cleanup:
    if (NULL != procs) {
        PMIX_PROC_FREE(procs, nprocs);
    }
    if (NULL != info) {
        PMIX_INFO_FREE(info, ninfo);
    }
    return rc;
}

pmix_status_t pmix_server_register_events(pmix_peer_t *peer,
                                          pmix_buffer_t *buf,
                                          pmix_op_cbfunc_t cbfunc,
                                          void *cbdata)
{
    int32_t cnt;
    pmix_status_t rc, ret = PMIX_SUCCESS;
    pmix_status_t *codes = NULL;
    pmix_info_t *info = NULL;
    size_t ninfo=0, ncodes, n, k;
    pmix_regevents_info_t *reginfo;
    pmix_peer_events_info_t *prev = NULL;
    pmix_notify_caddy_t *cd;
    pmix_setup_caddy_t *scd;
    int i;
    bool enviro_events = false;
    bool found, matched;
    pmix_buffer_t *relay;
    pmix_cmd_t cmd = PMIX_NOTIFY_CMD;
    pmix_proc_t *affected = NULL;
    size_t naffected = 0;
    pmix_range_trkr_t rngtrk;
    pmix_proc_t proc;

    pmix_output_verbose(2, pmix_server_globals.event_output,
                        "recvd register events for peer %s:%d",
                        peer->info->pname.nspace, peer->info->pname.rank);

    /* unpack the number of codes */
    cnt=1;
    PMIX_BFROPS_UNPACK(rc, peer, buf, &ncodes, &cnt, PMIX_SIZE);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        return rc;
    }
    /* unpack the array of codes */
    if (0 < ncodes) {
        codes = (pmix_status_t*)malloc(ncodes * sizeof(pmix_status_t));
        if (NULL == codes) {
            rc = PMIX_ERR_NOMEM;
            goto cleanup;
        }
        cnt=ncodes;
        PMIX_BFROPS_UNPACK(rc, peer, buf, codes, &cnt, PMIX_STATUS);
        if (PMIX_SUCCESS != rc) {
            PMIX_ERROR_LOG(rc);
            goto cleanup;
        }
    }

    /* unpack the number of info objects */
    cnt=1;
    PMIX_BFROPS_UNPACK(rc, peer, buf, &ninfo, &cnt, PMIX_SIZE);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        return rc;
    }
    /* unpack the array of info objects */
    if (0 < ninfo) {
        PMIX_INFO_CREATE(info, ninfo);
        if (NULL == info) {
            rc = PMIX_ERR_NOMEM;
            goto cleanup;
        }
        cnt=ninfo;
        PMIX_BFROPS_UNPACK(rc, peer, buf, info, &cnt, PMIX_INFO);
        if (PMIX_SUCCESS != rc) {
            PMIX_ERROR_LOG(rc);
            goto cleanup;
        }
    }

    /* check the directives */
    for (n=0; n < ninfo; n++) {
        if (PMIX_CHECK_KEY(&info[n], PMIX_EVENT_AFFECTED_PROC)) {
            if (NULL != affected) {
                PMIX_ERROR_LOG(PMIX_ERR_BAD_PARAM);
                rc = PMIX_ERR_BAD_PARAM;
                goto cleanup;
            }
            naffected = 1;
            PMIX_PROC_CREATE(affected, naffected);
            memcpy(affected, info[n].value.data.proc, sizeof(pmix_proc_t));
        } else if (PMIX_CHECK_KEY(&info[n], PMIX_EVENT_AFFECTED_PROCS)) {
            if (NULL != affected) {
                PMIX_ERROR_LOG(PMIX_ERR_BAD_PARAM);
                rc = PMIX_ERR_BAD_PARAM;
                goto cleanup;
            }
            naffected = info[n].value.data.darray->size;
            PMIX_PROC_CREATE(affected, naffected);
            memcpy(affected, info[n].value.data.darray->array, naffected * sizeof(pmix_proc_t));
        }
    }

    /* check the codes for system events */
    for (n=0; n < ncodes; n++) {
        if (PMIX_SYSTEM_EVENT(codes[n])) {
            enviro_events = true;
            break;
        }
    }

    /* if they asked for enviro events, and our host doesn't support
     * register_events, then we cannot meet the request */
    if (enviro_events && NULL == pmix_host_server.register_events) {
        enviro_events = false;
        rc = PMIX_ERR_NOT_SUPPORTED;
        goto cleanup;
    }

    /* if they didn't send us any codes, then they are registering a
     * default event handler. In that case, check only for default
     * handlers and add this request to it, if not already present */
    if (0 == ncodes)  {
        PMIX_LIST_FOREACH(reginfo, &pmix_server_globals.events, pmix_regevents_info_t) {
            if (PMIX_MAX_ERR_CONSTANT == reginfo->code) {
                /* both are default handlers */
                prev = PMIX_NEW(pmix_peer_events_info_t);
                if (NULL == prev) {
                    rc = PMIX_ERR_NOMEM;
                    goto cleanup;
                }
                PMIX_RETAIN(peer);
                prev->peer = peer;
                if (NULL != affected) {
                    PMIX_PROC_CREATE(prev->affected, naffected);
                    prev->naffected = naffected;
                    memcpy(prev->affected, affected, naffected * sizeof(pmix_proc_t));
                }
                pmix_list_append(&reginfo->peers, &prev->super);
                break;
            }
        }
        rc = PMIX_OPERATION_SUCCEEDED;
        goto cleanup;
    }

    /* store the event registration info so we can call the registered
     * client when the server notifies the event */
    for (n=0; n < ncodes; n++) {
        found = false;
        PMIX_LIST_FOREACH(reginfo, &pmix_server_globals.events, pmix_regevents_info_t) {
            if (NULL == codes) {
                if (PMIX_MAX_ERR_CONSTANT == reginfo->code) {
                    /* both are default handlers */
                    found = true;
                    break;
                } else {
                    continue;
                }
            } else {
                if (PMIX_MAX_ERR_CONSTANT == reginfo->code) {
                    continue;
                } else if (codes[n] == reginfo->code) {
                    found = true;
                    break;
                }
            }
        }
        if (found) {
            /* found it - add this request */
            prev = PMIX_NEW(pmix_peer_events_info_t);
            if (NULL == prev) {
                rc = PMIX_ERR_NOMEM;
                goto cleanup;
            }
            PMIX_RETAIN(peer);
            prev->peer = peer;
            if (NULL != affected) {
                PMIX_PROC_CREATE(prev->affected, naffected);
                prev->naffected = naffected;
                memcpy(prev->affected, affected, naffected * sizeof(pmix_proc_t));
            }
            prev->enviro_events = enviro_events;
            pmix_list_append(&reginfo->peers, &prev->super);
        } else {
            /* if we get here, then we didn't find an existing registration for this code */
            reginfo = PMIX_NEW(pmix_regevents_info_t);
            if (NULL == reginfo) {
                rc = PMIX_ERR_NOMEM;
                goto cleanup;
            }
            if (NULL == codes) {
                reginfo->code = PMIX_MAX_ERR_CONSTANT;
            } else {
                reginfo->code = codes[n];
            }
            pmix_list_append(&pmix_server_globals.events, &reginfo->super);
            prev = PMIX_NEW(pmix_peer_events_info_t);
            if (NULL == prev) {
                rc = PMIX_ERR_NOMEM;
                goto cleanup;
            }
            PMIX_RETAIN(peer);
            prev->peer = peer;
            if (NULL != affected) {
                PMIX_PROC_CREATE(prev->affected, naffected);
                prev->naffected = naffected;
                memcpy(prev->affected, affected, naffected * sizeof(pmix_proc_t));
            }
            prev->enviro_events = enviro_events;
            pmix_list_append(&reginfo->peers, &prev->super);
        }
    }

    /* if they asked for enviro events, call the local server */
    if (enviro_events) {
        /* if they don't support this, then we cannot do it */
        if (NULL == pmix_host_server.register_events) {
            rc = PMIX_ERR_NOT_SUPPORTED;
            goto cleanup;
        }
        /* need to ensure the arrays don't go away until after the
         * host RM is done with them */
        scd = PMIX_NEW(pmix_setup_caddy_t);
        if (NULL == scd) {
            rc = PMIX_ERR_NOMEM;
            goto cleanup;
        }
        if (NULL != codes) {
            scd->codes = (pmix_status_t*)malloc(ncodes * sizeof(pmix_status_t));
            if (NULL == scd->codes) {
                rc = PMIX_ERR_NOMEM;
                PMIX_RELEASE(scd);
                goto cleanup;
            }
            memcpy(scd->codes, codes, ncodes * sizeof(pmix_status_t));
            scd->ncodes = ncodes;
        }
        if (NULL != info) {
            PMIX_INFO_CREATE(scd->info, ninfo);
            if (NULL == scd->info) {
                rc = PMIX_ERR_NOMEM;
                if (NULL != scd->codes) {
                    free(scd->codes);
                }
                PMIX_RELEASE(scd);
                goto cleanup;
            }
            /* copy the info across */
            for (n=0; n < ninfo; n++) {
                PMIX_INFO_XFER(&scd->info[n], &info[n]);
            }
            scd->ninfo = ninfo;
        }
        scd->opcbfunc = cbfunc;
        scd->cbdata = cbdata;
        if (PMIX_SUCCESS != (rc = pmix_host_server.register_events(scd->codes, scd->ncodes, scd->info, scd->ninfo, opcbfunc, scd))) {
            pmix_output_verbose(2, pmix_server_globals.event_output,
                                 "server register events: host server reg events returned rc =%d", rc);
            if (NULL != scd->codes) {
                free(scd->codes);
            }
            if (NULL != scd->info) {
                PMIX_INFO_FREE(scd->info, scd->ninfo);
            }
            PMIX_RELEASE(scd);
        }
    } else {
        rc = PMIX_OPERATION_SUCCEEDED;
    }

  cleanup:
    pmix_output_verbose(2, pmix_server_globals.event_output,
                        "server register events: ninfo =%lu rc =%d", ninfo, rc);
    if (NULL != info) {
        PMIX_INFO_FREE(info, ninfo);
    }
    if (PMIX_SUCCESS != rc && PMIX_OPERATION_SUCCEEDED != rc) {
        if (NULL != codes) {
            free(codes);
        }
        if (NULL != affected) {
            PMIX_PROC_FREE(affected, naffected);
        }
        return rc;
    }

    /* check if any matching notifications have been cached */
    rngtrk.procs = NULL;
    rngtrk.nprocs = 0;
    for (i=0; i < pmix_globals.max_events; i++) {
        pmix_hotel_knock(&pmix_globals.notifications, i, (void**)&cd);
        if (NULL == cd) {
            continue;
        }
        found = false;
        if (NULL == codes) {
            if (!cd->nondefault) {
                /* they registered a default event handler - always matches */
                found = true;
            }
        } else {
            for (k=0; k < ncodes; k++) {
                if (codes[k] == cd->status) {
                    found = true;
                    break;
                }
            }
        }
        if (!found) {
            continue;
        }
        /* check if the affected procs (if given) match those they
         * wanted to know about */
        if (!pmix_notify_check_affected(cd->affected, cd->naffected,
                                        affected, naffected)) {
            continue;
        }
        /* check the range */
        if (NULL == cd->targets) {
            rngtrk.procs = &cd->source;
            rngtrk.nprocs = 1;
        } else {
            rngtrk.procs = cd->targets;
            rngtrk.nprocs = cd->ntargets;
        }
        rngtrk.range = cd->range;
        PMIX_LOAD_PROCID(&proc, peer->info->pname.nspace, peer->info->pname.rank);
        if (!pmix_notify_check_range(&rngtrk, &proc)) {
            continue;
        }
        /* if we were given specific targets, check if this is one */
        found = false;
        if (NULL != cd->targets) {
            matched = false;
            for (n=0; n < cd->ntargets; n++) {
                /* if the source of the event is the same peer just registered, then ignore it
                 * as the event notification system will have already locally
                 * processed it */
                if (PMIX_CHECK_PROCID(&cd->source, &peer->info->pname)) {
                    continue;
                }
                if (PMIX_CHECK_PROCID(&peer->info->pname, &cd->targets[n])) {
                    matched = true;
                    /* track the number of targets we have left to notify */
                    --cd->nleft;
                    /* if this is the last one, then evict this event
                     * from the cache */
                    if (0 == cd->nleft) {
                        pmix_hotel_checkout(&pmix_globals.notifications, cd->room);
                        found = true;  // mark that we should release cd
                    }
                    break;
                }
            }
            if (!matched) {
                /* do not notify this one */
                continue;
            }
        }

        /* all matches - notify */
        relay = PMIX_NEW(pmix_buffer_t);
        if (NULL == relay) {
            /* nothing we can do */
            PMIX_ERROR_LOG(PMIX_ERR_NOMEM);
            ret = PMIX_ERR_NOMEM;
            break;
        }
        /* pack the info data stored in the event */
        PMIX_BFROPS_PACK(ret, peer, relay, &cmd, 1, PMIX_COMMAND);
        if (PMIX_SUCCESS != ret) {
            PMIX_ERROR_LOG(ret);
            break;
        }
        PMIX_BFROPS_PACK(ret, peer, relay, &cd->status, 1, PMIX_STATUS);
        if (PMIX_SUCCESS != ret) {
            PMIX_ERROR_LOG(ret);
            break;
        }
        PMIX_BFROPS_PACK(ret, peer, relay, &cd->source, 1, PMIX_PROC);
        if (PMIX_SUCCESS != ret) {
            PMIX_ERROR_LOG(ret);
            break;
        }
        PMIX_BFROPS_PACK(ret, peer, relay, &cd->ninfo, 1, PMIX_SIZE);
        if (PMIX_SUCCESS != ret) {
            PMIX_ERROR_LOG(ret);
            break;
        }
        if (0 < cd->ninfo) {
            PMIX_BFROPS_PACK(ret, peer, relay, cd->info, cd->ninfo, PMIX_INFO);
            if (PMIX_SUCCESS != ret) {
                PMIX_ERROR_LOG(ret);
                break;
            }
        }
        PMIX_SERVER_QUEUE_REPLY(ret, peer, 0, relay);
        if (PMIX_SUCCESS != ret) {
            PMIX_RELEASE(relay);
        }
        if (found) {
            PMIX_RELEASE(cd);
        }
    }

    if (NULL != codes) {
        free(codes);
    }
    if (NULL != affected) {
        PMIX_PROC_FREE(affected, naffected);
    }
    if (PMIX_SUCCESS != ret) {
        rc = ret;
    }
    return rc;
}

void pmix_server_deregister_events(pmix_peer_t *peer,
                                   pmix_buffer_t *buf)
{
    int32_t cnt;
    pmix_status_t rc, code;
    pmix_regevents_info_t *reginfo = NULL;
    pmix_regevents_info_t *reginfo_next;
    pmix_peer_events_info_t *prev;

    pmix_output_verbose(2, pmix_server_globals.event_output,
                        "recvd deregister events");

    /* unpack codes and process until done */
    cnt=1;
    PMIX_BFROPS_UNPACK(rc, peer, buf, &code, &cnt, PMIX_STATUS);
    while (PMIX_SUCCESS == rc) {
        PMIX_LIST_FOREACH_SAFE(reginfo, reginfo_next, &pmix_server_globals.events, pmix_regevents_info_t) {
            if (code == reginfo->code) {
                /* found it - remove this peer from the list */
                PMIX_LIST_FOREACH(prev, &reginfo->peers, pmix_peer_events_info_t) {
                    if (prev->peer == peer) {
                        /* found it */
                        pmix_list_remove_item(&reginfo->peers, &prev->super);
                        PMIX_RELEASE(prev);
                        break;
                    }
                }
                /* if all of the peers for this code are now gone, then remove it */
                if (0 == pmix_list_get_size(&reginfo->peers)) {
                    pmix_list_remove_item(&pmix_server_globals.events, &reginfo->super);
                    /* if this was registered with the host, then deregister it */
                    PMIX_RELEASE(reginfo);
                }
            }
        }
        cnt=1;
        PMIX_BFROPS_UNPACK(rc, peer, buf, &code, &cnt, PMIX_STATUS);
    }
    if (PMIX_ERR_UNPACK_READ_PAST_END_OF_BUFFER != rc) {
        PMIX_ERROR_LOG(rc);
    }
}


static void local_cbfunc(pmix_status_t status, void *cbdata)
{
    pmix_notify_caddy_t *cd = (pmix_notify_caddy_t*)cbdata;

    if (NULL != cd->cbfunc) {
        cd->cbfunc(status, cd->cbdata);
    }
    PMIX_RELEASE(cd);
}

static void intermed_step(pmix_status_t status, void *cbdata)
{
    pmix_notify_caddy_t *cd = (pmix_notify_caddy_t*)cbdata;
    pmix_status_t rc;

    if (PMIX_SUCCESS != status) {
        rc = status;
        goto complete;
    }

    /* check the range directive - if it is LOCAL, then we are
     * done. Otherwise, it needs to go up to our
     * host for dissemination */
    if (PMIX_RANGE_LOCAL == cd->range) {
        rc = PMIX_SUCCESS;
        goto complete;
    }

    if (NULL == pmix_host_server.notify_event) {
        rc = PMIX_ERR_NOT_SUPPORTED;
        goto complete;
    }

    /* since our host is going to send this everywhere, it may well
     * come back to us. We already processed it, so mark it here
     * to ensure we don't do it again. We previously inserted the
     * PMIX_SERVER_INTERNAL_NOTIFY key at the very end of the
     * info array - just overwrite that position */
    PMIX_INFO_LOAD(&cd->info[cd->ninfo-1], PMIX_EVENT_PROXY, &pmix_globals.myid, PMIX_PROC);

    /* pass it to our host RM for distribution */
    rc = pmix_host_server.notify_event(cd->status, &cd->source, cd->range,
                                       cd->info, cd->ninfo, local_cbfunc, cd);
    if (PMIX_SUCCESS == rc) {
        /* let the callback function respond for us */
        return;
    }
    if (PMIX_OPERATION_SUCCEEDED == rc) {
        rc = PMIX_SUCCESS;  // local_cbfunc will not be called
    }

  complete:
    if (NULL != cd->cbfunc) {
        cd->cbfunc(rc, cd->cbdata);
    }
    PMIX_RELEASE(cd);
}

/* Receive an event sent by the client library. Since it was sent
 * to us by one client, we have to both process it locally to ensure
 * we notify all relevant local clients AND (assuming a range other
 * than LOCAL) deliver to our host, requesting that they send it
 * to all peer servers in the current session */
pmix_status_t pmix_server_event_recvd_from_client(pmix_peer_t *peer,
                                                  pmix_buffer_t *buf,
                                                  pmix_op_cbfunc_t cbfunc,
                                                  void *cbdata)
{
    int32_t cnt;
    pmix_status_t rc;
    pmix_notify_caddy_t *cd;
    size_t ninfo, n;

    pmix_output_verbose(2, pmix_server_globals.event_output,
                        "%s:%d recvd event notification from client %s:%d",
                        pmix_globals.myid.nspace, pmix_globals.myid.rank,
                        peer->info->pname.nspace, peer->info->pname.rank);

    cd = PMIX_NEW(pmix_notify_caddy_t);
    if (NULL == cd) {
        return PMIX_ERR_NOMEM;
    }
    cd->cbfunc = cbfunc;
    cd->cbdata = cbdata;
    /* set the source */
    PMIX_LOAD_PROCID(&cd->source, peer->info->pname.nspace, peer->info->pname.rank);

    /* unpack status */
    cnt = 1;
    PMIX_BFROPS_UNPACK(rc, peer, buf, &cd->status, &cnt, PMIX_STATUS);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        goto exit;
    }

    /* unpack the range */
    cnt = 1;
    PMIX_BFROPS_UNPACK(rc, peer, buf, &cd->range, &cnt, PMIX_DATA_RANGE);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        goto exit;
    }

    /* unpack the info keys */
    cnt = 1;
    PMIX_BFROPS_UNPACK(rc, peer, buf, &ninfo, &cnt, PMIX_SIZE);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        goto exit;
    }
    cd->ninfo = ninfo + 1;
    PMIX_INFO_CREATE(cd->info, cd->ninfo);
    if (NULL == cd->info) {
        rc = PMIX_ERR_NOMEM;
        goto exit;
    }
    if (0 < ninfo) {
        cnt = ninfo;
        PMIX_BFROPS_UNPACK(rc, peer, buf, cd->info, &cnt, PMIX_INFO);
        if (PMIX_SUCCESS != rc) {
            PMIX_ERROR_LOG(rc);
            goto exit;
        }
    }

    /* check to see if we already processed this event - it is possible
     * that a local client "echoed" it back to us and we want to avoid
     * a potential infinite loop */
    for (n=0; n < ninfo; n++) {
        if (PMIX_CHECK_KEY(&cd->info[n], PMIX_SERVER_INTERNAL_NOTIFY)) {
            /* yep, we did - so don't do it again! */
            rc = PMIX_OPERATION_SUCCEEDED;
            goto exit;
        }
    }

    /* add an info object to mark that we recvd this internally */
    PMIX_INFO_LOAD(&cd->info[ninfo], PMIX_SERVER_INTERNAL_NOTIFY, NULL, PMIX_BOOL);
    /* process it */
    if (PMIX_SUCCESS != (rc = pmix_server_notify_client_of_event(cd->status,
                                                                 &cd->source,
                                                                 cd->range,
                                                                 cd->info, cd->ninfo,
                                                                 intermed_step, cd))) {
        goto exit;
    }
    if (PMIX_SUCCESS != rc) {
        PMIX_RELEASE(cd);
    }
    return rc;

  exit:
    PMIX_RELEASE(cd);
    return rc;
}

pmix_status_t pmix_server_query(pmix_peer_t *peer,
                                pmix_buffer_t *buf,
                                pmix_info_cbfunc_t cbfunc,
                                void *cbdata)
{
    int32_t cnt;
    pmix_status_t rc;
    pmix_query_caddy_t *cd;
    pmix_proc_t proc;
    pmix_cb_t cb;
    size_t n, p;
    pmix_list_t results;
    pmix_kval_t *kv, *kvnxt;

    pmix_output_verbose(2, pmix_server_globals.base_output,
                        "recvd query from client");

    cd = PMIX_NEW(pmix_query_caddy_t);
    if (NULL == cd) {
        return PMIX_ERR_NOMEM;
    }
    cd->cbdata = cbdata;
    /* unpack the number of queries */
    cnt = 1;
    PMIX_BFROPS_UNPACK(rc, peer, buf, &cd->nqueries, &cnt, PMIX_SIZE);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        PMIX_RELEASE(cd);
        return rc;
    }
    /* unpack the queries */
    if (0 < cd->nqueries) {
        PMIX_QUERY_CREATE(cd->queries, cd->nqueries);
        if (NULL == cd->queries) {
            rc = PMIX_ERR_NOMEM;
            PMIX_RELEASE(cd);
            return rc;
        }
        cnt = cd->nqueries;
        PMIX_BFROPS_UNPACK(rc, peer, buf, cd->queries, &cnt, PMIX_QUERY);
        if (PMIX_SUCCESS != rc) {
            PMIX_ERROR_LOG(rc);
            PMIX_RELEASE(cd);
            return rc;
        }
    }

    /* check the directives to see if they want us to refresh
     * the local cached results - if we wanted to optimize this
     * more, we would check each query and allow those that don't
     * want to be refreshed to be executed locally, and those that
     * did would be sent to the host. However, for now we simply
     * determine that if we don't have it, then ask for everything */
    memset(proc.nspace, 0, PMIX_MAX_NSLEN+1);
    proc.rank = PMIX_RANK_INVALID;
    PMIX_CONSTRUCT(&results, pmix_list_t);

    for (n=0; n < cd->nqueries; n++) {
        for (p=0; p < cd->queries[n].nqual; p++) {
            if (PMIX_CHECK_KEY(&cd->queries[n].qualifiers[p], PMIX_QUERY_REFRESH_CACHE)) {
                if (PMIX_INFO_TRUE(&cd->queries[n].qualifiers[p])) {
                    PMIX_LIST_DESTRUCT(&results);
                    goto query;
                }
            } else if (PMIX_CHECK_KEY(&cd->queries[n].qualifiers[p], PMIX_PROCID)) {
                PMIX_LOAD_NSPACE(proc.nspace, cd->queries[n].qualifiers[p].value.data.proc->nspace);
                proc.rank = cd->queries[n].qualifiers[p].value.data.proc->rank;
            } else if (PMIX_CHECK_KEY(&cd->queries[n].qualifiers[p], PMIX_NSPACE)) {
                PMIX_LOAD_NSPACE(proc.nspace, cd->queries[n].qualifiers[p].value.data.string);
            } else if (PMIX_CHECK_KEY(&cd->queries[n].qualifiers[p], PMIX_RANK)) {
                proc.rank = cd->queries[n].qualifiers[p].value.data.rank;
            } else if (PMIX_CHECK_KEY(&cd->queries[n].qualifiers[p], PMIX_HOSTNAME)) {
                if (0 != strcmp(cd->queries[n].qualifiers[p].value.data.string, pmix_globals.hostname)) {
                    /* asking about a different host, so ask for the info */
                    PMIX_LIST_DESTRUCT(&results);
                    goto query;
                }
            }
        }
        /* we get here if a refresh isn't required - first try a local
         * "get" on the data to see if we already have it */
        PMIX_CONSTRUCT(&cb, pmix_cb_t);
        cb.copy = false;
        /* set the proc */
        if (PMIX_RANK_INVALID == proc.rank &&
            0 == strlen(proc.nspace)) {
            /* use our id */
            cb.proc = &pmix_globals.myid;
        } else {
            if (0 == strlen(proc.nspace)) {
                /* use our nspace */
                PMIX_LOAD_NSPACE(cb.proc->nspace, pmix_globals.myid.nspace);
            }
            if (PMIX_RANK_INVALID == proc.rank) {
                /* user the wildcard rank */
                proc.rank = PMIX_RANK_WILDCARD;
            }
            cb.proc = &proc;
        }
        for (p=0; NULL != cd->queries[n].keys[p]; p++) {
            cb.key = cd->queries[n].keys[p];
            PMIX_GDS_FETCH_KV(rc, pmix_globals.mypeer, &cb);
            if (PMIX_SUCCESS != rc) {
                /* needs to be passed to the host */
                PMIX_LIST_DESTRUCT(&results);
                PMIX_DESTRUCT(&cb);
                goto query;
            }
            /* need to retain this result */
            PMIX_LIST_FOREACH_SAFE(kv, kvnxt, &cb.kvs, pmix_kval_t) {
                pmix_list_remove_item(&cb.kvs, &kv->super);
                pmix_list_append(&results, &kv->super);
            }
            PMIX_DESTRUCT(&cb);
        }
    }

    /* if we get here, then all queries were completely locally
     * resolved, so construct the results for return */
    rc = PMIX_ERR_NOT_FOUND;
    if (0 < (cd->ninfo = pmix_list_get_size(&results))) {
        PMIX_INFO_CREATE(cd->info, cd->ninfo);
        n = 0;
        PMIX_LIST_FOREACH_SAFE(kv, kvnxt, &results, pmix_kval_t) {
            PMIX_LOAD_KEY(cd->info[n].key, kv->key);
            rc = pmix_value_xfer(&cd->info[n].value, kv->value);
            if (PMIX_SUCCESS != rc) {
                PMIX_INFO_FREE(cd->info, cd->ninfo);
                cd->info = NULL;
                cd->ninfo = 0;
                break;
            }
            ++n;
        }
    }
    /* done with the list of results */
    PMIX_LIST_DESTRUCT(&results);
    /* we can just call the cbfunc here as we are already
     * in an event - let our internal cbfunc do a threadshift
     * if necessary */
    cbfunc(PMIX_SUCCESS, cd->info, cd->ninfo, cd, NULL, NULL);
    return PMIX_SUCCESS;

  query:
    if (NULL == pmix_host_server.query) {
        PMIX_RELEASE(cd);
        return PMIX_ERR_NOT_SUPPORTED;
    }

    /* setup the requesting peer name */
    PMIX_LOAD_PROCID(&proc, peer->info->pname.nspace, peer->info->pname.rank);

    /* ask the host for the info */
    if (PMIX_SUCCESS != (rc = pmix_host_server.query(&proc, cd->queries, cd->nqueries,
                                                     cbfunc, cd))) {
        PMIX_RELEASE(cd);
    }
    return rc;
}

static void logcbfn(pmix_status_t status, void *cbdata)
{
    pmix_shift_caddy_t *cd = (pmix_shift_caddy_t*)cbdata;

    if (NULL != cd->cbfunc.opcbfn) {
        cd->cbfunc.opcbfn(status, cd->cbdata);
    }
    PMIX_RELEASE(cd);
}
pmix_status_t pmix_server_log(pmix_peer_t *peer,
                              pmix_buffer_t *buf,
                              pmix_op_cbfunc_t cbfunc,
                              void *cbdata)
{
    int32_t cnt;
    pmix_status_t rc;
    pmix_shift_caddy_t *cd;
    pmix_proc_t proc;
    time_t timestamp;

    pmix_output_verbose(2, pmix_server_globals.base_output,
                        "recvd log from client");

    /* we need to deliver this to our internal log capability,
     * which may upcall it to our host if it cannot process
     * the request itself */

    /* setup the requesting peer name */
    pmix_strncpy(proc.nspace, peer->info->pname.nspace, PMIX_MAX_NSLEN);
    proc.rank = peer->info->pname.rank;

    cd = PMIX_NEW(pmix_shift_caddy_t);
    if (NULL == cd) {
        return PMIX_ERR_NOMEM;
    }
    cd->cbfunc.opcbfn = cbfunc;
    cd->cbdata = cbdata;
    /* unpack the timestamp */
    cnt = 1;
    PMIX_BFROPS_UNPACK(rc, peer, buf, &timestamp, &cnt, PMIX_TIME);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        goto exit;
    }

    /* unpack the number of data */
    cnt = 1;
    PMIX_BFROPS_UNPACK(rc, peer, buf, &cd->ninfo, &cnt, PMIX_SIZE);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        goto exit;
    }
    cnt = cd->ninfo;
    PMIX_INFO_CREATE(cd->info, cd->ninfo);
    /* unpack the data */
    if (0 < cnt) {
        PMIX_BFROPS_UNPACK(rc, peer, buf, cd->info, &cnt, PMIX_INFO);
        if (PMIX_SUCCESS != rc) {
            PMIX_ERROR_LOG(rc);
            goto exit;
        }
    }
    /* unpack the number of directives */
    cnt = 1;
    PMIX_BFROPS_UNPACK(rc, peer, buf, &cd->ndirs, &cnt, PMIX_SIZE);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        goto exit;
    }
    cnt = cd->ndirs;
    /* always add the source to the directives so we
     * can tell downstream if this gets "upcalled" to
     * our host for relay */
    cd->ndirs = cnt + 1;
    /* if a timestamp was sent, then we add it to the directives */
    if (0 < timestamp) {
        cd->ndirs++;
        PMIX_INFO_CREATE(cd->directives, cd->ndirs);
        PMIX_INFO_LOAD(&cd->directives[cnt], PMIX_LOG_SOURCE, &proc, PMIX_PROC);
        PMIX_INFO_LOAD(&cd->directives[cnt+1], PMIX_LOG_TIMESTAMP, &timestamp, PMIX_TIME);
    } else {
        PMIX_INFO_CREATE(cd->directives, cd->ndirs);
        PMIX_INFO_LOAD(&cd->directives[cnt], PMIX_LOG_SOURCE, &proc, PMIX_PROC);
    }

    /* unpack the directives */
    if (0 < cnt) {
        PMIX_BFROPS_UNPACK(rc, peer, buf, cd->directives, &cnt, PMIX_INFO);
        if (PMIX_SUCCESS != rc) {
            PMIX_ERROR_LOG(rc);
            goto exit;
        }
    }

    /* pass it down */
    rc = pmix_plog.log(&proc, cd->info, cd->ninfo,
                       cd->directives, cd->ndirs,
                       logcbfn, cd);
    return rc;

  exit:
    PMIX_RELEASE(cd);
    return rc;
}

pmix_status_t pmix_server_alloc(pmix_peer_t *peer,
                                pmix_buffer_t *buf,
                                pmix_info_cbfunc_t cbfunc,
                                void *cbdata)
{
    int32_t cnt;
    pmix_status_t rc;
    pmix_query_caddy_t *cd;
    pmix_proc_t proc;
    pmix_alloc_directive_t directive;

    pmix_output_verbose(2, pmix_server_globals.base_output,
                        "recvd query from client");

    if (NULL == pmix_host_server.allocate) {
        return PMIX_ERR_NOT_SUPPORTED;
    }

    cd = PMIX_NEW(pmix_query_caddy_t);
    if (NULL == cd) {
        return PMIX_ERR_NOMEM;
    }
    cd->cbdata = cbdata;

    /* unpack the directive */
    cnt = 1;
    PMIX_BFROPS_UNPACK(rc, peer, buf, &directive, &cnt, PMIX_ALLOC_DIRECTIVE);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        goto exit;
    }

    /* unpack the number of info objects */
    cnt = 1;
    PMIX_BFROPS_UNPACK(rc, peer, buf, &cd->ninfo, &cnt, PMIX_SIZE);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        goto exit;
    }
    /* unpack the info */
    if (0 < cd->ninfo) {
        PMIX_INFO_CREATE(cd->info, cd->ninfo);
        cnt = cd->ninfo;
        PMIX_BFROPS_UNPACK(rc, peer, buf, cd->info, &cnt, PMIX_INFO);
        if (PMIX_SUCCESS != rc) {
            PMIX_ERROR_LOG(rc);
            goto exit;
        }
    }

    /* setup the requesting peer name */
    pmix_strncpy(proc.nspace, peer->info->pname.nspace, PMIX_MAX_NSLEN);
    proc.rank = peer->info->pname.rank;

    /* ask the host to execute the request */
    if (PMIX_SUCCESS != (rc = pmix_host_server.allocate(&proc, directive,
                                                        cd->info, cd->ninfo,
                                                        cbfunc, cd))) {
        goto exit;
    }
    return PMIX_SUCCESS;

  exit:
    PMIX_RELEASE(cd);
    return rc;
}

typedef struct {
    pmix_list_item_t super;
    pmix_epilog_t *epi;
} pmix_srvr_epi_caddy_t;
static PMIX_CLASS_INSTANCE(pmix_srvr_epi_caddy_t,
                           pmix_list_item_t,
                           NULL, NULL);

pmix_status_t pmix_server_job_ctrl(pmix_peer_t *peer,
                                   pmix_buffer_t *buf,
                                   pmix_info_cbfunc_t cbfunc,
                                   void *cbdata)
{
    int32_t cnt, m;
    pmix_status_t rc;
    pmix_query_caddy_t *cd;
    pmix_namespace_t *nptr, *tmp;
    pmix_peer_t *pr;
    pmix_proc_t proc;
    size_t n;
    bool recurse = false, leave_topdir = false, duplicate;
    pmix_list_t cachedirs, cachefiles, ignorefiles, epicache;
    pmix_srvr_epi_caddy_t *epicd = NULL;
    pmix_cleanup_file_t *cf, *cf2, *cfptr;
    pmix_cleanup_dir_t *cdir, *cdir2, *cdirptr;

    pmix_output_verbose(2, pmix_server_globals.base_output,
                        "recvd job control request from client");

    if (NULL == pmix_host_server.job_control) {
        return PMIX_ERR_NOT_SUPPORTED;
    }

    cd = PMIX_NEW(pmix_query_caddy_t);
    if (NULL == cd) {
        return PMIX_ERR_NOMEM;
    }
    cd->cbdata = cbdata;

    PMIX_CONSTRUCT(&epicache, pmix_list_t);

    /* unpack the number of targets */
    cnt = 1;
    PMIX_BFROPS_UNPACK(rc, peer, buf, &cd->ntargets, &cnt, PMIX_SIZE);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        goto exit;
    }
    if (0 < cd->ntargets) {
        PMIX_PROC_CREATE(cd->targets, cd->ntargets);
        cnt = cd->ntargets;
        PMIX_BFROPS_UNPACK(rc, peer, buf, cd->targets, &cnt, PMIX_PROC);
        if (PMIX_SUCCESS != rc) {
            PMIX_ERROR_LOG(rc);
            goto exit;
        }
    }

    /* check targets to find proper place to put any epilog requests */
    if (NULL == cd->targets) {
        epicd = PMIX_NEW(pmix_srvr_epi_caddy_t);
        epicd->epi = &peer->nptr->epilog;
        pmix_list_append(&epicache, &epicd->super);
    } else {
        for (n=0; n < cd->ntargets; n++) {
            /* find the nspace of this proc */
            nptr = NULL;
            PMIX_LIST_FOREACH(tmp, &pmix_server_globals.nspaces, pmix_namespace_t) {
                if (0 == strcmp(tmp->nspace, cd->targets[n].nspace)) {
                    nptr = tmp;
                    break;
                }
            }
            if (NULL == nptr) {
                nptr = PMIX_NEW(pmix_namespace_t);
                if (NULL == nptr) {
                    rc = PMIX_ERR_NOMEM;
                    goto exit;
                }
                nptr->nspace = strdup(cd->targets[n].nspace);
                pmix_list_append(&pmix_server_globals.nspaces, &nptr->super);
            }
            /* if the rank is wildcard, then we use the epilog for the nspace */
            if (PMIX_RANK_WILDCARD == cd->targets[n].rank) {
                epicd = PMIX_NEW(pmix_srvr_epi_caddy_t);
                epicd->epi = &nptr->epilog;
                pmix_list_append(&epicache, &epicd->super);
            } else {
                /* we need to find the precise peer - we can only
                 * do cleanup for a local client */
                for (m=0; m < pmix_server_globals.clients.size; m++) {
                    if (NULL == (pr = (pmix_peer_t*)pmix_pointer_array_get_item(&pmix_server_globals.clients, m))) {
                        continue;
                    }
                    if (0 != strncmp(pr->info->pname.nspace, cd->targets[n].nspace, PMIX_MAX_NSLEN)) {
                        continue;
                    }
                    if (pr->info->pname.rank == cd->targets[n].rank) {
                        epicd = PMIX_NEW(pmix_srvr_epi_caddy_t);
                        epicd->epi = &pr->epilog;
                        pmix_list_append(&epicache, &epicd->super);
                        break;
                    }
                }
            }
        }
    }

    /* unpack the number of info objects */
    cnt = 1;
    PMIX_BFROPS_UNPACK(rc, peer, buf, &cd->ninfo, &cnt, PMIX_SIZE);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        goto exit;
    }
    /* unpack the info */
    if (0 < cd->ninfo) {
        PMIX_INFO_CREATE(cd->info, cd->ninfo);
        cnt = cd->ninfo;
        PMIX_BFROPS_UNPACK(rc, peer, buf, cd->info, &cnt, PMIX_INFO);
        if (PMIX_SUCCESS != rc) {
            PMIX_ERROR_LOG(rc);
            goto exit;
        }
    }

    /* if this includes a request for post-termination cleanup, we handle
     * that request ourselves */
    PMIX_CONSTRUCT(&cachedirs, pmix_list_t);
    PMIX_CONSTRUCT(&cachefiles, pmix_list_t);
    PMIX_CONSTRUCT(&ignorefiles, pmix_list_t);

    cnt = 0;  // track how many infos are cleanup related
    for (n=0; n < cd->ninfo; n++) {
        if (0 == strncmp(cd->info[n].key, PMIX_REGISTER_CLEANUP, PMIX_MAX_KEYLEN)) {
            ++cnt;
            if (PMIX_STRING != cd->info[n].value.type ||
                NULL == cd->info[n].value.data.string) {
                /* return an error */
                rc = PMIX_ERR_BAD_PARAM;
                goto exit;
            }
            cf = PMIX_NEW(pmix_cleanup_file_t);
            if (NULL == cf) {
                /* return an error */
                rc = PMIX_ERR_NOMEM;
                goto exit;
            }
            cf->path = strdup(cd->info[n].value.data.string);
            pmix_list_append(&cachefiles, &cf->super);
        } else if (0 == strncmp(cd->info[n].key, PMIX_REGISTER_CLEANUP_DIR, PMIX_MAX_KEYLEN)) {
            ++cnt;
            if (PMIX_STRING != cd->info[n].value.type ||
                NULL == cd->info[n].value.data.string) {
                /* return an error */
                rc = PMIX_ERR_BAD_PARAM;
                goto exit;
            }
            cdir = PMIX_NEW(pmix_cleanup_dir_t);
            if (NULL == cdir) {
                /* return an error */
                rc = PMIX_ERR_NOMEM;
                goto exit;
            }
            cdir->path = strdup(cd->info[n].value.data.string);
            pmix_list_append(&cachedirs, &cdir->super);
        } else if (0 == strncmp(cd->info[n].key, PMIX_CLEANUP_RECURSIVE, PMIX_MAX_KEYLEN)) {
            recurse = PMIX_INFO_TRUE(&cd->info[n]);
            ++cnt;
        } else if (0 == strncmp(cd->info[n].key, PMIX_CLEANUP_IGNORE, PMIX_MAX_KEYLEN)) {
            if (PMIX_STRING != cd->info[n].value.type ||
                NULL == cd->info[n].value.data.string) {
                /* return an error */
                rc = PMIX_ERR_BAD_PARAM;
                goto exit;
            }
            cf = PMIX_NEW(pmix_cleanup_file_t);
            if (NULL == cf) {
                /* return an error */
                rc = PMIX_ERR_NOMEM;
                goto exit;
            }
            cf->path = strdup(cd->info[n].value.data.string);
            pmix_list_append(&ignorefiles, &cf->super);
            ++cnt;
        } else if (0 == strncmp(cd->info[n].key, PMIX_CLEANUP_LEAVE_TOPDIR, PMIX_MAX_KEYLEN)) {
            leave_topdir = PMIX_INFO_TRUE(&cd->info[n]);
            ++cnt;
        }
    }
    if (0 < cnt) {
        /* handle any ignore directives first */
        PMIX_LIST_FOREACH(cf, &ignorefiles, pmix_cleanup_file_t) {
            PMIX_LIST_FOREACH(epicd, &epicache, pmix_srvr_epi_caddy_t) {
                /* scan the existing list of files for any duplicate */
                duplicate = false;
                PMIX_LIST_FOREACH(cf2, &epicd->epi->cleanup_files, pmix_cleanup_file_t) {
                    if (0 == strcmp(cf2->path, cf->path)) {
                        duplicate = true;
                        break;
                    }
                }
                if (!duplicate) {
                    /* append it to the end of the list */
                    cfptr = PMIX_NEW(pmix_cleanup_file_t);
                    cfptr->path = strdup(cf->path);
                    pmix_list_append(&epicd->epi->ignores, &cf->super);
                }
            }
        }
        PMIX_LIST_DESTRUCT(&ignorefiles);
        /* now look at the directories */
        PMIX_LIST_FOREACH(cdir, &cachedirs, pmix_cleanup_dir_t) {
            PMIX_LIST_FOREACH(epicd, &epicache, pmix_srvr_epi_caddy_t) {
                /* scan the existing list of directories for any duplicate */
                duplicate = false;
                PMIX_LIST_FOREACH(cdir2, &epicd->epi->cleanup_dirs, pmix_cleanup_dir_t) {
                    if (0 == strcmp(cdir2->path, cdir->path)) {
                        /* duplicate - check for difference in flags per RFC
                         * precedence rules */
                        if (!cdir->recurse && recurse) {
                            cdir->recurse = recurse;
                        }
                        if (!cdir->leave_topdir && leave_topdir) {
                            cdir->leave_topdir = leave_topdir;
                        }
                        duplicate = true;
                        break;
                    }
                }
                if (!duplicate) {
                    /* check for conflict with ignore */
                    PMIX_LIST_FOREACH(cf, &epicd->epi->ignores, pmix_cleanup_file_t) {
                        if (0 == strcmp(cf->path, cdir->path)) {
                            /* return an error */
                            rc = PMIX_ERR_CONFLICTING_CLEANUP_DIRECTIVES;
                            PMIX_LIST_DESTRUCT(&cachedirs);
                            PMIX_LIST_DESTRUCT(&cachefiles);
                            goto exit;
                        }
                    }
                    /* append it to the end of the list */
                    cdirptr = PMIX_NEW(pmix_cleanup_dir_t);
                    cdirptr->path = strdup(cdir->path);
                    cdirptr->recurse = recurse;
                    cdirptr->leave_topdir = leave_topdir;
                    pmix_list_append(&epicd->epi->cleanup_dirs, &cdirptr->super);
                }
            }
        }
        PMIX_LIST_DESTRUCT(&cachedirs);
        PMIX_LIST_FOREACH(cf, &cachefiles, pmix_cleanup_file_t) {
            PMIX_LIST_FOREACH(epicd, &epicache, pmix_srvr_epi_caddy_t) {
                /* scan the existing list of files for any duplicate */
                duplicate = false;
                PMIX_LIST_FOREACH(cf2, &epicd->epi->cleanup_files, pmix_cleanup_file_t) {
                    if (0 == strcmp(cf2->path, cf->path)) {
                        duplicate = true;
                        break;
                    }
                }
                if (!duplicate) {
                    /* check for conflict with ignore */
                    PMIX_LIST_FOREACH(cf2, &epicd->epi->ignores, pmix_cleanup_file_t) {
                        if (0 == strcmp(cf->path, cf2->path)) {
                            /* return an error */
                            rc = PMIX_ERR_CONFLICTING_CLEANUP_DIRECTIVES;
                            PMIX_LIST_DESTRUCT(&cachedirs);
                            PMIX_LIST_DESTRUCT(&cachefiles);
                            goto exit;
                        }
                    }
                    /* append it to the end of the list */
                    cfptr = PMIX_NEW(pmix_cleanup_file_t);
                    cfptr->path = strdup(cf->path);
                    pmix_list_append(&epicd->epi->cleanup_files, &cfptr->super);
                }
            }
        }
        PMIX_LIST_DESTRUCT(&cachefiles);
        if (cnt == (int)cd->ninfo) {
            /* nothing more to do */
            rc = PMIX_OPERATION_SUCCEEDED;
            goto exit;
        }
    }

    /* setup the requesting peer name */
    pmix_strncpy(proc.nspace, peer->info->pname.nspace, PMIX_MAX_NSLEN);
    proc.rank = peer->info->pname.rank;

    /* ask the host to execute the request */
    if (PMIX_SUCCESS != (rc = pmix_host_server.job_control(&proc,
                                                           cd->targets, cd->ntargets,
                                                           cd->info, cd->ninfo,
                                                           cbfunc, cd))) {
        goto exit;
    }
    PMIX_LIST_DESTRUCT(&epicache);
    return PMIX_SUCCESS;

  exit:
    PMIX_RELEASE(cd);
    PMIX_LIST_DESTRUCT(&epicache);
    return rc;
}

pmix_status_t pmix_server_monitor(pmix_peer_t *peer,
                                  pmix_buffer_t *buf,
                                  pmix_info_cbfunc_t cbfunc,
                                  void *cbdata)
{
    int32_t cnt;
    pmix_info_t monitor;
    pmix_status_t rc, error;
    pmix_query_caddy_t *cd;
    pmix_proc_t proc;

    pmix_output_verbose(2, pmix_server_globals.base_output,
                        "recvd monitor request from client");


    cd = PMIX_NEW(pmix_query_caddy_t);
    if (NULL == cd) {
        return PMIX_ERR_NOMEM;
    }
    cd->cbdata = cbdata;

    /* unpack what is to be monitored */
    PMIX_INFO_CONSTRUCT(&monitor);
    cnt = 1;
    PMIX_BFROPS_UNPACK(rc, peer, buf, &monitor, &cnt, PMIX_INFO);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        goto exit;
    }

    /* unpack the error code */
    cnt = 1;
    PMIX_BFROPS_UNPACK(rc, peer, buf, &error, &cnt, PMIX_STATUS);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        goto exit;
    }

    /* unpack the number of directives */
    cnt = 1;
    PMIX_BFROPS_UNPACK(rc, peer, buf, &cd->ninfo, &cnt, PMIX_SIZE);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        goto exit;
    }
    /* unpack the directives */
    if (0 < cd->ninfo) {
        PMIX_INFO_CREATE(cd->info, cd->ninfo);
        cnt = cd->ninfo;
        PMIX_BFROPS_UNPACK(rc, peer, buf, cd->info, &cnt, PMIX_INFO);
        if (PMIX_SUCCESS != rc) {
            PMIX_ERROR_LOG(rc);
            goto exit;
        }
    }

    /* see if they are requesting one of the monitoring
     * methods we internally support */
    rc = pmix_psensor.start(peer, error, &monitor, cd->info, cd->ninfo);
    if (PMIX_SUCCESS == rc) {
        rc = PMIX_OPERATION_SUCCEEDED;
        goto exit;
    }
    if (PMIX_ERR_NOT_SUPPORTED != rc) {
        goto exit;
    }

    /* if we don't internally support it, see if
     * our host does */
    if (NULL == pmix_host_server.monitor) {
        rc = PMIX_ERR_NOT_SUPPORTED;
        goto exit;
    }

    /* setup the requesting peer name */
    pmix_strncpy(proc.nspace, peer->info->pname.nspace, PMIX_MAX_NSLEN);
    proc.rank = peer->info->pname.rank;

    /* ask the host to execute the request */
    if (PMIX_SUCCESS != (rc = pmix_host_server.monitor(&proc, &monitor, error,
                                                       cd->info, cd->ninfo,
                                                       cbfunc, cd))) {
        goto exit;
    }
    return PMIX_SUCCESS;

  exit:
    PMIX_INFO_DESTRUCT(&monitor);
    PMIX_RELEASE(cd);
    return rc;
}

pmix_status_t pmix_server_get_credential(pmix_peer_t *peer,
                                         pmix_buffer_t *buf,
                                         pmix_credential_cbfunc_t cbfunc,
                                         void *cbdata)
{
    int32_t cnt;
    pmix_status_t rc;
    pmix_query_caddy_t *cd;
    pmix_proc_t proc;

    pmix_output_verbose(2, pmix_globals.debug_output,
                        "recvd get credential request from client");

    if (NULL == pmix_host_server.get_credential) {
        return PMIX_ERR_NOT_SUPPORTED;
    }

    cd = PMIX_NEW(pmix_query_caddy_t);
    if (NULL == cd) {
        return PMIX_ERR_NOMEM;
    }
    cd->cbdata = cbdata;

    /* unpack the number of directives */
    cnt = 1;
    PMIX_BFROPS_UNPACK(rc, peer, buf, &cd->ninfo, &cnt, PMIX_SIZE);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        goto exit;
    }
    /* unpack the directives */
    if (0 < cd->ninfo) {
        PMIX_INFO_CREATE(cd->info, cd->ninfo);
        cnt = cd->ninfo;
        PMIX_BFROPS_UNPACK(rc, peer, buf, cd->info, &cnt, PMIX_INFO);
        if (PMIX_SUCCESS != rc) {
            PMIX_ERROR_LOG(rc);
            goto exit;
        }
    }

    /* setup the requesting peer name */
    pmix_strncpy(proc.nspace, peer->info->pname.nspace, PMIX_MAX_NSLEN);
    proc.rank = peer->info->pname.rank;

    /* ask the host to execute the request */
    if (PMIX_SUCCESS != (rc = pmix_host_server.get_credential(&proc, cd->info, cd->ninfo,
                                                              cbfunc, cd))) {
        goto exit;
    }
    return PMIX_SUCCESS;

  exit:
    PMIX_RELEASE(cd);
    return rc;
}

pmix_status_t pmix_server_validate_credential(pmix_peer_t *peer,
                                              pmix_buffer_t *buf,
                                              pmix_validation_cbfunc_t cbfunc,
                                              void *cbdata)
{
    int32_t cnt;
    pmix_status_t rc;
    pmix_query_caddy_t *cd;
    pmix_proc_t proc;

    pmix_output_verbose(2, pmix_globals.debug_output,
                        "recvd validate credential request from client");

    if (NULL == pmix_host_server.validate_credential) {
        return PMIX_ERR_NOT_SUPPORTED;
    }

    cd = PMIX_NEW(pmix_query_caddy_t);
    if (NULL == cd) {
        return PMIX_ERR_NOMEM;
    }
    cd->cbdata = cbdata;

    /* unpack the credential */
    cnt = 1;
    PMIX_BFROPS_UNPACK(rc, peer, buf, &cd->bo, &cnt, PMIX_BYTE_OBJECT);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        goto exit;
    }

    /* unpack the number of directives */
    cnt = 1;
    PMIX_BFROPS_UNPACK(rc, peer, buf, &cd->ninfo, &cnt, PMIX_SIZE);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        goto exit;
    }
    /* unpack the directives */
    if (0 < cd->ninfo) {
        PMIX_INFO_CREATE(cd->info, cd->ninfo);
        cnt = cd->ninfo;
        PMIX_BFROPS_UNPACK(rc, peer, buf, cd->info, &cnt, PMIX_INFO);
        if (PMIX_SUCCESS != rc) {
            PMIX_ERROR_LOG(rc);
            goto exit;
        }
    }

    /* setup the requesting peer name */
    pmix_strncpy(proc.nspace, peer->info->pname.nspace, PMIX_MAX_NSLEN);
    proc.rank = peer->info->pname.rank;

    /* ask the host to execute the request */
    if (PMIX_SUCCESS != (rc = pmix_host_server.validate_credential(&proc, &cd->bo,
                                                                   cd->info, cd->ninfo,
                                                                   cbfunc, cd))) {
        goto exit;
    }
    return PMIX_SUCCESS;

  exit:
    PMIX_RELEASE(cd);
    return rc;
}

pmix_status_t pmix_server_iofreg(pmix_peer_t *peer,
                                 pmix_buffer_t *buf,
                                 pmix_op_cbfunc_t cbfunc,
                                 void *cbdata)
{
    int32_t cnt;
    pmix_status_t rc;
    pmix_setup_caddy_t *cd;
    pmix_iof_req_t *req;
    bool notify, match;
    size_t n;
    pmix_buffer_t *msg;
    pmix_iof_cache_t *iof, *ionext;

    pmix_output_verbose(2, pmix_server_globals.iof_output,
                        "recvd IOF PULL request from client");

    if (NULL == pmix_host_server.iof_pull) {
        return PMIX_ERR_NOT_SUPPORTED;
    }

    cd = PMIX_NEW(pmix_setup_caddy_t);
    if (NULL == cd) {
        return PMIX_ERR_NOMEM;
    }
    cd->cbdata = cbdata;  // this is the pmix_server_caddy_t

    /* unpack the number of procs */
    cnt = 1;
    PMIX_BFROPS_UNPACK(rc, peer, buf, &cd->nprocs, &cnt, PMIX_SIZE);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        goto exit;
    }
    /* unpack the procs */
    if (0 < cd->nprocs) {
        PMIX_PROC_CREATE(cd->procs, cd->nprocs);
        cnt = cd->nprocs;
        PMIX_BFROPS_UNPACK(rc, peer, buf, cd->procs, &cnt, PMIX_PROC);
        if (PMIX_SUCCESS != rc) {
            PMIX_ERROR_LOG(rc);
            goto exit;
        }
    }

    /* unpack the number of directives */
    cnt = 1;
    PMIX_BFROPS_UNPACK(rc, peer, buf, &cd->ninfo, &cnt, PMIX_SIZE);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        goto exit;
    }
    /* unpack the directives */
    if (0 < cd->ninfo) {
        PMIX_INFO_CREATE(cd->info, cd->ninfo);
        cnt = cd->ninfo;
        PMIX_BFROPS_UNPACK(rc, peer, buf, cd->info, &cnt, PMIX_INFO);
        if (PMIX_SUCCESS != rc) {
            PMIX_ERROR_LOG(rc);
            goto exit;
        }
    }

    /* unpack the channels */
    cnt = 1;
    PMIX_BFROPS_UNPACK(rc, peer, buf, &cd->channels, &cnt, PMIX_IOF_CHANNEL);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        goto exit;
    }

    /* check to see if we have already registered this source/channel combination */
    notify = false;
    for (n=0; n < cd->nprocs; n++) {
        match = false;
        PMIX_LIST_FOREACH(req, &pmix_globals.iof_requests, pmix_iof_req_t) {
            /* is this request from the same peer? */
            if (peer != req->peer) {
                continue;
            }
            /* do we already have this source for this peer? */
            if (0 == strncmp(cd->procs[n].nspace, req->pname.nspace, PMIX_MAX_NSLEN) &&
                (PMIX_RANK_WILDCARD == req->pname.rank || cd->procs[n].rank == req->pname.rank)) {
                match = true;
                if ((req->channels & cd->channels) != cd->channels) {
                    /* this is a channel update */
                    req->channels |= cd->channels;
                    /* we need to notify the host */
                    notify = true;
                }
                break;
            }
        }
        /* if we didn't find the matching entry, then add it */
        if (!match) {
            /* record the request */
            req = PMIX_NEW(pmix_iof_req_t);
            if (NULL == req) {
                rc = PMIX_ERR_NOMEM;
                goto exit;
            }
            PMIX_RETAIN(peer);
            req->peer = peer;
            req->pname.nspace = strdup(cd->procs[n].nspace);
            req->pname.rank = cd->procs[n].rank;
            req->channels = cd->channels;
            pmix_list_append(&pmix_globals.iof_requests, &req->super);
        }
        /* process any cached IO */
        PMIX_LIST_FOREACH_SAFE(iof, ionext, &pmix_server_globals.iof, pmix_iof_cache_t) {
            /* if the channels don't match, then ignore it */
            if (!(iof->channel & req->channels)) {
                continue;
            }
            /* if the source does not match the request, then ignore it */
            if (!PMIX_CHECK_PROCID(&iof->source, &req->pname)) {
                continue;
            }
            /* never forward back to the source! This can happen if the source
             * is a launcher */
            if (PMIX_CHECK_PROCID(&iof->source, &req->peer->info->pname)) {
                continue;
            }
            pmix_output_verbose(2, pmix_server_globals.iof_output,
                                "PMIX:SERVER:IOFREQ delivering cached IOF from %s:%d to %s:%d",
                                iof->source.nspace, iof->source.rank,
                                req->peer->info->pname.nspace, req->peer->info->pname.rank);
            /* setup the msg */
            if (NULL == (msg = PMIX_NEW(pmix_buffer_t))) {
                PMIX_ERROR_LOG(PMIX_ERR_OUT_OF_RESOURCE);
                rc = PMIX_ERR_OUT_OF_RESOURCE;
                break;
            }
            /* provide the source */
            PMIX_BFROPS_PACK(rc, req->peer, msg, &iof->source, 1, PMIX_PROC);
            if (PMIX_SUCCESS != rc) {
                PMIX_ERROR_LOG(rc);
                PMIX_RELEASE(msg);
                break;
            }
            /* provide the channel */
            PMIX_BFROPS_PACK(rc, req->peer, msg, &iof->channel, 1, PMIX_IOF_CHANNEL);
            if (PMIX_SUCCESS != rc) {
                PMIX_ERROR_LOG(rc);
                PMIX_RELEASE(msg);
                break;
            }
            /* pack the data */
            PMIX_BFROPS_PACK(rc, req->peer, msg, iof->bo, 1, PMIX_BYTE_OBJECT);
            if (PMIX_SUCCESS != rc) {
                PMIX_ERROR_LOG(rc);
                PMIX_RELEASE(msg);
                break;
            }
            /* send it to the requestor */
            PMIX_PTL_SEND_ONEWAY(rc, req->peer, msg, PMIX_PTL_TAG_IOF);
            if (PMIX_SUCCESS != rc) {
                PMIX_ERROR_LOG(rc);
                PMIX_RELEASE(msg);
            }
            /* remove it from the list since it has now been forwarded */
            pmix_list_remove_item(&pmix_server_globals.iof, &iof->super);
            PMIX_RELEASE(iof);
        }
    }
    if (notify) {
        /* ask the host to execute the request */
        if (PMIX_SUCCESS != (rc = pmix_host_server.iof_pull(cd->procs, cd->nprocs,
                                                            cd->info, cd->ninfo,
                                                            cd->channels,
                                                            cbfunc, cd))) {
            goto exit;
        }
    }
    return PMIX_SUCCESS;

  exit:
    PMIX_RELEASE(cd);
    return rc;
}

static void stdcbfunc(pmix_status_t status, void *cbdata)
{
    pmix_setup_caddy_t *cd = (pmix_setup_caddy_t*)cbdata;

    if (NULL != cd->opcbfunc) {
        cd->opcbfunc(status, cd->cbdata);
    }
    if (NULL != cd->procs) {
        PMIX_PROC_FREE(cd->procs, cd->nprocs);
    }
    if (NULL != cd->info) {
        PMIX_INFO_FREE(cd->info, cd->ninfo);
    }
    if (NULL != cd->bo) {
        PMIX_BYTE_OBJECT_FREE(cd->bo, 1);
    }
    PMIX_RELEASE(cd);
}

pmix_status_t pmix_server_iofstdin(pmix_peer_t *peer,
                                   pmix_buffer_t *buf,
                                   pmix_op_cbfunc_t cbfunc,
                                   void *cbdata)
{
    int32_t cnt;
    pmix_status_t rc;
    pmix_proc_t source;
    pmix_setup_caddy_t *cd;

    pmix_output_verbose(2, pmix_server_globals.iof_output,
                        "recvd stdin IOF data from tool");

    if (NULL == pmix_host_server.push_stdin) {
        return PMIX_ERR_NOT_SUPPORTED;
    }

    cd = PMIX_NEW(pmix_setup_caddy_t);
    if (NULL == cd) {
        return PMIX_ERR_NOMEM;
    }
    cd->opcbfunc = cbfunc;
    cd->cbdata = cbdata;

    /* unpack the number of targets */
    cnt = 1;
    PMIX_BFROPS_UNPACK(rc, peer, buf, &cd->nprocs, &cnt, PMIX_SIZE);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        goto error;
    }
    if (0 < cd->nprocs) {
        PMIX_PROC_CREATE(cd->procs, cd->nprocs);
        if (NULL == cd->procs) {
            rc = PMIX_ERR_NOMEM;
            goto error;
        }
        cnt = cd->nprocs;
        PMIX_BFROPS_UNPACK(rc, peer, buf, cd->procs, &cnt, PMIX_PROC);
        if (PMIX_SUCCESS != rc) {
            PMIX_ERROR_LOG(rc);
            goto error;
        }
    }

    /* unpack the number of directives */
    cnt = 1;
    PMIX_BFROPS_UNPACK(rc, peer, buf, &cd->ninfo, &cnt, PMIX_SIZE);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        goto error;
    }
    if (0 < cd->ninfo) {
        PMIX_INFO_CREATE(cd->info, cd->ninfo);
        if (NULL == cd->info) {
            rc = PMIX_ERR_NOMEM;
            goto error;
        }
        cnt = cd->ninfo;
        PMIX_BFROPS_UNPACK(rc, peer, buf, cd->info, &cnt, PMIX_INFO);
        if (PMIX_SUCCESS != rc) {
            PMIX_ERROR_LOG(rc);
            goto error;
        }
    }

    /* unpack the data */
    PMIX_BYTE_OBJECT_CREATE(cd->bo, 1);
    if (NULL == cd->bo) {
        rc = PMIX_ERR_NOMEM;
        goto error;
    }

    cnt = 1;
    PMIX_BFROPS_UNPACK(rc, peer, buf, cd->bo, &cnt, PMIX_BYTE_OBJECT);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        goto error;
    }

    /* pass the data to the host */
    pmix_strncpy(source.nspace, peer->nptr->nspace, PMIX_MAX_NSLEN);
    source.rank = peer->info->pname.rank;
    if (PMIX_SUCCESS != (rc = pmix_host_server.push_stdin(&source, cd->procs, cd->nprocs,
                                                          cd->info, cd->ninfo, cd->bo,
                                                          stdcbfunc, cd))) {
        goto error;
    }
    return PMIX_SUCCESS;

  error:
    PMIX_RELEASE(cd);
    return rc;
}

/*****    INSTANCE SERVER LIBRARY CLASSES    *****/
static void tcon(pmix_server_trkr_t *t)
{
    t->event_active = false;
    t->lost_connection = false;
    t->id = NULL;
    memset(t->pname.nspace, 0, PMIX_MAX_NSLEN+1);
    t->pname.rank = PMIX_RANK_UNDEF;
    t->pcs = NULL;
    t->npcs = 0;
    PMIX_CONSTRUCT_LOCK(&t->lock);
    t->def_complete = false;
    PMIX_CONSTRUCT(&t->local_cbs, pmix_list_t);
    t->nlocal = 0;
    t->local_cnt = 0;
    t->info = NULL;
    t->ninfo = 0;
    /* this needs to be set explicitly */
    t->collect_type = PMIX_COLLECT_INVALID;
    t->modexcbfunc = NULL;
    t->op_cbfunc = NULL;
    t->hybrid = false;
}
static void tdes(pmix_server_trkr_t *t)
{
    if (NULL != t->id) {
        free(t->id);
    }
    PMIX_DESTRUCT_LOCK(&t->lock);
    if (NULL != t->pcs) {
        free(t->pcs);
    }
    PMIX_LIST_DESTRUCT(&t->local_cbs);
    if (NULL != t->info) {
        PMIX_INFO_FREE(t->info, t->ninfo);
    }
}
PMIX_CLASS_INSTANCE(pmix_server_trkr_t,
                   pmix_list_item_t,
                   tcon, tdes);

static void cdcon(pmix_server_caddy_t *cd)
{
    memset(&cd->ev, 0, sizeof(pmix_event_t));
    cd->event_active = false;
    cd->trk = NULL;
    cd->peer = NULL;
}
static void cddes(pmix_server_caddy_t *cd)
{
    if (cd->event_active) {
        pmix_event_del(&cd->ev);
    }
    if (NULL != cd->trk) {
        PMIX_RELEASE(cd->trk);
    }
    if (NULL != cd->peer) {
        PMIX_RELEASE(cd->peer);
    }
}
PMIX_CLASS_INSTANCE(pmix_server_caddy_t,
                   pmix_list_item_t,
                   cdcon, cddes);


static void scadcon(pmix_setup_caddy_t *p)
{
    p->peer = NULL;
    memset(&p->proc, 0, sizeof(pmix_proc_t));
    PMIX_CONSTRUCT_LOCK(&p->lock);
    p->nspace = NULL;
    p->codes = NULL;
    p->ncodes = 0;
    p->procs = NULL;
    p->nprocs = 0;
    p->apps = NULL;
    p->napps = 0;
    p->server_object = NULL;
    p->nlocalprocs = 0;
    p->info = NULL;
    p->ninfo = 0;
    p->keys = NULL;
    p->channels = PMIX_FWD_NO_CHANNELS;
    p->bo = NULL;
    p->nbo = 0;
    p->cbfunc = NULL;
    p->opcbfunc = NULL;
    p->setupcbfunc = NULL;
    p->lkcbfunc = NULL;
    p->spcbfunc = NULL;
    p->cbdata = NULL;
}
static void scaddes(pmix_setup_caddy_t *p)
{
    if (NULL != p->peer) {
        PMIX_RELEASE(p->peer);
    }
    PMIX_PROC_FREE(p->procs, p->nprocs);
    if (NULL != p->apps) {
        PMIX_APP_FREE(p->apps, p->napps);
    }
    if (NULL != p->bo) {
        PMIX_BYTE_OBJECT_FREE(p->bo, p->nbo);
    }
    PMIX_DESTRUCT_LOCK(&p->lock);
}
PMIX_EXPORT PMIX_CLASS_INSTANCE(pmix_setup_caddy_t,
                                pmix_object_t,
                                scadcon, scaddes);

static void ncon(pmix_notify_caddy_t *p)
{
    struct timespec tp;

    PMIX_CONSTRUCT_LOCK(&p->lock);
    clock_gettime(CLOCK_MONOTONIC, &tp);
    p->ts = tp.tv_sec;
    p->room = -1;
    memset(p->source.nspace, 0, PMIX_MAX_NSLEN+1);
    p->source.rank = PMIX_RANK_UNDEF;
    p->range = PMIX_RANGE_UNDEF;
    p->targets = NULL;
    p->ntargets = 0;
    p->nleft = SIZE_MAX;
    p->affected = NULL;
    p->naffected = 0;
    p->nondefault = false;
    p->info = NULL;
    p->ninfo = 0;
}
static void ndes(pmix_notify_caddy_t *p)
{
    PMIX_DESTRUCT_LOCK(&p->lock);
    if (NULL != p->info) {
        PMIX_INFO_FREE(p->info, p->ninfo);
    }
    PMIX_PROC_FREE(p->affected, p->naffected);
    if (NULL != p->targets) {
        free(p->targets);
    }
}
PMIX_CLASS_INSTANCE(pmix_notify_caddy_t,
                    pmix_object_t,
                    ncon, ndes);


PMIX_CLASS_INSTANCE(pmix_trkr_caddy_t,
                    pmix_object_t,
                    NULL, NULL);

static void dmcon(pmix_dmdx_remote_t *p)
{
    p->cd = NULL;
}
static void dmdes(pmix_dmdx_remote_t *p)
{
    if (NULL != p->cd) {
        PMIX_RELEASE(p->cd);
    }
}
PMIX_CLASS_INSTANCE(pmix_dmdx_remote_t,
                    pmix_list_item_t,
                    dmcon, dmdes);

static void dmrqcon(pmix_dmdx_request_t *p)
{
    memset(&p->ev, 0, sizeof(pmix_event_t));
    p->event_active = false;
    p->lcd = NULL;
}
static void dmrqdes(pmix_dmdx_request_t *p)
{
    if (p->event_active) {
        pmix_event_del(&p->ev);
    }
    if (NULL != p->lcd) {
        PMIX_RELEASE(p->lcd);
    }
}
PMIX_CLASS_INSTANCE(pmix_dmdx_request_t,
                    pmix_list_item_t,
                    dmrqcon, dmrqdes);

static void lmcon(pmix_dmdx_local_t *p)
{
    memset(&p->proc, 0, sizeof(pmix_proc_t));
    PMIX_CONSTRUCT(&p->loc_reqs, pmix_list_t);
    p->info = NULL;
    p->ninfo = 0;
}
static void lmdes(pmix_dmdx_local_t *p)
{
    if (NULL != p->info) {
        PMIX_INFO_FREE(p->info, p->ninfo);
    }
    PMIX_LIST_DESTRUCT(&p->loc_reqs);
}
PMIX_CLASS_INSTANCE(pmix_dmdx_local_t,
                    pmix_list_item_t,
                    lmcon, lmdes);

static void prevcon(pmix_peer_events_info_t *p)
{
    p->peer = NULL;
    p->affected = NULL;
    p->naffected = 0;
}
static void prevdes(pmix_peer_events_info_t *p)
{
    if (NULL != p->peer) {
        PMIX_RELEASE(p->peer);
    }
    if (NULL != p->affected) {
        PMIX_PROC_FREE(p->affected, p->naffected);
    }
}
PMIX_CLASS_INSTANCE(pmix_peer_events_info_t,
                    pmix_list_item_t,
                    prevcon, prevdes);

static void regcon(pmix_regevents_info_t *p)
{
    PMIX_CONSTRUCT(&p->peers, pmix_list_t);
}
static void regdes(pmix_regevents_info_t *p)
{
    PMIX_LIST_DESTRUCT(&p->peers);
}
PMIX_CLASS_INSTANCE(pmix_regevents_info_t,
                    pmix_list_item_t,
                    regcon, regdes);

static void ilcon(pmix_inventory_rollup_t *p)
{
    PMIX_CONSTRUCT_LOCK(&p->lock);
    p->lock.active = false;
    p->status = PMIX_SUCCESS;
    p->requests = 0;
    p->replies = 0;
    PMIX_CONSTRUCT(&p->payload, pmix_list_t);
    p->info = NULL;
    p->ninfo = 0;
    p->cbfunc = NULL;
    p->infocbfunc = NULL;
    p->opcbfunc = NULL;
    p->cbdata = NULL;
}
static void ildes(pmix_inventory_rollup_t *p)
{
    PMIX_DESTRUCT_LOCK(&p->lock);
    PMIX_LIST_DESTRUCT(&p->payload);
}
PMIX_CLASS_INSTANCE(pmix_inventory_rollup_t,
                    pmix_object_t,
                    ilcon, ildes);

static void iocon(pmix_iof_cache_t *p)
{
    p->bo = NULL;
}
static void iodes(pmix_iof_cache_t *p)
{
    PMIX_BYTE_OBJECT_FREE(p->bo, 1);  // macro protects against NULL
}
PMIX_CLASS_INSTANCE(pmix_iof_cache_t,
                    pmix_list_item_t,
                    iocon, iodes);
