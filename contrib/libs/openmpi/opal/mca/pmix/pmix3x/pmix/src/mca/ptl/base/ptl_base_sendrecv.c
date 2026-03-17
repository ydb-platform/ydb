/*
 * Copyright (c) 2014-2019 Intel, Inc.  All rights reserved.
 * Copyright (c) 2014      Artem Y. Polyakov <artpol84@gmail.com>.
 *                         All rights reserved.
 * Copyright (c) 2015-2017 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
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

#include "src/class/pmix_pointer_array.h"
#include "src/include/pmix_globals.h"
#include "src/client/pmix_client_ops.h"
#include "src/server/pmix_server_ops.h"
#include "src/util/error.h"
#include "src/util/show_help.h"
#include "src/mca/psensor/psensor.h"

#include "src/mca/ptl/base/base.h"

static void _notify_complete(pmix_status_t status, void *cbdata)
{
    pmix_event_chain_t *chain = (pmix_event_chain_t*)cbdata;
    PMIX_RELEASE(chain);
}

static void _timeout(int sd, short args, void *cbdata)
{
    pmix_server_trkr_t *trk = (pmix_server_trkr_t*)cbdata;

    PMIX_RELEASE(trk);
}

static void lcfn(pmix_status_t status, void *cbdata)
{
    pmix_peer_t *peer = (pmix_peer_t*)cbdata;
    PMIX_RELEASE(peer);
}

void pmix_ptl_base_lost_connection(pmix_peer_t *peer, pmix_status_t err)
{
    pmix_server_trkr_t *trk, *tnxt;
    pmix_server_caddy_t *rinfo, *rnext;
    pmix_rank_info_t *info, *pinfo;
    pmix_ptl_posted_recv_t *rcv;
    pmix_buffer_t buf;
    pmix_ptl_hdr_t hdr;
    struct timeval tv = {1200, 0};
    pmix_proc_t proc;
    pmix_status_t rc;

    /* stop all events */
    if (peer->recv_ev_active) {
        pmix_event_del(&peer->recv_event);
        peer->recv_ev_active = false;
    }
    if (peer->send_ev_active) {
        pmix_event_del(&peer->send_event);
        peer->send_ev_active = false;
    }
    if (NULL != peer->recv_msg) {
        PMIX_RELEASE(peer->recv_msg);
        peer->recv_msg = NULL;
    }
    CLOSE_THE_SOCKET(peer->sd);

    if (PMIX_PROC_IS_SERVER(pmix_globals.mypeer) &&
        !PMIX_PROC_IS_TOOL(pmix_globals.mypeer)) {
        /* if I am a server, then we need to ensure that
         * we properly account for the loss of this client
         * from any local collectives in which it was
         * participating - note that the proc would not
         * have been added to any collective tracker until
         * after it successfully connected */
        PMIX_LIST_FOREACH_SAFE(trk, tnxt, &pmix_server_globals.collectives, pmix_server_trkr_t) {
            /* see if this proc is participating in this tracker */
            PMIX_LIST_FOREACH_SAFE(rinfo, rnext, &trk->local_cbs, pmix_server_caddy_t) {
                if (!PMIX_CHECK_PROCID(&rinfo->peer->info->pname, &peer->info->pname)) {
                    continue;
                }
                /* it is - adjust the count */
                --trk->nlocal;
                /* remove it from the list */
                pmix_list_remove_item(&trk->local_cbs, &rinfo->super);
                PMIX_RELEASE(rinfo);
                trk->lost_connection = true;  // mark that a peer's connection was lost
                if (0 == pmix_list_get_size(&trk->local_cbs)) {
                    /* this tracker is complete, so release it - there
                     * is nobody waiting for a response */
                    pmix_list_remove_item(&pmix_server_globals.collectives, &trk->super);
                    /* do NOT release the tracker here as the host may
                     * have a copy they will return later. However, they
                     * might never call back, so set a LONG timeout to
                     * we avoid a memory leak if they don't */
                    pmix_event_evtimer_set(pmix_globals.evbase, &trk->ev,
                                           _timeout, trk);
                    pmix_event_evtimer_add(&trk->ev, &tv);
                    trk->event_active = true;
                    break;
                }
                /* if there are other participants waiting for a response,
                 * we need to let them know that this proc has disappeared
                 * as otherwise the collective will never complete */
                if (PMIX_FENCENB_CMD == trk->type) {
                    if (NULL != trk->modexcbfunc) {
                        /* do NOT release the tracker here as the host may
                         * have a copy they will return later. However, they
                         * might never call back, so set a LONG timeout to
                         * we avoid a memory leak if they don't */
                        pmix_event_evtimer_set(pmix_globals.evbase, &trk->ev,
                                               _timeout, trk);
                        pmix_event_evtimer_add(&trk->ev, &tv);
                        trk->event_active = true;
                        trk->modexcbfunc(PMIX_ERR_LOST_CONNECTION_TO_CLIENT, NULL, 0, trk, NULL, NULL);
                    }
                } else if (PMIX_CONNECTNB_CMD == trk->type) {
                    if (NULL != trk->op_cbfunc) {
                        /* do NOT release the tracker here as the host may
                         * have a copy they will return later. However, they
                         * might never call back, so set a LONG timeout to
                         * we avoid a memory leak if they don't */
                        pmix_event_evtimer_set(pmix_globals.evbase, &trk->ev,
                                               _timeout, trk);
                        pmix_event_evtimer_add(&trk->ev, &tv);
                        trk->event_active = true;
                        trk->op_cbfunc(PMIX_ERR_LOST_CONNECTION_TO_CLIENT, trk);
                    }
                } else if (PMIX_DISCONNECTNB_CMD == trk->type) {
                    if (NULL != trk->op_cbfunc) {
                        /* do NOT release the tracker here as the host may
                         * have a copy they will return later. However, they
                         * might never call back, so set a LONG timeout to
                         * we avoid a memory leak if they don't */
                        pmix_event_evtimer_set(pmix_globals.evbase, &trk->ev,
                                               _timeout, trk);
                        pmix_event_evtimer_add(&trk->ev, &tv);
                        trk->event_active = true;
                        trk->op_cbfunc(PMIX_ERR_LOST_CONNECTION_TO_CLIENT, trk);
                    }
                }
            }
        }

        /* remove this proc from the list of ranks for this nspace if it is
         * still there - we must check for multiple copies as there will be
         * one for each "clone" of this peer */
        PMIX_LIST_FOREACH_SAFE(info, pinfo, &(peer->nptr->ranks), pmix_rank_info_t) {
            if (info == peer->info) {
                pmix_list_remove_item(&(peer->nptr->ranks), &(peer->info->super));
            }
        }
        /* reduce the number of local procs */
        if (0 < peer->nptr->nlocalprocs) {
            --peer->nptr->nlocalprocs;
        }

        /* remove this client from our array */
        pmix_pointer_array_set_item(&pmix_server_globals.clients,
                                    peer->index, NULL);

        /* purge any notifications cached for this client */
        pmix_server_purge_events(peer, NULL);

        if (PMIX_PROC_IS_LAUNCHER(pmix_globals.mypeer)) {
            /* only connection I can lose is to my server, so mark it */
            pmix_globals.connected = false;
        } else {
            /* cleanup any sensors that are monitoring them */
            pmix_psensor.stop(peer, NULL);
        }

        if (!peer->finalized && !PMIX_PROC_IS_TOOL(peer) && !pmix_globals.mypeer->finalized) {
            /* if this peer already called finalize, then
             * we are just seeing their connection go away
             * when they terminate - so do not generate
             * an event. If not, then we do */
            PMIX_REPORT_EVENT(err, peer, PMIX_RANGE_PROC_LOCAL, _notify_complete);
        }
        /* now decrease the refcount - might actually free the object */
        PMIX_RELEASE(peer->info);

        /* be sure to let the host know that the tool or client
         * is gone - otherwise, it won't know to cleanup the
         * resources it allocated to it */
        if (NULL != pmix_host_server.client_finalized && !peer->finalized) {
            pmix_strncpy(proc.nspace, peer->info->pname.nspace, PMIX_MAX_NSLEN);
            proc.rank = peer->info->pname.rank;
            /* now tell the host server */
            rc = pmix_host_server.client_finalized(&proc, peer->info->server_object,
                                                   lcfn, peer);
            if (PMIX_SUCCESS == rc) {
                /* we will release the peer when the server calls us back */
                peer->finalized = true;
                return;
            }
        }
        /* mark the peer as "gone" since a release doesn't guarantee
         * that the peer object doesn't persist */
        peer->finalized = true;
        /* Release peer info */
        PMIX_RELEASE(peer);
     } else {
        /* if I am a client, there is only
         * one connection we can have */
        pmix_globals.connected = false;
         /* set the public error status */
        err = PMIX_ERR_LOST_CONNECTION_TO_SERVER;
        /* it is possible that we have sendrecv's in progress where
         * we are waiting for a response to arrive. Since we have
         * lost connection to the server, that will never happen.
         * Thus, to preclude any chance of hanging, cycle thru
         * the list of posted recvs and complete any that are
         * the return call from a sendrecv - i.e., any that are
         * waiting on dynamic tags */
        PMIX_CONSTRUCT(&buf, pmix_buffer_t);
        /* must set the buffer type so it doesn't fail in unpack */
        buf.type = pmix_client_globals.myserver->nptr->compat.type;
        hdr.nbytes = 0; // initialize the hdr to something safe
        PMIX_LIST_FOREACH(rcv, &pmix_ptl_globals.posted_recvs, pmix_ptl_posted_recv_t) {
            if (UINT_MAX != rcv->tag && NULL != rcv->cbfunc) {
                /* construct and load the buffer */
                hdr.tag = rcv->tag;
                rcv->cbfunc(pmix_globals.mypeer, &hdr, &buf, rcv->cbdata);
            }
        }
        PMIX_DESTRUCT(&buf);
        /* if I called finalize, then don't generate an event */
        if (!pmix_globals.mypeer->finalized) {
            PMIX_REPORT_EVENT(err, pmix_client_globals.myserver, PMIX_RANGE_PROC_LOCAL, _notify_complete);
        }
    }
}

static pmix_status_t send_msg(int sd, pmix_ptl_send_t *msg)
{
    struct iovec iov[2];
    int iov_count;
    ssize_t remain = msg->sdbytes, rc;

    iov[0].iov_base = msg->sdptr;
    iov[0].iov_len = msg->sdbytes;
    if (!msg->hdr_sent && NULL != msg->data) {
        iov[1].iov_base = msg->data->base_ptr;
        iov[1].iov_len = ntohl(msg->hdr.nbytes);
        remain += ntohl(msg->hdr.nbytes);
        iov_count = 2;
    } else {
        iov_count = 1;
    }
  retry:
    rc = writev(sd, iov, iov_count);
    if (PMIX_LIKELY(rc == remain)) {
        /* we successfully sent the header and the msg data if any */
        msg->hdr_sent = true;
        msg->sdbytes = 0;
        msg->sdptr = (char *)iov[iov_count-1].iov_base + iov[iov_count-1].iov_len;
        return PMIX_SUCCESS;
    } else if (rc < 0) {
        if (pmix_socket_errno == EINTR) {
            goto retry;
        } else if (pmix_socket_errno == EAGAIN) {
            /* tell the caller to keep this message on active,
             * but let the event lib cycle so other messages
             * can progress while this socket is busy
             */
            return PMIX_ERR_RESOURCE_BUSY;
        } else if (pmix_socket_errno == EWOULDBLOCK) {
            /* tell the caller to keep this message on active,
             * but let the event lib cycle so other messages
             * can progress while this socket is busy
             */
            return PMIX_ERR_WOULD_BLOCK;
        } else {
            /* we hit an error and cannot progress this message */
            pmix_output(0, "pmix_ptl_base: send_msg: write failed: %s (%d) [sd = %d]",
                        strerror(pmix_socket_errno),
                        pmix_socket_errno, sd);
            return PMIX_ERR_UNREACH;
        }
    } else {
        /* short writev. This usually means the kernel buffer is full,
         * so there is no point for retrying at that time.
         * simply update the msg and return with PMIX_ERR_RESOURCE_BUSY */
        if ((size_t)rc < msg->sdbytes) {
            /* partial write of the header or the msg data */
            msg->sdptr = (char *)msg->sdptr + rc;
            msg->sdbytes -= rc;
        } else {
            /* header was fully written, but only a part of the msg data was written */
            msg->hdr_sent = true;
            rc -= msg->sdbytes;
            if (NULL != msg->data) {
                /* technically, this should never happen as iov_count
                 * would be 1 for a zero-byte message, and so we cannot
                 * have a case where we write the header and part of the
                 * msg. However, code checkers don't know that and are
                 * fooled by our earlier check for NULL, and so
                 * we silence their warnings by using this check */
                msg->sdptr = (char *)msg->data->base_ptr + rc;
            }
            msg->sdbytes = ntohl(msg->hdr.nbytes) - rc;
        }
        return PMIX_ERR_RESOURCE_BUSY;
    }
}

static pmix_status_t read_bytes(int sd, char **buf, size_t *remain)
{
    pmix_status_t ret = PMIX_SUCCESS;
    int rc;
    char *ptr = *buf;

    /* read until all bytes recvd or error */
    while (0 < *remain) {
        rc = read(sd, ptr, *remain);
        if (rc < 0) {
            if(pmix_socket_errno == EINTR) {
                continue;
            } else if (pmix_socket_errno == EAGAIN) {
                /* tell the caller to keep this message on active,
                 * but let the event lib cycle so other messages
                 * can progress while this socket is busy
                 */
                ret = PMIX_ERR_RESOURCE_BUSY;
                goto exit;
            } else if (pmix_socket_errno == EWOULDBLOCK) {
                /* tell the caller to keep this message on active,
                 * but let the event lib cycle so other messages
                 * can progress while this socket is busy
                 */
                ret = PMIX_ERR_WOULD_BLOCK;
                goto exit;
            }
            /* we hit an error and cannot progress this message - report
             * the error back to the RML and let the caller know
             * to abort this message
             */
            pmix_output_verbose(2, pmix_ptl_base_framework.framework_output,
                                "pmix_ptl_base_msg_recv: readv failed: %s (%d)",
                                strerror(pmix_socket_errno),
                                pmix_socket_errno);
            ret = PMIX_ERR_UNREACH;
            goto exit;
        } else if (0 == rc) {
            /* the remote peer closed the connection */
            ret = PMIX_ERR_UNREACH;
            goto exit;
        }
        /* we were able to read something, so adjust counters and location */
        *remain -= rc;
        ptr += rc;
    }
    /* we read the full data block */
  exit:
    *buf = ptr;
    return ret;
}

/*
 * A file descriptor is available/ready for send. Check the state
 * of the socket and take the appropriate action.
 */
void pmix_ptl_base_send_handler(int sd, short flags, void *cbdata)
{
    pmix_peer_t *peer = (pmix_peer_t*)cbdata;
    pmix_ptl_send_t *msg = peer->send_msg;
    pmix_status_t rc;

    /* acquire the object */
    PMIX_ACQUIRE_OBJECT(peer);

    pmix_output_verbose(2, pmix_ptl_base_framework.framework_output,
                        "%s:%d ptl:base:send_handler SENDING TO PEER %s:%d tag %u with %s msg",
                        pmix_globals.myid.nspace, pmix_globals.myid.rank,
                        peer->info->pname.nspace, peer->info->pname.rank,
                        (NULL == msg) ? UINT_MAX : ntohl(msg->hdr.tag),
                        (NULL == msg) ? "NULL" : "NON-NULL");

    if (NULL != msg) {
        pmix_output_verbose(2, pmix_ptl_base_framework.framework_output,
                            "ptl:base:send_handler SENDING MSG TO %s:%d TAG %u",
                            peer->info->pname.nspace, peer->info->pname.rank,
                            ntohl(msg->hdr.tag));
        if (PMIX_SUCCESS == (rc = send_msg(peer->sd, msg))) {
            // message is complete
            pmix_output_verbose(2, pmix_ptl_base_framework.framework_output,
                                "ptl:base:send_handler MSG SENT");
            PMIX_RELEASE(msg);
            peer->send_msg = NULL;
        } else if (PMIX_ERR_RESOURCE_BUSY == rc ||
                   PMIX_ERR_WOULD_BLOCK == rc) {
            /* exit this event and let the event lib progress */
            pmix_output_verbose(2, pmix_ptl_base_framework.framework_output,
                                "ptl:base:send_handler RES BUSY OR WOULD BLOCK");
            /* ensure we post the modified peer object before another thread
             * picks it back up */
            PMIX_POST_OBJECT(peer);
            return;
        } else {
            pmix_output_verbose(5, pmix_ptl_base_framework.framework_output,
                                "%s:%d SEND ERROR %s",
                                pmix_globals.myid.nspace, pmix_globals.myid.rank,
                                PMIx_Error_string(rc));
            // report the error
            pmix_event_del(&peer->send_event);
            peer->send_ev_active = false;
            PMIX_RELEASE(msg);
            peer->send_msg = NULL;
            pmix_ptl_base_lost_connection(peer, rc);
            /* ensure we post the modified peer object before another thread
             * picks it back up */
            PMIX_POST_OBJECT(peer);
            return;
        }

        /* if current message completed - progress any pending sends by
         * moving the next in the queue into the "on-deck" position. Note
         * that this doesn't mean we send the message right now - we will
         * wait for another send_event to fire before doing so. This gives
         * us a chance to service any pending recvs.
         */
        peer->send_msg = (pmix_ptl_send_t*)
            pmix_list_remove_first(&peer->send_queue);
    }

    /* if nothing else to do unregister for send event notifications */
    if (NULL == peer->send_msg && peer->send_ev_active) {
        pmix_event_del(&peer->send_event);
        peer->send_ev_active = false;
    }
    /* ensure we post the modified peer object before another thread
     * picks it back up */
    PMIX_POST_OBJECT(peer);
}

/*
 * Dispatch to the appropriate action routine based on the state
 * of the connection with the peer.
 */

void pmix_ptl_base_recv_handler(int sd, short flags, void *cbdata)
{
    pmix_status_t rc;
    pmix_peer_t *peer = (pmix_peer_t*)cbdata;
    pmix_ptl_recv_t *msg = NULL;
    pmix_ptl_hdr_t hdr;
    size_t nbytes;
    char *ptr;

    /* acquire the object */
    PMIX_ACQUIRE_OBJECT(peer);

    pmix_output_verbose(2, pmix_ptl_base_framework.framework_output,
                        "%s:%d ptl:base:recv:handler called with peer %s:%d",
                        pmix_globals.myid.nspace, pmix_globals.myid.rank,
                        (NULL == peer) ? "NULL" : peer->info->pname.nspace,
                        (NULL == peer) ? PMIX_RANK_UNDEF : peer->info->pname.rank);

    if (NULL == peer) {
        return;
    }
    /* allocate a new message and setup for recv */
    if (NULL == peer->recv_msg) {
        pmix_output_verbose(2, pmix_ptl_base_framework.framework_output,
                            "ptl:base:recv:handler allocate new recv msg");
        peer->recv_msg = PMIX_NEW(pmix_ptl_recv_t);
        if (NULL == peer->recv_msg) {
            pmix_output(0, "sptl:base:recv_handler: unable to allocate recv message\n");
            goto err_close;
        }
        PMIX_RETAIN(peer);
        peer->recv_msg->peer = peer;  // provide a handle back to the peer object
        /* start by reading the header */
        peer->recv_msg->rdptr = (char*)&peer->recv_msg->hdr;
        peer->recv_msg->rdbytes = sizeof(pmix_ptl_hdr_t);
    }
    msg = peer->recv_msg;
    msg->sd = sd;
    /* if the header hasn't been completely read, read it */
    if (!msg->hdr_recvd) {
         pmix_output_verbose(2, pmix_ptl_base_framework.framework_output,
                            "ptl:base:recv:handler read hdr on socket %d", peer->sd);
        nbytes = sizeof(pmix_ptl_hdr_t);
        ptr = (char*)&hdr;
        if (PMIX_SUCCESS == (rc = read_bytes(peer->sd, &ptr, &nbytes))) {
            /* completed reading the header */
            peer->recv_msg->hdr_recvd = true;
            /* convert the hdr to host format */
            peer->recv_msg->hdr.pindex = ntohl(hdr.pindex);
            peer->recv_msg->hdr.tag = ntohl(hdr.tag);
            peer->recv_msg->hdr.nbytes = ntohl(hdr.nbytes);
            pmix_output_verbose(2, pmix_ptl_base_framework.framework_output,
                                "RECVD MSG FOR TAG %d SIZE %d",
                                (int)peer->recv_msg->hdr.tag,
                                (int)peer->recv_msg->hdr.nbytes);
            /* if this is a zero-byte message, then we are done */
            if (0 == peer->recv_msg->hdr.nbytes) {
                pmix_output_verbose(2, pmix_ptl_base_framework.framework_output,
                                    "RECVD ZERO-BYTE MESSAGE FROM %s:%u for tag %d",
                                    peer->info->pname.nspace, peer->info->pname.rank,
                                    peer->recv_msg->hdr.tag);
                peer->recv_msg->data = NULL;  // make sure
                peer->recv_msg->rdptr = NULL;
                peer->recv_msg->rdbytes = 0;
                /* post it for delivery */
                PMIX_ACTIVATE_POST_MSG(peer->recv_msg);
                peer->recv_msg = NULL;
                PMIX_POST_OBJECT(peer);
                return;
            } else {
                pmix_output_verbose(2, pmix_ptl_base_framework.framework_output,
                                    "ptl:base:recv:handler allocate data region of size %lu",
                                    (unsigned long)peer->recv_msg->hdr.nbytes);
                /* allocate the data region */
                if (pmix_ptl_globals.max_msg_size < peer->recv_msg->hdr.nbytes) {
                    pmix_show_help("help-pmix-runtime.txt", "ptl:msg_size", true,
                                   (unsigned long)peer->recv_msg->hdr.nbytes,
                                   (unsigned long)pmix_ptl_globals.max_msg_size);
                    goto err_close;
                }
                peer->recv_msg->data = (char*)malloc(peer->recv_msg->hdr.nbytes);
                memset(peer->recv_msg->data, 0, peer->recv_msg->hdr.nbytes);
                /* point to it */
                peer->recv_msg->rdptr = peer->recv_msg->data;
                peer->recv_msg->rdbytes = peer->recv_msg->hdr.nbytes;
            }
            /* fall thru and attempt to read the data */
        } else if (PMIX_ERR_RESOURCE_BUSY == rc ||
                   PMIX_ERR_WOULD_BLOCK == rc) {
            /* exit this event and let the event lib progress */
            return;
        } else {
            /* the remote peer closed the connection - report that condition
             * and let the caller know
             */
            pmix_output_verbose(2, pmix_ptl_base_framework.framework_output,
                                "ptl:base:msg_recv: peer %s:%d closed connection",
                                peer->nptr->nspace, peer->info->pname.rank);
            goto err_close;
        }
    }

    if (peer->recv_msg->hdr_recvd) {
        /* continue to read the data block - we start from
         * wherever we left off, which could be at the
         * beginning or somewhere in the message
         */
        if (PMIX_SUCCESS == (rc = read_bytes(peer->sd, &msg->rdptr, &msg->rdbytes))) {
            /* we recvd all of the message */
            pmix_output_verbose(2, pmix_ptl_base_framework.framework_output,
                                "%s:%d RECVD COMPLETE MESSAGE FROM SERVER OF %d BYTES FOR TAG %d ON PEER SOCKET %d",
                                pmix_globals.myid.nspace, pmix_globals.myid.rank,
                                (int)peer->recv_msg->hdr.nbytes,
                                peer->recv_msg->hdr.tag, peer->sd);
            /* post it for delivery */
            PMIX_ACTIVATE_POST_MSG(peer->recv_msg);
            peer->recv_msg = NULL;
            /* ensure we post the modified peer object before another thread
             * picks it back up */
            PMIX_POST_OBJECT(peer);
            return;
        } else if (PMIX_ERR_RESOURCE_BUSY == rc ||
                   PMIX_ERR_WOULD_BLOCK == rc) {
            /* exit this event and let the event lib progress */
            /* ensure we post the modified peer object before another thread
             * picks it back up */
            PMIX_POST_OBJECT(peer);
            return;
        } else {
            /* the remote peer closed the connection - report that condition
             * and let the caller know
             */
            pmix_output_verbose(2, pmix_ptl_base_framework.framework_output,
                                "%s:%d ptl:base:msg_recv: peer %s:%d closed connection",
                                pmix_globals.myid.nspace, pmix_globals.myid.rank,
                                peer->nptr->nspace, peer->info->pname.rank);
            goto err_close;
        }
    }
    /* success */
    return;

  err_close:
    /* stop all events */
    if (peer->recv_ev_active) {
        pmix_event_del(&peer->recv_event);
        peer->recv_ev_active = false;
    }
    if (peer->send_ev_active) {
        pmix_event_del(&peer->send_event);
        peer->send_ev_active = false;
    }
    if (NULL != peer->recv_msg) {
        PMIX_RELEASE(peer->recv_msg);
        peer->recv_msg = NULL;
    }
    pmix_ptl_base_lost_connection(peer, PMIX_ERR_UNREACH);
    /* ensure we post the modified peer object before another thread
     * picks it back up */
    PMIX_POST_OBJECT(peer);
}

void pmix_ptl_base_send(int sd, short args, void *cbdata)
{
    pmix_ptl_queue_t *queue = (pmix_ptl_queue_t*)cbdata;
    pmix_ptl_send_t *snd;

    /* acquire the object */
    PMIX_ACQUIRE_OBJECT(queue);

    if (NULL == queue->peer || queue->peer->sd < 0 ||
        NULL == queue->peer->info || NULL == queue->peer->nptr) {
        /* this peer has lost connection */
        if (NULL != queue->buf) {
            PMIX_RELEASE(queue->buf);
        }
        PMIX_RELEASE(queue);
        return;
    }

    pmix_output_verbose(2, pmix_ptl_base_framework.framework_output,
                        "[%s:%d] send to %s:%u on tag %d",
                        __FILE__, __LINE__,
                        (queue->peer)->info->pname.nspace,
                        (queue->peer)->info->pname.rank, (queue->tag));

    if (NULL == queue->buf) {
        /* nothing to send? */
        PMIX_RELEASE(queue);
        return;
    }

    snd = PMIX_NEW(pmix_ptl_send_t);
    snd->hdr.pindex = htonl(pmix_globals.pindex);
    snd->hdr.tag = htonl(queue->tag);
    snd->hdr.nbytes = htonl((queue->buf)->bytes_used);
    snd->data = (queue->buf);
    /* always start with the header */
    snd->sdptr = (char*)&snd->hdr;
    snd->sdbytes = sizeof(pmix_ptl_hdr_t);

    /* if there is no message on-deck, put this one there */
    if (NULL == (queue->peer)->send_msg) {
        (queue->peer)->send_msg = snd;
    } else {
        /* add it to the queue */
        pmix_list_append(&(queue->peer)->send_queue, &snd->super);
    }
    /* ensure the send event is active */
    if (!(queue->peer)->send_ev_active) {
        (queue->peer)->send_ev_active = true;
        PMIX_POST_OBJECT(queue->peer);
        pmix_event_add(&(queue->peer)->send_event, 0);
    }
    PMIX_RELEASE(queue);
    PMIX_POST_OBJECT(snd);
}

void pmix_ptl_base_send_recv(int fd, short args, void *cbdata)
{
    pmix_ptl_sr_t *ms = (pmix_ptl_sr_t*)cbdata;
    pmix_ptl_posted_recv_t *req;
    pmix_ptl_send_t *snd;
    uint32_t tag;

    /* acquire the object */
    PMIX_ACQUIRE_OBJECT(ms);

    if (NULL == ms->peer || ms->peer->sd < 0 ||
        NULL == ms->peer->info || NULL == ms->peer->nptr) {
        /* this peer has lost connection */
        if (NULL != ms->bfr) {
            PMIX_RELEASE(ms->bfr);
        }
        PMIX_RELEASE(ms);
        return;
    }

    if (NULL == ms->bfr) {
        /* nothing to send? */
        PMIX_RELEASE(ms);
        return;
    }

    /* take the next tag in the sequence */
    pmix_ptl_globals.current_tag++;
    if (UINT32_MAX == pmix_ptl_globals.current_tag ) {
        pmix_ptl_globals.current_tag = PMIX_PTL_TAG_DYNAMIC;
    }
    tag = pmix_ptl_globals.current_tag;

    if (NULL != ms->cbfunc) {
        /* if a callback msg is expected, setup a recv for it */
        req = PMIX_NEW(pmix_ptl_posted_recv_t);
        req->tag = tag;
        req->cbfunc = ms->cbfunc;
        req->cbdata = ms->cbdata;

        pmix_output_verbose(5, pmix_ptl_base_framework.framework_output,
                            "posting recv on tag %d", req->tag);
        /* add it to the list of recvs - we cannot have unexpected messages
         * in this subsystem as the server never sends us something that
         * we didn't previously request */
        pmix_list_prepend(&pmix_ptl_globals.posted_recvs, &req->super);
    }

    pmix_output_verbose(2, pmix_ptl_base_framework.framework_output,
                        "QUEIENG MSG TO SERVER OF SIZE %d",
                        (int)ms->bfr->bytes_used);
    snd = PMIX_NEW(pmix_ptl_send_t);
    snd->hdr.pindex = htonl(pmix_globals.pindex);
    snd->hdr.tag = htonl(tag);
    snd->hdr.nbytes = htonl(ms->bfr->bytes_used);
    snd->data = ms->bfr;
    /* always start with the header */
    snd->sdptr = (char*)&snd->hdr;
    snd->sdbytes = sizeof(pmix_ptl_hdr_t);

    /* if there is no message on-deck, put this one there */
    if (NULL == ms->peer->send_msg) {
        ms->peer->send_msg = snd;
    } else {
        /* add it to the queue */
        pmix_list_append(&ms->peer->send_queue, &snd->super);
    }
    /* ensure the send event is active */
    if (!ms->peer->send_ev_active) {
        ms->peer->send_ev_active = true;
        PMIX_POST_OBJECT(snd);
        pmix_event_add(&ms->peer->send_event, 0);
    }
    /* cleanup */
    PMIX_RELEASE(ms);
    PMIX_POST_OBJECT(snd);
}

void pmix_ptl_base_process_msg(int fd, short flags, void *cbdata)
{
    pmix_ptl_recv_t *msg = (pmix_ptl_recv_t*)cbdata;
    pmix_ptl_posted_recv_t *rcv;
    pmix_buffer_t buf;

    /* acquire the object */
    PMIX_ACQUIRE_OBJECT(msg);

    pmix_output_verbose(5, pmix_ptl_base_framework.framework_output,
                        "%s:%d message received %d bytes for tag %u on socket %d",
                        pmix_globals.myid.nspace, pmix_globals.myid.rank,
                        (int)msg->hdr.nbytes, msg->hdr.tag, msg->sd);

    /* see if we have a waiting recv for this message */
    PMIX_LIST_FOREACH(rcv, &pmix_ptl_globals.posted_recvs, pmix_ptl_posted_recv_t) {
        pmix_output_verbose(5, pmix_ptl_base_framework.framework_output,
                            "checking msg on tag %u for tag %u",
                            msg->hdr.tag, rcv->tag);

        if (msg->hdr.tag == rcv->tag || UINT_MAX == rcv->tag) {
            if (NULL != rcv->cbfunc) {
                /* construct and load the buffer */
                PMIX_CONSTRUCT(&buf, pmix_buffer_t);
                if (NULL != msg->data) {
                    PMIX_LOAD_BUFFER(msg->peer, &buf, msg->data, msg->hdr.nbytes);
                } else {
                    /* we need to at least set the buffer type so
                     * unpack of a zero-byte message doesn't error */
                    buf.type = msg->peer->nptr->compat.type;
                }
                msg->data = NULL;  // protect the data region
                pmix_output_verbose(5, pmix_ptl_base_framework.framework_output,
                                     "%s:%d EXECUTE CALLBACK for tag %u",
                                     pmix_globals.myid.nspace, pmix_globals.myid.rank,
                                     msg->hdr.tag);
                rcv->cbfunc(msg->peer, &msg->hdr, &buf, rcv->cbdata);
                pmix_output_verbose(5, pmix_ptl_base_framework.framework_output,
                                    "%s:%d CALLBACK COMPLETE",
                                    pmix_globals.myid.nspace, pmix_globals.myid.rank);
                PMIX_DESTRUCT(&buf);  // free's the msg data
            }
            /* done with the recv if it is a dynamic tag */
            if (PMIX_PTL_TAG_DYNAMIC <= rcv->tag && UINT_MAX != rcv->tag) {
                pmix_list_remove_item(&pmix_ptl_globals.posted_recvs, &rcv->super);
                PMIX_RELEASE(rcv);
            }
            PMIX_RELEASE(msg);
            return;
        }
    }

    /* if the tag in this message is above the dynamic marker, then
     * that is an error */
    if (PMIX_PTL_TAG_DYNAMIC <= msg->hdr.tag) {
        pmix_output(0, "UNEXPECTED MESSAGE tag = %d from source %s:%d",
                    msg->hdr.tag, msg->peer->info->pname.nspace,
                    msg->peer->info->pname.rank);
        PMIX_REPORT_EVENT(PMIX_ERROR, msg->peer, PMIX_RANGE_NAMESPACE, _notify_complete);
        PMIX_RELEASE(msg);
        return;
    }

    /* it is possible that someone may post a recv for this message
     * at some point, so we have to hold onto it */
    pmix_list_append(&pmix_ptl_globals.unexpected_msgs, &msg->super);
    /* ensure we post the modified object before another thread
     * picks it back up */
    PMIX_POST_OBJECT(msg);
}
