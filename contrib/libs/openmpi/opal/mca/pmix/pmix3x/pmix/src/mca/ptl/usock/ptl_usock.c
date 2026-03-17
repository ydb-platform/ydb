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
 * Copyright (c) 2010-2011 Oak Ridge National Labs.  All rights reserved.
 * Copyright (c) 2011-2014 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2011-2013 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2013-2018 Intel, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 */

#include <src/include/pmix_config.h>
#include "pmix_common.h"

#ifdef HAVE_FCNTL_H
#include <fcntl.h>
#endif
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
#ifdef HAVE_SYS_STAT_H
#include <sys/stat.h>
#endif

#include "src/util/argv.h"
#include "src/util/error.h"
#include "src/client/pmix_client_ops.h"
#include "src/include/pmix_globals.h"
#include "src/include/pmix_socket_errno.h"
#include "src/mca/bfrops/base/base.h"
#include "src/mca/psec/base/base.h"

#include "src/mca/ptl/base/base.h"
#include "ptl_usock.h"

static pmix_status_t init(void);
static void finalize(void);
static pmix_status_t connect_to_peer(struct pmix_peer_t *peer,
                                     pmix_info_t *info, size_t ninfo);
static pmix_status_t send_recv(struct pmix_peer_t *peer,
                               pmix_buffer_t *bfr,
                               pmix_ptl_cbfunc_t cbfunc,
                               void *cbdata);
static pmix_status_t send_oneway(struct pmix_peer_t *peer,
                                 pmix_buffer_t *bfr,
                                 pmix_ptl_tag_t tag);

pmix_ptl_module_t pmix_ptl_usock_module = {
    .init = init,
    .finalize = finalize,
    .send_recv = send_recv,
    .send = send_oneway,
    .connect_to_peer = connect_to_peer
};

static pmix_status_t recv_connect_ack(int sd);
static pmix_status_t send_connect_ack(int sd);

static pmix_status_t init(void)
{
    return PMIX_SUCCESS;
}

static void finalize(void)
{
}

static void pmix_usock_send_recv(int fd, short args, void *cbdata);
static void pmix_usock_send(int fd, short args, void *cbdata);

static pmix_status_t connect_to_peer(struct pmix_peer_t *peer,
                                     pmix_info_t *info, size_t ninfo)
{
    struct sockaddr_un *address;
    char *evar, **uri;
    pmix_status_t rc;
    int sd;
    pmix_socklen_t len;
    bool retried = false;

    pmix_output_verbose(2, pmix_ptl_base_framework.framework_output,
                        "[%s:%d] connect to server",
                        __FILE__, __LINE__);

    /* if we are not a client, there is nothing we can do */
    if (!PMIX_PROC_IS_CLIENT(pmix_globals.mypeer)) {
        return PMIX_ERR_NOT_SUPPORTED;
    }

    /* if we don't have a path to the daemon rendezvous point,
     * then we need to return an error */
    if (NULL != (evar = getenv("PMIX_SERVER_URI2USOCK"))) {
        /* this is a v2.1+ server */
        pmix_globals.mypeer->nptr->compat.bfrops = pmix_bfrops_base_assign_module("v21");
        if (NULL == pmix_globals.mypeer->nptr->compat.bfrops) {
            return PMIX_ERR_INIT;
        }
    } else if (NULL != (evar = getenv("PMIX_SERVER_URI"))) {
        /* this is a pre-v2.1 server - must use the v12 bfrops module */
        pmix_globals.mypeer->nptr->compat.bfrops = pmix_bfrops_base_assign_module("v12");
        if (NULL == pmix_globals.mypeer->nptr->compat.bfrops) {
            return PMIX_ERR_INIT;
        }
    } else {
        /* let the caller know that the server isn't available */
        return PMIX_ERR_SERVER_NOT_AVAIL;
    }
    /* the server will be using the same bfrops as us */
    pmix_client_globals.myserver->nptr->compat.bfrops = pmix_globals.mypeer->nptr->compat.bfrops;
    /* mark that we are using the V1 protocol */
    pmix_globals.mypeer->protocol = PMIX_PROTOCOL_V1;

    uri = pmix_argv_split(evar, ':');
    if (3 != pmix_argv_count(uri)) {
        pmix_argv_free(uri);
        PMIX_ERROR_LOG(PMIX_ERROR);
        return PMIX_ERROR;
    }
    /* set the server nspace */
    if (NULL == pmix_client_globals.myserver->info) {
        pmix_client_globals.myserver->info = PMIX_NEW(pmix_rank_info_t);
    }
    if (NULL == pmix_client_globals.myserver->nptr) {
        pmix_client_globals.myserver->nptr = PMIX_NEW(pmix_namespace_t);
    }
    if (NULL == pmix_client_globals.myserver->nptr->nspace) {
        pmix_client_globals.myserver->nptr->nspace = strdup(uri[0]);
    }
    if (NULL == pmix_client_globals.myserver->info->pname.nspace) {
        pmix_client_globals.myserver->info->pname.nspace = strdup(uri[0]);
    }

    /* set the server rank */
    pmix_client_globals.myserver->info->pname.rank = strtoull(uri[1], NULL, 10);

    /* setup the path to the daemon rendezvous point */
    memset(&mca_ptl_usock_component.connection, 0, sizeof(struct sockaddr_storage));
    address = (struct sockaddr_un*)&mca_ptl_usock_component.connection;
    address->sun_family = AF_UNIX;
    snprintf(address->sun_path, sizeof(address->sun_path)-1, "%s", uri[2]);
    /* if the rendezvous file doesn't exist, that's an error */
    if (0 != access(uri[2], R_OK)) {
        pmix_argv_free(uri);
        PMIX_ERROR_LOG(PMIX_ERR_NOT_FOUND);
        return PMIX_ERR_NOT_FOUND;
    }
    pmix_argv_free(uri);

  retry:
    /* establish the connection */
    len = sizeof(struct sockaddr_un);
    if (PMIX_SUCCESS != (rc = pmix_ptl_base_connect(&mca_ptl_usock_component.connection, len, &sd))) {
        PMIX_ERROR_LOG(rc);
        return rc;
    }
    pmix_client_globals.myserver->sd = sd;

    /* send our identity and any authentication credentials to the server */
    if (PMIX_SUCCESS != (rc = send_connect_ack(sd))) {
        CLOSE_THE_SOCKET(sd);
        return rc;
    }

    /* do whatever handshake is required */
    if (PMIX_SUCCESS != (rc = recv_connect_ack(sd))) {
        CLOSE_THE_SOCKET(sd);
        if (PMIX_ERR_TEMP_UNAVAILABLE == rc) {
            /* give it two tries */
            if (!retried) {
                retried = true;
                goto retry;
            }
        }
        return rc;
    }

    pmix_output_verbose(2, pmix_ptl_base_framework.framework_output,
                        "sock_peer_try_connect: Connection across to server succeeded");

    /* mark the connection as made */
    pmix_globals.connected = true;

    pmix_ptl_base_set_nonblocking(sd);

    /* setup recv event */
    pmix_event_assign(&pmix_client_globals.myserver->recv_event,
                      pmix_globals.evbase,
                      pmix_client_globals.myserver->sd,
                      EV_READ | EV_PERSIST,
                      pmix_usock_recv_handler, pmix_client_globals.myserver);
    pmix_event_add(&pmix_client_globals.myserver->recv_event, 0);
    pmix_client_globals.myserver->recv_ev_active = true;
    PMIX_POST_OBJECT(pmix_client_globals.myserver);
    pmix_event_add(&pmix_client_globals.myserver->recv_event, 0);

    /* setup send event */
    pmix_event_assign(&pmix_client_globals.myserver->send_event,
                      pmix_globals.evbase,
                      pmix_client_globals.myserver->sd,
                      EV_WRITE|EV_PERSIST,
                      pmix_usock_send_handler, pmix_client_globals.myserver);
    pmix_client_globals.myserver->send_ev_active = false;

    return PMIX_SUCCESS;
}

static pmix_status_t send_recv(struct pmix_peer_t *peer,
                               pmix_buffer_t *bfr,
                               pmix_ptl_cbfunc_t cbfunc,
                               void *cbdata)
{
    pmix_ptl_sr_t *ms;
    pmix_peer_t *pr = (pmix_peer_t*)peer;

    pmix_output_verbose(2, pmix_ptl_base_framework.framework_output,
                        "[%s:%d] post send to server",
                        __FILE__, __LINE__);

    ms = PMIX_NEW(pmix_ptl_sr_t);
    PMIX_RETAIN(pr);
    ms->peer = pr;
    ms->bfr = bfr;
    ms->cbfunc = cbfunc;
    ms->cbdata = cbdata;
    PMIX_THREADSHIFT(ms, pmix_usock_send_recv);
    return PMIX_SUCCESS;
}

static pmix_status_t send_oneway(struct pmix_peer_t *peer,
                                 pmix_buffer_t *bfr,
                                 pmix_ptl_tag_t tag)
{
    pmix_ptl_queue_t *q;
    pmix_peer_t *pr = (pmix_peer_t*)peer;

    /* we have to transfer this to an event for thread
     * safety as we need to post this message on the
     * peer's send queue */
    q = PMIX_NEW(pmix_ptl_queue_t);
    PMIX_RETAIN(pr);
    q->peer = peer;
    q->buf = bfr;
    q->tag = tag;
    PMIX_THREADSHIFT(q, pmix_usock_send);

    return PMIX_SUCCESS;
}

static pmix_status_t send_connect_ack(int sd)
{
    char *msg;
    pmix_usock_hdr_t hdr;
    size_t sdsize=0, csize=0;
    pmix_byte_object_t cred;
    pmix_status_t rc;
    char *sec, *bfrops, *gds;
    pmix_bfrop_buffer_type_t bftype;

    pmix_output_verbose(2, pmix_ptl_base_framework.framework_output,
                        "pmix: SEND CONNECT ACK");

    /* setup the header */
    memset(&hdr, 0, sizeof(pmix_usock_hdr_t));
    hdr.pindex = -1;
    hdr.tag = UINT32_MAX;

    /* reserve space for the nspace and rank info */
    sdsize = strlen(pmix_globals.myid.nspace) + 1 + sizeof(int);

    /* get a credential, if the security system provides one. Not
     * every SPC will do so, thus we must first check */
    PMIX_BYTE_OBJECT_CONSTRUCT(&cred);
    PMIX_PSEC_CREATE_CRED(rc, pmix_globals.mypeer,
                          NULL, 0, NULL, 0, &cred);
    if (PMIX_SUCCESS != rc) {
        return rc;
    }

    /* add the name of our active sec module - we selected it
     * in pmix_client.c prior to entering here */
    sec = pmix_globals.mypeer->nptr->compat.psec->name;

    /* add our active bfrops module name */
    bfrops = pmix_globals.mypeer->nptr->compat.bfrops->version;
    /* and the type of buffer we are using */
    bftype = pmix_globals.mypeer->nptr->compat.type;

    /* add our active gds module for working with the server */
    gds = (char*)pmix_client_globals.myserver->nptr->compat.gds->name;

    /* set the number of bytes to be read beyond the header */
    hdr.nbytes = sdsize + (strlen(PMIX_VERSION) + 1) + \
                (sizeof(size_t) + cred.size) + \
                (strlen(sec) + 1) + \
                (strlen(bfrops) + 1) + sizeof(bftype) + \
                (strlen(gds) + 1);  // must NULL terminate the strings!

    /* create a space for our message */
    sdsize = (sizeof(hdr) + hdr.nbytes);
    if (NULL == (msg = (char*)malloc(sdsize))) {
        PMIX_BYTE_OBJECT_DESTRUCT(&cred);
        return PMIX_ERR_OUT_OF_RESOURCE;
    }
    memset(msg, 0, sdsize);

    /* load the message */
    csize=0;
    memcpy(msg, &hdr, sizeof(pmix_usock_hdr_t));
    csize += sizeof(pmix_usock_hdr_t);
    /* pass our nspace */
    memcpy(msg+csize, pmix_globals.myid.nspace, strlen(pmix_globals.myid.nspace));
    csize += strlen(pmix_globals.myid.nspace)+1;
    /* pass our rank */
    memcpy(msg+csize, &pmix_globals.myid.rank, sizeof(int));
    csize += sizeof(int);

    /* pass our version string */
    memcpy(msg+csize, PMIX_VERSION, strlen(PMIX_VERSION));
    csize += strlen(PMIX_VERSION)+1;

    /* pass the size of the credential */
    memcpy(msg+csize, &cred.size, sizeof(size_t));
    csize += sizeof(size_t);
    if (0 < cred.size) {
        memcpy(msg+csize, cred.bytes, cred.size);
        csize += cred.size;
    }
    PMIX_BYTE_OBJECT_DESTRUCT(&cred);

    /* pass our active sec module */
    memcpy(msg+csize, sec, strlen(sec));
    csize += strlen(sec)+1;

    /* provide our active bfrops module */
    memcpy(msg+csize, bfrops, strlen(bfrops));
    csize += strlen(bfrops)+1;

    /* provide the bfrops type */
    memcpy(msg+csize, &bftype, sizeof(bftype));
    csize += sizeof(bftype);

    /* provide the gds module */
    memcpy(msg+csize, gds, strlen(gds));

    /* send the entire msg across */
    if (PMIX_SUCCESS != pmix_ptl_base_send_blocking(sd, msg, sdsize)) {
        free(msg);
        return PMIX_ERR_UNREACH;
    }
    free(msg);
    return PMIX_SUCCESS;
}

/* we receive a connection acknowledgement from the server,
 * consisting of nothing more than a status report. If success,
 * then we initiate authentication method */
static pmix_status_t recv_connect_ack(int sd)
{
    pmix_status_t reply;
    pmix_status_t rc;
    struct timeval tv, save;
    pmix_socklen_t sz;
    bool sockopt = true;

    pmix_output_verbose(2, pmix_ptl_base_framework.framework_output,
                        "pmix: RECV CONNECT ACK FROM SERVER");

    /* get the current timeout value so we can reset to it */
    sz = sizeof(save);
    if (0 != getsockopt(sd, SOL_SOCKET, SO_RCVTIMEO, (void*)&save, &sz)) {
        if (ENOPROTOOPT == errno || EOPNOTSUPP == errno) {
            sockopt = false;
        } else {
             return PMIX_ERR_UNREACH;
        }
    } else {
        /* set a timeout on the blocking recv so we don't hang */
        tv.tv_sec  = 2;
        tv.tv_usec = 0;
        if (0 != setsockopt(sd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv))) {
            pmix_output_verbose(2, pmix_ptl_base_framework.framework_output,
                                "pmix: recv_connect_ack could not setsockopt SO_RCVTIMEO");
            return PMIX_ERR_UNREACH;
        }
    }

    /* receive the status reply */
    rc = pmix_ptl_base_recv_blocking(sd, (char*)&reply, sizeof(int));
    if (PMIX_SUCCESS != rc) {
        if (sockopt) {
            /* return the socket to normal */
            if (0 != setsockopt(sd, SOL_SOCKET, SO_RCVTIMEO, &save, sz)) {
                return PMIX_ERR_UNREACH;
            }
        }
        return rc;
    }

    /* see if they want us to do the handshake */
    if (PMIX_ERR_READY_FOR_HANDSHAKE == reply) {
        PMIX_PSEC_CLIENT_HANDSHAKE(rc, pmix_client_globals.myserver, sd);
        if (PMIX_SUCCESS != rc) {
            return rc;
        }
    } else if (PMIX_SUCCESS != reply) {
        return reply;
    }

    pmix_output_verbose(2, pmix_ptl_base_framework.framework_output,
                        "pmix: RECV CONNECT CONFIRMATION");

    /* receive our index into the server's client array */
    rc = pmix_ptl_base_recv_blocking(sd, (char*)&pmix_globals.pindex, sizeof(int));
    if (PMIX_SUCCESS != rc) {
        return rc;
    }
    if (sockopt) {
        /* return the socket to normal */
        if (0 != setsockopt(sd, SOL_SOCKET, SO_RCVTIMEO, &save, sz)) {
            return PMIX_ERR_UNREACH;
        }
    }

    return PMIX_SUCCESS;
}

static pmix_status_t send_bytes(int sd, char **buf, size_t *remain)
{
    pmix_status_t ret = PMIX_SUCCESS;
    int rc;
    char *ptr = *buf;
    while (0 < *remain) {
        rc = write(sd, ptr, *remain);
        if (rc < 0) {
            if (pmix_socket_errno == EINTR) {
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
            /* we hit an error and cannot progress this message */
            pmix_output(0, "pmix_usock_msg_send_bytes: write failed: %s (%d) [sd = %d]",
                        strerror(pmix_socket_errno),
                        pmix_socket_errno, sd);
            ret = PMIX_ERR_COMM_FAILURE;
            goto exit;
        }
        /* update location */
        (*remain) -= rc;
        ptr += rc;
    }
    /* we sent the full data block */
exit:
    *buf = ptr;
    return ret;
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
                                "pmix_usock_msg_recv: readv failed: %s (%d)",
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
void pmix_usock_send_handler(int sd, short flags, void *cbdata)
{
    pmix_peer_t *peer = (pmix_peer_t*)cbdata;
    pmix_ptl_send_t *msg = peer->send_msg;
    pmix_status_t rc;
    uint32_t nbytes;

    /* acquire the object */
    PMIX_ACQUIRE_OBJECT(peer);

    pmix_output_verbose(2, pmix_ptl_base_framework.framework_output,
                        "%s:%d usock:send_handler SENDING TO PEER %s:%d tag %u with %s msg",
                        pmix_globals.myid.nspace, pmix_globals.myid.rank,
                        peer->info->pname.nspace, peer->info->pname.rank,
                        (NULL == msg) ? UINT_MAX : msg->hdr.tag,
                        (NULL == msg) ? "NULL" : "NON-NULL");

    if (NULL != msg) {
        if (!msg->hdr_sent) {
            if (PMIX_PROC_IS_SERVER(pmix_globals.mypeer)) {
            /* we have to convert the header back to host-byte order */
                msg->hdr.pindex = ntohl(msg->hdr.pindex);
                msg->hdr.tag = ntohl(msg->hdr.tag);
                nbytes = msg->hdr.nbytes;
                msg->hdr.nbytes = ntohl(nbytes);
            }
            pmix_output_verbose(2, pmix_ptl_base_framework.framework_output,
                                "usock:send_handler SENDING HEADER WITH MSG IDX %d TAG %d SIZE %lu",
                                msg->hdr.pindex, msg->hdr.tag, (unsigned long)msg->hdr.nbytes);
            if (PMIX_SUCCESS == (rc = send_bytes(peer->sd, &msg->sdptr, &msg->sdbytes))) {
                /* header is completely sent */
                pmix_output_verbose(2, pmix_ptl_base_framework.framework_output,
                                    "usock:send_handler HEADER SENT");
                msg->hdr_sent = true;
                /* setup to send the data */
                if (NULL == msg->data) {
                    /* this was a zero-byte msg - nothing more to do */
                    PMIX_RELEASE(msg);
                    peer->send_msg = NULL;
                    goto next;
                } else {
                    /* send the data as a single block */
                    msg->sdptr = msg->data->base_ptr;
                    msg->sdbytes = msg->hdr.nbytes;
                }
                /* fall thru and let the send progress */
            } else if (PMIX_ERR_RESOURCE_BUSY == rc ||
                       PMIX_ERR_WOULD_BLOCK == rc) {
                /* exit this event and let the event lib progress */
                pmix_output_verbose(2, pmix_ptl_base_framework.framework_output,
                                    "usock:send_handler RES BUSY OR WOULD BLOCK");
                if (PMIX_PROC_IS_SERVER(pmix_globals.mypeer)) {
                    /* have to convert back again so we are correct when we re-enter */
                    msg->hdr.pindex = htonl(msg->hdr.pindex);
                    msg->hdr.tag = htonl(msg->hdr.tag);
                    nbytes = msg->hdr.nbytes;
                    msg->hdr.nbytes = htonl(nbytes);
                }
                /* ensure we post the modified peer object before another thread
                 * picks it back up */
                PMIX_POST_OBJECT(peer);
                return;
            } else {
                // report the error
                event_del(&peer->send_event);
                peer->send_ev_active = false;
                PMIX_RELEASE(msg);
                peer->send_msg = NULL;
                pmix_ptl_base_lost_connection(peer, rc);
                /* ensure we post the modified peer object before another thread
                 * picks it back up */
                PMIX_POST_OBJECT(peer);
                return;
            }
        }

        if (msg->hdr_sent) {
            pmix_output_verbose(2, pmix_ptl_base_framework.framework_output,
                                "usock:send_handler SENDING BODY OF MSG");
            if (PMIX_SUCCESS == (rc = send_bytes(peer->sd, &msg->sdptr, &msg->sdbytes))) {
                // message is complete
                pmix_output_verbose(2, pmix_ptl_base_framework.framework_output,
                                    "usock:send_handler BODY SENT");
                PMIX_RELEASE(msg);
                peer->send_msg = NULL;
            } else if (PMIX_ERR_RESOURCE_BUSY == rc ||
                       PMIX_ERR_WOULD_BLOCK == rc) {
                /* exit this event and let the event lib progress */
                pmix_output_verbose(2, pmix_ptl_base_framework.framework_output,
                                    "usock:send_handler RES BUSY OR WOULD BLOCK");
                /* ensure we post the modified peer object before another thread
                 * picks it back up */
                PMIX_POST_OBJECT(peer);
                return;
            } else {
                // report the error
                pmix_output(0, "pmix_usock_peer_send_handler: unable to send message ON SOCKET %d",
                            peer->sd);
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
        }

    next:
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

void pmix_usock_recv_handler(int sd, short flags, void *cbdata)
{
    pmix_status_t rc;
    pmix_peer_t *peer = (pmix_peer_t*)cbdata;
    pmix_ptl_recv_t *msg = NULL;

    /* acquire the object */
    PMIX_ACQUIRE_OBJECT(peer);

    pmix_output_verbose(2, pmix_ptl_base_framework.framework_output,
                        "usock:recv:handler called with peer %s:%d",
                        (NULL == peer) ? "NULL" : peer->info->pname.nspace,
                        (NULL == peer) ? PMIX_RANK_UNDEF : peer->info->pname.rank);

    if (NULL == peer) {
        return;
    }
    /* allocate a new message and setup for recv */
    if (NULL == peer->recv_msg) {
        pmix_output_verbose(2, pmix_ptl_base_framework.framework_output,
                            "usock:recv:handler allocate new recv msg");
        peer->recv_msg = PMIX_NEW(pmix_ptl_recv_t);
        if (NULL == peer->recv_msg) {
            pmix_output(0, "usock_recv_handler: unable to allocate recv message\n");
            goto err_close;
        }
        PMIX_RETAIN(peer);
        peer->recv_msg->peer = peer;  // provide a handle back to the peer object
        /* start by reading the header */
        peer->recv_msg->rdptr = (char*)&peer->recv_msg->hdr;
        peer->recv_msg->rdbytes = sizeof(pmix_usock_hdr_t);
    }
    msg = peer->recv_msg;
    msg->sd = sd;
    /* if the header hasn't been completely read, read it */
    if (!msg->hdr_recvd) {
        pmix_output_verbose(2, pmix_ptl_base_framework.framework_output,
                            "usock:recv:handler read hdr on socket %d", peer->sd);
        if (PMIX_SUCCESS == (rc = read_bytes(peer->sd, &msg->rdptr, &msg->rdbytes))) {
            /* completed reading the header */
            peer->recv_msg->hdr_recvd = true;
            pmix_output_verbose(2, pmix_ptl_base_framework.framework_output,
                                "RECVD MSG FOR TAG %d SIZE %d",
                                (int)peer->recv_msg->hdr.tag,
                                (int)peer->recv_msg->hdr.nbytes);
            /* if this is a zero-byte message, then we are done */
            if (0 == peer->recv_msg->hdr.nbytes) {
                pmix_output_verbose(2, pmix_ptl_base_framework.framework_output,
                                    "RECVD ZERO-BYTE MESSAGE FROM %s:%d for tag %d",
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
                                    "usock:recv:handler allocate data region of size %lu",
                                    (unsigned long)peer->recv_msg->hdr.nbytes);
                /* allocate the data region */
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
                                "pmix_usock_msg_recv: peer closed connection");
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
                                "RECVD COMPLETE MESSAGE FROM SERVER OF %d BYTES FOR TAG %d ON PEER SOCKET %d",
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
                                "pmix_usock_msg_recv: peer closed connection");
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

void pmix_usock_send_recv(int fd, short args, void *cbdata)
{
    pmix_ptl_sr_t *ms = (pmix_ptl_sr_t*)cbdata;
    pmix_ptl_posted_recv_t *req;
    pmix_ptl_send_t *snd;
    uint32_t tag;

    /* acquire the object */
    PMIX_ACQUIRE_OBJECT(ms);

    if (ms->peer->sd < 0) {
        /* this peer's socket has been closed */
        PMIX_RELEASE(ms);
        /* ensure we post the object before another thread
         * picks it back up */
        PMIX_POST_OBJECT(NULL);
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

    snd = PMIX_NEW(pmix_ptl_send_t);
    snd->hdr.pindex = pmix_globals.pindex;
    snd->hdr.tag = tag;
    snd->hdr.nbytes = ms->bfr->bytes_used;
    snd->data = ms->bfr;
    /* always start with the header */
    snd->sdptr = (char*)&snd->hdr;
    snd->sdbytes = sizeof(pmix_usock_hdr_t);

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

static void pmix_usock_send(int sd, short args, void *cbdata)
{
    pmix_ptl_queue_t *queue = (pmix_ptl_queue_t*)cbdata;
    pmix_ptl_send_t *snd;

    /* acquire the object */
    PMIX_ACQUIRE_OBJECT(queue);

    if (NULL == queue->peer || queue->peer->sd < 0 ||
        NULL == queue->peer->info || NULL == queue->peer->nptr) {
        /* this peer has lost connection */
        PMIX_RELEASE(queue);
        /* ensure we post the object before another thread
         * picks it back up */
        PMIX_POST_OBJECT(queue);
        return;
    }

    pmix_output_verbose(2, pmix_ptl_base_framework.framework_output,
                        "[%s:%d] send to %s:%u on tag %d",
                        __FILE__, __LINE__,
                        (queue->peer)->info->pname.nspace,
                        (queue->peer)->info->pname.rank, (queue->tag));

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
