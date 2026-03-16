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
 * Copyright (c) 2006-2013 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2009      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2011      Oak Ridge National Labs.  All rights reserved.
 * Copyright (c) 2013-2017 Intel, Inc.  All rights reserved.
 * Copyright (c) 2017      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 * In windows, many of the socket functions return an EWOULDBLOCK
 * instead of \ things like EAGAIN, EINPROGRESS, etc. It has been
 * verified that this will \ not conflict with other error codes that
 * are returned by these functions \ under UNIX/Linux environments
 */

#include "orte_config.h"

#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#include <fcntl.h>
#ifdef HAVE_SYS_UIO_H
#include <sys/uio.h>
#endif
#ifdef HAVE_NET_UIO_H
#include <net/uio.h>
#endif
#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
#include "opal/opal_socket_errno.h"
#ifdef HAVE_NETINET_IN_H
#include <netinet/in.h>
#endif
#ifdef HAVE_ARPA_INET_H
#include <arpa/inet.h>
#endif
#ifdef HAVE_NETINET_TCP_H
#include <netinet/tcp.h>
#endif

#include "opal_stdint.h"
#include "opal/types.h"
#include "opal/mca/backtrace/backtrace.h"
#include "opal/util/output.h"
#include "opal/util/net.h"
#include "opal/util/error.h"
#include "opal/class/opal_hash_table.h"
#include "opal/mca/event/event.h"

#include "orte/util/name_fns.h"
#include "orte/util/threads.h"
#include "orte/runtime/orte_globals.h"
#include "orte/mca/errmgr/errmgr.h"
#include "orte/mca/ess/ess.h"
#include "orte/mca/routed/routed.h"
#include "orte/mca/state/state.h"
#include "orte/runtime/orte_wait.h"

#include "oob_tcp.h"
#include "orte/mca/oob/tcp/oob_tcp_component.h"
#include "orte/mca/oob/tcp/oob_tcp_peer.h"
#include "orte/mca/oob/tcp/oob_tcp_common.h"
#include "orte/mca/oob/tcp/oob_tcp_connection.h"

#define OOB_SEND_MAX_RETRIES 3

void mca_oob_tcp_queue_msg(int sd, short args, void *cbdata)
{
    mca_oob_tcp_send_t *snd = (mca_oob_tcp_send_t*)cbdata;
    mca_oob_tcp_peer_t *peer;

    ORTE_ACQUIRE_OBJECT(snd);
    peer = (mca_oob_tcp_peer_t*)snd->peer;

    /* if there is no message on-deck, put this one there */
    if (NULL == peer->send_msg) {
        peer->send_msg = snd;
    } else {
        /* add it to the queue */
        opal_list_append(&peer->send_queue, &snd->super);
    }
    if (snd->activate) {
        /* if we aren't connected, then start connecting */
        if (MCA_OOB_TCP_CONNECTED != peer->state) {
            peer->state = MCA_OOB_TCP_CONNECTING;
            ORTE_ACTIVATE_TCP_CONN_STATE(peer, mca_oob_tcp_peer_try_connect);
        } else {
            /* ensure the send event is active */
            if (!peer->send_ev_active) {
                peer->send_ev_active = true;
                ORTE_POST_OBJECT(peer);
                opal_event_add(&peer->send_event, 0);
            }
        }
    }
}

static int send_msg(mca_oob_tcp_peer_t* peer, mca_oob_tcp_send_t* msg)
{
    struct iovec iov[2];
    int iov_count, retries = 0;
    ssize_t remain = msg->sdbytes, rc;

    iov[0].iov_base = msg->sdptr;
    iov[0].iov_len = msg->sdbytes;
    if (!msg->hdr_sent) {
        if (NULL != msg->data) {
            /* relay message - just send that data */
            iov[1].iov_base = msg->data;
        } else if (NULL != msg->msg->buffer) {
            /* buffer send */
            iov[1].iov_base = msg->msg->buffer->base_ptr;
        } else {
            iov[1].iov_base = msg->msg->data;
        }
        iov[1].iov_len = ntohl(msg->hdr.nbytes);
        remain += ntohl(msg->hdr.nbytes);
        iov_count = 2;
    } else {
        iov_count = 1;
    }

  retry:
    rc = writev(peer->sd, iov, iov_count);
    if (OPAL_LIKELY(rc == remain)) {
        /* we successfully sent the header and the msg data if any */
        msg->hdr_sent = true;
        msg->sdbytes = 0;
        msg->sdptr = (char *)iov[iov_count-1].iov_base + iov[iov_count-1].iov_len;
        return ORTE_SUCCESS;
    } else if (rc < 0) {
        if (opal_socket_errno == EINTR) {
            goto retry;
        } else if (opal_socket_errno == EAGAIN) {
            /* tell the caller to keep this message on active,
             * but let the event lib cycle so other messages
             * can progress while this socket is busy
             */
            ++retries;
            if (retries < OOB_SEND_MAX_RETRIES) {
                goto retry;
            }
            return ORTE_ERR_RESOURCE_BUSY;
        } else if (opal_socket_errno == EWOULDBLOCK) {
            /* tell the caller to keep this message on active,
             * but let the event lib cycle so other messages
             * can progress while this socket is busy
             */
            ++retries;
            if (retries < OOB_SEND_MAX_RETRIES) {
                goto retry;
            }
            return ORTE_ERR_WOULD_BLOCK;
        } else {
            /* we hit an error and cannot progress this message */
            opal_output(0, "oob:tcp: send_msg: write failed: %s (%d) [sd = %d]",
                        strerror(opal_socket_errno),
                        opal_socket_errno, peer->sd);
            return ORTE_ERR_UNREACH;
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
            assert(2 == iov_count);
            msg->sdptr = (char *)iov[1].iov_base + rc;
            msg->sdbytes = ntohl(msg->hdr.nbytes) - rc;
        }
        return ORTE_ERR_RESOURCE_BUSY;
    }
}

/*
 * A file descriptor is available/ready for send. Check the state
 * of the socket and take the appropriate action.
 */
void mca_oob_tcp_send_handler(int sd, short flags, void *cbdata)
{
    mca_oob_tcp_peer_t* peer = (mca_oob_tcp_peer_t*)cbdata;
    mca_oob_tcp_send_t* msg;
    int rc;

    ORTE_ACQUIRE_OBJECT(peer);
    msg = peer->send_msg;

    opal_output_verbose(OOB_TCP_DEBUG_CONNECT, orte_oob_base_framework.framework_output,
                        "%s tcp:send_handler called to send to peer %s",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                        ORTE_NAME_PRINT(&peer->name));

    switch (peer->state) {
    case MCA_OOB_TCP_CONNECTING:
    case MCA_OOB_TCP_CLOSED:
        opal_output_verbose(OOB_TCP_DEBUG_CONNECT, orte_oob_base_framework.framework_output,
                            "%s tcp:send_handler %s",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                            mca_oob_tcp_state_print(peer->state));
        mca_oob_tcp_peer_complete_connect(peer);
        /* de-activate the send event until the connection
         * handshake completes
         */
        if (peer->send_ev_active) {
            opal_event_del(&peer->send_event);
            peer->send_ev_active = false;
        }
        break;
    case MCA_OOB_TCP_CONNECTED:
        opal_output_verbose(OOB_TCP_DEBUG_CONNECT, orte_oob_base_framework.framework_output,
                            "%s tcp:send_handler SENDING TO %s",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                            (NULL == peer->send_msg) ? "NULL" : ORTE_NAME_PRINT(&peer->name));
        if (NULL != msg) {
            opal_output_verbose(2, orte_oob_base_framework.framework_output,
                                "oob:tcp:send_handler SENDING MSG");
            if (ORTE_SUCCESS == (rc = send_msg(peer, msg))) {
                /* this msg is complete */
                if (NULL != msg->data || NULL == msg->msg) {
                    /* the relay is complete - release the data */
                    opal_output_verbose(2, orte_oob_base_framework.framework_output,
                                        "%s MESSAGE RELAY COMPLETE TO %s OF %d BYTES ON SOCKET %d",
                                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                        ORTE_NAME_PRINT(&(peer->name)),
                                        (int)ntohl(msg->hdr.nbytes), peer->sd);
                    OBJ_RELEASE(msg);
                    peer->send_msg = NULL;
                } else if (NULL != msg->msg->buffer) {
                    /* we are done - notify the RML */
                    opal_output_verbose(2, orte_oob_base_framework.framework_output,
                                        "%s MESSAGE SEND COMPLETE TO %s OF %d BYTES ON SOCKET %d",
                                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                        ORTE_NAME_PRINT(&(peer->name)),
                                        (int)ntohl(msg->hdr.nbytes), peer->sd);
                    msg->msg->status = ORTE_SUCCESS;
                    ORTE_RML_SEND_COMPLETE(msg->msg);
                    OBJ_RELEASE(msg);
                    peer->send_msg = NULL;
                } else if (NULL != msg->msg->data) {
                    /* this was a relay we have now completed - no need to
                     * notify the RML as the local proc didn't initiate
                     * the send
                     */
                    opal_output_verbose(2, orte_oob_base_framework.framework_output,
                                        "%s MESSAGE RELAY COMPLETE TO %s OF %d BYTES ON SOCKET %d",
                                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                        ORTE_NAME_PRINT(&(peer->name)),
                                        (int)ntohl(msg->hdr.nbytes), peer->sd);
                    msg->msg->status = ORTE_SUCCESS;
                    OBJ_RELEASE(msg);
                    peer->send_msg = NULL;
                } else {
                    /* rotate to the next iovec */
                    msg->iovnum++;
                    if (msg->iovnum < msg->msg->count) {
                        msg->sdptr = msg->msg->iov[msg->iovnum].iov_base;
                        msg->sdbytes = msg->msg->iov[msg->iovnum].iov_len;
                        /* exit this event to give the event lib
                         * a chance to progress any other pending
                         * actions
                         */
                        return;
                    } else {
                        /* this message is complete - notify the RML */
                        opal_output_verbose(2, orte_oob_base_framework.framework_output,
                                            "%s MESSAGE SEND COMPLETE TO %s OF %d BYTES ON SOCKET %d",
                                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                            ORTE_NAME_PRINT(&(peer->name)),
                                            (int)ntohl(msg->hdr.nbytes), peer->sd);
                        msg->msg->status = ORTE_SUCCESS;
                        ORTE_RML_SEND_COMPLETE(msg->msg);
                        OBJ_RELEASE(msg);
                        peer->send_msg = NULL;
                    }
                }
                /* fall thru to queue the next message */
            } else if (ORTE_ERR_RESOURCE_BUSY == rc ||
                       ORTE_ERR_WOULD_BLOCK == rc) {
                /* exit this event and let the event lib progress */
                return;
            } else {
                // report the error
                opal_output(0, "%s-%s mca_oob_tcp_peer_send_handler: unable to send message ON SOCKET %d",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                            ORTE_NAME_PRINT(&(peer->name)), peer->sd);
                opal_event_del(&peer->send_event);
                msg->msg->status = rc;
                ORTE_RML_SEND_COMPLETE(msg->msg);
                OBJ_RELEASE(msg);
                peer->send_msg = NULL;
                ORTE_FORCED_TERMINATE(1);
                return;
            }

            /* if current message completed - progress any pending sends by
             * moving the next in the queue into the "on-deck" position. Note
             * that this doesn't mean we send the message right now - we will
             * wait for another send_event to fire before doing so. This gives
             * us a chance to service any pending recvs.
             */
            peer->send_msg = (mca_oob_tcp_send_t*)
                opal_list_remove_first(&peer->send_queue);
        }

        /* if nothing else to do unregister for send event notifications */
        if (NULL == peer->send_msg && peer->send_ev_active) {
            opal_event_del(&peer->send_event);
            peer->send_ev_active = false;
        }
        break;
    default:
        opal_output(0, "%s-%s mca_oob_tcp_peer_send_handler: invalid connection state (%d) on socket %d",
                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                    ORTE_NAME_PRINT(&(peer->name)),
                    peer->state, peer->sd);
        if (peer->send_ev_active) {
            opal_event_del(&peer->send_event);
            peer->send_ev_active = false;
        }
        break;
    }
}

static int read_bytes(mca_oob_tcp_peer_t* peer)
{
    int rc;

    /* read until all bytes recvd or error */
    while (0 < peer->recv_msg->rdbytes) {
        rc = read(peer->sd, peer->recv_msg->rdptr, peer->recv_msg->rdbytes);
        if (rc < 0) {
            if(opal_socket_errno == EINTR) {
                continue;
            } else if (opal_socket_errno == EAGAIN) {
                /* tell the caller to keep this message on active,
                 * but let the event lib cycle so other messages
                 * can progress while this socket is busy
                 */
                return ORTE_ERR_RESOURCE_BUSY;
            } else if (opal_socket_errno == EWOULDBLOCK) {
                /* tell the caller to keep this message on active,
                 * but let the event lib cycle so other messages
                 * can progress while this socket is busy
                 */
                return ORTE_ERR_WOULD_BLOCK;
            }
            /* we hit an error and cannot progress this message - report
             * the error back to the RML and let the caller know
             * to abort this message
             */
            opal_output_verbose(OOB_TCP_DEBUG_FAIL, orte_oob_base_framework.framework_output,
                                "%s-%s mca_oob_tcp_msg_recv: readv failed: %s (%d)",
                                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                ORTE_NAME_PRINT(&(peer->name)),
                                strerror(opal_socket_errno),
                                opal_socket_errno);
            // mca_oob_tcp_peer_close(peer);
            // if (NULL != mca_oob_tcp.oob_exception_callback) {
            // mca_oob_tcp.oob_exception_callback(&peer->name, ORTE_RML_PEER_DISCONNECTED);
            //}
            return ORTE_ERR_COMM_FAILURE;
        } else if (rc == 0)  {
            /* the remote peer closed the connection - report that condition
             * and let the caller know
             */
            opal_output_verbose(OOB_TCP_DEBUG_FAIL, orte_oob_base_framework.framework_output,
                                "%s-%s mca_oob_tcp_msg_recv: peer closed connection",
                                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                ORTE_NAME_PRINT(&(peer->name)));
            /* stop all events */
            if (peer->recv_ev_active) {
                opal_event_del(&peer->recv_event);
                peer->recv_ev_active = false;
            }
            if (peer->timer_ev_active) {
                opal_event_del(&peer->timer_event);
                peer->timer_ev_active = false;
            }
            if (peer->send_ev_active) {
                opal_event_del(&peer->send_event);
                peer->send_ev_active = false;
            }
            if (NULL != peer->recv_msg) {
                OBJ_RELEASE(peer->recv_msg);
                peer->recv_msg = NULL;
            }
            mca_oob_tcp_peer_close(peer);
            //if (NULL != mca_oob_tcp.oob_exception_callback) {
            //   mca_oob_tcp.oob_exception_callback(&peer->peer_name, ORTE_RML_PEER_DISCONNECTED);
            //}
            return ORTE_ERR_WOULD_BLOCK;
        }
        /* we were able to read something, so adjust counters and location */
        peer->recv_msg->rdbytes -= rc;
        peer->recv_msg->rdptr += rc;
    }

    /* we read the full data block */
    return ORTE_SUCCESS;
}

/*
 * Dispatch to the appropriate action routine based on the state
 * of the connection with the peer.
 */

void mca_oob_tcp_recv_handler(int sd, short flags, void *cbdata)
{
    mca_oob_tcp_peer_t* peer = (mca_oob_tcp_peer_t*)cbdata;
    int rc;
    orte_rml_send_t *snd;

    ORTE_ACQUIRE_OBJECT(peer);

    opal_output_verbose(OOB_TCP_DEBUG_CONNECT, orte_oob_base_framework.framework_output,
                        "%s:tcp:recv:handler called for peer %s",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                        ORTE_NAME_PRINT(&peer->name));

    switch (peer->state) {
    case MCA_OOB_TCP_CONNECT_ACK:
        if (ORTE_SUCCESS == (rc = mca_oob_tcp_peer_recv_connect_ack(peer, peer->sd, NULL))) {
            opal_output_verbose(OOB_TCP_DEBUG_CONNECT, orte_oob_base_framework.framework_output,
                                "%s:tcp:recv:handler starting send/recv events",
                                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
            /* we connected! Start the send/recv events */
            if (!peer->recv_ev_active) {
                peer->recv_ev_active = true;
                ORTE_POST_OBJECT(peer);
                opal_event_add(&peer->recv_event, 0);
            }
            if (peer->timer_ev_active) {
                opal_event_del(&peer->timer_event);
                peer->timer_ev_active = false;
            }
            /* if there is a message waiting to be sent, queue it */
            if (NULL == peer->send_msg) {
                peer->send_msg = (mca_oob_tcp_send_t*)opal_list_remove_first(&peer->send_queue);
            }
            if (NULL != peer->send_msg && !peer->send_ev_active) {
                peer->send_ev_active = true;
                ORTE_POST_OBJECT(peer);
                opal_event_add(&peer->send_event, 0);
            }
            /* update our state */
            peer->state = MCA_OOB_TCP_CONNECTED;
        } else if (ORTE_ERR_UNREACH != rc) {
            /* we get an unreachable error returned if a connection
             * completes but is rejected - otherwise, we don't want
             * to terminate as we might be retrying the connection */
            opal_output_verbose(OOB_TCP_DEBUG_CONNECT, orte_oob_base_framework.framework_output,
                                "%s UNABLE TO COMPLETE CONNECT ACK WITH %s",
                                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                ORTE_NAME_PRINT(&peer->name));
            opal_event_del(&peer->recv_event);
            ORTE_FORCED_TERMINATE(1);
            return;
        }
        break;
    case MCA_OOB_TCP_CONNECTED:
        opal_output_verbose(OOB_TCP_DEBUG_CONNECT, orte_oob_base_framework.framework_output,
                            "%s:tcp:recv:handler CONNECTED",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
        /* allocate a new message and setup for recv */
        if (NULL == peer->recv_msg) {
            opal_output_verbose(OOB_TCP_DEBUG_CONNECT, orte_oob_base_framework.framework_output,
                                "%s:tcp:recv:handler allocate new recv msg",
                                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
            peer->recv_msg = OBJ_NEW(mca_oob_tcp_recv_t);
            if (NULL == peer->recv_msg) {
                opal_output(0, "%s-%s mca_oob_tcp_peer_recv_handler: unable to allocate recv message\n",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                            ORTE_NAME_PRINT(&(peer->name)));
                return;
            }
            /* start by reading the header */
            peer->recv_msg->rdptr = (char*)&peer->recv_msg->hdr;
            peer->recv_msg->rdbytes = sizeof(mca_oob_tcp_hdr_t);
        }
        /* if the header hasn't been completely read, read it */
        if (!peer->recv_msg->hdr_recvd) {
            opal_output_verbose(OOB_TCP_DEBUG_CONNECT, orte_oob_base_framework.framework_output,
                                "%s:tcp:recv:handler read hdr",
                                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
            if (ORTE_SUCCESS == (rc = read_bytes(peer))) {
                /* completed reading the header */
                peer->recv_msg->hdr_recvd = true;
                /* convert the header */
                MCA_OOB_TCP_HDR_NTOH(&peer->recv_msg->hdr);
                /* if this is a zero-byte message, then we are done */
                if (0 == peer->recv_msg->hdr.nbytes) {
                    opal_output_verbose(OOB_TCP_DEBUG_CONNECT, orte_oob_base_framework.framework_output,
                                        "%s RECVD ZERO-BYTE MESSAGE FROM %s for tag %d",
                                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                        ORTE_NAME_PRINT(&peer->name), peer->recv_msg->hdr.tag);
                    peer->recv_msg->data = NULL;  // make sure
                } else {
                    opal_output_verbose(OOB_TCP_DEBUG_CONNECT, orte_oob_base_framework.framework_output,
                                        "%s:tcp:recv:handler allocate data region of size %lu",
                                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), (unsigned long)peer->recv_msg->hdr.nbytes);
                    /* allocate the data region */
                    peer->recv_msg->data = (char*)malloc(peer->recv_msg->hdr.nbytes);
                    /* point to it */
                    peer->recv_msg->rdptr = peer->recv_msg->data;
                    peer->recv_msg->rdbytes = peer->recv_msg->hdr.nbytes;
                }
                /* fall thru and attempt to read the data */
            } else if (ORTE_ERR_RESOURCE_BUSY == rc ||
                       ORTE_ERR_WOULD_BLOCK == rc) {
                /* exit this event and let the event lib progress */
                return;
            } else {
                /* close the connection */
                opal_output_verbose(OOB_TCP_DEBUG_CONNECT, orte_oob_base_framework.framework_output,
                                    "%s:tcp:recv:handler error reading bytes - closing connection",
                                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
                mca_oob_tcp_peer_close(peer);
                return;
            }
        }

        if (peer->recv_msg->hdr_recvd) {
            /* continue to read the data block - we start from
             * wherever we left off, which could be at the
             * beginning or somewhere in the message
             */
            if (ORTE_SUCCESS == (rc = read_bytes(peer))) {
                /* we recvd all of the message */
                opal_output_verbose(OOB_TCP_DEBUG_CONNECT, orte_oob_base_framework.framework_output,
                                    "%s RECVD COMPLETE MESSAGE FROM %s (ORIGIN %s) OF %d BYTES FOR DEST %s TAG %d",
                                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                    ORTE_NAME_PRINT(&peer->name),
                                    ORTE_NAME_PRINT(&peer->recv_msg->hdr.origin),
                                    (int)peer->recv_msg->hdr.nbytes,
                                    ORTE_NAME_PRINT(&peer->recv_msg->hdr.dst),
                                    peer->recv_msg->hdr.tag);

                /* am I the intended recipient (header was already converted back to host order)? */
                if (peer->recv_msg->hdr.dst.jobid == ORTE_PROC_MY_NAME->jobid &&
                    peer->recv_msg->hdr.dst.vpid == ORTE_PROC_MY_NAME->vpid) {
                    /* yes - post it to the RML for delivery */
                    opal_output_verbose(OOB_TCP_DEBUG_CONNECT, orte_oob_base_framework.framework_output,
                                        "%s DELIVERING TO RML tag = %d seq_num = %d",
                                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                        peer->recv_msg->hdr.tag,
                                        peer->recv_msg->hdr.seq_num);
                    ORTE_RML_POST_MESSAGE(&peer->recv_msg->hdr.origin,
                                          peer->recv_msg->hdr.tag,
                                          peer->recv_msg->hdr.seq_num,
                                          peer->recv_msg->data,
                                          peer->recv_msg->hdr.nbytes);
                    OBJ_RELEASE(peer->recv_msg);
                } else {
                    /* promote this to the OOB as some other transport might
                     * be the next best hop */
                    opal_output_verbose(OOB_TCP_DEBUG_CONNECT, orte_oob_base_framework.framework_output,
                                        "%s TCP PROMOTING ROUTED MESSAGE FOR %s TO OOB",
                                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                        ORTE_NAME_PRINT(&peer->recv_msg->hdr.dst));
                    snd = OBJ_NEW(orte_rml_send_t);
                    snd->dst = peer->recv_msg->hdr.dst;
                    snd->origin = peer->recv_msg->hdr.origin;
                    snd->tag = peer->recv_msg->hdr.tag;
                    snd->data = peer->recv_msg->data;
                    snd->seq_num = peer->recv_msg->hdr.seq_num;
                    snd->count = peer->recv_msg->hdr.nbytes;
                    snd->routed = strdup(peer->recv_msg->hdr.routed);
                    snd->cbfunc.iov = NULL;
                    snd->cbdata = NULL;
                    /* activate the OOB send state */
                    ORTE_OOB_SEND(snd);
                    /* protect the data */
                    peer->recv_msg->data = NULL;
                    /* cleanup */
                    OBJ_RELEASE(peer->recv_msg);
                }
                peer->recv_msg = NULL;
                return;
            } else if (ORTE_ERR_RESOURCE_BUSY == rc ||
                       ORTE_ERR_WOULD_BLOCK == rc) {
                /* exit this event and let the event lib progress */
                return;
            } else {
                // report the error
                opal_output(0, "%s-%s mca_oob_tcp_peer_recv_handler: unable to recv message",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                            ORTE_NAME_PRINT(&(peer->name)));
                /* turn off the recv event */
                opal_event_del(&peer->recv_event);
                ORTE_FORCED_TERMINATE(1);
                return;
            }
        }
        break;
    default:
        opal_output(0, "%s-%s mca_oob_tcp_peer_recv_handler: invalid socket state(%d)",
                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                    ORTE_NAME_PRINT(&(peer->name)),
                    peer->state);
        // mca_oob_tcp_peer_close(peer);
        break;
    }
}

static void snd_cons(mca_oob_tcp_send_t *ptr)
{
    memset(&ptr->hdr, 0, sizeof(mca_oob_tcp_hdr_t));
    ptr->msg = NULL;
    ptr->data = NULL;
    ptr->hdr_sent = false;
    ptr->iovnum = 0;
    ptr->sdptr = NULL;
    ptr->sdbytes = 0;
}
/* we don't destruct any RML msg that is
 * attached to our send as the RML owns
 * that memory. However, if we relay a
 * msg, the data in the relay belongs to
 * us and must be free'd
 */
static void snd_des(mca_oob_tcp_send_t *ptr)
{
    if (NULL != ptr->data) {
        free(ptr->data);
    }
}
OBJ_CLASS_INSTANCE(mca_oob_tcp_send_t,
                   opal_list_item_t,
                   snd_cons, snd_des);

static void rcv_cons(mca_oob_tcp_recv_t *ptr)
{
    memset(&ptr->hdr, 0, sizeof(mca_oob_tcp_hdr_t));
    ptr->hdr_recvd = false;
    ptr->rdptr = NULL;
    ptr->rdbytes = 0;
}
OBJ_CLASS_INSTANCE(mca_oob_tcp_recv_t,
                   opal_list_item_t,
                   rcv_cons, NULL);

static void err_cons(mca_oob_tcp_msg_error_t *ptr)
{
    ptr->rmsg = NULL;
    ptr->snd = NULL;
}
OBJ_CLASS_INSTANCE(mca_oob_tcp_msg_error_t,
                   opal_object_t,
                   err_cons, NULL);
