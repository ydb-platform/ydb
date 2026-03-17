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
 * Copyright (c) 2009-2012 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2011      Oak Ridge National Labs.  All rights reserved.
 * Copyright (c) 2013-2017 Intel, Inc.  All rights reserved.
 * Copyright (c) 2016      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 */

#include "orte_config.h"
#include "orte/types.h"
#include "opal/types.h"

#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
#include <fcntl.h>
#ifdef HAVE_NETINET_IN_H
#include <netinet/in.h>
#endif
#ifdef HAVE_ARPA_INET_H
#include <arpa/inet.h>
#endif
#ifdef HAVE_NETDB_H
#include <netdb.h>
#endif
#include <ctype.h>

#include "opal/runtime/opal_progress_threads.h"
#include "opal/util/show_help.h"
#include "opal/util/error.h"
#include "opal/util/output.h"
#include "opal/opal_socket_errno.h"
#include "opal/util/if.h"
#include "opal/util/net.h"
#include "opal/util/argv.h"
#include "opal/class/opal_hash_table.h"

#include "orte/mca/errmgr/errmgr.h"
#include "orte/mca/ess/ess.h"
#include "orte/mca/routed/routed.h"
#include "orte/util/name_fns.h"
#include "orte/util/parse_options.h"
#include "orte/util/show_help.h"
#include "orte/util/threads.h"
#include "orte/runtime/orte_globals.h"

#include "orte/mca/oob/tcp/oob_tcp.h"
#include "orte/mca/oob/tcp/oob_tcp_component.h"
#include "orte/mca/oob/tcp/oob_tcp_peer.h"
#include "orte/mca/oob/tcp/oob_tcp_common.h"
#include "orte/mca/oob/tcp/oob_tcp_connection.h"
#include "orte/mca/oob/tcp/oob_tcp_sendrecv.h"

static void accept_connection(const int accepted_fd,
                              const struct sockaddr *addr);
static void ping(const orte_process_name_t *proc);
static void send_nb(orte_rml_send_t *msg);
static void ft_event(int state);

mca_oob_tcp_module_t mca_oob_tcp_module = {
    .accept_connection = accept_connection,
    .ping = ping,
    .send_nb = send_nb,
    .ft_event = ft_event
};

/*
 * Local utility functions
 */
static void recv_handler(int sd, short flags, void* user);

/* Called by mca_oob_tcp_accept() and connection_handler() on
 * a socket that has been accepted.  This call finishes processing the
 * socket, including setting socket options and registering for the
 * OOB-level connection handshake.  Used in both the threaded and
 * event listen modes.
 */
static void accept_connection(const int accepted_fd,
                              const struct sockaddr *addr)
{
    opal_output_verbose(OOB_TCP_DEBUG_CONNECT, orte_oob_base_framework.framework_output,
                        "%s accept_connection: %s:%d\n",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                        opal_net_get_hostname(addr),
                        opal_net_get_port(addr));

   /* setup socket options */
    orte_oob_tcp_set_socket_options(accepted_fd);

    /* use a one-time event to wait for receipt of peer's
     *  process ident message to complete this connection
     */
    ORTE_ACTIVATE_TCP_ACCEPT_STATE(accepted_fd, addr, recv_handler);
}

/* API functions */
static void ping(const orte_process_name_t *proc)
{
    mca_oob_tcp_peer_t *peer;

    opal_output_verbose(2, orte_oob_base_framework.framework_output,
                        "%s:[%s:%d] processing ping to peer %s",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                        __FILE__, __LINE__,
                        ORTE_NAME_PRINT(proc));

    /* do we know this peer? */
    if (NULL == (peer = mca_oob_tcp_peer_lookup(proc))) {
        /* push this back to the component so it can try
         * another module within this transport. If no
         * module can be found, the component can push back
         * to the framework so another component can try
         */
        opal_output_verbose(2, orte_oob_base_framework.framework_output,
                            "%s:[%s:%d] hop %s unknown",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                            __FILE__, __LINE__,
                            ORTE_NAME_PRINT(proc));
        ORTE_ACTIVATE_TCP_MSG_ERROR(NULL, NULL, proc, mca_oob_tcp_component_hop_unknown);
        return;
    }

    /* has this peer had a progress thread assigned yet? */
    if (NULL == peer->ev_base) {
        /* nope - assign one */
        ORTE_OOB_TCP_NEXT_BASE(peer);
    }

    /* if we are already connected, there is nothing to do */
    if (MCA_OOB_TCP_CONNECTED == peer->state) {
        opal_output_verbose(2, orte_oob_base_framework.framework_output,
                            "%s:[%s:%d] already connected to peer %s",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                            __FILE__, __LINE__,
                            ORTE_NAME_PRINT(proc));
        return;
    }

    /* if we are already connecting, there is nothing to do */
    if (MCA_OOB_TCP_CONNECTING == peer->state ||
        MCA_OOB_TCP_CONNECT_ACK == peer->state) {
        opal_output_verbose(2, orte_oob_base_framework.framework_output,
                            "%s:[%s:%d] already connecting to peer %s",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                            __FILE__, __LINE__,
                            ORTE_NAME_PRINT(proc));
        return;
    }

    /* attempt the connection */
    peer->state = MCA_OOB_TCP_CONNECTING;
    ORTE_ACTIVATE_TCP_CONN_STATE(peer, mca_oob_tcp_peer_try_connect);
}

static void send_nb(orte_rml_send_t *msg)
{
    mca_oob_tcp_peer_t *peer;
    orte_process_name_t hop;


    /* do we have a route to this peer (could be direct)? */
    hop = orte_routed.get_route(msg->routed, &msg->dst);
    /* do we know this hop? */
    if (NULL == (peer = mca_oob_tcp_peer_lookup(&hop))) {
        /* push this back to the component so it can try
         * another module within this transport. If no
         * module can be found, the component can push back
         * to the framework so another component can try
         */
        opal_output_verbose(2, orte_oob_base_framework.framework_output,
                            "%s:[%s:%d] processing send to peer %s:%d seq_num = %d hop %s unknown",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                            __FILE__, __LINE__,
                            ORTE_NAME_PRINT(&msg->dst), msg->tag, msg->seq_num,
                            ORTE_NAME_PRINT(&hop));
        ORTE_ACTIVATE_TCP_NO_ROUTE(msg, &hop, mca_oob_tcp_component_no_route);
        return;
    }

    opal_output_verbose(2, orte_oob_base_framework.framework_output,
                        "%s:[%s:%d] processing send to peer %s:%d seq_num = %d via %s",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                        __FILE__, __LINE__,
                        ORTE_NAME_PRINT(&msg->dst), msg->tag, msg->seq_num,
                        ORTE_NAME_PRINT(&peer->name));
    /* has this peer had a progress thread assigned yet? */
    if (NULL == peer->ev_base) {
        /* nope - assign one */
        ORTE_OOB_TCP_NEXT_BASE(peer);
    }
    /* add the msg to the hop's send queue */
    if (MCA_OOB_TCP_CONNECTED == peer->state) {
        opal_output_verbose(2, orte_oob_base_framework.framework_output,
                            "%s tcp:send_nb: already connected to %s - queueing for send",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                            ORTE_NAME_PRINT(&peer->name));
        MCA_OOB_TCP_QUEUE_SEND(msg, peer);
        return;
    }

    /* add the message to the queue for sending after the
     * connection is formed
     */
    MCA_OOB_TCP_QUEUE_PENDING(msg, peer);

    if (MCA_OOB_TCP_CONNECTING != peer->state &&
        MCA_OOB_TCP_CONNECT_ACK != peer->state) {
        /* we have to initiate the connection - again, we do not
         * want to block while the connection is created.
         * So throw us into an event that will create
         * the connection via a mini-state-machine :-)
         */
        opal_output_verbose(2, orte_oob_base_framework.framework_output,
                            "%s tcp:send_nb: initiating connection to %s",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                            ORTE_NAME_PRINT(&peer->name));
        peer->state = MCA_OOB_TCP_CONNECTING;
        ORTE_ACTIVATE_TCP_CONN_STATE(peer, mca_oob_tcp_peer_try_connect);
    }
}

/*
 * Event callback when there is data available on the registered
 * socket to recv.  This is called for the listen sockets to accept an
 * incoming connection, on new sockets trying to complete the software
 * connection process, and for probes.  Data on an established
 * connection is handled elsewhere.
 */
static void recv_handler(int sd, short flg, void *cbdata)
{
    mca_oob_tcp_conn_op_t *op = (mca_oob_tcp_conn_op_t*)cbdata;
    int flags;
    mca_oob_tcp_hdr_t hdr;
    mca_oob_tcp_peer_t *peer;

    ORTE_ACQUIRE_OBJECT(op);

    opal_output_verbose(OOB_TCP_DEBUG_CONNECT, orte_oob_base_framework.framework_output,
                        "%s:tcp:recv:handler called",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));

    /* get the handshake */
    if (ORTE_SUCCESS != mca_oob_tcp_peer_recv_connect_ack(NULL, sd, &hdr)) {
        goto cleanup;
    }

    /* finish processing ident */
    if (MCA_OOB_TCP_IDENT == hdr.type) {
        if (NULL == (peer = mca_oob_tcp_peer_lookup(&hdr.origin))) {
            /* should never happen */
            mca_oob_tcp_peer_close(peer);
            goto cleanup;
        }
        /* set socket up to be non-blocking */
        if ((flags = fcntl(sd, F_GETFL, 0)) < 0) {
            opal_output(0, "%s mca_oob_tcp_recv_connect: fcntl(F_GETFL) failed: %s (%d)",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), strerror(opal_socket_errno), opal_socket_errno);
        } else {
            flags |= O_NONBLOCK;
            if (fcntl(sd, F_SETFL, flags) < 0) {
                opal_output(0, "%s mca_oob_tcp_recv_connect: fcntl(F_SETFL) failed: %s (%d)",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), strerror(opal_socket_errno), opal_socket_errno);
            }
        }
        /* is the peer instance willing to accept this connection */
        peer->sd = sd;
        if (mca_oob_tcp_peer_accept(peer) == false) {
            if (OOB_TCP_DEBUG_CONNECT <= opal_output_get_verbosity(orte_oob_base_framework.framework_output)) {
                opal_output(0, "%s-%s mca_oob_tcp_recv_connect: "
                            "rejected connection from %s connection state %d",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                            ORTE_NAME_PRINT(&(peer->name)),
                            ORTE_NAME_PRINT(&(hdr.origin)),
                            peer->state);
            }
            CLOSE_THE_SOCKET(sd);
        }
    }

 cleanup:
    OBJ_RELEASE(op);
}

/* Dummy function for when we are not using FT. */
#if OPAL_ENABLE_FT_CR == 0
static void ft_event(int state)
{
    return;
}

#else
static void ft_event(int state) {
#if 0
    opal_list_item_t *item;
#endif

    if(OPAL_CRS_CHECKPOINT == state) {
#if 0
        /*
         * Disable event processing while we are working
         */
        opal_event_disable();
#endif
    }
    else if(OPAL_CRS_CONTINUE == state) {
#if 0
        /*
         * Resume event processing
         */
        opal_event_enable();
    }
    else if(OPAL_CRS_RESTART == state) {
        /*
         * Clean out cached connection information
         * Select pieces of finalize/init
         */
        for (item = opal_list_remove_first(&mca_oob_tcp_module.peer_list);
            item != NULL;
            item = opal_list_remove_first(&mca_oob_tcp_module.peer_list)) {
            mca_oob_tcp_peer_t* peer = (mca_oob_tcp_peer_t*)item;
            /* JJH: Use the below command for debugging restarts with invalid sockets
             * mca_oob_tcp_peer_dump(peer, "RESTART CLEAN")
             */
            MCA_OOB_TCP_PEER_RETURN(peer);
        }

        OBJ_DESTRUCT(&mca_oob_tcp_module.peer_free);
        OBJ_DESTRUCT(&mca_oob_tcp_module.peer_names);
        OBJ_DESTRUCT(&mca_oob_tcp_module.peers);
        OBJ_DESTRUCT(&mca_oob_tcp_module.peer_list);

        OBJ_CONSTRUCT(&mca_oob_tcp_module.peer_list,     opal_list_t);
        OBJ_CONSTRUCT(&mca_oob_tcp_module.peers,         opal_hash_table_t);
        OBJ_CONSTRUCT(&mca_oob_tcp_module.peer_names,    opal_hash_table_t);
        OBJ_CONSTRUCT(&mca_oob_tcp_module.peer_free,     opal_free_list_t);

        /*
         * Resume event processing
         */
        opal_event_enable();
#endif
    }
    else if(OPAL_CRS_TERM == state ) {
        ;
    }
    else {
        ;
    }

    return;
}
#endif
