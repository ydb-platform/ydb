/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2006 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2006-2013 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2010-2011 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2015-2017 Intel, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef _MCA_OOB_TCP_PEER_H_
#define _MCA_OOB_TCP_PEER_H_

#include "orte_config.h"

#include "opal/mca/event/event.h"

#include "orte/util/threads.h"
#include "oob_tcp.h"
#include "oob_tcp_sendrecv.h"

typedef struct {
    opal_list_item_t super;
    struct sockaddr_storage addr; // an address where a peer can be found
    int retries;                  // number of times we have tried to connect to this address
    mca_oob_tcp_state_t state;    // state of this address
} mca_oob_tcp_addr_t;
OBJ_CLASS_DECLARATION(mca_oob_tcp_addr_t);

/* object for tracking peers in the module */
typedef struct {
    opal_list_item_t super;
    /* although not required, there is enough debug
     * value that retaining the name makes sense
     */
    orte_process_name_t name;
    char *auth_method;  // method they used to authenticate
    int sd;
    opal_list_t addrs;
    mca_oob_tcp_addr_t *active_addr;
    mca_oob_tcp_state_t state;
    int num_retries;
    opal_event_base_t *ev_base; // progress thread this peer is assigned to
    opal_event_t send_event;    /**< registration with event thread for send events */
    bool send_ev_active;
    opal_event_t recv_event;    /**< registration with event thread for recv events */
    bool recv_ev_active;
    opal_event_t timer_event;   /**< timer for retrying connection failures */
    bool timer_ev_active;
    opal_list_t send_queue;      /**< list of messages to send */
    mca_oob_tcp_send_t *send_msg; /**< current send in progress */
    mca_oob_tcp_recv_t *recv_msg; /**< current recv in progress */
} mca_oob_tcp_peer_t;
OBJ_CLASS_DECLARATION(mca_oob_tcp_peer_t);

/* state machine for processing peer data */
typedef struct {
    opal_object_t super;
    opal_event_t ev;
    orte_process_name_t peer;
    uint16_t af_family;
    char *net;
    char *port;
    char *rtmod;
} mca_oob_tcp_peer_op_t;
OBJ_CLASS_DECLARATION(mca_oob_tcp_peer_op_t);

#define ORTE_ACTIVATE_TCP_CMP_OP(p, r, cbfunc)                          \
    do {                                                                \
        mca_oob_tcp_peer_op_t *pop;                                     \
        char *proxy;                                                    \
        pop = OBJ_NEW(mca_oob_tcp_peer_op_t);                           \
        pop->peer.jobid = (p)->name.jobid;                              \
        pop->peer.vpid = (p)->name.vpid;                                \
        proxy = (r);                                                    \
        if (NULL != proxy) {                                            \
            pop->rtmod = strdup(proxy);                                 \
        }                                                               \
        ORTE_THREADSHIFT(pop, orte_oob_base.ev_base,                    \
                         (cbfunc), ORTE_MSG_PRI);                       \
    } while(0);

#endif /* _MCA_OOB_TCP_PEER_H_ */
