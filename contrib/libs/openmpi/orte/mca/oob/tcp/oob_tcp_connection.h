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
 * Copyright (c) 2014-2017 Intel, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef _MCA_OOB_TCP_CONNECTION_H_
#define _MCA_OOB_TCP_CONNECTION_H_

#include "orte_config.h"

#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
#ifdef HAVE_SYS_SOCKET_H
#include <sys/socket.h>
#endif

#include "orte/util/threads.h"
#include "oob_tcp.h"
#include "oob_tcp_peer.h"

/* State machine for connection operations */
typedef struct {
    opal_object_t super;
    mca_oob_tcp_peer_t *peer;
    opal_event_t ev;
} mca_oob_tcp_conn_op_t;
OBJ_CLASS_DECLARATION(mca_oob_tcp_conn_op_t);

#define CLOSE_THE_SOCKET(socket)    \
    do {                            \
        shutdown(socket, 2);        \
        close(socket);              \
    } while(0)

#define ORTE_ACTIVATE_TCP_CONN_STATE(p, cbfunc)                         \
    do {                                                                \
        mca_oob_tcp_conn_op_t *cop;                                     \
        opal_output_verbose(5, orte_oob_base_framework.framework_output, \
                            "%s:[%s:%d] connect to %s",                 \
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),         \
                            __FILE__, __LINE__,                         \
                            ORTE_NAME_PRINT((&(p)->name)));             \
        cop = OBJ_NEW(mca_oob_tcp_conn_op_t);                           \
        cop->peer = (p);                                                \
        ORTE_THREADSHIFT(cop, (p)->ev_base, (cbfunc), ORTE_MSG_PRI);    \
    } while(0);

#define ORTE_ACTIVATE_TCP_ACCEPT_STATE(s, a, cbfunc)            \
    do {                                                        \
        mca_oob_tcp_conn_op_t *cop;                             \
        cop = OBJ_NEW(mca_oob_tcp_conn_op_t);                   \
        opal_event_set(orte_oob_base.ev_base, &cop->ev, s,      \
                       OPAL_EV_READ, (cbfunc), cop);            \
        opal_event_set_priority(&cop->ev, ORTE_MSG_PRI);        \
        ORTE_POST_OBJECT(cop);                                  \
        opal_event_add(&cop->ev, 0);                            \
    } while(0);

#define ORTE_RETRY_TCP_CONN_STATE(p, cbfunc, tv)                        \
    do {                                                                \
        mca_oob_tcp_conn_op_t *cop;                                     \
        opal_output_verbose(5, orte_oob_base_framework.framework_output, \
                            "%s:[%s:%d] retry connect to %s",           \
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),         \
                            __FILE__, __LINE__,                         \
                            ORTE_NAME_PRINT((&(p)->name)));             \
        cop = OBJ_NEW(mca_oob_tcp_conn_op_t);                           \
        cop->peer = (p);                                                \
        opal_event_evtimer_set((p)->ev_base,                            \
                               &cop->ev,                                \
                               (cbfunc), cop);                          \
        ORTE_POST_OBJECT(cop);                                          \
        opal_event_evtimer_add(&cop->ev, (tv));                         \
    } while(0);

ORTE_MODULE_DECLSPEC void mca_oob_tcp_peer_try_connect(int fd, short args, void *cbdata);
ORTE_MODULE_DECLSPEC void mca_oob_tcp_peer_dump(mca_oob_tcp_peer_t* peer, const char* msg);
ORTE_MODULE_DECLSPEC bool mca_oob_tcp_peer_accept(mca_oob_tcp_peer_t* peer);
ORTE_MODULE_DECLSPEC void mca_oob_tcp_peer_complete_connect(mca_oob_tcp_peer_t* peer);
ORTE_MODULE_DECLSPEC int mca_oob_tcp_peer_recv_connect_ack(mca_oob_tcp_peer_t* peer,
                                                           int sd, mca_oob_tcp_hdr_t *dhdr);
ORTE_MODULE_DECLSPEC void mca_oob_tcp_peer_close(mca_oob_tcp_peer_t *peer);

#endif /* _MCA_OOB_TCP_CONNECTION_H_ */
