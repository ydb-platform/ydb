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
 * Copyright (c) 2010-2013 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2013-2017 Intel, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef _MCA_OOB_TCP_SENDRECV_H_
#define _MCA_OOB_TCP_SENDRECV_H_

#include "orte_config.h"

#include "opal/class/opal_list.h"

#include "orte/mca/rml/base/base.h"
#include "orte/util/threads.h"
#include "oob_tcp.h"
#include "oob_tcp_hdr.h"

/* forward declare */
struct mca_oob_tcp_peer_t;

/* tcp structure for sending a message */
typedef struct {
    opal_list_item_t super;
    opal_event_t ev;
    struct mca_oob_tcp_peer_t *peer;
    bool activate;
    mca_oob_tcp_hdr_t hdr;
    orte_rml_send_t *msg;
    char *data;
    bool hdr_sent;
    int iovnum;
    char *sdptr;
    size_t sdbytes;
} mca_oob_tcp_send_t;
OBJ_CLASS_DECLARATION(mca_oob_tcp_send_t);

/* tcp structure for recving a message */
typedef struct {
    opal_list_item_t super;
    mca_oob_tcp_hdr_t hdr;
    bool hdr_recvd;
    char *data;
    char *rdptr;
    size_t rdbytes;
} mca_oob_tcp_recv_t;
OBJ_CLASS_DECLARATION(mca_oob_tcp_recv_t);

/* Queue a message to be sent to a specified peer. The macro
 * checks to see if a message is already in position to be
 * sent - if it is, then the message provided is simply added
 * to the peer's message queue. If not, then the provided message
 * is placed in the "ready" position
 *
 * If the provided boolean is true, then the send event for the
 * peer is checked and activated if not already active. This allows
 * the macro to either immediately send the message, or to queue
 * it as "pending" for later transmission - e.g., after the
 * connection procedure is completed
 *
 * p => pointer to mca_oob_tcp_peer_t
 * s => pointer to mca_oob_tcp_send_t
 * f => true if send event is to be activated
 */
#define MCA_OOB_TCP_QUEUE_MSG(p, s, f)                                  \
    do {                                                                \
        (s)->peer = (struct mca_oob_tcp_peer_t*)(p);                    \
        (s)->activate = (f);                                            \
        ORTE_THREADSHIFT((s), (p)->ev_base,                             \
                         mca_oob_tcp_queue_msg, ORTE_MSG_PRI);          \
    } while(0)

/* queue a message to be sent by one of our modules - must
 * provide the following params:
 *
 * m - the RML message to be sent
 * p - the final recipient
 */
#define MCA_OOB_TCP_QUEUE_SEND(m, p)                                    \
    do {                                                                \
        mca_oob_tcp_send_t *_s;                                         \
        int i;                                                          \
        opal_output_verbose(5, orte_oob_base_framework.framework_output, \
                            "%s:[%s:%d] queue send to %s",              \
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),        \
                             __FILE__, __LINE__,                        \
                            ORTE_NAME_PRINT(&((m)->dst)));              \
        _s = OBJ_NEW(mca_oob_tcp_send_t);                              \
        /* setup the header */                                          \
        _s->hdr.origin = (m)->origin;                                  \
        _s->hdr.dst = (m)->dst;                                        \
        _s->hdr.type = MCA_OOB_TCP_USER;                               \
        _s->hdr.tag = (m)->tag;                                        \
        _s->hdr.seq_num = (m)->seq_num;                                \
        if (NULL != (m)->routed) {                                      \
            (void)strncpy(_s->hdr.routed, (m)->routed,                 \
                          ORTE_MAX_RTD_SIZE);                           \
        }                                                               \
        /* point to the actual message */                               \
        _s->msg = (m);                                                 \
        /* set the total number of bytes to be sent */                  \
        if (NULL != (m)->buffer) {                                      \
            _s->hdr.nbytes = (m)->buffer->bytes_used;                  \
        } else if (NULL != (m)->iov) {                                  \
            _s->hdr.nbytes = 0;                                        \
            for (i=0; i < (m)->count; i++) {                            \
                _s->hdr.nbytes += (m)->iov[i].iov_len;                 \
            }                                                           \
        } else {                                                        \
            _s->hdr.nbytes = (m)->count;                               \
        }                                                               \
        /* prep header for xmission */                                  \
        MCA_OOB_TCP_HDR_HTON(&_s->hdr);                                \
        /* start the send with the header */                            \
        _s->sdptr = (char*)&_s->hdr;                                  \
        _s->sdbytes = sizeof(mca_oob_tcp_hdr_t);                       \
        /* add to the msg queue for this peer */                        \
        MCA_OOB_TCP_QUEUE_MSG((p), _s, true);                          \
    } while(0)

/* queue a message to be sent by one of our modules upon completing
 * the connection process - must provide the following params:
 *
 * m - the RML message to be sent
 * p - the final recipient
 */
#define MCA_OOB_TCP_QUEUE_PENDING(m, p)                                 \
    do {                                                                \
        mca_oob_tcp_send_t *_s;                                        \
        int i;                                                          \
        opal_output_verbose(5, orte_oob_base_framework.framework_output, \
                            "%s:[%s:%d] queue pending to %s",           \
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),         \
                            __FILE__, __LINE__,                         \
                            ORTE_NAME_PRINT(&((m)->dst)));              \
        _s = OBJ_NEW(mca_oob_tcp_send_t);                              \
        /* setup the header */                                          \
        _s->hdr.origin = (m)->origin;                                  \
        _s->hdr.dst = (m)->dst;                                        \
        _s->hdr.type = MCA_OOB_TCP_USER;                               \
        _s->hdr.tag = (m)->tag;                                        \
        _s->hdr.seq_num = (m)->seq_num;                                \
        if (NULL != (m)->routed) {                                      \
            (void)strncpy(_s->hdr.routed, (m)->routed,                 \
                          ORTE_MAX_RTD_SIZE);                           \
        }                                                               \
        /* point to the actual message */                               \
        _s->msg = (m);                                                 \
        /* set the total number of bytes to be sent */                  \
        if (NULL != (m)->buffer) {                                      \
            _s->hdr.nbytes = (m)->buffer->bytes_used;                  \
        } else if (NULL != (m)->iov) {                                  \
            _s->hdr.nbytes = 0;                                        \
            for (i=0; i < (m)->count; i++) {                            \
                _s->hdr.nbytes += (m)->iov[i].iov_len;                 \
            }                                                           \
        } else {                                                        \
            _s->hdr.nbytes = (m)->count;                               \
        }                                                               \
        /* prep header for xmission */                                  \
        MCA_OOB_TCP_HDR_HTON(&_s->hdr);                                \
        /* start the send with the header */                            \
        _s->sdptr = (char*)&_s->hdr;                                  \
        _s->sdbytes = sizeof(mca_oob_tcp_hdr_t);                       \
        /* add to the msg queue for this peer */                        \
        MCA_OOB_TCP_QUEUE_MSG((p), _s, false);                         \
    } while(0)

/* queue a message for relay by one of our modules - must
 * provide the following params:
 *
 * m = the mca_oob_tcp_recv_t that was received
 * p - the next hop
*/
#define MCA_OOB_TCP_QUEUE_RELAY(m, p)                                   \
    do {                                                                \
        mca_oob_tcp_send_t *_s;                                        \
        opal_output_verbose(5, orte_oob_base_framework.framework_output, \
                            "%s:[%s:%d] queue relay to %s",             \
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),         \
                            __FILE__, __LINE__,                         \
                            ORTE_NAME_PRINT(&((p)->name)));             \
        _s = OBJ_NEW(mca_oob_tcp_send_t);                              \
        /* setup the header */                                          \
        _s->hdr.origin = (m)->hdr.origin;                              \
        _s->hdr.dst = (m)->hdr.dst;                                    \
        _s->hdr.type = MCA_OOB_TCP_USER;                               \
        _s->hdr.tag = (m)->hdr.tag;                                    \
        (void)strncpy(_s->hdr.routed, (m)->hdr.routed,                 \
                      ORTE_MAX_RTD_SIZE);                               \
        /* point to the actual message */                               \
        _s->data = (m)->data;                                          \
        /* set the total number of bytes to be sent */                  \
        _s->hdr.nbytes = (m)->hdr.nbytes;                              \
        /* prep header for xmission */                                  \
        MCA_OOB_TCP_HDR_HTON(&_s->hdr);                                \
        /* start the send with the header */                            \
        _s->sdptr = (char*)&_s->hdr;                                  \
        _s->sdbytes = sizeof(mca_oob_tcp_hdr_t);                       \
        /* add to the msg queue for this peer */                        \
        MCA_OOB_TCP_QUEUE_MSG((p), _s, true);                          \
    } while(0)

/* State machine for processing message */
typedef struct {
    opal_object_t super;
    opal_event_t ev;
    orte_rml_send_t *msg;
} mca_oob_tcp_msg_op_t;
OBJ_CLASS_DECLARATION(mca_oob_tcp_msg_op_t);

#define ORTE_ACTIVATE_TCP_POST_SEND(ms, cbfunc)                         \
    do {                                                                \
        mca_oob_tcp_msg_op_t *mop;                                      \
        opal_output_verbose(5, orte_oob_base_framework.framework_output, \
                            "%s:[%s:%d] post send to %s",               \
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),         \
                            __FILE__, __LINE__,                         \
                            ORTE_NAME_PRINT(&((ms)->dst)));             \
        mop = OBJ_NEW(mca_oob_tcp_msg_op_t);                            \
        mop->msg = (ms);                                                \
        ORTE_THREADSHIFT(mop, (ms)->peer->ev_base,                      \
                         (cbfunc), ORTE_MSG_PRI);                       \
    } while(0);

typedef struct {
    opal_object_t super;
    opal_event_t ev;
    orte_rml_send_t *rmsg;
    mca_oob_tcp_send_t *snd;
    orte_process_name_t hop;
} mca_oob_tcp_msg_error_t;
OBJ_CLASS_DECLARATION(mca_oob_tcp_msg_error_t);

#define ORTE_ACTIVATE_TCP_MSG_ERROR(s, r, h, cbfunc)                    \
    do {                                                                \
        mca_oob_tcp_msg_error_t *mop;                                   \
        mca_oob_tcp_send_t *snd;                                        \
        mca_oob_tcp_recv_t *proxy;                                      \
        opal_output_verbose(5, orte_oob_base_framework.framework_output, \
                            "%s:[%s:%d] post msg error to %s",          \
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),         \
                            __FILE__, __LINE__,                         \
                            ORTE_NAME_PRINT((h)));                      \
        mop = OBJ_NEW(mca_oob_tcp_msg_error_t);                         \
        if (NULL != (s)) {                                              \
            mop->snd = (s);                                             \
        } else if (NULL != (r)) {                                       \
            /* use a proxy so we can pass NULL into the macro */        \
            proxy = (r);                                                \
            /* create a send object for this message */                 \
            snd = OBJ_NEW(mca_oob_tcp_send_t);                          \
            mop->snd = snd;                                             \
            /* transfer and prep the header */                          \
            snd->hdr = proxy->hdr;                                      \
            MCA_OOB_TCP_HDR_HTON(&snd->hdr);                            \
            /* point to the data */                                     \
            snd->data = proxy->data;                                    \
            /* start the message with the header */                     \
            snd->sdptr = (char*)&snd->hdr;                              \
            snd->sdbytes = sizeof(mca_oob_tcp_hdr_t);                   \
            /* protect the data */                                      \
            proxy->data = NULL;                                         \
        }                                                               \
        mop->hop.jobid = (h)->jobid;                                    \
        mop->hop.vpid = (h)->vpid;                                      \
        /* this goes to the OOB framework, so use that event base */    \
        ORTE_THREADSHIFT(mop, orte_oob_base.ev_base,                    \
                         (cbfunc), ORTE_MSG_PRI);                       \
    } while(0)

#define ORTE_ACTIVATE_TCP_NO_ROUTE(r, h, c)                             \
    do {                                                                \
        mca_oob_tcp_msg_error_t *mop;                                   \
        opal_output_verbose(5, orte_oob_base_framework.framework_output, \
                            "%s:[%s:%d] post no route to %s",           \
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),         \
                            __FILE__, __LINE__,                         \
                            ORTE_NAME_PRINT((h)));                      \
        mop = OBJ_NEW(mca_oob_tcp_msg_error_t);                         \
        mop->rmsg = (r);                                                \
        mop->hop.jobid = (h)->jobid;                                    \
        mop->hop.vpid = (h)->vpid;                                      \
        /* this goes to the component, so use the framework             \
         * event base */                                                \
        ORTE_THREADSHIFT(mop, orte_oob_base.ev_base,                    \
                         (c), ORTE_MSG_PRI);                            \
    } while(0)

#endif /* _MCA_OOB_TCP_SENDRECV_H_ */
