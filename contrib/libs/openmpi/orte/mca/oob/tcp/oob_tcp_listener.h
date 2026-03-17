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
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef _MCA_OOB_TCP_LISTENER_H_
#define _MCA_OOB_TCP_LISTENER_H_

#include "orte_config.h"

#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
#ifdef HAVE_SYS_SOCKET_H
#include <sys/socket.h>
#endif

#include "opal/class/opal_list.h"
#include "opal/mca/event/event.h"

/*
 * Data structure for accepting connections.
 */
struct mca_oob_tcp_listener_t {
    opal_list_item_t item;
    bool ev_active;
    opal_event_t event;
    bool tcp6;
    int sd;
    uint16_t port;
};
typedef struct mca_oob_tcp_listener_t mca_oob_tcp_listener_t;
OBJ_CLASS_DECLARATION(mca_oob_tcp_listener_t);

typedef struct {
    opal_object_t super;
    opal_event_t ev;
    int fd;
    struct sockaddr_storage addr;
} mca_oob_tcp_pending_connection_t;
OBJ_CLASS_DECLARATION(mca_oob_tcp_pending_connection_t);

ORTE_MODULE_DECLSPEC int orte_oob_tcp_start_listening(void);

#endif /* _MCA_OOB_TCP_LISTENER_H_ */
