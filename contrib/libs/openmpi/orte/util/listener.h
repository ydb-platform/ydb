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
 * Copyright (c) 2015      Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef ORTE_LISTENER_H
#define ORTE_LISTENER_H

#include "orte_config.h"

#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
#ifdef HAVE_SYS_SOCKET_H
#include <sys/socket.h>
#endif

#include "opal/class/opal_list.h"
#include "opal/mca/event/event.h"

/* callback prototype */
typedef void (*orte_listener_callback_fn_t)(int sd, short args, void *cbdata);

/*
 * Data structure for accepting connections.
 */
typedef struct orte_listener_t {
    opal_list_item_t item;
    int sd;
    opal_event_base_t *evbase;
    orte_listener_callback_fn_t handler;
} orte_listener_t;
OBJ_CLASS_DECLARATION(orte_listener_t);

typedef struct {
    opal_object_t super;
    opal_event_t ev;
    int fd;
    struct sockaddr_storage addr;
} orte_pending_connection_t;
OBJ_CLASS_DECLARATION(orte_pending_connection_t);

ORTE_DECLSPEC int orte_start_listening(void);
ORTE_DECLSPEC void orte_stop_listening(void);
ORTE_DECLSPEC int orte_register_listener(struct sockaddr* address, opal_socklen_t addrlen,
                                         opal_event_base_t *evbase,
                                         orte_listener_callback_fn_t handler);

#endif /* ORTE_LISTENER_H */
