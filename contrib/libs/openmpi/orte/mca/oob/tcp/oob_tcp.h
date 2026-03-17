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

#ifndef _MCA_OOB_TCP_H_
#define _MCA_OOB_TCP_H_

#include "orte_config.h"

#include "orte/types.h"

#include "opal/mca/base/base.h"
#include "opal/class/opal_free_list.h"
#include "opal/class/opal_hash_table.h"
#include "opal/mca/event/event.h"

#include "orte/mca/oob/oob.h"
#include "orte/mca/oob/base/base.h"


BEGIN_C_DECLS

/* define some debug levels */
#define OOB_TCP_DEBUG_FAIL      2
#define OOB_TCP_DEBUG_CONNECT   7

/* forward declare a couple of structures */
struct mca_oob_tcp_module_t;
struct mca_oob_tcp_msg_error_t;

/* define a struct for tracking NIC addresses */
typedef struct {
    opal_list_item_t super;
    uint16_t af_family;
    struct sockaddr addr;
} mca_oob_tcp_nicaddr_t;
OBJ_CLASS_DECLARATION(mca_oob_tcp_nicaddr_t);

/* Module definition */
typedef void (*mca_oob_tcp_module_accept_connection_fn_t)(const int accepted_fd,
                                                          const struct sockaddr *addr);
typedef void (*mca_oob_tcp_module_ping_fn_t)(const orte_process_name_t *proc);
typedef void (*mca_oob_tcp_module_send_nb_fn_t)(orte_rml_send_t *msg);
typedef void (*mca_oob_tcp_module_ft_event_fn_t)(int state);

typedef struct {
    mca_oob_tcp_module_accept_connection_fn_t  accept_connection;
    mca_oob_tcp_module_ping_fn_t               ping;
    mca_oob_tcp_module_send_nb_fn_t            send_nb;
    mca_oob_tcp_module_ft_event_fn_t           ft_event;
} mca_oob_tcp_module_t;
ORTE_MODULE_DECLSPEC extern mca_oob_tcp_module_t mca_oob_tcp_module;

/**
 * the state of the connection
 */
typedef enum {
    MCA_OOB_TCP_UNCONNECTED,
    MCA_OOB_TCP_CLOSED,
    MCA_OOB_TCP_RESOLVE,
    MCA_OOB_TCP_CONNECTING,
    MCA_OOB_TCP_CONNECT_ACK,
    MCA_OOB_TCP_CONNECTED,
    MCA_OOB_TCP_FAILED,
    MCA_OOB_TCP_ACCEPTING
} mca_oob_tcp_state_t;

/* module-level shared functions */
ORTE_MODULE_DECLSPEC void mca_oob_tcp_send_handler(int fd, short args, void *cbdata);
ORTE_MODULE_DECLSPEC void mca_oob_tcp_recv_handler(int fd, short args, void *cbdata);
ORTE_MODULE_DECLSPEC void mca_oob_tcp_queue_msg(int sd, short args, void *cbdata);


END_C_DECLS

#endif /* MCA_OOB_TCP_H_ */
