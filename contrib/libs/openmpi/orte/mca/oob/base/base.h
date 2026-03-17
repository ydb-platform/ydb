/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2012-2013 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2017      Intel, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/** @file:
 *
 * the oob framework
 */

#ifndef _MCA_OOB_BASE_H_
#define _MCA_OOB_BASE_H_

#include "orte_config.h"

#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef HAVE_SYS_UIO_H
#include <sys/uio.h>
#endif
#ifdef HAVE_NET_UIO_H
#include <net/uio.h>
#endif

#include "opal/class/opal_bitmap.h"
#include "opal/class/opal_hash_table.h"
#include "opal/class/opal_list.h"
#include "opal/util/timings.h"
#include "opal/mca/event/event.h"

#include "orte/mca/mca.h"
#include "orte/util/threads.h"

#include "orte/mca/oob/oob.h"

BEGIN_C_DECLS

/*
 * Convenience Typedef
 */
typedef struct {
    opal_event_base_t *ev_base;
    char *include;
    char *exclude;
    opal_list_t components;
    opal_list_t actives;
    int max_uri_length;
    opal_hash_table_t peers;
    int num_threads;
#if OPAL_ENABLE_TIMING
    bool timing;
#endif
} orte_oob_base_t;
ORTE_DECLSPEC extern orte_oob_base_t orte_oob_base;

typedef struct {
    opal_object_t super;
    mca_oob_base_component_t *component;
    opal_bitmap_t addressable;
} orte_oob_base_peer_t;
OBJ_CLASS_DECLARATION(orte_oob_base_peer_t);

/* MCA framework */
ORTE_DECLSPEC extern mca_base_framework_t orte_oob_base_framework;
ORTE_DECLSPEC int orte_oob_base_select(void);

/* Access the OOB internal functions via set of event-based macros
 * for inserting messages and other commands into the
 * OOB event base. This ensures that all OOB operations occur
 * asynchronously in a thread-safe environment.
 * Note that this doesn't mean that messages will be *sent*
 * in order as that depends on the specific transport being
 * used, when that module's event base indicates the transport
 * is available, etc.
 */
typedef struct {
    opal_object_t super;
    opal_event_t ev;
    orte_rml_send_t *msg;
} orte_oob_send_t;
OBJ_CLASS_DECLARATION(orte_oob_send_t);

/* All OOB sends are based on iovec's and are async as the RML
 * acts as the initial interface to prepare all communications.
 * The send_nb function will enter the message into the OOB
 * base, which will then check to see if a transport for the
 * intended target has already been assigned. If so, the message
 * is immediately placed into that module's event base for
 * transmission. If not, the function will loop across all available
 * components until one identifies that it has a module capable
 * of reaching the target.
 */
typedef void (*mca_oob_send_callback_fn_t)(int status,
                                           struct iovec *iov,
                                           int count, void *cbdata);

ORTE_DECLSPEC void orte_oob_base_send_nb(int fd, short args, void *cbdata);
#define ORTE_OOB_SEND(m)                                                \
    do {                                                                \
        orte_oob_send_t *cd;                                            \
        opal_output_verbose(1,                                          \
                            orte_oob_base_framework.framework_output,   \
                            "%s OOB_SEND: %s:%d",                       \
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),         \
                            __FILE__, __LINE__);                        \
        cd = OBJ_NEW(orte_oob_send_t);                                  \
        cd->msg = (m);                                                  \
        ORTE_THREADSHIFT(cd, orte_oob_base.ev_base,                     \
                         orte_oob_base_send_nb, ORTE_MSG_PRI);          \
    }while(0)

/* During initial wireup, we can only transfer contact info on the daemon
 * command line. This limits what we can send to a string representation of
 * the actual contact info, which gets sent in a uri-like form. Not every
 * oob module can support this transaction, so this function will loop
 * across all oob components/modules, letting each add to the uri string if
 * it supports bootstrap operations. An error will be returned in the cbfunc
 * if NO component can successfully provide a contact.
 *
 * Note: since there is a limit to what an OS will allow on a cmd line, we
 * impose a limit on the length of the resulting uri via an MCA param. The
 * default value of -1 implies unlimited - however, users with large numbers
 * of interfaces on their nodes may wish to restrict the size.
 *
 * Since all components define their address info at component start,
 * it is unchanged and does not require acess via event
 */
ORTE_DECLSPEC void orte_oob_base_get_addr(char **uri);

/* Get the available transports and their attributes */
#define ORTE_OOB_GET_TRANSPORTS(u) orte_oob_base_get_transports(u)
ORTE_DECLSPEC void orte_oob_base_get_transports(opal_list_t *transports);


#if OPAL_ENABLE_FT_CR == 1
ORTE_DECLSPEC void orte_oob_base_ft_event(int fd, short args, void *cbdata);
#endif

END_C_DECLS
#endif
