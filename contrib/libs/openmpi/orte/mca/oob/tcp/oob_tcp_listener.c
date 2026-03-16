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
 * Copyright (c) 2009-2015 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2011      Oak Ridge National Labs.  All rights reserved.
 * Copyright (c) 2013-2017 Intel, Inc.  All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 * In windows, many of the socket functions return an EWOULDBLOCK
 * instead of things like EAGAIN, EINPROGRESS, etc. It has been
 * verified that this will not conflict with other error codes that
 * are returned by these functions under UNIX/Linux environments
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

#include "opal/util/show_help.h"
#include "opal/util/error.h"
#include "opal/util/output.h"
#include "opal/opal_socket_errno.h"
#include "opal/util/if.h"
#include "opal/util/net.h"
#include "opal/util/argv.h"
#include "opal/util/fd.h"
#include "opal/class/opal_hash_table.h"
#include "opal/class/opal_list.h"

#include "orte/mca/errmgr/errmgr.h"
#include "orte/mca/ess/ess.h"
#include "orte/util/name_fns.h"
#include "orte/util/parse_options.h"
#include "orte/util/show_help.h"
#include "orte/util/threads.h"
#include "orte/runtime/orte_globals.h"

#include "orte/mca/oob/tcp/oob_tcp.h"
#include "orte/mca/oob/tcp/oob_tcp_component.h"
#include "orte/mca/oob/tcp/oob_tcp_peer.h"
#include "orte/mca/oob/tcp/oob_tcp_connection.h"
#include "orte/mca/oob/tcp/oob_tcp_listener.h"
#include "orte/mca/oob/tcp/oob_tcp_common.h"

static void connection_event_handler(int incoming_sd, short flags, void* cbdata);
static void* listen_thread(opal_object_t *obj);
static int create_listen(void);
#if OPAL_ENABLE_IPV6
static int create_listen6(void);
#endif
static void connection_handler(int sd, short flags, void* cbdata);
static void connection_event_handler(int sd, short flags, void* cbdata);

/*
 * Component initialization - create a module for each available
 * TCP interface and initialize the static resources associated
 * with that module.
 *
 * Also initializes the list of devices that will be used/supported by
 * the module, using the if_include and if_exclude variables.  This is
 * the only place that this sorting should occur -- all other places
 * should use the tcp_avaiable_devices list.  This is a change from
 * previous versions of this component.
 */
int orte_oob_tcp_start_listening(void)
{
    int rc;
    mca_oob_tcp_listener_t *listener;

    /* if we don't have any TCP interfaces, we shouldn't be here */
    if (NULL == mca_oob_tcp_component.ipv4conns
#if OPAL_ENABLE_IPV6
        && NULL == mca_oob_tcp_component.ipv6conns
#endif
        ) {
        ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
        return ORTE_ERR_NOT_FOUND;
    }

    /* create listen socket(s) for incoming connection attempts */
    if (ORTE_SUCCESS != (rc = create_listen())) {
        ORTE_ERROR_LOG(rc);
        return rc;
    }

#if OPAL_ENABLE_IPV6
    /* create listen socket(s) for incoming connection attempts */
    if (ORTE_SUCCESS != (rc = create_listen6())) {
        ORTE_ERROR_LOG(rc);
        return rc;
    }
#endif

    /* if I am the HNP, start a listening thread so we can
     * harvest connection requests as rapidly as possible
     */
    if (ORTE_PROC_IS_HNP) {
        if (0 > pipe(mca_oob_tcp_component.stop_thread)) {
            ORTE_ERROR_LOG(ORTE_ERR_OUT_OF_RESOURCE);
            return ORTE_ERR_OUT_OF_RESOURCE;
        }

        /* Make sure the pipe FDs are set to close-on-exec so that
           they don't leak into children */
        if (opal_fd_set_cloexec(mca_oob_tcp_component.stop_thread[0]) != OPAL_SUCCESS ||
            opal_fd_set_cloexec(mca_oob_tcp_component.stop_thread[1]) != OPAL_SUCCESS) {
            close(mca_oob_tcp_component.stop_thread[0]);
            close(mca_oob_tcp_component.stop_thread[1]);
            ORTE_ERROR_LOG(ORTE_ERR_IN_ERRNO);
            return ORTE_ERR_IN_ERRNO;
        }

        mca_oob_tcp_component.listen_thread_active = true;
        mca_oob_tcp_component.listen_thread.t_run = listen_thread;
        mca_oob_tcp_component.listen_thread.t_arg = NULL;
        if (OPAL_SUCCESS != (rc = opal_thread_start(&mca_oob_tcp_component.listen_thread))) {
            ORTE_ERROR_LOG(rc);
            opal_output(0, "%s Unable to start listen thread", ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
        }
        return rc;
    }

    /* otherwise, setup to listen via the event lib */
    OPAL_LIST_FOREACH(listener, &mca_oob_tcp_component.listeners, mca_oob_tcp_listener_t) {
        listener->ev_active = true;
        opal_event_set(orte_oob_base.ev_base, &listener->event,
                       listener->sd,
                       OPAL_EV_READ|OPAL_EV_PERSIST,
                       connection_event_handler,
                       0);
        opal_event_set_priority(&listener->event, ORTE_MSG_PRI);
        ORTE_POST_OBJECT(listener);
        opal_event_add(&listener->event, 0);
    }

    return ORTE_SUCCESS;
}

/*
 * Create an IPv4 listen socket and bind to all interfaces.
 *
 * At one time, this also registered a callback with the event library
 * for when connections were received on the listen socket.  This is
 * no longer the case -- the caller must register any events required.
 *
 * Called by both the threaded and event based listen modes.
 */
static int create_listen(void)
{
    int flags, i;
    uint16_t port=0;
    struct sockaddr_storage inaddr;
    opal_socklen_t addrlen;
    char **ports=NULL;
    int sd = -1;
    char *tconn;
    mca_oob_tcp_listener_t *conn;

    /* If an explicit range of ports was given, find the first open
     * port in the range.  Otherwise, tcp_port_min will be 0, which
     * means "pick any port"
     */
    if (ORTE_PROC_IS_DAEMON) {
        if (NULL != mca_oob_tcp_component.tcp_static_ports) {
            /* if static ports were provided, take the
             * first entry in the list
             */
            opal_argv_append_nosize(&ports, mca_oob_tcp_component.tcp_static_ports[0]);
            /* flag that we are using static ports */
            orte_static_ports = true;
        } else if (NULL != mca_oob_tcp_component.tcp_dyn_ports) {
            /* take the entire range */
            ports = opal_argv_copy(mca_oob_tcp_component.tcp_dyn_ports);
            orte_static_ports = false;
        } else {
            /* flag the system to dynamically take any available port */
            opal_argv_append_nosize(&ports, "0");
            orte_static_ports = false;
        }
    } else if (ORTE_PROC_IS_HNP) {
        if (NULL != mca_oob_tcp_component.tcp_static_ports) {
            /* if static ports were provided, take the
             * first entry in the list
             */
            opal_argv_append_nosize(&ports, mca_oob_tcp_component.tcp_static_ports[0]);
            /* flag that we are using static ports */
            orte_static_ports = true;
        } else if (NULL != mca_oob_tcp_component.tcp_dyn_ports) {
            /* take the entire range */
            ports = opal_argv_copy(mca_oob_tcp_component.tcp_dyn_ports);
            orte_static_ports = false;
        } else {
            /* flag the system to dynamically take any available port */
            opal_argv_append_nosize(&ports, "0");
            orte_static_ports = false;
        }
    } else if (ORTE_PROC_IS_MPI) {
        if (NULL != mca_oob_tcp_component.tcp_static_ports) {
            /* if static ports were provided, an mpi proc takes its
             * node_local_rank entry in the list IF it has that info
             * AND enough ports were provided - otherwise, we "pick any port"
             */
            orte_node_rank_t nrank;
            /* do I know my node_local_rank yet? */
            if (ORTE_NODE_RANK_INVALID != (nrank = orte_process_info.my_node_rank) &&
                (nrank+1) < opal_argv_count(mca_oob_tcp_component.tcp_static_ports)) {
                /* any daemon takes the first entry, so we start with the second */
                opal_argv_append_nosize(&ports, mca_oob_tcp_component.tcp_static_ports[nrank+1]);
                /* flag that we are using static ports */
                orte_static_ports = true;
            } else {
                /* flag the system to dynamically take any available port */
                opal_argv_append_nosize(&ports, "0");
                orte_static_ports = false;
            }
        } else if (NULL != mca_oob_tcp_component.tcp_dyn_ports) {
            /* take the entire range */
            ports = opal_argv_copy(mca_oob_tcp_component.tcp_dyn_ports);
            orte_static_ports = false;
        } else {
            /* flag the system to dynamically take any available port */
            opal_argv_append_nosize(&ports, "0");
            orte_static_ports = false;
        }
    } else {
        /* if we are a tool, then we must let the
         * system pick any port
         */
        opal_argv_append_nosize(&ports, "0");
        /* if static ports were specified, flag it
         * so the HNP does the right thing
         */
        if (NULL != mca_oob_tcp_component.tcp_static_ports) {
            orte_static_ports = true;
        } else {
            orte_static_ports = false;
        }
    }

    /* bozo check - this should be impossible, but... */
    if (NULL == ports) {
        return ORTE_ERROR;
    }

    /* get the address info for this interface */
    memset(&inaddr, 0, sizeof(inaddr));
    ((struct sockaddr_in*) &inaddr)->sin_family = AF_INET;
    ((struct sockaddr_in*) &inaddr)->sin_addr.s_addr = INADDR_ANY;
    addrlen = sizeof(struct sockaddr_in);

    /* loop across all the specified ports, establishing a socket
     * for each one - note that application procs will ONLY have
     * one socket, but that orterun and daemons will have multiple
     * sockets to support more flexible wireup protocols
     */
    for (i=0; i < opal_argv_count(ports); i++) {
        opal_output_verbose(5, orte_oob_base_framework.framework_output,
                            "%s attempting to bind to IPv4 port %s",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                            ports[i]);
        /* get the port number */
        port = strtol(ports[i], NULL, 10);
        /* convert it to network-byte-order */
        port = htons(port);

        ((struct sockaddr_in*) &inaddr)->sin_port = port;

        /* create a listen socket for incoming connections on this port */
        sd = socket(AF_INET, SOCK_STREAM, 0);
        if (sd < 0) {
            if (EAFNOSUPPORT != opal_socket_errno) {
                opal_output(0,"mca_oob_tcp_component_init: socket() failed: %s (%d)",
                            strerror(opal_socket_errno), opal_socket_errno);
            }
            opal_argv_free(ports);
            return ORTE_ERR_IN_ERRNO;
        }

        /* Enable/disable reusing ports */
        if (orte_static_ports) {
            flags = 1;
        } else {
            flags = 0;
        }
        if (setsockopt (sd, SOL_SOCKET, SO_REUSEADDR, (const char *)&flags, sizeof(flags)) < 0) {
            opal_output(0, "mca_oob_tcp_create_listen: unable to set the "
                        "SO_REUSEADDR option (%s:%d)\n",
                        strerror(opal_socket_errno), opal_socket_errno);
            CLOSE_THE_SOCKET(sd);
            opal_argv_free(ports);
            return ORTE_ERROR;
        }

        /* Set the socket to close-on-exec so that no children inherit
           this FD */
        if (opal_fd_set_cloexec(sd) != OPAL_SUCCESS) {
            opal_output(0, "mca_oob_tcp_create_listen: unable to set the "
                        "listening socket to CLOEXEC (%s:%d)\n",
                        strerror(opal_socket_errno), opal_socket_errno);
            CLOSE_THE_SOCKET(sd);
            opal_argv_free(ports);
            return ORTE_ERROR;
        }

        if (bind(sd, (struct sockaddr*)&inaddr, addrlen) < 0) {
            if( (EADDRINUSE == opal_socket_errno) || (EADDRNOTAVAIL == opal_socket_errno) ) {
                continue;
            }
            opal_output(0, "%s bind() failed for port %d: %s (%d)",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                        (int)ntohs(port),
                        strerror(opal_socket_errno),
                        opal_socket_errno );
            CLOSE_THE_SOCKET(sd);
            opal_argv_free(ports);
            return ORTE_ERROR;
        }
        /* resolve assigned port */
        if (getsockname(sd, (struct sockaddr*)&inaddr, &addrlen) < 0) {
            opal_output(0, "mca_oob_tcp_create_listen: getsockname(): %s (%d)",
                        strerror(opal_socket_errno), opal_socket_errno);
            CLOSE_THE_SOCKET(sd);
            opal_argv_free(ports);
            return ORTE_ERROR;
        }

        /* setup listen backlog to maximum allowed by kernel */
        if (listen(sd, SOMAXCONN) < 0) {
            opal_output(0, "mca_oob_tcp_component_init: listen(): %s (%d)",
                        strerror(opal_socket_errno), opal_socket_errno);
            CLOSE_THE_SOCKET(sd);
            opal_argv_free(ports);
            return ORTE_ERROR;
        }

        /* set socket up to be non-blocking, otherwise accept could block */
        if ((flags = fcntl(sd, F_GETFL, 0)) < 0) {
            opal_output(0, "mca_oob_tcp_component_init: fcntl(F_GETFL) failed: %s (%d)",
                        strerror(opal_socket_errno), opal_socket_errno);
            CLOSE_THE_SOCKET(sd);
            opal_argv_free(ports);
            return ORTE_ERROR;
        }
        flags |= O_NONBLOCK;
        if (fcntl(sd, F_SETFL, flags) < 0) {
            opal_output(0, "mca_oob_tcp_component_init: fcntl(F_SETFL) failed: %s (%d)",
                        strerror(opal_socket_errno), opal_socket_errno);
            CLOSE_THE_SOCKET(sd);
            opal_argv_free(ports);
            return ORTE_ERROR;
        }

        /* add this port to our connections */
        conn = OBJ_NEW(mca_oob_tcp_listener_t);
        conn->sd = sd;
        conn->port = ntohs(((struct sockaddr_in*) &inaddr)->sin_port);
        if (0 == orte_process_info.my_port) {
            /* save the first one */
            orte_process_info.my_port = conn->port;
        }
        opal_list_append(&mca_oob_tcp_component.listeners, &conn->item);
        /* and to our ports */
        asprintf(&tconn, "%d", ntohs(((struct sockaddr_in*) &inaddr)->sin_port));
        opal_argv_append_nosize(&mca_oob_tcp_component.ipv4ports, tconn);
        free(tconn);
        if (OOB_TCP_DEBUG_CONNECT <= opal_output_get_verbosity(orte_oob_base_framework.framework_output)) {
            port = ntohs(((struct sockaddr_in*) &inaddr)->sin_port);
            opal_output(0, "%s assigned IPv4 port %d",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), port);
        }

        if (!ORTE_PROC_IS_HNP) {
            /* only the HNP binds to multiple ports */
            break;
        }
    }
    /* done with this, so release it */
    opal_argv_free(ports);

    if (0 == opal_list_get_size(&mca_oob_tcp_component.listeners)) {
        /* cleanup */
        if (0 <= sd) {
            CLOSE_THE_SOCKET(sd);
        }
        return ORTE_ERR_SOCKET_NOT_AVAILABLE;
    }

    return ORTE_SUCCESS;
}

#if OPAL_ENABLE_IPV6
/*
 * Create an IPv6 listen socket and bind to all interfaces.
 *
 * At one time, this also registered a callback with the event library
 * for when connections were received on the listen socket.  This is
 * no longer the case -- the caller must register any events required.
 *
 * Called by both the threaded and event based listen modes.
 */
static int create_listen6(void)
{
    int flags, i;
    uint16_t port=0;
    struct sockaddr_storage inaddr;
    opal_socklen_t addrlen;
    char **ports=NULL;
    int sd;
    char *tconn;
    mca_oob_tcp_listener_t *conn;

    /* If an explicit range of ports was given, find the first open
     * port in the range.  Otherwise, tcp_port_min will be 0, which
     * means "pick any port"
     */
    if (ORTE_PROC_IS_DAEMON) {
        if (NULL != mca_oob_tcp_component.tcp6_static_ports) {
            /* if static ports were provided, take the
             * first entry in the list
             */
            opal_argv_append_nosize(&ports, mca_oob_tcp_component.tcp6_static_ports[0]);
            /* flag that we are using static ports */
            orte_static_ports = true;
        } else if (NULL != mca_oob_tcp_component.tcp6_dyn_ports) {
            /* take the entire range */
            ports = opal_argv_copy(mca_oob_tcp_component.tcp6_dyn_ports);
            orte_static_ports = false;
        } else {
            /* flag the system to dynamically take any available port */
            opal_argv_append_nosize(&ports, "0");
            orte_static_ports = false;
        }
    } else if (ORTE_PROC_IS_HNP) {
        if (NULL != mca_oob_tcp_component.tcp6_static_ports) {
            /* if static ports were provided, take the
             * first entry in the list
             */
            opal_argv_append_nosize(&ports, mca_oob_tcp_component.tcp6_static_ports[0]);
            /* flag that we are using static ports */
            orte_static_ports = true;
        } else if (NULL != mca_oob_tcp_component.tcp6_dyn_ports) {
            /* take the entire range */
            ports = opal_argv_copy(mca_oob_tcp_component.tcp6_dyn_ports);
            orte_static_ports = false;
        } else {
            /* flag the system to dynamically take any available port */
            opal_argv_append_nosize(&ports, "0");
            orte_static_ports = false;
        }
    } else if (ORTE_PROC_IS_MPI) {
        if (NULL != mca_oob_tcp_component.tcp6_static_ports) {
            /* if static ports were provided, an mpi proc takes its
             * node_local_rank entry in the list IF it has that info
             * AND enough ports were provided - otherwise, we "pick any port"
             */
            orte_node_rank_t nrank;
            /* do I know my node_local_rank yet? */
            if (ORTE_NODE_RANK_INVALID != (nrank = orte_process_info.my_node_rank) &&
                (nrank+1) < opal_argv_count(mca_oob_tcp_component.tcp6_static_ports)) {
                /* any daemon takes the first entry, so we start with the second */
                opal_argv_append_nosize(&ports, mca_oob_tcp_component.tcp6_static_ports[nrank+1]);
                /* flag that we are using static ports */
                orte_static_ports = true;
            } else {
                /* flag the system to dynamically take any available port */
                opal_argv_append_nosize(&ports, "0");
                orte_static_ports = false;
            }
        } else if (NULL != mca_oob_tcp_component.tcp6_dyn_ports) {
            /* take the entire range */
            ports = opal_argv_copy(mca_oob_tcp_component.tcp6_dyn_ports);
            orte_static_ports = false;
        } else {
            /* flag the system to dynamically take any available port */
            opal_argv_append_nosize(&ports, "0");
            orte_static_ports = false;
        }
    } else {
        /* if we are a tool, then we must let the
         * system pick any port
         */
        opal_argv_append_nosize(&ports, "0");
        /* if static ports were specified, flag it
         * so the HNP does the right thing
         */
        if (NULL != mca_oob_tcp_component.tcp6_static_ports) {
            orte_static_ports = true;
        } else {
            orte_static_ports = false;
        }
    }

    /* bozo check - this should be impossible, but... */
    if (NULL == ports) {
        return ORTE_ERROR;
    }

    /* get the address info for this interface */
    memset(&inaddr, 0, sizeof(inaddr));
    ((struct sockaddr_in6*) &inaddr)->sin6_family = AF_INET6;
    ((struct sockaddr_in6*) &inaddr)->sin6_addr = in6addr_any;
    addrlen = sizeof(struct sockaddr_in6);

    /* loop across all the specified ports, establishing a socket
     * for each one - note that application procs will ONLY have
     * one socket, but that orterun and daemons will have multiple
     * sockets to support more flexible wireup protocols
     */
    for (i=0; i < opal_argv_count(ports); i++) {
        opal_output_verbose(5, orte_oob_base_framework.framework_output,
                            "%s attempting to bind to IPv6 port %s",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                            ports[i]);
        /* get the port number */
        port = strtol(ports[i], NULL, 10);
        /* convert it to network-byte-order */
        port = htons(port);

        ((struct sockaddr_in6*) &inaddr)->sin6_port = port;

        /* create a listen socket for incoming connections on this port */
        sd = socket(AF_INET6, SOCK_STREAM, 0);
        if (sd < 0) {
            if (EAFNOSUPPORT != opal_socket_errno) {
                opal_output(0,"mca_oob_tcp_component_init: socket() failed: %s (%d)",
                            strerror(opal_socket_errno), opal_socket_errno);
            }
            return ORTE_ERR_IN_ERRNO;
        }
        /* Set the socket to close-on-exec so that no children inherit
           this FD */
        if (opal_fd_set_cloexec(sd) != OPAL_SUCCESS) {
            opal_output(0, "mca_oob_tcp_create_listen6: unable to set the "
                        "listening socket to CLOEXEC (%s:%d)\n",
                        strerror(opal_socket_errno), opal_socket_errno);
            CLOSE_THE_SOCKET(sd);
            opal_argv_free(ports);
            return ORTE_ERROR;
        }

        /* Enable/disable reusing ports */
        if (orte_static_ports) {
            flags = 1;
        } else {
            flags = 0;
        }
        if (setsockopt (sd, SOL_SOCKET, SO_REUSEADDR, (const char *)&flags, sizeof(flags)) < 0) {
            opal_output(0, "mca_oob_tcp_create_listen: unable to set the "
                        "SO_REUSEADDR option (%s:%d)\n",
                        strerror(opal_socket_errno), opal_socket_errno);
            CLOSE_THE_SOCKET(sd);
            opal_argv_free(ports);
            return ORTE_ERROR;
        }

        if (bind(sd, (struct sockaddr*)&inaddr, addrlen) < 0) {
            if( (EADDRINUSE == opal_socket_errno) || (EADDRNOTAVAIL == opal_socket_errno) ) {
                continue;
            }
            opal_output(0, "%s bind() failed for port %d: %s (%d)",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                        (int)ntohs(port),
                        strerror(opal_socket_errno),
                        opal_socket_errno );
            CLOSE_THE_SOCKET(sd);
            opal_argv_free(ports);
            return ORTE_ERROR;
        }
        /* resolve assigned port */
        if (getsockname(sd, (struct sockaddr*)&inaddr, &addrlen) < 0) {
            opal_output(0, "mca_oob_tcp_create_listen: getsockname(): %s (%d)",
                        strerror(opal_socket_errno), opal_socket_errno);
            CLOSE_THE_SOCKET(sd);
            return ORTE_ERROR;
        }

        /* setup listen backlog to maximum allowed by kernel */
        if (listen(sd, SOMAXCONN) < 0) {
            opal_output(0, "mca_oob_tcp_component_init: listen(): %s (%d)",
                        strerror(opal_socket_errno), opal_socket_errno);
            return ORTE_ERROR;
        }

        /* set socket up to be non-blocking, otherwise accept could block */
        if ((flags = fcntl(sd, F_GETFL, 0)) < 0) {
            opal_output(0, "mca_oob_tcp_component_init: fcntl(F_GETFL) failed: %s (%d)",
                        strerror(opal_socket_errno), opal_socket_errno);
            return ORTE_ERROR;
        }
        flags |= O_NONBLOCK;
        if (fcntl(sd, F_SETFL, flags) < 0) {
            opal_output(0, "mca_oob_tcp_component_init: fcntl(F_SETFL) failed: %s (%d)",
                        strerror(opal_socket_errno), opal_socket_errno);
            return ORTE_ERROR;
        }

        /* add this port to our connections */
        conn = OBJ_NEW(mca_oob_tcp_listener_t);
        conn->tcp6 = true;
        conn->sd = sd;
        conn->port = ntohs(((struct sockaddr_in6*) &inaddr)->sin6_port);
        opal_list_append(&mca_oob_tcp_component.listeners, &conn->item);
        /* and to our ports */
        asprintf(&tconn, "%d", ntohs(((struct sockaddr_in6*) &inaddr)->sin6_port));
        opal_argv_append_nosize(&mca_oob_tcp_component.ipv6ports, tconn);
        free(tconn);
        if (OOB_TCP_DEBUG_CONNECT <= opal_output_get_verbosity(orte_oob_base_framework.framework_output)) {
            opal_output(0, "%s assigned IPv6 port %d",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                        (int)ntohs(((struct sockaddr_in6*) &inaddr)->sin6_port));
        }

        if (!ORTE_PROC_IS_HNP) {
            /* only the HNP binds to multiple ports */
            break;
        }
    }
    if (0 == opal_list_get_size(&mca_oob_tcp_component.listeners)) {
        /* cleanup */
        CLOSE_THE_SOCKET(sd);
        opal_argv_free(ports);
        return ORTE_ERR_SOCKET_NOT_AVAILABLE;
    }

    /* done with this, so release it */
    opal_argv_free(ports);

    return ORTE_SUCCESS;
}
#endif

/*
 * The listen thread created when listen_mode is threaded.  Accepts
 * incoming connections and places them in a queue for further
 * processing
 *
 * Runs until mca_oob_tcp_compnent.shutdown is set to true.
 */
static void* listen_thread(opal_object_t *obj)
{
    int rc, max, accepted_connections, sd;
    opal_socklen_t addrlen = sizeof(struct sockaddr_storage);
    mca_oob_tcp_pending_connection_t *pending_connection;
    struct timeval timeout;
    fd_set readfds;
    mca_oob_tcp_listener_t *listener;

    /* only execute during the initial VM startup stage - once
     * all the initial daemons have reported in, we will revert
     * to the event method for handling any further connections
     * so as to minimize overhead
     */
    while (mca_oob_tcp_component.listen_thread_active) {
        FD_ZERO(&readfds);
        max = -1;
        OPAL_LIST_FOREACH(listener, &mca_oob_tcp_component.listeners, mca_oob_tcp_listener_t) {
            FD_SET(listener->sd, &readfds);
            max = (listener->sd > max) ? listener->sd : max;
        }
        /* add the stop_thread fd */
        FD_SET(mca_oob_tcp_component.stop_thread[0], &readfds);
        max = (mca_oob_tcp_component.stop_thread[0] > max) ? mca_oob_tcp_component.stop_thread[0] : max;

        /* set timeout interval */
        timeout.tv_sec = mca_oob_tcp_component.listen_thread_tv.tv_sec;
        timeout.tv_usec = mca_oob_tcp_component.listen_thread_tv.tv_usec;

        /* Block in a select to avoid hammering the cpu.  If a connection
         * comes in, we'll get woken up right away.
         */
        rc = select(max + 1, &readfds, NULL, NULL, &timeout);
        if (!mca_oob_tcp_component.listen_thread_active) {
            /* we've been asked to terminate */
            close(mca_oob_tcp_component.stop_thread[0]);
            close(mca_oob_tcp_component.stop_thread[1]);
            return NULL;
        }
        if (rc < 0) {
            if (EAGAIN != opal_socket_errno && EINTR != opal_socket_errno) {
                perror("select");
            }
            continue;
        }

        /* Spin accepting connections until all active listen sockets
         * do not have any incoming connections, pushing each connection
         * onto the event queue for processing
         */
        do {
            accepted_connections = 0;
            OPAL_LIST_FOREACH(listener, &mca_oob_tcp_component.listeners, mca_oob_tcp_listener_t) {
                sd = listener->sd;

                /* according to the man pages, select replaces the given descriptor
                 * set with a subset consisting of those descriptors that are ready
                 * for the specified operation - in this case, a read. So we need to
                 * first check to see if this file descriptor is included in the
                 * returned subset
                 */
                if (0 == FD_ISSET(sd, &readfds)) {
                    /* this descriptor is not included */
                    continue;
                }

                /* this descriptor is ready to be read, which means a connection
                 * request has been received - so harvest it. All we want to do
                 * here is accept the connection and push the info onto the event
                 * library for subsequent processing - we don't want to actually
                 * process the connection here as it takes too long, and so the
                 * OS might start rejecting connections due to timeout.
                 */
                pending_connection = OBJ_NEW(mca_oob_tcp_pending_connection_t);
                opal_event_set(orte_oob_base.ev_base, &pending_connection->ev, -1,
                               OPAL_EV_WRITE, connection_handler, pending_connection);
                opal_event_set_priority(&pending_connection->ev, ORTE_MSG_PRI);
                pending_connection->fd = accept(sd,
                                                (struct sockaddr*)&(pending_connection->addr),
                                                &addrlen);

                /* check for < 0 as indicating an error upon accept */
                if (pending_connection->fd < 0) {
                    OBJ_RELEASE(pending_connection);

                    /* Non-fatal errors */
                    if (EAGAIN == opal_socket_errno ||
                        EWOULDBLOCK == opal_socket_errno) {
                        continue;
                    }

                    /* If we run out of file descriptors, log an extra
                       warning (so that the user can know to fix this
                       problem) and abandon all hope. */
                    else if (EMFILE == opal_socket_errno) {
                        CLOSE_THE_SOCKET(sd);
                        ORTE_ERROR_LOG(ORTE_ERR_SYS_LIMITS_SOCKETS);
                        orte_show_help("help-oob-tcp.txt",
                                       "accept failed",
                                       true,
                                       opal_process_info.nodename,
                                       opal_socket_errno,
                                       strerror(opal_socket_errno),
                                       "Out of file descriptors");
                        goto done;
                    }

                    /* For all other cases, print a
                       warning but try to continue */
                    else {
                        orte_show_help("help-oob-tcp.txt",
                                       "accept failed",
                                       true,
                                       opal_process_info.nodename,
                                       opal_socket_errno,
                                       strerror(opal_socket_errno),
                                       "Unknown cause; job will try to continue");
                        continue;
                    }
                }

                opal_output_verbose(OOB_TCP_DEBUG_CONNECT, orte_oob_base_framework.framework_output,
                                    "%s mca_oob_tcp_listen_thread: incoming connection: "
                                    "(%d, %d) %s:%d\n",
                                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                    pending_connection->fd, opal_socket_errno,
                                    opal_net_get_hostname((struct sockaddr*) &pending_connection->addr),
                                    opal_net_get_port((struct sockaddr*) &pending_connection->addr));

                /* if we are on a privileged port, we only accept connections
                 * from other privileged sockets. A privileged port is one
                 * whose port is less than 1024 on Linux, so we'll check for that. */
                if (1024 >= listener->port) {
                    uint16_t inport;
                    inport = opal_net_get_port((struct sockaddr*) &pending_connection->addr);
                    if (1024 < inport) {
                        /* someone tried to cross-connect privileges,
                         * say something */
                        orte_show_help("help-oob-tcp.txt",
                                       "privilege failure", true,
                                       opal_process_info.nodename, listener->port,
                                       opal_net_get_hostname((struct sockaddr*) &pending_connection->addr),
                                       inport);
                        CLOSE_THE_SOCKET(pending_connection->fd);
                        OBJ_RELEASE(pending_connection);
                        continue;
                    }
                }

                /* activate the event */
                ORTE_POST_OBJECT(pending_connection);
                opal_event_active(&pending_connection->ev, OPAL_EV_WRITE, 1);
                accepted_connections++;
            }
        } while (accepted_connections > 0);
    }

 done:
#if 0
    /* once we complete the initial launch, the "flood" of connections
     * will end - only connection requests from local procs, connect/accept
     * operations across mpirun instances, or the occasional tool will need
     * to be serviced. As these are relatively small events, we can easily
     * handle them in the context of the event library and no longer require
     * a separate connection harvesting thread. So switch over to the event
     * lib handler now
     */
    opal_output_verbose(OOB_TCP_DEBUG_CONNECT, orte_oob_base_framework.framework_output,
                        "%s mca_oob_tcp_listen_thread: switching to event lib",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
    /* setup to listen via event library */
    OPAL_LIST_FOREACH(listener, &mca_oob_tcp_component.listeners, mca_oob_tcp_listener_t) {
        opal_event_set(orte_event_base, listener->event,
                   listener->sd,
                   OPAL_EV_READ|OPAL_EV_PERSIST,
                   connection_event_handler,
                   0);
        opal_event_set_priority(listener->event, ORTE_MSG_PRI);
        opal_event_add(listener->event, 0);
    }
#endif
    return NULL;
}

/*
 * Handler for accepting connections from the listen thread
 */
static void connection_handler(int sd, short flags, void* cbdata)
{
    mca_oob_tcp_pending_connection_t *new_connection;

    new_connection = (mca_oob_tcp_pending_connection_t*)cbdata;

    ORTE_ACQUIRE_OBJECT(new_connection);

    opal_output_verbose(4, orte_oob_base_framework.framework_output,
                        "%s connection_handler: working connection "
                        "(%d, %d) %s:%d\n",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                        new_connection->fd, opal_socket_errno,
                        opal_net_get_hostname((struct sockaddr*) &new_connection->addr),
                        opal_net_get_port((struct sockaddr*) &new_connection->addr));

    /* process the connection */
    mca_oob_tcp_module.accept_connection(new_connection->fd,
                                         (struct sockaddr*) &(new_connection->addr));
    /* cleanup */
    OBJ_RELEASE(new_connection);
}

/*
 * Handler for accepting connections from the event library
 */
static void connection_event_handler(int incoming_sd, short flags, void* cbdata)
{
    struct sockaddr addr;
    opal_socklen_t addrlen = sizeof(struct sockaddr);
    int sd;

    sd = accept(incoming_sd, (struct sockaddr*)&addr, &addrlen);
    opal_output_verbose(OOB_TCP_DEBUG_CONNECT, orte_oob_base_framework.framework_output,
                        "%s connection_event_handler: working connection "
                        "(%d, %d) %s:%d\n",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                        sd, opal_socket_errno,
                        opal_net_get_hostname((struct sockaddr*) &addr),
                        opal_net_get_port((struct sockaddr*) &addr));
    if (sd < 0) {
        /* Non-fatal errors */
        if (EINTR == opal_socket_errno ||
            EAGAIN == opal_socket_errno ||
            EWOULDBLOCK == opal_socket_errno) {
            return;
        }

        /* If we run out of file descriptors, log an extra warning (so
           that the user can know to fix this problem) and abandon all
           hope. */
        else if (EMFILE == opal_socket_errno) {
            CLOSE_THE_SOCKET(incoming_sd);
            ORTE_ERROR_LOG(ORTE_ERR_SYS_LIMITS_SOCKETS);
            orte_show_help("help-oob-tcp.txt",
                           "accept failed",
                           true,
                           opal_process_info.nodename,
                           opal_socket_errno,
                           strerror(opal_socket_errno),
                           "Out of file descriptors");
            orte_errmgr.abort(ORTE_ERROR_DEFAULT_EXIT_CODE, NULL);
            return;
        }

        /* For all other cases, close the socket, print a warning but
           try to continue */
        else {
            CLOSE_THE_SOCKET(incoming_sd);
            orte_show_help("help-oob-tcp.txt",
                           "accept failed",
                           true,
                           opal_process_info.nodename,
                           opal_socket_errno,
                           strerror(opal_socket_errno),
                           "Unknown cause; job will try to continue");
            return;
        }
    }

    /* process the connection */
    mca_oob_tcp_module.accept_connection(sd, &addr);
}


static void tcp_ev_cons(mca_oob_tcp_listener_t* event)
{
    event->ev_active = false;
    event->tcp6 = false;
    event->sd = -1;
    event->port = 0;
}
static void tcp_ev_des(mca_oob_tcp_listener_t* event)
{
    if (event->ev_active) {
        opal_event_del(&event->event);
    }
    event->ev_active = false;
    if (0 <= event->sd) {
        CLOSE_THE_SOCKET(event->sd);
        event->sd = -1;
    }
}

OBJ_CLASS_INSTANCE(mca_oob_tcp_listener_t,
                   opal_list_item_t,
                   tcp_ev_cons, tcp_ev_des);

OBJ_CLASS_INSTANCE(mca_oob_tcp_pending_connection_t,
                   opal_object_t,
                   NULL,
                   NULL);
