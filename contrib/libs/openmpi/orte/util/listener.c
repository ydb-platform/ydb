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
 * Copyright (c) 2013-2015 Intel, Inc.  All rights reserved.
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
#ifdef HAVE_SYS_SOCKET_H
#include <sys/socket.h>
#endif

#include <ctype.h>

#include "opal/util/error.h"
#include "opal/util/output.h"
#include "opal/opal_socket_errno.h"
#include "opal/util/if.h"
#include "opal/util/net.h"
#include "opal/util/fd.h"
#include "opal/class/opal_list.h"

#include "orte/mca/errmgr/errmgr.h"
#include "orte/util/name_fns.h"
#include "orte/runtime/orte_globals.h"
#include "orte/util/show_help.h"

#include "orte/util/listener.h"

static void* listen_thread_fn(opal_object_t *obj);
static opal_list_t mylisteners;
static bool initialized = false;
static opal_thread_t listen_thread;
static volatile bool listen_thread_active = false;
static struct timeval listen_thread_tv;
static int stop_thread[2];

#define CLOSE_THE_SOCKET(socket)    \
    do {                            \
        shutdown(socket, 2);        \
        close(socket);              \
        socket = -1;                \
    } while(0)


int orte_register_listener(struct sockaddr* address, opal_socklen_t addrlen,
                           opal_event_base_t *evbase,
                           orte_listener_callback_fn_t handler)
{
    orte_listener_t *conn;
    int flags;
    int sd = -1;

    if (!initialized) {
        OBJ_CONSTRUCT(&mylisteners, opal_list_t);
        OBJ_CONSTRUCT(&listen_thread, opal_thread_t);
        if (0 > pipe(stop_thread)) {
            ORTE_ERROR_LOG(ORTE_ERR_OUT_OF_RESOURCE);
            return ORTE_ERR_OUT_OF_RESOURCE;
        }
        /* Make sure the pipe FDs are set to close-on-exec so that
           they don't leak into children */
        if (opal_fd_set_cloexec(stop_thread[0]) != OPAL_SUCCESS ||
            opal_fd_set_cloexec(stop_thread[1]) != OPAL_SUCCESS) {
            close(stop_thread[0]);
            close(stop_thread[1]);
            ORTE_ERROR_LOG(ORTE_ERR_IN_ERRNO);
            return ORTE_ERR_IN_ERRNO;
        }
        listen_thread_tv.tv_sec = 3600;
        listen_thread_tv.tv_usec = 0;
        initialized = true;
    }

    /* create a listen socket for incoming connection attempts */
    sd = socket(PF_UNIX, SOCK_STREAM, 0);
    if (sd < 0) {
        if (EAFNOSUPPORT != opal_socket_errno) {
            opal_output(0,"pmix_server_start_listening: socket() failed: %s (%d)",
                        strerror(opal_socket_errno), opal_socket_errno);
        }
        return ORTE_ERR_IN_ERRNO;
    }
    /* Set the socket to close-on-exec so that no children inherit
       this FD */
    if (opal_fd_set_cloexec(sd) != OPAL_SUCCESS) {
        opal_output(0, "pmix_server: unable to set the "
                    "listening socket to CLOEXEC (%s:%d)\n",
                    strerror(opal_socket_errno), opal_socket_errno);
        CLOSE_THE_SOCKET(sd);
        return ORTE_ERROR;
    }


    if (bind(sd, (struct sockaddr*)address, addrlen) < 0) {
        opal_output(0, "%s bind() failed on error %s (%d)",
                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                    strerror(opal_socket_errno),
                    opal_socket_errno );
        CLOSE_THE_SOCKET(sd);
        return ORTE_ERROR;
    }

    /* setup listen backlog to maximum allowed by kernel */
    if (listen(sd, SOMAXCONN) < 0) {
        opal_output(0, "orte_listener: listen() failed: %s (%d)",
                    strerror(opal_socket_errno), opal_socket_errno);
        CLOSE_THE_SOCKET(sd);
        return ORTE_ERROR;
    }

    /* set socket up to be non-blocking, otherwise accept could block */
    if ((flags = fcntl(sd, F_GETFL, 0)) < 0) {
        opal_output(0, "orte_listener: fcntl(F_GETFL) failed: %s (%d)",
                    strerror(opal_socket_errno), opal_socket_errno);
        CLOSE_THE_SOCKET(sd);
        return ORTE_ERROR;
    }
    flags |= O_NONBLOCK;
    if (fcntl(sd, F_SETFL, flags) < 0) {
        opal_output(0, "orte_listener: fcntl(F_SETFL) failed: %s (%d)",
                    strerror(opal_socket_errno), opal_socket_errno);
        CLOSE_THE_SOCKET(sd);
        return ORTE_ERROR;
    }

    /* add this port to our connections */
    conn = OBJ_NEW(orte_listener_t);
    conn->sd = sd;
    conn->evbase = evbase;
    conn->handler = handler;
    opal_list_append(&mylisteners, &conn->item);

    return ORTE_SUCCESS;
}

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
int orte_start_listening(void)
{
    int rc;

    /* if we aren't initialized, or have nothing
     * registered, or are already listening, then return SUCCESS */
    if (!initialized || 0 == opal_list_get_size(&mylisteners) ||
        listen_thread_active) {
        return ORTE_SUCCESS;
    }

    /* start our listener thread */
    listen_thread_active = true;
    listen_thread.t_run = listen_thread_fn;
    listen_thread.t_arg = NULL;
    if (OPAL_SUCCESS != (rc = opal_thread_start(&listen_thread))) {
        ORTE_ERROR_LOG(rc);
        opal_output(0, "%s Unable to start listen thread", ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
    }
    return rc;
}

void orte_stop_listening(void)
{
    int i=0;

    if (!listen_thread_active) {
        return;
    }

    listen_thread_active = false;
    /* tell the thread to exit */
    write(stop_thread[1], &i, sizeof(int));
    opal_thread_join(&listen_thread, NULL);
    OBJ_DESTRUCT(&listen_thread);
    OPAL_LIST_DESTRUCT(&mylisteners);
}

/*
 * The listen thread accepts incoming connections and places them
 * in a queue for further processing
 *
 * Runs until orte_listener_shutdown is set to true.
 */
static void* listen_thread_fn(opal_object_t *obj)
{
    int rc, max, accepted_connections, sd;
    opal_socklen_t addrlen = sizeof(struct sockaddr_storage);
    orte_pending_connection_t *pending_connection;
    struct timeval timeout;
    fd_set readfds;
    orte_listener_t *listener;

    while (listen_thread_active) {
        FD_ZERO(&readfds);
        max = -1;
        OPAL_LIST_FOREACH(listener, &mylisteners, orte_listener_t) {
            FD_SET(listener->sd, &readfds);
            max = (listener->sd > max) ? listener->sd : max;
        }
        /* add the stop_thread fd */
        FD_SET(stop_thread[0], &readfds);
        max = (stop_thread[0] > max) ? stop_thread[0] : max;

        /* set timeout interval */
        timeout.tv_sec = listen_thread_tv.tv_sec;
        timeout.tv_usec = listen_thread_tv.tv_usec;

        /* Block in a select to avoid hammering the cpu.  If a connection
         * comes in, we'll get woken up right away.
         */
        rc = select(max + 1, &readfds, NULL, NULL, &timeout);
        if (!listen_thread_active) {
            /* we've been asked to terminate */
            goto done;
        }
        if (rc < 0) {
            if (EAGAIN != opal_socket_errno && EINTR != opal_socket_errno) {
                perror("select");
            }
            continue;
        }

        /* Spin accepting connections until all active listen sockets
         * do not have any incoming connections, pushing each connection
         * onto its respective event queue for processing
         */
        do {
            accepted_connections = 0;
            OPAL_LIST_FOREACH(listener, &mylisteners, orte_listener_t) {
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
                pending_connection = OBJ_NEW(orte_pending_connection_t);
                opal_event_set(listener->evbase, &pending_connection->ev, -1,
                               OPAL_EV_WRITE, listener->handler, pending_connection);
                opal_event_set_priority(&pending_connection->ev, ORTE_MSG_PRI);
                pending_connection->fd = accept(sd,
                                                (struct sockaddr*)&(pending_connection->addr),
                                                &addrlen);
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

                    /* For all other cases, close the socket, print a
                       warning but try to continue */
                    else {
                        CLOSE_THE_SOCKET(sd);
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

                /* activate the event */
                opal_event_active(&pending_connection->ev, OPAL_EV_WRITE, 1);
                accepted_connections++;
            }
        } while (accepted_connections > 0);
    }

 done:
    close(stop_thread[0]);
    close(stop_thread[1]);
    return NULL;
}


/* INSTANTIATE CLASSES */
static void lcons(orte_listener_t *p)
{
    p->sd = -1;
    p->evbase = NULL;
    p->handler = NULL;
}
static void ldes(orte_listener_t *p)
{
    if (0 <= p->sd) {
        CLOSE_THE_SOCKET(p->sd);
    }
}
OBJ_CLASS_INSTANCE(orte_listener_t,
                   opal_list_item_t,
                   lcons, ldes);

OBJ_CLASS_INSTANCE(orte_pending_connection_t,
                   opal_object_t,
                   NULL,
                   NULL);
