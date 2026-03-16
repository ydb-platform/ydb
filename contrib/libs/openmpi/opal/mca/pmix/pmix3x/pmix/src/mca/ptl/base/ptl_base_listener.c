/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2014-2017 Intel, Inc.  All rights reserved.
 * Copyright (c) 2014-2016 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2014-2015 Artem Y. Polyakov <artpol84@gmail.com>.
 *                         All rights reserved.
 * Copyright (c) 2016      Mellanox Technologies, Inc.
 *                         All rights reserved.
 * Copyright (c) 2016      IBM Corporation.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include <src/include/pmix_config.h>

#include <src/include/types.h>
#include <src/include/pmix_stdint.h>
#include <src/include/pmix_socket_errno.h>

#include <pmix_server.h>
#include "src/include/pmix_globals.h"

#ifdef HAVE_STRING_H
#include <string.h>
#endif
#include <fcntl.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef HAVE_SYS_SOCKET_H
#include <sys/socket.h>
#endif
#ifdef HAVE_SYS_UN_H
#include <sys/un.h>
#endif
#ifdef HAVE_SYS_UIO_H
#include <sys/uio.h>
#endif
#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
#include <ctype.h>
#include <sys/stat.h>
#include PMIX_EVENT_HEADER
#include <pthread.h>

#include "src/class/pmix_list.h"
#include "src/util/error.h"
#include "src/util/fd.h"
#include "src/util/output.h"
#include "src/util/pmix_environ.h"

 #include "src/mca/ptl/base/base.h"

// local functions for connection support
static void* listen_thread(void *obj);
static pthread_t engine;
static bool setup_complete = false;

/* Cycle across all available plugins and provide them with
 * an opportunity to register rendezvous points (server-side
 * function). Function is to return PMIX_SUCCESS if at least
 * one rendezvous can be defined. */
static pmix_status_t setup_listeners(pmix_info_t *info, size_t ninfo, bool *need_listener)
{
    pmix_ptl_base_active_t *active;
    pmix_status_t rc;
    size_t n;
    bool single = false;

    if (!pmix_ptl_globals.initialized) {
        return PMIX_ERR_INIT;
    }

    /* scan the directives to see if they want only one listener setup */
    if (NULL != info) {
        for (n=0; n < ninfo; n++) {
            if (0 == strncmp(info[n].key, PMIX_SINGLE_LISTENER, PMIX_MAX_KEYLEN)) {
                single = PMIX_INFO_TRUE(&info[n]);
                break;
            }
        }
    }

    PMIX_LIST_FOREACH(active, &pmix_ptl_globals.actives, pmix_ptl_base_active_t) {
        if (NULL != active->component->setup_listener) {
            rc = active->component->setup_listener(info, ninfo, need_listener);
            if (PMIX_SUCCESS != rc && PMIX_ERR_NOT_AVAILABLE != rc) {
                return rc;
            }
            if (single) {
                return PMIX_SUCCESS;
            }
        }
    }
    /* we must have at least one listener */
    if (0 == pmix_list_get_size(&pmix_ptl_globals.listeners)) {
        return PMIX_ERR_INIT;
    }
    return PMIX_SUCCESS;
}

/*
 * start listening thread
 */
pmix_status_t pmix_ptl_base_start_listening(pmix_info_t *info, size_t ninfo)
{
    pmix_status_t rc;
    bool need_listener = false;

    /* setup the listeners */
    if (!setup_complete) {
        if (PMIX_SUCCESS != (rc = setup_listeners(info, ninfo, &need_listener))) {
            return rc;
        }
    }
    setup_complete = true;

    /* if we don't need a listener thread, then we are done */
    if (!need_listener) {
        return PMIX_SUCCESS;
    }

    /*** spawn internal listener thread */
    if (0 > pipe(pmix_ptl_globals.stop_thread)) {
        PMIX_ERROR_LOG(PMIX_ERR_IN_ERRNO);
        return PMIX_ERR_OUT_OF_RESOURCE;
    }
    /* Make sure the pipe FDs are set to close-on-exec so that
       they don't leak into children */
    if (pmix_fd_set_cloexec(pmix_ptl_globals.stop_thread[0]) != PMIX_SUCCESS ||
        pmix_fd_set_cloexec(pmix_ptl_globals.stop_thread[1]) != PMIX_SUCCESS) {
        PMIX_ERROR_LOG(PMIX_ERR_IN_ERRNO);
        close(pmix_ptl_globals.stop_thread[0]);
        close(pmix_ptl_globals.stop_thread[1]);
        return PMIX_ERR_OUT_OF_RESOURCE;
    }
    /* fork off the listener thread */
    pmix_ptl_globals.listen_thread_active = true;
    if (0 > pthread_create(&engine, NULL, listen_thread, NULL)) {
        pmix_ptl_globals.listen_thread_active = false;
        return PMIX_ERROR;
    }

    return PMIX_SUCCESS;
}

void pmix_ptl_base_stop_listening(void)
{
    int i;
    pmix_listener_t *lt;

    pmix_output_verbose(8, pmix_ptl_base_framework.framework_output,
                        "listen_thread: shutdown");

    if (!pmix_ptl_globals.listen_thread_active) {
        /* nothing we can do */
        return;
    }

    /* mark it as inactive */
    pmix_ptl_globals.listen_thread_active = false;
    /* use the block to break it loose just in
     * case the thread is blocked in a call to select for
     * a long time */
    i=1;
    if (0 > write(pmix_ptl_globals.stop_thread[1], &i, sizeof(int))) {
        return;
    }
    /* wait for thread to exit */
    pthread_join(engine, NULL);
    /* close the sockets to remove the connection points */
    PMIX_LIST_FOREACH(lt, &pmix_ptl_globals.listeners, pmix_listener_t) {
        CLOSE_THE_SOCKET(lt->socket);
        lt->socket = -1;
    }
}

static void* listen_thread(void *obj)
{
    int rc, max, accepted_connections;
    socklen_t addrlen = sizeof(struct sockaddr_storage);
    pmix_pending_connection_t *pending_connection;
    struct timeval timeout;
    fd_set readfds;
    pmix_listener_t *lt;

    pmix_output_verbose(8, pmix_ptl_base_framework.framework_output,
                        "listen_thread: active");


    while (pmix_ptl_globals.listen_thread_active) {
        FD_ZERO(&readfds);
        max = -1;
        PMIX_LIST_FOREACH(lt, &pmix_ptl_globals.listeners, pmix_listener_t) {
            FD_SET(lt->socket, &readfds);
            max = (lt->socket > max) ? lt->socket : max;
        }
        /* add the stop_thread fd */
        FD_SET(pmix_ptl_globals.stop_thread[0], &readfds);
        max = (pmix_ptl_globals.stop_thread[0] > max) ? pmix_ptl_globals.stop_thread[0] : max;

        /* set timeout interval */
        timeout.tv_sec = 2;
        timeout.tv_usec = 0;

        /* Block in a select to avoid hammering the cpu.  If a connection
         * comes in, we'll get woken up right away.
         */
        rc = select(max + 1, &readfds, NULL, NULL, &timeout);
        if (!pmix_ptl_globals.listen_thread_active) {
            /* we've been asked to terminate */
            close(pmix_ptl_globals.stop_thread[0]);
            close(pmix_ptl_globals.stop_thread[1]);
            return NULL;
        }
        if (rc < 0) {
            continue;
        }

        /* Spin accepting connections until all active listen sockets
         * do not have any incoming connections, pushing each connection
         * onto the event queue for processing
         */
        do {
            accepted_connections = 0;
            PMIX_LIST_FOREACH(lt, &pmix_ptl_globals.listeners, pmix_listener_t) {

                /* according to the man pages, select replaces the given descriptor
                 * set with a subset consisting of those descriptors that are ready
                 * for the specified operation - in this case, a read. So we need to
                 * first check to see if this file descriptor is included in the
                 * returned subset
                 */
                if (0 == FD_ISSET(lt->socket, &readfds)) {
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
                pending_connection = PMIX_NEW(pmix_pending_connection_t);
                pending_connection->protocol = lt->protocol;
                pending_connection->ptl = lt->ptl;
                pmix_event_assign(&pending_connection->ev, pmix_globals.evbase, -1,
                                  EV_WRITE, lt->cbfunc, pending_connection);
                pending_connection->sd = accept(lt->socket,
                                                (struct sockaddr*)&(pending_connection->addr),
                                                &addrlen);
                if (pending_connection->sd < 0) {
                    PMIX_RELEASE(pending_connection);
                    if (pmix_socket_errno != EAGAIN ||
                        pmix_socket_errno != EWOULDBLOCK) {
                        if (EMFILE == pmix_socket_errno ||
                            ENOBUFS == pmix_socket_errno ||
                            ENOMEM == pmix_socket_errno) {
                            PMIX_ERROR_LOG(PMIX_ERR_OUT_OF_RESOURCE);
                        } else if (EINVAL == pmix_socket_errno ||
                                   EINTR == pmix_socket_errno) {
                            /* race condition at finalize */
                            goto done;
                        } else if (ECONNABORTED == pmix_socket_errno) {
                            /* they aborted the attempt */
                            continue;
                        } else {
                            pmix_output(0, "listen_thread: accept() failed: %s (%d).",
                                        strerror(pmix_socket_errno), pmix_socket_errno);
                        }
                        goto done;
                    }
                    continue;
                }

                pmix_output_verbose(8, pmix_ptl_base_framework.framework_output,
                                    "listen_thread: new connection: (%d, %d)",
                                    pending_connection->sd, pmix_socket_errno);
                /* post the object */
                PMIX_POST_OBJECT(pending_connection);
                /* activate the event */
                pmix_event_active(&pending_connection->ev, EV_WRITE, 1);
                accepted_connections++;
            }
        } while (accepted_connections > 0);
    }

 done:
    pmix_ptl_globals.listen_thread_active = false;
    return NULL;
}
