/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2006 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2015-2017 Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include <src/include/pmix_config.h>
#include "include/pmix_stdint.h"

#include <stdio.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef HAVE_FCNTL_H
#include <fcntl.h>
#endif
#ifdef HAVE_SYS_SOCKET_H
#include <sys/socket.h>
#endif

#include "include/pmix_socket_errno.h"
#include "src/util/argv.h"
#include "src/util/error.h"
#include "src/util/getid.h"
#include "src/util/strnlen.h"
#include "src/include/pmix_globals.h"
#include "src/client/pmix_client_ops.h"
#include "src/server/pmix_server_ops.h"

#include "src/mca/ptl/base/base.h"

pmix_status_t pmix_ptl_base_set_nonblocking(int sd)
{
    int flags;
     /* setup the socket as non-blocking */
    if ((flags = fcntl(sd, F_GETFL, 0)) < 0) {
        pmix_output(0, "ptl:base:set_nonblocking: fcntl(F_GETFL) failed: %s (%d)\n",
                    strerror(pmix_socket_errno),
                    pmix_socket_errno);
    } else {
        flags |= O_NONBLOCK;
        if(fcntl(sd, F_SETFL, flags) < 0)
            pmix_output(0, "ptl:base:set_nonblocking: fcntl(F_SETFL) failed: %s (%d)\n",
                        strerror(pmix_socket_errno),
                        pmix_socket_errno);
    }
    return PMIX_SUCCESS;
}

pmix_status_t pmix_ptl_base_set_blocking(int sd)
{
    int flags;
     /* setup the socket as non-blocking */
    if ((flags = fcntl(sd, F_GETFL, 0)) < 0) {
        pmix_output(0, "ptl:base:set_blocking: fcntl(F_GETFL) failed: %s (%d)\n",
                    strerror(pmix_socket_errno),
                    pmix_socket_errno);
    } else {
        flags &= ~(O_NONBLOCK);
        if(fcntl(sd, F_SETFL, flags) < 0)
            pmix_output(0, "ptl:base:set_blocking: fcntl(F_SETFL) failed: %s (%d)\n",
                        strerror(pmix_socket_errno),
                        pmix_socket_errno);
    }
    return PMIX_SUCCESS;
}

/*
 * A blocking send on a non-blocking socket. Used to send the small amount of connection
 * information that identifies the peers endpoint.
 */
pmix_status_t pmix_ptl_base_send_blocking(int sd, char *ptr, size_t size)
{
    size_t cnt = 0;
    int retval;

    pmix_output_verbose(8, pmix_ptl_base_framework.framework_output,
                        "send blocking of %"PRIsize_t" bytes to socket %d",
                        size, sd );
    while (cnt < size) {
        retval = send(sd, (char*)ptr+cnt, size-cnt, 0);
        if (retval < 0) {
            if (EAGAIN == pmix_socket_errno ||
                EWOULDBLOCK == pmix_socket_errno) {
                /* just cycle and let it try again */
                pmix_output_verbose(8, pmix_ptl_base_framework.framework_output,
                                    "blocking_send received error %d:%s from remote - cycling",
                                    pmix_socket_errno, strerror(pmix_socket_errno));
                continue;
            }
            if (pmix_socket_errno != EINTR) {
                pmix_output_verbose(8, pmix_ptl_base_framework.framework_output,
                                    "ptl:base:peer_send_blocking: send() to socket %d failed: %s (%d)\n",
                                    sd, strerror(pmix_socket_errno),
                                    pmix_socket_errno);
                return PMIX_ERR_UNREACH;
            }
            continue;
        }
        cnt += retval;
    }

    pmix_output_verbose(8, pmix_ptl_base_framework.framework_output,
                        "blocking send complete to socket %d", sd);
    return PMIX_SUCCESS;
}

/*
 * A blocking recv on a non-blocking socket. Used to receive the small amount of connection
 * information that identifies the peers endpoint.
 */
pmix_status_t pmix_ptl_base_recv_blocking(int sd, char *data, size_t size)
{
    size_t cnt = 0;

    pmix_output_verbose(8, pmix_ptl_base_framework.framework_output,
                        "waiting for blocking recv of %"PRIsize_t" bytes", size);

    while (cnt < size) {
        int retval = recv(sd, (char *)data+cnt, size-cnt, MSG_WAITALL);

        /* remote closed connection */
        if (retval == 0) {
            pmix_output_verbose(8, pmix_ptl_base_framework.framework_output,
                                "ptl:base:recv_blocking: remote closed connection");
            return PMIX_ERR_UNREACH;
        }

        /* handle errors */
        if (retval < 0) {
            if (EAGAIN == pmix_socket_errno ||
                EWOULDBLOCK == pmix_socket_errno) {
                /* just cycle and let it try again */
                pmix_output_verbose(8, pmix_ptl_base_framework.framework_output,
                                    "blocking_recv received error %d:%s from remote - cycling",
                                    pmix_socket_errno, strerror(pmix_socket_errno));
                return PMIX_ERR_TEMP_UNAVAILABLE;
            }
            if (pmix_socket_errno != EINTR ) {
                /* If we overflow the listen backlog, it's
                   possible that even though we finished the three
                   way handshake, the remote host was unable to
                   transition the connection from half connected
                   (received the initial SYN) to fully connected
                   (in the listen backlog).  We likely won't see
                   the failure until we try to receive, due to
                   timing and the like.  The first thing we'll get
                   in that case is a RST packet, which receive
                   will turn into a connection reset by peer
                   errno.  In that case, leave the socket in
                   CONNECT_ACK and propogate the error up to
                   recv_connect_ack, who will try to establish the
                   connection again */
                pmix_output_verbose(8, pmix_ptl_base_framework.framework_output,
                                    "blocking_recv received error %d:%s from remote - aborting",
                                    pmix_socket_errno, strerror(pmix_socket_errno));
                return PMIX_ERR_UNREACH;
            }
            continue;
        }
        cnt += retval;
    }

    pmix_output_verbose(8, pmix_ptl_base_framework.framework_output,
                        "blocking receive complete from remote");
    return PMIX_SUCCESS;
}

#define PMIX_MAX_RETRIES 10

pmix_status_t pmix_ptl_base_connect(struct sockaddr_storage *addr,
                                    pmix_socklen_t addrlen, int *fd)
{
    int sd = -1;
    int retries = 0;

    pmix_output_verbose(2, pmix_ptl_base_framework.framework_output,
                        "ptl_base_connect: attempting to connect to server");

    while (retries < PMIX_MAX_RETRIES) {
        retries++;
        /* Create the new socket */
        sd = socket(addr->ss_family, SOCK_STREAM, 0);
        if (sd < 0) {
            pmix_output(0, "pmix:create_socket: socket() failed: %s (%d)\n",
                        strerror(pmix_socket_errno),
                        pmix_socket_errno);
            continue;
        }
        pmix_output_verbose(2, pmix_ptl_base_framework.framework_output,
                            "pmix_ptl_base_connect: attempting to connect to server on socket %d", sd);
        /* try to connect */
        if (connect(sd, (struct sockaddr*)addr, addrlen) < 0) {
            if (pmix_socket_errno == ETIMEDOUT) {
                /* The server may be too busy to accept new connections */
                pmix_output_verbose(2, pmix_ptl_base_framework.framework_output,
                                    "timeout connecting to server");
                CLOSE_THE_SOCKET(sd);
                continue;
            }

            /* Some kernels (Linux 2.6) will automatically software
             abort a connection that was ECONNREFUSED on the last
             attempt, without even trying to establish the
             connection.  Handle that case in a semi-rational
             way by trying twice before giving up */
            if (ECONNABORTED == pmix_socket_errno) {
                pmix_output_verbose(2, pmix_ptl_base_framework.framework_output,
                                    "connection to server aborted by OS - retrying");
                CLOSE_THE_SOCKET(sd);
                continue;
            } else {
                pmix_output_verbose(2, pmix_ptl_base_framework.framework_output,
                                    "Connect failed: %s (%d)", strerror(pmix_socket_errno),
                                    pmix_socket_errno);
                CLOSE_THE_SOCKET(sd);
                continue;
            }
        } else {
            /* otherwise, the connect succeeded - so break out of the loop */
            break;
        }
    }

    if (retries == PMIX_MAX_RETRIES || sd < 0){
        /* We were unsuccessful in establishing this connection, and are
         * not likely to suddenly become successful */
        if (0 <= sd) {
            CLOSE_THE_SOCKET(sd);
        }
        return PMIX_ERR_UNREACH;
    }
    *fd = sd;

    return PMIX_SUCCESS;
}
