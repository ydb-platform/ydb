/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2012      Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2015-2018 Cisco Systems, Inc.  All rights reserved
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
#include "ompi_config.h"
#include <stdio.h>
#include <string.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
#ifdef HAVE_SYS_SOCKET_H
#include <sys/socket.h>
#endif
#include <errno.h>
#ifdef HAVE_NETINET_IN_H
#include <netinet/in.h>
#endif

#include "opal/util/show_help.h"

#include "ompi/mpi/c/bindings.h"
#include "ompi/runtime/params.h"
#include "ompi/runtime/mpiruntime.h"
#include "ompi/communicator/communicator.h"
#include "ompi/errhandler/errhandler.h"
#include "ompi/dpm/dpm.h"


#if OMPI_BUILD_MPI_PROFILING
#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak MPI_Comm_join = PMPI_Comm_join
#endif
#define MPI_Comm_join PMPI_Comm_join
#endif

static const char FUNC_NAME[] = "MPI_Comm_join";

static int ompi_socket_send (int fd, char *buf, int len );
static int ompi_socket_recv (int fd, char *buf, int len );

int MPI_Comm_join(int fd, MPI_Comm *intercomm)
{
    int rc;
    uint32_t len, rlen, llen, lrlen;
    int send_first=0;
    ompi_process_name_t rname, tmp_name;

    ompi_communicator_t *newcomp;
    char port_name[MPI_MAX_PORT_NAME];

    if ( MPI_PARAM_CHECK ) {
        OMPI_ERR_INIT_FINALIZE(FUNC_NAME);

        if ( NULL == intercomm ) {
            return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_ARG,
                                          FUNC_NAME);
        }
    }

    if (!ompi_mpi_dynamics_is_enabled(FUNC_NAME)) {
        return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, OMPI_ERR_NOT_SUPPORTED,
                                      FUNC_NAME);
    }

    OPAL_CR_ENTER_LIBRARY();

    /* send my process name */
    tmp_name = *OMPI_PROC_MY_NAME;
    OMPI_PROCESS_NAME_HTON(tmp_name);
    ompi_socket_send(fd, (char*) &tmp_name, sizeof(tmp_name));

    /* recv the remote name */
    ompi_socket_recv(fd, (char*) &rname, sizeof(rname));
    OMPI_PROCESS_NAME_NTOH(rname);

    /* compare the two to get send_first */
    if (OMPI_PROC_MY_NAME->jobid == rname.jobid) {
        if (OMPI_PROC_MY_NAME->vpid < rname.vpid) {
            send_first = true;
        } else if (OMPI_PROC_MY_NAME->vpid == rname.vpid) {
            /* joining to myself is not allowed */
            *intercomm = MPI_COMM_NULL;
            OPAL_CR_EXIT_LIBRARY();
            return MPI_ERR_INTERN;
        } else {
            send_first = false;
        }
    } else if (OMPI_PROC_MY_NAME->jobid < rname.jobid) {
        send_first = true;
    }

    /* Assumption: socket_send should not block, even if the socket
       is not configured to be non-blocking, because the message length are
       so short. */

    /* we will only use the send_first proc's port name,
     * so pass it to the recv_first participant */
    if (send_first) {
        // The port_name that we get back will be \0-terminated.  The
        // strlen+\0 will be <= MPI_MAX_PORT_NAME characters.
        if (OMPI_SUCCESS != (rc = ompi_dpm_open_port(port_name))) {
            goto error;
        }
        // Send the strlen+1 so that we both send the \0 and the
        // receiver receives the \0.
        llen   = (uint32_t)(strlen(port_name)+1);
        len    = htonl(llen);
        ompi_socket_send( fd, (char *) &len, sizeof(uint32_t));
        ompi_socket_send (fd, port_name, llen);
    } else {
        ompi_socket_recv (fd, (char *) &rlen, sizeof(uint32_t));
        // The lrlen that we receive will be the strlen+1 (to account
        // for \0), and will be <= MPI_MAX_PORT_NAME.
        lrlen  = ntohl(rlen);
        ompi_socket_recv (fd, port_name, lrlen);
    }

    /* use the port to connect/accept */
    rc = ompi_dpm_connect_accept (MPI_COMM_SELF, 0, port_name, send_first, &newcomp);

    OPAL_CR_EXIT_LIBRARY();

    *intercomm = newcomp;

 error:
    OPAL_CR_EXIT_LIBRARY();

    if (OPAL_ERR_NOT_SUPPORTED == rc) {
        opal_show_help("help-mpi-api.txt",
                       "MPI function not supported",
                       true,
                       FUNC_NAME,
                       "Underlying runtime environment does not support join functionality");
    }

    OMPI_ERRHANDLER_RETURN (rc, MPI_COMM_SELF, rc, FUNC_NAME);
}


static int ompi_socket_send (int fd, char *buf, int len )
{
    int num;
    size_t s_num;
    ssize_t a;
    char *c_ptr;
    int ret = OMPI_SUCCESS;

    num   = len;
    c_ptr = buf;

    do {
        s_num = (size_t) num;
        a = write ( fd, c_ptr, s_num );
        if ( a == -1 ) {
            if ( errno == EINTR ) {
                /* Catch EINTR on, mainly on IBM RS6000 */
                continue;
            }
#ifdef __SR8000
            else if ( errno == EWOULDBLOCK ) {
                /*Catch EWOULDBLOCK on Hitachi SR8000 */
                continue;
            }
            else if ( errno == EAGAIN ) {
                /* Catch EAGAIN on Hitachi SR8000 */
                continue;
            }
#endif
            else {
                /* Another error occured */
                fprintf (stderr,"ompi_socket_send: error while writing to socket"
                         " error:%s", strerror (errno) );
                return MPI_ERR_OTHER;
            }
        }
        num      -= a;
        c_ptr    += a;
    }   while ( num > 0 );


    if ( num < 0 )  {
        fprintf (stderr, "ompi_socket_send: more data written then available");
        ret = MPI_ERR_INTERN;
    }

    return ret;
}

static int ompi_socket_recv (int fd, char *buf, int len )
{
    int num;
    size_t s_num;
    ssize_t a;
    char *c_ptr;
    int ret = MPI_SUCCESS;

    num      = len;
    c_ptr      = buf;

    do {
        s_num = (size_t ) num;
        a = read ( fd, c_ptr, s_num );
        if ( a == -1 ) {
            if ( errno == EINTR ) {
                /* Catch EINTR on, mainly on IBM RS6000 */
                continue;
            }
#ifdef __SR8000
            else if ( errno == EWOULDBLOCK ) {
                /*Catch EWOULDBLOCK on Hitachi SR8000 */
                continue;
            }
            else if ( errno == EAGAIN ) {
                /* Catch EAGAIN on Hitachi SR8000 */
                continue;
            }
#endif
            else {
                /* Another error occured */
                fprintf (stderr,"ompi_socket_recv: error while reading from socket"
                         " error:%s", strerror (errno) );
                return MPI_ERR_OTHER;
            }
        }
        num    -= a;
        c_ptr  += a;
    }   while ( num > 0 );

    if ( num < 0 )  {
        fprintf (stderr, "ompi_socket_recv: more data read then available");
        ret = MPI_ERR_INTERN;
    }

    return ret;
}
