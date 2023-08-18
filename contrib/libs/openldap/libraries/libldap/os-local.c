/* os-local.c -- platform-specific domain socket code */
/* $OpenLDAP$ */
/* This work is part of OpenLDAP Software <http://www.openldap.org/>.
 *
 * Copyright 1998-2022 The OpenLDAP Foundation.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted only as authorized by the OpenLDAP
 * Public License.
 *
 * A copy of this license is available in the file LICENSE in the
 * top-level directory of the distribution or, alternatively, at
 * <http://www.OpenLDAP.org/license.html>.
 */
/* Portions Copyright (c) 1995 Regents of the University of Michigan.
 * All rights reserved. 
 */
/* Portions (C) Copyright PADL Software Pty Ltd. 1999
 * Redistribution and use in source and binary forms, with or without 
 * modification, are permitted provided that this notice is preserved
 * and that due credit is given to PADL Software Pty Ltd. This software
 * is provided ``as is'' without express or implied warranty.  
 */

#include "portable.h"

#ifdef LDAP_PF_LOCAL

#include <stdio.h>

#include <ac/stdlib.h>

#include <ac/errno.h>
#include <ac/socket.h>
#include <ac/string.h>
#include <ac/time.h>
#include <ac/unistd.h>

#ifdef HAVE_SYS_STAT_H
#include <sys/stat.h>
#endif
#ifdef HAVE_SYS_UIO_H
#include <sys/uio.h>
#endif

#ifdef HAVE_IO_H
#include <io.h>
#endif /* HAVE_IO_H */
#ifdef HAVE_FCNTL_H
#include <fcntl.h>
#endif

#include "ldap-int.h"
#include "ldap_defaults.h"

static void
ldap_pvt_set_errno(int err)
{
	errno = err;
}

static int
ldap_pvt_ndelay_on(LDAP *ld, int fd)
{
	Debug1(LDAP_DEBUG_TRACE, "ldap_ndelay_on: %d\n",fd );
	return ber_pvt_socket_set_nonblock( fd, 1 );
}
   
static int
ldap_pvt_ndelay_off(LDAP *ld, int fd)
{
	Debug1(LDAP_DEBUG_TRACE, "ldap_ndelay_off: %d\n",fd );
	return ber_pvt_socket_set_nonblock( fd, 0 );
}

static ber_socket_t
ldap_pvt_socket(LDAP *ld)
{
	ber_socket_t s = socket(PF_LOCAL, SOCK_STREAM, 0);
	Debug1(LDAP_DEBUG_TRACE, "ldap_new_socket: %d\n",s );
#ifdef FD_CLOEXEC
	fcntl(s, F_SETFD, FD_CLOEXEC);
#endif
	return ( s );
}

static int
ldap_pvt_close_socket(LDAP *ld, int s)
{
	Debug1(LDAP_DEBUG_TRACE, "ldap_close_socket: %d\n",s );
	return tcp_close(s);
}

#undef TRACE
#define TRACE do { \
	char ebuf[128]; \
	int saved_errno = errno; \
	Debug3(LDAP_DEBUG_TRACE, "ldap_is_socket_ready: error on socket %d: errno: %d (%s)\n", \
		s, \
		saved_errno, \
		AC_STRERROR_R(saved_errno, ebuf, sizeof ebuf)); \
} while( 0 )

/*
 * check the socket for errors after select returned.
 */
static int
ldap_pvt_is_socket_ready(LDAP *ld, int s)
{
	Debug1(LDAP_DEBUG_TRACE, "ldap_is_sock_ready: %d\n",s );

#if defined( notyet ) /* && defined( SO_ERROR ) */
{
	int so_errno;
	ber_socklen_t dummy = sizeof(so_errno);
	if ( getsockopt( s, SOL_SOCKET, SO_ERROR, &so_errno, &dummy )
		== AC_SOCKET_ERROR )
	{
		return -1;
	}
	if ( so_errno ) {
		ldap_pvt_set_errno(so_errno);
		TRACE;
		return -1;
	}
	return 0;
}
#else
{
	/* error slippery */
	struct sockaddr_un sa;
	char ch;
	ber_socklen_t dummy = sizeof(sa);
	if ( getpeername( s, (struct sockaddr *) &sa, &dummy )
		== AC_SOCKET_ERROR )
	{
		/* XXX: needs to be replace with ber_stream_read() */
		(void)read(s, &ch, 1);
		TRACE;
		return -1;
	}
	return 0;
}
#endif
	return -1;
}
#undef TRACE

#ifdef LDAP_PF_LOCAL_SENDMSG
static const char abandonPDU[] = {LDAP_TAG_MESSAGE, 6,
	LDAP_TAG_MSGID, 1, 0, LDAP_REQ_ABANDON, 1, 0};
#endif

static int
ldap_pvt_connect(LDAP *ld, ber_socket_t s, struct sockaddr_un *sa, int async)
{
	int rc;
	struct timeval	tv, *opt_tv = NULL;

	if ( ld->ld_options.ldo_tm_net.tv_sec >= 0 ) {
		tv = ld->ld_options.ldo_tm_net;
		opt_tv = &tv;
	}

	Debug3(LDAP_DEBUG_TRACE,
		"ldap_connect_timeout: fd: %d tm: %ld async: %d\n",
		s, opt_tv ? tv.tv_sec : -1L, async);

	if ( ldap_pvt_ndelay_on(ld, s) == -1 ) return -1;

	if ( connect(s, (struct sockaddr *) sa, sizeof(struct sockaddr_un))
		!= AC_SOCKET_ERROR )
	{
		if ( ldap_pvt_ndelay_off(ld, s) == -1 ) return -1;

#ifdef LDAP_PF_LOCAL_SENDMSG
	/* Send a dummy message with access rights. Remote side will
	 * obtain our uid/gid by fstat'ing this descriptor. The
	 * descriptor permissions must match exactly, and we also
	 * send the socket name, which must also match.
	 */
sendcred:
		{
			int fds[2];
			ber_socklen_t salen = sizeof(*sa);
			if (pipe(fds) == 0) {
				/* Abandon, noop, has no reply */
				struct iovec iov;
				struct msghdr msg = {0};
# ifdef HAVE_STRUCT_MSGHDR_MSG_CONTROL
# ifndef CMSG_SPACE
# define CMSG_SPACE(len)	(_CMSG_ALIGN( sizeof(struct cmsghdr)) + _CMSG_ALIGN(len) )
# endif
# ifndef CMSG_LEN
# define CMSG_LEN(len)		(_CMSG_ALIGN( sizeof(struct cmsghdr)) + (len) )
# endif
				union {
					struct cmsghdr cm;
					unsigned char control[CMSG_SPACE(sizeof(int))];
				} control_un;
				struct cmsghdr *cmsg;
# endif /* HAVE_STRUCT_MSGHDR_MSG_CONTROL */
				msg.msg_name = NULL;
				msg.msg_namelen = 0;
				iov.iov_base = (char *) abandonPDU;
				iov.iov_len = sizeof abandonPDU;
				msg.msg_iov = &iov;
				msg.msg_iovlen = 1;
# ifdef HAVE_STRUCT_MSGHDR_MSG_CONTROL
				msg.msg_control = control_un.control;
				msg.msg_controllen = sizeof( control_un.control );
				msg.msg_flags = 0;

				cmsg = CMSG_FIRSTHDR( &msg );
				cmsg->cmsg_len = CMSG_LEN( sizeof(int) );
				cmsg->cmsg_level = SOL_SOCKET;
				cmsg->cmsg_type = SCM_RIGHTS;

				*((int *)CMSG_DATA(cmsg)) = fds[0];
# else
				msg.msg_accrights = (char *)fds;
				msg.msg_accrightslen = sizeof(int);
# endif /* HAVE_STRUCT_MSGHDR_MSG_CONTROL */
				getpeername( s, (struct sockaddr *) sa, &salen );
				fchmod( fds[0], S_ISUID|S_IRWXU );
				write( fds[1], sa, salen );
				sendmsg( s, &msg, 0 );
				close(fds[0]);
				close(fds[1]);
			}
		}
#endif
		return 0;
	}

	if ( errno != EINPROGRESS && errno != EWOULDBLOCK ) return -1;
	
#ifdef notyet
	if ( async ) return -2;
#endif

#ifdef HAVE_POLL
	{
		struct pollfd fd;
		int timeout = INFTIM;

		if( opt_tv != NULL ) timeout = TV2MILLISEC( &tv );

		fd.fd = s;
		fd.events = POLL_WRITE;

		do {
			fd.revents = 0;
			rc = poll( &fd, 1, timeout );
		} while( rc == AC_SOCKET_ERROR && errno == EINTR &&
			LDAP_BOOL_GET(&ld->ld_options, LDAP_BOOL_RESTART ));

		if( rc == AC_SOCKET_ERROR ) return rc;

		if( fd.revents & POLL_WRITE ) {
			if ( ldap_pvt_is_socket_ready(ld, s) == -1 ) return -1;
			if ( ldap_pvt_ndelay_off(ld, s) == -1 ) return -1;
#ifdef LDAP_PF_LOCAL_SENDMSG
			goto sendcred;
#else
			return ( 0 );
#endif
		}
	}
#else
	{
		fd_set wfds, *z=NULL;

#ifdef FD_SETSIZE
		if ( s >= FD_SETSIZE ) {
			rc = AC_SOCKET_ERROR;
			tcp_close( s );
			ldap_pvt_set_errno( EMFILE );
			return rc;
		}
#endif
		do { 
			FD_ZERO(&wfds);
			FD_SET(s, &wfds );
			rc = select( ldap_int_tblsize, z, &wfds, z, opt_tv ? &tv : NULL );
		} while( rc == AC_SOCKET_ERROR && errno == EINTR &&
			LDAP_BOOL_GET(&ld->ld_options, LDAP_BOOL_RESTART ));

		if( rc == AC_SOCKET_ERROR ) return rc;

		if ( FD_ISSET(s, &wfds) ) {
			if ( ldap_pvt_is_socket_ready(ld, s) == -1 ) return -1;
			if ( ldap_pvt_ndelay_off(ld, s) == -1 ) return -1;
#ifdef LDAP_PF_LOCAL_SENDMSG
			goto sendcred;
#else
			return ( 0 );
#endif
		}
	}
#endif

	Debug0(LDAP_DEBUG_TRACE, "ldap_connect_timeout: timed out\n" );
	ldap_pvt_set_errno( ETIMEDOUT );
	return ( -1 );
}

int
ldap_connect_to_path(LDAP *ld, Sockbuf *sb, LDAPURLDesc *srv, int async)
{
	struct sockaddr_un	server;
	ber_socket_t		s;
	int			rc;
	const char *path = srv->lud_host;

	Debug0(LDAP_DEBUG_TRACE, "ldap_connect_to_path\n" );

	if ( path == NULL || path[0] == '\0' ) {
		path = LDAPI_SOCK;
	} else {
		if ( strlen(path) > (sizeof( server.sun_path ) - 1) ) {
			ldap_pvt_set_errno( ENAMETOOLONG );
			return -1;
		}
	}

	s = ldap_pvt_socket( ld );
	if ( s == AC_SOCKET_INVALID ) {
		return -1;
	}

	Debug1(LDAP_DEBUG_TRACE, "ldap_connect_to_path: Trying %s\n", path );

	memset( &server, '\0', sizeof(server) );
	server.sun_family = AF_LOCAL;
	strcpy( server.sun_path, path );

	rc = ldap_pvt_connect(ld, s, &server, async);

	if (rc == 0) {
		rc = ldap_int_connect_cbs( ld, sb, &s, srv, (struct sockaddr *)&server );
	}
	if ( rc ) {
		ldap_pvt_close_socket(ld, s);
	}
	return rc;
}
#else
static int dummy; /* generate also a warning: 'dummy' defined but not used (at least here) */
#endif /* LDAP_PF_LOCAL */
