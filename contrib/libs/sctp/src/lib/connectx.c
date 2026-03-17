/* SCTP kernel Implementation: User API extensions.
 *
 * connectx.c
 *
 * Distributed under the terms of the LGPL v2.1 as described in
 * http://www.gnu.org/copyleft/lesser.txt.
 *
 * This file is part of the user library that offers support for the
 * SCTP kernel Implementation. The main purpose of this
 * code is to provide the SCTP Socket API mappings for user
 * application to interface with the SCTP in kernel.
 *
 * This implementation is based on the Socket API Extensions for SCTP
 * defined in <draft-ietf-tsvwg-sctpsocket-10.txt.
 *
 * (C) Copyright IBM Corp. 2001, 2005
 *
 * Written or modified by:
 *   Frank Filz     <ffilz@us.ibm.com>
 */

#include <sys/socket.h>   /* struct sockaddr_storage, setsockopt() */
#include <netinet/in.h>
#include <netinet/sctp.h> /* SCTP_SOCKOPT_CONNECTX_* */
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include "config.h"

#define __SYMPFX(pfx, sym) #pfx sym
#define _SYMPFX(pfx, sym) __SYMPFX(pfx, sym)
#define SYMPFX(sym) _SYMPFX(__USER_LABEL_PREFIX__, #sym)

#if HAVE_ATTRIBUTE_SYMVER
#define SYMVER(name, name2) __attribute__ ((symver (SYMPFX(name2))))
#else
#define SYMVER(name, name2) __asm__(".symver " SYMPFX(name) "," SYMPFX(name2));
#endif


/* Support the sctp_connectx() interface.
 *
 * See Sockets API Extensions for SCTP. Section 8.1.
 *
 * Instead of implementing through a socket call in sys_socketcall(),
 * tunnel the request through setsockopt().
 */
static int __connectx_addrsize(const struct sockaddr *addrs,
				const int addrcnt)
{
	const char *addrbuf;
	const struct sockaddr *sa_addr;
	int addrs_size = 0;
	int i;

	addrbuf = (char *)addrs;
	for (i = 0; i < addrcnt; i++) {
		sa_addr = (const struct sockaddr *)addrbuf;
		switch (sa_addr->sa_family) {
		case AF_INET:
			addrs_size += sizeof(struct sockaddr_in);
			addrbuf += sizeof(struct sockaddr_in);
			break;
		case AF_INET6:
			addrs_size += sizeof(struct sockaddr_in6);
			addrbuf += sizeof(struct sockaddr_in6);
			break;
		default:
			errno = EINVAL;
			return -1;
		}
	}

	return addrs_size;
}
			

SYMVER(__sctp_connectx, sctp_connectx@)
int __sctp_connectx(int fd, struct sockaddr *addrs, int addrcnt)
{
	int addrs_size = __connectx_addrsize(addrs, addrcnt);

	if (addrs_size < 0)
		return addrs_size;

	return setsockopt(fd, SOL_SCTP, SCTP_SOCKOPT_CONNECTX_OLD, addrs,
			    addrs_size);
}

SYMVER(sctp_connectx_orig, sctp_connectx@VERS_1)
extern int sctp_connectx_orig (int)
	__attribute ((alias ("__sctp_connectx")));


static int __connectx(int fd, struct sockaddr *addrs, socklen_t addrs_size,
			sctp_assoc_t *id)
{
	int status;

	if (id)
		*id = 0;

	status = setsockopt(fd, SOL_SCTP, SCTP_SOCKOPT_CONNECTX, addrs,
			    addrs_size);

	/* Normalize status and set association id */
	if (status > 0) {
		if (id)
			*id = status;
		return 0;
	}

	/* The error is something other then "Option not supported" */
	if (status < 0 && errno != ENOPROTOOPT)
		return status;

	/* At this point, if the application wanted the id, we can't
	 * really provide it, so we can return ENOPROTOOPT.
	 */
	if (id) {
		errno = ENOPROTOOPT;
		return -1;
	}

	/* Finally, try the old API */
	return setsockopt(fd, SOL_SCTP, SCTP_SOCKOPT_CONNECTX_OLD,
			  addrs, addrs_size);
}

SYMVER(sctp_connectx2, sctp_connectx@VERS_2)
int sctp_connectx2(int fd, struct sockaddr *addrs, int addrcnt,
		      sctp_assoc_t *id)
{
	int addrs_size = __connectx_addrsize(addrs, addrcnt);

	if (addrs_size < 0)
		return addrs_size;

	return __connectx(fd, addrs, addrs_size, id);
}

SYMVER(sctp_connectx, sctp_connectx@@VERS_3)
int sctp_connectx(int fd, struct sockaddr *addrs, int addrcnt,
		      sctp_assoc_t *id)
{
	int addrs_size = __connectx_addrsize(addrs, addrcnt);
	int status;
	struct sctp_getaddrs_old param;
	socklen_t opt_len = sizeof(param);

	if (addrs_size < 0)
		return addrs_size;

	/* First try the new socket api
	 * Because the id is returned in the option buffer we have prepend
	 * 32bit to it for the returned association id
	 */
	param.assoc_id = 0;
	param.addr_num = addrs_size;
	param.addrs = addrs;
	status = getsockopt(fd, SOL_SCTP, SCTP_SOCKOPT_CONNECTX3,
		            &param, &opt_len);
	if (status == 0 || errno == EINPROGRESS) {
		/* Succeeded immediately, or initiated on non-blocking
		 * socket.
		 */
		if (id)
			*id = param.assoc_id;
	}

	if (errno != ENOPROTOOPT) {
		/* No point in trying the fallbacks*/
		return status;
	}

	/* The first incarnation of updated connectx api didn't work for
	 * non-blocking sockets.  So if the application wants the association
	 * id and the socket is non-blocking, we can't really do anything.
	 */
	if (id) {
		/* Program wants the association-id returned. We can only do
		 * that if the socket is blocking */
		status = fcntl(fd, F_GETFL);
		if (status < 0)
			return status;

		if (status & O_NONBLOCK) {
			/* Socket is non-blocking. Fail */
			errno = ENOPROTOOPT;
			return -1;
		}
	}

	return __connectx(fd, addrs, addrs_size, id);
}

