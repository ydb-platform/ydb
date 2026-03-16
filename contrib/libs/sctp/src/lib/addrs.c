/* SCTP kernel Implementation: User API extensions.
 *
 * addrs.c
 *
 * Distributed under the terms of the LGPL v2.1 as described in
 *    http://www.gnu.org/copyleft/lesser.txt 
 *
 * This file is part of the user library that offers support for the
 * SCTP kernel Implementation. The main purpose of this
 * code is to provide the SCTP Socket API mappings for user
 * application to interface with the SCTP in kernel.
 *
 * This implementation is based on the Socket API Extensions for SCTP
 * defined in <draft-ietf-tsvwg-sctpsocket-10.txt.
 *
 * (C) Copyright IBM Corp. 2003
 * Copyright (c) 2001-2002 Intel Corp.
 *
 * Written or modified by:
 *  Ardelle Fan     <ardelle.fan@intel.com>
 *  Sridhar Samudrala <sri@us.ibm.com>
 *  Ivan Skytte Jørgensen <isj-sctp@i1.dk>
 */

#include <malloc.h>
#include <netinet/in.h>
#include <netinet/sctp.h>
#include <string.h>
#include <errno.h>

/* 
 * Common getsockopt() layer 
 * If the NEW getsockopt() API fails this function will fall back to using
 * the old API
 */
static int
sctp_getaddrs(int sd, sctp_assoc_t id, int optname_new,
	      struct sockaddr **addrs)
{
	int cnt, err;
	socklen_t len;
	size_t bufsize = 4096; /*enough for most cases*/

	struct sctp_getaddrs *getaddrs = (struct sctp_getaddrs*)malloc(bufsize);
	if(!getaddrs)
		return -1;
	
	for(;;) {
		char *new_buf;

		len = bufsize;
		getaddrs->assoc_id = id;
		err = getsockopt(sd, SOL_SCTP, optname_new, getaddrs, &len);
		if (err == 0) {
			/*got it*/
			break;
		}
		if (errno != ENOMEM ) {
			/*unknown error*/
			free(getaddrs);
			return -1;
		}
		/*expand buffer*/
		if (bufsize > 128*1024) {
			/*this is getting ridiculous*/
			free(getaddrs);
			errno = ENOBUFS;
			return -1;
		}
		new_buf = realloc(getaddrs, bufsize+4096);
		if (!new_buf) {
			free(getaddrs);
			return -1;
		}
		bufsize += 4096;
		getaddrs = (struct sctp_getaddrs*)new_buf;
	}

	/* we skip traversing the list, allocating a new buffer etc. and enjoy
	 * a simple hack*/
	cnt = getaddrs->addr_num;
	memmove(getaddrs, getaddrs + 1, len);
	*addrs = (struct sockaddr*)getaddrs;

	return cnt;
} /* sctp_getaddrs() */

/* Get all peer address on a socket.  This is a new SCTP API
 * described in the section 8.3 of the Sockets API Extensions for SCTP.
 * This is implemented using the getsockopt() interface.
 */
int
sctp_getpaddrs(int sd, sctp_assoc_t id, struct sockaddr **addrs)
{
	return sctp_getaddrs(sd, id,
			     SCTP_GET_PEER_ADDRS,
			     addrs);
} /* sctp_getpaddrs() */

/* Frees all resources allocated by sctp_getpaddrs().  This is a new SCTP API
 * described in the section 8.4 of the Sockets API Extensions for SCTP.
 */
int
sctp_freepaddrs(struct sockaddr *addrs)
{
	free(addrs);
	return 0;

} /* sctp_freepaddrs() */

/* Get all locally bound address on a socket.  This is a new SCTP API
 * described in the section 8.5 of the Sockets API Extensions for SCTP.
 * This is implemented using the getsockopt() interface.
 */
int
sctp_getladdrs(int sd, sctp_assoc_t id, struct sockaddr **addrs)
{
	return sctp_getaddrs(sd, id,
			     SCTP_GET_LOCAL_ADDRS,
			     addrs);
} /* sctp_getladdrs() */

/* Frees all resources allocated by sctp_getladdrs().  This is a new SCTP API
 * described in the section 8.6 of the Sockets API Extensions for SCTP.
 */
int
sctp_freeladdrs(struct sockaddr *addrs)
{
	free(addrs);
	return 0;

} /* sctp_freeladdrs() */

int
sctp_getaddrlen(sa_family_t family)
{
	/* We could call into the kernel to see what it thinks the size should
	 * be, but hardcoding the address families here is: (a) faster,
	 * (b) easier, and (c) probably good enough for forseeable future.
	 */
	switch(family) {
	case AF_INET:
		return sizeof(struct sockaddr_in);
	case AF_INET6:
		return sizeof(struct sockaddr_in6);
	default:
		/* Currently there is no defined error handling in
		 * draft-ietf-tsvwg-sctpsocket-13.txt.
		 * -1 might cause the application to overwrite buffer
		 * or misinterpret data. 0 is more likely to cause
		 * an endless loop.
		 */
		return 0;
	}
}
