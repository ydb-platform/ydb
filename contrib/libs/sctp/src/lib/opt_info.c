/* SCTP kernel Implementation: User API extensions.
 *
 * opt_info.c
 *
 * Distributed under the terms of the LGPL v2.1 as described in
 *    http://www.gnu.org/copyleft/lesser.txt 
 *
 * This file is part of the user library that offers support for the
 * SCTP kernel Implementation. The main purpose of this
 * code if to provide the SCTP Socket API mappings for user
 * application to interface with the SCTP in kernel.
 *
 * This implementation is based on the Socket API Extensions for SCTP
 * defined in <draft-ietf-tsvwg-sctpsocket-10.txt.
 *
 * (C) Copyright IBM Corp. 2003
 * Copyright (c) 2002 Intel Corporation.
 *
 * Written or modified by:
 *   Ardelle Fan <ardelle.fan@intel.com>
 */

#include <sys/socket.h>   /* struct sockaddr_storage, setsockopt() */
#include <netinet/sctp.h> /* SCTP_SOCKOPT_BINDX_* */
#include <errno.h>

/* Support the sctp_opt_info() interface.
 *
 * See Sockets API Extensions for SCTP. Section 7.
 *
 * Pass sctp option information pass both in to and out of the SCTP stack.
 * This is a new SCTP API described in the section 7 of the Sockets API
 * Extensions for SCTP. This is implemented using the getsockopt() interface.
 */
int
sctp_opt_info(int sd, sctp_assoc_t id, int opt, void *arg, socklen_t *size)
{
	switch (opt) {
	case SCTP_RTOINFO:
	case SCTP_ASSOCINFO:
	case SCTP_INITMSG:
	case SCTP_NODELAY:
	case SCTP_AUTOCLOSE:
	case SCTP_PRIMARY_ADDR:
	case SCTP_DISABLE_FRAGMENTS:
	case SCTP_PEER_ADDR_PARAMS:
	case SCTP_DEFAULT_SEND_PARAM:
	case SCTP_EVENTS:
	case SCTP_I_WANT_MAPPED_V4_ADDR:
	case SCTP_MAXSEG:
	case SCTP_STATUS:
	case SCTP_GET_PEER_ADDR_INFO:
	case SCTP_AUTH_ACTIVE_KEY:
	case SCTP_PEER_AUTH_CHUNKS:
	case SCTP_LOCAL_AUTH_CHUNKS:
		*(sctp_assoc_t *)arg = id;
		return getsockopt(sd, IPPROTO_SCTP, opt, arg, size);
	default:
		return ENOTSUP;
	}

} /* sctp_opt_info() */
