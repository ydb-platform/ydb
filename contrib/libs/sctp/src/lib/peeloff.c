/* SCTP kernel Implementation: User API extensions.
 *
 * peeloff.c
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
 * (C) Copyright IBM Corp. 2001, 2003
 * 
 * Written or modified by: 
 *  Sridhar Samudrala     <sri@us.ibm.com>
 */

#include <sys/socket.h>   /* struct sockaddr_storage, setsockopt() */
#include <netinet/sctp.h> /* SCTP_SOCKOPT_BINDX_* */
#include <errno.h>

#ifdef HAVE_SCTP_PEELOFF_FLAGS
int
sctp_peeloff_flags(int fd, sctp_assoc_t associd, unsigned flags)
{
	socklen_t peeloff_size = sizeof(sctp_peeloff_flags_arg_t);
	sctp_peeloff_flags_arg_t peeloff;
	int err;

	if (!flags)
		return sctp_peeloff(fd, associd);

	peeloff.p_arg.associd = associd;
	peeloff.p_arg.sd = 0;
	peeloff.flags = flags;

	err = getsockopt(fd, SOL_SCTP, SCTP_SOCKOPT_PEELOFF_FLAGS, &peeloff,
			 &peeloff_size);

	if (err < 0)
		return err;

	return peeloff.p_arg.sd;
}
#endif

/* Branch off an association into a seperate socket.  This is a new SCTP API
 * described in the section 8.2 of the Sockets API Extensions for SCTP. 
 * This is implemented using the getsockopt() interface.
 */
int
sctp_peeloff(int fd, sctp_assoc_t associd)
{
	socklen_t peeloff_size = sizeof(sctp_peeloff_arg_t);
	sctp_peeloff_arg_t peeloff;
	int err;

	peeloff.associd = associd;
	peeloff.sd = 0;

	err = getsockopt(fd, SOL_SCTP, SCTP_SOCKOPT_PEELOFF, &peeloff,
			 &peeloff_size);

	if (err < 0)
		return err;

	return peeloff.sd;
}
