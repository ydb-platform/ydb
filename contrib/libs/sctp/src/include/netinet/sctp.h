/* src/include/netinet/sctp.h.  Generated from sctp.h.in by configure.  */
/* SCTP kernel Implementation: User API extensions.
 *
 * sctp.h
 *
 * Distributed under the terms of the LGPL v2.1 as described in
 *    http://www.gnu.org/copyleft/lesser.txt
 *
 * This file is part of the user library that offers support for the
 * Linux Kernel SCTP Implementation. The main purpose of this
 * code is to provide the SCTP Socket API mappings for user
 * application to interface with SCTP in kernel.
 *
 * This header represents the structures and constants needed to support
 * the SCTP Extension to the Sockets API.
 *
 * (C) Copyright IBM Corp. 2001, 2004
 * Copyright (c) 1999-2000 Cisco, Inc.
 * Copyright (c) 1999-2001 Motorola, Inc.
 *
 * Written or modified by:
 *    La Monte H.P. Yarroll    <piggy@acm.org>
 *    R. Stewart               <randall@sctp.chicago.il.us>
 *    K. Morneau               <kmorneau@cisco.com>
 *    Q. Xie                   <qxie1@email.mot.com>
 *    Karl Knutson             <karl@athena.chicago.il.us>
 *    Jon Grimm                <jgrimm@austin.ibm.com>
 *    Daisy Chang              <daisyc@us.ibm.com>
 *    Inaky Perez-Gonzalez     <inaky.gonzalez@intel.com>
 *    Sridhar Samudrala        <sri@us.ibm.com>
 *    Vlad Yasevich		<vladislav.yasevich@hp.com>
 */

#ifndef __linux_sctp_h__
#define __linux_sctp_h__

#include <stdint.h>
#include <linux/types.h>
#include <sys/socket.h>
#include <linux/sctp.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Socket option layer for SCTP */
#ifndef SOL_SCTP
#define SOL_SCTP	132
#endif

#ifndef IPPROTO_SCTP
#define IPPROTO_SCTP    132
#endif

/* 9. Preprocessor constants */
#define HAVE_SCTP
#define HAVE_KERNEL_SCTP
#define HAVE_SCTP_MULTIBUF
#define HAVE_SCTP_NOCONNECT
#define HAVE_SCTP_PRSCTP
#define HAVE_SCTP_ADDIP
#define HAVE_SCTP_CANSET_PRIMARY

#define HAVE_SCTP_STREAM_RESET_EVENT 1
#define HAVE_SCTP_STREAM_RECONFIG 1
#define HAVE_SCTP_PEELOFF_FLAGS 1
#define HAVE_SCTP_PDAPI_EVENT_PDAPI_STREAM 1
#define HAVE_SCTP_PDAPI_EVENT_PDAPI_SEQ 1
#define HAVE_SCTP_SENDV 1
#define HAVE_SCTP_AUTH_NO_AUTH 1
#define HAVE_SCTP_SPP_IPV6_FLOWLABEL 1
#define HAVE_SCTP_SPP_DSCP 1

int sctp_bindx(int sd, struct sockaddr *addrs, int addrcnt, int flags);

int sctp_connectx(int sd, struct sockaddr *addrs, int addrcnt,
		  sctp_assoc_t *id);

int sctp_peeloff(int sd, sctp_assoc_t assoc_id);
int sctp_peeloff_flags(int sd, sctp_assoc_t assoc_id, unsigned flags);

/* Prototype for the library function sctp_opt_info defined in
 * API 7. Socket Options.
 */
int sctp_opt_info(int sd, sctp_assoc_t id, int opt, void *arg, socklen_t *size);

/* Get all peer address on a socket.  This is a new SCTP API
 * described in the section 8.3 of the Sockets API Extensions for SCTP.
 * This is implemented using the getsockopt() interface.
 */
int sctp_getpaddrs(int sd, sctp_assoc_t id, struct sockaddr **addrs);

/* Frees all resources allocated by sctp_getpaddrs().  This is a new SCTP API
 * described in the section 8.4 of the Sockets API Extensions for SCTP.
 */
int sctp_freepaddrs(struct sockaddr *addrs);

/* Get all locally bound address on a socket.  This is a new SCTP API
 * described in the section 8.5 of the Sockets API Extensions for SCTP.
 * This is implemented using the getsockopt() interface.
 */
int sctp_getladdrs(int sd, sctp_assoc_t id, struct sockaddr **addrs);

/* Frees all resources allocated by sctp_getladdrs().  This is a new SCTP API
 * described in the section 8.6 of the Sockets API Extensions for SCTP.
 */
int sctp_freeladdrs(struct sockaddr *addrs);

/* This library function assists the user with the advanced features
 * of SCTP.  This is a new SCTP API described in the section 8.7 of the
 * Sockets API Extensions for SCTP. This is implemented using the
 * sendmsg() interface.
 */
int sctp_sendmsg(int s, const void *msg, size_t len, struct sockaddr *to,
		 socklen_t tolen, uint32_t ppid, uint32_t flags,
		 uint16_t stream_no, uint32_t timetolive, uint32_t context);

/* This library function assist the user with sending a message without
 * dealing directly with the CMSG header.
 */
int sctp_send(int s, const void *msg, size_t len,
              const struct sctp_sndrcvinfo *sinfo, int flags);

/* This library function assists the user with the advanced features
 * of SCTP.  This is a new SCTP API described in the section 8.8 of the
 * Sockets API Extensions for SCTP. This is implemented using the
 * recvmsg() interface.
 */
int sctp_recvmsg(int s, void *msg, size_t len, struct sockaddr *from,
		 socklen_t *fromlen, struct sctp_sndrcvinfo *sinfo,
		 int *msg_flags);

/* Return the address length for an address family. */
int sctp_getaddrlen(sa_family_t family);


#ifdef HAVE_SCTP_SENDV
/* sendv infotype */
enum {
	SCTP_SENDV_NOINFO,
	SCTP_SENDV_SNDINFO,
	SCTP_SENDV_PRINFO,
	SCTP_SENDV_AUTHINFO,
	SCTP_SENDV_SPA
};

/* sendv_flags */
#define SCTP_SEND_SNDINFO_VALID		0x1
#define SCTP_SEND_PRINFO_VALID		0x2
#define SCTP_SEND_AUTHINFO_VALID	0x4
#endif

struct sctp_sendv_spa {
	uint32_t sendv_flags;
	struct sctp_sndinfo sendv_sndinfo;
#ifdef HAVE_SCTP_SENDV
	struct sctp_prinfo sendv_prinfo;
	struct sctp_authinfo sendv_authinfo;
#endif
};

#ifdef HAVE_SCTP_SENDV
int sctp_sendv(int s, const struct iovec *iov, int iovcnt,
	       struct sockaddr *addrs, int addrcnt, void *info,
	       socklen_t infolen, unsigned int infotype, int flags);

/* recvv infotype */
enum {
	SCTP_RECVV_NOINFO,
	SCTP_RECVV_RCVINFO,
	SCTP_RECVV_NXTINFO,
	SCTP_RECVV_RN
};

struct sctp_recvv_rn {
	struct sctp_rcvinfo recvv_rcvinfo;
	struct sctp_nxtinfo recvv_nxtinfo;
};

int sctp_recvv(int s, const struct iovec *iov, int iovlen,
	       struct sockaddr *from, socklen_t *fromlen, void *info,
	       socklen_t *infolen, unsigned int *infotype, int *flags);
#endif

#ifdef __cplusplus
}
#endif

#endif /* __linux_sctp_h__ */
