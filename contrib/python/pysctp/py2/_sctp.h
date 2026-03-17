/* SCTP bindings for Python
 *
 * _sctp.h: paliative C-side definitions
 * 
 * This library is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as published by the
 * Free Software Foundation; either version 2.1 of the License, or (at your
 * option) any later version.
 *
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library; If not, see <http://www.gnu.org/licenses/>.
 *
 * Philippe Langlois (Philippe.Langlois@gmail.com)
 * Copyright (c) 2009 Philippe Langlois
 */

#ifndef IPPROTO_SCTP
#define IPPROTO_SCTP					132
#endif // IPPROTO_SCTP

#ifndef SOL_SCTP
#define SOL_SCTP						132
#endif

#ifndef MSG_FIN
#define MSG_FIN							0x200
#endif

#ifndef MSG_NOTIFICATION
#define MSG_NOTIFICATION 				0x8000
#endif

/* Notification error codes */
#ifndef SCTP_NOTIFY_DATAGRAM_UNSENT
#define SCTP_NOTIFY_DATAGRAM_UNSENT     0x0001
#endif
#ifndef SCTP_NOTIFY_DATAGRAM_SENT  
#define SCTP_NOTIFY_DATAGRAM_SENT       0x0002
#endif
#ifndef SCTP_FAILED_THRESHOLD
#define SCTP_FAILED_THRESHOLD           0x0004
#endif
#ifndef SCTP_HEARTBEAT_SUCCESS
#define SCTP_HEARTBEAT_SUCCESS          0x0008
#endif
#ifndef SCTP_RESPONSE_TO_USER_REQ
#define SCTP_RESPONSE_TO_USER_REQ       0x000f
#endif
#ifndef SCTP_INTERNAL_ERROR
#define SCTP_INTERNAL_ERROR             0x0010
#endif
#ifndef SCTP_SHUTDOWN_GUARD_EXPIRES
#define SCTP_SHUTDOWN_GUARD_EXPIRES     0x0020
#endif
#ifndef SCTP_RECEIVED_SACK
#define SCTP_RECEIVED_SACK              0x0040
#endif
#ifndef SCTP_PEER_FAULTY
#define SCTP_PEER_FAULTY                0x0080
#endif

/* SCTP Association states. */
#ifndef SCTP_EMPTY
#define SCTP_EMPTY						0
#endif
#ifndef SCTP_CLOSED
#define SCTP_CLOSED						1
#endif

#ifndef SCTP_SN_TYPE_BASE
#define SCTP_SN_TYPE_BASE				(1<<15)
#endif

#ifndef linux
/*
 * 7.1.10 Set Primary Address (SCTP_PRIMARY_ADDR)
 *
 *  Requests that the local SCTP stack use the enclosed peer address as
 *  the association primary. The enclosed address must be one of the
 *  association peer's addresses. The following structure is used to
 *  make a set peer primary request:
 */
struct sctp_prim {
        sctp_assoc_t            ssp_assoc_id;
        struct sockaddr_storage ssp_addr;
};
#endif
