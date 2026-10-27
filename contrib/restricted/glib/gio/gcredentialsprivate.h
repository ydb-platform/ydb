/* GIO - GLib Input, Output and Streaming Library
 *
 * Copyright 2013 Red Hat, Inc.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General
 * Public License along with this library; if not, see <http://www.gnu.org/licenses/>.
 */

#ifndef __G_CREDENTIALS_PRIVATE_H__
#define __G_CREDENTIALS_PRIVATE_H__

#include "gio/gcredentials.h"
#include "gio/gnetworking.h"

/*
 * G_CREDENTIALS_SUPPORTED:
 *
 * Defined to 1 if GCredentials works.
 */
#undef G_CREDENTIALS_SUPPORTED

/*
 * G_CREDENTIALS_USE_LINUX_UCRED, etc.:
 *
 * Defined to 1 if GCredentials uses Linux `struct ucred`, etc.
 */
#undef G_CREDENTIALS_USE_LINUX_UCRED
#undef G_CREDENTIALS_USE_FREEBSD_CMSGCRED
#undef G_CREDENTIALS_USE_NETBSD_UNPCBID
#undef G_CREDENTIALS_USE_OPENBSD_SOCKPEERCRED
#undef G_CREDENTIALS_USE_SOLARIS_UCRED
#undef G_CREDENTIALS_USE_APPLE_XUCRED
#undef G_CREDENTIALS_USE_WIN32_PID

/*
 * G_CREDENTIALS_NATIVE_TYPE:
 *
 * Defined to one of G_CREDENTIALS_TYPE_LINUX_UCRED, etc.
 */
#undef G_CREDENTIALS_NATIVE_TYPE

/*
 * G_CREDENTIALS_NATIVE_SIZE:
 *
 * Defined to the size of the %G_CREDENTIALS_NATIVE_TYPE
 */
#undef G_CREDENTIALS_NATIVE_SIZE

/*
 * G_CREDENTIALS_UNIX_CREDENTIALS_MESSAGE_SUPPORTED:
 *
 * Defined to 1 if we have a message-passing API in which credentials
 * are attached to a particular message, such as `SCM_CREDENTIALS` on Linux
 * or `SCM_CREDS` on FreeBSD.
 */
#undef G_CREDENTIALS_UNIX_CREDENTIALS_MESSAGE_SUPPORTED

/*
 * G_CREDENTIALS_SOCKET_GET_CREDENTIALS_SUPPORTED:
 *
 * Defined to 1 if we have a `getsockopt()`-style API in which one end of
 * a socket connection can directly query the credentials of the process
 * that initiated the other end, such as `getsockopt SO_PEERCRED` on Linux
 * or `getpeereid()` on multiple operating systems.
 */
#undef G_CREDENTIALS_SOCKET_GET_CREDENTIALS_SUPPORTED

/*
 * G_CREDENTIALS_SPOOFING_SUPPORTED:
 *
 * Defined to 1 if privileged processes can spoof their credentials when
 * using the message-passing API.
 */
#undef G_CREDENTIALS_SPOOFING_SUPPORTED

/*
 * G_CREDENTIALS_PREFER_MESSAGE_PASSING:
 *
 * Defined to 1 if the data structure transferred by the message-passing
 * API is strictly more informative than the one transferred by the
 * `getsockopt()`-style API, and hence should be preferred, even for
 * protocols like D-Bus that are defined in terms of the credentials of
 * the (process that opened the) socket, as opposed to the credentials
 * of an individual message.
 */
#undef G_CREDENTIALS_PREFER_MESSAGE_PASSING

/*
 * G_CREDENTIALS_HAS_PID:
 *
 * Defined to 1 if the %G_CREDENTIALS_NATIVE_TYPE contains the process ID.
 */
#undef G_CREDENTIALS_HAS_PID

#ifdef __linux__
#define G_CREDENTIALS_SUPPORTED 1
#define G_CREDENTIALS_USE_LINUX_UCRED 1
#define G_CREDENTIALS_NATIVE_TYPE G_CREDENTIALS_TYPE_LINUX_UCRED
#define G_CREDENTIALS_NATIVE_SIZE (sizeof (struct ucred))
#define G_CREDENTIALS_UNIX_CREDENTIALS_MESSAGE_SUPPORTED 1
#define G_CREDENTIALS_SOCKET_GET_CREDENTIALS_SUPPORTED 1
#define G_CREDENTIALS_SPOOFING_SUPPORTED 1
#define G_CREDENTIALS_HAS_PID 1

#elif defined(__FreeBSD__)                                  || \
      defined(__FreeBSD_kernel__) /* Debian GNU/kFreeBSD */ || \
      defined(__GNU__)            /* GNU Hurd */            || \
      defined(__DragonFly__)      /* DragonFly BSD */
#define G_CREDENTIALS_SUPPORTED 1
#define G_CREDENTIALS_USE_FREEBSD_CMSGCRED 1
#define G_CREDENTIALS_NATIVE_TYPE G_CREDENTIALS_TYPE_FREEBSD_CMSGCRED
#define G_CREDENTIALS_NATIVE_SIZE (sizeof (struct cmsgcred))
#define G_CREDENTIALS_UNIX_CREDENTIALS_MESSAGE_SUPPORTED 1
#define G_CREDENTIALS_SPOOFING_SUPPORTED 1
/* GLib doesn't implement it yet, but FreeBSD's getsockopt()-style API
 * is getpeereid(), which is not as informative as struct cmsgcred -
 * it does not tell us the PID. As a result, libdbus prefers to use
 * SCM_CREDS, and if we implement getpeereid() in future, we should
 * do the same. */
#define G_CREDENTIALS_PREFER_MESSAGE_PASSING 1
#define G_CREDENTIALS_HAS_PID 1

#elif defined(__NetBSD__)
#define G_CREDENTIALS_SUPPORTED 1
#define G_CREDENTIALS_USE_NETBSD_UNPCBID 1
#define G_CREDENTIALS_NATIVE_TYPE G_CREDENTIALS_TYPE_NETBSD_UNPCBID
#define G_CREDENTIALS_NATIVE_SIZE (sizeof (struct unpcbid))
/* #undef G_CREDENTIALS_UNIX_CREDENTIALS_MESSAGE_SUPPORTED */
#define G_CREDENTIALS_SPOOFING_SUPPORTED 1
#define G_CREDENTIALS_HAS_PID 1

#elif defined(__OpenBSD__)
#define G_CREDENTIALS_SUPPORTED 1
#define G_CREDENTIALS_USE_OPENBSD_SOCKPEERCRED 1
#define G_CREDENTIALS_NATIVE_TYPE G_CREDENTIALS_TYPE_OPENBSD_SOCKPEERCRED
#define G_CREDENTIALS_NATIVE_SIZE (sizeof (struct sockpeercred))
#define G_CREDENTIALS_SOCKET_GET_CREDENTIALS_SUPPORTED 1
#define G_CREDENTIALS_SPOOFING_SUPPORTED 1
#define G_CREDENTIALS_HAS_PID 1

#elif defined(__sun__) || defined(__illumos__) || defined (__OpenSolaris_kernel__)
#include <ucred.h>
#define G_CREDENTIALS_SUPPORTED 1
#define G_CREDENTIALS_USE_SOLARIS_UCRED 1
#define G_CREDENTIALS_NATIVE_TYPE G_CREDENTIALS_TYPE_SOLARIS_UCRED
#define G_CREDENTIALS_NATIVE_SIZE (ucred_size ())
#define G_CREDENTIALS_UNIX_CREDENTIALS_MESSAGE_SUPPORTED 1
#define G_CREDENTIALS_SOCKET_GET_CREDENTIALS_SUPPORTED 1
#define G_CREDENTIALS_HAS_PID 1

#elif defined(__APPLE__)
#include <sys/ucred.h>
#define G_CREDENTIALS_SUPPORTED 1
#define G_CREDENTIALS_USE_APPLE_XUCRED 1
#define G_CREDENTIALS_NATIVE_TYPE G_CREDENTIALS_TYPE_APPLE_XUCRED
#define G_CREDENTIALS_NATIVE_SIZE (sizeof (struct xucred))
#undef G_CREDENTIALS_UNIX_CREDENTIALS_MESSAGE_SUPPORTED
#define G_CREDENTIALS_SOCKET_GET_CREDENTIALS_SUPPORTED 1
#define G_CREDENTIALS_SPOOFING_SUPPORTED 1
#define G_CREDENTIALS_HAS_PID 0

void _g_credentials_set_local_peerid (GCredentials *credentials,
                                      pid_t         pid);

#elif defined(_WIN32)
#define G_CREDENTIALS_SUPPORTED 1
#define G_CREDENTIALS_USE_WIN32_PID 1
#define G_CREDENTIALS_NATIVE_TYPE G_CREDENTIALS_TYPE_WIN32_PID
#define G_CREDENTIALS_NATIVE_SIZE (sizeof (DWORD))
#define G_CREDENTIALS_SOCKET_GET_CREDENTIALS_SUPPORTED 1
#define G_CREDENTIALS_HAS_PID 1

#endif

#endif /* __G_CREDENTIALS_PRIVATE_H__ */
