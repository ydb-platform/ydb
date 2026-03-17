//
// SocketDefs.h
//
// Library: Net
// Package: NetCore
// Module:  SocketDefs
//
// Include platform-specific header files for sockets.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef CHDB_Net_SocketDefs_INCLUDED
#define CHDB_Net_SocketDefs_INCLUDED


#define CHDB_POCO_ENOERR 0


#if   defined(CHDB_POCO_OS_FAMILY_UNIX)
#    include <unistd.h>
#    include <errno.h>
#    include <sys/types.h>
#    include <sys/socket.h>
#    include <sys/un.h>
#    include <fcntl.h>
#    if CHDB_POCO_OS != CHDB_POCO_OS_HPUX
#        include <sys/select.h>
#    endif
#    include <sys/ioctl.h>
#    include <arpa/inet.h>
#    include <netinet/in.h>
#    include <netinet/tcp.h>
#    include <netdb.h>
#    if defined(CHDB_POCO_OS_FAMILY_UNIX)
#        if (CHDB_POCO_OS == CHDB_POCO_OS_LINUX) || (CHDB_POCO_OS == CHDB_POCO_OS_ANDROID)
// Net/src/NetworkInterface.cpp changed #include <linux/if.h> to #include <net/if.h>
// no more conflict, can use #include <net/if.h> here
#            include <net/if.h>
#        elif (CHDB_POCO_OS == CHDB_POCO_OS_HPUX)
extern "C" {
#            include <net/if.h>
}
#        else
#            include <net/if.h>
#        endif
#    endif
#    if (CHDB_POCO_OS == CHDB_POCO_OS_SOLARIS) || (CHDB_POCO_OS == CHDB_POCO_OS_MAC_OS_X)
#        include <sys/sockio.h>
#        include <sys/filio.h>
#    endif
#    define CHDB_POCO_INVALID_SOCKET -1
#    define CHDB_poco_socket_t int
#    define CHDB_poco_socklen_t socklen_t
#    define CHDB_poco_fcntl_request_t int
#    if defined(CHDB_POCO_OS_FAMILY_BSD)
#        define CHDB_poco_ioctl_request_t unsigned long
#    else
#        define CHDB_poco_ioctl_request_t int
#    endif
#    define CHDB_poco_closesocket(s) ::close(s)
#    define CHDB_POCO_EINTR EINTR
#    define CHDB_POCO_EACCES EACCES
#    define CHDB_POCO_EFAULT EFAULT
#    define CHDB_POCO_EINVAL EINVAL
#    define CHDB_POCO_EMFILE EMFILE
#    define CHDB_POCO_EAGAIN EAGAIN
#    define CHDB_POCO_EWOULDBLOCK EWOULDBLOCK
#    define CHDB_POCO_EINPROGRESS EINPROGRESS
#    define CHDB_POCO_EALREADY EALREADY
#    define CHDB_POCO_ENOTSOCK ENOTSOCK
#    define CHDB_POCO_EDESTADDRREQ EDESTADDRREQ
#    define CHDB_POCO_EMSGSIZE EMSGSIZE
#    define CHDB_POCO_EPROTOTYPE EPROTOTYPE
#    define CHDB_POCO_ENOPROTOOPT ENOPROTOOPT
#    define CHDB_POCO_EPROTONOSUPPORT EPROTONOSUPPORT
#    if defined(ESOCKTNOSUPPORT)
#        define CHDB_POCO_ESOCKTNOSUPPORT ESOCKTNOSUPPORT
#    else
#        define CHDB_POCO_ESOCKTNOSUPPORT -1
#    endif
#    define CHDB_POCO_ENOTSUP ENOTSUP
#    define CHDB_POCO_EPFNOSUPPORT EPFNOSUPPORT
#    define CHDB_POCO_EAFNOSUPPORT EAFNOSUPPORT
#    define CHDB_POCO_EADDRINUSE EADDRINUSE
#    define CHDB_POCO_EADDRNOTAVAIL EADDRNOTAVAIL
#    define CHDB_POCO_ENETDOWN ENETDOWN
#    define CHDB_POCO_ENETUNREACH ENETUNREACH
#    define CHDB_POCO_ENETRESET ENETRESET
#    define CHDB_POCO_ECONNABORTED ECONNABORTED
#    define CHDB_POCO_ECONNRESET ECONNRESET
#    define CHDB_POCO_ENOBUFS ENOBUFS
#    define CHDB_POCO_EISCONN EISCONN
#    define CHDB_POCO_ENOTCONN ENOTCONN
#    if defined(ESHUTDOWN)
#        define CHDB_POCO_ESHUTDOWN ESHUTDOWN
#    else
#        define CHDB_POCO_ESHUTDOWN -2
#    endif
#    define CHDB_POCO_ETIMEDOUT ETIMEDOUT
#    define CHDB_POCO_ECONNREFUSED ECONNREFUSED
#    if defined(EHOSTDOWN)
#        define CHDB_POCO_EHOSTDOWN EHOSTDOWN
#    else
#        define CHDB_POCO_EHOSTDOWN -3
#    endif
#    define CHDB_POCO_EHOSTUNREACH EHOSTUNREACH
#    define CHDB_POCO_ESYSNOTREADY -4
#    define CHDB_POCO_ENOTINIT -5
#    define CHDB_POCO_HOST_NOT_FOUND HOST_NOT_FOUND
#    define CHDB_POCO_TRY_AGAIN TRY_AGAIN
#    define CHDB_POCO_NO_RECOVERY NO_RECOVERY
#    define CHDB_POCO_NO_DATA NO_DATA
#endif


#if defined(CHDB_POCO_OS_FAMILY_BSD) || (CHDB_POCO_OS == CHDB_POCO_OS_TRU64) || (CHDB_POCO_OS == CHDB_POCO_OS_AIX) || (CHDB_POCO_OS == CHDB_POCO_OS_IRIX) \
    || (CHDB_POCO_OS == CHDB_POCO_OS_QNX) || (CHDB_POCO_OS == CHDB_POCO_OS_VXWORKS)
#    define CHDB_POCO_HAVE_SALEN 1
#endif


#if CHDB_POCO_OS != CHDB_POCO_OS_VXWORKS && !defined(POCO_NET_NO_ADDRINFO)
#    define CHDB_POCO_HAVE_ADDRINFO 1
#endif

/// Without this option, Poco library will restart recv after EINTR,
/// but it doesn't update socket receive timeout that leads to infinite wait
/// when query profiler is activated. The issue persisted in delayed_replica_failover
/// integration test after we enabled query profiler by default.
#define CHDB_POCO_BROKEN_TIMEOUTS 1


#if defined(CHDB_POCO_HAVE_ADDRINFO)
#    ifndef AI_PASSIVE
#        define AI_PASSIVE 0
#    endif
#    ifndef AI_CANONNAME
#        define AI_CANONNAME 0
#    endif
#    ifndef AI_NUMERICHOST
#        define AI_NUMERICHOST 0
#    endif
#    ifndef AI_NUMERICSERV
#        define AI_NUMERICSERV 0
#    endif
#    ifndef AI_ALL
#        define AI_ALL 0
#    endif
#    ifndef AI_ADDRCONFIG
#        define AI_ADDRCONFIG 0
#    endif
#    ifndef AI_V4MAPPED
#        define AI_V4MAPPED 0
#    endif
#endif


#if defined(CHDB_POCO_HAVE_SALEN)
#    define CHDB_poco_set_sa_len(pSA, len) (pSA)->sa_len = (len)
#    define CHDB_poco_set_sin_len(pSA) (pSA)->sin_len = sizeof(struct sockaddr_in)
#    if defined(CHDB_POCO_HAVE_IPv6)
#        define CHDB_poco_set_sin6_len(pSA) (pSA)->sin6_len = sizeof(struct sockaddr_in6)
#    endif
#    if defined(CHDB_POCO_OS_FAMILY_UNIX)
#        define CHDB_poco_set_sun_len(pSA, len) (pSA)->sun_len = (len)
#    endif
#else
#    define CHDB_poco_set_sa_len(pSA, len) (void)0
#    define CHDB_poco_set_sin_len(pSA) (void)0
#    define CHDB_poco_set_sin6_len(pSA) (void)0
#    define CHDB_poco_set_sun_len(pSA, len) (void)0
#endif


#ifndef INADDR_NONE
#    define INADDR_NONE 0xffffffff
#endif

#ifndef INADDR_ANY
#    define INADDR_ANY 0x00000000
#endif

#ifndef INADDR_BROADCAST
#    define INADDR_BROADCAST 0xffffffff
#endif

#ifndef INADDR_LOOPBACK
#    define INADDR_LOOPBACK 0x7f000001
#endif

#ifndef INADDR_UNSPEC_GROUP
#    define INADDR_UNSPEC_GROUP 0xe0000000
#endif

#ifndef INADDR_ALLHOSTS_GROUP
#    define INADDR_ALLHOSTS_GROUP 0xe0000001
#endif

#ifndef INADDR_ALLRTRS_GROUP
#    define INADDR_ALLRTRS_GROUP 0xe0000002
#endif

#ifndef INADDR_MAX_LOCAL_GROUP
#    define INADDR_MAX_LOCAL_GROUP 0xe00000ff
#endif

#if defined(CHDB_POCO_ARCH_BIG_ENDIAN)
#    define CHDB_poco_ntoh_16(x) (x)
#    define CHDB_poco_ntoh_32(x) (x)
#else
#    define CHDB_poco_ntoh_16(x) ((((x) >> 8) & 0x00ff) | (((x) << 8) & 0xff00))
#    define CHDB_poco_ntoh_32(x) \
        ((((x) >> 24) & 0x000000ff) | (((x) >> 8) & 0x0000ff00) | (((x) << 8) & 0x00ff0000) | (((x) << 24) & 0xff000000))
#endif
#define CHDB_poco_hton_16(x) CHDB_poco_ntoh_16(x)
#define CHDB_poco_hton_32(x) CHDB_poco_ntoh_32(x)


#if !defined(s6_addr16)
#        define s6_addr16 __u6_addr.__u6_addr16
#endif


#if !defined(s6_addr32)
#    if defined(CHDB_POCO_OS_FAMILY_UNIX)
#        if (CHDB_POCO_OS == CHDB_POCO_OS_SOLARIS)
#            define s6_addr32 _S6_un._S6_u32
#        else
#            define s6_addr32 __u6_addr.__u6_addr32
#        endif
#    endif
#endif


namespace CHDBPoco
{
namespace Net
{


    struct AddressFamily
    /// AddressFamily::Family replaces the previously used IPAddress::Family
    /// enumeration and is now used for IPAddress::Family and SocketAddress::Family.
    {
        enum Family
        /// Possible address families for socket addresses.
        {
            IPv4,
        /// IPv4 address family.
#if defined(CHDB_POCO_HAVE_IPv6)
            IPv6,
        /// IPv6 address family.
#endif
#if defined(CHDB_POCO_OS_FAMILY_UNIX)
            UNIX_LOCAL
        /// UNIX domain socket address family. Available on UNIX/POSIX platforms only.
#endif
        };
    };


}
} // namespace CHDBPoco::Net


#endif // CHDB_Net_SocketDefs_INCLUDED
