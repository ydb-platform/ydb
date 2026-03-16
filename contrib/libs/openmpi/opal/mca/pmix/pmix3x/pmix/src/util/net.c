/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007      Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2009-2015 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2013-2018 Intel, Inc. All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "pmix_config.h"
#include "pmix_common.h"

#include <stdio.h>
#include <string.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#include <errno.h>
#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
#ifdef HAVE_SYS_SOCKET_H
#include <sys/socket.h>
#endif
#ifdef HAVE_SYS_SOCKIO_H
#include <sys/sockio.h>
#endif
#ifdef HAVE_SYS_IOCTL_H
#include <sys/ioctl.h>
#endif
#ifdef HAVE_NETINET_IN_H
#include <netinet/in.h>
#endif
#ifdef HAVE_ARPA_INET_H
#include <arpa/inet.h>
#endif
#ifdef HAVE_NET_IF_H
#include <net/if.h>
#endif
#ifdef HAVE_NETDB_H
#include <netdb.h>
#endif
#ifdef HAVE_IFADDRS_H
#include <ifaddrs.h>
#endif

#include "src/util/net.h"
#include "src/util/output.h"
#include "src/util/argv.h"
#include "src/util/show_help.h"
#include "src/threads/tsd.h"
#include "src/runtime/pmix_rte.h"

/* this function doesn't depend on sockaddr_h */
bool pmix_net_isaddr(const char *name)
{
    struct addrinfo hint, *res = NULL;

    /* initialize the hint */
    memset(&hint, '\0', sizeof hint);

    /* indicate that we don't know the family */
    hint.ai_family = PF_UNSPEC;
    hint.ai_flags = AI_NUMERICHOST;

    if (0 != getaddrinfo(name, NULL, &hint, &res)) {
        /* the input wasn't a recognizable address */
        return false;
    }
    /* we don't care what family - all we care is that
     * it is indeed an address
     */
    freeaddrinfo(res);
    return true;
}

#ifdef HAVE_STRUCT_SOCKADDR_IN

typedef struct private_ipv4_t {
    in_addr_t addr;
    uint32_t netmask_bits;
} private_ipv4_t;

static private_ipv4_t* private_ipv4 = NULL;

static pmix_tsd_key_t hostname_tsd_key;


static void
hostname_cleanup(void *value)
{
    if (NULL != value) free(value);
}


static char*
get_hostname_buffer(void)
{
    void *buffer;
    int ret;

    ret = pmix_tsd_getspecific(hostname_tsd_key, &buffer);
    if (PMIX_SUCCESS != ret) return NULL;

    if (NULL == buffer) {
        buffer = (void*) malloc((NI_MAXHOST + 1) * sizeof(char));
        ret = pmix_tsd_setspecific(hostname_tsd_key, buffer);
    }

    return (char*) buffer;
}

int
pmix_net_init(void)
{
    char **args, *arg;
    uint32_t a, b, c, d, bits, addr;
    int i, count, found_bad = 0;

    args = pmix_argv_split(pmix_net_private_ipv4, ';');
    if( NULL != args ) {
        count = pmix_argv_count(args);
        private_ipv4 = (private_ipv4_t*)malloc( (count + 1) * sizeof(private_ipv4_t));
        if( NULL == private_ipv4 ) {
            pmix_output(0, "Unable to allocate memory for the private addresses array" );
            pmix_argv_free(args);
            goto do_local_init;
        }
        for( i = 0; i < count; i++ ) {
            arg = args[i];

            (void)sscanf( arg, "%u.%u.%u.%u/%u", &a, &b, &c, &d, &bits );

            if( (a > 255) || (b > 255) || (c > 255) ||
                (d > 255) || (bits > 32) ) {
                if (0 == found_bad) {
                    pmix_show_help("help-pmix-runtime.txt",
                                   "malformed net_private_ipv4",
                                   true, args[i]);
                    found_bad = 1;
                }
                continue;
            }
            addr = (a << 24) | (b << 16) | (c << 8) | d;
            private_ipv4[i].addr = htonl(addr);
            private_ipv4[i].netmask_bits = bits;
        }
        private_ipv4[i].addr         = 0;
        private_ipv4[i].netmask_bits = 0;
        pmix_argv_free(args);
    }

 do_local_init:
    return pmix_tsd_key_create(&hostname_tsd_key, hostname_cleanup);
}


int
pmix_net_finalize()
{
    free(private_ipv4);
    private_ipv4 = NULL;

    return PMIX_SUCCESS;
}


/* convert a CIDR prefixlen to netmask (in network byte order) */
uint32_t
pmix_net_prefix2netmask(uint32_t prefixlen)
{
    return htonl (((1 << prefixlen) - 1) << (32 - prefixlen));
}


bool
pmix_net_islocalhost(const struct sockaddr *addr)
{
    switch (addr->sa_family) {
    case AF_INET:
        {
            const struct sockaddr_in *inaddr = (struct sockaddr_in*) addr;
            /* if it's in the 127. domain, it shouldn't be routed
               (0x7f == 127) */
            if (0x7F000000 == (0x7F000000 & ntohl(inaddr->sin_addr.s_addr))) {
                return true;
            }
            return false;
        }
        break;

    case AF_INET6:
        {
            const struct sockaddr_in6 *inaddr = (struct sockaddr_in6*) addr;
            if (IN6_IS_ADDR_LOOPBACK (&inaddr->sin6_addr)) {
               return true; /* Bug, FIXME: check for 127.0.0.1/8 */
            }
            return false;
        }
        break;

    default:
        pmix_output(0, "unhandled sa_family %d passed to pmix_net_islocalhost",
                    addr->sa_family);
        return false;
        break;
    }
}


bool
pmix_net_samenetwork(const struct sockaddr *addr1,
                     const struct sockaddr *addr2,
                     uint32_t plen)
{
    uint32_t prefixlen;

    if(addr1->sa_family != addr2->sa_family) {
        return false; /* address families must be equal */
    }

    switch (addr1->sa_family) {
    case AF_INET:
        {
            if (0 == plen) {
                prefixlen = 32;
            } else {
                prefixlen = plen;
            }
            struct sockaddr_in inaddr1, inaddr2;
            /* Use temporary variables and memcpy's so that we don't
               run into bus errors on Solaris/SPARC */
            memcpy(&inaddr1, addr1, sizeof(inaddr1));
            memcpy(&inaddr2, addr2, sizeof(inaddr2));
            uint32_t netmask = pmix_net_prefix2netmask (prefixlen);

            if((inaddr1.sin_addr.s_addr & netmask) ==
               (inaddr2.sin_addr.s_addr & netmask)) {
                return true;
            }
            return false;
        }
        break;

    case AF_INET6:
        {
            struct sockaddr_in6 inaddr1, inaddr2;
            /* Use temporary variables and memcpy's so that we don't
               run into bus errors on Solaris/SPARC */
            memcpy(&inaddr1, addr1, sizeof(inaddr1));
            memcpy(&inaddr2, addr2, sizeof(inaddr2));
            struct in6_addr *a6_1 = (struct in6_addr*) &inaddr1.sin6_addr;
            struct in6_addr *a6_2 = (struct in6_addr*) &inaddr2.sin6_addr;

            if (0 == plen) {
                prefixlen = 64;
            } else {
                prefixlen = plen;
            }
            if (64 == prefixlen) {
                /* prefixlen is always /64, any other case would be routing.
                   Compare the first eight bytes (64 bits) and hope that
                   endianess is not an issue on any system as long as
                   addresses are always stored in network byte order.
                */
                if (((const uint32_t *) (a6_1))[0] ==
                    ((const uint32_t *) (a6_2))[0] &&
                    ((const uint32_t *) (a6_1))[1] ==
                    ((const uint32_t *) (a6_2))[1]) {
                    return true;
                }
            }
            return false;
        }
        break;

    default:
        pmix_output(0, "unhandled sa_family %d passed to pmix_samenetwork",
                    addr1->sa_family);
    }

    return false;
}


/**
 * Returns true if the given address is a public IPv4 address.
 */
bool
pmix_net_addr_isipv4public(const struct sockaddr *addr)
{
    switch (addr->sa_family) {
        case AF_INET6:
            return false;

        case AF_INET:
            {
                const struct sockaddr_in *inaddr = (struct sockaddr_in*) addr;
                int i;

                if( NULL == private_ipv4 ) {
                    return true;
                }

                for( i = 0; private_ipv4[i].addr != 0; i++ ) {
                    if( private_ipv4[i].addr == (inaddr->sin_addr.s_addr &
                                                 pmix_net_prefix2netmask(private_ipv4[i].netmask_bits)) )
                        return false;
                }

            }
            return true;
        default:
            pmix_output (0,
                         "unhandled sa_family %d passed to pmix_net_addr_isipv4public\n",
                         addr->sa_family);
    }

    return false;
}


char*
pmix_net_get_hostname(const struct sockaddr *addr)
{
    char *name = get_hostname_buffer();
    int error;
    socklen_t addrlen;
    char *p;

    if (NULL == name) {
        pmix_output(0, "pmix_sockaddr2str: malloc() failed\n");
        return NULL;
    }
    memset(name, 0, sizeof(*name));

    switch (addr->sa_family) {
    case AF_INET:
        addrlen = sizeof (struct sockaddr_in);
        break;
    case AF_INET6:
#if defined( __NetBSD__)
        /* hotfix for netbsd: on my netbsd machine, getnameinfo
           returns an unkown error code. */
        if(NULL == inet_ntop(AF_INET6, &((struct sockaddr_in6*) addr)->sin6_addr,
                             name, NI_MAXHOST)) {
            pmix_output(0, "pmix_sockaddr2str failed with error code %d", errno);
            free(name);
            return NULL;
        }
        return name;
#else
        addrlen = sizeof (struct sockaddr_in6);
#endif
        break;
    default:
        free(name);
        return NULL;
    }

    error = getnameinfo(addr, addrlen,
                        name, NI_MAXHOST, NULL, 0, NI_NUMERICHOST);

    if (error) {
       int err = errno;
       pmix_output (0, "pmix_sockaddr2str failed:%s (return code %i)\n",
                    gai_strerror(err), error);
       free (name);
       return NULL;
    }
    /* strip any trailing % data as it isn't pertinent */
    if (NULL != (p = strrchr(name, '%'))) {
        *p = '\0';
    }
    return name;
}


int
pmix_net_get_port(const struct sockaddr *addr)
{
    switch (addr->sa_family) {
    case AF_INET:
        return ntohs(((struct sockaddr_in*) addr)->sin_port);
        break;

    case AF_INET6:
        return ntohs(((struct sockaddr_in6*) addr)->sin6_port);
        break;
    }

    return -1;
}


#else /* HAVE_STRUCT_SOCKADDR_IN */

int
pmix_net_init()
{
    return PMIX_SUCCESS;
}


int
pmix_net_finalize()
{
    return PMIX_SUCCESS;
}


uint32_t
pmix_net_prefix2netmask(uint32_t prefixlen)
{
    return 0;
}


bool
pmix_net_islocalhost(const struct sockaddr *addr)
{
    return false;
}


bool
pmix_net_samenetwork(const struct sockaddr *addr1,
                     const struct sockaddr *addr2,
                     uint32_t prefixlen)
{
    return false;
}


bool
pmix_net_addr_isipv4public(const struct sockaddr *addr)
{
    return false;
}


char*
pmix_net_get_hostname(const struct sockaddr *addr)
{
    return NULL;
}


int
pmix_net_get_port(const struct sockaddr *addr)
{
    return -1;
}

#endif /* HAVE_STRUCT_SOCKADDR_IN */
