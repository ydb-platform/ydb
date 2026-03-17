/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2009 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2008      Sun Microsystems, Inc.  All rights reserved.
 * Copyright (c) 2010-2015 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2014-2017 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2015-2016 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"

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
#if defined(__APPLE__) && defined(_LP64)
/* Apple engineering suggested using options align=power as a
   workaround for a bug in OS X 10.4 (Tiger) that prevented ioctl(...,
   SIOCGIFCONF, ...) from working properly in 64 bit mode on Power PC.
   It turns out that the underlying issue is the size of struct
   ifconf, which the kernel expects to be 12 and natural 64 bit
   alignment would make 16.  The same bug appears in 64 bit mode on
   Intel macs, but align=power is a no-op there, so instead, use the
   pack pragma to instruct the compiler to pack on 4 byte words, which
   has the same effect as align=power for our needs and works on both
   Intel and Power PC Macs. */
#pragma pack(push,4)
#endif
#include <net/if.h>
#if defined(__APPLE__) && defined(_LP64)
#pragma pack(pop)
#endif
#endif
#ifdef HAVE_NETDB_H
#include <netdb.h>
#endif
#ifdef HAVE_IFADDRS_H
#include <ifaddrs.h>
#endif
#include <ctype.h>

#include "opal/class/opal_list.h"
#include "opal/util/if.h"
#include "opal/util/net.h"
#include "opal/util/output.h"
#include "opal/util/argv.h"
#include "opal/util/show_help.h"
#include "opal/constants.h"

#include "opal/mca/if/base/base.h"

#ifdef HAVE_STRUCT_SOCKADDR_IN

#ifndef MIN
#  define MIN(a,b)                ((a) < (b) ? (a) : (b))
#endif

/*
 *  Look for interface by name and returns its address
 *  as a dotted decimal formatted string.
 */

int opal_ifnametoaddr(const char* if_name, struct sockaddr* addr, int length)
{
    opal_if_t* intf;

    OPAL_LIST_FOREACH(intf, &opal_if_list, opal_if_t) {
        if (strcmp(intf->if_name, if_name) == 0) {
            memcpy(addr, &intf->if_addr, length);
            return OPAL_SUCCESS;
        }
    }
    return OPAL_ERROR;
}


/*
 *  Look for interface by name and returns its
 *  corresponding opal_list index.
 */

int opal_ifnametoindex(const char* if_name)
{
    opal_if_t* intf;

    OPAL_LIST_FOREACH(intf, &opal_if_list, opal_if_t) {
        if (strcmp(intf->if_name, if_name) == 0) {
            return intf->if_index;
        }
    }
    return -1;
}


/*
 *  Look for interface by name and returns its
 *  corresponding kernel index.
 */

int opal_ifnametokindex(const char* if_name)
{
    opal_if_t* intf;

    OPAL_LIST_FOREACH(intf, &opal_if_list, opal_if_t) {
        if (strcmp(intf->if_name, if_name) == 0) {
            return intf->if_kernel_index;
        }
    }
    return -1;
}


/*
 *  Look for interface by opal_list index and returns its
 *  corresponding kernel index.
 */

int opal_ifindextokindex(int if_index)
{
    opal_if_t* intf;

    OPAL_LIST_FOREACH(intf, &opal_if_list, opal_if_t) {
        if (if_index == intf->if_index) {
            return intf->if_kernel_index;
        }
    }
    return -1;
}


/*
 *  Attempt to resolve the adddress (given as either IPv4/IPv6 string
 *  or hostname) and lookup corresponding interface.
 */

int opal_ifaddrtoname(const char* if_addr, char* if_name, int length)
{
    opal_if_t* intf;
    int error;
    struct addrinfo hints, *res = NULL, *r;

    /* if the user asked us not to resolve interfaces, then just return */
    if (opal_if_do_not_resolve) {
        /* return not found so ifislocal will declare
         * the node to be non-local
         */
        return OPAL_ERR_NOT_FOUND;
    }

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = PF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    error = getaddrinfo(if_addr, NULL, &hints, &res);

    if (error) {
        if (NULL != res) {
            freeaddrinfo (res);
        }
        return OPAL_ERR_NOT_FOUND;
    }

    for (r = res; r != NULL; r = r->ai_next) {
        OPAL_LIST_FOREACH(intf, &opal_if_list, opal_if_t) {
            if (AF_INET == r->ai_family) {
                struct sockaddr_in ipv4;
                struct sockaddr_in *inaddr;

                inaddr = (struct sockaddr_in*) &intf->if_addr;
                memcpy (&ipv4, r->ai_addr, r->ai_addrlen);

                if (inaddr->sin_addr.s_addr == ipv4.sin_addr.s_addr) {
                    strncpy(if_name, intf->if_name, length);
                    freeaddrinfo (res);
                    return OPAL_SUCCESS;
                }
            }
#if OPAL_ENABLE_IPV6
            else {
                if (IN6_ARE_ADDR_EQUAL(&((struct sockaddr_in6*) &intf->if_addr)->sin6_addr,
                    &((struct sockaddr_in6*) r->ai_addr)->sin6_addr)) {
                    strncpy(if_name, intf->if_name, length);
                    freeaddrinfo (res);
                    return OPAL_SUCCESS;
                }
            }
#endif
        }
    }
    if (NULL != res) {
        freeaddrinfo (res);
    }

    /* if we get here, it wasn't found */
    return OPAL_ERR_NOT_FOUND;
}

/*
 *  Attempt to resolve the address (given as either IPv4/IPv6 string
 *  or hostname) and return the kernel index of the interface
 *  on the same network as the specified address
 */
int opal_ifaddrtokindex(const char* if_addr)
{
    opal_if_t* intf;
    int error;
    struct addrinfo hints, *res = NULL, *r;
    int if_kernel_index;
    size_t len;

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = PF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    error = getaddrinfo(if_addr, NULL, &hints, &res);

    if (error) {
        if (NULL != res) {
            freeaddrinfo (res);
        }
        return OPAL_ERR_NOT_FOUND;
    }

    for (r = res; r != NULL; r = r->ai_next) {
        OPAL_LIST_FOREACH(intf, &opal_if_list, opal_if_t) {
            if (AF_INET == r->ai_family && AF_INET == intf->af_family) {
                struct sockaddr_in ipv4;
                len = (r->ai_addrlen < sizeof(struct sockaddr_in)) ? r->ai_addrlen : sizeof(struct sockaddr_in);
                memcpy(&ipv4, r->ai_addr, len);
                if (opal_net_samenetwork((struct sockaddr*)&ipv4, (struct sockaddr*)&intf->if_addr, intf->if_mask)) {
                    if_kernel_index = intf->if_kernel_index;
                    freeaddrinfo (res);
                    return if_kernel_index;
                }
            }
#if OPAL_ENABLE_IPV6
            else if (AF_INET6 == r->ai_family && AF_INET6 == intf->af_family) {
                struct sockaddr_in6 ipv6;
                len = (r->ai_addrlen < sizeof(struct sockaddr_in6)) ? r->ai_addrlen : sizeof(struct sockaddr_in6);
                memcpy(&ipv6, r->ai_addr, len);
                if (opal_net_samenetwork((struct sockaddr*)((struct sockaddr_in6*)&intf->if_addr),
                                         (struct sockaddr*)&ipv6, intf->if_mask)) {
                    if_kernel_index = intf->if_kernel_index;
                    freeaddrinfo (res);
                    return if_kernel_index;
                }
            }
#endif
        }
    }
    if (NULL != res) {
        freeaddrinfo (res);
    }
    return OPAL_ERR_NOT_FOUND;
}

/*
 *  Return the number of discovered interface.
 */

int opal_ifcount(void)
{
    return opal_list_get_size(&opal_if_list);
}


/*
 *  Return the opal_list interface index for the first
 *  interface in our list.
 */

int opal_ifbegin(void)
{
    opal_if_t *intf;

    intf = (opal_if_t*)opal_list_get_first(&opal_if_list);
    if (NULL != intf)
        return intf->if_index;
    return (-1);
}


/*
 *  Located the current position in the list by if_index and
 *  return the interface index of the next element in our list
 *  (if it exists).
 */

int opal_ifnext(int if_index)
{
    opal_if_t *intf;

    OPAL_LIST_FOREACH(intf, &opal_if_list, opal_if_t) {
        if (intf->if_index == if_index) {
            do {
                opal_if_t* if_next = (opal_if_t*)opal_list_get_next(intf);
                opal_if_t* if_end =  (opal_if_t*)opal_list_get_end(&opal_if_list);
                if (if_next == if_end) {
                    return -1;
                }
                intf = if_next;
            } while(intf->if_index == if_index);
            return intf->if_index;
        }
    }
    return (-1);
}


/*
 *  Lookup the interface by opal_list index and return the
 *  primary address assigned to the interface.
 */

int opal_ifindextoaddr(int if_index, struct sockaddr* if_addr, unsigned int length)
{
    opal_if_t* intf;

    OPAL_LIST_FOREACH(intf, &opal_if_list, opal_if_t) {
        if (intf->if_index == if_index) {
            memcpy(if_addr, &intf->if_addr, MIN(length, sizeof (intf->if_addr)));
            return OPAL_SUCCESS;
        }
    }
    return OPAL_ERROR;
}


/*
 *  Lookup the interface by opal_list kindex and return the
 *  primary address assigned to the interface.
 */
int opal_ifkindextoaddr(int if_kindex, struct sockaddr* if_addr, unsigned int length)
{
    opal_if_t* intf;

    OPAL_LIST_FOREACH(intf, &opal_if_list, opal_if_t) {
        if (intf->if_kernel_index == if_kindex) {
            memcpy(if_addr, &intf->if_addr, MIN(length, sizeof (intf->if_addr)));
            return OPAL_SUCCESS;
        }
    }
    return OPAL_ERROR;
}


/*
 *  Lookup the interface by opal_list index and return the
 *  network mask assigned to the interface.
 */

int opal_ifindextomask(int if_index, uint32_t* if_mask, int length)
{
    opal_if_t* intf;

    OPAL_LIST_FOREACH(intf, &opal_if_list, opal_if_t) {
        if (intf->if_index == if_index) {
            memcpy(if_mask, &intf->if_mask, length);
            return OPAL_SUCCESS;
        }
    }
    return OPAL_ERROR;
}

/*
 *  Lookup the interface by opal_list index and return the
 *  MAC assigned to the interface.
 */

int opal_ifindextomac(int if_index, uint8_t mac[6])
{
    opal_if_t* intf;

    OPAL_LIST_FOREACH(intf, &opal_if_list, opal_if_t) {
        if (intf->if_index == if_index) {
            memcpy(mac, &intf->if_mac, 6);
            return OPAL_SUCCESS;
        }
    }
    return OPAL_ERROR;
}

/*
 *  Lookup the interface by opal_list index and return the
 *  MTU assigned to the interface.
 */

int opal_ifindextomtu(int if_index, int *mtu)
{
    opal_if_t* intf;

    OPAL_LIST_FOREACH(intf, &opal_if_list, opal_if_t) {
        if (intf->if_index == if_index) {
            *mtu = intf->ifmtu;
            return OPAL_SUCCESS;
        }
    }
    return OPAL_ERROR;
}

/*
 *  Lookup the interface by opal_list index and return the
 *  flags assigned to the interface.
 */

int opal_ifindextoflags(int if_index, uint32_t* if_flags)
{
    opal_if_t* intf;

    OPAL_LIST_FOREACH(intf, &opal_if_list, opal_if_t) {
        if (intf->if_index == if_index) {
            memcpy(if_flags, &intf->if_flags, sizeof(uint32_t));
            return OPAL_SUCCESS;
        }
    }
    return OPAL_ERROR;
}



/*
 *  Lookup the interface by opal_list index and return
 *  the associated name.
 */

int opal_ifindextoname(int if_index, char* if_name, int length)
{
    opal_if_t *intf;

    OPAL_LIST_FOREACH(intf, &opal_if_list, opal_if_t) {
        if (intf->if_index == if_index) {
            strncpy(if_name, intf->if_name, length);
            return OPAL_SUCCESS;
        }
    }
    return OPAL_ERROR;
}


/*
 *  Lookup the interface by kernel index and return
 *  the associated name.
 */

int opal_ifkindextoname(int if_kindex, char* if_name, int length)
{
    opal_if_t *intf;

    OPAL_LIST_FOREACH(intf, &opal_if_list, opal_if_t) {
        if (intf->if_kernel_index == if_kindex) {
            strncpy(if_name, intf->if_name, length);
            return OPAL_SUCCESS;
        }
    }
    return OPAL_ERROR;
}


#define ADDRLEN 100
bool
opal_ifislocal(const char *hostname)
{
#if OPAL_ENABLE_IPV6
    char addrname[NI_MAXHOST]; /* should be larger than ADDRLEN, but I think
                                  they really mean IFNAMESIZE */
#else
    char addrname[ADDRLEN + 1];
#endif

    if (OPAL_SUCCESS == opal_ifaddrtoname(hostname, addrname, ADDRLEN)) {
        return true;
    }

    return false;
}

static int parse_ipv4_dots(const char *addr, uint32_t* net, int* dots)
{
    const char *start = addr, *end;
    uint32_t n[]={0,0,0,0};
    int i;

    /* now assemble the address */
    for( i = 0; i < 4; i++ ) {
        n[i] = strtoul(start, (char**)&end, 10);
        if( end == start ) {
            /* this is not an error, but indicates that
             * we were given a partial address - e.g.,
             * 192.168 - usually indicating an IP range
             * in CIDR notation. So just return what we have
             */
            break;
        }
        /* did we read something sensible? */
        if( n[i] > 255 ) {
            return OPAL_ERR_NETWORK_NOT_PARSEABLE;
        }
        /* skip all the . */
        for( start = end; '\0' != *start; start++ )
            if( '.' != *start ) break;
    }
    *dots = i;
    *net = OPAL_IF_ASSEMBLE_NETWORK(n[0], n[1], n[2], n[3]);
    return OPAL_SUCCESS;
}

int
opal_iftupletoaddr(const char *inaddr, uint32_t *net, uint32_t *mask)
{
    int pval, dots, rc = OPAL_SUCCESS;
    const char *ptr;

    /* if a mask was desired... */
    if (NULL != mask) {
        /* set default */
        *mask = 0xFFFFFFFF;

        /* if entry includes mask, split that off */
        if (NULL != (ptr = strchr(inaddr, '/'))) {
            ptr = ptr + 1;  /* skip the / */
            /* is the mask a tuple? */
            if (NULL != strchr(ptr, '.')) {
                /* yes - extract mask from it */
                rc = parse_ipv4_dots(ptr, mask, &dots);
            } else {
                /* no - must be an int telling us how much of the addr to use: e.g., /16
                 * For more information please read http://en.wikipedia.org/wiki/Subnetwork.
                 */
                pval = strtol(ptr, NULL, 10);
                if ((pval > 31) || (pval < 1)) {
                    opal_output(0, "opal_iftupletoaddr: unknown mask");
                    return OPAL_ERR_NETWORK_NOT_PARSEABLE;
                }
                *mask = 0xFFFFFFFF << (32 - pval);
            }
        } else {
            /* use the number of dots to determine it */
            for (ptr = inaddr, pval = 0; '\0'!= *ptr; ptr++) {
                if ('.' == *ptr) {
                    pval++;
                }
            }
            /* if we have three dots, then we have four
             * fields since it is a full address, so the
             * default netmask is fine
             */
            if (3 == pval) {
                *mask = 0xFFFFFFFF;
            } else if (2 == pval) {         /* 2 dots */
                *mask = 0xFFFFFF00;
            } else if (1 == pval) {  /* 1 dot */
                *mask = 0xFFFF0000;
            } else if (0 == pval) {  /* no dots */
                *mask = 0xFF000000;
            } else {
                opal_output(0, "opal_iftupletoaddr: unknown mask");
                return OPAL_ERR_NETWORK_NOT_PARSEABLE;
            }
        }
    }

    /* if network addr is desired... */
    if (NULL != net) {
        /* now assemble the address */
        rc = parse_ipv4_dots(inaddr, net, &dots);
    }

    return rc;
}

/*
 *  Determine if the specified interface is loopback
 */

bool opal_ifisloopback(int if_index)
{
    opal_if_t* intf;

    OPAL_LIST_FOREACH(intf, &opal_if_list, opal_if_t) {
        if (intf->if_index == if_index) {
            if ((intf->if_flags & IFF_LOOPBACK) != 0) {
                return true;
            }
        }
    }
    return false;
}

/* Determine if an interface matches any entry in the given list, taking
 * into account that the list entries could be given as named interfaces,
 * IP addrs, or subnet+mask
 */
int opal_ifmatches(int kidx, char **nets)
{
    bool named_if;
    int i, rc;
    size_t j;
    int kindex;
    struct sockaddr_in inaddr;
    uint32_t addr, netaddr, netmask;

    /* get the address info for the given network in case we need it */
    if (OPAL_SUCCESS != (rc = opal_ifkindextoaddr(kidx, (struct sockaddr*)&inaddr, sizeof(inaddr)))) {
        return rc;
    }
    addr = ntohl(inaddr.sin_addr.s_addr);

    for (i=0; NULL != nets[i]; i++) {
        /* if the specified interface contains letters in it, then it
         * was given as an interface name and not an IP tuple
         */
        named_if = false;
        for (j=0; j < strlen(nets[i]); j++) {
            if (isalpha(nets[i][j]) && '.' != nets[i][j]) {
                named_if = true;
                break;
            }
        }
        if (named_if) {
            if (0 > (kindex = opal_ifnametokindex(nets[i]))) {
                continue;
            }
            if (kindex == kidx) {
                return OPAL_SUCCESS;
            }
        } else {
            if (OPAL_SUCCESS != (rc = opal_iftupletoaddr(nets[i], &netaddr, &netmask))) {
                opal_show_help("help-opal-util.txt", "invalid-net-mask", true, nets[i]);
                return rc;
            }
            if (netaddr == (addr & netmask)) {
                return OPAL_SUCCESS;
            }
        }
    }
    /* get here if not found */
    return OPAL_ERR_NOT_FOUND;
}

void opal_ifgetaliases(char ***aliases)
{
    opal_if_t* intf;
    char ipv4[INET_ADDRSTRLEN];
    struct sockaddr_in *addr;
#if OPAL_ENABLE_IPV6
    char ipv6[INET6_ADDRSTRLEN];
    struct sockaddr_in6 *addr6;
#endif

    /* set default answer */
    *aliases = NULL;

    OPAL_LIST_FOREACH(intf, &opal_if_list, opal_if_t) {
        addr = (struct sockaddr_in*) &intf->if_addr;
        /* ignore purely loopback interfaces */
        if ((intf->if_flags & IFF_LOOPBACK) != 0) {
            continue;
        }
        if (addr->sin_family == AF_INET) {
            inet_ntop(AF_INET, &(addr->sin_addr.s_addr), ipv4, INET_ADDRSTRLEN);
            opal_argv_append_nosize(aliases, ipv4);
        }
#if OPAL_ENABLE_IPV6
        else {
            addr6 = (struct sockaddr_in6*) &intf->if_addr;
            inet_ntop(AF_INET6, &(addr6->sin6_addr), ipv6, INET6_ADDRSTRLEN);
            opal_argv_append_nosize(aliases, ipv6);
        }
#endif
    }
}

#else /* HAVE_STRUCT_SOCKADDR_IN */

/* if we don't have struct sockaddr_in, we don't have traditional
   ethernet devices.  Just make everything a no-op error call */

int
opal_ifnametoaddr(const char* if_name,
                  struct sockaddr* if_addr, int size)
{
    return OPAL_ERR_NOT_SUPPORTED;
}

int
opal_ifaddrtoname(const char* if_addr,
                  char* if_name, int size)
{
    return OPAL_ERR_NOT_SUPPORTED;
}

int
opal_ifnametoindex(const char* if_name)
{
    return OPAL_ERR_NOT_SUPPORTED;
}

int
opal_ifnametokindex(const char* if_name)
{
    return OPAL_ERR_NOT_SUPPORTED;
}

int
opal_ifindextokindex(int if_index)
{
    return OPAL_ERR_NOT_SUPPORTED;
}

int
opal_ifcount(void)
{
    return OPAL_ERR_NOT_SUPPORTED;
}

int
opal_ifbegin(void)
{
    return OPAL_ERR_NOT_SUPPORTED;
}

int
opal_ifnext(int if_index)
{
    return OPAL_ERR_NOT_SUPPORTED;
}

int
opal_ifindextoname(int if_index, char* if_name, int length)
{
    return OPAL_ERR_NOT_SUPPORTED;
}

int
opal_ifkindextoname(int kif_index, char* if_name, int length)
{
    return OPAL_ERR_NOT_SUPPORTED;
}

int
opal_ifindextoaddr(int if_index, struct sockaddr* if_addr, unsigned int length)
{
    return OPAL_ERR_NOT_SUPPORTED;
}

int
opal_ifindextomask(int if_index, uint32_t* if_addr, int length)
{
    return OPAL_ERR_NOT_SUPPORTED;
}

bool
opal_ifislocal(const char *hostname)
{
    return false;
}

int
opal_iftupletoaddr(const char *inaddr, uint32_t *net, uint32_t *mask)
{
    return 0;
}

int opal_ifmatches(int idx, char **nets)
{
    return OPAL_ERR_NOT_SUPPORTED;
}

void opal_ifgetaliases(char ***aliases)
{
    /* set default answer */
    *aliases = NULL;
}

#endif /* HAVE_STRUCT_SOCKADDR_IN */

