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
 * Copyright (c) 2014      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2015-2016 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2016-2018 Intel, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "pmix_config.h"
#include "pmix_common.h"

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
#include <ctype.h>

#include "src/class/pmix_list.h"
#include "src/util/error.h"
#include "src/util/pif.h"
#include "src/util/net.h"
#include "src/util/output.h"
#include "src/util/argv.h"
#include "src/util/show_help.h"

#include "src/mca/pif/base/base.h"

#ifdef HAVE_STRUCT_SOCKADDR_IN

#ifndef MIN
#  define MIN(a,b)                ((a) < (b) ? (a) : (b))
#endif

/*
 *  Look for interface by name and returns its address
 *  as a dotted decimal formatted string.
 */

int pmix_ifnametoaddr(const char* if_name, struct sockaddr* addr, int length)
{
    pmix_pif_t* intf;

    for (intf =  (pmix_pif_t*)pmix_list_get_first(&pmix_if_list);
        intf != (pmix_pif_t*)pmix_list_get_end(&pmix_if_list);
        intf =  (pmix_pif_t*)pmix_list_get_next(intf)) {
        if (strcmp(intf->if_name, if_name) == 0) {
            memcpy(addr, &intf->if_addr, length);
            return PMIX_SUCCESS;
        }
    }
    return PMIX_ERROR;
}


/*
 *  Look for interface by name and returns its
 *  corresponding pmix_list index.
 */

int pmix_ifnametoindex(const char* if_name)
{
    pmix_pif_t* intf;

    for (intf =  (pmix_pif_t*)pmix_list_get_first(&pmix_if_list);
        intf != (pmix_pif_t*)pmix_list_get_end(&pmix_if_list);
        intf =  (pmix_pif_t*)pmix_list_get_next(intf)) {
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

int16_t pmix_ifnametokindex(const char* if_name)
{
    pmix_pif_t* intf;

    for (intf =  (pmix_pif_t*)pmix_list_get_first(&pmix_if_list);
        intf != (pmix_pif_t*)pmix_list_get_end(&pmix_if_list);
        intf =  (pmix_pif_t*)pmix_list_get_next(intf)) {
        if (strcmp(intf->if_name, if_name) == 0) {
            return intf->if_kernel_index;
        }
    }
    return -1;
}


/*
 *  Look for interface by pmix_list index and returns its
 *  corresponding kernel index.
 */

int pmix_ifindextokindex(int if_index)
{
    pmix_pif_t* intf;

    for (intf =  (pmix_pif_t*)pmix_list_get_first(&pmix_if_list);
        intf != (pmix_pif_t*)pmix_list_get_end(&pmix_if_list);
        intf =  (pmix_pif_t*)pmix_list_get_next(intf)) {
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

int pmix_ifaddrtoname(const char* if_addr, char* if_name, int length)
{
    pmix_pif_t* intf;
    int error;
    struct addrinfo hints, *res = NULL, *r;

    /* if the user asked us not to resolve interfaces, then just return */
    if (pmix_if_do_not_resolve) {
        /* return not found so ifislocal will declare
         * the node to be non-local
         */
        return PMIX_ERR_NOT_FOUND;
    }

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = PF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    error = getaddrinfo(if_addr, NULL, &hints, &res);

    if (error) {
        if (NULL != res) {
            freeaddrinfo (res);
        }
        return PMIX_ERR_NOT_FOUND;
    }

    for (r = res; r != NULL; r = r->ai_next) {
        for (intf =  (pmix_pif_t*)pmix_list_get_first(&pmix_if_list);
            intf != (pmix_pif_t*)pmix_list_get_end(&pmix_if_list);
            intf =  (pmix_pif_t*)pmix_list_get_next(intf)) {

            if (AF_INET == r->ai_family) {
                struct sockaddr_in ipv4;
                struct sockaddr_in *inaddr;

                inaddr = (struct sockaddr_in*) &intf->if_addr;
                memcpy (&ipv4, r->ai_addr, r->ai_addrlen);

                if (inaddr->sin_addr.s_addr == ipv4.sin_addr.s_addr) {
                    pmix_strncpy(if_name, intf->if_name, length-1);
                    freeaddrinfo (res);
                    return PMIX_SUCCESS;
                }
            }
            else {
                if (IN6_ARE_ADDR_EQUAL(&((struct sockaddr_in6*) &intf->if_addr)->sin6_addr,
                    &((struct sockaddr_in6*) r->ai_addr)->sin6_addr)) {
                    pmix_strncpy(if_name, intf->if_name, length-1);
                    freeaddrinfo (res);
                    return PMIX_SUCCESS;
                }
            }
        }
    }
    if (NULL != res) {
        freeaddrinfo (res);
    }

    /* if we get here, it wasn't found */
    return PMIX_ERR_NOT_FOUND;
}

/*
 *  Attempt to resolve the address (given as either IPv4/IPv6 string
 *  or hostname) and return the kernel index of the interface
 *  on the same network as the specified address
 */
int16_t pmix_ifaddrtokindex(const char* if_addr)
{
    pmix_pif_t* intf;
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
        return PMIX_ERR_NOT_FOUND;
    }

    for (r = res; r != NULL; r = r->ai_next) {
        PMIX_LIST_FOREACH(intf, &pmix_if_list, pmix_pif_t) {
            if (AF_INET == r->ai_family && AF_INET == intf->af_family) {
                struct sockaddr_in ipv4, intv4;
                memset(&ipv4, 0, sizeof(struct sockaddr_in));
                len = (r->ai_addrlen < sizeof(struct sockaddr_in)) ? r->ai_addrlen : sizeof(struct sockaddr_in);
                memcpy(&ipv4, r->ai_addr, len);
                memset(&intv4, 0, sizeof(struct sockaddr_in));
                memcpy(&intv4, &intf->if_addr, sizeof(struct sockaddr_in));
                if (pmix_net_samenetwork((struct sockaddr*)&ipv4, (struct sockaddr*)&intv4, intf->if_mask)) {
                    if_kernel_index = intf->if_kernel_index;
                    freeaddrinfo (res);
                    return if_kernel_index;
                }
            } else if (AF_INET6 == r->ai_family && AF_INET6 == intf->af_family) {
                struct sockaddr_in6 ipv6, intv6;
                memset(&ipv6, 0, sizeof(struct sockaddr));
                len = (r->ai_addrlen < sizeof(struct sockaddr_in6)) ? r->ai_addrlen : sizeof(struct sockaddr_in6);
                memcpy(&ipv6, r->ai_addr, len);
                memset(&intv6, 0, sizeof(struct sockaddr));
                memcpy(&intv6, &intf->if_addr, sizeof(struct sockaddr_in6));
                if (pmix_net_samenetwork((struct sockaddr*)&intv6,
                                         (struct sockaddr*)&ipv6, intf->if_mask)) {
                    if_kernel_index = intf->if_kernel_index;
                    freeaddrinfo (res);
                    return if_kernel_index;
                }
            }
        }
    }
    if (NULL != res) {
        freeaddrinfo (res);
    }
    return PMIX_ERR_NOT_FOUND;
}

/*
 *  Return the number of discovered interface.
 */

int pmix_ifcount(void)
{
    return pmix_list_get_size(&pmix_if_list);
}


/*
 *  Return the pmix_list interface index for the first
 *  interface in our list.
 */

int pmix_ifbegin(void)
{
    pmix_pif_t *intf;

    intf = (pmix_pif_t*)pmix_list_get_first(&pmix_if_list);
    if (NULL != intf)
        return intf->if_index;
    return (-1);
}


/*
 *  Located the current position in the list by if_index and
 *  return the interface index of the next element in our list
 *  (if it exists).
 */

int pmix_ifnext(int if_index)
{
    pmix_pif_t *intf;

    for (intf =  (pmix_pif_t*)pmix_list_get_first(&pmix_if_list);
        intf != (pmix_pif_t*)pmix_list_get_end(&pmix_if_list);
        intf =  (pmix_pif_t*)pmix_list_get_next(intf)) {
        if (intf->if_index == if_index) {
            do {
                pmix_pif_t* if_next = (pmix_pif_t*)pmix_list_get_next(intf);
                pmix_pif_t* if_end =  (pmix_pif_t*)pmix_list_get_end(&pmix_if_list);
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
 *  Lookup the interface by pmix_list index and return the
 *  primary address assigned to the interface.
 */

int pmix_ifindextoaddr(int if_index, struct sockaddr* if_addr, unsigned int length)
{
    pmix_pif_t* intf;

    for (intf =  (pmix_pif_t*)pmix_list_get_first(&pmix_if_list);
         intf != (pmix_pif_t*)pmix_list_get_end(&pmix_if_list);
         intf =  (pmix_pif_t*)pmix_list_get_next(intf)) {
        if (intf->if_index == if_index) {
            memcpy(if_addr, &intf->if_addr, MIN(length, sizeof (intf->if_addr)));
            return PMIX_SUCCESS;
        }
    }
    return PMIX_ERROR;
}


/*
 *  Lookup the interface by pmix_list kindex and return the
 *  primary address assigned to the interface.
 */
int pmix_ifkindextoaddr(int if_kindex, struct sockaddr* if_addr, unsigned int length)
{
    pmix_pif_t* intf;

    for (intf =  (pmix_pif_t*)pmix_list_get_first(&pmix_if_list);
         intf != (pmix_pif_t*)pmix_list_get_end(&pmix_if_list);
         intf =  (pmix_pif_t*)pmix_list_get_next(intf)) {
        if (intf->if_kernel_index == if_kindex) {
            memcpy(if_addr, &intf->if_addr, MIN(length, sizeof (intf->if_addr)));
            return PMIX_SUCCESS;
        }
    }
    return PMIX_ERROR;
}


/*
 *  Lookup the interface by pmix_list index and return the
 *  network mask assigned to the interface.
 */

int pmix_ifindextomask(int if_index, uint32_t* if_mask, int length)
{
    pmix_pif_t* intf;

    for (intf =  (pmix_pif_t*)pmix_list_get_first(&pmix_if_list);
        intf != (pmix_pif_t*)pmix_list_get_end(&pmix_if_list);
        intf =  (pmix_pif_t*)pmix_list_get_next(intf)) {
        if (intf->if_index == if_index) {
            memcpy(if_mask, &intf->if_mask, length);
            return PMIX_SUCCESS;
        }
    }
    return PMIX_ERROR;
}

/*
 *  Lookup the interface by pmix_list index and return the
 *  MAC assigned to the interface.
 */

int pmix_ifindextomac(int if_index, uint8_t mac[6])
{
    pmix_pif_t* intf;

    for (intf = (pmix_pif_t*)pmix_list_get_first(&pmix_if_list);
        intf != (pmix_pif_t*)pmix_list_get_end(&pmix_if_list);
        intf = (pmix_pif_t*)pmix_list_get_next(intf)) {
        if (intf->if_index == if_index) {
            memcpy(mac, &intf->if_mac, 6);
            return PMIX_SUCCESS;
        }
    }
    return PMIX_ERROR;
}

/*
 *  Lookup the interface by pmix_list index and return the
 *  MTU assigned to the interface.
 */

int pmix_ifindextomtu(int if_index, int *mtu)
{
    pmix_pif_t* intf;

    for (intf = (pmix_pif_t*)pmix_list_get_first(&pmix_if_list);
        intf != (pmix_pif_t*)pmix_list_get_end(&pmix_if_list);
        intf = (pmix_pif_t*)pmix_list_get_next(intf)) {
        if (intf->if_index == if_index) {
            *mtu = intf->ifmtu;
            return PMIX_SUCCESS;
        }
    }
    return PMIX_ERROR;
}

/*
 *  Lookup the interface by pmix_list index and return the
 *  flags assigned to the interface.
 */

int pmix_ifindextoflags(int if_index, uint32_t* if_flags)
{
    pmix_pif_t* intf;

    for (intf =  (pmix_pif_t*)pmix_list_get_first(&pmix_if_list);
        intf != (pmix_pif_t*)pmix_list_get_end(&pmix_if_list);
        intf =  (pmix_pif_t*)pmix_list_get_next(intf)) {
        if (intf->if_index == if_index) {
            memcpy(if_flags, &intf->if_flags, sizeof(uint32_t));
            return PMIX_SUCCESS;
        }
    }
    return PMIX_ERROR;
}



/*
 *  Lookup the interface by pmix_list index and return
 *  the associated name.
 */

int pmix_ifindextoname(int if_index, char* if_name, int length)
{
    pmix_pif_t *intf;

    for (intf =  (pmix_pif_t*)pmix_list_get_first(&pmix_if_list);
        intf != (pmix_pif_t*)pmix_list_get_end(&pmix_if_list);
        intf =  (pmix_pif_t*)pmix_list_get_next(intf)) {
        if (intf->if_index == if_index) {
            pmix_strncpy(if_name, intf->if_name, length-1);
            return PMIX_SUCCESS;
        }
    }
    return PMIX_ERROR;
}


/*
 *  Lookup the interface by kernel index and return
 *  the associated name.
 */

int pmix_ifkindextoname(int if_kindex, char* if_name, int length)
{
    pmix_pif_t *intf;

    for (intf =  (pmix_pif_t*)pmix_list_get_first(&pmix_if_list);
        intf != (pmix_pif_t*)pmix_list_get_end(&pmix_if_list);
        intf =  (pmix_pif_t*)pmix_list_get_next(intf)) {
        if (intf->if_kernel_index == if_kindex) {
            pmix_strncpy(if_name, intf->if_name, length-1);
            return PMIX_SUCCESS;
        }
    }
    return PMIX_ERROR;
}


#define ADDRLEN 100
bool
pmix_ifislocal(const char *hostname)
{
    char addrname[NI_MAXHOST]; /* should be larger than ADDRLEN, but I think
                                  they really mean IFNAMESIZE */

    if (PMIX_SUCCESS == pmix_ifaddrtoname(hostname, addrname, ADDRLEN)) {
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
            return PMIX_ERR_NETWORK_NOT_PARSEABLE;
        }
        /* skip all the . */
        for( start = end; '\0' != *start; start++ )
            if( '.' != *start ) break;
    }
    *dots = i;
    *net = PMIX_PIF_ASSEMBLE_NETWORK(n[0], n[1], n[2], n[3]);
    return PMIX_SUCCESS;
}

int
pmix_iftupletoaddr(const char *inaddr, uint32_t *net, uint32_t *mask)
{
    int pval, dots, rc = PMIX_SUCCESS;
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
                    pmix_output(0, "pmix_iftupletoaddr: unknown mask");
                    return PMIX_ERR_NETWORK_NOT_PARSEABLE;
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
                pmix_output(0, "pmix_iftupletoaddr: unknown mask");
                return PMIX_ERR_NETWORK_NOT_PARSEABLE;
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

bool pmix_ifisloopback(int if_index)
{
    pmix_pif_t* intf;

    for (intf =  (pmix_pif_t*)pmix_list_get_first(&pmix_if_list);
        intf != (pmix_pif_t*)pmix_list_get_end(&pmix_if_list);
        intf =  (pmix_pif_t*)pmix_list_get_next(intf)) {
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
int pmix_ifmatches(int kidx, char **nets)
{
    bool named_if;
    int i, rc;
    size_t j;
    int kindex;
    struct sockaddr_in inaddr;
    uint32_t addr, netaddr, netmask;

    /* get the address info for the given network in case we need it */
    if (PMIX_SUCCESS != (rc = pmix_ifkindextoaddr(kidx, (struct sockaddr*)&inaddr, sizeof(inaddr)))) {
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
            if (0 > (kindex = pmix_ifnametokindex(nets[i]))) {
                continue;
            }
            if (kindex == kidx) {
                return PMIX_SUCCESS;
            }
        } else {
            if (PMIX_SUCCESS != (rc = pmix_iftupletoaddr(nets[i], &netaddr, &netmask))) {
                pmix_show_help("help-pmix-util.txt", "invalid-net-mask", true, nets[i]);
                return rc;
            }
            if (netaddr == (addr & netmask)) {
                return PMIX_SUCCESS;
            }
        }
    }
    /* get here if not found */
    return PMIX_ERR_NOT_FOUND;
}

void pmix_ifgetaliases(char ***aliases)
{
    pmix_pif_t* intf;
    char ipv4[INET_ADDRSTRLEN];
    struct sockaddr_in *addr;
    char ipv6[INET6_ADDRSTRLEN];
    struct sockaddr_in6 *addr6;

    /* set default answer */
    *aliases = NULL;

    for (intf =  (pmix_pif_t*)pmix_list_get_first(&pmix_if_list);
        intf != (pmix_pif_t*)pmix_list_get_end(&pmix_if_list);
        intf =  (pmix_pif_t*)pmix_list_get_next(intf)) {
        addr = (struct sockaddr_in*) &intf->if_addr;
        /* ignore purely loopback interfaces */
        if ((intf->if_flags & IFF_LOOPBACK) != 0) {
            continue;
        }
        if (addr->sin_family == AF_INET) {
            inet_ntop(AF_INET, &(addr->sin_addr.s_addr), ipv4, INET_ADDRSTRLEN);
            pmix_argv_append_nosize(aliases, ipv4);
        } else {
            addr6 = (struct sockaddr_in6*) &intf->if_addr;
            inet_ntop(AF_INET6, &(addr6->sin6_addr), ipv6, INET6_ADDRSTRLEN);
            pmix_argv_append_nosize(aliases, ipv6);
        }
    }
}

#else /* HAVE_STRUCT_SOCKADDR_IN */

/* if we don't have struct sockaddr_in, we don't have traditional
   ethernet devices.  Just make everything a no-op error call */

int
pmix_ifnametoaddr(const char* if_name,
                  struct sockaddr* if_addr, int size)
{
    return PMIX_ERR_NOT_SUPPORTED;
}

int
pmix_ifaddrtoname(const char* if_addr,
                  char* if_name, int size)
{
    return PMIX_ERR_NOT_SUPPORTED;
}

int
pmix_ifnametoindex(const char* if_name)
{
    return PMIX_ERR_NOT_SUPPORTED;
}

int16_t
pmix_ifnametokindex(const char* if_name)
{
    return PMIX_ERR_NOT_SUPPORTED;
}

int
pmix_ifindextokindex(int if_index)
{
    return PMIX_ERR_NOT_SUPPORTED;
}

int
pmix_ifcount(void)
{
    return PMIX_ERR_NOT_SUPPORTED;
}

int
pmix_ifbegin(void)
{
    return PMIX_ERR_NOT_SUPPORTED;
}

int
pmix_ifnext(int if_index)
{
    return PMIX_ERR_NOT_SUPPORTED;
}

int
pmix_ifindextoname(int if_index, char* if_name, int length)
{
    return PMIX_ERR_NOT_SUPPORTED;
}

int
pmix_ifkindextoname(int kif_index, char* if_name, int length)
{
    return PMIX_ERR_NOT_SUPPORTED;
}

int
pmix_ifindextoaddr(int if_index, struct sockaddr* if_addr, unsigned int length)
{
    return PMIX_ERR_NOT_SUPPORTED;
}

int
pmix_ifindextomask(int if_index, uint32_t* if_addr, int length)
{
    return PMIX_ERR_NOT_SUPPORTED;
}

bool
pmix_ifislocal(const char *hostname)
{
    return false;
}

int
pmix_iftupletoaddr(const char *inaddr, uint32_t *net, uint32_t *mask)
{
    return 0;
}

int pmix_ifmatches(int idx, char **nets)
{
    return PMIX_ERR_NOT_SUPPORTED;
}

void pmix_ifgetaliases(char ***aliases)
{
    /* set default answer */
    *aliases = NULL;
}

#endif /* HAVE_STRUCT_SOCKADDR_IN */
