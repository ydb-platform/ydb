/*
 * Copyright (c) 2010-2013 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2010      Oracle and/or its affiliates.  All rights reserved.
 * Copyright (c) 2013      The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2015-2018 Intel, Inc. All rights reserved.
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

#include "src/util/output.h"
#include "src/util/pif.h"
#include "src/mca/pif/pif.h"
#include "src/mca/pif/base/base.h"

static int if_posix_open(void);

/* Supports all flavors of posix except those
 * BSD-flavors supported elsewhere
 */
pmix_pif_base_component_t mca_pif_posix_ipv4_component = {
    /* First, the mca_component_t struct containing meta information
       about the component itself */
    .base = {
        PMIX_PIF_BASE_VERSION_2_0_0,

        /* Component name and version */
        "posix_ipv4",
        PMIX_MAJOR_VERSION,
        PMIX_MINOR_VERSION,
        PMIX_RELEASE_VERSION,

        /* Component open and close functions */
        if_posix_open,
        NULL
    },
    .data = {
        /* This component is checkpointable */
        PMIX_MCA_BASE_METADATA_PARAM_CHECKPOINT
    },
};

/* convert a netmask (in network byte order) to CIDR notation */
static int prefix (uint32_t netmask)
{
    uint32_t mask = ntohl(netmask);
    int plen = 0;

    if (0 == mask) {
        plen = 32;
    } else {
        while ((mask % 2) == 0) {
            plen += 1;
            mask /= 2;
        }
    }

    return (32 - plen);
}

/* configure using getifaddrs(3) */
static int if_posix_open(void)
{
    int sd;
    int lastlen, rem;
    char *ptr;
    struct ifconf ifconf;
    int ifc_len;
    bool successful_locate = false;

    /* Create the internet socket to test with.  Must use AF_INET;
       using AF_UNSPEC or AF_INET6 will cause everything to
       fail. */
    if ((sd = socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
        pmix_output(0, "pmix_ifinit: socket() failed with errno=%d\n",
                    errno);
        return PMIX_ERROR;
    }

    /*
     * Get Network Interface configuration
     *
     * Some notes on the behavior of ioctl(..., SIOCGIFCONF,...)
     * when not enough space is allocated for all the entries.
     *
     * - Solaris returns -1, errno EINVAL if there is not enough
     *   space
     * - OS X returns 0, sets .ifc_len to the space used by the
     *   by the entries that did fit.
     * - Linux returns 0, sets .ifc_len to the space required to
     *   hold all the entries (although it only writes what will
     *   fit in the buffer of .ifc_len passed to the function).
     * - FreeBSD returns 0, sets .ifc_len to 0.
     *
     * Everyone else seems to do one of the four.
     */
    lastlen = 0;
    ifc_len = sizeof(struct ifreq) * DEFAULT_NUMBER_INTERFACES;
    do {
        ifconf.ifc_len = ifc_len;
        ifconf.ifc_req = malloc(ifc_len);
        if (NULL == ifconf.ifc_req) {
            close(sd);
            return PMIX_ERROR;
        }

        /* initialize the memory so valgrind and purify won't
         * complain.  Since this isn't performance critical, just
         * always memset.
         */
        memset(ifconf.ifc_req, 0, ifconf.ifc_len);

        if (ioctl(sd, SIOCGIFCONF, &ifconf) < 0) {
            /* if we got an einval, we probably don't have enough
               space.  so we'll fall down and try to expand our
               space */
            if (errno != EINVAL && lastlen != 0) {
                pmix_output(0, "pmix_ifinit: ioctl(SIOCGIFCONF) \
                            failed with errno=%d",
                            errno);
                free(ifconf.ifc_req);
                close(sd);
                return PMIX_ERROR;
            }
        } else {
            /* if ifc_len is 0 or different than what we set it to
               at call to ioctl, try again with a bigger buffer.
               else stop */
            if (ifconf.ifc_len == lastlen && ifconf.ifc_len > 0) {
                /* we didn't expand.  we're done */
                successful_locate = true;
                break;
            }
            lastlen = ifconf.ifc_len;
        }

        /* Yes, we overflowed (or had an EINVAL on the ioctl).
           Loop back around and try again with a bigger buffer */
        free(ifconf.ifc_req);
        ifc_len = (ifc_len == 0) ? 1 : ifc_len * 2;
    } while (ifc_len < MAX_PIFCONF_SIZE);
    if (!successful_locate) {
        pmix_output(0, "pmix_ifinit: unable to find network interfaces.");
        close(sd);
        return PMIX_ERROR;
    }

    /*
     * Setup indexes
     */
    ptr = (char*) ifconf.ifc_req;
    rem = ifconf.ifc_len;

    /* loop through all interfaces */
    while (rem > 0) {
        struct ifreq* ifr = (struct ifreq*) ptr;
        pmix_pif_t *intf;
        int length;

        /* compute offset for entries */
#ifdef HAVE_STRUCT_SOCKADDR_SA_LEN
        length = sizeof(struct sockaddr);

        if (ifr->ifr_addr.sa_len > length) {
            length = ifr->ifr_addr.sa_len;
        }

        length += sizeof(ifr->ifr_name);
#else
        length = sizeof(struct ifreq);
#endif

        rem -= length;
        ptr += length;

        /* see if we like this entry */
        if (AF_INET != ifr->ifr_addr.sa_family) {
            continue;
        }

        if (ioctl(sd, SIOCGIFFLAGS, ifr) < 0) {
            pmix_output(0, "pmix_ifinit: ioctl(SIOCGIFFLAGS) failed with errno=%d", errno);
            continue;
        }
        if ((ifr->ifr_flags & IFF_UP) == 0) {
            continue;
        }
#ifdef IFF_SLAVE
        /* Is this a slave to a load balancer or bonded channel?
           If so, don't use it -- pick up the master instead */
        if ((ifr->ifr_flags & IFF_SLAVE) != 0) {
            continue;
        }
#endif
#if 0
        if (!pmix_if_retain_loopback && (ifr->ifr_flags & IFF_LOOPBACK) != 0) {
            continue;
        }
#endif

        intf = PMIX_NEW(pmix_pif_t);
        if (NULL == intf) {
            pmix_output(0, "pmix_ifinit: unable to allocated %lu bytes\n", (unsigned long)sizeof(pmix_pif_t));
            free(ifconf.ifc_req);
            close(sd);
            return PMIX_ERR_OUT_OF_RESOURCE;
        }
        intf->af_family = AF_INET;

        /* copy entry over into our data structure */
        memset(intf->if_name, 0, sizeof(intf->if_name));
        pmix_strncpy(intf->if_name, ifr->ifr_name, sizeof(intf->if_name) - 1);
        intf->if_flags = ifr->ifr_flags;

        /* every new address gets its own internal if_index */
        intf->if_index = pmix_list_get_size(&pmix_if_list)+1;

        pmix_output_verbose(1, pmix_pif_base_framework.framework_output,
                            "found interface %s", intf->if_name);

        /* assign the kernel index to distinguish different NICs */
#ifndef SIOCGIFINDEX
        intf->if_kernel_index = intf->if_index;
#else
        if (ioctl(sd, SIOCGIFINDEX, ifr) < 0) {
            pmix_output(0,"pmix_ifinit: ioctl(SIOCGIFINDEX) failed with errno=%d", errno);
            PMIX_RELEASE(intf);
            continue;
        }
#if defined(ifr_ifindex)
        intf->if_kernel_index = ifr->ifr_ifindex;
#elif defined(ifr_index)
        intf->if_kernel_index = ifr->ifr_index;
#else
        intf->if_kernel_index = -1;
#endif
#endif /* SIOCGIFINDEX */

        /* This call returns IPv4 addresses only. Use SIOCGLIFADDR
           instead */
        if (ioctl(sd, SIOCGIFADDR, ifr) < 0) {
            pmix_output(0, "pmix_ifinit: ioctl(SIOCGIFADDR) failed with errno=%d", errno);
            PMIX_RELEASE(intf);
            break;
        }
        if (AF_INET != ifr->ifr_addr.sa_family) {
            PMIX_RELEASE(intf);
            continue;
        }

        /* based on above, we know this is an IPv4 address... */
        memcpy(&intf->if_addr, &ifr->ifr_addr, sizeof(struct sockaddr_in));

        if (ioctl(sd, SIOCGIFNETMASK, ifr) < 0) {
            pmix_output(0, "pmix_ifinit: ioctl(SIOCGIFNETMASK) failed with errno=%d", errno);
            PMIX_RELEASE(intf);
            continue;
        }

        /* generate CIDR and assign to netmask */
        intf->if_mask = prefix(((struct sockaddr_in*) &ifr->ifr_addr)->sin_addr.s_addr);

#if defined(SIOCGIFHWADDR) && defined(HAVE_STRUCT_IFREQ_IFR_HWADDR)
        /* get the MAC address */
        if (ioctl(sd, SIOCGIFHWADDR, ifr) < 0) {
            pmix_output(0, "pmix_ifinit: ioctl(SIOCGIFHWADDR) failed with errno=%d", errno);
            break;
        }
        memcpy(intf->if_mac, ifr->ifr_hwaddr.sa_data, 6);
#endif

#if defined(SIOCGIFMTU) && defined(HAVE_STRUCT_IFREQ_IFR_MTU)
        /* get the MTU */
        if (ioctl(sd, SIOCGIFMTU, ifr) < 0) {
            pmix_output(0, "pmix_ifinit: ioctl(SIOCGIFMTU) failed with errno=%d", errno);
            break;
        }
        intf->ifmtu = ifr->ifr_mtu;
#endif
        pmix_output_verbose(1, pmix_pif_base_framework.framework_output,
                            "adding interface %s", intf->if_name);
        pmix_list_append(&pmix_if_list, &(intf->super));
    }
    free(ifconf.ifc_req);
    close(sd);

    return PMIX_SUCCESS;
}
