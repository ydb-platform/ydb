/*
 * Copyright (c) 2010      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2010      Oracle and/or its affiliates.  All rights reserved.
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

#include "opal/constants.h"
#include "opal/util/if.h"
#include "opal/util/output.h"
#include "opal/mca/if/if.h"
#include "opal/mca/if/base/base.h"

static int if_linux_ipv6_open(void);

/* Discovers Linux IPv6 interfaces */
opal_if_base_component_t mca_if_linux_ipv6_component = {
    /* First, the mca_component_t struct containing meta information
       about the component itself */
    {
        OPAL_IF_BASE_VERSION_2_0_0,

        /* Component name and version */
        "linux_ipv6",
        OPAL_MAJOR_VERSION,
        OPAL_MINOR_VERSION,
        OPAL_RELEASE_VERSION,

        /* Component open and close functions */
        if_linux_ipv6_open,
        NULL
    },
    {
        /* This component is checkpointable */
        MCA_BASE_METADATA_PARAM_CHECKPOINT
    },
};

static int hex2int(char hex)
{
    if ('0' <= hex && hex <= '9') return hex - '0';
    if ('A' <= hex && hex <= 'F') return hex - 'A' + 10;
    if ('a' <= hex && hex <= 'f') return hex - 'a' + 10;
    abort();
}

static void hexdecode(const char *src, char *dst, size_t dstsize)
{
    for (size_t i = 0; i < dstsize; i++) {
        dst[i] = hex2int(src[i * 2]) * 16 + hex2int(src[i * 2 + 1]);
    }
}

static int if_linux_ipv6_open(void)
{
#if OPAL_ENABLE_IPV6
    FILE *f;
    if ((f = fopen("/proc/net/if_inet6", "r"))) {
        char ifname[IF_NAMESIZE];
        unsigned int idx, pfxlen, scope, dadstat;
        struct in6_addr a6;
        uint32_t flag;
        char addrhex[sizeof a6.s6_addr * 2 + 1];
        char addrstr[INET6_ADDRSTRLEN];

        while (fscanf(f, "%s %x %x %x %x %s\n", addrhex,
                      &idx, &pfxlen, &scope, &dadstat, ifname) != EOF) {
            opal_if_t *intf;

            hexdecode(addrhex, a6.s6_addr, sizeof a6.s6_addr);
            inet_ntop(AF_INET6, a6.s6_addr, addrstr, sizeof addrstr);

            opal_output_verbose(1, opal_if_base_framework.framework_output,
                                "found interface %s inet6 %s scope %x\n",
                                ifname, addrstr, scope);

            /* Only interested in global (0x00) scope */
            if (scope != 0x00)  {
                opal_output_verbose(1, opal_if_base_framework.framework_output,
                                    "skipped interface %s inet6 %s scope %x\n",
                                    ifname, addrstr, scope);
                continue;
            }

            intf = OBJ_NEW(opal_if_t);
            if (NULL == intf) {
                opal_output(0, "opal_ifinit: unable to allocate %lu bytes\n",
                            (unsigned long)sizeof(opal_if_t));
                fclose(f);
                return OPAL_ERR_OUT_OF_RESOURCE;
            }
            intf->af_family = AF_INET6;

            /* now construct the opal_if_t */
            strncpy(intf->if_name, ifname, IF_NAMESIZE);
            intf->if_index = opal_list_get_size(&opal_if_list)+1;
            intf->if_kernel_index = (uint16_t) idx;
            ((struct sockaddr_in6*) &intf->if_addr)->sin6_addr = a6;
            ((struct sockaddr_in6*) &intf->if_addr)->sin6_family = AF_INET6;
            ((struct sockaddr_in6*) &intf->if_addr)->sin6_scope_id = scope;
            intf->if_mask = pfxlen;
            if (OPAL_SUCCESS == opal_ifindextoflags(opal_ifnametoindex (ifname), &flag)) {
                intf->if_flags = flag;
            } else {
                intf->if_flags = IFF_UP;
            }

            /* copy new interface information to heap and append
               to list */
            opal_list_append(&opal_if_list, &(intf->super));
            opal_output_verbose(1, opal_if_base_framework.framework_output,
                                "added interface %s inet6 %s scope %x\n",
                                ifname, addrstr, scope);
        } /* of while */
        fclose(f);
    }
#endif  /* OPAL_ENABLE_IPV6 */

    return OPAL_SUCCESS;
}
