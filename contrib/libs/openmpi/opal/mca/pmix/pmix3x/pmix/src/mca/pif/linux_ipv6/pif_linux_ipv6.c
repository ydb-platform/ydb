/*
 * Copyright (c) 2010      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2010      Oracle and/or its affiliates.  All rights reserved.
 * Copyright (c) 2018      Intel, Inc.  All rights reserved.
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

static int if_linux_ipv6_open(void);

/* Discovers Linux IPv6 interfaces */
pmix_pif_base_component_t mca_pif_linux_ipv6_component = {
    /* First, the mca_component_t struct containing meta information
       about the component itself */
    .base = {
        PMIX_PIF_BASE_VERSION_2_0_0,

        /* Component name and version */
        "linux_ipv6",
        PMIX_MAJOR_VERSION,
        PMIX_MINOR_VERSION,
        PMIX_RELEASE_VERSION,

        /* Component open and close functions */
        if_linux_ipv6_open,
        NULL
    },
    .data = {
        /* This component is checkpointable */
        PMIX_MCA_BASE_METADATA_PARAM_CHECKPOINT
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
            pmix_pif_t *intf;

            hexdecode(addrhex, a6.s6_addr, sizeof a6.s6_addr);
            inet_ntop(AF_INET6, a6.s6_addr, addrstr, sizeof addrstr);

            pmix_output_verbose(1, pmix_pif_base_framework.framework_output,
                                "found interface %s inet6 %s scope %x\n",
                                ifname, addrstr, scope);

            /* Only interested in global (0x00) scope */
            if (scope != 0x00)  {
                pmix_output_verbose(1, pmix_pif_base_framework.framework_output,
                                    "skipped interface %s inet6 %s scope %x\n",
                                    ifname, addrstr, scope);
                continue;
            }

            intf = PMIX_NEW(pmix_pif_t);
            if (NULL == intf) {
                pmix_output(0, "pmix_ifinit: unable to allocate %lu bytes\n",
                            (unsigned long)sizeof(pmix_pif_t));
                fclose(f);
                return PMIX_ERR_OUT_OF_RESOURCE;
            }
            intf->af_family = AF_INET6;

            /* now construct the pmix_pif_t */
            pmix_strncpy(intf->if_name, ifname, IF_NAMESIZE);
            intf->if_index = pmix_list_get_size(&pmix_if_list)+1;
            intf->if_kernel_index = (uint16_t) idx;
            ((struct sockaddr_in6*) &intf->if_addr)->sin6_addr = a6;
            ((struct sockaddr_in6*) &intf->if_addr)->sin6_family = AF_INET6;
            ((struct sockaddr_in6*) &intf->if_addr)->sin6_scope_id = scope;
            intf->if_mask = pfxlen;
            if (PMIX_SUCCESS == pmix_ifindextoflags(pmix_ifnametoindex (ifname), &flag)) {
                intf->if_flags = flag;
            } else {
                intf->if_flags = IFF_UP;
            }

            /* copy new interface information to heap and append
               to list */
            pmix_list_append(&pmix_if_list, &(intf->super));
            pmix_output_verbose(1, pmix_pif_base_framework.framework_output,
                                "added interface %s inet6 %s scope %x\n",
                                ifname, addrstr, scope);
        } /* of while */
        fclose(f);
    }

    return PMIX_SUCCESS;
}
