/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2014-2015 Intel, Inc.  All rights reserved.
 * Copyright (c) 2014      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2014      Mellanox Technologies, Inc.
 *                         All rights reserved.
 * Copyright (c) 2017      Amazon.com, Inc. or its affiliates.
 *                         All Rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"
#include "opal/constants.h"
#include "opal/types.h"

#include <string.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef HAVE_MATH_H
#include <math.h>
#endif

#include "opal/mca/if/if.h"

#include "opal/mca/reachable/base/base.h"
#include "reachable_weighted.h"
#include "opal/util/net.h"

static int weighted_init(void);
static int weighted_fini(void);
static opal_reachable_t* weighted_reachable(opal_list_t *local_if,
                                            opal_list_t *remote_if);

static int get_weights(opal_if_t *local_if, opal_if_t *remote_if);
static int calculate_weight(int bandwidth_local, int bandwidth_remote,
                            int connection_quality);

/*
 * Describes the quality of a possible connection between a local and
 * a remote network interface.  Highest connection quality is assigned
 * to connections between interfaces on same network.  This is because
 * same network implies a single hop to destination.  Public addresses
 * are preferred over private addresses.  This is all guessing,
 * because we don't know actual network topology.
 */
enum connection_quality {
    CQ_NO_CONNECTION = 0,
    CQ_PRIVATE_DIFFERENT_NETWORK = 50,
    CQ_PRIVATE_SAME_NETWORK = 80,
    CQ_PUBLIC_DIFFERENT_NETWORK = 90,
    CQ_PUBLIC_SAME_NETWORK = 100
};

const opal_reachable_base_module_t opal_reachable_weighted_module = {
    weighted_init,
    weighted_fini,
    weighted_reachable
};

// local variables
static int init_cntr = 0;


static int weighted_init(void)
{
    ++init_cntr;

    return OPAL_SUCCESS;
}

static int weighted_fini(void)
{
    --init_cntr;

    return OPAL_SUCCESS;
}


static opal_reachable_t* weighted_reachable(opal_list_t *local_if,
                                            opal_list_t *remote_if)
{
    opal_reachable_t *reachable_results = NULL;
    int i, j;
    opal_if_t *local_iter, *remote_iter;

    reachable_results = opal_reachable_allocate(opal_list_get_size(local_if),
                                                opal_list_get_size(remote_if));
    if (NULL == reachable_results) {
        return NULL;
    }

    i = 0;
    OPAL_LIST_FOREACH(local_iter, local_if, opal_if_t) {
        j = 0;
        OPAL_LIST_FOREACH(remote_iter, remote_if, opal_if_t) {
            reachable_results->weights[i][j] = get_weights(local_iter, remote_iter);
            j++;
        }
        i++;
    }

    return reachable_results;
}


static int get_weights(opal_if_t *local_if, opal_if_t *remote_if)
{
    char str_local[128], str_remote[128], *conn_type;
    struct sockaddr *local_sockaddr, *remote_sockaddr;
    int weight;

    local_sockaddr = (struct sockaddr *)&local_if->if_addr;
    remote_sockaddr = (struct sockaddr *)&remote_if->if_addr;

    /* opal_net_get_hostname returns a static buffer.  Great for
       single address printfs, need to copy in this case */
    strncpy(str_local, opal_net_get_hostname(local_sockaddr), sizeof(str_local));
    str_local[sizeof(str_local) - 1] = '\0';
    strncpy(str_remote, opal_net_get_hostname(remote_sockaddr), sizeof(str_remote));
    str_remote[sizeof(str_remote) - 1] = '\0';

    /*  initially, assume no connection is possible */
    weight = calculate_weight(0, 0, CQ_NO_CONNECTION);

    if (AF_INET == local_sockaddr->sa_family &&
        AF_INET == remote_sockaddr->sa_family) {

        if (opal_net_addr_isipv4public(local_sockaddr) &&
            opal_net_addr_isipv4public(remote_sockaddr)) {
            if (opal_net_samenetwork(local_sockaddr,
                                     remote_sockaddr,
                                     local_if->if_mask)) {
                conn_type = "IPv4 PUBLIC SAME NETWORK";
                weight = calculate_weight(local_if->if_bandwidth,
                                          remote_if->if_bandwidth,
                                          CQ_PUBLIC_SAME_NETWORK);
            } else {
                conn_type = "IPv4 PUBLIC DIFFERENT NETWORK";
                weight = calculate_weight(local_if->if_bandwidth,
                                          remote_if->if_bandwidth,
                                          CQ_PUBLIC_DIFFERENT_NETWORK);
            }
        } else if (!opal_net_addr_isipv4public(local_sockaddr) &&
                   !opal_net_addr_isipv4public(remote_sockaddr)) {
            if (opal_net_samenetwork(local_sockaddr,
                                     remote_sockaddr,
                                     local_if->if_mask)) {
                conn_type = "IPv4 PRIVATE SAME NETWORK";
                weight = calculate_weight(local_if->if_bandwidth,
                                          remote_if->if_bandwidth,
                                          CQ_PRIVATE_SAME_NETWORK);
            } else {
                conn_type = "IPv4 PRIVATE DIFFERENT NETWORK";
                weight = calculate_weight(local_if->if_bandwidth,
                                          remote_if->if_bandwidth,
                                          CQ_PRIVATE_DIFFERENT_NETWORK);
            }
        } else {
            /* one private, one public address.  likely not a match. */
            conn_type = "IPv4 NO CONNECTION";
            weight = calculate_weight(local_if->if_bandwidth,
                                      remote_if->if_bandwidth,
                                      CQ_NO_CONNECTION);
        }

#if OPAL_ENABLE_IPV6
    } else if (AF_INET6 == local_sockaddr->sa_family &&
               AF_INET6 == remote_sockaddr->sa_family) {
        if (opal_net_addr_isipv6linklocal(local_sockaddr) &&
            opal_net_addr_isipv6linklocal(remote_sockaddr)) {
            /* we can't actually tell if link local addresses are on
             * the same network or not with the weighted component.
             * Assume they are on the same network, so that they'll be
             * most likely to be paired together, breaking the fewest
             * number of connections.
             *
             * There used to be a comment in this code (and one in the
             * BTL TCP code as well) that the opal_if code doesn't
             * pass link-local addresses through.  However, this is
             * demonstratably not true on Linux, where link-local
             * interfaces are created.  Since it's easy to handle
             * either case, do so.
             */
            conn_type = "IPv6 LINK-LOCAL SAME NETWORK";
            weight = calculate_weight(local_if->if_bandwidth,
                                      remote_if->if_bandwidth,
                                      CQ_PRIVATE_SAME_NETWORK);
        } else if (!opal_net_addr_isipv6linklocal(local_sockaddr) &&
                   !opal_net_addr_isipv6linklocal(remote_sockaddr)) {
            if (opal_net_samenetwork(local_sockaddr,
                                     remote_sockaddr,
                                     local_if->if_mask)) {
                conn_type = "IPv6 PUBLIC SAME NETWORK";
                weight = calculate_weight(local_if->if_bandwidth,
                                          remote_if->if_bandwidth,
                                          CQ_PUBLIC_SAME_NETWORK);
            } else {
                conn_type = "IPv6 PUBLIC DIFFERENT NETWORK";
                weight = calculate_weight(local_if->if_bandwidth,
                                          remote_if->if_bandwidth,
                                          CQ_PUBLIC_DIFFERENT_NETWORK);
            }
        } else {
            /* one link-local, one public address.  likely not a match. */
            conn_type = "IPv6 NO CONNECTION";
            weight = calculate_weight(local_if->if_bandwidth,
                                      remote_if->if_bandwidth,
                                      CQ_NO_CONNECTION);
        }
#endif /* #if OPAL_ENABLE_IPV6 */

    } else {
        /* we don't have an address family match, so assume no
           connection */
        conn_type = "Address type mismatch";
        weight = calculate_weight(0, 0, CQ_NO_CONNECTION);
    }

    opal_output_verbose(20, opal_reachable_base_framework.framework_output,
                        "reachable:weighted: path from %s to %s: %s",
                        str_local, str_remote, conn_type);

    return weight;
}


/*
 * Weights determined by bandwidth between
 * interfaces (limited by lower bandwidth
 * interface).  A penalty is added to minimize
 * the discrepancy in bandwidth.  This helps
 * prevent pairing of fast and slow interfaces
 *
 * Formula: connection_quality * (min(a,b) + 1/(1 + |a-b|))
 *
 * Examples: a     b     f(a,b)
 *           0     0     1
 *           0     1     0.5
 *           1     1     2
 *           1     2     1.5
 *           1     3     1.33
 *           1     10    1.1
 *           10    10    11
 *           10    14    10.2
 *           11    14    11.25
 *           11    15    11.2
 *
 * NOTE: connection_quality of 1 is assumed for examples.
 * In reality, since we're using integers, we need
 * connection_quality to be large enough
 * to capture decimals
 */
static int calculate_weight(int bandwidth_local, int bandwidth_remote,
                            int connection_quality)
{
    int weight = connection_quality * (MIN(bandwidth_local, bandwidth_remote) +
                                       1.0 / (1.0 + (double)abs(bandwidth_local - bandwidth_remote)));
    return weight;
}
