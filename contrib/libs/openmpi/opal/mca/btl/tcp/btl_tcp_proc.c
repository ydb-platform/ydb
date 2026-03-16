/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2006 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2017 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2008-2010 Oracle and/or its affiliates.  All rights reserved
 * Copyright (c) 2013-2017 Intel, Inc.  All rights reserved.
 * Copyright (c) 2014-2016 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2015-2016 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2015-2018 Cisco Systems, Inc.  All rights reserved
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"

#ifdef HAVE_NETINET_IN_H
#include <netinet/in.h>
#endif
#ifdef HAVE_ARPA_INET_H
#include <arpa/inet.h>
#endif

#include "opal/class/opal_hash_table.h"
#include "opal/mca/btl/base/btl_base_error.h"
#include "opal/mca/pmix/pmix.h"
#include "opal/util/arch.h"
#include "opal/util/argv.h"
#include "opal/util/if.h"
#include "opal/util/net.h"
#include "opal/util/proc.h"
#include "opal/util/show_help.h"

#include "btl_tcp.h"
#include "btl_tcp_proc.h"

static void mca_btl_tcp_proc_construct(mca_btl_tcp_proc_t* proc);
static void mca_btl_tcp_proc_destruct(mca_btl_tcp_proc_t* proc);

struct mca_btl_tcp_proc_data_t {
    mca_btl_tcp_interface_t** local_interfaces;
    opal_hash_table_t local_kindex_to_index;
    size_t num_local_interfaces, max_local_interfaces;
    size_t num_peer_interfaces;
    opal_hash_table_t peer_kindex_to_index;
    unsigned int *best_assignment;
    int max_assignment_weight;
    int max_assignment_cardinality;
    enum mca_btl_tcp_connection_quality **weights;
    struct mca_btl_tcp_addr_t ***best_addr;
};

typedef struct mca_btl_tcp_proc_data_t mca_btl_tcp_proc_data_t;

OBJ_CLASS_INSTANCE( mca_btl_tcp_proc_t,
                    opal_list_item_t,
                    mca_btl_tcp_proc_construct,
                    mca_btl_tcp_proc_destruct );

void mca_btl_tcp_proc_construct(mca_btl_tcp_proc_t* tcp_proc)
{
    tcp_proc->proc_opal           = NULL;
    tcp_proc->proc_addrs          = NULL;
    tcp_proc->proc_addr_count     = 0;
    tcp_proc->proc_endpoints      = NULL;
    tcp_proc->proc_endpoint_count = 0;
    OBJ_CONSTRUCT(&tcp_proc->proc_lock, opal_mutex_t);
}

/*
 * Cleanup ib proc instance
 */

void mca_btl_tcp_proc_destruct(mca_btl_tcp_proc_t* tcp_proc)
{
    if( NULL != tcp_proc->proc_opal ) {
        /* remove from list of all proc instances */
        OPAL_THREAD_LOCK(&mca_btl_tcp_component.tcp_lock);
        opal_proc_table_remove_value(&mca_btl_tcp_component.tcp_procs,
                                     tcp_proc->proc_opal->proc_name);
        OPAL_THREAD_UNLOCK(&mca_btl_tcp_component.tcp_lock);
        OBJ_RELEASE(tcp_proc->proc_opal);
        tcp_proc->proc_opal = NULL;
    }
    /* release resources */
    if(NULL != tcp_proc->proc_endpoints) {
        free(tcp_proc->proc_endpoints);
    }
    if(NULL != tcp_proc->proc_addrs) {
        free(tcp_proc->proc_addrs);
    }
    OBJ_DESTRUCT(&tcp_proc->proc_lock);
}

/*
 * Create a TCP process structure. There is a one-to-one correspondence
 * between a opal_proc_t and a mca_btl_tcp_proc_t instance. We cache
 * additional data (specifically the list of mca_btl_tcp_endpoint_t instances,
 * and published addresses) associated w/ a given destination on this
 * datastructure.
 */

mca_btl_tcp_proc_t* mca_btl_tcp_proc_create(opal_proc_t* proc)
{
    mca_btl_tcp_proc_t* btl_proc;
    size_t size;
    int rc;

    OPAL_THREAD_LOCK(&mca_btl_tcp_component.tcp_lock);
    rc = opal_proc_table_get_value(&mca_btl_tcp_component.tcp_procs,
                                   proc->proc_name, (void**)&btl_proc);
    if(OPAL_SUCCESS == rc) {
        OPAL_THREAD_UNLOCK(&mca_btl_tcp_component.tcp_lock);
        return btl_proc;
    }

    do {  /* This loop is only necessary so that we can break out of the serial code */
        btl_proc = OBJ_NEW(mca_btl_tcp_proc_t);
        if(NULL == btl_proc) {
            rc = OPAL_ERR_OUT_OF_RESOURCE;
            break;
        }

        /* Retain the proc, but don't store the ref into the btl_proc just yet. This
         * provides a way to release the btl_proc in case of failure without having to
         * unlock the mutex.
         */
        OBJ_RETAIN(proc);

        /* lookup tcp parameters exported by this proc */
        OPAL_MODEX_RECV(rc, &mca_btl_tcp_component.super.btl_version,
                        &proc->proc_name, (uint8_t**)&btl_proc->proc_addrs, &size);
        if(rc != OPAL_SUCCESS) {
            if(OPAL_ERR_NOT_FOUND != rc)
                BTL_ERROR(("opal_modex_recv: failed with return value=%d", rc));
            break;
        }

        if(0 != (size % sizeof(mca_btl_tcp_addr_t))) {
            BTL_ERROR(("opal_modex_recv: invalid size %lu: btl-size: %lu\n",
                       (unsigned long) size, (unsigned long)sizeof(mca_btl_tcp_addr_t)));
            rc = OPAL_ERROR;
            break;
        }

        btl_proc->proc_addr_count = size / sizeof(mca_btl_tcp_addr_t);

        /* allocate space for endpoint array - one for each exported address */
        btl_proc->proc_endpoints = (mca_btl_base_endpoint_t**)
            malloc((1 + btl_proc->proc_addr_count) *
                   sizeof(mca_btl_base_endpoint_t*));
        if(NULL == btl_proc->proc_endpoints) {
            rc = OPAL_ERR_OUT_OF_RESOURCE;
            break;
        }

        /* convert the OPAL addr_family field to OS constants,
         * so we can check for AF_INET (or AF_INET6) and don't have
         * to deal with byte ordering anymore.
         */
        for (unsigned int i = 0; i < btl_proc->proc_addr_count; i++) {
            if (MCA_BTL_TCP_AF_INET == btl_proc->proc_addrs[i].addr_family) {
                btl_proc->proc_addrs[i].addr_family = AF_INET;
            }
#if OPAL_ENABLE_IPV6
            if (MCA_BTL_TCP_AF_INET6 == btl_proc->proc_addrs[i].addr_family) {
                btl_proc->proc_addrs[i].addr_family = AF_INET6;
            }
#endif
        }
    } while (0);

    if (OPAL_SUCCESS == rc) {
        btl_proc->proc_opal = proc;  /* link with the proc */
        /* add to hash table of all proc instance. */
        opal_proc_table_set_value(&mca_btl_tcp_component.tcp_procs,
                                  proc->proc_name, btl_proc);
    } else {
        if (btl_proc) {
            OBJ_RELEASE(btl_proc);  /* release the local proc */
            OBJ_RELEASE(proc);      /* and the ref on the OMPI proc */
            btl_proc = NULL;
        }
    }

    OPAL_THREAD_UNLOCK(&mca_btl_tcp_component.tcp_lock);

    return btl_proc;
}



static void evaluate_assignment(mca_btl_tcp_proc_data_t *proc_data, int *a) {
    size_t i;
    unsigned int max_interfaces = proc_data->num_local_interfaces;
    int assignment_weight = 0;
    int assignment_cardinality = 0;

    if(max_interfaces < proc_data->num_peer_interfaces) {
        max_interfaces = proc_data->num_peer_interfaces;
    }

    for(i = 0; i < max_interfaces; ++i) {
        if(0 < proc_data->weights[i][a[i]-1]) {
            ++assignment_cardinality;
            assignment_weight += proc_data->weights[i][a[i]-1];
        }
    }

    /*
     * check wether current solution beats all previous solutions
     */
    if(assignment_cardinality > proc_data->max_assignment_cardinality
            || (assignment_cardinality == proc_data->max_assignment_cardinality
                && assignment_weight > proc_data->max_assignment_weight)) {

        for(i = 0; i < max_interfaces; ++i) {
             proc_data->best_assignment[i] = a[i]-1;
        }
        proc_data->max_assignment_weight = assignment_weight;
        proc_data->max_assignment_cardinality = assignment_cardinality;
    }
}

static void visit(mca_btl_tcp_proc_data_t *proc_data, int k, int level, int siz, int *a)
{
    level = level+1; a[k] = level;

    if (level == siz) {
        evaluate_assignment(proc_data, a);
    } else {
        int i;
        for ( i = 0; i < siz; i++)
            if (a[i] == 0)
                visit(proc_data, i, level, siz, a);
    }

    level = level-1; a[k] = 0;
}


static void mca_btl_tcp_initialise_interface(mca_btl_tcp_interface_t* tcp_interface,
        int ifk_index, int index)
{
    tcp_interface->kernel_index = ifk_index;
    tcp_interface->peer_interface = -1;
    tcp_interface->ipv4_address = NULL;
    tcp_interface->ipv6_address =  NULL;
    tcp_interface->index = index;
    tcp_interface->inuse = 0;
}

static mca_btl_tcp_interface_t** mca_btl_tcp_retrieve_local_interfaces(mca_btl_tcp_proc_data_t *proc_data)
{
    struct sockaddr_storage local_addr;
    char local_if_name[IF_NAMESIZE];
    char **include, **exclude, **argv;
    int idx;
    mca_btl_tcp_interface_t * local_interface;

    assert (NULL == proc_data->local_interfaces);
    if( NULL != proc_data->local_interfaces )
        return proc_data->local_interfaces;

    proc_data->max_local_interfaces = MAX_KERNEL_INTERFACES;
    proc_data->num_local_interfaces = 0;
    proc_data->local_interfaces = (mca_btl_tcp_interface_t**)calloc( proc_data->max_local_interfaces, sizeof(mca_btl_tcp_interface_t*) );
    if( NULL == proc_data->local_interfaces )
        return NULL;

    /* Collect up the list of included and excluded interfaces, if any */
    include = opal_argv_split(mca_btl_tcp_component.tcp_if_include,',');
    exclude = opal_argv_split(mca_btl_tcp_component.tcp_if_exclude,',');

    /*
     * identify all kernel interfaces and the associated addresses of
     * the local node
     */
    for( idx = opal_ifbegin(); idx >= 0; idx = opal_ifnext (idx) ) {
        int kindex;
        uint64_t index;
        bool skip = false;

        opal_ifindextoaddr (idx, (struct sockaddr*) &local_addr, sizeof (local_addr));
        opal_ifindextoname (idx, local_if_name, sizeof (local_if_name));

        /* If we were given a list of included interfaces, then check
         * to see if the current one is a member of this set.  If so,
         * drop down and complete processing.  If not, skip it and
         * continue on to the next one.  Note that providing an include
         * list will override providing an exclude list as the two are
         * mutually exclusive.  This matches how it works in
         * mca_btl_tcp_component_create_instances() which is the function
         * that exports the interfaces.  */
        if(NULL != include) {
            argv = include;
            skip = true;
            while(argv && *argv) {
                /* When comparing included interfaces, we look for exact matches.
                   That is why we are using strcmp() here. */
                if (0 == strcmp(*argv, local_if_name)) {
                    skip = false;
                    break;
                }
                argv++;
            }
        } else if (NULL != exclude) {
            /* If we were given a list of excluded interfaces, then check to see if the
             * current one is a member of this set.  If not, drop down and complete
             * processing.  If so, skip it and continue on to the next one. */
            argv = exclude;
            while(argv && *argv) {
                /* When looking for interfaces to exclude, we only look at
                 * the number of characters equal to what the user provided.
                 * For example, excluding "lo" excludes "lo", "lo0" and
                 * anything that starts with "lo" */
                if(0 == strncmp(*argv, local_if_name, strlen(*argv))) {
                    skip = true;
                    break;
                }
                argv++;
            }
        }
        if (true == skip) {
            /* This interface is not part of the requested set, so skip it */
            continue;
        }

        kindex = opal_ifindextokindex(idx);
        int rc = opal_hash_table_get_value_uint32(&proc_data->local_kindex_to_index, kindex, (void**) &index);

        /* create entry for this kernel index previously not seen */
        if (OPAL_SUCCESS != rc) {
            index = proc_data->num_local_interfaces++;
            opal_hash_table_set_value_uint32(&proc_data->local_kindex_to_index, kindex, (void*)(uintptr_t) index);

            if( proc_data->num_local_interfaces == proc_data->max_local_interfaces ) {
                proc_data->max_local_interfaces <<= 1;
                proc_data->local_interfaces = (mca_btl_tcp_interface_t**)realloc( proc_data->local_interfaces,
                                                                                  proc_data->max_local_interfaces * sizeof(mca_btl_tcp_interface_t*) );
                if( NULL == proc_data->local_interfaces )
                    goto cleanup;
            }
            proc_data->local_interfaces[index] = (mca_btl_tcp_interface_t *) malloc(sizeof(mca_btl_tcp_interface_t));
            assert(NULL != proc_data->local_interfaces[index]);
            mca_btl_tcp_initialise_interface(proc_data->local_interfaces[index], kindex, index);
        }

        local_interface = proc_data->local_interfaces[index];
        switch(local_addr.ss_family) {
        case AF_INET:
            /* if AF is disabled, skip it completely */
            if (4 == mca_btl_tcp_component.tcp_disable_family) {
                continue;
            }

            local_interface->ipv4_address =
                (struct sockaddr_storage*) malloc(sizeof(local_addr));
            memcpy(local_interface->ipv4_address,
                   &local_addr, sizeof(local_addr));
            opal_ifindextomask(idx,
                               &local_interface->ipv4_netmask,
                               sizeof(int));
            break;
        case AF_INET6:
            /* if AF is disabled, skip it completely */
            if (6 == mca_btl_tcp_component.tcp_disable_family) {
                continue;
            }

            local_interface->ipv6_address
                = (struct sockaddr_storage*) malloc(sizeof(local_addr));
            memcpy(local_interface->ipv6_address,
                   &local_addr, sizeof(local_addr));
            opal_ifindextomask(idx,
                               &local_interface->ipv6_netmask,
                               sizeof(int));
            break;
        default:
            opal_output(0, "unknown address family for tcp: %d\n",
                        local_addr.ss_family);
        }
    }
cleanup:
    if (NULL != include) {
        opal_argv_free(include);
    }
    if (NULL != exclude) {
        opal_argv_free(exclude);
    }

    return proc_data->local_interfaces;
}
/*
 * Note that this routine must be called with the lock on the process
 * already held.  Insert a btl instance into the proc array and assign
 * it an address.
 */
int mca_btl_tcp_proc_insert( mca_btl_tcp_proc_t* btl_proc,
                             mca_btl_base_endpoint_t* btl_endpoint )
{
    struct sockaddr_storage endpoint_addr_ss;
    const char *proc_hostname;
    unsigned int perm_size = 0;
    int rc, *a = NULL;
    size_t i, j;
    mca_btl_tcp_interface_t** peer_interfaces = NULL;
    mca_btl_tcp_proc_data_t _proc_data, *proc_data=&_proc_data;
    size_t max_peer_interfaces;
    char str_local[128], str_remote[128];

    if (NULL == (proc_hostname = opal_get_proc_hostname(btl_proc->proc_opal))) {
        return OPAL_ERR_UNREACH;
    }

    memset(proc_data, 0, sizeof(mca_btl_tcp_proc_data_t));
    OBJ_CONSTRUCT(&_proc_data.local_kindex_to_index, opal_hash_table_t);
    opal_hash_table_init(&_proc_data.local_kindex_to_index, 8);
    OBJ_CONSTRUCT(&_proc_data.peer_kindex_to_index, opal_hash_table_t);
    opal_hash_table_init(&_proc_data.peer_kindex_to_index, 8);

#ifndef WORDS_BIGENDIAN
    /* if we are little endian and our peer is not so lucky, then we
       need to put all information sent to him in big endian (aka
       Network Byte Order) and expect all information received to
       be in NBO.  Since big endian machines always send and receive
       in NBO, we don't care so much about that case. */
    if (btl_proc->proc_opal->proc_arch & OPAL_ARCH_ISBIGENDIAN) {
        btl_endpoint->endpoint_nbo = true;
    }
#endif

    /* insert into endpoint array */
    btl_endpoint->endpoint_proc = btl_proc;
    btl_proc->proc_endpoints[btl_proc->proc_endpoint_count++] = btl_endpoint;

    /* sanity checks */
    if( NULL == mca_btl_tcp_retrieve_local_interfaces(proc_data) )
        return OPAL_ERR_OUT_OF_RESOURCE;
    if( 0 == proc_data->num_local_interfaces ) {
        return OPAL_ERR_UNREACH;
    }

    max_peer_interfaces = proc_data->max_local_interfaces;
    peer_interfaces = (mca_btl_tcp_interface_t**)calloc( max_peer_interfaces, sizeof(mca_btl_tcp_interface_t*) );
    if (NULL == peer_interfaces) {
        max_peer_interfaces = 0;
        rc = OPAL_ERR_OUT_OF_RESOURCE;
        goto exit;
    }
    proc_data->num_peer_interfaces = 0;

    /*
     * identify all kernel interfaces and the associated addresses of
     * the peer
     */

    for( i = 0; i < btl_proc->proc_addr_count; i++ ) {

        uint64_t index;

        mca_btl_tcp_addr_t* endpoint_addr = btl_proc->proc_addrs + i;

        mca_btl_tcp_proc_tosocks (endpoint_addr, &endpoint_addr_ss);

        rc = opal_hash_table_get_value_uint32(&proc_data->peer_kindex_to_index, endpoint_addr->addr_ifkindex, (void**) &index);

        if (OPAL_SUCCESS != rc) {
            index = proc_data->num_peer_interfaces++;
            opal_hash_table_set_value_uint32(&proc_data->peer_kindex_to_index, endpoint_addr->addr_ifkindex, (void*)(uintptr_t) index);
            if( proc_data->num_peer_interfaces == max_peer_interfaces ) {
                max_peer_interfaces <<= 1;
                peer_interfaces = (mca_btl_tcp_interface_t**)realloc( peer_interfaces,
                                                                      max_peer_interfaces * sizeof(mca_btl_tcp_interface_t*) );
                if( NULL == peer_interfaces ) {
                    return OPAL_ERR_OUT_OF_RESOURCE;
                }
            }
            peer_interfaces[index] = (mca_btl_tcp_interface_t *) malloc(sizeof(mca_btl_tcp_interface_t));
            mca_btl_tcp_initialise_interface(peer_interfaces[index],
                                             endpoint_addr->addr_ifkindex, index);
        }

        /*
         * in case the peer address has created all intended connections,
         * mark the complete peer interface as 'not available'
         */
        if(endpoint_addr->addr_inuse >=  mca_btl_tcp_component.tcp_num_links) {
            peer_interfaces[index]->inuse = 1;
        }

        switch(endpoint_addr_ss.ss_family) {
        case AF_INET:
            peer_interfaces[index]->ipv4_address = (struct sockaddr_storage*) malloc(sizeof(endpoint_addr_ss));
            peer_interfaces[index]->ipv4_endpoint_addr = endpoint_addr;
            memcpy(peer_interfaces[index]->ipv4_address,
                   &endpoint_addr_ss, sizeof(endpoint_addr_ss));
            break;
        case AF_INET6:
            peer_interfaces[index]->ipv6_address = (struct sockaddr_storage*) malloc(sizeof(endpoint_addr_ss));
            peer_interfaces[index]->ipv6_endpoint_addr = endpoint_addr;
            memcpy(peer_interfaces[index]->ipv6_address,
                   &endpoint_addr_ss, sizeof(endpoint_addr_ss));
            break;
        default:
            opal_output(0, "unknown address family for tcp: %d\n",
                        endpoint_addr_ss.ss_family);
            return OPAL_ERR_UNREACH;
        }
    }

    /*
     * assign weights to each possible pair of interfaces
     */

    perm_size = proc_data->num_local_interfaces;
    if(proc_data->num_peer_interfaces > perm_size) {
        perm_size = proc_data->num_peer_interfaces;
    }

    proc_data->weights = (enum mca_btl_tcp_connection_quality**) malloc(perm_size
                                                             * sizeof(enum mca_btl_tcp_connection_quality*));
    assert(NULL != proc_data->weights);

    proc_data->best_addr = (mca_btl_tcp_addr_t ***) malloc(perm_size
                                                * sizeof(mca_btl_tcp_addr_t **));
    assert(NULL != proc_data->best_addr);
    for(i = 0; i < perm_size; ++i) {
        proc_data->weights[i] = (enum mca_btl_tcp_connection_quality*) calloc(perm_size,
                                                                   sizeof(enum mca_btl_tcp_connection_quality));
        assert(NULL != proc_data->weights[i]);

        proc_data->best_addr[i] = (mca_btl_tcp_addr_t **) calloc(perm_size,
                                                      sizeof(mca_btl_tcp_addr_t *));
        assert(NULL != proc_data->best_addr[i]);
    }


    for( i = 0; i < proc_data->num_local_interfaces; ++i ) {
        mca_btl_tcp_interface_t* local_interface = proc_data->local_interfaces[i];
        for( j = 0; j < proc_data->num_peer_interfaces; ++j ) {

            /*  initially, assume no connection is possible */
            proc_data->weights[i][j] = CQ_NO_CONNECTION;

            /* check state of ipv4 address pair */
            if(NULL != proc_data->local_interfaces[i]->ipv4_address &&
               NULL != peer_interfaces[j]->ipv4_address) {

                /* Convert the IPv4 addresses into nicely-printable strings for verbose debugging output */
                inet_ntop(AF_INET, &(((struct sockaddr_in*) proc_data->local_interfaces[i]->ipv4_address))->sin_addr,
                          str_local, sizeof(str_local));
                inet_ntop(AF_INET, &(((struct sockaddr_in*) peer_interfaces[j]->ipv4_address))->sin_addr,
                          str_remote, sizeof(str_remote));

                if(opal_net_addr_isipv4public((struct sockaddr*) local_interface->ipv4_address) &&
                   opal_net_addr_isipv4public((struct sockaddr*) peer_interfaces[j]->ipv4_address)) {
                    if(opal_net_samenetwork((struct sockaddr*) local_interface->ipv4_address,
                                            (struct sockaddr*) peer_interfaces[j]->ipv4_address,
                                            local_interface->ipv4_netmask)) {
                        proc_data->weights[i][j] = CQ_PUBLIC_SAME_NETWORK;
                        opal_output_verbose(20, opal_btl_base_framework.framework_output,
                                            "btl:tcp: path from %s to %s: IPV4 PUBLIC SAME NETWORK",
                                            str_local, str_remote);
                    } else {
                        proc_data->weights[i][j] = CQ_PUBLIC_DIFFERENT_NETWORK;
                        opal_output_verbose(20, opal_btl_base_framework.framework_output,
                                            "btl:tcp: path from %s to %s: IPV4 PUBLIC DIFFERENT NETWORK",
                                            str_local, str_remote);
                    }
                    proc_data->best_addr[i][j] = peer_interfaces[j]->ipv4_endpoint_addr;
                    continue;
                }
                if(opal_net_samenetwork((struct sockaddr*) local_interface->ipv4_address,
                                        (struct sockaddr*) peer_interfaces[j]->ipv4_address,
                                        local_interface->ipv4_netmask)) {
                    proc_data->weights[i][j] = CQ_PRIVATE_SAME_NETWORK;
                    opal_output_verbose(20, opal_btl_base_framework.framework_output,
                                       "btl:tcp: path from %s to %s: IPV4 PRIVATE SAME NETWORK",
                                       str_local, str_remote);
                } else {
                    proc_data->weights[i][j] = CQ_PRIVATE_DIFFERENT_NETWORK;
                    opal_output_verbose(20, opal_btl_base_framework.framework_output,
                                       "btl:tcp: path from %s to %s: IPV4 PRIVATE DIFFERENT NETWORK",
                                       str_local, str_remote);
                }
                proc_data->best_addr[i][j] = peer_interfaces[j]->ipv4_endpoint_addr;
                continue;
            }

            /* check state of ipv6 address pair - ipv6 is always public,
             * since link-local addresses are skipped in opal_ifinit()
             */
            if(NULL != local_interface->ipv6_address &&
               NULL != peer_interfaces[j]->ipv6_address) {

                /* Convert the IPv6 addresses into nicely-printable strings for verbose debugging output */
                inet_ntop(AF_INET6, &(((struct sockaddr_in6*) local_interface->ipv6_address))->sin6_addr,
                          str_local, sizeof(str_local));
                inet_ntop(AF_INET6, &(((struct sockaddr_in6*) peer_interfaces[j]->ipv6_address))->sin6_addr,
                          str_remote, sizeof(str_remote));

                if(opal_net_samenetwork((struct sockaddr*) local_interface->ipv6_address,
                                         (struct sockaddr*) peer_interfaces[j]->ipv6_address,
                                         local_interface->ipv6_netmask)) {
                    proc_data->weights[i][j] = CQ_PUBLIC_SAME_NETWORK;
                    opal_output_verbose(20, opal_btl_base_framework.framework_output,
                                       "btl:tcp: path from %s to %s: IPV6 PUBLIC SAME NETWORK",
                                       str_local, str_remote);
                } else {
                    proc_data->weights[i][j] = CQ_PUBLIC_DIFFERENT_NETWORK;
                    opal_output_verbose(20, opal_btl_base_framework.framework_output,
                                       "btl:tcp: path from %s to %s: IPV6 PUBLIC DIFFERENT NETWORK",
                                       str_local, str_remote);
                }
                proc_data->best_addr[i][j] = peer_interfaces[j]->ipv6_endpoint_addr;
                continue;
            }

        } /* for each peer interface */
    } /* for each local interface */

    /*
     * determine the size of the set to permute (max number of
     * interfaces
     */

    proc_data->best_assignment = (unsigned int *) malloc (perm_size * sizeof(int));

    a = (int *) malloc(perm_size * sizeof(int));
    if (NULL == a) {
        rc = OPAL_ERR_OUT_OF_RESOURCE;
        goto exit;
    }

    /* Can only find the best set of connections when the number of
     * interfaces is not too big.  When it gets larger, we fall back
     * to a simpler and faster (and not as optimal) algorithm.
     * See ticket https://svn.open-mpi.org/trac/ompi/ticket/2031
     * for more details about this issue.  */
    if (perm_size <= MAX_PERMUTATION_INTERFACES) {
        memset(a, 0, perm_size * sizeof(int));
        proc_data->max_assignment_cardinality = -1;
        proc_data->max_assignment_weight = -1;
        visit(proc_data, 0, -1, perm_size, a);

        rc = OPAL_ERR_UNREACH;
        for(i = 0; i < perm_size; ++i) {
            unsigned int best = proc_data->best_assignment[i];
            if(best > proc_data->num_peer_interfaces
               || proc_data->weights[i][best] == CQ_NO_CONNECTION
               || peer_interfaces[best]->inuse
               || NULL == peer_interfaces[best]) {
                continue;
            }
            peer_interfaces[best]->inuse++;
            btl_endpoint->endpoint_addr = proc_data->best_addr[i][best];
            btl_endpoint->endpoint_addr->addr_inuse++;
            rc = OPAL_SUCCESS;
            break;
        }
    } else {
        enum mca_btl_tcp_connection_quality max;
        int i_max = 0, j_max = 0;
        /* Find the best connection that is not in use.  Save away
         * the indices of the best location. */
        max = CQ_NO_CONNECTION;
        for(i=0; i<proc_data->num_local_interfaces; ++i) {
            for(j=0; j<proc_data->num_peer_interfaces; ++j) {
                if (!peer_interfaces[j]->inuse) {
                    if (proc_data->weights[i][j] > max) {
                        max = proc_data->weights[i][j];
                        i_max = i;
                        j_max = j;
                    }
                }
            }
        }
        /* Now see if there is a some type of connection available. */
        rc = OPAL_ERR_UNREACH;
        if (CQ_NO_CONNECTION != max) {
            peer_interfaces[j_max]->inuse++;
            btl_endpoint->endpoint_addr = proc_data->best_addr[i_max][j_max];
            btl_endpoint->endpoint_addr->addr_inuse++;
            rc = OPAL_SUCCESS;
        }
    }
    if (OPAL_ERR_UNREACH == rc) {
        opal_output_verbose(10, opal_btl_base_framework.framework_output,
                            "btl:tcp: host %s, process %s UNREACHABLE",
                            proc_hostname,
                            OPAL_NAME_PRINT(btl_proc->proc_opal->proc_name));
    }

 exit:
    // Ok to always free because proc_data() was memset() to 0 before
    // any possible return (and free(NULL) is fine).
    for(i = 0; i < perm_size; ++i) {
        free(proc_data->weights[i]);
        free(proc_data->best_addr[i]);
    }

    for(i = 0; i < proc_data->num_peer_interfaces; ++i) {
        if(NULL != peer_interfaces[i]->ipv4_address) {
            free(peer_interfaces[i]->ipv4_address);
        }
        if(NULL != peer_interfaces[i]->ipv6_address) {
            free(peer_interfaces[i]->ipv6_address);
        }
        free(peer_interfaces[i]);
    }
    free(peer_interfaces);

    for(i = 0; i < proc_data->num_local_interfaces; ++i) {
        if(NULL != proc_data->local_interfaces[i]->ipv4_address) {
            free(proc_data->local_interfaces[i]->ipv4_address);
        }
        if(NULL != proc_data->local_interfaces[i]->ipv6_address) {
            free(proc_data->local_interfaces[i]->ipv6_address);
        }
        free(proc_data->local_interfaces[i]);
    }
    free(proc_data->local_interfaces); proc_data->local_interfaces = NULL;
    proc_data->max_local_interfaces = 0;

    free(proc_data->weights); proc_data->weights = NULL;
    free(proc_data->best_addr); proc_data->best_addr = NULL;
    free(proc_data->best_assignment); proc_data->best_assignment = NULL;

    OBJ_DESTRUCT(&_proc_data.local_kindex_to_index);
    OBJ_DESTRUCT(&_proc_data.peer_kindex_to_index);

    free(a);

    return rc;
}

/*
 * Remove an endpoint from the proc array and indicate the address is
 * no longer in use.
 */

int mca_btl_tcp_proc_remove(mca_btl_tcp_proc_t* btl_proc, mca_btl_base_endpoint_t* btl_endpoint)
{
    size_t i;
    if (NULL != btl_proc) {
        OPAL_THREAD_LOCK(&btl_proc->proc_lock);
        for(i = 0; i < btl_proc->proc_endpoint_count; i++) {
            if(btl_proc->proc_endpoints[i] == btl_endpoint) {
                memmove(btl_proc->proc_endpoints+i, btl_proc->proc_endpoints+i+1,
                        (btl_proc->proc_endpoint_count-i-1)*sizeof(mca_btl_base_endpoint_t*));
                if(--btl_proc->proc_endpoint_count == 0) {
                    OPAL_THREAD_UNLOCK(&btl_proc->proc_lock);
                    OBJ_RELEASE(btl_proc);
                    return OPAL_SUCCESS;
                }
                /* The endpoint_addr may still be NULL if this endpoint is
                   being removed early in the wireup sequence (e.g., if it
                   is unreachable by all other procs) */
                if (NULL != btl_endpoint->endpoint_addr) {
                    btl_endpoint->endpoint_addr->addr_inuse--;
                }
                break;
            }
        }
        OPAL_THREAD_UNLOCK(&btl_proc->proc_lock);
    }
    return OPAL_SUCCESS;
}

/*
 * Look for an existing TCP process instance based on the globally unique
 * process identifier.
 */
mca_btl_tcp_proc_t* mca_btl_tcp_proc_lookup(const opal_process_name_t *name)
{
    mca_btl_tcp_proc_t* proc = NULL;

    OPAL_THREAD_LOCK(&mca_btl_tcp_component.tcp_lock);
    opal_proc_table_get_value(&mca_btl_tcp_component.tcp_procs,
                              *name, (void**)&proc);
    OPAL_THREAD_UNLOCK(&mca_btl_tcp_component.tcp_lock);
    if (OPAL_UNLIKELY(NULL == proc)) {
        mca_btl_base_endpoint_t *endpoint;
        opal_proc_t *opal_proc;

        BTL_VERBOSE(("adding tcp proc for unknown peer {%s}",
                     OPAL_NAME_PRINT(*name)));

        opal_proc = opal_proc_for_name (*name);
        if (NULL == opal_proc) {
            return NULL;
        }

        /* try adding this proc to each btl until */
        for( uint32_t i = 0; i < mca_btl_tcp_component.tcp_num_btls; ++i ) {
            endpoint = NULL;
            (void) mca_btl_tcp_add_procs (&mca_btl_tcp_component.tcp_btls[i]->super, 1, &opal_proc,
                                          &endpoint, NULL);
            if (NULL != endpoint && NULL == proc) {
                /* construct all the endpoints and get the proc */
                proc = endpoint->endpoint_proc;
            }
        }
    }

    return proc;
}

/*
 * loop through all available BTLs for one matching the source address
 * of the request.
 */
void mca_btl_tcp_proc_accept(mca_btl_tcp_proc_t* btl_proc, struct sockaddr* addr, int sd)
{
    OPAL_THREAD_LOCK(&btl_proc->proc_lock);
    int found_match = 0;
    mca_btl_base_endpoint_t* match_btl_endpoint;

    for( size_t i = 0; i < btl_proc->proc_endpoint_count; i++ ) {
        mca_btl_base_endpoint_t* btl_endpoint = btl_proc->proc_endpoints[i];
        /* We are not here to make a decision about what is good socket
         * and what is not. We simply check that this socket fit the endpoint
         * end we prepare for the real decision function mca_btl_tcp_endpoint_accept. */
        if( btl_endpoint->endpoint_addr->addr_family != addr->sa_family) {
            continue;
        }
        switch (addr->sa_family) {
        case AF_INET:
            if( memcmp( &btl_endpoint->endpoint_addr->addr_inet,
                        &(((struct sockaddr_in*)addr)->sin_addr),
                        sizeof(struct in_addr) ) ) {
                char tmp[2][16];
                opal_output_verbose(20, opal_btl_base_framework.framework_output,
                                    "btl: tcp: Match incoming connection from %s %s with locally known IP %s failed (iface %d/%d)!\n",
                                    OPAL_NAME_PRINT(btl_proc->proc_opal->proc_name),
                                    inet_ntop(AF_INET, (void*)&((struct sockaddr_in*)addr)->sin_addr,
                                              tmp[0], 16),
                                    inet_ntop(AF_INET, (void*)(struct in_addr*)&btl_endpoint->endpoint_addr->addr_inet,
                                              tmp[1], 16),
                                    (int)i, (int)btl_proc->proc_endpoint_count);
                continue;
            } else if (btl_endpoint->endpoint_state != MCA_BTL_TCP_CLOSED) {
                 found_match = 1;
                 match_btl_endpoint = btl_endpoint;
                 continue;
            }
            break;
#if OPAL_ENABLE_IPV6
        case AF_INET6:
            if( memcmp( &btl_endpoint->endpoint_addr->addr_inet,
                        &(((struct sockaddr_in6*)addr)->sin6_addr),
                        sizeof(struct in6_addr) ) ) {
                char tmp[2][INET6_ADDRSTRLEN];
                opal_output_verbose(20, opal_btl_base_framework.framework_output,
                                    "btl: tcp: Match incoming connection from %s %s with locally known IP %s failed (iface %d/%d)!\n",
                                    OPAL_NAME_PRINT(btl_proc->proc_opal->proc_name),
                                    inet_ntop(AF_INET6, (void*)&((struct sockaddr_in6*)addr)->sin6_addr,
                                              tmp[0], INET6_ADDRSTRLEN),
                                    inet_ntop(AF_INET6, (void*)(struct in6_addr*)&btl_endpoint->endpoint_addr->addr_inet,
                                              tmp[1], INET6_ADDRSTRLEN),
                                    (int)i, (int)btl_proc->proc_endpoint_count);
                continue;
            } else if (btl_endpoint->endpoint_state != MCA_BTL_TCP_CLOSED) {
                 found_match = 1;
                 match_btl_endpoint = btl_endpoint;
                 continue;
            }
            break;
#endif
        default:
            ;
        }

        /* Set state to CONNECTING to ensure that subsequent conenctions do not attempt to re-use endpoint in the num_links > 1 case*/
        btl_endpoint->endpoint_state = MCA_BTL_TCP_CONNECTING;
        (void)mca_btl_tcp_endpoint_accept(btl_endpoint, addr, sd);
        OPAL_THREAD_UNLOCK(&btl_proc->proc_lock);
        return;
    }
    /* In this case the connection was inbound to an address exported, but was not in a CLOSED state.
     * mca_btl_tcp_endpoint_accept() has logic to deal with the race condition that has likely caused this
     * scenario, so call it here.*/
    if (found_match) {
        (void)mca_btl_tcp_endpoint_accept(match_btl_endpoint, addr, sd);
        OPAL_THREAD_UNLOCK(&btl_proc->proc_lock);
        return;
    }
    /* No further use of this socket. Close it */
    CLOSE_THE_SOCKET(sd);
    {
        char *addr_str = NULL, *tmp;
        char ip[128];
        ip[sizeof(ip) - 1] = '\0';

        for (size_t i = 0; i < btl_proc->proc_endpoint_count; i++) {
            mca_btl_base_endpoint_t* btl_endpoint = btl_proc->proc_endpoints[i];
            if (btl_endpoint->endpoint_addr->addr_family != addr->sa_family) {
                continue;
            }
            inet_ntop(btl_endpoint->endpoint_addr->addr_family,
                      (void*) &(btl_endpoint->endpoint_addr->addr_inet),
                      ip, sizeof(ip) - 1);
            if (NULL == addr_str) {
                (void)asprintf(&tmp, "\n\t%s", ip);
            } else {
                (void)asprintf(&tmp, "%s\n\t%s", addr_str, ip);
                free(addr_str);
            }
            addr_str = tmp;
        }
        opal_show_help("help-mpi-btl-tcp.txt", "dropped inbound connection",
                       true, opal_process_info.nodename,
                       getpid(),
                       btl_proc->proc_opal->proc_hostname,
                       OPAL_NAME_PRINT(btl_proc->proc_opal->proc_name),
                       opal_net_get_hostname((struct sockaddr*)addr),
                       (NULL == addr_str) ? "NONE" : addr_str);
        if (NULL != addr_str) {
            free(addr_str);
        }
    }
    OPAL_THREAD_UNLOCK(&btl_proc->proc_lock);
}

/*
 * convert internal data structure (mca_btl_tcp_addr_t) to sockaddr_storage
 *
 */
bool mca_btl_tcp_proc_tosocks(mca_btl_tcp_addr_t* proc_addr,
                              struct sockaddr_storage* output)
{
    memset(output, 0, sizeof (*output));
    switch (proc_addr->addr_family) {
    case AF_INET:
        output->ss_family = AF_INET;
        memcpy(&((struct sockaddr_in*)output)->sin_addr,
               &proc_addr->addr_inet, sizeof(struct in_addr));
        ((struct sockaddr_in*)output)->sin_port = proc_addr->addr_port;
        break;
#if OPAL_ENABLE_IPV6
    case AF_INET6:
        {
            struct sockaddr_in6* inaddr = (struct sockaddr_in6*)output;
            output->ss_family = AF_INET6;
            memcpy(&inaddr->sin6_addr, &proc_addr->addr_inet,
                   sizeof (proc_addr->addr_inet));
            inaddr->sin6_port = proc_addr->addr_port;
            inaddr->sin6_scope_id = 0;
            inaddr->sin6_flowinfo = 0;
        }
        break;
#endif
    default:
        opal_output( 0, "mca_btl_tcp_proc: unknown af_family received: %d\n",
                     proc_addr->addr_family );
        return false;
    }
    return true;
}

