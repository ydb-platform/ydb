/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2007 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/**
 * @file
 */
#ifndef MCA_BTL_TCP_ADDR_H
#define MCA_BTL_TCP_ADDR_H

#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
#ifdef HAVE_SYS_SOCKET_H
#include <sys/socket.h>
#endif
#ifdef HAVE_NETINET_IN_H
#include <netinet/in.h>
#endif


/**
 * Structure used to publish TCP connection information to peers.
 */

struct mca_btl_tcp_addr_t {
    /* the following information is exchanged between different
       machines (read: byte order), so use network byte order
       for everything and don't add padding
    */
#if OPAL_ENABLE_IPV6
    struct in6_addr addr_inet;    /**< IPv4/IPv6 listen address > */
#else
    /* Bug, FIXME: needs testing */
    struct my_in6_addr {
        union {
            uint32_t u6_addr32[4];
            struct _my_in6_addr {
                struct in_addr _addr_inet;
                uint32_t _pad[3];
            } _addr__inet;
        } _union_inet;
    } addr_inet;
#endif
    in_port_t      addr_port;     /**< listen port */
    uint16_t       addr_ifkindex; /**< remote interface index assigned with
                                    this address */
    unsigned short addr_inuse;    /**< local meaning only */
    uint8_t        addr_family;   /**< AF_INET or AF_INET6 */
};
typedef struct mca_btl_tcp_addr_t mca_btl_tcp_addr_t;

#define MCA_BTL_TCP_AF_INET     0
#if OPAL_ENABLE_IPV6
# define MCA_BTL_TCP_AF_INET6   1
#endif

#endif

