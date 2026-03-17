/*
 * Copyright (c) 2004-2008 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2006 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2016-2018 Intel, Inc. All rights reserved.
 * Copyright (c) 2018      IBM Corporation.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef PMIX_PTL_TCP_H
#define PMIX_PTL_TCP_H

#ifdef HAVE_NETINET_IN_H
#include <netinet/in.h>
#endif
#ifdef HAVE_ARPA_INET_H
#include <arpa/inet.h>
#endif
#ifdef HAVE_NETDB_H
#include <netdb.h>
#endif

#include "src/mca/ptl/ptl.h"

BEGIN_C_DECLS

typedef struct {
    pmix_ptl_base_component_t super;
    char *session_tmpdir;
    char *system_tmpdir;
    char *if_include;
    char *if_exclude;
    int ipv4_port;
    int ipv6_port;
    bool disable_ipv4_family;
    bool disable_ipv6_family;
    struct sockaddr_storage connection;
    char *session_filename;
    char *nspace_filename;
    char *system_filename;
    char *rendezvous_filename;
    int wait_to_connect;
    int max_retries;
    char *report_uri;
    bool remote_connections;
    int handshake_wait_time;
    int handshake_max_retries;
} pmix_ptl_tcp_component_t;

extern pmix_ptl_tcp_component_t mca_ptl_tcp_component;

extern pmix_ptl_module_t pmix_ptl_tcp_module;

END_C_DECLS

#endif /* PMIX_PTL_TCP_H */
