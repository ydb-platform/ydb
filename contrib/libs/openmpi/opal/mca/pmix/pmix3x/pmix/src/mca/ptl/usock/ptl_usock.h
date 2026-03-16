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
 * Copyright (c) 2016-2017 Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef PMIX_PTL_USOCK_H
#define PMIX_PTL_USOCK_H

#include "src/mca/ptl/ptl.h"

BEGIN_C_DECLS

typedef struct {
    pmix_ptl_base_component_t super;
    char *tmpdir;
    struct sockaddr_storage connection;
    char *filename;
} pmix_ptl_usock_component_t;

/* header for messages */
typedef struct {
    int pindex;
    uint32_t tag;
    size_t nbytes;
} pmix_usock_hdr_t;

extern pmix_ptl_usock_component_t mca_ptl_usock_component;

extern pmix_ptl_module_t pmix_ptl_usock_module;

void pmix_usock_send_handler(int sd, short args, void *cbdata);

void pmix_usock_recv_handler(int sd, short args, void *cbdata);

END_C_DECLS

#endif /* PMIX_PTL_USOCK_H */
