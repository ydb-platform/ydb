/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/**
 * @file
 */
#ifndef MCA_RCACHE_BASE_MEM_CB_H
#define MCA_RCACHE_BASE_MEM_CB_H

#include "opal_config.h"

BEGIN_C_DECLS

/*
 *  memory hook callback, called when memory is free'd out from under us
 */
void mca_rcache_base_mem_cb (void* base, size_t size, void* cbdata, bool from_alloc);

END_C_DECLS

#endif /* MCA_RCACHE_BASE_MEM_CB_H */
