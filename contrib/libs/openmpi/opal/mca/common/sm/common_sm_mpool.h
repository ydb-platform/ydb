/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2006 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007      Sun Microsystems, Inc.  All rights reserved.
 * Copyright (c) 2009      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2010-2015 Los Alamos National Security, LLC.
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
#ifndef MCA_COMMON_SM_MPOOL_H
#define MCA_COMMON_SM_MPOOL_H

#include "opal_config.h"

#include "opal/mca/event/event.h"
#include "opal/mca/shmem/shmem.h"

#include "opal/mca/mpool/mpool.h"
#include "opal/mca/allocator/allocator.h"

BEGIN_C_DECLS

struct mca_common_sm_module_t;

typedef struct mca_common_sm_mpool_resources_t {
    size_t  size;
    int32_t mem_node;
    const char *allocator;
    /* backing store metadata */
    opal_shmem_ds_t bs_meta_buf;
} mca_common_sm_mpool_resources_t;

typedef struct mca_common_sm_mpool_module_t {
    mca_mpool_base_module_t super;
    long sm_size;
    mca_allocator_base_module_t *sm_allocator;
    struct mca_common_sm_mpool_mmap_t *sm_mmap;
    struct mca_common_sm_module_t *sm_common_module;
    int32_t mem_node;
} mca_common_sm_mpool_module_t;

OPAL_DECLSPEC mca_mpool_base_module_t *common_sm_mpool_create (mca_common_sm_mpool_resources_t *);

END_C_DECLS

#endif
