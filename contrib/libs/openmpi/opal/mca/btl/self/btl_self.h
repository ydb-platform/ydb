/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2009 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2014-2016 Los Alamos National Security, LLC. All rights
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
#ifndef MCA_BTL_SELF_H
#define MCA_BTL_SELF_H

#include "opal_config.h"

#include <stdlib.h>
#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif  /* HAVE_SYS_TYPES_H */

#include "opal/mca/btl/btl.h"
#include "opal/mca/btl/base/base.h"

BEGIN_C_DECLS

#define MCA_BTL_SELF_MAX_INLINE_SIZE 128

/**
 * Shared Memory (SELF) BTL module.
 */
struct mca_btl_self_component_t {
    mca_btl_base_component_3_0_0_t super;  /**< base BTL component */
    int free_list_num;                     /**< initial size of free lists */
    int free_list_max;                     /**< maximum size of free lists */
    int free_list_inc;                     /**< number of elements to alloc when growing free lists */
    opal_free_list_t self_frags_eager;     /**< free list of self first */
    opal_free_list_t self_frags_send;      /**< free list of self second */
    opal_free_list_t self_frags_rdma;      /**< free list of self second */
};
typedef struct mca_btl_self_component_t mca_btl_self_component_t;
OPAL_MODULE_DECLSPEC extern mca_btl_self_component_t mca_btl_self_component;

extern mca_btl_base_module_t mca_btl_self;

END_C_DECLS

#endif

