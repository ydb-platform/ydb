/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2006 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2006      Voltaire. All rights reserved.
 * Copyright (c) 2012-2015 NVIDIA Corporation.  All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC.  All rights
 *                         reserved.
 *
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/**
 * @file
 */
#ifndef MCA_RCACHE_GPUSM_H
#define MCA_RCACHE_GPUSM_H

#include "opal_config.h"
#include "opal/class/opal_list.h"
#include "opal/mca/rcache/rcache.h"

BEGIN_C_DECLS

#define MEMHANDLE_SIZE 8
#define EVTHANDLE_SIZE 8
struct mca_rcache_gpusm_registration_t {
    mca_rcache_base_registration_t base;
    uint64_t memHandle[MEMHANDLE_SIZE]; /* CUipcMemHandle */
    uint64_t evtHandle[EVTHANDLE_SIZE]; /* CUipcEventHandle */
    uintptr_t event;                    /* CUevent */
};
typedef struct mca_rcache_gpusm_registration_t mca_rcache_gpusm_registration_t;
OPAL_DECLSPEC OBJ_CLASS_DECLARATION(mca_rcache_gpusm_registration_t);

struct mca_rcache_gpusm_component_t {
    mca_rcache_base_component_t super;
};
typedef struct mca_rcache_gpusm_component_t mca_rcache_gpusm_component_t;

OPAL_DECLSPEC extern mca_rcache_gpusm_component_t mca_rcache_gpusm_component;

struct mca_rcache_gpusm_module_t {
    mca_rcache_base_module_t super;
    opal_free_list_t reg_list;
}; typedef struct mca_rcache_gpusm_module_t mca_rcache_gpusm_module_t;

/*
 *  Initializes the rcache module.
 */
void mca_rcache_gpusm_module_init(mca_rcache_gpusm_module_t *rcache);

/**
  * register block of memory
  */
int mca_rcache_gpusm_register(mca_rcache_base_module_t* rcache, void *addr,
        size_t size, uint32_t flags, int32_t access_flags, mca_rcache_base_registration_t **reg);

/**
 * deregister memory
 */
int mca_rcache_gpusm_deregister(mca_rcache_base_module_t *rcache,
        mca_rcache_base_registration_t *reg);

/**
 * find registration for a given block of memory
 */
int mca_rcache_gpusm_find(struct mca_rcache_base_module_t* rcache, void* addr,
        size_t size, mca_rcache_base_registration_t **reg);

/**
 * finalize rcache
 */
void mca_rcache_gpusm_finalize(struct mca_rcache_base_module_t *rcache);

END_C_DECLS
#endif
