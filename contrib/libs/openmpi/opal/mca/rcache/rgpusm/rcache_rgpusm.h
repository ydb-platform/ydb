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
#ifndef MCA_RCACHE_RGPUSM_H
#define MCA_RCACHE_RGPUSM_H

#include "opal_config.h"
#include "opal/class/opal_list.h"
#include "opal/class/opal_free_list.h"
#include "opal/mca/rcache/rcache.h"

BEGIN_C_DECLS

struct mca_rcache_rgpusm_component_t {
    mca_rcache_base_component_t super;
    char* rcache_name;
    unsigned long long rcache_size_limit;
    bool print_stats;
    int leave_pinned;
    int output;
    bool empty_cache;
};
typedef struct mca_rcache_rgpusm_component_t mca_rcache_rgpusm_component_t;

OPAL_DECLSPEC extern mca_rcache_rgpusm_component_t mca_rcache_rgpusm_component;

struct mca_rcache_rgpusm_module_t {
    mca_rcache_base_module_t super;
    opal_free_list_t reg_list;
    opal_list_t lru_list;
    uint32_t stat_cache_hit;
    uint32_t stat_cache_valid;
    uint32_t stat_cache_invalid;
    uint32_t stat_cache_miss;
    uint32_t stat_evicted;
    uint32_t stat_cache_found;
    uint32_t stat_cache_notfound;
    mca_rcache_base_vma_module_t *vma_module;
}; typedef struct mca_rcache_rgpusm_module_t mca_rcache_rgpusm_module_t;

/*
 *  Initializes the rcache module.
 */
void mca_rcache_rgpusm_module_init(mca_rcache_rgpusm_module_t *rcache);

/**
  * register block of memory
  */
int mca_rcache_rgpusm_register(mca_rcache_base_module_t* rcache, void *addr,
        size_t size, uint32_t flags, int32_t access_flags, mca_rcache_base_registration_t **reg);

/**
 * deregister memory
 */
int mca_rcache_rgpusm_deregister(mca_rcache_base_module_t *rcache,
        mca_rcache_base_registration_t *reg);

/**
  * free memory allocated by alloc function
  */
void mca_rcache_rgpusm_free(mca_rcache_base_module_t *rcache, void * addr,
        mca_rcache_base_registration_t *reg);

/**
 * find registration for a given block of memory
 */
int mca_rcache_rgpusm_find(struct mca_rcache_base_module_t* rcache, void* addr,
        size_t size, mca_rcache_base_registration_t **reg);

/**
 * unregister all registration covering the block of memory
 */
int mca_rcache_rgpusm_release_memory(mca_rcache_base_module_t* rcache, void *base,
        size_t size);

/**
 * finalize rcache
 */
void mca_rcache_rgpusm_finalize(struct mca_rcache_base_module_t *rcache);

END_C_DECLS
#endif
