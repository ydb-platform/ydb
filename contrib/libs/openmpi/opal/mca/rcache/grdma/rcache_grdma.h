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
 * Copyright (c) 2011-2018 Los Alamos National Security, LLC. All rights
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
#ifndef MCA_RCACHE_GRDMA_H
#define MCA_RCACHE_GRDMA_H

#include "opal_config.h"
#include "opal/class/opal_list.h"
#include "opal/mca/event/event.h"
#include "opal/mca/rcache/rcache.h"
#if HAVE_SYS_MMAN_H
#include <sys/mman.h>
#endif

#define MCA_RCACHE_GRDMA_REG_FLAG_IN_LRU MCA_RCACHE_FLAGS_MOD_RESV0

BEGIN_C_DECLS

struct mca_rcache_grdma_cache_t {
    opal_list_item_t super;
    char *cache_name;
    opal_list_t lru_list;
    opal_lifo_t gc_lifo;
    mca_rcache_base_vma_module_t *vma_module;
};
typedef struct mca_rcache_grdma_cache_t mca_rcache_grdma_cache_t;

OBJ_CLASS_DECLARATION(mca_rcache_grdma_cache_t);

struct mca_rcache_grdma_component_t {
    mca_rcache_base_component_t super;
    opal_list_t caches;
    char *rcache_name;
    bool print_stats;
    int leave_pinned;
};
typedef struct mca_rcache_grdma_component_t mca_rcache_grdma_component_t;

OPAL_DECLSPEC extern mca_rcache_grdma_component_t mca_rcache_grdma_component;

struct mca_rcache_grdma_module_t;

struct mca_rcache_grdma_module_t {
    mca_rcache_base_module_t super;
    struct mca_rcache_base_resources_t resources;
    mca_rcache_grdma_cache_t *cache;
    opal_free_list_t reg_list;
    uint32_t stat_cache_hit;
    uint32_t stat_cache_miss;
    uint32_t stat_evicted;
    uint32_t stat_cache_found;
    uint32_t stat_cache_notfound;
};
typedef struct mca_rcache_grdma_module_t mca_rcache_grdma_module_t;

/*
 *  Initializes the rcache module.
 */
void mca_rcache_grdma_module_init(mca_rcache_grdma_module_t *rcache, mca_rcache_grdma_cache_t *cache);

END_C_DECLS
#endif
