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
 * Copyright (c) 2012-2015 Los Alamos National Security, LLC. All rights
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
#ifndef MCA_RCACHE_BASE_H
#define MCA_RCACHE_BASE_H

#include "opal_config.h"

#include "opal/class/opal_list.h"
#include "opal/mca/mca.h"
#include "opal/mca/rcache/rcache.h"
#include "opal/mca/memory/base/base.h"

BEGIN_C_DECLS

/*
 * create a module by name
 */
OPAL_DECLSPEC mca_rcache_base_module_t *mca_rcache_base_module_create (const char *name, void *user_data,
                                                                       mca_rcache_base_resources_t *rcache_resources);

/*
 * MCA framework
 */
OPAL_DECLSPEC extern mca_base_framework_t opal_rcache_base_framework;

struct mca_rcache_base_selected_module_t {
    opal_list_item_t super;
    mca_rcache_base_component_t *rcache_component;
    mca_rcache_base_module_t *rcache_module;
    void *user_data;
};
typedef struct mca_rcache_base_selected_module_t mca_rcache_base_selected_module_t;

OPAL_DECLSPEC OBJ_CLASS_DECLARATION(mca_rcache_base_selected_module_t);

OPAL_DECLSPEC mca_rcache_base_component_t *mca_rcache_base_component_lookup(const char *name);
OPAL_DECLSPEC mca_rcache_base_module_t *mca_rcache_base_module_lookup (const char *name);
OPAL_DECLSPEC int mca_rcache_base_module_destroy(mca_rcache_base_module_t *module);

extern opal_free_list_t mca_rcache_base_vma_tree_items;
extern bool mca_rcache_base_vma_tree_items_inited;
extern unsigned int mca_rcache_base_vma_tree_items_min;
extern int mca_rcache_base_vma_tree_items_max;
extern unsigned int mca_rcache_base_vma_tree_items_inc;

/* only used within base -- no need to DECLSPEC */
extern int mca_rcache_base_used_mem_hooks;

/*
 * Globals
 */
OPAL_DECLSPEC extern opal_list_t mca_rcache_base_modules;

END_C_DECLS

#endif /* MCA_RCACHE_BASE_H */
