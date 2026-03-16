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
 * Copyright (c) 2006      Voltaire. All rights reserved.
 * Copyright (c) 2007-2014 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2012-2015 Los Alamos National Security, LLC. All rights
 *                         reserved.
 *
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#define OPAL_DISABLE_ENABLE_MEM_DEBUG 1
#include "opal_config.h"
#include "opal/mca/base/base.h"
#include "opal/runtime/opal_params.h"
#include "rcache_grdma.h"
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#include <stdlib.h>
#include <fcntl.h>

/*
 * Local functions
 */
static int grdma_open(void);
static int grdma_close(void);
static int grdma_register(void);
static mca_rcache_base_module_t* grdma_init(
        struct mca_rcache_base_resources_t* resources);

mca_rcache_grdma_component_t mca_rcache_grdma_component = {
    {
      /* First, the mca_base_component_t struct containing meta
         information about the component itself */

        .rcache_version = {
            MCA_RCACHE_BASE_VERSION_3_0_0,

            .mca_component_name = "grdma",
            MCA_BASE_MAKE_VERSION(component, OPAL_MAJOR_VERSION, OPAL_MINOR_VERSION,
                                  OPAL_RELEASE_VERSION),
            .mca_open_component = grdma_open,
            .mca_close_component = grdma_close,
            .mca_register_component_params = grdma_register,
        },
        .rcache_data = {
            /* The component is checkpoint ready */
            MCA_BASE_METADATA_PARAM_CHECKPOINT
        },

        .rcache_init = grdma_init,
    }
};

/**
  * component open/close/init function
  */
static int grdma_open(void)
{
    OBJ_CONSTRUCT(&mca_rcache_grdma_component.caches, opal_list_t);

    return OPAL_SUCCESS;
}


static int grdma_register(void)
{
    mca_rcache_grdma_component.print_stats = false;
    (void) mca_base_component_var_register(&mca_rcache_grdma_component.super.rcache_version,
                                           "print_stats", "print registration cache usage statistics at the end of the run",
                                           MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                           OPAL_INFO_LVL_9,
                                           MCA_BASE_VAR_SCOPE_READONLY,
                                           &mca_rcache_grdma_component.print_stats);

    return OPAL_SUCCESS;
}


static int grdma_close(void)
{
    OPAL_LIST_DESTRUCT(&mca_rcache_grdma_component.caches);
    return OPAL_SUCCESS;
}


static mca_rcache_base_module_t *
grdma_init(struct mca_rcache_base_resources_t *resources)
{
    mca_rcache_grdma_module_t *rcache_module;
    mca_rcache_grdma_cache_t *cache = NULL, *item;

    /* Set this here (vs in component.c) because
       opal_leave_pinned* may have been set after MCA params were
       read (e.g., by the openib btl) */
    mca_rcache_grdma_component.leave_pinned = (int)
        (1 == opal_leave_pinned || opal_leave_pinned_pipeline);

    /* find the specified pool */
    OPAL_LIST_FOREACH(item, &mca_rcache_grdma_component.caches, mca_rcache_grdma_cache_t) {
        if (0 == strcmp (item->cache_name, resources->cache_name)) {
            cache = item;
            break;
        }
    }

    if (NULL == cache) {
        /* create new cache */
        cache = OBJ_NEW(mca_rcache_grdma_cache_t);
        if (NULL == cache) {
            return NULL;
        }

        cache->cache_name = strdup (resources->cache_name);

        opal_list_append (&mca_rcache_grdma_component.caches, &cache->super);
    }

    rcache_module = (mca_rcache_grdma_module_t *) malloc (sizeof (*rcache_module));

    rcache_module->resources = *resources;

    mca_rcache_grdma_module_init (rcache_module, cache);

    return &rcache_module->super;
}
