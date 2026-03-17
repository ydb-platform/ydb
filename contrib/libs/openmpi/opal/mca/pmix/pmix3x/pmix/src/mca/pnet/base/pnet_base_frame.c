/* -*- Mode: C; c-basic-offset:4 ; -*- */
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
 * Copyright (c) 2012-2013 Los Alamos National Security, Inc.  All rights reserved.
 * Copyright (c) 2014-2018 Intel, Inc. All rights reserved.
 * Copyright (c) 2015-2016 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/** @file:
 *
 */
#include <src/include/pmix_config.h>

#include <pmix_common.h>

#ifdef HAVE_STRING_H
#include <string.h>
#endif

#include "src/class/pmix_list.h"
#include "src/mca/base/base.h"
#include "src/mca/pnet/base/base.h"

/*
 * The following file was created by configure.  It contains extern
 * statements and the definition of an array of pointers to each
 * component's public mca_base_component_t struct.
 */

#include "src/mca/pnet/base/static-components.h"

/* Instantiate the global vars */
pmix_pnet_globals_t pmix_pnet_globals = {{0}};
pmix_pnet_API_module_t pmix_pnet = {
    .allocate = pmix_pnet_base_allocate,
    .setup_local_network = pmix_pnet_base_setup_local_network,
    .setup_fork = pmix_pnet_base_setup_fork,
    .child_finalized = pmix_pnet_base_child_finalized,
    .local_app_finalized = pmix_pnet_base_local_app_finalized,
    .deregister_nspace = pmix_pnet_base_deregister_nspace,
    .collect_inventory = pmix_pnet_base_collect_inventory,
    .deliver_inventory = pmix_pnet_base_deliver_inventory
};

static pmix_status_t pmix_pnet_close(void)
{
  pmix_pnet_base_active_module_t *active, *prev;

    if (!pmix_pnet_globals.initialized) {
        return PMIX_SUCCESS;
    }
    pmix_pnet_globals.initialized = false;

    PMIX_LIST_FOREACH_SAFE(active, prev, &pmix_pnet_globals.actives, pmix_pnet_base_active_module_t) {
      pmix_list_remove_item(&pmix_pnet_globals.actives, &active->super);
      if (NULL != active->module->finalize) {
        active->module->finalize();
      }
      PMIX_RELEASE(active);
    }
    PMIX_DESTRUCT(&pmix_pnet_globals.actives);

    PMIX_LIST_DESTRUCT(&pmix_pnet_globals.jobs);
    PMIX_LIST_DESTRUCT(&pmix_pnet_globals.nodes);

    PMIX_DESTRUCT_LOCK(&pmix_pnet_globals.lock);
    return pmix_mca_base_framework_components_close(&pmix_pnet_base_framework, NULL);
}

static pmix_status_t pmix_pnet_open(pmix_mca_base_open_flag_t flags)
{
    /* initialize globals */
    pmix_pnet_globals.initialized = true;
    PMIX_CONSTRUCT_LOCK(&pmix_pnet_globals.lock);
    pmix_pnet_globals.lock.active = false;
    PMIX_CONSTRUCT(&pmix_pnet_globals.actives, pmix_list_t);
    PMIX_CONSTRUCT(&pmix_pnet_globals.jobs, pmix_list_t);
    PMIX_CONSTRUCT(&pmix_pnet_globals.nodes, pmix_list_t);

    /* Open up all available components */
    return pmix_mca_base_framework_components_open(&pmix_pnet_base_framework, flags);
}

PMIX_MCA_BASE_FRAMEWORK_DECLARE(pmix, pnet, "PMIx Network Operations",
                                NULL, pmix_pnet_open, pmix_pnet_close,
                                mca_pnet_base_static_components, 0);

PMIX_CLASS_INSTANCE(pmix_pnet_base_active_module_t,
                    pmix_list_item_t,
                    NULL, NULL);

static void lpcon(pmix_pnet_local_procs_t *p)
{
    p->nspace = NULL;
    p->ranks = NULL;
    p->np = 0;
}
static void lpdes(pmix_pnet_local_procs_t *p)
{
    if (NULL != p->nspace) {
        free(p->nspace);
    }
    if (NULL != p->ranks) {
        free(p->ranks);
    }
}
PMIX_CLASS_INSTANCE(pmix_pnet_local_procs_t,
                    pmix_list_item_t,
                    lpcon, lpdes);

static void ndcon(pmix_pnet_node_t *p)
{
    p->name = NULL;
    PMIX_CONSTRUCT(&p->local_jobs, pmix_list_t);
    PMIX_CONSTRUCT(&p->resources, pmix_list_t);
}
static void nddes(pmix_pnet_node_t *p)
{
    if (NULL != p->name) {
        free(p->name);
    }
    PMIX_LIST_DESTRUCT(&p->local_jobs);
    PMIX_LIST_DESTRUCT(&p->resources);
}
PMIX_CLASS_INSTANCE(pmix_pnet_node_t,
                    pmix_list_item_t,
                    ndcon, nddes);

static void jcon(pmix_pnet_job_t *p)
{
    p->nspace = NULL;
    PMIX_CONSTRUCT(&p->nodes, pmix_pointer_array_t);
    pmix_pointer_array_init(&p->nodes, 1, INT_MAX, 1);
}
static void jdes(pmix_pnet_job_t *p)
{
    int n;
    pmix_pnet_node_t *nd;

    if (NULL != p->nspace) {
        free(p->nspace);
    }
    for (n=0; n < p->nodes.size; n++) {
        if (NULL != (nd = (pmix_pnet_node_t*)pmix_pointer_array_get_item(&p->nodes, n))) {
            pmix_pointer_array_set_item(&p->nodes, n, NULL);
            PMIX_RELEASE(nd);
        }
    }
    PMIX_DESTRUCT(&p->nodes);
}
PMIX_CLASS_INSTANCE(pmix_pnet_job_t,
                    pmix_list_item_t,
                    jcon, jdes);

static void rcon(pmix_pnet_resource_t *p)
{
    p->name = NULL;
    PMIX_CONSTRUCT(&p->resources, pmix_list_t);
}
static void rdes(pmix_pnet_resource_t *p)
{
    if (NULL != p->name) {
        free(p->name);
    }
    PMIX_LIST_DESTRUCT(&p->resources);
}
PMIX_CLASS_INSTANCE(pmix_pnet_resource_t,
                    pmix_list_item_t,
                    rcon, rdes);
