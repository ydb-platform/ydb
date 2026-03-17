/* -*- C -*-
 *
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
 * Copyright (c) 2012      Los Alamos National Security, Inc.  All rights reserved.
 * Copyright (c) 2014-2018 Intel, Inc. All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 */
#ifndef PMIX_PNET_BASE_H_
#define PMIX_PNET_BASE_H_

#include <src/include/pmix_config.h>


#ifdef HAVE_SYS_TIME_H
#include <sys/time.h> /* for struct timeval */
#endif
#ifdef HAVE_STRING_H
#include <string.h>
#endif

#include "src/class/pmix_list.h"
#include "src/class/pmix_pointer_array.h"
#include "src/mca/mca.h"
#include "src/mca/base/pmix_mca_base_framework.h"

#include "src/mca/pnet/pnet.h"


BEGIN_C_DECLS

/*
 * MCA Framework
 */
PMIX_EXPORT extern pmix_mca_base_framework_t pmix_pnet_base_framework;
/**
 * PNET select function
 *
 * Cycle across available components and construct the list
 * of active modules
 */
PMIX_EXPORT pmix_status_t pmix_pnet_base_select(void);

/**
 * Track an active component / module
 */
struct pmix_pnet_base_active_module_t {
    pmix_list_item_t super;
    int pri;
    pmix_pnet_module_t *module;
    pmix_pnet_base_component_t *component;
};
typedef struct pmix_pnet_base_active_module_t pmix_pnet_base_active_module_t;
PMIX_EXPORT PMIX_CLASS_DECLARATION(pmix_pnet_base_active_module_t);

typedef struct {
    pmix_list_item_t super;
    char *nspace;
    pmix_rank_t *ranks;
    size_t np;
} pmix_pnet_local_procs_t;
PMIX_EXPORT PMIX_CLASS_DECLARATION(pmix_pnet_local_procs_t);

typedef struct {
    pmix_list_item_t super;
    char *name;
    pmix_list_t resources;
}pmix_pnet_resource_t;
PMIX_EXPORT PMIX_CLASS_DECLARATION(pmix_pnet_resource_t);

typedef struct {
    pmix_list_item_t super;
    size_t index;
    char *name;
    pmix_list_t local_jobs;    // list of pmix_pnet_local_procs_t
    pmix_list_t resources;     // list of pmix_pnet_resource_t
} pmix_pnet_node_t;
PMIX_EXPORT PMIX_CLASS_DECLARATION(pmix_pnet_node_t);

typedef struct {
    pmix_list_item_t super;
    char *nspace;
    pmix_pointer_array_t nodes;
} pmix_pnet_job_t;
PMIX_EXPORT PMIX_CLASS_DECLARATION(pmix_pnet_job_t);

/* framework globals */
struct pmix_pnet_globals_t {
    pmix_lock_t lock;
    pmix_list_t actives;
    bool initialized;
    pmix_list_t jobs;
    pmix_list_t nodes;
};
typedef struct pmix_pnet_globals_t pmix_pnet_globals_t;

PMIX_EXPORT extern pmix_pnet_globals_t pmix_pnet_globals;

PMIX_EXPORT pmix_status_t pmix_pnet_base_allocate(char *nspace,
                                                  pmix_info_t info[], size_t ninfo,
                                                  pmix_list_t *ilist);
PMIX_EXPORT pmix_status_t pmix_pnet_base_setup_local_network(char *nspace,
                                                             pmix_info_t info[],
                                                             size_t ninfo);
PMIX_EXPORT pmix_status_t pmix_pnet_base_setup_fork(const pmix_proc_t *peer, char ***env);
PMIX_EXPORT void pmix_pnet_base_child_finalized(pmix_proc_t *peer);
PMIX_EXPORT void pmix_pnet_base_local_app_finalized(pmix_namespace_t *nptr);
PMIX_EXPORT void pmix_pnet_base_deregister_nspace(char *nspace);
PMIX_EXPORT void pmix_pnet_base_collect_inventory(pmix_info_t directives[], size_t ndirs,
                                                  pmix_inventory_cbfunc_t cbfunc,
                                                  void *cbdata);
PMIX_EXPORT void pmix_pnet_base_deliver_inventory(pmix_info_t info[], size_t ninfo,
                                                  pmix_info_t directives[], size_t ndirs,
                                                  pmix_op_cbfunc_t cbfunc, void *cbdata);
PMIX_EXPORT pmix_status_t pmix_pnet_base_harvest_envars(char **incvars, char **excvars,
                                                        pmix_list_t *ilist);

END_C_DECLS

#endif
