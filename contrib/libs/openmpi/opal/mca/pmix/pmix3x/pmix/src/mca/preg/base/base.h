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
 * Copyright (c) 2014-2017 Intel, Inc. All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 */
#ifndef PMIX_PREG_BASE_H_
#define PMIX_PREG_BASE_H_

#include <src/include/pmix_config.h>


#ifdef HAVE_SYS_TIME_H
#include <sys/time.h> /* for struct timeval */
#endif
#ifdef HAVE_STRING_H
#include <string.h>
#endif

#include "src/class/pmix_pointer_array.h"
#include "src/mca/mca.h"
#include "src/mca/base/pmix_mca_base_framework.h"

#include "src/mca/preg/preg.h"


BEGIN_C_DECLS

/*
 * MCA Framework
 */
PMIX_EXPORT extern pmix_mca_base_framework_t pmix_preg_base_framework;
/**
 * PREG select function
 *
 * Cycle across available components and construct the list
 * of active modules
 */
PMIX_EXPORT pmix_status_t pmix_preg_base_select(void);

/**
 * Track an active component / module
 */
struct pmix_preg_base_active_module_t {
    pmix_list_item_t super;
    int pri;
    pmix_preg_module_t *module;
    pmix_mca_base_component_t *component;
};
typedef struct pmix_preg_base_active_module_t pmix_preg_base_active_module_t;
PMIX_CLASS_DECLARATION(pmix_preg_base_active_module_t);


/* framework globals */
struct pmix_preg_globals_t {
  pmix_list_t actives;
  bool initialized;
};
typedef struct pmix_preg_globals_t pmix_preg_globals_t;

PMIX_EXPORT extern pmix_preg_globals_t pmix_preg_globals;

PMIX_EXPORT pmix_status_t pmix_preg_base_generate_node_regex(const char *input,
                                                             char **regex);
PMIX_EXPORT pmix_status_t pmix_preg_base_generate_ppn(const char *input,
                                                      char **ppn);
PMIX_EXPORT pmix_status_t pmix_preg_base_parse_nodes(const char *regexp,
                                                     char ***names);
PMIX_EXPORT pmix_status_t pmix_preg_base_parse_procs(const char *regexp,
                                                     char ***procs);
PMIX_EXPORT pmix_status_t pmix_preg_base_resolve_peers(const char *nodename,
                                                       const char *nspace,
                                                       pmix_proc_t **procs, size_t *nprocs);
PMIX_EXPORT pmix_status_t pmix_preg_base_resolve_nodes(const char *nspace,
                                                       char **nodelist);


END_C_DECLS

#endif
