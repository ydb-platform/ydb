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
 * Copyright (c) 2015-2017 Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include <src/include/pmix_config.h>

#include <stdio.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif

#include "src/util/argv.h"
#include "src/util/error.h"
#include "src/include/pmix_globals.h"

#include "src/mca/preg/base/base.h"

pmix_status_t pmix_preg_base_generate_node_regex(const char *input,
                                                 char **regex)
{
    pmix_preg_base_active_module_t *active;

    PMIX_LIST_FOREACH(active, &pmix_preg_globals.actives, pmix_preg_base_active_module_t) {
        if (NULL != active->module->generate_node_regex) {
            if (PMIX_SUCCESS == active->module->generate_node_regex(input, regex)) {
                return PMIX_SUCCESS;
            }
        }
    }

    return PMIX_ERR_NOT_SUPPORTED;
}

pmix_status_t pmix_preg_base_generate_ppn(const char *input,
                                          char **ppn)
{
    pmix_preg_base_active_module_t *active;

    PMIX_LIST_FOREACH(active, &pmix_preg_globals.actives, pmix_preg_base_active_module_t) {
        if (NULL != active->module->generate_ppn) {
            if (PMIX_SUCCESS == active->module->generate_ppn(input, ppn)) {
                return PMIX_SUCCESS;
            }
        }
    }

    return PMIX_ERR_NOT_SUPPORTED;
}

pmix_status_t pmix_preg_base_parse_nodes(const char *regexp,
                                         char ***names)
{
    pmix_preg_base_active_module_t *active;

    PMIX_LIST_FOREACH(active, &pmix_preg_globals.actives, pmix_preg_base_active_module_t) {
        if (NULL != active->module->parse_nodes) {
            if (PMIX_SUCCESS == active->module->parse_nodes(regexp, names)) {
                return PMIX_SUCCESS;
            }
        }
    }

    return PMIX_ERR_NOT_SUPPORTED;
}

pmix_status_t pmix_preg_base_parse_procs(const char *regexp,
                                         char ***procs)
{
    pmix_preg_base_active_module_t *active;

    PMIX_LIST_FOREACH(active, &pmix_preg_globals.actives, pmix_preg_base_active_module_t) {
        if (NULL != active->module->parse_procs) {
            if (PMIX_SUCCESS == active->module->parse_procs(regexp, procs)) {
                return PMIX_SUCCESS;
            }
        }
    }

    return PMIX_ERR_NOT_SUPPORTED;
}

pmix_status_t pmix_preg_base_resolve_peers(const char *nodename,
                                           const char *nspace,
                                           pmix_proc_t **procs, size_t *nprocs)
{
    pmix_preg_base_active_module_t *active;

    PMIX_LIST_FOREACH(active, &pmix_preg_globals.actives, pmix_preg_base_active_module_t) {
        if (NULL != active->module->resolve_peers) {
            if (PMIX_SUCCESS == active->module->resolve_peers(nodename, nspace, procs, nprocs)) {
                return PMIX_SUCCESS;
            }
        }
    }

    return PMIX_ERR_NOT_SUPPORTED;
}

pmix_status_t pmix_preg_base_resolve_nodes(const char *nspace,
                                           char **nodelist)
{
    pmix_preg_base_active_module_t *active;

    PMIX_LIST_FOREACH(active, &pmix_preg_globals.actives, pmix_preg_base_active_module_t) {
        if (NULL != active->module->resolve_nodes) {
            if (PMIX_SUCCESS == active->module->resolve_nodes(nspace, nodelist)) {
                return PMIX_SUCCESS;
            }
        }
    }

    return PMIX_ERR_NOT_SUPPORTED;
}
