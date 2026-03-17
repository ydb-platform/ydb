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
 * Copyright (c) 2014-2017 Intel, Inc. All rights reserved.
 * Copyright (c) 2015-2016 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2019      IBM Corporation.  All rights reserved.
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
#include "src/mca/preg/base/base.h"

/*
 * The following file was created by configure.  It contains extern
 * statements and the definition of an array of pointers to each
 * component's public mca_base_component_t struct.
 */

#include "src/mca/preg/base/static-components.h"

/* Instantiate the global vars */
pmix_preg_globals_t pmix_preg_globals = {{{0}}};
pmix_preg_module_t pmix_preg = {
    .generate_node_regex = pmix_preg_base_generate_node_regex,
    .generate_ppn = pmix_preg_base_generate_ppn,
    .parse_nodes = pmix_preg_base_parse_nodes,
    .parse_procs = pmix_preg_base_parse_procs,
    .resolve_peers = pmix_preg_base_resolve_peers,
    .resolve_nodes = pmix_preg_base_resolve_nodes
};

static pmix_status_t pmix_preg_close(void)
{
    if (!pmix_preg_globals.initialized) {
        return PMIX_SUCCESS;
    }
    pmix_preg_globals.initialized = false;

    PMIX_LIST_DESTRUCT(&pmix_preg_globals.actives);

    return pmix_mca_base_framework_components_close(&pmix_preg_base_framework, NULL);
}

static pmix_status_t pmix_preg_open(pmix_mca_base_open_flag_t flags)
{
    /* initialize globals */
    pmix_preg_globals.initialized = true;
    PMIX_CONSTRUCT(&pmix_preg_globals.actives, pmix_list_t);

    /* Open up all available components */
    return pmix_mca_base_framework_components_open(&pmix_preg_base_framework, flags);
}

PMIX_MCA_BASE_FRAMEWORK_DECLARE(pmix, preg, "PMIx Regex Operations",
                                NULL, pmix_preg_open, pmix_preg_close,
                                mca_preg_base_static_components, 0);

PMIX_CLASS_INSTANCE(pmix_preg_base_active_module_t,
                    pmix_list_item_t,
                    NULL, NULL);

static void rcon(pmix_regex_range_t *p)
{
    p->start = 0;
    p->cnt = 0;
}
PMIX_CLASS_INSTANCE(pmix_regex_range_t,
                    pmix_list_item_t,
                    rcon, NULL);

static void rvcon(pmix_regex_value_t *p)
{
    p->prefix = NULL;
    p->suffix = NULL;
    p->num_digits = 0;
    p->skip = false;
    PMIX_CONSTRUCT(&p->ranges, pmix_list_t);
}
static void rvdes(pmix_regex_value_t *p)
{
    if (NULL != p->prefix) {
        free(p->prefix);
    }
    if (NULL != p->suffix) {
        free(p->suffix);
    }
    PMIX_LIST_DESTRUCT(&p->ranges);
}
PMIX_CLASS_INSTANCE(pmix_regex_value_t,
                    pmix_list_item_t,
                    rvcon, rvdes);
