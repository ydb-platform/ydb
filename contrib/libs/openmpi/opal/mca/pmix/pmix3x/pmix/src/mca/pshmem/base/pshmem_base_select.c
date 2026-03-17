/*
 * Copyright (c) 2004-2008 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2016-2017 Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include <src/include/pmix_config.h>
#include <pmix_common.h>

#include <string.h>

#include "src/mca/mca.h"
#include "src/mca/base/base.h"

#include "src/mca/pshmem/base/base.h"

static bool selected = false;

/* Function for selecting a prioritized list of components
 * from all those that are available. */
int pmix_pshmem_base_select(void)
{
    pmix_mca_base_component_list_item_t *cli;
    pmix_mca_base_component_t *component;
    pmix_mca_base_module_t *module;
    pmix_pshmem_base_module_t *nmodule;
    int rc, priority, best_pri = -1;
    bool inserted = false;

    if (selected) {
        /* ensure we don't do this twice */
        return PMIX_SUCCESS;
    }
    selected = true;

    /* Query all available components and ask if they have a module */
    PMIX_LIST_FOREACH(cli, &pmix_pshmem_base_framework.framework_components, pmix_mca_base_component_list_item_t) {
        component = (pmix_mca_base_component_t *) cli->cli_component;

        pmix_output_verbose(5, pmix_pshmem_base_framework.framework_output,
                            "mca:pshmem:select: checking available component %s", component->pmix_mca_component_name);

        /* If there's no query function, skip it */
        if (NULL == component->pmix_mca_query_component) {
            pmix_output_verbose(5, pmix_pshmem_base_framework.framework_output,
                                "mca:pshmem:select: Skipping component [%s]. It does not implement a query function",
                                component->pmix_mca_component_name );
            continue;
        }

        /* Query the component */
        pmix_output_verbose(5, pmix_pshmem_base_framework.framework_output,
                            "mca:pshmem:select: Querying component [%s]",
                            component->pmix_mca_component_name);
        rc = component->pmix_mca_query_component(&module, &priority);

        /* If no module was returned, then skip component */
        if (PMIX_SUCCESS != rc || NULL == module) {
            pmix_output_verbose(5, pmix_pshmem_base_framework.framework_output,
                                "mca:pshmem:select: Skipping component [%s]. Query failed to return a module",
                                component->pmix_mca_component_name );
            continue;
        }

        /* If we got a module, try to initialize it */
        nmodule = (pmix_pshmem_base_module_t*) module;
        if (NULL != nmodule->init && PMIX_SUCCESS != nmodule->init()) {
            continue;
        }

        /* keep only the highest priority module */
        if (best_pri < priority) {
            best_pri = priority;
            /* give any prior module a chance to finalize */
            if (NULL != pmix_pshmem.finalize) {
                pmix_pshmem.finalize();
            }
            pmix_pshmem = *nmodule;
            inserted = true;
        }
    }

    if (!inserted) {
        return PMIX_ERR_NOT_FOUND;
    }
    return PMIX_SUCCESS;
}
