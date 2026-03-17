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
#include "src/util/error.h"
#include "src/util/show_help.h"

#include "src/mca/preg/base/base.h"

static bool selected = false;

/* Function for selecting a prioritized list of components
 * from all those that are available. */
int pmix_preg_base_select(void)
{
    pmix_mca_base_component_list_item_t *cli = NULL;
    pmix_mca_base_component_t *component = NULL;
    pmix_mca_base_module_t *module = NULL;
    pmix_preg_module_t *nmodule;
    pmix_preg_base_active_module_t *newmodule, *mod;
    int rc, priority;
    bool inserted;

    if (selected) {
        /* ensure we don't do this twice */
        return PMIX_SUCCESS;
    }
    selected = true;

    /* Query all available components and ask if they have a module */
    PMIX_LIST_FOREACH(cli, &pmix_preg_base_framework.framework_components, pmix_mca_base_component_list_item_t) {
        component = (pmix_mca_base_component_t *) cli->cli_component;

        pmix_output_verbose(5, pmix_preg_base_framework.framework_output,
                            "mca:preg:select: checking available component %s", component->pmix_mca_component_name);

        /* If there's no query function, skip it */
        if (NULL == component->pmix_mca_query_component) {
            pmix_output_verbose(5, pmix_preg_base_framework.framework_output,
                                "mca:preg:select: Skipping component [%s]. It does not implement a query function",
                                component->pmix_mca_component_name );
            continue;
        }

        /* Query the component */
        pmix_output_verbose(5, pmix_preg_base_framework.framework_output,
                            "mca:preg:select: Querying component [%s]",
                            component->pmix_mca_component_name);
        rc = component->pmix_mca_query_component(&module, &priority);

        /* If no module was returned, then skip component */
        if (PMIX_SUCCESS != rc || NULL == module) {
            pmix_output_verbose(5, pmix_preg_base_framework.framework_output,
                                "mca:preg:select: Skipping component [%s]. Query failed to return a module",
                                component->pmix_mca_component_name );
            continue;
        }

        /* If we got a module, keep it */
        nmodule = (pmix_preg_module_t*) module;
        /* add to the list of selected modules */
        newmodule = PMIX_NEW(pmix_preg_base_active_module_t);
        newmodule->pri = priority;
        newmodule->module = nmodule;
        newmodule->component = (pmix_mca_base_component_t*)cli->cli_component;

        /* maintain priority order */
        inserted = false;
        PMIX_LIST_FOREACH(mod, &pmix_preg_globals.actives, pmix_preg_base_active_module_t) {
            if (priority > mod->pri) {
                pmix_list_insert_pos(&pmix_preg_globals.actives,
                                     (pmix_list_item_t*)mod, &newmodule->super);
                inserted = true;
                break;
            }
        }
        if (!inserted) {
            /* must be lowest priority - add to end */
            pmix_list_append(&pmix_preg_globals.actives, &newmodule->super);
        }
    }

    /* if no modules were found, then that's an error as we require at least one */
    if (0 == pmix_list_get_size(&pmix_preg_globals.actives)) {
        pmix_show_help("help-pmix-runtime.txt", "no-plugins", true, "PREG");
        return PMIX_ERR_SILENT;
    }

    if (4 < pmix_output_get_verbosity(pmix_preg_base_framework.framework_output)) {
        pmix_output(0, "Final preg priorities");
        /* show the prioritized list */
        PMIX_LIST_FOREACH(mod, &pmix_preg_globals.actives, pmix_preg_base_active_module_t) {
            pmix_output(0, "\tpreg: %s Priority: %d", mod->component->pmix_mca_component_name, mod->pri);
        }
    }

    return PMIX_SUCCESS;;
}
