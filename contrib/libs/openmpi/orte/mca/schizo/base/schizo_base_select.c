/*
 * Copyright (c) 2015      Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "orte_config.h"
#include "orte/constants.h"

#include <stdio.h>
#include <string.h>

#include "orte/mca/mca.h"
#include "opal/util/output.h"
#include "opal/mca/base/base.h"

#include "orte/util/show_help.h"

#include "orte/runtime/orte_globals.h"
#include "orte/mca/schizo/schizo.h"
#include "orte/mca/schizo/base/base.h"

/**
 * Function for selecting all runnable modules from those that are
 * available.
 */

int orte_schizo_base_select(void)
{
    mca_base_component_list_item_t *cli = NULL;
    mca_base_component_t *component = NULL;
    mca_base_module_t *module = NULL;
    orte_schizo_base_module_t *nmodule;
    orte_schizo_base_active_module_t *newmodule, *mod;
    int rc, priority;
    bool inserted;

    if (0 < opal_list_get_size(&orte_schizo_base.active_modules)) {
        /* ensure we don't do this twice */
        return ORTE_SUCCESS;
    }

    /* Query all available components and ask if they have a module */
    OPAL_LIST_FOREACH(cli, &orte_schizo_base_framework.framework_components, mca_base_component_list_item_t) {
        component = (mca_base_component_t *) cli->cli_component;

        opal_output_verbose(5, orte_schizo_base_framework.framework_output,
                            "mca:schizo:select: checking available component %s", component->mca_component_name);

        /* If there's no query function, skip it */
        if (NULL == component->mca_query_component) {
            opal_output_verbose(5, orte_schizo_base_framework.framework_output,
                                "mca:schizo:select: Skipping component [%s]. It does not implement a query function",
                                component->mca_component_name );
            continue;
        }

        /* Query the component */
        opal_output_verbose(5, orte_schizo_base_framework.framework_output,
                            "mca:schizo:select: Querying component [%s]",
                            component->mca_component_name);
        rc = component->mca_query_component(&module, &priority);

        /* If no module was returned, then skip component */
        if (ORTE_SUCCESS != rc || NULL == module) {
            opal_output_verbose(5, orte_schizo_base_framework.framework_output,
                                "mca:schizo:select: Skipping component [%s]. Query failed to return a module",
                                component->mca_component_name );
            continue;
        }

        /* If we got a module, keep it */
        nmodule = (orte_schizo_base_module_t*) module;
        /* add to the list of active modules */
        newmodule = OBJ_NEW(orte_schizo_base_active_module_t);
        newmodule->pri = priority;
        newmodule->module = nmodule;
        newmodule->component = component;

        /* maintain priority order */
        inserted = false;
        OPAL_LIST_FOREACH(mod, &orte_schizo_base.active_modules, orte_schizo_base_active_module_t) {
            if (priority > mod->pri) {
                opal_list_insert_pos(&orte_schizo_base.active_modules,
                                     (opal_list_item_t*)mod, &newmodule->super);
                inserted = true;
                break;
            }
        }
        if (!inserted) {
            /* must be lowest priority - add to end */
            opal_list_append(&orte_schizo_base.active_modules, &newmodule->super);
        }
    }

    if (4 < opal_output_get_verbosity(orte_schizo_base_framework.framework_output)) {
        opal_output(0, "Final schizo priorities");
        /* show the prioritized list */
        OPAL_LIST_FOREACH(mod, &orte_schizo_base.active_modules, orte_schizo_base_active_module_t) {
            opal_output(0, "\tSchizo: %s Priority: %d", mod->component->mca_component_name, mod->pri);
        }
    }

    return ORTE_SUCCESS;;
}
