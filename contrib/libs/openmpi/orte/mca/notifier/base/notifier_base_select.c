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
 * Copyright (c) 2009      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2014-2015 Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */


#include "orte_config.h"

#include <string.h>

#include "orte/mca/mca.h"
#include "opal/mca/base/base.h"
#include "opal/util/argv.h"
#include "opal/util/output.h"
#include "opal/class/opal_pointer_array.h"

#include "orte/mca/notifier/base/base.h"

/* Global variables */
/*
 * orte_notifier_base_selected is set to true if at least 1 module has
 * been selected for the notifier log API interface.
 */
static bool orte_notifier_base_selected = false;

/**
 * Function for weeding out notifier components that don't want to run.
 *
 * Call the init function on all available compoenent to find out if
 * they want to run. Select all components that don't fail. Failing
 * Components will be closed and unloaded. The selected modules will
 * be returned to the called in a opal_list_t.
 */

int orte_notifier_base_select(void)
{
    mca_base_component_list_item_t *cli = NULL;
    orte_notifier_base_component_t *component = NULL;
    mca_base_module_t *module = NULL;
    int priority;
    orte_notifier_active_module_t *tmp_module;
    orte_notifier_base_module_t *bmod;

    if (orte_notifier_base_selected) {
        return ORTE_SUCCESS;
    }
    orte_notifier_base_selected = true;

    opal_output_verbose(10, orte_notifier_base_framework.framework_output,
                        "notifier:base:select: Auto-selecting components");

    /*
     * Traverse the list of available components.
     * For each call their 'query' functions to see if they are available.
     */
    OPAL_LIST_FOREACH(cli, &orte_notifier_base_framework.framework_components, mca_base_component_list_item_t) {
        component = (orte_notifier_base_component_t *) cli->cli_component;

        /*
         * If there is a query function then use it.
         */
        if (NULL == component->base_version.mca_query_component) {
            opal_output_verbose(5, orte_notifier_base_framework.framework_output,
                                "notifier:base:select Skipping component [%s]. It does not implement a query function",
                                component->base_version.mca_component_name );
            continue;
        }

        /*
         * Query this component for the module and priority
         */
        opal_output_verbose(5, orte_notifier_base_framework.framework_output,
                            "notifier:base:select Querying component [%s]",
                            component->base_version.mca_component_name);

        component->base_version.mca_query_component(&module, &priority);

        /*
         * If no module was returned or negative priority, then skip component
         */
        if (NULL == module || priority < 0) {
            opal_output_verbose(5, orte_notifier_base_framework.framework_output,
                                "notifier:base:select Skipping component [%s]. Query failed to return a module",
                                component->base_version.mca_component_name );
            continue;
        }
        bmod = (orte_notifier_base_module_t*)module;

        /* see if it can be init'd */
        if (NULL != bmod->init) {
            opal_output_verbose(5, orte_notifier_base_framework.framework_output,
                                "notifier:base:init module called with priority [%s] %d",
                                component->base_version.mca_component_name, priority);
            if (ORTE_SUCCESS != bmod->init()) {
                continue;
            }
        }
        /*
         * Append them to the list
         */
        opal_output_verbose(5, orte_notifier_base_framework.framework_output,
                            "notifier:base:select adding component [%s]",
                            component->base_version.mca_component_name);
        tmp_module = OBJ_NEW(orte_notifier_active_module_t);
        tmp_module->component = component;
        tmp_module->module    = (orte_notifier_base_module_t*)module;

        opal_list_append(&orte_notifier_base.modules, (void*)tmp_module);
    }

    return ORTE_SUCCESS;
}
