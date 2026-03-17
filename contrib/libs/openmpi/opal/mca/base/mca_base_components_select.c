/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2008 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2015      Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif

#include "opal/runtime/opal.h"
#include "opal/class/opal_list.h"
#include "opal/util/output.h"
#include "opal/mca/mca.h"
#include "opal/mca/base/base.h"
#include "opal/mca/base/mca_base_component_repository.h"
#include "opal/constants.h"


int mca_base_select(const char *type_name, int output_id,
                    opal_list_t *components_available,
                    mca_base_module_t **best_module,
                    mca_base_component_t **best_component,
                    int *priority_out)
{
    mca_base_component_list_item_t *cli = NULL;
    mca_base_component_t *component = NULL;
    mca_base_module_t *module = NULL;
    int priority = 0, best_priority = INT32_MIN;
    int rc;

    *best_module = NULL;
    *best_component = NULL;

    opal_output_verbose (MCA_BASE_VERBOSE_COMPONENT, output_id,
                         "mca:base:select: Auto-selecting %s components",
                         type_name);

    /*
     * Traverse the list of available components.
     * For each call their 'query' functions to determine relative priority.
     */
    OPAL_LIST_FOREACH(cli, components_available, mca_base_component_list_item_t) {
        component = (mca_base_component_t *) cli->cli_component;

        /*
         * If there is a query function then use it.
         */
        if (NULL == component->mca_query_component) {
            opal_output_verbose (MCA_BASE_VERBOSE_COMPONENT, output_id,
                                 "mca:base:select:(%5s) Skipping component [%s]. It does not implement a query function",
                                 type_name, component->mca_component_name );
            continue;
        }

        /*
         * Query this component for the module and priority
         */
        opal_output_verbose (MCA_BASE_VERBOSE_COMPONENT, output_id,
                             "mca:base:select:(%5s) Querying component [%s]",
                             type_name, component->mca_component_name);

        rc = component->mca_query_component(&module, &priority);
        if (OPAL_ERR_FATAL == rc) {
            /* a fatal error was detected by this component - e.g., the
             * user specified a required element and the component could
             * not find it. In this case, we must not continue as we might
             * find some other component that could run, causing us to do
             * something the user didn't want */
             return rc;
        } else if (OPAL_SUCCESS != rc) {
            /* silently skip this component */
            continue;
        }

        /*
         * If no module was returned, then skip component
         */
        if (NULL == module) {
            opal_output_verbose (MCA_BASE_VERBOSE_COMPONENT, output_id,
                                 "mca:base:select:(%5s) Skipping component [%s]. Query failed to return a module",
                                 type_name, component->mca_component_name );
            continue;
        }

        /*
         * Determine if this is the best module we have seen by looking the priority
         */
        opal_output_verbose (MCA_BASE_VERBOSE_COMPONENT, output_id,
                             "mca:base:select:(%5s) Query of component [%s] set priority to %d",
                             type_name, component->mca_component_name, priority);
        if (priority > best_priority) {
            best_priority  = priority;
            *best_component = component;
            *best_module    = module;
        }
    }

    if (priority_out) {
        *priority_out = best_priority;
    }

    /*
     * Finished querying all components.
     * Make sure we found something in the process.
     */
    if (NULL == *best_component) {
        opal_output_verbose (MCA_BASE_VERBOSE_COMPONENT, output_id,
                            "mca:base:select:(%5s) No component selected!",
                            type_name);
        /*
         * Still close the non-selected components
         */
        mca_base_components_close(0, /* Pass 0 to keep this from closing the output handle */
                                  components_available,
                                  NULL);
        return OPAL_ERR_NOT_FOUND;
    }

    opal_output_verbose (MCA_BASE_VERBOSE_COMPONENT, output_id,
                         "mca:base:select:(%5s) Selected component [%s]",
                         type_name, (*best_component)->mca_component_name);

    /*
     * Close the non-selected components
     */
    mca_base_components_close(output_id,
                              components_available,
                              (mca_base_component_t *) (*best_component));


    return OPAL_SUCCESS;
}
