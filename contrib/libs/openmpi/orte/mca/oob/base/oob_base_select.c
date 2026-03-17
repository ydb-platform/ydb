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
 * Copyright (c) 2007      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2012-2013 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2014-2015 Intel, Inc. All rights reserved.
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
#include "orte/mca/oob/oob.h"
#include "orte/mca/oob/base/base.h"


/**
 * Function for selecting all runnable modules from those that are
 * available.
 *
 * Call the init function on all available modules.
 */
int orte_oob_base_select(void)
{
    mca_base_component_list_item_t *cli, *cmp, *c2;
    mca_oob_base_component_t *component, *c3;
    bool added;
    int i, rc;

    /* Query all available components and ask if their transport is available */
    OPAL_LIST_FOREACH(cli, &orte_oob_base_framework.framework_components, mca_base_component_list_item_t) {
        component = (mca_oob_base_component_t *) cli->cli_component;

        opal_output_verbose(5, orte_oob_base_framework.framework_output,
                            "mca:oob:select: checking available component %s",
                            component->oob_base.mca_component_name);

        /* If there's no query function, skip it */
        if (NULL == component->available) {
            opal_output_verbose(5, orte_oob_base_framework.framework_output,
                                "mca:oob:select: Skipping component [%s]. It does not implement a query function",
                                component->oob_base.mca_component_name );
            continue;
        }

        /* Query the component */
        opal_output_verbose(5, orte_oob_base_framework.framework_output,
                            "mca:oob:select: Querying component [%s]",
                            component->oob_base.mca_component_name);

        rc = component->available();

        /* If the component is not available, then skip it as
         * it has no available interfaces
         */
        if (ORTE_SUCCESS != rc && ORTE_ERR_FORCE_SELECT != rc) {
            opal_output_verbose(5, orte_oob_base_framework.framework_output,
                                "mca:oob:select: Skipping component [%s] - no available interfaces",
                                component->oob_base.mca_component_name );
            continue;
        }

        /* if it fails to startup, then skip it */
        if (ORTE_SUCCESS != component->startup()) {
            opal_output_verbose(5, orte_oob_base_framework.framework_output,
                                "mca:oob:select: Skipping component [%s] - failed to startup",
                                component->oob_base.mca_component_name );
            continue;
        }

        if (ORTE_ERR_FORCE_SELECT == rc) {
            /* this component shall be the *only* component allowed
             * for use, so shutdown and remove any prior ones */
            while (NULL != (cmp = (mca_base_component_list_item_t*)opal_list_remove_first(&orte_oob_base.actives))) {
                c3 = (mca_oob_base_component_t *) cmp->cli_component;
                if (NULL != c3->shutdown) {
                    c3->shutdown();
                }
                OBJ_RELEASE(cmp);
            }
            c2 = OBJ_NEW(mca_base_component_list_item_t);
            c2->cli_component = (mca_base_component_t*)component;
            opal_list_append(&orte_oob_base.actives, &c2->super);
            break;
        }

        /* record it, but maintain priority order */
        added = false;
        OPAL_LIST_FOREACH(cmp, &orte_oob_base.actives, mca_base_component_list_item_t) {
            c3 = (mca_oob_base_component_t *) cmp->cli_component;
            if (c3->priority > component->priority) {
                continue;
            }
            opal_output_verbose(5, orte_oob_base_framework.framework_output,
                                "mca:oob:select: Inserting component");
            c2 = OBJ_NEW(mca_base_component_list_item_t);
            c2->cli_component = (mca_base_component_t*)component;
            opal_list_insert_pos(&orte_oob_base.actives,
                                 &cmp->super, &c2->super);
            added = true;
            break;
        }
        if (!added) {
            /* add to end */
            opal_output_verbose(5, orte_oob_base_framework.framework_output,
                                "mca:oob:select: Adding component to end");
            c2 = OBJ_NEW(mca_base_component_list_item_t);
            c2->cli_component = (mca_base_component_t*)component;
            opal_list_append(&orte_oob_base.actives, &c2->super);
        }
    }

    if (0 == opal_list_get_size(&orte_oob_base.actives) &&
        !orte_standalone_operation) {
        /* no support available means we really cannot run unless
         * we are a singleton */
        opal_output_verbose(5, orte_oob_base_framework.framework_output,
                            "mca:oob:select: Init failed to return any available transports");
        orte_show_help("help-oob-base.txt", "no-interfaces-avail", true);
        return ORTE_ERR_SILENT;
    }

    /* provide them an index so we can track their usability in a bitmap */
    i=0;
    OPAL_LIST_FOREACH(cmp, &orte_oob_base.actives, mca_base_component_list_item_t) {
        c3 = (mca_oob_base_component_t *) cmp->cli_component;
        c3->idx = i++;
    }

    opal_output_verbose(5, orte_oob_base_framework.framework_output,
                        "mca:oob:select: Found %d active transports",
                        (int)opal_list_get_size(&orte_oob_base.actives));
    return ORTE_SUCCESS;
}
