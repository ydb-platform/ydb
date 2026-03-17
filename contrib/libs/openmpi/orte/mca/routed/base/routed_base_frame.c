/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2007-2015 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2007      Sun Microsystems, Inc.  All rights reserved.
 * Copyright (c) 2004-2010 The Trustees of Indiana University.
 *                         All rights reserved.
 * Copyright (c) 2004-2011 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2016-2017 Intel, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "orte_config.h"
#include "orte/constants.h"

#include "orte/mca/mca.h"
#include "opal/class/opal_bitmap.h"
#include "opal/util/output.h"
#include "opal/mca/base/mca_base_component_repository.h"

#include "orte/mca/errmgr/errmgr.h"
#include "orte/util/proc_info.h"
#include "orte/runtime/orte_globals.h"

#include "orte/mca/routed/routed.h"
#include "orte/mca/routed/base/base.h"


/* The following file was created by configure.  It contains extern
 * statements and the definition of an array of pointers to each
 * component's public mca_base_component_t struct. */
#include "orte/mca/routed/base/static-components.h"

orte_routed_base_t orte_routed_base = {{{0}}};
orte_routed_API_t orte_routed = {
    .assign_module = orte_routed_base_assign_module,
    .delete_route = orte_routed_base_delete_route,
    .update_route = orte_routed_base_update_route,
    .get_route = orte_routed_base_get_route,
    .route_lost = orte_routed_base_route_lost,
    .route_is_defined = orte_routed_base_route_is_defined,
    .set_lifeline = orte_routed_base_set_lifeline,
    .update_routing_plan = orte_routed_base_update_routing_plan,
    .get_routing_list = orte_routed_base_get_routing_list,
    .num_routes = orte_routed_base_num_routes,
    .ft_event = orte_routed_base_ft_event
};

static int orte_routed_base_open(mca_base_open_flag_t flags)
{
    /* setup our list of actives */
    OBJ_CONSTRUCT(&orte_routed_base.actives, opal_list_t);
    /* start with routing DISABLED */
    orte_routed_base.routing_enabled = false;

    /* Open up all available components */
    return mca_base_framework_components_open(&orte_routed_base_framework, flags);
}

static int orte_routed_base_close(void)
{
    orte_routed_base_active_t *active;

    while (NULL != (active = (orte_routed_base_active_t *)opal_list_remove_first(&orte_routed_base.actives))) {
        active->module->finalize();
        OBJ_RELEASE(active);
    }
    OPAL_LIST_DESTRUCT(&orte_routed_base.actives);

    return mca_base_framework_components_close(&orte_routed_base_framework, NULL);
}

MCA_BASE_FRAMEWORK_DECLARE(orte, routed, "ORTE Message Routing Subsystem", NULL,
                           orte_routed_base_open, orte_routed_base_close,
                           mca_routed_base_static_components, 0);

static bool selected = false;

int orte_routed_base_select(void)
{
    mca_base_component_list_item_t *cli=NULL;
    orte_routed_component_t *component=NULL;
    orte_routed_base_active_t *newmodule, *mod;
    mca_base_module_t *module;
    bool inserted;
    int pri;

    if (selected) {
       return ORTE_SUCCESS;
    }
    selected = true;

    OPAL_LIST_FOREACH(cli, &orte_routed_base_framework.framework_components, mca_base_component_list_item_t ) {
        component = (orte_routed_component_t*) cli->cli_component;

        opal_output_verbose(10, orte_routed_base_framework.framework_output,
                            "orte_routed_base_select: Initializing %s component %s",
                             component->base_version.mca_type_name,
                             component->base_version.mca_component_name);

        if (ORTE_SUCCESS != component->base_version.mca_query_component(&module, &pri)) {
            continue;
        }

        /* add to the list of available components */
        newmodule = OBJ_NEW(orte_routed_base_active_t);
        newmodule->pri = pri;
        newmodule->component = component;
        newmodule->module = (orte_routed_module_t*)module;

        if (ORTE_SUCCESS != newmodule->module->initialize()) {
            OBJ_RELEASE(newmodule);
            continue;
        }

        /* maintain priority order */
        inserted = false;
        OPAL_LIST_FOREACH(mod, &orte_routed_base.actives, orte_routed_base_active_t) {
            if (newmodule->pri > mod->pri) {
                opal_list_insert_pos(&orte_routed_base.actives,
                             (opal_list_item_t*)mod, &newmodule->super);
                inserted = true;
                break;
            }
        }
        if (!inserted) {
            /* must be lowest priority - add to end */
            opal_list_append(&orte_routed_base.actives, &newmodule->super);
        }
    }

    if (4 < opal_output_get_verbosity(orte_routed_base_framework.framework_output)) {
        opal_output(0, "%s: Final routed priorities", ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
        /* show the prioritized list */
        OPAL_LIST_FOREACH(mod, &orte_routed_base.actives, orte_routed_base_active_t) {
            opal_output(0, "\tComponent: %s Priority: %d", mod->component->base_version.mca_component_name, mod->pri);
        }
    }

    return ORTE_SUCCESS;
}

static void construct(orte_routed_tree_t *rt)
{
    rt->vpid = ORTE_VPID_INVALID;
    OBJ_CONSTRUCT(&rt->relatives, opal_bitmap_t);
}
static void destruct(orte_routed_tree_t *rt)
{
    OBJ_DESTRUCT(&rt->relatives);
}
OBJ_CLASS_INSTANCE(orte_routed_tree_t,
                   opal_list_item_t,
                   construct, destruct);

OBJ_CLASS_INSTANCE(orte_routed_base_active_t,
                   opal_list_item_t,
                   NULL, NULL);
