/*
 * Copyright (c) 2007-2013 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2014-2017 Intel, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef MCA_ROUTED_BASE_H
#define MCA_ROUTED_BASE_H

#include "orte_config.h"

#include "orte/mca/mca.h"

#include "opal/class/opal_pointer_array.h"
#include "opal/dss/dss_types.h"

#include "orte/mca/rml/rml_types.h"
#include "orte/mca/routed/routed.h"

BEGIN_C_DECLS

/*
 * MCA Framework
 */
ORTE_DECLSPEC extern mca_base_framework_t orte_routed_base_framework;
/* select a component */
ORTE_DECLSPEC    int orte_routed_base_select(void);

typedef struct {
    opal_list_item_t super;
    int pri;
    orte_routed_component_t *component;
    orte_routed_module_t *module;
} orte_routed_base_active_t;
OBJ_CLASS_DECLARATION(orte_routed_base_active_t);

typedef struct {
    opal_list_t actives;
    bool routing_enabled;
} orte_routed_base_t;
ORTE_DECLSPEC extern orte_routed_base_t orte_routed_base;


/* base API wrapper functions */
ORTE_DECLSPEC char* orte_routed_base_assign_module(char *modules);

ORTE_DECLSPEC int orte_routed_base_delete_route(char *module, orte_process_name_t *proc);
ORTE_DECLSPEC int orte_routed_base_update_route(char *module, orte_process_name_t *target,
                                                orte_process_name_t *route);
ORTE_DECLSPEC orte_process_name_t orte_routed_base_get_route(char *module,
                                                             orte_process_name_t *target);
ORTE_DECLSPEC int orte_routed_base_route_lost(char *module,
                                              const orte_process_name_t *route);
ORTE_DECLSPEC bool orte_routed_base_route_is_defined(char *module,
                                                     const orte_process_name_t *target);
ORTE_DECLSPEC void orte_routed_base_update_routing_plan(char *module);
ORTE_DECLSPEC void orte_routed_base_get_routing_list(char *module, opal_list_t *coll);
ORTE_DECLSPEC int orte_routed_base_set_lifeline(char *module, orte_process_name_t *proc);
ORTE_DECLSPEC size_t orte_routed_base_num_routes(char *module);
ORTE_DECLSPEC int orte_routed_base_ft_event(char *module, int state);

/* specialized support functions */
ORTE_DECLSPEC void orte_routed_base_xcast_routing(opal_list_t *coll,
                                                  opal_list_t *my_children);

ORTE_DECLSPEC int orte_routed_base_process_callback(orte_jobid_t job,
                                                    opal_buffer_t *buffer);
ORTE_DECLSPEC void orte_routed_base_update_hnps(opal_buffer_t *buf);

END_C_DECLS

#endif /* MCA_ROUTED_BASE_H */
