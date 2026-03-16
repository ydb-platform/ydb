/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2007-2015 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2004-2008 The Trustees of Indiana University.
 *                         All rights reserved.
 * Copyright (c) 2004-2011 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2014-2016 Intel, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/**
 * @file
 *
 * Routing table for the RML
 *
 * A flexible routing infrastructure for the RML.  Provides "next hop"
 * service.  Only deals with orte_process_name_ts.
 */


#ifndef ORTE_MCA_ROUTED_ROUTED_H_
#define ORTE_MCA_ROUTED_ROUTED_H_

#include "orte_config.h"

#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif

#include "orte/types.h"
#include "orte/mca/mca.h"

#include "opal/dss/dss_types.h"

#include "opal/mca/crs/crs.h"
#include "opal/mca/crs/base/base.h"

#include "orte/mca/routed/routed_types.h"

BEGIN_C_DECLS


/* ******************************************************************** */


struct opal_buffer_t;
struct orte_rml_module_t;


/* ******************************************************************** */
/**
 * Initialize the routed module
 *
 * Do whatever needs to be done to initialize the selected module
 *
 * @retval ORTE_SUCCESS Success
 * @retval ORTE_ERROR  Error code from whatever was encountered
 */
typedef int (*orte_routed_module_init_fn_t)(void);

/**
 * Finalize the routed module
 *
 * Finalize the routed module, ending cleaning up all resources
 * associated with the module.  After the finalize function is called,
 * all interface functions (and the module structure itself) are not
 * available for use.
 *
 * @note Whether or not the finalize function returns successfully,
 * the module should not be used once this function is called.
 *
 * @retval ORTE_SUCCESS Success
 * @retval ORTE_ERROR   An unspecified error occurred
 */
typedef int (*orte_routed_module_finalize_fn_t)(void);


/*
 * Delete route
 *
 * Delete the route to the specified proc from the routing table. Note
 * that wildcards are supported to remove routes from, for example, all
 * procs in a given job
 */
typedef int (*orte_routed_module_delete_route_fn_t)(orte_process_name_t *proc);

/**
 * Update route table with new information
 *
 * Update routing table with a new entry.  If an existing exact match
 * for the entry exists, it will be replaced with the current
 * information.  If the entry is new, it will be inserted behind all
 * entries of similar "mask".  So a wildcard cellid entry will be
 * inserted after any fully-specified entries and any other wildcard
 * cellid entries, but before any wildcard cellid and jobid entries.
 *
 * @retval ORTE_SUCCESS Success
 * @retval ORTE_ERR_NOT_SUPPORTED The updated is not supported.  This
 *                      is likely due to using partially-specified
 *                      names with a component that does not support
 *                      such functionality
 * @retval ORTE_ERROR   An unspecified error occurred
 */
typedef int (*orte_routed_module_update_route_fn_t)(orte_process_name_t *target,
                                                    orte_process_name_t *route);

/**
 * Get the next hop towards the target
 *
 * Obtain the next process on the route to the target. ORTE's routing system
 * works one hop at-a-time, so this function doesn't return the entire path
 * to the target - it only returns the next hop. This could be the target itself,
 * or it could be an intermediate relay. By design, we -never- use application
 * procs as relays, so any relay will be an orted.
 */
typedef orte_process_name_t (*orte_routed_module_get_route_fn_t)(orte_process_name_t *target);

/**
 * Report a route as "lost"
 *
 * Report that an existing connection has been lost, therefore potentially
 * "breaking" a route in the routing table. It is critical that broken
 * connections be reported so that the selected routing module has the
 * option of dealing with it. This could consist of nothing more than
 * removing that route from the routing table, or could - in the case
 * of a "lifeline" connection - result in abort of the process.
 */
typedef int (*orte_routed_module_route_lost_fn_t)(const orte_process_name_t *route);

/*
 * Is this route defined?
 *
 * Check to see if a route to the specified target has been defined. The
 * function returns "true" if it has, and "false" if no route to the
 * target was previously defined.
 *
 * This is needed because routed modules will return their "wildcard"
 * route if we request a route to a target that they don't know about.
 * In some cases, though, we truly -do- need to know if a route was
 * specifically defined.
 */
typedef bool (*orte_routed_module_route_is_defined_fn_t)(const orte_process_name_t *target);

/*
 * Update the module's routing plan
 *
 * Called only by a daemon and the HNP, this function creates a plan
 * for routing messages within ORTE, especially for routing collectives
 * used during wireup
 */
typedef void (*orte_routed_module_update_routing_plan_fn_t)(void);

/*
 * Get the routing list for an xcast collective
 *
 * Fills the target list with orte_namelist_t so that
 * the grpcomm framework will know who to send xcast to
 * next
 */
typedef void (*orte_routed_module_get_routing_list_fn_t)(opal_list_t *coll);

/*
 * Set lifeline process
 *
 * Defines the lifeline to be the specified process. Should contact to
 * that process be lost, the errmgr will be called, possibly resulting
 * in termination of the process and job.
 */
typedef int (*orte_routed_module_set_lifeline_fn_t)(orte_process_name_t *proc);

/*
 * Get the number of routes supported by this process
 *
 * Returns the size of the routing tree using an O(1) function
 */
typedef size_t (*orte_routed_module_num_routes_fn_t)(void);

/**
 * Handle fault tolerance updates
 *
 * @param[in] state Fault tolerance state update
 *
 * @retval ORTE_SUCCESS The operation completed successfully
 * @retval ORTE_ERROR   An unspecifed error occurred
 */
typedef int  (*orte_routed_module_ft_event_fn_t)(int state);

/* ******************************************************************** */


/**
 * routed module interface
 *
 * Module interface to the routed communication system.  A global
 * instance of this module, orte_routed, provices an interface into the
 * active routed interface.
 */
typedef struct {
    /** Startup/shutdown the communication system and clean up resources */
    orte_routed_module_init_fn_t                    initialize;
    orte_routed_module_finalize_fn_t                finalize;
    /* API functions */
    orte_routed_module_delete_route_fn_t            delete_route;
    orte_routed_module_update_route_fn_t            update_route;
    orte_routed_module_get_route_fn_t               get_route;
    orte_routed_module_route_lost_fn_t              route_lost;
    orte_routed_module_route_is_defined_fn_t        route_is_defined;
    orte_routed_module_set_lifeline_fn_t            set_lifeline;
    /* fns for daemons */
    orte_routed_module_update_routing_plan_fn_t     update_routing_plan;
    orte_routed_module_get_routing_list_fn_t        get_routing_list;
    orte_routed_module_num_routes_fn_t              num_routes;
    /* FT Notification */
    orte_routed_module_ft_event_fn_t                ft_event;
} orte_routed_module_t;

/* define an equivalent set of API functions - these will be implemented
 * as "stubs" in the framework base */
typedef char* (*orte_routed_API_assign_module_fn_t)(char *modules);

typedef int (*orte_routed_API_delete_route_fn_t)(char *module,
                                                 orte_process_name_t *proc);
typedef int (*orte_routed_API_update_route_fn_t)(char *module,
                                                 orte_process_name_t *target,
                                                 orte_process_name_t *route);
typedef orte_process_name_t (*orte_routed_API_get_route_fn_t)(char *module,
                                                              orte_process_name_t *target);
typedef int (*orte_routed_API_route_lost_fn_t)(char *module,
                                               const orte_process_name_t *route);
typedef bool (*orte_routed_API_route_is_defined_fn_t)(char *module,
                                                      const orte_process_name_t *target);
typedef void (*orte_routed_API_update_routing_plan_fn_t)(char *module);
typedef void (*orte_routed_API_get_routing_list_fn_t)(char *module, opal_list_t *coll);
typedef int (*orte_routed_API_set_lifeline_fn_t)(char *module, orte_process_name_t *proc);
typedef size_t (*orte_routed_API_num_routes_fn_t)(char *module);
typedef int  (*orte_routed_API_ft_event_fn_t)(char *module, int state);


typedef struct {
    /* API functions */
    orte_routed_API_assign_module_fn_t          assign_module;
    orte_routed_API_delete_route_fn_t           delete_route;
    orte_routed_API_update_route_fn_t           update_route;
    orte_routed_API_get_route_fn_t              get_route;
    orte_routed_API_route_lost_fn_t             route_lost;
    orte_routed_API_route_is_defined_fn_t       route_is_defined;
    orte_routed_API_set_lifeline_fn_t           set_lifeline;
    /* fns for daemons */
    orte_routed_API_update_routing_plan_fn_t    update_routing_plan;
    orte_routed_API_get_routing_list_fn_t       get_routing_list;
    orte_routed_API_num_routes_fn_t             num_routes;
    /* FT Notification */
    orte_routed_API_ft_event_fn_t               ft_event;
} orte_routed_API_t;

/* provide an interface to the routed framework stub functions */
ORTE_DECLSPEC extern orte_routed_API_t orte_routed;

/* ******************************************************************** */

/**
 * routed component interface
 *
 * Component interface for the routed framework.  A public instance of
 * this structure, called mca_routed_[component name]_component, must
 * exist in any routed component.
 */

struct orte_routed_component_3_0_0_t {
    /* Base component description */
    mca_base_component_t base_version;
    /* Base component data block */
    mca_base_component_data_t base_data;
    /* priority */
    int priority;
};
/** Convienence typedef */
typedef struct orte_routed_component_3_0_0_t orte_routed_component_t;


/* ******************************************************************** */


/** Macro for use in components that are of type routed  */
#define ORTE_ROUTED_BASE_VERSION_3_0_0 \
    ORTE_MCA_BASE_VERSION_2_1_0("routed", 3, 0, 0)


/* ******************************************************************** */


END_C_DECLS

#endif
