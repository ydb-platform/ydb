/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2011      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2011-2013 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2014-2018 Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/** @file:
 * rmaps framework base functionality.
 */

#ifndef ORTE_MCA_RMAPS_BASE_H
#define ORTE_MCA_RMAPS_BASE_H

/*
 * includes
 */
#include "orte_config.h"
#include "orte/types.h"

#include "opal/class/opal_list.h"
#include "orte/mca/mca.h"

#include "orte/runtime/orte_globals.h"

#include "orte/mca/rmaps/rmaps.h"

BEGIN_C_DECLS

/*
 * MCA Framework
 */
ORTE_DECLSPEC extern mca_base_framework_t orte_rmaps_base_framework;
/* select a component */
ORTE_DECLSPEC    int orte_rmaps_base_select(void);

/*
 * Global functions for MCA overall collective open and close
 */

/**
 * Struct to hold data global to the rmaps framework
 */
typedef struct {
    /* list of selected modules */
    opal_list_t selected_modules;
    /* default ppr */
    char *ppr;
    /* cpus per rank */
    int cpus_per_rank;
    /* display the map after it is computed */
    bool display_map;
    /* slot list, if provided by user */
    char *slot_list;
    /* default mapping directives */
    orte_mapping_policy_t mapping;
    orte_ranking_policy_t ranking;
    /* device specification for min distance mapping */
    char *device;
    /* whether or not child jobs should inherit launch directives */
    bool inherit;
} orte_rmaps_base_t;

/**
 * Global instance of rmaps-wide framework data
 */
ORTE_DECLSPEC extern orte_rmaps_base_t orte_rmaps_base;

/**
 * Global MCA variables
 */
ORTE_DECLSPEC extern bool orte_rmaps_base_pernode;
ORTE_DECLSPEC extern int orte_rmaps_base_n_pernode;
ORTE_DECLSPEC extern int orte_rmaps_base_n_persocket;

/**
 * Select an rmaps component / module
 */
typedef struct {
    opal_list_item_t super;
    int pri;
    orte_rmaps_base_module_t *module;
    mca_base_component_t *component;
} orte_rmaps_base_selected_module_t;
OBJ_CLASS_DECLARATION(orte_rmaps_base_selected_module_t);

/*
 * Map a job
 */
ORTE_DECLSPEC void orte_rmaps_base_map_job(int sd, short args, void *cbdata);
ORTE_DECLSPEC int orte_rmaps_base_assign_locations(orte_job_t *jdata);

/**
 * Utility routines to get/set vpid mapping for the job
 */

ORTE_DECLSPEC int orte_rmaps_base_get_vpid_range(orte_jobid_t jobid,
    orte_vpid_t *start, orte_vpid_t *range);
ORTE_DECLSPEC int orte_rmaps_base_set_vpid_range(orte_jobid_t jobid,
    orte_vpid_t start, orte_vpid_t range);

/* pretty-print functions */
ORTE_DECLSPEC char* orte_rmaps_base_print_mapping(orte_mapping_policy_t mapping);
ORTE_DECLSPEC char* orte_rmaps_base_print_ranking(orte_ranking_policy_t ranking);

ORTE_DECLSPEC int orte_rmaps_base_prep_topology(hwloc_topology_t topo);

ORTE_DECLSPEC int orte_rmaps_base_filter_nodes(orte_app_context_t *app,
                                               opal_list_t *nodes,
                                               bool remove);

ORTE_DECLSPEC int orte_rmaps_base_set_mapping_policy(orte_job_t *jdata,
                                                     orte_mapping_policy_t *policy,
                                                     char **device, char *spec);
ORTE_DECLSPEC int orte_rmaps_base_set_ranking_policy(orte_ranking_policy_t *policy,
                                                     orte_mapping_policy_t mapping,
                                                     char *spec);

ORTE_DECLSPEC void orte_rmaps_base_display_map(orte_job_t *jdata);

END_C_DECLS

#endif
