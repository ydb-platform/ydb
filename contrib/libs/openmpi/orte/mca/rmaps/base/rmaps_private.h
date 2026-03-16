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
 * Copyright (c) 2011-2012 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2017      Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/** @file:
 */

#ifndef ORTE_MCA_RMAPS_PRIVATE_H
#define ORTE_MCA_RMAPS_PRIVATE_H

/*
 * includes
 */
#include "orte_config.h"
#include "orte/types.h"

#include "orte/runtime/orte_globals.h"

#include "orte/mca/rmaps/rmaps.h"

BEGIN_C_DECLS

/*
 * Base API functions
 */

/* LOCAL FUNCTIONS for use by RMAPS components */

ORTE_DECLSPEC int orte_rmaps_base_get_target_nodes(opal_list_t* node_list,
                                                   orte_std_cntr_t *total_num_slots,
                                                   orte_app_context_t *app,
                                                   orte_mapping_policy_t policy,
                                                   bool initial_map, bool silent);

ORTE_DECLSPEC orte_proc_t* orte_rmaps_base_setup_proc(orte_job_t *jdata,
                                                      orte_node_t *node,
                                                      orte_app_idx_t idx);

ORTE_DECLSPEC orte_node_t* orte_rmaps_base_get_starting_point(opal_list_t *node_list,
                                                              orte_job_t *jdata);

ORTE_DECLSPEC int orte_rmaps_base_compute_vpids(orte_job_t *jdata);

ORTE_DECLSPEC int orte_rmaps_base_compute_local_ranks(orte_job_t *jdata);

ORTE_DECLSPEC int orte_rmaps_base_compute_bindings(orte_job_t *jdata);

ORTE_DECLSPEC void orte_rmaps_base_update_local_ranks(orte_job_t *jdata, orte_node_t *oldnode,
                                                      orte_node_t *newnode, orte_proc_t *newproc);

ORTE_DECLSPEC int orte_rmaps_base_rearrange_map(orte_app_context_t *app, orte_job_map_t *map, opal_list_t *procs);

END_C_DECLS

#endif
