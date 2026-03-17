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
 * Copyright (c) 2015-2017 Intel, Inc. All rights reserved.
 * Copyright (c) 2017      Cisco Systems, Inc.  All rights reserved
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/**
 * @file
 *
 * Resource Mapping
 */
#ifndef ORTE_RMAPS_RR_H
#define ORTE_RMAPS_RR_H

#include "orte_config.h"

#include "opal/mca/hwloc/hwloc-internal.h"
#include "opal/class/opal_list.h"

#include "orte/mca/rmaps/rmaps.h"

BEGIN_C_DECLS

ORTE_MODULE_DECLSPEC extern orte_rmaps_base_component_t mca_rmaps_round_robin_component;
extern orte_rmaps_base_module_t orte_rmaps_round_robin_module;

ORTE_MODULE_DECLSPEC int orte_rmaps_rr_bynode(orte_job_t *jdata,
                                              orte_app_context_t *app,
                                              opal_list_t *node_list,
                                              orte_std_cntr_t num_slots,
                                              orte_vpid_t nprocs);
ORTE_MODULE_DECLSPEC int orte_rmaps_rr_byslot(orte_job_t *jdata,
                                              orte_app_context_t *app,
                                              opal_list_t *node_list,
                                              orte_std_cntr_t num_slots,
                                              orte_vpid_t nprocs);

ORTE_MODULE_DECLSPEC int orte_rmaps_rr_byobj(orte_job_t *jdata, orte_app_context_t *app,
                                             opal_list_t *node_list,
                                             orte_std_cntr_t num_slots,
                                             orte_vpid_t num_procs,
                                             hwloc_obj_type_t target, unsigned cache_level);

ORTE_MODULE_DECLSPEC int orte_rmaps_rr_assign_root_level(orte_job_t *jdata);

ORTE_MODULE_DECLSPEC int orte_rmaps_rr_assign_byobj(orte_job_t *jdata,
                                                    hwloc_obj_type_t target,
                                                    unsigned cache_level);


END_C_DECLS

#endif
