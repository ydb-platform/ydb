/*
 * Copyright (c) 2009      Cisco Systems, Inc.  All rights reserved.
 *
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
#ifndef ORTE_RMAPS_RESILIENT_H
#define ORTE_RMAPS_RESILIENT_H

#include "orte_config.h"

#include "opal/class/opal_pointer_array.h"

#include "orte/mca/rmaps/rmaps.h"

BEGIN_C_DECLS

struct orte_rmaps_res_component_t {
    orte_rmaps_base_component_t super;
    char *fault_group_file;
    opal_list_t fault_grps;
};
typedef struct orte_rmaps_res_component_t orte_rmaps_res_component_t;

typedef struct {
    opal_list_item_t super;
    int ftgrp;
    bool used;
    bool included;
    opal_pointer_array_t nodes;
} orte_rmaps_res_ftgrp_t;
ORTE_DECLSPEC OBJ_CLASS_DECLARATION(orte_rmaps_res_ftgrp_t);

ORTE_MODULE_DECLSPEC extern orte_rmaps_res_component_t mca_rmaps_resilient_component;
extern orte_rmaps_base_module_t orte_rmaps_resilient_module;


END_C_DECLS

#endif
