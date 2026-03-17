/*
 * Copyright (c) 2011      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2015      Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef ORTE_RAS_SIM_H
#define ORTE_RAS_SIM_H

#include "orte_config.h"
#include "orte/mca/ras/ras.h"
#include "orte/mca/ras/base/base.h"

BEGIN_C_DECLS

struct orte_ras_sim_component_t {
    orte_ras_base_component_t super;
    char *num_nodes;
    char * slots;
    char * slots_max;
    char *topofiles;
    char *topologies;
    bool have_cpubind;
    bool have_membind;
};
typedef struct orte_ras_sim_component_t orte_ras_sim_component_t;

ORTE_DECLSPEC extern orte_ras_sim_component_t mca_ras_simulator_component;
ORTE_DECLSPEC extern orte_ras_base_module_t orte_ras_sim_module;

END_C_DECLS

#endif
