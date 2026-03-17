/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2008 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2015      Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "orte_config.h"
#include "orte/constants.h"

#include "opal/mca/base/base.h"
#include "opal/mca/if/if.h"

#include "orte/util/name_fns.h"
#include "orte/runtime/orte_globals.h"

#include "orte/mca/ras/base/ras_private.h"
#include "ras_sim.h"

/*
 * Local functions
 */
static int ras_sim_register(void);
static int ras_sim_component_query(mca_base_module_t **module, int *priority);


orte_ras_sim_component_t mca_ras_simulator_component = {
    {
        /* First, the mca_base_component_t struct containing meta
           information about the component itself */

        .base_version = {
            ORTE_RAS_BASE_VERSION_2_0_0,

            /* Component name and version */
            .mca_component_name = "simulator",
            MCA_BASE_MAKE_VERSION(component, ORTE_MAJOR_VERSION, ORTE_MINOR_VERSION,
                                  ORTE_RELEASE_VERSION),
            .mca_query_component = ras_sim_component_query,
            .mca_register_component_params = ras_sim_register,
        },
        .base_data = {
            /* The component is checkpoint ready */
            MCA_BASE_METADATA_PARAM_CHECKPOINT
        },
    }
};


static int ras_sim_register(void)
{
    mca_base_component_t *component = &mca_ras_simulator_component.super.base_version;

    mca_ras_simulator_component.slots = "1";
    (void) mca_base_component_var_register (component, "slots",
                                            "Comma-separated list of number of slots on each node to simulate",
                                            MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0,
                                            OPAL_INFO_LVL_9,
                                            MCA_BASE_VAR_SCOPE_READONLY,
                                            &mca_ras_simulator_component.slots);

    mca_ras_simulator_component.slots_max = "0";
    (void) mca_base_component_var_register (component, "max_slots",
                                            "Comma-separated list of number of max slots on each node to simulate",
                                            MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0,
                                            OPAL_INFO_LVL_9,
                                            MCA_BASE_VAR_SCOPE_READONLY,
                                            &mca_ras_simulator_component.slots_max);
    mca_ras_simulator_component.num_nodes = NULL;
    (void) mca_base_component_var_register (component, "num_nodes",
                                            "Comma-separated list of number of nodes to simulate for each topology",
                                            MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0,
                                            OPAL_INFO_LVL_9,
                                            MCA_BASE_VAR_SCOPE_READONLY,
                                            &mca_ras_simulator_component.num_nodes);
    mca_ras_simulator_component.topofiles = NULL;
    (void) mca_base_component_var_register (component, "topo_files",
                                            "Comma-separated list of files containing xml topology descriptions for simulated nodes",
                                            MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0,
                                            OPAL_INFO_LVL_9,
                                            MCA_BASE_VAR_SCOPE_READONLY,
                                            &mca_ras_simulator_component.topofiles);
    mca_ras_simulator_component.topologies = NULL;
    (void) mca_base_component_var_register (component, "topologies",
                                            "Comma-separated list of topology descriptions for simulated nodes",
                                            MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0,
                                            OPAL_INFO_LVL_9,
                                            MCA_BASE_VAR_SCOPE_READONLY,
                                            &mca_ras_simulator_component.topologies);
    mca_ras_simulator_component.have_cpubind = true;
    (void) mca_base_component_var_register (component, "have_cpubind",
                                            "Topology supports binding to cpus",
                                            MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                            OPAL_INFO_LVL_9,
                                            MCA_BASE_VAR_SCOPE_READONLY,
                                                &mca_ras_simulator_component.have_cpubind);
    mca_ras_simulator_component.have_membind = true;
    (void) mca_base_component_var_register (component, "have_membind",
                                            "Topology supports binding to memory",
                                            MCA_BASE_VAR_TYPE_BOOL,NULL, 0, 0,
                                            OPAL_INFO_LVL_9,
                                            MCA_BASE_VAR_SCOPE_READONLY,
                                            &mca_ras_simulator_component.have_membind);
    return ORTE_SUCCESS;
}


static int ras_sim_component_query(mca_base_module_t **module, int *priority)
{
    if (NULL != mca_ras_simulator_component.num_nodes) {
        *module = (mca_base_module_t *) &orte_ras_sim_module;
        *priority = 1000;
        /* cannot launch simulated nodes or resolve their names to addresses */
        orte_do_not_launch = true;
        opal_if_do_not_resolve = true;
        return ORTE_SUCCESS;
    }

    /* Sadly, no */
    *module = NULL;
    *priority = 0;
    return ORTE_ERROR;
}
