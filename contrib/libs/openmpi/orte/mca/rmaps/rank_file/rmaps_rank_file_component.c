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
 * Copyright (c) 2008      Voltaire. All rights reserved
 * Copyright (c) 2011      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2014-2018 Intel, Inc. All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "orte_config.h"
#include "orte/constants.h"

#include <string.h>

#include "opal/mca/base/base.h"
#include "opal/mca/hwloc/base/base.h"

#include "orte/util/show_help.h"

#include "orte/mca/rmaps/base/base.h"
#include "orte/mca/rmaps/base/rmaps_private.h"
#include "orte/mca/rmaps/rank_file/rmaps_rank_file.h"
#include "orte/mca/rmaps/rank_file/rmaps_rank_file_lex.h"

/*
 * Local functions
 */

static int orte_rmaps_rank_file_register(void);
static int orte_rmaps_rank_file_open(void);
static int orte_rmaps_rank_file_close(void);
static int orte_rmaps_rank_file_query(mca_base_module_t **module, int *priority);

static int my_priority;

orte_rmaps_rf_component_t mca_rmaps_rank_file_component = {
    {
        /* First, the mca_base_component_t struct containing meta
           information about the component itself */

        .base_version = {
            ORTE_RMAPS_BASE_VERSION_2_0_0,

            .mca_component_name = "rank_file",
            MCA_BASE_MAKE_VERSION(component, ORTE_MAJOR_VERSION, ORTE_MINOR_VERSION,
                                  ORTE_RELEASE_VERSION),
            .mca_open_component = orte_rmaps_rank_file_open,
            .mca_close_component = orte_rmaps_rank_file_close,
            .mca_query_component = orte_rmaps_rank_file_query,
            .mca_register_component_params = orte_rmaps_rank_file_register,
        },
        .base_data = {
            /* The component is checkpoint ready */
            MCA_BASE_METADATA_PARAM_CHECKPOINT
        },
    }
};

/**
  * component register/open/close/init function
  */
static int orte_rmaps_rank_file_register(void)
{
    mca_base_component_t *c = &mca_rmaps_rank_file_component.super.base_version;
    int tmp;

    my_priority = 0;
    (void) mca_base_component_var_register(c, "priority", "Priority of the rank_file rmaps component",
                                           MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                           OPAL_INFO_LVL_9,
                                           MCA_BASE_VAR_SCOPE_READONLY, &my_priority);
    orte_rankfile = NULL;
    tmp = mca_base_component_var_register(c, "path",
                                          "Name of the rankfile to be used for mapping processes (relative or absolute path)",
                                          MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0,
                                          OPAL_INFO_LVL_5,
                                          MCA_BASE_VAR_SCOPE_READONLY, &orte_rankfile);
    (void) mca_base_var_register_synonym(tmp, "orte", "orte", NULL, "rankfile", 0);

    mca_rmaps_rank_file_component.physical = false;
    (void) mca_base_component_var_register(c, "physical", "Rankfile contains physical cpu designations",
                                           MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                           OPAL_INFO_LVL_5,
                                           MCA_BASE_VAR_SCOPE_READONLY,
                                           &mca_rmaps_rank_file_component.physical);


    return ORTE_SUCCESS;
}

static int orte_rmaps_rank_file_open(void)
{
    /* ensure we flag mapping by user */
    if ((NULL != opal_hwloc_base_cpu_list && !OPAL_BIND_ORDERED_REQUESTED(opal_hwloc_binding_policy)) ||
        NULL != orte_rankfile) {
        if (ORTE_MAPPING_GIVEN & ORTE_GET_MAPPING_DIRECTIVE(orte_rmaps_base.mapping)) {
            /* if a non-default mapping is already specified, then we
             * have an error
             */
            orte_show_help("help-orte-rmaps-base.txt", "redefining-policy", true, "mapping",
                           "RANK_FILE", orte_rmaps_base_print_mapping(orte_rmaps_base.mapping));
            ORTE_SET_MAPPING_DIRECTIVE(orte_rmaps_base.mapping, ORTE_MAPPING_CONFLICTED);
            return ORTE_ERR_SILENT;
        }
        ORTE_SET_MAPPING_POLICY(orte_rmaps_base.mapping, ORTE_MAPPING_BYUSER);
        ORTE_SET_MAPPING_DIRECTIVE(orte_rmaps_base.mapping, ORTE_MAPPING_GIVEN);
        /* we are going to bind to cpuset since the user is specifying the cpus */
        OPAL_SET_BINDING_POLICY(opal_hwloc_binding_policy, OPAL_BIND_TO_CPUSET);
        /* make us first */
        my_priority = 10000;
    }

    return ORTE_SUCCESS;
}

static int orte_rmaps_rank_file_query(mca_base_module_t **module, int *priority)
{
    *priority = my_priority;
    *module = (mca_base_module_t *)&orte_rmaps_rank_file_module;
    return ORTE_SUCCESS;
}

/**
 *  Close all subsystems.
 */

static int orte_rmaps_rank_file_close(void)
{
    int tmp = mca_base_var_find("orte", "orte", NULL, "rankfile");

    if (0 <= tmp) {
        mca_base_var_deregister(tmp);
    }

    return ORTE_SUCCESS;
}

static void rf_map_construct(orte_rmaps_rank_file_map_t *ptr)
{
    ptr->node_name = NULL;
    memset(ptr->slot_list, (char)0x00, 64);
}
static void rf_map_destruct(orte_rmaps_rank_file_map_t *ptr)
{
    if (NULL != ptr->node_name) free(ptr->node_name);
}
OBJ_CLASS_INSTANCE(orte_rmaps_rank_file_map_t,
                   opal_object_t,
                   rf_map_construct,
                   rf_map_destruct);
