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
 * Copyright (c) 2009      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2014      Intel, Inc.  All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
*/

/*
 * includes
 */
#include "orte_config.h"
#include "orte/constants.h"

#include "notifier_syslog.h"


static int orte_notifier_syslog_component_query(mca_base_module_t **module,
                                                int *priority);

/*
 * Struct of function pointers that need to be initialized
 */
orte_notifier_base_component_t mca_notifier_syslog_component = {
    .base_version = {
        ORTE_NOTIFIER_BASE_VERSION_1_0_0,

        .mca_component_name = "syslog",
        MCA_BASE_MAKE_VERSION(component, ORTE_MAJOR_VERSION, ORTE_MINOR_VERSION,
                              ORTE_RELEASE_VERSION),
        .mca_query_component = orte_notifier_syslog_component_query,
    },
    .base_data = {
        /* The component is checkpoint ready */
        MCA_BASE_METADATA_PARAM_CHECKPOINT
    },
};

static int orte_notifier_syslog_component_query(mca_base_module_t **module,
                                                int *priority)
{
    *priority = 1;
    *module = (mca_base_module_t *)&orte_notifier_syslog_module;
    return ORTE_SUCCESS;
}
