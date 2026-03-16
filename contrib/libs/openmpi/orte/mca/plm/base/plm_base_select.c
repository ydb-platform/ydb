/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2008 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2007 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2011-2015 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2015      Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "orte_config.h"
#include "orte/constants.h"

#include "orte/mca/mca.h"
#include "opal/mca/base/base.h"
#include "opal/mca/base/mca_base_component_repository.h"

#include "orte/util/proc_info.h"

#include "orte/mca/plm/base/plm_private.h"
#include "orte/mca/plm/base/base.h"


/**
 * Function for selecting one component from all those that are
 * available.
 */

int orte_plm_base_select(void)
{
    int rc;
    orte_plm_base_component_t *best_component = NULL;
    orte_plm_base_module_t *best_module = NULL;

    /*
     * Select the best component
     */
    if (OPAL_SUCCESS == (rc = mca_base_select("plm", orte_plm_base_framework.framework_output,
                                              &orte_plm_base_framework.framework_components,
                                              (mca_base_module_t **) &best_module,
                                              (mca_base_component_t **) &best_component, NULL))) {
        /* Save the winner */
        orte_plm = *best_module;
    }

    return rc;
}
