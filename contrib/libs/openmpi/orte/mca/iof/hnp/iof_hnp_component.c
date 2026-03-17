/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007      Sun Microsystems, Inc.  All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2016      Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "orte_config.h"

#include "opal/mca/base/base.h"
#include "opal/util/output.h"
#include "opal/mca/event/event.h"

#include "orte/util/proc_info.h"

#include "orte/mca/iof/base/base.h"
#include "iof_hnp.h"

/*
 * Local functions
 */
static int orte_iof_hnp_open(void);
static int orte_iof_hnp_close(void);
static int orte_iof_hnp_query(mca_base_module_t **module, int *priority);

/*
 * Public string showing the iof hnp component version number
 */
const char *mca_iof_hnp_component_version_string =
    "Open MPI hnp iof MCA component version " ORTE_VERSION;

orte_iof_hnp_component_t mca_iof_hnp_component = {
    {
        /* First, the mca_base_component_t struct containing meta
         information about the component itself */

        .iof_version = {
            ORTE_IOF_BASE_VERSION_2_0_0,

            .mca_component_name = "hnp",
            MCA_BASE_MAKE_VERSION(component, ORTE_MAJOR_VERSION, ORTE_MINOR_VERSION,
                                  ORTE_RELEASE_VERSION),

            /* Component open, close, and query functions */
            .mca_open_component = orte_iof_hnp_open,
            .mca_close_component = orte_iof_hnp_close,
            .mca_query_component = orte_iof_hnp_query,
        },
        .iof_data = {
            /* The component is checkpoint ready */
            MCA_BASE_METADATA_PARAM_CHECKPOINT
        },
    }
};

/**
  * component open/close/init function
  */
static int orte_iof_hnp_open(void)
{
    /* Nothing to do */
    return ORTE_SUCCESS;
}


static int orte_iof_hnp_close(void)
{
    return ORTE_SUCCESS;
}

/**
 * Module query
 */

static int orte_iof_hnp_query(mca_base_module_t **module, int *priority)
{
    /* if we are not the HNP, then don't use this module */
    if (!ORTE_PROC_IS_HNP && !ORTE_PROC_IS_MASTER) {
        *priority = -1;
        *module = NULL;
        return ORTE_ERROR;
    }

    *priority = 100;
    *module = (mca_base_module_t *) &orte_iof_hnp_module;

    return ORTE_SUCCESS;
}
