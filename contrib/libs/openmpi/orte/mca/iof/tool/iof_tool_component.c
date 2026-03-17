/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2006 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2018      Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "orte_config.h"

#include "opal/mca/base/base.h"

#include "orte/mca/rml/rml.h"
#include "orte/mca/rml/rml_types.h"
#include "orte/util/proc_info.h"
#include "orte/mca/errmgr/errmgr.h"
#include "orte/runtime/orte_globals.h"

#include "iof_tool.h"

/*
 * Local functions
 */
static int orte_iof_tool_open(void);
static int orte_iof_tool_close(void);
static int orte_iof_tool_query(mca_base_module_t **module, int *priority);


/*
 * Public string showing the iof tool component version number
 */
const char *mca_iof_tool_component_version_string =
"Open MPI tool iof MCA component version " ORTE_VERSION;


orte_iof_tool_component_t mca_iof_tool_component = {
    {
        .iof_version = {
            ORTE_IOF_BASE_VERSION_2_0_0,

            .mca_component_name = "tool",
            MCA_BASE_MAKE_VERSION(component, ORTE_MAJOR_VERSION, ORTE_MINOR_VERSION,
                                  ORTE_RELEASE_VERSION),

            /* Component open, close, and query functions */
            .mca_open_component = orte_iof_tool_open,
            .mca_close_component = orte_iof_tool_close,
            .mca_query_component = orte_iof_tool_query,
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
static int orte_iof_tool_open(void)
{
    /* Nothing to do */
    return ORTE_SUCCESS;
}

static int orte_iof_tool_close(void)
{
    return ORTE_SUCCESS;
}


static int orte_iof_tool_query(mca_base_module_t **module, int *priority)
{
    /* if we are not a tool, then don't use this module */
    if (!ORTE_PROC_IS_TOOL) {
        *module = NULL;
        *priority = -1;
        return ORTE_ERROR;
    }

    *priority = 100;
    *module = (mca_base_module_t *) &orte_iof_tool_module;

    return ORTE_SUCCESS;
}
