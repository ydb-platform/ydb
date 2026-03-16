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
 * Copyright (c) 2015-2017 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2017      Intel, Inc.  All rights reserved.
 * Copyright (c) 2017      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 * These symbols are in a file by themselves to provide nice linker
 * semantics.  Since linkers generally pull in symbols by object
 * files, keeping these symbols as the only symbols in this file
 * prevents utility programs such as "ompi_info" from having to import
 * entire components just to query their version and parameters.
 */

#include "opal/util/argv.h"

#include "orte_config.h"
#include "orte/constants.h"

#include "orte/util/proc_info.h"
#include "orte/util/show_help.h"

#include "orte/mca/ess/ess.h"
#include "orte/mca/ess/hnp/ess_hnp.h"
#include "orte/runtime/orte_globals.h"

extern orte_ess_base_module_t orte_ess_hnp_module;
static int hnp_component_open(void);
static int hnp_component_close(void);
static int hnp_component_query(mca_base_module_t **module, int *priority);

/*
 * Instantiate the public struct with all of our public information
 * and pointers to our public functions in it
 */
orte_ess_base_component_t mca_ess_hnp_component = {
    .base_version = {
        ORTE_ESS_BASE_VERSION_3_0_0,

        /* Component name and version */
        .mca_component_name = "hnp",
        MCA_BASE_MAKE_VERSION(component, ORTE_MAJOR_VERSION, ORTE_MINOR_VERSION,
                              ORTE_RELEASE_VERSION),

        /* Component open and close functions */
        .mca_open_component = hnp_component_open,
        .mca_close_component = hnp_component_close,
        .mca_query_component = hnp_component_query
    },
    .base_data = {
        /* The component is checkpoint ready */
        MCA_BASE_METADATA_PARAM_CHECKPOINT
    }
};

static int hnp_component_open(void)
{

    return ORTE_SUCCESS;
}


static int hnp_component_query(mca_base_module_t **module, int *priority)
{

    /* we are the hnp module - we need to be selected
     * IFF we are designated as the hnp
     */
    if (ORTE_PROC_IS_HNP) {
        *priority = 100;
        *module = (mca_base_module_t *)&orte_ess_hnp_module;
        return ORTE_SUCCESS;
    }

    /* else, we are not */
    *priority = -1;
    *module = NULL;
    return ORTE_ERROR;
}


static int hnp_component_close(void)
{
    return ORTE_SUCCESS;
}
