/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2011      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2011-2015 Los Alamos National Security, LLC. All
 *                         rights reserved.
 * Copyright (c) 2014-2016 Intel, Inc. All rights reserved.
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

#include "orte_config.h"
#include "orte/constants.h"

#include "opal/runtime/opal_params.h"
#include "opal/mca/pmix/pmix.h"
#include "opal/mca/pmix/base/base.h"

#include "orte/util/proc_info.h"
#include "orte/mca/schizo/schizo.h"

#include "orte/mca/ess/ess.h"
#include "orte/mca/ess/pmi/ess_pmi.h"

extern orte_ess_base_module_t orte_ess_pmi_module;

static int pmi_component_open(void);
static int pmi_component_close(void);
static int pmi_component_query(mca_base_module_t **module, int *priority);

/*
 * Instantiate the public struct with all of our public information
 * and pointers to our public functions in it
 */
orte_ess_base_component_t mca_ess_pmi_component = {
    .base_version = {
        ORTE_ESS_BASE_VERSION_3_0_0,

        /* Component name and version */
        .mca_component_name = "pmi",
        MCA_BASE_MAKE_VERSION(component, ORTE_MAJOR_VERSION, ORTE_MINOR_VERSION,
                              ORTE_RELEASE_VERSION),

        /* Component open and close functions */
        .mca_open_component = pmi_component_open,
        .mca_close_component = pmi_component_close,
        .mca_query_component = pmi_component_query,
    },
    .base_data = {
        /* The component is checkpoint ready */
        MCA_BASE_METADATA_PARAM_CHECKPOINT
    },
};

static int pmi_component_open(void)
{
    return ORTE_SUCCESS;
}

static int pmi_component_query(mca_base_module_t **module, int *priority)
{
    orte_schizo_launch_environ_t ret;

    if (!ORTE_PROC_IS_APP) {
        *module = NULL;
        *priority = 0;
        return ORTE_ERROR;
    }

    /* find out what our environment looks like */
    ret = orte_schizo.check_launch_environment();
    if (ORTE_SCHIZO_UNMANAGED_SINGLETON == ret ||
        ORTE_SCHIZO_MANAGED_SINGLETON == ret) {
        /* not us */
        *module = NULL;
        *priority = 0;
        return ORTE_ERROR;
    }

    *priority = 35;
    *module = (mca_base_module_t *)&orte_ess_pmi_module;
    return ORTE_SUCCESS;
}


static int pmi_component_close(void)
{
    return ORTE_SUCCESS;
}

