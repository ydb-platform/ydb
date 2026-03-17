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
 * Copyright (c) 2015      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2016-2017 Intel, Inc. All rights reserved.
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

#include "opal/mca/pmix/pmix.h"
#include "opal/mca/pmix/base/base.h"

#include "orte/util/proc_info.h"
#include "orte/util/show_help.h"
#include "orte/mca/schizo/schizo.h"

#include "orte/mca/ess/ess.h"
#include "orte/mca/ess/singleton/ess_singleton.h"

extern orte_ess_base_module_t orte_ess_singleton_module;

static int component_open(void);
static int component_close(void);
static int component_query(mca_base_module_t **module, int *priority);
static int component_register(void);

/*
 * Instantiate the public struct with all of our public information
 * and pointers to our public functions in it
 */
orte_ess_singleton_component_t mca_ess_singleton_component = {
    {
        /* First, the mca_component_t struct containing meta information
           about the component itself */
        .base_version = {
            ORTE_ESS_BASE_VERSION_3_0_0,

            /* Component name and version */
            .mca_component_name = "singleton",
            MCA_BASE_MAKE_VERSION(component, ORTE_MAJOR_VERSION, ORTE_MINOR_VERSION,
                                  ORTE_RELEASE_VERSION),

            /* Component open and close functions */
            .mca_open_component = component_open,
            .mca_close_component = component_close,
            .mca_query_component = component_query,
            .mca_register_component_params = component_register,
        },
        .base_data = {
            /* The component is not checkpoint ready */
            MCA_BASE_METADATA_PARAM_NONE
        },
    },
    .server_uri = NULL,
    .isolated = false
};

static int component_register(void)
{
    int ret;

    mca_ess_singleton_component.server_uri = NULL;
    ret = mca_base_component_var_register(&mca_ess_singleton_component.super.base_version,
                                          "server",
                                          "Server to be used as HNP - [file|FILE]:<filename> or just uri",
                                          MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0,
                                          OPAL_INFO_LVL_9,
                                          MCA_BASE_VAR_SCOPE_READONLY,
                                          &mca_ess_singleton_component.server_uri);
    (void) mca_base_var_register_synonym(ret, "orte", "orte", NULL, "server", 0);

    ret = mca_base_component_var_register(&mca_ess_singleton_component.super.base_version,
                                          "isolated",
                                          "Do not start a supporting daemon as this process will never attempt to spawn",
                                          MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                          OPAL_INFO_LVL_9,
                                          MCA_BASE_VAR_SCOPE_READONLY,
                                          &mca_ess_singleton_component.isolated);

    return ORTE_SUCCESS;
}

static int component_open(void)
{
    return ORTE_SUCCESS;
}

static int component_query(mca_base_module_t **module, int *priority)
{
    orte_schizo_launch_environ_t ret;

    /* if we are an HNP, daemon, or tool, then we
     * are definitely not a singleton!
     */
    if (ORTE_PROC_IS_HNP ||
        ORTE_PROC_IS_DAEMON ||
        ORTE_PROC_IS_TOOL) {
        *module = NULL;
        *priority = 0;
        return ORTE_ERROR;
    }

    /* find out what our environment looks like */
    ret = orte_schizo.check_launch_environment();
    if (ORTE_SCHIZO_UNMANAGED_SINGLETON != ret &&
        ORTE_SCHIZO_MANAGED_SINGLETON != ret) {
        /* not us */
        *module = NULL;
        *priority = 0;
        return ORTE_ERROR;
    }

    /* we may be incorrectly trying to run as a singleton - e.g.,
     * someone direct-launched us under SLURM without building
     * ORTE --with-slurm or in a slurm environment (so we didn't
     * autodetect slurm). Try to detect that here. Sadly, we
     * cannot just use the schizo framework to help us here as
     * the corresponding schizo component may not have even
     * been build. So we have to do things a little uglier */

    if (ORTE_SCHIZO_UNMANAGED_SINGLETON == ret) {
        /* see if we are in a SLURM allocation */
        if (NULL != getenv("SLURM_NODELIST")) {
            /* emit a hopefully helpful error message and abort */
            orte_show_help("help-ess-base.txt", "slurm-error2", true);
            *module = NULL;
            *priority = 0;
            return ORTE_ERR_SILENT;
        }
        /* see if we are under ALPS */
        if (NULL != getenv("ALPS_APP_ID")) {
            orte_show_help("help-ess-base.txt", "alps-error2", true);
            *module = NULL;
            *priority = 0;
            return ORTE_ERR_SILENT;
        }
    }

    /* okay, we want to be selected as we must be a singleton */
    *priority = 100;
    *module = (mca_base_module_t *)&orte_ess_singleton_module;
    return ORTE_SUCCESS;
}


static int component_close(void)
{
    return ORTE_SUCCESS;
}
