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
 * Copyright (c) 2017      Intel, Inc. All rights reserved.
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

#include "opal/util/opal_environ.h"
#include "orte/util/name_fns.h"
#include "orte/util/show_help.h"
#include "orte/runtime/orte_globals.h"

#include "orte/mca/plm/plm.h"
#include "orte/mca/plm/base/plm_private.h"
#include "plm_slurm.h"


/*
 * Public string showing the plm ompi_slurm component version number
 */
const char *mca_plm_slurm_component_version_string =
  "Open MPI slurm plm MCA component version " ORTE_VERSION;


/*
 * Local functions
 */
static int plm_slurm_register(void);
static int plm_slurm_open(void);
static int plm_slurm_close(void);
static int orte_plm_slurm_component_query(mca_base_module_t **module, int *priority);


/*
 * Instantiate the public struct with all of our public information
 * and pointers to our public functions in it
 */

orte_plm_slurm_component_t mca_plm_slurm_component = {

    {
        /* First, the mca_component_t struct containing meta
           information about the component itself */

        .base_version = {
            ORTE_PLM_BASE_VERSION_2_0_0,

            /* Component name and version */
            .mca_component_name = "slurm",
            MCA_BASE_MAKE_VERSION(component, ORTE_MAJOR_VERSION, ORTE_MINOR_VERSION,
                                  ORTE_RELEASE_VERSION),

            /* Component open and close functions */
            .mca_open_component = plm_slurm_open,
            .mca_close_component = plm_slurm_close,
            .mca_query_component = orte_plm_slurm_component_query,
            .mca_register_component_params = plm_slurm_register,
        },
        .base_data = {
            /* The component is checkpoint ready */
            MCA_BASE_METADATA_PARAM_CHECKPOINT
        },
    }

    /* Other orte_plm_slurm_component_t items -- left uninitialized
       here; will be initialized in plm_slurm_open() */
};


static int plm_slurm_register(void)
{
    mca_base_component_t *comp = &mca_plm_slurm_component.super.base_version;

    mca_plm_slurm_component.custom_args = NULL;
    (void) mca_base_component_var_register (comp, "args", "Custom arguments to srun",
                                            MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0,
                                            OPAL_INFO_LVL_9,
                                            MCA_BASE_VAR_SCOPE_READONLY,
                                            &mca_plm_slurm_component.custom_args);

    mca_plm_slurm_component.slurm_warning_msg = true;
    (void) mca_base_component_var_register (comp, "warning", "Turn off warning message",
                                            MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                            OPAL_INFO_LVL_9,
                                            MCA_BASE_VAR_SCOPE_READONLY,
                                            &mca_plm_slurm_component.slurm_warning_msg);

    return ORTE_SUCCESS;
}

static int plm_slurm_open(void)
{
    return ORTE_SUCCESS;
}

static int orte_plm_slurm_component_query(mca_base_module_t **module, int *priority)
{
    /* Are we running under a SLURM job? */

    if (NULL != getenv("SLURM_JOBID")) {
        *priority = 75;

        OPAL_OUTPUT_VERBOSE((1, orte_plm_base_framework.framework_output,
                             "%s plm:slurm: available for selection",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));

        *module = (mca_base_module_t *)&orte_plm_slurm_module;
        return ORTE_SUCCESS;
    }

    /* Sadly, no */
    *module = NULL;
    return ORTE_ERROR;
}


static int plm_slurm_close(void)
{
    return ORTE_SUCCESS;
}
