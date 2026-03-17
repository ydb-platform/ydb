/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c)      2010 The Trustees of Indiana University.
 *                         All rights reserved.
 * Copyright (c) 2012      The University of Wisconsin-La Crosse. All rights
 *                         reserved.
 * Copyright (c) 2013      Los Alamos National Security, LLC.  All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "orte_config.h"

#include "orte/constants.h"
#include "orte/mca/mca.h"
#include "opal/util/output.h"
#include "opal/util/opal_environ.h"
#include "opal/mca/base/base.h"

#include "orte/util/proc_info.h"

#include "orte/mca/sstore/sstore.h"
#include "orte/mca/sstore/base/base.h"

#include "orte/mca/sstore/base/static-components.h"

/*
 * Globals
 */
orte_sstore_base_module_t orte_sstore = {
    NULL, /* sstore_init     */
    NULL, /* ssotore_finalize */

    NULL, /* request_checkpoint_handle    */
    NULL, /* request_restart_handle       */
    NULL, /* request_global_snapshot_data */
    NULL, /* register_handle              */
    NULL, /* get_attr */
    NULL, /* set_attr */
    NULL, /* sync     */
    NULL, /* remove   */
    NULL, /* pack     */
    NULL, /* unpack   */
    NULL, /* fetch_app_deps */
    NULL  /* wait_all_deps  */
};
int orte_sstore_context = -1;

bool   orte_sstore_base_is_checkpoint_available = false;
char * orte_sstore_base_local_metadata_filename = NULL;
char * orte_sstore_base_global_metadata_filename = NULL;
char * orte_sstore_base_local_snapshot_fmt = NULL;
char * orte_sstore_base_global_snapshot_dir = NULL;
char * orte_sstore_base_global_snapshot_ref = NULL;
char * orte_sstore_base_prelaunch_location  = NULL;

orte_sstore_base_handle_t orte_sstore_handle_current = {0};
orte_sstore_base_handle_t orte_sstore_handle_last_stable = {0};

/* Determine the context of this module */
int orte_sstore_base_determine_context(void);

static int orte_sstore_base_register(mca_base_register_flag_t flags)
{
    int mca_index;
    /*
     * Base Global Snapshot directory
     */
    orte_sstore_base_global_snapshot_dir = (char *) opal_home_directory();
    mca_index = mca_base_var_register("orte", "sstore", "base", "global_snapshot_dir",
                                      "The base directory to use when storing global snapshots",
                                      MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0,
                                      OPAL_INFO_LVL_9,
                                      MCA_BASE_VAR_SCOPE_READONLY,
                                      &orte_sstore_base_global_snapshot_dir);
    mca_base_var_register_synonym(mca_index, "orte", "snapc", "base", "global_snapshot_dir",
                                  MCA_BASE_VAR_SYN_FLAG_DEPRECATED);

    /*
     * User defined snapshot reference to use for this job
     */
    orte_sstore_base_global_snapshot_ref = NULL;
    mca_index = mca_base_var_register("orte", "sstore", "base", "global_snapshot_ref",
                                      "The global snapshot reference to be used for this job. "
                                      " [Default = ompi_global_snapshot_MPIRUNPID.ckpt]",
                                      MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0,
                                      OPAL_INFO_LVL_9,
                                      MCA_BASE_VAR_SCOPE_READONLY,
                                      &orte_sstore_base_global_snapshot_ref);
    mca_base_var_register_synonym(mca_index, "orte", "snapc", "base", "global_snapshot_ref",
                                  MCA_BASE_VAR_SYN_FLAG_DEPRECATED);

    return ORTE_SUCCESS;
}

static int orte_sstore_base_close(void)
{
    /* Close the selected component */
    if( NULL != orte_sstore.sstore_finalize ) {
        orte_sstore.sstore_finalize();
    }

    return mca_base_framework_components_close(&orte_sstore_base_framework, NULL);
}

/**
 * Function for finding and opening either all MCA components,
 * or the one that was specifically requested via a MCA parameter.
 */
static int orte_sstore_base_open(mca_base_open_flag_t flags)
{
    int rc;

    orte_sstore_handle_current     = ORTE_SSTORE_HANDLE_INVALID;
    orte_sstore_handle_last_stable = ORTE_SSTORE_HANDLE_INVALID;

    orte_sstore_base_local_metadata_filename  = strdup("snapshot_meta.data");
    orte_sstore_base_global_metadata_filename = strdup("global_snapshot_meta.data");
    orte_sstore_base_local_snapshot_fmt       = strdup("opal_snapshot_%d.ckpt");

    /*
     * Setup the prelaunch variable to point to the first possible snapshot
     * location
     */
    if( NULL != orte_sstore_base_global_snapshot_ref ) {
        asprintf(&orte_sstore_base_prelaunch_location,
                 "%s/%s/%d",
                 orte_sstore_base_global_snapshot_dir,
                 orte_sstore_base_global_snapshot_ref,
                 0);
    }

    /* Open up all available components */
    if (ORTE_SUCCESS != (rc = mca_base_framework_components_open(&orte_sstore_base_framework, flags))) {
        return rc;
    }

    opal_output_verbose(10, orte_sstore_base_framework.framework_output,
                        "sstore:base: open()");
    opal_output_verbose(10, orte_sstore_base_framework.framework_output,
                        "sstore:base: open: Global snapshot directory = %s",
                        orte_sstore_base_global_snapshot_dir);
    opal_output_verbose(10, orte_sstore_base_framework.framework_output,
                        "sstore:base: open: Global snapshot reference = %s",
                        (NULL == orte_sstore_base_global_snapshot_ref ? "Default" : orte_sstore_base_global_snapshot_ref));
    opal_output_verbose(10, orte_sstore_base_framework.framework_output,
                        "sstore:base: open: Prelaunch location        = %s",
                        (NULL == orte_sstore_base_prelaunch_location ? "Undefined" : orte_sstore_base_prelaunch_location));

    orte_sstore_context = ORTE_SSTORE_UNASSIGN_TYPE;
    orte_sstore_base_determine_context();

    return ORTE_SUCCESS;
}

MCA_BASE_FRAMEWORK_DECLARE(orte, sstore, "ORTE Sstore", orte_sstore_base_register,
                           orte_sstore_base_open, orte_sstore_base_close,
                           mca_sstore_base_static_components, 0);


int orte_sstore_base_determine_context(void)
{
    if( ORTE_PROC_IS_HNP) {
        orte_sstore_context |= ORTE_SSTORE_GLOBAL_TYPE;
        if( ORTE_PROC_IS_DAEMON ) {
            orte_sstore_context |= ORTE_SSTORE_LOCAL_TYPE;
        }
    }
    else if( ORTE_PROC_IS_DAEMON ) {
        orte_sstore_context |= ORTE_SSTORE_LOCAL_TYPE;
    }
    else if( ORTE_PROC_IS_TOOL ) {
        orte_sstore_context |= ORTE_SSTORE_TOOL_TYPE;
    }
    else if( !ORTE_PROC_IS_DAEMON ) {
        orte_sstore_context |= ORTE_SSTORE_APP_TYPE;
    }

    return ORTE_SUCCESS;
}
