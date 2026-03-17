/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2010 The Trustees of Indiana University.
 *                         All rights reserved.
 *
 * Copyright (c) 2015 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"

#include <string.h>
#include <sys/types.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif  /* HAVE_UNISTD_H */

#include "opal/util/opal_environ.h"
#include "opal/util/output.h"
#include "opal/util/argv.h"
#include "opal/util/show_help.h"
#include "opal/util/opal_environ.h"

#include "opal/constants.h"
#include "opal/mca/base/mca_base_var.h"

#include "opal/mca/crs/crs.h"
#include "opal/mca/crs/base/base.h"
#include "opal/runtime/opal_cr.h"

#include "crs_none.h"

int opal_crs_none_module_init(void)
{
    /*
     * If not a tool, and requesting C/R support print a warning.
     */
    if( opal_crs_none_select_warning &&
        !opal_cr_is_tool && opal_cr_is_enabled ) {
        opal_show_help("help-opal-crs-none.txt", "none:select-warning",
                       true);
    }

    return OPAL_SUCCESS;
}

int opal_crs_none_module_finalize(void)
{
    return OPAL_SUCCESS;
}

int opal_crs_none_checkpoint(pid_t pid,
                             opal_crs_base_snapshot_t *snapshot,
                             opal_crs_base_ckpt_options_t *options,
                             opal_crs_state_type_t *state)
{
    *state = OPAL_CRS_CONTINUE;

    snapshot->component_name  = strdup("none");
    snapshot->cold_start      = false;

    /*
     * Update the snapshot metadata
     */
    if( NULL == snapshot->metadata ) {
        if (NULL == (snapshot->metadata = fopen(snapshot->metadata_filename, "a")) ) {
            opal_output(0,
                        "crs:none: checkpoint(): Error: Unable to open the file (%s)",
                        snapshot->metadata_filename);
            return OPAL_ERROR;
        }
    }
    fprintf(snapshot->metadata, "%s%s\n", CRS_METADATA_COMP, snapshot->component_name);
    fclose(snapshot->metadata);
    snapshot->metadata = NULL;

    if( options->stop ) {
        opal_output(0,
                    "crs:none: checkpoint(): Error: SIGSTOP Not currently supported!");
    }

    return OPAL_SUCCESS;
}

int opal_crs_none_restart(opal_crs_base_snapshot_t *base_snapshot, bool spawn_child, pid_t *child_pid)
{
    int exit_status = OPAL_SUCCESS;
    char **tmp_argv = NULL;
    char **cr_argv = NULL;
    int status;

    *child_pid = getpid();

    if( NULL == base_snapshot->metadata ) {
        if (NULL == (base_snapshot->metadata = fopen(base_snapshot->metadata_filename, "a")) ) {
            opal_output(0,
                        "crs:none: checkpoint(): Error: Unable to open the file (%s)",
                        base_snapshot->metadata_filename);
            return OPAL_ERROR;
        }
    }

    opal_crs_base_metadata_read_token(base_snapshot->metadata, CRS_METADATA_CONTEXT, &tmp_argv);

    if( NULL == tmp_argv ) {
        opal_output(opal_crs_base_framework.framework_output,
                    "crs:none: none_restart: Error: Failed to read the %s token from the local checkpoint in %s",
                    CRS_METADATA_CONTEXT, base_snapshot->metadata_filename);
        exit_status = OPAL_ERROR;
        goto cleanup;
    }

    if( opal_argv_count(tmp_argv) <= 0 ) {
        opal_output_verbose(10, opal_crs_base_framework.framework_output,
                            "crs:none: none_restart: No command line to exec, so just returning");
        exit_status = OPAL_SUCCESS;
        goto cleanup;
    }

    if ( NULL == (cr_argv = opal_argv_split(tmp_argv[0], ' ')) ) {
        exit_status = OPAL_ERROR;
        goto cleanup;
    }

    if( !spawn_child ) {
        opal_output_verbose(10, opal_crs_base_framework.framework_output,
                            "crs:none: none_restart: exec :(%s, %s):",
                            cr_argv[0], tmp_argv[0]);

        status = execvp(cr_argv[0], cr_argv);

        if(status < 0) {
            opal_output(opal_crs_base_framework.framework_output,
                        "crs:none: none_restart: Child failed to execute :(%d):", status);
        }
        opal_output(opal_crs_base_framework.framework_output,
                    "crs:none: none_restart: execvp returned %d", status);
        exit_status = status;
        goto cleanup;
    } else {
        opal_output(opal_crs_base_framework.framework_output,
                   "crs:none: none_restart: Spawn not implemented");
        exit_status = OPAL_ERR_NOT_IMPLEMENTED;
        goto cleanup;
    }

 cleanup:
    if (cr_argv) {
        opal_argv_free (cr_argv);
    }

    fclose(base_snapshot->metadata);

    return exit_status;
}

int opal_crs_none_disable_checkpoint(void)
{
    return OPAL_SUCCESS;
}

int opal_crs_none_enable_checkpoint(void)
{
    return OPAL_SUCCESS;
}

int opal_crs_none_prelaunch(int32_t rank,
                            char *base_snapshot_dir,
                            char **app,
                            char **cwd,
                            char ***argv,
                            char ***env)
{
    char * tmp_env_var = NULL;

    (void) mca_base_var_env_name("opal_cr_is_tool", &tmp_env_var);
    opal_setenv(tmp_env_var,
                "0", true, env);
    free(tmp_env_var);
    tmp_env_var = NULL;

    return OPAL_SUCCESS;
}

int opal_crs_none_reg_thread(void)
{
    return OPAL_SUCCESS;
}
