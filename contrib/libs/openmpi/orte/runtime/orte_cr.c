/*
 * Copyright (c) 2004-2010 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007      Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/** @file
 *
 * ORTE Layer Checkpoint/Restart Runtime functions
 *
 */

#include "orte_config.h"
#include "orte/constants.h"

#include <errno.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif  /* HAVE_UNISTD_H */
#ifdef HAVE_FCNTL_H
#include <fcntl.h>
#endif  /* HAVE_FCNTL_H */
#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif  /* HAVE_SYS_TYPES_H */
#ifdef HAVE_SYS_STAT_H
#include <sys/stat.h>  /* for mkfifo */
#endif  /* HAVE_SYS_STAT_H */

#include "opal/util/opal_environ.h"
#include "opal/util/output.h"
#include "opal/util/basename.h"
#include "opal/mca/event/event.h"
#include "opal/mca/crs/crs.h"
#include "opal/mca/crs/base/base.h"
#include "opal/runtime/opal_cr.h"

#include "orte/runtime/orte_cr.h"
#include "orte/util/proc_info.h"
#include "orte/runtime/orte_globals.h"

#include "orte/mca/plm/base/base.h"
#include "orte/mca/ess/ess.h"
#include "orte/mca/ess/base/base.h"
#include "orte/mca/routed/base/base.h"
#include "orte/mca/routed/routed.h"
#include "orte/mca/rml/base/base.h"
#include "orte/mca/iof/base/base.h"
#include "orte/mca/snapc/snapc.h"
#include "orte/mca/snapc/base/base.h"
#include "orte/mca/filem/base/base.h"

/*************
 * Local functions
 *************/
static int orte_cr_coord_pre_ckpt(void);
static int orte_cr_coord_pre_restart(void);
static int orte_cr_coord_pre_continue(void);

static int orte_cr_coord_post_ckpt(void);
static int orte_cr_coord_post_restart(void);
static int orte_cr_coord_post_continue(void);

bool orte_cr_flush_restart_files = true;

/*************
 * Local vars
 *************/
static opal_cr_coord_callback_fn_t  prev_coord_callback = NULL;

static int orte_cr_output = -1;
static int orte_cr_verbose = 0;

/*
 * CR Init
 */
int orte_cr_init(void)
{
    int ret, exit_status = ORTE_SUCCESS;

    /*
     * OPAL Frameworks
     */
    if (OPAL_SUCCESS != (ret = opal_cr_init() ) ) {
        exit_status = ret;
        goto cleanup;
    }

    /*
     * Register MCA Parameters
     */
    orte_cr_verbose = 0;
    (void) mca_base_var_register ("orte", "orte_cr", NULL, "verbose",
                                  "Verbose output for the ORTE Checkpoint/Restart functionality",
                                  MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                  OPAL_INFO_LVL_8,
                                  MCA_BASE_VAR_SCOPE_READONLY,
                                  &orte_cr_verbose);

    /*** RHC: This is going to crash-and-burn when the output conversion is
     * completed as opal_output will have no idea what opal_cr_output stream means,
     * or even worse, will have assigned it to someone else!
     */

    if(0 != orte_cr_verbose) {
        orte_cr_output = opal_output_open(NULL);
        opal_output_set_verbosity(orte_cr_output, orte_cr_verbose);
    } else {
        orte_cr_output = opal_cr_output;
    }

    opal_output_verbose(10, orte_cr_output,
                        "orte_cr: init: orte_cr_init()\n");

    /* Init ORTE Entry Point Function */
    if( ORTE_SUCCESS != (ret = orte_cr_entry_point_init()) ) {
        exit_status = ret;
        goto cleanup;
    }

    /* Register the ORTE interlevel coordination callback */
    opal_cr_reg_coord_callback(orte_cr_coord, &prev_coord_callback);

    /* Typically this is not needed. Individual BTLs will set this as needed */
    opal_cr_continue_like_restart = false;
    orte_cr_flush_restart_files   = true;

 cleanup:

    return exit_status;
}

/*
 * Finalize
 */
int orte_cr_finalize(void)
{
    opal_output_verbose(10, orte_cr_output,
                        "orte_cr: finalize: orte_cr_finalize()");

    orte_cr_entry_point_finalize();

    /*
     * OPAL Frameworks...
     */
    opal_cr_finalize();

    return ORTE_SUCCESS;
}

/*
 * Interlayer coordination callback
 */
int orte_cr_coord(int state)
{
    int ret, exit_status = ORTE_SUCCESS;

    opal_output_verbose(10, orte_cr_output,
                        "orte_cr: coord: orte_cr_coord(%s)",
                        opal_crs_base_state_str((opal_crs_state_type_t)state));

    /*
     * Before calling the previous callback, we have the opportunity to
     * take action given the state.
     */
    if(OPAL_CRS_CHECKPOINT == state) {
        /* Do Checkpoint Phase work */
        orte_cr_coord_pre_ckpt();
    }
    else if (OPAL_CRS_CONTINUE == state ) {
        /* Do Continue Phase work */
        orte_cr_coord_pre_continue();
    }
    else if (OPAL_CRS_RESTART == state ) {
        /* Do Restart Phase work */
        orte_cr_coord_pre_restart();
    }
    else if (OPAL_CRS_TERM == state ) {
        /* Do Continue Phase work in prep to terminate the application */
    }
    else {
        /* We must have been in an error state from the checkpoint
         * recreate everything, as in the Continue Phase
         */
    }

    /*
     * Call the previous callback, which should be OPAL
     */
    if(OPAL_SUCCESS != (ret = prev_coord_callback(state)) ) {
        exit_status = ret;
        goto cleanup;
    }


    /*
     * After calling the previous callback, we have the opportunity to
     * take action given the state to tidy up.
     */
    if(OPAL_CRS_CHECKPOINT == state) {
        /* Do Checkpoint Phase work */
        orte_cr_coord_post_ckpt();
    }
    else if (OPAL_CRS_CONTINUE == state ) {
        /* Do Continue Phase work */
        orte_cr_coord_post_continue();
    }
    else if (OPAL_CRS_RESTART == state ) {
        /* Do Restart Phase work */
        orte_cr_coord_post_restart();
    }
    else if (OPAL_CRS_TERM == state ) {
        /* Do Continue Phase work in prep to terminate the application */
    }
    else {
        /* We must have been in an error state from the checkpoint
         * recreate everything, as in the Continue Phase
         */
    }

 cleanup:
    return exit_status;
}

/*************
 * Pre Lower Layer
 *************/
static int orte_cr_coord_pre_ckpt(void) {
    int ret, exit_status = ORTE_SUCCESS;

    /*
     * All the checkpoint heavey lifting in here...
     */
    opal_output_verbose(10, orte_cr_output,
                        "orte_cr: coord_pre_ckpt: orte_cr_coord_pre_ckpt()");

    /*
     * Notify the ESS
     */
    if( NULL != orte_ess.ft_event ) {
        if( ORTE_SUCCESS != (ret = orte_ess.ft_event(OPAL_CRS_CHECKPOINT))) {
            exit_status = ret;
            goto cleanup;
        }
    }

 cleanup:
    return exit_status;
}

static int orte_cr_coord_pre_restart(void) {
    /*
     * Can not really do much until OPAL is up and running,
     * so defer action until the post_restart function.
     */
    opal_output_verbose(10, orte_cr_output,
                        "orte_cr: coord_pre_restart: orte_cr_coord_pre_restart()");

    return ORTE_SUCCESS;
}

static int orte_cr_coord_pre_continue(void) {
    /*
     * Can not really do much until OPAL is up and running,
     * so defer action until the post_continue function.
     */
    opal_output_verbose(10, orte_cr_output,
                        "orte_cr: coord_pre_continue: orte_cr_coord_pre_continue()");

    return ORTE_SUCCESS;
}

/*************
 * Post Lower Layer
 *************/
static int orte_cr_coord_post_ckpt(void) {
    /*
     * Now that OPAL is shutdown, we really can't do much
     * so assume pre_ckpt took care of everything.
     */
    opal_output_verbose(10, orte_cr_output,
                        "orte_cr: coord_post_ckpt: orte_cr_coord_post_ckpt()");

    return ORTE_SUCCESS;
}

static int orte_cr_coord_post_restart(void) {
    int ret, exit_status = ORTE_SUCCESS;
    orte_proc_type_t prev_type = ORTE_PROC_TYPE_NONE;
    char * tmp_dir = NULL;

    opal_output_verbose(10, orte_cr_output,
                        "orte_cr: coord_post_restart: orte_cr_coord_post_restart()");

    /*
     * Add the previous session directory for cleanup
     */
    opal_crs_base_cleanup_append(orte_process_info.job_session_dir, true);
    tmp_dir = orte_process_info.jobfam_session_dir;
    if( NULL != tmp_dir ) {
        opal_crs_base_cleanup_append(tmp_dir, true);
        free(tmp_dir);
        tmp_dir = NULL;
    }

    /*
     * Refresh System information
     */
    prev_type = orte_process_info.proc_type;
    if( ORTE_SUCCESS != (ret = orte_proc_info_finalize()) ) {
        exit_status = ret;
    }

    if( NULL != orte_process_info.my_hnp_uri ) {
        free(orte_process_info.my_hnp_uri);
        orte_process_info.my_hnp_uri = NULL;
    }

    if( NULL != orte_process_info.my_daemon_uri ) {
        free(orte_process_info.my_daemon_uri);
        orte_process_info.my_daemon_uri = NULL;
    }

    if( ORTE_SUCCESS != (ret = orte_proc_info()) ) {
        exit_status = ret;
    }

    orte_process_info.proc_type = prev_type;
    orte_process_info.my_name = *ORTE_NAME_INVALID;

    /*
     * Notify the ESS
     */
    if( NULL != orte_ess.ft_event ) {
        if( ORTE_SUCCESS != (ret = orte_ess.ft_event(OPAL_CRS_RESTART))) {
            exit_status = ret;
            goto cleanup;
        }
    }

 cleanup:
    return exit_status;
}

static int orte_cr_coord_post_continue(void) {
    int ret, exit_status = ORTE_SUCCESS;

    opal_output_verbose(10, orte_cr_output,
                        "orte_cr: coord_post_continue: orte_cr_coord_post_continue()\n");

    /*
     * Notify the ESS
     */
    if( NULL != orte_ess.ft_event ) {
        if( ORTE_SUCCESS != (ret = orte_ess.ft_event(OPAL_CRS_CONTINUE))) {
            exit_status = ret;
            goto cleanup;
        }
    }

 cleanup:

    return exit_status;
}

/*************************************************
 * ORTE Entry Point functionality
 *************************************************/
int orte_cr_entry_point_init(void)
{
#if 0
    /* JJH XXX
     * Make sure to finalize the OPAL Entry Point function if it is active.
     */
    opal_cr_entry_point_finalize();
#endif

    return ORTE_SUCCESS;
}

int orte_cr_entry_point_finalize(void)
{
    /* Nothing to do here... */
    return ORTE_SUCCESS;
}

