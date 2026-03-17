/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 * Copyright (c) 2004-2010 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2017 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2012      The University of Wisconsin-La Crosse. All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/** @file
 *
 * OMPI Layer Checkpoint/Restart Runtime functions
 *
 */

#include "ompi_config.h"

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

#include "opal/mca/event/event.h"
#include "opal/util/output.h"
#include "opal/mca/crs/crs.h"
#include "opal/mca/crs/base/base.h"
#include "opal/mca/installdirs/installdirs.h"
#include "opal/runtime/opal_cr.h"
#include "opal/mca/btl/base/base.h"

#if OPAL_ENABLE_FT_CR == 1
#include "orte/mca/snapc/snapc.h"
#include "orte/mca/snapc/base/base.h"
#endif

#include "ompi/constants.h"
#include "ompi/mca/pml/pml.h"
#include "ompi/mca/pml/base/base.h"
#include "ompi/mca/crcp/crcp.h"
#include "ompi/mca/crcp/base/base.h"
#include "ompi/communicator/communicator.h"
#include "ompi/runtime/ompi_cr.h"
#if OPAL_ENABLE_CRDEBUG == 1
#include "ompi/debuggers/debuggers.h"
#endif

#if OPAL_ENABLE_CRDEBUG == 1
OMPI_DECLSPEC int MPIR_checkpointable = 0;
OMPI_DECLSPEC char * MPIR_controller_hostname = NULL;
OMPI_DECLSPEC char * MPIR_checkpoint_command  = NULL;
OMPI_DECLSPEC char * MPIR_restart_command     = NULL;
OMPI_DECLSPEC char * MPIR_checkpoint_listing_command  = NULL;
#endif

/*************
 * Local functions
 *************/
static int ompi_cr_coord_pre_ckpt(void);
static int ompi_cr_coord_pre_restart(void);
static int ompi_cr_coord_pre_continue(void);

static int ompi_cr_coord_post_ckpt(void);
static int ompi_cr_coord_post_restart(void);
static int ompi_cr_coord_post_continue(void);

/*************
 * Local vars
 *************/
static opal_cr_coord_callback_fn_t  prev_coord_callback = NULL;

int ompi_cr_output = -1;
int ompi_cr_verbosity = 0;

#define NUM_COLLECTIVES 16

#define SIGNAL(comm, modules, highest_module, msg, ret, func)   \
    do {                                                        \
        bool found = false;                                     \
        int k;                                                  \
        mca_coll_base_module_t *my_module =                     \
            comm->c_coll->coll_ ## func ## _module;             \
        if (NULL != my_module) {                                \
            for (k = 0 ; k < highest_module ; ++k) {            \
                if (my_module == modules[k]) found = true;      \
            }                                                   \
            if (!found) {                                       \
                modules[highest_module++] = my_module;          \
                if (NULL != my_module->ft_event) {              \
                    ret = my_module->ft_event(msg);             \
                    if( OMPI_SUCCESS != ret ) {                 \
                        return ret;                             \
                    }                                           \
                }                                               \
            }                                                   \
        }                                                       \
    } while (0)


static int
notify_collectives(int msg)
{
    mca_coll_base_module_t *modules[NUM_COLLECTIVES];
    int i, max, ret, highest_module = 0;

    memset(&modules, 0, sizeof(mca_coll_base_module_t*) * NUM_COLLECTIVES);

    max = opal_pointer_array_get_size(&ompi_mpi_communicators);
    for (i = 0 ; i < max ; ++i) {
        ompi_communicator_t *comm =
            (ompi_communicator_t *)opal_pointer_array_get_item(&ompi_mpi_communicators, i);
        if (NULL == comm) continue;

        SIGNAL(comm, modules, highest_module, msg, ret, allgather);
        SIGNAL(comm, modules, highest_module, msg, ret, allgatherv);
        SIGNAL(comm, modules, highest_module, msg, ret, allreduce);
        SIGNAL(comm, modules, highest_module, msg, ret, alltoall);
        SIGNAL(comm, modules, highest_module, msg, ret, alltoallv);
        SIGNAL(comm, modules, highest_module, msg, ret, alltoallw);
        SIGNAL(comm, modules, highest_module, msg, ret, barrier);
        SIGNAL(comm, modules, highest_module, msg, ret, bcast);
        SIGNAL(comm, modules, highest_module, msg, ret, exscan);
        SIGNAL(comm, modules, highest_module, msg, ret, gather);
        SIGNAL(comm, modules, highest_module, msg, ret, gatherv);
        SIGNAL(comm, modules, highest_module, msg, ret, reduce);
        SIGNAL(comm, modules, highest_module, msg, ret, reduce_scatter);
        SIGNAL(comm, modules, highest_module, msg, ret, scan);
        SIGNAL(comm, modules, highest_module, msg, ret, scatter);
        SIGNAL(comm, modules, highest_module, msg, ret, scatterv);
    }

    return OMPI_SUCCESS;
}


/*
 * CR Init
 */
int ompi_cr_init(void)
{
    /*
     * Register some MCA variables
     */
    ompi_cr_verbosity = 0;
    (void) mca_base_var_register("ompi", "ompi", "cr", "verbose",
                                 "Verbose output for the OMPI Checkpoint/Restart functionality",
                                 MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                 OPAL_INFO_LVL_9,
                                 MCA_BASE_VAR_SCOPE_READONLY,
                                 &ompi_cr_verbosity);
    if(0 != ompi_cr_verbosity) {
        ompi_cr_output = opal_output_open(NULL);
        opal_output_set_verbosity(ompi_cr_output, ompi_cr_verbosity);
    } else {
        ompi_cr_output = opal_cr_output;
    }

    opal_output_verbose(10, ompi_cr_output,
                        "ompi_cr: init: ompi_cr_init()");

    /* Register the OMPI interlevel coordination callback */
    opal_cr_reg_coord_callback(ompi_cr_coord, &prev_coord_callback);

#if OPAL_ENABLE_CRDEBUG == 1
    /* Check for C/R enabled debugging */
    if( MPIR_debug_with_checkpoint ) {
        char *uri = NULL;
        char *sep = NULL;
        char *hostname = NULL;

        /* Mark as debuggable with C/R */
        MPIR_checkpointable = 1;

        /* Set the checkpoint and restart commands */
        /* Add the full path to the binary */
        asprintf(&MPIR_checkpoint_command,
                 "%s/ompi-checkpoint --crdebug --hnp-jobid %u",
                 opal_install_dirs.bindir,
                 ORTE_PROC_MY_HNP->jobid);
        asprintf(&MPIR_restart_command,
                 "%s/ompi-restart --crdebug ",
                 opal_install_dirs.bindir);
        asprintf(&MPIR_checkpoint_listing_command,
                 "%s/ompi-checkpoint -l --crdebug ",
                 opal_install_dirs.bindir);

        /* Set contact information for HNP */
        uri = strdup(ompi_process_info.my_hnp_uri);
        hostname = strchr(uri, ';') + 1;
        sep = strchr(hostname, ';');
        if (sep) {
            *sep = 0;
        }
        if (strncmp(hostname, "tcp://", 6) == 0) {
            hostname += 6;
            sep = strchr(hostname, ':');
            *sep = 0;
            MPIR_controller_hostname = strdup(hostname);
        } else {
            MPIR_controller_hostname = strdup("localhost");
        }

        /* Cleanup */
        if( NULL != uri ) {
            free(uri);
            uri = NULL;
        }
    }
#endif

    return OMPI_SUCCESS;
}

/*
 * Finalize
 */
int ompi_cr_finalize(void)
{
    opal_output_verbose(10, ompi_cr_output,
                        "ompi_cr: finalize: ompi_cr_finalize()");

    return OMPI_SUCCESS;
}

/*
 * Interlayer coordination callback
 */
int ompi_cr_coord(int state)
{
    int ret, exit_status = OMPI_SUCCESS;

    opal_output_verbose(10, ompi_cr_output,
                        "ompi_cr: coord: ompi_cr_coord(%s)\n",
                        opal_crs_base_state_str((opal_crs_state_type_t)state));

    /*
     * Before calling the previous callback, we have the opportunity to
     * take action given the state.
     */
    if(OPAL_CRS_CHECKPOINT == state) {
        /* Do Checkpoint Phase work */
        ret = ompi_cr_coord_pre_ckpt();
        if( ret == OMPI_EXISTS) {
            return ret;
        }
        else if( ret != OMPI_SUCCESS) {
            return ret;
        }
    }
    else if (OPAL_CRS_CONTINUE == state ) {
        /* Do Continue Phase work */
        ompi_cr_coord_pre_continue();
    }
    else if (OPAL_CRS_RESTART == state ) {
        /* Do Restart Phase work */
        ompi_cr_coord_pre_restart();
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
     * Call the previous callback, which should be ORTE [which will handle OPAL]
     */
    if(OMPI_SUCCESS != (ret = prev_coord_callback(state)) ) {
        exit_status = ret;
        goto cleanup;
    }


    /*
     * After calling the previous callback, we have the opportunity to
     * take action given the state to tidy up.
     */
    if(OPAL_CRS_CHECKPOINT == state) {
        /* Do Checkpoint Phase work */
        ompi_cr_coord_post_ckpt();
    }
    else if (OPAL_CRS_CONTINUE == state ) {
        /* Do Continue Phase work */
        ompi_cr_coord_post_continue();

#if OPAL_ENABLE_CRDEBUG == 1
        /*
         * If C/R enabled debugging,
         * wait here for debugger to attach
         */
        if( MPIR_debug_with_checkpoint ) {
            MPIR_checkpoint_debugger_breakpoint();
        }
#endif
    }
    else if (OPAL_CRS_RESTART == state ) {
        /* Do Restart Phase work */
        ompi_cr_coord_post_restart();

#if OPAL_ENABLE_CRDEBUG == 1
        /*
         * If C/R enabled debugging,
         * wait here for debugger to attach
         */
        if( MPIR_debug_with_checkpoint ) {
            MPIR_checkpoint_debugger_breakpoint();
        }
#endif
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
static int ompi_cr_coord_pre_ckpt(void) {
    int ret, exit_status = OMPI_SUCCESS;

    /*
     * All the checkpoint heavey lifting in here...
     */
    opal_output_verbose(10, ompi_cr_output,
                        "ompi_cr: coord_pre_ckpt: ompi_cr_coord_pre_ckpt()\n");

    /*
     * Notify Collectives
     * - Need to do this on a per communicator basis
     *   Traverse all communicators...
     */
    if (OMPI_SUCCESS != (ret = notify_collectives(OPAL_CR_CHECKPOINT))) {
        goto cleanup;
    }

    /*
     * Notify PML
     *  - Will notify BML and BTL's
     */
    if( OMPI_SUCCESS != (ret = mca_pml.pml_ft_event(OPAL_CRS_CHECKPOINT))) {
        exit_status = ret;
        goto cleanup;
    }

 cleanup:

    return exit_status;
}

static int ompi_cr_coord_pre_restart(void) {
    int ret, exit_status = OMPI_SUCCESS;

    opal_output_verbose(10, ompi_cr_output,
                        "ompi_cr: coord_pre_restart: ompi_cr_coord_pre_restart()");

    /*
     * Notify PML
     *  - Will notify BML and BTL's
     *  - The intention here is to have the PML shutdown all the old components
     *    and handles. On the second pass (once ORTE is restarted) we can
     *    reconnect processes.
     */
    if( OMPI_SUCCESS != (ret = mca_pml.pml_ft_event(OPAL_CRS_RESTART_PRE))) {
        exit_status = ret;
        goto cleanup;
    }

 cleanup:
    return exit_status;
}

static int ompi_cr_coord_pre_continue(void) {
#if OPAL_ENABLE_FT_CR == 1
    int ret, exit_status = OMPI_SUCCESS;

    /*
     * Can not really do much until ORTE is up and running,
     * so defer action until the post_continue function.
     */
    opal_output_verbose(10, ompi_cr_output,
                        "ompi_cr: coord_pre_continue: ompi_cr_coord_pre_continue()");

    if (opal_cr_continue_like_restart) {
        /* Mimic ompi_cr_coord_pre_restart(); */
        if( OMPI_SUCCESS != (ret = mca_pml.pml_ft_event(OPAL_CRS_CONTINUE))) {
            exit_status = ret;
            goto cleanup;
        }
    }
    else {
        if( opal_cr_timing_barrier_enabled ) {
            OPAL_CR_SET_TIMER(OPAL_CR_TIMER_P2PBR1);
        }
        OPAL_CR_SET_TIMER(OPAL_CR_TIMER_P2P3);
        if( opal_cr_timing_barrier_enabled ) {
            OPAL_CR_SET_TIMER(OPAL_CR_TIMER_P2PBR2);
        }
        OPAL_CR_SET_TIMER(OPAL_CR_TIMER_CRCP1);
    }

 cleanup:
    return exit_status;
#else
    return OMPI_SUCCESS;
#endif
}

/*************
 * Post Lower Layer
 *************/
static int ompi_cr_coord_post_ckpt(void) {
    /*
     * Now that ORTE/OPAL are shutdown, we really can't do much
     * so assume pre_ckpt took care of everything.
     */
    opal_output_verbose(10, ompi_cr_output,
                        "ompi_cr: coord_post_ckpt: ompi_cr_coord_post_ckpt()");

    return OMPI_SUCCESS;
}

static int ompi_cr_coord_post_restart(void) {
    int ret, exit_status = OMPI_SUCCESS;

    opal_output_verbose(10, ompi_cr_output,
                        "ompi_cr: coord_post_restart: ompi_cr_coord_post_restart()");

    /*
     * Notify PML
     *  - Will notify BML and BTL's
     */
    if( OMPI_SUCCESS != (ret = mca_pml.pml_ft_event(OPAL_CRS_RESTART))) {
        exit_status = ret;
        goto cleanup;
    }

    /*
     * Notify Collectives
     * - Need to do this on a per communicator basis
     *   Traverse all communicators...
     */
    if (OMPI_SUCCESS != (ret = notify_collectives(OPAL_CRS_RESTART))) {
        goto cleanup;
    }

 cleanup:

    return exit_status;
}

static int ompi_cr_coord_post_continue(void) {
    int ret, exit_status = OMPI_SUCCESS;

    opal_output_verbose(10, ompi_cr_output,
                        "ompi_cr: coord_post_continue: ompi_cr_coord_post_continue()");

    /*
     * Notify PML
     *  - Will notify BML and BTL's
     */
    if( OMPI_SUCCESS != (ret = mca_pml.pml_ft_event(OPAL_CRS_CONTINUE))) {
        exit_status = ret;
        goto cleanup;
    }

    /*
     * Notify Collectives
     * - Need to do this on a per communicator basis
     *   Traverse all communicators...
     */
    if (OMPI_SUCCESS != (ret = notify_collectives(OPAL_CRS_CONTINUE))) {
        goto cleanup;
    }

 cleanup:

    return exit_status;
}
