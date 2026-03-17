/*
 * Copyright (c) 2004-2010 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2012 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007-2017 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2011      Oak Ridge National Labs.  All rights reserved.
 * Copyright (c) 2012-2013 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2017      IBM Corporation. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/** @file
 *
 * OPAL Layer Checkpoint/Restart Runtime functions
 *
 */

#include "opal_config.h"

#include <string.h>
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
#include <signal.h>

#include "opal/class/opal_object.h"
#include "opal/util/opal_environ.h"
#include "opal/util/show_help.h"
#include "opal/util/output.h"
#include "opal/util/malloc.h"
#include "opal/util/keyval_parse.h"
#include "opal/util/opal_environ.h"
#include "opal/util/argv.h"
#include "opal/memoryhooks/memory.h"

#include "opal/mca/base/base.h"
#include "opal/runtime/opal_cr.h"
#include "opal/runtime/opal.h"
#include "opal/constants.h"

#include "opal/mca/if/base/base.h"
#include "opal/mca/memcpy/base/base.h"
#include "opal/mca/memory/base/base.h"
#include "opal/mca/timer/base/base.h"

#include "opal/threads/mutex.h"
#include "opal/threads/threads.h"
#include "opal/mca/crs/base/base.h"

/******************
 * Global Var Decls
 ******************/
#if OPAL_ENABLE_CRDEBUG == 1
static opal_thread_t **opal_cr_debug_free_threads = NULL;
static int opal_cr_debug_num_free_threads = 0;
static int opal_cr_debug_threads_already_waiting = false;

int MPIR_debug_with_checkpoint = 0;
static volatile int MPIR_checkpoint_debug_gate = 0;

int    opal_cr_debug_signal     = 0;
#endif

bool opal_cr_stall_check       = false;
bool opal_cr_currently_stalled = false;
int  opal_cr_output = -1;
int  opal_cr_verbose = 0;
int opal_cr_initalized = 0;

static double opal_cr_get_time(void);
static void display_indv_timer_core(double diff, char *str);
static double timer_start[OPAL_CR_TIMER_MAX];
bool opal_cr_timing_barrier_enabled = false;
bool opal_cr_timing_enabled = false;
int  opal_cr_timing_my_rank = 0;
int  opal_cr_timing_target_rank = 0;

/******************
 * Local Functions & Var Decls
 ******************/
static int extract_env_vars(int prev_pid, char * file_name);

static void opal_cr_sigpipe_debug_signal_handler (int signo);

static opal_cr_user_inc_callback_fn_t cur_user_coord_callback[OPAL_CR_INC_MAX] = {NULL};
static opal_cr_coord_callback_fn_t  cur_coord_callback = NULL;
static opal_cr_notify_callback_fn_t cur_notify_callback = NULL;

static int core_prev_pid = 0;

/******************
 * Interface Functions & Vars
 ******************/
char * opal_cr_pipe_dir   = NULL;
int    opal_cr_entry_point_signal     = 0;
bool   opal_cr_is_enabled = true;
bool   opal_cr_is_tool    = false;

/* Current checkpoint state */
int    opal_cr_checkpointing_state = OPAL_CR_STATUS_NONE;

/* Current checkpoint request channel state */
int    opal_cr_checkpoint_request  = OPAL_CR_STATUS_NONE;

static bool   opal_cr_debug_sigpipe = false;

bool opal_cr_continue_like_restart = false;

#if OPAL_ENABLE_FT_THREAD == 1
/*****************
 * Threading Functions and Variables
 *****************/
static void* opal_cr_thread_fn(opal_object_t *obj);
bool    opal_cr_thread_is_done    = false;
bool    opal_cr_thread_is_active  = false;
bool    opal_cr_thread_in_library = false;
bool    opal_cr_thread_use_if_avail = true;
int32_t opal_cr_thread_num_in_library = 0;
int     opal_cr_thread_sleep_check = 0;
int     opal_cr_thread_sleep_wait = 0;
opal_thread_t opal_cr_thread;
opal_mutex_t  opal_cr_thread_lock;
#if 0
#define OPAL_CR_LOCK()           opal_cr_thread_in_library = true;  opal_mutex_lock(&opal_cr_thread_lock);
#define OPAL_CR_UNLOCK()         opal_cr_thread_in_library = false; opal_mutex_unlock(&opal_cr_thread_lock);
#define OPAL_CR_THREAD_LOCK()    opal_mutex_lock(&opal_cr_thread_lock);
#define OPAL_CR_THREAD_UNLOCK()  opal_mutex_unlock(&opal_cr_thread_lock);
#else
/* This technique will potentially starve the thread, but that is OK since
 * it is only there as support for when the process is not in the MPI library
 */
static const uint32_t ThreadFlag = 0x1;
static const uint32_t ProcInc    = 0x2;

#define OPAL_CR_LOCK()                                            \
 {                                                                \
    opal_cr_thread_in_library = true;                             \
    OPAL_THREAD_ADD_FETCH32(&opal_cr_thread_num_in_library, ProcInc);   \
    while( (opal_cr_thread_num_in_library & ThreadFlag ) != 0 ) { \
      sched_yield();                                              \
    }                                                             \
 }
#define OPAL_CR_UNLOCK()                                         \
 {                                                               \
    OPAL_THREAD_ADD_FETCH32(&opal_cr_thread_num_in_library, -ProcInc); \
    if( opal_cr_thread_num_in_library <= 0 ) {                   \
      opal_cr_thread_in_library = false;                         \
    }                                                            \
 }
#define OPAL_CR_THREAD_LOCK()                                           \
    {                                                                   \
      int32_t _tmp_value = 0;                                           \
      while(!OPAL_ATOMIC_COMPARE_EXCHANGE_STRONG_32 (&opal_cr_thread_num_in_library, &_tmp_value, ThreadFlag)) { \
          if( !opal_cr_thread_is_active && opal_cr_thread_is_done) {    \
              break;                                                    \
          }                                                             \
          sched_yield();                                                \
          usleep(opal_cr_thread_sleep_check);                           \
      }                                                                 \
 }
#define OPAL_CR_THREAD_UNLOCK()                                     \
 {                                                                  \
    OPAL_THREAD_ADD_FETCH32(&opal_cr_thread_num_in_library, -ThreadFlag); \
 }
#endif

#endif /* OPAL_ENABLE_FT_THREAD == 1 */

int opal_cr_set_enabled(bool en)
{
    opal_cr_is_enabled = en;
    return OPAL_SUCCESS;
}

static int opal_cr_register (void)
{
    int ret;
#if OPAL_ENABLE_CRDEBUG == 1
    int t;
#endif

    /*
     * Some startup MCA parameters
     */
    ret = mca_base_var_register ("opal", "opal", "cr", "verbose",
                                 "Verbose output level for the runtime OPAL Checkpoint/Restart functionality",
                                 MCA_BASE_VAR_TYPE_INT, NULL, 0, MCA_BASE_VAR_FLAG_SETTABLE,
                                 OPAL_INFO_LVL_8, MCA_BASE_VAR_SCOPE_LOCAL,
                                 &opal_cr_verbose);
    if (0 > ret) {
        return ret;
    }

    opal_cr_is_enabled = false;
    (void) mca_base_var_register("opal", "ft", "cr", "enabled",
                                 "Enable fault tolerance for this program",
                                 MCA_BASE_VAR_TYPE_BOOL, NULL, 0, MCA_BASE_VAR_FLAG_SETTABLE,
                                 OPAL_INFO_LVL_8, MCA_BASE_VAR_SCOPE_ALL_EQ,
                                 &opal_cr_is_enabled);

    opal_cr_timing_enabled = false;
    (void) mca_base_var_register ("opal", "opal", "cr", "enable_timer",
                                  "Enable Checkpoint timer (Default: Disabled)",
                                  MCA_BASE_VAR_TYPE_BOOL, NULL, 0, MCA_BASE_VAR_FLAG_SETTABLE,
                                  OPAL_INFO_LVL_8, MCA_BASE_VAR_SCOPE_ALL_EQ,
                                  &opal_cr_timing_enabled);

    opal_cr_timing_barrier_enabled = false;
    (void) mca_base_var_register ("opal", "opal", "cr", "enable_timer_barrier",
                                  "Enable Checkpoint timer Barrier. Must have opal_cr_enable_timer set. (Default: Disabled)",
                                  MCA_BASE_VAR_TYPE_BOOL, NULL, 0, opal_cr_timing_enabled ? MCA_BASE_VAR_FLAG_SETTABLE : 0,
                                  OPAL_INFO_LVL_8, MCA_BASE_VAR_SCOPE_ALL_EQ,
                                  &opal_cr_timing_barrier_enabled);
    opal_cr_timing_barrier_enabled = opal_cr_timing_barrier_enabled && opal_cr_timing_enabled;

    (void) mca_base_var_register ("opal", "opal", "cr", "timer_target_rank",
                                  "Target Rank for the timer (Default: 0)",
                                  MCA_BASE_VAR_TYPE_INT, NULL, 0, MCA_BASE_VAR_FLAG_SETTABLE,
                                  OPAL_INFO_LVL_8, MCA_BASE_VAR_SCOPE_ALL_EQ,
                                  &opal_cr_timing_target_rank);

#if OPAL_ENABLE_FT_THREAD == 1
    opal_cr_thread_use_if_avail = false;
    (void) mca_base_var_register ("opal", "opal", "cr", "use_thread",
                                  "Use an async thread to checkpoint this program (Default: Disabled)",
                                  MCA_BASE_VAR_TYPE_BOOL, NULL, 0, MCA_BASE_VAR_FLAG_SETTABLE,
                                  OPAL_INFO_LVL_8, MCA_BASE_VAR_SCOPE_ALL_EQ,
                                  &opal_cr_thread_use_if_avail);

    opal_cr_thread_sleep_check = 0;
    (void) mca_base_var_register ("opal", "opal", "cr", "thread_sleep_check",
                                  "Time to sleep between checking for a checkpoint (Default: 0)",
                                  MCA_BASE_VAR_TYPE_INT, NULL, 0, MCA_BASE_VAR_FLAG_SETTABLE,
                                  OPAL_INFO_LVL_8, MCA_BASE_VAR_SCOPE_ALL_EQ,
                                  &opal_cr_thread_sleep_check);

    opal_cr_thread_sleep_wait = 100;
    (void) mca_base_var_register ("opal", "opal", "cr", "thread_sleep_wait",
                                  "Time to sleep waiting for process to exit MPI library (Default: 1000)",
                                  MCA_BASE_VAR_TYPE_INT, NULL, 0, MCA_BASE_VAR_FLAG_SETTABLE,
                                  OPAL_INFO_LVL_8, MCA_BASE_VAR_SCOPE_ALL_EQ,
                                  &opal_cr_thread_sleep_wait);
#endif

    opal_cr_is_tool = false;
    (void) mca_base_var_register ("opal", "opal", "cr", "is_tool",
                                  "Is this a tool program, meaning does it require a fully operational OPAL or just enough to exec.",
                                  MCA_BASE_VAR_TYPE_BOOL, NULL, 0, MCA_BASE_VAR_FLAG_SETTABLE,
                                  OPAL_INFO_LVL_8, MCA_BASE_VAR_SCOPE_ALL_EQ,
                                  &opal_cr_is_tool);

#ifndef __WINDOWS__
    opal_cr_entry_point_signal = SIGUSR1;
    (void) mca_base_var_register ("opal", "opal", "cr", "signal",
                                  "Checkpoint/Restart signal used to initialize an OPAL Only checkpoint of a program",
                                  MCA_BASE_VAR_TYPE_INT, NULL, 0, MCA_BASE_VAR_FLAG_SETTABLE,
                                  OPAL_INFO_LVL_8, MCA_BASE_VAR_SCOPE_ALL_EQ,
                                  &opal_cr_entry_point_signal);

    opal_cr_debug_sigpipe = false;
    (void) mca_base_var_register ("opal", "opal", "cr", "debug_sigpipe",
                                  "Activate a signal handler for debugging SIGPIPE Errors that can happen on restart. (Default: Disabled)",
                                  MCA_BASE_VAR_TYPE_BOOL, NULL, 0, MCA_BASE_VAR_FLAG_SETTABLE,
                                  OPAL_INFO_LVL_8, MCA_BASE_VAR_SCOPE_ALL_EQ,
                                  &opal_cr_debug_sigpipe);
#else
    opal_cr_is_tool = true;  /* no support for CR on Windows yet */
#endif  /* __WINDOWS__ */

#if OPAL_ENABLE_CRDEBUG == 1
    MPIR_debug_with_checkpoint = 0;
    (void) mca_base_var_register ("opal", "opal", "cr", "enable_crdebug",
                                  "Enable checkpoint/restart debugging",
                                  MCA_BASE_VAR_TYPE_INT, NULL, 0, MCA_BASE_VAR_FLAG_SETTABLE,
                                  OPAL_INFO_LVL_8, MCA_BASE_VAR_SCOPE_ALL_EQ,
                                  &MPIR_debug_with_checkpoint);

    opal_cr_debug_num_free_threads = 3;
    opal_cr_debug_free_threads = (opal_thread_t **)malloc(sizeof(opal_thread_t *) * opal_cr_debug_num_free_threads );
    for(t = 0; t < opal_cr_debug_num_free_threads; ++t ) {
        opal_cr_debug_free_threads[t] = NULL;
    }

    opal_cr_debug_signal = SIGTSTP;
    (void) mca_base_var_register ("opal", "opal", "cr", "crdebug_signal",
                                  "Checkpoint/Restart signal used to hold threads when debugging",
                                  MCA_BASE_VAR_TYPE_INT, NULL, 0, MCA_BASE_VAR_FLAG_SETTABLE,
                                  OPAL_INFO_LVL_8, MCA_BASE_VAR_SCOPE_ALL_EQ,
                                  &opal_cr_debug_signal);
#endif

    opal_cr_pipe_dir = (char *) opal_tmp_directory();
    (void) mca_base_var_register ("opal", "opal", "cr", "tmp_dir",
                                  "Temporary directory to place rendezvous files for a checkpoint",
                                  MCA_BASE_VAR_TYPE_STRING, NULL, 0, MCA_BASE_VAR_FLAG_SETTABLE,
                                  OPAL_INFO_LVL_8, MCA_BASE_VAR_SCOPE_ALL_EQ,
                                  &opal_cr_pipe_dir);

    return OPAL_SUCCESS;
}


int opal_cr_init(void )
{
    int ret, exit_status = OPAL_SUCCESS;
    opal_cr_coord_callback_fn_t prev_coord_func;

    if( ++opal_cr_initalized != 1 ) {
        if( opal_cr_initalized < 1 ) {
            exit_status = OPAL_ERROR;
            goto cleanup;
        }
        exit_status = OPAL_SUCCESS;
        goto cleanup;
    }

    ret = opal_cr_register ();
    if (OPAL_SUCCESS != ret) {
        return ret;
    }

    if(0 != opal_cr_verbose) {
        opal_cr_output = opal_output_open(NULL);
        opal_output_set_verbosity(opal_cr_output, opal_cr_verbose);
    }

    opal_output_verbose(10, opal_cr_output,
                        "opal_cr: init: Verbose Level: %d",
                        opal_cr_verbose);


    opal_output_verbose(10, opal_cr_output,
                        "opal_cr: init: FT Enabled: %s",
                        opal_cr_is_enabled ? "true" : "false");


    opal_output_verbose(10, opal_cr_output,
                        "opal_cr: init: Is a tool program: %s",
                        opal_cr_is_tool ? "true" : "false");

    opal_output_verbose(10, opal_cr_output,
                        "opal_cr: init: Debug SIGPIPE: %d (%s)",
                        opal_cr_verbose, (opal_cr_debug_sigpipe ? "True" : "False"));

    opal_output_verbose(10, opal_cr_output,
                        "opal_cr: init: Checkpoint Signal: %d",
                        opal_cr_entry_point_signal);

#if OPAL_ENABLE_FT_THREAD == 1
    opal_output_verbose(10, opal_cr_output,
                        "opal_cr: init: FT Use thread: %s",
                        opal_cr_thread_use_if_avail ? "true" : "false");

    opal_output_verbose(10, opal_cr_output,
                        "opal_cr: init: FT thread sleep: check = %d, wait = %d",
                        opal_cr_thread_sleep_check, opal_cr_thread_sleep_wait);

    /* If we have a thread, then attach the SIGPIPE signal handler there since
     * it is most likely to be the one that needs it.
     */
    if( opal_cr_debug_sigpipe && !opal_cr_thread_use_if_avail ) {
        if( SIG_ERR == signal(SIGPIPE, opal_cr_sigpipe_debug_signal_handler) ) {
            ;
        }
    }
#else
    if( opal_cr_debug_sigpipe ) {
        if( SIG_ERR == signal(SIGPIPE, opal_cr_sigpipe_debug_signal_handler) ) {
            ;
        }
    }
#endif

#if OPAL_ENABLE_CRDEBUG == 1
    opal_output_verbose(10, opal_cr_output,
                        "opal_cr: init: C/R Debugging Enabled [%s]\n",
                        (MPIR_debug_with_checkpoint ? "True": "False"));

    opal_output_verbose(10, opal_cr_output,
                        "opal_cr: init: Checkpoint Signal (Debug): %d",
                        opal_cr_debug_signal);

    if( SIG_ERR == signal(opal_cr_debug_signal, MPIR_checkpoint_debugger_signal_handler) ) {
        opal_output(opal_cr_output,
                    "opal_cr: init: Failed to register C/R debug signal (%d)",
                    opal_cr_debug_signal);
    }
#endif

    opal_output_verbose(10, opal_cr_output,
                        "opal_cr: init: Temp Directory: %s",
                        opal_cr_pipe_dir);

    if( !opal_cr_is_tool ) {
        /* Register the OPAL interlevel coordination callback */
        opal_cr_reg_coord_callback(opal_cr_coord, &prev_coord_func);

        opal_cr_stall_check = false;
        opal_cr_currently_stalled = false;

    } /* End opal_cr_is_tool = true */

    /*
     * If fault tolerance was not compiled in then
     * we need to make sure that the listener thread is active to tell
     * the tools that this is not a checkpointable job.
     * We don't need the CRS framework to be initalized.
     */
#if OPAL_ENABLE_FT_CR    == 1
    /*
     * Open the checkpoint / restart service components
     */
    if (OPAL_SUCCESS != (ret = mca_base_framework_open(&opal_crs_base_framework, 0))) {
        opal_show_help( "help-opal-runtime.txt",
                        "opal_cr_init:no-crs", true,
                        "opal_crs_base_open", ret );
        exit_status = ret;
        goto cleanup;
    }

    if (OPAL_SUCCESS != (ret = opal_crs_base_select())) {
        opal_show_help( "help-opal-runtime.txt",
                        "opal_cr_init:no-crs", true,
                        "opal_crs_base_select", ret );
        exit_status = ret;
        goto cleanup;
    }
#endif

#if OPAL_ENABLE_FT_THREAD == 1
    if( !opal_cr_is_tool && opal_cr_thread_use_if_avail) {
        opal_output_verbose(10, opal_cr_output,
                            "opal_cr: init: starting the thread\n");

        /* JJH: We really do need this line below since it enables
         *      actual locks for threads. However currently the
         *      upper layers will deadlock if it is enabled.
         *      So hack around the problem for now, while working
         *      on a complete solution. See ticket #2741 for more
         *      details.
         * opal_set_using_threads(true);
         */

        /*
         * Start the thread
         */
        OBJ_CONSTRUCT(&opal_cr_thread,     opal_thread_t);
        OBJ_CONSTRUCT(&opal_cr_thread_lock, opal_mutex_t);

        opal_cr_thread_is_done    = false;
        opal_cr_thread_is_active  = false;
        opal_cr_thread_in_library = false;
        opal_cr_thread_num_in_library = 0;

        opal_cr_thread.t_run = opal_cr_thread_fn;
        opal_cr_thread.t_arg = NULL;
        opal_thread_start(&opal_cr_thread);

    } /* End opal_cr_is_tool = true */
    else {
        opal_output_verbose(10, opal_cr_output,
                            "opal_cr: init: *Not* Using C/R thread\n");
    }
#endif /* OPAL_ENABLE_FT_THREAD == 1 */

 cleanup:
    return exit_status;
}

int opal_cr_finalize(void)
{
    int exit_status = OPAL_SUCCESS;

    if( --opal_cr_initalized != 0 ) {
        if( opal_cr_initalized < 0 ) {
            return OPAL_ERROR;
        }
        return OPAL_SUCCESS;
    }

    if( !opal_cr_is_tool ) {
#if OPAL_ENABLE_FT_THREAD == 1
        if( opal_cr_thread_use_if_avail ) {
            void *data;
            /*
             * Stop the thread
             */
            opal_cr_thread_is_done    = true;
            opal_cr_thread_is_active  = false;
            opal_cr_thread_in_library = true;

            opal_thread_join(&opal_cr_thread, &data);
            OBJ_DESTRUCT(&opal_cr_thread);
            OBJ_DESTRUCT(&opal_cr_thread_lock);
        }
#endif /* OPAL_ENABLE_FT_THREAD == 1 */

        /* Nothing to do for just process notifications */
        opal_cr_checkpointing_state = OPAL_CR_STATUS_TERM;
        opal_cr_checkpoint_request  = OPAL_CR_STATUS_TERM;
    }

#if OPAL_ENABLE_CRDEBUG == 1
    if( NULL != opal_cr_debug_free_threads ) {
        free( opal_cr_debug_free_threads );
        opal_cr_debug_free_threads = NULL;
    }
    opal_cr_debug_num_free_threads = 0;
#endif

    if (NULL != opal_cr_pipe_dir) {
        free(opal_cr_pipe_dir);
        opal_cr_pipe_dir = NULL;
    }

#if OPAL_ENABLE_FT_CR    == 1
    /*
     * Close the checkpoint / restart service components
     */
    (void) mca_base_framework_close(&opal_crs_base_framework);
#endif

    return exit_status;
}

/*
 * Check if a checkpoint request needs to be operated upon
 */
void opal_cr_test_if_checkpoint_ready(void)
{
    int ret;

    if( opal_cr_currently_stalled) {
        opal_output_verbose(20, opal_cr_output,
                            "opal_cr:opal_test_if_ready: JUMPING to Post Stall stage");
        goto STAGE_1;
    }

    /*
     * If there is no checkpoint request to act on
     * then just return
     */
    if(OPAL_CR_STATUS_REQUESTED != opal_cr_checkpoint_request ) {
        return;
    }

    /*
     * If we are currently checkpointing:
     *  - If a request is pending then cancel it
     *  - o.w., skip it.
     */
    if(OPAL_CR_STATUS_RUNNING == opal_cr_checkpointing_state ) {
        if( OPAL_SUCCESS != (ret = cur_notify_callback(OPAL_CHECKPOINT_CMD_IN_PROGRESS) ) ) {
            opal_output(opal_cr_output,
                        "Error: opal_cr: test_if_checkpoint_ready: Respond [In Progress] Failed. (%d)",
                        ret);
        }
        opal_cr_checkpoint_request = OPAL_CR_STATUS_NONE;
        return;
    }

    /*
     * If no CRS module is loaded return an error
     */
    if (NULL == opal_crs.crs_checkpoint ) {
         if( OPAL_SUCCESS != (ret = cur_notify_callback(OPAL_CHECKPOINT_CMD_NULL) ) ) {
             opal_output(opal_cr_output,
                         "Error: opal_cr: test_if_checkpoint_ready: Respond [Not Able/NULL] Failed. (%d)",
                         ret);
         }
         opal_cr_checkpoint_request = OPAL_CR_STATUS_NONE;
         return;
    }

    /*
     * Start the checkpoint
     */
    opal_cr_checkpointing_state = OPAL_CR_STATUS_RUNNING;
    opal_cr_checkpoint_request  = OPAL_CR_STATUS_NONE;

 STAGE_1:
    if( OPAL_SUCCESS != (ret = cur_notify_callback(OPAL_CHECKPOINT_CMD_START) ) ) {
        opal_output(opal_cr_output,
                    "Error: opal_cr: test_if_checkpoint_ready: Respond [Start Ckpt] Failed. (%d)",
                    ret);
    }

    return;
}

/*******************************
 * Notification Routines
 *******************************/
int opal_cr_inc_core_prep(void)
{
    int ret;

    /*
     * Call User Level INC
     */
    if(OPAL_SUCCESS != (ret = ompi_trigger_user_inc_callback(OPAL_CR_INC_PRE_CRS_PRE_MPI,
                                                        OPAL_CR_INC_STATE_PREPARE)) ) {
        return ret;
    }

    /*
     * Use the registered coordination routine
     */
    if(OPAL_SUCCESS != (ret = cur_coord_callback(OPAL_CRS_CHECKPOINT)) ) {
        if ( OPAL_EXISTS != ret ) {
            opal_output(opal_cr_output,
                        "opal_cr: inc_core: Error: cur_coord_callback(%d) failed! %d\n",
                        OPAL_CRS_CHECKPOINT, ret);
        }
        return ret;
    }

    /*
     * Call User Level INC
     */
    if(OPAL_SUCCESS != (ret = ompi_trigger_user_inc_callback(OPAL_CR_INC_PRE_CRS_POST_MPI,
                                                        OPAL_CR_INC_STATE_PREPARE)) ) {
        return ret;
    }

    core_prev_pid = getpid();

    return OPAL_SUCCESS;
}

int opal_cr_inc_core_ckpt(pid_t pid,
                          opal_crs_base_snapshot_t *snapshot,
                          opal_crs_base_ckpt_options_t *options,
                          int *state)
{
    int ret, exit_status = OPAL_SUCCESS;

    OPAL_CR_SET_TIMER(OPAL_CR_TIMER_CORE0);
    if(OPAL_SUCCESS != (ret = opal_crs.crs_checkpoint(pid,
                                                      snapshot,
                                                      options,
                                                      (opal_crs_state_type_t *)state))) {
        opal_output(opal_cr_output,
                    "opal_cr: inc_core: Error: The checkpoint failed. %d\n", ret);
        exit_status = ret;
    }

    if(*state == OPAL_CRS_CONTINUE) {
        OPAL_CR_SET_TIMER(OPAL_CR_TIMER_CORE1);

        if(options->term) {
            *state = OPAL_CRS_TERM;
            opal_cr_checkpointing_state  = OPAL_CR_STATUS_TERM;
        } else {
            opal_cr_checkpointing_state  = OPAL_CR_STATUS_CONTINUE;
        }
    }
    else {
        options->term = false;
    }

    /*
     * If restarting read environment stuff that opal-restart left us.
     */
    if(*state == OPAL_CRS_RESTART) {
        opal_cr_refresh_environ(core_prev_pid);
        opal_cr_checkpointing_state  = OPAL_CR_STATUS_RESTART_PRE;
    }

    return exit_status;
}

int opal_cr_inc_core_recover(int state)
{
    int ret;
    opal_cr_user_inc_callback_state_t cb_state;

    if( opal_cr_checkpointing_state != OPAL_CR_STATUS_TERM &&
        opal_cr_checkpointing_state != OPAL_CR_STATUS_CONTINUE &&
        opal_cr_checkpointing_state != OPAL_CR_STATUS_RESTART_PRE &&
        opal_cr_checkpointing_state != OPAL_CR_STATUS_RESTART_POST ) {

        if(state == OPAL_CRS_CONTINUE) {
            OPAL_CR_SET_TIMER(OPAL_CR_TIMER_CORE1);
            opal_cr_checkpointing_state  = OPAL_CR_STATUS_CONTINUE;
        }
        /*
         * If restarting read environment stuff that opal-restart left us.
         */
        else if(state == OPAL_CRS_RESTART) {
            opal_cr_refresh_environ(core_prev_pid);
            opal_cr_checkpointing_state  = OPAL_CR_STATUS_RESTART_PRE;
        }
    }

    /*
     * Call User Level INC
     */
    if( OPAL_CRS_CONTINUE == state ) {
        cb_state = OPAL_CR_INC_STATE_CONTINUE;
    }
    else if( OPAL_CRS_RESTART == state ) {
        cb_state = OPAL_CR_INC_STATE_RESTART;
    }
    else {
        cb_state = OPAL_CR_INC_STATE_ERROR;
    }

    if(OPAL_SUCCESS != (ret = ompi_trigger_user_inc_callback(OPAL_CR_INC_POST_CRS_PRE_MPI,
                                                        cb_state)) ) {
        return ret;
    }

    /*
     * Use the registered coordination routine
     */
    if(OPAL_SUCCESS != (ret = cur_coord_callback(state)) ) {
        if ( OPAL_EXISTS != ret ) {
            opal_output(opal_cr_output,
                        "opal_cr: inc_core: Error: cur_coord_callback(%d) failed! %d\n",
                        state, ret);
        }
        return ret;
    }

    if(OPAL_SUCCESS != (ret = ompi_trigger_user_inc_callback(OPAL_CR_INC_POST_CRS_POST_MPI,
                                                        cb_state)) ) {
        return ret;
    }

#if OPAL_ENABLE_CRDEBUG == 1
    opal_cr_debug_clear_current_ckpt_thread();
#endif

    return OPAL_SUCCESS;
}

int opal_cr_inc_core(pid_t pid,
                     opal_crs_base_snapshot_t *snapshot,
                     opal_crs_base_ckpt_options_t *options,
                     int *state)
{
    int ret, exit_status = OPAL_SUCCESS;

    /*
     * INC: Prepare stack using the registered coordination routine
     */
    if(OPAL_SUCCESS != (ret = opal_cr_inc_core_prep() ) ) {
        return ret;
    }

    /*
     * INC: Take the checkpoint
     */
    if(OPAL_SUCCESS != (ret = opal_cr_inc_core_ckpt(pid, snapshot, options, state) ) ) {
        exit_status = ret;
        /* Don't return here since we want to restart the OPAL level stuff */
    }

    /*
     * INC: Recover stack using the registered coordination routine
     */
    if(OPAL_SUCCESS != (ret = opal_cr_inc_core_recover(*state) ) ) {
        return ret;
    }

    return exit_status;
}

/*******************************
 * Coordination Routines
 *******************************/
/**
 * Current Coordination callback routines
 */
int opal_cr_coord(int state)
{
    if(OPAL_CRS_CHECKPOINT == state) {
        /* Do Checkpoint Phase work */
    }
    else if (OPAL_CRS_CONTINUE == state ) {
        /* Do Continue Phase work */
    }
    else if (OPAL_CRS_RESTART == state ) {
        /* Do Restart Phase work */

        /*
         * Re-initialize the event engine
         * Otherwise it may/will use stale file descriptors which will disrupt
         * the intended users of the soon-to-be newly assigned file descriptors.
         */
        opal_event_reinit(opal_sync_event_base);

        /*
         * Flush if() functionality, since it caches system specific info.
         */
        (void) mca_base_framework_close(&opal_if_base_framework);
        /* Since opal_ifinit() is not exposed, the necessary
         * functions will call it when needed. Just make sure we
         * finalized this code so we don't get old socket addrs.
         */
        opal_output_reopen_all();
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
     * Here we are returning to either:
     *  - [orte | ompi]_notify()
     */
    opal_cr_checkpointing_state  = OPAL_CR_STATUS_RESTART_POST;

    return OPAL_SUCCESS;
}

int opal_cr_reg_notify_callback(opal_cr_notify_callback_fn_t  new_func,
                                opal_cr_notify_callback_fn_t *prev_func)
{
    /*
     * Preserve the previous callback
     */
    if( NULL != cur_notify_callback) {
        *prev_func = cur_notify_callback;
    }
    else {
        *prev_func = NULL;
    }

    /*
     * Update the callbacks
     */
    cur_notify_callback     = new_func;

    return OPAL_SUCCESS;
}

int opal_cr_user_inc_register_callback(opal_cr_user_inc_callback_event_t event,
                                       opal_cr_user_inc_callback_fn_t  function,
                                       opal_cr_user_inc_callback_fn_t  *prev_function)
{
    if (event >= OPAL_CR_INC_MAX) {
        return OPAL_ERROR;
    }

    if( NULL != cur_user_coord_callback[event] ) {
        *prev_function = cur_user_coord_callback[event];
    } else {
        *prev_function = NULL;
    }

    cur_user_coord_callback[event] = function;

    return OPAL_SUCCESS;
}

int ompi_trigger_user_inc_callback(opal_cr_user_inc_callback_event_t event,
                              opal_cr_user_inc_callback_state_t state)
{
    if( NULL == cur_user_coord_callback[event] ) {
        return OPAL_SUCCESS;
    }

    if (event >= OPAL_CR_INC_MAX) {
        return OPAL_ERROR;
    }

    return ((cur_user_coord_callback[event])(event, state));
}

int opal_cr_reg_coord_callback(opal_cr_coord_callback_fn_t  new_func,
                               opal_cr_coord_callback_fn_t *prev_func)
{
    /*
     * Preserve the previous callback
     */
    if( NULL != cur_coord_callback) {
        *prev_func = cur_coord_callback;
    }
    else {
        *prev_func = NULL;
    }

    /*
     * Update the callbacks
     */
    cur_coord_callback     = new_func;

    return OPAL_SUCCESS;
}

int opal_cr_refresh_environ(int prev_pid) {
    char *file_name;
#if OPAL_ENABLE_CRDEBUG == 1
    char *tmp;
#endif
    struct stat file_status;

    if( 0 >= prev_pid ) {
        prev_pid = getpid();
    }

    /*
     * Make sure the file exists. If it doesn't then this means 2 things:
     *  1) We have already executed this function, and
     *  2) The file has been deleted on the previous round.
     */
    asprintf(&file_name, "%s/%s-%d", opal_tmp_directory(), OPAL_CR_BASE_ENV_NAME, prev_pid);
    if (NULL == file_name) {
        return OPAL_ERR_OUT_OF_RESOURCE;
    }
    if(0 != stat(file_name, &file_status) ){
        free(file_name);
        return OPAL_SUCCESS;
    }

#if OPAL_ENABLE_CRDEBUG == 1
    mca_base_var_env_name ("opal_cr_enable_crdebug", &tmp);
    opal_unsetenv(tmp, &environ);
    free (tmp);
#endif

    extract_env_vars(prev_pid, file_name);

#if OPAL_ENABLE_CRDEBUG == 1
    MPIR_debug_with_checkpoint = 0;
    (void) mca_base_var_register ("opal", "opal", "cr", "enable_crdebug",
                                  "Enable checkpoint/restart debugging",
                                  MCA_BASE_VAR_TYPE_INT, NULL, 0, MCA_BASE_VAR_FLAG_SETTABLE,
                                  OPAL_INFO_LVL_8, MCA_BASE_VAR_SCOPE_ALL_EQ,
                                  &MPIR_debug_with_checkpoint);

    opal_output_verbose(10, opal_cr_output,
                        "opal_cr: init: C/R Debugging Enabled [%s] (refresh)\n",
                        (MPIR_debug_with_checkpoint ? "True": "False"));
#endif

    free(file_name);

    return OPAL_SUCCESS;
}

/*
 * Extract environment variables from a saved file
 * and place them in the environment.
 */
static int extract_env_vars(int prev_pid, char * file_name)
{
    int exit_status = OPAL_SUCCESS;
    FILE *env_data = NULL;
    int len = OPAL_PATH_MAX;
    char * tmp_str = NULL;

    if( 0 >= prev_pid ) {
        opal_output(opal_cr_output,
                    "opal_cr: extract_env_vars: Invalid PID (%d)\n",
                    prev_pid);
        exit_status = OPAL_ERROR;
        goto cleanup;
    }

    if (NULL == (env_data = fopen(file_name, "r")) ) {
        exit_status = OPAL_ERROR;
        goto cleanup;
    }

    tmp_str = (char *) malloc(sizeof(char) * OPAL_PATH_MAX);
    if( NULL == tmp_str) {
        exit_status = OPAL_ERR_OUT_OF_RESOURCE;
        goto cleanup;
    }
    /* Extract an env var */
    while(!feof(env_data) ) {
        char **t_set = NULL;

        if( NULL == fgets(tmp_str, OPAL_PATH_MAX, env_data) ) {
            exit_status = OPAL_ERROR;
            goto cleanup;
        }
        len = strlen(tmp_str);
        if(tmp_str[len - 1] == '\n') {
            tmp_str[len - 1] = '\0';
        } else {
            opal_output(opal_cr_output,
                        "opal_cr: extract_env_vars: Error: Parameter too long (%s)\n",
                        tmp_str);
            continue;
        }

        if( NULL == (t_set = opal_argv_split(tmp_str, '=')) ) {
            break;
        }

        opal_setenv(t_set[0], t_set[1], true, &environ);

        opal_argv_free(t_set);
    }

 cleanup:
    if( NULL != env_data ) {
        fclose(env_data);
    }
    unlink(file_name);

    if( NULL != tmp_str ){
        free(tmp_str);
    }

    return exit_status;
}

/*****************************************
 * OPAL CR Entry Point Functionality
*****************************************/
/*
 * Used only for debugging SIGPIPE problems
 */
static void opal_cr_sigpipe_debug_signal_handler (int signo)
{
    int sleeper = 1;

    if( !opal_cr_debug_sigpipe ) {
        opal_output_verbose(10, opal_cr_output,
                            "opal_cr: sigpipe_debug: Debug SIGPIPE Not enabled :(\n");
        return;
    }

    opal_output(0,
                "opal_cr: sigpipe_debug: Debug SIGPIPE [%d]: PID (%d)\n",
                signo, getpid());
    while(sleeper == 1 ) {
        sleep(1);
    }
}

#if OPAL_ENABLE_FT_THREAD == 1
static void* opal_cr_thread_fn(opal_object_t *obj)
{
    /* Sanity Check */
    if( !opal_cr_thread_use_if_avail ) {
        return NULL;
    }

    if( opal_cr_debug_sigpipe ) {
        if( SIG_ERR == signal(SIGPIPE, opal_cr_sigpipe_debug_signal_handler) ) {
            ;
        }
    }

    /*
     * Register this thread with the OPAL CRS
     */
    if( NULL != opal_crs.crs_reg_thread ) {
        if( OPAL_SUCCESS != opal_crs.crs_reg_thread() ) {
            opal_output(0, "Error: Thread registration failed\n");
            return NULL;
        }
    }

#if OPAL_ENABLE_CRDEBUG == 1
    opal_cr_debug_free_threads[1] = opal_thread_get_self();
#endif

    /*
     * Wait to become active
     */
    while( !opal_cr_thread_is_active && !opal_cr_thread_is_done) {
        sched_yield();
    }

    if( opal_cr_thread_is_done ) {
        return NULL;
    }

    /*
     * While active
     */
    while( opal_cr_thread_is_active && !opal_cr_thread_is_done) {
        /*
         * While no threads are in the MPI library then try to process
         * checkpoint requests.
         */
        OPAL_CR_THREAD_LOCK();

        while ( !opal_cr_thread_in_library ) {
            sched_yield();
            usleep(opal_cr_thread_sleep_check);

            OPAL_CR_TEST_CHECKPOINT_READY();
            /* Sanity check */
            if( OPAL_UNLIKELY(opal_cr_currently_stalled) ) {
                OPAL_CR_TEST_CHECKPOINT_READY();
            }
        }

        /*
         * While they are in the MPI library yield
         */
        OPAL_CR_THREAD_UNLOCK();

        while ( opal_cr_thread_in_library && opal_cr_thread_is_active ) {
            usleep(opal_cr_thread_sleep_wait);
        }
    }

    return NULL;
}

void opal_cr_thread_init_library(void)
{
    if( !opal_cr_thread_use_if_avail ) {
        OPAL_CR_TEST_CHECKPOINT_READY();
    } else {
        /* Activate the CR Thread */
        opal_cr_thread_in_library = false;
        opal_cr_thread_is_done    = false;
        opal_cr_thread_is_active  = true;
    }
}

void opal_cr_thread_finalize_library(void)
{
    if( !opal_cr_thread_use_if_avail ) {
        OPAL_CR_TEST_CHECKPOINT_READY();
    } else {
        /* Deactivate the CR Thread */
        opal_cr_thread_is_done    = true;
        opal_cr_thread_is_active  = false;
        OPAL_CR_LOCK();
        opal_cr_thread_in_library = true;
    }
}

void opal_cr_thread_abort_library(void)
{
    if( !opal_cr_thread_use_if_avail ) {
        OPAL_CR_TEST_CHECKPOINT_READY();
    } else {
        /* Deactivate the CR Thread */
        opal_cr_thread_is_done    = true;
        opal_cr_thread_is_active  = false;
        OPAL_CR_LOCK();
        opal_cr_thread_in_library = true;
    }
}

void opal_cr_thread_enter_library(void)
{
    if( !opal_cr_thread_use_if_avail ) {
        OPAL_CR_TEST_CHECKPOINT_READY();
    } else {
        /* Lock out the CR Thread */
        OPAL_CR_LOCK();
    }
}

void opal_cr_thread_exit_library(void)
{
    if( !opal_cr_thread_use_if_avail ) {
        OPAL_CR_TEST_CHECKPOINT_READY();
    } else {
        /* Allow CR Thread to continue */
        OPAL_CR_UNLOCK();
    }
}

void opal_cr_thread_noop_progress(void)
{
    if( !opal_cr_thread_use_if_avail ) {
        OPAL_CR_TEST_CHECKPOINT_READY();
    }
}

#endif /* OPAL_ENABLE_FT_THREAD == 1 */

static double opal_cr_get_time() {
    double wtime;

#if OPAL_TIMER_USEC_NATIVE
    wtime = (double)opal_timer_base_get_usec() / 1000000.0;
#else
    struct timeval tv;
    gettimeofday(&tv, NULL);
    wtime = tv.tv_sec;
    wtime += (double)tv.tv_usec / 1000000.0;
#endif

    return wtime;
}

void opal_cr_set_time(int idx)
{
    if(idx < OPAL_CR_TIMER_MAX ) {
        if( timer_start[idx] <= 0.0 ) {
            timer_start[idx] = opal_cr_get_time();
        }
    }
}

void opal_cr_clear_timers(void)
{
    int i;
    for(i = 0; i < OPAL_CR_TIMER_MAX; ++i) {
        timer_start[i] = 0.0;
    }
}

static void display_indv_timer_core(double diff, char *str) {
    double total = 0;
    double perc  = 0;

    total = timer_start[OPAL_CR_TIMER_MAX-1] - timer_start[OPAL_CR_TIMER_ENTRY0];
    perc = (diff/total) * 100;

    opal_output(0,
                "opal_cr: timing: %-20s = %10.2f s\t%10.2f s\t%6.2f\n",
                str,
                diff,
                total,
                perc);
    return;
}

void opal_cr_display_all_timers(void)
{
    double diff = 0.0;
    char * label = NULL;

    if( opal_cr_timing_target_rank != opal_cr_timing_my_rank ) {
        return;
    }

    opal_output(0, "OPAL CR Timing: ******************** Summary Begin\n");

    /********** Entry into the system **********/
    label = strdup("Start Entry Point");
    if( opal_cr_timing_barrier_enabled ) {
        diff = timer_start[OPAL_CR_TIMER_CRCPBR0] - timer_start[OPAL_CR_TIMER_ENTRY0];
    } else {
        diff = timer_start[OPAL_CR_TIMER_CRCP0]   - timer_start[OPAL_CR_TIMER_ENTRY0];
    }
    display_indv_timer_core(diff, label);
    free(label);

    /********** CRCP Protocol **********/
    label = strdup("CRCP Protocol");
    if( opal_cr_timing_barrier_enabled ) {
        diff = timer_start[OPAL_CR_TIMER_CRCPBR1] - timer_start[OPAL_CR_TIMER_CRCP0];
    } else {
        diff = timer_start[OPAL_CR_TIMER_P2P0]    - timer_start[OPAL_CR_TIMER_CRCP0];
    }
    display_indv_timer_core(diff, label);
    free(label);

    /********** P2P Suspend **********/
    label = strdup("P2P Suspend");
    if( opal_cr_timing_barrier_enabled ) {
        diff = timer_start[OPAL_CR_TIMER_P2PBR0]     - timer_start[OPAL_CR_TIMER_P2P0];
    } else {
        diff = timer_start[OPAL_CR_TIMER_CORE0]     - timer_start[OPAL_CR_TIMER_P2P0];
    }
    display_indv_timer_core(diff, label);
    free(label);

    /********** Checkpoint to Disk  **********/
    label = strdup("Checkpoint");
    diff = timer_start[OPAL_CR_TIMER_CORE1]    - timer_start[OPAL_CR_TIMER_CORE0];
    display_indv_timer_core(diff, label);
    free(label);

    /********** P2P Reactivation **********/
    label = strdup("P2P Reactivation");
    if( opal_cr_timing_barrier_enabled ) {
        diff = timer_start[OPAL_CR_TIMER_P2PBR2] - timer_start[OPAL_CR_TIMER_CORE1];
    } else {
        diff = timer_start[OPAL_CR_TIMER_CRCP1]  - timer_start[OPAL_CR_TIMER_CORE1];
    }
    display_indv_timer_core(diff, label);
    free(label);

    /********** CRCP Protocol Finalize **********/
    label = strdup("CRCP Cleanup");
    if( opal_cr_timing_barrier_enabled ) {
        diff = timer_start[OPAL_CR_TIMER_COREBR1] - timer_start[OPAL_CR_TIMER_CRCP1];
    } else {
        diff = timer_start[OPAL_CR_TIMER_CORE2]   - timer_start[OPAL_CR_TIMER_CRCP1];
    }
    display_indv_timer_core(diff, label);
    free(label);

    /********** Exit the system **********/
    label = strdup("Finish Entry Point");
    diff = timer_start[OPAL_CR_TIMER_ENTRY4] - timer_start[OPAL_CR_TIMER_CORE2];
    display_indv_timer_core(diff, label);
    free(label);

    opal_output(0, "OPAL CR Timing: ******************** Summary End\n");
}

#if OPAL_ENABLE_CRDEBUG == 1
int opal_cr_debug_set_current_ckpt_thread_self(void)
{
    int t;

    if( NULL == opal_cr_debug_free_threads ) {
        opal_cr_debug_num_free_threads = 3;
        opal_cr_debug_free_threads = (opal_thread_t **)malloc(sizeof(opal_thread_t *) * opal_cr_debug_num_free_threads );
        for(t = 0; t < opal_cr_debug_num_free_threads; ++t ) {
            opal_cr_debug_free_threads[t] = NULL;
        }
    }

    opal_cr_debug_free_threads[0] = opal_thread_get_self();

    return OPAL_SUCCESS;
}

int opal_cr_debug_clear_current_ckpt_thread(void)
{
    opal_cr_debug_free_threads[0] = NULL;

    return OPAL_SUCCESS;
}

int MPIR_checkpoint_debugger_detach(void) {
    /* This function is meant to be a noop function for checkpoint/restart
     * enabled debugging functionality */
#if 0
    /* Once the debugger can successfully force threads into the function below,
     * then we can uncomment this line */
    if( MPIR_debug_with_checkpoint ) {
        opal_cr_debug_threads_already_waiting = true;
    }
#endif
    return OPAL_SUCCESS;
}

void MPIR_checkpoint_debugger_signal_handler(int signo)
{
    opal_output_verbose(1, opal_cr_output,
                        "crs: MPIR_checkpoint_debugger_signal_handler(): Enter Debug signal handler...");

    MPIR_checkpoint_debugger_waitpoint();

    opal_output_verbose(1, opal_cr_output,
                        "crs: MPIR_checkpoint_debugger_signal_handler(): Leave Debug signal handler...");
}

void *MPIR_checkpoint_debugger_waitpoint(void)
{
    int t;
    opal_thread_t *thr = NULL;

    thr = opal_thread_get_self();

    /*
     * Sanity check, if the debugger is not going to attach, then do not wait
     * Make sure to open the debug gate, so that threads can get out
     */
    if( !MPIR_debug_with_checkpoint ) {
        opal_output_verbose(1, opal_cr_output,
                            "crs: MPIR_checkpoint_debugger_waitpoint(): Debugger is not attaching... (%d)",
                            (int)thr->t_handle);
        MPIR_checkpoint_debug_gate = 1;
        return NULL;
    }
    else {
        opal_output_verbose(1, opal_cr_output,
                            "crs: MPIR_checkpoint_debugger_waitpoint(): Waiting for the Debugger to attach... (%d)",
                            (int)thr->t_handle);
        MPIR_checkpoint_debug_gate = 0;
    }

    /*
     * Let special threads escape without waiting, they will wait later
     */
    for(t = 0; t < opal_cr_debug_num_free_threads; ++t) {
        if( opal_cr_debug_free_threads[t] != NULL &&
            opal_thread_self_compare(opal_cr_debug_free_threads[t]) ) {
            opal_output_verbose(1, opal_cr_output,
                                "crs: MPIR_checkpoint_debugger_waitpoint(): Checkpointing thread does not wait here... (%d)",
                                (int)thr->t_handle);
            return NULL;
        }
    }

    /*
     * Force all other threads into the waiting function,
     * unless they are already in there, then just return so we do not nest
     * calls into this wait function and potentially confuse the debugger.
     */
    if( opal_cr_debug_threads_already_waiting ) {
        opal_output_verbose(1, opal_cr_output,
                            "crs: MPIR_checkpoint_debugger_waitpoint(): Threads are already waiting from debugger detach, do not wait here... (%d)",
                            (int)thr->t_handle);
        return NULL;
    } else {
        opal_output_verbose(1, opal_cr_output,
                            "crs: MPIR_checkpoint_debugger_waitpoint(): Wait... (%d)",
                            (int)thr->t_handle);
        return MPIR_checkpoint_debugger_breakpoint();
    }
}

/*
 * A tight loop to wait for debugger to release this process from the
 * breakpoint.
 */
void *MPIR_checkpoint_debugger_breakpoint(void)
{
    /* spin until debugger attaches and releases us */
    while (MPIR_checkpoint_debug_gate == 0) {
#if defined(HAVE_USLEEP)
        usleep(100000); /* microseconds */
#else
        sleep(1);       /* seconds */
#endif
    }
    opal_cr_debug_threads_already_waiting = false;
    return NULL;
}
#endif
