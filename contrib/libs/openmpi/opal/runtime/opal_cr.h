/*
 * Copyright (c) 2004-2010 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2007 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2008      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2017      IBM Corporation. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/**
 * @file
 *
 * Checkpoint functionality for Open MPI
 */

#include "opal_config.h"
#include "opal/mca/crs/crs.h"
#include "opal/mca/event/event.h"
#include "opal/util/output.h"
#include "opal/prefetch.h"

#ifndef OPAL_CR_H
#define OPAL_CR_H


BEGIN_C_DECLS

/*
 * Some defines shared with opal-[checkpoint|restart] commands
 */
#define OPAL_CR_DONE       ((char) 0)
#define OPAL_CR_ACK        ((char) 1)
#define OPAL_CR_CHECKPOINT ((char) 2)
#define OPAL_CR_NAMED_PROG_R  ("opal_cr_prog_read")
#define OPAL_CR_NAMED_PROG_W  ("opal_cr_prog_write")
#define OPAL_CR_BASE_ENV_NAME ("opal_cr_restart-env")

/*
 * Possible responses to a checkpoint request from opal-checkpoint
 */
enum opal_cr_ckpt_cmd_state_t {
    OPAL_CHECKPOINT_CMD_START,       /* Checkpoint is starting on this request */
    OPAL_CHECKPOINT_CMD_IN_PROGRESS, /* Checkpoint is currently running */
    OPAL_CHECKPOINT_CMD_NULL,        /* Checkpoint cannot be started because it is not supported */
    OPAL_CHECKPOINT_CMD_ERROR,       /* An error occurred such that the checkpoint cannot be completed */
    /* State of the checkpoint operation */
    OPAL_CR_STATUS_NONE,       /* No checkpoint in progress */
    OPAL_CR_STATUS_REQUESTED,  /* Checkpoint has been requested */
    OPAL_CR_STATUS_RUNNING,    /* Checkpoint is currently running */
    OPAL_CR_STATUS_TERM,       /* Checkpoint is running and will terminate process upon completion */
    /* State of the continue operation */
    OPAL_CR_STATUS_CONTINUE,
    /* State of the restart operation */
    OPAL_CR_STATUS_RESTART_PRE,
    OPAL_CR_STATUS_RESTART_POST
};
typedef enum opal_cr_ckpt_cmd_state_t opal_cr_ckpt_cmd_state_t;

    /* An output handle to be used by the cr runtime
     * functionality as an argument to opal_output() */
    OPAL_DECLSPEC extern int    opal_cr_output;

    /* Directory containing the named pipes for communication
     * with the opal-checkpoint tool  */
    OPAL_DECLSPEC extern char * opal_cr_pipe_dir;

    /* Signal that opal-checkpoint uses to contact the
     * application process */
    OPAL_DECLSPEC extern int    opal_cr_entry_point_signal;

    /* If Checkpointing is enabled in this application */
    OPAL_DECLSPEC extern bool   opal_cr_is_enabled;

    /* If the application running is a tool
     * (e.g., opal-checkpoint, orted, ...) */
    OPAL_DECLSPEC extern bool   opal_cr_is_tool;

    /* If a checkpoint has been requested */
    OPAL_DECLSPEC extern int opal_cr_checkpoint_request;

    /* The current state of a checkpoint operation */
    OPAL_DECLSPEC extern int opal_cr_checkpointing_state;

    /*
     * If one of the BTLs that shutdown require a full, clean rebuild of the
     * point-to-point stack on 'continue' as well as 'restart'.
     */
    OPAL_DECLSPEC extern bool opal_cr_continue_like_restart;

#if OPAL_ENABLE_CRDEBUG == 1
    /* Whether or not C/R Debugging is enabled for this process */
    OPAL_DECLSPEC extern int MPIR_debug_with_checkpoint;

    /*
     * Set/clear the current thread id for the checkpointing thread
     */
    OPAL_DECLSPEC int opal_cr_debug_set_current_ckpt_thread_self(void);
    OPAL_DECLSPEC int opal_cr_debug_clear_current_ckpt_thread(void);

    /*
     * This MPI Debugger function needs to be accessed here and have a specific
     * name. Thus we are breaking the traditional naming conventions to provide this functionality.
     */
    OPAL_DECLSPEC int MPIR_checkpoint_debugger_detach(void);

    /**
     * A tight loop to wait for debugger to release this process from the
     * breakpoint.
     */
    OPAL_DECLSPEC void *MPIR_checkpoint_debugger_breakpoint(void);

    /**
     * A function for the debugger or CRS to force all threads into
     */
    OPAL_DECLSPEC void *MPIR_checkpoint_debugger_waitpoint(void);

    /**
     * A signal handler to force all threads to wait when debugger detaches
     */
    OPAL_DECLSPEC void MPIR_checkpoint_debugger_signal_handler(int signo);
#endif

    /*
     * Refresh environment variables after a restart
     */
    OPAL_DECLSPEC int opal_cr_refresh_environ(int prev_pid);

    /*
     * If this is an application that doesn't want to have
     * a notification callback installed, set this to false.
     * To see the effect, this must be called before opal_cr_init().
     * Default: Enabled
     */
    OPAL_DECLSPEC int opal_cr_set_enabled(bool);

    /**
     * Initialize the notification and coordination
     *  elements.
     */
    OPAL_DECLSPEC int opal_cr_init(void);

    /**
     * Finalize the notification and coordination
     *  elements.
     */
    OPAL_DECLSPEC int opal_cr_finalize(void);

    /*************************************************
     * Check to see if a checkpoint has been requested
     *
     * When the checkpoint thread is disabled:
     *   This will be checked whenever the MPI Library
     *   is entered by the application. It will stop
     *   the application for the duration of the entire
     *   checkpoint.
     * When the checkpoint thread is enabled:
     *   The request is handled in the thread parallel
     *   with the execution of the program regardless
     *   of where the program is in exection.
     *   The problem with this method is that it
     *   requires the support of progress threads
     *   which is currently not working properly :/
     *
     *************************************************/
    OPAL_DECLSPEC void opal_cr_test_if_checkpoint_ready(void);

    /* If the checkpoint operation should be stalled to
     * wait for another sevice to complete before
     * continuing with the checkpoint */
    OPAL_DECLSPEC extern bool opal_cr_stall_check;
    OPAL_DECLSPEC extern bool opal_cr_currently_stalled;

#if OPAL_ENABLE_FT_THREAD == 1
    /* Some thread functions */
    OPAL_DECLSPEC void opal_cr_thread_init_library(void);
    OPAL_DECLSPEC void opal_cr_thread_finalize_library(void);
    OPAL_DECLSPEC void opal_cr_thread_abort_library(void);
    OPAL_DECLSPEC void opal_cr_thread_enter_library(void);
    OPAL_DECLSPEC void opal_cr_thread_exit_library(void);
    OPAL_DECLSPEC void opal_cr_thread_noop_progress(void);
#endif /* OPAL_ENABLE_FT_THREAD == 1 */

    /*
     * If not using FT then make the #defines noops
     */
#if OPAL_ENABLE_FT == 0 || OPAL_ENABLE_FT_CR == 0
#define OPAL_CR_TEST_CHECKPOINT_READY() ;
#define OPAL_CR_TEST_CHECKPOINT_READY_STALL() ;
#define OPAL_CR_INIT_LIBRARY() ;
#define OPAL_CR_FINALIZE_LIBRARY() ;
#define OPAL_CR_ABORT_LIBRARY() ;
#define OPAL_CR_ENTER_LIBRARY() ;
#define OPAL_CR_EXIT_LIBRARY() ;
#define OPAL_CR_NOOP_PROGRESS() ;
#endif /* #if OPAL_ENABLE_FT == 0 || OPAL_ENABLE_FT_CR == 0 */

    /*
     * If using FT
     */
#if OPAL_ENABLE_FT_CR == 1
#define OPAL_CR_TEST_CHECKPOINT_READY()      \
  {                                          \
    if(OPAL_UNLIKELY(opal_cr_is_enabled) ) { \
      opal_cr_test_if_checkpoint_ready();    \
    }                                        \
  }

#define OPAL_CR_TEST_CHECKPOINT_READY_STALL()        \
  {                                                  \
    if(OPAL_UNLIKELY(opal_cr_is_enabled && !opal_cr_stall_check)) { \
      opal_cr_test_if_checkpoint_ready();            \
    }                                                \
  }

/* If *not* using FT thread */
#if OPAL_ENABLE_FT_THREAD == 0
#define OPAL_CR_INIT_LIBRARY()     OPAL_CR_TEST_CHECKPOINT_READY();
#define OPAL_CR_FINALIZE_LIBRARY() OPAL_CR_TEST_CHECKPOINT_READY();
#define OPAL_CR_ABORT_LIBRARY()    OPAL_CR_TEST_CHECKPOINT_READY();
#define OPAL_CR_ENTER_LIBRARY()    OPAL_CR_TEST_CHECKPOINT_READY();
#define OPAL_CR_EXIT_LIBRARY()     OPAL_CR_TEST_CHECKPOINT_READY();
#define OPAL_CR_NOOP_PROGRESS()    OPAL_CR_TEST_CHECKPOINT_READY();
#endif /* OPAL_ENABLE_FT_THREAD == 0 */

/* If using FT thread */
#if OPAL_ENABLE_FT_THREAD == 1
#define OPAL_CR_INIT_LIBRARY()    \
 {                                \
   opal_cr_thread_init_library(); \
 }
#define OPAL_CR_FINALIZE_LIBRARY()    \
 {                                    \
   opal_cr_thread_finalize_library(); \
 }
#define OPAL_CR_ABORT_LIBRARY()    \
 {                                 \
   opal_cr_thread_abort_library(); \
 }
#define OPAL_CR_ENTER_LIBRARY()    \
 {                                 \
   opal_cr_thread_enter_library(); \
 }
#define OPAL_CR_EXIT_LIBRARY()    \
 {                                \
   opal_cr_thread_exit_library(); \
 }
#define OPAL_CR_NOOP_PROGRESS()    \
 {                                 \
   opal_cr_thread_noop_progress(); \
 }
#endif /* OPAL_ENABLE_FT_THREAD == 1 */

#endif /* OPAL_ENABLE_FT_CR == 1 */

    /*******************************
     * Notification Routines
     *******************************/
    /*******************************
     * Notification Routines
     *******************************/
    /**
     * A function to respond to the async checkpoint request
     * this is useful when figuring out who should respond
     * when stalling.
     */
    typedef int (*opal_cr_notify_callback_fn_t) (opal_cr_ckpt_cmd_state_t);

    OPAL_DECLSPEC int opal_cr_reg_notify_callback
    (opal_cr_notify_callback_fn_t new_func,
     opal_cr_notify_callback_fn_t *prev_func);

    /**
     * Function to go through the INC
     * - Call Registered INC_Coord(CHECKPOINT)
     * - Call the CRS.checkpoint()
     * - Call Registered INC_Coord(state)
     */
    OPAL_DECLSPEC int opal_cr_inc_core(pid_t pid,
                                       opal_crs_base_snapshot_t *snapshot,
                                       opal_crs_base_ckpt_options_t *options,
                                       int *state);

    OPAL_DECLSPEC int opal_cr_inc_core_prep(void);
    OPAL_DECLSPEC int opal_cr_inc_core_ckpt(pid_t pid,
                                            opal_crs_base_snapshot_t *snapshot,
                                            opal_crs_base_ckpt_options_t *options,
                                            int *state);
    OPAL_DECLSPEC int opal_cr_inc_core_recover(int state);


    /*******************************
     * User Coordination Routines
     *******************************/
    typedef enum {
        OPAL_CR_INC_PRE_CRS_PRE_MPI   = 0,
        OPAL_CR_INC_PRE_CRS_POST_MPI  = 1,
        OPAL_CR_INC_CRS_PRE_CKPT      = 2,
        OPAL_CR_INC_CRS_POST_CKPT     = 3,
        OPAL_CR_INC_POST_CRS_PRE_MPI  = 4,
        OPAL_CR_INC_POST_CRS_POST_MPI = 5,
        OPAL_CR_INC_MAX               = 6
    } opal_cr_user_inc_callback_event_t;

    typedef enum {
        OPAL_CR_INC_STATE_PREPARE  = 0,
        OPAL_CR_INC_STATE_CONTINUE = 1,
        OPAL_CR_INC_STATE_RESTART  = 2,
        OPAL_CR_INC_STATE_ERROR    = 3
    } opal_cr_user_inc_callback_state_t;

    /**
     * User coordination callback routine
     */
    typedef int (*opal_cr_user_inc_callback_fn_t)(opal_cr_user_inc_callback_event_t event,
                                                  opal_cr_user_inc_callback_state_t state);

    OPAL_DECLSPEC int opal_cr_user_inc_register_callback
                      (opal_cr_user_inc_callback_event_t event,
                       opal_cr_user_inc_callback_fn_t  function,
                       opal_cr_user_inc_callback_fn_t  *prev_function);

    OPAL_DECLSPEC int ompi_trigger_user_inc_callback(opal_cr_user_inc_callback_event_t event,
                                                opal_cr_user_inc_callback_state_t state);


    /*******************************
     * Coordination Routines
     *******************************/
    /**
     * Coordination callback routine signature
     */
    typedef int (*opal_cr_coord_callback_fn_t) (int);

    /**
     * Register a checkpoint coodination routine
     * for a higher level.
     */
     OPAL_DECLSPEC int opal_cr_reg_coord_callback
     (opal_cr_coord_callback_fn_t  new_func,
      opal_cr_coord_callback_fn_t *prev_func);

    /**
     * OPAL Checkpoint Coordination Routine
     */
    OPAL_DECLSPEC int opal_cr_coord(int state);

    /**
     * Checkpoint life-cycle timing
     */
    OPAL_DECLSPEC void opal_cr_set_time(int idx);
    OPAL_DECLSPEC void opal_cr_display_all_timers(void);
    OPAL_DECLSPEC void opal_cr_clear_timers(void);

    OPAL_DECLSPEC extern bool opal_cr_timing_enabled;
    OPAL_DECLSPEC extern bool opal_cr_timing_barrier_enabled;
    OPAL_DECLSPEC extern int  opal_cr_timing_my_rank;
    OPAL_DECLSPEC extern int  opal_cr_timing_target_rank;


#define OPAL_CR_TIMER_ENTRY0    0
#define OPAL_CR_TIMER_ENTRY1    1
#define OPAL_CR_TIMER_ENTRY2    2
#define OPAL_CR_TIMER_CRCPBR0   3
#define OPAL_CR_TIMER_CRCP0     4
#define OPAL_CR_TIMER_CRCPBR1   5
#define OPAL_CR_TIMER_P2P0      6
#define OPAL_CR_TIMER_P2P1      7
#define OPAL_CR_TIMER_P2PBR0    8
#define OPAL_CR_TIMER_CORE0     9
#define OPAL_CR_TIMER_CORE1    10
#define OPAL_CR_TIMER_COREBR0  11
#define OPAL_CR_TIMER_P2P2     12
#define OPAL_CR_TIMER_P2PBR1   13
#define OPAL_CR_TIMER_P2P3     14
#define OPAL_CR_TIMER_P2PBR2   15
#define OPAL_CR_TIMER_CRCP1    16
#define OPAL_CR_TIMER_COREBR1  17
#define OPAL_CR_TIMER_CORE2    18
#define OPAL_CR_TIMER_ENTRY3   19
#define OPAL_CR_TIMER_ENTRY4   20
#define OPAL_CR_TIMER_MAX      21


#define OPAL_CR_CLEAR_TIMERS()                          \
    {                                                   \
        if(OPAL_UNLIKELY(opal_cr_timing_enabled > 0)) { \
            opal_cr_clear_timers();                     \
        }                                               \
    }

#define OPAL_CR_SET_TIMER(idx)                          \
    {                                                   \
        if(OPAL_UNLIKELY(opal_cr_timing_enabled > 0)) { \
            opal_cr_set_time(idx);                      \
        }                                               \
    }

#define OPAL_CR_DISPLAY_ALL_TIMERS()                    \
    {                                                   \
        if(OPAL_UNLIKELY(opal_cr_timing_enabled > 0)) { \
            opal_cr_display_all_timers();               \
        }                                               \
    }

END_C_DECLS

#endif /* OPAL_CR_H */

