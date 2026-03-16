/*
 * Copyright (c) 2004-2010 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2008 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2011-2012 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2014-2018 Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef ORTE_PLM_TYPES_H
#define ORTE_PLM_TYPES_H

#include "orte_config.h"
#include "orte/types.h"



BEGIN_C_DECLS

/*
 * Process exit codes
 */

typedef int32_t orte_exit_code_t;
#define ORTE_EXIT_CODE_T OPAL_INT32

/*
 * Process state codes
 */

typedef uint32_t orte_proc_state_t;
#define ORTE_PROC_STATE_T   OPAL_UINT32
#define ORTE_PROC_STATE_ANY 0xffff

#define ORTE_PROC_STATE_UNDEF                    0  /* undefined process state */
#define ORTE_PROC_STATE_INIT                     1  /* process entry has been created by rmaps */
#define ORTE_PROC_STATE_RESTART                  2  /* the proc is ready for restart */
#define ORTE_PROC_STATE_TERMINATE                3  /* process is marked for termination */
#define ORTE_PROC_STATE_RUNNING                  4  /* daemon has locally fork'd process */
#define ORTE_PROC_STATE_REGISTERED               5  /* proc registered sync */
#define ORTE_PROC_STATE_IOF_COMPLETE             6  /* io forwarding pipes have closed */
#define ORTE_PROC_STATE_WAITPID_FIRED            7  /* waitpid fired on process */
#define ORTE_PROC_STATE_MODEX_READY              8  /* all modex info has been stored */
/*
 * Define a "boundary" so we can easily and quickly determine
 * if a proc is still running or not - any value less than
 * this one means that we are not terminated
 */
#define ORTE_PROC_STATE_UNTERMINATED            15

#define ORTE_PROC_STATE_TERMINATED              20  /* process has terminated and is no longer running */
/* Define a boundary so we can easily and quickly determine
 * if a proc abnormally terminated - leave a little room
 * for future expansion
 */
#define ORTE_PROC_STATE_ERROR                   50
/* Define specific error code values */
#define ORTE_PROC_STATE_KILLED_BY_CMD           (ORTE_PROC_STATE_ERROR +  1)  /* process was killed by ORTE cmd */
#define ORTE_PROC_STATE_ABORTED                 (ORTE_PROC_STATE_ERROR +  2)  /* process aborted */
#define ORTE_PROC_STATE_FAILED_TO_START         (ORTE_PROC_STATE_ERROR +  3)  /* process failed to start */
#define ORTE_PROC_STATE_ABORTED_BY_SIG          (ORTE_PROC_STATE_ERROR +  4)  /* process aborted by signal */
#define ORTE_PROC_STATE_TERM_WO_SYNC            (ORTE_PROC_STATE_ERROR +  5)  /* process exit'd w/o required sync */
#define ORTE_PROC_STATE_COMM_FAILED             (ORTE_PROC_STATE_ERROR +  6)  /* process communication has failed */
#define ORTE_PROC_STATE_SENSOR_BOUND_EXCEEDED   (ORTE_PROC_STATE_ERROR +  7)  /* process exceeded a sensor limit */
#define ORTE_PROC_STATE_CALLED_ABORT            (ORTE_PROC_STATE_ERROR +  8)  /* process called "errmgr.abort" */
#define ORTE_PROC_STATE_HEARTBEAT_FAILED        (ORTE_PROC_STATE_ERROR +  9)  /* heartbeat failed to arrive */
#define ORTE_PROC_STATE_MIGRATING               (ORTE_PROC_STATE_ERROR + 10)  /* process failed and is waiting for resources before restarting */
#define ORTE_PROC_STATE_CANNOT_RESTART          (ORTE_PROC_STATE_ERROR + 11)  /* process failed and cannot be restarted */
#define ORTE_PROC_STATE_TERM_NON_ZERO           (ORTE_PROC_STATE_ERROR + 12)  /* process exited with a non-zero status, indicating abnormal */
#define ORTE_PROC_STATE_FAILED_TO_LAUNCH        (ORTE_PROC_STATE_ERROR + 13)  /* unable to launch process */
#define ORTE_PROC_STATE_UNABLE_TO_SEND_MSG      (ORTE_PROC_STATE_ERROR + 14)  /* unable to send a message */
#define ORTE_PROC_STATE_LIFELINE_LOST           (ORTE_PROC_STATE_ERROR + 15)  /* connection to lifeline lost */
#define ORTE_PROC_STATE_NO_PATH_TO_TARGET       (ORTE_PROC_STATE_ERROR + 16)  /* no path for communicating to target peer */
#define ORTE_PROC_STATE_FAILED_TO_CONNECT       (ORTE_PROC_STATE_ERROR + 17)  /* unable to connect to target peer */
#define ORTE_PROC_STATE_PEER_UNKNOWN            (ORTE_PROC_STATE_ERROR + 18)  /* unknown peer */

/* Define a boundary so that external developers
 * have a starting point for defining their own
 * proc states
 */
#define ORTE_PROC_STATE_DYNAMIC 100

/*
 * App_context state codes
 */
typedef int32_t orte_app_state_t;
#define ORTE_APP_STATE_T    OPAL_INT32

#define ORTE_APP_STATE_UNDEF                0
#define ORTE_APP_STATE_INIT                 1
#define ORTE_APP_STATE_ALL_MAPPED           2
#define ORTE_APP_STATE_RUNNING              3
#define ORTE_APP_STATE_COMPLETED            4

/*
 * Job state codes
 */

typedef int32_t orte_job_state_t;
#define ORTE_JOB_STATE_T    OPAL_INT32
#define ORTE_JOB_STATE_ANY  INT_MAX

#define ORTE_JOB_STATE_UNDEF                     0
#define ORTE_JOB_STATE_INIT                      1  /* ready to be assigned id */
#define ORTE_JOB_STATE_INIT_COMPLETE             2  /* jobid assigned and setup */
#define ORTE_JOB_STATE_ALLOCATE                  3  /* ready to be allocated */
#define ORTE_JOB_STATE_ALLOCATION_COMPLETE       4  /* allocation completed */
#define ORTE_JOB_STATE_MAP                       5  /* ready to be mapped */
#define ORTE_JOB_STATE_MAP_COMPLETE              6  /* mapping complete */
#define ORTE_JOB_STATE_SYSTEM_PREP               7  /* ready for final sanity check and system values updated */
#define ORTE_JOB_STATE_LAUNCH_DAEMONS            8  /* ready to launch daemons */
#define ORTE_JOB_STATE_DAEMONS_LAUNCHED          9  /* daemons for this job have been launched */
#define ORTE_JOB_STATE_DAEMONS_REPORTED         10  /* all launched daemons have reported */
#define ORTE_JOB_STATE_VM_READY                 11  /* the VM is ready for operation */
#define ORTE_JOB_STATE_LAUNCH_APPS              12  /* ready to launch apps */
#define ORTE_JOB_STATE_SEND_LAUNCH_MSG          13  /* send launch msg to daemons */
#define ORTE_JOB_STATE_RUNNING                  14  /* all procs have been fork'd */
#define ORTE_JOB_STATE_SUSPENDED                15  /* job has been suspended */
#define ORTE_JOB_STATE_REGISTERED               16  /* all procs registered for sync */
#define ORTE_JOB_STATE_READY_FOR_DEBUGGERS      17  /* job ready for debugger init after spawn */
#define ORTE_JOB_STATE_LOCAL_LAUNCH_COMPLETE    18  /* all local procs have attempted launch */
#define ORTE_JOB_STATE_DEBUGGER_DETACH          19  /* a debugger has detached */

/*
 * Define a "boundary" so we can easily and quickly determine
 * if a job is still running or not - any value less than
 * this one means that we are not terminated
 */
#define ORTE_JOB_STATE_UNTERMINATED             30

#define ORTE_JOB_STATE_TERMINATED               31  /* all processes have terminated and job is no longer running */
#define ORTE_JOB_STATE_ALL_JOBS_COMPLETE        32
#define ORTE_JOB_STATE_DAEMONS_TERMINATED       33
#define ORTE_JOB_STATE_NOTIFY_COMPLETED         34  /* callback to notify when job completes */
#define ORTE_JOB_STATE_NOTIFIED                 35

/* Define a boundary so we can easily and quickly determine
 * if a job abnormally terminated - leave a little room
 * for future expansion
 */
#define ORTE_JOB_STATE_ERROR                   50
/* Define specific error code values */
#define ORTE_JOB_STATE_KILLED_BY_CMD           (ORTE_JOB_STATE_ERROR +  1)  /* job was killed by ORTE cmd */
#define ORTE_JOB_STATE_ABORTED                 (ORTE_JOB_STATE_ERROR +  2)  /* at least one process aborted, causing job to abort */
#define ORTE_JOB_STATE_FAILED_TO_START         (ORTE_JOB_STATE_ERROR +  3)  /* at least one process failed to start */
#define ORTE_JOB_STATE_ABORTED_BY_SIG          (ORTE_JOB_STATE_ERROR +  4)  /* job was killed by a signal */
#define ORTE_JOB_STATE_ABORTED_WO_SYNC         (ORTE_JOB_STATE_ERROR +  5)  /* job was aborted because proc exit'd w/o required sync */
#define ORTE_JOB_STATE_COMM_FAILED             (ORTE_JOB_STATE_ERROR +  6)  /* communication has failed */
#define ORTE_JOB_STATE_SENSOR_BOUND_EXCEEDED   (ORTE_JOB_STATE_ERROR +  7)  /* job had a process that exceeded a sensor limit */
#define ORTE_JOB_STATE_CALLED_ABORT            (ORTE_JOB_STATE_ERROR +  8)  /* at least one process called "errmgr.abort" */
#define ORTE_JOB_STATE_HEARTBEAT_FAILED        (ORTE_JOB_STATE_ERROR +  9)  /* heartbeat failed to arrive */
#define ORTE_JOB_STATE_NEVER_LAUNCHED          (ORTE_JOB_STATE_ERROR + 10)  /* the job never even attempted to launch due to
                                                                             * an error earlier in the
                                                                             * launch procedure
                                                                             */
#define ORTE_JOB_STATE_ABORT_ORDERED           (ORTE_JOB_STATE_ERROR + 11)  /* the processes in this job have been ordered to "die",
                                                                             * but may not have completed it yet. Don't order it again
                                                                             */
#define ORTE_JOB_STATE_NON_ZERO_TERM           (ORTE_JOB_STATE_ERROR + 12)  /* at least one process exited with non-zero status */
#define ORTE_JOB_STATE_FAILED_TO_LAUNCH        (ORTE_JOB_STATE_ERROR + 13)
#define ORTE_JOB_STATE_FORCED_EXIT             (ORTE_JOB_STATE_ERROR + 14)
#define ORTE_JOB_STATE_SILENT_ABORT            (ORTE_JOB_STATE_ERROR + 16)  /* an error occurred and was reported elsewhere, so error out quietly */

#define ORTE_JOB_STATE_REPORT_PROGRESS         (ORTE_JOB_STATE_ERROR + 17)  /* report launch progress - not an error */
#define ORTE_JOB_STATE_ALLOC_FAILED            (ORTE_JOB_STATE_ERROR + 18)  /* job failed to obtain an allocation */
#define ORTE_JOB_STATE_MAP_FAILED              (ORTE_JOB_STATE_ERROR + 19)  /* job failed to map */
#define ORTE_JOB_STATE_CANNOT_LAUNCH           (ORTE_JOB_STATE_ERROR + 20)  /* resources were busy and so the job cannot be launched */

/* define an FT event */
#define ORTE_JOB_STATE_FT_CHECKPOINT           (ORTE_JOB_STATE_ERROR + 21)
#define ORTE_JOB_STATE_FT_CONTINUE             (ORTE_JOB_STATE_ERROR + 22)
#define ORTE_JOB_STATE_FT_RESTART              (ORTE_JOB_STATE_ERROR + 23)


/* Define a boundary so that external developers
 * have a starting point for defining their own
 * job states
 */
#define ORTE_JOB_STATE_DYNAMIC 100


/**
 * Node State, corresponding to the ORTE_NODE_STATE_* #defines,
 * below.  These are #defines instead of an enum because the thought
 * is that we may have lots and lots of entries of these in the
 * registry and by making this an int8_t, it's only 1 byte, whereas an
 * enum defaults to an int (probably 4 bytes).  So it's a bit of a
 * space savings.
 */
typedef int8_t orte_node_state_t;
#define ORTE_NODE_STATE_T OPAL_INT8

#define ORTE_NODE_STATE_UNDEF         0  // Node is undefined
#define ORTE_NODE_STATE_UNKNOWN       1  // Node is defined but in an unknown state
#define ORTE_NODE_STATE_DOWN          2  // Node is down
#define ORTE_NODE_STATE_UP            3  // Node is up / available for use
#define ORTE_NODE_STATE_REBOOT        4  // Node is rebooting
#define ORTE_NODE_STATE_DO_NOT_USE    5  // Node is up, but not available for use for the next mapping
#define ORTE_NODE_STATE_NOT_INCLUDED  6  // Node is up, but not part of the node pool for jobs
#define ORTE_NODE_STATE_ADDED         7  // Node was dynamically added to pool

/* Define a boundary so that external developers
 * have a starting point for defining their own
 * node states
 */
#define ORTE_NODE_STATE_DYNAMIC 100

/*
 * PLM commands
 */
typedef uint8_t orte_plm_cmd_flag_t;
#define ORTE_PLM_CMD    OPAL_UINT8
#define ORTE_PLM_LAUNCH_JOB_CMD         1
#define ORTE_PLM_UPDATE_PROC_STATE      2
#define ORTE_PLM_REGISTERED_CMD         3

END_C_DECLS

#endif
