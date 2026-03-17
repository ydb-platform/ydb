/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2010-2016 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2011-2013 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2014-2018 Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/** @file **/

#include "orte_config.h"
#include "orte/constants.h"

#include <stdio.h>
#ifdef HAVE_SYS_SIGNAL_H
#include <sys/signal.h>
#else
#include <signal.h>
#endif

#include "orte/mca/plm/plm_types.h"
#include "orte/util/error_strings.h"
#include "orte/runtime/orte_globals.h"

int orte_err2str(int errnum, const char **errmsg)
{
    const char *retval;
    switch (errnum) {
    case ORTE_SUCCESS:
        retval = "Success";
        break;
    case ORTE_ERR_RECV_LESS_THAN_POSTED:
        retval = "Receive was less than posted size";
        break;
    case ORTE_ERR_RECV_MORE_THAN_POSTED:
        retval = "Receive was greater than posted size";
        break;
    case ORTE_ERR_NO_MATCH_YET:
        retval = "No match for receive posted";
        break;
    case ORTE_ERR_REQUEST:
        retval = "Request error";
        break;
    case ORTE_ERR_NO_CONNECTION_ALLOWED:
        retval = "No connection allowed";
        break;
    case ORTE_ERR_CONNECTION_REFUSED:
        retval = "Connection refused";
        break;
    case ORTE_ERR_TYPE_MISMATCH:
        retval = "Type mismatch";
        break;
    case ORTE_ERR_COMPARE_FAILURE:
        retval = "Data comparison failure";
        break;
    case ORTE_ERR_COPY_FAILURE:
        retval = "Data copy failure";
        break;
    case ORTE_ERR_PROC_STATE_MISSING:
        retval = "The process state information is missing on the registry";
        break;
    case ORTE_ERR_PROC_EXIT_STATUS_MISSING:
        retval = "The process exit status is missing on the registry";
        break;
    case ORTE_ERR_INDETERMINATE_STATE_INFO:
        retval = "Request for state returned multiple responses";
        break;
    case ORTE_ERR_NODE_FULLY_USED:
        retval = "All the slots on a given node have been used";
        break;
    case ORTE_ERR_INVALID_NUM_PROCS:
        retval = "Multiple applications were specified, but at least one failed to specify the number of processes to run";
        break;
    case ORTE_ERR_SILENT:
        if (orte_report_silent_errors) {
            retval = "Silent error";
        } else {
            retval = "";
        }
        break;
    case ORTE_ERR_ADDRESSEE_UNKNOWN:
        retval = "A message is attempting to be sent to a process whose contact information is unknown";
        break;
    case ORTE_ERR_SYS_LIMITS_PIPES:
        retval = "The system limit on number of pipes a process can open was reached";
        break;
    case ORTE_ERR_PIPE_SETUP_FAILURE:
        retval = "A pipe could not be setup between a daemon and one of its local processes";
        break;
    case ORTE_ERR_SYS_LIMITS_CHILDREN:
        retval = "The system limit on number of children a process can have was reached";
        break;
    case ORTE_ERR_FAILED_GET_TERM_ATTRS:
        retval = "The I/O forwarding system was unable to get the attributes of your terminal";
        break;
    case ORTE_ERR_WDIR_NOT_FOUND:
        retval = "The specified working directory could not be found";
        break;
    case ORTE_ERR_EXE_NOT_FOUND:
        retval = "The specified executable could not be found";
        break;
    case ORTE_ERR_PIPE_READ_FAILURE:
        retval = "A pipe could not be read";
        break;
    case ORTE_ERR_EXE_NOT_ACCESSIBLE:
        retval = "The specified executable could not be executed";
        break;
    case ORTE_ERR_FAILED_TO_START:
        retval = "The specified application failed to start";
        break;
    case ORTE_ERR_FILE_NOT_EXECUTABLE:
        retval = "A system-required executable either could not be found or was not executable by this user";
        break;
    case ORTE_ERR_HNP_COULD_NOT_START:
        retval = "Unable to start a daemon on the local node";
        break;
    case ORTE_ERR_SYS_LIMITS_SOCKETS:
        retval = "The system limit on number of network connections a process can open was reached";
        break;
    case ORTE_ERR_SOCKET_NOT_AVAILABLE:
        retval = "Unable to open a TCP socket for out-of-band communications";
        break;
    case ORTE_ERR_SYSTEM_WILL_BOOTSTRAP:
        retval = "System will determine resources during bootstrap of daemons";
        break;
    case ORTE_ERR_RESTART_LIMIT_EXCEEDED:
        retval = "Limit on number of process restarts was exceeded";
        break;
    case ORTE_ERR_INVALID_NODE_RANK:
        retval = "Invalid node rank";
        break;
    case ORTE_ERR_INVALID_LOCAL_RANK:
        retval = "Invalid local rank";
        break;
    case ORTE_ERR_UNRECOVERABLE:
        retval = "Unrecoverable error";
        break;
    case ORTE_ERR_MEM_LIMIT_EXCEEDED:
        retval = "Memory limit exceeded";
        break;
    case ORTE_ERR_HEARTBEAT_LOST:
        retval = "Heartbeat lost";
        break;
    case ORTE_ERR_PROC_STALLED:
        retval = "Proc appears to be stalled";
        break;
    case ORTE_ERR_NO_APP_SPECIFIED:
        retval = "No application specified";
        break;
    case ORTE_ERR_NO_EXE_SPECIFIED:
        retval = "No executable specified";
        break;
    case ORTE_ERR_COMM_DISABLED:
        retval = "Communications have been disabled";
        break;
    case ORTE_ERR_FAILED_TO_MAP:
        retval = "Unable to map job";
        break;
    case ORTE_ERR_TAKE_NEXT_OPTION:
        if (orte_report_silent_errors) {
            retval = "Next option";
        } else {
            retval = "";
        }
        break;
    case ORTE_ERR_SENSOR_LIMIT_EXCEEDED:
        retval = "Sensor limit exceeded";
        break;
    case ORTE_ERR_PROC_ENTRY_NOT_FOUND:
        retval = "Proc entry not found";
        break;
    case ORTE_ERR_DATA_VALUE_NOT_FOUND:
        retval = "Data not found";
        break;
    case ORTE_ERR_ALLOCATION_PENDING:
        retval = "Allocation pending";
        break;
    case ORTE_ERR_NO_PATH_TO_TARGET:
        retval = "No OOB path to target";
        break;
    case ORTE_ERR_OP_IN_PROGRESS:
        retval = "Operation in progress";
        break;
    case ORTE_ERR_OPEN_CONDUIT_FAIL:
        retval = "Open messaging conduit failed";
        break;
    case ORTE_ERR_OUT_OF_ORDER_MSG:
        retval = "Out of order message";
        break;
    case ORTE_ERR_FORCE_SELECT:
        retval = "Force select";
        break;
    case ORTE_ERR_JOB_CANCELLED:
        retval = "Job cancelled";
        break;
    case ORTE_ERR_CONDUIT_SEND_FAIL:
        retval = " Transport Conduit returned send error";
        break;
    case ORTE_ERR_DEBUGGER_RELEASE:
        retval = "Debugger release";
        break;
    case ORTE_ERR_PARTIAL_SUCCESS:
        retval = "Partial success";
        break;
    default:
        retval = "Unknown error";
    }

    *errmsg = retval;
    return ORTE_SUCCESS;
}

const char *orte_job_state_to_str(orte_job_state_t state)
{
    switch(state) {
    case ORTE_JOB_STATE_UNDEF:
        return "UNDEFINED";
    case ORTE_JOB_STATE_INIT:
        return "PENDING INIT";
    case ORTE_JOB_STATE_INIT_COMPLETE:
        return "INIT_COMPLETE";
    case ORTE_JOB_STATE_ALLOCATE:
        return "PENDING ALLOCATION";
    case ORTE_JOB_STATE_ALLOCATION_COMPLETE:
        return "ALLOCATION COMPLETE";
    case ORTE_JOB_STATE_MAP:
        return "PENDING MAPPING";
    case ORTE_JOB_STATE_MAP_COMPLETE:
        return "MAP COMPLETE";
    case ORTE_JOB_STATE_SYSTEM_PREP:
        return "PENDING FINAL SYSTEM PREP";
    case ORTE_JOB_STATE_LAUNCH_DAEMONS:
        return "PENDING DAEMON LAUNCH";
    case ORTE_JOB_STATE_DAEMONS_LAUNCHED:
        return "DAEMONS LAUNCHED";
    case ORTE_JOB_STATE_DAEMONS_REPORTED:
        return "ALL DAEMONS REPORTED";
    case ORTE_JOB_STATE_VM_READY:
        return "VM READY";
    case ORTE_JOB_STATE_LAUNCH_APPS:
        return "PENDING APP LAUNCH";
    case ORTE_JOB_STATE_SEND_LAUNCH_MSG:
        return "SENDING LAUNCH MSG";
    case ORTE_JOB_STATE_RUNNING:
        return "RUNNING";
    case ORTE_JOB_STATE_SUSPENDED:
        return "SUSPENDED";
    case ORTE_JOB_STATE_REGISTERED:
        return "SYNC REGISTERED";
    case ORTE_JOB_STATE_READY_FOR_DEBUGGERS:
        return "READY FOR DEBUGGERS";
    case ORTE_JOB_STATE_LOCAL_LAUNCH_COMPLETE:
        return "LOCAL LAUNCH COMPLETE";
    case ORTE_JOB_STATE_UNTERMINATED:
        return "UNTERMINATED";
    case ORTE_JOB_STATE_TERMINATED:
        return "NORMALLY TERMINATED";
    case ORTE_JOB_STATE_NOTIFY_COMPLETED:
        return "NOTIFY COMPLETED";
    case ORTE_JOB_STATE_NOTIFIED:
        return "NOTIFIED";
    case ORTE_JOB_STATE_ALL_JOBS_COMPLETE:
        return "ALL JOBS COMPLETE";
    case ORTE_JOB_STATE_ERROR:
        return "ARTIFICIAL BOUNDARY - ERROR";
    case ORTE_JOB_STATE_KILLED_BY_CMD:
        return "KILLED BY INTERNAL COMMAND";
    case ORTE_JOB_STATE_ABORTED:
        return "ABORTED";
    case ORTE_JOB_STATE_FAILED_TO_START:
        return "FAILED TO START";
    case ORTE_JOB_STATE_ABORTED_BY_SIG:
        return "ABORTED BY SIGNAL";
    case ORTE_JOB_STATE_ABORTED_WO_SYNC:
        return "TERMINATED WITHOUT SYNC";
    case ORTE_JOB_STATE_COMM_FAILED:
        return "COMMUNICATION FAILURE";
    case ORTE_JOB_STATE_SENSOR_BOUND_EXCEEDED:
        return "SENSOR BOUND EXCEEDED";
    case ORTE_JOB_STATE_CALLED_ABORT:
        return "PROC CALLED ABORT";
    case ORTE_JOB_STATE_HEARTBEAT_FAILED:
        return "HEARTBEAT FAILED";
    case ORTE_JOB_STATE_NEVER_LAUNCHED:
        return "NEVER LAUNCHED";
    case ORTE_JOB_STATE_ABORT_ORDERED:
        return "ABORT IN PROGRESS";
    case ORTE_JOB_STATE_NON_ZERO_TERM:
        return "AT LEAST ONE PROCESS EXITED WITH NON-ZERO STATUS";
    case ORTE_JOB_STATE_FAILED_TO_LAUNCH:
        return "FAILED TO LAUNCH";
    case ORTE_JOB_STATE_FORCED_EXIT:
        return "FORCED EXIT";
    case ORTE_JOB_STATE_DAEMONS_TERMINATED:
        return "DAEMONS TERMINATED";
    case ORTE_JOB_STATE_SILENT_ABORT:
        return "ERROR REPORTED ELSEWHERE";
    case ORTE_JOB_STATE_REPORT_PROGRESS:
        return "REPORT PROGRESS";
    case ORTE_JOB_STATE_ALLOC_FAILED:
        return "ALLOCATION FAILED";
    case ORTE_JOB_STATE_MAP_FAILED:
        return "MAP FAILED";
    case ORTE_JOB_STATE_CANNOT_LAUNCH:
        return "CANNOT LAUNCH";
    case ORTE_JOB_STATE_FT_CHECKPOINT:
        return "FAULT TOLERANCE CHECKPOINT";
    case ORTE_JOB_STATE_FT_CONTINUE:
        return "FAULT TOLERANCE CONTINUE";
    case ORTE_JOB_STATE_FT_RESTART:
        return "FAULT TOLERANCE RESTART";
    case ORTE_JOB_STATE_ANY:
        return "ANY";
    case ORTE_JOB_STATE_DEBUGGER_DETACH:
        return "DEBUGGER DETACH";
    default:
        return "UNKNOWN STATE!";
    }
}

const char *orte_app_ctx_state_to_str(orte_app_state_t state)
{
    switch(state) {
    case ORTE_APP_STATE_UNDEF:
        return "UNDEFINED";
    case ORTE_APP_STATE_INIT:
        return "PENDING INIT";
    case ORTE_APP_STATE_ALL_MAPPED:
        return "ALL MAPPED";
    case ORTE_APP_STATE_RUNNING:
        return "RUNNING";
    case ORTE_APP_STATE_COMPLETED:
        return "COMPLETED";
    default:
        return "UNKNOWN STATE!";
    }
}

const char *orte_proc_state_to_str(orte_proc_state_t state)
{
    switch(state) {
    case ORTE_PROC_STATE_UNDEF:
        return "UNDEFINED";
    case ORTE_PROC_STATE_INIT:
        return "INITIALIZED";
    case ORTE_PROC_STATE_RESTART:
        return "RESTARTING";
    case ORTE_PROC_STATE_TERMINATE:
        return "MARKED FOR TERMINATION";
    case ORTE_PROC_STATE_RUNNING:
        return "RUNNING";
    case ORTE_PROC_STATE_REGISTERED:
        return "SYNC REGISTERED";
    case ORTE_PROC_STATE_IOF_COMPLETE:
        return "IOF COMPLETE";
    case ORTE_PROC_STATE_WAITPID_FIRED:
        return "WAITPID FIRED";
    case ORTE_PROC_STATE_UNTERMINATED:
        return "UNTERMINATED";
    case ORTE_PROC_STATE_TERMINATED:
        return "NORMALLY TERMINATED";
    case ORTE_PROC_STATE_ERROR:
        return "ARTIFICIAL BOUNDARY - ERROR";
    case ORTE_PROC_STATE_KILLED_BY_CMD:
        return "KILLED BY INTERNAL COMMAND";
    case ORTE_PROC_STATE_ABORTED:
        return "ABORTED";
    case ORTE_PROC_STATE_FAILED_TO_START:
        return "FAILED TO START";
    case ORTE_PROC_STATE_ABORTED_BY_SIG:
        return "ABORTED BY SIGNAL";
    case ORTE_PROC_STATE_TERM_WO_SYNC:
        return "TERMINATED WITHOUT SYNC";
    case ORTE_PROC_STATE_COMM_FAILED:
        return "COMMUNICATION FAILURE";
    case ORTE_PROC_STATE_SENSOR_BOUND_EXCEEDED:
        return "SENSOR BOUND EXCEEDED";
    case ORTE_PROC_STATE_CALLED_ABORT:
        return "CALLED ABORT";
    case ORTE_PROC_STATE_HEARTBEAT_FAILED:
        return "HEARTBEAT FAILED";
    case ORTE_PROC_STATE_MIGRATING:
        return "MIGRATING";
    case ORTE_PROC_STATE_CANNOT_RESTART:
        return "CANNOT BE RESTARTED";
    case ORTE_PROC_STATE_TERM_NON_ZERO:
        return "EXITED WITH NON-ZERO STATUS";
    case ORTE_PROC_STATE_FAILED_TO_LAUNCH:
        return "FAILED TO LAUNCH";
    case ORTE_PROC_STATE_UNABLE_TO_SEND_MSG:
        return "UNABLE TO SEND MSG";
    case ORTE_PROC_STATE_LIFELINE_LOST:
        return "LIFELINE LOST";
    case ORTE_PROC_STATE_NO_PATH_TO_TARGET:
        return "NO PATH TO TARGET";
    case ORTE_PROC_STATE_FAILED_TO_CONNECT:
        return "FAILED TO CONNECT";
    case ORTE_PROC_STATE_PEER_UNKNOWN:
        return "PEER UNKNOWN";
    case ORTE_PROC_STATE_ANY:
        return "ANY";
    default:
        return "UNKNOWN STATE!";
    }
}

const char *orte_node_state_to_str(orte_node_state_t state)
{
    switch(state) {
    case ORTE_NODE_STATE_UNDEF:
        return "UNDEF";
    case ORTE_NODE_STATE_UNKNOWN:
        return "UNKNOWN";
    case ORTE_NODE_STATE_DOWN:
        return "DOWN";
    case ORTE_NODE_STATE_UP:
        return "UP";
    case ORTE_NODE_STATE_REBOOT:
        return "REBOOT";
    case ORTE_NODE_STATE_DO_NOT_USE:
        return "DO_NOT_USE";
    case ORTE_NODE_STATE_NOT_INCLUDED:
        return "NOT_INCLUDED";
    case ORTE_NODE_STATE_ADDED:
        return "ADDED";
   default:
        return "UNKNOWN STATE!";
    }
}
