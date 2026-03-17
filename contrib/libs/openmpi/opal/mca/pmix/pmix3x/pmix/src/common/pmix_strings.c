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
 * Copyright (c) 2007-2012 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2014-2018 Intel, Inc. All rights reserved.
 * Copyright (c) 2018      Research Organization for Information Science
 *                         and Technology (RIST).  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include <src/include/pmix_config.h>


#ifdef HAVE_STRING_H
#include <string.h>
#endif
#include <errno.h>
#include <stdio.h>
#ifdef HAVE_STDLIB_H
#include <stdlib.h>
#endif

#include <pmix_common.h>
#include <pmix_rename.h>

#include "src/include/pmix_globals.h"

PMIX_EXPORT const char* PMIx_Proc_state_string(pmix_proc_state_t state)
{
    switch(state) {
        case PMIX_PROC_STATE_UNDEF:
            return "UNDEFINED";
        case PMIX_PROC_STATE_PREPPED:
            return "PREPPED FOR LAUNCH";
        case PMIX_PROC_STATE_LAUNCH_UNDERWAY:
            return "LAUNCH UNDERWAY";
        case PMIX_PROC_STATE_RESTART:
            return "PROC READY FOR RESTART";
        case PMIX_PROC_STATE_TERMINATE:
            return "PROC MARKED FOR TERMINATION";
        case PMIX_PROC_STATE_RUNNING:
            return "PROC EXECUTING";
        case PMIX_PROC_STATE_CONNECTED:
            return "PROC HAS CONNECTED TO LOCAL PMIX SERVER";
        case PMIX_PROC_STATE_UNTERMINATED:
            return "PROC HAS NOT TERMINATED";
        case PMIX_PROC_STATE_TERMINATED:
            return "PROC HAS TERMINATED";
        case PMIX_PROC_STATE_ERROR:
            return "PROC ERROR";
        case PMIX_PROC_STATE_KILLED_BY_CMD:
            return "PROC KILLED BY CMD";
        case PMIX_PROC_STATE_ABORTED:
            return "PROC ABNORMALLY ABORTED";
        case PMIX_PROC_STATE_FAILED_TO_START:
            return "PROC FAILED TO START";
        case PMIX_PROC_STATE_ABORTED_BY_SIG:
            return "PROC ABORTED BY SIGNAL";
        case PMIX_PROC_STATE_TERM_WO_SYNC:
            return "PROC TERMINATED WITHOUT CALLING PMIx_Finalize";
        case PMIX_PROC_STATE_COMM_FAILED:
            return "PROC LOST COMMUNICATION";
        case PMIX_PROC_STATE_SENSOR_BOUND_EXCEEDED:
            return "PROC SENSOR BOUND EXCEEDED";
        case PMIX_PROC_STATE_CALLED_ABORT:
            return "PROC CALLED PMIx_Abort";
        case PMIX_PROC_STATE_HEARTBEAT_FAILED:
            return "PROC FAILED TO REPORT HEARTBEAT";
        case PMIX_PROC_STATE_MIGRATING:
            return "PROC WAITING TO MIGRATE";
        case PMIX_PROC_STATE_CANNOT_RESTART:
            return "PROC CANNOT BE RESTARTED";
        case PMIX_PROC_STATE_TERM_NON_ZERO:
            return "PROC TERMINATED WITH NON-ZERO STATUS";
        case PMIX_PROC_STATE_FAILED_TO_LAUNCH:
            return "PROC FAILED TO LAUNCH";
        default:
            return "UNKNOWN STATE";
    }
}

PMIX_EXPORT const char* PMIx_Scope_string(pmix_scope_t scope)
{
    switch(scope) {
        case PMIX_SCOPE_UNDEF:
            return "UNDEFINED";
        case PMIX_LOCAL:
            return "SHARE ON LOCAL NODE ONLY";
        case PMIX_REMOTE:
            return "SHARE ON REMOTE NODES ONLY";
        case PMIX_GLOBAL:
            return "SHARE ACROSS ALL NODES";
        case PMIX_INTERNAL:
            return "STORE INTERNALLY";
        default:
            return "UNKNOWN SCOPE";
    }
}

PMIX_EXPORT const char* PMIx_Persistence_string(pmix_persistence_t persist)
{
    switch(persist) {
        case PMIX_PERSIST_INDEF:
            return "INDEFINITE";
        case PMIX_PERSIST_FIRST_READ:
            return "DELETE ON FIRST ACCESS";
        case PMIX_PERSIST_PROC:
            return "RETAIN UNTIL PUBLISHING PROCESS TERMINATES";
        case PMIX_PERSIST_APP:
            return "RETAIN UNTIL APPLICATION OF PUBLISHING PROCESS TERMINATES";
        case PMIX_PERSIST_SESSION:
            return "RETAIN UNTIL ALLOCATION OF PUBLISHING PROCESS TERMINATES";
        case PMIX_PERSIST_INVALID:
            return "INVALID";
        default:
            return "UNKNOWN PERSISTENCE";
    }
}

PMIX_EXPORT const char* PMIx_Data_range_string(pmix_data_range_t range)
{
    switch(range) {
        case PMIX_RANGE_UNDEF:
            return "UNDEFINED";
        case PMIX_RANGE_RM:
            return "INTENDED FOR HOST RESOURCE MANAGER ONLY";
        case PMIX_RANGE_LOCAL:
            return "AVAIL ON LOCAL NODE ONLY";
        case PMIX_RANGE_NAMESPACE:
            return "AVAIL TO PROCESSES IN SAME JOB ONLY";
        case PMIX_RANGE_SESSION:
            return "AVAIL TO PROCESSES IN SAME ALLOCATION ONLY";
        case PMIX_RANGE_GLOBAL:
            return "AVAIL TO ANYONE WITH AUTHORIZATION";
        case PMIX_RANGE_CUSTOM:
            return "AVAIL AS SPECIFIED IN DIRECTIVES";
        case PMIX_RANGE_PROC_LOCAL:
            return "AVAIL ON LOCAL PROC ONLY";
        case PMIX_RANGE_INVALID:
            return "INVALID";
        default:
            return "UNKNOWN";
    }
}

PMIX_EXPORT const char* PMIx_Info_directives_string(pmix_info_directives_t directives)
{
    switch(directives) {
        case PMIX_INFO_REQD:
            return "REQUIRED";
        default:
            return "UNSPECIFIED";
    }
}

PMIX_EXPORT const char* PMIx_Alloc_directive_string(pmix_alloc_directive_t directive)
{
    switch(directive) {
        case PMIX_ALLOC_NEW:
            return "NEW";
        case PMIX_ALLOC_EXTEND:
            return "EXTEND";
        case PMIX_ALLOC_RELEASE:
            return "RELEASE";
        case PMIX_ALLOC_REAQUIRE:
            return "REACQUIRE";
        default:
            return "UNSPECIFIED";
    }
}


PMIX_EXPORT const char* pmix_command_string(pmix_cmd_t cmd)
{
    switch(cmd) {
        case PMIX_REQ_CMD:
            return "REQUEST INIT INFO";
        case PMIX_ABORT_CMD:
            return "ABORT";
        case PMIX_COMMIT_CMD:
            return "COMMIT";
        case PMIX_FENCENB_CMD:
            return "FENCE";
        case PMIX_GETNB_CMD:
            return "GET";
        case PMIX_FINALIZE_CMD:
            return "FINALIZE";
        case PMIX_PUBLISHNB_CMD:
            return "PUBLISH";
        case PMIX_LOOKUPNB_CMD:
            return "LOOKUP";
        case PMIX_UNPUBLISHNB_CMD:
            return "UNPUBLISH";
        case PMIX_SPAWNNB_CMD:
            return "SPAWN";
        case PMIX_CONNECTNB_CMD:
            return "CONNECT";
        case PMIX_DISCONNECTNB_CMD:
            return "DISCONNECT";
        case PMIX_NOTIFY_CMD:
            return "NOTIFY";
        case PMIX_REGEVENTS_CMD:
            return "REGISTER EVENT HANDLER";
        case PMIX_DEREGEVENTS_CMD:
            return "DEREGISTER EVENT HANDLER";
        case PMIX_QUERY_CMD:
            return "QUERY";
        case PMIX_LOG_CMD:
            return "LOG";
        case PMIX_ALLOC_CMD:
            return "ALLOCATE";
        case PMIX_JOB_CONTROL_CMD:
            return "JOB CONTROL";
        case PMIX_MONITOR_CMD:
            return "MONITOR";
        case PMIX_IOF_PUSH_CMD:
            return "IOF PUSH";
        case PMIX_IOF_PULL_CMD:
            return "IOF PULL";
        default:
            return "UNKNOWN";
    }
}

/* this is not a thread-safe implementation. To correctly implement this,
 * we need to port the thread-safe data code from OPAL and use it here */
static char answer[300];

PMIX_EXPORT const char* PMIx_IOF_channel_string(pmix_iof_channel_t channel)
{
    size_t cnt=0;

    if (PMIX_FWD_STDIN_CHANNEL & channel) {
        strcpy(&answer[cnt], "STDIN ");
        cnt += strlen("STDIN ");
    }
    if (PMIX_FWD_STDOUT_CHANNEL & channel) {
        strcpy(&answer[cnt], "STDOUT ");
        cnt += strlen("STDOUT ");
    }
    if (PMIX_FWD_STDERR_CHANNEL & channel) {
        strcpy(&answer[cnt], "STDERR ");
        cnt += strlen("STDERR ");
    }
    if (PMIX_FWD_STDDIAG_CHANNEL & channel) {
        strcpy(&answer[cnt], "STDDIAG ");
        cnt += strlen("STDDIAG ");
    }
    if (0 == cnt) {
        strcpy(&answer[cnt], "NONE");
    }
    return answer;
}
