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
#include "src/include/pmix_globals.h"
#include "src/util/error.h"

PMIX_EXPORT const char* PMIx_Error_string(pmix_status_t errnum)
{
    switch(errnum) {

    case PMIX_SUCCESS:
        return "SUCCESS";
    case PMIX_ERROR:
        return "ERROR";
    case PMIX_ERR_SILENT:
        return "SILENT_ERROR";


    case PMIX_ERR_DEBUGGER_RELEASE:
        return "DEBUGGER-RELEASE";


    case PMIX_ERR_PROC_RESTART:
        return "PROC_RESTART";
    case PMIX_ERR_PROC_CHECKPOINT:
        return "PROC-CHECKPOINT-ERROR";
    case PMIX_ERR_PROC_MIGRATE:
        return "PROC-MIGRATE";


    case PMIX_ERR_PROC_ABORTED:
        return "PROC-ABORTED";
    case PMIX_ERR_PROC_REQUESTED_ABORT:
        return "PROC-ABORT-REQUESTED";
    case PMIX_ERR_PROC_ABORTING:
        return "PROC-ABORTING";


    case PMIX_ERR_SERVER_FAILED_REQUEST:
        return "SERVER FAILED REQUEST";
    case PMIX_EXISTS:
        return "EXISTS";
    case PMIX_ERR_INVALID_CRED:
        return "INVALID-CREDENTIAL";
    case PMIX_ERR_HANDSHAKE_FAILED:
        return "HANDSHAKE-FAILED";
    case PMIX_ERR_READY_FOR_HANDSHAKE:
        return "READY-FOR-HANDSHAKE";
    case PMIX_ERR_WOULD_BLOCK:
        return "WOULD-BLOCK";
    case PMIX_ERR_UNKNOWN_DATA_TYPE:
        return "UNKNOWN-DATA-TYPE";
    case PMIX_ERR_PROC_ENTRY_NOT_FOUND:
        return "PROC-ENTRY-NOT-FOUND";
    case PMIX_ERR_TYPE_MISMATCH:
        return "TYPE-MISMATCH";
    case PMIX_ERR_UNPACK_INADEQUATE_SPACE:
        return "UNPACK-INADEQUATE-SPACE";
    case PMIX_ERR_UNPACK_FAILURE:
        return "UNPACK-FAILURE";
    case PMIX_ERR_PACK_FAILURE:
        return "PACK-FAILURE";
    case PMIX_ERR_PACK_MISMATCH:
        return "PACK-MISMATCH";
    case PMIX_ERR_NO_PERMISSIONS:
        return "NO-PERMISSIONS";
    case PMIX_ERR_TIMEOUT:
        return "TIMEOUT";
    case PMIX_ERR_UNREACH:
        return "UNREACHABLE";
    case PMIX_ERR_IN_ERRNO:
        return "ERR-IN-ERRNO";
    case PMIX_ERR_BAD_PARAM:
        return "BAD-PARAM";
    case PMIX_ERR_RESOURCE_BUSY:
        return "RESOURCE-BUSY";
    case PMIX_ERR_OUT_OF_RESOURCE:
        return "OUT-OF-RESOURCE";
    case PMIX_ERR_DATA_VALUE_NOT_FOUND:
        return "DATA-VALUE-NOT-FOUND";
    case PMIX_ERR_INIT:
        return "INIT";
    case PMIX_ERR_NOMEM:
        return "NO-MEM";
    case PMIX_ERR_INVALID_ARG:
        return "INVALID-ARG";
    case PMIX_ERR_INVALID_KEY:
        return "INVALID-KEY";
    case PMIX_ERR_INVALID_KEY_LENGTH:
        return "INVALID-KEY-LENGTH";
    case PMIX_ERR_INVALID_VAL:
        return "INVALID-VAL";
    case PMIX_ERR_INVALID_VAL_LENGTH:
        return "INVALID-VAL-LENGTH";
    case PMIX_ERR_INVALID_LENGTH:
        return "INVALID-LENGTH";
    case PMIX_ERR_INVALID_NUM_ARGS:
        return "INVALID-NUM-ARGS";
    case PMIX_ERR_INVALID_ARGS:
        return "INVALID-ARGS";
    case PMIX_ERR_INVALID_NUM_PARSED:
        return "INVALID-NUM-PARSED";
    case PMIX_ERR_INVALID_KEYVALP:
        return "INVALID-KEYVAL";
    case PMIX_ERR_INVALID_SIZE:
        return "INVALID-SIZE";
    case PMIX_ERR_INVALID_NAMESPACE:
        return "INVALID-NAMESPACE";
    case PMIX_ERR_SERVER_NOT_AVAIL:
        return "SERVER-NOT-AVAIL";
    case PMIX_ERR_NOT_FOUND:
        return "NOT-FOUND";
    case PMIX_ERR_NOT_SUPPORTED:
        return "NOT-SUPPORTED";
    case PMIX_ERR_NOT_IMPLEMENTED:
        return "NOT-IMPLEMENTED";
    case PMIX_ERR_COMM_FAILURE:
        return "COMM-FAILURE";
    case PMIX_ERR_UNPACK_READ_PAST_END_OF_BUFFER:
        return "UNPACK-PAST-END";
    case PMIX_ERR_CONFLICTING_CLEANUP_DIRECTIVES:
        return "PMIX CONFLICTING CLEANUP DIRECTIVES";


    case PMIX_ERR_LOST_CONNECTION_TO_SERVER:
        return "LOST_CONNECTION_TO_SERVER";
    case PMIX_ERR_LOST_PEER_CONNECTION:
        return "LOST-PEER-CONNECTION";
    case PMIX_ERR_LOST_CONNECTION_TO_CLIENT:
        return "LOST-CONNECTION-TO-CLIENT";
    case PMIX_QUERY_PARTIAL_SUCCESS:
        return "QUERY-PARTIAL-SUCCESS";
    case PMIX_NOTIFY_ALLOC_COMPLETE:
        return "PMIX ALLOC OPERATION COMPLETE";
    case PMIX_JCTRL_CHECKPOINT:
        return "PMIX JOB CONTROL CHECKPOINT";
    case PMIX_JCTRL_CHECKPOINT_COMPLETE:
        return "PMIX JOB CONTROL CHECKPOINT COMPLETE";
    case PMIX_JCTRL_PREEMPT_ALERT:
        return "PMIX PRE-EMPTION ALERT";
    case PMIX_MONITOR_HEARTBEAT_ALERT:
        return "PMIX HEARTBEAT ALERT";
    case PMIX_MONITOR_FILE_ALERT:
        return "PMIX FILE MONITOR ALERT";
    case PMIX_PROC_TERMINATED:
        return "PROC-TERMINATED";
    case PMIX_ERR_INVALID_TERMINATION:
        return "INVALID-TERMINATION";

    case PMIX_ERR_EVENT_REGISTRATION:
        return "EVENT-REGISTRATION";
    case PMIX_ERR_JOB_TERMINATED:
        return "PMIX_ERR_JOB_TERMINATED";
    case PMIX_ERR_UPDATE_ENDPOINTS:
        return "UPDATE-ENDPOINTS";
    case PMIX_MODEL_DECLARED:
        return "PMIX MODEL DECLARED";
    case PMIX_GDS_ACTION_COMPLETE:
        return "GDS-ACTION-COMPLETE";
    case PMIX_PROC_HAS_CONNECTED:
        return "PROC-HAS-CONNECTED";
    case PMIX_CONNECT_REQUESTED:
        return "CONNECT-REQUESTED";
    case PMIX_OPENMP_PARALLEL_ENTERED:
        return "OPENMP-PARALLEL-ENTERED";
    case PMIX_OPENMP_PARALLEL_EXITED:
        return "OPENMP-PARALLEL-EXITED";

    case PMIX_LAUNCH_DIRECTIVE:
        return "LAUNCH-DIRECTIVE";
    case PMIX_LAUNCHER_READY:
        return "LAUNCHER-READY";
    case PMIX_OPERATION_IN_PROGRESS:
        return "OPERATION-IN-PROGRESS";
    case PMIX_OPERATION_SUCCEEDED:
        return "OPERATION-SUCCEEDED";
    case PMIX_ERR_INVALID_OPERATION:
        return "INVALID-OPERATION";

    case PMIX_ERR_NODE_DOWN:
        return "NODE-DOWN";
    case PMIX_ERR_NODE_OFFLINE:
        return "NODE-OFFLINE";
    case PMIX_ERR_SYS_OTHER:
        return "UNDEFINED-SYSTEM-EVENT";

    case PMIX_EVENT_NO_ACTION_TAKEN:
        return "EVENT-NO-ACTION-TAKEN";
    case PMIX_EVENT_PARTIAL_ACTION_TAKEN:
        return "EVENT-PARTIAL-ACTION-TAKEN";
    case PMIX_EVENT_ACTION_DEFERRED:
        return "EVENT-ACTION-DEFERRED";
    case PMIX_EVENT_ACTION_COMPLETE:
        return "EVENT-ACTION-COMPLETE";


    case PMIX_ERR_NOT_AVAILABLE:
        return "PMIX_ERR_NOT_AVAILABLE";
    case PMIX_ERR_FATAL:
        return "PMIX_ERR_FATAL";
    case PMIX_ERR_VALUE_OUT_OF_BOUNDS:
        return "PMIX_ERR_VALUE_OUT_OF_BOUNDS";
    case PMIX_ERR_PERM:
        return "PMIX_ERR_PERM";
    case PMIX_ERR_NETWORK_NOT_PARSEABLE:
        return "PMIX_ERR_NETWORK_NOT_PARSEABLE";
    case PMIX_ERR_FILE_OPEN_FAILURE:
        return "PMIX_ERR_FILE_OPEN_FAILURE";
    case PMIX_ERR_FILE_READ_FAILURE:
        return "PMIX_ERR_FILE_READ_FAILURE";
    case PMIX_ERR_TAKE_NEXT_OPTION:
        return "TAKE-NEXT-OPTION";
    case PMIX_ERR_TEMP_UNAVAILABLE:
        return "PMIX TEMPORARILY UNAVAILABLE";


    case PMIX_MAX_ERR_CONSTANT:
        return "PMIX_ERR_WILDCARD";


    default:
        return "ERROR STRING NOT FOUND";
    }
}
