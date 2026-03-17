/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2006 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2014      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2015-2018 Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef ORTE_CONSTANTS_H
#define ORTE_CONSTANTS_H

#include "opal/constants.h"
#include "orte_config.h"

BEGIN_C_DECLS

#define ORTE_ERR_BASE            OPAL_ERR_MAX


enum {
    /* Error codes inherited from OPAL.  Still enum values so that we
       get the nice debugger help. */

    ORTE_SUCCESS                            = OPAL_SUCCESS,

    ORTE_ERROR                              = OPAL_ERROR,
    ORTE_ERR_OUT_OF_RESOURCE                = OPAL_ERR_OUT_OF_RESOURCE,
    ORTE_ERR_TEMP_OUT_OF_RESOURCE           = OPAL_ERR_TEMP_OUT_OF_RESOURCE,
    ORTE_ERR_RESOURCE_BUSY                  = OPAL_ERR_RESOURCE_BUSY,
    ORTE_ERR_BAD_PARAM                      = OPAL_ERR_BAD_PARAM,
    ORTE_ERR_FATAL                          = OPAL_ERR_FATAL,
    ORTE_ERR_NOT_IMPLEMENTED                = OPAL_ERR_NOT_IMPLEMENTED,
    ORTE_ERR_NOT_SUPPORTED                  = OPAL_ERR_NOT_SUPPORTED,
    ORTE_ERR_INTERUPTED                     = OPAL_ERR_INTERRUPTED,
    ORTE_ERR_WOULD_BLOCK                    = OPAL_ERR_WOULD_BLOCK,
    ORTE_ERR_IN_ERRNO                       = OPAL_ERR_IN_ERRNO,
    ORTE_ERR_UNREACH                        = OPAL_ERR_UNREACH,
    ORTE_ERR_NOT_FOUND                      = OPAL_ERR_NOT_FOUND,
    ORTE_EXISTS                             = OPAL_EXISTS,
    ORTE_ERR_TIMEOUT                        = OPAL_ERR_TIMEOUT,
    ORTE_ERR_NOT_AVAILABLE                  = OPAL_ERR_NOT_AVAILABLE,
    ORTE_ERR_PERM                           = OPAL_ERR_PERM,
    ORTE_ERR_VALUE_OUT_OF_BOUNDS            = OPAL_ERR_VALUE_OUT_OF_BOUNDS,
    ORTE_ERR_FILE_READ_FAILURE              = OPAL_ERR_FILE_READ_FAILURE,
    ORTE_ERR_FILE_WRITE_FAILURE             = OPAL_ERR_FILE_WRITE_FAILURE,
    ORTE_ERR_FILE_OPEN_FAILURE              = OPAL_ERR_FILE_OPEN_FAILURE,
    ORTE_ERR_PACK_MISMATCH                  = OPAL_ERR_PACK_MISMATCH,
    ORTE_ERR_PACK_FAILURE                   = OPAL_ERR_PACK_FAILURE,
    ORTE_ERR_UNPACK_FAILURE                 = OPAL_ERR_UNPACK_FAILURE,
    ORTE_ERR_UNPACK_INADEQUATE_SPACE        = OPAL_ERR_UNPACK_INADEQUATE_SPACE,
    ORTE_ERR_UNPACK_READ_PAST_END_OF_BUFFER = OPAL_ERR_UNPACK_READ_PAST_END_OF_BUFFER,
    ORTE_ERR_TYPE_MISMATCH                  = OPAL_ERR_TYPE_MISMATCH,
    ORTE_ERR_OPERATION_UNSUPPORTED          = OPAL_ERR_OPERATION_UNSUPPORTED,
    ORTE_ERR_UNKNOWN_DATA_TYPE              = OPAL_ERR_UNKNOWN_DATA_TYPE,
    ORTE_ERR_BUFFER                         = OPAL_ERR_BUFFER,
    ORTE_ERR_DATA_TYPE_REDEF                = OPAL_ERR_DATA_TYPE_REDEF,
    ORTE_ERR_DATA_OVERWRITE_ATTEMPT         = OPAL_ERR_DATA_OVERWRITE_ATTEMPT,
    ORTE_ERR_MODULE_NOT_FOUND               = OPAL_ERR_MODULE_NOT_FOUND,
    ORTE_ERR_TOPO_SLOT_LIST_NOT_SUPPORTED   = OPAL_ERR_TOPO_SLOT_LIST_NOT_SUPPORTED,
    ORTE_ERR_TOPO_SOCKET_NOT_SUPPORTED      = OPAL_ERR_TOPO_SOCKET_NOT_SUPPORTED,
    ORTE_ERR_TOPO_CORE_NOT_SUPPORTED        = OPAL_ERR_TOPO_CORE_NOT_SUPPORTED,
    ORTE_ERR_NOT_ENOUGH_SOCKETS             = OPAL_ERR_NOT_ENOUGH_SOCKETS,
    ORTE_ERR_NOT_ENOUGH_CORES               = OPAL_ERR_NOT_ENOUGH_CORES,
    ORTE_ERR_INVALID_PHYS_CPU               = OPAL_ERR_INVALID_PHYS_CPU,
    ORTE_ERR_MULTIPLE_AFFINITIES            = OPAL_ERR_MULTIPLE_AFFINITIES,
    ORTE_ERR_SLOT_LIST_RANGE                = OPAL_ERR_SLOT_LIST_RANGE,
    ORTE_ERR_SILENT                         = OPAL_ERR_SILENT,
    ORTE_ERR_NOT_INITIALIZED                = OPAL_ERR_NOT_INITIALIZED,
    ORTE_ERR_NOT_BOUND                      = OPAL_ERR_NOT_BOUND,
    ORTE_ERR_TAKE_NEXT_OPTION               = OPAL_ERR_TAKE_NEXT_OPTION,
    ORTE_ERR_PROC_ENTRY_NOT_FOUND           = OPAL_ERR_PROC_ENTRY_NOT_FOUND,
    ORTE_ERR_DATA_VALUE_NOT_FOUND           = OPAL_ERR_DATA_VALUE_NOT_FOUND,
    ORTE_ERR_CONNECTION_FAILED              = OPAL_ERR_CONNECTION_FAILED,
    ORTE_ERR_AUTHENTICATION_FAILED          = OPAL_ERR_AUTHENTICATION_FAILED,
    ORTE_ERR_COMM_FAILURE                   = OPAL_ERR_COMM_FAILURE,
    ORTE_ERR_DEBUGGER_RELEASE               = OPAL_ERR_DEBUGGER_RELEASE,
    ORTE_ERR_PARTIAL_SUCCESS                = OPAL_ERR_PARTIAL_SUCCESS,
    ORTE_ERR_PROC_ABORTED                   = OPAL_ERR_PROC_ABORTED,
    ORTE_ERR_PROC_REQUESTED_ABORT           = OPAL_ERR_PROC_REQUESTED_ABORT,
    ORTE_ERR_PROC_ABORTING                  = OPAL_ERR_PROC_ABORTING,
    ORTE_ERR_NODE_DOWN                      = OPAL_ERR_NODE_DOWN,
    ORTE_ERR_NODE_OFFLINE                   = OPAL_ERR_NODE_OFFLINE,
    ORTE_OPERATION_SUCCEEDED                = OPAL_OPERATION_SUCCEEDED,

/* error codes specific to ORTE - don't forget to update
    orte/util/error_strings.c when adding new error codes!!
    Otherwise, the error reporting system will potentially crash,
    or at the least not be able to report the new error correctly.
 */
    ORTE_ERR_RECV_LESS_THAN_POSTED          = (ORTE_ERR_BASE -  1),
    ORTE_ERR_RECV_MORE_THAN_POSTED          = (ORTE_ERR_BASE -  2),
    ORTE_ERR_NO_MATCH_YET                   = (ORTE_ERR_BASE -  3),
    ORTE_ERR_REQUEST                        = (ORTE_ERR_BASE -  4),
    ORTE_ERR_NO_CONNECTION_ALLOWED          = (ORTE_ERR_BASE -  5),
    ORTE_ERR_CONNECTION_REFUSED             = (ORTE_ERR_BASE -  6),
    ORTE_ERR_COMPARE_FAILURE                = (ORTE_ERR_BASE -  9),
    ORTE_ERR_COPY_FAILURE                   = (ORTE_ERR_BASE - 10),
    ORTE_ERR_PROC_STATE_MISSING             = (ORTE_ERR_BASE - 11),
    ORTE_ERR_PROC_EXIT_STATUS_MISSING       = (ORTE_ERR_BASE - 12),
    ORTE_ERR_INDETERMINATE_STATE_INFO       = (ORTE_ERR_BASE - 13),
    ORTE_ERR_NODE_FULLY_USED                = (ORTE_ERR_BASE - 14),
    ORTE_ERR_INVALID_NUM_PROCS              = (ORTE_ERR_BASE - 15),
    ORTE_ERR_ADDRESSEE_UNKNOWN              = (ORTE_ERR_BASE - 16),
    ORTE_ERR_SYS_LIMITS_PIPES               = (ORTE_ERR_BASE - 17),
    ORTE_ERR_PIPE_SETUP_FAILURE             = (ORTE_ERR_BASE - 18),
    ORTE_ERR_SYS_LIMITS_CHILDREN            = (ORTE_ERR_BASE - 19),
    ORTE_ERR_FAILED_GET_TERM_ATTRS          = (ORTE_ERR_BASE - 20),
    ORTE_ERR_WDIR_NOT_FOUND                 = (ORTE_ERR_BASE - 21),
    ORTE_ERR_EXE_NOT_FOUND                  = (ORTE_ERR_BASE - 22),
    ORTE_ERR_PIPE_READ_FAILURE              = (ORTE_ERR_BASE - 23),
    ORTE_ERR_EXE_NOT_ACCESSIBLE             = (ORTE_ERR_BASE - 24),
    ORTE_ERR_FAILED_TO_START                = (ORTE_ERR_BASE - 25),
    ORTE_ERR_FILE_NOT_EXECUTABLE            = (ORTE_ERR_BASE - 26),
    ORTE_ERR_HNP_COULD_NOT_START            = (ORTE_ERR_BASE - 27),
    ORTE_ERR_SYS_LIMITS_SOCKETS             = (ORTE_ERR_BASE - 28),
    ORTE_ERR_SOCKET_NOT_AVAILABLE           = (ORTE_ERR_BASE - 29),
    ORTE_ERR_SYSTEM_WILL_BOOTSTRAP          = (ORTE_ERR_BASE - 30),
    ORTE_ERR_RESTART_LIMIT_EXCEEDED         = (ORTE_ERR_BASE - 31),
    ORTE_ERR_INVALID_NODE_RANK              = (ORTE_ERR_BASE - 32),
    ORTE_ERR_INVALID_LOCAL_RANK             = (ORTE_ERR_BASE - 33),
    ORTE_ERR_UNRECOVERABLE                  = (ORTE_ERR_BASE - 34),
    ORTE_ERR_MEM_LIMIT_EXCEEDED             = (ORTE_ERR_BASE - 35),
    ORTE_ERR_HEARTBEAT_LOST                 = (ORTE_ERR_BASE - 36),
    ORTE_ERR_PROC_STALLED                   = (ORTE_ERR_BASE - 37),
    ORTE_ERR_NO_APP_SPECIFIED               = (ORTE_ERR_BASE - 38),
    ORTE_ERR_NO_EXE_SPECIFIED               = (ORTE_ERR_BASE - 39),
    ORTE_ERR_COMM_DISABLED                  = (ORTE_ERR_BASE - 40),
    ORTE_ERR_FAILED_TO_MAP                  = (ORTE_ERR_BASE - 41),
    ORTE_ERR_SENSOR_LIMIT_EXCEEDED          = (ORTE_ERR_BASE - 42),
    ORTE_ERR_ALLOCATION_PENDING             = (ORTE_ERR_BASE - 43),
    ORTE_ERR_NO_PATH_TO_TARGET              = (ORTE_ERR_BASE - 44),
    ORTE_ERR_OP_IN_PROGRESS                 = (ORTE_ERR_BASE - 45),
    ORTE_ERR_OPEN_CONDUIT_FAIL              = (ORTE_ERR_BASE - 46),
    ORTE_ERR_DUPLICATE_MSG                  = (ORTE_ERR_BASE - 47),
    ORTE_ERR_OUT_OF_ORDER_MSG               = (ORTE_ERR_BASE - 48),
    ORTE_ERR_FORCE_SELECT                   = (ORTE_ERR_BASE - 49),
    ORTE_ERR_JOB_CANCELLED                  = (ORTE_ERR_BASE - 50),
    ORTE_ERR_CONDUIT_SEND_FAIL              = (ORTE_ERR_BASE - 51)
};

#define ORTE_ERR_MAX                      (ORTE_ERR_BASE - 100)

END_C_DECLS

#endif /* ORTE_CONSTANTS_H */
