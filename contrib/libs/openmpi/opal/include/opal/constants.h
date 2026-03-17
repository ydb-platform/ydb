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
 * Copyright (c) 2010-2012 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2014-2018 Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef OPAL_CONSTANTS_H
#define OPAL_CONSTANTS_H

/* error codes - don't forget to update opal/rutime/opal_init.c when
   adding to this list */
#define OPAL_ERR_BASE             0 /* internal use only */

enum {
    OPAL_SUCCESS                            = (OPAL_ERR_BASE),

    OPAL_ERROR                              = (OPAL_ERR_BASE -  1),
    OPAL_ERR_OUT_OF_RESOURCE                = (OPAL_ERR_BASE -  2), /* fatal error */
    OPAL_ERR_TEMP_OUT_OF_RESOURCE           = (OPAL_ERR_BASE -  3), /* try again later */
    OPAL_ERR_RESOURCE_BUSY                  = (OPAL_ERR_BASE -  4),
    OPAL_ERR_BAD_PARAM                      = (OPAL_ERR_BASE -  5),  /* equivalent to MPI_ERR_ARG error code */
    OPAL_ERR_FATAL                          = (OPAL_ERR_BASE -  6),
    OPAL_ERR_NOT_IMPLEMENTED                = (OPAL_ERR_BASE -  7),
    OPAL_ERR_NOT_SUPPORTED                  = (OPAL_ERR_BASE -  8),
    OPAL_ERR_INTERRUPTED                    = (OPAL_ERR_BASE -  9),
    OPAL_ERR_WOULD_BLOCK                    = (OPAL_ERR_BASE - 10),
    OPAL_ERR_IN_ERRNO                       = (OPAL_ERR_BASE - 11),
    OPAL_ERR_UNREACH                        = (OPAL_ERR_BASE - 12),
    OPAL_ERR_NOT_FOUND                      = (OPAL_ERR_BASE - 13),
    OPAL_EXISTS                             = (OPAL_ERR_BASE - 14), /* indicates that the specified object already exists */
    OPAL_ERR_TIMEOUT                        = (OPAL_ERR_BASE - 15),
    OPAL_ERR_NOT_AVAILABLE                  = (OPAL_ERR_BASE - 16),
    OPAL_ERR_PERM                           = (OPAL_ERR_BASE - 17), /* no permission */
    OPAL_ERR_VALUE_OUT_OF_BOUNDS            = (OPAL_ERR_BASE - 18),
    OPAL_ERR_FILE_READ_FAILURE              = (OPAL_ERR_BASE - 19),
    OPAL_ERR_FILE_WRITE_FAILURE             = (OPAL_ERR_BASE - 20),
    OPAL_ERR_FILE_OPEN_FAILURE              = (OPAL_ERR_BASE - 21),
    OPAL_ERR_PACK_MISMATCH                  = (OPAL_ERR_BASE - 22),
    OPAL_ERR_PACK_FAILURE                   = (OPAL_ERR_BASE - 23),
    OPAL_ERR_UNPACK_FAILURE                 = (OPAL_ERR_BASE - 24),
    OPAL_ERR_UNPACK_INADEQUATE_SPACE        = (OPAL_ERR_BASE - 25),
    OPAL_ERR_UNPACK_READ_PAST_END_OF_BUFFER = (OPAL_ERR_BASE - 26),
    OPAL_ERR_TYPE_MISMATCH                  = (OPAL_ERR_BASE - 27),
    OPAL_ERR_OPERATION_UNSUPPORTED          = (OPAL_ERR_BASE - 28),
    OPAL_ERR_UNKNOWN_DATA_TYPE              = (OPAL_ERR_BASE - 29),
    OPAL_ERR_BUFFER                         = (OPAL_ERR_BASE - 30),
    OPAL_ERR_DATA_TYPE_REDEF                = (OPAL_ERR_BASE - 31),
    OPAL_ERR_DATA_OVERWRITE_ATTEMPT         = (OPAL_ERR_BASE - 32),
    OPAL_ERR_MODULE_NOT_FOUND               = (OPAL_ERR_BASE - 33),
    OPAL_ERR_TOPO_SLOT_LIST_NOT_SUPPORTED   = (OPAL_ERR_BASE - 34),
    OPAL_ERR_TOPO_SOCKET_NOT_SUPPORTED      = (OPAL_ERR_BASE - 35),
    OPAL_ERR_TOPO_CORE_NOT_SUPPORTED        = (OPAL_ERR_BASE - 36),
    OPAL_ERR_NOT_ENOUGH_SOCKETS             = (OPAL_ERR_BASE - 37),
    OPAL_ERR_NOT_ENOUGH_CORES               = (OPAL_ERR_BASE - 38),
    OPAL_ERR_INVALID_PHYS_CPU               = (OPAL_ERR_BASE - 39),
    OPAL_ERR_MULTIPLE_AFFINITIES            = (OPAL_ERR_BASE - 40),
    OPAL_ERR_SLOT_LIST_RANGE                = (OPAL_ERR_BASE - 41),
    OPAL_ERR_NETWORK_NOT_PARSEABLE          = (OPAL_ERR_BASE - 42),
    OPAL_ERR_SILENT                         = (OPAL_ERR_BASE - 43),
    OPAL_ERR_NOT_INITIALIZED                = (OPAL_ERR_BASE - 44),
    OPAL_ERR_NOT_BOUND                      = (OPAL_ERR_BASE - 45),
    OPAL_ERR_TAKE_NEXT_OPTION               = (OPAL_ERR_BASE - 46),
    OPAL_ERR_PROC_ENTRY_NOT_FOUND           = (OPAL_ERR_BASE - 47),
    OPAL_ERR_DATA_VALUE_NOT_FOUND           = (OPAL_ERR_BASE - 48),
    OPAL_ERR_CONNECTION_FAILED              = (OPAL_ERR_BASE - 49),
    OPAL_ERR_AUTHENTICATION_FAILED          = (OPAL_ERR_BASE - 50),
    OPAL_ERR_COMM_FAILURE                   = (OPAL_ERR_BASE - 51),
    OPAL_ERR_SERVER_NOT_AVAIL               = (OPAL_ERR_BASE - 52),
    OPAL_ERR_IN_PROCESS                     = (OPAL_ERR_BASE - 53),
    /* PMIx equivalents for notification support */
    OPAL_ERR_DEBUGGER_RELEASE               = (OPAL_ERR_BASE - 54),
    OPAL_ERR_HANDLERS_COMPLETE              = (OPAL_ERR_BASE - 55),
    OPAL_ERR_PARTIAL_SUCCESS                = (OPAL_ERR_BASE - 56),
    OPAL_ERR_PROC_ABORTED                   = (OPAL_ERR_BASE - 57),
    OPAL_ERR_PROC_REQUESTED_ABORT           = (OPAL_ERR_BASE - 58),
    OPAL_ERR_PROC_ABORTING                  = (OPAL_ERR_BASE - 59),
    OPAL_ERR_NODE_DOWN                      = (OPAL_ERR_BASE - 60),
    OPAL_ERR_NODE_OFFLINE                   = (OPAL_ERR_BASE - 61),
    OPAL_ERR_JOB_TERMINATED                 = (OPAL_ERR_BASE - 62),
    OPAL_ERR_PROC_RESTART                   = (OPAL_ERR_BASE - 63),
    OPAL_ERR_PROC_CHECKPOINT                = (OPAL_ERR_BASE - 64),
    OPAL_ERR_PROC_MIGRATE                   = (OPAL_ERR_BASE - 65),
    OPAL_ERR_EVENT_REGISTRATION             = (OPAL_ERR_BASE - 66),
    OPAL_ERR_HEARTBEAT_ALERT                = (OPAL_ERR_BASE - 67),
    OPAL_ERR_FILE_ALERT                     = (OPAL_ERR_BASE - 68),
    OPAL_ERR_MODEL_DECLARED                 = (OPAL_ERR_BASE - 69),
    OPAL_PMIX_LAUNCH_DIRECTIVE              = (OPAL_ERR_BASE - 70),
    OPAL_OPERATION_SUCCEEDED                = (OPAL_ERR_BASE - 71)
};

#define OPAL_ERR_MAX                (OPAL_ERR_BASE - 100)

#endif /* OPAL_CONSTANTS_H */
