/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 * Copyright (c) 2009      Sun Microsystems, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
#ifndef OMPI_REQUEST_DBG_H
#define OMPI_REQUEST_DBG_H

/*
 * This file contains definitions used by both OMPI and debugger plugins.
 * For more information on why we do this see the Notice to developers
 * comment at the top of the ompi_msgq_dll.c file.
 */

/**
 * Enum inidicating the type of the request
 */
typedef enum {
    OMPI_REQUEST_PML,      /**< MPI point-to-point request */
    OMPI_REQUEST_IO,       /**< MPI-2 IO request */
    OMPI_REQUEST_GEN,      /**< MPI-2 generalized request */
    OMPI_REQUEST_WIN,      /**< MPI-2 one-sided request */
    OMPI_REQUEST_COLL,     /**< MPI-3 non-blocking collectives request */
    OMPI_REQUEST_NULL,     /**< NULL request */
    OMPI_REQUEST_NOOP,     /**< A request that does nothing (e.g., to PROC_NULL) */
    OMPI_REQUEST_COMM,     /**< MPI-3 non-blocking communicator duplication */
    OMPI_REQUEST_MAX       /**< Maximum request type */
} ompi_request_type_t;

/**
 * Enum indicating the state of the request
 */
typedef enum {
    /** Indicates that the request should not be progressed */
    OMPI_REQUEST_INVALID,
    /** A defined, but inactive request (i.e., it's valid, but should
        not be progressed) */
    OMPI_REQUEST_INACTIVE,
    /** A valid and progressing request */
    OMPI_REQUEST_ACTIVE,
    /** The request has been cancelled */
    OMPI_REQUEST_CANCELLED
} ompi_request_state_t;

#endif
