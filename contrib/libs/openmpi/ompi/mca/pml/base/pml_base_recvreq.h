/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2016 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2006      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2016      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/**
 * @file
 */
#ifndef MCA_PML_BASE_RECV_REQUEST_H
#define MCA_PML_BASE_RECV_REQUEST_H

#include "ompi_config.h"
#include "ompi/mca/pml/base/pml_base_request.h"
#include "opal/datatype/opal_convertor.h"
#include "ompi/peruse/peruse-internal.h"

BEGIN_C_DECLS

/**
 * Base type for receive requests.
 */
struct mca_pml_base_recv_request_t {
   mca_pml_base_request_t req_base;  /**< base request */
   size_t req_bytes_packed;          /**< size of message being received */
};
typedef struct mca_pml_base_recv_request_t mca_pml_base_recv_request_t;

OMPI_DECLSPEC OBJ_CLASS_DECLARATION(mca_pml_base_recv_request_t);

/**
 * Initialize a receive request with call parameters.
 *
 * @param request (IN)       Receive request.
 * @param addr (IN)          User buffer.
 * @param count (IN)         Number of elements of indicated datatype.
 * @param datatype (IN)      User defined datatype.
 * @param src (IN)           Source rank w/in the communicator.
 * @param tag (IN)           User defined tag.
 * @param comm (IN)          Communicator.
 * @param persistent (IN)    Is this a persistent request.
 */
#define MCA_PML_BASE_RECV_REQUEST_INIT(                                  \
    request,                                                             \
    addr,                                                                \
    count,                                                               \
    datatype,                                                            \
    src,                                                                 \
    tag,                                                                 \
    comm,                                                                \
    persistent)                                                          \
{                                                                        \
    /* increment reference count on communicator */                      \
    OBJ_RETAIN(comm);                                                    \
    OMPI_DATATYPE_RETAIN(datatype);                                      \
                                                                         \
    OMPI_REQUEST_INIT(&(request)->req_base.req_ompi, persistent);        \
    (request)->req_base.req_ompi.req_mpi_object.comm = comm;             \
    (request)->req_bytes_packed = 0;                                     \
    (request)->req_base.req_addr = addr;                                 \
    (request)->req_base.req_count = count;                               \
    (request)->req_base.req_peer = src;                                  \
    (request)->req_base.req_tag = tag;                                   \
    (request)->req_base.req_comm = comm;                                 \
    (request)->req_base.req_proc = NULL;                                 \
    (request)->req_base.req_sequence = 0;                                \
    (request)->req_base.req_datatype = datatype;                         \
    /* What about req_type ? */                                          \
    (request)->req_base.req_pml_complete = false;                        \
    (request)->req_base.req_free_called = false;                         \
}
/**
 *
 *
 */
#define MCA_PML_BASE_RECV_START( request )                                      \
    do {                                                                        \
        (request)->req_bytes_packed = 0;                                        \
        (request)->req_base.req_pml_complete = false;                           \
                                                                                \
        /* always set the req_status.MPI_TAG to ANY_TAG before starting the     \
         * request. This field is used if cancelled to find out if the request  \
         * has been matched or not.                                             \
         */                                                                     \
        (request)->req_base.req_ompi.req_status.MPI_SOURCE = OMPI_ANY_SOURCE;   \
        (request)->req_base.req_ompi.req_status.MPI_TAG = OMPI_ANY_TAG;         \
        (request)->req_base.req_ompi.req_status.MPI_ERROR = OMPI_SUCCESS;       \
        (request)->req_base.req_ompi.req_status._ucount = 0;                    \
        (request)->req_base.req_ompi.req_status._cancelled = 0;                 \
                                                                                \
        (request)->req_base.req_ompi.req_complete = REQUEST_PENDING;            \
        (request)->req_base.req_ompi.req_state = OMPI_REQUEST_ACTIVE;           \
    } while (0)

/**
 *  Return a receive request. Handle the release of the communicator and the
 *  attached datatype.
 *
 *  @param request (IN)     Receive request.
 */
#define MCA_PML_BASE_RECV_REQUEST_FINI( request )                       \
    do {                                                                \
        OMPI_REQUEST_FINI(&(request)->req_base.req_ompi);               \
        OBJ_RELEASE( (request)->req_base.req_comm);                     \
        OMPI_DATATYPE_RELEASE( (request)->req_base.req_datatype );      \
        opal_convertor_cleanup( &((request)->req_base.req_convertor) ); \
    } while (0)

END_C_DECLS

#endif

