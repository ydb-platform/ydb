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
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2016      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2017      Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/**
 * @file
 */
#ifndef MCA_PML_BASE_SEND_REQUEST_H
#define MCA_PML_BASE_SEND_REQUEST_H

#include "ompi_config.h"
#include "ompi/mca/pml/pml.h"
#include "ompi/mca/pml/base/pml_base_request.h"
#include "opal/datatype/opal_convertor.h"
#include "ompi/peruse/peruse-internal.h"

BEGIN_C_DECLS

/**
 * Base type for send requests
 */
struct mca_pml_base_send_request_t {
    mca_pml_base_request_t req_base;         /**< base request type - common data structure for use by wait/test */
    const void *req_addr;                    /**< pointer to send buffer - may not be application buffer */
    size_t req_bytes_packed;                 /**< packed size of a message given the datatype and count */
    mca_pml_base_send_mode_t req_send_mode;  /**< type of send */
};
typedef struct mca_pml_base_send_request_t mca_pml_base_send_request_t;

OMPI_DECLSPEC OBJ_CLASS_DECLARATION( mca_pml_base_send_request_t );

/**
 * Initialize a send request with call parameters.
 *
 * @param request (IN)         Send request
 * @param addr (IN)            User buffer
 * @param count (IN)           Number of elements of indicated datatype.
 * @param datatype (IN)        User defined datatype
 * @param peer (IN)            Destination rank
 * @param tag (IN)             User defined tag
 * @param comm (IN)            Communicator
 * @param mode (IN)            Send mode (STANDARD,BUFFERED,SYNCHRONOUS,READY)
 * @param persistent (IN)      Is request persistent.
 * @param convertor_flags (IN) Flags to pass to convertor
 *
 * Perform a any one-time initialization. Note that per-use initialization
 * is done in the send request start routine.
 */

#define MCA_PML_BASE_SEND_REQUEST_INIT( request,                          \
                                        addr,                             \
                                        count,                            \
                                        datatype,                         \
                                        peer,                             \
                                        tag,                              \
                                        comm,                             \
                                        mode,                             \
                                        persistent,                       \
                                        convertor_flags)                  \
   {                                                                      \
      /* increment reference counts */                                    \
      OBJ_RETAIN(comm);                                                   \
                                                                          \
      OMPI_REQUEST_INIT(&(request)->req_base.req_ompi, persistent);       \
      (request)->req_base.req_ompi.req_mpi_object.comm = comm;            \
      (request)->req_addr = addr;                                         \
      (request)->req_send_mode = mode;                                    \
      (request)->req_base.req_addr = (void *)addr;                        \
      (request)->req_base.req_count = count;                              \
      (request)->req_base.req_datatype = datatype;                        \
      (request)->req_base.req_peer = (int32_t)peer;                       \
      (request)->req_base.req_tag = (int32_t)tag;                         \
      (request)->req_base.req_comm = comm;                                \
      /* (request)->req_base.req_proc is set on request allocation */     \
      (request)->req_base.req_pml_complete = false;                       \
      (request)->req_base.req_free_called = false;                        \
      (request)->req_base.req_ompi.req_status._cancelled = 0;             \
      (request)->req_bytes_packed = 0;                                    \
                                                                          \
      /* initialize datatype convertor for this request */                \
      if( count > 0 ) {                                                   \
          OMPI_DATATYPE_RETAIN(datatype);                                 \
         /* We will create a convertor specialized for the        */      \
         /* remote architecture and prepared with the datatype.   */      \
         opal_convertor_copy_and_prepare_for_send(                        \
                            (request)->req_base.req_proc->super.proc_convertor, \
                            &((request)->req_base.req_datatype->super),   \
                            (request)->req_base.req_count,                \
                            (request)->req_base.req_addr,                 \
                            convertor_flags,                              \
                            &(request)->req_base.req_convertor );         \
         opal_convertor_get_packed_size( &(request)->req_base.req_convertor, \
                                         &((request)->req_bytes_packed) );\
      }                                                                   \
   }

#define MCA_PML_BASE_SEND_REQUEST_RESET(request)                        \
    if ((request)->req_bytes_packed > 0) {                              \
        size_t cnt = 0;                                                 \
        opal_convertor_set_position(&(sendreq)->req_send.req_base.req_convertor, \
                                    &cnt);                      \
    }

/**
 * Mark the request as started from the PML base point of view.
 *
 *  @param request (IN)    The send request.
 */

#define MCA_PML_BASE_SEND_START( request )                    \
    do {                                                      \
        (request)->req_base.req_pml_complete = false;         \
        (request)->req_base.req_ompi.req_complete = REQUEST_PENDING;    \
        (request)->req_base.req_ompi.req_state = OMPI_REQUEST_ACTIVE;   \
        (request)->req_base.req_ompi.req_status._cancelled = 0;         \
        MCA_PML_BASE_SEND_REQUEST_RESET(request);             \
    } while (0)

/**
 *  Release the ref counts on the communicator and datatype.
 *
 *  @param request (IN)    The send request.
 */

#define MCA_PML_BASE_SEND_REQUEST_FINI( request )                         \
    do {                                                                  \
        OMPI_REQUEST_FINI(&(request)->req_base.req_ompi);                 \
        OBJ_RELEASE((request)->req_base.req_comm);                        \
        if( 0 != (request)->req_base.req_count )                          \
            OMPI_DATATYPE_RELEASE((request)->req_base.req_datatype);      \
        opal_convertor_cleanup( &((request)->req_base.req_convertor) );   \
    } while (0)


END_C_DECLS

#endif
