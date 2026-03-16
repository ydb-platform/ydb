/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2017 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2007 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2006-2013 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2009      Sun Microsystems, Inc.  All rights reserved.
 * Copyright (c) 2012      Oak Ridge National Labs.  All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2018      FUJITSU LIMITED.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include "ompi/communicator/communicator.h"
#include "opal/class/opal_object.h"
#include "ompi/request/request.h"
#include "ompi/request/request_default.h"
#include "ompi/constants.h"

opal_pointer_array_t             ompi_request_f_to_c_table = {{0}};
ompi_predefined_request_t        ompi_request_null = {{{{{0}}}}};
ompi_predefined_request_t        *ompi_request_null_addr = &ompi_request_null;
ompi_request_t                   ompi_request_empty = {{{{0}}}};
ompi_status_public_t             ompi_status_empty = {0};
ompi_request_fns_t               ompi_request_functions = {
    ompi_request_default_test,
    ompi_request_default_test_any,
    ompi_request_default_test_all,
    ompi_request_default_test_some,
    ompi_request_default_wait,
    ompi_request_default_wait_any,
    ompi_request_default_wait_all,
    ompi_request_default_wait_some
};

static void ompi_request_construct(ompi_request_t* req)
{
    /* don't call _INIT, we don't to set the request to _INACTIVE and there will
     * be no matching _FINI invocation */
    req->req_state        = OMPI_REQUEST_INVALID;
    req->req_complete     = false;
    req->req_persistent   = false;
    req->req_start        = NULL;
    req->req_free         = NULL;
    req->req_cancel       = NULL;
    req->req_complete_cb  = NULL;
    req->req_complete_cb_data = NULL;
    req->req_f_to_c_index = MPI_UNDEFINED;
    req->req_mpi_object.comm = (struct ompi_communicator_t*) NULL;
}

static void ompi_request_destruct(ompi_request_t* req)
{
    assert( MPI_UNDEFINED == req->req_f_to_c_index );
    assert( OMPI_REQUEST_INVALID == req->req_state );
}

static int ompi_request_null_free(ompi_request_t** request)
{
    return OMPI_SUCCESS;
}

static int ompi_request_null_cancel(ompi_request_t* request, int flag)
{
    return OMPI_SUCCESS;
}

static int ompi_request_empty_free(ompi_request_t** request)
{
    *request = &ompi_request_null.request;
    return OMPI_SUCCESS;
}

static int ompi_request_persistent_noop_free(ompi_request_t** request)
{
    OMPI_REQUEST_FINI(*request);
    (*request)->req_state = OMPI_REQUEST_INVALID;
    OBJ_RELEASE(*request);
    *request = &ompi_request_null.request;
    return OMPI_SUCCESS;
}


OBJ_CLASS_INSTANCE(
    ompi_request_t,
    opal_free_list_item_t,
    ompi_request_construct,
    ompi_request_destruct);


int ompi_request_init(void)
{

    OBJ_CONSTRUCT(&ompi_request_null, ompi_request_t);
    OBJ_CONSTRUCT(&ompi_request_f_to_c_table, opal_pointer_array_t);
    if( OPAL_SUCCESS != opal_pointer_array_init(&ompi_request_f_to_c_table,
                                                0, OMPI_FORTRAN_HANDLE_MAX, 32) ) {
        return OMPI_ERROR;
    }
    ompi_request_null.request.req_type = OMPI_REQUEST_NULL;
    ompi_request_null.request.req_status.MPI_SOURCE = MPI_ANY_SOURCE;
    ompi_request_null.request.req_status.MPI_TAG = MPI_ANY_TAG;
    ompi_request_null.request.req_status.MPI_ERROR = MPI_SUCCESS;
    ompi_request_null.request.req_status._ucount = 0;
    ompi_request_null.request.req_status._cancelled = 0;

    ompi_request_null.request.req_complete = REQUEST_COMPLETED;
    ompi_request_null.request.req_state = OMPI_REQUEST_INACTIVE;
    ompi_request_null.request.req_persistent = false;
    ompi_request_null.request.req_f_to_c_index =
        opal_pointer_array_add(&ompi_request_f_to_c_table, &ompi_request_null);
    ompi_request_null.request.req_start = NULL; /* should not be called */
    ompi_request_null.request.req_free = ompi_request_null_free;
    ompi_request_null.request.req_cancel = ompi_request_null_cancel;
    ompi_request_null.request.req_mpi_object.comm = &ompi_mpi_comm_world.comm;

    if (0 != ompi_request_null.request.req_f_to_c_index) {
        return OMPI_ERR_REQUEST;
    }

    /* We need a way to distinguish between the user provided
     * MPI_REQUEST_NULL to MPI_Wait* and a non-active (MPI_PROC_NULL)
     * request passed to any P2P non-blocking function.
     *
     * The main difference to ompi_request_null is
     * req_state being OMPI_REQUEST_ACTIVE, so that MPI_Waitall
     * does not set the status to ompi_status_empty and the different
     * req_free function, which resets the
     * request to MPI_REQUEST_NULL.
     * The req_cancel function need not be changed.
     */
    OBJ_CONSTRUCT(&ompi_request_empty, ompi_request_t);
    ompi_request_empty.req_type = OMPI_REQUEST_NULL;
    ompi_request_empty.req_status.MPI_SOURCE = MPI_PROC_NULL;
    ompi_request_empty.req_status.MPI_TAG = MPI_ANY_TAG;
    ompi_request_empty.req_status.MPI_ERROR = MPI_SUCCESS;
    ompi_request_empty.req_status._ucount = 0;
    ompi_request_empty.req_status._cancelled = 0;

    ompi_request_empty.req_complete = REQUEST_COMPLETED;
    ompi_request_empty.req_state = OMPI_REQUEST_ACTIVE;
    ompi_request_empty.req_persistent = false;
    ompi_request_empty.req_f_to_c_index =
        opal_pointer_array_add(&ompi_request_f_to_c_table, &ompi_request_empty);
    ompi_request_empty.req_start = NULL; /* should not be called */
    ompi_request_empty.req_free = ompi_request_empty_free;
    ompi_request_empty.req_cancel = ompi_request_null_cancel;
    ompi_request_empty.req_mpi_object.comm = &ompi_mpi_comm_world.comm;

    if (1 != ompi_request_empty.req_f_to_c_index) {
        return OMPI_ERR_REQUEST;
    }

    ompi_status_empty.MPI_SOURCE = MPI_ANY_SOURCE;
    ompi_status_empty.MPI_TAG = MPI_ANY_TAG;
    ompi_status_empty.MPI_ERROR = MPI_SUCCESS;
    ompi_status_empty._ucount = 0;
    ompi_status_empty._cancelled = 0;

    return OMPI_SUCCESS;
}


int ompi_request_finalize(void)
{
    OMPI_REQUEST_FINI( &ompi_request_null.request );
    OBJ_DESTRUCT( &ompi_request_null.request );
    OMPI_REQUEST_FINI( &ompi_request_empty );
    OBJ_DESTRUCT( &ompi_request_empty );
    OBJ_DESTRUCT( &ompi_request_f_to_c_table );
    return OMPI_SUCCESS;
}


int ompi_request_persistent_noop_create(ompi_request_t** request)
{
    ompi_request_t *req;

    req = OBJ_NEW(ompi_request_t);
    if (NULL == req) {
        return OMPI_ERR_OUT_OF_RESOURCE;
    }

    /* Other fields were initialized by the constructor for
       ompi_request_t */
    req->req_type = OMPI_REQUEST_NOOP;
    req->req_status = ompi_request_empty.req_status;
    req->req_complete = REQUEST_COMPLETED;
    req->req_state = OMPI_REQUEST_INACTIVE;
    req->req_persistent = true;
    req->req_free = ompi_request_persistent_noop_free;

    *request = req;
    return OMPI_SUCCESS;
}
