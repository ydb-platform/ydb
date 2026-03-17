/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2011 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC.  All rights
 *                         reseved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include <stdio.h>
#include <string.h>
#include "mpi.h"

#include "ompi/errhandler/errcode-internal.h"

/* Table holding all error codes */
opal_pointer_array_t ompi_errcodes_intern = {{0}};
int ompi_errcode_intern_lastused=0;

static ompi_errcode_intern_t ompi_success_intern;
static ompi_errcode_intern_t ompi_error_intern;
static ompi_errcode_intern_t ompi_err_out_of_resource_intern;
static ompi_errcode_intern_t ompi_err_temp_out_of_resource_intern;
static ompi_errcode_intern_t ompi_err_resource_busy_intern;
static ompi_errcode_intern_t ompi_err_bad_param_intern;
static ompi_errcode_intern_t ompi_err_fatal_intern;
static ompi_errcode_intern_t ompi_err_not_implemented_intern;
static ompi_errcode_intern_t ompi_err_not_supported_intern;
static ompi_errcode_intern_t ompi_err_interupted_intern;
static ompi_errcode_intern_t ompi_err_would_block_intern;
static ompi_errcode_intern_t ompi_err_in_errno_intern;
static ompi_errcode_intern_t ompi_err_unreach_intern;
static ompi_errcode_intern_t ompi_err_not_found_intern;
static ompi_errcode_intern_t ompi_err_request_intern;
static ompi_errcode_intern_t ompi_err_buffer_intern;
static ompi_errcode_intern_t ompi_err_rma_sync_intern;
static ompi_errcode_intern_t ompi_err_rma_shared_intern;
static ompi_errcode_intern_t ompi_err_rma_attach_intern;
static ompi_errcode_intern_t ompi_err_rma_range_intern;
static ompi_errcode_intern_t ompi_err_rma_conflict_intern;
static ompi_errcode_intern_t ompi_err_win_intern;
static ompi_errcode_intern_t ompi_err_rma_flavor_intern;

static void ompi_errcode_intern_construct(ompi_errcode_intern_t* errcode);
static void ompi_errcode_intern_destruct(ompi_errcode_intern_t* errcode);

OBJ_CLASS_INSTANCE(ompi_errcode_intern_t,opal_object_t,ompi_errcode_intern_construct, ompi_errcode_intern_destruct);

int ompi_errcode_intern_init (void)
{
    int pos=0;
    /* Initialize the pointer array, which will hold the references to
       the error objects */
    OBJ_CONSTRUCT(&ompi_errcodes_intern, opal_pointer_array_t);
    if( OPAL_SUCCESS != opal_pointer_array_init(&ompi_errcodes_intern,
                                                0, OMPI_FORTRAN_HANDLE_MAX, 64) ) {
        return OMPI_ERROR;
    }

    /* Initialize now each predefined error code and register
       it in the pointer-array. */
    OBJ_CONSTRUCT(&ompi_success_intern, ompi_errcode_intern_t);
    ompi_success_intern.code = OMPI_SUCCESS;
    ompi_success_intern.mpi_code = MPI_SUCCESS;
    ompi_success_intern.index = pos++;
    strncpy(ompi_success_intern.errstring, "OMPI_SUCCESS", OMPI_MAX_ERROR_STRING);
    opal_pointer_array_set_item(&ompi_errcodes_intern, ompi_success_intern.index,
                                &ompi_success_intern);

    OBJ_CONSTRUCT(&ompi_error_intern, ompi_errcode_intern_t);
    ompi_error_intern.code = OMPI_ERROR;
    ompi_error_intern.mpi_code = MPI_ERR_OTHER;
    ompi_error_intern.index = pos++;
    strncpy(ompi_error_intern.errstring, "OMPI_ERROR", OMPI_MAX_ERROR_STRING);
    opal_pointer_array_set_item(&ompi_errcodes_intern, ompi_error_intern.index,
                                &ompi_error_intern);

    OBJ_CONSTRUCT(&ompi_err_out_of_resource_intern, ompi_errcode_intern_t);
    ompi_err_out_of_resource_intern.code = OMPI_ERR_OUT_OF_RESOURCE;
    ompi_err_out_of_resource_intern.mpi_code = MPI_ERR_INTERN;
    ompi_err_out_of_resource_intern.index = pos++;
    strncpy(ompi_err_out_of_resource_intern.errstring, "OMPI_ERR_OUT_OF_RESOURCE", OMPI_MAX_ERROR_STRING);
    opal_pointer_array_set_item(&ompi_errcodes_intern, ompi_err_out_of_resource_intern.index,
                                &ompi_err_out_of_resource_intern);

    OBJ_CONSTRUCT(&ompi_err_temp_out_of_resource_intern, ompi_errcode_intern_t);
    ompi_err_temp_out_of_resource_intern.code = OMPI_ERR_TEMP_OUT_OF_RESOURCE;
    ompi_err_temp_out_of_resource_intern.mpi_code = MPI_ERR_INTERN;
    ompi_err_temp_out_of_resource_intern.index = pos++;
    strncpy(ompi_err_temp_out_of_resource_intern.errstring, "OMPI_ERR_TEMP_OUT_OF_RESOURCE", OMPI_MAX_ERROR_STRING);
    opal_pointer_array_set_item(&ompi_errcodes_intern, ompi_err_temp_out_of_resource_intern.index,
                                &ompi_err_temp_out_of_resource_intern);

    OBJ_CONSTRUCT(&ompi_err_resource_busy_intern, ompi_errcode_intern_t);
    ompi_err_resource_busy_intern.code = OMPI_ERR_RESOURCE_BUSY;
    ompi_err_resource_busy_intern.mpi_code = MPI_ERR_INTERN;
    ompi_err_resource_busy_intern.index = pos++;
    strncpy(ompi_err_resource_busy_intern.errstring, "OMPI_ERR_RESOURCE_BUSY", OMPI_MAX_ERROR_STRING);
    opal_pointer_array_set_item(&ompi_errcodes_intern, ompi_err_resource_busy_intern.index,
                                &ompi_err_resource_busy_intern);

    OBJ_CONSTRUCT(&ompi_err_bad_param_intern, ompi_errcode_intern_t);
    ompi_err_bad_param_intern.code = OMPI_ERR_BAD_PARAM;
    ompi_err_bad_param_intern.mpi_code = MPI_ERR_ARG;
    ompi_err_bad_param_intern.index = pos++;
    strncpy(ompi_err_bad_param_intern.errstring, "OMPI_ERR_BAD_PARAM", OMPI_MAX_ERROR_STRING);
    opal_pointer_array_set_item(&ompi_errcodes_intern, ompi_err_bad_param_intern.index,
                                &ompi_err_bad_param_intern);

    OBJ_CONSTRUCT(&ompi_err_fatal_intern, ompi_errcode_intern_t);
    ompi_err_fatal_intern.code = OMPI_ERR_FATAL;
    ompi_err_fatal_intern.mpi_code = MPI_ERR_INTERN;
    ompi_err_fatal_intern.index = pos++;
    strncpy(ompi_err_fatal_intern.errstring, "OMPI_ERR_FATAL", OMPI_MAX_ERROR_STRING);
    opal_pointer_array_set_item(&ompi_errcodes_intern, ompi_err_fatal_intern.index,
                                &ompi_err_fatal_intern);

    OBJ_CONSTRUCT(&ompi_err_not_implemented_intern, ompi_errcode_intern_t);
    ompi_err_not_implemented_intern.code = OMPI_ERR_NOT_IMPLEMENTED;
    ompi_err_not_implemented_intern.mpi_code = MPI_ERR_INTERN;
    ompi_err_not_implemented_intern.index = pos++;
    strncpy(ompi_err_not_implemented_intern.errstring, "OMPI_ERR_NOT_IMPLEMENTED", OMPI_MAX_ERROR_STRING);
    opal_pointer_array_set_item(&ompi_errcodes_intern, ompi_err_not_implemented_intern.index,
                                &ompi_err_not_implemented_intern);

    OBJ_CONSTRUCT(&ompi_err_not_supported_intern, ompi_errcode_intern_t);
    ompi_err_not_supported_intern.code = OMPI_ERR_NOT_SUPPORTED;
    ompi_err_not_supported_intern.mpi_code = MPI_ERR_INTERN;
    ompi_err_not_supported_intern.index = pos++;
    strncpy(ompi_err_not_supported_intern.errstring, "OMPI_ERR_NOT_SUPPORTED", OMPI_MAX_ERROR_STRING);
    opal_pointer_array_set_item(&ompi_errcodes_intern, ompi_err_not_supported_intern.index,
                                &ompi_err_not_supported_intern);

    OBJ_CONSTRUCT(&ompi_err_interupted_intern, ompi_errcode_intern_t);
    ompi_err_interupted_intern.code = OMPI_ERR_INTERUPTED;
    ompi_err_interupted_intern.mpi_code = MPI_ERR_INTERN;
    ompi_err_interupted_intern.index = pos++;
    strncpy(ompi_err_interupted_intern.errstring, "OMPI_ERR_INTERUPTED", OMPI_MAX_ERROR_STRING);
    opal_pointer_array_set_item(&ompi_errcodes_intern, ompi_err_interupted_intern.index,
                                &ompi_err_interupted_intern);

    OBJ_CONSTRUCT(&ompi_err_would_block_intern, ompi_errcode_intern_t);
    ompi_err_would_block_intern.code = OMPI_ERR_WOULD_BLOCK;
    ompi_err_would_block_intern.mpi_code = MPI_ERR_INTERN;
    ompi_err_would_block_intern.index = pos++;
    strncpy(ompi_err_would_block_intern.errstring, "OMPI_ERR_WOULD_BLOCK", OMPI_MAX_ERROR_STRING);
    opal_pointer_array_set_item(&ompi_errcodes_intern, ompi_err_would_block_intern.index,
                                &ompi_err_would_block_intern);

    OBJ_CONSTRUCT(&ompi_err_in_errno_intern, ompi_errcode_intern_t);
    ompi_err_in_errno_intern.code = OMPI_ERR_IN_ERRNO;
    ompi_err_in_errno_intern.mpi_code = MPI_ERR_INTERN;
    ompi_err_in_errno_intern.index = pos++;
    strncpy(ompi_err_in_errno_intern.errstring, "OMPI_ERR_IN_ERRNO", OMPI_MAX_ERROR_STRING);
    opal_pointer_array_set_item(&ompi_errcodes_intern, ompi_err_in_errno_intern.index,
                                &ompi_err_in_errno_intern);

    OBJ_CONSTRUCT(&ompi_err_unreach_intern, ompi_errcode_intern_t);
    ompi_err_unreach_intern.code = OMPI_ERR_UNREACH;
    ompi_err_unreach_intern.mpi_code = MPI_ERR_INTERN;
    ompi_err_unreach_intern.index = pos++;
    strncpy(ompi_err_unreach_intern.errstring, "OMPI_ERR_UNREACH", OMPI_MAX_ERROR_STRING);
    opal_pointer_array_set_item(&ompi_errcodes_intern, ompi_err_unreach_intern.index,
                                &ompi_err_unreach_intern);

    OBJ_CONSTRUCT(&ompi_err_not_found_intern, ompi_errcode_intern_t);
    ompi_err_not_found_intern.code = OMPI_ERR_NOT_FOUND;
    ompi_err_not_found_intern.mpi_code = MPI_ERR_INTERN;
    ompi_err_not_found_intern.index = pos++;
    strncpy(ompi_err_not_found_intern.errstring, "OMPI_ERR_NOT_FOUND", OMPI_MAX_ERROR_STRING);
    opal_pointer_array_set_item(&ompi_errcodes_intern, ompi_err_not_found_intern.index,
                                &ompi_err_not_found_intern);

    OBJ_CONSTRUCT(&ompi_err_buffer_intern, ompi_errcode_intern_t);
    ompi_err_buffer_intern.code = OMPI_ERR_BUFFER;
    ompi_err_buffer_intern.mpi_code = MPI_ERR_BUFFER;
    ompi_err_buffer_intern.index = pos++;
    strncpy(ompi_err_buffer_intern.errstring, "OMPI_ERR_BUFFER", OMPI_MAX_ERROR_STRING);
    opal_pointer_array_set_item(&ompi_errcodes_intern, ompi_err_buffer_intern.index,
                                &ompi_err_buffer_intern);

    OBJ_CONSTRUCT(&ompi_err_request_intern, ompi_errcode_intern_t);
    ompi_err_request_intern.code = OMPI_ERR_REQUEST;
    ompi_err_request_intern.mpi_code = MPI_ERR_REQUEST;
    ompi_err_request_intern.index = pos++;
    strncpy(ompi_err_request_intern.errstring, "OMPI_ERR_REQUEST", OMPI_MAX_ERROR_STRING);
    opal_pointer_array_set_item(&ompi_errcodes_intern, ompi_err_request_intern.index,
                                &ompi_err_request_intern);

    OBJ_CONSTRUCT(&ompi_err_rma_sync_intern, ompi_errcode_intern_t);
    ompi_err_rma_sync_intern.code = OMPI_ERR_RMA_SYNC;
    ompi_err_rma_sync_intern.mpi_code = MPI_ERR_RMA_SYNC;
    ompi_err_rma_sync_intern.index = pos++;
    strncpy(ompi_err_rma_sync_intern.errstring, "OMPI_ERR_RMA_SYNC", OMPI_MAX_ERROR_STRING);
    opal_pointer_array_set_item(&ompi_errcodes_intern, ompi_err_rma_sync_intern.index,
                                &ompi_err_rma_sync_intern);

    OBJ_CONSTRUCT(&ompi_err_rma_shared_intern, ompi_errcode_intern_t);
    ompi_err_rma_shared_intern.code = OMPI_ERR_RMA_SHARED;
    ompi_err_rma_shared_intern.mpi_code = MPI_ERR_RMA_SHARED;
    ompi_err_rma_shared_intern.index = pos++;
    strncpy(ompi_err_rma_shared_intern.errstring, "OMPI_ERR_RMA_SHARED", OMPI_MAX_ERROR_STRING);
    opal_pointer_array_set_item(&ompi_errcodes_intern, ompi_err_rma_shared_intern.index,
                                &ompi_err_rma_shared_intern);

    OBJ_CONSTRUCT(&ompi_err_rma_attach_intern, ompi_errcode_intern_t);
    ompi_err_rma_attach_intern.code = OMPI_ERR_RMA_ATTACH;
    ompi_err_rma_attach_intern.mpi_code = MPI_ERR_RMA_ATTACH;
    ompi_err_rma_attach_intern.index = pos++;
    strncpy(ompi_err_rma_attach_intern.errstring, "OMPI_ERR_RMA_ATTACH", OMPI_MAX_ERROR_STRING);
    opal_pointer_array_set_item(&ompi_errcodes_intern, ompi_err_rma_attach_intern.index,
                                &ompi_err_rma_attach_intern);

    OBJ_CONSTRUCT(&ompi_err_rma_range_intern, ompi_errcode_intern_t);
    ompi_err_rma_range_intern.code = OMPI_ERR_RMA_RANGE;
    ompi_err_rma_range_intern.mpi_code = MPI_ERR_RMA_RANGE;
    ompi_err_rma_range_intern.index = pos++;
    strncpy(ompi_err_rma_range_intern.errstring, "OMPI_ERR_RMA_RANGE", OMPI_MAX_ERROR_STRING);
    opal_pointer_array_set_item(&ompi_errcodes_intern, ompi_err_rma_range_intern.index,
                                &ompi_err_rma_range_intern);

    OBJ_CONSTRUCT(&ompi_err_rma_conflict_intern, ompi_errcode_intern_t);
    ompi_err_rma_conflict_intern.code = OMPI_ERR_RMA_CONFLICT;
    ompi_err_rma_conflict_intern.mpi_code = MPI_ERR_RMA_CONFLICT;
    ompi_err_rma_conflict_intern.index = pos++;
    strncpy(ompi_err_rma_conflict_intern.errstring, "OMPI_ERR_RMA_CONFLICT", OMPI_MAX_ERROR_STRING);
    opal_pointer_array_set_item(&ompi_errcodes_intern, ompi_err_rma_conflict_intern.index,
                                &ompi_err_rma_conflict_intern);

    OBJ_CONSTRUCT(&ompi_err_win_intern, ompi_errcode_intern_t);
    ompi_err_win_intern.code = OMPI_ERR_WIN;
    ompi_err_win_intern.mpi_code = MPI_ERR_WIN;
    ompi_err_win_intern.index = pos++;
    strncpy(ompi_err_win_intern.errstring, "OMPI_ERR_WIN", OMPI_MAX_ERROR_STRING);
    opal_pointer_array_set_item(&ompi_errcodes_intern, ompi_err_win_intern.index,
                                &ompi_err_win_intern);

    OBJ_CONSTRUCT(&ompi_err_rma_flavor_intern, ompi_errcode_intern_t);
    ompi_err_rma_flavor_intern.code = OMPI_ERR_RMA_FLAVOR;
    ompi_err_rma_flavor_intern.mpi_code = MPI_ERR_RMA_FLAVOR;
    ompi_err_rma_flavor_intern.index = pos++;
    strncpy(ompi_err_rma_flavor_intern.errstring, "OMPI_ERR_RMA_FLAVOR", OMPI_MAX_ERROR_STRING);
    opal_pointer_array_set_item(&ompi_errcodes_intern, ompi_err_rma_flavor_intern.index,
                                &ompi_err_rma_flavor_intern);

    ompi_errcode_intern_lastused=pos;
    return OMPI_SUCCESS;
}

int ompi_errcode_intern_finalize(void)
{

    OBJ_DESTRUCT(&ompi_success_intern);
    OBJ_DESTRUCT(&ompi_error_intern);
    OBJ_DESTRUCT(&ompi_err_out_of_resource_intern);
    OBJ_DESTRUCT(&ompi_err_temp_out_of_resource_intern);
    OBJ_DESTRUCT(&ompi_err_resource_busy_intern);
    OBJ_DESTRUCT(&ompi_err_bad_param_intern);
    OBJ_DESTRUCT(&ompi_err_fatal_intern);
    OBJ_DESTRUCT(&ompi_err_not_implemented_intern);
    OBJ_DESTRUCT(&ompi_err_not_supported_intern);
    OBJ_DESTRUCT(&ompi_err_interupted_intern);
    OBJ_DESTRUCT(&ompi_err_would_block_intern);
    OBJ_DESTRUCT(&ompi_err_in_errno_intern);
    OBJ_DESTRUCT(&ompi_err_unreach_intern);
    OBJ_DESTRUCT(&ompi_err_not_found_intern);
    OBJ_DESTRUCT(&ompi_err_buffer_intern);
    OBJ_DESTRUCT(&ompi_err_request_intern);
    OBJ_DESTRUCT(&ompi_err_rma_sync_intern);
    OBJ_DESTRUCT(&ompi_err_rma_shared_intern);
    OBJ_DESTRUCT(&ompi_err_rma_attach_intern);
    OBJ_DESTRUCT(&ompi_err_rma_range_intern);
    OBJ_DESTRUCT(&ompi_err_rma_conflict_intern);
    OBJ_DESTRUCT(&ompi_err_win_intern);
    OBJ_DESTRUCT(&ompi_err_rma_flavor_intern);

    OBJ_DESTRUCT(&ompi_errcodes_intern);
    return OMPI_SUCCESS;
}

static void ompi_errcode_intern_construct(ompi_errcode_intern_t *errcode)
{
    errcode->code     = MPI_UNDEFINED;
    errcode->mpi_code = MPI_UNDEFINED;
    errcode->index    = MPI_UNDEFINED;
    memset ( errcode->errstring, 0, OMPI_MAX_ERROR_STRING);
    return;
}

static void ompi_errcode_intern_destruct(ompi_errcode_intern_t *errcode)
{
    opal_pointer_array_set_item(&ompi_errcodes_intern, errcode->index, NULL);
    return;
}
