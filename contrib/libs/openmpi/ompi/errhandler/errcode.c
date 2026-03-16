/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2017 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2006      University of Houston. All rights reserved.
 * Copyright (c) 2013 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2013      Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
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

#include "ompi/errhandler/errcode.h"
#include "ompi/constants.h"

/* Table holding all error codes */
opal_pointer_array_t ompi_mpi_errcodes = {{0}};
int ompi_mpi_errcode_lastused=0;
int ompi_mpi_errcode_lastpredefined=0;

static ompi_mpi_errcode_t ompi_success;
static ompi_mpi_errcode_t ompi_err_buffer;
static ompi_mpi_errcode_t ompi_err_count;
static ompi_mpi_errcode_t ompi_err_type;
static ompi_mpi_errcode_t ompi_err_tag;
static ompi_mpi_errcode_t ompi_err_comm;
static ompi_mpi_errcode_t ompi_err_rank;
static ompi_mpi_errcode_t ompi_err_request;
static ompi_mpi_errcode_t ompi_err_root;
static ompi_mpi_errcode_t ompi_err_group;
static ompi_mpi_errcode_t ompi_err_op;
static ompi_mpi_errcode_t ompi_err_topology;
static ompi_mpi_errcode_t ompi_err_dims;
static ompi_mpi_errcode_t ompi_err_arg;
ompi_mpi_errcode_t ompi_err_unknown = {{0}};
static ompi_mpi_errcode_t ompi_err_truncate;
static ompi_mpi_errcode_t ompi_err_other;
static ompi_mpi_errcode_t ompi_err_intern;
static ompi_mpi_errcode_t ompi_err_in_status;
static ompi_mpi_errcode_t ompi_err_pending;

static ompi_mpi_errcode_t ompi_err_access;
static ompi_mpi_errcode_t ompi_err_amode;
static ompi_mpi_errcode_t ompi_err_assert;
static ompi_mpi_errcode_t ompi_err_bad_file;
static ompi_mpi_errcode_t ompi_err_base;
static ompi_mpi_errcode_t ompi_err_conversion;
static ompi_mpi_errcode_t ompi_err_disp;
static ompi_mpi_errcode_t ompi_err_dup_datarep;
static ompi_mpi_errcode_t ompi_err_file_exists;
static ompi_mpi_errcode_t ompi_err_file_in_use;
static ompi_mpi_errcode_t ompi_err_file;
static ompi_mpi_errcode_t ompi_err_info_key;
static ompi_mpi_errcode_t ompi_err_info_nokey;
static ompi_mpi_errcode_t ompi_err_info_value;
static ompi_mpi_errcode_t ompi_err_info;
static ompi_mpi_errcode_t ompi_err_io;
static ompi_mpi_errcode_t ompi_err_keyval;
static ompi_mpi_errcode_t ompi_err_locktype;
static ompi_mpi_errcode_t ompi_err_name;
static ompi_mpi_errcode_t ompi_err_no_mem;
static ompi_mpi_errcode_t ompi_err_not_same;
static ompi_mpi_errcode_t ompi_err_no_space;
static ompi_mpi_errcode_t ompi_err_no_such_file;
static ompi_mpi_errcode_t ompi_err_port;
static ompi_mpi_errcode_t ompi_err_quota;
static ompi_mpi_errcode_t ompi_err_read_only;
static ompi_mpi_errcode_t ompi_err_rma_conflict;
static ompi_mpi_errcode_t ompi_err_rma_sync;
static ompi_mpi_errcode_t ompi_err_service;
static ompi_mpi_errcode_t ompi_err_size;
static ompi_mpi_errcode_t ompi_err_spawn;
static ompi_mpi_errcode_t ompi_err_unsupported_datarep;
static ompi_mpi_errcode_t ompi_err_unsupported_operation;
static ompi_mpi_errcode_t ompi_err_win;
static ompi_mpi_errcode_t ompi_t_err_memory;
static ompi_mpi_errcode_t ompi_t_err_not_initialized;
static ompi_mpi_errcode_t ompi_t_err_cannot_init;
static ompi_mpi_errcode_t ompi_t_err_invalid_index;
static ompi_mpi_errcode_t ompi_t_err_invalid_item;
static ompi_mpi_errcode_t ompi_t_err_invalid_handle;
static ompi_mpi_errcode_t ompi_t_err_out_of_handles;
static ompi_mpi_errcode_t ompi_t_err_out_of_sessions;
static ompi_mpi_errcode_t ompi_t_err_invalid_session;
static ompi_mpi_errcode_t ompi_t_err_cvar_set_not_now;
static ompi_mpi_errcode_t ompi_t_err_cvar_set_never;
static ompi_mpi_errcode_t ompi_t_err_pvar_no_startstop;
static ompi_mpi_errcode_t ompi_t_err_pvar_no_write;
static ompi_mpi_errcode_t ompi_t_err_pvar_no_atomic;
static ompi_mpi_errcode_t ompi_err_rma_range;
static ompi_mpi_errcode_t ompi_err_rma_attach;
static ompi_mpi_errcode_t ompi_err_rma_flavor;
static ompi_mpi_errcode_t ompi_err_rma_shared;
static ompi_mpi_errcode_t ompi_t_err_invalid;
static ompi_mpi_errcode_t ompi_t_err_invalid_name;

static void ompi_mpi_errcode_construct(ompi_mpi_errcode_t* errcode);
static void ompi_mpi_errcode_destruct(ompi_mpi_errcode_t* errcode);

OBJ_CLASS_INSTANCE(ompi_mpi_errcode_t,opal_object_t,ompi_mpi_errcode_construct, ompi_mpi_errcode_destruct);

#define CONSTRUCT_ERRCODE(VAR, ERRCODE, TXT )                             \
do {                                                                      \
    OBJ_CONSTRUCT(&(VAR), ompi_mpi_errcode_t);                            \
    (VAR).code = (ERRCODE);                                               \
    (VAR).cls = (ERRCODE);                                                \
    strncpy((VAR).errstring, (TXT), MPI_MAX_ERROR_STRING);                \
    opal_pointer_array_set_item(&ompi_mpi_errcodes, (ERRCODE), &(VAR));   \
} while (0)

int ompi_mpi_errcode_init (void)
{
    /* Initialize the pointer array, which will hold the references to
       the error objects */
    OBJ_CONSTRUCT(&ompi_mpi_errcodes, opal_pointer_array_t);
    if( OPAL_SUCCESS != opal_pointer_array_init(&ompi_mpi_errcodes, 64,
                                                OMPI_FORTRAN_HANDLE_MAX, 32) ) {
        return OMPI_ERROR;
    }

    /* Initialize now each predefined error code and register
       it in the pointer-array. */
    CONSTRUCT_ERRCODE( ompi_success, MPI_SUCCESS, "MPI_SUCCESS: no errors" );
    CONSTRUCT_ERRCODE( ompi_err_buffer, MPI_ERR_BUFFER, "MPI_ERR_BUFFER: invalid buffer pointer");
    CONSTRUCT_ERRCODE( ompi_err_count, MPI_ERR_COUNT, "MPI_ERR_COUNT: invalid count argument" );
    CONSTRUCT_ERRCODE( ompi_err_type, MPI_ERR_TYPE, "MPI_ERR_TYPE: invalid datatype" );
    CONSTRUCT_ERRCODE( ompi_err_tag, MPI_ERR_TAG, "MPI_ERR_TAG: invalid tag" );
    CONSTRUCT_ERRCODE( ompi_err_comm, MPI_ERR_COMM, "MPI_ERR_COMM: invalid communicator" );
    CONSTRUCT_ERRCODE( ompi_err_rank, MPI_ERR_RANK, "MPI_ERR_RANK: invalid rank" );
    CONSTRUCT_ERRCODE( ompi_err_request, MPI_ERR_REQUEST, "MPI_ERR_REQUEST: invalid request" );
    CONSTRUCT_ERRCODE( ompi_err_root, MPI_ERR_ROOT, "MPI_ERR_ROOT: invalid root" );
    CONSTRUCT_ERRCODE( ompi_err_group, MPI_ERR_GROUP, "MPI_ERR_GROUP: invalid group" );
    CONSTRUCT_ERRCODE( ompi_err_op, MPI_ERR_OP, "MPI_ERR_OP: invalid reduce operation" );
    CONSTRUCT_ERRCODE( ompi_err_topology, MPI_ERR_TOPOLOGY, "MPI_ERR_TOPOLOGY: invalid communicator topology" );
    CONSTRUCT_ERRCODE( ompi_err_dims, MPI_ERR_DIMS, "MPI_ERR_DIMS: invalid topology dimension" );
    CONSTRUCT_ERRCODE( ompi_err_arg, MPI_ERR_ARG, "MPI_ERR_ARG: invalid argument of some other kind" );
    CONSTRUCT_ERRCODE( ompi_err_unknown, MPI_ERR_UNKNOWN, "MPI_ERR_UNKNOWN: unknown error" );
    CONSTRUCT_ERRCODE( ompi_err_truncate, MPI_ERR_TRUNCATE, "MPI_ERR_TRUNCATE: message truncated" );
    CONSTRUCT_ERRCODE( ompi_err_other, MPI_ERR_OTHER, "MPI_ERR_OTHER: known error not in list" );
    CONSTRUCT_ERRCODE( ompi_err_intern, MPI_ERR_INTERN, "MPI_ERR_INTERN: internal error" );
    CONSTRUCT_ERRCODE( ompi_err_in_status, MPI_ERR_IN_STATUS, "MPI_ERR_IN_STATUS: error code in status" );
    CONSTRUCT_ERRCODE( ompi_err_pending, MPI_ERR_PENDING, "MPI_ERR_PENDING: pending request" );
    CONSTRUCT_ERRCODE( ompi_err_access, MPI_ERR_ACCESS, "MPI_ERR_ACCESS: invalid access mode" );
    CONSTRUCT_ERRCODE( ompi_err_amode, MPI_ERR_AMODE, "MPI_ERR_AMODE: invalid amode argument" );
    CONSTRUCT_ERRCODE( ompi_err_assert, MPI_ERR_ASSERT, "MPI_ERR_ASSERT: invalid assert argument" );
    CONSTRUCT_ERRCODE( ompi_err_bad_file, MPI_ERR_BAD_FILE, "MPI_ERR_BAD_FILE: bad file" );
    CONSTRUCT_ERRCODE( ompi_err_base, MPI_ERR_BASE, "MPI_ERR_BASE: invalid base" );
    CONSTRUCT_ERRCODE( ompi_err_conversion, MPI_ERR_CONVERSION, "MPI_ERR_CONVERSION: error in data conversion" );
    CONSTRUCT_ERRCODE( ompi_err_disp, MPI_ERR_DISP, "MPI_ERR_DISP: invalid displacement" );
    CONSTRUCT_ERRCODE( ompi_err_dup_datarep, MPI_ERR_DUP_DATAREP, "MPI_ERR_DUP_DATAREP: error duplicating data representation" );
    CONSTRUCT_ERRCODE( ompi_err_file_exists, MPI_ERR_FILE_EXISTS, "MPI_ERR_FILE_EXISTS: file exists alreay" );
    CONSTRUCT_ERRCODE( ompi_err_file_in_use, MPI_ERR_FILE_IN_USE, "MPI_ERR_FILE_IN_USE: file already in use" );
    CONSTRUCT_ERRCODE( ompi_err_file, MPI_ERR_FILE, "MPI_ERR_FILE: invalid file" );
    CONSTRUCT_ERRCODE( ompi_err_info_key, MPI_ERR_INFO_KEY, "MPI_ERR_INFO_KEY: invalid key argument for info object" );
    CONSTRUCT_ERRCODE( ompi_err_info_nokey, MPI_ERR_INFO_NOKEY, "MPI_ERR_INFO_NOKEY: unknown key for given info object" );
    CONSTRUCT_ERRCODE( ompi_err_info_value, MPI_ERR_INFO_VALUE, "MPI_ERR_INFO_VALUE: invalid value argument for info object" );
    CONSTRUCT_ERRCODE( ompi_err_info, MPI_ERR_INFO, "MPI_ERR_INFO: invalid info object" );
    CONSTRUCT_ERRCODE( ompi_err_io, MPI_ERR_IO, "MPI_ERR_IO: input/output error" );
    CONSTRUCT_ERRCODE( ompi_err_keyval, MPI_ERR_KEYVAL, "MPI_ERR_KEYVAL: invalid key value" );
    CONSTRUCT_ERRCODE( ompi_err_locktype, MPI_ERR_LOCKTYPE, "MPI_ERR_LOCKTYPE: invalid lock" );
    CONSTRUCT_ERRCODE( ompi_err_name, MPI_ERR_NAME, "MPI_ERR_NAME: invalid name argument" );
    CONSTRUCT_ERRCODE( ompi_err_no_mem, MPI_ERR_NO_MEM, "MPI_ERR_NO_MEM: out of memory" );
    CONSTRUCT_ERRCODE( ompi_err_not_same, MPI_ERR_NOT_SAME, "MPI_ERR_NOT_SAME: objects are not identical");
    CONSTRUCT_ERRCODE( ompi_err_no_space, MPI_ERR_NO_SPACE, "MPI_ERR_NO_SPACE: no space left on device" );
    CONSTRUCT_ERRCODE( ompi_err_no_such_file, MPI_ERR_NO_SUCH_FILE, "MPI_ERR_NO_SUCH_FILE: no such file or directory" );
    CONSTRUCT_ERRCODE( ompi_err_port, MPI_ERR_PORT, "MPI_ERR_PORT: invalid port" );
    CONSTRUCT_ERRCODE( ompi_err_quota, MPI_ERR_QUOTA, "MPI_ERR_QUOTA: out of quota" );
    CONSTRUCT_ERRCODE( ompi_err_read_only, MPI_ERR_READ_ONLY, "MPI_ERR_READ_ONLY: file is read only" );
    CONSTRUCT_ERRCODE( ompi_err_rma_conflict, MPI_ERR_RMA_CONFLICT, "MPI_ERR_RMA_CONFLICT: rma conflict during operation" );
    CONSTRUCT_ERRCODE( ompi_err_rma_sync, MPI_ERR_RMA_SYNC, "MPI_ERR_RMA_SYNC: error executing rma sync" );
    CONSTRUCT_ERRCODE( ompi_err_service, MPI_ERR_SERVICE, "MPI_ERR_SERVICE: unknown service name" );
    CONSTRUCT_ERRCODE( ompi_err_size, MPI_ERR_SIZE, "MPI_ERR_SIZE: invalid size" );
    CONSTRUCT_ERRCODE( ompi_err_spawn, MPI_ERR_SPAWN, "MPI_ERR_SPAWN: could not spawn processes" );
    CONSTRUCT_ERRCODE( ompi_err_unsupported_datarep, MPI_ERR_UNSUPPORTED_DATAREP, "MPI_ERR_UNSUPPORTED_DATAREP: data representation not supported" );
    CONSTRUCT_ERRCODE( ompi_err_unsupported_operation, MPI_ERR_UNSUPPORTED_OPERATION, "MPI_ERR_UNSUPPORTED_OPERATION: operation not supported" );
    CONSTRUCT_ERRCODE( ompi_err_win, MPI_ERR_WIN, "MPI_ERR_WIN: invalid window" );
    CONSTRUCT_ERRCODE( ompi_t_err_memory, MPI_T_ERR_MEMORY, "MPI_T_ERR_MEMORY: out of memory" );
    CONSTRUCT_ERRCODE( ompi_t_err_not_initialized, MPI_T_ERR_NOT_INITIALIZED, "MPI_T_ERR_NOT_INITIALIZED: interface not initialized" );
    CONSTRUCT_ERRCODE( ompi_t_err_cannot_init, MPI_T_ERR_CANNOT_INIT, "MPI_T_ERR_CANNOT_INIT: interface not in the state to be initialized" );
    CONSTRUCT_ERRCODE( ompi_t_err_invalid_index, MPI_T_ERR_INVALID_INDEX, "MPI_T_ERR_INVALID_INDEX: invalid index" );
    CONSTRUCT_ERRCODE( ompi_t_err_invalid_item, MPI_T_ERR_INVALID_ITEM, "MPI_T_ERR_INVALID_ITEM: the item index queried is out of range" );
    CONSTRUCT_ERRCODE( ompi_t_err_invalid_handle, MPI_T_ERR_INVALID_HANDLE, "MPI_T_ERR_INVALID_HANDLE: the handle is invalid" );
    CONSTRUCT_ERRCODE( ompi_t_err_out_of_handles, MPI_T_ERR_OUT_OF_HANDLES, "MPI_T_ERR_OUT_OF_HANDLES: no more handles available" );
    CONSTRUCT_ERRCODE( ompi_t_err_out_of_sessions, MPI_T_ERR_OUT_OF_SESSIONS, "MPI_T_ERR_OUT_OF_SESSIONS: no more sessions available" );
    CONSTRUCT_ERRCODE( ompi_t_err_invalid_session, MPI_T_ERR_INVALID_SESSION, "MPI_T_ERR_INVALID_SESSION: session argument is not a valid session" );
    CONSTRUCT_ERRCODE( ompi_t_err_cvar_set_not_now, MPI_T_ERR_CVAR_SET_NOT_NOW, "MPI_T_ERR_CVAR_SET_NOT_NOW: variable cannot be set at this moment" );
    CONSTRUCT_ERRCODE( ompi_t_err_cvar_set_never, MPI_T_ERR_CVAR_SET_NEVER, "MPI_T_ERR_CVAR_SET_NEVER: variable cannot be set until end of execution" );
    CONSTRUCT_ERRCODE( ompi_t_err_pvar_no_startstop, MPI_T_ERR_PVAR_NO_STARTSTOP, "MPI_T_ERR_PVAR_NO_STARTSTOP: variable cannot be started or stopped" );
    CONSTRUCT_ERRCODE( ompi_t_err_pvar_no_write, MPI_T_ERR_PVAR_NO_WRITE, "MPI_T_ERR_PVAR_NO_WRITE: variable cannot be written or reset" );
    CONSTRUCT_ERRCODE( ompi_t_err_pvar_no_atomic, MPI_T_ERR_PVAR_NO_ATOMIC, "MPI_T_ERR_PVAR_NO_ATOMIC: variable cannot be read and written atomically" );
    CONSTRUCT_ERRCODE( ompi_err_rma_range, MPI_ERR_RMA_RANGE, "MPI_ERR_RMA_RANGE: invalid RMA address range" );
    CONSTRUCT_ERRCODE( ompi_err_rma_attach, MPI_ERR_RMA_ATTACH, "MPI_ERR_RMA_ATTACH: Could not attach RMA segment" );
    CONSTRUCT_ERRCODE( ompi_err_rma_flavor, MPI_ERR_RMA_FLAVOR, "MPI_ERR_RMA_FLAVOR: Invalid type of window" );
    CONSTRUCT_ERRCODE( ompi_err_rma_shared, MPI_ERR_RMA_SHARED, "MPI_ERR_RMA_SHARED: Memory cannot be shared" );
    CONSTRUCT_ERRCODE( ompi_t_err_invalid, MPI_T_ERR_INVALID, "MPI_T_ERR_INVALID: Invalid use of the interface or bad parameter value(s)" );
    CONSTRUCT_ERRCODE( ompi_t_err_invalid_name, MPI_T_ERR_INVALID_NAME, "MPI_T_ERR_INVALID_NAME: The variable or category name is invalid" );

    /* Per MPI-3 p353:27-32, MPI_LASTUSEDCODE must be >=
       MPI_ERR_LASTCODE.  So just start it as == MPI_ERR_LASTCODE. */
    ompi_mpi_errcode_lastused = MPI_ERR_LASTCODE;
    ompi_mpi_errcode_lastpredefined = MPI_ERR_LASTCODE;
    return OMPI_SUCCESS;
}

int ompi_mpi_errcode_finalize(void)
{
    int i;
    ompi_mpi_errcode_t *errc;

    for (i=ompi_mpi_errcode_lastpredefined+1; i<=ompi_mpi_errcode_lastused; i++) {
        /*
         * there are some user defined error-codes, which
         * we have to free.
         */
        errc = (ompi_mpi_errcode_t *)opal_pointer_array_get_item(&ompi_mpi_errcodes, i);
        OBJ_RELEASE (errc);
    }

    OBJ_DESTRUCT(&ompi_success);
    OBJ_DESTRUCT(&ompi_err_buffer);
    OBJ_DESTRUCT(&ompi_err_count);
    OBJ_DESTRUCT(&ompi_err_type);
    OBJ_DESTRUCT(&ompi_err_tag);
    OBJ_DESTRUCT(&ompi_err_comm);
    OBJ_DESTRUCT(&ompi_err_rank);
    OBJ_DESTRUCT(&ompi_err_request);
    OBJ_DESTRUCT(&ompi_err_root);
    OBJ_DESTRUCT(&ompi_err_group);
    OBJ_DESTRUCT(&ompi_err_op);
    OBJ_DESTRUCT(&ompi_err_topology);
    OBJ_DESTRUCT(&ompi_err_dims);
    OBJ_DESTRUCT(&ompi_err_arg);
    OBJ_DESTRUCT(&ompi_err_unknown);
    OBJ_DESTRUCT(&ompi_err_truncate);
    OBJ_DESTRUCT(&ompi_err_other);
    OBJ_DESTRUCT(&ompi_err_intern);
    OBJ_DESTRUCT(&ompi_err_in_status);
    OBJ_DESTRUCT(&ompi_err_pending);
    OBJ_DESTRUCT(&ompi_err_access);
    OBJ_DESTRUCT(&ompi_err_amode);
    OBJ_DESTRUCT(&ompi_err_assert);
    OBJ_DESTRUCT(&ompi_err_bad_file);
    OBJ_DESTRUCT(&ompi_err_base);
    OBJ_DESTRUCT(&ompi_err_conversion);
    OBJ_DESTRUCT(&ompi_err_disp);
    OBJ_DESTRUCT(&ompi_err_dup_datarep);
    OBJ_DESTRUCT(&ompi_err_file_exists);
    OBJ_DESTRUCT(&ompi_err_file_in_use);
    OBJ_DESTRUCT(&ompi_err_file);
    OBJ_DESTRUCT(&ompi_err_info_key);
    OBJ_DESTRUCT(&ompi_err_info_nokey);
    OBJ_DESTRUCT(&ompi_err_info_value);
    OBJ_DESTRUCT(&ompi_err_info);
    OBJ_DESTRUCT(&ompi_err_io);
    OBJ_DESTRUCT(&ompi_err_keyval);
    OBJ_DESTRUCT(&ompi_err_locktype);
    OBJ_DESTRUCT(&ompi_err_name);
    OBJ_DESTRUCT(&ompi_err_no_mem);
    OBJ_DESTRUCT(&ompi_err_not_same);
    OBJ_DESTRUCT(&ompi_err_no_space);
    OBJ_DESTRUCT(&ompi_err_no_such_file);
    OBJ_DESTRUCT(&ompi_err_port);
    OBJ_DESTRUCT(&ompi_err_quota);
    OBJ_DESTRUCT(&ompi_err_read_only);
    OBJ_DESTRUCT(&ompi_err_rma_conflict);
    OBJ_DESTRUCT(&ompi_err_rma_sync);
    OBJ_DESTRUCT(&ompi_err_service);
    OBJ_DESTRUCT(&ompi_err_size);
    OBJ_DESTRUCT(&ompi_err_spawn);
    OBJ_DESTRUCT(&ompi_err_unsupported_datarep);
    OBJ_DESTRUCT(&ompi_err_unsupported_operation);
    OBJ_DESTRUCT(&ompi_err_win);
    OBJ_DESTRUCT(&ompi_t_err_memory);
    OBJ_DESTRUCT(&ompi_t_err_not_initialized);
    OBJ_DESTRUCT(&ompi_t_err_cannot_init);
    OBJ_DESTRUCT(&ompi_t_err_invalid_index);
    OBJ_DESTRUCT(&ompi_t_err_invalid_item);
    OBJ_DESTRUCT(&ompi_t_err_invalid_handle);
    OBJ_DESTRUCT(&ompi_t_err_out_of_handles);
    OBJ_DESTRUCT(&ompi_t_err_out_of_sessions);
    OBJ_DESTRUCT(&ompi_t_err_invalid_session);
    OBJ_DESTRUCT(&ompi_t_err_cvar_set_not_now);
    OBJ_DESTRUCT(&ompi_t_err_cvar_set_never);
    OBJ_DESTRUCT(&ompi_t_err_pvar_no_startstop);
    OBJ_DESTRUCT(&ompi_t_err_pvar_no_write);
    OBJ_DESTRUCT(&ompi_t_err_pvar_no_atomic);
    OBJ_DESTRUCT(&ompi_err_rma_range);
    OBJ_DESTRUCT(&ompi_err_rma_attach);
    OBJ_DESTRUCT(&ompi_err_rma_flavor);
    OBJ_DESTRUCT(&ompi_err_rma_shared);
    OBJ_DESTRUCT(&ompi_t_err_invalid);
    OBJ_DESTRUCT(&ompi_t_err_invalid_name);

    OBJ_DESTRUCT(&ompi_mpi_errcodes);
    return OMPI_SUCCESS;
}

int ompi_mpi_errcode_add(int errclass )
{
    ompi_mpi_errcode_t *newerrcode;

    newerrcode = OBJ_NEW(ompi_mpi_errcode_t);
    newerrcode->code = (ompi_mpi_errcode_lastused+1);
    newerrcode->cls = errclass;
    opal_pointer_array_set_item(&ompi_mpi_errcodes, newerrcode->code, newerrcode);

    ompi_mpi_errcode_lastused++;
    return newerrcode->code;
}

int ompi_mpi_errclass_add(void)
{
    ompi_mpi_errcode_t *newerrcode;

    newerrcode = OBJ_NEW(ompi_mpi_errcode_t);
    newerrcode->cls = ( ompi_mpi_errcode_lastused+1);
    opal_pointer_array_set_item(&ompi_mpi_errcodes, newerrcode->cls, newerrcode);

    ompi_mpi_errcode_lastused++;
    return newerrcode->cls;
}

int ompi_mpi_errnum_add_string(int errnum, const char *errstring, int len)
{
    ompi_mpi_errcode_t *errcodep;

    errcodep = (ompi_mpi_errcode_t *)opal_pointer_array_get_item(&ompi_mpi_errcodes, errnum);
    if ( NULL == errcodep ) {
        return OMPI_ERROR;
    }

    if ( MPI_MAX_ERROR_STRING > len ) {
        len = MPI_MAX_ERROR_STRING;
    }

    strncpy ( errcodep->errstring, errstring, len );
    return OMPI_SUCCESS;
}

static void ompi_mpi_errcode_construct(ompi_mpi_errcode_t *errcode)
{
    errcode->code = MPI_UNDEFINED;
    errcode->cls = MPI_UNDEFINED;
    memset ( errcode->errstring, 0, MPI_MAX_ERROR_STRING);
    return;
}

static void ompi_mpi_errcode_destruct(ompi_mpi_errcode_t *errcode)
{
    if (MPI_UNDEFINED != errcode->code) {
        opal_pointer_array_set_item(&ompi_mpi_errcodes, errcode->code, NULL);
    } else if (MPI_UNDEFINED != errcode->cls) {
        opal_pointer_array_set_item(&ompi_mpi_errcodes, errcode->cls, NULL);
    }
}
