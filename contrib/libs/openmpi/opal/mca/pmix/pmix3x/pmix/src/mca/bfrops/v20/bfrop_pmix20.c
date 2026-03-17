/*
 * Copyright (c) 2004-2010 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2011 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2010-2011 Oak Ridge National Labs.  All rights reserved.
 * Copyright (c) 2011-2014 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2011-2013 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2013-2017 Intel, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 */

#include <src/include/pmix_config.h>

#include "src/util/error.h"
#include "src/include/pmix_globals.h"
#include "src/client/pmix_client_ops.h"
#include "src/mca/bfrops/base/base.h"
#include "bfrop_pmix20.h"
#include "internal.h"

static pmix_status_t init(void);
static void finalize(void);
static pmix_status_t register_type(const char *name,
                                   pmix_data_type_t type,
                                   pmix_bfrop_pack_fn_t pack,
                                   pmix_bfrop_unpack_fn_t unpack,
                                   pmix_bfrop_copy_fn_t copy,
                                   pmix_bfrop_print_fn_t print);
static const char* data_type_string(pmix_data_type_t type);

pmix_bfrops_module_t pmix_bfrops_pmix20_module = {
    .version = "v20",
    .init = init,
    .finalize = finalize,
    .pack = pmix20_bfrop_pack,
    .unpack = pmix20_bfrop_unpack,
    .copy = pmix20_bfrop_copy,
    .print = pmix20_bfrop_print,
    .copy_payload = pmix20_bfrop_copy_payload,
    .value_xfer = pmix20_bfrop_value_xfer,
    .value_load = pmix20_bfrop_value_load,
    .value_unload = pmix20_bfrop_value_unload,
    .value_cmp = pmix20_bfrop_value_cmp,
    .register_type = register_type,
    .data_type_string = data_type_string
};

static pmix_status_t init(void)
{
    /* some standard types don't require anything special */
    PMIX_REGISTER_TYPE("PMIX_BOOL", PMIX_BOOL,
                       pmix20_bfrop_pack_bool,
                       pmix20_bfrop_unpack_bool,
                       pmix20_bfrop_std_copy,
                       pmix20_bfrop_print_bool,
                       &mca_bfrops_v20_component.types);

    PMIX_REGISTER_TYPE("PMIX_BYTE", PMIX_BYTE,
                       pmix20_bfrop_pack_byte,
                       pmix20_bfrop_unpack_byte,
                       pmix20_bfrop_std_copy,
                       pmix20_bfrop_print_byte,
                       &mca_bfrops_v20_component.types);

    PMIX_REGISTER_TYPE("PMIX_STRING", PMIX_STRING,
                       pmix20_bfrop_pack_string,
                       pmix20_bfrop_unpack_string,
                       pmix20_bfrop_copy_string,
                       pmix20_bfrop_print_string,
                       &mca_bfrops_v20_component.types);

    /* Register the rest of the standard generic types to point to internal functions */
    PMIX_REGISTER_TYPE("PMIX_SIZE", PMIX_SIZE,
                       pmix20_bfrop_pack_sizet,
                       pmix20_bfrop_unpack_sizet,
                       pmix20_bfrop_std_copy,
                       pmix20_bfrop_print_size,
                       &mca_bfrops_v20_component.types);

    PMIX_REGISTER_TYPE("PMIX_PID", PMIX_PID,
                       pmix20_bfrop_pack_pid,
                       pmix20_bfrop_unpack_pid,
                       pmix20_bfrop_std_copy,
                       pmix20_bfrop_print_pid,
                       &mca_bfrops_v20_component.types);

    PMIX_REGISTER_TYPE("PMIX_INT", PMIX_INT,
                       pmix20_bfrop_pack_int,
                       pmix20_bfrop_unpack_int,
                       pmix20_bfrop_std_copy,
                       pmix20_bfrop_print_int,
                       &mca_bfrops_v20_component.types);

    /* Register all the standard fixed types to point to base functions */
    PMIX_REGISTER_TYPE("PMIX_INT8", PMIX_INT8,
                       pmix20_bfrop_pack_byte,
                       pmix20_bfrop_unpack_byte,
                       pmix20_bfrop_std_copy,
                       pmix20_bfrop_print_int8,
                       &mca_bfrops_v20_component.types);

    PMIX_REGISTER_TYPE("PMIX_INT16", PMIX_INT16,
                       pmix20_bfrop_pack_int16,
                       pmix20_bfrop_unpack_int16,
                       pmix20_bfrop_std_copy,
                       pmix20_bfrop_print_int16,
                       &mca_bfrops_v20_component.types);

    PMIX_REGISTER_TYPE("PMIX_INT32", PMIX_INT32,
                       pmix20_bfrop_pack_int32,
                       pmix20_bfrop_unpack_int32,
                       pmix20_bfrop_std_copy,
                       pmix20_bfrop_print_int32,
                       &mca_bfrops_v20_component.types);

    PMIX_REGISTER_TYPE("PMIX_INT64", PMIX_INT64,
                       pmix20_bfrop_pack_int64,
                       pmix20_bfrop_unpack_int64,
                       pmix20_bfrop_std_copy,
                       pmix20_bfrop_print_int64,
                       &mca_bfrops_v20_component.types);

    PMIX_REGISTER_TYPE("PMIX_UINT", PMIX_UINT,
                       pmix20_bfrop_pack_int,
                       pmix20_bfrop_unpack_int,
                       pmix20_bfrop_std_copy,
                       pmix20_bfrop_print_uint,
                       &mca_bfrops_v20_component.types);

    PMIX_REGISTER_TYPE("PMIX_UINT8", PMIX_UINT8,
                       pmix20_bfrop_pack_byte,
                       pmix20_bfrop_unpack_byte,
                       pmix20_bfrop_std_copy,
                       pmix20_bfrop_print_uint8,
                       &mca_bfrops_v20_component.types);

    PMIX_REGISTER_TYPE("PMIX_UINT16", PMIX_UINT16,
                       pmix20_bfrop_pack_int16,
                       pmix20_bfrop_unpack_int16,
                       pmix20_bfrop_std_copy,
                       pmix20_bfrop_print_uint16,
                       &mca_bfrops_v20_component.types);

    PMIX_REGISTER_TYPE("PMIX_UINT32", PMIX_UINT32,
                       pmix20_bfrop_pack_int32,
                       pmix20_bfrop_unpack_int32,
                       pmix20_bfrop_std_copy,
                       pmix20_bfrop_print_uint32,
                       &mca_bfrops_v20_component.types);

    PMIX_REGISTER_TYPE("PMIX_UINT64", PMIX_UINT64,
                       pmix20_bfrop_pack_int64,
                       pmix20_bfrop_unpack_int64,
                       pmix20_bfrop_std_copy,
                       pmix20_bfrop_print_uint64,
                       &mca_bfrops_v20_component.types);

    PMIX_REGISTER_TYPE("PMIX_FLOAT", PMIX_FLOAT,
                       pmix20_bfrop_pack_float,
                       pmix20_bfrop_unpack_float,
                       pmix20_bfrop_std_copy,
                       pmix20_bfrop_print_float,
                       &mca_bfrops_v20_component.types);

    PMIX_REGISTER_TYPE("PMIX_DOUBLE", PMIX_DOUBLE,
                       pmix20_bfrop_pack_double,
                       pmix20_bfrop_unpack_double,
                       pmix20_bfrop_std_copy,
                       pmix20_bfrop_print_double,
                       &mca_bfrops_v20_component.types);

    PMIX_REGISTER_TYPE("PMIX_TIMEVAL", PMIX_TIMEVAL,
                       pmix20_bfrop_pack_timeval,
                       pmix20_bfrop_unpack_timeval,
                       pmix20_bfrop_std_copy,
                       pmix20_bfrop_print_timeval,
                       &mca_bfrops_v20_component.types);

    PMIX_REGISTER_TYPE("PMIX_TIME", PMIX_TIME,
                       pmix20_bfrop_pack_time,
                       pmix20_bfrop_unpack_time,
                       pmix20_bfrop_std_copy,
                       pmix20_bfrop_print_time,
                       &mca_bfrops_v20_component.types);

    PMIX_REGISTER_TYPE("PMIX_STATUS", PMIX_STATUS,
                       pmix20_bfrop_pack_status,
                       pmix20_bfrop_unpack_status,
                       pmix20_bfrop_std_copy,
                       pmix20_bfrop_print_status,
                       &mca_bfrops_v20_component.types);

    PMIX_REGISTER_TYPE("PMIX_VALUE", PMIX_VALUE,
                       pmix20_bfrop_pack_value,
                       pmix20_bfrop_unpack_value,
                       pmix20_bfrop_copy_value,
                       pmix20_bfrop_print_value,
                       &mca_bfrops_v20_component.types);

    PMIX_REGISTER_TYPE("PMIX_PROC", PMIX_PROC,
                       pmix20_bfrop_pack_proc,
                       pmix20_bfrop_unpack_proc,
                       pmix20_bfrop_copy_proc,
                       pmix20_bfrop_print_proc,
                       &mca_bfrops_v20_component.types);

    PMIX_REGISTER_TYPE("PMIX_APP", PMIX_APP,
                       pmix20_bfrop_pack_app,
                       pmix20_bfrop_unpack_app,
                       pmix20_bfrop_copy_app,
                       pmix20_bfrop_print_app,
                       &mca_bfrops_v20_component.types);

    PMIX_REGISTER_TYPE("PMIX_INFO", PMIX_INFO,
                       pmix20_bfrop_pack_info,
                       pmix20_bfrop_unpack_info,
                       pmix20_bfrop_copy_info,
                       pmix20_bfrop_print_info,
                       &mca_bfrops_v20_component.types);

    PMIX_REGISTER_TYPE("PMIX_PDATA", PMIX_PDATA,
                       pmix20_bfrop_pack_pdata,
                       pmix20_bfrop_unpack_pdata,
                       pmix20_bfrop_copy_pdata,
                       pmix20_bfrop_print_pdata,
                       &mca_bfrops_v20_component.types);

    PMIX_REGISTER_TYPE("PMIX_BUFFER", PMIX_BUFFER,
                       pmix20_bfrop_pack_buf,
                       pmix20_bfrop_unpack_buf,
                       pmix20_bfrop_copy_buf,
                       pmix20_bfrop_print_buf,
                       &mca_bfrops_v20_component.types);

    PMIX_REGISTER_TYPE("PMIX_BYTE_OBJECT", PMIX_BYTE_OBJECT,
                       pmix20_bfrop_pack_bo,
                       pmix20_bfrop_unpack_bo,
                       pmix20_bfrop_copy_bo,
                       pmix20_bfrop_print_bo,
                       &mca_bfrops_v20_component.types);

    PMIX_REGISTER_TYPE("PMIX_KVAL", PMIX_KVAL,
                       pmix20_bfrop_pack_kval,
                       pmix20_bfrop_unpack_kval,
                       pmix20_bfrop_copy_kval,
                       pmix20_bfrop_print_kval,
                       &mca_bfrops_v20_component.types);

    PMIX_REGISTER_TYPE("PMIX_MODEX", PMIX_MODEX,
                       pmix20_bfrop_pack_modex,
                       pmix20_bfrop_unpack_modex,
                       pmix20_bfrop_copy_modex,
                       pmix20_bfrop_print_modex,
                       &mca_bfrops_v20_component.types);

    /* these are fixed-sized values and can be done by base */
    PMIX_REGISTER_TYPE("PMIX_PERSIST", PMIX_PERSIST,
                       pmix20_bfrop_pack_persist,
                       pmix20_bfrop_unpack_persist,
                       pmix20_bfrop_std_copy,
                       pmix20_bfrop_print_persist,
                       &mca_bfrops_v20_component.types);

    PMIX_REGISTER_TYPE("PMIX_POINTER", PMIX_POINTER,
                       pmix20_bfrop_pack_ptr,
                       pmix20_bfrop_unpack_ptr,
                       pmix20_bfrop_std_copy,
                       pmix20_bfrop_print_ptr,
                       &mca_bfrops_v20_component.types);

    PMIX_REGISTER_TYPE("PMIX_SCOPE", PMIX_SCOPE,
                       pmix20_bfrop_pack_scope,
                       pmix20_bfrop_unpack_scope,
                       pmix20_bfrop_std_copy,
                       pmix20_bfrop_std_copy,
                       &mca_bfrops_v20_component.types);

    PMIX_REGISTER_TYPE("PMIX_DATA_RANGE", PMIX_DATA_RANGE,
                       pmix20_bfrop_pack_range,
                       pmix20_bfrop_unpack_range,
                       pmix20_bfrop_std_copy,
                       pmix20_bfrop_print_ptr,
                       &mca_bfrops_v20_component.types);

    PMIX_REGISTER_TYPE("PMIX_COMMAND", PMIX_COMMAND,
                       pmix20_bfrop_pack_cmd,
                       pmix20_bfrop_unpack_cmd,
                       pmix20_bfrop_std_copy,
                       pmix20_bfrop_print_cmd,
                       &mca_bfrops_v20_component.types);

    PMIX_REGISTER_TYPE("PMIX_INFO_DIRECTIVES", PMIX_INFO_DIRECTIVES,
                       pmix20_bfrop_pack_infodirs,
                       pmix20_bfrop_unpack_infodirs,
                       pmix20_bfrop_std_copy,
                       pmix20_bfrop_print_infodirs,
                       &mca_bfrops_v20_component.types);

    PMIX_REGISTER_TYPE("PMIX_DATA_TYPE", PMIX_DATA_TYPE,
                       pmix20_bfrop_pack_datatype,
                       pmix20_bfrop_unpack_datatype,
                       pmix20_bfrop_std_copy,
                       pmix_bfrops_base_print_datatype,
                       &mca_bfrops_v20_component.types);

    PMIX_REGISTER_TYPE("PMIX_PROC_STATE", PMIX_PROC_STATE,
                       pmix20_bfrop_pack_pstate,
                       pmix20_bfrop_unpack_pstate,
                       pmix20_bfrop_std_copy,
                       pmix20_bfrop_print_pstate,
                       &mca_bfrops_v20_component.types);

    PMIX_REGISTER_TYPE("PMIX_PROC_INFO", PMIX_PROC_INFO,
                       pmix20_bfrop_pack_pinfo,
                       pmix20_bfrop_unpack_pinfo,
                       pmix20_bfrop_copy_pinfo,
                       pmix20_bfrop_print_pinfo,
                       &mca_bfrops_v20_component.types);

    PMIX_REGISTER_TYPE("PMIX_DATA_ARRAY", PMIX_DATA_ARRAY,
                       pmix20_bfrop_pack_darray,
                       pmix20_bfrop_unpack_darray,
                       pmix20_bfrop_copy_darray,
                       pmix20_bfrop_print_darray,
                       &mca_bfrops_v20_component.types);

    PMIX_REGISTER_TYPE("PMIX_PROC_RANK", PMIX_PROC_RANK,
                       pmix20_bfrop_pack_rank,
                       pmix20_bfrop_unpack_rank,
                       pmix20_bfrop_std_copy,
                       pmix20_bfrop_print_rank,
                       &mca_bfrops_v20_component.types);

    PMIX_REGISTER_TYPE("PMIX_QUERY", PMIX_QUERY,
                       pmix20_bfrop_pack_query,
                       pmix20_bfrop_unpack_query,
                       pmix20_bfrop_copy_query,
                       pmix20_bfrop_print_query,
                       &mca_bfrops_v20_component.types);

    PMIX_REGISTER_TYPE("PMIX_COMPRESSED_STRING",
                       PMIX_COMPRESSED_STRING,
                       pmix20_bfrop_pack_bo,
                       pmix20_bfrop_unpack_bo,
                       pmix20_bfrop_copy_bo,
                       pmix20_bfrop_print_bo,
                       &mca_bfrops_v20_component.types);

    PMIX_REGISTER_TYPE("PMIX_ALLOC_DIRECTIVE",
                       PMIX_ALLOC_DIRECTIVE,
                       pmix20_bfrop_pack_alloc_directive,
                       pmix20_bfrop_unpack_alloc_directive,
                       pmix20_bfrop_std_copy,
                       pmix20_bfrop_print_alloc_directive,
                       &mca_bfrops_v20_component.types);

    /**** DEPRECATED ****/
    PMIX_REGISTER_TYPE("PMIX_INFO_ARRAY", PMIX_INFO_ARRAY,
                       pmix20_bfrop_pack_array,
                       pmix20_bfrop_unpack_array,
                       pmix20_bfrop_copy_array,
                       pmix20_bfrop_print_array,
                       &mca_bfrops_v20_component.types);
    /********************/


    return PMIX_SUCCESS;
}

static void finalize(void)
{
    int n;
    pmix_bfrop_type_info_t *info;

    for (n=0; n < mca_bfrops_v20_component.types.size; n++) {
        if (NULL != (info = (pmix_bfrop_type_info_t*)pmix_pointer_array_get_item(&mca_bfrops_v20_component.types, n))) {
            PMIX_RELEASE(info);
            pmix_pointer_array_set_item(&mca_bfrops_v20_component.types, n, NULL);
        }
    }
}

static pmix_status_t register_type(const char *name, pmix_data_type_t type,
                                   pmix_bfrop_pack_fn_t pack,
                                   pmix_bfrop_unpack_fn_t unpack,
                                   pmix_bfrop_copy_fn_t copy,
                                   pmix_bfrop_print_fn_t print)
{
    PMIX_REGISTER_TYPE(name, type,
                       pack, unpack,
                       copy, print,
                       &mca_bfrops_v20_component.types);
    return PMIX_SUCCESS;
}

static const char* data_type_string(pmix_data_type_t type)
{
    pmix_bfrop_type_info_t *info;

    if (NULL == (info = (pmix_bfrop_type_info_t*)pmix_pointer_array_get_item(&mca_bfrops_v20_component.types, type))) {
        return NULL;
    }
    return info->odti_name;
}

pmix_data_type_t pmix20_v21_to_v20_datatype(pmix_data_type_t v21type)
{
    pmix_data_type_t v20type;

    /* iI'm passing the data type to
     * a PMIx v20 compatible client. The data type was redefined
     * in v21, and so we have to do some conversions here */
    switch(v21type) {
        case PMIX_COMMAND:
            /* the peer unfortunately didn't separate these out,
             * but instead set them to PMIX_UINT32 */
            v20type = PMIX_UINT32;
            break;

        default:
            v20type = v21type;
    }
    return v20type;
}

pmix_status_t pmix20_bfrop_store_data_type(pmix_buffer_t *buffer, pmix_data_type_t type)
{
    pmix_data_type_t v20type;

    v20type = pmix20_v21_to_v20_datatype(type);
    return pmix20_bfrop_pack_datatype(buffer, &v20type, 1, PMIX_DATA_TYPE);
}

pmix_status_t pmix20_bfrop_get_data_type(pmix_buffer_t *buffer, pmix_data_type_t *type)
{
    int32_t n=1;
    pmix_status_t rc;

    rc = pmix20_bfrop_unpack_datatype(buffer, type, &n, PMIX_DATA_TYPE);

    return rc;
}

void pmix20_bfrop_value_load(pmix_value_t *v,
                             const void *data,
                             pmix_data_type_t type)
{
    pmix_byte_object_t *bo;
    pmix_proc_info_t *pi;

    v->type = type;
    if (NULL == data) {
        /* just set the fields to zero */
        memset(&v->data, 0, sizeof(v->data));
        if (PMIX_BOOL == type) {
          v->data.flag = true; // existence of the attribute indicates true unless specified different
        }
    } else {
        switch(type) {
        case PMIX_UNDEF:
            break;
        case PMIX_BOOL:
            memcpy(&(v->data.flag), data, 1);
            break;
        case PMIX_BYTE:
            memcpy(&(v->data.byte), data, 1);
            break;
        case PMIX_STRING:
            v->data.string = strdup(data);
            break;
        case PMIX_SIZE:
            memcpy(&(v->data.size), data, sizeof(size_t));
            break;
        case PMIX_PID:
            memcpy(&(v->data.pid), data, sizeof(pid_t));
            break;
        case PMIX_INT:
            memcpy(&(v->data.integer), data, sizeof(int));
            break;
        case PMIX_INT8:
            memcpy(&(v->data.int8), data, 1);
            break;
        case PMIX_INT16:
            memcpy(&(v->data.int16), data, 2);
            break;
        case PMIX_INT32:
            memcpy(&(v->data.int32), data, 4);
            break;
        case PMIX_INT64:
            memcpy(&(v->data.int64), data, 8);
            break;
        case PMIX_UINT:
            memcpy(&(v->data.uint), data, sizeof(int));
            break;
        case PMIX_UINT8:
            memcpy(&(v->data.uint8), data, 1);
            break;
        case PMIX_UINT16:
            memcpy(&(v->data.uint16), data, 2);
            break;
        case PMIX_UINT32:
            memcpy(&(v->data.uint32), data, 4);
            break;
        case PMIX_UINT64:
            memcpy(&(v->data.uint64), data, 8);
            break;
        case PMIX_FLOAT:
            memcpy(&(v->data.fval), data, sizeof(float));
            break;
        case PMIX_DOUBLE:
            memcpy(&(v->data.dval), data, sizeof(double));
            break;
        case PMIX_TIMEVAL:
            memcpy(&(v->data.tv), data, sizeof(struct timeval));
            break;
        case PMIX_TIME:
            memcpy(&(v->data.time), data, sizeof(time_t));
            break;
        case PMIX_STATUS:
            memcpy(&(v->data.status), data, sizeof(pmix_status_t));
            break;
        case PMIX_PROC_RANK:
            memcpy(&(v->data.rank), data, sizeof(pmix_rank_t));
            break;
        case PMIX_PROC:
            PMIX_PROC_CREATE(v->data.proc, 1);
            if (NULL == v->data.proc) {
                PMIX_ERROR_LOG(PMIX_ERR_NOMEM);
                return;
            }
            memcpy(v->data.proc, data, sizeof(pmix_proc_t));
            break;
        case PMIX_BYTE_OBJECT:
            bo = (pmix_byte_object_t*)data;
            v->data.bo.bytes = bo->bytes;
            memcpy(&(v->data.bo.size), &bo->size, sizeof(size_t));
            break;
        case PMIX_PERSIST:
            memcpy(&(v->data.persist), data, sizeof(pmix_persistence_t));
            break;
        case PMIX_SCOPE:
            memcpy(&(v->data.scope), data, sizeof(pmix_scope_t));
            break;
        case PMIX_DATA_RANGE:
            memcpy(&(v->data.range), data, sizeof(pmix_data_range_t));
            break;
        case PMIX_PROC_STATE:
            memcpy(&(v->data.state), data, sizeof(pmix_proc_state_t));
            break;
        case PMIX_PROC_INFO:
            PMIX_PROC_INFO_CREATE(v->data.pinfo, 1);
            if (NULL == v->data.pinfo) {
                PMIX_ERROR_LOG(PMIX_ERR_NOMEM);
                return;
            }
            pi = (pmix_proc_info_t*)data;
            memcpy(&(v->data.pinfo->proc), &pi->proc, sizeof(pmix_proc_t));
            if (NULL != pi->hostname) {
                v->data.pinfo->hostname = strdup(pi->hostname);
            }
            if (NULL != pi->executable_name) {
                v->data.pinfo->executable_name = strdup(pi->executable_name);
            }
            memcpy(&(v->data.pinfo->pid), &pi->pid, sizeof(pid_t));
            memcpy(&(v->data.pinfo->exit_code), &pi->exit_code, sizeof(int));
            break;
        case PMIX_POINTER:
            memcpy(&(v->data.ptr), data, sizeof(void*));
            break;
        default:
            /* silence warnings */
            PMIX_ERROR_LOG(PMIX_ERR_UNKNOWN_DATA_TYPE);
            break;
        }
    }
}

pmix_status_t pmix20_bfrop_value_unload(pmix_value_t *kv,
                                        void **data,
                                        size_t *sz)
{
    pmix_status_t rc;
    pmix_proc_t *pc;

    rc = PMIX_SUCCESS;
    if (NULL == data ||
        (NULL == *data &&
         PMIX_STRING != kv->type &&
         PMIX_BYTE_OBJECT != kv->type)) {
        rc = PMIX_ERR_BAD_PARAM;
    } else {
        switch(kv->type) {
        case PMIX_UNDEF:
            rc = PMIX_ERR_UNKNOWN_DATA_TYPE;
            break;
        case PMIX_BOOL:
            memcpy(*data, &(kv->data.flag), 1);
            *sz = 1;
            break;
        case PMIX_BYTE:
            memcpy(*data, &(kv->data.byte), 1);
            *sz = 1;
            break;
        case PMIX_STRING:
            if (NULL != kv->data.string) {
                *data = strdup(kv->data.string);
                *sz = strlen(kv->data.string);
            }
            break;
        case PMIX_SIZE:
            memcpy(*data, &(kv->data.size), sizeof(size_t));
            *sz = sizeof(size_t);
            break;
        case PMIX_PID:
            memcpy(*data, &(kv->data.pid), sizeof(pid_t));
            *sz = sizeof(pid_t);
            break;
        case PMIX_INT:
            memcpy(*data, &(kv->data.integer), sizeof(int));
            *sz = sizeof(int);
            break;
        case PMIX_INT8:
            memcpy(*data, &(kv->data.int8), 1);
            *sz = 1;
            break;
        case PMIX_INT16:
            memcpy(*data, &(kv->data.int16), 2);
            *sz = 2;
            break;
        case PMIX_INT32:
            memcpy(*data, &(kv->data.int32), 4);
            *sz = 4;
            break;
        case PMIX_INT64:
            memcpy(*data, &(kv->data.int64), 8);
            *sz = 8;
            break;
        case PMIX_UINT:
            memcpy(*data, &(kv->data.uint), sizeof(int));
            *sz = sizeof(int);
            break;
        case PMIX_UINT8:
            memcpy(*data, &(kv->data.uint8), 1);
            *sz = 1;
            break;
        case PMIX_UINT16:
            memcpy(*data, &(kv->data.uint16), 2);
            *sz = 2;
            break;
        case PMIX_UINT32:
            memcpy(*data, &(kv->data.uint32), 4);
            *sz = 4;
            break;
        case PMIX_UINT64:
            memcpy(*data, &(kv->data.uint64), 8);
            *sz = 8;
            break;
        case PMIX_FLOAT:
            memcpy(*data, &(kv->data.fval), sizeof(float));
            *sz = sizeof(float);
            break;
        case PMIX_DOUBLE:
            memcpy(*data, &(kv->data.dval), sizeof(double));
            *sz = sizeof(double);
            break;
        case PMIX_TIMEVAL:
            memcpy(*data, &(kv->data.tv), sizeof(struct timeval));
            *sz = sizeof(struct timeval);
            break;
        case PMIX_TIME:
            memcpy(*data, &(kv->data.time), sizeof(time_t));
            *sz = sizeof(time_t);
            break;
        case PMIX_STATUS:
            memcpy(*data, &(kv->data.status), sizeof(pmix_status_t));
            *sz = sizeof(pmix_status_t);
            break;
        case PMIX_PROC_RANK:
            memcpy(*data, &(kv->data.rank), sizeof(pmix_rank_t));
            *sz = sizeof(pmix_rank_t);
            break;
        case PMIX_PROC:
            PMIX_PROC_CREATE(pc, 1);
            if (NULL == pc) {
                PMIX_ERROR_LOG(PMIX_ERR_NOMEM);
                rc = PMIX_ERR_NOMEM;
                break;
            }
            memcpy(pc, kv->data.proc, sizeof(pmix_proc_t));
            *sz = sizeof(pmix_proc_t);
            *data = pc;
            break;
        case PMIX_BYTE_OBJECT:
            if (NULL != kv->data.bo.bytes && 0 < kv->data.bo.size) {
                *data = kv->data.bo.bytes;
                *sz = kv->data.bo.size;
            } else {
                *data = NULL;
                *sz = 0;
            }
            break;
        case PMIX_PERSIST:
            memcpy(*data, &(kv->data.persist), sizeof(pmix_persistence_t));
            *sz = sizeof(pmix_persistence_t);
            break;
        case PMIX_SCOPE:
            memcpy(*data, &(kv->data.scope), sizeof(pmix_scope_t));
            *sz = sizeof(pmix_scope_t);
            break;
        case PMIX_DATA_RANGE:
            memcpy(*data, &(kv->data.range), sizeof(pmix_data_range_t));
            *sz = sizeof(pmix_data_range_t);
            break;
        case PMIX_PROC_STATE:
            memcpy(*data, &(kv->data.state), sizeof(pmix_proc_state_t));
            *sz = sizeof(pmix_proc_state_t);
            break;
        case PMIX_POINTER:
            memcpy(*data, &(kv->data.ptr), sizeof(void*));
            *sz = sizeof(void*);
            break;
        default:
            /* silence warnings */
            rc = PMIX_ERROR;
            break;
        }
    }
    return rc;
}
