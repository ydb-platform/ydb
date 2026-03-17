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
 * Copyright (c) 2013-2018 Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 */

#include <src/include/pmix_config.h>

#include "src/mca/bfrops/base/base.h"
#include "bfrop_pmix3.h"

static pmix_status_t init(void);
static void finalize(void);
static pmix_status_t pmix3_pack(pmix_buffer_t *buffer,
                                const void *src, int num_vals,
                                pmix_data_type_t type);
static pmix_status_t pmix3_unpack(pmix_buffer_t *buffer, void *dest,
                                  int32_t *num_vals, pmix_data_type_t type);
static pmix_status_t pmix3_copy(void **dest, void *src,
                                pmix_data_type_t type);
static pmix_status_t pmix3_print(char **output, char *prefix,
                                 void *src, pmix_data_type_t type);
static pmix_status_t register_type(const char *name,
                                   pmix_data_type_t type,
                                   pmix_bfrop_pack_fn_t pack,
                                   pmix_bfrop_unpack_fn_t unpack,
                                   pmix_bfrop_copy_fn_t copy,
                                   pmix_bfrop_print_fn_t print);
static const char* data_type_string(pmix_data_type_t type);

pmix_bfrops_module_t pmix_bfrops_pmix3_module = {
    .version = "v3",
    .init = init,
    .finalize = finalize,
    .pack = pmix3_pack,
    .unpack = pmix3_unpack,
    .copy = pmix3_copy,
    .print = pmix3_print,
    .copy_payload = pmix_bfrops_base_copy_payload,
    .value_xfer = pmix_bfrops_base_value_xfer,
    .value_load = pmix_bfrops_base_value_load,
    .value_unload = pmix_bfrops_base_value_unload,
    .value_cmp = pmix_bfrops_base_value_cmp,
    .register_type = register_type,
    .data_type_string = data_type_string
};

/* DEPRECATED data type values */
#define PMIX_MODEX        29
#define PMIX_INFO_ARRAY   44

/****    PMIX MODEX STRUCT  -  DEPRECATED    ****/
typedef struct pmix_modex_data {
    char nspace[PMIX_MAX_NSLEN+1];
    int rank;
    uint8_t *blob;
    size_t size;
} pmix_modex_data_t;

static pmix_status_t pmix3_bfrop_pack_array(pmix_buffer_t *buffer, const void *src,
                                            int32_t num_vals, pmix_data_type_t type);
static pmix_status_t pmix3_bfrop_pack_modex(pmix_buffer_t *buffer, const void *src,
                                    int32_t num_vals, pmix_data_type_t type);
static pmix_status_t pmix3_bfrop_unpack_array(pmix_buffer_t *buffer, void *dest,
                                              int32_t *num_vals, pmix_data_type_t type);
static pmix_status_t pmix3_bfrop_unpack_modex(pmix_buffer_t *buffer, void *dest,
                                       int32_t *num_vals, pmix_data_type_t type);
static pmix_status_t pmix3_bfrop_copy_array(pmix_info_array_t **dest,
                                            pmix_info_array_t *src,
                                            pmix_data_type_t type);
static pmix_status_t pmix3_bfrop_copy_modex(pmix_modex_data_t **dest, pmix_modex_data_t *src,
                                    pmix_data_type_t type);
static pmix_status_t pmix3_bfrop_print_array(char **output, char *prefix,
                                             pmix_info_array_t *src, pmix_data_type_t type);
static pmix_status_t pmix3_bfrop_print_modex(char **output, char *prefix,
                                     pmix_modex_data_t *src, pmix_data_type_t type);


static pmix_status_t init(void)
{
    /* some standard types don't require anything special */
    PMIX_REGISTER_TYPE("PMIX_BOOL", PMIX_BOOL,
                       pmix_bfrops_base_pack_bool,
                       pmix_bfrops_base_unpack_bool,
                       pmix_bfrops_base_std_copy,
                       pmix_bfrops_base_print_bool,
                       &mca_bfrops_v3_component.types);

    PMIX_REGISTER_TYPE("PMIX_BYTE", PMIX_BYTE,
                       pmix_bfrops_base_pack_byte,
                       pmix_bfrops_base_unpack_byte,
                       pmix_bfrops_base_std_copy,
                       pmix_bfrops_base_print_byte,
                       &mca_bfrops_v3_component.types);

    PMIX_REGISTER_TYPE("PMIX_STRING", PMIX_STRING,
                       pmix_bfrops_base_pack_string,
                       pmix_bfrops_base_unpack_string,
                       pmix_bfrops_base_copy_string,
                       pmix_bfrops_base_print_string,
                       &mca_bfrops_v3_component.types);

    /* Register the rest of the standard generic types to point to internal functions */
    PMIX_REGISTER_TYPE("PMIX_SIZE", PMIX_SIZE,
                       pmix_bfrops_base_pack_sizet,
                       pmix_bfrops_base_unpack_sizet,
                       pmix_bfrops_base_std_copy,
                       pmix_bfrops_base_print_size,
                       &mca_bfrops_v3_component.types);

    PMIX_REGISTER_TYPE("PMIX_PID", PMIX_PID,
                       pmix_bfrops_base_pack_pid,
                       pmix_bfrops_base_unpack_pid,
                       pmix_bfrops_base_std_copy,
                       pmix_bfrops_base_print_pid,
                       &mca_bfrops_v3_component.types);

    PMIX_REGISTER_TYPE("PMIX_INT", PMIX_INT,
                       pmix_bfrops_base_pack_int,
                       pmix_bfrops_base_unpack_int,
                       pmix_bfrops_base_std_copy,
                       pmix_bfrops_base_print_int,
                       &mca_bfrops_v3_component.types);

    /* Register all the standard fixed types to point to base functions */
    PMIX_REGISTER_TYPE("PMIX_INT8", PMIX_INT8,
                       pmix_bfrops_base_pack_byte,
                       pmix_bfrops_base_unpack_byte,
                       pmix_bfrops_base_std_copy,
                       pmix_bfrops_base_print_int8,
                       &mca_bfrops_v3_component.types);

    PMIX_REGISTER_TYPE("PMIX_INT16", PMIX_INT16,
                       pmix_bfrops_base_pack_int16,
                       pmix_bfrops_base_unpack_int16,
                       pmix_bfrops_base_std_copy,
                       pmix_bfrops_base_print_int16,
                       &mca_bfrops_v3_component.types);

    PMIX_REGISTER_TYPE("PMIX_INT32", PMIX_INT32,
                       pmix_bfrops_base_pack_int32,
                       pmix_bfrops_base_unpack_int32,
                       pmix_bfrops_base_std_copy,
                       pmix_bfrops_base_print_int32,
                       &mca_bfrops_v3_component.types);

    PMIX_REGISTER_TYPE("PMIX_INT64", PMIX_INT64,
                       pmix_bfrops_base_pack_int64,
                       pmix_bfrops_base_unpack_int64,
                       pmix_bfrops_base_std_copy,
                       pmix_bfrops_base_print_int64,
                       &mca_bfrops_v3_component.types);

    PMIX_REGISTER_TYPE("PMIX_UINT", PMIX_UINT,
                       pmix_bfrops_base_pack_int,
                       pmix_bfrops_base_unpack_int,
                       pmix_bfrops_base_std_copy,
                       pmix_bfrops_base_print_uint,
                       &mca_bfrops_v3_component.types);

    PMIX_REGISTER_TYPE("PMIX_UINT8", PMIX_UINT8,
                       pmix_bfrops_base_pack_byte,
                       pmix_bfrops_base_unpack_byte,
                       pmix_bfrops_base_std_copy,
                       pmix_bfrops_base_print_uint8,
                       &mca_bfrops_v3_component.types);

    PMIX_REGISTER_TYPE("PMIX_UINT16", PMIX_UINT16,
                       pmix_bfrops_base_pack_int16,
                       pmix_bfrops_base_unpack_int16,
                       pmix_bfrops_base_std_copy,
                       pmix_bfrops_base_print_uint16,
                       &mca_bfrops_v3_component.types);

    PMIX_REGISTER_TYPE("PMIX_UINT32", PMIX_UINT32,
                       pmix_bfrops_base_pack_int32,
                       pmix_bfrops_base_unpack_int32,
                       pmix_bfrops_base_std_copy,
                       pmix_bfrops_base_print_uint32,
                       &mca_bfrops_v3_component.types);

    PMIX_REGISTER_TYPE("PMIX_UINT64", PMIX_UINT64,
                       pmix_bfrops_base_pack_int64,
                       pmix_bfrops_base_unpack_int64,
                       pmix_bfrops_base_std_copy,
                       pmix_bfrops_base_print_uint64,
                       &mca_bfrops_v3_component.types);

    PMIX_REGISTER_TYPE("PMIX_FLOAT", PMIX_FLOAT,
                       pmix_bfrops_base_pack_float,
                       pmix_bfrops_base_unpack_float,
                       pmix_bfrops_base_std_copy,
                       pmix_bfrops_base_print_float,
                       &mca_bfrops_v3_component.types);

    PMIX_REGISTER_TYPE("PMIX_DOUBLE", PMIX_DOUBLE,
                       pmix_bfrops_base_pack_double,
                       pmix_bfrops_base_unpack_double,
                       pmix_bfrops_base_std_copy,
                       pmix_bfrops_base_print_double,
                       &mca_bfrops_v3_component.types);

    PMIX_REGISTER_TYPE("PMIX_TIMEVAL", PMIX_TIMEVAL,
                       pmix_bfrops_base_pack_timeval,
                       pmix_bfrops_base_unpack_timeval,
                       pmix_bfrops_base_std_copy,
                       pmix_bfrops_base_print_timeval,
                       &mca_bfrops_v3_component.types);

    PMIX_REGISTER_TYPE("PMIX_TIME", PMIX_TIME,
                       pmix_bfrops_base_pack_time,
                       pmix_bfrops_base_unpack_time,
                       pmix_bfrops_base_std_copy,
                       pmix_bfrops_base_print_time,
                       &mca_bfrops_v3_component.types);

    PMIX_REGISTER_TYPE("PMIX_STATUS", PMIX_STATUS,
                       pmix_bfrops_base_pack_status,
                       pmix_bfrops_base_unpack_status,
                       pmix_bfrops_base_std_copy,
                       pmix_bfrops_base_print_status,
                       &mca_bfrops_v3_component.types);

    PMIX_REGISTER_TYPE("PMIX_VALUE", PMIX_VALUE,
                       pmix_bfrops_base_pack_value,
                       pmix_bfrops_base_unpack_value,
                       pmix_bfrops_base_copy_value,
                       pmix_bfrops_base_print_value,
                       &mca_bfrops_v3_component.types);

    PMIX_REGISTER_TYPE("PMIX_PROC", PMIX_PROC,
                       pmix_bfrops_base_pack_proc,
                       pmix_bfrops_base_unpack_proc,
                       pmix_bfrops_base_copy_proc,
                       pmix_bfrops_base_print_proc,
                       &mca_bfrops_v3_component.types);

    PMIX_REGISTER_TYPE("PMIX_APP", PMIX_APP,
                       pmix_bfrops_base_pack_app,
                       pmix_bfrops_base_unpack_app,
                       pmix_bfrops_base_copy_app,
                       pmix_bfrops_base_print_app,
                       &mca_bfrops_v3_component.types);

    PMIX_REGISTER_TYPE("PMIX_INFO", PMIX_INFO,
                       pmix_bfrops_base_pack_info,
                       pmix_bfrops_base_unpack_info,
                       pmix_bfrops_base_copy_info,
                       pmix_bfrops_base_print_info,
                       &mca_bfrops_v3_component.types);

    PMIX_REGISTER_TYPE("PMIX_PDATA", PMIX_PDATA,
                       pmix_bfrops_base_pack_pdata,
                       pmix_bfrops_base_unpack_pdata,
                       pmix_bfrops_base_copy_pdata,
                       pmix_bfrops_base_print_pdata,
                       &mca_bfrops_v3_component.types);

    PMIX_REGISTER_TYPE("PMIX_BUFFER", PMIX_BUFFER,
                       pmix_bfrops_base_pack_buf,
                       pmix_bfrops_base_unpack_buf,
                       pmix_bfrops_base_copy_buf,
                       pmix_bfrops_base_print_buf,
                       &mca_bfrops_v3_component.types);

    PMIX_REGISTER_TYPE("PMIX_BYTE_OBJECT", PMIX_BYTE_OBJECT,
                       pmix_bfrops_base_pack_bo,
                       pmix_bfrops_base_unpack_bo,
                       pmix_bfrops_base_copy_bo,
                       pmix_bfrops_base_print_bo,
                       &mca_bfrops_v3_component.types);

    PMIX_REGISTER_TYPE("PMIX_KVAL", PMIX_KVAL,
                       pmix_bfrops_base_pack_kval,
                       pmix_bfrops_base_unpack_kval,
                       pmix_bfrops_base_copy_kval,
                       pmix_bfrops_base_print_kval,
                       &mca_bfrops_v3_component.types);

    PMIX_REGISTER_TYPE("PMIX_MODEX", PMIX_MODEX,
                       pmix3_bfrop_pack_modex,
                       pmix3_bfrop_unpack_modex,
                       pmix3_bfrop_copy_modex,
                       pmix3_bfrop_print_modex,
                       &mca_bfrops_v3_component.types);

    /* these are fixed-sized values and can be done by base */
    PMIX_REGISTER_TYPE("PMIX_PERSIST", PMIX_PERSIST,
                       pmix_bfrops_base_pack_persist,
                       pmix_bfrops_base_unpack_persist,
                       pmix_bfrops_base_std_copy,
                       pmix_bfrops_base_print_persist,
                       &mca_bfrops_v3_component.types);

    PMIX_REGISTER_TYPE("PMIX_POINTER", PMIX_POINTER,
                       pmix_bfrops_base_pack_ptr,
                       pmix_bfrops_base_unpack_ptr,
                       pmix_bfrops_base_std_copy,
                       pmix_bfrops_base_print_ptr,
                       &mca_bfrops_v3_component.types);

    PMIX_REGISTER_TYPE("PMIX_SCOPE", PMIX_SCOPE,
                       pmix_bfrops_base_pack_scope,
                       pmix_bfrops_base_unpack_scope,
                       pmix_bfrops_base_std_copy,
                       pmix_bfrops_base_std_copy,
                       &mca_bfrops_v3_component.types);

    PMIX_REGISTER_TYPE("PMIX_DATA_RANGE", PMIX_DATA_RANGE,
                       pmix_bfrops_base_pack_range,
                       pmix_bfrops_base_unpack_range,
                       pmix_bfrops_base_std_copy,
                       pmix_bfrops_base_print_ptr,
                       &mca_bfrops_v3_component.types);

    PMIX_REGISTER_TYPE("PMIX_COMMAND", PMIX_COMMAND,
                       pmix_bfrops_base_pack_cmd,
                       pmix_bfrops_base_unpack_cmd,
                       pmix_bfrops_base_std_copy,
                       pmix_bfrops_base_print_cmd,
                       &mca_bfrops_v3_component.types);

    PMIX_REGISTER_TYPE("PMIX_INFO_DIRECTIVES", PMIX_INFO_DIRECTIVES,
                       pmix_bfrops_base_pack_info_directives,
                       pmix_bfrops_base_unpack_info_directives,
                       pmix_bfrops_base_std_copy,
                       pmix_bfrops_base_print_info_directives,
                       &mca_bfrops_v3_component.types);

    PMIX_REGISTER_TYPE("PMIX_DATA_TYPE", PMIX_DATA_TYPE,
                       pmix_bfrops_base_pack_datatype,
                       pmix_bfrops_base_unpack_datatype,
                       pmix_bfrops_base_std_copy,
                       pmix_bfrops_base_print_datatype,
                       &mca_bfrops_v3_component.types);

    PMIX_REGISTER_TYPE("PMIX_PROC_STATE", PMIX_PROC_STATE,
                       pmix_bfrops_base_pack_pstate,
                       pmix_bfrops_base_unpack_pstate,
                       pmix_bfrops_base_std_copy,
                       pmix_bfrops_base_print_pstate,
                       &mca_bfrops_v3_component.types);

    PMIX_REGISTER_TYPE("PMIX_PROC_INFO", PMIX_PROC_INFO,
                       pmix_bfrops_base_pack_pinfo,
                       pmix_bfrops_base_unpack_pinfo,
                       pmix_bfrops_base_copy_pinfo,
                       pmix_bfrops_base_print_pinfo,
                       &mca_bfrops_v3_component.types);

    PMIX_REGISTER_TYPE("PMIX_DATA_ARRAY", PMIX_DATA_ARRAY,
                       pmix_bfrops_base_pack_darray,
                       pmix_bfrops_base_unpack_darray,
                       pmix_bfrops_base_copy_darray,
                       pmix_bfrops_base_print_darray,
                       &mca_bfrops_v3_component.types);

    PMIX_REGISTER_TYPE("PMIX_PROC_RANK", PMIX_PROC_RANK,
                       pmix_bfrops_base_pack_rank,
                       pmix_bfrops_base_unpack_rank,
                       pmix_bfrops_base_std_copy,
                       pmix_bfrops_base_print_rank,
                       &mca_bfrops_v3_component.types);

    PMIX_REGISTER_TYPE("PMIX_QUERY", PMIX_QUERY,
                       pmix_bfrops_base_pack_query,
                       pmix_bfrops_base_unpack_query,
                       pmix_bfrops_base_copy_query,
                       pmix_bfrops_base_print_query,
                       &mca_bfrops_v3_component.types);

    PMIX_REGISTER_TYPE("PMIX_COMPRESSED_STRING",
                       PMIX_COMPRESSED_STRING,
                       pmix_bfrops_base_pack_bo,
                       pmix_bfrops_base_unpack_bo,
                       pmix_bfrops_base_copy_bo,
                       pmix_bfrops_base_print_bo,
                       &mca_bfrops_v3_component.types);

    PMIX_REGISTER_TYPE("PMIX_ALLOC_DIRECTIVE",
                       PMIX_ALLOC_DIRECTIVE,
                       pmix_bfrops_base_pack_alloc_directive,
                       pmix_bfrops_base_unpack_alloc_directive,
                       pmix_bfrops_base_std_copy,
                       pmix_bfrops_base_print_alloc_directive,
                       &mca_bfrops_v3_component.types);

    PMIX_REGISTER_TYPE("PMIX_IOF_CHANNEL",
                       PMIX_IOF_CHANNEL,
                       pmix_bfrops_base_pack_iof_channel,
                       pmix_bfrops_base_unpack_iof_channel,
                       pmix_bfrops_base_std_copy,
                       pmix_bfrops_base_print_iof_channel,
                       &mca_bfrops_v3_component.types);

    PMIX_REGISTER_TYPE("PMIX_ENVAR",
                       PMIX_ENVAR,
                       pmix_bfrops_base_pack_envar,
                       pmix_bfrops_base_unpack_envar,
                       pmix_bfrops_base_copy_envar,
                       pmix_bfrops_base_print_envar,
                       &mca_bfrops_v3_component.types);

    /**** DEPRECATED ****/
    PMIX_REGISTER_TYPE("PMIX_INFO_ARRAY", PMIX_INFO_ARRAY,
                       pmix3_bfrop_pack_array,
                       pmix3_bfrop_unpack_array,
                       pmix3_bfrop_copy_array,
                       pmix3_bfrop_print_array,
                       &mca_bfrops_v3_component.types);
    /********************/


    return PMIX_SUCCESS;
}

static void finalize(void)
{
    int n;
    pmix_bfrop_type_info_t *info;

    for (n=0; n < mca_bfrops_v3_component.types.size; n++) {
        if (NULL != (info = (pmix_bfrop_type_info_t*)pmix_pointer_array_get_item(&mca_bfrops_v3_component.types, n))) {
            PMIX_RELEASE(info);
            pmix_pointer_array_set_item(&mca_bfrops_v3_component.types, n, NULL);
        }
    }
}

static pmix_status_t pmix3_pack(pmix_buffer_t *buffer,
                                const void *src, int num_vals,
                                pmix_data_type_t type)
{
    /* kick the process off by passing this in to the base */
    return pmix_bfrops_base_pack(&mca_bfrops_v3_component.types,
                                 buffer, src, num_vals, type);
}

static pmix_status_t pmix3_unpack(pmix_buffer_t *buffer, void *dest,
                                  int32_t *num_vals, pmix_data_type_t type)
{
     /* kick the process off by passing this in to the base */
    return pmix_bfrops_base_unpack(&mca_bfrops_v3_component.types,
                                   buffer, dest, num_vals, type);
}

static pmix_status_t pmix3_copy(void **dest, void *src,
                                pmix_data_type_t type)
{
    return pmix_bfrops_base_copy(&mca_bfrops_v3_component.types,
                                 dest, src, type);
}

static pmix_status_t pmix3_print(char **output, char *prefix,
                                 void *src, pmix_data_type_t type)
{
    return pmix_bfrops_base_print(&mca_bfrops_v3_component.types,
                                  output, prefix, src, type);
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
                       &mca_bfrops_v3_component.types);
    return PMIX_SUCCESS;
}

static const char* data_type_string(pmix_data_type_t type)
{
    return pmix_bfrops_base_data_type_string(&mca_bfrops_v3_component.types, type);
}

/**** DEPRECATED ****/
static pmix_status_t pmix3_bfrop_pack_array(pmix_buffer_t *buffer, const void *src,
                                            int32_t num_vals, pmix_data_type_t type)
{
    pmix_info_array_t *ptr;
    int32_t i;
    pmix_status_t ret;

    ptr = (pmix_info_array_t *) src;

    for (i = 0; i < num_vals; ++i) {
        /* pack the size */
        if (PMIX_SUCCESS != (ret = pmix_bfrops_base_pack_sizet(buffer, &ptr[i].size, 1, PMIX_SIZE))) {
            return ret;
        }
        if (0 < ptr[i].size) {
            /* pack the values */
            if (PMIX_SUCCESS != (ret = pmix_bfrops_base_pack_info(buffer, ptr[i].array, ptr[i].size, PMIX_INFO))) {
                return ret;
            }
        }
    }

    return PMIX_SUCCESS;
}

static pmix_status_t pmix3_bfrop_pack_modex(pmix_buffer_t *buffer, const void *src,
                                    int32_t num_vals, pmix_data_type_t type)
{
    pmix_modex_data_t *ptr;
    int32_t i;
    pmix_status_t ret;

    ptr = (pmix_modex_data_t *) src;

    for (i = 0; i < num_vals; ++i) {
        if (PMIX_SUCCESS != (ret = pmix_bfrops_base_pack_sizet(buffer, &ptr[i].size, 1, PMIX_SIZE))) {
            return ret;
        }
        if( 0 < ptr[i].size){
            if (PMIX_SUCCESS != (ret = pmix_bfrops_base_pack_byte(buffer, ptr[i].blob, ptr[i].size, PMIX_UINT8))) {
                return ret;
            }
        }
    }
    return PMIX_SUCCESS;
}


/********************/

/**** DEPRECATED ****/
static pmix_status_t pmix3_bfrop_unpack_array(pmix_buffer_t *buffer, void *dest,
                                              int32_t *num_vals, pmix_data_type_t type)
{
    pmix_info_array_t *ptr;
    int32_t i, n, m;
    pmix_status_t ret;

    pmix_output_verbose(20, pmix_bfrops_base_framework.framework_output,
                        "pmix3_bfrop_unpack: %d info arrays", *num_vals);

    ptr = (pmix_info_array_t*) dest;
    n = *num_vals;

    for (i = 0; i < n; ++i) {
        pmix_output_verbose(20, pmix_bfrops_base_framework.framework_output,
                            "pmix3_bfrop_unpack: init array[%d]", i);
        memset(&ptr[i], 0, sizeof(pmix_info_array_t));
        /* unpack the size of this array */
        m=1;
        if (PMIX_SUCCESS != (ret = pmix_bfrops_base_unpack_sizet(buffer, &ptr[i].size, &m, PMIX_SIZE))) {
            return ret;
        }
        if (0 < ptr[i].size) {
            ptr[i].array = (pmix_info_t*)malloc(ptr[i].size * sizeof(pmix_info_t));
            m=ptr[i].size;
            if (PMIX_SUCCESS != (ret = pmix_bfrops_base_unpack_value(buffer, ptr[i].array, &m, PMIX_INFO))) {
                return ret;
            }
        }
    }
    return PMIX_SUCCESS;
}

static pmix_status_t pmix3_bfrop_unpack_modex(pmix_buffer_t *buffer, void *dest,
                            int32_t *num_vals, pmix_data_type_t type)
{
    pmix_modex_data_t *ptr;
    int32_t i, n, m;
    pmix_status_t ret;

    pmix_output_verbose(20, pmix_bfrops_base_framework.framework_output,
                        "pmix20_bfrop_unpack: %d modex", *num_vals);

    ptr = (pmix_modex_data_t *) dest;
    n = *num_vals;

    for (i = 0; i < n; ++i) {
        memset(&ptr[i], 0, sizeof(pmix_modex_data_t));
        /* unpack the number of bytes */
        m=1;
        if (PMIX_SUCCESS != (ret = pmix_bfrops_base_unpack_sizet(buffer, &ptr[i].size, &m, PMIX_SIZE))) {
            return ret;
        }
        if (0 < ptr[i].size) {
            ptr[i].blob = (uint8_t*)malloc(ptr[i].size * sizeof(uint8_t));
            m=ptr[i].size;
            if (PMIX_SUCCESS != (ret = pmix_bfrops_base_unpack_byte(buffer, ptr[i].blob, &m, PMIX_UINT8))) {
                return ret;
            }
        }
    }
    return PMIX_SUCCESS;
}


/********************/

/**** DEPRECATED ****/
static pmix_status_t pmix3_bfrop_copy_array(pmix_info_array_t **dest,
                                            pmix_info_array_t *src,
                                            pmix_data_type_t type)
{
    pmix_info_t *d1, *s1;

    *dest = (pmix_info_array_t*)malloc(sizeof(pmix_info_array_t));
    (*dest)->size = src->size;
    (*dest)->array = (pmix_info_t*)malloc(src->size * sizeof(pmix_info_t));
    d1 = (pmix_info_t*)(*dest)->array;
    s1 = (pmix_info_t*)src->array;
    memcpy(d1, s1, src->size * sizeof(pmix_info_t));
    return PMIX_SUCCESS;
}

static pmix_status_t pmix3_bfrop_copy_modex(pmix_modex_data_t **dest, pmix_modex_data_t *src,
                                    pmix_data_type_t type)
{
    *dest = (pmix_modex_data_t*)malloc(sizeof(pmix_modex_data_t));
    if (NULL == *dest) {
        return PMIX_ERR_OUT_OF_RESOURCE;
    }
    (*dest)->blob = NULL;
    (*dest)->size = 0;
    if (NULL != src->blob) {
        (*dest)->blob = (uint8_t*)malloc(src->size * sizeof(uint8_t));
        if (NULL == (*dest)->blob) {
            return PMIX_ERR_OUT_OF_RESOURCE;
        }
        memcpy((*dest)->blob, src->blob, src->size * sizeof(uint8_t));
        (*dest)->size = src->size;
    }
    return PMIX_SUCCESS;
}

/*******************/

/**** DEPRECATED ****/
static pmix_status_t pmix3_bfrop_print_array(char **output, char *prefix,
                                             pmix_info_array_t *src, pmix_data_type_t type)
{
    size_t j;
    char *tmp, *tmp2, *tmp3, *pfx;
    pmix_info_t *s1;

    if (0 > asprintf(&tmp, "%sARRAY SIZE: %ld", prefix, (long)src->size)) {
        return PMIX_ERR_NOMEM;
    }
    if (0 > asprintf(&pfx, "\n%s\t",  (NULL == prefix) ? "" : prefix)) {
        free(tmp);
        return PMIX_ERR_NOMEM;
    }
    s1 = (pmix_info_t*)src->array;

    for (j=0; j < src->size; j++) {
        pmix_bfrops_base_print_info(&tmp2, pfx, &s1[j], PMIX_INFO);
        if (0 > asprintf(&tmp3, "%s%s", tmp, tmp2)) {
            free(tmp);
            free(tmp2);
            return PMIX_ERR_NOMEM;
        }
        free(tmp);
        free(tmp2);
        tmp = tmp3;
    }
    *output = tmp;
    return PMIX_SUCCESS;
}

static pmix_status_t pmix3_bfrop_print_modex(char **output, char *prefix,
                                     pmix_modex_data_t *src, pmix_data_type_t type)
{
    return PMIX_SUCCESS;
}


/********************/
