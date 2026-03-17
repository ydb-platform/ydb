/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2006 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2014-2018 Intel, Inc.  All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2016      IBM Corporation.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include <src/include/pmix_config.h>


#include "src/util/argv.h"
#include "src/util/error.h"
#include "src/util/output.h"
#include "src/mca/bfrops/base/base.h"
#include "bfrop_v12.h"
#include "internal.h"

pmix_status_t pmix12_bfrop_copy(void **dest, void *src, pmix_data_type_t type)
{
    pmix_bfrop_type_info_t *info;

    /* check for error */
    if (NULL == dest) {
        PMIX_ERROR_LOG(PMIX_ERR_BAD_PARAM);
        return PMIX_ERR_BAD_PARAM;
    }
    if (NULL == src) {
        PMIX_ERROR_LOG(PMIX_ERR_BAD_PARAM);
        return PMIX_ERR_BAD_PARAM;
    }

   /* Lookup the copy function for this type and call it */

    if (NULL == (info = (pmix_bfrop_type_info_t*)pmix_pointer_array_get_item(&mca_bfrops_v12_component.types, type))) {
        PMIX_ERROR_LOG(PMIX_ERR_UNKNOWN_DATA_TYPE);
        return PMIX_ERR_UNKNOWN_DATA_TYPE;
    }

    return info->odti_copy_fn(dest, src, type);
}

pmix_status_t pmix12_bfrop_copy_payload(pmix_buffer_t *dest, pmix_buffer_t *src)
{
    size_t to_copy = 0;
    char *ptr;
    /* deal with buffer type */
    if( NULL == dest->base_ptr ){
        /* destination buffer is empty - derive src buffer type */
        dest->type = src->type;
    } else if( dest->type != src->type ){
        /* buffer types mismatch */
        PMIX_ERROR_LOG(PMIX_ERR_BAD_PARAM);
        return PMIX_ERR_BAD_PARAM;
    }

    to_copy = src->pack_ptr - src->unpack_ptr;
    if( NULL == (ptr = pmix_bfrop_buffer_extend(dest, to_copy)) ){
        PMIX_ERROR_LOG(PMIX_ERR_OUT_OF_RESOURCE);
        return PMIX_ERR_OUT_OF_RESOURCE;
    }
    memcpy(ptr,src->unpack_ptr, to_copy);
    dest->bytes_used += to_copy;
    dest->pack_ptr += to_copy;
    return PMIX_SUCCESS;
}


/*
 * STANDARD COPY FUNCTION - WORKS FOR EVERYTHING NON-STRUCTURED
 */
pmix_status_t pmix12_bfrop_std_copy(void **dest, void *src, pmix_data_type_t type)
{
    size_t datasize;
    uint8_t *val = NULL;

    switch(type) {
    case PMIX_BOOL:
        datasize = sizeof(bool);
        break;

    case PMIX_INT:
    case PMIX_UINT:
        datasize = sizeof(int);
        break;

    case PMIX_SIZE:
        datasize = sizeof(size_t);
        break;

    case PMIX_PID:
        datasize = sizeof(pid_t);
        break;

    case PMIX_BYTE:
    case PMIX_INT8:
    case PMIX_UINT8:
        datasize = 1;
        break;

    case PMIX_INT16:
    case PMIX_UINT16:
        datasize = 2;
        break;

    case PMIX_INT32:
    case PMIX_UINT32:
        datasize = 4;
        break;

    case PMIX_INT64:
    case PMIX_UINT64:
        datasize = 8;
        break;

    case PMIX_FLOAT:
        datasize = sizeof(float);
        break;

    case PMIX_TIMEVAL:
        datasize = sizeof(struct timeval);
        break;

    case PMIX_TIME:
        datasize = sizeof(time_t);
        break;

    default:
        return PMIX_ERR_UNKNOWN_DATA_TYPE;
    }

    val = (uint8_t*)malloc(datasize);
    if (NULL == val) {
        return PMIX_ERR_OUT_OF_RESOURCE;
    }

    memcpy(val, src, datasize);
    *dest = val;

    return PMIX_SUCCESS;
}

/* COPY FUNCTIONS FOR NON-STANDARD SYSTEM TYPES */

/*
 * STRING
 */
pmix_status_t pmix12_bfrop_copy_string(char **dest, char *src, pmix_data_type_t type)
{
    if (NULL == src) {  /* got zero-length string/NULL pointer - store NULL */
        *dest = NULL;
    } else {
        *dest = strdup(src);
    }

    return PMIX_SUCCESS;
}
/* compare function for pmix_value_t*/
pmix_value_cmp_t pmix12_bfrop_value_cmp(pmix_value_t *p, pmix_value_t *p1)
{
    bool rc = false;
    switch (p->type) {
        case PMIX_BOOL:
            rc = (p->data.flag == p1->data.flag);
            break;
        case PMIX_BYTE:
            rc = (p->data.byte == p1->data.byte);
            break;
        case PMIX_SIZE:
            rc = (p->data.size == p1->data.size);
            break;
        case PMIX_INT:
            rc = (p->data.integer == p1->data.integer);
            break;
        case PMIX_INT8:
            rc = (p->data.int8 == p1->data.int8);
            break;
        case PMIX_INT16:
            rc = (p->data.int16 == p1->data.int16);
            break;
        case PMIX_INT32:
            rc = (p->data.int32 == p1->data.int32);
            break;
        case PMIX_INT64:
            rc = (p->data.int64 == p1->data.int64);
            break;
        case PMIX_UINT:
            rc = (p->data.uint == p1->data.uint);
            break;
        case PMIX_UINT8:
            rc = (p->data.uint8 == p1->data.int8);
            break;
        case PMIX_UINT16:
            rc = (p->data.uint16 == p1->data.uint16);
            break;
        case PMIX_UINT32:
            rc = (p->data.uint32 == p1->data.uint32);
            break;
        case PMIX_UINT64:
            rc = (p->data.uint64 == p1->data.uint64);
            break;
        case PMIX_STRING:
            rc = strcmp(p->data.string, p1->data.string);
            break;
        default:
            pmix_output(0, "COMPARE-PMIX-VALUE: UNSUPPORTED TYPE %d", (int)p->type);
    }
    if (rc) {
        return PMIX_EQUAL;
    }
    return PMIX_VALUE1_GREATER;
}
/* COPY FUNCTIONS FOR GENERIC PMIX TYPES */
pmix_status_t pmix12_bfrop_value_xfer(pmix_value_t *p, const pmix_value_t *src)
{
    /* copy the right field */
    p->type = src->type;
    switch (src->type) {
    case PMIX_BOOL:
        p->data.flag = src->data.flag;
        break;
    case PMIX_BYTE:
        p->data.byte = src->data.byte;
        break;
    case PMIX_STRING:
        if (NULL != src->data.string) {
            p->data.string = strdup(src->data.string);
        } else {
            p->data.string = NULL;
        }
        break;
    case PMIX_SIZE:
        p->data.size = src->data.size;
        break;
    case PMIX_PID:
        p->data.pid = src->data.pid;
        break;
    case PMIX_INT:
        /* to avoid alignment issues */
        memcpy(&p->data.integer, &src->data.integer, sizeof(int));
        break;
    case PMIX_INT8:
        p->data.int8 = src->data.int8;
        break;
    case PMIX_INT16:
        /* to avoid alignment issues */
        memcpy(&p->data.int16, &src->data.int16, 2);
        break;
    case PMIX_INT32:
        /* to avoid alignment issues */
        memcpy(&p->data.int32, &src->data.int32, 4);
        break;
    case PMIX_INT64:
        /* to avoid alignment issues */
        memcpy(&p->data.int64, &src->data.int64, 8);
        break;
    case PMIX_UINT:
        /* to avoid alignment issues */
        memcpy(&p->data.uint, &src->data.uint, sizeof(unsigned int));
        break;
    case PMIX_UINT8:
        p->data.uint8 = src->data.uint8;
        break;
    case PMIX_UINT16:
        /* to avoid alignment issues */
        memcpy(&p->data.uint16, &src->data.uint16, 2);
        break;
    case PMIX_UINT32:
        /* to avoid alignment issues */
        memcpy(&p->data.uint32, &src->data.uint32, 4);
        break;
    case PMIX_UINT64:
        /* to avoid alignment issues */
        memcpy(&p->data.uint64, &src->data.uint64, 8);
        break;
    case PMIX_FLOAT:
        p->data.fval = src->data.fval;
        break;
    case PMIX_DOUBLE:
        p->data.dval = src->data.dval;
        break;
    case PMIX_TIMEVAL:
        p->data.tv.tv_sec = src->data.tv.tv_sec;
        p->data.tv.tv_usec = src->data.tv.tv_usec;
        break;
    case PMIX_INFO_ARRAY:
        return PMIX_ERR_NOT_SUPPORTED;
    case PMIX_BYTE_OBJECT:
        if (NULL != src->data.bo.bytes && 0 < src->data.bo.size) {
            p->data.bo.bytes = malloc(src->data.bo.size);
            memcpy(p->data.bo.bytes, src->data.bo.bytes, src->data.bo.size);
            p->data.bo.size = src->data.bo.size;
        } else {
            p->data.bo.bytes = NULL;
            p->data.bo.size = 0;
        }
        break;
    default:
        pmix_output(0, "COPY-PMIX-VALUE: UNSUPPORTED TYPE %d", (int)src->type);
        return PMIX_ERROR;
    }
    return PMIX_SUCCESS;
}

/* PMIX_VALUE */
pmix_status_t pmix12_bfrop_copy_value(pmix_value_t **dest, pmix_value_t *src,
                                     pmix_data_type_t type)
{
    pmix_value_t *p;

    /* create the new object */
    *dest = (pmix_value_t*)malloc(sizeof(pmix_value_t));
    if (NULL == *dest) {
        return PMIX_ERR_OUT_OF_RESOURCE;
    }
    p = *dest;

    /* copy the type */
    p->type = src->type;
    /* copy the data */
    return pmix_value_xfer(p, src);
}

pmix_status_t pmix12_bfrop_copy_info(pmix_info_t **dest, pmix_info_t *src,
                                    pmix_data_type_t type)
{
    *dest = (pmix_info_t*)malloc(sizeof(pmix_info_t));
    pmix_strncpy((*dest)->key, src->key, PMIX_MAX_KEYLEN);
    return pmix_value_xfer(&(*dest)->value, &src->value);
}

pmix_status_t pmix12_bfrop_copy_buf(pmix_buffer_t **dest, pmix_buffer_t *src,
                                   pmix_data_type_t type)
{
    *dest = PMIX_NEW(pmix_buffer_t);
    pmix_bfrops_base_copy_payload(*dest, src);
    return PMIX_SUCCESS;
}

pmix_status_t pmix12_bfrop_copy_app(pmix_app_t **dest, pmix_app_t *src,
                                   pmix_data_type_t type)
{
    size_t j;

    *dest = (pmix_app_t*)malloc(sizeof(pmix_app_t));
    (*dest)->cmd = strdup(src->cmd);
    (*dest)->argv = pmix_argv_copy(src->argv);
    (*dest)->env = pmix_argv_copy(src->env);
    (*dest)->maxprocs = src->maxprocs;
    (*dest)->ninfo = src->ninfo;
    (*dest)->info = (pmix_info_t*)malloc(src->ninfo * sizeof(pmix_info_t));
    for (j=0; j < src->ninfo; j++) {
        pmix_strncpy((*dest)->info[j].key, src->info[j].key, PMIX_MAX_KEYLEN);
        pmix_value_xfer(&(*dest)->info[j].value, &src->info[j].value);
    }
    return PMIX_SUCCESS;
}

pmix_status_t pmix12_bfrop_copy_kval(pmix_kval_t **dest, pmix_kval_t *src,
                                    pmix_data_type_t type)
{
    pmix_kval_t *p;

    /* create the new object */
    *dest = PMIX_NEW(pmix_kval_t);
    if (NULL == *dest) {
        return PMIX_ERR_OUT_OF_RESOURCE;
    }
    p = *dest;

    /* copy the type */
    p->value->type = src->value->type;
    /* copy the data */
    return pmix_value_xfer(p->value, src->value);
}

pmix_status_t pmix12_bfrop_copy_array(pmix_info_array_t **dest,
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

pmix_status_t pmix12_bfrop_copy_proc(pmix_proc_t **dest, pmix_proc_t *src,
                                    pmix_data_type_t type)
{
    *dest = (pmix_proc_t*)malloc(sizeof(pmix_proc_t));
    if (NULL == *dest) {
        return PMIX_ERR_OUT_OF_RESOURCE;
    }
    pmix_strncpy((*dest)->nspace, src->nspace, PMIX_MAX_NSLEN);
    (*dest)->rank = src->rank;
    return PMIX_SUCCESS;
}

pmix_status_t pmix12_bfrop_copy_modex(pmix_modex_data_t **dest,
                                     pmix_modex_data_t *src,
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

pmix_status_t pmix12_bfrop_copy_persist(pmix_persistence_t **dest,
                                       pmix_persistence_t *src,
                                       pmix_data_type_t type)
{
    *dest = (pmix_persistence_t*)malloc(sizeof(pmix_persistence_t));
    if (NULL == *dest) {
        return PMIX_ERR_OUT_OF_RESOURCE;
    }
    memcpy(*dest, src, sizeof(pmix_persistence_t));
    return PMIX_SUCCESS;
}

pmix_status_t pmix12_bfrop_copy_bo(pmix_byte_object_t **dest,
                                  pmix_byte_object_t *src,
                                  pmix_data_type_t type)
{
    *dest = (pmix_byte_object_t*)malloc(sizeof(pmix_byte_object_t));
    if (NULL == *dest) {
        return PMIX_ERR_OUT_OF_RESOURCE;
    }
    (*dest)->bytes = (char*)malloc(src->size);
    memcpy((*dest)->bytes, src->bytes, src->size);
    (*dest)->size = src->size;
    return PMIX_SUCCESS;
}

pmix_status_t pmix12_bfrop_copy_pdata(pmix_pdata_t **dest,
                                     pmix_pdata_t *src,
                                     pmix_data_type_t type)
{
    *dest = (pmix_pdata_t*)malloc(sizeof(pmix_pdata_t));
    pmix_strncpy((*dest)->proc.nspace, src->proc.nspace, PMIX_MAX_NSLEN);
    (*dest)->proc.rank = src->proc.rank;
    pmix_strncpy((*dest)->key, src->key, PMIX_MAX_KEYLEN);
    return pmix_value_xfer(&(*dest)->value, &src->value);
}

pmix_status_t pmix12_bfrop_copy_darray(pmix_pdata_t **dest, pmix_data_array_t *src,
                                      pmix_data_type_t type)
{
    return PMIX_ERR_NOT_SUPPORTED;
}

pmix_status_t pmix12_bfrop_copy_proc_info(pmix_pdata_t **dest, pmix_proc_info_t *src,
                                      pmix_data_type_t type)
{
    return PMIX_ERR_NOT_SUPPORTED;
}

pmix_status_t pmix12_bfrop_copy_query(pmix_pdata_t **dest, pmix_query_t *src,
                                      pmix_data_type_t type)
{
    return PMIX_ERR_NOT_SUPPORTED;
}
