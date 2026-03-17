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
#include "bfrop_pmix20.h"
#include "internal.h"

 pmix_status_t pmix20_bfrop_copy(void **dest, void *src, pmix_data_type_t type)
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

    if (NULL == (info = (pmix_bfrop_type_info_t*)pmix_pointer_array_get_item(&mca_bfrops_v20_component.types, type))) {
        PMIX_ERROR_LOG(PMIX_ERR_UNKNOWN_DATA_TYPE);
        return PMIX_ERR_UNKNOWN_DATA_TYPE;
    }

    return info->odti_copy_fn(dest, src, type);
}

pmix_status_t pmix20_bfrop_copy_payload(pmix_buffer_t *dest, pmix_buffer_t *src)
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
 pmix_status_t pmix20_bfrop_std_copy(void **dest, void *src, pmix_data_type_t type)
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

    case PMIX_STATUS:
        datasize = sizeof(pmix_status_t);
        break;

    case PMIX_PROC_RANK:
        datasize = sizeof(pmix_rank_t);
        break;

    case PMIX_PERSIST:
        datasize = sizeof(pmix_persistence_t);
        break;

    case PMIX_POINTER:
        datasize = sizeof(char*);
        break;

    case PMIX_SCOPE:
        datasize = sizeof(pmix_scope_t);
        break;

    case PMIX_DATA_RANGE:
        datasize = sizeof(pmix_data_range_t);
        break;

    case PMIX_COMMAND:
        datasize = sizeof(pmix_cmd_t);
        break;

    case PMIX_INFO_DIRECTIVES:
        datasize = sizeof(pmix_info_directives_t);
        break;

    case PMIX_PROC_STATE:
        datasize = sizeof(pmix_proc_state_t);
        break;

    case PMIX_ALLOC_DIRECTIVE:
        datasize = sizeof(pmix_alloc_directive_t);
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

/* compare function for pmix_value_t*/
pmix_value_cmp_t pmix20_bfrop_value_cmp(pmix_value_t *p, pmix_value_t *p1)
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

/* COPY FUNCTIONS FOR NON-STANDARD SYSTEM TYPES */

/*
 * STRING
 */
 pmix_status_t pmix20_bfrop_copy_string(char **dest, char *src, pmix_data_type_t type)
{
    if (NULL == src) {  /* got zero-length string/NULL pointer - store NULL */
        *dest = NULL;
    } else {
        *dest = strdup(src);
    }

    return PMIX_SUCCESS;
}
/* compare function for pmix_value_t */
bool pmix_value_cmp(pmix_value_t *p, pmix_value_t *p1)
{
    bool rc = false;

    if (p->type != p1->type) {
        return rc;
    }

    switch (p->type) {
        case PMIX_UNDEF:
            rc = true;
            break;
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
        case PMIX_COMPRESSED_STRING:
            if (p->data.bo.size != p1->data.bo.size) {
                return false;
            } else {
                return true;
            }
        case PMIX_STATUS:
            rc = (p->data.status == p1->data.status);
            break;
        default:
            pmix_output(0, "COMPARE-PMIX-VALUE: UNSUPPORTED TYPE %d", (int)p->type);
    }
    return rc;
}
/* COPY FUNCTIONS FOR GENERIC PMIX TYPES - we
 * are not allocating memory and so we cannot
 * use the regular copy functions */
pmix_status_t pmix20_bfrop_value_xfer(pmix_value_t *p, const pmix_value_t *src)
{
    size_t n, m;
    pmix_status_t rc;
    char **prarray, **strarray;
    pmix_value_t *pv, *sv;
    pmix_info_t *p1, *s1;
    pmix_app_t *pa, *sa;
    pmix_pdata_t *pd, *sd;
    pmix_buffer_t *pb, *sb;
    pmix_byte_object_t *pbo, *sbo;
    pmix_kval_t *pk, *sk;
    pmix_modex_data_t *pm, *sm;
    pmix_proc_info_t *pi, *si;
    pmix_query_t *pq, *sq;

    /* copy the right field */
    p->type = src->type;
    switch (src->type) {
        case PMIX_UNDEF:
            break;
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
            memcpy(&p->data.tv, &src->data.tv, sizeof(struct timeval));
            break;
        case PMIX_TIME:
            memcpy(&p->data.time, &src->data.time, sizeof(time_t));
            break;
        case PMIX_STATUS:
            memcpy(&p->data.status, &src->data.status, sizeof(pmix_status_t));
            break;
        case PMIX_PROC:
            memcpy(&p->data.proc, &src->data.proc, sizeof(pmix_proc_t));
            break;
        case PMIX_PROC_RANK:
            memcpy(&p->data.proc, &src->data.rank, sizeof(pmix_rank_t));
            break;
        case PMIX_BYTE_OBJECT:
        case PMIX_COMPRESSED_STRING:
            memset(&p->data.bo, 0, sizeof(pmix_byte_object_t));
            if (NULL != src->data.bo.bytes && 0 < src->data.bo.size) {
                p->data.bo.bytes = malloc(src->data.bo.size);
                memcpy(p->data.bo.bytes, src->data.bo.bytes, src->data.bo.size);
                p->data.bo.size = src->data.bo.size;
            } else {
                p->data.bo.bytes = NULL;
                p->data.bo.size = 0;
            }
            break;
        case PMIX_PERSIST:
            memcpy(&p->data.persist, &src->data.persist, sizeof(pmix_persistence_t));
            break;
        case PMIX_SCOPE:
            memcpy(&p->data.scope, &src->data.scope, sizeof(pmix_scope_t));
            break;
        case PMIX_DATA_RANGE:
            memcpy(&p->data.range, &src->data.range, sizeof(pmix_data_range_t));
            break;
        case PMIX_PROC_STATE:
            memcpy(&p->data.state, &src->data.state, sizeof(pmix_proc_state_t));
            break;
        case PMIX_PROC_INFO:
            PMIX_PROC_INFO_CREATE(p->data.pinfo, 1);
            if (NULL != src->data.pinfo->hostname) {
                p->data.pinfo->hostname = strdup(src->data.pinfo->hostname);
            }
            if (NULL != src->data.pinfo->executable_name) {
                p->data.pinfo->executable_name = strdup(src->data.pinfo->executable_name);
            }
            memcpy(&p->data.pinfo->pid, &src->data.pinfo->pid, sizeof(pid_t));
            memcpy(&p->data.pinfo->exit_code, &src->data.pinfo->exit_code, sizeof(int));
            memcpy(&p->data.pinfo->state, &src->data.pinfo->state, sizeof(pmix_proc_state_t));
            break;
        case PMIX_DATA_ARRAY:
            p->data.darray = (pmix_data_array_t*)calloc(1, sizeof(pmix_data_array_t));
            p->data.darray->type = src->data.darray->type;
            p->data.darray->size = src->data.darray->size;
            if (0 == p->data.darray->size || NULL == src->data.darray->array) {
                p->data.darray->array = NULL;
                p->data.darray->size = 0;
                break;
            }
            /* allocate space and do the copy */
            switch (src->data.darray->type) {
                case PMIX_UINT8:
                case PMIX_INT8:
                case PMIX_BYTE:
                    p->data.darray->array = (char*)malloc(src->data.darray->size);
                    if (NULL == p->data.darray->array) {
                        return PMIX_ERR_NOMEM;
                    }
                    memcpy(p->data.darray->array, src->data.darray->array, src->data.darray->size);
                    break;
                case PMIX_UINT16:
                case PMIX_INT16:
                    p->data.darray->array = (char*)malloc(src->data.darray->size * sizeof(uint16_t));
                    if (NULL == p->data.darray->array) {
                        return PMIX_ERR_NOMEM;
                    }
                    memcpy(p->data.darray->array, src->data.darray->array, src->data.darray->size * sizeof(uint16_t));
                    break;
                case PMIX_UINT32:
                case PMIX_INT32:
                    p->data.darray->array = (char*)malloc(src->data.darray->size * sizeof(uint32_t));
                    if (NULL == p->data.darray->array) {
                        return PMIX_ERR_NOMEM;
                    }
                    memcpy(p->data.darray->array, src->data.darray->array, src->data.darray->size * sizeof(uint32_t));
                    break;
                case PMIX_UINT64:
                case PMIX_INT64:
                    p->data.darray->array = (char*)malloc(src->data.darray->size * sizeof(uint64_t));
                    if (NULL == p->data.darray->array) {
                        return PMIX_ERR_NOMEM;
                    }
                    memcpy(p->data.darray->array, src->data.darray->array, src->data.darray->size * sizeof(uint64_t));
                    break;
                case PMIX_BOOL:
                    p->data.darray->array = (char*)malloc(src->data.darray->size * sizeof(bool));
                    if (NULL == p->data.darray->array) {
                        return PMIX_ERR_NOMEM;
                    }
                    memcpy(p->data.darray->array, src->data.darray->array, src->data.darray->size * sizeof(bool));
                    break;
                case PMIX_SIZE:
                    p->data.darray->array = (char*)malloc(src->data.darray->size * sizeof(size_t));
                    if (NULL == p->data.darray->array) {
                        return PMIX_ERR_NOMEM;
                    }
                    memcpy(p->data.darray->array, src->data.darray->array, src->data.darray->size * sizeof(size_t));
                    break;
                case PMIX_PID:
                    p->data.darray->array = (char*)malloc(src->data.darray->size * sizeof(pid_t));
                    if (NULL == p->data.darray->array) {
                        return PMIX_ERR_NOMEM;
                    }
                    memcpy(p->data.darray->array, src->data.darray->array, src->data.darray->size * sizeof(pid_t));
                    break;
                case PMIX_STRING:
                    p->data.darray->array = (char**)malloc(src->data.darray->size * sizeof(char*));
                    if (NULL == p->data.darray->array) {
                        return PMIX_ERR_NOMEM;
                    }
                    prarray = (char**)p->data.darray->array;
                    strarray = (char**)src->data.darray->array;
                    for (n=0; n < src->data.darray->size; n++) {
                        if (NULL != strarray[n]) {
                            prarray[n] = strdup(strarray[n]);
                        }
                    }
                    break;
                case PMIX_INT:
                case PMIX_UINT:
                    p->data.darray->array = (char*)malloc(src->data.darray->size * sizeof(int));
                    if (NULL == p->data.darray->array) {
                        return PMIX_ERR_NOMEM;
                    }
                    memcpy(p->data.darray->array, src->data.darray->array, src->data.darray->size * sizeof(int));
                    break;
                case PMIX_FLOAT:
                    p->data.darray->array = (char*)malloc(src->data.darray->size * sizeof(float));
                    if (NULL == p->data.darray->array) {
                        return PMIX_ERR_NOMEM;
                    }
                    memcpy(p->data.darray->array, src->data.darray->array, src->data.darray->size * sizeof(float));
                    break;
                case PMIX_DOUBLE:
                    p->data.darray->array = (char*)malloc(src->data.darray->size * sizeof(double));
                    if (NULL == p->data.darray->array) {
                        return PMIX_ERR_NOMEM;
                    }
                    memcpy(p->data.darray->array, src->data.darray->array, src->data.darray->size * sizeof(double));
                    break;
                case PMIX_TIMEVAL:
                    p->data.darray->array = (struct timeval*)malloc(src->data.darray->size * sizeof(struct timeval));
                    if (NULL == p->data.darray->array) {
                        return PMIX_ERR_NOMEM;
                    }
                    memcpy(p->data.darray->array, src->data.darray->array, src->data.darray->size * sizeof(struct timeval));
                    break;
                case PMIX_TIME:
                    p->data.darray->array = (time_t*)malloc(src->data.darray->size * sizeof(time_t));
                    if (NULL == p->data.darray->array) {
                        return PMIX_ERR_NOMEM;
                    }
                    memcpy(p->data.darray->array, src->data.darray->array, src->data.darray->size * sizeof(time_t));
                    break;
                case PMIX_STATUS:
                    p->data.darray->array = (pmix_status_t*)malloc(src->data.darray->size * sizeof(pmix_status_t));
                    if (NULL == p->data.darray->array) {
                        return PMIX_ERR_NOMEM;
                    }
                    memcpy(p->data.darray->array, src->data.darray->array, src->data.darray->size * sizeof(pmix_status_t));
                    break;
                case PMIX_VALUE:
                    PMIX_VALUE_CREATE(p->data.darray->array, src->data.darray->size);
                    if (NULL == p->data.darray->array) {
                        return PMIX_ERR_NOMEM;
                    }
                    pv = (pmix_value_t*)p->data.darray->array;
                    sv = (pmix_value_t*)src->data.darray->array;
                    for (n=0; n < src->data.darray->size; n++) {
                        if (PMIX_SUCCESS != (rc = pmix20_bfrop_value_xfer(&pv[n], &sv[n]))) {
                            PMIX_VALUE_FREE(pv, src->data.darray->size);
                            return rc;
                        }
                    }
                    break;
                case PMIX_PROC:
                    PMIX_PROC_CREATE(p->data.darray->array, src->data.darray->size);
                    if (NULL == p->data.darray->array) {
                        return PMIX_ERR_NOMEM;
                    }
                    memcpy(p->data.darray->array, src->data.darray->array, src->data.darray->size * sizeof(pmix_proc_t));
                    break;
                case PMIX_APP:
                    PMIX_APP_CREATE(p->data.darray->array, src->data.darray->size);
                    if (NULL == p->data.darray->array) {
                        return PMIX_ERR_NOMEM;
                    }
                    pa = (pmix_app_t*)p->data.darray->array;
                    sa = (pmix_app_t*)src->data.darray->array;
                    for (n=0; n < src->data.darray->size; n++) {
                        if (NULL != sa[n].cmd) {
                            pa[n].cmd = strdup(sa[n].cmd);
                        }
                        if (NULL != sa[n].argv) {
                            pa[n].argv = pmix_argv_copy(sa[n].argv);
                        }
                        if (NULL != sa[n].env) {
                            pa[n].env = pmix_argv_copy(sa[n].env);
                        }
                        if (NULL != sa[n].cwd) {
                            pa[n].cwd = strdup(sa[n].cwd);
                        }
                        pa[n].maxprocs = sa[n].maxprocs;
                        if (0 < sa[n].ninfo && NULL != sa[n].info) {
                            PMIX_INFO_CREATE(pa[n].info, sa[n].ninfo);
                            if (NULL == pa[n].info) {
                                PMIX_APP_FREE(pa, src->data.darray->size);
                                return PMIX_ERR_NOMEM;
                            }
                            pa[n].ninfo = sa[n].ninfo;
                            for (m=0; m < pa[n].ninfo; m++) {
                                PMIX_INFO_XFER(&pa[n].info[m], &sa[n].info[m]);
                            }
                        }
                    }
                    break;
                case PMIX_INFO:
                    PMIX_INFO_CREATE(p->data.darray->array, src->data.darray->size);
                    p1 = (pmix_info_t*)p->data.darray->array;
                    s1 = (pmix_info_t*)src->data.darray->array;
                    for (n=0; n < src->data.darray->size; n++) {
                        PMIX_INFO_LOAD(&p1[n], s1[n].key, &s1[n].value.data.flag, s1[n].value.type);
                    }
                    break;
                case PMIX_PDATA:
                    PMIX_PDATA_CREATE(p->data.darray->array, src->data.darray->size);
                    if (NULL == p->data.darray->array) {
                        return PMIX_ERR_NOMEM;
                    }
                    pd = (pmix_pdata_t*)p->data.darray->array;
                    sd = (pmix_pdata_t*)src->data.darray->array;
                    for (n=0; n < src->data.darray->size; n++) {
                        PMIX_PDATA_LOAD(&pd[n], &sd[n].proc, sd[n].key, &sd[n].value.data.flag, sd[n].value.type);
                    }
                    break;
                case PMIX_BUFFER:
                    p->data.darray->array = (pmix_buffer_t*)malloc(src->data.darray->size * sizeof(pmix_buffer_t));
                    if (NULL == p->data.darray->array) {
                        return PMIX_ERR_NOMEM;
                    }
                    pb = (pmix_buffer_t*)p->data.darray->array;
                    sb = (pmix_buffer_t*)src->data.darray->array;
                    for (n=0; n < src->data.darray->size; n++) {
                        PMIX_CONSTRUCT(&pb[n], pmix_buffer_t);
                        pmix20_bfrop_copy_payload(&pb[n], &sb[n]);
                    }
                    break;
                case PMIX_BYTE_OBJECT:
                case PMIX_COMPRESSED_STRING:
                    p->data.darray->array = (pmix_byte_object_t*)malloc(src->data.darray->size * sizeof(pmix_byte_object_t));
                    if (NULL == p->data.darray->array) {
                        return PMIX_ERR_NOMEM;
                    }
                    pbo = (pmix_byte_object_t*)p->data.darray->array;
                    sbo = (pmix_byte_object_t*)src->data.darray->array;
                    for (n=0; n < src->data.darray->size; n++) {
                        if (NULL != sbo[n].bytes && 0 < sbo[n].size) {
                            pbo[n].size = sbo[n].size;
                            pbo[n].bytes = (char*)malloc(pbo[n].size);
                            memcpy(pbo[n].bytes, sbo[n].bytes, pbo[n].size);
                        } else {
                            pbo[n].bytes = NULL;
                            pbo[n].size = 0;
                        }
                    }
                    break;
                case PMIX_KVAL:
                    p->data.darray->array = (pmix_kval_t*)calloc(src->data.darray->size , sizeof(pmix_kval_t));
                    if (NULL == p->data.darray->array) {
                        return PMIX_ERR_NOMEM;
                    }
                    pk = (pmix_kval_t*)p->data.darray->array;
                    sk = (pmix_kval_t*)src->data.darray->array;
                    for (n=0; n < src->data.darray->size; n++) {
                        if (NULL != sk[n].key) {
                            pk[n].key = strdup(sk[n].key);
                        }
                        if (NULL != sk[n].value) {
                            PMIX_VALUE_CREATE(pk[n].value, 1);
                            if (NULL == pk[n].value) {
                                free(p->data.darray->array);
                                return PMIX_ERR_NOMEM;
                            }
                            if (PMIX_SUCCESS != (rc = pmix20_bfrop_value_xfer(pk[n].value, sk[n].value))) {
                                return rc;
                            }
                        }
                    }
                    break;
                case PMIX_MODEX:
                    PMIX_MODEX_CREATE(p->data.darray->array, src->data.darray->size);
                    if (NULL == p->data.darray->array) {
                        return PMIX_ERR_NOMEM;
                    }
                    pm = (pmix_modex_data_t*)p->data.darray->array;
                    sm = (pmix_modex_data_t*)src->data.darray->array;
                    for (n=0; n < src->data.darray->size; n++) {
                        memcpy(&pm[n], &sm[n], sizeof(pmix_modex_data_t));
                        if (NULL != sm[n].blob && 0 < sm[n].size) {
                            pm[n].blob = (uint8_t*)malloc(sm[n].size);
                            if (NULL == pm[n].blob) {
                                return PMIX_ERR_NOMEM;
                            }
                            memcpy(pm[n].blob, sm[n].blob, sm[n].size);
                            pm[n].size = sm[n].size;
                        } else {
                            pm[n].blob = NULL;
                            pm[n].size = 0;
                        }
                    }
                    break;
                case PMIX_PERSIST:
                    p->data.darray->array = (pmix_persistence_t*)malloc(src->data.darray->size * sizeof(pmix_persistence_t));
                    if (NULL == p->data.darray->array) {
                        return PMIX_ERR_NOMEM;
                    }
                    memcpy(p->data.darray->array, src->data.darray->array, src->data.darray->size * sizeof(pmix_persistence_t));
                    break;
                case PMIX_POINTER:
                    p->data.darray->array = (char**)malloc(src->data.darray->size * sizeof(char*));
                    if (NULL == p->data.darray->array) {
                        return PMIX_ERR_NOMEM;
                    }
                    prarray = (char**)p->data.darray->array;
                    strarray = (char**)src->data.darray->array;
                    for (n=0; n < src->data.darray->size; n++) {
                        prarray[n] = strarray[n];
                    }
                    break;
                case PMIX_SCOPE:
                    p->data.darray->array = (pmix_scope_t*)malloc(src->data.darray->size * sizeof(pmix_scope_t));
                    if (NULL == p->data.darray->array) {
                        return PMIX_ERR_NOMEM;
                    }
                    memcpy(p->data.darray->array, src->data.darray->array, src->data.darray->size * sizeof(pmix_scope_t));
                    break;
                case PMIX_DATA_RANGE:
                    p->data.darray->array = (pmix_data_range_t*)malloc(src->data.darray->size * sizeof(pmix_data_range_t));
                    if (NULL == p->data.darray->array) {
                        return PMIX_ERR_NOMEM;
                    }
                    memcpy(p->data.darray->array, src->data.darray->array, src->data.darray->size * sizeof(pmix_data_range_t));
                    break;
                case PMIX_COMMAND:
                    p->data.darray->array = (pmix_cmd_t*)malloc(src->data.darray->size * sizeof(pmix_cmd_t));
                    if (NULL == p->data.darray->array) {
                        return PMIX_ERR_NOMEM;
                    }
                    memcpy(p->data.darray->array, src->data.darray->array, src->data.darray->size * sizeof(pmix_cmd_t));
                    break;
                case PMIX_INFO_DIRECTIVES:
                    p->data.darray->array = (pmix_info_directives_t*)malloc(src->data.darray->size * sizeof(pmix_info_directives_t));
                    if (NULL == p->data.darray->array) {
                        return PMIX_ERR_NOMEM;
                    }
                    memcpy(p->data.darray->array, src->data.darray->array, src->data.darray->size * sizeof(pmix_info_directives_t));
                    break;
                case PMIX_PROC_INFO:
                    PMIX_PROC_INFO_CREATE(p->data.darray->array, src->data.darray->size);
                    if (NULL == p->data.darray->array) {
                        return PMIX_ERR_NOMEM;
                    }
                    pi = (pmix_proc_info_t*)p->data.darray->array;
                    si = (pmix_proc_info_t*)src->data.darray->array;
                    for (n=0; n < src->data.darray->size; n++) {
                        memcpy(&pi[n].proc, &si[n].proc, sizeof(pmix_proc_t));
                        if (NULL != si[n].hostname) {
                            pi[n].hostname = strdup(si[n].hostname);
                        } else {
                            pi[n].hostname = NULL;
                        }
                        if (NULL != si[n].executable_name) {
                            pi[n].executable_name = strdup(si[n].executable_name);
                        } else {
                            pi[n].executable_name = NULL;
                        }
                        pi[n].pid = si[n].pid;
                        pi[n].exit_code = si[n].exit_code;
                        pi[n].state = si[n].state;
                    }
                    break;
                case PMIX_DATA_ARRAY:
                    return PMIX_ERR_NOT_SUPPORTED;  // don't support iterative arrays
                case PMIX_QUERY:
                    PMIX_QUERY_CREATE(p->data.darray->array, src->data.darray->size);
                    if (NULL == p->data.darray->array) {
                        return PMIX_ERR_NOMEM;
                    }
                    pq = (pmix_query_t*)p->data.darray->array;
                    sq = (pmix_query_t*)src->data.darray->array;
                    for (n=0; n < src->data.darray->size; n++) {
                        if (NULL != sq[n].keys) {
                            pq[n].keys = pmix_argv_copy(sq[n].keys);
                        }
                        if (NULL != sq[n].qualifiers && 0 < sq[n].nqual) {
                            PMIX_INFO_CREATE(pq[n].qualifiers, sq[n].nqual);
                            if (NULL == pq[n].qualifiers) {
                                PMIX_QUERY_FREE(pq, src->data.darray->size);
                                return PMIX_ERR_NOMEM;
                            }
                            for (m=0; m < sq[n].nqual; m++) {
                                PMIX_INFO_XFER(&pq[n].qualifiers[m], &sq[n].qualifiers[m]);
                            }
                            pq[n].nqual = sq[n].nqual;
                        } else {
                            pq[n].qualifiers = NULL;
                            pq[n].nqual = 0;
                        }
                    }
                    break;
                default:
                    return PMIX_ERR_UNKNOWN_DATA_TYPE;
            }
            break;
        case PMIX_POINTER:
            memcpy(&p->data.ptr, &src->data.ptr, sizeof(void*));
            break;
        /**** DEPRECATED ****/
        case PMIX_INFO_ARRAY:
            return PMIX_ERR_NOT_SUPPORTED;
        /********************/
        default:
            pmix_output(0, "COPY-PMIX-VALUE: UNSUPPORTED TYPE %d", (int)src->type);
            return PMIX_ERROR;
    }
    return PMIX_SUCCESS;
}

/* PMIX_VALUE */
pmix_status_t pmix20_bfrop_copy_value(pmix_value_t **dest, pmix_value_t *src,
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
    return pmix20_bfrop_value_xfer(p, src);
}

pmix_status_t pmix20_bfrop_copy_info(pmix_info_t **dest, pmix_info_t *src,
                                   pmix_data_type_t type)
{
    *dest = (pmix_info_t*)malloc(sizeof(pmix_info_t));
    pmix_strncpy((*dest)->key, src->key, PMIX_MAX_KEYLEN);
    (*dest)->flags = src->flags;
    return pmix20_bfrop_value_xfer(&(*dest)->value, &src->value);
}

pmix_status_t pmix20_bfrop_copy_buf(pmix_buffer_t **dest, pmix_buffer_t *src,
                                  pmix_data_type_t type)
{
    *dest = PMIX_NEW(pmix_buffer_t);
    pmix20_bfrop_copy_payload(*dest, src);
    return PMIX_SUCCESS;
}

pmix_status_t pmix20_bfrop_copy_app(pmix_app_t **dest, pmix_app_t *src,
                                  pmix_data_type_t type)
{
    size_t j;

    *dest = (pmix_app_t*)malloc(sizeof(pmix_app_t));
    (*dest)->cmd = strdup(src->cmd);
    (*dest)->argv = pmix_argv_copy(src->argv);
    (*dest)->env = pmix_argv_copy(src->env);
    if (NULL != src->cwd) {
        (*dest)->cwd = strdup(src->cwd);
    }
    (*dest)->maxprocs = src->maxprocs;
    (*dest)->ninfo = src->ninfo;
    (*dest)->info = (pmix_info_t*)malloc(src->ninfo * sizeof(pmix_info_t));
    for (j=0; j < src->ninfo; j++) {
        pmix_strncpy((*dest)->info[j].key, src->info[j].key, PMIX_MAX_KEYLEN);
        pmix20_bfrop_value_xfer(&(*dest)->info[j].value, &src->info[j].value);
    }
    return PMIX_SUCCESS;
}

pmix_status_t pmix20_bfrop_copy_kval(pmix_kval_t **dest, pmix_kval_t *src,
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
    return pmix20_bfrop_value_xfer(p->value, src->value);
}

pmix_status_t pmix20_bfrop_copy_proc(pmix_proc_t **dest, pmix_proc_t *src,
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

pmix_status_t pmix20_bfrop_copy_modex(pmix_modex_data_t **dest, pmix_modex_data_t *src,
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

pmix_status_t pmix20_bfrop_copy_persist(pmix_persistence_t **dest, pmix_persistence_t *src,
                                      pmix_data_type_t type)
{
    *dest = (pmix_persistence_t*)malloc(sizeof(pmix_persistence_t));
    if (NULL == *dest) {
        return PMIX_ERR_OUT_OF_RESOURCE;
    }
    memcpy(*dest, src, sizeof(pmix_persistence_t));
    return PMIX_SUCCESS;
}

pmix_status_t pmix20_bfrop_copy_bo(pmix_byte_object_t **dest, pmix_byte_object_t *src,
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

pmix_status_t pmix20_bfrop_copy_pdata(pmix_pdata_t **dest, pmix_pdata_t *src,
                                    pmix_data_type_t type)
{
    *dest = (pmix_pdata_t*)malloc(sizeof(pmix_pdata_t));
    pmix_strncpy((*dest)->proc.nspace, src->proc.nspace, PMIX_MAX_NSLEN);
    (*dest)->proc.rank = src->proc.rank;
    pmix_strncpy((*dest)->key, src->key, PMIX_MAX_KEYLEN);
    return pmix20_bfrop_value_xfer(&(*dest)->value, &src->value);
}

pmix_status_t pmix20_bfrop_copy_pinfo(pmix_proc_info_t **dest, pmix_proc_info_t *src,
                                    pmix_data_type_t type)
{
    *dest = (pmix_proc_info_t*)malloc(sizeof(pmix_proc_info_t));
    pmix_strncpy((*dest)->proc.nspace, src->proc.nspace, PMIX_MAX_NSLEN);
    (*dest)->proc.rank = src->proc.rank;
    if (NULL != src->hostname) {
        (*dest)->hostname = strdup(src->hostname);
    }
    if (NULL != src->executable_name) {
        (*dest)->executable_name = strdup(src->executable_name);
    }
    (*dest)->pid = src->pid;
    (*dest)->exit_code = src->exit_code;
    (*dest)->state = src->state;
    return PMIX_SUCCESS;
}

/* the pmix_data_array_t is a little different in that it
 * is an array of values, and so we cannot just copy one
 * value at a time. So handle all value types here */
pmix_status_t pmix20_bfrop_copy_darray(pmix_data_array_t **dest,
                                     pmix_data_array_t *src,
                                     pmix_data_type_t type)
{
    pmix_data_array_t *p;
    size_t n, m;
    pmix_status_t rc;
    char **prarray, **strarray;
    pmix_value_t *pv, *sv;
    pmix_app_t *pa, *sa;
    pmix_info_t *p1, *s1;
    pmix_pdata_t *pd, *sd;
    pmix_buffer_t *pb, *sb;
    pmix_byte_object_t *pbo, *sbo;
    pmix_kval_t *pk, *sk;
    pmix_modex_data_t *pm, *sm;
    pmix_proc_info_t *pi, *si;
    pmix_query_t *pq, *sq;

    p = (pmix_data_array_t*)calloc(1, sizeof(pmix_data_array_t));
    if (NULL == p) {
        return PMIX_ERR_NOMEM;
    }
    p->type = src->type;
    p->size = src->size;
    if (0 == p->size || NULL == src->array) {
        *dest = p;
        return PMIX_SUCCESS;
    }

    /* process based on type of array element */
    switch (src->type) {
        case PMIX_UINT8:
        case PMIX_INT8:
        case PMIX_BYTE:
            p->array = (char*)malloc(src->size);
            if (NULL == p->array) {
                free(p);
                return PMIX_ERR_NOMEM;
            }
            memcpy(p->array, src->array, src->size);
            break;
        case PMIX_UINT16:
        case PMIX_INT16:
            p->array = (char*)malloc(src->size * sizeof(uint16_t));
            if (NULL == p->array) {
                free(p);
                return PMIX_ERR_NOMEM;
            }
            memcpy(p->array, src->array, src->size * sizeof(uint16_t));
            break;
        case PMIX_UINT32:
        case PMIX_INT32:
            p->array = (char*)malloc(src->size * sizeof(uint32_t));
            if (NULL == p->array) {
                free(p);
                return PMIX_ERR_NOMEM;
            }
            memcpy(p->array, src->array, src->size * sizeof(uint32_t));
            break;
        case PMIX_UINT64:
        case PMIX_INT64:
            p->array = (char*)malloc(src->size * sizeof(uint64_t));
            if (NULL == p->array) {
                free(p);
                return PMIX_ERR_NOMEM;
            }
            memcpy(p->array, src->array, src->size * sizeof(uint64_t));
            break;
        case PMIX_BOOL:
            p->array = (char*)malloc(src->size * sizeof(bool));
            if (NULL == p->array) {
                free(p);
                return PMIX_ERR_NOMEM;
            }
            memcpy(p->array, src->array, src->size * sizeof(bool));
            break;
        case PMIX_SIZE:
            p->array = (char*)malloc(src->size * sizeof(size_t));
            if (NULL == p->array) {
                free(p);
                return PMIX_ERR_NOMEM;
            }
            memcpy(p->array, src->array, src->size * sizeof(size_t));
            break;
        case PMIX_PID:
            p->array = (char*)malloc(src->size * sizeof(pid_t));
            if (NULL == p->array) {
                free(p);
                return PMIX_ERR_NOMEM;
            }
            memcpy(p->array, src->array, src->size * sizeof(pid_t));
            break;
        case PMIX_STRING:
            p->array = (char**)malloc(src->size * sizeof(char*));
            if (NULL == p->array) {
                free(p);
                return PMIX_ERR_NOMEM;
            }
            prarray = (char**)p->array;
            strarray = (char**)src->array;
            for (n=0; n < src->size; n++) {
                if (NULL != strarray[n]) {
                    prarray[n] = strdup(strarray[n]);
                }
            }
            break;
        case PMIX_INT:
        case PMIX_UINT:
            p->array = (char*)malloc(src->size * sizeof(int));
            if (NULL == p->array) {
                free(p);
                return PMIX_ERR_NOMEM;
            }
            memcpy(p->array, src->array, src->size * sizeof(int));
            break;
        case PMIX_FLOAT:
            p->array = (char*)malloc(src->size * sizeof(float));
            if (NULL == p->array) {
                free(p);
                return PMIX_ERR_NOMEM;
            }
            memcpy(p->array, src->array, src->size * sizeof(float));
            break;
        case PMIX_DOUBLE:
            p->array = (char*)malloc(src->size * sizeof(double));
            if (NULL == p->array) {
                free(p);
                return PMIX_ERR_NOMEM;
            }
            memcpy(p->array, src->array, src->size * sizeof(double));
            break;
        case PMIX_TIMEVAL:
            p->array = (struct timeval*)malloc(src->size * sizeof(struct timeval));
            if (NULL == p->array) {
                free(p);
                return PMIX_ERR_NOMEM;
            }
            memcpy(p->array, src->array, src->size * sizeof(struct timeval));
            break;
        case PMIX_TIME:
            p->array = (time_t*)malloc(src->size * sizeof(time_t));
            if (NULL == p->array) {
                free(p);
                return PMIX_ERR_NOMEM;
            }
            memcpy(p->array, src->array, src->size * sizeof(time_t));
            break;
        case PMIX_STATUS:
            p->array = (pmix_status_t*)malloc(src->size * sizeof(pmix_status_t));
            if (NULL == p->array) {
                free(p);
                return PMIX_ERR_NOMEM;
            }
            memcpy(p->array, src->array, src->size * sizeof(pmix_status_t));
            break;
        case PMIX_VALUE:
            PMIX_VALUE_CREATE(p->array, src->size);
            if (NULL == p->array) {
                free(p);
                return PMIX_ERR_NOMEM;
            }
            pv = (pmix_value_t*)p->array;
            sv = (pmix_value_t*)src->array;
            for (n=0; n < src->size; n++) {
                if (PMIX_SUCCESS != (rc = pmix20_bfrop_value_xfer(&pv[n], &sv[n]))) {
                    PMIX_VALUE_FREE(pv, src->size);
                    free(p);
                    return rc;
                }
            }
            break;
        case PMIX_PROC:
            PMIX_PROC_CREATE(p->array, src->size);
            if (NULL == p->array) {
                free(p);
                return PMIX_ERR_NOMEM;
            }
            memcpy(p->array, src->array, src->size * sizeof(pmix_proc_t));
            break;
        case PMIX_PROC_RANK:
            p->array = (char*)malloc(src->size * sizeof(pmix_rank_t));
            if (NULL == p->array) {
                free(p);
                return PMIX_ERR_NOMEM;
            }
            memcpy(p->array, src->array, src->size * sizeof(pmix_proc_t));
            break;
        case PMIX_APP:
            PMIX_APP_CREATE(p->array, src->size);
            if (NULL == p->array) {
                free(p);
                return PMIX_ERR_NOMEM;
            }
            pa = (pmix_app_t*)p->array;
            sa = (pmix_app_t*)src->array;
            for (n=0; n < src->size; n++) {
                if (NULL != sa[n].cmd) {
                    pa[n].cmd = strdup(sa[n].cmd);
                }
                if (NULL != sa[n].argv) {
                    pa[n].argv = pmix_argv_copy(sa[n].argv);
                }
                if (NULL != sa[n].env) {
                    pa[n].env = pmix_argv_copy(sa[n].env);
                }
                if (NULL != sa[n].cwd) {
                    pa[n].cwd = strdup(sa[n].cwd);
                }
                pa[n].maxprocs = sa[n].maxprocs;
                if (0 < sa[n].ninfo && NULL != sa[n].info) {
                    PMIX_INFO_CREATE(pa[n].info, sa[n].ninfo);
                    if (NULL == pa[n].info) {
                        PMIX_APP_FREE(pa, p->size);
                        free(p);
                        return PMIX_ERR_NOMEM;
                    }
                    pa[n].ninfo = sa[n].ninfo;
                    for (m=0; m < pa[n].ninfo; m++) {
                        PMIX_INFO_XFER(&pa[n].info[m], &sa[n].info[m]);
                    }
                }
            }
            break;
        case PMIX_INFO:
            PMIX_INFO_CREATE(p->array, src->size);
            if (NULL == p->array) {
                free(p);
                return PMIX_ERR_NOMEM;
            }
            p1 = (pmix_info_t*)p->array;
            s1 = (pmix_info_t*)src->array;
            for (n=0; n < src->size; n++) {
                PMIX_INFO_LOAD(&p1[n], s1[n].key, &s1[n].value.data.flag, s1[n].value.type);
            }
            break;
        case PMIX_PDATA:
            PMIX_PDATA_CREATE(p->array, src->size);
            if (NULL == p->array) {
                free(p);
                return PMIX_ERR_NOMEM;
            }
            pd = (pmix_pdata_t*)p->array;
            sd = (pmix_pdata_t*)src->array;
            for (n=0; n < src->size; n++) {
                PMIX_PDATA_LOAD(&pd[n], &sd[n].proc, sd[n].key, &sd[n].value.data, sd[n].value.type);
            }
            break;
        case PMIX_BUFFER:
            p->array = (pmix_buffer_t*)malloc(src->size * sizeof(pmix_buffer_t));
            if (NULL == p->array) {
                free(p);
                return PMIX_ERR_NOMEM;
            }
            pb = (pmix_buffer_t*)p->array;
            sb = (pmix_buffer_t*)src->array;
            for (n=0; n < src->size; n++) {
                PMIX_CONSTRUCT(&pb[n], pmix_buffer_t);
                pmix20_bfrop_copy_payload(&pb[n], &sb[n]);
            }
            break;
        case PMIX_BYTE_OBJECT:
        case PMIX_COMPRESSED_STRING:
            p->array = (pmix_byte_object_t*)malloc(src->size * sizeof(pmix_byte_object_t));
            if (NULL == p->array) {
                free(p);
                return PMIX_ERR_NOMEM;
            }
            pbo = (pmix_byte_object_t*)p->array;
            sbo = (pmix_byte_object_t*)src->array;
            for (n=0; n < src->size; n++) {
                if (NULL != sbo[n].bytes && 0 < sbo[n].size) {
                    pbo[n].size = sbo[n].size;
                    pbo[n].bytes = (char*)malloc(pbo[n].size);
                    memcpy(pbo[n].bytes, sbo[n].bytes, pbo[n].size);
                } else {
                    pbo[n].bytes = NULL;
                    pbo[n].size = 0;
                }
            }
            break;
        case PMIX_KVAL:
            p->array = (pmix_kval_t*)calloc(src->size , sizeof(pmix_kval_t));
            if (NULL == p->array) {
                free(p);
                return PMIX_ERR_NOMEM;
            }
            pk = (pmix_kval_t*)p->array;
            sk = (pmix_kval_t*)src->array;
            for (n=0; n < src->size; n++) {
                if (NULL != sk[n].key) {
                    pk[n].key = strdup(sk[n].key);
                }
                if (NULL != sk[n].value) {
                    PMIX_VALUE_CREATE(pk[n].value, 1);
                    if (NULL == pk[n].value) {
                        PMIX_VALUE_FREE(pk[n].value, 1);
                        free(p);
                        return PMIX_ERR_NOMEM;
                    }
                    if (PMIX_SUCCESS != (rc = pmix20_bfrop_value_xfer(pk[n].value, sk[n].value))) {
                        PMIX_VALUE_FREE(pk[n].value, 1);
                        free(p);
                        return rc;
                    }
                }
            }
            break;
        case PMIX_MODEX:
            PMIX_MODEX_CREATE(p->array, src->size);
            if (NULL == p->array) {
                free(p);
                return PMIX_ERR_NOMEM;
            }
            pm = (pmix_modex_data_t*)p->array;
            sm = (pmix_modex_data_t*)src->array;
            for (n=0; n < src->size; n++) {
                memcpy(&pm[n], &sm[n], sizeof(pmix_modex_data_t));
                if (NULL != sm[n].blob && 0 < sm[n].size) {
                    pm[n].blob = (uint8_t*)malloc(sm[n].size);
                    if (NULL == pm[n].blob) {
                        PMIX_MODEX_FREE(pm, src->size);
                        free(p);
                        return PMIX_ERR_NOMEM;
                    }
                    memcpy(pm[n].blob, sm[n].blob, sm[n].size);
                    pm[n].size = sm[n].size;
                } else {
                    pm[n].blob = NULL;
                    pm[n].size = 0;
                }
            }
            break;
        case PMIX_PERSIST:
            p->array = (pmix_persistence_t*)malloc(src->size * sizeof(pmix_persistence_t));
            if (NULL == p->array) {
                free(p);
                return PMIX_ERR_NOMEM;
            }
            memcpy(p->array, src->array, src->size * sizeof(pmix_persistence_t));
            break;
        case PMIX_POINTER:
            p->array = (char**)malloc(src->size * sizeof(char*));
            prarray = (char**)p->array;
            strarray = (char**)src->array;
            for (n=0; n < src->size; n++) {
                prarray[n] = strarray[n];
            }
            break;
        case PMIX_SCOPE:
            p->array = (pmix_scope_t*)malloc(src->size * sizeof(pmix_scope_t));
            if (NULL == p->array) {
                free(p);
                return PMIX_ERR_NOMEM;
            }
            memcpy(p->array, src->array, src->size * sizeof(pmix_scope_t));
            break;
        case PMIX_DATA_RANGE:
            p->array = (pmix_data_range_t*)malloc(src->size * sizeof(pmix_data_range_t));
            if (NULL == p->array) {
                free(p);
                return PMIX_ERR_NOMEM;
            }
            memcpy(p->array, src->array, src->size * sizeof(pmix_data_range_t));
            break;
        case PMIX_COMMAND:
            p->array = (pmix_cmd_t*)malloc(src->size * sizeof(pmix_cmd_t));
            if (NULL == p->array) {
                free(p);
                return PMIX_ERR_NOMEM;
            }
            memcpy(p->array, src->array, src->size * sizeof(pmix_cmd_t));
            break;
        case PMIX_INFO_DIRECTIVES:
            p->array = (pmix_info_directives_t*)malloc(src->size * sizeof(pmix_info_directives_t));
            if (NULL == p->array) {
                free(p);
                return PMIX_ERR_NOMEM;
            }
            memcpy(p->array, src->array, src->size * sizeof(pmix_info_directives_t));
            break;
        case PMIX_PROC_INFO:
            PMIX_PROC_INFO_CREATE(p->array, src->size);
            if (NULL == p->array) {
                free(p);
                return PMIX_ERR_NOMEM;
            }
            pi = (pmix_proc_info_t*)p->array;
            si = (pmix_proc_info_t*)src->array;
            for (n=0; n < src->size; n++) {
                memcpy(&pi[n].proc, &si[n].proc, sizeof(pmix_proc_t));
                if (NULL != si[n].hostname) {
                    pi[n].hostname = strdup(si[n].hostname);
                } else {
                    pi[n].hostname = NULL;
                }
                if (NULL != si[n].executable_name) {
                    pi[n].executable_name = strdup(si[n].executable_name);
                } else {
                    pi[n].executable_name = NULL;
                }
                pi[n].pid = si[n].pid;
                pi[n].exit_code = si[n].exit_code;
                pi[n].state = si[n].state;
            }
            break;
        case PMIX_DATA_ARRAY:
            free(p);
            return PMIX_ERR_NOT_SUPPORTED;  // don't support iterative arrays
        case PMIX_QUERY:
            PMIX_QUERY_CREATE(p->array, src->size);
            if (NULL == p->array) {
                free(p);
                return PMIX_ERR_NOMEM;
            }
            pq = (pmix_query_t*)p->array;
            sq = (pmix_query_t*)src->array;
            for (n=0; n < src->size; n++) {
                if (NULL != sq[n].keys) {
                    pq[n].keys = pmix_argv_copy(sq[n].keys);
                }
                if (NULL != sq[n].qualifiers && 0 < sq[n].nqual) {
                    PMIX_INFO_CREATE(pq[n].qualifiers, sq[n].nqual);
                    if (NULL == pq[n].qualifiers) {
                        PMIX_INFO_FREE(pq[n].qualifiers, sq[n].nqual);
                        free(p);
                        return PMIX_ERR_NOMEM;
                    }
                    for (m=0; m < sq[n].nqual; m++) {
                        PMIX_INFO_XFER(&pq[n].qualifiers[m], &sq[n].qualifiers[m]);
                    }
                    pq[n].nqual = sq[n].nqual;
                } else {
                    pq[n].qualifiers = NULL;
                    pq[n].nqual = 0;
                }
            }
            break;
        default:
            free(p);
            return PMIX_ERR_UNKNOWN_DATA_TYPE;
    }

    (*dest) = p;
    return PMIX_SUCCESS;
}

pmix_status_t pmix20_bfrop_copy_query(pmix_query_t **dest,
                                    pmix_query_t *src,
                                    pmix_data_type_t type)
{
    pmix_status_t rc;

    *dest = (pmix_query_t*)malloc(sizeof(pmix_query_t));
    if (NULL != src->keys) {
        (*dest)->keys = pmix_argv_copy(src->keys);
    }
    (*dest)->nqual = src->nqual;
    if (NULL != src->qualifiers) {
        if (PMIX_SUCCESS != (rc = pmix20_bfrop_copy_info(&((*dest)->qualifiers), src->qualifiers, PMIX_INFO))) {
            free(*dest);
            return rc;
        }
    }
    return PMIX_SUCCESS;
}

/**** DEPRECATED ****/
pmix_status_t pmix20_bfrop_copy_array(pmix_info_array_t **dest,
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
/*******************/
