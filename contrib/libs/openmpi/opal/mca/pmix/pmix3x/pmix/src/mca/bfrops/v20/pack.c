/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2007 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2011-2013 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2014-2018 Intel, Inc.  All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2016      Mellanox Technologies, Inc.
 *                         All rights reserved.
 * Copyright (c) 2016      IBM Corporation.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include <src/include/pmix_config.h>

#include <src/include/types.h>

#ifdef HAVE_ARPA_INET_H
#include <arpa/inet.h>
#endif

#include "src/util/argv.h"
#include "src/util/error.h"
#include "src/util/output.h"
#include "src/mca/bfrops/base/base.h"
#include "bfrop_pmix20.h"
#include "internal.h"

pmix_status_t pmix20_bfrop_pack(pmix_buffer_t *buffer,
                              const void *src, int32_t num_vals,
                              pmix_data_type_t type)
 {
    pmix_status_t rc;

    /* check for error */
    if (NULL == buffer) {
        return PMIX_ERR_BAD_PARAM;
    }

    /* Pack the number of values */
    if (PMIX_BFROP_BUFFER_FULLY_DESC == buffer->type) {
        if (PMIX_SUCCESS != (rc = pmix20_bfrop_store_data_type(buffer, PMIX_INT32))) {
            return rc;
        }
    }
    if (PMIX_SUCCESS != (rc = pmix20_bfrop_pack_int32(buffer, &num_vals, 1, PMIX_INT32))) {
        return rc;
    }

    /* Pack the value(s) */
    return pmix20_bfrop_pack_buffer(buffer, src, num_vals, type);
}

pmix_status_t pmix20_bfrop_pack_buffer(pmix_buffer_t *buffer,
                                     const void *src, int32_t num_vals,
                                     pmix_data_type_t type)
{
    pmix_data_type_t v20type;
    pmix_status_t rc;
    pmix_bfrop_type_info_t *info;

    pmix_output_verbose(20, pmix_bfrops_base_framework.framework_output,
                        "pmix20_bfrop_pack_buffer( %p, %p, %lu, %d )\n",
                        (void*)buffer, src, (long unsigned int)num_vals, (int)type);

    /* some v20 types are simply declared differently */
    switch (type) {
        case PMIX_COMMAND:
            v20type = PMIX_UINT32;
            break;
        default:
            v20type = type;
    }

    /* Pack the declared data type */
    if (PMIX_BFROP_BUFFER_FULLY_DESC == buffer->type) {
        if (PMIX_SUCCESS != (rc = pmix20_bfrop_store_data_type(buffer, v20type))) {
            return rc;
        }
    }

    /* Lookup the pack function for this type and call it */

    if (NULL == (info = (pmix_bfrop_type_info_t*)pmix_pointer_array_get_item(&mca_bfrops_v20_component.types, v20type))) {
        return PMIX_ERR_PACK_FAILURE;
    }

    return info->odti_pack_fn(buffer, src, num_vals, v20type);
}


/* PACK FUNCTIONS FOR GENERIC SYSTEM TYPES */

/*
 * BOOL
 */
pmix_status_t pmix20_bfrop_pack_bool(pmix_buffer_t *buffer, const void *src,
                                   int32_t num_vals, pmix_data_type_t type)
 {
    uint8_t *dst;
    int32_t i;
    bool *s = (bool*)src;

    pmix_output_verbose(20, pmix_bfrops_base_framework.framework_output,
                        "pmix20_bfrop_pack_bool * %d\n", num_vals);
    /* check to see if buffer needs extending */
    if (NULL == (dst = (uint8_t*)pmix_bfrop_buffer_extend(buffer, num_vals))) {
        return PMIX_ERR_OUT_OF_RESOURCE;
    }

    /* store the data */
    for (i=0; i < num_vals; i++) {
        if (s[i]) {
            dst[i] = 1;
        } else {
            dst[i] = 0;
        }
    }

    /* update buffer pointers */
    buffer->pack_ptr += num_vals;
    buffer->bytes_used += num_vals;

    return PMIX_SUCCESS;
}

/*
 * INT
 */
pmix_status_t pmix20_bfrop_pack_int(pmix_buffer_t *buffer, const void *src,
                                  int32_t num_vals, pmix_data_type_t type)
 {
    pmix_status_t ret;

    /* System types need to always be described so we can properly
       unpack them */
    if (PMIX_SUCCESS != (ret = pmix20_bfrop_store_data_type(buffer, BFROP_TYPE_INT))) {
        return ret;
    }

    /* Turn around and pack the real type */
    return pmix20_bfrop_pack_buffer(buffer, src, num_vals, BFROP_TYPE_INT);
}

/*
 * SIZE_T
 */
pmix_status_t pmix20_bfrop_pack_sizet(pmix_buffer_t *buffer, const void *src,
                                    int32_t num_vals, pmix_data_type_t type)
 {
    pmix_status_t ret;

    /* System types need to always be described so we can properly
       unpack them. */
    if (PMIX_SUCCESS != (ret = pmix20_bfrop_store_data_type(buffer, BFROP_TYPE_SIZE_T))) {
        return ret;
    }

    return pmix20_bfrop_pack_buffer(buffer, src, num_vals, BFROP_TYPE_SIZE_T);
}

/*
 * PID_T
 */
pmix_status_t pmix20_bfrop_pack_pid(pmix_buffer_t *buffer, const void *src,
                                  int32_t num_vals, pmix_data_type_t type)
 {
    pmix_status_t ret;

    /* System types need to always be described so we can properly
       unpack them. */
    if (PMIX_SUCCESS != (ret = pmix20_bfrop_store_data_type(buffer, BFROP_TYPE_PID_T))) {
        return ret;
    }

    /* Turn around and pack the real type */
    return pmix20_bfrop_pack_buffer(buffer, src, num_vals, BFROP_TYPE_PID_T);
}


/* PACK FUNCTIONS FOR NON-GENERIC SYSTEM TYPES */

/*
 * BYTE, CHAR, INT8
 */
pmix_status_t pmix20_bfrop_pack_byte(pmix_buffer_t *buffer, const void *src,
                                   int32_t num_vals, pmix_data_type_t type)
 {
    char *dst;

    pmix_output_verbose(20, pmix_bfrops_base_framework.framework_output,
                        "pmix20_bfrop_pack_byte * %d\n", num_vals);
    /* check to see if buffer needs extending */
    if (NULL == (dst = pmix_bfrop_buffer_extend(buffer, num_vals))) {
        return PMIX_ERR_OUT_OF_RESOURCE;
    }

    /* store the data */
    memcpy(dst, src, num_vals);

    /* update buffer pointers */
    buffer->pack_ptr += num_vals;
    buffer->bytes_used += num_vals;

    return PMIX_SUCCESS;
}

/*
 * INT16
 */
pmix_status_t pmix20_bfrop_pack_int16(pmix_buffer_t *buffer, const void *src,
                                    int32_t num_vals, pmix_data_type_t type)
 {
    int32_t i;
    uint16_t tmp, *srctmp = (uint16_t*) src;
    char *dst;

    pmix_output_verbose(20, pmix_bfrops_base_framework.framework_output,
                        "pmix20_bfrop_pack_int16 * %d\n", num_vals);
    /* check to see if buffer needs extending */
    if (NULL == (dst = pmix_bfrop_buffer_extend(buffer, num_vals*sizeof(tmp)))) {
        return PMIX_ERR_OUT_OF_RESOURCE;
    }

    for (i = 0; i < num_vals; ++i) {
        tmp = pmix_htons(srctmp[i]);
        memcpy(dst, &tmp, sizeof(tmp));
        dst += sizeof(tmp);
    }
    buffer->pack_ptr += num_vals * sizeof(tmp);
    buffer->bytes_used += num_vals * sizeof(tmp);

    return PMIX_SUCCESS;
}

/*
 * INT32
 */
pmix_status_t pmix20_bfrop_pack_int32(pmix_buffer_t *buffer, const void *src,
                                    int32_t num_vals, pmix_data_type_t type)
 {
    int32_t i;
    uint32_t tmp, *srctmp = (uint32_t*) src;
    char *dst;

    pmix_output_verbose(20, pmix_bfrops_base_framework.framework_output,
                        "pmix20_bfrop_pack_int32 * %d\n", num_vals);
    /* check to see if buffer needs extending */
    if (NULL == (dst = pmix_bfrop_buffer_extend(buffer, num_vals*sizeof(tmp)))) {
        return PMIX_ERR_OUT_OF_RESOURCE;
    }

    for (i = 0; i < num_vals; ++i) {
        tmp = htonl(srctmp[i]);
        memcpy(dst, &tmp, sizeof(tmp));
        dst += sizeof(tmp);
    }
    buffer->pack_ptr += num_vals * sizeof(tmp);
    buffer->bytes_used += num_vals * sizeof(tmp);

    return PMIX_SUCCESS;
}

pmix_status_t pmix20_bfrop_pack_datatype(pmix_buffer_t *buffer, const void *src,
                                       int32_t num_vals, pmix_data_type_t type)
{
    return pmix20_bfrop_pack_int16(buffer, src, num_vals, type);
}

/*
 * INT64
 */
pmix_status_t pmix20_bfrop_pack_int64(pmix_buffer_t *buffer, const void *src,
                                    int32_t num_vals, pmix_data_type_t type)
 {
    int32_t i;
    uint64_t tmp, tmp2;
    char *dst;
    size_t bytes_packed = num_vals * sizeof(tmp);

    pmix_output_verbose(20, pmix_bfrops_base_framework.framework_output,
                        "pmix20_bfrop_pack_int64 * %d\n", num_vals);
    /* check to see if buffer needs extending */
    if (NULL == (dst = pmix_bfrop_buffer_extend(buffer, bytes_packed))) {
        return PMIX_ERR_OUT_OF_RESOURCE;
    }

    for (i = 0; i < num_vals; ++i) {
        memcpy(&tmp2, (char *)src+i*sizeof(uint64_t), sizeof(uint64_t));
        tmp = pmix_hton64(tmp2);
        memcpy(dst, &tmp, sizeof(tmp));
        dst += sizeof(tmp);
    }
    buffer->pack_ptr += bytes_packed;
    buffer->bytes_used += bytes_packed;

    return PMIX_SUCCESS;
}

/*
 * STRING
 */
pmix_status_t pmix20_bfrop_pack_string(pmix_buffer_t *buffer, const void *src,
                                     int32_t num_vals, pmix_data_type_t type)
 {
    pmix_status_t ret = PMIX_SUCCESS;
    int32_t i, len;
    char **ssrc = (char**) src;

    for (i = 0; i < num_vals; ++i) {
        if (NULL == ssrc[i]) {  /* got zero-length string/NULL pointer - store NULL */
        len = 0;
        if (PMIX_SUCCESS != (ret = pmix20_bfrop_pack_int32(buffer, &len, 1, PMIX_INT32))) {
            return ret;
        }
    } else {
        len = (int32_t)strlen(ssrc[i]) + 1;
        if (PMIX_SUCCESS != (ret = pmix20_bfrop_pack_int32(buffer, &len, 1, PMIX_INT32))) {
            return ret;
        }
        if (PMIX_SUCCESS != (ret =
            pmix20_bfrop_pack_byte(buffer, ssrc[i], len, PMIX_BYTE))) {
            return ret;
    }
}
}

return PMIX_SUCCESS;
}

/* FLOAT */
pmix_status_t pmix20_bfrop_pack_float(pmix_buffer_t *buffer, const void *src,
                                    int32_t num_vals, pmix_data_type_t type)
{
    pmix_status_t ret = PMIX_SUCCESS;
    int32_t i;
    float *ssrc = (float*)src;
    char *convert;

    for (i = 0; i < num_vals; ++i) {
        if (0 > asprintf(&convert, "%f", ssrc[i])) {
            return PMIX_ERR_NOMEM;
        }
        if (PMIX_SUCCESS != (ret = pmix20_bfrop_pack_string(buffer, &convert, 1, PMIX_STRING))) {
            free(convert);
            return ret;
        }
        free(convert);
    }

    return PMIX_SUCCESS;
}

/* DOUBLE */
pmix_status_t pmix20_bfrop_pack_double(pmix_buffer_t *buffer, const void *src,
                                     int32_t num_vals, pmix_data_type_t type)
{
    pmix_status_t ret = PMIX_SUCCESS;
    int32_t i;
    double *ssrc = (double*)src;
    char *convert;

    for (i = 0; i < num_vals; ++i) {
        if (0 > asprintf(&convert, "%f", ssrc[i])) {
            return PMIX_ERR_NOMEM;
        }
        if (PMIX_SUCCESS != (ret = pmix20_bfrop_pack_string(buffer, &convert, 1, PMIX_STRING))) {
            free(convert);
            return ret;
        }
        free(convert);
    }

    return PMIX_SUCCESS;
}

/* TIMEVAL */
pmix_status_t pmix20_bfrop_pack_timeval(pmix_buffer_t *buffer, const void *src,
                                      int32_t num_vals, pmix_data_type_t type)
{
    int64_t tmp[2];
    pmix_status_t ret = PMIX_SUCCESS;
    int32_t i;
    struct timeval *ssrc = (struct timeval *)src;

    for (i = 0; i < num_vals; ++i) {
        tmp[0] = (int64_t)ssrc[i].tv_sec;
        tmp[1] = (int64_t)ssrc[i].tv_usec;
        if (PMIX_SUCCESS != (ret = pmix20_bfrop_pack_int64(buffer, tmp, 2, PMIX_INT64))) {
            return ret;
        }
    }

    return PMIX_SUCCESS;
}

/* TIME */
pmix_status_t pmix20_bfrop_pack_time(pmix_buffer_t *buffer, const void *src,
                                   int32_t num_vals, pmix_data_type_t type)
{
    pmix_status_t ret = PMIX_SUCCESS;
    int32_t i;
    time_t *ssrc = (time_t *)src;
    uint64_t ui64;

    /* time_t is a system-dependent size, so cast it
     * to uint64_t as a generic safe size
     */
     for (i = 0; i < num_vals; ++i) {
        ui64 = (uint64_t)ssrc[i];
        if (PMIX_SUCCESS != (ret = pmix20_bfrop_pack_int64(buffer, &ui64, 1, PMIX_UINT64))) {
            return ret;
        }
    }

    return PMIX_SUCCESS;
}

/* STATUS */
pmix_status_t pmix20_bfrop_pack_status(pmix_buffer_t *buffer, const void *src,
                                     int32_t num_vals, pmix_data_type_t type)
{
    pmix_status_t ret = PMIX_SUCCESS;
    int32_t i;
    pmix_status_t *ssrc = (pmix_status_t *)src;
    int32_t status;

    for (i = 0; i < num_vals; ++i) {
        status = (int32_t)ssrc[i];
        if (PMIX_SUCCESS != (ret = pmix20_bfrop_pack_int32(buffer, &status, 1, PMIX_INT32))) {
            return ret;
        }
    }

    return PMIX_SUCCESS;
}


/* PACK FUNCTIONS FOR GENERIC PMIX TYPES */
static pmix_status_t pack_val(pmix_buffer_t *buffer,
                              pmix_value_t *p)
{
    pmix_status_t ret;

    switch (p->type) {
        case PMIX_UNDEF:
            break;
        case PMIX_BOOL:
            if (PMIX_SUCCESS != (ret = pmix20_bfrop_pack_buffer(buffer, &p->data.flag, 1, PMIX_BOOL))) {
                return ret;
            }
            break;
        case PMIX_BYTE:
            if (PMIX_SUCCESS != (ret = pmix20_bfrop_pack_buffer(buffer, &p->data.byte, 1, PMIX_BYTE))) {
                return ret;
            }
            break;
        case PMIX_STRING:
            if (PMIX_SUCCESS != (ret = pmix20_bfrop_pack_buffer(buffer, &p->data.string, 1, PMIX_STRING))) {
                return ret;
            }
            break;
        case PMIX_SIZE:
            if (PMIX_SUCCESS != (ret = pmix20_bfrop_pack_buffer(buffer, &p->data.size, 1, PMIX_SIZE))) {
                return ret;
            }
            break;
        case PMIX_PID:
            if (PMIX_SUCCESS != (ret = pmix20_bfrop_pack_buffer(buffer, &p->data.pid, 1, PMIX_PID))) {
                return ret;
            }
            break;
        case PMIX_INT:
            if (PMIX_SUCCESS != (ret = pmix20_bfrop_pack_buffer(buffer, &p->data.integer, 1, PMIX_INT))) {
                return ret;
            }
            break;
        case PMIX_INT8:
            if (PMIX_SUCCESS != (ret = pmix20_bfrop_pack_buffer(buffer, &p->data.int8, 1, PMIX_INT8))) {
                return ret;
            }
            break;
        case PMIX_INT16:
            if (PMIX_SUCCESS != (ret = pmix20_bfrop_pack_buffer(buffer, &p->data.int16, 1, PMIX_INT16))) {
                return ret;
            }
            break;
        case PMIX_INT32:
            if (PMIX_SUCCESS != (ret = pmix20_bfrop_pack_buffer(buffer, &p->data.int32, 1, PMIX_INT32))) {
                return ret;
            }
            break;
        case PMIX_INT64:
            if (PMIX_SUCCESS != (ret = pmix20_bfrop_pack_buffer(buffer, &p->data.int64, 1, PMIX_INT64))) {
                return ret;
            }
            break;
        case PMIX_UINT:
            if (PMIX_SUCCESS != (ret = pmix20_bfrop_pack_buffer(buffer, &p->data.uint, 1, PMIX_UINT))) {
                return ret;
            }
            break;
        case PMIX_UINT8:
            if (PMIX_SUCCESS != (ret = pmix20_bfrop_pack_buffer(buffer, &p->data.uint8, 1, PMIX_UINT8))) {
                return ret;
            }
            break;
        case PMIX_UINT16:
            if (PMIX_SUCCESS != (ret = pmix20_bfrop_pack_buffer(buffer, &p->data.uint16, 1, PMIX_UINT16))) {
                return ret;
            }
            break;
        case PMIX_UINT32:
            if (PMIX_SUCCESS != (ret = pmix20_bfrop_pack_buffer(buffer, &p->data.uint32, 1, PMIX_UINT32))) {
                return ret;
            }
            break;
        case PMIX_UINT64:
            if (PMIX_SUCCESS != (ret = pmix20_bfrop_pack_buffer(buffer, &p->data.uint64, 1, PMIX_UINT64))) {
                return ret;
            }
            break;
        case PMIX_FLOAT:
            if (PMIX_SUCCESS != (ret = pmix20_bfrop_pack_buffer(buffer, &p->data.fval, 1, PMIX_FLOAT))) {
                return ret;
            }
            break;
        case PMIX_DOUBLE:
            if (PMIX_SUCCESS != (ret = pmix20_bfrop_pack_buffer(buffer, &p->data.dval, 1, PMIX_DOUBLE))) {
                return ret;
            }
            break;
        case PMIX_TIMEVAL:
            if (PMIX_SUCCESS != (ret = pmix20_bfrop_pack_buffer(buffer, &p->data.tv, 1, PMIX_TIMEVAL))) {
                return ret;
            }
            break;
        case PMIX_TIME:
            if (PMIX_SUCCESS != (ret = pmix20_bfrop_pack_buffer(buffer, &p->data.time, 1, PMIX_TIME))) {
                return ret;
            }
            break;
        case PMIX_STATUS:
            if (PMIX_SUCCESS != (ret = pmix20_bfrop_pack_buffer(buffer, &p->data.status, 1, PMIX_STATUS))) {
                return ret;
            }
            break;
        case PMIX_PROC:
            if (PMIX_SUCCESS != (ret = pmix20_bfrop_pack_buffer(buffer, p->data.proc, 1, PMIX_PROC))) {
                return ret;
            }
            break;
        case PMIX_PROC_RANK:
            if (PMIX_SUCCESS != (ret = pmix20_bfrop_pack_buffer(buffer, &p->data.rank, 1, PMIX_PROC_RANK))) {
                return ret;
            }
            break;
        case PMIX_BYTE_OBJECT:
        case PMIX_COMPRESSED_STRING:
            if (PMIX_SUCCESS != (ret = pmix20_bfrop_pack_buffer(buffer, &p->data.bo, 1, PMIX_BYTE_OBJECT))) {
                return ret;
            }
            break;
        case PMIX_PERSIST:
            if (PMIX_SUCCESS != (ret = pmix20_bfrop_pack_buffer(buffer, &p->data.persist, 1, PMIX_PERSIST))) {
                return ret;
            }
            break;
        case PMIX_POINTER:
            if (PMIX_SUCCESS != (ret = pmix20_bfrop_pack_buffer(buffer, &p->data.ptr, 1, PMIX_POINTER))) {
                return ret;
            }
            break;
        case PMIX_SCOPE:
            if (PMIX_SUCCESS != (ret = pmix20_bfrop_pack_buffer(buffer, &p->data.scope, 1, PMIX_SCOPE))) {
                return ret;
            }
            break;
        case PMIX_DATA_RANGE:
            if (PMIX_SUCCESS != (ret = pmix20_bfrop_pack_buffer(buffer, &p->data.range, 1, PMIX_DATA_RANGE))) {
                return ret;
            }
            break;
        case PMIX_PROC_STATE:
            if (PMIX_SUCCESS != (ret = pmix20_bfrop_pack_buffer(buffer, &p->data.state, 1, PMIX_PROC_STATE))) {
                return ret;
            }
            break;
        case PMIX_PROC_INFO:
            if (PMIX_SUCCESS != (ret = pmix20_bfrop_pack_buffer(buffer, p->data.pinfo, 1, PMIX_PROC_INFO))) {
                return ret;
            }
            break;
        case PMIX_DATA_ARRAY:
            if (PMIX_SUCCESS != (ret = pmix20_bfrop_pack_buffer(buffer, p->data.darray, 1, PMIX_DATA_ARRAY))) {
                return ret;
            }
            break;
        case PMIX_QUERY:
            if (PMIX_SUCCESS != (ret = pmix20_bfrop_pack_buffer(buffer, p->data.darray, 1, PMIX_QUERY))) {
                return ret;
            }
            break;
        default:
        pmix_output(0, "PACK-PMIX-VALUE: UNSUPPORTED TYPE %d", (int)p->type);
        return PMIX_ERROR;
    }
    return PMIX_SUCCESS;
}

/*
 * PMIX_VALUE
 */
 pmix_status_t pmix20_bfrop_pack_value(pmix_buffer_t *buffer, const void *src,
                                     int32_t num_vals, pmix_data_type_t type)
 {
    pmix_value_t *ptr;
    int32_t i;
    pmix_status_t ret;

    ptr = (pmix_value_t *) src;

    for (i = 0; i < num_vals; ++i) {
        /* pack the type */
        if (PMIX_SUCCESS != (ret = pmix20_bfrop_store_data_type(buffer, ptr[i].type))) {
            return ret;
        }
        /* now pack the right field */
        if (PMIX_SUCCESS != (ret = pack_val(buffer, &ptr[i]))) {
            return ret;
        }
    }

    return PMIX_SUCCESS;
}


pmix_status_t pmix20_bfrop_pack_info(pmix_buffer_t *buffer, const void *src,
                                   int32_t num_vals, pmix_data_type_t type)
{
    pmix_info_t *info;
    int32_t i;
    pmix_status_t ret;
    char *foo;

    info = (pmix_info_t *) src;

    for (i = 0; i < num_vals; ++i) {
        /* pack key */
        foo = info[i].key;
        if (PMIX_SUCCESS != (ret = pmix20_bfrop_pack_string(buffer, &foo, 1, PMIX_STRING))) {
            return ret;
        }
        /* pack info directives flag */
        if (PMIX_SUCCESS != (ret = pmix20_bfrop_pack_infodirs(buffer, &info[i].flags, 1, PMIX_INFO_DIRECTIVES))) {
            return ret;
        }
        /* pack the type */
        if (PMIX_SUCCESS != (ret = pmix20_bfrop_pack_int(buffer, &info[i].value.type, 1, PMIX_INT))) {
            return ret;
        }
        /* pack value */
        if (PMIX_SUCCESS != (ret = pack_val(buffer, &info[i].value))) {
            return ret;
        }
    }
    return PMIX_SUCCESS;
}

pmix_status_t pmix20_bfrop_pack_pdata(pmix_buffer_t *buffer, const void *src,
                                    int32_t num_vals, pmix_data_type_t type)
{
    pmix_pdata_t *pdata;
    int32_t i;
    pmix_status_t ret;
    char *foo;

    pdata = (pmix_pdata_t *) src;

    for (i = 0; i < num_vals; ++i) {
        /* pack the proc */
        if (PMIX_SUCCESS != (ret = pmix20_bfrop_pack_proc(buffer, &pdata[i].proc, 1, PMIX_PROC))) {
            return ret;
        }
        /* pack key */
        foo = pdata[i].key;
        if (PMIX_SUCCESS != (ret = pmix20_bfrop_pack_string(buffer, &foo, 1, PMIX_STRING))) {
            return ret;
        }
        /* pack the type */
        if (PMIX_SUCCESS != (ret = pmix20_bfrop_pack_int(buffer, &pdata[i].value.type, 1, PMIX_INT))) {
            return ret;
        }
        /* pack value */
        if (PMIX_SUCCESS != (ret = pack_val(buffer, &pdata[i].value))) {
            return ret;
        }
    }
    return PMIX_SUCCESS;
}

pmix_status_t pmix20_bfrop_pack_buf(pmix_buffer_t *buffer, const void *src,
                                  int32_t num_vals, pmix_data_type_t type)
{
    pmix_buffer_t *ptr;
    int32_t i;
    pmix_status_t ret;

    ptr = (pmix_buffer_t *) src;

    for (i = 0; i < num_vals; ++i) {
        /* pack the number of bytes */
        if (PMIX_SUCCESS != (ret = pmix20_bfrop_pack_sizet(buffer, &ptr[i].bytes_used, 1, PMIX_SIZE))) {
            return ret;
        }
        /* pack the bytes */
        if (0 < ptr[i].bytes_used) {
            if (PMIX_SUCCESS != (ret = pmix20_bfrop_pack_byte(buffer, ptr[i].base_ptr, ptr[i].bytes_used, PMIX_BYTE))) {
                return ret;
            }
        }
    }
    return PMIX_SUCCESS;
}

pmix_status_t pmix20_bfrop_pack_proc(pmix_buffer_t *buffer, const void *src,
                                   int32_t num_vals, pmix_data_type_t type)
{
    pmix_proc_t *proc;
    int32_t i;
    pmix_status_t ret;

    proc = (pmix_proc_t *) src;

    for (i = 0; i < num_vals; ++i) {
        char *ptr = proc[i].nspace;
        if (PMIX_SUCCESS != (ret = pmix20_bfrop_pack_string(buffer, &ptr, 1, PMIX_STRING))) {
            return ret;
        }
        if (PMIX_SUCCESS != (ret = pmix20_bfrop_pack_rank(buffer, &proc[i].rank, 1, PMIX_PROC_RANK))) {
            return ret;
        }
    }
    return PMIX_SUCCESS;
}

pmix_status_t pmix20_bfrop_pack_app(pmix_buffer_t *buffer, const void *src,
                                  int32_t num_vals, pmix_data_type_t type)
{
    pmix_app_t *app;
    int32_t i, j, nvals;
    pmix_status_t ret;

    app = (pmix_app_t *) src;

    for (i = 0; i < num_vals; ++i) {
        if (PMIX_SUCCESS != (ret = pmix20_bfrop_pack_string(buffer, &app[i].cmd, 1, PMIX_STRING))) {
            return ret;
        }
        /* argv */
        nvals = pmix_argv_count(app[i].argv);
        if (PMIX_SUCCESS != (ret = pmix20_bfrop_pack_int(buffer, &nvals, 1, PMIX_INT32))) {
            return ret;
        }
        for (j=0; j < nvals; j++) {
            if (PMIX_SUCCESS != (ret = pmix20_bfrop_pack_string(buffer, &app[i].argv[j], 1, PMIX_STRING))) {
                return ret;
            }
        }
        /* env */
        nvals = pmix_argv_count(app[i].env);
        if (PMIX_SUCCESS != (ret = pmix20_bfrop_pack_int32(buffer, &nvals, 1, PMIX_INT32))) {
            return ret;
        }
        for (j=0; j < nvals; j++) {
            if (PMIX_SUCCESS != (ret = pmix20_bfrop_pack_string(buffer, &app[i].env[j], 1, PMIX_STRING))) {
                return ret;
            }
        }
        /* cwd */
        if (PMIX_SUCCESS != (ret = pmix20_bfrop_pack_string(buffer, &app[i].cwd, 1, PMIX_STRING))) {
            return ret;
        }
        /* maxprocs */
        if (PMIX_SUCCESS != (ret = pmix20_bfrop_pack_int(buffer, &app[i].maxprocs, 1, PMIX_INT))) {
            return ret;
        }
        /* info array */
        if (PMIX_SUCCESS != (ret = pmix20_bfrop_pack_sizet(buffer, &app[i].ninfo, 1, PMIX_SIZE))) {
            return ret;
        }
        if (0 < app[i].ninfo) {
            if (PMIX_SUCCESS != (ret = pmix20_bfrop_pack_info(buffer, app[i].info, app[i].ninfo, PMIX_INFO))) {
                return ret;
            }
        }
    }
    return PMIX_SUCCESS;
}


pmix_status_t pmix20_bfrop_pack_kval(pmix_buffer_t *buffer, const void *src,
                                   int32_t num_vals, pmix_data_type_t type)
{
    pmix_kval_t *ptr;
    int32_t i;
    pmix_status_t ret;
    char *st;

    ptr = (pmix_kval_t *) src;

    for (i = 0; i < num_vals; ++i) {
        /* pack the key */
        st = ptr[i].key;
        if (PMIX_SUCCESS != (ret = pmix20_bfrop_pack_string(buffer, &st, 1, PMIX_STRING))) {
            return ret;
        }
        /* pack the value */
        if (PMIX_SUCCESS != (ret = pmix20_bfrop_pack_value(buffer, ptr[i].value, 1, PMIX_VALUE))) {
            return ret;
        }
    }

    return PMIX_SUCCESS;
}

pmix_status_t pmix20_bfrop_pack_modex(pmix_buffer_t *buffer, const void *src,
                                    int32_t num_vals, pmix_data_type_t type)
{
    pmix_modex_data_t *ptr;
    int32_t i;
    pmix_status_t ret;

    ptr = (pmix_modex_data_t *) src;

    for (i = 0; i < num_vals; ++i) {
        if (PMIX_SUCCESS != (ret = pmix20_bfrop_pack_sizet(buffer, &ptr[i].size, 1, PMIX_SIZE))) {
            return ret;
        }
        if( 0 < ptr[i].size){
            if (PMIX_SUCCESS != (ret = pmix20_bfrop_pack_byte(buffer, ptr[i].blob, ptr[i].size, PMIX_UINT8))) {
                return ret;
            }
        }
    }
    return PMIX_SUCCESS;
}

pmix_status_t pmix20_bfrop_pack_persist(pmix_buffer_t *buffer, const void *src,
                                      int32_t num_vals, pmix_data_type_t type)
{
    return pmix20_bfrop_pack_byte(buffer, src, num_vals, PMIX_UINT8);
}

pmix_status_t pmix20_bfrop_pack_scope(pmix_buffer_t *buffer, const void *src,
                                    int32_t num_vals, pmix_data_type_t type)
{
    return pmix20_bfrop_pack_byte(buffer, src, num_vals, PMIX_UINT8);
}

pmix_status_t pmix20_bfrop_pack_range(pmix_buffer_t *buffer, const void *src,
                                    int32_t num_vals, pmix_data_type_t type)
{
    return pmix20_bfrop_pack_byte(buffer, src, num_vals, PMIX_UINT8);
}

pmix_status_t pmix20_bfrop_pack_cmd(pmix_buffer_t *buffer, const void *src,
                                  int32_t num_vals, pmix_data_type_t type)
{
    return pmix20_bfrop_pack_byte(buffer, src, num_vals, PMIX_UINT8);
}

pmix_status_t pmix20_bfrop_pack_infodirs(pmix_buffer_t *buffer, const void *src,
                                       int32_t num_vals, pmix_data_type_t type)
{
    return pmix20_bfrop_pack_int32(buffer, src, num_vals, PMIX_UINT32);
}

pmix_status_t pmix20_bfrop_pack_bo(pmix_buffer_t *buffer, const void *src,
                                 int32_t num_vals, pmix_data_type_t type)
{
    pmix_status_t ret;
    int i;
    pmix_byte_object_t *bo;

    bo = (pmix_byte_object_t*)src;
    for (i=0; i < num_vals; i++) {
        if (PMIX_SUCCESS != (ret = pmix20_bfrop_pack_sizet(buffer, &bo[i].size, 1, PMIX_SIZE))) {
            return ret;
        }
        if (0 < bo[i].size) {
            if (PMIX_SUCCESS != (ret = pmix20_bfrop_pack_byte(buffer, bo[i].bytes, bo[i].size, PMIX_BYTE))) {
                return ret;
            }
        }
    }
    return PMIX_SUCCESS;
}

pmix_status_t pmix20_bfrop_pack_ptr(pmix_buffer_t *buffer, const void *src,
                        int32_t num_vals, pmix_data_type_t type)
{
    uint8_t foo=1;
    /* it obviously makes no sense to pack a pointer and
     * send it somewhere else, so we just pack a sentinel */
    return pmix20_bfrop_pack_byte(buffer, &foo, 1, PMIX_UINT8);
}

pmix_status_t pmix20_bfrop_pack_pstate(pmix_buffer_t *buffer, const void *src,
                                     int32_t num_vals, pmix_data_type_t type)
{
    return pmix20_bfrop_pack_byte(buffer, src, num_vals, PMIX_UINT8);
}

pmix_status_t pmix20_bfrop_pack_pinfo(pmix_buffer_t *buffer, const void *src,
                                    int32_t num_vals, pmix_data_type_t type)
{
    pmix_proc_info_t *pinfo = (pmix_proc_info_t*)src;
    pmix_status_t ret;
    int32_t i;

    for (i=0; i < num_vals; i++) {
        /* pack the proc identifier */
        if (PMIX_SUCCESS != (ret = pmix20_bfrop_pack_proc(buffer, &pinfo[i].proc, 1, PMIX_PROC))) {
            return ret;
        }
        /* pack the hostname and exec */
        if (PMIX_SUCCESS != (ret = pmix20_bfrop_pack_string(buffer, &pinfo[i].hostname, 1, PMIX_STRING))) {
            return ret;
        }
        if (PMIX_SUCCESS != (ret = pmix20_bfrop_pack_string(buffer, &pinfo[i].executable_name, 1, PMIX_STRING))) {
            return ret;
        }
        /* pack the pid and state */
        if (PMIX_SUCCESS != (ret = pmix20_bfrop_pack_pid(buffer, &pinfo[i].pid, 1, PMIX_PID))) {
            return ret;
        }
        if (PMIX_SUCCESS != (ret = pmix20_bfrop_pack_pstate(buffer, &pinfo[i].state, 1, PMIX_PROC_STATE))) {
            return ret;
        }
    }
    return PMIX_SUCCESS;
}

pmix_status_t pmix20_bfrop_pack_darray(pmix_buffer_t *buffer, const void *src,
                                    int32_t num_vals, pmix_data_type_t type)
{
    pmix_data_array_t *p = (pmix_data_array_t*)src;
    pmix_status_t ret;
    int32_t i;

    for (i=0; i < num_vals; i++) {
        /* pack the actual type in the array */
        if (PMIX_SUCCESS != (ret = pmix20_bfrop_pack_datatype(buffer, &p[i].type, 1, PMIX_DATA_TYPE))) {
            return ret;
        }
        /* pack the number of array elements */
        if (PMIX_SUCCESS != (ret = pmix20_bfrop_pack_sizet(buffer, &p[i].size, 1, PMIX_SIZE))) {
            return ret;
        }
        if (0 == p[i].size || PMIX_UNDEF == p[i].type) {
            /* nothing left to do */
            continue;
        }
        /* pack the actual elements */
        if (PMIX_SUCCESS != (ret = pmix20_bfrop_pack_buffer(buffer, p[i].array, p[i].size, p[i].type))) {
            return ret;
        }
    }
    return PMIX_SUCCESS;
}

pmix_status_t pmix20_bfrop_pack_rank(pmix_buffer_t *buffer, const void *src,
                                     int32_t num_vals, pmix_data_type_t type)
{
    return pmix20_bfrop_pack_int32(buffer, src, num_vals, PMIX_UINT32);
}

pmix_status_t pmix20_bfrop_pack_query(pmix_buffer_t *buffer, const void *src,
                                    int32_t num_vals, pmix_data_type_t type)
{
    pmix_query_t *pq = (pmix_query_t*)src;
    pmix_status_t ret;
    int32_t i;
    int32_t nkeys;

    for (i=0; i < num_vals; i++) {
        /* pack the number of keys */
        nkeys = pmix_argv_count(pq[i].keys);
        if (PMIX_SUCCESS != (ret = pmix20_bfrop_pack_int32(buffer, &nkeys, 1, PMIX_INT32))) {
            return ret;
        }
        if (0 < nkeys) {
            /* pack the keys */
            if (PMIX_SUCCESS != (ret = pmix20_bfrop_pack_string(buffer, pq[i].keys, nkeys, PMIX_STRING))) {
                return ret;
            }
        }
        /* pack the number of qualifiers */
        if (PMIX_SUCCESS != (ret = pmix20_bfrop_pack_sizet(buffer, &pq[i].nqual, 1, PMIX_SIZE))) {
            return ret;
        }
        if (0 < pq[i].nqual) {
            /* pack any provided qualifiers */
            if (PMIX_SUCCESS != (ret = pmix20_bfrop_pack_info(buffer, pq[i].qualifiers, pq[i].nqual, PMIX_INFO))) {
                return ret;
            }
        }
    }
    return PMIX_SUCCESS;
}

pmix_status_t pmix20_bfrop_pack_alloc_directive(pmix_buffer_t *buffer, const void *src,
                                              int32_t num_vals, pmix_data_type_t type)
{
    return pmix20_bfrop_pack_byte(buffer, src, num_vals, PMIX_UINT8);
}

/**** DEPRECATED ****/
pmix_status_t pmix20_bfrop_pack_array(pmix_buffer_t *buffer, const void *src,
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
/********************/

