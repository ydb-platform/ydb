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
 * Copyright (c) 2012      Los Alamos National Security, Inc.  All rights reserved.
 * Copyright (c) 2014-2017 Intel, Inc. All rights reserved.
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

#include <src/include/pmix_stdint.h>

#include <stdio.h>
#ifdef HAVE_TIME_H
#include <time.h>
#endif

#include "src/mca/bfrops/base/base.h"
#include "bfrop_v12.h"
#include "internal.h"

pmix_status_t pmix12_bfrop_print(char **output, char *prefix, void *src, pmix_data_type_t type)
{
    pmix_bfrop_type_info_t *info;

    /* check for error */
    if (NULL == output) {
        return PMIX_ERR_BAD_PARAM;
    }

    /* Lookup the print function for this type and call it */

    if(NULL == (info = (pmix_bfrop_type_info_t*)pmix_pointer_array_get_item(&mca_bfrops_v12_component.types, type))) {
        return PMIX_ERR_UNKNOWN_DATA_TYPE;
    }

    return info->odti_print_fn(output, prefix, src, type);
}

/*
 * STANDARD PRINT FUNCTIONS FOR SYSTEM TYPES
 */
pmix_status_t pmix12_bfrop_print_bool(char **output, char *prefix, bool *src, pmix_data_type_t type)
{
    char *prefx;

    /* deal with NULL prefix */
    if (NULL == prefix) {
        if (0 > asprintf(&prefx, " ")) {
            return PMIX_ERR_NOMEM;
        }
    }
    else {
        prefx = prefix;
    }

    /* if src is NULL, just print data type and return */
    if (NULL == src) {
        if (0 > asprintf(output, "%sData type: PMIX_BOOL\tValue: NULL pointer", prefx)) {
            return PMIX_ERR_NOMEM;
        }
        if (prefx != prefix) {
            free(prefx);
        }
        return PMIX_SUCCESS;
    }

    if (0 > asprintf(output, "%sData type: PMIX_BOOL\tValue: %s", prefix,
             (*src) ? "TRUE" : "FALSE")) {
        return PMIX_ERR_NOMEM;
    }
    if (prefx != prefix) {
        free(prefx);
    }

    return PMIX_SUCCESS;
}

pmix_status_t pmix12_bfrop_print_byte(char **output, char *prefix, uint8_t *src, pmix_data_type_t type)
{
    char *prefx;

    /* deal with NULL prefix */
    if (NULL == prefix) {
        if (0 > asprintf(&prefx, " ")) {
            return PMIX_ERR_NOMEM;
        }
    } else {
        prefx = prefix;
    }

    /* if src is NULL, just print data type and return */
    if (NULL == src) {
        if (0 > asprintf(output, "%sData type: PMIX_BYTE\tValue: NULL pointer", prefx)) {
            return PMIX_ERR_NOMEM;
        }
        if (prefx != prefix) {
            free(prefx);
        }
        return PMIX_SUCCESS;
    }

    if (0 > asprintf(output, "%sData type: PMIX_BYTE\tValue: %x", prefix, *src)) {
        return PMIX_ERR_NOMEM;
    }
    if (prefx != prefix) {
        free(prefx);
    }

    return PMIX_SUCCESS;
}

pmix_status_t pmix12_bfrop_print_string(char **output, char *prefix, char *src, pmix_data_type_t type)
{
    char *prefx;

    /* deal with NULL prefix */
    if (NULL == prefix) {
        if (0 > asprintf(&prefx, " ")) {
            return PMIX_ERR_NOMEM;
        }
    } else {
        prefx = prefix;
    }

    /* if src is NULL, just print data type and return */
    if (NULL == src) {
        if (0 > asprintf(output, "%sData type: PMIX_STRING\tValue: NULL pointer", prefx)) {
            return PMIX_ERR_NOMEM;
        }
        if (prefx != prefix) {
            free(prefx);
        }
        return PMIX_SUCCESS;
    }

    if (0 > asprintf(output, "%sData type: PMIX_STRING\tValue: %s", prefx, src)) {
        return PMIX_ERR_NOMEM;
    }
    if (prefx != prefix) {
        free(prefx);
    }

    return PMIX_SUCCESS;
}

pmix_status_t pmix12_bfrop_print_size(char **output, char *prefix, size_t *src, pmix_data_type_t type)
{
    char *prefx;

    /* deal with NULL prefix */
    if (NULL == prefix) {
        if (0 > asprintf(&prefx, " ")) {
            return PMIX_ERR_NOMEM;
        }
    } else {
        prefx = prefix;
    }

    /* if src is NULL, just print data type and return */
    if (NULL == src) {
        if (0 > asprintf(output, "%sData type: PMIX_SIZE\tValue: NULL pointer", prefx)) {
            return PMIX_ERR_NOMEM;
        }
        if (prefx != prefix) {
            free(prefx);
        }
        return PMIX_SUCCESS;
    }

    if (0 > asprintf(output, "%sData type: PMIX_SIZE\tValue: %lu", prefx, (unsigned long) *src)) {
        return PMIX_ERR_NOMEM;
    }
    if (prefx != prefix) {
        free(prefx);
    }

    return PMIX_SUCCESS;
}

pmix_status_t pmix12_bfrop_print_pid(char **output, char *prefix, pid_t *src, pmix_data_type_t type)
{
    char *prefx;

    /* deal with NULL prefix */
    if (NULL == prefix) {
        if (0 > asprintf(&prefx, " ")) {
            return PMIX_ERR_NOMEM;
        }
    } else {
        prefx = prefix;
    }

    /* if src is NULL, just print data type and return */
    if (NULL == src) {
        if (0 > asprintf(output, "%sData type: PMIX_PID\tValue: NULL pointer", prefx)) {
            return PMIX_ERR_NOMEM;
        }
        if (prefx != prefix) {
            free(prefx);
        }
        return PMIX_SUCCESS;
    }

    if (0 > asprintf(output, "%sData type: PMIX_PID\tValue: %lu", prefx, (unsigned long) *src)) {
        return PMIX_ERR_NOMEM;
    }
    if (prefx != prefix) {
        free(prefx);
    }
    return PMIX_SUCCESS;
}

pmix_status_t pmix12_bfrop_print_int(char **output, char *prefix, int *src, pmix_data_type_t type)
{
    char *prefx;

    /* deal with NULL prefix */
    if (NULL == prefix) {
        if (0 > asprintf(&prefx, " ")) {
            return PMIX_ERR_NOMEM;
        }
    } else {
        prefx = prefix;
    }

    /* if src is NULL, just print data type and return */
    if (NULL == src) {
        if (0 > asprintf(output, "%sData type: PMIX_INT\tValue: NULL pointer", prefx)) {
            return PMIX_ERR_NOMEM;
        }
        if (prefx != prefix) {
            free(prefx);
        }
        return PMIX_SUCCESS;
    }

    if (0 > asprintf(output, "%sData type: PMIX_INT\tValue: %ld", prefx, (long) *src)) {
        return PMIX_ERR_NOMEM;
    }
    if (prefx != prefix) {
        free(prefx);
    }

    return PMIX_SUCCESS;
}

pmix_status_t pmix12_bfrop_print_uint(char **output, char *prefix, uint *src, pmix_data_type_t type)
{
    char *prefx;

    /* deal with NULL prefix */
    if (NULL == prefix) {
        if (0 > asprintf(&prefx, " ")) {
            return PMIX_ERR_NOMEM;
        }
    } else {
        prefx = prefix;
    }

    /* if src is NULL, just print data type and return */
    if (NULL == src) {
        if (0 > asprintf(output, "%sData type: PMIX_UINT\tValue: NULL pointer", prefx)) {
            return PMIX_ERR_NOMEM;
        }
        if (prefx != prefix) {
            free(prefx);
        }
        return PMIX_SUCCESS;
    }

    if (0 > asprintf(output, "%sData type: PMIX_UINT\tValue: %lu", prefx, (unsigned long) *src)) {
        return PMIX_ERR_NOMEM;
    }
    if (prefx != prefix) {
        free(prefx);
    }

    return PMIX_SUCCESS;
}

pmix_status_t pmix12_bfrop_print_uint8(char **output, char *prefix, uint8_t *src, pmix_data_type_t type)
{
    char *prefx;

    /* deal with NULL prefix */
    if (NULL == prefix) {
        if (0 > asprintf(&prefx, " ")) {
            return PMIX_ERR_NOMEM;
        }
    } else {
        prefx = prefix;
    }

    /* if src is NULL, just print data type and return */
    if (NULL == src) {
        if (0 > asprintf(output, "%sData type: PMIX_UINT8\tValue: NULL pointer", prefx)) {
            return PMIX_ERR_NOMEM;
        }
        if (prefx != prefix) {
            free(prefx);
        }
        return PMIX_SUCCESS;
    }

    if (0 > asprintf(output, "%sData type: PMIX_UINT8\tValue: %u", prefx, (unsigned int) *src)) {
        return PMIX_ERR_NOMEM;
    }
    if (prefx != prefix) {
        free(prefx);
    }

    return PMIX_SUCCESS;
}

pmix_status_t pmix12_bfrop_print_uint16(char **output, char *prefix, uint16_t *src, pmix_data_type_t type)
{
    char *prefx;

    /* deal with NULL prefix */
    if (NULL == prefix) {
        if (0 > asprintf(&prefx, " ")) {
            return PMIX_ERR_NOMEM;
        }
    } else {
        prefx = prefix;
    }

    /* if src is NULL, just print data type and return */
    if (NULL == src) {
        if (0 > asprintf(output, "%sData type: PMIX_UINT16\tValue: NULL pointer", prefx)) {
            return PMIX_ERR_NOMEM;
        }
        if (prefx != prefix) {
            free(prefx);
        }
        return PMIX_SUCCESS;
    }

    if (0 > asprintf(output, "%sData type: PMIX_UINT16\tValue: %u", prefx, (unsigned int) *src)) {
        return PMIX_ERR_NOMEM;
    }
    if (prefx != prefix) {
        free(prefx);
    }

    return PMIX_SUCCESS;
}

pmix_status_t pmix12_bfrop_print_uint32(char **output, char *prefix,
                            uint32_t *src, pmix_data_type_t type)
{
    char *prefx;

    /* deal with NULL prefix */
    if (NULL == prefix) {
        if (0 > asprintf(&prefx, " ")) {
            return PMIX_ERR_NOMEM;
        }
    } else {
        prefx = prefix;
    }

    /* if src is NULL, just print data type and return */
    if (NULL == src) {
        if (0 > asprintf(output, "%sData type: PMIX_UINT32\tValue: NULL pointer", prefx)) {
            return PMIX_ERR_NOMEM;
        }
        if (prefx != prefix) {
            free(prefx);
        }
        return PMIX_SUCCESS;
    }

    if (0 > asprintf(output, "%sData type: PMIX_UINT32\tValue: %u", prefx, (unsigned int) *src)) {
        return PMIX_ERR_NOMEM;
    }
    if (prefx != prefix) {
        free(prefx);
    }

    return PMIX_SUCCESS;
}

pmix_status_t pmix12_bfrop_print_int8(char **output, char *prefix,
                          int8_t *src, pmix_data_type_t type)
{
    char *prefx;

    /* deal with NULL prefix */
    if (NULL == prefix) {
        if (0 > asprintf(&prefx, " ")) {
            return PMIX_ERR_NOMEM;
        }
    } else {
        prefx = prefix;
    }

    /* if src is NULL, just print data type and return */
    if (NULL == src) {
        if (0 > asprintf(output, "%sData type: PMIX_INT8\tValue: NULL pointer", prefx)) {
            return PMIX_ERR_NOMEM;
        }
        if (prefx != prefix) {
            free(prefx);
        }
        return PMIX_SUCCESS;
    }

    if (0 > asprintf(output, "%sData type: PMIX_INT8\tValue: %d", prefx, (int) *src)) {
        return PMIX_ERR_NOMEM;
    }
    if (prefx != prefix) {
        free(prefx);
    }

    return PMIX_SUCCESS;
}

pmix_status_t pmix12_bfrop_print_int16(char **output, char *prefix,
                           int16_t *src, pmix_data_type_t type)
{
    char *prefx;

    /* deal with NULL prefix */
    if (NULL == prefix) {
        if (0 > asprintf(&prefx, " ")) {
            return PMIX_ERR_NOMEM;
        }
    } else {
        prefx = prefix;
    }

    /* if src is NULL, just print data type and return */
    if (NULL == src) {
        if (0 > asprintf(output, "%sData type: PMIX_INT16\tValue: NULL pointer", prefx)) {
            return PMIX_ERR_NOMEM;
        }
        if (prefx != prefix) {
            free(prefx);
        }
        return PMIX_SUCCESS;
    }

    if (0 > asprintf(output, "%sData type: PMIX_INT16\tValue: %d", prefx, (int) *src)) {
        return PMIX_ERR_NOMEM;
    }
    if (prefx != prefix) {
        free(prefx);
    }

    return PMIX_SUCCESS;
}

pmix_status_t pmix12_bfrop_print_int32(char **output, char *prefix, int32_t *src, pmix_data_type_t type)
{
    char *prefx;

    /* deal with NULL prefix */
    if (NULL == prefix) {
        if (0 > asprintf(&prefx, " ")) {
            return PMIX_ERR_NOMEM;
        }
    } else {
        prefx = prefix;
    }

    /* if src is NULL, just print data type and return */
    if (NULL == src) {
        if (0 > asprintf(output, "%sData type: PMIX_INT32\tValue: NULL pointer", prefx)) {
            return PMIX_ERR_NOMEM;
        }
        if (prefx != prefix) {
            free(prefx);
        }
        return PMIX_SUCCESS;
    }

    if (0 > asprintf(output, "%sData type: PMIX_INT32\tValue: %d", prefx, (int) *src)) {
        return PMIX_ERR_NOMEM;
    }
    if (prefx != prefix) {
        free(prefx);
    }

    return PMIX_SUCCESS;
}
pmix_status_t pmix12_bfrop_print_uint64(char **output, char *prefix,
                            uint64_t *src,
                            pmix_data_type_t type)
{
    char *prefx;

    /* deal with NULL prefix */
    if (NULL == prefix) {
        if (0 > asprintf(&prefx, " ")) {
            return PMIX_ERR_NOMEM;
        }
    } else {
        prefx = prefix;
    }

    /* if src is NULL, just print data type and return */
    if (NULL == src) {
        if (0 > asprintf(output, "%sData type: PMIX_UINT64\tValue: NULL pointer", prefx)) {
            return PMIX_ERR_NOMEM;
        }
        if (prefx != prefix) {
            free(prefx);
        }
        return PMIX_SUCCESS;
    }

    if (0 > asprintf(output, "%sData type: PMIX_UINT64\tValue: %lu", prefx, (unsigned long) *src)) {
        return PMIX_ERR_NOMEM;
    }
    if (prefx != prefix) {
        free(prefx);
    }

    return PMIX_SUCCESS;
}

pmix_status_t pmix12_bfrop_print_int64(char **output, char *prefix,
                           int64_t *src,
                           pmix_data_type_t type)
{
    char *prefx;

    /* deal with NULL prefix */
    if (NULL == prefix) {
        if (0 > asprintf(&prefx, " ")) {
            return PMIX_ERR_NOMEM;
        }
    } else {
        prefx = prefix;
    }

    /* if src is NULL, just print data type and return */
    if (NULL == src) {
        if (0 > asprintf(output, "%sData type: PMIX_INT64\tValue: NULL pointer", prefx)) {
            return PMIX_ERR_NOMEM;
        }
        if (prefx != prefix) {
            free(prefx);
        }
        return PMIX_SUCCESS;
    }

    if (0 > asprintf(output, "%sData type: PMIX_INT64\tValue: %ld", prefx, (long) *src)) {
        return PMIX_ERR_NOMEM;
    }
    if (prefx != prefix) {
        free(prefx);
    }

    return PMIX_SUCCESS;
}

pmix_status_t pmix12_bfrop_print_float(char **output, char *prefix,
                           float *src, pmix_data_type_t type)
{
    char *prefx;

    /* deal with NULL prefix */
    if (NULL == prefix) {
        if (0 > asprintf(&prefx, " ")) {
            return PMIX_ERR_NOMEM;
        }
    } else {
        prefx = prefix;
    }

    /* if src is NULL, just print data type and return */
    if (NULL == src) {
        if (0 > asprintf(output, "%sData type: PMIX_FLOAT\tValue: NULL pointer", prefx)) {
            return PMIX_ERR_NOMEM;
        }
        if (prefx != prefix) {
            free(prefx);
        }
        return PMIX_SUCCESS;
    }

    if (0 > asprintf(output, "%sData type: PMIX_FLOAT\tValue: %f", prefx, *src)) {
        return PMIX_ERR_NOMEM;
    }
    if (prefx != prefix) {
        free(prefx);
    }

    return PMIX_SUCCESS;
}

pmix_status_t pmix12_bfrop_print_double(char **output, char *prefix,
                            double *src, pmix_data_type_t type)
{
    char *prefx;

    /* deal with NULL prefix */
    if (NULL == prefix) {
        if (0 > asprintf(&prefx, " ")) {
            return PMIX_ERR_NOMEM;
        }
    } else {
        prefx = prefix;
    }

    /* if src is NULL, just print data type and return */
    if (NULL == src) {
        if (0 > asprintf(output, "%sData type: PMIX_DOUBLE\tValue: NULL pointer", prefx)) {
            return PMIX_ERR_NOMEM;
        }
        if (prefx != prefix) {
            free(prefx);
        }
        return PMIX_SUCCESS;
    }

    if (0 > asprintf(output, "%sData type: PMIX_DOUBLE\tValue: %f", prefx, *src)) {
        return PMIX_ERR_NOMEM;
    }
    if (prefx != prefix) {
        free(prefx);
    }

    return PMIX_SUCCESS;
}

pmix_status_t pmix12_bfrop_print_time(char **output, char *prefix,
                          time_t *src, pmix_data_type_t type)
{
    char *prefx;
    char *t;

    /* deal with NULL prefix */
    if (NULL == prefix) {
        if (0 > asprintf(&prefx, " ")) {
            return PMIX_ERR_NOMEM;
        }
    } else {
        prefx = prefix;
    }

    /* if src is NULL, just print data type and return */
    if (NULL == src) {
        if (0 > asprintf(output, "%sData type: PMIX_TIME\tValue: NULL pointer", prefx)) {
            return PMIX_ERR_NOMEM;
        }
        if (prefx != prefix) {
            free(prefx);
        }
        return PMIX_SUCCESS;
    }

    t = ctime(src);
    t[strlen(t)-1] = '\0';  // remove trailing newline

    if (0 > asprintf(output, "%sData type: PMIX_TIME\tValue: %s", prefx, t)) {
        return PMIX_ERR_NOMEM;
    }
    if (prefx != prefix) {
        free(prefx);
    }

    return PMIX_SUCCESS;
}

pmix_status_t pmix12_bfrop_print_timeval(char **output, char *prefix,
                             struct timeval *src, pmix_data_type_t type)
{
    char *prefx;

    /* deal with NULL prefix */
    if (NULL == prefix) {
        if (0 > asprintf(&prefx, " ")) {
            return PMIX_ERR_NOMEM;
        }
    } else {
        prefx = prefix;
    }

    /* if src is NULL, just print data type and return */
    if (NULL == src) {
        if (0 > asprintf(output, "%sData type: PMIX_TIMEVAL\tValue: NULL pointer", prefx)) {
            return PMIX_ERR_NOMEM;
        }
        if (prefx != prefix) {
            free(prefx);
        }
        return PMIX_SUCCESS;
    }

    if (0 > asprintf(output, "%sData type: PMIX_TIMEVAL\tValue: %ld.%06ld", prefx,
                    (long)src->tv_sec, (long)src->tv_usec)) {
        return PMIX_ERR_NOMEM;
    }
    if (prefx != prefix) {
        free(prefx);
    }

    return PMIX_SUCCESS;
}

/* PRINT FUNCTIONS FOR GENERIC PMIX TYPES */

/*
 * PMIX_VALUE
 */
pmix_status_t pmix12_bfrop_print_value(char **output, char *prefix,
                           pmix_value_t *src, pmix_data_type_t type)
{
    char *prefx;
    int rc;

    /* deal with NULL prefix */
    if (NULL == prefix) {
        if (0 > asprintf(&prefx, " ")) {
            return PMIX_ERR_NOMEM;
        }
    } else {
        prefx = prefix;
    }

    /* if src is NULL, just print data type and return */
    if (NULL == src) {
        if (0 > asprintf(output, "%sData type: PMIX_VALUE\tValue: NULL pointer", prefx)) {
            return PMIX_ERR_NOMEM;
        }
        if (prefx != prefix) {
            free(prefx);
        }
        return PMIX_SUCCESS;
    }

    switch (src->type) {
        case PMIX_BYTE:
        rc = asprintf(output, "%sPMIX_VALUE: Data type: PMIX_BYTE\tValue: %x",
                      prefx, src->data.byte);
        break;
        case PMIX_STRING:
        rc = asprintf(output, "%sPMIX_VALUE: Data type: PMIX_STRING\tValue: %s",
                      prefx, src->data.string);
        break;
        case PMIX_SIZE:
        rc = asprintf(output, "%sPMIX_VALUE: Data type: PMIX_SIZE\tValue: %lu",
                      prefx, (unsigned long)src->data.size);
        break;
        case PMIX_PID:
        rc = asprintf(output, "%sPMIX_VALUE: Data type: PMIX_PID\tValue: %lu",
                      prefx, (unsigned long)src->data.pid);
        break;
        case PMIX_INT:
        rc = asprintf(output, "%sPMIX_VALUE: Data type: PMIX_INT\tValue: %d",
                      prefx, src->data.integer);
        break;
        case PMIX_INT8:
        rc = asprintf(output, "%sPMIX_VALUE: Data type: PMIX_INT8\tValue: %d",
                      prefx, (int)src->data.int8);
        break;
        case PMIX_INT16:
        rc = asprintf(output, "%sPMIX_VALUE: Data type: PMIX_INT16\tValue: %d",
                      prefx, (int)src->data.int16);
        break;
        case PMIX_INT32:
        rc = asprintf(output, "%sPMIX_VALUE: Data type: PMIX_INT32\tValue: %d",
                      prefx, src->data.int32);
        break;
        case PMIX_INT64:
        rc = asprintf(output, "%sPMIX_VALUE: Data type: PMIX_INT64\tValue: %ld",
                      prefx, (long)src->data.int64);
        break;
        case PMIX_UINT:
        rc = asprintf(output, "%sPMIX_VALUE: Data type: PMIX_UINT\tValue: %u",
                      prefx, src->data.uint);
        break;
        case PMIX_UINT8:
        rc = asprintf(output, "%sPMIX_VALUE: Data type: PMIX_UINT8\tValue: %u",
                      prefx, (unsigned int)src->data.uint8);
        break;
        case PMIX_UINT16:
        rc = asprintf(output, "%sPMIX_VALUE: Data type: PMIX_UINT16\tValue: %u",
                      prefx, (unsigned int)src->data.uint16);
        break;
        case PMIX_UINT32:
        rc = asprintf(output, "%sPMIX_VALUE: Data type: PMIX_UINT32\tValue: %u",
                      prefx, src->data.uint32);
        break;
        case PMIX_UINT64:
        rc = asprintf(output, "%sPMIX_VALUE: Data type: PMIX_UINT64\tValue: %lu",
                      prefx, (unsigned long)src->data.uint64);
        break;
        case PMIX_FLOAT:
        rc = asprintf(output, "%sPMIX_VALUE: Data type: PMIX_FLOAT\tValue: %f",
                      prefx, src->data.fval);
        break;
        case PMIX_DOUBLE:
        rc = asprintf(output, "%sPMIX_VALUE: Data type: PMIX_DOUBLE\tValue: %f",
                      prefx, src->data.dval);
        break;
        case PMIX_TIMEVAL:
        rc = asprintf(output, "%sPMIX_VALUE: Data type: PMIX_TIMEVAL\tValue: %ld.%06ld", prefx,
                     (long)src->data.tv.tv_sec, (long)src->data.tv.tv_usec);
        break;

        default:
        rc = asprintf(output, "%sPMIX_VALUE: Data type: UNKNOWN\tValue: UNPRINTABLE", prefx);
        break;
    }
    if (prefx != prefix) {
        free(prefx);
    }
    if (0 > rc) {
        return PMIX_ERR_NOMEM;
    }
    return PMIX_SUCCESS;
}

pmix_status_t pmix12_bfrop_print_info(char **output, char *prefix,
                          pmix_info_t *src, pmix_data_type_t type)
{
    char *tmp;
    int rc;

    pmix12_bfrop_print_value(&tmp, NULL, &src->value, PMIX_VALUE);
    rc = asprintf(output, "%sKEY: %s %s", prefix, src->key,
                  (NULL == tmp) ? "PMIX_VALUE: NULL" : tmp);
    if (NULL != tmp) {
        free(tmp);
    }
    if (0 > rc) {
        return PMIX_ERR_NOMEM;
    }
    return PMIX_SUCCESS;
}

pmix_status_t pmix12_bfrop_print_pdata(char **output, char *prefix,
                          pmix_pdata_t *src, pmix_data_type_t type)
{
    char *tmp1, *tmp2;
    int rc;

    pmix12_bfrop_print_proc(&tmp1, NULL, &src->proc, PMIX_PROC);
    pmix12_bfrop_print_value(&tmp2, NULL, &src->value, PMIX_VALUE);
    rc = asprintf(output, "%s  %s  KEY: %s %s", prefix, tmp1, src->key,
                 (NULL == tmp2) ? "NULL" : tmp2);
    if (NULL != tmp1) {
        free(tmp1);
    }
    if (NULL != tmp2) {
        free(tmp2);
    }
    if (0 > rc) {
        return PMIX_ERR_NOMEM;
    }
    return PMIX_SUCCESS;
}

pmix_status_t pmix12_bfrop_print_buf(char **output, char *prefix,
                         pmix_buffer_t *src, pmix_data_type_t type)
{
    return PMIX_SUCCESS;
}

pmix_status_t pmix12_bfrop_print_app(char **output, char *prefix,
                         pmix_app_t *src, pmix_data_type_t type)
{
    return PMIX_SUCCESS;
}

pmix_status_t pmix12_bfrop_print_proc(char **output, char *prefix,
                           pmix_proc_t *src, pmix_data_type_t type)
{
    char *prefx;

    /* deal with NULL prefix */
    if (NULL == prefix) {
        if (0 > asprintf(&prefx, " ")) {
            return PMIX_ERR_NOMEM;
        }
    } else {
        prefx = prefix;
    }

    if (0 > asprintf(output, "%sPROC: %s:%d", prefx, src->nspace, src->rank)) {
        return PMIX_ERR_NOMEM;
    }
    return PMIX_SUCCESS;
}

pmix_status_t pmix12_bfrop_print_kval(char **output, char *prefix,
                          pmix_kval_t *src, pmix_data_type_t type)
{
    return PMIX_SUCCESS;
}

pmix_status_t pmix12_bfrop_print_array(char **output, char *prefix,
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
        pmix12_bfrop_print_info(&tmp2, pfx, &s1[j], PMIX_INFO);
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

pmix_status_t pmix12_bfrop_print_modex(char **output, char *prefix,
                           pmix_modex_data_t *src, pmix_data_type_t type)
{
    return PMIX_SUCCESS;
}

pmix_status_t pmix12_bfrop_print_persist(char **output, char *prefix, pmix_persistence_t *src, pmix_data_type_t type)
{
    char *prefx;

    /* deal with NULL prefix */
    if (NULL == prefix) {
        if (0 > asprintf(&prefx, " ")) {
            return PMIX_ERR_NOMEM;
        }
    } else {
        prefx = prefix;
    }

    /* if src is NULL, just print data type and return */
    if (NULL == src) {
        if (0 > asprintf(output, "%sData type: PMIX_PERSIST\tValue: NULL pointer", prefx)) {
            return PMIX_ERR_NOMEM;
        }
        if (prefx != prefix) {
            free(prefx);
        }
        return PMIX_SUCCESS;
    }

    if (0 > asprintf(output, "%sData type: PMIX_PERSIST\tValue: %ld", prefx, (long) *src)) {
        return PMIX_ERR_NOMEM;
    }
    if (prefx != prefix) {
        free(prefx);
    }

    return PMIX_SUCCESS;
}

pmix_status_t pmix12_bfrop_print_bo(char **output, char *prefix,
                                  pmix_byte_object_t *src, pmix_data_type_t type)
{
    char *prefx;

    /* deal with NULL prefix */
    if (NULL == prefix) {
        if (0 > asprintf(&prefx, " ")) {
            return PMIX_ERR_NOMEM;
        }
    } else {
        prefx = prefix;
    }

    /* if src is NULL, just print data type and return */
    if (NULL == src) {
        if (0 > asprintf(output, "%sData type: PMIX_BYTE_OBJECT\tValue: NULL pointer", prefx)) {
            return PMIX_ERR_NOMEM;
        }
        if (prefx != prefix) {
            free(prefx);
        }
        return PMIX_SUCCESS;
    }

    if (0 > asprintf(output, "%sData type: PMIX_BYTE_OBJECT\tSize: %ld", prefx, (long)src->size)) {
        return PMIX_ERR_NOMEM;
    }
    if (prefx != prefix) {
        free(prefx);
    }

    return PMIX_SUCCESS;
}

pmix_status_t pmix12_bfrop_print_scope(char **output, char *prefix,
                                      pmix_scope_t *src, pmix_data_type_t type)
{
    return PMIX_ERR_NOT_SUPPORTED;
}

pmix_status_t pmix12_bfrop_print_status(char **output, char *prefix,
                                       pmix_status_t *src, pmix_data_type_t type)
{
    return PMIX_ERR_NOT_SUPPORTED;
}

pmix_status_t pmix12_bfrop_print_ptr(char **output, char *prefix,
                                    void *src, pmix_data_type_t type)
{
    return PMIX_ERR_NOT_SUPPORTED;
}

pmix_status_t pmix12_bfrop_print_cmd(char **output, char *prefix,
                                    void *src, pmix_data_type_t type)
{
    return PMIX_ERR_NOT_SUPPORTED;
}

pmix_status_t pmix12_bfrop_print_info_directives(char **output, char *prefix,
                                                void *src, pmix_data_type_t type)
{
    return PMIX_ERR_NOT_SUPPORTED;
}

pmix_status_t pmix12_bfrop_print_datatype(char **output, char *prefix,
                                         pmix_data_type_t *src, pmix_data_type_t type)
{
    return PMIX_ERR_NOT_SUPPORTED;
}

pmix_status_t pmix12_bfrop_print_proc_state(char **output, char *prefix,
                                           pmix_data_type_t *src, pmix_data_type_t type)
{
    return PMIX_ERR_NOT_SUPPORTED;
}

pmix_status_t pmix12_bfrop_print_darray(char **output, char *prefix,
                                       pmix_data_array_t *src, pmix_data_type_t type)
{
    return PMIX_ERR_NOT_SUPPORTED;
}

pmix_status_t pmix12_bfrop_print_proc_info(char **output, char *prefix,
                                          pmix_proc_info_t *src, pmix_data_type_t type)
{
    return PMIX_ERR_NOT_SUPPORTED;
}

pmix_status_t pmix12_bfrop_print_query(char **output, char *prefix,
                                      pmix_query_t *src, pmix_data_type_t type)
{
    return PMIX_ERR_NOT_SUPPORTED;
}

pmix_status_t pmix12_bfrop_print_rank(char **output, char *prefix,
                                     pmix_rank_t *src, pmix_data_type_t type)
{
    return PMIX_ERR_NOT_SUPPORTED;
}
