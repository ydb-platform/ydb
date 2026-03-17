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
 * Copyright (c) 2014-2018 Intel, Inc. All rights reserved.
 * Copyright (c) 2014      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"

#include "opal_stdint.h"
#include <stdio.h>

#include "opal/util/error.h"
#include "opal/dss/dss_internal.h"

int opal_dss_print(char **output, char *prefix, void *src, opal_data_type_t type)
{
    opal_dss_type_info_t *info;

    /* check for error */
    if (NULL == output) {
        return OPAL_ERR_BAD_PARAM;
    }

    /* Lookup the print function for this type and call it */

    if(NULL == (info = (opal_dss_type_info_t*)opal_pointer_array_get_item(&opal_dss_types, type))) {
        return OPAL_ERR_UNKNOWN_DATA_TYPE;
    }

    return info->odti_print_fn(output, prefix, src, type);
}

/*
 * STANDARD PRINT FUNCTIONS FOR SYSTEM TYPES
 */
int opal_dss_print_byte(char **output, char *prefix, uint8_t *src, opal_data_type_t type)
{
    char *prefx;

    /* deal with NULL prefix */
    if (NULL == prefix) asprintf(&prefx, " ");
    else prefx = prefix;

    /* if src is NULL, just print data type and return */
    if (NULL == src) {
        asprintf(output, "%sData type: OPAL_BYTE\tValue: NULL pointer", prefx);
        if (prefx != prefix) {
            free(prefx);
        }
        return OPAL_SUCCESS;
    }

    asprintf(output, "%sData type: OPAL_BYTE\tValue: %x", prefix, *src);
    if (prefx != prefix) {
        free(prefx);
    }

    return OPAL_SUCCESS;
}

int opal_dss_print_string(char **output, char *prefix, char *src, opal_data_type_t type)
{
    char *prefx;

    /* deal with NULL prefix */
    if (NULL == prefix) asprintf(&prefx, " ");
    else prefx = prefix;

    /* if src is NULL, just print data type and return */
    if (NULL == src) {
        asprintf(output, "%sData type: OPAL_STRING\tValue: NULL pointer", prefx);
        if (prefx != prefix) {
            free(prefx);
        }
        return OPAL_SUCCESS;
    }

    asprintf(output, "%sData type: OPAL_STRING\tValue: %s", prefx, src);
    if (prefx != prefix) {
        free(prefx);
    }

    return OPAL_SUCCESS;
}

int opal_dss_print_size(char **output, char *prefix, size_t *src, opal_data_type_t type)
{
    char *prefx;

    /* deal with NULL prefix */
    if (NULL == prefix) asprintf(&prefx, " ");
    else prefx = prefix;

    /* if src is NULL, just print data type and return */
    if (NULL == src) {
        asprintf(output, "%sData type: OPAL_SIZE\tValue: NULL pointer", prefx);
        if (prefx != prefix) {
            free(prefx);
        }
        return OPAL_SUCCESS;
    }

    asprintf(output, "%sData type: OPAL_SIZE\tValue: %lu", prefx, (unsigned long) *src);
    if (prefx != prefix) {
        free(prefx);
    }

    return OPAL_SUCCESS;
}

int opal_dss_print_pid(char **output, char *prefix, pid_t *src, opal_data_type_t type)
{
    char *prefx;

    /* deal with NULL prefix */
    if (NULL == prefix) asprintf(&prefx, " ");
    else prefx = prefix;

    /* if src is NULL, just print data type and return */
    if (NULL == src) {
        asprintf(output, "%sData type: OPAL_PID\tValue: NULL pointer", prefx);
        if (prefx != prefix) {
            free(prefx);
        }
        return OPAL_SUCCESS;
    }

    asprintf(output, "%sData type: OPAL_PID\tValue: %lu", prefx, (unsigned long) *src);
    if (prefx != prefix) {
        free(prefx);
    }
    return OPAL_SUCCESS;
}

int opal_dss_print_bool(char **output, char *prefix, bool *src, opal_data_type_t type)
{
    char *prefx;

    /* deal with NULL prefix */
    if (NULL == prefix) asprintf(&prefx, " ");
    else prefx = prefix;

    /* if src is NULL, just print data type and return */
    if (NULL == src) {
        asprintf(output, "%sData type: OPAL_BOOL\tValue: NULL pointer", prefx);
        if (prefx != prefix) {
            free(prefx);
        }
        return OPAL_SUCCESS;
    }

    asprintf(output, "%sData type: OPAL_BOOL\tValue: %s", prefx, *src ? "TRUE" : "FALSE");
    if (prefx != prefix) {
        free(prefx);
    }

    return OPAL_SUCCESS;
}

int opal_dss_print_int(char **output, char *prefix, int *src, opal_data_type_t type)
{
    char *prefx;

    /* deal with NULL prefix */
    if (NULL == prefix) asprintf(&prefx, " ");
    else prefx = prefix;

    /* if src is NULL, just print data type and return */
    if (NULL == src) {
        asprintf(output, "%sData type: OPAL_INT\tValue: NULL pointer", prefx);
        if (prefx != prefix) {
            free(prefx);
        }
        return OPAL_SUCCESS;
    }

    asprintf(output, "%sData type: OPAL_INT\tValue: %ld", prefx, (long) *src);
    if (prefx != prefix) {
        free(prefx);
    }

    return OPAL_SUCCESS;
}

int opal_dss_print_uint(char **output, char *prefix, int *src, opal_data_type_t type)
{
    char *prefx;

    /* deal with NULL prefix */
    if (NULL == prefix) asprintf(&prefx, " ");
    else prefx = prefix;

    /* if src is NULL, just print data type and return */
    if (NULL == src) {
        asprintf(output, "%sData type: OPAL_UINT\tValue: NULL pointer", prefx);
        if (prefx != prefix) {
            free(prefx);
        }
        return OPAL_SUCCESS;
    }

    asprintf(output, "%sData type: OPAL_UINT\tValue: %lu", prefx, (unsigned long) *src);
    if (prefx != prefix) {
        free(prefx);
    }

    return OPAL_SUCCESS;
}

int opal_dss_print_uint8(char **output, char *prefix, uint8_t *src, opal_data_type_t type)
{
    char *prefx;

    /* deal with NULL prefix */
    if (NULL == prefix) asprintf(&prefx, " ");
    else prefx = prefix;

    /* if src is NULL, just print data type and return */
    if (NULL == src) {
        asprintf(output, "%sData type: OPAL_UINT8\tValue: NULL pointer", prefx);
        if (prefx != prefix) {
            free(prefx);
        }
        return OPAL_SUCCESS;
    }

    asprintf(output, "%sData type: OPAL_UINT8\tValue: %u", prefx, (unsigned int) *src);
    if (prefx != prefix) {
        free(prefx);
    }

    return OPAL_SUCCESS;
}

int opal_dss_print_uint16(char **output, char *prefix, uint16_t *src, opal_data_type_t type)
{
    char *prefx;

    /* deal with NULL prefix */
    if (NULL == prefix) asprintf(&prefx, " ");
    else prefx = prefix;

    /* if src is NULL, just print data type and return */
    if (NULL == src) {
        asprintf(output, "%sData type: OPAL_UINT16\tValue: NULL pointer", prefx);
        if (prefx != prefix) {
            free(prefx);
        }
        return OPAL_SUCCESS;
    }

    asprintf(output, "%sData type: OPAL_UINT16\tValue: %u", prefx, (unsigned int) *src);
    if (prefx != prefix) {
        free(prefx);
    }

    return OPAL_SUCCESS;
}

int opal_dss_print_uint32(char **output, char *prefix, uint32_t *src, opal_data_type_t type)
{
    char *prefx;

    /* deal with NULL prefix */
    if (NULL == prefix) asprintf(&prefx, " ");
    else prefx = prefix;

    /* if src is NULL, just print data type and return */
    if (NULL == src) {
        asprintf(output, "%sData type: OPAL_UINT32\tValue: NULL pointer", prefx);
        if (prefx != prefix) {
            free(prefx);
        }
        return OPAL_SUCCESS;
    }

    asprintf(output, "%sData type: OPAL_UINT32\tValue: %u", prefx, (unsigned int) *src);
    if (prefx != prefix) {
        free(prefx);
    }

    return OPAL_SUCCESS;
}

int opal_dss_print_int8(char **output, char *prefix, int8_t *src, opal_data_type_t type)
{
    char *prefx;

    /* deal with NULL prefix */
    if (NULL == prefix) asprintf(&prefx, " ");
    else prefx = prefix;

    /* if src is NULL, just print data type and return */
    if (NULL == src) {
        asprintf(output, "%sData type: OPAL_INT8\tValue: NULL pointer", prefx);
        if (prefx != prefix) {
            free(prefx);
        }
        return OPAL_SUCCESS;
    }

    asprintf(output, "%sData type: OPAL_INT8\tValue: %d", prefx, (int) *src);
    if (prefx != prefix) {
        free(prefx);
    }

    return OPAL_SUCCESS;
}

int opal_dss_print_int16(char **output, char *prefix, int16_t *src, opal_data_type_t type)
{
    char *prefx;

    /* deal with NULL prefix */
    if (NULL == prefix) asprintf(&prefx, " ");
    else prefx = prefix;

    /* if src is NULL, just print data type and return */
    if (NULL == src) {
        asprintf(output, "%sData type: OPAL_INT16\tValue: NULL pointer", prefx);
        if (prefx != prefix) {
            free(prefx);
        }
        return OPAL_SUCCESS;
    }

    asprintf(output, "%sData type: OPAL_INT16\tValue: %d", prefx, (int) *src);
    if (prefx != prefix) {
        free(prefx);
    }

    return OPAL_SUCCESS;
}

int opal_dss_print_int32(char **output, char *prefix, int32_t *src, opal_data_type_t type)
{
    char *prefx;

    /* deal with NULL prefix */
    if (NULL == prefix) asprintf(&prefx, " ");
    else prefx = prefix;

    /* if src is NULL, just print data type and return */
    if (NULL == src) {
        asprintf(output, "%sData type: OPAL_INT32\tValue: NULL pointer", prefx);
        if (prefx != prefix) {
            free(prefx);
        }
        return OPAL_SUCCESS;
    }

    asprintf(output, "%sData type: OPAL_INT32\tValue: %d", prefx, (int) *src);
    if (prefx != prefix) {
        free(prefx);
    }

    return OPAL_SUCCESS;
}
int opal_dss_print_uint64(char **output, char *prefix,
#ifdef HAVE_INT64_T
                          uint64_t *src,
#else
                          void *src,
#endif  /* HAVE_INT64_T */
                          opal_data_type_t type)
{
    char *prefx;

    /* deal with NULL prefix */
    if (NULL == prefix) asprintf(&prefx, " ");
    else prefx = prefix;

    /* if src is NULL, just print data type and return */
    if (NULL == src) {
        asprintf(output, "%sData type: OPAL_UINT64\tValue: NULL pointer", prefx);
        if (prefx != prefix) {
            free(prefx);
        }
        return OPAL_SUCCESS;
    }

#ifdef HAVE_INT64_T
    asprintf(output, "%sData type: OPAL_UINT64\tValue: %lu", prefx, (unsigned long) *src);
#else
    asprintf(output, "%sData type: OPAL_UINT64\tValue: unsupported", prefx);
#endif  /* HAVE_INT64_T */
    if (prefx != prefix) {
        free(prefx);
    }

    return OPAL_SUCCESS;
}

int opal_dss_print_int64(char **output, char *prefix,
#ifdef HAVE_INT64_T
                         int64_t *src,
#else
                         void *src,
#endif  /* HAVE_INT64_T */
                         opal_data_type_t type)
{
    char *prefx;

    /* deal with NULL prefix */
    if (NULL == prefix) asprintf(&prefx, " ");
    else prefx = prefix;

    /* if src is NULL, just print data type and return */
    if (NULL == src) {
        asprintf(output, "%sData type: OPAL_INT64\tValue: NULL pointer", prefx);
        if (prefx != prefix) {
            free(prefx);
        }
        return OPAL_SUCCESS;
    }

#ifdef HAVE_INT64_T
    asprintf(output, "%sData type: OPAL_INT64\tValue: %ld", prefx, (long) *src);
#else
    asprintf(output, "%sData type: OPAL_INT64\tValue: unsupported", prefx);
#endif  /* HAVE_INT64_T */
    if (prefx != prefix) {
        free(prefx);
    }

    return OPAL_SUCCESS;
}

int opal_dss_print_float(char **output, char *prefix,
                         float *src, opal_data_type_t type)
{
    char *prefx;

    /* deal with NULL prefix */
    if (NULL == prefix) asprintf(&prefx, " ");
    else prefx = prefix;

    /* if src is NULL, just print data type and return */
    if (NULL == src) {
        asprintf(output, "%sData type: OPAL_FLOAT\tValue: NULL pointer", prefx);
        if (prefx != prefix) {
            free(prefx);
        }
        return OPAL_SUCCESS;
    }

    asprintf(output, "%sData type: OPAL_FLOAT\tValue: %f", prefx, *src);
    if (prefx != prefix) {
        free(prefx);
    }

    return OPAL_SUCCESS;
}

int opal_dss_print_double(char **output, char *prefix,
                          double *src, opal_data_type_t type)
{
    char *prefx;

    /* deal with NULL prefix */
    if (NULL == prefix) asprintf(&prefx, " ");
    else prefx = prefix;

    /* if src is NULL, just print data type and return */
    if (NULL == src) {
        asprintf(output, "%sData type: OPAL_DOUBLE\tValue: NULL pointer", prefx);
        if (prefx != prefix) {
            free(prefx);
        }
        return OPAL_SUCCESS;
    }

    asprintf(output, "%sData type: OPAL_DOUBLE\tValue: %f", prefx, *src);
    if (prefx != prefix) {
        free(prefx);
    }

    return OPAL_SUCCESS;
}

int opal_dss_print_time(char **output, char *prefix,
                        time_t *src, opal_data_type_t type)
{
    char *prefx;
    char *t;

    /* deal with NULL prefix */
    if (NULL == prefix) asprintf(&prefx, " ");
    else prefx = prefix;

    /* if src is NULL, just print data type and return */
    if (NULL == src) {
        asprintf(output, "%sData type: OPAL_TIME\tValue: NULL pointer", prefx);
        if (prefx != prefix) {
            free(prefx);
        }
        return OPAL_SUCCESS;
    }

    t = ctime(src);
    t[strlen(t)-1] = '\0';  // remove trailing newline

    asprintf(output, "%sData type: OPAL_TIME\tValue: %s", prefx, t);
    if (prefx != prefix) {
        free(prefx);
    }

    return OPAL_SUCCESS;
}

int opal_dss_print_timeval(char **output, char *prefix,
                           struct timeval *src, opal_data_type_t type)
{
    char *prefx;

    /* deal with NULL prefix */
    if (NULL == prefix) asprintf(&prefx, " ");
    else prefx = prefix;

    /* if src is NULL, just print data type and return */
    if (NULL == src) {
        asprintf(output, "%sData type: OPAL_TIMEVAL\tValue: NULL pointer", prefx);
        if (prefx != prefix) {
            free(prefx);
        }
        return OPAL_SUCCESS;
    }

    asprintf(output, "%sData type: OPAL_TIMEVAL\tValue: %ld.%06ld", prefx,
             (long)src->tv_sec, (long)src->tv_usec);
    if (prefx != prefix) {
        free(prefx);
    }

    return OPAL_SUCCESS;
}

int opal_dss_print_null(char **output, char *prefix, void *src, opal_data_type_t type)
{
    char *prefx;

    /* deal with NULL prefix */
    if (NULL == prefix) asprintf(&prefx, " ");
    else prefx = prefix;

    /* if src is NULL, just print data type and return */
    if (NULL == src) {
        asprintf(output, "%sData type: OPAL_NULL\tValue: NULL pointer", prefx);
        if (prefx != prefix) {
            free(prefx);
        }
        return OPAL_SUCCESS;
    }

    asprintf(output, "%sData type: OPAL_NULL", prefx);
    if (prefx != prefix) {
        free(prefx);
    }

    return OPAL_SUCCESS;
}


/* PRINT FUNCTIONS FOR GENERIC OPAL TYPES */

/*
 * OPAL_DATA_TYPE
 */
int opal_dss_print_data_type(char **output, char *prefix, opal_data_type_t *src, opal_data_type_t type)
{
    char *prefx;

    /* deal with NULL prefix */
    if (NULL == prefix) asprintf(&prefx, " ");
    else prefx = prefix;

    /* if src is NULL, just print data type and return */
    if (NULL == src) {
        asprintf(output, "%sData type: OPAL_DATA_TYPE\tValue: NULL pointer", prefx);
        if (prefx != prefix) {
            free(prefx);
        }
        return OPAL_SUCCESS;
    }

    asprintf(output, "%sData type: OPAL_DATA_TYPE\tValue: %lu", prefx, (unsigned long) *src);
    if (prefx != prefix) {
        free(prefx);
    }
    return OPAL_SUCCESS;
}

/*
 * OPAL_BYTE_OBJECT
 */
int opal_dss_print_byte_object(char **output, char *prefix, opal_byte_object_t *src, opal_data_type_t type)
{
    char *prefx;

    /* deal with NULL prefix */
    if (NULL == prefix) asprintf(&prefx, " ");
    else prefx = prefix;

    /* if src is NULL, just print data type and return */
    if (NULL == src) {
        asprintf(output, "%sData type: OPAL_BYTE_OBJECT\tValue: NULL pointer", prefx);
        if (prefx != prefix) {
            free(prefx);
        }
        return OPAL_SUCCESS;
    }

    asprintf(output, "%sData type: OPAL_BYTE_OBJECT\tSize: %lu", prefx, (unsigned long) src->size);
    if (prefx != prefix) {
        free(prefx);
    }

    return OPAL_SUCCESS;
}

/*
 * OPAL_PSTAT
 */
int opal_dss_print_pstat(char **output, char *prefix, opal_pstats_t *src, opal_data_type_t type)
{
    char *prefx;

    /* deal with NULL prefix */
    if (NULL == prefix) asprintf(&prefx, " ");
    else prefx = prefix;

    /* if src is NULL, just print data type and return */
    if (NULL == src) {
        asprintf(output, "%sData type: OPAL_PSTATS\tValue: NULL pointer", prefx);
        if (prefx != prefix) {
            free(prefx);
        }
        return OPAL_SUCCESS;
    }
    asprintf(output, "%sOPAL_PSTATS SAMPLED AT: %ld.%06ld\n%snode: %s rank: %d pid: %d cmd: %s state: %c pri: %d #threads: %d Processor: %d\n"
             "%s\ttime: %ld.%06ld cpu: %5.2f  PSS: %8.2f  VMsize: %8.2f PeakVMSize: %8.2f RSS: %8.2f\n",
             prefx, (long)src->sample_time.tv_sec, (long)src->sample_time.tv_usec,
             prefx, src->node, src->rank, src->pid, src->cmd, src->state[0], src->priority, src->num_threads, src->processor,
             prefx, (long)src->time.tv_sec, (long)src->time.tv_usec, src->percent_cpu, src->pss, src->vsize, src->peak_vsize, src->rss);
    if (prefx != prefix) {
        free(prefx);
    }

    return OPAL_SUCCESS;
}

/*
 * OPAL_NODE_STAT
 */
int opal_dss_print_node_stat(char **output, char *prefix, opal_node_stats_t *src, opal_data_type_t type)
{
    char *prefx;

    /* deal with NULL prefix */
    if (NULL == prefix) asprintf(&prefx, " ");
    else prefx = prefix;

    /* if src is NULL, just print data type and return */
    if (NULL == src) {
        asprintf(output, "%sData type: OPAL_NODE_STATS\tValue: NULL pointer", prefx);
        if (prefx != prefix) {
            free(prefx);
        }
        return OPAL_SUCCESS;
    }
    asprintf(output, "%sOPAL_NODE_STATS SAMPLED AT: %ld.%06ld\n%sTotal Mem: %5.2f Free Mem: %5.2f Buffers: %5.2f Cached: %5.2f\n"
             "%sSwapCached: %5.2f SwapTotal: %5.2f SwapFree: %5.2f Mapped: %5.2f\n"
             "%s\tla: %5.2f\tla5: %5.2f\tla15: %5.2f\n",
             prefx, (long)src->sample_time.tv_sec, (long)src->sample_time.tv_usec,
             prefx, src->total_mem, src->free_mem, src->buffers, src->cached,
             prefx, src->swap_cached, src->swap_total, src->swap_free, src->mapped,
             prefx, src->la, src->la5, src->la15);
    if (prefx != prefix) {
        free(prefx);
    }

    return OPAL_SUCCESS;
}

/*
 * OPAL_VALUE
 */
int opal_dss_print_value(char **output, char *prefix, opal_value_t *src, opal_data_type_t type)
{
    char *prefx;

    /* deal with NULL prefix */
    if (NULL == prefix) asprintf(&prefx, " ");
    else prefx = prefix;

    /* if src is NULL, just print data type and return */
    if (NULL == src) {
        asprintf(output, "%sData type: OPAL_VALUE\tValue: NULL pointer", prefx);
        if (prefx != prefix) {
            free(prefx);
        }
        return OPAL_SUCCESS;
    }

    switch (src->type) {
    case OPAL_BOOL:
        asprintf(output, "%sOPAL_VALUE: Data type: OPAL_BOOL\tKey: %s\tValue: %s",
                 prefx, src->key, src->data.flag ? "true" : "false");
        break;
    case OPAL_BYTE:
        asprintf(output, "%sOPAL_VALUE: Data type: OPAL_BYTE\tKey: %s\tValue: %x",
                 prefx, src->key, src->data.byte);
        break;
    case OPAL_STRING:
        asprintf(output, "%sOPAL_VALUE: Data type: OPAL_STRING\tKey: %s\tValue: %s",
                 prefx, src->key, src->data.string);
        break;
    case OPAL_SIZE:
        asprintf(output, "%sOPAL_VALUE: Data type: OPAL_SIZE\tKey: %s\tValue: %lu",
                 prefx, src->key, (unsigned long)src->data.size);
        break;
    case OPAL_PID:
        asprintf(output, "%sOPAL_VALUE: Data type: OPAL_PID\tKey: %s\tValue: %lu",
                 prefx, src->key, (unsigned long)src->data.pid);
        break;
    case OPAL_INT:
        asprintf(output, "%sOPAL_VALUE: Data type: OPAL_INT\tKey: %s\tValue: %d",
                 prefx, src->key, src->data.integer);
        break;
    case OPAL_INT8:
        asprintf(output, "%sOPAL_VALUE: Data type: OPAL_INT8\tKey: %s\tValue: %d",
                 prefx, src->key, (int)src->data.int8);
        break;
    case OPAL_INT16:
        asprintf(output, "%sOPAL_VALUE: Data type: OPAL_INT16\tKey: %s\tValue: %d",
                 prefx, src->key, (int)src->data.int16);
        break;
    case OPAL_INT32:
        asprintf(output, "%sOPAL_VALUE: Data type: OPAL_INT32\tKey: %s\tValue: %d",
                 prefx, src->key, src->data.int32);
        break;
    case OPAL_INT64:
        asprintf(output, "%sOPAL_VALUE: Data type: OPAL_INT64\tKey: %s\tValue: %ld",
                 prefx, src->key, (long)src->data.int64);
        break;
    case OPAL_UINT:
        asprintf(output, "%sOPAL_VALUE: Data type: OPAL_UINT\tKey: %s\tValue: %u",
                 prefx, src->key, src->data.uint);
        break;
    case OPAL_UINT8:
        asprintf(output, "%sOPAL_VALUE: Data type: OPAL_UINT8\tKey: %s\tValue: %u",
                 prefx, src->key, (unsigned int)src->data.uint8);
        break;
    case OPAL_UINT16:
        asprintf(output, "%sOPAL_VALUE: Data type: OPAL_UINT16\tKey: %s\tValue: %u",
                 prefx, src->key, (unsigned int)src->data.uint16);
        break;
    case OPAL_UINT32:
        asprintf(output, "%sOPAL_VALUE: Data type: OPAL_UINT32\tKey: %s\tValue: %u",
                 prefx, src->key, src->data.uint32);
        break;
    case OPAL_UINT64:
        asprintf(output, "%sOPAL_VALUE: Data type: OPAL_UINT64\tKey: %s\tValue: %lu",
                 prefx, src->key, (unsigned long)src->data.uint64);
        break;
    case OPAL_FLOAT:
        asprintf(output, "%sOPAL_VALUE: Data type: OPAL_FLOAT\tKey: %s\tValue: %f",
                 prefx, src->key, src->data.fval);
        break;
    case OPAL_DOUBLE:
        asprintf(output, "%sOPAL_VALUE: Data type: OPAL_DOUBLE\tKey: %s\tValue: %f",
                 prefx, src->key, src->data.dval);
        break;
    case OPAL_BYTE_OBJECT:
        asprintf(output, "%sOPAL_VALUE: Data type: OPAL_BYTE_OBJECT\tKey: %s\tData: %s\tSize: %lu",
                 prefx, src->key, (NULL == src->data.bo.bytes) ? "NULL" : "NON-NULL", (unsigned long)src->data.bo.size);
        break;
    case OPAL_TIMEVAL:
        asprintf(output, "%sOPAL_VALUE: Data type: OPAL_TIMEVAL\tKey: %s\tValue: %ld.%06ld", prefx,
                 src->key, (long)src->data.tv.tv_sec, (long)src->data.tv.tv_usec);
        break;
    case OPAL_TIME:
        asprintf(output, "%sOPAL_VALUE: Data type: OPAL_TIME\tKey: %s\tValue: %s", prefx,
                 src->key, ctime(&src->data.time));
        break;
    case OPAL_NAME:
        asprintf(output, "%sOPAL_VALUE: Data type: OPAL_NAME\tKey: %s\tValue: %s", prefx,
                 src->key, OPAL_NAME_PRINT(src->data.name));
        break;
    case OPAL_PTR:
        asprintf(output, "%sOPAL_VALUE: Data type: OPAL_PTR\tKey: %s", prefx, src->key);
        break;
    case OPAL_ENVAR:
        asprintf(output, "%sOPAL_VALUE: Data type: OPAL_ENVAR\tKey: %s\tName: %s\tValue: %s\tSeparator: %c",
                 prefx, src->key,
                 (NULL == src->data.envar.envar) ? "NULL" : src->data.envar.envar,
                 (NULL == src->data.envar.value) ? "NULL" : src->data.envar.value,
                 ('\0' == src->data.envar.separator) ? ' ' : src->data.envar.separator);
        break;
    default:
        asprintf(output, "%sOPAL_VALUE: Data type: UNKNOWN\tKey: %s\tValue: UNPRINTABLE",
                 prefx, src->key);
        break;
    }
    if (prefx != prefix) {
        free(prefx);
    }
    return OPAL_SUCCESS;
}

int opal_dss_print_buffer_contents(char **output, char *prefix,
                                   opal_buffer_t *src, opal_data_type_t type)
{
    return OPAL_SUCCESS;
}

/*
 * NAME
 */
int opal_dss_print_name(char **output, char *prefix, opal_process_name_t *name, opal_data_type_t type)
{
    /* set default result */
    *output = NULL;

    if (NULL == name) {
        asprintf(output, "%sData type: ORTE_PROCESS_NAME\tData Value: NULL",
                 (NULL == prefix ? " " : prefix));
    } else {
        asprintf(output, "%sData type: ORTE_PROCESS_NAME\tData Value: [%d,%d]",
                 (NULL == prefix ? " " : prefix), name->jobid, name->vpid);
    }

    return OPAL_SUCCESS;
}

int opal_dss_print_jobid(char **output, char *prefix,
                         opal_process_name_t *src, opal_data_type_t type)
{
    char *prefx = " ";

    /* deal with NULL prefix */
    if (NULL != prefix) prefx = prefix;

    /* if src is NULL, just print data type and return */
    if (NULL == src) {
        asprintf(output, "%sData type: OPAL_JOBID\tValue: NULL pointer", prefx);
        return OPAL_SUCCESS;
    }

    asprintf(output, "%sData type: OPAL_JOBID\tValue: %s", prefx, opal_jobid_print(src->jobid));
    return OPAL_SUCCESS;
}

int opal_dss_print_vpid(char **output, char *prefix,
                         opal_process_name_t *src, opal_data_type_t type)
{
    char *prefx = " ";

    /* deal with NULL prefix */
    if (NULL != prefix) prefx = prefix;

    /* if src is NULL, just print data type and return */
    if (NULL == src) {
        asprintf(output, "%sData type: OPAL_VPID\tValue: NULL pointer", prefx);
        return OPAL_SUCCESS;
    }

    asprintf(output, "%sData type: OPAL_VPID\tValue: %s", prefx, opal_vpid_print(src->vpid));
    return OPAL_SUCCESS;
}

int opal_dss_print_status(char **output, char *prefix,
                          int *src, opal_data_type_t type)
{
    char *prefx = " ";

    /* deal with NULL prefix */
    if (NULL != prefix) prefx = prefix;

    /* if src is NULL, just print data type and return */
    if (NULL == src) {
        asprintf(output, "%sData type: OPAL_STATUS\tValue: NULL pointer", prefx);
        return OPAL_SUCCESS;
    }

    asprintf(output, "%sData type: OPAL_STATUS\tValue: %s", prefx, opal_strerror(*src));
    return OPAL_SUCCESS;
}


int opal_dss_print_envar(char **output, char *prefix,
                         opal_envar_t *src, opal_data_type_t type)
{
    char *prefx = " ";

    /* deal with NULL prefix */
    if (NULL != prefix) prefx = prefix;

    /* if src is NULL, just print data type and return */
    if (NULL == src) {
        asprintf(output, "%sData type: OPAL_ENVAR\tValue: NULL pointer", prefx);
        return OPAL_SUCCESS;
    }

    asprintf(output, "%sOPAL_VALUE: Data type: OPAL_ENVAR\tName: %s\tValue: %s\tSeparator: %c",
             prefx, (NULL == src->envar) ? "NULL" : src->envar,
             (NULL == src->value) ? "NULL" : src->value,
             ('\0' == src->separator) ? ' ' : src->separator);
    return OPAL_SUCCESS;
}
