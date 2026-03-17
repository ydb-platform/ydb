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
 * Copyright (c) 2014-2016 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2014-2018 Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"

#include <stdlib.h>
#ifdef HAVE_SYS_TIME_H
#include <sys/time.h> /* for struct timeval */
#endif

#include "opal/dss/dss_internal.h"

int opal_dss_compare(const void *value1, const void *value2, opal_data_type_t type)
{
    opal_dss_type_info_t *info;

    /* check for error */
    if (NULL == value1 || NULL == value2) {
        return OPAL_ERR_BAD_PARAM;
    }

    /* Lookup the compare function for this type and call it */

    if (NULL == (info = (opal_dss_type_info_t*)opal_pointer_array_get_item(&opal_dss_types, type))) {
        return OPAL_ERR_UNKNOWN_DATA_TYPE;
    }

    return info->odti_compare_fn(value1, value2, type);
}

/*
 * NUMERIC COMPARE FUNCTIONS
 */
int opal_dss_compare_int(int *value1, int *value2, opal_data_type_t type)
{
    if (*value1 > *value2) return OPAL_VALUE1_GREATER;

    if (*value2 > *value1) return OPAL_VALUE2_GREATER;

    return OPAL_EQUAL;
}

int opal_dss_compare_uint(unsigned int *value1, unsigned int *value2, opal_data_type_t type)
{
    if (*value1 > *value2) return OPAL_VALUE1_GREATER;

    if (*value2 > *value1) return OPAL_VALUE2_GREATER;

    return OPAL_EQUAL;
}

int opal_dss_compare_size(size_t *value1, size_t *value2, opal_data_type_t type)
{
    if (*value1 > *value2) return OPAL_VALUE1_GREATER;

    if (*value2 > *value1) return OPAL_VALUE2_GREATER;

    return OPAL_EQUAL;
}

int opal_dss_compare_pid(pid_t *value1, pid_t *value2, opal_data_type_t type)
{
    if (*value1 > *value2) return OPAL_VALUE1_GREATER;

    if (*value2 > *value1) return OPAL_VALUE2_GREATER;

    return OPAL_EQUAL;
}

int opal_dss_compare_byte(char *value1, char *value2, opal_data_type_t type)
{
    if (*value1 > *value2) return OPAL_VALUE1_GREATER;

    if (*value2 > *value1) return OPAL_VALUE2_GREATER;

    return OPAL_EQUAL;
}

int opal_dss_compare_char(char *value1, char *value2, opal_data_type_t type)
{
    if (*value1 > *value2) return OPAL_VALUE1_GREATER;

    if (*value2 > *value1) return OPAL_VALUE2_GREATER;

    return OPAL_EQUAL;
}

int opal_dss_compare_int8(int8_t *value1, int8_t *value2, opal_data_type_t type)
{
    if (*value1 > *value2) return OPAL_VALUE1_GREATER;

    if (*value2 > *value1) return OPAL_VALUE2_GREATER;

    return OPAL_EQUAL;
}

int opal_dss_compare_uint8(uint8_t *value1, uint8_t *value2, opal_data_type_t type)
{
    if (*value1 > *value2) return OPAL_VALUE1_GREATER;

    if (*value2 > *value1) return OPAL_VALUE2_GREATER;

    return OPAL_EQUAL;
}

int opal_dss_compare_int16(int16_t *value1, int16_t *value2, opal_data_type_t type)
{
    if (*value1 > *value2) return OPAL_VALUE1_GREATER;

    if (*value2 > *value1) return OPAL_VALUE2_GREATER;

    return OPAL_EQUAL;
}

int opal_dss_compare_uint16(uint16_t *value1, uint16_t *value2, opal_data_type_t type)
{
    if (*value1 > *value2) return OPAL_VALUE1_GREATER;

    if (*value2 > *value1) return OPAL_VALUE2_GREATER;

    return OPAL_EQUAL;
}

int opal_dss_compare_int32(int32_t *value1, int32_t *value2, opal_data_type_t type)
{
    if (*value1 > *value2) return OPAL_VALUE1_GREATER;

    if (*value2 > *value1) return OPAL_VALUE2_GREATER;

    return OPAL_EQUAL;
}

int opal_dss_compare_uint32(uint32_t *value1, uint32_t *value2, opal_data_type_t type)
{
    if (*value1 > *value2) return OPAL_VALUE1_GREATER;

    if (*value2 > *value1) return OPAL_VALUE2_GREATER;

    return OPAL_EQUAL;
}

int opal_dss_compare_int64(int64_t *value1, int64_t *value2, opal_data_type_t type)
{
    if (*value1 > *value2) return OPAL_VALUE1_GREATER;

    if (*value2 > *value1) return OPAL_VALUE2_GREATER;

    return OPAL_EQUAL;
}

int opal_dss_compare_uint64(uint64_t *value1, uint64_t *value2, opal_data_type_t type)
{
    if (*value1 > *value2) return OPAL_VALUE1_GREATER;

    if (*value2 > *value1) return OPAL_VALUE2_GREATER;

    return OPAL_EQUAL;
}

int opal_dss_compare_float(float *value1, float *value2, opal_data_type_t type)
{
    if (*value1 > *value2) return OPAL_VALUE1_GREATER;

    if (*value2 > *value1) return OPAL_VALUE2_GREATER;

    return OPAL_EQUAL;
}

int opal_dss_compare_double(double *value1, double *value2, opal_data_type_t type)
{
    if (*value1 > *value2) return OPAL_VALUE1_GREATER;

    if (*value2 > *value1) return OPAL_VALUE2_GREATER;

    return OPAL_EQUAL;
}

/*
 * NON-NUMERIC SYSTEM TYPES
 */

/* NULL */
int opal_dss_compare_null(char *value1, char *value2, opal_data_type_t type)
{
    return OPAL_EQUAL;
}

/* BOOL */
int opal_dss_compare_bool(bool *value1, bool *value2, opal_data_type_t type)
{
    if (*value1 && !(*value2)) return OPAL_VALUE1_GREATER;

    if (*value2 && !(*value1)) return OPAL_VALUE2_GREATER;

    return OPAL_EQUAL;

}

/* STRING */
int opal_dss_compare_string(char *value1, char *value2, opal_data_type_t type)
{
    if (0 < strcmp(value1, value2)) return OPAL_VALUE2_GREATER;

    if (0 > strcmp(value1, value2)) return OPAL_VALUE1_GREATER;

    return OPAL_EQUAL;
}

/* TIMEVAL */
int opal_dss_compare_timeval(struct timeval *value1, struct timeval *value2, opal_data_type_t type)
{
    if (value1->tv_sec > value2->tv_sec) return OPAL_VALUE1_GREATER;
    if (value2->tv_sec > value1->tv_sec) return OPAL_VALUE2_GREATER;

    /* seconds were equal - check usec's */
    if (value1->tv_usec > value2->tv_usec) return OPAL_VALUE1_GREATER;
    if (value2->tv_usec > value1->tv_usec) return OPAL_VALUE2_GREATER;

    return OPAL_EQUAL;
}

/* TIME */
int opal_dss_compare_time(time_t *value1, time_t *value2, opal_data_type_t type)
{
    if (value1 > value2) return OPAL_VALUE1_GREATER;
    if (value2 > value1) return OPAL_VALUE2_GREATER;

    return OPAL_EQUAL;
}

/* COMPARE FUNCTIONS FOR GENERIC OPAL TYPES */
/* OPAL_DATA_TYPE */
int opal_dss_compare_dt(opal_data_type_t *value1, opal_data_type_t *value2, opal_data_type_t type)
{
    if (*value1 > *value2) return OPAL_VALUE1_GREATER;

    if (*value2 > *value1) return OPAL_VALUE2_GREATER;

    return OPAL_EQUAL;
}

/* OPAL_BYTE_OBJECT */
int opal_dss_compare_byte_object(opal_byte_object_t *value1, opal_byte_object_t *value2, opal_data_type_t type)
{
    int checksum, diff;
    int32_t i;

    /* compare the sizes first - bigger size object is "greater than" */
    if (value1->size > value2->size) return OPAL_VALUE1_GREATER;

    if (value2->size > value1->size) return OPAL_VALUE2_GREATER;

    /* get here if the two sizes are identical - now do a simple checksum-style
     * calculation to determine "biggest"
     */
    checksum = 0;

    for (i=0; i < value1->size; i++) {
        /* protect against overflows */
        diff = value1->bytes[i] - value2->bytes[i];
        if (INT_MAX-abs(checksum)-abs(diff) < 0) { /* got an overflow condition */
            checksum = 0;
        }
        checksum += diff;
    }

    if (0 > checksum) return OPAL_VALUE2_GREATER;  /* sum of value2 bytes was greater */

    if (0 < checksum) return OPAL_VALUE1_GREATER;  /* of value1 bytes was greater */

    return OPAL_EQUAL;  /* sum of both value's bytes was identical */
}

/* OPAL_PSTAT */
int opal_dss_compare_pstat(opal_pstats_t *value1, opal_pstats_t *value2, opal_data_type_t type)
{
    return OPAL_EQUAL;  /* eventually compare field to field */
}

/* OPAL_NODE_STAT */
int opal_dss_compare_node_stat(opal_node_stats_t *value1, opal_node_stats_t *value2, opal_data_type_t type)
{
    return OPAL_EQUAL;  /* eventually compare field to field */
}

/* OPAL_VALUE */
int opal_dss_compare_value(opal_value_t *value1, opal_value_t *value2, opal_data_type_t type)
{
    if (NULL == value1 && NULL == value2) {
        return OPAL_EQUAL;
    }
    if (NULL == value2) {
        return OPAL_VALUE1_GREATER;
    }
    if (NULL == value1) {
        return OPAL_VALUE2_GREATER;
    }
    if (value1->type != value2->type) {
        opal_output(0, "COMPARE-OPAL-VALUE: INCONSISTENT TYPE %d vs %d", (int)value1->type, (int)value2->type);
        return OPAL_EQUAL;
    }
    switch (value1->type) {
    case OPAL_BYTE:
        return opal_dss_compare_byte((char *)&value1->data.byte, (char *)&value2->data.byte, type);
    case OPAL_STRING:
        return opal_dss_compare_string(value1->data.string, value2->data.string, type);
    case OPAL_PID:
        return opal_dss_compare_pid(&value1->data.pid, &value2->data.pid, type);
    case OPAL_INT:
        return opal_dss_compare_int(&value1->data.integer, &value2->data.integer, type);
    case OPAL_INT8:
        return opal_dss_compare_int8(&value1->data.int8, &value2->data.int8, type);
    case OPAL_INT16:
        return opal_dss_compare_int16(&value1->data.int16, &value2->data.int16, type);
    case OPAL_INT32:
        return opal_dss_compare_int32(&value1->data.int32, &value2->data.int32, type);
    case OPAL_INT64:
        return opal_dss_compare_int64(&value1->data.int64, &value2->data.int64, type);
    case OPAL_UINT:
        return opal_dss_compare_uint(&value1->data.uint, &value2->data.uint, type);
    case OPAL_UINT8:
        return opal_dss_compare_uint8(&value1->data.uint8, &value2->data.uint8, type);
    case OPAL_UINT16:
        return opal_dss_compare_uint16(&value1->data.uint16, &value2->data.uint16, type);
    case OPAL_UINT32:
        return opal_dss_compare_uint32(&value1->data.uint32, &value2->data.uint32, type);
    case OPAL_UINT64:
        return opal_dss_compare_uint64(&value1->data.uint64, &value2->data.uint64, type);
    case OPAL_BYTE_OBJECT:
        return opal_dss_compare_byte_object(&value1->data.bo, &value2->data.bo, type);
    case OPAL_SIZE:
        return opal_dss_compare_size(&value1->data.size, &value2->data.size, type);
    case OPAL_FLOAT:
        return opal_dss_compare_float(&value1->data.fval, &value2->data.fval, type);
    case OPAL_DOUBLE:
        return opal_dss_compare_double(&value1->data.dval, &value2->data.dval, type);
    case OPAL_BOOL:
        return opal_dss_compare_bool(&value1->data.flag, &value2->data.flag, type);
    case OPAL_TIMEVAL:
        return opal_dss_compare_timeval(&value1->data.tv, &value2->data.tv, type);
    case OPAL_NAME:
        return opal_dss_compare_name(&value1->data.name, &value2->data.name, type);
    case OPAL_ENVAR:
        return opal_dss_compare_envar(&value1->data.envar, &value2->data.envar, type);
    default:
        opal_output(0, "COMPARE-OPAL-VALUE: UNSUPPORTED TYPE %d", (int)value1->type);
        return OPAL_EQUAL;
    }
}

/* OPAL_BUFFER */
int opal_dss_compare_buffer_contents(opal_buffer_t *value1, opal_buffer_t *value2, opal_data_type_t type)
{
    return OPAL_EQUAL;  /* eventually compare bytes in buffers */
}

/* OPAL_NAME */
int opal_dss_compare_name(opal_process_name_t *value1,
                          opal_process_name_t *value2,
                          opal_data_type_t type)
{
    if (NULL == value1 && NULL == value2) {
        return OPAL_EQUAL;
    } else if (NULL == value1) {
        return OPAL_VALUE2_GREATER;
    } else if (NULL == value2) {
        return OPAL_VALUE1_GREATER;
    }

    /* If any of the fields are wildcard,
    * then we want to just ignore that one field. In the case
    * of OPAL_NAME_WILDCARD (where ALL of the fields are wildcard), this
    * will automatically result in OPAL_EQUAL for any name in the other
    * value - a totally useless result, but consistent in behavior.
    */

    /** check the jobids - if one of them is WILDCARD, then ignore
    * this field since anything is okay
    */
    if (value1->jobid != OPAL_JOBID_WILDCARD &&
        value2->jobid != OPAL_JOBID_WILDCARD) {
        if (value1->jobid < value2->jobid) {
            return OPAL_VALUE2_GREATER;
        } else if (value1->jobid > value2->jobid) {
            return OPAL_VALUE1_GREATER;
        }
    }

    /** check the vpids - if one of them is WILDCARD, then ignore
    * this field since anything is okay
    */
    if (value1->vpid != OPAL_VPID_WILDCARD &&
        value2->vpid != OPAL_VPID_WILDCARD) {
        if (value1->vpid < value2->vpid) {
            return OPAL_VALUE2_GREATER;
        } else if (value1->vpid > value2->vpid) {
            return OPAL_VALUE1_GREATER;
        }
    }

    /** only way to get here is if all fields are equal or WILDCARD */
    return OPAL_EQUAL;
}

int opal_dss_compare_vpid(opal_vpid_t *value1,
                          opal_vpid_t *value2,
                          opal_data_type_t type)
{
    /** if either value is WILDCARD, then return equal */
    if (*value1 == OPAL_VPID_WILDCARD ||
        *value2 == OPAL_VPID_WILDCARD) return OPAL_EQUAL;

    if (*value1 > *value2) return OPAL_VALUE1_GREATER;

    if (*value2 > *value1) return OPAL_VALUE2_GREATER;

    return OPAL_EQUAL;
}

int opal_dss_compare_jobid(opal_jobid_t *value1,
                           opal_jobid_t *value2,
                           opal_data_type_t type)
{
    /** if either value is WILDCARD, then return equal */
    if (*value1 == OPAL_JOBID_WILDCARD ||
        *value2 == OPAL_JOBID_WILDCARD) return OPAL_EQUAL;

    if (*value1 > *value2) return OPAL_VALUE1_GREATER;

    if (*value2 > *value1) return OPAL_VALUE2_GREATER;

    return OPAL_EQUAL;
}

int opal_dss_compare_status(int *value1, int *value2, opal_data_type_t type)
{
    if (*value1 > *value2) return OPAL_VALUE1_GREATER;

    if (*value2 > *value1) return OPAL_VALUE2_GREATER;

    return OPAL_EQUAL;
}

int opal_dss_compare_envar(opal_envar_t *value1, opal_envar_t *value2, opal_data_type_t type)
{
    int rc;

    if (NULL != value1->envar) {
        if (NULL == value2->envar) {
            return OPAL_VALUE1_GREATER;
        }
        rc = strcmp(value1->envar, value2->envar);
        if (rc < 0) {
            return OPAL_VALUE2_GREATER;
        } else if (0 < rc) {
            return OPAL_VALUE1_GREATER;
        }
    } else if (NULL != value2->envar) {
        /* we know value1->envar had to be NULL */
        return OPAL_VALUE2_GREATER;
    }

    /* if both are NULL or are equal, then check value */
    if (NULL != value1->value) {
        if (NULL == value2->value) {
            return OPAL_VALUE1_GREATER;
        }
        rc = strcmp(value1->value, value2->value);
        if (rc < 0) {
            return OPAL_VALUE2_GREATER;
        } else if (0 < rc) {
            return OPAL_VALUE1_GREATER;
        }
    } else if (NULL != value2->value) {
        /* we know value1->value had to be NULL */
        return OPAL_VALUE2_GREATER;
    }

    /* finally, check separator */
    if (value1->separator < value2->separator) {
        return OPAL_VALUE2_GREATER;
    }
    if (value2->separator < value1->separator) {
        return OPAL_VALUE1_GREATER;
    }
    return OPAL_EQUAL;
}
