/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
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
 * Copyright (c) 2012-2015 Los Alamos National Security, Inc.  All rights reserved.
 * Copyright (c) 2014-2018 Intel, Inc. All rights reserved.
 * Copyright (c) 2014-2015 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"
#include "opal/types.h"
#include "opal/util/error.h"
#include "opal/util/output.h"
#include "opal/dss/dss_internal.h"

int opal_dss_unpack(opal_buffer_t *buffer, void *dst, int32_t *num_vals,
                    opal_data_type_t type)
{
    int rc, ret;
    int32_t local_num, n=1;
    opal_data_type_t local_type;

    /* check for error */
    if (NULL == buffer || NULL == dst || NULL == num_vals) {
        return OPAL_ERR_BAD_PARAM;
    }

    /* if user provides a zero for num_vals, then there is no storage allocated
     * so return an appropriate error
     */
    if (0 == *num_vals) {
        OPAL_OUTPUT( ( opal_dss_verbose, "opal_dss_unpack: inadequate space ( %p, %p, %lu, %d )\n",
                       (void*)buffer, dst, (long unsigned int)*num_vals, (int)type ) );
        return OPAL_ERR_UNPACK_INADEQUATE_SPACE;
    }

    /** Unpack the declared number of values
     * REMINDER: it is possible that the buffer is corrupted and that
     * the DSS will *think* there is a proper int32_t variable at the
     * beginning of the unpack region - but that the value is bogus (e.g., just
     * a byte field in a string array that so happens to have a value that
     * matches the int32_t data type flag). Therefore, this error check is
     * NOT completely safe. This is true for ALL unpack functions, not just
     * int32_t as used here.
     */
    if (OPAL_DSS_BUFFER_FULLY_DESC == buffer->type) {
        if (OPAL_SUCCESS != (
            rc = opal_dss_get_data_type(buffer, &local_type))) {
            *num_vals = 0;
            return rc;
        }
        if (OPAL_INT32 != local_type) { /* if the length wasn't first, then error */
            *num_vals = 0;
            return OPAL_ERR_UNPACK_FAILURE;
        }
    }

    n=1;
    if (OPAL_SUCCESS != (rc = opal_dss_unpack_int32(buffer, &local_num, &n, OPAL_INT32))) {
        *num_vals = 0;
        return rc;
    }

    /** if the storage provided is inadequate, set things up
     * to unpack as much as we can and to return an error code
     * indicating that everything was not unpacked - the buffer
     * is left in a state where it can not be further unpacked.
     */
    if (local_num > *num_vals) {
        local_num = *num_vals;
        OPAL_OUTPUT( ( opal_dss_verbose, "opal_dss_unpack: inadequate space ( %p, %p, %lu, %d )\n",
                       (void*)buffer, dst, (long unsigned int)*num_vals, (int)type ) );
        ret = OPAL_ERR_UNPACK_INADEQUATE_SPACE;
    } else {  /** enough or more than enough storage */
        *num_vals = local_num;  /** let the user know how many we actually unpacked */
        ret = OPAL_SUCCESS;
    }

    /** Unpack the value(s) */
    if (OPAL_SUCCESS != (rc = opal_dss_unpack_buffer(buffer, dst, &local_num, type))) {
        *num_vals = 0;
        ret = rc;
    }

    return ret;
}

int opal_dss_unpack_buffer(opal_buffer_t *buffer, void *dst, int32_t *num_vals,
                    opal_data_type_t type)
{
    int rc;
    opal_data_type_t local_type;
    opal_dss_type_info_t *info;

    OPAL_OUTPUT( ( opal_dss_verbose, "opal_dss_unpack_buffer( %p, %p, %lu, %d )\n",
                   (void*)buffer, dst, (long unsigned int)*num_vals, (int)type ) );

    /** Unpack the declared data type */
    if (OPAL_DSS_BUFFER_FULLY_DESC == buffer->type) {
        if (OPAL_SUCCESS != (rc = opal_dss_get_data_type(buffer, &local_type))) {
            return rc;
        }
        /* if the data types don't match, then return an error */
        if (type != local_type) {
            opal_output(0, "OPAL dss:unpack: got type %d when expecting type %d", local_type, type);
            return OPAL_ERR_PACK_MISMATCH;
        }
    }

    /* Lookup the unpack function for this type and call it */

    if (NULL == (info = (opal_dss_type_info_t*)opal_pointer_array_get_item(&opal_dss_types, type))) {
        return OPAL_ERR_UNPACK_FAILURE;
    }

    return info->odti_unpack_fn(buffer, dst, num_vals, type);
}


/* UNPACK GENERIC SYSTEM TYPES */

/*
 * BOOL
 */
int opal_dss_unpack_bool(opal_buffer_t *buffer, void *dest,
                         int32_t *num_vals, opal_data_type_t type)
{
    int ret;
    opal_data_type_t remote_type;

    if (OPAL_DSS_BUFFER_FULLY_DESC == buffer->type) {
        /* see what type was actually packed */
        if (OPAL_SUCCESS != (ret = opal_dss_peek_type(buffer, &remote_type))) {
            return ret;
        }
    } else {
        if (OPAL_SUCCESS != (ret = opal_dss_get_data_type(buffer, &remote_type))) {
            return ret;
        }
    }

    if (remote_type == DSS_TYPE_BOOL) {
        /* fast path it if the sizes are the same */
        /* Turn around and unpack the real type */
        if (OPAL_SUCCESS != (ret = opal_dss_unpack_buffer(buffer, dest, num_vals, DSS_TYPE_BOOL))) {
        }
    } else {
        /* slow path - types are different sizes */
        UNPACK_SIZE_MISMATCH(bool, remote_type, ret);
    }
    return ret;
}

/*
 * INT
 */
int opal_dss_unpack_int(opal_buffer_t *buffer, void *dest,
                        int32_t *num_vals, opal_data_type_t type)
{
    int ret;
    opal_data_type_t remote_type;

    if (OPAL_DSS_BUFFER_FULLY_DESC == buffer->type) {
        /* see what type was actually packed */
        if (OPAL_SUCCESS != (ret = opal_dss_peek_type(buffer, &remote_type))) {
            return ret;
        }
    } else {
        if (OPAL_SUCCESS != (ret = opal_dss_get_data_type(buffer, &remote_type))) {
            return ret;
        }
    }

    if (remote_type == DSS_TYPE_INT) {
        /* fast path it if the sizes are the same */
        /* Turn around and unpack the real type */
        if (OPAL_SUCCESS != (ret = opal_dss_unpack_buffer(buffer, dest, num_vals, DSS_TYPE_INT))) {
        }
    } else {
        /* slow path - types are different sizes */
        UNPACK_SIZE_MISMATCH(int, remote_type, ret);
    }

    return ret;
}

/*
 * SIZE_T
 */
int opal_dss_unpack_sizet(opal_buffer_t *buffer, void *dest,
                          int32_t *num_vals, opal_data_type_t type)
{
    int ret;
    opal_data_type_t remote_type;

    if (OPAL_DSS_BUFFER_FULLY_DESC == buffer->type) {
        /* see what type was actually packed */
        if (OPAL_SUCCESS != (ret = opal_dss_peek_type(buffer, &remote_type))) {
            return ret;
        }
    } else {
        if (OPAL_SUCCESS != (ret = opal_dss_get_data_type(buffer, &remote_type))) {
            return ret;
        }
    }

    if (remote_type == DSS_TYPE_SIZE_T) {
        /* fast path it if the sizes are the same */
        /* Turn around and unpack the real type */
        if (OPAL_SUCCESS != (ret = opal_dss_unpack_buffer(buffer, dest, num_vals, DSS_TYPE_SIZE_T))) {
        }
    } else {
        /* slow path - types are different sizes */
        UNPACK_SIZE_MISMATCH(size_t, remote_type, ret);
    }

    return ret;
}

/*
 * PID_T
 */
int opal_dss_unpack_pid(opal_buffer_t *buffer, void *dest,
                        int32_t *num_vals, opal_data_type_t type)
{
    int ret;
    opal_data_type_t remote_type;

    if (OPAL_DSS_BUFFER_FULLY_DESC == buffer->type) {
        /* see what type was actually packed */
        if (OPAL_SUCCESS != (ret = opal_dss_peek_type(buffer, &remote_type))) {
            return ret;
        }
    } else {
        if (OPAL_SUCCESS != (ret = opal_dss_get_data_type(buffer, &remote_type))) {
            return ret;
        }
    }

    if (remote_type == DSS_TYPE_PID_T) {
        /* fast path it if the sizes are the same */
        /* Turn around and unpack the real type */
        if (OPAL_SUCCESS != (ret = opal_dss_unpack_buffer(buffer, dest, num_vals, DSS_TYPE_PID_T))) {
        }
    } else {
        /* slow path - types are different sizes */
        UNPACK_SIZE_MISMATCH(pid_t, remote_type, ret);
    }

    return ret;
}


/* UNPACK FUNCTIONS FOR NON-GENERIC SYSTEM TYPES */

/*
 * NULL
 */
int opal_dss_unpack_null(opal_buffer_t *buffer, void *dest,
                         int32_t *num_vals, opal_data_type_t type)
{
    OPAL_OUTPUT( ( opal_dss_verbose, "opal_dss_unpack_null * %d\n", (int)*num_vals ) );
    /* check to see if there's enough data in buffer */
    if (opal_dss_too_small(buffer, *num_vals)) {
        return OPAL_ERR_UNPACK_READ_PAST_END_OF_BUFFER;
    }

    /* unpack the data */
    memcpy(dest, buffer->unpack_ptr, *num_vals);

    /* update buffer pointer */
    buffer->unpack_ptr += *num_vals;

    return OPAL_SUCCESS;
}

/*
 * BYTE, CHAR, INT8
 */
int opal_dss_unpack_byte(opal_buffer_t *buffer, void *dest,
                         int32_t *num_vals, opal_data_type_t type)
{
    OPAL_OUTPUT( ( opal_dss_verbose, "opal_dss_unpack_byte * %d\n", (int)*num_vals ) );
    /* check to see if there's enough data in buffer */
    if (opal_dss_too_small(buffer, *num_vals)) {
        return OPAL_ERR_UNPACK_READ_PAST_END_OF_BUFFER;
    }

    /* unpack the data */
    memcpy(dest, buffer->unpack_ptr, *num_vals);

    /* update buffer pointer */
    buffer->unpack_ptr += *num_vals;

    return OPAL_SUCCESS;
}

int opal_dss_unpack_int16(opal_buffer_t *buffer, void *dest,
                          int32_t *num_vals, opal_data_type_t type)
{
    int32_t i;
    uint16_t tmp, *desttmp = (uint16_t*) dest;

   OPAL_OUTPUT( ( opal_dss_verbose, "opal_dss_unpack_int16 * %d\n", (int)*num_vals ) );
    /* check to see if there's enough data in buffer */
    if (opal_dss_too_small(buffer, (*num_vals)*sizeof(tmp))) {
        return OPAL_ERR_UNPACK_READ_PAST_END_OF_BUFFER;
    }

    /* unpack the data */
    for (i = 0; i < (*num_vals); ++i) {
        memcpy( &(tmp), buffer->unpack_ptr, sizeof(tmp) );
        tmp = ntohs(tmp);
        memcpy(&desttmp[i], &tmp, sizeof(tmp));
        buffer->unpack_ptr += sizeof(tmp);
    }

    return OPAL_SUCCESS;
}

int opal_dss_unpack_int32(opal_buffer_t *buffer, void *dest,
                          int32_t *num_vals, opal_data_type_t type)
{
    int32_t i;
    uint32_t tmp, *desttmp = (uint32_t*) dest;

   OPAL_OUTPUT( ( opal_dss_verbose, "opal_dss_unpack_int32 * %d\n", (int)*num_vals ) );
    /* check to see if there's enough data in buffer */
    if (opal_dss_too_small(buffer, (*num_vals)*sizeof(tmp))) {
        return OPAL_ERR_UNPACK_READ_PAST_END_OF_BUFFER;
    }

    /* unpack the data */
    for (i = 0; i < (*num_vals); ++i) {
        memcpy( &(tmp), buffer->unpack_ptr, sizeof(tmp) );
        tmp = ntohl(tmp);
        memcpy(&desttmp[i], &tmp, sizeof(tmp));
        buffer->unpack_ptr += sizeof(tmp);
    }

    return OPAL_SUCCESS;
}

int opal_dss_unpack_int64(opal_buffer_t *buffer, void *dest,
                          int32_t *num_vals, opal_data_type_t type)
{
    int32_t i;
    uint64_t tmp, *desttmp = (uint64_t*) dest;

   OPAL_OUTPUT( ( opal_dss_verbose, "opal_dss_unpack_int64 * %d\n", (int)*num_vals ) );
    /* check to see if there's enough data in buffer */
    if (opal_dss_too_small(buffer, (*num_vals)*sizeof(tmp))) {
        return OPAL_ERR_UNPACK_READ_PAST_END_OF_BUFFER;
    }

    /* unpack the data */
    for (i = 0; i < (*num_vals); ++i) {
        memcpy( &(tmp), buffer->unpack_ptr, sizeof(tmp) );
        tmp = ntoh64(tmp);
        memcpy(&desttmp[i], &tmp, sizeof(tmp));
        buffer->unpack_ptr += sizeof(tmp);
    }

    return OPAL_SUCCESS;
}

int opal_dss_unpack_string(opal_buffer_t *buffer, void *dest,
                           int32_t *num_vals, opal_data_type_t type)
{
    int ret;
    int32_t i, len, n=1;
    char **sdest = (char**) dest;

    for (i = 0; i < (*num_vals); ++i) {
        if (OPAL_SUCCESS != (ret = opal_dss_unpack_int32(buffer, &len, &n, OPAL_INT32))) {
            return ret;
        }
        if (0 ==  len) {   /* zero-length string - unpack the NULL */
            sdest[i] = NULL;
        } else {
        sdest[i] = (char*)malloc(len);
            if (NULL == sdest[i]) {
                return OPAL_ERR_OUT_OF_RESOURCE;
            }
            if (OPAL_SUCCESS != (ret = opal_dss_unpack_byte(buffer, sdest[i], &len, OPAL_BYTE))) {
                return ret;
            }
        }
    }

    return OPAL_SUCCESS;
}

int opal_dss_unpack_float(opal_buffer_t *buffer, void *dest,
                          int32_t *num_vals, opal_data_type_t type)
{
    int32_t i, n;
    float *desttmp = (float*) dest, tmp;
    int ret;
    char *convert;

   OPAL_OUTPUT( ( opal_dss_verbose, "opal_dss_unpack_float * %d\n", (int)*num_vals ) );
    /* check to see if there's enough data in buffer */
    if (opal_dss_too_small(buffer, (*num_vals)*sizeof(float))) {
        return OPAL_ERR_UNPACK_READ_PAST_END_OF_BUFFER;
    }

    /* unpack the data */
    for (i = 0; i < (*num_vals); ++i) {
        n=1;
        if (OPAL_SUCCESS != (ret = opal_dss_unpack_string(buffer, &convert, &n, OPAL_STRING))) {
            return ret;
        }
        if (NULL == convert) {
            return OPAL_ERR_UNPACK_FAILURE;
        }
        tmp = strtof(convert, NULL);
        memcpy(&desttmp[i], &tmp, sizeof(tmp));
        free(convert);
        convert = NULL;
    }
    return OPAL_SUCCESS;
}

int opal_dss_unpack_double(opal_buffer_t *buffer, void *dest,
                           int32_t *num_vals, opal_data_type_t type)
{
    int32_t i, n;
    double *desttmp = (double*) dest, tmp;
    int ret;
    char *convert;

   OPAL_OUTPUT( ( opal_dss_verbose, "opal_dss_unpack_double * %d\n", (int)*num_vals ) );
    /* check to see if there's enough data in buffer */
    if (opal_dss_too_small(buffer, (*num_vals)*sizeof(double))) {
        return OPAL_ERR_UNPACK_READ_PAST_END_OF_BUFFER;
    }

    /* unpack the data */
    for (i = 0; i < (*num_vals); ++i) {
        n=1;
        if (OPAL_SUCCESS != (ret = opal_dss_unpack_string(buffer, &convert, &n, OPAL_STRING))) {
            return ret;
        }
        if (NULL == convert) {
            return OPAL_ERR_UNPACK_FAILURE;
        }
        tmp = strtod(convert, NULL);
        memcpy(&desttmp[i], &tmp, sizeof(tmp));
        free(convert);
        convert = NULL;
    }
    return OPAL_SUCCESS;
}

int opal_dss_unpack_timeval(opal_buffer_t *buffer, void *dest,
                            int32_t *num_vals, opal_data_type_t type)
{
    int32_t i, n;
    int64_t tmp[2];
    struct timeval *desttmp = (struct timeval *) dest, tt;
    int ret;

   OPAL_OUTPUT( ( opal_dss_verbose, "opal_dss_unpack_timeval * %d\n", (int)*num_vals ) );
    /* check to see if there's enough data in buffer */
    if (opal_dss_too_small(buffer, (*num_vals)*sizeof(struct timeval))) {
        return OPAL_ERR_UNPACK_READ_PAST_END_OF_BUFFER;
    }

    /* unpack the data */
    for (i = 0; i < (*num_vals); ++i) {
        n=2;
        if (OPAL_SUCCESS != (ret = opal_dss_unpack_int64(buffer, tmp, &n, OPAL_INT64))) {
            return ret;
        }
        tt.tv_sec = tmp[0];
        tt.tv_usec = tmp[1];
        memcpy(&desttmp[i], &tt, sizeof(tt));
    }
    return OPAL_SUCCESS;
}

int opal_dss_unpack_time(opal_buffer_t *buffer, void *dest,
                         int32_t *num_vals, opal_data_type_t type)
{
    int32_t i, n;
    time_t *desttmp = (time_t *) dest, tmp;
    int ret;
    uint64_t ui64;

    /* time_t is a system-dependent size, so cast it
     * to uint64_t as a generic safe size
     */

   OPAL_OUTPUT( ( opal_dss_verbose, "opal_dss_unpack_time * %d\n", (int)*num_vals ) );
    /* check to see if there's enough data in buffer */
   if (opal_dss_too_small(buffer, (*num_vals)*(sizeof(uint64_t)))) {
        return OPAL_ERR_UNPACK_READ_PAST_END_OF_BUFFER;
    }

    /* unpack the data */
    for (i = 0; i < (*num_vals); ++i) {
        n=1;
        if (OPAL_SUCCESS != (ret = opal_dss_unpack_int64(buffer, &ui64, &n, OPAL_UINT64))) {
            return ret;
        }
        tmp = (time_t)ui64;
        memcpy(&desttmp[i], &tmp, sizeof(tmp));
    }
    return OPAL_SUCCESS;
}


/* UNPACK FUNCTIONS FOR GENERIC OPAL TYPES */

/*
 * OPAL_DATA_TYPE
 */
int opal_dss_unpack_data_type(opal_buffer_t *buffer, void *dest, int32_t *num_vals,
                             opal_data_type_t type)
{
     /* turn around and unpack the real type */
    return opal_dss_unpack_buffer(buffer, dest, num_vals, OPAL_DATA_TYPE_T);
}

/*
 * OPAL_BYTE_OBJECT
 */
int opal_dss_unpack_byte_object(opal_buffer_t *buffer, void *dest, int32_t *num,
                             opal_data_type_t type)
{
    int ret;
    int32_t i, n, m=1;
    opal_byte_object_t **dbyteptr;

    dbyteptr = (opal_byte_object_t**)dest;
    n = *num;
    for(i=0; i<n; i++) {
        /* allocate memory for the byte object itself */
        dbyteptr[i] = (opal_byte_object_t*)malloc(sizeof(opal_byte_object_t));
        if (NULL == dbyteptr[i]) {
            return OPAL_ERR_OUT_OF_RESOURCE;
        }

        /* unpack object size in bytes */
        if (OPAL_SUCCESS != (ret = opal_dss_unpack_int32(buffer, &(dbyteptr[i]->size), &m, OPAL_INT32))) {
            return ret;
        }
        if (0 < dbyteptr[i]->size) {
            dbyteptr[i]->bytes = (uint8_t*)malloc(dbyteptr[i]->size);
            if (NULL == dbyteptr[i]->bytes) {
                return OPAL_ERR_OUT_OF_RESOURCE;
            }
            if (OPAL_SUCCESS != (ret = opal_dss_unpack_byte(buffer, (dbyteptr[i]->bytes),
                                            &(dbyteptr[i]->size), OPAL_BYTE))) {
                return ret;
            }
        } else {
            /* be sure to init the bytes pointer to NULL! */
            dbyteptr[i]->bytes = NULL;
        }
    }

    return OPAL_SUCCESS;
}

/*
 * OPAL_PSTAT
 */
int opal_dss_unpack_pstat(opal_buffer_t *buffer, void *dest,
                          int32_t *num_vals, opal_data_type_t type)
{
    opal_pstats_t **ptr;
    int32_t i, n, m;
    int ret;
    char *cptr;

    ptr = (opal_pstats_t **) dest;
    n = *num_vals;

    for (i = 0; i < n; ++i) {
        /* allocate the new object */
        ptr[i] = OBJ_NEW(opal_pstats_t);
        if (NULL == ptr[i]) {
            return OPAL_ERR_OUT_OF_RESOURCE;
        }
        m=1;
        if (OPAL_SUCCESS != (ret = opal_dss_unpack_buffer(buffer, &cptr, &m, OPAL_STRING))) {
            OPAL_ERROR_LOG(ret);
            return ret;
        }
        memmove(ptr[i]->node, cptr, strlen(cptr));
        free(cptr);
        m=1;
        if (OPAL_SUCCESS != (ret = opal_dss_unpack_buffer(buffer, &ptr[i]->rank, &m, OPAL_INT32))) {
            OPAL_ERROR_LOG(ret);
            return ret;
        }
        m=1;
        if (OPAL_SUCCESS != (ret = opal_dss_unpack_buffer(buffer, &ptr[i]->pid, &m, OPAL_PID))) {
            OPAL_ERROR_LOG(ret);
            return ret;
        }
        m=1;
        if (OPAL_SUCCESS != (ret = opal_dss_unpack_buffer(buffer, &cptr, &m, OPAL_STRING))) {
            OPAL_ERROR_LOG(ret);
            return ret;
        }
        memmove(ptr[i]->cmd, cptr, strlen(cptr));
        free(cptr);
        m=1;
        if (OPAL_SUCCESS != (ret = opal_dss_unpack_buffer(buffer, &ptr[i]->state[0], &m, OPAL_BYTE))) {
            OPAL_ERROR_LOG(ret);
            return ret;
        }
        m=1;
        if (OPAL_SUCCESS != (ret = opal_dss_unpack_buffer(buffer, &ptr[i]->time, &m, OPAL_TIMEVAL))) {
            OPAL_ERROR_LOG(ret);
            return ret;
        }
        m=1;
        if (OPAL_SUCCESS != (ret = opal_dss_unpack_buffer(buffer, &ptr[i]->priority, &m, OPAL_INT32))) {
            OPAL_ERROR_LOG(ret);
            return ret;
        }
        m=1;
        if (OPAL_SUCCESS != (ret = opal_dss_unpack_buffer(buffer, &ptr[i]->num_threads, &m, OPAL_INT16))) {
            OPAL_ERROR_LOG(ret);
            return ret;
        }
        m=1;
        if (OPAL_SUCCESS != (ret = opal_dss_unpack_float(buffer, &ptr[i]->pss, &m, OPAL_FLOAT))) {
            OPAL_ERROR_LOG(ret);
            return ret;
        }
        m=1;
        if (OPAL_SUCCESS != (ret = opal_dss_unpack_float(buffer, &ptr[i]->vsize, &m, OPAL_FLOAT))) {
            OPAL_ERROR_LOG(ret);
            return ret;
        }
        m=1;
        if (OPAL_SUCCESS != (ret = opal_dss_unpack_float(buffer, &ptr[i]->rss, &m, OPAL_FLOAT))) {
            OPAL_ERROR_LOG(ret);
            return ret;
        }
        m=1;
        if (OPAL_SUCCESS != (ret = opal_dss_unpack_float(buffer, &ptr[i]->peak_vsize, &m, OPAL_FLOAT))) {
            OPAL_ERROR_LOG(ret);
            return ret;
        }
        m=1;
        if (OPAL_SUCCESS != (ret = opal_dss_unpack_buffer(buffer, &ptr[i]->processor, &m, OPAL_INT16))) {
            OPAL_ERROR_LOG(ret);
            return ret;
        }
        m=1;
        if (OPAL_SUCCESS != (ret = opal_dss_unpack_buffer(buffer, &ptr[i]->sample_time, &m, OPAL_TIMEVAL))) {
            OPAL_ERROR_LOG(ret);
            return ret;
        }
    }

    return OPAL_SUCCESS;
}

static int unpack_disk_stats(opal_buffer_t *buffer, opal_node_stats_t *ns)
{
    int32_t i, m, n;
    int ret;
    opal_diskstats_t *dk;
    uint64_t i64;

    /* unpack the number of disk stat objects */
    m=1;
    if (OPAL_SUCCESS != (ret = opal_dss_unpack_buffer(buffer, &n, &m, OPAL_INT32))) {
        OPAL_ERROR_LOG(ret);
        return ret;
    }
    /* unpack them */
    for (i=0; i < n; i++) {
        dk = OBJ_NEW(opal_diskstats_t);
        assert(dk);
        m=1;
        if (OPAL_SUCCESS != (ret = opal_dss_unpack_buffer(buffer, &dk->disk, &m, OPAL_STRING))) {
            OPAL_ERROR_LOG(ret);
            OBJ_RELEASE(dk);
            return ret;
        }
        m=1;
        if (OPAL_SUCCESS != (ret = opal_dss_unpack_buffer(buffer, &i64, &m, OPAL_UINT64))) {
            OPAL_ERROR_LOG(ret);
            OBJ_RELEASE(dk);
            return ret;
        }
        dk->num_reads_completed = i64;
        m=1;
        if (OPAL_SUCCESS != (ret = opal_dss_unpack_buffer(buffer, &i64, &m, OPAL_UINT64))) {
            OPAL_ERROR_LOG(ret);
            OBJ_RELEASE(dk);
            return ret;
        }
        dk->num_reads_merged = i64;
        m=1;
        if (OPAL_SUCCESS != (ret = opal_dss_unpack_buffer(buffer, &i64, &m, OPAL_UINT64))) {
            OPAL_ERROR_LOG(ret);
            OBJ_RELEASE(dk);
            return ret;
        }
        dk->num_sectors_read = i64;
        m=1;
        if (OPAL_SUCCESS != (ret = opal_dss_unpack_buffer(buffer, &i64, &m, OPAL_UINT64))) {
            OPAL_ERROR_LOG(ret);
            OBJ_RELEASE(dk);
            return ret;
        }
        dk->milliseconds_reading = i64;
        m=1;
        if (OPAL_SUCCESS != (ret = opal_dss_unpack_buffer(buffer, &i64, &m, OPAL_UINT64))) {
            OPAL_ERROR_LOG(ret);
            OBJ_RELEASE(dk);
            return ret;
        }
        dk->num_writes_completed = i64;
        m=1;
        if (OPAL_SUCCESS != (ret = opal_dss_unpack_buffer(buffer, &i64, &m, OPAL_UINT64))) {
            OPAL_ERROR_LOG(ret);
            OBJ_RELEASE(dk);
            return ret;
        }
        dk->num_writes_merged = i64;
        m=1;
        if (OPAL_SUCCESS != (ret = opal_dss_unpack_buffer(buffer, &i64, &m, OPAL_UINT64))) {
            OPAL_ERROR_LOG(ret);
            OBJ_RELEASE(dk);
            return ret;
        }
        dk->num_sectors_written = i64;
        m=1;
        if (OPAL_SUCCESS != (ret = opal_dss_unpack_buffer(buffer, &i64, &m, OPAL_UINT64))) {
            OPAL_ERROR_LOG(ret);
            OBJ_RELEASE(dk);
            return ret;
        }
        dk->milliseconds_writing = i64;
        m=1;
        if (OPAL_SUCCESS != (ret = opal_dss_unpack_buffer(buffer, &i64, &m, OPAL_UINT64))) {
            OPAL_ERROR_LOG(ret);
            OBJ_RELEASE(dk);
            return ret;
        }
        dk->num_ios_in_progress = i64;
        m=1;
        if (OPAL_SUCCESS != (ret = opal_dss_unpack_buffer(buffer, &i64, &m, OPAL_UINT64))) {
            OPAL_ERROR_LOG(ret);
            OBJ_RELEASE(dk);
            return ret;
        }
        dk->milliseconds_io = i64;
        m=1;
        if (OPAL_SUCCESS != (ret = opal_dss_unpack_buffer(buffer, &i64, &m, OPAL_UINT64))) {
            OPAL_ERROR_LOG(ret);
            OBJ_RELEASE(dk);
            return ret;
        }
        dk->weighted_milliseconds_io = i64;
        opal_list_append(&ns->diskstats, &dk->super);
    }
    return OPAL_SUCCESS;
}

static int unpack_net_stats(opal_buffer_t *buffer, opal_node_stats_t *ns)
{
    int32_t i, m, n;
    int ret;
    opal_netstats_t *net;
    uint64_t i64;

    /* unpack the number of net stat objects */
    m=1;
    if (OPAL_SUCCESS != (ret = opal_dss_unpack_buffer(buffer, &n, &m, OPAL_INT32))) {
        OPAL_ERROR_LOG(ret);
        return ret;
    }
    /* unpack them */
    for (i=0; i < n; i++) {
        net = OBJ_NEW(opal_netstats_t);
        assert(net);
        m=1;
        if (OPAL_SUCCESS != (ret = opal_dss_unpack_buffer(buffer, &net->net_interface, &m, OPAL_STRING))) {
            OPAL_ERROR_LOG(ret);
            OBJ_RELEASE(net);
            return ret;
        }
        m=1;
        if (OPAL_SUCCESS != (ret = opal_dss_unpack_buffer(buffer, &i64, &m, OPAL_UINT64))) {
            OPAL_ERROR_LOG(ret);
            OBJ_RELEASE(net);
            return ret;
        }
        net->num_bytes_recvd = i64;
        m=1;
        if (OPAL_SUCCESS != (ret = opal_dss_unpack_buffer(buffer, &i64, &m, OPAL_UINT64))) {
            OPAL_ERROR_LOG(ret);
            OBJ_RELEASE(net);
            return ret;
        }
        net->num_packets_recvd = i64;
        m=1;
        if (OPAL_SUCCESS != (ret = opal_dss_unpack_buffer(buffer, &i64, &m, OPAL_UINT64))) {
            OPAL_ERROR_LOG(ret);
            OBJ_RELEASE(net);
            return ret;
        }
        net->num_recv_errs = i64;
        m=1;
        if (OPAL_SUCCESS != (ret = opal_dss_unpack_buffer(buffer, &i64, &m, OPAL_UINT64))) {
            OPAL_ERROR_LOG(ret);
            OBJ_RELEASE(net);
            return ret;
        }
        net->num_bytes_sent = i64;
        m=1;
        if (OPAL_SUCCESS != (ret = opal_dss_unpack_buffer(buffer, &i64, &m, OPAL_UINT64))) {
            OPAL_ERROR_LOG(ret);
            OBJ_RELEASE(net);
            return ret;
        }
        net->num_packets_sent = i64;
        m=1;
        if (OPAL_SUCCESS != (ret = opal_dss_unpack_buffer(buffer, &i64, &m, OPAL_UINT64))) {
            OPAL_ERROR_LOG(ret);
            OBJ_RELEASE(net);
            return ret;
        }
        net->num_send_errs = i64;
        opal_list_append(&ns->netstats, &net->super);
    }
    return OPAL_SUCCESS;
}

/*
 * OPAL_NODE_STAT
 */
int opal_dss_unpack_node_stat(opal_buffer_t *buffer, void *dest,
                              int32_t *num_vals, opal_data_type_t type)
{
    opal_node_stats_t **ptr;
    int32_t i, n, m;
    int ret;

    ptr = (opal_node_stats_t **) dest;
    n = *num_vals;

    for (i = 0; i < n; ++i) {
        /* allocate the new object */
        ptr[i] = OBJ_NEW(opal_node_stats_t);
        if (NULL == ptr[i]) {
            return OPAL_ERR_OUT_OF_RESOURCE;
        }
        m=1;
        if (OPAL_SUCCESS != (ret = opal_dss_unpack_float(buffer, &ptr[i]->la, &m, OPAL_FLOAT))) {
            OPAL_ERROR_LOG(ret);
            return ret;
        }
        m=1;
        if (OPAL_SUCCESS != (ret = opal_dss_unpack_float(buffer, &ptr[i]->la5, &m, OPAL_FLOAT))) {
            OPAL_ERROR_LOG(ret);
            return ret;
        }
        m=1;
        if (OPAL_SUCCESS != (ret = opal_dss_unpack_float(buffer, &ptr[i]->la15, &m, OPAL_FLOAT))) {
            OPAL_ERROR_LOG(ret);
            return ret;
        }
        m=1;
        if (OPAL_SUCCESS != (ret = opal_dss_unpack_float(buffer, &ptr[i]->total_mem, &m, OPAL_FLOAT))) {
            OPAL_ERROR_LOG(ret);
            return ret;
        }
        m=1;
        if (OPAL_SUCCESS != (ret = opal_dss_unpack_float(buffer, &ptr[i]->free_mem, &m, OPAL_FLOAT))) {
            OPAL_ERROR_LOG(ret);
            return ret;
        }
        m=1;
        if (OPAL_SUCCESS != (ret = opal_dss_unpack_float(buffer, &ptr[i]->buffers, &m, OPAL_FLOAT))) {
            OPAL_ERROR_LOG(ret);
            return ret;
        }
        m=1;
        if (OPAL_SUCCESS != (ret = opal_dss_unpack_float(buffer, &ptr[i]->cached, &m, OPAL_FLOAT))) {
            OPAL_ERROR_LOG(ret);
            return ret;
        }
        m=1;
        if (OPAL_SUCCESS != (ret = opal_dss_unpack_float(buffer, &ptr[i]->swap_cached, &m, OPAL_FLOAT))) {
            OPAL_ERROR_LOG(ret);
            return ret;
        }
        m=1;
        if (OPAL_SUCCESS != (ret = opal_dss_unpack_float(buffer, &ptr[i]->swap_total, &m, OPAL_FLOAT))) {
            OPAL_ERROR_LOG(ret);
            return ret;
        }
        m=1;
        if (OPAL_SUCCESS != (ret = opal_dss_unpack_float(buffer, &ptr[i]->swap_free, &m, OPAL_FLOAT))) {
            OPAL_ERROR_LOG(ret);
            return ret;
        }
        m=1;
        if (OPAL_SUCCESS != (ret = opal_dss_unpack_float(buffer, &ptr[i]->mapped, &m, OPAL_FLOAT))) {
            OPAL_ERROR_LOG(ret);
            return ret;
        }
        m=1;
        if (OPAL_SUCCESS != (ret = opal_dss_unpack_buffer(buffer, &ptr[i]->sample_time, &m, OPAL_TIMEVAL))) {
            OPAL_ERROR_LOG(ret);
            return ret;
        }
        /* unpack the disk stat objects */
        if (OPAL_SUCCESS != (ret = unpack_disk_stats(buffer, ptr[i]))) {
            OPAL_ERROR_LOG(ret);
            return ret;
        }
        /* unpack the net stat objects */
        if (OPAL_SUCCESS != (ret = unpack_net_stats(buffer, ptr[i]))) {
            OPAL_ERROR_LOG(ret);
            return ret;
        }
        OBJ_RELEASE(ptr[i]);
    }

    return OPAL_SUCCESS;
}

/*
 * OPAL_VALUE
 */
int opal_dss_unpack_value(opal_buffer_t *buffer, void *dest,
                          int32_t *num_vals, opal_data_type_t type)
{
    opal_value_t **ptr;
    int32_t i, n, m;
    int ret;

    ptr = (opal_value_t **) dest;
    n = *num_vals;

    for (i = 0; i < n; ++i) {
        /* allocate the new object */
        ptr[i] = OBJ_NEW(opal_value_t);
        if (NULL == ptr[i]) {
            return OPAL_ERR_OUT_OF_RESOURCE;
        }
        /* unpack the key and type */
        m=1;
        if (OPAL_SUCCESS != (ret = opal_dss_unpack_string(buffer, &ptr[i]->key, &m, OPAL_STRING))) {
            return ret;
        }
        m=1;
        if (OPAL_SUCCESS != (ret = opal_dss_unpack_data_type(buffer, &ptr[i]->type, &m, OPAL_DATA_TYPE))) {
            return ret;
        }
        /* now unpack the right field */
        m=1;
        switch (ptr[i]->type) {
        case OPAL_BOOL:
            if (OPAL_SUCCESS != (ret = opal_dss_unpack_buffer(buffer, &ptr[i]->data.flag, &m, OPAL_BOOL))) {
                return ret;
            }
            break;
        case OPAL_BYTE:
            if (OPAL_SUCCESS != (ret = opal_dss_unpack_buffer(buffer, &ptr[i]->data.byte, &m, OPAL_BYTE))) {
                return ret;
            }
            break;
        case OPAL_STRING:
            if (OPAL_SUCCESS != (ret = opal_dss_unpack_buffer(buffer, &ptr[i]->data.string, &m, OPAL_STRING))) {
                return ret;
            }
            break;
        case OPAL_SIZE:
            if (OPAL_SUCCESS != (ret = opal_dss_unpack_buffer(buffer, &ptr[i]->data.size, &m, OPAL_SIZE))) {
                return ret;
            }
            break;
        case OPAL_PID:
            if (OPAL_SUCCESS != (ret = opal_dss_unpack_buffer(buffer, &ptr[i]->data.pid, &m, OPAL_PID))) {
                return ret;
            }
            break;
        case OPAL_INT:
            if (OPAL_SUCCESS != (ret = opal_dss_unpack_buffer(buffer, &ptr[i]->data.integer, &m, OPAL_INT))) {
                return ret;
            }
            break;
        case OPAL_INT8:
            if (OPAL_SUCCESS != (ret = opal_dss_unpack_buffer(buffer, &ptr[i]->data.int8, &m, OPAL_INT8))) {
                return ret;
            }
            break;
        case OPAL_INT16:
            if (OPAL_SUCCESS != (ret = opal_dss_unpack_buffer(buffer, &ptr[i]->data.int16, &m, OPAL_INT16))) {
                return ret;
            }
            break;
        case OPAL_INT32:
            if (OPAL_SUCCESS != (ret = opal_dss_unpack_buffer(buffer, &ptr[i]->data.int32, &m, OPAL_INT32))) {
                return ret;
            }
            break;
        case OPAL_INT64:
            if (OPAL_SUCCESS != (ret = opal_dss_unpack_buffer(buffer, &ptr[i]->data.int64, &m, OPAL_INT64))) {
                return ret;
            }
            break;
        case OPAL_UINT:
            if (OPAL_SUCCESS != (ret = opal_dss_unpack_buffer(buffer, &ptr[i]->data.uint, &m, OPAL_UINT))) {
                return ret;
            }
            break;
        case OPAL_UINT8:
            if (OPAL_SUCCESS != (ret = opal_dss_unpack_buffer(buffer, &ptr[i]->data.uint8, &m, OPAL_UINT8))) {
                return ret;
            }
            break;
        case OPAL_UINT16:
            if (OPAL_SUCCESS != (ret = opal_dss_unpack_buffer(buffer, &ptr[i]->data.uint16, &m, OPAL_UINT16))) {
                return ret;
            }
            break;
        case OPAL_UINT32:
            if (OPAL_SUCCESS != (ret = opal_dss_unpack_buffer(buffer, &ptr[i]->data.uint32, &m, OPAL_UINT32))) {
                return ret;
            }
            break;
        case OPAL_UINT64:
            if (OPAL_SUCCESS != (ret = opal_dss_unpack_buffer(buffer, &ptr[i]->data.uint64, &m, OPAL_UINT64))) {
                return ret;
            }
            break;
        case OPAL_BYTE_OBJECT:
            /* cannot use byte object unpack as it allocates memory, so unpack object size in bytes */
            if (OPAL_SUCCESS != (ret = opal_dss_unpack_int32(buffer, &(ptr[i]->data.bo.size), &m, OPAL_INT32))) {
                return ret;
            }
            if (0 < ptr[i]->data.bo.size) {
                ptr[i]->data.bo.bytes = (uint8_t*)malloc(ptr[i]->data.bo.size);
                if (NULL == ptr[i]->data.bo.bytes) {
                    return OPAL_ERR_OUT_OF_RESOURCE;
                }
                if (OPAL_SUCCESS != (ret = opal_dss_unpack_byte(buffer, ptr[i]->data.bo.bytes,
                                                                &(ptr[i]->data.bo.size), OPAL_BYTE))) {
                    return ret;
                }
            } else {
                ptr[i]->data.bo.bytes = NULL;
            }
            break;
        case OPAL_FLOAT:
            if (OPAL_SUCCESS != (ret = opal_dss_unpack_buffer(buffer, &ptr[i]->data.fval, &m, OPAL_FLOAT))) {
                return ret;
            }
            break;
        case OPAL_DOUBLE:
            if (OPAL_SUCCESS != (ret = opal_dss_unpack_buffer(buffer, &ptr[i]->data.dval, &m, OPAL_DOUBLE))) {
                return ret;
            }
            break;
        case OPAL_TIMEVAL:
            if (OPAL_SUCCESS != (ret = opal_dss_unpack_buffer(buffer, &ptr[i]->data.tv, &m, OPAL_TIMEVAL))) {
                return ret;
            }
            break;
        case OPAL_PTR:
            /* just ignore these values */
            break;
        case OPAL_NAME:
            if (OPAL_SUCCESS != (ret = opal_dss_unpack_buffer(buffer, &ptr[i]->data.name, &m, OPAL_NAME))) {
                return ret;
            }
            break;
        case OPAL_STATUS:
            if (OPAL_SUCCESS != (ret = opal_dss_unpack_buffer(buffer, &ptr[i]->data.status, &m, OPAL_INT))) {
                return ret;
            }
            break;
        case OPAL_ENVAR:
            if (OPAL_SUCCESS != (ret = opal_dss_unpack_buffer(buffer, &ptr[i]->data.envar, &m, OPAL_ENVAR))) {
                return ret;
            }
            break;
        default:
            opal_output(0, "UNPACK-OPAL-VALUE: UNSUPPORTED TYPE %d FOR KEY %s", (int)ptr[i]->type, ptr[i]->key);
            return OPAL_ERROR;
        }
    }

    return OPAL_SUCCESS;
}

/*
 * OPAL_BUFFER
 */
int opal_dss_unpack_buffer_contents(opal_buffer_t *buffer, void *dest,
                                    int32_t *num_vals, opal_data_type_t type)
{
    opal_buffer_t **ptr;
    int32_t i, n, m;
    int ret;
    size_t nbytes;

    ptr = (opal_buffer_t **) dest;
    n = *num_vals;

    for (i = 0; i < n; ++i) {
        /* allocate the new object */
        ptr[i] = OBJ_NEW(opal_buffer_t);
        if (NULL == ptr[i]) {
            return OPAL_ERR_OUT_OF_RESOURCE;
        }
        /* unpack the number of bytes */
        m=1;
        if (OPAL_SUCCESS != (ret = opal_dss_unpack_sizet(buffer, &nbytes, &m, OPAL_SIZE))) {
            return ret;
        }
        m = nbytes;
        /* setup the buffer's data region */
        if (0 < nbytes) {
            ptr[i]->base_ptr = (char*)malloc(nbytes);
            /* unpack the bytes */
            if (OPAL_SUCCESS != (ret = opal_dss_unpack_byte(buffer, ptr[i]->base_ptr, &m, OPAL_BYTE))) {
                return ret;
            }
        }
        ptr[i]->pack_ptr = ptr[i]->base_ptr + m;
        ptr[i]->unpack_ptr = ptr[i]->base_ptr;
        ptr[i]->bytes_allocated = nbytes;
        ptr[i]->bytes_used = m;
    }
    return OPAL_SUCCESS;
}

/*
 * NAME
 */
int opal_dss_unpack_name(opal_buffer_t *buffer, void *dest,
                        int32_t *num_vals, opal_data_type_t type)
{
    int rc;
    int32_t i, num;
    opal_process_name_t* proc;
    opal_jobid_t *jobid;
    opal_vpid_t *vpid;

    num = *num_vals;

    /* allocate space for all the jobids in a contiguous array */
    jobid = (opal_jobid_t*)malloc(num * sizeof(opal_jobid_t));
    if (NULL == jobid) {
        OPAL_ERROR_LOG(OPAL_ERR_OUT_OF_RESOURCE);
        *num_vals = 0;
        return OPAL_ERR_OUT_OF_RESOURCE;
    }
    /* now unpack them in one shot */
    if (OPAL_SUCCESS != (rc =
                         opal_dss_unpack_jobid(buffer, jobid, num_vals, OPAL_JOBID))) {
        OPAL_ERROR_LOG(rc);
        *num_vals = 0;
        free(jobid);
        return rc;
    }

    /* collect all the vpids in a contiguous array */
    vpid = (opal_vpid_t*)malloc(num * sizeof(opal_vpid_t));
    if (NULL == vpid) {
        OPAL_ERROR_LOG(OPAL_ERR_OUT_OF_RESOURCE);
        *num_vals = 0;
        free(jobid);
        return OPAL_ERR_OUT_OF_RESOURCE;
    }
    /* now unpack them in one shot */
    if (OPAL_SUCCESS != (rc =
                         opal_dss_unpack_vpid(buffer, vpid, num_vals, OPAL_VPID))) {
        OPAL_ERROR_LOG(rc);
        *num_vals = 0;
        free(vpid);
        free(jobid);
        return rc;
    }

    /* build the names from the jobid/vpid arrays */
    proc = (opal_process_name_t*)dest;
    for (i=0; i < num; i++) {
        proc->jobid = jobid[i];
        proc->vpid = vpid[i];
        proc++;
    }

    /* cleanup */
    free(vpid);
    free(jobid);

    return OPAL_SUCCESS;
}

/*
 * JOBID
 */
int opal_dss_unpack_jobid(opal_buffer_t *buffer, void *dest,
                         int32_t *num_vals, opal_data_type_t type)
{
    int ret;

    /* Turn around and unpack the real type */
    if (OPAL_SUCCESS != (ret = opal_dss_unpack_buffer(buffer, dest, num_vals, OPAL_JOBID_T))) {
        OPAL_ERROR_LOG(ret);
    }

    return ret;
}

/*
 * VPID
 */
int opal_dss_unpack_vpid(opal_buffer_t *buffer, void *dest,
                        int32_t *num_vals, opal_data_type_t type)
{
    int ret;

    /* Turn around and unpack the real type */
    if (OPAL_SUCCESS != (ret = opal_dss_unpack_buffer(buffer, dest, num_vals, OPAL_VPID_T))) {
        OPAL_ERROR_LOG(ret);
    }

    return ret;
}

/*
 * STATUS
 */
int opal_dss_unpack_status(opal_buffer_t *buffer, void *dest,
                           int32_t *num_vals, opal_data_type_t type)
{
    int ret;

    /* Turn around and unpack the real type */
    ret = opal_dss_unpack_buffer(buffer, dest, num_vals, OPAL_INT);
    if (OPAL_SUCCESS != ret) {
        OPAL_ERROR_LOG(ret);
    }

    return ret;
}


int opal_dss_unpack_envar(opal_buffer_t *buffer, void *dest,
                          int32_t *num_vals, opal_data_type_t type)
{
    opal_envar_t *ptr;
    int32_t i, n, m;
    int ret;

    ptr = (opal_envar_t *) dest;
    n = *num_vals;

    for (i = 0; i < n; ++i) {
        m=1;
        if (OPAL_SUCCESS != (ret = opal_dss_unpack_string(buffer, &ptr[i].envar, &m, OPAL_STRING))) {
            OPAL_ERROR_LOG(ret);
            return ret;
        }
        m=1;
        if (OPAL_SUCCESS != (ret = opal_dss_unpack_string(buffer, &ptr[i].value, &m, OPAL_STRING))) {
            OPAL_ERROR_LOG(ret);
            return ret;
        }
        m=1;
        if (OPAL_SUCCESS != (ret = opal_dss_unpack_byte(buffer, &ptr[i].separator, &m, OPAL_BYTE))) {
            OPAL_ERROR_LOG(ret);
            return ret;
        }
    }

    return OPAL_SUCCESS;
}
