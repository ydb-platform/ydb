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

#include "opal/types.h"
#include "opal/util/error.h"
#include "opal/util/output.h"
#include "opal/dss/dss_internal.h"

int opal_dss_pack(opal_buffer_t *buffer,
                  const void *src, int32_t num_vals,
                  opal_data_type_t type)
{
    int rc;

    /* check for error */
    if (NULL == buffer) {
        return OPAL_ERR_BAD_PARAM;
    }

    /* Pack the number of values */
    if (OPAL_DSS_BUFFER_FULLY_DESC == buffer->type) {
        if (OPAL_SUCCESS != (rc = opal_dss_store_data_type(buffer, OPAL_INT32))) {
            return rc;
        }
    }
    if (OPAL_SUCCESS != (rc = opal_dss_pack_int32(buffer, &num_vals, 1, OPAL_INT32))) {
        return rc;
    }

    /* Pack the value(s) */
    return opal_dss_pack_buffer(buffer, src, num_vals, type);
}

int opal_dss_pack_buffer(opal_buffer_t *buffer,
                         const void *src, int32_t num_vals,
                         opal_data_type_t type)
{
    int rc;
    opal_dss_type_info_t *info;

    OPAL_OUTPUT( ( opal_dss_verbose, "opal_dss_pack_buffer( %p, %p, %lu, %d )\n",
                   (void*)buffer, src, (long unsigned int)num_vals, (int)type ) );

    /* Pack the declared data type */
    if (OPAL_DSS_BUFFER_FULLY_DESC == buffer->type) {
        if (OPAL_SUCCESS != (rc = opal_dss_store_data_type(buffer, type))) {
            return rc;
        }
    }

    /* Lookup the pack function for this type and call it */

    if (NULL == (info = (opal_dss_type_info_t*)opal_pointer_array_get_item(&opal_dss_types, type))) {
        return OPAL_ERR_PACK_FAILURE;
    }

    return info->odti_pack_fn(buffer, src, num_vals, type);
}


/* PACK FUNCTIONS FOR GENERIC SYSTEM TYPES */

/*
 * BOOL
 */
int opal_dss_pack_bool(opal_buffer_t *buffer, const void *src,
                       int32_t num_vals, opal_data_type_t type)
{
    int ret;

    /* System types need to always be described so we can properly
       unpack them.  If we aren't fully described, then add the
       description for this type... */
    if (OPAL_DSS_BUFFER_FULLY_DESC != buffer->type) {
        if (OPAL_SUCCESS != (ret = opal_dss_store_data_type(buffer, DSS_TYPE_BOOL))) {
            return ret;
        }
    }

    /* Turn around and pack the real type */
    return opal_dss_pack_buffer(buffer, src, num_vals, DSS_TYPE_BOOL);
}

/*
 * INT
 */
int opal_dss_pack_int(opal_buffer_t *buffer, const void *src,
                      int32_t num_vals, opal_data_type_t type)
{
    int ret;

    /* System types need to always be described so we can properly
       unpack them.  If we aren't fully described, then add the
       description for this type... */
    if (OPAL_DSS_BUFFER_FULLY_DESC != buffer->type) {
        if (OPAL_SUCCESS != (ret = opal_dss_store_data_type(buffer, DSS_TYPE_INT))) {
            return ret;
        }
    }

    /* Turn around and pack the real type */
    return opal_dss_pack_buffer(buffer, src, num_vals, DSS_TYPE_INT);
}

/*
 * SIZE_T
 */
int opal_dss_pack_sizet(opal_buffer_t *buffer, const void *src,
                        int32_t num_vals, opal_data_type_t type)
{
    int ret;

    /* System types need to always be described so we can properly
       unpack them.  If we aren't fully described, then add the
       description for this type... */
    if (OPAL_DSS_BUFFER_FULLY_DESC != buffer->type) {
        if (OPAL_SUCCESS != (ret = opal_dss_store_data_type(buffer, DSS_TYPE_SIZE_T))) {
            return ret;
        }
    }

    return opal_dss_pack_buffer(buffer, src, num_vals, DSS_TYPE_SIZE_T);
}

/*
 * PID_T
 */
int opal_dss_pack_pid(opal_buffer_t *buffer, const void *src,
                      int32_t num_vals, opal_data_type_t type)
{
    int ret;

    /* System types need to always be described so we can properly
       unpack them.  If we aren't fully described, then add the
       description for this type... */
    if (OPAL_DSS_BUFFER_FULLY_DESC != buffer->type) {
        if (OPAL_SUCCESS != (ret = opal_dss_store_data_type(buffer, DSS_TYPE_PID_T))) {
            return ret;
        }
    }

    /* Turn around and pack the real type */
    return opal_dss_pack_buffer(buffer, src, num_vals, DSS_TYPE_PID_T);
}


/* PACK FUNCTIONS FOR NON-GENERIC SYSTEM TYPES */

/*
 * NULL
 */
int opal_dss_pack_null(opal_buffer_t *buffer, const void *src,
                       int32_t num_vals, opal_data_type_t type)
{
    char null=0x00;
    char *dst;

    OPAL_OUTPUT( ( opal_dss_verbose, "opal_dss_pack_null * %d\n", num_vals ) );
    /* check to see if buffer needs extending */
    if (NULL == (dst = opal_dss_buffer_extend(buffer, num_vals))) {
        return OPAL_ERR_OUT_OF_RESOURCE;
    }

    /* store the nulls */
    memset(dst, (int)null, num_vals);

    /* update buffer pointers */
    buffer->pack_ptr += num_vals;
    buffer->bytes_used += num_vals;

    return OPAL_SUCCESS;
}

/*
 * BYTE, CHAR, INT8
 */
int opal_dss_pack_byte(opal_buffer_t *buffer, const void *src,
                       int32_t num_vals, opal_data_type_t type)
{
    char *dst;

    OPAL_OUTPUT( ( opal_dss_verbose, "opal_dss_pack_byte * %d\n", num_vals ) );
    /* check to see if buffer needs extending */
    if (NULL == (dst = opal_dss_buffer_extend(buffer, num_vals))) {
        return OPAL_ERR_OUT_OF_RESOURCE;
    }

    /* store the data */
    memcpy(dst, src, num_vals);

    /* update buffer pointers */
    buffer->pack_ptr += num_vals;
    buffer->bytes_used += num_vals;

    return OPAL_SUCCESS;
}

/*
 * INT16
 */
int opal_dss_pack_int16(opal_buffer_t *buffer, const void *src,
                        int32_t num_vals, opal_data_type_t type)
{
    int32_t i;
    uint16_t tmp, *srctmp = (uint16_t*) src;
    char *dst;

    OPAL_OUTPUT( ( opal_dss_verbose, "opal_dss_pack_int16 * %d\n", num_vals ) );
    /* check to see if buffer needs extending */
    if (NULL == (dst = opal_dss_buffer_extend(buffer, num_vals*sizeof(tmp)))) {
        return OPAL_ERR_OUT_OF_RESOURCE;
    }

    for (i = 0; i < num_vals; ++i) {
        tmp = htons(srctmp[i]);
        memcpy(dst, &tmp, sizeof(tmp));
        dst += sizeof(tmp);
    }
    buffer->pack_ptr += num_vals * sizeof(tmp);
    buffer->bytes_used += num_vals * sizeof(tmp);

    return OPAL_SUCCESS;
}

/*
 * INT32
 */
int opal_dss_pack_int32(opal_buffer_t *buffer, const void *src,
                        int32_t num_vals, opal_data_type_t type)
{
    int32_t i;
    uint32_t tmp, *srctmp = (uint32_t*) src;
    char *dst;

    OPAL_OUTPUT( ( opal_dss_verbose, "opal_dss_pack_int32 * %d\n", num_vals ) );
    /* check to see if buffer needs extending */
    if (NULL == (dst = opal_dss_buffer_extend(buffer, num_vals*sizeof(tmp)))) {
        return OPAL_ERR_OUT_OF_RESOURCE;
    }

    for (i = 0; i < num_vals; ++i) {
        tmp = htonl(srctmp[i]);
        memcpy(dst, &tmp, sizeof(tmp));
        dst += sizeof(tmp);
    }
    buffer->pack_ptr += num_vals * sizeof(tmp);
    buffer->bytes_used += num_vals * sizeof(tmp);

    return OPAL_SUCCESS;
}

/*
 * INT64
 */
int opal_dss_pack_int64(opal_buffer_t *buffer, const void *src,
                        int32_t num_vals, opal_data_type_t type)
{
    int32_t i;
    uint64_t tmp, *srctmp = (uint64_t*) src;
    char *dst;
    size_t bytes_packed = num_vals * sizeof(tmp);

    OPAL_OUTPUT( ( opal_dss_verbose, "opal_dss_pack_int64 * %d\n", num_vals ) );
    /* check to see if buffer needs extending */
    if (NULL == (dst = opal_dss_buffer_extend(buffer, bytes_packed))) {
        return OPAL_ERR_OUT_OF_RESOURCE;
    }

    for (i = 0; i < num_vals; ++i) {
        tmp = hton64(srctmp[i]);
        memcpy(dst, &tmp, sizeof(tmp));
        dst += sizeof(tmp);
    }
    buffer->pack_ptr += bytes_packed;
    buffer->bytes_used += bytes_packed;

    return OPAL_SUCCESS;
}

/*
 * STRING
 */
int opal_dss_pack_string(opal_buffer_t *buffer, const void *src,
                         int32_t num_vals, opal_data_type_t type)
{
    int ret = OPAL_SUCCESS;
    int32_t i, len;
    char **ssrc = (char**) src;

    for (i = 0; i < num_vals; ++i) {
        if (NULL == ssrc[i]) {  /* got zero-length string/NULL pointer - store NULL */
            len = 0;
            if (OPAL_SUCCESS != (ret = opal_dss_pack_int32(buffer, &len, 1, OPAL_INT32))) {
                return ret;
            }
        } else {
            len = (int32_t)strlen(ssrc[i]) + 1;
            if (OPAL_SUCCESS != (ret = opal_dss_pack_int32(buffer, &len, 1, OPAL_INT32))) {
                return ret;
            }
            if (OPAL_SUCCESS != (ret =
                opal_dss_pack_byte(buffer, ssrc[i], len, OPAL_BYTE))) {
                return ret;
            }
        }
    }

    return OPAL_SUCCESS;
}

/* FLOAT */
int opal_dss_pack_float(opal_buffer_t *buffer, const void *src,
                        int32_t num_vals, opal_data_type_t type)
{
    int ret = OPAL_SUCCESS;
    int32_t i;
    float *ssrc = (float*)src;
    char *convert;

    for (i = 0; i < num_vals; ++i) {
        asprintf(&convert, "%f", ssrc[i]);
        if (OPAL_SUCCESS != (ret = opal_dss_pack_string(buffer, &convert, 1, OPAL_STRING))) {
            free(convert);
            return ret;
        }
        free(convert);
    }

    return OPAL_SUCCESS;
}

/* DOUBLE */
int opal_dss_pack_double(opal_buffer_t *buffer, const void *src,
                         int32_t num_vals, opal_data_type_t type)
{
    int ret = OPAL_SUCCESS;
    int32_t i;
    double *ssrc = (double*)src;
    char *convert;

    for (i = 0; i < num_vals; ++i) {
        asprintf(&convert, "%f", ssrc[i]);
        if (OPAL_SUCCESS != (ret = opal_dss_pack_string(buffer, &convert, 1, OPAL_STRING))) {
            free(convert);
            return ret;
        }
        free(convert);
    }

    return OPAL_SUCCESS;
}

/* TIMEVAL */
int opal_dss_pack_timeval(opal_buffer_t *buffer, const void *src,
                          int32_t num_vals, opal_data_type_t type)
{
    int64_t tmp[2];
    int ret = OPAL_SUCCESS;
    int32_t i;
    struct timeval *ssrc = (struct timeval *)src;

    for (i = 0; i < num_vals; ++i) {
        tmp[0] = (int64_t)ssrc[i].tv_sec;
        tmp[1] = (int64_t)ssrc[i].tv_usec;
        if (OPAL_SUCCESS != (ret = opal_dss_pack_int64(buffer, tmp, 2, OPAL_INT64))) {
            return ret;
        }
    }

    return OPAL_SUCCESS;
}

/* TIME */
int opal_dss_pack_time(opal_buffer_t *buffer, const void *src,
                       int32_t num_vals, opal_data_type_t type)
{
    int ret = OPAL_SUCCESS;
    int32_t i;
    time_t *ssrc = (time_t *)src;
    uint64_t ui64;

    /* time_t is a system-dependent size, so cast it
     * to uint64_t as a generic safe size
     */
    for (i = 0; i < num_vals; ++i) {
        ui64 = (uint64_t)ssrc[i];
        if (OPAL_SUCCESS != (ret = opal_dss_pack_int64(buffer, &ui64, 1, OPAL_UINT64))) {
            return ret;
        }
    }

    return OPAL_SUCCESS;
}


/* PACK FUNCTIONS FOR GENERIC OPAL TYPES */

/*
 * OPAL_DATA_TYPE
 */
int opal_dss_pack_data_type(opal_buffer_t *buffer, const void *src, int32_t num_vals,
                            opal_data_type_t type)
{
    int ret;

    /* Turn around and pack the real type */
    if (OPAL_SUCCESS != (ret = opal_dss_pack_buffer(buffer, src, num_vals, OPAL_DATA_TYPE_T))) {
    }

    return ret;
}

/*
 * OPAL_BYTE_OBJECT
 */
int opal_dss_pack_byte_object(opal_buffer_t *buffer, const void *src, int32_t num,
                             opal_data_type_t type)
{
    opal_byte_object_t **sbyteptr;
    int32_t i, n;
    int ret;

    sbyteptr = (opal_byte_object_t **) src;

    for (i = 0; i < num; ++i) {
        n = sbyteptr[i]->size;
        if (OPAL_SUCCESS != (ret = opal_dss_pack_int32(buffer, &n, 1, OPAL_INT32))) {
            return ret;
        }
        if (0 < n) {
            if (OPAL_SUCCESS != (ret =
                opal_dss_pack_byte(buffer, sbyteptr[i]->bytes, n, OPAL_BYTE))) {
                return ret;
            }
        }
    }

    return OPAL_SUCCESS;
}

/*
 * OPAL_PSTAT
 */
int opal_dss_pack_pstat(opal_buffer_t *buffer, const void *src,
                        int32_t num_vals, opal_data_type_t type)
{
    opal_pstats_t **ptr;
    int32_t i;
    int ret;
    char *cptr;

    ptr = (opal_pstats_t **) src;

    for (i = 0; i < num_vals; ++i) {
        cptr = ptr[i]->node;
        if (OPAL_SUCCESS != (ret = opal_dss_pack_buffer(buffer, &cptr, 1, OPAL_STRING))) {
            return ret;
        }
        if (OPAL_SUCCESS != (ret = opal_dss_pack_buffer(buffer, &ptr[i]->rank, 1, OPAL_INT32))) {
            return ret;
        }
        if (OPAL_SUCCESS != (ret = opal_dss_pack_buffer(buffer, &ptr[i]->pid, 1, OPAL_PID))) {
            return ret;
        }
        cptr = ptr[i]->cmd;
        if (OPAL_SUCCESS != (ret = opal_dss_pack_buffer(buffer, &cptr, 1, OPAL_STRING))) {
            return ret;
        }
        if (OPAL_SUCCESS != (ret = opal_dss_pack_buffer(buffer, &ptr[i]->state[0], 1, OPAL_BYTE))) {
            return ret;
        }
        if (OPAL_SUCCESS != (ret = opal_dss_pack_buffer(buffer, &ptr[i]->time, 1, OPAL_TIMEVAL))) {
            return ret;
        }
        if (OPAL_SUCCESS != (ret = opal_dss_pack_buffer(buffer, &ptr[i]->priority, 1, OPAL_INT32))) {
            return ret;
        }
        if (OPAL_SUCCESS != (ret = opal_dss_pack_buffer(buffer, &ptr[i]->num_threads, 1, OPAL_INT16))) {
            return ret;
        }
        if (OPAL_SUCCESS != (ret = opal_dss_pack_float(buffer, &ptr[i]->pss, 1, OPAL_FLOAT))) {
            return ret;
        }
        if (OPAL_SUCCESS != (ret = opal_dss_pack_float(buffer, &ptr[i]->vsize, 1, OPAL_FLOAT))) {
            return ret;
        }
        if (OPAL_SUCCESS != (ret = opal_dss_pack_float(buffer, &ptr[i]->rss, 1, OPAL_FLOAT))) {
            return ret;
        }
        if (OPAL_SUCCESS != (ret = opal_dss_pack_float(buffer, &ptr[i]->peak_vsize, 1, OPAL_FLOAT))) {
            return ret;
        }
        if (OPAL_SUCCESS != (ret = opal_dss_pack_buffer(buffer, &ptr[i]->processor, 1, OPAL_INT16))) {
            return ret;
        }
        if (OPAL_SUCCESS != (ret = opal_dss_pack_buffer(buffer, &ptr[i]->sample_time, 1, OPAL_TIMEVAL))) {
            return ret;
        }
    }

    return OPAL_SUCCESS;
}

static int pack_disk_stats(opal_buffer_t *buffer, opal_diskstats_t *dk)
{
    uint64_t i64;
    int ret;

    if (OPAL_SUCCESS != (ret = opal_dss_pack_buffer(buffer, &dk->disk, 1, OPAL_STRING))) {
        return ret;
    }
    i64 = (uint64_t)dk->num_reads_completed;
    if (OPAL_SUCCESS != (ret = opal_dss_pack_buffer(buffer, &i64, 1, OPAL_UINT64))) {
        return ret;
    }
    i64 = (uint64_t)dk->num_reads_merged;
    if (OPAL_SUCCESS != (ret = opal_dss_pack_buffer(buffer, &i64, 1, OPAL_UINT64))) {
        return ret;
    }
    i64 = (uint64_t)dk->num_sectors_read;
    if (OPAL_SUCCESS != (ret = opal_dss_pack_buffer(buffer, &i64, 1, OPAL_UINT64))) {
        return ret;
    }
    i64 = (uint64_t)dk->milliseconds_reading;
    if (OPAL_SUCCESS != (ret = opal_dss_pack_buffer(buffer, &i64, 1, OPAL_UINT64))) {
        return ret;
    }
    i64 = (uint64_t)dk->num_writes_completed;
    if (OPAL_SUCCESS != (ret = opal_dss_pack_buffer(buffer, &i64, 1, OPAL_UINT64))) {
        return ret;
    }
    i64 = (uint64_t)dk->num_writes_merged;
    if (OPAL_SUCCESS != (ret = opal_dss_pack_buffer(buffer, &i64, 1, OPAL_UINT64))) {
        return ret;
    }
    i64 = (uint64_t)dk->num_sectors_written;
    if (OPAL_SUCCESS != (ret = opal_dss_pack_buffer(buffer, &i64, 1, OPAL_UINT64))) {
        return ret;
    }
    i64 = (uint64_t)dk->milliseconds_writing;
    if (OPAL_SUCCESS != (ret = opal_dss_pack_buffer(buffer, &i64, 1, OPAL_UINT64))) {
        return ret;
    }
    i64 = (uint64_t)dk->num_ios_in_progress;
    if (OPAL_SUCCESS != (ret = opal_dss_pack_buffer(buffer, &i64, 1, OPAL_UINT64))) {
        return ret;
    }
    i64 = (uint64_t)dk->milliseconds_io;
    if (OPAL_SUCCESS != (ret = opal_dss_pack_buffer(buffer, &i64, 1, OPAL_UINT64))) {
        return ret;
    }
    i64 = (uint64_t)dk->weighted_milliseconds_io;
    if (OPAL_SUCCESS != (ret = opal_dss_pack_buffer(buffer, &i64, 1, OPAL_UINT64))) {
        return ret;
    }
    return OPAL_SUCCESS;
}

static int pack_net_stats(opal_buffer_t *buffer, opal_netstats_t *ns)
{
    uint64_t i64;
    int ret;

    if (OPAL_SUCCESS != (ret = opal_dss_pack_buffer(buffer, &ns->net_interface, 1, OPAL_STRING))) {
        return ret;
    }
    i64 = (uint64_t)ns->num_bytes_recvd;
    if (OPAL_SUCCESS != (ret = opal_dss_pack_buffer(buffer, &i64, 1, OPAL_UINT64))) {
        return ret;
    }
    i64 = (uint64_t)ns->num_packets_recvd;
    if (OPAL_SUCCESS != (ret = opal_dss_pack_buffer(buffer, &i64, 1, OPAL_UINT64))) {
        return ret;
    }
    i64 = (uint64_t)ns->num_recv_errs;
    if (OPAL_SUCCESS != (ret = opal_dss_pack_buffer(buffer, &i64, 1, OPAL_UINT64))) {
        return ret;
    }
    i64 = (uint64_t)ns->num_bytes_sent;
    if (OPAL_SUCCESS != (ret = opal_dss_pack_buffer(buffer, &i64, 1, OPAL_UINT64))) {
        return ret;
    }
    i64 = (uint64_t)ns->num_packets_sent;
    if (OPAL_SUCCESS != (ret = opal_dss_pack_buffer(buffer, &i64, 1, OPAL_UINT64))) {
        return ret;
    }
    i64 = (uint64_t)ns->num_send_errs;
    if (OPAL_SUCCESS != (ret = opal_dss_pack_buffer(buffer, &i64, 1, OPAL_UINT64))) {
        return ret;
    }
    return OPAL_SUCCESS;
}

/*
 * OPAL_NODE_STAT
 */
int opal_dss_pack_node_stat(opal_buffer_t *buffer, const void *src,
                            int32_t num_vals, opal_data_type_t type)
{
    opal_node_stats_t **ptr;
    int32_t i, j;
    int ret;
    opal_diskstats_t *ds;
    opal_netstats_t *ns;

    ptr = (opal_node_stats_t **) src;

    for (i = 0; i < num_vals; ++i) {
        if (OPAL_SUCCESS != (ret = opal_dss_pack_float(buffer, &ptr[i]->la, 1, OPAL_FLOAT))) {
            return ret;
        }
        if (OPAL_SUCCESS != (ret = opal_dss_pack_float(buffer, &ptr[i]->la5, 1, OPAL_FLOAT))) {
            return ret;
        }
        if (OPAL_SUCCESS != (ret = opal_dss_pack_float(buffer, &ptr[i]->la15, 1, OPAL_FLOAT))) {
            return ret;
        }
        if (OPAL_SUCCESS != (ret = opal_dss_pack_float(buffer, &ptr[i]->total_mem, 1, OPAL_FLOAT))) {
            return ret;
        }
        if (OPAL_SUCCESS != (ret = opal_dss_pack_float(buffer, &ptr[i]->free_mem, 1, OPAL_FLOAT))) {
            return ret;
        }
        if (OPAL_SUCCESS != (ret = opal_dss_pack_float(buffer, &ptr[i]->buffers, 1, OPAL_FLOAT))) {
            return ret;
        }
        if (OPAL_SUCCESS != (ret = opal_dss_pack_float(buffer, &ptr[i]->cached, 1, OPAL_FLOAT))) {
            return ret;
        }
        if (OPAL_SUCCESS != (ret = opal_dss_pack_float(buffer, &ptr[i]->swap_cached, 1, OPAL_FLOAT))) {
            return ret;
        }
        if (OPAL_SUCCESS != (ret = opal_dss_pack_float(buffer, &ptr[i]->swap_total, 1, OPAL_FLOAT))) {
            return ret;
        }
        if (OPAL_SUCCESS != (ret = opal_dss_pack_float(buffer, &ptr[i]->swap_free, 1, OPAL_FLOAT))) {
            return ret;
        }
        if (OPAL_SUCCESS != (ret = opal_dss_pack_float(buffer, &ptr[i]->mapped, 1, OPAL_FLOAT))) {
            return ret;
        }
        if (OPAL_SUCCESS != (ret = opal_dss_pack_buffer(buffer, &ptr[i]->sample_time, 1, OPAL_TIMEVAL))) {
            return ret;
        }
        /* pack the number of disk stat objects on the list */
        j = opal_list_get_size(&ptr[i]->diskstats);
        if (OPAL_SUCCESS != (ret = opal_dss_pack_buffer(buffer, &j, 1, OPAL_INT32))) {
            return ret;
        }
        if (0 < j) {
            /* pack them */
            OPAL_LIST_FOREACH(ds, &ptr[i]->diskstats, opal_diskstats_t) {
                if (OPAL_SUCCESS != (ret = pack_disk_stats(buffer, ds))) {
                    return ret;
                }
            }
        }
        /* pack the number of net stat objects on the list */
        j = opal_list_get_size(&ptr[i]->netstats);
        if (OPAL_SUCCESS != (ret = opal_dss_pack_buffer(buffer, &j, 1, OPAL_INT32))) {
            return ret;
        }
        if (0 < j) {
            /* pack them */
            OPAL_LIST_FOREACH(ns, &ptr[i]->netstats, opal_netstats_t) {
                if (OPAL_SUCCESS != (ret = pack_net_stats(buffer, ns))) {
                    return ret;
                }
            }
        }
    }

    return OPAL_SUCCESS;
}

/*
 * OPAL_VALUE
 */
int opal_dss_pack_value(opal_buffer_t *buffer, const void *src,
                        int32_t num_vals, opal_data_type_t type)
{
    opal_value_t **ptr;
    int32_t i, n;
    int ret;

    ptr = (opal_value_t **) src;

    for (i = 0; i < num_vals; ++i) {
        /* pack the key and type */
        if (OPAL_SUCCESS != (ret = opal_dss_pack_string(buffer, &ptr[i]->key, 1, OPAL_STRING))) {
            return ret;
        }
        if (OPAL_SUCCESS != (ret = opal_dss_pack_data_type(buffer, &ptr[i]->type, 1, OPAL_DATA_TYPE))) {
            return ret;
        }
        /* now pack the right field */
        switch (ptr[i]->type) {
        case OPAL_BOOL:
            if (OPAL_SUCCESS != (ret = opal_dss_pack_buffer(buffer, &ptr[i]->data.flag, 1, OPAL_BOOL))) {
                return ret;
            }
            break;
        case OPAL_BYTE:
            if (OPAL_SUCCESS != (ret = opal_dss_pack_buffer(buffer, &ptr[i]->data.byte, 1, OPAL_BYTE))) {
                return ret;
            }
            break;
        case OPAL_STRING:
            if (OPAL_SUCCESS != (ret = opal_dss_pack_buffer(buffer, &ptr[i]->data.string, 1, OPAL_STRING))) {
                return ret;
            }
            break;
        case OPAL_SIZE:
            if (OPAL_SUCCESS != (ret = opal_dss_pack_buffer(buffer, &ptr[i]->data.size, 1, OPAL_SIZE))) {
                return ret;
            }
            break;
        case OPAL_PID:
            if (OPAL_SUCCESS != (ret = opal_dss_pack_buffer(buffer, &ptr[i]->data.pid, 1, OPAL_PID))) {
                return ret;
            }
            break;
        case OPAL_INT:
            if (OPAL_SUCCESS != (ret = opal_dss_pack_buffer(buffer, &ptr[i]->data.integer, 1, OPAL_INT))) {
                return ret;
            }
            break;
        case OPAL_INT8:
            if (OPAL_SUCCESS != (ret = opal_dss_pack_buffer(buffer, &ptr[i]->data.int8, 1, OPAL_INT8))) {
                return ret;
            }
            break;
        case OPAL_INT16:
            if (OPAL_SUCCESS != (ret = opal_dss_pack_buffer(buffer, &ptr[i]->data.int16, 1, OPAL_INT16))) {
                return ret;
            }
            break;
        case OPAL_INT32:
            if (OPAL_SUCCESS != (ret = opal_dss_pack_buffer(buffer, &ptr[i]->data.int32, 1, OPAL_INT32))) {
                return ret;
            }
            break;
        case OPAL_INT64:
            if (OPAL_SUCCESS != (ret = opal_dss_pack_buffer(buffer, &ptr[i]->data.int64, 1, OPAL_INT64))) {
                return ret;
            }
            break;
        case OPAL_UINT:
            if (OPAL_SUCCESS != (ret = opal_dss_pack_buffer(buffer, &ptr[i]->data.uint, 1, OPAL_UINT))) {
                return ret;
            }
            break;
        case OPAL_UINT8:
        case OPAL_PERSIST:
        case OPAL_SCOPE:
        case OPAL_DATA_RANGE:
        case OPAL_PROC_STATE:
            if (OPAL_SUCCESS != (ret = opal_dss_pack_buffer(buffer, &ptr[i]->data.uint8, 1, OPAL_UINT8))) {
                return ret;
            }
            break;
        case OPAL_UINT16:
            if (OPAL_SUCCESS != (ret = opal_dss_pack_buffer(buffer, &ptr[i]->data.uint16, 1, OPAL_UINT16))) {
                return ret;
            }
            break;
        case OPAL_UINT32:
        case OPAL_INFO_DIRECTIVES:
            if (OPAL_SUCCESS != (ret = opal_dss_pack_buffer(buffer, &ptr[i]->data.uint32, 1, OPAL_UINT32))) {
                return ret;
            }
            break;
        case OPAL_UINT64:
            if (OPAL_SUCCESS != (ret = opal_dss_pack_buffer(buffer, &ptr[i]->data.uint64, 1, OPAL_UINT64))) {
                return ret;
            }
            break;
        case OPAL_BYTE_OBJECT:
            /* have to pack by hand so we can match unpack without allocation */
            n = ptr[i]->data.bo.size;
            if (OPAL_SUCCESS != (ret = opal_dss_pack_int32(buffer, &n, 1, OPAL_INT32))) {
                return ret;
            }
            if (0 < n) {
                if (OPAL_SUCCESS != (ret = opal_dss_pack_byte(buffer, ptr[i]->data.bo.bytes, n, OPAL_BYTE))) {
                    return ret;
                }
            }
            break;
        case OPAL_FLOAT:
            if (OPAL_SUCCESS != (ret = opal_dss_pack_buffer(buffer, &ptr[i]->data.fval, 1, OPAL_FLOAT))) {
                return ret;
            }
            break;
        case OPAL_DOUBLE:
            if (OPAL_SUCCESS != (ret = opal_dss_pack_buffer(buffer, &ptr[i]->data.dval, 1, OPAL_DOUBLE))) {
                return ret;
            }
            break;
        case OPAL_TIMEVAL:
            if (OPAL_SUCCESS != (ret = opal_dss_pack_buffer(buffer, &ptr[i]->data.tv, 1, OPAL_TIMEVAL))) {
                return ret;
            }
            break;
        case OPAL_PTR:
            /* just ignore these values */
            break;
        case OPAL_NAME:
            if (OPAL_SUCCESS != (ret = opal_dss_pack_buffer(buffer, &ptr[i]->data.name, 1, OPAL_NAME))) {
                return ret;
            }
            break;
        case OPAL_STATUS:
            if (OPAL_SUCCESS != (ret = opal_dss_pack_buffer(buffer, &ptr[i]->data.status, 1, OPAL_INT))) {
                return ret;
            }
            break;
        case OPAL_ENVAR:
            if (OPAL_SUCCESS != (ret = opal_dss_pack_buffer(buffer, &ptr[i]->data.envar, 1, OPAL_ENVAR))) {
                return ret;
            }
            break;
        default:
            opal_output(0, "PACK-OPAL-VALUE: UNSUPPORTED TYPE %d FOR KEY %s", (int)ptr[i]->type, ptr[i]->key);
            return OPAL_ERROR;
        }
    }

    return OPAL_SUCCESS;
}


/*
 * BUFFER CONTENTS
 */
int opal_dss_pack_buffer_contents(opal_buffer_t *buffer, const void *src,
                                  int32_t num_vals, opal_data_type_t type)
{
    opal_buffer_t **ptr;
    int32_t i;
    int ret;

    ptr = (opal_buffer_t **) src;

    for (i = 0; i < num_vals; ++i) {
        /* pack the number of bytes */
        OPAL_OUTPUT((opal_dss_verbose, "opal_dss_pack_buffer_contents: bytes_used %u\n", (unsigned)ptr[i]->bytes_used));
        if (OPAL_SUCCESS != (ret = opal_dss_pack_sizet(buffer, &ptr[i]->bytes_used, 1, OPAL_SIZE))) {
            return ret;
        }
        /* pack the bytes */
        if (0 < ptr[i]->bytes_used) {
            if (OPAL_SUCCESS != (ret = opal_dss_pack_byte(buffer, ptr[i]->base_ptr, ptr[i]->bytes_used, OPAL_BYTE))) {
                return ret;
            }
        } else {
            ptr[i]->base_ptr = NULL;
        }
    }
    return OPAL_SUCCESS;
}

/*
 * NAME
 */
int opal_dss_pack_name(opal_buffer_t *buffer, const void *src,
                           int32_t num_vals, opal_data_type_t type)
{
    int rc;
    int32_t i;
    opal_process_name_t* proc;
    opal_jobid_t *jobid;
    opal_vpid_t *vpid;

    /* collect all the jobids in a contiguous array */
    jobid = (opal_jobid_t*)malloc(num_vals * sizeof(opal_jobid_t));
    if (NULL == jobid) {
        OPAL_ERROR_LOG(OPAL_ERR_OUT_OF_RESOURCE);
        return OPAL_ERR_OUT_OF_RESOURCE;
    }
    proc = (opal_process_name_t*)src;
    for (i=0; i < num_vals; i++) {
        jobid[i] = proc->jobid;
        proc++;
    }
    /* now pack them in one shot */
    if (OPAL_SUCCESS != (rc =
                         opal_dss_pack_jobid(buffer, jobid, num_vals, OPAL_JOBID))) {
        OPAL_ERROR_LOG(rc);
        free(jobid);
        return rc;
    }
    free(jobid);

    /* collect all the vpids in a contiguous array */
    vpid = (opal_vpid_t*)malloc(num_vals * sizeof(opal_vpid_t));
    if (NULL == vpid) {
        OPAL_ERROR_LOG(OPAL_ERR_OUT_OF_RESOURCE);
        return OPAL_ERR_OUT_OF_RESOURCE;
    }
    proc = (opal_process_name_t*)src;
    for (i=0; i < num_vals; i++) {
        vpid[i] = proc->vpid;
        proc++;
    }
    /* now pack them in one shot */
    if (OPAL_SUCCESS != (rc =
                         opal_dss_pack_vpid(buffer, vpid, num_vals, OPAL_VPID))) {
        OPAL_ERROR_LOG(rc);
        free(vpid);
        return rc;
    }
    free(vpid);

    return OPAL_SUCCESS;
}

/*
 * JOBID
 */
int opal_dss_pack_jobid(opal_buffer_t *buffer, const void *src,
                            int32_t num_vals, opal_data_type_t type)
{
    int ret;

    /* Turn around and pack the real type */
    if (OPAL_SUCCESS != (
                         ret = opal_dss_pack_buffer(buffer, src, num_vals, OPAL_JOBID_T))) {
        OPAL_ERROR_LOG(ret);
    }

    return ret;
}

/*
 * VPID
 */
int opal_dss_pack_vpid(opal_buffer_t *buffer, const void *src,
                           int32_t num_vals, opal_data_type_t type)
{
    int ret;

    /* Turn around and pack the real type */
    if (OPAL_SUCCESS != (
                         ret = opal_dss_pack_buffer(buffer, src, num_vals, OPAL_VPID_T))) {
        OPAL_ERROR_LOG(ret);
    }

    return ret;
}

/*
 * STATUS
 */
int opal_dss_pack_status(opal_buffer_t *buffer, const void *src,
                         int32_t num_vals, opal_data_type_t type)
{
    int ret;

    /* Turn around and pack the real type */
    ret = opal_dss_pack_buffer(buffer, src, num_vals, OPAL_INT);
    if (OPAL_SUCCESS != ret) {
        OPAL_ERROR_LOG(ret);
    }

    return ret;
}

int opal_dss_pack_envar(opal_buffer_t *buffer, const void *src,
                        int32_t num_vals, opal_data_type_t type)
{
    int ret;
    int32_t n;
    opal_envar_t *ptr = (opal_envar_t*)src;

    for (n=0; n < num_vals; n++) {
        if (OPAL_SUCCESS != (ret = opal_dss_pack_string(buffer, &ptr[n].envar, 1, OPAL_STRING))) {
            return ret;
        }
        if (OPAL_SUCCESS != (ret = opal_dss_pack_string(buffer, &ptr[n].value, 1, OPAL_STRING))) {
            return ret;
        }
        if (OPAL_SUCCESS != (ret = opal_dss_pack_byte(buffer, &ptr[n].separator, 1, OPAL_BYTE))) {
            return ret;
        }
    }
    return OPAL_SUCCESS;
}
