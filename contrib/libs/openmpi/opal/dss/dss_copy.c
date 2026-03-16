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

#include "opal/util/output.h"
#include "opal/dss/dss_internal.h"
#include "opal/util/error.h"

int opal_dss_copy(void **dest, void *src, opal_data_type_t type)
{
    opal_dss_type_info_t *info;

    /* check for error */
    if (NULL == dest) {
        return OPAL_ERR_BAD_PARAM;
    }
    if (NULL == src && (OPAL_NULL != type && OPAL_STRING != type)) {
        return OPAL_ERR_BAD_PARAM;
    }

   /* Lookup the copy function for this type and call it */

    if (NULL == (info = (opal_dss_type_info_t*)opal_pointer_array_get_item(&opal_dss_types, type))) {
        return OPAL_ERR_UNKNOWN_DATA_TYPE;
    }

    return info->odti_copy_fn(dest, src, type);
}

/*
 * STANDARD COPY FUNCTION - WORKS FOR EVERYTHING NON-STRUCTURED
 */
int opal_dss_std_copy(void **dest, void *src, opal_data_type_t type)
{
    size_t datasize;
    uint8_t *val = NULL;

    switch(type) {
    case OPAL_BOOL:
        datasize = sizeof(bool);
        break;

    case OPAL_INT:
    case OPAL_UINT:
    case OPAL_STATUS:
        datasize = sizeof(int);
        break;

    case OPAL_SIZE:
        datasize = sizeof(size_t);
        break;

    case OPAL_PID:
        datasize = sizeof(pid_t);
        break;

    case OPAL_BYTE:
    case OPAL_INT8:
    case OPAL_UINT8:
        datasize = 1;
        break;

    case OPAL_INT16:
    case OPAL_UINT16:
        datasize = 2;
        break;

    case OPAL_INT32:
    case OPAL_UINT32:
        datasize = 4;
        break;

    case OPAL_INT64:
    case OPAL_UINT64:
        datasize = 8;
        break;

    case OPAL_DATA_TYPE:
        datasize = sizeof(opal_data_type_t);
        break;

    case OPAL_FLOAT:
        datasize = sizeof(float);
        break;

    case OPAL_TIMEVAL:
        datasize = sizeof(struct timeval);
        break;

    case OPAL_TIME:
        datasize = sizeof(time_t);
        break;

    case OPAL_NAME:
        datasize = sizeof(opal_process_name_t);
        break;

    default:
        return OPAL_ERR_UNKNOWN_DATA_TYPE;
    }

    val = (uint8_t*)malloc(datasize);
    if (NULL == val) {
        return OPAL_ERR_OUT_OF_RESOURCE;
    }

    memcpy(val, src, datasize);
    *dest = val;

    return OPAL_SUCCESS;
}

/* COPY FUNCTIONS FOR NON-STANDARD SYSTEM TYPES */

/*
 * NULL
 */
int opal_dss_copy_null(char **dest, char *src, opal_data_type_t type)
{
    char *val;

    *dest = (char*)malloc(sizeof(char));
    if (NULL == *dest) {
        return OPAL_ERR_OUT_OF_RESOURCE;
    }

    val = *dest;  /* save the address of the value */

    /* set the dest to null */
    *val = 0x00;

    return OPAL_SUCCESS;
}

/*
 * STRING
 */
int opal_dss_copy_string(char **dest, char *src, opal_data_type_t type)
{
    if (NULL == src) {  /* got zero-length string/NULL pointer - store NULL */
        *dest = NULL;
    } else {
        *dest = strdup(src);
    }

    return OPAL_SUCCESS;
}

/* COPY FUNCTIONS FOR GENERIC OPAL TYPES */

/*
 * OPAL_BYTE_OBJECT
 */
int opal_dss_copy_byte_object(opal_byte_object_t **dest, opal_byte_object_t *src,
                              opal_data_type_t type)
{
    /* allocate space for the new object */
    *dest = (opal_byte_object_t*)malloc(sizeof(opal_byte_object_t));
    if (NULL == *dest) {
        return OPAL_ERR_OUT_OF_RESOURCE;
    }

    (*dest)->size = src->size;

    /* allocate the required space for the bytes */
    if (NULL == src->bytes) {
        (*dest)->bytes = NULL;
    } else {
        (*dest)->bytes = (uint8_t*)malloc(src->size);
        if (NULL == (*dest)->bytes) {
            OBJ_RELEASE(*dest);
            return OPAL_ERR_OUT_OF_RESOURCE;
        }
        /* copy the data across */
        memcpy((*dest)->bytes, src->bytes, src->size);
    }

    return OPAL_SUCCESS;
}

/* OPAL_PSTAT */
int opal_dss_copy_pstat(opal_pstats_t **dest, opal_pstats_t *src,
                        opal_data_type_t type)
{
    opal_pstats_t *p;

    /* create the new object */
    *dest = OBJ_NEW(opal_pstats_t);
    if (NULL == *dest) {
        return OPAL_ERR_OUT_OF_RESOURCE;
    }
    p = *dest;

    /* copy the individual fields */
    memcpy(p->node, src->node, sizeof(src->node));
    p->rank = src->rank;
    p->pid = src->pid;
    memcpy(p->cmd, src->cmd, sizeof(src->cmd));
    p->state[0] = src->state[0];
    p->time = src->time;
    p->priority = src->priority;
    p->num_threads = src->num_threads;
    p->pss = src->pss;
    p->vsize = src->vsize;
    p->rss = src->rss;
    p->peak_vsize = src->peak_vsize;
    p->processor = src->processor;
    p->sample_time.tv_sec = src->sample_time.tv_sec;
    p->sample_time.tv_usec = src->sample_time.tv_usec;
    return OPAL_SUCCESS;
}

/* OPAL_NODE_STAT */
int opal_dss_copy_node_stat(opal_node_stats_t **dest, opal_node_stats_t *src,
                            opal_data_type_t type)
{
    opal_node_stats_t *p;

    /* create the new object */
    *dest = OBJ_NEW(opal_node_stats_t);
    if (NULL == *dest) {
        return OPAL_ERR_OUT_OF_RESOURCE;
    }
    p = *dest;

    /* copy the individual fields */
    p->la = src->la;
    p->la5 = src->la5;
    p->la15 = src->la15;
    p->total_mem = src->total_mem;
    p->free_mem = src->free_mem;
    p->sample_time.tv_sec = src->sample_time.tv_sec;
    p->sample_time.tv_usec = src->sample_time.tv_usec;
    return OPAL_SUCCESS;
}

/* OPAL_VALUE */
int opal_dss_copy_value(opal_value_t **dest, opal_value_t *src,
                        opal_data_type_t type)
{
    opal_value_t *p;

    /* create the new object */
    *dest = OBJ_NEW(opal_value_t);
    if (NULL == *dest) {
        return OPAL_ERR_OUT_OF_RESOURCE;
    }
    p = *dest;

    /* copy the type and key */
    if (NULL != src->key) {
        p->key = strdup(src->key);
    }
    p->type = src->type;

    /* copy the right field */
    switch (src->type) {
    case OPAL_BYTE:
        p->data.byte = src->data.byte;
        break;
    case OPAL_STRING:
        if (NULL != src->data.string) {
            p->data.string = strdup(src->data.string);
        } else {
            p->data.string = NULL;
        }
        break;
    case OPAL_PID:
        p->data.pid = src->data.pid;
        break;
    case OPAL_INT:
        /* to avoid alignment issues */
        memcpy(&p->data.integer, &src->data.integer, sizeof(int));
        break;
    case OPAL_INT8:
        p->data.int8 = src->data.int8;
        break;
    case OPAL_INT16:
        /* to avoid alignment issues */
        memcpy(&p->data.int16, &src->data.int16, 2);
        break;
    case OPAL_INT32:
        /* to avoid alignment issues */
        memcpy(&p->data.int32, &src->data.int32, 4);
        break;
    case OPAL_INT64:
        /* to avoid alignment issues */
        memcpy(&p->data.int64, &src->data.int64, 8);
        break;
    case OPAL_UINT:
        /* to avoid alignment issues */
        memcpy(&p->data.uint, &src->data.uint, sizeof(unsigned int));
        break;
    case OPAL_UINT8:
        p->data.uint8 = src->data.uint8;
        break;
    case OPAL_UINT16:
        /* to avoid alignment issues */
        memcpy(&p->data.uint16, &src->data.uint16, 2);
        break;
    case OPAL_UINT32:
        /* to avoid alignment issues */
        memcpy(&p->data.uint32, &src->data.uint32, 4);
        break;
    case OPAL_UINT64:
        /* to avoid alignment issues */
        memcpy(&p->data.uint64, &src->data.uint64, 8);
        break;
    case OPAL_BYTE_OBJECT:
        if (NULL != src->data.bo.bytes && 0 < src->data.bo.size) {
            p->data.bo.bytes = malloc(src->data.bo.size);
            memcpy(p->data.bo.bytes, src->data.bo.bytes, src->data.bo.size);
            p->data.bo.size = src->data.bo.size;
        } else {
            p->data.bo.bytes = NULL;
            p->data.bo.size = 0;
        }
        break;
    case OPAL_NAME:
        memcpy(&p->data.name, &src->data.name, sizeof(opal_process_name_t));
        break;
    case OPAL_ENVAR:
        OBJ_CONSTRUCT(&p->data.envar, opal_envar_t);
        if (NULL != src->data.envar.envar) {
            p->data.envar.envar = strdup(src->data.envar.envar);
        }
        if (NULL != src->data.envar.value) {
            p->data.envar.value = strdup(src->data.envar.value);
        }
        p->data.envar.separator = src->data.envar.separator;
        break;
    default:
        opal_output(0, "COPY-OPAL-VALUE: UNSUPPORTED TYPE %d", (int)src->type);
        return OPAL_ERROR;
    }

    return OPAL_SUCCESS;
}

int opal_dss_copy_buffer_contents(opal_buffer_t **dest, opal_buffer_t *src,
                                  opal_data_type_t type)
{
    *dest = OBJ_NEW(opal_buffer_t);
    opal_dss.copy_payload(*dest, src);
    return OPAL_SUCCESS;
}

/* PROCESS NAME */
int opal_dss_copy_name(opal_process_name_t **dest, opal_process_name_t *src, opal_data_type_t type)
{
    opal_process_name_t *val;

    val = (opal_process_name_t*)malloc(sizeof(opal_process_name_t));
    if (NULL == val) {
        OPAL_ERROR_LOG(OPAL_ERR_OUT_OF_RESOURCE);
        return OPAL_ERR_OUT_OF_RESOURCE;
    }

    val->jobid = src->jobid;
    val->vpid = src->vpid;

    *dest = val;
    return OPAL_SUCCESS;
}

/*
 * JOBID
 */
int opal_dss_copy_jobid(opal_jobid_t **dest, opal_jobid_t *src, opal_data_type_t type)
{
    opal_jobid_t *val;

    val = (opal_jobid_t*)malloc(sizeof(opal_jobid_t));
    if (NULL == val) {
        OPAL_ERROR_LOG(OPAL_ERR_OUT_OF_RESOURCE);
        return OPAL_ERR_OUT_OF_RESOURCE;
    }

    *val = *src;
    *dest = val;

    return OPAL_SUCCESS;
}

/*
 * VPID
 */
int opal_dss_copy_vpid(opal_vpid_t **dest, opal_vpid_t *src, opal_data_type_t type)
{
    opal_vpid_t *val;

    val = (opal_vpid_t*)malloc(sizeof(opal_vpid_t));
    if (NULL == val) {
        OPAL_ERROR_LOG(OPAL_ERR_OUT_OF_RESOURCE);
        return OPAL_ERR_OUT_OF_RESOURCE;
    }

    *val = *src;
    *dest = val;

    return OPAL_SUCCESS;
}

int opal_dss_copy_envar(opal_envar_t **dest, opal_envar_t *src, opal_data_type_t type)
{
    opal_envar_t *val;

    val = OBJ_NEW(opal_envar_t);
    if (NULL == val) {
        OPAL_ERROR_LOG(OPAL_ERR_OUT_OF_RESOURCE);
        return OPAL_ERR_OUT_OF_RESOURCE;
    }

    if (NULL != src->envar) {
        val->envar = strdup(src->envar);
    }
    if (NULL != src->value) {
        val->value = strdup(src->value);
    }
    val->separator = src->separator;
    *dest = val;

    return OPAL_SUCCESS;
}
