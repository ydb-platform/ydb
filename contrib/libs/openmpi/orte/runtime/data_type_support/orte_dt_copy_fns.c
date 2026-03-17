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
 * Copyright (c) 2011      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2011-2013 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2014-2016 Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "orte_config.h"

#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
#include <string.h>

#include "opal/util/argv.h"
#include "opal/dss/dss.h"

#include "orte/mca/errmgr/errmgr.h"
#include "orte/runtime/data_type_support/orte_dt_support.h"

/* ORTE_STD_CNTR */
int orte_dt_copy_std_cntr(orte_std_cntr_t **dest, orte_std_cntr_t *src, opal_data_type_t type)
{
    orte_std_cntr_t *val;

    val = (orte_std_cntr_t*)malloc(sizeof(orte_std_cntr_t));
    if (NULL == val) {
        ORTE_ERROR_LOG(ORTE_ERR_OUT_OF_RESOURCE);
        return ORTE_ERR_OUT_OF_RESOURCE;
    }

    *val = *src;
    *dest = val;

    return ORTE_SUCCESS;
}

/**
 * JOB
 */
int orte_dt_copy_job(orte_job_t **dest, orte_job_t *src, opal_data_type_t type)
{
    (*dest) = src;
    OBJ_RETAIN(src);

    return ORTE_SUCCESS;
}

/**
* NODE
 */
int orte_dt_copy_node(orte_node_t **dest, orte_node_t *src, opal_data_type_t type)
{
    orte_node_t *node;

    node = OBJ_NEW(orte_node_t);
    node->name = strdup(src->name);
    node->state = src->state;
    node->slots = src->slots;
    node->slots_inuse = src->slots_inuse;
    node->slots_max = src->slots_max;
    node->topology = src->topology;
    node->flags = src->flags;
    (*dest) = node;

    return ORTE_SUCCESS;
}

/**
 * PROC
 */
int orte_dt_copy_proc(orte_proc_t **dest, orte_proc_t *src, opal_data_type_t type)
{
    (*dest) = src;
    OBJ_RETAIN(src);
    return ORTE_SUCCESS;
}

/*
 * APP CONTEXT
 */
int orte_dt_copy_app_context(orte_app_context_t **dest, orte_app_context_t *src, opal_data_type_t type)
{
    opal_value_t *kv, *kvnew;

    /* create the new object */
    *dest = OBJ_NEW(orte_app_context_t);
    if (NULL == *dest) {
        ORTE_ERROR_LOG(ORTE_ERR_OUT_OF_RESOURCE);
        return ORTE_ERR_OUT_OF_RESOURCE;
    }

    /* copy data into it */
    (*dest)->idx = src->idx;
    if (NULL != src->app) {
        (*dest)->app = strdup(src->app);
    }
    (*dest)->num_procs = src->num_procs;
    (*dest)->argv = opal_argv_copy(src->argv);
    (*dest)->env = opal_argv_copy(src->env);
    if (NULL != src->cwd) {
        (*dest)->cwd = strdup(src->cwd);
    }

    OPAL_LIST_FOREACH(kv, &src->attributes, opal_value_t) {
        opal_dss.copy((void**)&kvnew, kv, OPAL_VALUE);
        opal_list_append(&(*dest)->attributes, &kvnew->super);
    }

    return ORTE_SUCCESS;
}

int orte_dt_copy_proc_state(orte_proc_state_t **dest, orte_proc_state_t *src, opal_data_type_t type)
{
    orte_proc_state_t *ps;

    ps = (orte_proc_state_t*)malloc(sizeof(orte_proc_state_t));
    if (NULL == ps) {
        ORTE_ERROR_LOG(ORTE_ERR_OUT_OF_RESOURCE);
        return ORTE_ERR_OUT_OF_RESOURCE;
    }

    *ps = *src;
    *dest = ps;

    return ORTE_SUCCESS;
}

int orte_dt_copy_job_state(orte_job_state_t **dest, orte_job_state_t *src, opal_data_type_t type)
{
    orte_job_state_t *ps;

    ps = (orte_job_state_t*)malloc(sizeof(orte_job_state_t));
    if (NULL == ps) {
        ORTE_ERROR_LOG(ORTE_ERR_OUT_OF_RESOURCE);
        return ORTE_ERR_OUT_OF_RESOURCE;
    }

    *ps = *src;
    *dest = ps;

    return ORTE_SUCCESS;
}

int orte_dt_copy_node_state(orte_node_state_t **dest, orte_node_state_t *src, opal_data_type_t type)
{
    orte_node_state_t *ps;

    ps = (orte_node_state_t*)malloc(sizeof(orte_node_state_t));
    if (NULL == ps) {
        ORTE_ERROR_LOG(ORTE_ERR_OUT_OF_RESOURCE);
        return ORTE_ERR_OUT_OF_RESOURCE;
    }

    *ps = *src;
    *dest = ps;

    return ORTE_SUCCESS;
}

int orte_dt_copy_exit_code(orte_exit_code_t **dest, orte_exit_code_t *src, opal_data_type_t type)
{
    orte_exit_code_t *ps;

    ps = (orte_exit_code_t*)malloc(sizeof(orte_exit_code_t));
    if (NULL == ps) {
        ORTE_ERROR_LOG(ORTE_ERR_OUT_OF_RESOURCE);
        return ORTE_ERR_OUT_OF_RESOURCE;
    }

    *ps = *src;
    *dest = ps;

    return ORTE_SUCCESS;
}

/*
 * JOB_MAP
 */
int orte_dt_copy_map(orte_job_map_t **dest, orte_job_map_t *src, opal_data_type_t type)
{
    orte_std_cntr_t i;

    if (NULL == src) {
        *dest = NULL;
        return ORTE_SUCCESS;
    }

    /* create the new object */
    *dest = OBJ_NEW(orte_job_map_t);
    if (NULL == *dest) {
        ORTE_ERROR_LOG(ORTE_ERR_OUT_OF_RESOURCE);
        return ORTE_ERR_OUT_OF_RESOURCE;
    }

    /* copy data into it */
    (*dest)->mapping = src->mapping;
    (*dest)->ranking = src->ranking;
    (*dest)->binding = src->binding;
    if (NULL != src->ppr) {
        (*dest)->ppr = strdup(src->ppr);
    }
    (*dest)->display_map = src->display_map;
    (*dest)->num_new_daemons = src->num_new_daemons;
    (*dest)->daemon_vpid_start = src->daemon_vpid_start;
    (*dest)->num_nodes = src->num_nodes;

    /* copy the pointer array - have to do this manually
        * as no dss.copy function is setup for that object
        */
    (*dest)->nodes->lowest_free = src->nodes->lowest_free;
    (*dest)->nodes->number_free = src->nodes->number_free;
    (*dest)->nodes->size = src->nodes->size;
    (*dest)->nodes->max_size = src->nodes->max_size;
    (*dest)->nodes->block_size = src->nodes->block_size;
    for (i=0; i < src->nodes->size; i++) {
        (*dest)->nodes->addr[i] = src->nodes->addr[i];
    }

    return ORTE_SUCCESS;
}

/*
 * RML tag
 */
int orte_dt_copy_tag(orte_rml_tag_t **dest, orte_rml_tag_t *src, opal_data_type_t type)
{
    orte_rml_tag_t *tag;

    if (NULL == src) {
        *dest = NULL;
        return ORTE_SUCCESS;
    }

    /* create the new space */
    tag = (orte_rml_tag_t*)malloc(sizeof(orte_rml_tag_t));
    if (NULL == tag) {
        ORTE_ERROR_LOG(ORTE_ERR_OUT_OF_RESOURCE);
        return ORTE_ERR_OUT_OF_RESOURCE;
    }

    /* copy data into it */
    *tag = *src;
    *dest = tag;

    return ORTE_SUCCESS;
}

int orte_dt_copy_daemon_cmd(orte_daemon_cmd_flag_t **dest, orte_daemon_cmd_flag_t *src, opal_data_type_t type)
{
    size_t datasize;

    datasize = sizeof(orte_daemon_cmd_flag_t);

    *dest = (orte_daemon_cmd_flag_t*)malloc(datasize);
    if (NULL == *dest) {
        ORTE_ERROR_LOG(ORTE_ERR_OUT_OF_RESOURCE);
        return ORTE_ERR_OUT_OF_RESOURCE;
    }

    memcpy(*dest, src, datasize);

    return ORTE_SUCCESS;
}

int orte_dt_copy_iof_tag(orte_iof_tag_t **dest, orte_iof_tag_t *src, opal_data_type_t type)
{
    size_t datasize;

    datasize = sizeof(orte_iof_tag_t);

    *dest = (orte_iof_tag_t*)malloc(datasize);
    if (NULL == *dest) {
        ORTE_ERROR_LOG(ORTE_ERR_OUT_OF_RESOURCE);
        return ORTE_ERR_OUT_OF_RESOURCE;
    }

    memcpy(*dest, src, datasize);

    return ORTE_SUCCESS;
}

int orte_dt_copy_attr(orte_attribute_t **dest, orte_attribute_t *src, opal_data_type_t type)
{
    *dest = OBJ_NEW(orte_attribute_t);
    if (NULL == *dest) {
        ORTE_ERROR_LOG(ORTE_ERR_OUT_OF_RESOURCE);
        return ORTE_ERR_OUT_OF_RESOURCE;
    }
    (*dest)->key = src->key;
    (*dest)->type = src->type;

    memcpy(&(*dest)->data, &src->data, sizeof(src->data));

    return ORTE_SUCCESS;
}

int orte_dt_copy_sig(orte_grpcomm_signature_t **dest, orte_grpcomm_signature_t *src, opal_data_type_t type)
{
    *dest = OBJ_NEW(orte_grpcomm_signature_t);
    if (NULL == *dest) {
        ORTE_ERROR_LOG(ORTE_ERR_OUT_OF_RESOURCE);
        return ORTE_ERR_OUT_OF_RESOURCE;
    }
    (*dest)->sz = src->sz;
    (*dest)->signature = (orte_process_name_t*)malloc(src->sz * sizeof(orte_process_name_t));
    if (NULL == (*dest)->signature) {
        ORTE_ERROR_LOG(ORTE_ERR_OUT_OF_RESOURCE);
        OBJ_RELEASE(*dest);
        return ORTE_ERR_OUT_OF_RESOURCE;
    }
    memcpy((*dest)->signature, src->signature, src->sz * sizeof(orte_process_name_t));
    return ORTE_SUCCESS;
}
