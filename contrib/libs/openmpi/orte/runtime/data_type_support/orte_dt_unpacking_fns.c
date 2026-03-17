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
 * Copyright (c) 2011-2017 Cisco Systems, Inc.  All rights reserved
 * Copyright (c) 2011-2013 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2014-2018 Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "orte_config.h"
#include "orte/types.h"

#include <sys/types.h>

#include "opal/dss/dss.h"
#include "opal/dss/dss_internal.h"
#include "opal/mca/hwloc/hwloc-internal.h"
#include "opal/util/argv.h"

#include "orte/mca/errmgr/errmgr.h"
#include "orte/runtime/data_type_support/orte_dt_support.h"

/*
 * ORTE_STD_CNTR
 */
int orte_dt_unpack_std_cntr(opal_buffer_t *buffer, void *dest,
                             int32_t *num_vals, opal_data_type_t type)
{
    int ret;

    /* Turn around and unpack the real type */
    if (ORTE_SUCCESS != (ret = opal_dss_unpack_buffer(buffer, dest, num_vals, ORTE_STD_CNTR_T))) {
        ORTE_ERROR_LOG(ret);
    }

    return ret;
}

/*
 * JOB
 * NOTE: We do not pack all of the job object's fields as many of them have no
 * value in sending them to another location. The only purpose in packing and
 * sending a job object is to communicate the data required to dynamically
 * spawn another job - so we only pack that limited set of required data.
 * Therefore, only unpack what was packed
 */
int orte_dt_unpack_job(opal_buffer_t *buffer, void *dest,
                       int32_t *num_vals, opal_data_type_t type)
{
    int rc;
    int32_t i, k, n, count, bookmark;
    orte_job_t **jobs;
    orte_app_idx_t j;
    orte_attribute_t *kv;
    char *tmp;
    opal_value_t *val;
    opal_list_t *cache;

    /* unpack into array of orte_job_t objects */
    jobs = (orte_job_t**) dest;
    for (i=0; i < *num_vals; i++) {

        /* create the orte_job_t object */
        jobs[i] = OBJ_NEW(orte_job_t);
        if (NULL == jobs[i]) {
            ORTE_ERROR_LOG(ORTE_ERR_OUT_OF_RESOURCE);
            return ORTE_ERR_OUT_OF_RESOURCE;
        }

        /* unpack the jobid */
        n = 1;
        if (ORTE_SUCCESS != (rc = opal_dss_unpack_buffer(buffer,
                                &(jobs[i]->jobid), &n, ORTE_JOBID))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }
        /* unpack the flags */
        n = 1;
        if (ORTE_SUCCESS != (rc = opal_dss_unpack_buffer(buffer,
                         (&(jobs[i]->flags)), &n, ORTE_JOB_FLAGS_T))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }

        /* unpack the attributes */
        n=1;
        if (ORTE_SUCCESS != (rc = opal_dss_unpack_buffer(buffer, &count,
                                                         &n, ORTE_STD_CNTR))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }
        for (k=0; k < count; k++) {
            n=1;
            if (ORTE_SUCCESS != (rc = opal_dss_unpack_buffer(buffer, &kv,
                                                             &n, ORTE_ATTRIBUTE))) {
                ORTE_ERROR_LOG(rc);
                return rc;
            }
            kv->local = ORTE_ATTR_GLOBAL;  // obviously not a local value
            opal_list_append(&jobs[i]->attributes, &kv->super);
        }
        /* unpack any job info */
        n=1;
        if (ORTE_SUCCESS != (rc = opal_dss_unpack_buffer(buffer, &count,
                                                         &n, ORTE_STD_CNTR))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }
        if (0 < count){
            cache = OBJ_NEW(opal_list_t);
            orte_set_attribute(&jobs[i]->attributes, ORTE_JOB_INFO_CACHE, ORTE_ATTR_LOCAL, (void*)cache, OPAL_PTR);
            for (k=0; k < count; k++) {
                n=1;
                if (ORTE_SUCCESS != (rc = opal_dss_unpack_buffer(buffer, &val,
                                                                 &n, OPAL_VALUE))) {
                    ORTE_ERROR_LOG(rc);
                    return rc;
                }
                opal_list_append(cache, &val->super);
            }
        }

        /* unpack the personality */
        n=1;
        if (ORTE_SUCCESS != (rc = opal_dss_unpack_buffer(buffer, &count, &n, OPAL_INT32))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }
        for (k=0; k < count; k++) {
            n=1;
            if (ORTE_SUCCESS != (rc = opal_dss_unpack_buffer(buffer, &tmp, &n, OPAL_STRING))) {
                ORTE_ERROR_LOG(rc);
                return rc;
            }
            opal_argv_append_nosize(&jobs[i]->personality, tmp);
            free(tmp);
        }

        /* unpack the num apps */
        n = 1;
        if (ORTE_SUCCESS != (rc = opal_dss_unpack_buffer(buffer,
                                (&(jobs[i]->num_apps)), &n, ORTE_APP_IDX))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }

        /* if there are apps, unpack them */
        if (0 < jobs[i]->num_apps) {
            orte_app_context_t *app;
            for (j=0; j < jobs[i]->num_apps; j++) {
                n = 1;
                if (ORTE_SUCCESS != (rc = opal_dss_unpack_buffer(buffer,
                               &app, &n, ORTE_APP_CONTEXT))) {
                    ORTE_ERROR_LOG(rc);
                    return rc;
                }
                opal_pointer_array_add(jobs[i]->apps, app);
            }
        }

        /* unpack num procs and offset */
        n = 1;
        if (ORTE_SUCCESS != (rc = opal_dss_unpack_buffer(buffer,
                                (&(jobs[i]->num_procs)), &n, ORTE_VPID))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }
        n = 1;
        if (ORTE_SUCCESS != (rc = opal_dss_unpack_buffer(buffer,
                                (&(jobs[i]->offset)), &n, ORTE_VPID))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }

        if (0 < jobs[i]->num_procs) {
            /* check attributes to see if this job was fully
             * described in the launch msg */
            if (orte_get_attribute(&jobs[i]->attributes, ORTE_JOB_FULLY_DESCRIBED, NULL, OPAL_BOOL)) {
                orte_proc_t *proc;
                for (j=0; j < jobs[i]->num_procs; j++) {
                    n = 1;
                    if (ORTE_SUCCESS != (rc = opal_dss_unpack_buffer(buffer,
                                   &proc, &n, ORTE_PROC))) {
                        ORTE_ERROR_LOG(rc);
                        return rc;
                    }
                    opal_pointer_array_add(jobs[i]->procs, proc);
                }
            }
        }

        /* unpack stdin target */
        n = 1;
        if (ORTE_SUCCESS != (rc = opal_dss_unpack_buffer(buffer,
                            (&(jobs[i]->stdin_target)), &n, ORTE_VPID))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }

        /* unpack the total slots allocated to the job */
        n = 1;
        if (ORTE_SUCCESS != (rc = opal_dss_unpack_buffer(buffer,
                         (&(jobs[i]->total_slots_alloc)), &n, ORTE_STD_CNTR))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }

        /* if the map is NULL, then we didn't pack it as there was
         * nothing to pack. Instead, we packed a flag to indicate whether or not
         * the map is included */
        n = 1;
        if (ORTE_SUCCESS != (rc = opal_dss_unpack_buffer(buffer,
                                            &j, &n, ORTE_STD_CNTR))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }
        if (0 < j) {
            /* unpack the map */
            n = 1;
            if (ORTE_SUCCESS != (rc = opal_dss_unpack_buffer(buffer,
                                            (&(jobs[i]->map)), &n, ORTE_JOB_MAP))) {
                ORTE_ERROR_LOG(rc);
                return rc;
            }
        }

        /* unpack the bookmark */
        n = 1;
        if (ORTE_SUCCESS != (rc = opal_dss_unpack_buffer(buffer,
                                            &bookmark, &n, OPAL_INT32))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }
        if (0 <= bookmark) {
            /* retrieve it */
            jobs[i]->bookmark = (orte_node_t*)opal_pointer_array_get_item(orte_node_pool, bookmark);
        }

        /* unpack the job state */
        n = 1;
        if (ORTE_SUCCESS != (rc = opal_dss_unpack_buffer(buffer,
                         (&(jobs[i]->state)), &n, ORTE_JOB_STATE))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }
    }

    return ORTE_SUCCESS;
}

/*
 * NODE
 */
int orte_dt_unpack_node(opal_buffer_t *buffer, void *dest,
                        int32_t *num_vals, opal_data_type_t type)
{
    int rc;
    int32_t i, n, k, count;
    orte_node_t **nodes;
    uint8_t flag;
    orte_attribute_t *kv;

    /* unpack into array of orte_node_t objects */
    nodes = (orte_node_t**) dest;
    for (i=0; i < *num_vals; i++) {

        /* create the node object */
        nodes[i] = OBJ_NEW(orte_node_t);
        if (NULL == nodes[i]) {
            ORTE_ERROR_LOG(ORTE_ERR_OUT_OF_RESOURCE);
            return ORTE_ERR_OUT_OF_RESOURCE;
        }

        /* do not unpack the index - meaningless here */

        /* unpack the node name */
        n = 1;
        if (ORTE_SUCCESS != (rc = opal_dss_unpack_buffer(buffer,
                         &(nodes[i]->name), &n, OPAL_STRING))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }

        /* do not unpack the daemon name or launch id */

        /* unpack the number of procs on the node */
        n = 1;
        if (ORTE_SUCCESS != (rc = opal_dss_unpack_buffer(buffer,
                         (&(nodes[i]->num_procs)), &n, ORTE_VPID))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }

        /* do not unpack the proc info */

        /* unpack whether we are oversubscribed */
        n = 1;
        if (ORTE_SUCCESS != (rc = opal_dss_unpack_buffer(buffer,
                         (&flag), &n, OPAL_UINT8))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }
        if (flag) {
            ORTE_FLAG_SET(nodes[i], ORTE_NODE_FLAG_OVERSUBSCRIBED);
        }

        /* unpack the state */
        n = 1;
        if (ORTE_SUCCESS != (rc = opal_dss_unpack_buffer(buffer,
                         (&(nodes[i]->state)), &n, ORTE_NODE_STATE))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }

        /* unpack the attributes */
        n=1;
        if (ORTE_SUCCESS != (rc = opal_dss_unpack_buffer(buffer, &count,
                                                         &n, ORTE_STD_CNTR))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }
        for (k=0; k < count; k++) {
            n=1;
            if (ORTE_SUCCESS != (rc = opal_dss_unpack_buffer(buffer, &kv,
                                                             &n, ORTE_ATTRIBUTE))) {
                ORTE_ERROR_LOG(rc);
                return rc;
            }
            kv->local = ORTE_ATTR_GLOBAL;  // obviously not a local value
            opal_list_append(&nodes[i]->attributes, &kv->super);
        }
    }
    return ORTE_SUCCESS;
}

/*
 * PROC
 */
int orte_dt_unpack_proc(opal_buffer_t *buffer, void *dest,
                        int32_t *num_vals, opal_data_type_t type)
{
    int rc;
    int32_t i, n, count, k;
    orte_attribute_t *kv;;
    orte_proc_t **procs;

    /* unpack into array of orte_proc_t objects */
    procs = (orte_proc_t**) dest;
    for (i=0; i < *num_vals; i++) {

        /* create the orte_proc_t object */
        procs[i] = OBJ_NEW(orte_proc_t);
        if (NULL == procs[i]) {
            ORTE_ERROR_LOG(ORTE_ERR_OUT_OF_RESOURCE);
            return ORTE_ERR_OUT_OF_RESOURCE;
        }

        /* unpack the name */
        n = 1;
        if (ORTE_SUCCESS != (rc = opal_dss_unpack_buffer(buffer,
                         &(procs[i]->name), &n, ORTE_NAME))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }

        /* unpack the node it is on */
        n = 1;
        if (ORTE_SUCCESS != (rc = opal_dss_unpack_buffer(buffer,
                         (&(procs[i]->parent)), &n, ORTE_VPID))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }

       /* unpack the local rank */
        n = 1;
        if (ORTE_SUCCESS != (rc = opal_dss_unpack_buffer(buffer,
                         (&(procs[i]->local_rank)), &n, ORTE_LOCAL_RANK))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }

        /* unpack the node rank */
        n = 1;
        if (ORTE_SUCCESS != (rc = opal_dss_unpack_buffer(buffer,
                          (&(procs[i]->node_rank)), &n, ORTE_NODE_RANK))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }

        /* unpack the state */
        n = 1;
        if (ORTE_SUCCESS != (rc = opal_dss_unpack_buffer(buffer,
                         (&(procs[i]->state)), &n, ORTE_PROC_STATE))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }

        /* unpack the app context index */
        n = 1;
        if (ORTE_SUCCESS != (rc = opal_dss_unpack_buffer(buffer,
                         (&(procs[i]->app_idx)), &n, ORTE_STD_CNTR))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }

        /* unpack the app_rank */
        n = 1;
        if (ORTE_SUCCESS != (rc = opal_dss_unpack_buffer(buffer,
                         (&(procs[i]->app_rank)), &n, OPAL_UINT32))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }

        /* unpack the attributes */
        n=1;
        if (ORTE_SUCCESS != (rc = opal_dss_unpack_buffer(buffer, &count,
                                                         &n, ORTE_STD_CNTR))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }
        for (k=0; k < count; k++) {
            n=1;
            if (ORTE_SUCCESS != (rc = opal_dss_unpack_buffer(buffer, &kv,
                                                             &n, ORTE_ATTRIBUTE))) {
                ORTE_ERROR_LOG(rc);
                return rc;
            }
            kv->local = ORTE_ATTR_GLOBAL;  // obviously not a local value
            opal_list_append(&procs[i]->attributes, &kv->super);
        }
    }
    return ORTE_SUCCESS;
}

/*
 * APP_CONTEXT
 */
int orte_dt_unpack_app_context(opal_buffer_t *buffer, void *dest,
                               int32_t *num_vals, opal_data_type_t type)
{
    int rc;
    orte_app_context_t **app_context;
    int32_t i, max_n=1, count, k;
    orte_attribute_t *kv;

    /* unpack into array of app_context objects */
    app_context = (orte_app_context_t**) dest;
    for (i=0; i < *num_vals; i++) {

        /* create the app_context object */
        app_context[i] = OBJ_NEW(orte_app_context_t);
        if (NULL == app_context[i]) {
            ORTE_ERROR_LOG(ORTE_ERR_OUT_OF_RESOURCE);
            return ORTE_ERR_OUT_OF_RESOURCE;
        }

        /* get the app index number */
        max_n = 1;
        if (ORTE_SUCCESS != (rc = opal_dss_unpack_buffer(buffer, &(app_context[i]->idx),
                                                         &max_n, ORTE_STD_CNTR))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }

        /* unpack the application name */
        max_n = 1;
        if (ORTE_SUCCESS != (rc = opal_dss_unpack_buffer(buffer, &(app_context[i]->app),
                                                         &max_n, OPAL_STRING))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }

        /* get the number of processes */
        max_n = 1;
        if (ORTE_SUCCESS != (rc = opal_dss_unpack_buffer(buffer, &(app_context[i]->num_procs),
                                                         &max_n, ORTE_STD_CNTR))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }

        /* get the first rank for this app */
        max_n = 1;
        if (ORTE_SUCCESS != (rc = opal_dss_unpack_buffer(buffer, &(app_context[i]->first_rank),
                                                         &max_n, ORTE_VPID))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }

        /* get the number of argv strings that were packed */
        max_n = 1;
        if (ORTE_SUCCESS != (rc = opal_dss_unpack_buffer(buffer, &count, &max_n, ORTE_STD_CNTR))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }

        /* if there are argv strings, allocate the required space for the char * pointers */
        if (0 < count) {
            app_context[i]->argv = (char **)malloc((count+1) * sizeof(char*));
            if (NULL == app_context[i]->argv) {
                ORTE_ERROR_LOG(ORTE_ERR_OUT_OF_RESOURCE);
                return ORTE_ERR_OUT_OF_RESOURCE;
            }
            app_context[i]->argv[count] = NULL;

            /* and unpack them */
            max_n = count;
            if (ORTE_SUCCESS != (rc = opal_dss_unpack_buffer(buffer, app_context[i]->argv, &max_n, OPAL_STRING))) {
                ORTE_ERROR_LOG(rc);
                return rc;
            }
        }

        /* get the number of env strings */
        max_n = 1;
        if (ORTE_SUCCESS != (rc = opal_dss_unpack_buffer(buffer, &count, &max_n, ORTE_STD_CNTR))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }

        /* if there are env strings, allocate the required space for the char * pointers */
        if (0 < count) {
            app_context[i]->env = (char **)malloc((count+1) * sizeof(char*));
            if (NULL == app_context[i]->env) {
                ORTE_ERROR_LOG(ORTE_ERR_OUT_OF_RESOURCE);
                return ORTE_ERR_OUT_OF_RESOURCE;
            }
            app_context[i]->env[count] = NULL;

            /* and unpack them */
            max_n = count;
            if (ORTE_SUCCESS != (rc = opal_dss_unpack_buffer(buffer, app_context[i]->env, &max_n, OPAL_STRING))) {
                ORTE_ERROR_LOG(rc);
                return rc;
            }
        }

        /* unpack the cwd */
        max_n = 1;
        if (ORTE_SUCCESS != (rc = opal_dss_unpack_buffer(buffer, &app_context[i]->cwd,
                                                         &max_n, OPAL_STRING))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }

        /* unpack the attributes */
        max_n=1;
        if (ORTE_SUCCESS != (rc = opal_dss_unpack_buffer(buffer, &count,
                                                         &max_n, ORTE_STD_CNTR))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }
        for (k=0; k < count; k++) {
            max_n=1;
            if (ORTE_SUCCESS != (rc = opal_dss_unpack_buffer(buffer, &kv,
                                                             &max_n, ORTE_ATTRIBUTE))) {
                ORTE_ERROR_LOG(rc);
                return rc;
            }
            /* obviously, this isn't a local value */
            kv->local = false;
            opal_list_append(&app_context[i]->attributes, &kv->super);
        }
    }

    return ORTE_SUCCESS;
}

/*
 * EXIT CODE
 */
int orte_dt_unpack_exit_code(opal_buffer_t *buffer, void *dest,
                                   int32_t *num_vals, opal_data_type_t type)
{
    int rc;

    if (ORTE_SUCCESS != (rc = opal_dss_unpack_buffer(buffer, dest, num_vals, ORTE_EXIT_CODE_T))) {
        ORTE_ERROR_LOG(rc);
    }

    return rc;
}

/*
 * NODE STATE
 */
int orte_dt_unpack_node_state(opal_buffer_t *buffer, void *dest,
                                    int32_t *num_vals, opal_data_type_t type)
{
    int rc;

    if (ORTE_SUCCESS != (rc = opal_dss_unpack_buffer(buffer, dest, num_vals, ORTE_NODE_STATE_T))) {
        ORTE_ERROR_LOG(rc);
    }

    return rc;
}

/*
 * PROC STATE
 */
int orte_dt_unpack_proc_state(opal_buffer_t *buffer, void *dest,
                                    int32_t *num_vals, opal_data_type_t type)
{
    int rc;

    if (ORTE_SUCCESS != (rc = opal_dss_unpack_buffer(buffer, dest, num_vals, ORTE_PROC_STATE_T))) {
        ORTE_ERROR_LOG(rc);
    }

    return rc;
}

/*
 * JOB STATE
 */
int orte_dt_unpack_job_state(opal_buffer_t *buffer, void *dest,
                                   int32_t *num_vals, opal_data_type_t type)
{
    int rc;

    if (ORTE_SUCCESS != (rc = opal_dss_unpack_buffer(buffer, dest, num_vals, ORTE_JOB_STATE_T))) {
        ORTE_ERROR_LOG(rc);
    }

    return rc;
}

/*
 * JOB_MAP
 * NOTE: There is no obvious reason to include all the node information when
 * sending a map - hence, we do not pack that field, so don't unpack it here
 */
int orte_dt_unpack_map(opal_buffer_t *buffer, void *dest,
                       int32_t *num_vals, opal_data_type_t type)
{
    int rc;
    int32_t i, n;
    orte_job_map_t **maps;

    /* unpack into array of orte_job_map_t objects */
    maps = (orte_job_map_t**) dest;
    for (i=0; i < *num_vals; i++) {

        /* create the orte_rmaps_base_map_t object */
        maps[i] = OBJ_NEW(orte_job_map_t);
        if (NULL == maps[i]) {
            ORTE_ERROR_LOG(ORTE_ERR_OUT_OF_RESOURCE);
            return ORTE_ERR_OUT_OF_RESOURCE;
        }

        /* unpack the requested mapper */
        n = 1;
        if (ORTE_SUCCESS != (rc = opal_dss_unpack_buffer(buffer,
                                                         &(maps[i]->req_mapper), &n, OPAL_STRING))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }

        /* unpack the last mapper */
        n = 1;
        if (ORTE_SUCCESS != (rc = opal_dss_unpack_buffer(buffer,
                                                         &(maps[i]->last_mapper), &n, OPAL_STRING))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }

        /* unpack the policies */
        n = 1;
        if (ORTE_SUCCESS != (rc = opal_dss_unpack_buffer(buffer,
                                                         &(maps[i]->mapping), &n, ORTE_MAPPING_POLICY))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }
        n = 1;
        if (ORTE_SUCCESS != (rc = opal_dss_unpack_buffer(buffer,
                                                         &(maps[i]->ranking), &n, ORTE_RANKING_POLICY))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }
        n = 1;
        if (ORTE_SUCCESS != (rc = opal_dss_unpack_buffer(buffer,
                                                         &(maps[i]->binding), &n, OPAL_BINDING_POLICY))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }
        /* unpack the ppr */
        n = 1;
        if (ORTE_SUCCESS != (rc = opal_dss_unpack_buffer(buffer,
                                                         &(maps[i]->ppr), &n, OPAL_STRING))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }
        /* unpack the cpus/rank */
        n = 1;
        if (ORTE_SUCCESS != (rc = opal_dss_unpack_buffer(buffer,
                                                         &(maps[i]->cpus_per_rank), &n, OPAL_INT16))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }
        /* unpack the display map flag */
        n = 1;
        if (ORTE_SUCCESS != (rc = opal_dss_unpack_buffer(buffer,
                                                         &(maps[i]->display_map), &n, OPAL_BOOL))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }
        /* unpack the number of nodes involved in the job */
        n = 1;
        if (ORTE_SUCCESS != (rc = opal_dss_unpack_buffer(buffer,
                                                         &(maps[i]->num_nodes), &n, OPAL_UINT32))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }
    }

    return ORTE_SUCCESS;
}

/*
 * RML_TAG
 */
int orte_dt_unpack_tag(opal_buffer_t *buffer, void *dest,
                       int32_t *num_vals, opal_data_type_t type)
{
    int ret;

    /* Turn around and unpack the real type */
    if (ORTE_SUCCESS != (ret = opal_dss_unpack_buffer(buffer, dest, num_vals, ORTE_RML_TAG_T))) {
        ORTE_ERROR_LOG(ret);
    }

    return ret;
}

/*
 * ORTE_DAEMON_CMD
 */
int orte_dt_unpack_daemon_cmd(opal_buffer_t *buffer, void *dest, int32_t *num_vals,
                              opal_data_type_t type)
{
    int ret;

    /* turn around and unpack the real type */
    ret = opal_dss_unpack_buffer(buffer, dest, num_vals, ORTE_DAEMON_CMD_T);

    return ret;
}

/*
 * ORTE_IOF_TAG
 */
int orte_dt_unpack_iof_tag(opal_buffer_t *buffer, void *dest, int32_t *num_vals,
                           opal_data_type_t type)
{
    int ret;

    /* turn around and unpack the real type */
    ret = opal_dss_unpack_buffer(buffer, dest, num_vals, ORTE_IOF_TAG_T);

    return ret;
}

/*
 * ORTE_ATTR
 */

int orte_dt_unpack_attr(opal_buffer_t *buffer, void *dest, int32_t *num_vals,
                        opal_data_type_t type)
{
    orte_attribute_t **ptr;
    int32_t i, n, m;
    int ret;

    ptr = (orte_attribute_t **) dest;
    n = *num_vals;

    for (i = 0; i < n; ++i) {
        /* allocate the new object */
        ptr[i] = OBJ_NEW(orte_attribute_t);
        if (NULL == ptr[i]) {
            return OPAL_ERR_OUT_OF_RESOURCE;
        }
        /* unpack the key and type */
        m=1;
        if (OPAL_SUCCESS != (ret = opal_dss_unpack_buffer(buffer, &ptr[i]->key, &m, ORTE_ATTR_KEY_T))) {
            return ret;
        }
        m=1;
        if (OPAL_SUCCESS != (ret = opal_dss_unpack_buffer(buffer, &ptr[i]->type, &m, OPAL_DATA_TYPE))) {
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
        case OPAL_TIMEVAL:
            if (OPAL_SUCCESS != (ret = opal_dss_unpack_buffer(buffer, &ptr[i]->data.tv, &m, OPAL_TIMEVAL))) {
                return ret;
            }
            break;
        case OPAL_VPID:
            if (OPAL_SUCCESS != (ret = opal_dss_unpack_buffer(buffer, &ptr[i]->data.vpid, &m, ORTE_VPID))) {
                return ret;
            }
            break;
        case OPAL_JOBID:
            if (OPAL_SUCCESS != (ret = opal_dss_unpack_buffer(buffer, &ptr[i]->data.jobid, &m, ORTE_JOBID))) {
                return ret;
            }
            break;
        case OPAL_NAME:
            if (OPAL_SUCCESS != (ret = opal_dss_unpack_buffer(buffer, &ptr[i]->data.name, &m, ORTE_NAME))) {
                return ret;
            }
            break;
        case OPAL_ENVAR:
            if (OPAL_SUCCESS != (ret = opal_dss_unpack_buffer(buffer, &ptr[i]->data.envar, &m, OPAL_ENVAR))) {
                return ret;
            }
            break;

        default:
            opal_output(0, "PACK-ORTE-ATTR: UNSUPPORTED TYPE");
            return OPAL_ERROR;
        }
    }

    return OPAL_SUCCESS;
}

int orte_dt_unpack_sig(opal_buffer_t *buffer, void *dest, int32_t *num_vals,
                       opal_data_type_t type)
{
    orte_grpcomm_signature_t **ptr;
    int32_t i, n, cnt;
    int rc;

    ptr = (orte_grpcomm_signature_t **) dest;
    n = *num_vals;

    for (i = 0; i < n; ++i) {
        /* allocate the new object */
        ptr[i] = OBJ_NEW(orte_grpcomm_signature_t);
        if (NULL == ptr[i]) {
            return OPAL_ERR_OUT_OF_RESOURCE;
        }
        /* unpack the #procs */
        cnt = 1;
        if (OPAL_SUCCESS != (rc = opal_dss.unpack(buffer, &ptr[i]->sz, &cnt, OPAL_SIZE))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }
        if (0 < ptr[i]->sz) {
            /* allocate space for the array */
            ptr[i]->signature = (orte_process_name_t*)malloc(ptr[i]->sz * sizeof(orte_process_name_t));
            /* unpack the array - the array is our signature for the collective */
            cnt = ptr[i]->sz;
            if (OPAL_SUCCESS != (rc = opal_dss.unpack(buffer, ptr[i]->signature, &cnt, ORTE_NAME))) {
                ORTE_ERROR_LOG(rc);
                OBJ_RELEASE(ptr[i]);
                return rc;
            }
        }
    }
    return ORTE_SUCCESS;
}
