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

#include "opal/util/argv.h"
#include "opal/dss/dss.h"
#include "opal/dss/dss_internal.h"
#include "opal/mca/hwloc/base/base.h"
#include "opal/class/opal_pointer_array.h"

#include "orte/mca/errmgr/errmgr.h"
#include "opal/dss/dss.h"
#include "opal/dss/dss_internal.h"
#include "orte/runtime/data_type_support/orte_dt_support.h"

/*
 * ORTE_STD_CNTR
 */
int orte_dt_pack_std_cntr(opal_buffer_t *buffer, const void *src,
                            int32_t num_vals, opal_data_type_t type)
{
    int ret;

    /* Turn around and pack the real type */
    if (ORTE_SUCCESS != (
                         ret = opal_dss_pack_buffer(buffer, src, num_vals, ORTE_STD_CNTR_T))) {
        ORTE_ERROR_LOG(ret);
    }

    return ret;
}

/*
 * JOB
 * NOTE: We do not pack all of the job object's fields as many of them have no
 * value in sending them to another location. The only purpose in packing and
 * sending a job object is to communicate the data required to dynamically
 * spawn another job - so we only pack that limited set of required data
 */
int orte_dt_pack_job(opal_buffer_t *buffer, const void *src,
                     int32_t num_vals, opal_data_type_t type)
{
    int rc;
    int32_t i, j, count, bookmark;
    orte_job_t **jobs;
    orte_app_context_t *app;
    orte_proc_t *proc;
    orte_attribute_t *kv;
    opal_list_t *cache;
    opal_value_t *val;

    /* array of pointers to orte_job_t objects - need to pack the objects a set of fields at a time */
    jobs = (orte_job_t**) src;

    for (i=0; i < num_vals; i++) {
        /* pack the jobid */
        if (ORTE_SUCCESS != (rc = opal_dss_pack_buffer(buffer,
                        (void*)(&(jobs[i]->jobid)), 1, ORTE_JOBID))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }
        /* pack the flags */
        if (ORTE_SUCCESS != (rc = opal_dss_pack_buffer(buffer,
                        (void*)(&(jobs[i]->flags)), 1, ORTE_JOB_FLAGS_T))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }

        /* pack the attributes that need to be sent */
        count = 0;
        OPAL_LIST_FOREACH(kv, &jobs[i]->attributes, orte_attribute_t) {
            if (ORTE_ATTR_GLOBAL == kv->local) {
                ++count;
            }
        }
        if (ORTE_SUCCESS != (rc = opal_dss_pack_buffer(buffer, (void*)(&count), 1, ORTE_STD_CNTR))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }
        OPAL_LIST_FOREACH(kv, &jobs[i]->attributes, orte_attribute_t) {
            if (ORTE_ATTR_GLOBAL == kv->local) {
                if (ORTE_SUCCESS != (rc = opal_dss_pack_buffer(buffer, (void*)&kv, 1, ORTE_ATTRIBUTE))) {
                    ORTE_ERROR_LOG(rc);
                    return rc;
                }
            }
        }
        /* check for job info attribute */
        cache = NULL;
        if (orte_get_attribute(&jobs[i]->attributes, ORTE_JOB_INFO_CACHE, (void**)&cache, OPAL_PTR) &&
            NULL != cache) {
            /* we need to pack these as well, but they are composed
             * of opal_value_t's on a list. So first pack the number
             * of list elements */
            count = opal_list_get_size(cache);
            if (ORTE_SUCCESS != (rc = opal_dss_pack_buffer(buffer, (void*)(&count), 1, ORTE_STD_CNTR))) {
                ORTE_ERROR_LOG(rc);
                return rc;
            }
            /* now pack each element on the list */
            OPAL_LIST_FOREACH(val, cache, opal_value_t) {
                if (ORTE_SUCCESS != (rc = opal_dss_pack_buffer(buffer, (void*)&val, 1, OPAL_VALUE))) {
                    ORTE_ERROR_LOG(rc);
                    return rc;
                }
            }
        } else {
            /* pack a zero to indicate no job info is being passed */
            count = 0;
            if (ORTE_SUCCESS != (rc = opal_dss_pack_buffer(buffer, (void*)(&count), 1, ORTE_STD_CNTR))) {
                ORTE_ERROR_LOG(rc);
                return rc;
            }
        }

        /* pack the personality */
        count = opal_argv_count(jobs[i]->personality);
        if (ORTE_SUCCESS != (rc = opal_dss_pack_buffer(buffer, &count, 1, OPAL_INT32))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }
        for (j=0; j < count; j++) {
            if (ORTE_SUCCESS != (rc = opal_dss_pack_buffer(buffer, &jobs[i]->personality[j], 1, OPAL_STRING))) {
                ORTE_ERROR_LOG(rc);
                return rc;
            }
        }

        /* pack the number of apps */
        if (ORTE_SUCCESS != (rc = opal_dss_pack_buffer(buffer,
                         (void*)(&(jobs[i]->num_apps)), 1, ORTE_APP_IDX))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }

        /* if there are apps, pack the app_contexts */
        if (0 < jobs[i]->num_apps) {
            for (j=0; j < jobs[i]->apps->size; j++) {
                if (NULL == (app = (orte_app_context_t*)opal_pointer_array_get_item(jobs[i]->apps, j))) {
                    continue;
                }
                if (ORTE_SUCCESS != (rc = opal_dss_pack_buffer(buffer, (void*)&app, 1, ORTE_APP_CONTEXT))) {
                    ORTE_ERROR_LOG(rc);
                    return rc;
                }
            }
        }

        /* pack the number of procs and offset */
        if (ORTE_SUCCESS != (rc = opal_dss_pack_buffer(buffer,
                         (void*)(&(jobs[i]->num_procs)), 1, ORTE_VPID))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }
        if (ORTE_SUCCESS != (rc = opal_dss_pack_buffer(buffer,
                         (void*)(&(jobs[i]->offset)), 1, ORTE_VPID))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }

        if (0 < jobs[i]->num_procs) {
            /* check attributes to see if this job is to be fully
             * described in the launch msg */
            if (orte_get_attribute(&jobs[i]->attributes, ORTE_JOB_FULLY_DESCRIBED, NULL, OPAL_BOOL)) {
                for (j=0; j < jobs[i]->procs->size; j++) {
                    if (NULL == (proc = (orte_proc_t*)opal_pointer_array_get_item(jobs[i]->procs, j))) {
                        continue;
                    }
                    if (ORTE_SUCCESS != (rc = opal_dss_pack_buffer(buffer, (void*)&proc, 1, ORTE_PROC))) {
                        ORTE_ERROR_LOG(rc);
                        return rc;
                    }
                }
            }
        }

        /* pack the stdin target */
        if (ORTE_SUCCESS != (rc = opal_dss_pack_buffer(buffer,
                         (void*)(&(jobs[i]->stdin_target)), 1, ORTE_VPID))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }

        /* pack the total slots allocated to the job */
        if (ORTE_SUCCESS != (rc = opal_dss_pack_buffer(buffer,
                         (void*)(&(jobs[i]->total_slots_alloc)), 1, ORTE_STD_CNTR))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }

        /* if the map is NULL, then we cannot pack it as there is
         * nothing to pack. However, we have to flag whether or not
         * the map is included so the unpacking routine can know
         * what to do
         */
        if (NULL == jobs[i]->map) {
            /* pack a zero value */
            j=0;
        } else {
            /* pack a one to indicate a map is there */
            j = 1;
        }
        if (ORTE_SUCCESS != (rc = opal_dss_pack_buffer(buffer,
                            (void*)&j, 1, ORTE_STD_CNTR))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }

        /* pack the map - this will only pack the fields that control
         * HOW a job is to be mapped. We do -not- pack the mapped procs
         * or nodes as this info does not need to be transmitted
         */
        if (NULL != jobs[i]->map) {
            if (ORTE_SUCCESS != (rc = opal_dss_pack_buffer(buffer,
                             (void*)(&(jobs[i]->map)), 1, ORTE_JOB_MAP))) {
                ORTE_ERROR_LOG(rc);
                return rc;
            }
        }

        /* pack the bookmark */
        if (NULL == jobs[i]->bookmark) {
            bookmark = -1;
        } else {
            bookmark = jobs[i]->bookmark->index;
        }
        if (ORTE_SUCCESS != (rc = opal_dss_pack_buffer(buffer, &bookmark, 1, OPAL_INT32))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }

        /* pack the job state */
        if (ORTE_SUCCESS != (rc = opal_dss_pack_buffer(buffer,
                         (void*)(&(jobs[i]->state)), 1, ORTE_JOB_STATE))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }
    }
    return ORTE_SUCCESS;
}

/*
 *  NODE
 */
int orte_dt_pack_node(opal_buffer_t *buffer, const void *src,
                      int32_t num_vals, opal_data_type_t type)
{
    int rc;
    int32_t i, count;
    orte_node_t **nodes;
    uint8_t flag;
    orte_attribute_t *kv;

    /* array of pointers to orte_node_t objects - need to pack the objects a set of fields at a time */
    nodes = (orte_node_t**) src;

    for (i=0; i < num_vals; i++) {
        /* do not pack the index - it is meaningless on the other end */

        /* pack the node name */
        if (ORTE_SUCCESS != (rc = opal_dss_pack_buffer(buffer, (void*)(&(nodes[i]->name)), 1, OPAL_STRING))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }

        /* do not pack the daemon name or launch id */

        /* pack the number of procs on the node */
        if (ORTE_SUCCESS != (rc = opal_dss_pack_buffer(buffer, (void*)(&(nodes[i]->num_procs)), 1, ORTE_VPID))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }

        /* do not pack the procs */

        /* pack whether we are oversubscribed or not */
        flag = ORTE_FLAG_TEST(nodes[i], ORTE_NODE_FLAG_OVERSUBSCRIBED);
        if (ORTE_SUCCESS != (rc = opal_dss_pack_buffer(buffer, (void*)(&flag), 1, OPAL_UINT8))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }

        /* pack the state */
        if (ORTE_SUCCESS != (rc = opal_dss_pack_buffer(buffer, (void*)(&(nodes[i]->state)), 1, ORTE_NODE_STATE))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }

        /* pack any shared attributes */
        count = 0;
        OPAL_LIST_FOREACH(kv, &nodes[i]->attributes, orte_attribute_t) {
            if (ORTE_ATTR_GLOBAL == kv->local) {
                ++count;
            }
        }
        if (ORTE_SUCCESS != (rc = opal_dss_pack_buffer(buffer, (void*)(&count), 1, ORTE_STD_CNTR))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }
        OPAL_LIST_FOREACH(kv, &nodes[i]->attributes, orte_attribute_t) {
            if (ORTE_ATTR_GLOBAL == kv->local) {
                if (ORTE_SUCCESS != (rc = opal_dss_pack_buffer(buffer, (void*)&kv, 1, ORTE_ATTRIBUTE))) {
                    ORTE_ERROR_LOG(rc);
                    return rc;
                }
            }
        }
    }
    return ORTE_SUCCESS;
}

/*
 * PROC
 */
int orte_dt_pack_proc(opal_buffer_t *buffer, const void *src,
                      int32_t num_vals, opal_data_type_t type)
{
    int rc;
    int32_t i, count;
    orte_proc_t **procs;
    orte_attribute_t *kv;

    /* array of pointers to orte_proc_t objects - need to pack the objects a set of fields at a time */
    procs = (orte_proc_t**) src;

    for (i=0; i < num_vals; i++) {
        /* pack the name */
        if (ORTE_SUCCESS != (rc = opal_dss_pack_buffer(buffer,
                         (void*)(&(procs[i]->name)), 1, ORTE_NAME))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }

        /* pack the daemon/node it is on */
        if (ORTE_SUCCESS != (rc = opal_dss_pack_buffer(buffer,
                         (void*)(&(procs[i]->parent)), 1, ORTE_VPID))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }

        /* pack the local rank */
        if (ORTE_SUCCESS != (rc = opal_dss_pack_buffer(buffer,
                         (void*)(&(procs[i]->local_rank)), 1, ORTE_LOCAL_RANK))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }

        /* pack the node rank */
        if (ORTE_SUCCESS != (rc = opal_dss_pack_buffer(buffer,
                         (void*)(&(procs[i]->node_rank)), 1, ORTE_NODE_RANK))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }

        /* pack the state */
        if (ORTE_SUCCESS != (rc = opal_dss_pack_buffer(buffer,
                         (void*)(&(procs[i]->state)), 1, ORTE_PROC_STATE))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }

        /* pack the app context index */
        if (ORTE_SUCCESS != (rc = opal_dss_pack_buffer(buffer,
                         (void*)(&(procs[i]->app_idx)), 1, ORTE_STD_CNTR))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }

        /* pack the app rank */
        if (ORTE_SUCCESS != (rc = opal_dss_pack_buffer(buffer,
                         (void*)(&(procs[i]->app_rank)), 1, OPAL_UINT32))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }

        /* pack the attributes that will go */
        count = 0;
        OPAL_LIST_FOREACH(kv, &procs[i]->attributes, orte_attribute_t) {
            if (ORTE_ATTR_GLOBAL == kv->local) {
                ++count;
            }
        }
        if (ORTE_SUCCESS != (rc = opal_dss_pack_buffer(buffer, (void*)(&count), 1, ORTE_STD_CNTR))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }
        OPAL_LIST_FOREACH(kv, &procs[i]->attributes, orte_attribute_t) {
            if (ORTE_ATTR_GLOBAL == kv->local) {
                if (ORTE_SUCCESS != (rc = opal_dss_pack_buffer(buffer, (void*)&kv, 1, ORTE_ATTRIBUTE))) {
                    ORTE_ERROR_LOG(rc);
                    return rc;
                }
            }
        }
    }

    return ORTE_SUCCESS;
}

/*
 * APP CONTEXT
 */
int orte_dt_pack_app_context(opal_buffer_t *buffer, const void *src,
                             int32_t num_vals, opal_data_type_t type)
{
    int rc;
    int32_t i, count;
    orte_app_context_t **app_context;
    orte_attribute_t *kv;

    /* array of pointers to orte_app_context objects - need to pack the objects a set of fields at a time */
    app_context = (orte_app_context_t**) src;

    for (i=0; i < num_vals; i++) {
        /* pack the application index (for multiapp jobs) */
        if (ORTE_SUCCESS != (rc = opal_dss_pack_buffer(buffer,
                                                       (void*)(&(app_context[i]->idx)), 1, ORTE_STD_CNTR))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }

        /* pack the application name */
        if (ORTE_SUCCESS != (rc = opal_dss_pack_buffer(buffer,
                                                       (void*)(&(app_context[i]->app)), 1, OPAL_STRING))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }

        /* pack the number of processes */
        if (ORTE_SUCCESS != (rc = opal_dss_pack_buffer(buffer,
                                                       (void*)(&(app_context[i]->num_procs)), 1, ORTE_STD_CNTR))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }

        /* pack the first rank for this app */
        if (ORTE_SUCCESS != (rc = opal_dss_pack_buffer(buffer,
                                                       (void*)(&(app_context[i]->first_rank)), 1, ORTE_VPID))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }

        /* pack the number of entries in the argv array */
        count = opal_argv_count(app_context[i]->argv);
        if (ORTE_SUCCESS != (rc = opal_dss_pack_buffer(buffer, (void*)(&count), 1, ORTE_STD_CNTR))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }

        /* if there are entries, pack the argv entries */
        if (0 < count) {
            if (ORTE_SUCCESS != (rc = opal_dss_pack_buffer(buffer,
                                                           (void*)(app_context[i]->argv), count, OPAL_STRING))) {
                ORTE_ERROR_LOG(rc);
                return rc;
            }
        }

        /* pack the number of entries in the enviro array */
        count = opal_argv_count(app_context[i]->env);
        if (ORTE_SUCCESS != (rc = opal_dss_pack_buffer(buffer, (void*)(&count), 1, ORTE_STD_CNTR))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }

        /* if there are entries, pack the enviro entries */
        if (0 < count) {
            if (ORTE_SUCCESS != (rc = opal_dss_pack_buffer(buffer,
                                                           (void*)(app_context[i]->env), count, OPAL_STRING))) {
                ORTE_ERROR_LOG(rc);
                return rc;
            }
        }

        /* pack the cwd */
        if (ORTE_SUCCESS != (rc = opal_dss_pack_buffer(buffer,
                                                       (void*)(&(app_context[i]->cwd)), 1, OPAL_STRING))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }

        /* pack attributes */
        count = 0;
        OPAL_LIST_FOREACH(kv, &app_context[i]->attributes, orte_attribute_t) {
            if (ORTE_ATTR_GLOBAL == kv->local) {
                ++count;
            }
        }
        if (ORTE_SUCCESS != (rc = opal_dss_pack_buffer(buffer, (void*)(&count), 1, ORTE_STD_CNTR))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }
        OPAL_LIST_FOREACH(kv, &app_context[i]->attributes, orte_attribute_t) {
            if (ORTE_ATTR_GLOBAL == kv->local) {
                if (ORTE_SUCCESS != (rc = opal_dss_pack_buffer(buffer, (void*)&kv, 1, ORTE_ATTRIBUTE))) {
                    ORTE_ERROR_LOG(rc);
                    return rc;
                }
            }
        }
    }

    return ORTE_SUCCESS;
}

/*
 * EXIT CODE
 */
int orte_dt_pack_exit_code(opal_buffer_t *buffer, const void *src,
                                 int32_t num_vals, opal_data_type_t type)
{
    int rc;

    if (ORTE_SUCCESS != (rc = opal_dss_pack_buffer(buffer, src, num_vals, ORTE_EXIT_CODE_T))) {
        ORTE_ERROR_LOG(rc);
    }

    return rc;
}

/*
 * NODE STATE
 */
int orte_dt_pack_node_state(opal_buffer_t *buffer, const void *src,
                                  int32_t num_vals, opal_data_type_t type)
{
    int rc;

    if (ORTE_SUCCESS != (rc = opal_dss_pack_buffer(buffer, src, num_vals, ORTE_NODE_STATE_T))) {
        ORTE_ERROR_LOG(rc);
    }

    return rc;
}

/*
 * PROC STATE
 */
int orte_dt_pack_proc_state(opal_buffer_t *buffer, const void *src,
                                  int32_t num_vals, opal_data_type_t type)
{
    int rc;

    if (ORTE_SUCCESS != (rc = opal_dss_pack_buffer(buffer, src, num_vals, ORTE_PROC_STATE_T))) {
        ORTE_ERROR_LOG(rc);
    }

    return rc;
}

/*
 * JOB STATE
 */
int orte_dt_pack_job_state(opal_buffer_t *buffer, const void *src,
                                 int32_t num_vals, opal_data_type_t type)
{
    int rc;

    if (ORTE_SUCCESS != (rc = opal_dss_pack_buffer(buffer, src, num_vals, ORTE_JOB_STATE_T))) {
        ORTE_ERROR_LOG(rc);
    }

    return rc;
}

/*
 * JOB_MAP
 * NOTE: There is no obvious reason to include all the node information when
 * sending a map
 */
int orte_dt_pack_map(opal_buffer_t *buffer, const void *src,
                             int32_t num_vals, opal_data_type_t type)
{
    int rc;
    int32_t i;
    orte_job_map_t **maps;

    /* array of pointers to orte_job_map_t objects - need to pack the objects a set of fields at a time */
    maps = (orte_job_map_t**) src;

    for (i=0; i < num_vals; i++) {
        /* pack the requested mapper */
        if (ORTE_SUCCESS != (rc = opal_dss_pack_buffer(buffer, &(maps[i]->req_mapper), 1, OPAL_STRING))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }
        /* pack the last mapper */
        if (ORTE_SUCCESS != (rc = opal_dss_pack_buffer(buffer, &(maps[i]->last_mapper), 1, OPAL_STRING))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }
        /* pack the policies */
        if (ORTE_SUCCESS != (rc = opal_dss_pack_buffer(buffer, &(maps[i]->mapping), 1, ORTE_MAPPING_POLICY))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }
        if (ORTE_SUCCESS != (rc = opal_dss_pack_buffer(buffer, &(maps[i]->ranking), 1, ORTE_RANKING_POLICY))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }
        if (ORTE_SUCCESS != (rc = opal_dss_pack_buffer(buffer, &(maps[i]->binding), 1, OPAL_BINDING_POLICY))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }
        /* pack any ppr */
        if (ORTE_SUCCESS != (rc = opal_dss_pack_buffer(buffer, &(maps[i]->ppr), 1, OPAL_STRING))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }
        /* pack the cpus/rank */
        if (ORTE_SUCCESS != (rc = opal_dss_pack_buffer(buffer, &(maps[i]->cpus_per_rank), 1, OPAL_INT16))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }
        /* pack the display map flag */
        if (ORTE_SUCCESS != (rc = opal_dss_pack_buffer(buffer, &(maps[i]->display_map), 1, OPAL_BOOL))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }
        /* pack the number of nodes involved in the job */
        if (ORTE_SUCCESS != (rc = opal_dss_pack_buffer(buffer, &(maps[i]->num_nodes), 1, OPAL_UINT32))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }

    }

    return ORTE_SUCCESS;
}

/*
 * RML TAG
 */
int orte_dt_pack_tag(opal_buffer_t *buffer, const void *src,
                           int32_t num_vals, opal_data_type_t type)
{
    int rc;

    /* Turn around and pack the real type */
    if (ORTE_SUCCESS != (rc = opal_dss_pack_buffer(buffer, src, num_vals, ORTE_RML_TAG_T))) {
        ORTE_ERROR_LOG(rc);
    }

    return rc;
}

/*
 * ORTE_DAEMON_CMD
 */
int orte_dt_pack_daemon_cmd(opal_buffer_t *buffer, const void *src, int32_t num_vals,
                              opal_data_type_t type)
{
    int ret;

    /* Turn around and pack the real type */
    if (ORTE_SUCCESS != (ret = opal_dss_pack_buffer(buffer, src, num_vals, ORTE_DAEMON_CMD_T))) {
        ORTE_ERROR_LOG(ret);
    }

    return ret;
}

/*
 * ORTE_IOF_TAG
 */
int orte_dt_pack_iof_tag(opal_buffer_t *buffer, const void *src, int32_t num_vals,
                         opal_data_type_t type)
{
    int ret;

    /* Turn around and pack the real type */
    if (ORTE_SUCCESS != (ret = opal_dss_pack_buffer(buffer, src, num_vals, ORTE_IOF_TAG_T))) {
        ORTE_ERROR_LOG(ret);
    }

    return ret;
}


/*
 * ORTE_ATTR
 */
int orte_dt_pack_attr(opal_buffer_t *buffer, const void *src, int32_t num_vals,
                      opal_data_type_t type)
{
    orte_attribute_t **ptr;
    int32_t i, n;
    int ret;

    ptr = (orte_attribute_t **) src;

    for (i = 0; i < num_vals; ++i) {
        /* pack the key and type */
        if (OPAL_SUCCESS != (ret = opal_dss_pack_buffer(buffer, &ptr[i]->key, 1, ORTE_ATTR_KEY_T))) {
            return ret;
        }
        if (OPAL_SUCCESS != (ret = opal_dss_pack_buffer(buffer, &ptr[i]->type, 1, OPAL_DATA_TYPE))) {
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
        case OPAL_TIMEVAL:
            if (OPAL_SUCCESS != (ret = opal_dss_pack_buffer(buffer, &ptr[i]->data.tv, 1, OPAL_TIMEVAL))) {
                return ret;
            }
            break;
        case OPAL_PTR:
            /* just ignore these values */
            break;
        case OPAL_VPID:
            if (OPAL_SUCCESS != (ret = opal_dss_pack_buffer(buffer, &ptr[i]->data.vpid, 1, ORTE_VPID))) {
                return ret;
            }
            break;
        case OPAL_JOBID:
            if (OPAL_SUCCESS != (ret = opal_dss_pack_buffer(buffer, &ptr[i]->data.jobid, 1, ORTE_JOBID))) {
                return ret;
            }
            break;
        case OPAL_NAME:
            if (OPAL_SUCCESS != (ret = opal_dss_pack_buffer(buffer, &ptr[i]->data.name, 1, ORTE_NAME))) {
                return ret;
            }
            break;
        case OPAL_ENVAR:
            if (OPAL_SUCCESS != (ret = opal_dss_pack_buffer(buffer, &ptr[i]->data.envar, 1, OPAL_ENVAR))) {
                return ret;
            }
            break;

        default:
            opal_output(0, "PACK-ORTE-ATTR: UNSUPPORTED TYPE %d", (int)ptr[i]->type);
            return OPAL_ERROR;
        }
    }

    return OPAL_SUCCESS;
}

/*
 * ORTE_SIGNATURE
 */
int orte_dt_pack_sig(opal_buffer_t *buffer, const void *src, int32_t num_vals,
                     opal_data_type_t type)
{
    orte_grpcomm_signature_t **ptr;
    int32_t i;
    int rc;

    ptr = (orte_grpcomm_signature_t **) src;

    for (i = 0; i < num_vals; ++i) {
        /* pack the #procs */
        if (OPAL_SUCCESS != (rc = opal_dss.pack(buffer, &ptr[i]->sz, 1, OPAL_SIZE))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }
        if (0 < ptr[i]->sz) {
            /* pack the array */
            if (OPAL_SUCCESS != (rc = opal_dss.pack(buffer, ptr[i]->signature, ptr[i]->sz, ORTE_NAME))) {
                ORTE_ERROR_LOG(rc);
                return rc;
            }
        }
    }

    return ORTE_SUCCESS;
}
