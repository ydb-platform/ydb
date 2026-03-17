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
 * Copyright (c) 2006-2013 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2009-2018 Cisco Systems, Inc.  All rights reserved
 * Copyright (c) 2011      Oak Ridge National Labs.  All rights reserved.
 * Copyright (c) 2013-2019 Intel, Inc.  All rights reserved.
 * Copyright (c) 2014      Mellanox Technologies, Inc.
 *                         All rights reserved.
 * Copyright (c) 2014-2016 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2017      IBM Corporation.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 */

#include "orte_config.h"

#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#include <fcntl.h>

#include "opal_stdint.h"
#include "opal/types.h"
#include "opal/util/argv.h"
#include "opal/util/output.h"
#include "opal/util/error.h"
#include "opal/mca/hwloc/base/base.h"
#include "opal/mca/pmix/pmix.h"

#include "orte/util/name_fns.h"
#include "orte/runtime/orte_globals.h"
#include "orte/runtime/orte_wait.h"
#include "orte/mca/errmgr/errmgr.h"
#include "orte/mca/rmaps/base/base.h"

#include "pmix_server_internal.h"
#include "pmix_server.h"

static void mycbfunc(int status, void *cbdata);

/* stuff proc attributes for sending back to a proc */
int orte_pmix_server_register_nspace(orte_job_t *jdata, bool force)
{
    int rc;
    orte_proc_t *pptr;
    int i, k, n;
    opal_list_t *info, *pmap;
    opal_value_t *kv;
    orte_node_t *node, *mynode;
    opal_vpid_t vpid;
    char **list, **procs, **micro, *tmp, *regex;
    orte_job_t *dmns;
    orte_job_map_t *map;
    orte_app_context_t *app;
    uid_t uid;
    gid_t gid;
    opal_list_t *cache;
    hwloc_obj_t machine;
    opal_buffer_t buf, bucket;
    opal_byte_object_t bo, *boptr;
    orte_proc_t *proc;

    opal_output_verbose(2, orte_pmix_server_globals.output,
                        "%s register nspace for %s",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                        ORTE_JOBID_PRINT(jdata->jobid));

    /* setup the info list */
    info = OBJ_NEW(opal_list_t);
    uid = geteuid();
    gid = getegid();

    /* pass our nspace/rank */
    kv = OBJ_NEW(opal_value_t);
    kv->key = strdup(OPAL_PMIX_SERVER_NSPACE);
    kv->data.string = strdup(ORTE_JOBID_PRINT(ORTE_PROC_MY_NAME->jobid));
    kv->type = OPAL_STRING;
    opal_list_append(info, &kv->super);

    kv = OBJ_NEW(opal_value_t);
    kv->key = strdup(OPAL_PMIX_SERVER_RANK);
    kv->data.uint32 = ORTE_PROC_MY_NAME->vpid;
    kv->type = OPAL_UINT32;
    opal_list_append(info, &kv->super);

    /* jobid */
    kv = OBJ_NEW(opal_value_t);
    kv->key = strdup(OPAL_PMIX_JOBID);
    kv->data.string = strdup(ORTE_JOBID_PRINT(jdata->jobid));
    kv->type = OPAL_STRING;
    opal_list_append(info, &kv->super);

    /* offset */
    kv = OBJ_NEW(opal_value_t);
    kv->key = strdup(OPAL_PMIX_NPROC_OFFSET);
    kv->data.uint32 = jdata->offset;
    kv->type = OPAL_UINT32;
    opal_list_append(info, &kv->super);

    /* check for cached values to add to the job info */
    cache = NULL;
    if (orte_get_attribute(&jdata->attributes, ORTE_JOB_INFO_CACHE, (void**)&cache, OPAL_PTR) &&
        NULL != cache) {
        while (NULL != (kv = (opal_value_t*)opal_list_remove_first(cache))) {
            opal_list_append(info, &kv->super);
        }
        orte_remove_attribute(&jdata->attributes, ORTE_JOB_INFO_CACHE);
        OBJ_RELEASE(cache);
    }

    /* assemble the node and proc map info */
    list = NULL;
    procs = NULL;
    map = jdata->map;
    for (i=0; i < map->nodes->size; i++) {
        micro = NULL;
        if (NULL != (node = (orte_node_t*)opal_pointer_array_get_item(map->nodes, i))) {
            opal_argv_append_nosize(&list, node->name);
            /* assemble all the ranks for this job that are on this node */
            for (k=0; k < node->procs->size; k++) {
                if (NULL != (pptr = (orte_proc_t*)opal_pointer_array_get_item(node->procs, k))) {
                    if (jdata->jobid == pptr->name.jobid) {
                        opal_argv_append_nosize(&micro, ORTE_VPID_PRINT(pptr->name.vpid));
                    }
                }
            }
            /* assemble the rank/node map */
            if (NULL != micro) {
                tmp = opal_argv_join(micro, ',');
                opal_argv_free(micro);
                opal_argv_append_nosize(&procs, tmp);
                free(tmp);
            }
        }
    }
    /* let the PMIx server generate the nodemap regex */
    if (NULL != list) {
        tmp = opal_argv_join(list, ',');
        opal_argv_free(list);
        list = NULL;
        if (OPAL_SUCCESS != (rc = opal_pmix.generate_regex(tmp, &regex))) {
            ORTE_ERROR_LOG(rc);
            free(tmp);
            OPAL_LIST_RELEASE(info);
            return rc;
        }
        free(tmp);
        kv = OBJ_NEW(opal_value_t);
        kv->key = strdup(OPAL_PMIX_NODE_MAP);
        kv->type = OPAL_STRING;
        kv->data.string = regex;
        opal_list_append(info, &kv->super);
    }

    /* let the PMIx server generate the procmap regex */
    if (NULL != procs) {
        tmp = opal_argv_join(procs, ';');
        opal_argv_free(procs);
        procs = NULL;
        if (OPAL_SUCCESS != (rc = opal_pmix.generate_ppn(tmp, &regex))) {
            ORTE_ERROR_LOG(rc);
            free(tmp);
            OPAL_LIST_RELEASE(info);
            return rc;
        }
        free(tmp);
        kv = OBJ_NEW(opal_value_t);
        kv->key = strdup(OPAL_PMIX_PROC_MAP);
        kv->type = OPAL_STRING;
        kv->data.string = regex;
        opal_list_append(info, &kv->super);
    }

    /* get our local node */
    if (NULL == (dmns = orte_get_job_data_object(ORTE_PROC_MY_NAME->jobid))) {
        ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
        OPAL_LIST_RELEASE(info);
        return ORTE_ERR_NOT_FOUND;
    }
    if (NULL == (pptr = (orte_proc_t*)opal_pointer_array_get_item(dmns->procs, ORTE_PROC_MY_NAME->vpid))) {
        ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
        OPAL_LIST_RELEASE(info);
        return ORTE_ERR_NOT_FOUND;
    }
    mynode = pptr->node;
    if (NULL == mynode) {
        /* cannot happen */
        ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
        OPAL_LIST_RELEASE(info);
        return ORTE_ERR_NOT_FOUND;
    }
    /* pass our node ID */
    kv = OBJ_NEW(opal_value_t);
    kv->key = strdup(OPAL_PMIX_NODEID);
    kv->type = OPAL_UINT32;
    kv->data.uint32 = mynode->index;
    opal_list_append(info, &kv->super);

    /* pass our node size */
    kv = OBJ_NEW(opal_value_t);
    kv->key = strdup(OPAL_PMIX_NODE_SIZE);
    kv->type = OPAL_UINT32;
    kv->data.uint32 = mynode->num_procs;
    opal_list_append(info, &kv->super);

    /* pass the number of nodes in the job */
    kv = OBJ_NEW(opal_value_t);
    kv->key = strdup(OPAL_PMIX_NUM_NODES);
    kv->type = OPAL_UINT32;
    kv->data.uint32 = map->num_nodes;
    opal_list_append(info, &kv->super);

    /* univ size */
    kv = OBJ_NEW(opal_value_t);
    kv->key = strdup(OPAL_PMIX_UNIV_SIZE);
    kv->type = OPAL_UINT32;
    kv->data.uint32 = jdata->total_slots_alloc;
    opal_list_append(info, &kv->super);

    /* job size */
    kv = OBJ_NEW(opal_value_t);
    kv->key = strdup(OPAL_PMIX_JOB_SIZE);
    kv->type = OPAL_UINT32;
    kv->data.uint32 = jdata->num_procs;
    opal_list_append(info, &kv->super);

    /* number of apps in this job */
    kv = OBJ_NEW(opal_value_t);
    kv->key = strdup(OPAL_PMIX_JOB_NUM_APPS);
    kv->type = OPAL_UINT32;
    kv->data.uint32 = jdata->num_apps;
    opal_list_append(info, &kv->super);

    /* local size */
    kv = OBJ_NEW(opal_value_t);
    kv->key = strdup(OPAL_PMIX_LOCAL_SIZE);
    kv->type = OPAL_UINT32;
    kv->data.uint32 = jdata->num_local_procs;
    opal_list_append(info, &kv->super);

    /* max procs */
    kv = OBJ_NEW(opal_value_t);
    kv->key = strdup(OPAL_PMIX_MAX_PROCS);
    kv->type = OPAL_UINT32;
    kv->data.uint32 = jdata->total_slots_alloc;
    opal_list_append(info, &kv->super);

    /* topology signature */
    kv = OBJ_NEW(opal_value_t);
    kv->key = strdup(OPAL_PMIX_TOPOLOGY_SIGNATURE);
    kv->type = OPAL_STRING;
    kv->data.string = strdup(orte_topo_signature);
    opal_list_append(info, &kv->super);

    /* total available physical memory */
    machine = hwloc_get_next_obj_by_type (opal_hwloc_topology, HWLOC_OBJ_MACHINE, NULL);
    if (NULL != machine) {
        kv = OBJ_NEW(opal_value_t);
        kv->key = strdup(OPAL_PMIX_AVAIL_PHYS_MEMORY);
        kv->type = OPAL_UINT64;
#if HWLOC_API_VERSION < 0x20000
        kv->data.uint64 = machine->memory.total_memory;
#else
        kv->data.uint64 = machine->total_memory;
#endif
        opal_list_append(info, &kv->super);
    }

    /* pass the mapping policy used for this job */
    kv = OBJ_NEW(opal_value_t);
    kv->key = strdup(OPAL_PMIX_MAPBY);
    kv->type = OPAL_STRING;
    kv->data.string = strdup(orte_rmaps_base_print_mapping(jdata->map->mapping));
    opal_list_append(info, &kv->super);

    /* pass the ranking policy used for this job */
    kv = OBJ_NEW(opal_value_t);
    kv->key = strdup(OPAL_PMIX_RANKBY);
    kv->type = OPAL_STRING;
    kv->data.string = strdup(orte_rmaps_base_print_ranking(jdata->map->ranking));
    opal_list_append(info, &kv->super);

    /* pass the binding policy used for this job */
    kv = OBJ_NEW(opal_value_t);
    kv->key = strdup(OPAL_PMIX_BINDTO);
    kv->type = OPAL_STRING;
    kv->data.string = strdup(opal_hwloc_base_print_binding(jdata->map->binding));
    opal_list_append(info, &kv->super);



    /* register any local clients */
    vpid = ORTE_VPID_MAX;
    micro = NULL;
    for (i=0; i < mynode->procs->size; i++) {
        if (NULL == (pptr = (orte_proc_t*)opal_pointer_array_get_item(mynode->procs, i))) {
            continue;
        }
        if (pptr->name.jobid == jdata->jobid) {
            opal_argv_append_nosize(&micro, ORTE_VPID_PRINT(pptr->name.vpid));
            if (pptr->name.vpid < vpid) {
                vpid = pptr->name.vpid;
            }
            /* go ahead and register this client */
            if (OPAL_SUCCESS != (rc = opal_pmix.server_register_client(&pptr->name, uid, gid,
                                                                       (void*)pptr, NULL, NULL))) {
                ORTE_ERROR_LOG(rc);
            }
        }
    }
    if (NULL != micro) {
        /* pass the local peers */
        kv = OBJ_NEW(opal_value_t);
        kv->key = strdup(OPAL_PMIX_LOCAL_PEERS);
        kv->type = OPAL_STRING;
        kv->data.string = opal_argv_join(micro, ',');
        opal_argv_free(micro);
        opal_list_append(info, &kv->super);
    }

    /* pass the local ldr */
    kv = OBJ_NEW(opal_value_t);
    kv->key = strdup(OPAL_PMIX_LOCALLDR);
    kv->type = OPAL_VPID;
    kv->data.name.vpid = vpid;
    opal_list_append(info, &kv->super);

    /* for each proc in this job, create an object that
     * includes the info describing the proc so the recipient has a complete
     * picture. This allows procs to connect to each other without
     * any further info exchange, assuming the underlying transports
     * support it. We also pass all the proc-specific data here so
     * that each proc can lookup info about every other proc in the job */

    for (n=0; n < map->nodes->size; n++) {
        if (NULL == (node = (orte_node_t*)opal_pointer_array_get_item(map->nodes, n))) {
            continue;
        }
        /* cycle across each proc on this node, passing all data that
         * varies by proc */
        for (i=0; i < node->procs->size; i++) {
            if (NULL == (pptr = (orte_proc_t*)opal_pointer_array_get_item(node->procs, i))) {
                continue;
            }
            /* only consider procs from this job */
            if (pptr->name.jobid != jdata->jobid) {
                continue;
            }
            /* setup the proc map object */
            kv = OBJ_NEW(opal_value_t);
            kv->key = strdup(OPAL_PMIX_PROC_DATA);
            kv->type = OPAL_PTR;
            kv->data.ptr = OBJ_NEW(opal_list_t);
            opal_list_append(info, &kv->super);
            pmap = kv->data.ptr;

            /* must start with rank */
            kv = OBJ_NEW(opal_value_t);
            kv->key = strdup(OPAL_PMIX_RANK);
            kv->type = OPAL_VPID;
            kv->data.name.vpid = pptr->name.vpid;
            opal_list_append(pmap, &kv->super);

            /* location, for local procs */
            if (node == mynode) {
                tmp = NULL;
                if (orte_get_attribute(&pptr->attributes, ORTE_PROC_CPU_BITMAP, (void**)&tmp, OPAL_STRING) &&
                    NULL != tmp) {
                    kv = OBJ_NEW(opal_value_t);
                    kv->key = strdup(OPAL_PMIX_LOCALITY_STRING);
                    kv->type = OPAL_STRING;
                    kv->data.string = opal_hwloc_base_get_locality_string(opal_hwloc_topology, tmp);
                    opal_list_append(pmap, &kv->super);
                    free(tmp);
                } else {
                    /* the proc is not bound */
                    kv = OBJ_NEW(opal_value_t);
                    kv->key = strdup(OPAL_PMIX_LOCALITY_STRING);
                    kv->type = OPAL_STRING;
                    kv->data.string = NULL;
                    opal_list_append(pmap, &kv->super);
                }
            }

            /* global/univ rank */
            kv = OBJ_NEW(opal_value_t);
            kv->key = strdup(OPAL_PMIX_GLOBAL_RANK);
            kv->type = OPAL_VPID;
            kv->data.name.vpid = pptr->name.vpid + jdata->offset;
            opal_list_append(pmap, &kv->super);

            if (1 < jdata->num_apps) {
                /* appnum */
                kv = OBJ_NEW(opal_value_t);
                kv->key = strdup(OPAL_PMIX_APPNUM);
                kv->type = OPAL_UINT32;
                kv->data.uint32 = pptr->app_idx;
                opal_list_append(pmap, &kv->super);

                /* app ldr */
                app = (orte_app_context_t*)opal_pointer_array_get_item(jdata->apps, pptr->app_idx);
                kv = OBJ_NEW(opal_value_t);
                kv->key = strdup(OPAL_PMIX_APPLDR);
                kv->type = OPAL_VPID;
                kv->data.name.vpid = app->first_rank;
                opal_list_append(pmap, &kv->super);

                /* app rank */
                kv = OBJ_NEW(opal_value_t);
                kv->key = strdup(OPAL_PMIX_APP_RANK);
                kv->type = OPAL_VPID;
                kv->data.name.vpid = pptr->app_rank;
                opal_list_append(pmap, &kv->super);

                /* app size */
                kv = OBJ_NEW(opal_value_t);
                kv->key = strdup(OPAL_PMIX_APP_SIZE);
                kv->type = OPAL_UINT32;
                kv->data.uint32 = app->num_procs;
                opal_list_append(info, &kv->super);
            }

            /* local rank */
            kv = OBJ_NEW(opal_value_t);
            kv->key = strdup(OPAL_PMIX_LOCAL_RANK);
            kv->type = OPAL_UINT16;
            kv->data.uint16 = pptr->local_rank;
            opal_list_append(pmap, &kv->super);

            /* node rank */
            kv = OBJ_NEW(opal_value_t);
            kv->key = strdup(OPAL_PMIX_NODE_RANK);
            kv->type = OPAL_UINT16;
            kv->data.uint32 = pptr->node_rank;
            opal_list_append(pmap, &kv->super);

            /* node ID */
            kv = OBJ_NEW(opal_value_t);
            kv->key = strdup(OPAL_PMIX_NODEID);
            kv->type = OPAL_UINT32;
            kv->data.uint32 = pptr->node->index;
            opal_list_append(pmap, &kv->super);

            if (map->num_nodes < orte_hostname_cutoff) {
                kv = OBJ_NEW(opal_value_t);
                kv->key = strdup(OPAL_PMIX_HOSTNAME);
                kv->type = OPAL_STRING;
                kv->data.string = strdup(pptr->node->name);
                opal_list_append(pmap, &kv->super);
            }
        }
    }

    /* mark the job as registered */
    orte_set_attribute(&jdata->attributes, ORTE_JOB_NSPACE_REGISTERED, ORTE_ATTR_LOCAL, NULL, OPAL_BOOL);

    /* pass it down */
    /* we are in an event, so no need to callback */
    rc = opal_pmix.server_register_nspace(jdata->jobid,
                                          jdata->num_local_procs,
                                          info, NULL, NULL);
    OPAL_LIST_RELEASE(info);
    if (OPAL_SUCCESS != rc) {
        return rc;
    }

    /* if I am the HNP and this job is a member of my family, then we must
     * assume there could be some cross-mpirun exchange, and so
     * we protect against that situation by publishing the job info
     * for this job - this allows any subsequent "connect" to retrieve
     * the job info */

    if (ORTE_PROC_IS_HNP && ORTE_JOB_FAMILY(ORTE_PROC_MY_NAME->jobid) == ORTE_JOB_FAMILY(jdata->jobid)) {
        /* pack the job - note that this doesn't include the procs
         * or their locations */
        OBJ_CONSTRUCT(&buf, opal_buffer_t);
        if (OPAL_SUCCESS != (rc = opal_dss.pack(&buf, &jdata, 1, ORTE_JOB))) {
            ORTE_ERROR_LOG(rc);
            OBJ_DESTRUCT(&buf);
            return rc;
        }

        /* pack the hostname, daemon vpid and contact URI for each involved node */
        map = jdata->map;
        OBJ_CONSTRUCT(&bucket, opal_buffer_t);
        for (i=0; i < map->nodes->size; i++) {
            if (NULL == (node = (orte_node_t*)opal_pointer_array_get_item(map->nodes, i))) {
                continue;
            }
            opal_dss.pack(&bucket, &node->daemon->rml_uri, 1, OPAL_STRING);
        }
        opal_dss.unload(&bucket, (void**)&bo.bytes, &bo.size);
        boptr = &bo;
        opal_dss.pack(&buf, &boptr, 1, OPAL_BYTE_OBJECT);

        /* pack the proc name and daemon vpid for each proc */
        OBJ_CONSTRUCT(&bucket, opal_buffer_t);
        for (i=0; i < jdata->procs->size; i++) {
            if (NULL == (proc = (orte_proc_t*)opal_pointer_array_get_item(jdata->procs, i))) {
                continue;
            }
            opal_dss.pack(&bucket, &proc->name, 1, ORTE_NAME);
            opal_dss.pack(&bucket, &proc->node->daemon->name, 1, ORTE_NAME);
        }
        opal_dss.unload(&bucket, (void**)&bo.bytes, &bo.size);
        boptr = &bo;
        opal_dss.pack(&buf, &boptr, 1, OPAL_BYTE_OBJECT);

        info = OBJ_NEW(opal_list_t);
        /* create a key-value with the key being the string jobid
         * and the value being the byte object */
        kv = OBJ_NEW(opal_value_t);
        orte_util_convert_jobid_to_string(&kv->key, jdata->jobid);
        kv->type = OPAL_BYTE_OBJECT;
        opal_dss.unload(&buf, (void**)&kv->data.bo.bytes, &kv->data.bo.size);
        OBJ_DESTRUCT(&buf);
        opal_list_append(info, &kv->super);

        /* set the range to be session */
        kv = OBJ_NEW(opal_value_t);
        kv->key = strdup(OPAL_PMIX_RANGE);
        kv->type = OPAL_UINT;
        kv->data.uint = OPAL_PMIX_RANGE_SESSION;
        opal_list_append(info, &kv->super);

        /* set the persistence to be app */
        kv = OBJ_NEW(opal_value_t);
        kv->key = strdup(OPAL_PMIX_PERSISTENCE);
        kv->type = OPAL_INT;
        kv->data.integer = OPAL_PMIX_PERSIST_APP;
        opal_list_append(info, &kv->super);

        /* add our effective userid to the directives */
        kv = OBJ_NEW(opal_value_t);
        kv->key = strdup(OPAL_PMIX_USERID);
        kv->type = OPAL_UINT32;
        kv->data.uint32 = geteuid();
        opal_list_append(info, &kv->super);

        /* now publish it */
        if (ORTE_SUCCESS != (rc = pmix_server_publish_fn(ORTE_PROC_MY_NAME,
                                                         info, mycbfunc, info))) {
            ORTE_ERROR_LOG(rc);
        }
    }

    return rc;
}

static void mycbfunc(int status, void *cbdata)
{
    opal_list_t *info = (opal_list_t*)cbdata;

    if (ORTE_SUCCESS != status) {
        ORTE_ERROR_LOG(status);
    }
    OPAL_LIST_RELEASE(info);
}
