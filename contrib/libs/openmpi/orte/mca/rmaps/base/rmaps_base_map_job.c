/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2018 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2011-2018 Cisco Systems, Inc.  All rights reserved
 * Copyright (c) 2011-2012 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2014-2018 Intel, Inc. All rights reserved.
 * Copyright (c) 2016      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "orte_config.h"
#include "orte/constants.h"

#include <string.h>

#include "orte/mca/mca.h"
#include "opal/util/output.h"
#include "opal/mca/base/base.h"
#include "opal/mca/hwloc/base/base.h"
#include "opal/dss/dss.h"

#include "orte/mca/errmgr/errmgr.h"
#include "orte/runtime/orte_globals.h"
#include "orte/util/show_help.h"
#include "orte/util/threads.h"
#include "orte/mca/state/state.h"

#include "orte/mca/rmaps/base/base.h"
#include "orte/mca/rmaps/base/rmaps_private.h"


void orte_rmaps_base_map_job(int fd, short args, void *cbdata)
{
    orte_state_caddy_t *caddy = (orte_state_caddy_t*)cbdata;
    orte_job_t *jdata;
    orte_node_t *node;
    int rc, i, ppx = 0;
    bool did_map, given, pernode = false, persocket = false;
    orte_rmaps_base_selected_module_t *mod;
    orte_job_t *parent;
    orte_vpid_t nprocs;
    orte_app_context_t *app;
    bool inherit = false;

    ORTE_ACQUIRE_OBJECT(caddy);
    jdata = caddy->jdata;

    jdata->state = ORTE_JOB_STATE_MAP;

    opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                        "mca:rmaps: mapping job %s",
                        ORTE_JOBID_PRINT(jdata->jobid));

    /* if this is a dynamic job launch and they didn't explicitly
     * request inheritance, then don't inherit the launch directives */
    if (orte_get_attribute(&jdata->attributes, ORTE_JOB_LAUNCH_PROXY, NULL, OPAL_NAME)) {
        inherit = orte_rmaps_base.inherit;
        opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                            "mca:rmaps: dynamic job %s %s inherit launch directives",
                            ORTE_JOBID_PRINT(jdata->jobid),
                            inherit ? "will" : "will not");
    } else {
        /* initial launch always takes on MCA params */
        inherit = true;
    }

    if (inherit) {
        if (NULL == jdata->map->ppr && NULL != orte_rmaps_base.ppr) {
            jdata->map->ppr = strdup(orte_rmaps_base.ppr);
        }
        if (0 == jdata->map->cpus_per_rank) {
            jdata->map->cpus_per_rank = orte_rmaps_base.cpus_per_rank;
        }
    }
    if (NULL != jdata->map->ppr) {
        /* get the procs/object */
        ppx = strtoul(jdata->map->ppr, NULL, 10);
        if (NULL != strstr(jdata->map->ppr, "node")) {
            pernode = true;
        } else if (NULL != strstr(jdata->map->ppr, "socket")) {
            persocket = true;
        }
    }

    /* compute the number of procs and check validity */
    nprocs = 0;
    for (i=0; i < jdata->apps->size; i++) {
        if (NULL != (app = (orte_app_context_t*)opal_pointer_array_get_item(jdata->apps, i))) {
            if (0 == app->num_procs) {
                opal_list_t nodes;
                orte_std_cntr_t slots;
                OBJ_CONSTRUCT(&nodes, opal_list_t);
                orte_rmaps_base_get_target_nodes(&nodes, &slots, app, ORTE_MAPPING_BYNODE, true, true);
                slots = 0;
                if (pernode) {
                    slots = ppx * opal_list_get_size(&nodes);
                } else if (persocket) {
                    /* add in #sockets for each node */
                    OPAL_LIST_FOREACH(node, &nodes, orte_node_t) {
                        slots += ppx * opal_hwloc_base_get_nbobjs_by_type(node->topology->topo,
                                                                          HWLOC_OBJ_SOCKET, 0,
                                                                          OPAL_HWLOC_AVAILABLE);
                    }
                } else {
                    /* if we are in a managed allocation, then all is good - otherwise,
                     * we have to do a little more checking */
                    if (!orte_managed_allocation) {
                        /* if all the nodes have their slots given, then we are okay */
                        given = true;
                        OPAL_LIST_FOREACH(node, &nodes, orte_node_t) {
                            if (!ORTE_FLAG_TEST(node, ORTE_NODE_FLAG_SLOTS_GIVEN)) {
                                given = false;
                                break;
                            }
                        }
                        /* if -host or -hostfile was given, and the slots were not,
                         * then this is no longer allowed */
                        if (!given &&
                            (orte_get_attribute(&app->attributes, ORTE_APP_DASH_HOST, NULL, OPAL_STRING) ||
                             orte_get_attribute(&app->attributes, ORTE_APP_HOSTFILE, NULL, OPAL_STRING))) {
                            /* inform the user of the error */
                            orte_show_help("help-orte-rmaps-base.txt", "num-procs-not-specified", true);
                            OPAL_LIST_DESTRUCT(&nodes);
                            OBJ_RELEASE(caddy);
                            ORTE_ACTIVATE_JOB_STATE(jdata, ORTE_JOB_STATE_MAP_FAILED);
                            return;
                        }
                        OPAL_LIST_FOREACH(node, &nodes, orte_node_t) {
                            slots += node->slots;
                        }
                    }
                }
                app->num_procs = slots;
                OPAL_LIST_DESTRUCT(&nodes);
            }
            nprocs += app->num_procs;
        }
    }


    opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                        "mca:rmaps: setting mapping policies for job %s nprocs %d",
                        ORTE_JOBID_PRINT(jdata->jobid), (int)nprocs);

    if (inherit && !jdata->map->display_map) {
        jdata->map->display_map = orte_rmaps_base.display_map;
    }

    /* set the default mapping policy IFF it wasn't provided */
    if (!ORTE_MAPPING_POLICY_IS_SET(jdata->map->mapping)) {
        if (inherit && (ORTE_MAPPING_GIVEN & ORTE_GET_MAPPING_DIRECTIVE(orte_rmaps_base.mapping))) {
            opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                                "mca:rmaps mapping given by MCA param");
            jdata->map->mapping = orte_rmaps_base.mapping;
        } else {
            /* default based on number of procs */
            if (nprocs <= 2) {
                if (1 < orte_rmaps_base.cpus_per_rank) {
                    /* assigning multiple cpus to a rank requires that we map to
                     * objects that have multiple cpus in them, so default
                     * to byslot if nothing else was specified by the user.
                     */
                    opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                                        "mca:rmaps[%d] mapping not given - using byslot", __LINE__);
                    ORTE_SET_MAPPING_POLICY(jdata->map->mapping, ORTE_MAPPING_BYSLOT);
                } else if (opal_hwloc_use_hwthreads_as_cpus) {
                    opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                                        "mca:rmaps[%d] mapping not given - using byhwthread", __LINE__);
                    ORTE_SET_MAPPING_POLICY(jdata->map->mapping, ORTE_MAPPING_BYHWTHREAD);
                } else {
                    opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                                        "mca:rmaps[%d] mapping not given - using bycore", __LINE__);
                    ORTE_SET_MAPPING_POLICY(jdata->map->mapping, ORTE_MAPPING_BYCORE);
                }
            } else {
                /* if NUMA is available, map by that */
                if (NULL != hwloc_get_obj_by_type(opal_hwloc_topology, HWLOC_OBJ_NODE, 0)) {
                    opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                                        "mca:rmaps[%d] mapping not set by user - using bynuma", __LINE__);
                    ORTE_SET_MAPPING_POLICY(jdata->map->mapping, ORTE_MAPPING_BYNUMA);
                } else if (NULL != hwloc_get_obj_by_type(opal_hwloc_topology, HWLOC_OBJ_SOCKET, 0)) {
                    opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                                        "mca:rmaps[%d] mapping not set by user and no NUMA - using bysocket", __LINE__);
                    ORTE_SET_MAPPING_POLICY(jdata->map->mapping, ORTE_MAPPING_BYSOCKET);
                } else {
                    /* if we have neither, then just do by slot */
                    opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                                        "mca:rmaps[%d] mapping not given and no NUMA or sockets - using byslot", __LINE__);
                    ORTE_SET_MAPPING_POLICY(jdata->map->mapping, ORTE_MAPPING_BYSLOT);
                }
            }
        }
    }

    /* check for oversubscribe directives */
    if (!(ORTE_MAPPING_SUBSCRIBE_GIVEN & ORTE_GET_MAPPING_DIRECTIVE(jdata->map->mapping))) {
        if (!(ORTE_MAPPING_SUBSCRIBE_GIVEN & ORTE_GET_MAPPING_DIRECTIVE(orte_rmaps_base.mapping))) {
            ORTE_SET_MAPPING_DIRECTIVE(jdata->map->mapping, ORTE_MAPPING_NO_OVERSUBSCRIBE);
        } else if (ORTE_MAPPING_NO_OVERSUBSCRIBE & ORTE_GET_MAPPING_DIRECTIVE(orte_rmaps_base.mapping)) {
            ORTE_SET_MAPPING_DIRECTIVE(jdata->map->mapping, ORTE_MAPPING_NO_OVERSUBSCRIBE);
        } else {
            ORTE_UNSET_MAPPING_DIRECTIVE(jdata->map->mapping, ORTE_MAPPING_NO_OVERSUBSCRIBE);
            ORTE_SET_MAPPING_DIRECTIVE(jdata->map->mapping, ORTE_MAPPING_SUBSCRIBE_GIVEN);
        }
    }

    /* check for no-use-local directive */
    if (!(ORTE_MAPPING_LOCAL_GIVEN & ORTE_GET_MAPPING_DIRECTIVE(jdata->map->mapping))) {
        if (inherit && (ORTE_MAPPING_NO_USE_LOCAL & ORTE_GET_MAPPING_DIRECTIVE(orte_rmaps_base.mapping))) {
            ORTE_SET_MAPPING_DIRECTIVE(jdata->map->mapping, ORTE_MAPPING_NO_USE_LOCAL);
        }
    }

    /* we don't have logic to determine default rank policy, so
     * just inherit it if they didn't give us one */
    if (!ORTE_RANKING_POLICY_IS_SET(jdata->map->ranking)) {
        jdata->map->ranking = orte_rmaps_base.ranking;
    }

    /* define the binding policy for this job - if the user specified one
     * already (e.g., during the call to comm_spawn), then we don't
     * override it */
    if (!OPAL_BINDING_POLICY_IS_SET(jdata->map->binding)) {
        if (inherit && OPAL_BINDING_POLICY_IS_SET(opal_hwloc_binding_policy)) {
            /* if the user specified a default binding policy via
             * MCA param, then we use it - this can include a directive
             * to overload */
            opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                            "mca:rmaps[%d] binding policy given", __LINE__);
            jdata->map->binding = opal_hwloc_binding_policy;
        } else if (0 < jdata->map->cpus_per_rank) {
            /* bind to cpus */
            if (opal_hwloc_use_hwthreads_as_cpus) {
                /* if we are using hwthread cpus, then bind to those */
                opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                                "mca:rmaps[%d] binding not given - using byhwthread", __LINE__);
                OPAL_SET_DEFAULT_BINDING_POLICY(jdata->map->binding, OPAL_BIND_TO_HWTHREAD);
            } else {
                /* bind to core */
                opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                                "mca:rmaps[%d] binding not given - using bycore", __LINE__);
                OPAL_SET_DEFAULT_BINDING_POLICY(jdata->map->binding, OPAL_BIND_TO_CORE);
            }
        } else {
            /* if the user explicitly mapped-by some object, then we default
             * to binding to that object */
            orte_mapping_policy_t mpol;
            mpol = ORTE_GET_MAPPING_POLICY(jdata->map->mapping);
            if (ORTE_MAPPING_GIVEN & ORTE_GET_MAPPING_DIRECTIVE(jdata->map->mapping)) {
                if (ORTE_MAPPING_BYHWTHREAD == mpol) {
                    opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                                    "mca:rmaps[%d] binding not given - using byhwthread", __LINE__);
                    OPAL_SET_DEFAULT_BINDING_POLICY(jdata->map->binding, OPAL_BIND_TO_HWTHREAD);
                } else if (ORTE_MAPPING_BYCORE == mpol) {
                    opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                                    "mca:rmaps[%d] binding not given - using bycore", __LINE__);
                    OPAL_SET_DEFAULT_BINDING_POLICY(jdata->map->binding, OPAL_BIND_TO_CORE);
                } else if (ORTE_MAPPING_BYL1CACHE == mpol) {
                    opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                                    "mca:rmaps[%d] binding not given - using byL1", __LINE__);
                    OPAL_SET_DEFAULT_BINDING_POLICY(jdata->map->binding, OPAL_BIND_TO_L1CACHE);
                } else if (ORTE_MAPPING_BYL2CACHE == mpol) {
                    opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                                    "mca:rmaps[%d] binding not given - using byL2", __LINE__);
                    OPAL_SET_DEFAULT_BINDING_POLICY(jdata->map->binding, OPAL_BIND_TO_L2CACHE);
                } else if (ORTE_MAPPING_BYL3CACHE == mpol) {
                    opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                                    "mca:rmaps[%d] binding not given - using byL3", __LINE__);
                    OPAL_SET_DEFAULT_BINDING_POLICY(jdata->map->binding, OPAL_BIND_TO_L3CACHE);
                } else if (ORTE_MAPPING_BYSOCKET == mpol) {
                    opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                                    "mca:rmaps[%d] binding not given - using bysocket", __LINE__);
                    OPAL_SET_DEFAULT_BINDING_POLICY(jdata->map->binding, OPAL_BIND_TO_SOCKET);
                } else if (ORTE_MAPPING_BYNUMA == mpol) {
                    opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                                    "mca:rmaps[%d] binding not given - using bynuma", __LINE__);
                    OPAL_SET_DEFAULT_BINDING_POLICY(jdata->map->binding, OPAL_BIND_TO_NUMA);
                } else {
                    /* we are mapping by node or some other non-object method */
                    if (nprocs <= 2) {
                        if (opal_hwloc_use_hwthreads_as_cpus) {
                            /* if we are using hwthread cpus, then bind to those */
                            opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                                            "mca:rmaps[%d] binding not given - using byhwthread", __LINE__);
                            OPAL_SET_DEFAULT_BINDING_POLICY(jdata->map->binding, OPAL_BIND_TO_HWTHREAD);
                        } else {
                            /* for performance, bind to core */
                            opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                                            "mca:rmaps[%d] binding not given - using bycore", __LINE__);
                            OPAL_SET_DEFAULT_BINDING_POLICY(jdata->map->binding, OPAL_BIND_TO_CORE);
                        }
                    } else {
                        if (NULL != hwloc_get_obj_by_type(opal_hwloc_topology, HWLOC_OBJ_NODE, 0)) {
                            opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                                                "mca:rmaps[%d] binding not given - using bynuma", __LINE__);
                            OPAL_SET_DEFAULT_BINDING_POLICY(jdata->map->binding, OPAL_BIND_TO_NUMA);
                        } else if (NULL != hwloc_get_obj_by_type(opal_hwloc_topology, HWLOC_OBJ_SOCKET, 0)) {
                            opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                                                "mca:rmaps[%d] binding not given and no NUMA - using bysocket", __LINE__);
                            OPAL_SET_DEFAULT_BINDING_POLICY(jdata->map->binding, OPAL_BIND_TO_SOCKET);
                        } else {
                            /* if we have neither, then just don't bind */
                            opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                                                "mca:rmaps[%d] binding not given and no NUMA or sockets - not binding", __LINE__);
                            OPAL_SET_BINDING_POLICY(jdata->map->binding, OPAL_BIND_TO_NONE);
                        }
                    }
                }
            } else if (nprocs <= 2) {
                if (opal_hwloc_use_hwthreads_as_cpus) {
                    /* if we are using hwthread cpus, then bind to those */
                    opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                                    "mca:rmaps[%d] binding not given - using byhwthread", __LINE__);
                    OPAL_SET_DEFAULT_BINDING_POLICY(jdata->map->binding, OPAL_BIND_TO_HWTHREAD);
                } else {
                    /* for performance, bind to core */
                    opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                                    "mca:rmaps[%d] binding not given - using bycore", __LINE__);
                    OPAL_SET_DEFAULT_BINDING_POLICY(jdata->map->binding, OPAL_BIND_TO_CORE);
                }
            } else {
                /* for performance, bind to NUMA, if available */
                if (NULL != hwloc_get_obj_by_type(opal_hwloc_topology, HWLOC_OBJ_NODE, 0)) {
                    opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                                        "mca:rmaps[%d] binding not given - using bynuma", __LINE__);
                    OPAL_SET_DEFAULT_BINDING_POLICY(jdata->map->binding, OPAL_BIND_TO_NUMA);
                } else if (NULL != hwloc_get_obj_by_type(opal_hwloc_topology, HWLOC_OBJ_SOCKET, 0)) {
                    opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                                        "mca:rmaps[%d] binding not given and no NUMA - using bysocket", __LINE__);
                    OPAL_SET_DEFAULT_BINDING_POLICY(jdata->map->binding, OPAL_BIND_TO_SOCKET);
                } else {
                    /* if we have neither, then just don't bind */
                    opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                                        "mca:rmaps[%d] binding not given and no NUMA or sockets - not binding", __LINE__);
                    OPAL_SET_BINDING_POLICY(jdata->map->binding, OPAL_BIND_TO_NONE);
                }
            }
            if (OPAL_BIND_OVERLOAD_ALLOWED(opal_hwloc_binding_policy)) {
                jdata->map->binding |= OPAL_BIND_ALLOW_OVERLOAD;
            }
        }
    }

    /* if we are not going to launch, then we need to set any
     * undefined topologies to match our own so the mapper
     * can operate
     */
    if (orte_do_not_launch) {
        orte_node_t *node;
        orte_topology_t *t0;
        int i;
        if (NULL == (node = (orte_node_t*)opal_pointer_array_get_item(orte_node_pool, 0))) {
            ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
            OBJ_RELEASE(caddy);
            ORTE_ACTIVATE_JOB_STATE(jdata, ORTE_JOB_STATE_MAP_FAILED);
            return;
        }
        t0 = node->topology;
        for (i=1; i < orte_node_pool->size; i++) {
            if (NULL == (node = (orte_node_t*)opal_pointer_array_get_item(orte_node_pool, i))) {
                continue;
            }
            if (NULL == node->topology) {
                node->topology = t0;
            }
        }
    }

    /* cycle thru the available mappers until one agrees to map
     * the job
     */
    did_map = false;
    if (1 == opal_list_get_size(&orte_rmaps_base.selected_modules)) {
        /* forced selection */
        mod = (orte_rmaps_base_selected_module_t*)opal_list_get_first(&orte_rmaps_base.selected_modules);
        jdata->map->req_mapper = strdup(mod->component->mca_component_name);
    }
    OPAL_LIST_FOREACH(mod, &orte_rmaps_base.selected_modules, orte_rmaps_base_selected_module_t) {
        if (ORTE_SUCCESS == (rc = mod->module->map_job(jdata)) ||
            ORTE_ERR_RESOURCE_BUSY == rc) {
            did_map = true;
            break;
        }
        /* mappers return "next option" if they didn't attempt to
         * map the job. anything else is a true error.
         */
        if (ORTE_ERR_TAKE_NEXT_OPTION != rc) {
            ORTE_ERROR_LOG(rc);
            ORTE_ACTIVATE_JOB_STATE(jdata, ORTE_JOB_STATE_MAP_FAILED);
            goto cleanup;
        }
    }

    if (did_map && ORTE_ERR_RESOURCE_BUSY == rc) {
        /* the map was done but nothing could be mapped
         * for launch as all the resources were busy
         */
        orte_show_help("help-orte-rmaps-base.txt", "cannot-launch", true);
        ORTE_ACTIVATE_JOB_STATE(jdata, ORTE_JOB_STATE_MAP_FAILED);
        goto cleanup;
    }

    /* if we get here without doing the map, or with zero procs in
     * the map, then that's an error
     */
    if (!did_map || 0 == jdata->num_procs || 0 == jdata->map->num_nodes) {
        orte_show_help("help-orte-rmaps-base.txt", "failed-map", true,
                       did_map ? "mapped" : "unmapped",
                       jdata->num_procs, jdata->map->num_nodes);
        ORTE_ACTIVATE_JOB_STATE(jdata, ORTE_JOB_STATE_MAP_FAILED);
        goto cleanup;
    }

    /* if any node is oversubscribed, then check to see if a binding
     * directive was given - if not, then we want to clear the default
     * binding policy so we don't attempt to bind */
    if (ORTE_FLAG_TEST(jdata, ORTE_JOB_FLAG_OVERSUBSCRIBED)) {
        if (!OPAL_BINDING_POLICY_IS_SET(jdata->map->binding)) {
            /* clear any default binding policy we might have set */
            OPAL_SET_DEFAULT_BINDING_POLICY(jdata->map->binding, OPAL_BIND_TO_NONE);
        }
    }

    if (orte_do_not_launch) {
        /* compute the ranks and add the proc objects
         * to the jdata->procs array */
        if (ORTE_SUCCESS != (rc = orte_rmaps_base_compute_vpids(jdata))) {
            ORTE_ERROR_LOG(rc);
            ORTE_ACTIVATE_JOB_STATE(jdata, ORTE_JOB_STATE_MAP_FAILED);
            goto cleanup;
        }
        /* compute and save local ranks */
        if (ORTE_SUCCESS != (rc = orte_rmaps_base_compute_local_ranks(jdata))) {
            ORTE_ERROR_LOG(rc);
            ORTE_ACTIVATE_JOB_STATE(jdata, ORTE_JOB_STATE_MAP_FAILED);
            goto cleanup;
        }
        /* compute and save location assignments */
        if (ORTE_SUCCESS != (rc = orte_rmaps_base_assign_locations(jdata))) {
            ORTE_ERROR_LOG(rc);
            ORTE_ACTIVATE_JOB_STATE(jdata, ORTE_JOB_STATE_MAP_FAILED);
            goto cleanup;
        }
        /* compute and save bindings */
        if (ORTE_SUCCESS != (rc = orte_rmaps_base_compute_bindings(jdata))) {
            ORTE_ERROR_LOG(rc);
            ORTE_ACTIVATE_JOB_STATE(jdata, ORTE_JOB_STATE_MAP_FAILED);
            goto cleanup;
        }
    } else if (!orte_get_attribute(&jdata->attributes, ORTE_JOB_FULLY_DESCRIBED, NULL, OPAL_BOOL)) {
        /* compute and save location assignments */
        if (ORTE_SUCCESS != (rc = orte_rmaps_base_assign_locations(jdata))) {
            ORTE_ERROR_LOG(rc);
            ORTE_ACTIVATE_JOB_STATE(jdata, ORTE_JOB_STATE_MAP_FAILED);
            goto cleanup;
        }
    } else {
        /* compute and save local ranks */
        if (ORTE_SUCCESS != (rc = orte_rmaps_base_compute_local_ranks(jdata))) {
            ORTE_ERROR_LOG(rc);
            ORTE_ACTIVATE_JOB_STATE(jdata, ORTE_JOB_STATE_MAP_FAILED);
            goto cleanup;
        }

        /* compute and save bindings */
        if (ORTE_SUCCESS != (rc = orte_rmaps_base_compute_bindings(jdata))) {
            ORTE_ERROR_LOG(rc);
            ORTE_ACTIVATE_JOB_STATE(jdata, ORTE_JOB_STATE_MAP_FAILED);
            goto cleanup;
        }
    }

    /* set the offset so shared memory components can potentially
     * connect to any spawned jobs
     */
    jdata->offset = orte_total_procs;
    /* track the total number of procs launched by us */
    orte_total_procs += jdata->num_procs;

    /* if it is a dynamic spawn, save the bookmark on the parent's job too */
    if (ORTE_JOBID_INVALID != jdata->originator.jobid) {
        if (NULL != (parent = orte_get_job_data_object(jdata->originator.jobid))) {
            parent->bookmark = jdata->bookmark;
        }
    }

    if (orte_do_not_launch) {
        /* display the devel map */
        orte_rmaps_base_display_map(jdata);
    }

    /* set the job state to the next position */
    ORTE_ACTIVATE_JOB_STATE(jdata, ORTE_JOB_STATE_MAP_COMPLETE);

  cleanup:
      /* reset any node map flags we used so the next job will start clean */
       for (i=0; i < jdata->map->nodes->size; i++) {
           if (NULL != (node = (orte_node_t*)opal_pointer_array_get_item(jdata->map->nodes, i))) {
               ORTE_FLAG_UNSET(node, ORTE_NODE_FLAG_MAPPED);
           }
       }

    /* cleanup */
    OBJ_RELEASE(caddy);
}

void orte_rmaps_base_display_map(orte_job_t *jdata)
{
    /* ignore daemon job */
    char *output=NULL;
    int i, j, cnt;
    orte_node_t *node;
    orte_proc_t *proc;
    char tmp1[1024];
    hwloc_obj_t bd=NULL;;
    opal_hwloc_locality_t locality;
    orte_proc_t *p0;
    char *p0bitmap, *procbitmap;

    if (orte_display_diffable_output) {
        /* intended solely to test mapping methods, this output
         * can become quite long when testing at scale. Rather
         * than enduring all the malloc/free's required to
         * create an arbitrary-length string, custom-generate
         * the output a line at a time here
         */
        /* display just the procs in a diffable format */
        opal_output(orte_clean_output, "<map>\n");
        fflush(stderr);
        /* loop through nodes */
        cnt = 0;
        for (i=0; i < jdata->map->nodes->size; i++) {
            if (NULL == (node = (orte_node_t*)opal_pointer_array_get_item(jdata->map->nodes, i))) {
                continue;
            }
            opal_output(orte_clean_output, "\t<host num=%d>", cnt);
            fflush(stderr);
            cnt++;
            for (j=0; j < node->procs->size; j++) {
                if (NULL == (proc = (orte_proc_t*)opal_pointer_array_get_item(node->procs, j))) {
                    continue;
                }
                memset(tmp1, 0, sizeof(tmp1));
                if (orte_get_attribute(&proc->attributes, ORTE_PROC_HWLOC_BOUND, (void**)&bd, OPAL_PTR)) {
                    if (NULL == bd) {
                        (void)strncpy(tmp1, "UNBOUND", sizeof(tmp1));
                    } else {
                        if (OPAL_ERR_NOT_BOUND == opal_hwloc_base_cset2mapstr(tmp1, sizeof(tmp1), node->topology->topo, bd->cpuset)) {
                            (void)strncpy(tmp1, "UNBOUND", sizeof(tmp1));
                        }
                    }
                } else {
                    (void)strncpy(tmp1, "UNBOUND", sizeof(tmp1));
                }
                opal_output(orte_clean_output, "\t\t<process rank=%s app_idx=%ld local_rank=%lu node_rank=%lu binding=%s>",
                            ORTE_VPID_PRINT(proc->name.vpid),  (long)proc->app_idx,
                            (unsigned long)proc->local_rank,
                            (unsigned long)proc->node_rank, tmp1);
            }
            opal_output(orte_clean_output, "\t</host>");
            fflush(stderr);
        }

         /* test locality - for the first node, print the locality of each proc relative to the first one */
        node = (orte_node_t*)opal_pointer_array_get_item(jdata->map->nodes, 0);
        p0 = (orte_proc_t*)opal_pointer_array_get_item(node->procs, 0);
        p0bitmap = NULL;
        if (orte_get_attribute(&p0->attributes, ORTE_PROC_CPU_BITMAP, (void**)&p0bitmap, OPAL_STRING) &&
            NULL != p0bitmap) {
            opal_output(orte_clean_output, "\t<locality>");
            for (j=1; j < node->procs->size; j++) {
                if (NULL == (proc = (orte_proc_t*)opal_pointer_array_get_item(node->procs, j))) {
                    continue;
                }
                procbitmap = NULL;
                if (orte_get_attribute(&proc->attributes, ORTE_PROC_CPU_BITMAP, (void**)&procbitmap, OPAL_STRING) &&
                    NULL != procbitmap) {
                    locality = opal_hwloc_base_get_relative_locality(node->topology->topo,
                                                                     p0bitmap,
                                                                     procbitmap);
                    opal_output(orte_clean_output, "\t\t<rank=%s rank=%s locality=%s>",
                                ORTE_VPID_PRINT(p0->name.vpid),
                                ORTE_VPID_PRINT(proc->name.vpid),
                                opal_hwloc_base_print_locality(locality));
                }
            }
            opal_output(orte_clean_output, "\t</locality>\n</map>");
            fflush(stderr);
            if (NULL != p0bitmap) {
                free(p0bitmap);
            }
            if (NULL != procbitmap) {
                free(procbitmap);
            }
        }
    } else {
        opal_output(orte_clean_output, " Data for JOB %s offset %s Total slots allocated %lu",
                    ORTE_JOBID_PRINT(jdata->jobid), ORTE_VPID_PRINT(jdata->offset),
                    (long unsigned)jdata->total_slots_alloc);
        opal_dss.print(&output, NULL, jdata->map, ORTE_JOB_MAP);
        if (orte_xml_output) {
            fprintf(orte_xml_fp, "%s\n", output);
            fflush(orte_xml_fp);
        } else {
            opal_output(orte_clean_output, "%s", output);
        }
        free(output);
    }
}
