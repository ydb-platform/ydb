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
 * Copyright (c) 2011-2012 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2014-2018 Intel, Inc. All rights reserved.
 * Copyright (c) 2016      IBM Corporation.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "orte_config.h"
#include "orte/constants.h"

#include <sys/types.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif  /* HAVE_UNISTD_H */
#include <string.h>

#include "opal/util/argv.h"
#include "opal/util/if.h"
#include "opal/util/output.h"
#include "orte/mca/mca.h"
#include "opal/mca/base/base.h"
#include "opal/mca/hwloc/base/base.h"
#include "opal/threads/tsd.h"

#include "orte/types.h"
#include "orte/util/show_help.h"
#include "orte/util/name_fns.h"
#include "orte/runtime/orte_globals.h"
#include "orte/util/hostfile/hostfile.h"
#include "orte/util/dash_host/dash_host.h"
#include "orte/mca/errmgr/errmgr.h"
#include "orte/mca/ess/ess.h"
#include "orte/runtime/data_type_support/orte_dt_support.h"

#include "orte/mca/rmaps/base/rmaps_private.h"
#include "orte/mca/rmaps/base/base.h"

int orte_rmaps_base_filter_nodes(orte_app_context_t *app,
                                 opal_list_t *nodes, bool remove)
{
    int rc=ORTE_ERR_TAKE_NEXT_OPTION;
    char *hosts;

    /* did the app_context contain a hostfile? */
    if (orte_get_attribute(&app->attributes, ORTE_APP_HOSTFILE, (void**)&hosts, OPAL_STRING)) {
        /* yes - filter the node list through the file, removing
         * any nodes not found in the file
         */
        if (ORTE_SUCCESS != (rc = orte_util_filter_hostfile_nodes(nodes, hosts, remove))) {
            ORTE_ERROR_LOG(rc);
            free(hosts);
            return rc;
        }
        /** check that anything is here */
        if (0 == opal_list_get_size(nodes)) {
            orte_show_help("help-orte-rmaps-base.txt", "orte-rmaps-base:no-mapped-node",
                           true, app->app, "-hostfile", hosts);
            free(hosts);
            return ORTE_ERR_SILENT;
        }
        free(hosts);
    }
    /* did the app_context contain an add-hostfile? */
    if (orte_get_attribute(&app->attributes, ORTE_APP_ADD_HOSTFILE, (void**)&hosts, OPAL_STRING)) {
        /* yes - filter the node list through the file, removing
         * any nodes not found in the file
         */
        if (ORTE_SUCCESS != (rc = orte_util_filter_hostfile_nodes(nodes, hosts, remove))) {
            free(hosts);
            ORTE_ERROR_LOG(rc);
            return rc;
        }
        /** check that anything is here */
        if (0 == opal_list_get_size(nodes)) {
            orte_show_help("help-orte-rmaps-base.txt", "orte-rmaps-base:no-mapped-node",
                           true, app->app, "-add-hostfile", hosts);
            free(hosts);
            return ORTE_ERR_SILENT;
        }
        free(hosts);
    }
    /* now filter the list through any -host specification */
    if (!orte_soft_locations &&
        orte_get_attribute(&app->attributes, ORTE_APP_DASH_HOST, (void**)&hosts, OPAL_STRING)) {
        if (ORTE_SUCCESS != (rc = orte_util_filter_dash_host_nodes(nodes, hosts, remove))) {
            ORTE_ERROR_LOG(rc);
            free(hosts);
            return rc;
        }
        /** check that anything is left! */
        if (0 == opal_list_get_size(nodes)) {
            orte_show_help("help-orte-rmaps-base.txt", "orte-rmaps-base:no-mapped-node",
                           true, app->app, "-host", hosts);
            free(hosts);
            return ORTE_ERR_SILENT;
        }
        free(hosts);
    }
    /* now filter the list through any add-host specification */
    if (orte_get_attribute(&app->attributes, ORTE_APP_ADD_HOST, (void**)&hosts, OPAL_STRING)) {
        if (ORTE_SUCCESS != (rc = orte_util_filter_dash_host_nodes(nodes, hosts, remove))) {
            ORTE_ERROR_LOG(rc);
            free(hosts);
            return rc;
        }
        /** check that anything is left! */
        if (0 == opal_list_get_size(nodes)) {
            orte_show_help("help-orte-rmaps-base.txt", "orte-rmaps-base:no-mapped-node",
                           true, app->app, "-add-host", hosts);
            free(hosts);
            return ORTE_ERR_SILENT;
        }
        free(hosts);
    }

    return rc;
}


/*
 * Query the registry for all nodes allocated to a specified app_context
 */
int orte_rmaps_base_get_target_nodes(opal_list_t *allocated_nodes, orte_std_cntr_t *total_num_slots,
                                     orte_app_context_t *app, orte_mapping_policy_t policy,
                                     bool initial_map, bool silent)
{
    opal_list_item_t *item;
    orte_node_t *node, *nd, *nptr, *next;
    orte_std_cntr_t num_slots;
    orte_std_cntr_t i;
    int rc;
    orte_job_t *daemons;
    bool novm;
    opal_list_t nodes;
    char *hosts = NULL;

    /** set default answer */
    *total_num_slots = 0;

    /* get the daemon job object */
    daemons = orte_get_job_data_object(ORTE_PROC_MY_NAME->jobid);
    /* see if we have a vm or not */
    novm = orte_get_attribute(&daemons->attributes, ORTE_JOB_NO_VM, NULL, OPAL_BOOL);

    /* if this is NOT a managed allocation, then we use the nodes
     * that were specified for this app - there is no need to collect
     * all available nodes and "filter" them
     */
    if (!orte_managed_allocation) {
        OBJ_CONSTRUCT(&nodes, opal_list_t);
        /* if the app provided a dash-host, and we are not treating
         * them as requested or "soft" locations, then use those nodes
         */
        hosts = NULL;
        if (!orte_soft_locations &&
            orte_get_attribute(&app->attributes, ORTE_APP_DASH_HOST, (void**)&hosts, OPAL_STRING)) {
            OPAL_OUTPUT_VERBOSE((5, orte_rmaps_base_framework.framework_output,
                                 "%s using dash_host %s",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), hosts));
            if (ORTE_SUCCESS != (rc = orte_util_add_dash_host_nodes(&nodes, hosts, false))) {
                ORTE_ERROR_LOG(rc);
                free(hosts);
                return rc;
            }
            free(hosts);
        } else if (orte_get_attribute(&app->attributes, ORTE_APP_HOSTFILE, (void**)&hosts, OPAL_STRING)) {
            /* otherwise, if the app provided a hostfile, then use that */
            OPAL_OUTPUT_VERBOSE((5, orte_rmaps_base_framework.framework_output,
                                 "%s using hostfile %s",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), hosts));
            if (ORTE_SUCCESS != (rc = orte_util_add_hostfile_nodes(&nodes, hosts))) {
                free(hosts);
                ORTE_ERROR_LOG(rc);
                return rc;
            }
            free(hosts);
        } else {
            /* if nothing else was specified by the app, then use all known nodes, which
             * will include ourselves
             */
            OPAL_OUTPUT_VERBOSE((5, orte_rmaps_base_framework.framework_output,
                                 "%s using known nodes",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));
            goto addknown;
        }
        /** if we still don't have anything */
        if (0 == opal_list_get_size(&nodes)) {
            if (!silent) {
                orte_show_help("help-orte-rmaps-base.txt",
                               "orte-rmaps-base:no-available-resources",
                               true);
            }
            OBJ_DESTRUCT(&nodes);
            return ORTE_ERR_SILENT;
        }
        /* find the nodes in our node array and assemble them
         * in daemon order if the vm was launched
         */
        for (i=0; i < orte_node_pool->size; i++) {
            nd = NULL;
            if (NULL == (node = (orte_node_t*)opal_pointer_array_get_item(orte_node_pool, i))) {
                continue;
            }
            /* ignore nodes that are non-usable */
            if (ORTE_FLAG_TEST(node, ORTE_NODE_NON_USABLE)) {
                continue;
            }
            OPAL_LIST_FOREACH_SAFE(nptr, next, &nodes, orte_node_t) {
                if (0 != strcmp(node->name, nptr->name)) {
                    OPAL_OUTPUT_VERBOSE((10, orte_rmaps_base_framework.framework_output,
                                         "NODE %s DOESNT MATCH NODE %s",
                                         node->name, nptr->name));
                    continue;
                }
                /* ignore nodes that are marked as do-not-use for this mapping */
                if (ORTE_NODE_STATE_DO_NOT_USE == node->state) {
                    OPAL_OUTPUT_VERBOSE((10, orte_rmaps_base_framework.framework_output,
                                         "NODE %s IS MARKED NO_USE", node->name));
                    /* reset the state so it can be used another time */
                    node->state = ORTE_NODE_STATE_UP;
                    continue;
                }
                if (ORTE_NODE_STATE_DOWN == node->state) {
                    OPAL_OUTPUT_VERBOSE((10, orte_rmaps_base_framework.framework_output,
                                         "NODE %s IS DOWN", node->name));
                    continue;
                }
                if (ORTE_NODE_STATE_NOT_INCLUDED == node->state) {
                    OPAL_OUTPUT_VERBOSE((10, orte_rmaps_base_framework.framework_output,
                                         "NODE %s IS MARKED NO_INCLUDE", node->name));
                    /* not to be used */
                    continue;
                }
                /* if this node wasn't included in the vm (e.g., by -host), ignore it,
                 * unless we are mapping prior to launching the vm
                 */
                if (NULL == node->daemon && !novm) {
                    OPAL_OUTPUT_VERBOSE((10, orte_rmaps_base_framework.framework_output,
                                         "NODE %s HAS NO DAEMON", node->name));
                    continue;
                }
                /* retain a copy for our use in case the item gets
                 * destructed along the way
                 */
                OBJ_RETAIN(node);
                if (initial_map) {
                    /* if this is the first app_context we
                     * are getting for an initial map of a job,
                     * then mark all nodes as unmapped
                     */
                    ORTE_FLAG_UNSET(node, ORTE_NODE_FLAG_MAPPED);
                }
                if (NULL == nd || NULL == nd->daemon ||
                    NULL == node->daemon ||
                    nd->daemon->name.vpid < node->daemon->name.vpid) {
                    /* just append to end */
                    opal_list_append(allocated_nodes, &node->super);
                    nd = node;
                } else {
                    /* starting from end, put this node in daemon-vpid order */
                    while (node->daemon->name.vpid < nd->daemon->name.vpid) {
                        if (opal_list_get_begin(allocated_nodes) == opal_list_get_prev(&nd->super)) {
                            /* insert at beginning */
                            opal_list_prepend(allocated_nodes, &node->super);
                            goto moveon1;
                        }
                        nd = (orte_node_t*)opal_list_get_prev(&nd->super);
                    }
                    item = opal_list_get_next(&nd->super);
                    if (item == opal_list_get_end(allocated_nodes)) {
                        /* we are at the end - just append */
                        opal_list_append(allocated_nodes, &node->super);
                    } else {
                        nd = (orte_node_t*)item;
                        opal_list_insert_pos(allocated_nodes, item, &node->super);
                    }
                moveon1:
                    /* reset us back to the end for the next node */
                    nd = (orte_node_t*)opal_list_get_last(allocated_nodes);
                }
                opal_list_remove_item(&nodes, (opal_list_item_t*)nptr);
                OBJ_RELEASE(nptr);
            }
        }
        OBJ_DESTRUCT(&nodes);
        /* now prune for usage and compute total slots */
        goto complete;
    }

  addknown:
    /* add everything in the node pool that can be used - add them
     * in daemon order, which may be different than the order in the
     * node pool. Since an empty list is passed into us, the list at
     * this point either has the HNP node or nothing, and the HNP
     * node obviously has a daemon on it (us!)
     */
    if (0 == opal_list_get_size(allocated_nodes)) {
        /* the list is empty - if the HNP is allocated, then add it */
        if (orte_hnp_is_allocated) {
            nd = (orte_node_t*)opal_pointer_array_get_item(orte_node_pool, 0);
            OBJ_RETAIN(nd);
            opal_list_append(allocated_nodes, &nd->super);
        } else {
            nd = NULL;
        }
    } else {
        nd = (orte_node_t*)opal_list_get_last(allocated_nodes);
    }
    for (i=1; i < orte_node_pool->size; i++) {
        if (NULL != (node = (orte_node_t*)opal_pointer_array_get_item(orte_node_pool, i))) {
            /* ignore nodes that are non-usable */
            if (ORTE_FLAG_TEST(node, ORTE_NODE_NON_USABLE)) {
                continue;
            }
            /* ignore nodes that are marked as do-not-use for this mapping */
            if (ORTE_NODE_STATE_DO_NOT_USE == node->state) {
                OPAL_OUTPUT_VERBOSE((10, orte_rmaps_base_framework.framework_output,
                                     "NODE %s IS MARKED NO_USE", node->name));
                /* reset the state so it can be used another time */
                node->state = ORTE_NODE_STATE_UP;
                continue;
            }
            if (ORTE_NODE_STATE_DOWN == node->state) {
                OPAL_OUTPUT_VERBOSE((10, orte_rmaps_base_framework.framework_output,
                                     "NODE %s IS MARKED DOWN", node->name));
                continue;
            }
            if (ORTE_NODE_STATE_NOT_INCLUDED == node->state) {
                OPAL_OUTPUT_VERBOSE((10, orte_rmaps_base_framework.framework_output,
                                     "NODE %s IS MARKED NO_INCLUDE", node->name));
                /* not to be used */
                continue;
            }
            /* if this node wasn't included in the vm (e.g., by -host), ignore it,
             * unless we are mapping prior to launching the vm
             */
            if (NULL == node->daemon && !novm) {
                OPAL_OUTPUT_VERBOSE((10, orte_rmaps_base_framework.framework_output,
                                     "NODE %s HAS NO DAEMON", node->name));
                continue;
            }
            /* retain a copy for our use in case the item gets
             * destructed along the way
             */
            OBJ_RETAIN(node);
            if (initial_map) {
                /* if this is the first app_context we
                 * are getting for an initial map of a job,
                 * then mark all nodes as unmapped
                 */
                ORTE_FLAG_UNSET(node, ORTE_NODE_FLAG_MAPPED);
            }
            if (NULL == nd || NULL == nd->daemon ||
                NULL == node->daemon ||
                nd->daemon->name.vpid < node->daemon->name.vpid) {
                /* just append to end */
                opal_list_append(allocated_nodes, &node->super);
                nd = node;
            } else {
                /* starting from end, put this node in daemon-vpid order */
                while (node->daemon->name.vpid < nd->daemon->name.vpid) {
                    if (opal_list_get_begin(allocated_nodes) == opal_list_get_prev(&nd->super)) {
                        /* insert at beginning */
                        opal_list_prepend(allocated_nodes, &node->super);
                        goto moveon;
                    }
                    nd = (orte_node_t*)opal_list_get_prev(&nd->super);
                }
                item = opal_list_get_next(&nd->super);
                if (item == opal_list_get_end(allocated_nodes)) {
                    /* we are at the end - just append */
                    opal_list_append(allocated_nodes, &node->super);
                } else {
                    nd = (orte_node_t*)item;
                    opal_list_insert_pos(allocated_nodes, item, &node->super);
                }
            moveon:
                /* reset us back to the end for the next node */
                nd = (orte_node_t*)opal_list_get_last(allocated_nodes);
            }
        }
    }

    OPAL_OUTPUT_VERBOSE((5, orte_rmaps_base_framework.framework_output,
                         "%s Starting with %d nodes in list",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         (int)opal_list_get_size(allocated_nodes)));

    /** check that anything is here */
    if (0 == opal_list_get_size(allocated_nodes)) {
        if (!silent) {
            orte_show_help("help-orte-rmaps-base.txt",
                           "orte-rmaps-base:no-available-resources",
                           true);
        }
        return ORTE_ERR_SILENT;
    }

    /* filter the nodes thru any hostfile and dash-host options */
    OPAL_OUTPUT_VERBOSE((5, orte_rmaps_base_framework.framework_output,
                         "%s Filtering thru apps",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));

    if (ORTE_SUCCESS != (rc = orte_rmaps_base_filter_nodes(app, allocated_nodes, true))
        && ORTE_ERR_TAKE_NEXT_OPTION != rc) {
        ORTE_ERROR_LOG(rc);
        return rc;
    }
    OPAL_OUTPUT_VERBOSE((5, orte_rmaps_base_framework.framework_output,
                         "%s Retained %d nodes in list",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         (int)opal_list_get_size(allocated_nodes)));

  complete:
    num_slots = 0;
    /* remove all nodes that are already at max usage, and
     * compute the total number of allocated slots while
     * we do so - can ignore this if we are mapping debugger
     * daemons as they do not count against the allocation */
    if (ORTE_MAPPING_DEBUGGER & ORTE_GET_MAPPING_DIRECTIVE(policy)) {
        num_slots = opal_list_get_size(allocated_nodes);    // tell the mapper there is one slot/node for debuggers
    } else {
        item  = opal_list_get_first(allocated_nodes);
        OPAL_LIST_FOREACH_SAFE(node, next, allocated_nodes, orte_node_t) {
            /* if the hnp was not allocated, or flagged not to be used,
             * then remove it here */
            if (!orte_hnp_is_allocated || (ORTE_GET_MAPPING_DIRECTIVE(policy) & ORTE_MAPPING_NO_USE_LOCAL)) {
                if (0 == node->index) {
                    opal_list_remove_item(allocated_nodes, &node->super);
                    OBJ_RELEASE(node);  /* "un-retain" it */
                    continue;
                }
            }
            /** check to see if this node is fully used - remove if so */
            if (0 != node->slots_max && node->slots_inuse > node->slots_max) {
                OPAL_OUTPUT_VERBOSE((5, orte_rmaps_base_framework.framework_output,
                                     "%s Removing node %s: max %d inuse %d",
                                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                     node->name, node->slots_max, node->slots_inuse));
                opal_list_remove_item(allocated_nodes, &node->super);
                OBJ_RELEASE(node);  /* "un-retain" it */
                continue;
            }
            if (node->slots <= node->slots_inuse &&
                       (ORTE_MAPPING_NO_OVERSUBSCRIBE & ORTE_GET_MAPPING_DIRECTIVE(policy))) {
                /* remove the node as fully used */
                OPAL_OUTPUT_VERBOSE((5, orte_rmaps_base_framework.framework_output,
                                     "%s Removing node %s slots %d inuse %d",
                                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                     node->name, node->slots, node->slots_inuse));
                opal_list_remove_item(allocated_nodes, &node->super);
                OBJ_RELEASE(node);  /* "un-retain" it */
                continue;
            }
            if (node->slots > node->slots_inuse) {
                /* add the available slots */
                OPAL_OUTPUT_VERBOSE((5, orte_rmaps_base_framework.framework_output,
                                     "%s node %s has %d slots available",
                                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                     node->name, node->slots - node->slots_inuse));
                num_slots += node->slots - node->slots_inuse;
                continue;
            }
            if (!(ORTE_MAPPING_NO_OVERSUBSCRIBE & ORTE_GET_MAPPING_DIRECTIVE(policy))) {
                /* nothing needed to do here - we don't add slots to the
                 * count as we don't have any available. Just let the mapper
                 * do what it needs to do to meet the request
                 */
                OPAL_OUTPUT_VERBOSE((5, orte_rmaps_base_framework.framework_output,
                                     "%s node %s is fully used, but available for oversubscription",
                                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                     node->name));
            } else {
                /* if we cannot use it, remove it from list */
                opal_list_remove_item(allocated_nodes, &node->super);
                OBJ_RELEASE(node);  /* "un-retain" it */
            }
        }
    }

    /* Sanity check to make sure we have resources available */
    if (0 == opal_list_get_size(allocated_nodes)) {
        if (silent) {
            /* let the caller know that the resources exist,
             * but are currently busy
             */
            return ORTE_ERR_RESOURCE_BUSY;
        } else {
            orte_show_help("help-orte-rmaps-base.txt",
                           "orte-rmaps-base:all-available-resources-used", true);
            return ORTE_ERR_SILENT;
        }
    }

    /* pass back the total number of available slots */
    *total_num_slots = num_slots;

    if (4 < opal_output_get_verbosity(orte_rmaps_base_framework.framework_output)) {
        opal_output(0, "AVAILABLE NODES FOR MAPPING:");
        for (item = opal_list_get_first(allocated_nodes);
             item != opal_list_get_end(allocated_nodes);
             item = opal_list_get_next(item)) {
            node = (orte_node_t*)item;
            opal_output(0, "    node: %s daemon: %s", node->name,
                        (NULL == node->daemon) ? "NULL" : ORTE_VPID_PRINT(node->daemon->name.vpid));
        }
    }

    return ORTE_SUCCESS;
}

orte_proc_t* orte_rmaps_base_setup_proc(orte_job_t *jdata,
                                        orte_node_t *node,
                                        orte_app_idx_t idx)
{
    orte_proc_t *proc;
    int rc;

    proc = OBJ_NEW(orte_proc_t);
    /* set the jobid */
    proc->name.jobid = jdata->jobid;
    /* flag the proc as ready for launch */
    proc->state = ORTE_PROC_STATE_INIT;
    proc->app_idx = idx;
    /* mark the proc as UPDATED so it will be included in the launch */
    ORTE_FLAG_SET(proc, ORTE_PROC_FLAG_UPDATED);
    if (NULL == node->daemon) {
        proc->parent = ORTE_VPID_INVALID;
    } else {
        proc->parent = node->daemon->name.vpid;
    }

    OBJ_RETAIN(node);  /* maintain accounting on object */
    proc->node = node;
    /* if this is a debugger job, then it doesn't count against
     * available slots - otherwise, it does */
    if (!ORTE_FLAG_TEST(jdata, ORTE_JOB_FLAG_DEBUGGER_DAEMON)) {
        node->num_procs++;
        ++node->slots_inuse;
    }
    if (0 > (rc = opal_pointer_array_add(node->procs, (void*)proc))) {
        ORTE_ERROR_LOG(rc);
        OBJ_RELEASE(proc);
        return NULL;
    }
    /* retain the proc struct so that we correctly track its release */
    OBJ_RETAIN(proc);

    return proc;
}

/*
 * determine the proper starting point for the next mapping operation
 */
orte_node_t* orte_rmaps_base_get_starting_point(opal_list_t *node_list,
                                                orte_job_t *jdata)
{
    opal_list_item_t *item, *cur_node_item;
    orte_node_t *node, *nd1, *ndmin;
    int overload;

    /* if a bookmark exists from some prior mapping, set us to start there */
    if (NULL != jdata->bookmark) {
        cur_node_item = NULL;
        /* find this node on the list */
        for (item = opal_list_get_first(node_list);
             item != opal_list_get_end(node_list);
             item = opal_list_get_next(item)) {
            node = (orte_node_t*)item;

            if (node->index == jdata->bookmark->index) {
                cur_node_item = item;
                break;
            }
        }
        /* see if we found it - if not, just start at the beginning */
        if (NULL == cur_node_item) {
            cur_node_item = opal_list_get_first(node_list);
        }
    } else {
        /* if no bookmark, then just start at the beginning of the list */
        cur_node_item = opal_list_get_first(node_list);
    }

    OPAL_OUTPUT_VERBOSE((5, orte_rmaps_base_framework.framework_output,
                         "%s Starting bookmark at node %s",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         ((orte_node_t*)cur_node_item)->name));

    /* is this node fully subscribed? If so, then the first
     * proc we assign will oversubscribe it, so let's look
     * for another candidate
     */
    node = (orte_node_t*)cur_node_item;
    ndmin = node;
    overload = ndmin->slots_inuse - ndmin->slots;
    if (node->slots_inuse >= node->slots) {
        /* work down the list - is there another node that
         * would not be oversubscribed?
         */
        if (cur_node_item != opal_list_get_last(node_list)) {
            item = opal_list_get_next(cur_node_item);
        } else {
            item = opal_list_get_first(node_list);
        }
        nd1 = NULL;
        while (item != cur_node_item) {
            nd1 = (orte_node_t*)item;
            if (nd1->slots_inuse < nd1->slots) {
                /* this node is not oversubscribed! use it! */
                cur_node_item = item;
                goto process;
            }
            /* this one was also oversubscribed, keep track of the
             * node that has the least usage - if we can't
             * find anyone who isn't fully utilized, we will
             * start with the least used node
             */
            if (overload >= (nd1->slots_inuse - nd1->slots)) {
                ndmin = nd1;
                overload = ndmin->slots_inuse - ndmin->slots;
            }
            if (item == opal_list_get_last(node_list)) {
                item = opal_list_get_first(node_list);
            } else {
                item= opal_list_get_next(item);
            }
        }
        /* if we get here, then we cycled all the way around the
         * list without finding a better answer - just use the node
         * that is minimally overloaded if it is better than
         * what we already have
         */
        if (NULL != nd1 &&
            (nd1->slots_inuse - nd1->slots) < (node->slots_inuse - node->slots)) {
            cur_node_item = (opal_list_item_t*)ndmin;
        }
    }

 process:
    OPAL_OUTPUT_VERBOSE((5, orte_rmaps_base_framework.framework_output,
                         "%s Starting at node %s",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         ((orte_node_t*)cur_node_item)->name));

    /* make life easier - put the bookmark at the top of the list,
     * shifting everything above it to the end of the list while
     * preserving order
     */
    while (cur_node_item != (item = opal_list_get_first(node_list))) {
        opal_list_remove_item(node_list, item);
        opal_list_append(node_list, item);
    }

    return (orte_node_t*)cur_node_item;
}
