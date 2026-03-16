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
 * Copyright (c) 2009-2013 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2013-2018 Intel, Inc. All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
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

#include "opal/util/output.h"
#include "opal/mca/hwloc/base/base.h"

#include "orte/util/show_help.h"
#include "orte/util/name_fns.h"
#include "orte/runtime/orte_globals.h"
#include "orte/mca/errmgr/errmgr.h"

#include "orte/mca/rmaps/base/rmaps_private.h"
#include "orte/mca/rmaps/base/base.h"
#include "rmaps_rr.h"

int orte_rmaps_rr_byslot(orte_job_t *jdata,
                         orte_app_context_t *app,
                         opal_list_t *node_list,
                         orte_std_cntr_t num_slots,
                         orte_vpid_t num_procs)
{
    int i, nprocs_mapped;
    orte_node_t *node;
    orte_proc_t *proc;
    int num_procs_to_assign, extra_procs_to_assign=0, nxtra_nodes=0;
    hwloc_obj_t obj=NULL;
    float balance;
    bool add_one=false;

    opal_output_verbose(2, orte_rmaps_base_framework.framework_output,
                        "mca:rmaps:rr: mapping by slot for job %s slots %d num_procs %lu",
                        ORTE_JOBID_PRINT(jdata->jobid), (int)num_slots, (unsigned long)num_procs);

    /* check to see if we can map all the procs */
    if (num_slots < (int)app->num_procs) {
        if (ORTE_MAPPING_NO_OVERSUBSCRIBE & ORTE_GET_MAPPING_DIRECTIVE(jdata->map->mapping)) {
            orte_show_help("help-orte-rmaps-base.txt", "orte-rmaps-base:alloc-error",
                           true, app->num_procs, app->app, orte_process_info.nodename);
            ORTE_UPDATE_EXIT_STATUS(ORTE_ERROR_DEFAULT_EXIT_CODE);
            return ORTE_ERR_SILENT;
        }
    }

    /* first pass: map the number of procs to each node until we
     * map all specified procs or use all allocated slots
     */
    nprocs_mapped = 0;
    OPAL_LIST_FOREACH(node, node_list, orte_node_t) {
        opal_output_verbose(2, orte_rmaps_base_framework.framework_output,
                            "mca:rmaps:rr:slot working node %s",
                            node->name);
        /* get the root object as we are not assigning
         * locale here except at the node level
         */
        if (NULL != node->topology && NULL != node->topology->topo) {
            obj = hwloc_get_root_obj(node->topology->topo);
        }
        if (node->slots <= node->slots_inuse) {
            opal_output_verbose(2, orte_rmaps_base_framework.framework_output,
                                "mca:rmaps:rr:slot node %s is full - skipping",
                                node->name);
            continue;
        }
        if (orte_rmaps_base_pernode) {
            num_procs_to_assign = 1;
        } else if (0 < orte_rmaps_base_n_pernode) {
            num_procs_to_assign = orte_rmaps_base_n_pernode;
        } else if (0 < orte_rmaps_base_n_persocket) {
            if (NULL == node->topology) {
                orte_show_help("help-orte-rmaps-ppr.txt", "ppr-topo-missing",
                               true, node->name);
                return ORTE_ERR_SILENT;
            }
            num_procs_to_assign = orte_rmaps_base_n_persocket * opal_hwloc_base_get_nbobjs_by_type(node->topology->topo, HWLOC_OBJ_PACKAGE, 0, OPAL_HWLOC_AVAILABLE);
        } else {
            /* assign a number of procs equal to the number of available slots */
            num_procs_to_assign = node->slots - node->slots_inuse;
        }
        opal_output_verbose(2, orte_rmaps_base_framework.framework_output,
                            "mca:rmaps:rr:slot assigning %d procs to node %s",
                            (int)num_procs_to_assign, node->name);

        for (i=0; i < num_procs_to_assign && nprocs_mapped < app->num_procs; i++) {
            /* add this node to the map - do it only once */
            if (!ORTE_FLAG_TEST(node, ORTE_NODE_FLAG_MAPPED)) {
                ORTE_FLAG_SET(node, ORTE_NODE_FLAG_MAPPED);
                OBJ_RETAIN(node);
                opal_pointer_array_add(jdata->map->nodes, node);
                ++(jdata->map->num_nodes);
            }
            if (NULL == (proc = orte_rmaps_base_setup_proc(jdata, node, app->idx))) {
                return ORTE_ERR_OUT_OF_RESOURCE;
            }
            nprocs_mapped++;
            orte_set_attribute(&proc->attributes, ORTE_PROC_HWLOC_LOCALE, ORTE_ATTR_LOCAL, obj, OPAL_PTR);
        }
    }

    if (nprocs_mapped == app->num_procs) {
        /* we are done */
        return ORTE_SUCCESS;
    }

    opal_output_verbose(2, orte_rmaps_base_framework.framework_output,
                        "mca:rmaps:rr:slot job %s is oversubscribed - performing second pass",
                        ORTE_JOBID_PRINT(jdata->jobid));

    /* second pass: if we haven't mapped everyone yet, it is
     * because we are oversubscribed. Figure out how many procs
     * to add
     */
    balance = (float)((int)app->num_procs - nprocs_mapped) / (float)opal_list_get_size(node_list);
    extra_procs_to_assign = (int)balance;
    if (0 < (balance - (float)extra_procs_to_assign)) {
        /* compute how many nodes need an extra proc */
        nxtra_nodes = app->num_procs - nprocs_mapped - (extra_procs_to_assign * opal_list_get_size(node_list));
        /* add one so that we add an extra proc to the first nodes
         * until all procs are mapped
         */
        extra_procs_to_assign++;
        /* flag that we added one */
        add_one = true;
    }

    OPAL_LIST_FOREACH(node, node_list, orte_node_t) {
        opal_output_verbose(2, orte_rmaps_base_framework.framework_output,
                            "mca:rmaps:rr:slot working node %s",
                            node->name);
        /* get the root object as we are not assigning
         * locale except at the node level
         */
        if (NULL != node->topology && NULL != node->topology->topo) {
            obj = hwloc_get_root_obj(node->topology->topo);
        }

        /* add this node to the map - do it only once */
        if (!ORTE_FLAG_TEST(node, ORTE_NODE_FLAG_MAPPED)) {
            ORTE_FLAG_SET(node, ORTE_NODE_FLAG_MAPPED);
            OBJ_RETAIN(node);
            opal_pointer_array_add(jdata->map->nodes, node);
            ++(jdata->map->num_nodes);
        }
        if (add_one) {
            if (0 == nxtra_nodes) {
                --extra_procs_to_assign;
                add_one = false;
            } else {
                --nxtra_nodes;
            }
        }
        if(node->slots <= node->slots_inuse) {
            /* nodes are already oversubscribed */
            num_procs_to_assign = extra_procs_to_assign;
        }
        else {
            /* nodes have some room */
            num_procs_to_assign = node->slots - node->slots_inuse + extra_procs_to_assign;
        }
        opal_output_verbose(2, orte_rmaps_base_framework.framework_output,
                            "mca:rmaps:rr:slot adding up to %d procs to node %s",
                            num_procs_to_assign, node->name);
        for (i=0; i < num_procs_to_assign && nprocs_mapped < app->num_procs; i++) {
            if (NULL == (proc = orte_rmaps_base_setup_proc(jdata, node, app->idx))) {
                return ORTE_ERR_OUT_OF_RESOURCE;
            }
            nprocs_mapped++;
            orte_set_attribute(&proc->attributes, ORTE_PROC_HWLOC_LOCALE, ORTE_ATTR_LOCAL, obj, OPAL_PTR);
        }
        /* not all nodes are equal, so only set oversubscribed for
         * this node if it is in that state
         */
        if (node->slots < (int)node->num_procs) {
            /* flag the node as oversubscribed so that sched-yield gets
             * properly set
             */
            ORTE_FLAG_SET(node, ORTE_NODE_FLAG_OVERSUBSCRIBED);
            ORTE_FLAG_SET(jdata, ORTE_JOB_FLAG_OVERSUBSCRIBED);
            /* check for permission */
            if (ORTE_FLAG_TEST(node, ORTE_NODE_FLAG_SLOTS_GIVEN)) {
                /* if we weren't given a directive either way, then we will error out
                 * as the #slots were specifically given, either by the host RM or
                 * via hostfile/dash-host */
                if (!(ORTE_MAPPING_SUBSCRIBE_GIVEN & ORTE_GET_MAPPING_DIRECTIVE(jdata->map->mapping))) {
                    orte_show_help("help-orte-rmaps-base.txt", "orte-rmaps-base:alloc-error",
                                   true, app->num_procs, app->app, orte_process_info.nodename);
                    ORTE_UPDATE_EXIT_STATUS(ORTE_ERROR_DEFAULT_EXIT_CODE);
                    return ORTE_ERR_SILENT;
                } else if (ORTE_MAPPING_NO_OVERSUBSCRIBE & ORTE_GET_MAPPING_DIRECTIVE(jdata->map->mapping)) {
                    /* if we were explicitly told not to oversubscribe, then don't */
                    orte_show_help("help-orte-rmaps-base.txt", "orte-rmaps-base:alloc-error",
                                   true, app->num_procs, app->app, orte_process_info.nodename);
                    ORTE_UPDATE_EXIT_STATUS(ORTE_ERROR_DEFAULT_EXIT_CODE);
                    return ORTE_ERR_SILENT;
                }
            }
        }
        /* if we have mapped everything, then we are done */
        if (nprocs_mapped == app->num_procs) {
            break;
        }
    }
    return ORTE_SUCCESS;
}

int orte_rmaps_rr_bynode(orte_job_t *jdata,
                         orte_app_context_t *app,
                         opal_list_t *node_list,
                         orte_std_cntr_t num_slots,
                         orte_vpid_t num_procs)
{
    int j, nprocs_mapped, nnodes;
    orte_node_t *node;
    orte_proc_t *proc;
    int num_procs_to_assign, navg;
    int extra_procs_to_assign=0, nxtra_nodes=0;
    hwloc_obj_t obj=NULL;
    float balance;
    bool add_one=false;
    bool oversubscribed=false;

    opal_output_verbose(2, orte_rmaps_base_framework.framework_output,
                        "mca:rmaps:rr: mapping by node for job %s app %d slots %d num_procs %lu",
                        ORTE_JOBID_PRINT(jdata->jobid), (int)app->idx,
                        (int)num_slots, (unsigned long)num_procs);

    /* quick check to see if we can map all the procs */
    if (num_slots < (int)app->num_procs) {
        if (ORTE_MAPPING_NO_OVERSUBSCRIBE & ORTE_GET_MAPPING_DIRECTIVE(jdata->map->mapping)) {
            orte_show_help("help-orte-rmaps-base.txt", "orte-rmaps-base:alloc-error",
                           true, app->num_procs, app->app, orte_process_info.nodename);
            ORTE_UPDATE_EXIT_STATUS(ORTE_ERROR_DEFAULT_EXIT_CODE);
            return ORTE_ERR_SILENT;
        }
        oversubscribed = true;
    }

    nnodes = opal_list_get_size(node_list);
    nprocs_mapped = 0;

    do {
        /* divide the procs evenly across all nodes - this is the
         * average we have to maintain as we go, but we adjust
         * the number on each node to reflect its available slots.
         * Obviously, if all nodes have the same number of slots,
         * then the avg is what we get on each node - this is
         * the most common situation.
         */
        navg = ((int)app->num_procs - nprocs_mapped) / nnodes;
        if (0 == navg) {
            /* if there are less procs than nodes, we have to
             * place at least one/node
             */
            navg = 1;
        }

        /* compute how many extra procs to put on each node */
        balance = (float)(((int)app->num_procs - nprocs_mapped) - (navg * nnodes)) / (float)nnodes;
        extra_procs_to_assign = (int)balance;
        nxtra_nodes = 0;
        add_one = false;
        if (0 < (balance - (float)extra_procs_to_assign)) {
            /* compute how many nodes need an extra proc */
            nxtra_nodes = ((int)app->num_procs - nprocs_mapped) - ((navg + extra_procs_to_assign) * nnodes);
            /* add one so that we add an extra proc to the first nodes
             * until all procs are mapped
             */
            extra_procs_to_assign++;
            /* flag that we added one */
            add_one = true;
        }

        opal_output_verbose(2, orte_rmaps_base_framework.framework_output,
                            "mca:rmaps:rr: mapping by node navg %d extra_procs %d extra_nodes %d",
                            navg, extra_procs_to_assign, nxtra_nodes);

        nnodes = 0;
        OPAL_LIST_FOREACH(node, node_list, orte_node_t) {
            /* get the root object as we are not assigning
             * locale except at the node level
             */
            if (NULL != node->topology && NULL != node->topology->topo) {
                obj = hwloc_get_root_obj(node->topology->topo);
            }
            /* add this node to the map, but only do so once */
            if (!ORTE_FLAG_TEST(node, ORTE_NODE_FLAG_MAPPED)) {
                ORTE_FLAG_SET(node, ORTE_NODE_FLAG_MAPPED);
                OBJ_RETAIN(node);
                opal_pointer_array_add(jdata->map->nodes, node);
                ++(jdata->map->num_nodes);
            }
            if (orte_rmaps_base_pernode) {
                num_procs_to_assign = 1;
            } else if (0 < orte_rmaps_base_n_pernode) {
                num_procs_to_assign = orte_rmaps_base_n_pernode;
            } else if (0 < orte_rmaps_base_n_persocket) {
                num_procs_to_assign = orte_rmaps_base_n_persocket * opal_hwloc_base_get_nbobjs_by_type(node->topology->topo, HWLOC_OBJ_PACKAGE, 0, OPAL_HWLOC_AVAILABLE);
            } else  if (oversubscribed) {
                /* compute the number of procs to go on this node */
                if (add_one) {
                    if (0 == nxtra_nodes) {
                        --extra_procs_to_assign;
                        add_one = false;
                    } else {
                        --nxtra_nodes;
                    }
                }
                /* everybody just takes their share */
                num_procs_to_assign = navg + extra_procs_to_assign;
            } else if (node->slots <= node->slots_inuse) {
                /* since we are not oversubcribed, ignore this node */
                continue;
            } else {
                /* if we are not oversubscribed, then there are enough
                 * slots to handle all the procs. However, not every
                 * node will have the same number of slots, so we
                 * have to track how many procs to "shift" elsewhere
                 * to make up the difference
                 */

                /* compute the number of procs to go on this node */
                if (add_one) {
                    if (0 == nxtra_nodes) {
                        --extra_procs_to_assign;
                        add_one = false;
                    } else {
                        --nxtra_nodes;
                    }
                }
                /* if slots < avg + extra (adjusted for cpus/proc), then try to take all */
                if ((node->slots - node->slots_inuse) < (navg + extra_procs_to_assign)) {
                    num_procs_to_assign = node->slots - node->slots_inuse;
                    /* if we can't take any proc, skip following steps */
                    if (num_procs_to_assign == 0) {
                        continue;
                    }
                } else {
                /* take the avg + extra */
                    num_procs_to_assign = navg + extra_procs_to_assign;
                }
                OPAL_OUTPUT_VERBOSE((20, orte_rmaps_base_framework.framework_output,
                                     "%s NODE %s AVG %d ASSIGN %d EXTRA %d",
                                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), node->name,
                                     navg, num_procs_to_assign, extra_procs_to_assign));
            }
            nnodes++; // track how many nodes remain available
            OPAL_OUTPUT_VERBOSE((20, orte_rmaps_base_framework.framework_output,
                                 "%s NODE %s ASSIGNING %d",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), node->name,
                                 num_procs_to_assign));
            for (j=0; j < num_procs_to_assign && nprocs_mapped < app->num_procs; j++) {
                if (NULL == (proc = orte_rmaps_base_setup_proc(jdata, node, app->idx))) {
                    return ORTE_ERR_OUT_OF_RESOURCE;
                }
                nprocs_mapped++;
                orte_set_attribute(&proc->attributes, ORTE_PROC_HWLOC_LOCALE, ORTE_ATTR_LOCAL, obj, OPAL_PTR);
            }
            /* not all nodes are equal, so only set oversubscribed for
             * this node if it is in that state
             */
            if (node->slots < (int)node->num_procs) {
                /* flag the node as oversubscribed so that sched-yield gets
                 * properly set
                 */
                ORTE_FLAG_SET(node, ORTE_NODE_FLAG_OVERSUBSCRIBED);
                ORTE_FLAG_SET(jdata, ORTE_JOB_FLAG_OVERSUBSCRIBED);
                /* check for permission */
                if (ORTE_FLAG_TEST(node, ORTE_NODE_FLAG_SLOTS_GIVEN)) {
                    /* if we weren't given a directive either way, then we will error out
                     * as the #slots were specifically given, either by the host RM or
                     * via hostfile/dash-host */
                    if (!(ORTE_MAPPING_SUBSCRIBE_GIVEN & ORTE_GET_MAPPING_DIRECTIVE(jdata->map->mapping))) {
                        orte_show_help("help-orte-rmaps-base.txt", "orte-rmaps-base:alloc-error",
                                       true, app->num_procs, app->app, orte_process_info.nodename);
                        ORTE_UPDATE_EXIT_STATUS(ORTE_ERROR_DEFAULT_EXIT_CODE);
                        return ORTE_ERR_SILENT;
                    } else if (ORTE_MAPPING_NO_OVERSUBSCRIBE & ORTE_GET_MAPPING_DIRECTIVE(jdata->map->mapping)) {
                        /* if we were explicitly told not to oversubscribe, then don't */
                        orte_show_help("help-orte-rmaps-base.txt", "orte-rmaps-base:alloc-error",
                                       true, app->num_procs, app->app, orte_process_info.nodename);
                        ORTE_UPDATE_EXIT_STATUS(ORTE_ERROR_DEFAULT_EXIT_CODE);
                        return ORTE_ERR_SILENT;
                    }
                }
            }
            if (nprocs_mapped == app->num_procs) {
                /* we are done */
                break;
            }
        }
    } while (nprocs_mapped < app->num_procs && 0 < nnodes);

    /* now fillin as required until fully mapped */
    while (nprocs_mapped < app->num_procs) {
        OPAL_LIST_FOREACH(node, node_list, orte_node_t) {
            /* get the root object as we are not assigning
             * locale except at the node level
             */
            if (NULL != node->topology && NULL != node->topology->topo) {
                obj = hwloc_get_root_obj(node->topology->topo);
            }

           OPAL_OUTPUT_VERBOSE((20, orte_rmaps_base_framework.framework_output,
                                 "%s ADDING PROC TO NODE %s",
                                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), node->name));
            if (NULL == (proc = orte_rmaps_base_setup_proc(jdata, node, app->idx))) {
                return ORTE_ERR_OUT_OF_RESOURCE;
            }
            nprocs_mapped++;
            orte_set_attribute(&proc->attributes, ORTE_PROC_HWLOC_LOCALE, ORTE_ATTR_LOCAL, obj, OPAL_PTR);
            /* not all nodes are equal, so only set oversubscribed for
             * this node if it is in that state
             */
            if (node->slots < (int)node->num_procs) {
                /* flag the node as oversubscribed so that sched-yield gets
                 * properly set
                 */
                ORTE_FLAG_SET(node, ORTE_NODE_FLAG_OVERSUBSCRIBED);
                ORTE_FLAG_SET(jdata, ORTE_JOB_FLAG_OVERSUBSCRIBED);
            }
            if (nprocs_mapped == app->num_procs) {
                /* we are done */
                break;
            }
        }
    }

    return ORTE_SUCCESS;
}

static  int byobj_span(orte_job_t *jdata,
                       orte_app_context_t *app,
                       opal_list_t *node_list,
                       orte_std_cntr_t num_slots,
                       orte_vpid_t num_procs,
                       hwloc_obj_type_t target, unsigned cache_level);

/* mapping by hwloc object looks a lot like mapping by node,
 * but has the added complication of possibly having different
 * numbers of objects on each node
 */
int orte_rmaps_rr_byobj(orte_job_t *jdata,
                        orte_app_context_t *app,
                        opal_list_t *node_list,
                        orte_std_cntr_t num_slots,
                        orte_vpid_t num_procs,
                        hwloc_obj_type_t target, unsigned cache_level)
{
    int i, nmapped, nprocs_mapped;
    orte_node_t *node;
    orte_proc_t *proc;
    int nprocs, start;
    hwloc_obj_t obj=NULL;
    unsigned int nobjs;
    bool add_one;
    bool second_pass;

    /* there are two modes for mapping by object: span and not-span. The
     * span mode essentially operates as if there was just a single
     * "super-node" in the system - i.e., it balances the load across
     * all objects of the indicated type regardless of their location.
     * In essence, it acts as if we placed one proc on each object, cycling
     * across all objects on all nodes, and then wrapped around to place
     * another proc on each object, doing so until all procs were placed.
     *
     * In contrast, the non-span mode operates similar to byslot mapping.
     * All slots on each node are filled, assigning each proc to an object
     * on that node in a balanced fashion, and then the mapper moves on
     * to the next node. Thus, procs tend to be "front loaded" onto the
     * list of nodes, as opposed to being "load balanced" in the span mode
     */
    if (ORTE_MAPPING_SPAN & ORTE_GET_MAPPING_DIRECTIVE(jdata->map->mapping)) {
        return byobj_span(jdata, app, node_list, num_slots,
                          num_procs, target, cache_level);
    }

    opal_output_verbose(2, orte_rmaps_base_framework.framework_output,
                        "mca:rmaps:rr: mapping no-span by %s for job %s slots %d num_procs %lu",
                        hwloc_obj_type_string(target),
                        ORTE_JOBID_PRINT(jdata->jobid),
                        (int)num_slots, (unsigned long)num_procs);

    /* quick check to see if we can map all the procs */
    if (num_slots < app->num_procs) {
        if (ORTE_MAPPING_NO_OVERSUBSCRIBE & ORTE_GET_MAPPING_DIRECTIVE(jdata->map->mapping)) {
            orte_show_help("help-orte-rmaps-base.txt", "orte-rmaps-base:alloc-error",
                           true, app->num_procs, app->app, orte_process_info.nodename);
            ORTE_UPDATE_EXIT_STATUS(ORTE_ERROR_DEFAULT_EXIT_CODE);
            return ORTE_ERR_SILENT;
        }
    }

    /* we know we have enough slots, or that oversubscrption is allowed, so
     * start mapping procs onto objects, filling each object as we go until
     * all procs are mapped. If one pass doesn't catch all the required procs,
     * then loop thru the list again to handle the oversubscription
     */
    nprocs_mapped = 0;
    second_pass = false;
    do {
        add_one = false;
        OPAL_LIST_FOREACH(node, node_list, orte_node_t) {
            if (NULL == node->topology || NULL == node->topology->topo) {
                orte_show_help("help-orte-rmaps-ppr.txt", "ppr-topo-missing",
                               true, node->name);
                return ORTE_ERR_SILENT;
            }
            start = 0;
            /* get the number of objects of this type on this node */
            nobjs = opal_hwloc_base_get_nbobjs_by_type(node->topology->topo, target, cache_level, OPAL_HWLOC_AVAILABLE);
            if (0 == nobjs) {
                continue;
            }
            opal_output_verbose(2, orte_rmaps_base_framework.framework_output,
                                "mca:rmaps:rr: found %u %s objects on node %s",
                                nobjs, hwloc_obj_type_string(target), node->name);

            /* if this is a comm_spawn situation, start with the object
             * where the parent left off and increment */
            if (ORTE_JOBID_INVALID != jdata->originator.jobid) {
                start = (jdata->bkmark_obj + 1) % nobjs;
            }
            /* compute the number of procs to go on this node */
            if (orte_rmaps_base_pernode) {
                nprocs = 1;
            } else if (0 < orte_rmaps_base_n_pernode) {
                nprocs = orte_rmaps_base_n_pernode;
            } else if (0 < orte_rmaps_base_n_persocket) {
                if (HWLOC_OBJ_PACKAGE == target) {
                    nprocs = orte_rmaps_base_n_persocket * nobjs;
                } else {
                    nprocs = orte_rmaps_base_n_persocket * opal_hwloc_base_get_nbobjs_by_type(node->topology->topo, HWLOC_OBJ_PACKAGE, 0, OPAL_HWLOC_AVAILABLE);
                }
            } else {
                nprocs = node->slots - node->slots_inuse;
            }
            opal_output_verbose(2, orte_rmaps_base_framework.framework_output,
                                "mca:rmaps:rr: calculated nprocs %d", nprocs);
            if (nprocs < 1) {
                if (second_pass) {
                    /* already checked for oversubscription permission, so at least put
                     * one proc on it
                     */
                    nprocs = 1;
                    /* offset our starting object position to avoid always
                     * hitting the first one
                     */
                    start = node->num_procs % nobjs;
                } else {
                    continue;
                }
            }
            /* add this node to the map, if reqd */
            if (!ORTE_FLAG_TEST(node, ORTE_NODE_FLAG_MAPPED)) {
                ORTE_FLAG_SET(node, ORTE_NODE_FLAG_MAPPED);
                OBJ_RETAIN(node);
                opal_pointer_array_add(jdata->map->nodes, node);
                ++(jdata->map->num_nodes);
            }
            nmapped = 0;
            opal_output_verbose(2, orte_rmaps_base_framework.framework_output,
                                "mca:rmaps:rr: assigning nprocs %d", nprocs);
            do {
                /* loop through the number of objects */
                for (i=0; i < (int)nobjs && nmapped < nprocs && nprocs_mapped < (int)app->num_procs; i++) {
                    opal_output_verbose(20, orte_rmaps_base_framework.framework_output,
                                        "mca:rmaps:rr: assigning proc to object %d", (i+start) % nobjs);
                    /* get the hwloc object */
                    if (NULL == (obj = opal_hwloc_base_get_obj_by_type(node->topology->topo, target, cache_level, (i+start) % nobjs, OPAL_HWLOC_AVAILABLE))) {
                        ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
                        return ORTE_ERR_NOT_FOUND;
                    }
                    if (orte_rmaps_base.cpus_per_rank > (int)opal_hwloc_base_get_npus(node->topology->topo, obj)) {
                        orte_show_help("help-orte-rmaps-base.txt", "mapping-too-low", true,
                                       orte_rmaps_base.cpus_per_rank, opal_hwloc_base_get_npus(node->topology->topo, obj),
                                       orte_rmaps_base_print_mapping(orte_rmaps_base.mapping));
                        return ORTE_ERR_SILENT;
                    }
                    if (NULL == (proc = orte_rmaps_base_setup_proc(jdata, node, app->idx))) {
                        return ORTE_ERR_OUT_OF_RESOURCE;
                    }
                    nprocs_mapped++;
                    nmapped++;
                    orte_set_attribute(&proc->attributes, ORTE_PROC_HWLOC_LOCALE, ORTE_ATTR_LOCAL, obj, OPAL_PTR);
                }
            } while (nmapped < nprocs && nprocs_mapped < (int)app->num_procs);
            add_one = true;
            /* not all nodes are equal, so only set oversubscribed for
             * this node if it is in that state
             */
            if (node->slots < (int)node->num_procs) {
                /* flag the node as oversubscribed so that sched-yield gets
                 * properly set
                 */
                ORTE_FLAG_SET(node, ORTE_NODE_FLAG_OVERSUBSCRIBED);
                ORTE_FLAG_SET(jdata, ORTE_JOB_FLAG_OVERSUBSCRIBED);
                /* check for permission */
                if (ORTE_FLAG_TEST(node, ORTE_NODE_FLAG_SLOTS_GIVEN)) {
                    /* if we weren't given a directive either way, then we will error out
                     * as the #slots were specifically given, either by the host RM or
                     * via hostfile/dash-host */
                    if (!(ORTE_MAPPING_SUBSCRIBE_GIVEN & ORTE_GET_MAPPING_DIRECTIVE(jdata->map->mapping))) {
                        orte_show_help("help-orte-rmaps-base.txt", "orte-rmaps-base:alloc-error",
                                       true, app->num_procs, app->app, orte_process_info.nodename);
                        ORTE_UPDATE_EXIT_STATUS(ORTE_ERROR_DEFAULT_EXIT_CODE);
                        return ORTE_ERR_SILENT;
                    } else if (ORTE_MAPPING_NO_OVERSUBSCRIBE & ORTE_GET_MAPPING_DIRECTIVE(jdata->map->mapping)) {
                        /* if we were explicitly told not to oversubscribe, then don't */
                        orte_show_help("help-orte-rmaps-base.txt", "orte-rmaps-base:alloc-error",
                                       true, app->num_procs, app->app, orte_process_info.nodename);
                        ORTE_UPDATE_EXIT_STATUS(ORTE_ERROR_DEFAULT_EXIT_CODE);
                        return ORTE_ERR_SILENT;
                    }
                }
            }
            if (nprocs_mapped == app->num_procs) {
                /* we are done */
                break;
            }
        }
        second_pass = true;
    } while (add_one && nprocs_mapped < app->num_procs);

    if (nprocs_mapped < app->num_procs) {
        /* usually means there were no objects of the requested type */
        return ORTE_ERR_NOT_FOUND;
    }

    return ORTE_SUCCESS;
}

static int byobj_span(orte_job_t *jdata,
                      orte_app_context_t *app,
                      opal_list_t *node_list,
                      orte_std_cntr_t num_slots,
                      orte_vpid_t num_procs,
                      hwloc_obj_type_t target, unsigned cache_level)
{
    int i, j, nprocs_mapped, navg;
    orte_node_t *node;
    orte_proc_t *proc;
    int nprocs, nxtra_objs;
    hwloc_obj_t obj=NULL;
    unsigned int nobjs;

    opal_output_verbose(2, orte_rmaps_base_framework.framework_output,
                        "mca:rmaps:rr: mapping span by %s for job %s slots %d num_procs %lu",
                        hwloc_obj_type_string(target),
                        ORTE_JOBID_PRINT(jdata->jobid),
                        (int)num_slots, (unsigned long)num_procs);

    /* quick check to see if we can map all the procs */
    if (num_slots < (int)app->num_procs) {
        if (ORTE_MAPPING_NO_OVERSUBSCRIBE & ORTE_GET_MAPPING_DIRECTIVE(jdata->map->mapping)) {
            orte_show_help("help-orte-rmaps-base.txt", "orte-rmaps-base:alloc-error",
                           true, app->num_procs, app->app, orte_process_info.nodename);
            ORTE_UPDATE_EXIT_STATUS(ORTE_ERROR_DEFAULT_EXIT_CODE);
            return ORTE_ERR_SILENT;
        }
    }

    /* we know we have enough slots, or that oversubscrption is allowed, so
     * next determine how many total objects we have to work with
     */
    nobjs = 0;
    OPAL_LIST_FOREACH(node, node_list, orte_node_t) {
        if (NULL == node->topology || NULL == node->topology->topo) {
            orte_show_help("help-orte-rmaps-ppr.txt", "ppr-topo-missing",
                           true, node->name);
            return ORTE_ERR_SILENT;
        }
        /* get the number of objects of this type on this node */
        nobjs += opal_hwloc_base_get_nbobjs_by_type(node->topology->topo, target, cache_level, OPAL_HWLOC_AVAILABLE);
    }

    if (0 == nobjs) {
        return ORTE_ERR_NOT_FOUND;
    }

    /* divide the procs evenly across all objects */
    navg = app->num_procs / nobjs;
    if (0 == navg) {
        /* if there are less procs than objects, we have to
         * place at least one/obj
         */
        navg = 1;
    }

    /* compute how many objs need an extra proc */
    if (0 > (nxtra_objs = app->num_procs - (navg * nobjs))) {
        nxtra_objs = 0;
    }

    opal_output_verbose(2, orte_rmaps_base_framework.framework_output,
                        "mca:rmaps:rr: mapping by %s navg %d extra_objs %d",
                        hwloc_obj_type_string(target),
                        navg, nxtra_objs);

    nprocs_mapped = 0;
    OPAL_LIST_FOREACH(node, node_list, orte_node_t) {
        /* add this node to the map, if reqd */
        if (!ORTE_FLAG_TEST(node, ORTE_NODE_FLAG_MAPPED)) {
            ORTE_FLAG_SET(node, ORTE_NODE_FLAG_MAPPED);
            OBJ_RETAIN(node);
            opal_pointer_array_add(jdata->map->nodes, node);
            ++(jdata->map->num_nodes);
        }
        /* get the number of objects of this type on this node */
        nobjs = opal_hwloc_base_get_nbobjs_by_type(node->topology->topo, target, cache_level, OPAL_HWLOC_AVAILABLE);
        opal_output_verbose(2, orte_rmaps_base_framework.framework_output,
                            "mca:rmaps:rr:byobj: found %d objs on node %s", nobjs, node->name);
        /* loop through the number of objects */
        for (i=0; i < (int)nobjs && nprocs_mapped < (int)app->num_procs; i++) {
            /* get the hwloc object */
            if (NULL == (obj = opal_hwloc_base_get_obj_by_type(node->topology->topo, target, cache_level, i, OPAL_HWLOC_AVAILABLE))) {
                ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
                return ORTE_ERR_NOT_FOUND;
            }
            if (orte_rmaps_base.cpus_per_rank > (int)opal_hwloc_base_get_npus(node->topology->topo, obj)) {
                orte_show_help("help-orte-rmaps-base.txt", "mapping-too-low", true,
                               orte_rmaps_base.cpus_per_rank, opal_hwloc_base_get_npus(node->topology->topo, obj),
                               orte_rmaps_base_print_mapping(orte_rmaps_base.mapping));
                return ORTE_ERR_SILENT;
            }
            /* determine how many to map */
            if (orte_rmaps_base_pernode) {
                nprocs = 1;
            } else if (0 < orte_rmaps_base_n_pernode) {
                nprocs = orte_rmaps_base_n_pernode;
            } else if (0 < orte_rmaps_base_n_persocket) {
                if (HWLOC_OBJ_PACKAGE == target) {
                    nprocs = orte_rmaps_base_n_persocket * nobjs;
                } else {
                    nprocs = orte_rmaps_base_n_persocket * opal_hwloc_base_get_nbobjs_by_type(node->topology->topo, HWLOC_OBJ_PACKAGE, 0, OPAL_HWLOC_AVAILABLE);
                }
            } else {
                nprocs = navg;
            }
            if (0 < nxtra_objs) {
                nprocs++;
                nxtra_objs--;
            }
            /* map the reqd number of procs */
            for (j=0; j < nprocs && nprocs_mapped < app->num_procs; j++) {
                if (NULL == (proc = orte_rmaps_base_setup_proc(jdata, node, app->idx))) {
                    return ORTE_ERR_OUT_OF_RESOURCE;
                }
                nprocs_mapped++;
                orte_set_attribute(&proc->attributes, ORTE_PROC_HWLOC_LOCALE, ORTE_ATTR_LOCAL, obj, OPAL_PTR);
            }
            /* keep track of the node we last used */
            jdata->bookmark = node;
        }
        /* not all nodes are equal, so only set oversubscribed for
         * this node if it is in that state
         */
        if (node->slots < (int)node->num_procs) {
            /* flag the node as oversubscribed so that sched-yield gets
             * properly set
             */
            ORTE_FLAG_SET(node, ORTE_NODE_FLAG_OVERSUBSCRIBED);
            ORTE_FLAG_SET(jdata, ORTE_JOB_FLAG_OVERSUBSCRIBED);
        }
        if (nprocs_mapped == app->num_procs) {
            /* we are done */
            break;
        }
    }

    return ORTE_SUCCESS;
}
