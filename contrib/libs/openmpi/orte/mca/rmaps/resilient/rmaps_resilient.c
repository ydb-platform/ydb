/*
 * Copyright (c) 2009-2011 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2009-2010 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2011-2012 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2014-2017 Intel, Inc. All rights reserved.
 *
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "orte_config.h"
#include "orte/constants.h"
#include "orte/types.h"

#include <errno.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif  /* HAVE_UNISTD_H */
#include <string.h>
#include <stdio.h>

#include "opal/util/argv.h"
#include "opal/class/opal_pointer_array.h"

#include "orte/util/error_strings.h"
#include "orte/util/show_help.h"
#include "orte/mca/errmgr/errmgr.h"

#include "orte/mca/rmaps/base/rmaps_private.h"
#include "orte/mca/rmaps/base/base.h"
#include "rmaps_resilient.h"

static int orte_rmaps_resilient_map(orte_job_t *jdata);
static int resilient_assign(orte_job_t *jdata);

orte_rmaps_base_module_t orte_rmaps_resilient_module = {
    .map_job = orte_rmaps_resilient_map,
    .assign_locations = resilient_assign
};


/*
 * Local variable
 */
static char *orte_getline(FILE *fp);
static bool have_ftgrps=false, made_ftgrps=false;

static int construct_ftgrps(void);
static int get_ftgrp_target(orte_proc_t *proc,
                            orte_rmaps_res_ftgrp_t **target,
                            orte_node_t **nd);
static int get_new_node(orte_proc_t *proc,
                        orte_app_context_t *app,
                        orte_job_map_t *map,
                        orte_node_t **ndret);
static int map_to_ftgrps(orte_job_t *jdata);

/*
 * Loadbalance the cluster
 */
static int orte_rmaps_resilient_map(orte_job_t *jdata)
{
    orte_app_context_t *app;
    int i, j;
    int rc = ORTE_SUCCESS;
    orte_node_t *nd=NULL, *oldnode, *node, *nptr;
    orte_rmaps_res_ftgrp_t *target = NULL;
    orte_proc_t *proc;
    orte_vpid_t totprocs;
    opal_list_t node_list;
    orte_std_cntr_t num_slots;
    opal_list_item_t *item;
    mca_base_component_t *c = &mca_rmaps_resilient_component.super.base_version;
    bool found;

    if (!ORTE_FLAG_TEST(jdata, ORTE_JOB_FLAG_RESTART)) {
        if (NULL != jdata->map->req_mapper &&
            0 != strcasecmp(jdata->map->req_mapper, c->mca_component_name)) {
            /* a mapper has been specified, and it isn't me */
            opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                                "mca:rmaps:resilient: job %s not using resilient mapper",
                                ORTE_JOBID_PRINT(jdata->jobid));
            return ORTE_ERR_TAKE_NEXT_OPTION;
        }
        if (NULL == mca_rmaps_resilient_component.fault_group_file) {
            opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                                "mca:rmaps:resilient: cannot perform initial map of job %s - no fault groups",
                                ORTE_JOBID_PRINT(jdata->jobid));
            return ORTE_ERR_TAKE_NEXT_OPTION;
        }
    } else if (!ORTE_FLAG_TEST(jdata, ORTE_JOB_FLAG_PROCS_MIGRATING)) {
        opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                            "mca:rmaps:resilient: cannot map job %s - not in restart or migrating",
                            ORTE_JOBID_PRINT(jdata->jobid));
        return ORTE_ERR_TAKE_NEXT_OPTION;
    }

    opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                        "mca:rmaps:resilient: mapping job %s",
                        ORTE_JOBID_PRINT(jdata->jobid));

    /* flag that I did the mapping */
    if (NULL != jdata->map->last_mapper) {
        free(jdata->map->last_mapper);
    }
    jdata->map->last_mapper = strdup(c->mca_component_name);

    /* have we already constructed the fault group list? */
    if (!made_ftgrps) {
        construct_ftgrps();
    }

    if (ORTE_JOB_STATE_INIT == jdata->state) {
        /* this is an initial map - let the fault group mapper
         * handle it
         */
        return map_to_ftgrps(jdata);
    }

    /*
     * NOTE: if a proc is being ADDED to an existing job, then its
     * node field will be NULL.
     */
    OPAL_OUTPUT_VERBOSE((1, orte_rmaps_base_framework.framework_output,
                         "%s rmaps:resilient: remapping job %s",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         ORTE_JOBID_PRINT(jdata->jobid)));

    /* cycle through all the procs in this job to find the one(s) that failed */
    for (i=0; i < jdata->procs->size; i++) {
        /* get the proc object */
        if (NULL == (proc = (orte_proc_t*)opal_pointer_array_get_item(jdata->procs, i))) {
            continue;
        }
        OPAL_OUTPUT_VERBOSE((7, orte_rmaps_base_framework.framework_output,
                             "%s PROC %s STATE %s",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             ORTE_NAME_PRINT(&proc->name),
                             orte_proc_state_to_str(proc->state)));
        /* is this proc to be restarted? */
        if (proc->state != ORTE_PROC_STATE_RESTART) {
            continue;
        }
        /* save the current node */
        oldnode = proc->node;
        /* point to the app */
        app = (orte_app_context_t*)opal_pointer_array_get_item(jdata->apps, proc->app_idx);
        if( NULL == app ) {
            ORTE_ERROR_LOG(ORTE_ERR_FAILED_TO_MAP);
            rc = ORTE_ERR_FAILED_TO_MAP;
            goto error;
        }

        if (NULL == oldnode) {
            OPAL_OUTPUT_VERBOSE((1, orte_rmaps_base_framework.framework_output,
                                 "%s rmaps:resilient: proc %s is to be started",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 ORTE_NAME_PRINT(&proc->name)));
        } else {
            OPAL_OUTPUT_VERBOSE((1, orte_rmaps_base_framework.framework_output,
                                 "%s rmaps:resilient: proc %s from node %s[%s] is to be restarted",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 ORTE_NAME_PRINT(&proc->name),
                                 (NULL == oldnode->name) ? "NULL" : oldnode->name,
                                 (NULL == oldnode->daemon) ? "--" : ORTE_VPID_PRINT(oldnode->daemon->name.vpid)));
        }

        if (NULL == oldnode) {
            /* this proc was not previously running - likely it is being added
             * to the job. So place it on the node with the fewest procs to
             * balance the load
             */
            OBJ_CONSTRUCT(&node_list, opal_list_t);
            if (ORTE_SUCCESS != (rc = orte_rmaps_base_get_target_nodes(&node_list,
                                                                       &num_slots,
                                                                       app,
                                                                       jdata->map->mapping,
                                                                       false, false))) {
                ORTE_ERROR_LOG(rc);
                while (NULL != (item = opal_list_remove_first(&node_list))) {
                    OBJ_RELEASE(item);
                }
                OBJ_DESTRUCT(&node_list);
                goto error;
            }
            if (opal_list_is_empty(&node_list)) {
                /* put the proc on "hold" until resources are available */
                OBJ_DESTRUCT(&node_list);
                proc->state = ORTE_PROC_STATE_MIGRATING;
                rc = ORTE_ERR_OUT_OF_RESOURCE;
                goto error;
            }
            totprocs = 1000000;
            nd = NULL;
            while (NULL != (item = opal_list_remove_first(&node_list))) {
                node = (orte_node_t*)item;
                if (node->num_procs < totprocs) {
                    nd = node;
                    totprocs = node->num_procs;
                }
                OBJ_RELEASE(item); /* maintain accounting */
            }
            OBJ_DESTRUCT(&node_list);
            /* we already checked to ensure there was at least one node,
             * so we couldn't have come out of the loop with nd=NULL
             */
            OPAL_OUTPUT_VERBOSE((1, orte_rmaps_base_framework.framework_output,
                                 "%s rmaps:resilient: Placing new process on node %s[%s] (no ftgrp)",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 nd->name,
                                 (NULL == nd->daemon) ? "--" : ORTE_VPID_PRINT(nd->daemon->name.vpid)));
        } else {

            OPAL_OUTPUT_VERBOSE((1, orte_rmaps_base_framework.framework_output,
                                 "%s rmaps:resilient: proc %s from node %s is to be restarted",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 ORTE_NAME_PRINT(&proc->name),
                                 (NULL == proc->node) ? "NULL" : proc->node->name));

            /* if we have fault groups, use them */
            if (have_ftgrps) {
                if (ORTE_SUCCESS != (rc = get_ftgrp_target(proc, &target, &nd))) {
                    ORTE_ERROR_LOG(rc);
                    goto error;
                }
                OPAL_OUTPUT_VERBOSE((1, orte_rmaps_base_framework.framework_output,
                                     "%s rmaps:resilient: placing proc %s into fault group %d node %s",
                                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                     ORTE_NAME_PRINT(&proc->name), target->ftgrp, nd->name));
            } else {
                if (ORTE_SUCCESS != (rc = get_new_node(proc, app, jdata->map, &nd))) {
                    ORTE_ERROR_LOG(rc);
                    return rc;
                }
            }
        }
        /* add node to map if necessary - nothing we can do here
         * but search for it
         */
        found = false;
        for (j=0; j < jdata->map->nodes->size; j++) {
            if (NULL == (nptr = (orte_node_t*)opal_pointer_array_get_item(jdata->map->nodes, j))) {
                continue;
            }
            if (nptr == nd) {
                found = true;
                break;
            }
        }
        if (!found) {
            OBJ_RETAIN(nd);
            opal_pointer_array_add(jdata->map->nodes, nd);
            ORTE_FLAG_SET(nd, ORTE_NODE_FLAG_MAPPED);
        }
        OBJ_RETAIN(nd);  /* maintain accounting on object */
        proc->node = nd;
        nd->num_procs++;
        opal_pointer_array_add(nd->procs, (void*)proc);
        /* retain the proc struct so that we correctly track its release */
        OBJ_RETAIN(proc);

        /* flag the proc state as non-launched so we'll know to launch it */
        proc->state = ORTE_PROC_STATE_INIT;

        /* update the node and local ranks so static ports can
         * be properly selected if active
         */
        orte_rmaps_base_update_local_ranks(jdata, oldnode, nd, proc);
    }

 error:
    return rc;
}

static int resilient_assign(orte_job_t *jdata)
{
    mca_base_component_t *c = &mca_rmaps_resilient_component.super.base_version;

    if (NULL == jdata->map->last_mapper ||
        0 != strcasecmp(jdata->map->last_mapper, c->mca_component_name)) {
        /* a mapper has been specified, and it isn't me */
        opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                            "mca:rmaps:resilient: job %s not using resilient assign: %s",
                            ORTE_JOBID_PRINT(jdata->jobid),
                            (NULL == jdata->map->last_mapper) ? "NULL" : jdata->map->last_mapper);
        return ORTE_ERR_TAKE_NEXT_OPTION;
    }

    return ORTE_ERR_NOT_IMPLEMENTED;
}

static char *orte_getline(FILE *fp)
{
    char *ret, *buff;
    char input[1024];

    ret = fgets(input, 1024, fp);
    if (NULL != ret) {
        input[strlen(input)-1] = '\0';  /* remove newline */
        buff = strdup(input);
        return buff;
    }

    return NULL;
}


static int construct_ftgrps(void)
{
    orte_rmaps_res_ftgrp_t *ftgrp;
    orte_node_t *node;
    FILE *fp;
    char *ftinput;
    int grp;
    char **nodes;
    bool found;
    int i, k;

    /* flag that we did this */
    made_ftgrps = true;

    if (NULL == mca_rmaps_resilient_component.fault_group_file) {
        /* nothing to build */
        return ORTE_SUCCESS;
    }

    /* construct it */
    OPAL_OUTPUT_VERBOSE((1, orte_rmaps_base_framework.framework_output,
                         "%s rmaps:resilient: constructing fault groups",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));
    fp = fopen(mca_rmaps_resilient_component.fault_group_file, "r");
    if (NULL == fp) { /* not found */
        orte_show_help("help-orte-rmaps-resilient.txt", "orte-rmaps-resilient:file-not-found",
                       true, mca_rmaps_resilient_component.fault_group_file);
        return ORTE_ERR_FAILED_TO_MAP;
    }

    /* build list of fault groups */
    grp = 0;
    while (NULL != (ftinput = orte_getline(fp))) {
        ftgrp = OBJ_NEW(orte_rmaps_res_ftgrp_t);
        ftgrp->ftgrp = grp++;
        nodes = opal_argv_split(ftinput, ',');
        /* find the referenced nodes */
        for (k=0; k < opal_argv_count(nodes); k++) {
            found = false;
            for (i=0; i < orte_node_pool->size && !found; i++) {
                if (NULL == (node = opal_pointer_array_get_item(orte_node_pool, i))) {
                    continue;
                }
                if (0 == strcmp(node->name, nodes[k])) {
                    OBJ_RETAIN(node);
                    OPAL_OUTPUT_VERBOSE((1, orte_rmaps_base_framework.framework_output,
                                         "%s rmaps:resilient: adding node %s to fault group %d",
                                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                         node->name, ftgrp->ftgrp));
                    opal_pointer_array_add(&ftgrp->nodes, node);
                    found = true;
                    break;
                }
            }
        }
        opal_list_append(&mca_rmaps_resilient_component.fault_grps, &ftgrp->super);
        opal_argv_free(nodes);
        free(ftinput);
    }
    fclose(fp);

    /* flag that we have fault grps */
    have_ftgrps = true;
    return ORTE_SUCCESS;
}

static int get_ftgrp_target(orte_proc_t *proc,
                            orte_rmaps_res_ftgrp_t **tgt,
                            orte_node_t **ndret)
{
    opal_list_item_t *item;
    int k, totnodes;
    orte_node_t *node, *nd;
    orte_rmaps_res_ftgrp_t *target, *ftgrp;
    float avgload, minload;
    orte_vpid_t totprocs, lowprocs;

    /* set defaults */
    *tgt = NULL;
    *ndret = NULL;

    /* flag all the fault groups that
     * include this node so we don't reuse them
     */
    minload = 1000000.0;
    target = NULL;
    for (item = opal_list_get_first(&mca_rmaps_resilient_component.fault_grps);
         item != opal_list_get_end(&mca_rmaps_resilient_component.fault_grps);
         item = opal_list_get_next(item)) {
        ftgrp = (orte_rmaps_res_ftgrp_t*)item;
        /* see if the node is in this fault group */
        ftgrp->included = true;
        ftgrp->used = false;
        for (k=0; k < ftgrp->nodes.size; k++) {
            if (NULL == (node = (orte_node_t*)opal_pointer_array_get_item(&ftgrp->nodes, k))) {
                continue;
            }
            if (NULL != proc->node && 0 == strcmp(node->name, proc->node->name)) {
                /* yes - mark it to not be included */
                OPAL_OUTPUT_VERBOSE((1, orte_rmaps_base_framework.framework_output,
                                     "%s rmaps:resilient: node %s is in fault group %d, which will be excluded",
                                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                     proc->node->name, ftgrp->ftgrp));
                ftgrp->included = false;
                break;
            }
        }
        /* if this ftgrp is not included, then skip it */
        if (!ftgrp->included) {
            continue;
        }
        /* compute the load average on this fault group */
        totprocs = 0;
        totnodes = 0;
        for (k=0; k < ftgrp->nodes.size; k++) {
            if (NULL == (node = (orte_node_t*)opal_pointer_array_get_item(&ftgrp->nodes, k))) {
                continue;
            }
            totnodes++;
            totprocs += node->num_procs;
        }
        avgload = (float)totprocs / (float)totnodes;
        /* now find the lightest loaded of the included fault groups */
        if (avgload < minload) {
            minload = avgload;
            target = ftgrp;
            OPAL_OUTPUT_VERBOSE((2, orte_rmaps_base_framework.framework_output,
                                 "%s rmaps:resilient: found new min load ftgrp %d",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 ftgrp->ftgrp));
        }
    }

    if (NULL == target) {
        /* nothing found */
        return ORTE_ERR_NOT_FOUND;
    }

    /* if we did find a target, re-map the proc to the lightest loaded
     * node in that group
     */
    lowprocs = 1000000;
    nd = NULL;
    for (k=0; k < target->nodes.size; k++) {
        if (NULL == (node = (orte_node_t*)opal_pointer_array_get_item(&target->nodes, k))) {
            continue;
        }
        if (node->num_procs < lowprocs) {
            lowprocs = node->num_procs;
            nd = node;
        }
    }

    /* return the results */
    *tgt = target;
    *ndret = nd;

    return ORTE_SUCCESS;
}

static int get_new_node(orte_proc_t *proc,
                        orte_app_context_t *app,
                        orte_job_map_t *map,
                        orte_node_t **ndret)
{
    orte_node_t *nd, *oldnode, *node;
    orte_proc_t *pptr;
    int rc, j;
    opal_list_t node_list, candidates;
    opal_list_item_t *item, *next;
    orte_std_cntr_t num_slots;
    bool found;

    /* set defaults */
    *ndret = NULL;
    nd = NULL;
    oldnode = NULL;
    orte_get_attribute(&proc->attributes, ORTE_PROC_PRIOR_NODE, (void**)&oldnode, OPAL_PTR);

    /*
     * Get a list of all nodes
     */
    OBJ_CONSTRUCT(&node_list, opal_list_t);
    if (ORTE_SUCCESS != (rc = orte_rmaps_base_get_target_nodes(&node_list,
                                                               &num_slots,
                                                               app,
                                                               map->mapping,
                                                               false, false))) {
        ORTE_ERROR_LOG(rc);
        goto release;
    }
    if (opal_list_is_empty(&node_list)) {
        ORTE_ERROR_LOG(ORTE_ERR_OUT_OF_RESOURCE);
        rc = ORTE_ERR_OUT_OF_RESOURCE;
        goto release;
    }

    if (1 == opal_list_get_size(&node_list)) {
        /* if we have only one node, all we can do is put the proc on that
         * node, even if it is the same one - better than not restarting at
         * all
         */
        nd = (orte_node_t*)opal_list_get_first(&node_list);
        orte_set_attribute(&proc->attributes, ORTE_PROC_PRIOR_NODE, ORTE_ATTR_LOCAL, oldnode, OPAL_PTR);
        OPAL_OUTPUT_VERBOSE((1, orte_rmaps_base_framework.framework_output,
                             "%s rmaps:resilient: Placing process %s on node %s[%s] (only one avail node)",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             ORTE_NAME_PRINT(&proc->name),
                             nd->name,
                             (NULL == nd->daemon) ? "--" : ORTE_VPID_PRINT(nd->daemon->name.vpid)));
        goto release;
    }

    /*
     * Cycle thru the list, transferring
     * all available nodes to the candidate list
     * so we can get them in the right order
     *
     */
    OBJ_CONSTRUCT(&candidates, opal_list_t);
    while (NULL != (item = opal_list_remove_first(&node_list))) {
        node = (orte_node_t*)item;
        /* don't put it back on current node */
        if (node == oldnode) {
            OBJ_RELEASE(item);
            continue;
        }
        if (0 == node->num_procs) {
            OPAL_OUTPUT_VERBOSE((7, orte_rmaps_base_framework.framework_output,
                                 "%s PREPENDING EMPTY NODE %s[%s] TO CANDIDATES",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 (NULL == node->name) ? "NULL" : node->name,
                                 (NULL == node->daemon) ? "--" : ORTE_VPID_PRINT(node->daemon->name.vpid)));
            opal_list_prepend(&candidates, item);
        } else {
            OPAL_OUTPUT_VERBOSE((7, orte_rmaps_base_framework.framework_output,
                                 "%s APPENDING NON-EMPTY NODE %s[%s] TO CANDIDATES",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 (NULL == node->name) ? "NULL" : node->name,
                                 (NULL == node->daemon) ? "--" : ORTE_VPID_PRINT(node->daemon->name.vpid)));
            opal_list_append(&candidates, item);
        }
    }
    /* search the candidates
     * try to use a semi-intelligent selection logic here that:
     *
     * (a) avoids putting the proc on a node where a peer is already
     *     located as this degrades our fault tolerance
     *
     * (b) avoids "ricochet effect" where a process would ping-pong
     *     between two nodes as it fails
     */
    nd = NULL;
    item = opal_list_get_first(&candidates);
    while (item != opal_list_get_end(&candidates)) {
        node = (orte_node_t*)item;
        next = opal_list_get_next(item);
        /* don't return to our prior location to avoid
         * "ricochet" effect
         */
        if (NULL != oldnode && node == oldnode) {
            OPAL_OUTPUT_VERBOSE((7, orte_rmaps_base_framework.framework_output,
                                 "%s REMOVING PRIOR NODE %s[%s] FROM CANDIDATES",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 (NULL == node->name) ? "NULL" : node->name,
                                 (NULL == node->daemon) ? "--" : ORTE_VPID_PRINT(node->daemon->name.vpid)));
            opal_list_remove_item(&candidates, item);
            OBJ_RELEASE(item);  /* maintain acctg */
            item = next;
            continue;
        }
        /* if this node is empty, then it is the winner */
        if (0 == node->num_procs) {
            nd = node;
            orte_set_attribute(&proc->attributes, ORTE_PROC_PRIOR_NODE, ORTE_ATTR_LOCAL, oldnode, OPAL_PTR);
            break;
        }
        /* if this node has someone from my job, then skip it
         * to avoid (a)
         */
        found = false;
        for (j=0; j < node->procs->size; j++) {
            if (NULL == (pptr = (orte_proc_t*)opal_pointer_array_get_item(node->procs, j))) {
                continue;
            }
            if (pptr->name.jobid == proc->name.jobid) {
                OPAL_OUTPUT_VERBOSE((7, orte_rmaps_base_framework.framework_output,
                                     "%s FOUND PEER %s ON NODE %s[%s]",
                                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                     ORTE_NAME_PRINT(&pptr->name),
                                     (NULL == node->name) ? "NULL" : node->name,
                                     (NULL == node->daemon) ? "--" : ORTE_VPID_PRINT(node->daemon->name.vpid)));
                found = true;
                break;
            }
        }
        if (found) {
            item = next;
            continue;
        }
        /* get here if all tests pass - take this node */
        nd = node;
        orte_set_attribute(&proc->attributes, ORTE_PROC_PRIOR_NODE, ORTE_ATTR_LOCAL, oldnode, OPAL_PTR);
        break;
    }
    if (NULL == nd) {
        /* didn't find anything */
        if (NULL != oldnode) {
            nd = oldnode;
            OPAL_OUTPUT_VERBOSE((1, orte_rmaps_base_framework.framework_output,
                                 "%s rmaps:resilient: Placing process %s on prior node %s[%s] (no ftgrp)",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 ORTE_NAME_PRINT(&proc->name),
                                 (NULL == nd->name) ? "NULL" : nd->name,
                                 (NULL == nd->daemon) ? "--" : ORTE_VPID_PRINT(nd->daemon->name.vpid)));
        } else {
            nd = proc->node;
            orte_set_attribute(&proc->attributes, ORTE_PROC_PRIOR_NODE, ORTE_ATTR_LOCAL, nd, OPAL_PTR);
            OPAL_OUTPUT_VERBOSE((1, orte_rmaps_base_framework.framework_output,
                                 "%s rmaps:resilient: Placing process %s back on same node %s[%s] (no ftgrp)",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 ORTE_NAME_PRINT(&proc->name),
                                 (NULL == nd->name) ? "NULL" : nd->name,
                                 (NULL == nd->daemon) ? "--" : ORTE_VPID_PRINT(nd->daemon->name.vpid)));
        }

    }
    /* cleanup candidate list */
    while (NULL != (item = opal_list_remove_first(&candidates))) {
        OBJ_RELEASE(item);
    }
    OBJ_DESTRUCT(&candidates);

 release:
    OPAL_OUTPUT_VERBOSE((1, orte_rmaps_base_framework.framework_output,
                         "%s rmaps:resilient: Placing process on node %s[%s] (no ftgrp)",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         (NULL == nd->name) ? "NULL" : nd->name,
                         (NULL == nd->daemon) ? "--" : ORTE_VPID_PRINT(nd->daemon->name.vpid)));

    while (NULL != (item = opal_list_remove_first(&node_list))) {
        OBJ_RELEASE(item);
    }
    OBJ_DESTRUCT(&node_list);

    *ndret = nd;
    return rc;
}

static void flag_nodes(opal_list_t *node_list)
{
    opal_list_item_t *item, *nitem;
    orte_node_t *node, *nd;
    orte_rmaps_res_ftgrp_t *ftgrp;
    int k;

    for (item = opal_list_get_first(&mca_rmaps_resilient_component.fault_grps);
         item != opal_list_get_end(&mca_rmaps_resilient_component.fault_grps);
         item = opal_list_get_next(item)) {
        ftgrp = (orte_rmaps_res_ftgrp_t*)item;
        /* reset the flags */
        ftgrp->used = false;
        ftgrp->included = false;
        /* if at least one node in our list is included in this
         * ftgrp, then flag it as included
         */
        for (nitem = opal_list_get_first(node_list);
             !ftgrp->included && nitem != opal_list_get_end(node_list);
             nitem = opal_list_get_next(nitem)) {
            node = (orte_node_t*)nitem;
            for (k=0; k < ftgrp->nodes.size; k++) {
                if (NULL == (nd = (orte_node_t*)opal_pointer_array_get_item(&ftgrp->nodes, k))) {
                    continue;
                }
                if (0 == strcmp(nd->name, node->name)) {
                    ftgrp->included = true;
                    break;
                }
            }
        }
    }
}

static int map_to_ftgrps(orte_job_t *jdata)
{
    orte_job_map_t *map;
    orte_app_context_t *app;
    int i, j, k, totnodes;
    opal_list_t node_list;
    opal_list_item_t *item, *next, *curitem;
    orte_std_cntr_t num_slots;
    int rc = ORTE_SUCCESS;
    float avgload, minload;
    orte_node_t *node, *nd=NULL;
    orte_rmaps_res_ftgrp_t *ftgrp, *target = NULL;
    orte_vpid_t totprocs, num_assigned;
    bool initial_map=true;

    OPAL_OUTPUT_VERBOSE((1, orte_rmaps_base_framework.framework_output,
                         "%s rmaps:resilient: creating initial map for job %s",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         ORTE_JOBID_PRINT(jdata->jobid)));

    /* start at the beginning... */
    jdata->num_procs = 0;
    map = jdata->map;

    for (i=0; i < jdata->apps->size; i++) {
        /* get the app_context */
        if (NULL == (app = (orte_app_context_t*)opal_pointer_array_get_item(jdata->apps, i))) {
            continue;
        }
        /* you cannot use this mapper unless you specify the number of procs to
         * launch for each app
         */
        if (0 == app->num_procs) {
            orte_show_help("help-orte-rmaps-resilient.txt",
                           "orte-rmaps-resilient:num-procs",
                           true);
            return ORTE_ERR_SILENT;
        }
        num_assigned = 0;
        /* for each app_context, we have to get the list of nodes that it can
         * use since that can now be modified with a hostfile and/or -host
         * option
         */
        OBJ_CONSTRUCT(&node_list, opal_list_t);
        if (ORTE_SUCCESS != (rc = orte_rmaps_base_get_target_nodes(&node_list, &num_slots, app,
                                                                   map->mapping, initial_map, false))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }
        /* flag that all subsequent requests should not reset the node->mapped flag */
        initial_map = false;

        /* remove all nodes that are not "up" or do not have a running daemon on them */
        item = opal_list_get_first(&node_list);
        while (item != opal_list_get_end(&node_list)) {
            next = opal_list_get_next(item);
            node = (orte_node_t*)item;
            if (ORTE_NODE_STATE_UP != node->state ||
                NULL == node->daemon ||
                ORTE_PROC_STATE_RUNNING != node->daemon->state) {
                opal_list_remove_item(&node_list, item);
                OBJ_RELEASE(item);
            }
            item = next;
        }
        curitem = opal_list_get_first(&node_list);

        /* flag the fault groups included by these nodes */
        flag_nodes(&node_list);
        /* map each copy to a different fault group - if more copies are
         * specified than fault groups, then overlap in a round-robin fashion
         */
        for (j=0; j < app->num_procs; j++) {
            /* find unused included fault group with lowest average load - if none
             * found, then break
             */
            target = NULL;
            minload = 1000000000.0;
            for (item = opal_list_get_first(&mca_rmaps_resilient_component.fault_grps);
                 item != opal_list_get_end(&mca_rmaps_resilient_component.fault_grps);
                 item = opal_list_get_next(item)) {
                ftgrp = (orte_rmaps_res_ftgrp_t*)item;
                OPAL_OUTPUT_VERBOSE((2, orte_rmaps_base_framework.framework_output,
                                     "%s rmaps:resilient: fault group %d used: %s included %s",
                                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                     ftgrp->ftgrp,
                                     ftgrp->used ? "YES" : "NO",
                                     ftgrp->included ? "YES" : "NO" ));
                /* if this ftgrp has already been used or is not included, then
                 * skip it
                 */
                if (ftgrp->used || !ftgrp->included) {
                    continue;
                }
                /* compute the load average on this fault group */
                totprocs = 0;
                totnodes = 0;
                for (k=0; k < ftgrp->nodes.size; k++) {
                    if (NULL == (node = (orte_node_t*)opal_pointer_array_get_item(&ftgrp->nodes, k))) {
                        continue;
                    }
                    totnodes++;
                    totprocs += node->num_procs;
                }
                avgload = (float)totprocs / (float)totnodes;
                if (avgload < minload) {
                    minload = avgload;
                    target = ftgrp;
                    OPAL_OUTPUT_VERBOSE((2, orte_rmaps_base_framework.framework_output,
                                         "%s rmaps:resilient: found new min load ftgrp %d",
                                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                         ftgrp->ftgrp));
                }
            }
            /* if we have more procs than fault groups, then we simply
             * map the remaining procs on available nodes in a round-robin
             * fashion - it doesn't matter where they go as they will not
             * be contributing to fault tolerance by definition
             */
            if (NULL == target) {
                OPAL_OUTPUT_VERBOSE((2, orte_rmaps_base_framework.framework_output,
                                     "%s rmaps:resilient: more procs than fault groups - mapping excess rr",
                                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));
                nd = (orte_node_t*)curitem;
                curitem = opal_list_get_next(curitem);
                if (curitem == opal_list_get_end(&node_list)) {
                    curitem = opal_list_get_first(&node_list);
                }
            } else {
                /* pick node with lowest load from within that group */
                totprocs = 1000000;
                for (k=0; k < target->nodes.size; k++) {
                    if (NULL == (node = (orte_node_t*)opal_pointer_array_get_item(&target->nodes, k))) {
                        continue;
                    }
                    if (node->num_procs < totprocs) {
                        totprocs = node->num_procs;
                        nd = node;
                    }
                }
            }
            OPAL_OUTPUT_VERBOSE((1, orte_rmaps_base_framework.framework_output,
                                 "%s rmaps:resilient: placing proc into fault group %d node %s",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 (NULL == target) ? -1 : target->ftgrp, nd->name));
            /* if the node isn't in the map, add it */
            if (!ORTE_FLAG_TEST(nd, ORTE_NODE_FLAG_MAPPED)) {
                OBJ_RETAIN(nd);
                opal_pointer_array_add(map->nodes, nd);
                ORTE_FLAG_SET(nd, ORTE_NODE_FLAG_MAPPED);
            }
            if (NULL == orte_rmaps_base_setup_proc(jdata, nd, app->idx)) {
                ORTE_ERROR_LOG(ORTE_ERROR);
                return ORTE_ERROR;
            }
            if ((nd->slots < (int)nd->num_procs) ||
                (0 < nd->slots_max && nd->slots_max < (int)nd->num_procs)) {
                if (ORTE_MAPPING_NO_OVERSUBSCRIBE & ORTE_GET_MAPPING_DIRECTIVE(jdata->map->mapping)) {
                    orte_show_help("help-orte-rmaps-base.txt", "orte-rmaps-base:alloc-error",
                                   true, nd->num_procs, app->app);
                    ORTE_UPDATE_EXIT_STATUS(ORTE_ERROR_DEFAULT_EXIT_CODE);
                    return ORTE_ERR_SILENT;
                }
                /* flag the node as oversubscribed so that sched-yield gets
                 * properly set
                 */
                ORTE_FLAG_SET(nd, ORTE_NODE_FLAG_OVERSUBSCRIBED);
                ORTE_FLAG_SET(jdata, ORTE_JOB_FLAG_OVERSUBSCRIBED);
            }

            /* track number of procs mapped */
            num_assigned++;

            /* flag this fault group as used */
            if (NULL != target) {
                target->used = true;
            }
        }

        /* track number of procs */
        jdata->num_procs += app->num_procs;

        /* cleanup the node list - it can differ from one app_context
         * to another, so we have to get it every time
         */
        while (NULL != (item = opal_list_remove_first(&node_list))) {
            OBJ_RELEASE(item);
        }
        OBJ_DESTRUCT(&node_list);
    }

    return ORTE_SUCCESS;
}
