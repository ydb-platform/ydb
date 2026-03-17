/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2008 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2011-2017 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2014-2018 Intel, Inc.  All rights reserved.
 * Copyright (c) 2015-2018 Research Organization for Information Science
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

#include "opal/util/argv.h"
#include "opal/util/if.h"

#include "orte/mca/errmgr/errmgr.h"
#include "orte/mca/rmaps/base/base.h"
#include "orte/util/name_fns.h"
#include "orte/runtime/orte_globals.h"

#include "orte/mca/ras/base/ras_private.h"

/*
 * Add the specified node definitions to the global data store
 * NOTE: this removes all items from the list!
 */
int orte_ras_base_node_insert(opal_list_t* nodes, orte_job_t *jdata)
{
    opal_list_item_t* item;
    orte_std_cntr_t num_nodes;
    int rc, i;
    orte_node_t *node, *hnp_node, *nptr;
    char *ptr;
    bool hnp_alone = true, skiphnp = false;
    orte_attribute_t *kv;
    char **alias=NULL, **nalias;
    orte_proc_t *daemon;
    orte_job_t *djob;

    /* get the number of nodes */
    num_nodes = (orte_std_cntr_t)opal_list_get_size(nodes);
    if (0 == num_nodes) {
        return ORTE_SUCCESS;  /* nothing to do */
    }

    OPAL_OUTPUT_VERBOSE((5, orte_ras_base_framework.framework_output,
                         "%s ras:base:node_insert inserting %ld nodes",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         (long)num_nodes));

    /* mark the job as being a large-cluster sim if that was requested */
    if (1 < orte_ras_base.multiplier) {
        orte_set_attribute(&jdata->attributes, ORTE_JOB_MULTI_DAEMON_SIM,
                           ORTE_ATTR_GLOBAL, NULL, OPAL_BOOL);
    }

    /* set the size of the global array - this helps minimize time
     * spent doing realloc's
     */
    if (ORTE_SUCCESS != (rc = opal_pointer_array_set_size(orte_node_pool, num_nodes * orte_ras_base.multiplier))) {
        ORTE_ERROR_LOG(rc);
        return rc;
    }

    /* if we are not launching, get the daemon job */
    djob = orte_get_job_data_object(ORTE_PROC_MY_NAME->jobid);

    /* get the hnp node's info */
    hnp_node = (orte_node_t*)opal_pointer_array_get_item(orte_node_pool, 0);

    if ((orte_ras_base.launch_orted_on_hn == true) &&
        (orte_managed_allocation)) {
        if (NULL != hnp_node) {
            OPAL_LIST_FOREACH(node, nodes, orte_node_t) {
                if (orte_ifislocal(node->name)) {
                    orte_hnp_is_allocated = true;
                    break;
                }
            }
            if (orte_hnp_is_allocated && !(ORTE_GET_MAPPING_DIRECTIVE(orte_rmaps_base.mapping) &
                ORTE_MAPPING_NO_USE_LOCAL)) {
                hnp_node->name = strdup("mpirun");
                skiphnp = true;
                ORTE_SET_MAPPING_DIRECTIVE(orte_rmaps_base.mapping, ORTE_MAPPING_NO_USE_LOCAL);
            }
        }
    }

    /* cycle through the list */
    while (NULL != (item = opal_list_remove_first(nodes))) {
        node = (orte_node_t*)item;

        /* the HNP had to already enter its node on the array - that entry is in the
         * first position since it is the first one entered. We need to check to see
         * if this node is the same as the HNP's node so we don't double-enter it
         */
        if (!skiphnp && NULL != hnp_node && orte_ifislocal(node->name)) {
            OPAL_OUTPUT_VERBOSE((5, orte_ras_base_framework.framework_output,
                                 "%s ras:base:node_insert updating HNP [%s] info to %ld slots",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 node->name,
                                 (long)node->slots));

            /* flag that hnp has been allocated */
            orte_hnp_is_allocated = true;
            /* update the total slots in the job */
            orte_ras_base.total_slots_alloc += node->slots;
            /* copy the allocation data to that node's info */
            hnp_node->slots = node->slots;
            hnp_node->slots_max = node->slots_max;
            /* copy across any attributes */
            OPAL_LIST_FOREACH(kv, &node->attributes, orte_attribute_t) {
                orte_set_attribute(&node->attributes, kv->key, ORTE_ATTR_LOCAL, &kv->data, kv->type);
            }
            if (orte_managed_allocation || ORTE_FLAG_TEST(node, ORTE_NODE_FLAG_SLOTS_GIVEN)) {
                /* the slots are always treated as sacred
                 * in managed allocations
                 */
                ORTE_FLAG_SET(hnp_node, ORTE_NODE_FLAG_SLOTS_GIVEN);
            } else {
                ORTE_FLAG_UNSET(hnp_node, ORTE_NODE_FLAG_SLOTS_GIVEN);
            }
            /* use the local name for our node - don't trust what
             * we got from an RM. If requested, store the resolved
             * nodename info
             */
            if (orte_show_resolved_nodenames) {
                /* if the node name is different, store it as an alias */
                if (0 != strcmp(node->name, hnp_node->name)) {
                    /* get any current list of aliases */
                    ptr = NULL;
                    orte_get_attribute(&hnp_node->attributes, ORTE_NODE_ALIAS, (void**)&ptr, OPAL_STRING);
                    if (NULL != ptr) {
                        alias = opal_argv_split(ptr, ',');
                        free(ptr);
                    }
                    /* add to list of aliases for this node - only add if unique */
                    opal_argv_append_unique_nosize(&alias, node->name, false);
                }
                if (orte_get_attribute(&node->attributes, ORTE_NODE_ALIAS, (void**)&ptr, OPAL_STRING)) {
                    nalias = opal_argv_split(ptr, ',');
                    /* now copy over any aliases that are unique */
                    for (i=0; NULL != nalias[i]; i++) {
                        opal_argv_append_unique_nosize(&alias, nalias[i], false);
                    }
                    opal_argv_free(nalias);
                }
                /* and store the result */
                if (0 < opal_argv_count(alias)) {
                    ptr = opal_argv_join(alias, ',');
                    orte_set_attribute(&hnp_node->attributes, ORTE_NODE_ALIAS, ORTE_ATTR_LOCAL, ptr, OPAL_STRING);
                    free(ptr);
                }
                opal_argv_free(alias);
            }
            /* don't keep duplicate copy */
            OBJ_RELEASE(node);
            /* create copies, if required */
            for (i=1; i < orte_ras_base.multiplier; i++) {
                opal_dss.copy((void**)&node, hnp_node, ORTE_NODE);
                ORTE_FLAG_UNSET(node, ORTE_NODE_FLAG_DAEMON_LAUNCHED);
                node->index = opal_pointer_array_add(orte_node_pool, node);
            }
        } else {
            /* insert the object onto the orte_nodes global array */
            OPAL_OUTPUT_VERBOSE((5, orte_ras_base_framework.framework_output,
                                 "%s ras:base:node_insert node %s slots %d",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 (NULL == node->name) ? "NULL" : node->name,
                                 node->slots));
            if (orte_managed_allocation) {
                /* the slots are always treated as sacred
                 * in managed allocations
                 */
                ORTE_FLAG_SET(node, ORTE_NODE_FLAG_SLOTS_GIVEN);
            }
            /* insert it into the array */
            node->index = opal_pointer_array_add(orte_node_pool, (void*)node);
            if (ORTE_SUCCESS > (rc = node->index)) {
                ORTE_ERROR_LOG(rc);
                return rc;
            }
            if (orte_do_not_launch) {
                /* create a daemon for this node since we won't be launching
                 * and the mapper needs to see a daemon - this is used solely
                 * for testing the mappers */
                daemon = OBJ_NEW(orte_proc_t);
                daemon->name.jobid = ORTE_PROC_MY_NAME->jobid;
                daemon->name.vpid = node->index;
                daemon->state = ORTE_PROC_STATE_RUNNING;
                OBJ_RETAIN(node);
                daemon->node = node;
                opal_pointer_array_set_item(djob->procs, daemon->name.vpid, daemon);
                djob->num_procs++;
                OBJ_RETAIN(daemon);
                node->daemon = daemon;
            }
            /* update the total slots in the job */
            orte_ras_base.total_slots_alloc += node->slots;
            /* check if we have fqdn names in the allocation */
            if (NULL != strchr(node->name, '.')) {
                orte_have_fqdn_allocation = true;
            }
            /* indicate the HNP is not alone */
            hnp_alone = false;
            for (i=1; i < orte_ras_base.multiplier; i++) {
                opal_dss.copy((void**)&nptr, node, ORTE_NODE);
                nptr->index = opal_pointer_array_add(orte_node_pool, nptr);
            }
       }
    }

    /* if we didn't find any fqdn names in the allocation, then
     * ensure we don't have any domain info in the node record
     * for the hnp
     */
    if (NULL != hnp_node && !orte_have_fqdn_allocation && !hnp_alone) {
        if (NULL != (ptr = strchr(hnp_node->name, '.'))) {
            *ptr = '\0';
        }
    }

    return ORTE_SUCCESS;
}
