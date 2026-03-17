/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2013      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2014-2017 Intel, Inc.  All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2016      IBM Corporation.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "orte_config.h"

#include <string.h>
#include <ctype.h>

#include "orte/constants.h"
#include "orte/types.h"

#include "orte/util/show_help.h"
#include "opal/util/argv.h"
#include "opal/util/if.h"
#include "opal/util/net.h"

#include "orte/mca/ras/base/base.h"
#include "orte/mca/plm/plm_types.h"
#include "orte/mca/errmgr/errmgr.h"
#include "orte/util/proc_info.h"
#include "orte/runtime/orte_globals.h"

#include "dash_host.h"

/* we can only enter this routine if no other allocation
 * was found, so we only need to know that finding any
 * relative node syntax should generate an immediate error
 */
int orte_util_add_dash_host_nodes(opal_list_t *nodes,
                                  char *hosts, bool allocating)
{
    opal_list_item_t *item, *itm;
    orte_std_cntr_t i, j, k;
    int rc, nodeidx;
    char **host_argv=NULL;
    char **mapped_nodes = NULL, **mini_map, *ndname;
    orte_node_t *node, *nd;
    opal_list_t adds;
    bool found;
    int slots=0;
    bool slots_given;
    char *cptr, *ptr;

    OPAL_OUTPUT_VERBOSE((1, orte_ras_base_framework.framework_output,
                         "%s dashhost: parsing args %s",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), hosts));

    OBJ_CONSTRUCT(&adds, opal_list_t);
    host_argv = opal_argv_split(hosts, ',');

    /* Accumulate all of the host name mappings */
    for (j = 0; j < opal_argv_count(host_argv); ++j) {
        mini_map = opal_argv_split(host_argv[j], ',');

        if (mapped_nodes == NULL) {
            mapped_nodes = mini_map;
        } else {
            for (k = 0; NULL != mini_map[k]; ++k) {
                rc = opal_argv_append_nosize(&mapped_nodes,
                                             mini_map[k]);
                if (OPAL_SUCCESS != rc) {
                    opal_argv_free(host_argv);
                    opal_argv_free(mini_map);
                    goto cleanup;
                }
            }
            opal_argv_free(mini_map);
        }
    }
    opal_argv_free(host_argv);
    mini_map = NULL;

    /* Did we find anything? If not, then do nothing */
    if (NULL == mapped_nodes) {
        rc = ORTE_SUCCESS;
        goto cleanup;
    }

    for (i = 0; NULL != mapped_nodes[i]; ++i) {
        /* if the specified node contains a relative node syntax,
         * and we are allocating, then ignore it
         */
        if ('+' == mapped_nodes[i][0]) {
            if (!allocating) {
                if ('e' == mapped_nodes[i][1] ||
                    'E' == mapped_nodes[i][1]) {
                    /* request for empty nodes - do they want
                     * all of them?
                     */
                    if (NULL != (cptr = strchr(mapped_nodes[i], ':'))) {
                        /* the colon indicates a specific # are requested */
                        ++cptr;
                        j = strtoul(cptr, NULL, 10);
                    } else if ('\0' != mapped_nodes[0][2]) {
                        j = strtoul(&mapped_nodes[0][2], NULL, 10);
                    } else {
                        /* add them all */
                        j = orte_node_pool->size;
                    }
                    for (k=0; 0 < j && k < orte_node_pool->size; k++) {
                        if (NULL != (node = (orte_node_t*)opal_pointer_array_get_item(orte_node_pool, k))) {
                            if (0 == node->num_procs) {
                                opal_argv_append_nosize(&mini_map, node->name);
                                --j;
                            }
                        }
                    }
                } else if ('n' == mapped_nodes[i][1] ||
                           'N' == mapped_nodes[i][1]) {
                    /* they want a specific relative node #, so
                     * look it up on global pool
                     */
                    if ('\0' == mapped_nodes[i][2]) {
                        /* they forgot to tell us the # */
                        orte_show_help("help-dash-host.txt", "dash-host:invalid-relative-node-syntax",
                                       true, mapped_nodes[i]);
                        rc = ORTE_ERR_SILENT;
                        goto cleanup;
                    }
                    nodeidx = strtol(&mapped_nodes[i][2], NULL, 10);
                    if (nodeidx < 0 ||
                        nodeidx > (int)orte_node_pool->size) {
                        /* this is an error */
                        orte_show_help("help-dash-host.txt", "dash-host:relative-node-out-of-bounds",
                                       true, nodeidx, mapped_nodes[i]);
                        rc = ORTE_ERR_SILENT;
                        goto cleanup;
                    }
                    /* if the HNP is not allocated, then we need to
                     * adjust the index as the node pool is offset
                     * by one
                     */
                    if (!orte_hnp_is_allocated) {
                        nodeidx++;
                    }
                    /* see if that location is filled */

                    if (NULL == (node = (orte_node_t *) opal_pointer_array_get_item(orte_node_pool, nodeidx))) {
                        /* this is an error */
                        orte_show_help("help-dash-host.txt", "dash-host:relative-node-not-found",
                                       true, nodeidx, mapped_nodes[i]);
                        rc = ORTE_ERR_SILENT;
                        goto cleanup;
                    }
                    /* add this node to the list */
                    opal_argv_append_nosize(&mini_map, node->name);
                } else {
                    /* invalid relative node syntax */
                    orte_show_help("help-dash-host.txt", "dash-host:invalid-relative-node-syntax",
                                   true, mapped_nodes[i]);
                    rc = ORTE_ERR_SILENT;
                    goto cleanup;
                }
            }
        } else {
            /* just one node was given */
            opal_argv_append_nosize(&mini_map, mapped_nodes[i]);
        }
    }
    if (NULL == mini_map) {
        rc = ORTE_SUCCESS;
        goto cleanup;
    }

    /*  go through the names found and
        add them to the host list. If they're not unique, then
        bump the slots count for each duplicate */
    for (i=0; NULL != mini_map[i]; i++) {
        OPAL_OUTPUT_VERBOSE((1, orte_ras_base_framework.framework_output,
                             "%s dashhost: working node %s",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), mini_map[i]));

        /* see if the node contains the number of slots */
        slots_given = false;
        if (NULL != (cptr = strchr(mini_map[i], ':'))) {
            *cptr = '\0';
            ++cptr;
            if ('*' == *cptr || 0 == strcmp(cptr, "auto")) {
                /* auto-detect #slots */
                slots = -1;
                slots_given = false;
            } else {
                slots = strtol(cptr, NULL, 10);
                slots_given = true;
            }
        }

        /* check for local name */
        if (orte_ifislocal(mini_map[i])) {
            ndname = orte_process_info.nodename;
        } else {
            ndname = mini_map[i];
        }

        // Strip off the FQDN if present, ignore IP addresses
        if( !orte_keep_fqdn_hostnames && !opal_net_isaddr(ndname) ) {
            if (NULL != (ptr = strchr(ndname, '.'))) {
                *ptr = '\0';
            }
        }
        /* remove any modifier */
        if (NULL != (ptr = strchr(ndname, ':'))) {
            *ptr = '\0';
        }
        /* see if the node is already on the list */
        found = false;
        OPAL_LIST_FOREACH(node, &adds, orte_node_t) {
            if (0 == strcmp(node->name, ndname)) {
                found = true;
                if (slots_given) {
                    node->slots += slots;
                    if (0 < slots) {
                        ORTE_FLAG_SET(node, ORTE_NODE_FLAG_SLOTS_GIVEN);
                    }
                } else {
                    ++node->slots;
                    ORTE_FLAG_SET(node, ORTE_NODE_FLAG_SLOTS_GIVEN);
                }
                OPAL_OUTPUT_VERBOSE((1, orte_ras_base_framework.framework_output,
                                     "%s dashhost: node %s already on list - slots %d",
                                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), node->name, node->slots));
                break;
            }
        }

        /* If we didn't find it, add it to the list */
        if (!found) {
            node = OBJ_NEW(orte_node_t);
            if (NULL == node) {
                opal_argv_free(mapped_nodes);
                return ORTE_ERR_OUT_OF_RESOURCE;
            }
            node->name = strdup(ndname);
            OPAL_OUTPUT_VERBOSE((1, orte_ras_base_framework.framework_output,
                                 "%s dashhost: added node %s to list - slots %d",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), node->name, slots));
            node->state = ORTE_NODE_STATE_UP;
            node->slots_inuse = 0;
            node->slots_max = 0;
            if (slots_given) {
                node->slots = slots;
                if (0 < slots) {
                    ORTE_FLAG_SET(node, ORTE_NODE_FLAG_SLOTS_GIVEN);
                }
            } else if (slots < 0) {
                node->slots = 0;
                ORTE_FLAG_UNSET(node, ORTE_NODE_FLAG_SLOTS_GIVEN);
            } else {
                node->slots = 1;
                ORTE_FLAG_SET(node, ORTE_NODE_FLAG_SLOTS_GIVEN);
            }
            opal_list_append(&adds, &node->super);
        }
    }
    opal_argv_free(mini_map);

    /* transfer across all unique nodes */
    while (NULL != (item = opal_list_remove_first(&adds))) {
        nd = (orte_node_t*)item;
        found = false;
        for (itm = opal_list_get_first(nodes);
             itm != opal_list_get_end(nodes);
             itm = opal_list_get_next(itm)) {
            node = (orte_node_t*)itm;
            if (0 == strcmp(nd->name, node->name)) {
                found = true;
                OPAL_OUTPUT_VERBOSE((1, orte_ras_base_framework.framework_output,
                                     "%s dashhost: found existing node %s on input list - adding slots",
                                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), node->name));
                if (ORTE_FLAG_TEST(nd, ORTE_NODE_FLAG_SLOTS_GIVEN)) {
                    /* transfer across the number of slots */
                    node->slots = nd->slots;
                    ORTE_FLAG_SET(node, ORTE_NODE_FLAG_SLOTS_GIVEN);
                }
                break;
            }
        }
        if (!found) {
            OPAL_OUTPUT_VERBOSE((1, orte_ras_base_framework.framework_output,
                                 "%s dashhost: adding node %s with %d slots to final list",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), nd->name, nd->slots));
            opal_list_append(nodes, &nd->super);
        } else {
            OBJ_RELEASE(item);
        }
    }

    rc = ORTE_SUCCESS;

 cleanup:
    if (NULL != mapped_nodes) {
        opal_argv_free(mapped_nodes);
    }
    OPAL_LIST_DESTRUCT(&adds);

    return rc;
}


/* the -host option can always be used in both absolute
 * and relative mode, so we have to check for pre-existing
 * allocations if we are to use relative node syntax
 */
static int parse_dash_host(char ***mapped_nodes, char *hosts)
{
    orte_std_cntr_t j, k;
    int rc=ORTE_SUCCESS;
    char **mini_map=NULL, *cptr;
    int nodeidx;
    orte_node_t *node;
    char **host_argv=NULL;

    host_argv = opal_argv_split(hosts, ',');

    /* Accumulate all of the host name mappings */
    for (j = 0; j < opal_argv_count(host_argv); ++j) {
        mini_map = opal_argv_split(host_argv[j], ',');

        for (k = 0; NULL != mini_map[k]; ++k) {
            if ('+' == mini_map[k][0]) {
                /* see if we specified empty nodes */
                if ('e' == mini_map[k][1] ||
                    'E' == mini_map[k][1]) {
                    /* request for empty nodes - do they want
                     * all of them?
                     */
                    if (NULL != (cptr = strchr(mini_map[k], ':'))) {
                        /* the colon indicates a specific # are requested */
                        *cptr = '*';
                        opal_argv_append_nosize(mapped_nodes, cptr);
                    } else {
                        /* add a marker to the list */
                        opal_argv_append_nosize(mapped_nodes, "*");
                    }
                } else if ('n' == mini_map[k][1] ||
                           'N' == mini_map[k][1]) {
                    /* they want a specific relative node #, so
                     * look it up on global pool
                     */
                    nodeidx = strtol(&mini_map[k][2], NULL, 10);
                    if (nodeidx < 0 ||
                        nodeidx > (int)orte_node_pool->size) {
                        /* this is an error */
                        orte_show_help("help-dash-host.txt", "dash-host:relative-node-out-of-bounds",
                                       true, nodeidx, mini_map[k]);
                        rc = ORTE_ERR_SILENT;
                        goto cleanup;
                    }
                    /* if the HNP is not allocated, then we need to
                     * adjust the index as the node pool is offset
                     * by one
                     */
                    if (!orte_hnp_is_allocated) {
                        nodeidx++;
                    }
                    /* see if that location is filled */

                    if (NULL == (node = (orte_node_t *) opal_pointer_array_get_item(orte_node_pool, nodeidx))) {
                        /* this is an error */
                        orte_show_help("help-dash-host.txt", "dash-host:relative-node-not-found",
                                       true, nodeidx, mini_map[k]);
                        rc = ORTE_ERR_SILENT;
                        goto cleanup;
                    }
                    /* add this node to the list */
                    opal_argv_append_nosize(mapped_nodes, node->name);
                } else {
                    /* invalid relative node syntax */
                    orte_show_help("help-dash-host.txt", "dash-host:invalid-relative-node-syntax",
                                   true, mini_map[k]);
                    rc = ORTE_ERR_SILENT;
                    goto cleanup;
                }
            } else { /* non-relative syntax - add to list */
                /* remove any modifier */
                if (NULL != (cptr = strchr(mini_map[k], ':'))) {
                    *cptr = '\0';
                }
                /* check for local alias */
                if (orte_ifislocal(mini_map[k])) {
                    opal_argv_append_nosize(mapped_nodes, orte_process_info.nodename);
                } else {
                    opal_argv_append_nosize(mapped_nodes, mini_map[k]);
                }
            }
        }
        opal_argv_free(mini_map);
        mini_map = NULL;
    }

cleanup:
    if (NULL != host_argv) {
        opal_argv_free(host_argv);
    }
    if (NULL != mini_map) {
        opal_argv_free(mini_map);
    }
    return rc;
}

int orte_util_filter_dash_host_nodes(opal_list_t *nodes,
                                     char *hosts,
                                     bool remove)
{
    opal_list_item_t* item;
    opal_list_item_t *next;
    orte_std_cntr_t i, j, len_mapped_node=0;
    int rc, test;
    char **mapped_nodes = NULL;
    orte_node_t *node;
    int num_empty=0;
    opal_list_t keep;
    bool want_all_empty=false;
    char *cptr;
    size_t lst, lmn;

    /* if the incoming node list is empty, then there
     * is nothing to filter!
     */
    if (opal_list_is_empty(nodes)) {
        return ORTE_SUCCESS;
    }

    if (ORTE_SUCCESS != (rc = parse_dash_host(&mapped_nodes, hosts))) {
        ORTE_ERROR_LOG(rc);
        return rc;
    }
    /* Did we find anything? If not, then do nothing */
    if (NULL == mapped_nodes) {
        return ORTE_SUCCESS;
    }

    /* NOTE: The following logic is based on knowing that
     * any node can only be included on the incoming
     * nodes list ONCE.
     */

    len_mapped_node = opal_argv_count(mapped_nodes);
    /* setup a working list so we can put the final list
     * of nodes in order. This way, if the user specifies a
     * set of nodes, we will use them in the order in which
     * they were specifed. Note that empty node requests
     * will always be appended to the end
     */
    OBJ_CONSTRUCT(&keep, opal_list_t);

    for (i = 0; i < len_mapped_node; ++i) {
        /* check if we are supposed to add some number of empty
         * nodes here
         */
        if ('*' == mapped_nodes[i][0]) {
            /* if there is a number after the '*', then we are
             * to insert a specific # of nodes
             */
            if ('\0' == mapped_nodes[i][1]) {
                /* take all empty nodes from the list */
                num_empty = INT_MAX;
                want_all_empty = true;
            } else {
                /* extract number of nodes to take */
                num_empty = strtol(&mapped_nodes[i][1], NULL, 10);
            }
            /* search for empty nodes and take them */
            item = opal_list_get_first(nodes);
            while (0 < num_empty && item != opal_list_get_end(nodes)) {
                next = opal_list_get_next(item);  /* save this position */
                node = (orte_node_t*)item;
                /* see if this node is empty */
                if (0 == node->slots_inuse) {
                    /* check to see if it is specified later */
                    for (j=i+1; j < len_mapped_node; j++) {
                        if (0 == strcmp(mapped_nodes[j], node->name)) {
                            /* specified later - skip this one */
                            goto skipnode;
                        }
                    }
                    if (remove) {
                        /* remove item from list */
                        opal_list_remove_item(nodes, item);
                        /* xfer to keep list */
                        opal_list_append(&keep, item);
                    } else {
                        /* mark the node as found */
                        ORTE_FLAG_SET(node, ORTE_NODE_FLAG_MAPPED);
                    }
                    --num_empty;
                }
            skipnode:
                item = next;
            }
        } else {
            /* remove any modifier */
            if (NULL != (cptr = strchr(mapped_nodes[i], ':'))) {
                *cptr = '\0';
            }
            /* we are looking for a specific node on the list. The
             * parser will have substituted our local name for any
             * alias, so we only have to do a strcmp here. */
            cptr = NULL;
            lmn = strtoul(mapped_nodes[i], &cptr, 10);
            item = opal_list_get_first(nodes);
            while (item != opal_list_get_end(nodes)) {
                next = opal_list_get_next(item);  /* save this position */
                node = (orte_node_t*)item;
                /* search -host list to see if this one is found */
                if (orte_managed_allocation &&
                    (NULL == cptr || 0 == strlen(cptr))) {
                    /* if we are only given a number, then we test the
                     * value against the number in the node name. This allows support for
                     * launch_id-based environments. For example, a hostname
                     * of "nid0015" can be referenced by "--host 15" */
                    for (j=strlen(node->name)-1; 0 < j; j--) {
                        if (!isdigit(node->name[j])) {
                            j++;
                            break;
                        }
                    }
                    if (j >= (int)(strlen(node->name) - 1)) {
                        test = 0;
                    } else {
                        lst = strtoul(&node->name[j], NULL, 10);
                        test = (lmn == lst) ? 0 : 1;
                    }
                } else {
                    test = strcmp(node->name, mapped_nodes[i]);
                }
                if (0 == test) {
                    if (remove) {
                        /* remove item from list */
                        opal_list_remove_item(nodes, item);
                        /* xfer to keep list */
                        opal_list_append(&keep, item);
                    } else {
                        /* mark the node as found */
                        ORTE_FLAG_SET(node, ORTE_NODE_FLAG_MAPPED);
                    }
                    break;
                }
                item = next;
            }
        }
        /* done with the mapped entry */
        free(mapped_nodes[i]);
        mapped_nodes[i] = NULL;
    }

    /* was something specified that was -not- found? */
    for (i=0; i < len_mapped_node; i++) {
        if (NULL != mapped_nodes[i]) {
            orte_show_help("help-dash-host.txt", "not-all-mapped-alloc",
                           true, mapped_nodes[i]);
            rc = ORTE_ERR_SILENT;
            goto cleanup;
        }
    }

    if (!remove) {
        /* all done */
        rc = ORTE_SUCCESS;
        goto cleanup;
    }

    /* clear the rest of the nodes list */
    while (NULL != (item = opal_list_remove_first(nodes))) {
        OBJ_RELEASE(item);
    }

    /* the nodes list has been cleared - rebuild it in order */
    while (NULL != (item = opal_list_remove_first(&keep))) {
        opal_list_append(nodes, item);
    }

    /* did they ask for more than we could provide */
    if (!want_all_empty && 0 < num_empty) {
        orte_show_help("help-dash-host.txt", "dash-host:not-enough-empty",
                       true, num_empty);
        rc = ORTE_ERR_SILENT;
        goto cleanup;
    }

    rc = ORTE_SUCCESS;
    /* done filtering existing list */

cleanup:
    for (i=0; i < len_mapped_node; i++) {
        if (NULL != mapped_nodes[i]) {
            free(mapped_nodes[i]);
            mapped_nodes[i] = NULL;
        }
    }
    if (NULL != mapped_nodes) {
        free(mapped_nodes);
    }

    return rc;
}

int orte_util_get_ordered_dash_host_list(opal_list_t *nodes,
                                         char *hosts)
{
    int rc, i;
    char **mapped_nodes = NULL;
    orte_node_t *node;

    if (ORTE_SUCCESS != (rc = parse_dash_host(&mapped_nodes, hosts))) {
        ORTE_ERROR_LOG(rc);
    }

    /* for each entry, create a node entry on the list */
    for (i=0; NULL != mapped_nodes[i]; i++) {
        node = OBJ_NEW(orte_node_t);
        node->name = strdup(mapped_nodes[i]);
        opal_list_append(nodes, &node->super);
    }

    /* cleanup */
    opal_argv_free(mapped_nodes);
    return rc;
}
