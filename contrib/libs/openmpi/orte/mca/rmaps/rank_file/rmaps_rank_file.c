/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2011 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2006-2013 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2011-2013 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2008      Voltaire. All rights reserved
 * Copyright (c) 2010      Oracle and/or its affiliates.  All rights reserved.
 * Copyright (c) 2014-2018 Intel, Inc. All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2016      IBM Corporation.  All rights reserved.
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

#include "opal/util/argv.h"
#include "opal/util/if.h"
#include "opal/util/net.h"
#include "opal/class/opal_pointer_array.h"
#include "opal/mca/hwloc/base/base.h"

#include "orte/mca/errmgr/errmgr.h"
#include "orte/mca/ess/ess.h"
#include "orte/util/show_help.h"
#include "orte/mca/rmaps/base/rmaps_private.h"
#include "orte/mca/rmaps/base/base.h"
#include "orte/mca/rmaps/rank_file/rmaps_rank_file.h"
#include "orte/mca/rmaps/rank_file/rmaps_rank_file_lex.h"
#include "orte/runtime/orte_globals.h"

static int orte_rmaps_rf_map(orte_job_t *jdata);

orte_rmaps_base_module_t orte_rmaps_rank_file_module = {
    .map_job = orte_rmaps_rf_map
};


static int orte_rmaps_rank_file_parse(const char *);
static char *orte_rmaps_rank_file_parse_string_or_int(void);
static const char *orte_rmaps_rank_file_name_cur = NULL;
char *orte_rmaps_rank_file_slot_list = NULL;

/*
 * Local variable
 */
static opal_pointer_array_t rankmap;
static int num_ranks=0;

/*
 * Create a rank_file  mapping for the job.
 */
static int orte_rmaps_rf_map(orte_job_t *jdata)
{
    orte_job_map_t *map;
    orte_app_context_t *app=NULL;
    orte_std_cntr_t i, k;
    opal_list_t node_list;
    opal_list_item_t *item;
    orte_node_t *node, *nd, *root_node;
    orte_vpid_t rank, vpid_start;
    orte_std_cntr_t num_slots;
    orte_rmaps_rank_file_map_t *rfmap;
    orte_std_cntr_t relative_index, tmp_cnt;
    int rc;
    orte_proc_t *proc;
    mca_base_component_t *c = &mca_rmaps_rank_file_component.super.base_version;
    char *slots;
    bool initial_map=true;
    opal_hwloc_resource_type_t rtype;

    /* only handle initial launch of rf job */
    if (ORTE_FLAG_TEST(jdata, ORTE_JOB_FLAG_RESTART)) {
        opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                            "mca:rmaps:rf: job %s being restarted - rank_file cannot map",
                            ORTE_JOBID_PRINT(jdata->jobid));
        return ORTE_ERR_TAKE_NEXT_OPTION;
    }
    if (NULL != jdata->map->req_mapper &&
        0 != strcasecmp(jdata->map->req_mapper, c->mca_component_name)) {
        /* a mapper has been specified, and it isn't me */
        opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                            "mca:rmaps:rf: job %s not using rank_file mapper",
                            ORTE_JOBID_PRINT(jdata->jobid));
        return ORTE_ERR_TAKE_NEXT_OPTION;
    }
    if (ORTE_MAPPING_BYUSER != ORTE_GET_MAPPING_POLICY(orte_rmaps_base.mapping)) {
        /* NOT FOR US */
        return ORTE_ERR_TAKE_NEXT_OPTION;
    }
    if (OPAL_BIND_ORDERED_REQUESTED(jdata->map->binding)) {
        /* NOT FOR US */
        return ORTE_ERR_TAKE_NEXT_OPTION;
    }
    opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                        "mca:rmaps:rank_file: mapping job %s",
                        ORTE_JOBID_PRINT(jdata->jobid));

    /* flag that I did the mapping */
    if (NULL != jdata->map->last_mapper) {
        free(jdata->map->last_mapper);
    }
    jdata->map->last_mapper = strdup(c->mca_component_name);

    /* convenience def */
    map = jdata->map;

    /* default to LOGICAL processors */
    if (mca_rmaps_rank_file_component.physical) {
        opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                            "mca:rmaps:rank_file: using PHYSICAL processors");
        rtype = OPAL_HWLOC_PHYSICAL;
    } else {
        opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                            "mca:rmaps:rank_file: using LOGICAL processors");
        rtype = OPAL_HWLOC_LOGICAL;
    }

    /* setup the node list */
    OBJ_CONSTRUCT(&node_list, opal_list_t);

    /* pickup the first app - there must be at least one */
    if (NULL == (app = (orte_app_context_t*)opal_pointer_array_get_item(jdata->apps, 0))) {
        rc = ORTE_ERR_SILENT;
        goto error;
    }

    /* SANITY CHECKS */

    /* if the number of processes wasn't specified, then we know there can be only
     * one app_context allowed in the launch, and that we are to launch it across
     * all available slots.
     */
    if (0 == app->num_procs && 1 < jdata->num_apps) {
        orte_show_help("help-rmaps_rank_file.txt", "orte-rmaps-rf:multi-apps-and-zero-np",
                       true, jdata->num_apps, NULL);
        rc = ORTE_ERR_SILENT;
        goto error;
    }

    /* END SANITY CHECKS */

    /* start at the beginning... */
    vpid_start = 0;
    jdata->num_procs = 0;
    OBJ_CONSTRUCT(&rankmap, opal_pointer_array_t);

    /* parse the rankfile, storing its results in the rankmap */
    if ( NULL != orte_rankfile ) {
        if ( ORTE_SUCCESS != (rc = orte_rmaps_rank_file_parse(orte_rankfile))) {
            ORTE_ERROR_LOG(rc);
            goto error;
        }
    }

    /* cycle through the app_contexts, mapping them sequentially */
    for(i=0; i < jdata->apps->size; i++) {
        if (NULL == (app = (orte_app_context_t*)opal_pointer_array_get_item(jdata->apps, i))) {
            continue;
        }

        /* for each app_context, we have to get the list of nodes that it can
         * use since that can now be modified with a hostfile and/or -host
         * option
         */
        if(ORTE_SUCCESS != (rc = orte_rmaps_base_get_target_nodes(&node_list, &num_slots, app,
                                                                  map->mapping, initial_map, false))) {
            ORTE_ERROR_LOG(rc);
            goto error;
        }
        /* flag that all subsequent requests should not reset the node->mapped flag */
        initial_map = false;

        /* we already checked for sanity, so it's okay to just do here */
        if (0 == app->num_procs) {
            if (NULL != orte_rankfile) {
                /* if we were given a rankfile, then we set the number of procs
                 * to the number of entries in that rankfile
                 */
                app->num_procs = num_ranks;
            } else {
                /** set the num_procs to equal the number of slots on these mapped nodes */
                app->num_procs = num_slots;
            }
        }
        for (k=0; k < app->num_procs; k++) {
            rank = vpid_start + k;
            /* get the rankfile entry for this rank */
            if (NULL == (rfmap = (orte_rmaps_rank_file_map_t*)opal_pointer_array_get_item(&rankmap, rank))) {
                /* if we were give a default slot-list, then use it */
                if (NULL != opal_hwloc_base_cpu_list) {
                    slots = opal_hwloc_base_cpu_list;
                    /* take the next node off of the available list */
                    node = NULL;
                    OPAL_LIST_FOREACH(nd, &node_list, orte_node_t) {
                        /* if adding one to this node would oversubscribe it, then try
                         * the next one */
                        if (nd->slots <= (int)nd->num_procs) {
                            continue;
                        }
                        /* take this one */
                        node = nd;
                        break;
                    }
                    if (NULL == node) {
                        /* all would be oversubscribed, so take the least loaded one */
                        k = UINT32_MAX;
                        OPAL_LIST_FOREACH(nd, &node_list, orte_node_t) {
                            if (nd->num_procs < (orte_vpid_t)k) {
                                k = nd->num_procs;
                                node = nd;
                            }
                        }
                    }
                    /* if we still have nothing, then something is very wrong */
                    if (NULL == node) {
                        rc = ORTE_ERR_OUT_OF_RESOURCE;
                        goto error;
                    }
                } else {
                    /* all ranks must be specified */
                    orte_show_help("help-rmaps_rank_file.txt", "missing-rank", true, rank, orte_rankfile);
                    rc = ORTE_ERR_SILENT;
                    goto error;
                }
            } else {
                if (0 == strlen(rfmap->slot_list)) {
                    /* rank was specified but no slot list given - that's an error */
                    orte_show_help("help-rmaps_rank_file.txt","no-slot-list", true, rank, rfmap->node_name);
                    rc = ORTE_ERR_SILENT;
                    goto error;
                }
                slots = rfmap->slot_list;
                /* find the node where this proc was assigned */
                node = NULL;
                OPAL_LIST_FOREACH(nd, &node_list, orte_node_t) {
                    if (NULL != rfmap->node_name &&
                        0 == strcmp(nd->name, rfmap->node_name)) {
                        node = nd;
                        break;
                    } else if (NULL != rfmap->node_name &&
                               (('+' == rfmap->node_name[0]) &&
                                (('n' == rfmap->node_name[1]) ||
                                 ('N' == rfmap->node_name[1])))) {

                        relative_index=atoi(strtok(rfmap->node_name,"+n"));
                        if ( relative_index >= (int)opal_list_get_size (&node_list) || ( 0 > relative_index)){
                            orte_show_help("help-rmaps_rank_file.txt","bad-index", true,rfmap->node_name);
                            ORTE_ERROR_LOG(ORTE_ERR_BAD_PARAM);
                            return ORTE_ERR_BAD_PARAM;
                        }
                        root_node = (orte_node_t*) opal_list_get_first(&node_list);
                        for(tmp_cnt=0; tmp_cnt<relative_index; tmp_cnt++) {
                            root_node = (orte_node_t*) opal_list_get_next(root_node);
                        }
                        node = root_node;
                        break;
                    }
                }
            }
            if (NULL == node) {
                orte_show_help("help-rmaps_rank_file.txt","bad-host", true, rfmap->node_name);
                rc = ORTE_ERR_SILENT;
                goto error;
            }
            /* ensure the node is in the map */
            if (!ORTE_FLAG_TEST(node, ORTE_NODE_FLAG_MAPPED)) {
                OBJ_RETAIN(node);
                opal_pointer_array_add(map->nodes, node);
                ORTE_FLAG_SET(node, ORTE_NODE_FLAG_MAPPED);
                ++(jdata->map->num_nodes);
            }
            if (NULL == (proc = orte_rmaps_base_setup_proc(jdata, node, i))) {
                ORTE_ERROR_LOG(ORTE_ERR_OUT_OF_RESOURCE);
                rc = ORTE_ERR_OUT_OF_RESOURCE;
                goto error;
            }
            if ((node->slots < (int)node->num_procs) ||
                (0 < node->slots_max && node->slots_max < (int)node->num_procs)) {
                if (ORTE_MAPPING_NO_OVERSUBSCRIBE & ORTE_GET_MAPPING_DIRECTIVE(jdata->map->mapping)) {
                    orte_show_help("help-orte-rmaps-base.txt", "orte-rmaps-base:alloc-error",
                                   true, node->num_procs, app->app);
                    ORTE_UPDATE_EXIT_STATUS(ORTE_ERROR_DEFAULT_EXIT_CODE);
                    rc = ORTE_ERR_SILENT;
                    goto error;
                }
                /* flag the node as oversubscribed so that sched-yield gets
                 * properly set
                 */
                ORTE_FLAG_SET(node, ORTE_NODE_FLAG_OVERSUBSCRIBED);
                ORTE_FLAG_SET(jdata, ORTE_JOB_FLAG_OVERSUBSCRIBED);
            }
            /* set the vpid */
            proc->name.vpid = rank;

            if (NULL != slots) {
                /* setup the bitmap */
                hwloc_cpuset_t bitmap;
                char *cpu_bitmap;
                if (NULL == node->topology || NULL == node->topology->topo) {
                    /* not allowed - for rank-file, we must have
                     * the topology info
                     */
                    orte_show_help("help-orte-rmaps-base.txt", "rmaps:no-topology", true, node->name);
                    rc = ORTE_ERR_SILENT;
                    goto error;
                }
                bitmap = hwloc_bitmap_alloc();
                /* parse the slot_list to find the socket and core */
                if (ORTE_SUCCESS != (rc = opal_hwloc_base_cpu_list_parse(slots, node->topology->topo, rtype, bitmap))) {
                    ORTE_ERROR_LOG(rc);
                    hwloc_bitmap_free(bitmap);
                    goto error;
                }
                /* note that we cannot set the proc locale to any specific object
                 * as the slot list may have assigned it to more than one - so
                 * leave that field NULL
                 */
                /* set the proc to the specified map */
                hwloc_bitmap_list_asprintf(&cpu_bitmap, bitmap);
                orte_set_attribute(&proc->attributes, ORTE_PROC_CPU_BITMAP, ORTE_ATTR_GLOBAL, cpu_bitmap, OPAL_STRING);
                /* cleanup */
                free(cpu_bitmap);
                hwloc_bitmap_free(bitmap);
            }

            /* insert the proc into the proper place */
            if (ORTE_SUCCESS != (rc = opal_pointer_array_set_item(jdata->procs,
                                                                  proc->name.vpid, proc))) {
                ORTE_ERROR_LOG(rc);
                return rc;
            }
            jdata->num_procs++;
        }
        /* update the starting point */
        vpid_start += app->num_procs;
        /* cleanup the node list - it can differ from one app_context
         * to another, so we have to get it every time
         */
        while (NULL != (item = opal_list_remove_first(&node_list))) {
            OBJ_RELEASE(item);
        }
        OBJ_DESTRUCT(&node_list);
        OBJ_CONSTRUCT(&node_list, opal_list_t);
    }
    OBJ_DESTRUCT(&node_list);

    /* cleanup the rankmap */
    for (i=0; i < rankmap.size; i++) {
        if (NULL != (rfmap = opal_pointer_array_get_item(&rankmap, i))) {
            OBJ_RELEASE(rfmap);
        }
    }
    OBJ_DESTRUCT(&rankmap);
    /* mark the job as fully described */
    orte_set_attribute(&jdata->attributes, ORTE_JOB_FULLY_DESCRIBED, ORTE_ATTR_GLOBAL, NULL, OPAL_BOOL);

    return rc;

 error:
    OPAL_LIST_DESTRUCT(&node_list);

    return rc;
}

static int orte_rmaps_rank_file_parse(const char *rankfile)
{
    int token;
    int rc = ORTE_SUCCESS;
    int cnt;
    char* node_name = NULL;
    char** argv;
    char buff[64];
    char* value;
    int rank=-1;
    int i;
    orte_node_t *hnp_node;
    orte_rmaps_rank_file_map_t *rfmap=NULL;
    opal_pointer_array_t *assigned_ranks_array;
    char tmp_rank_assignment[64];

    /* keep track of rank assignments */
    assigned_ranks_array = OBJ_NEW(opal_pointer_array_t);

    /* get the hnp node's info */
    hnp_node = (orte_node_t*)(orte_node_pool->addr[0]);

    orte_rmaps_rank_file_name_cur = rankfile;
    orte_rmaps_rank_file_done = false;
    orte_rmaps_rank_file_in = fopen(rankfile, "r");

    if (NULL == orte_rmaps_rank_file_in) {
        orte_show_help("help-rmaps_rank_file.txt", "no-rankfile", true, rankfile);
        rc = OPAL_ERR_NOT_FOUND;
        ORTE_ERROR_LOG(rc);
        goto unlock;
    }

    while (!orte_rmaps_rank_file_done) {
        token = orte_rmaps_rank_file_lex();

        switch (token) {
            case ORTE_RANKFILE_ERROR:
                orte_show_help("help-rmaps_rank_file.txt", "bad-syntax", true, rankfile);
                rc = ORTE_ERR_BAD_PARAM;
                ORTE_ERROR_LOG(rc);
                goto unlock;
                break;
            case ORTE_RANKFILE_QUOTED_STRING:
                orte_show_help("help-rmaps_rank_file.txt", "not-supported-rankfile", true, "QUOTED_STRING", rankfile);
                rc = ORTE_ERR_BAD_PARAM;
                ORTE_ERROR_LOG(rc);
                goto unlock;
            case ORTE_RANKFILE_NEWLINE:
                rank = -1;
                if (NULL != node_name) {
                    free(node_name);
                }
                node_name = NULL;
                rfmap = NULL;
                break;
            case ORTE_RANKFILE_RANK:
                token = orte_rmaps_rank_file_lex();
                if (ORTE_RANKFILE_INT == token) {
                    rank = orte_rmaps_rank_file_value.ival;
                    rfmap = OBJ_NEW(orte_rmaps_rank_file_map_t);
                    opal_pointer_array_set_item(&rankmap, rank, rfmap);
                    num_ranks++;  // keep track of number of provided ranks
                } else {
                    orte_show_help("help-rmaps_rank_file.txt", "bad-syntax", true, rankfile);
                    rc = ORTE_ERR_BAD_PARAM;
                    ORTE_ERROR_LOG(rc);
                    goto unlock;
                }
                break;
            case ORTE_RANKFILE_USERNAME:
                orte_show_help("help-rmaps_rank_file.txt", "not-supported-rankfile", true, "USERNAME", rankfile);
                rc = ORTE_ERR_BAD_PARAM;
                ORTE_ERROR_LOG(rc);
                goto unlock;
                break;
            case ORTE_RANKFILE_EQUAL:
                if (rank < 0) {
                    orte_show_help("help-rmaps_rank_file.txt", "bad-syntax", true, rankfile);
                    rc = ORTE_ERR_BAD_PARAM;
                    ORTE_ERROR_LOG(rc);
                    goto unlock;
                }
                token = orte_rmaps_rank_file_lex();
                switch (token) {
                    case ORTE_RANKFILE_HOSTNAME:
                    case ORTE_RANKFILE_IPV4:
                    case ORTE_RANKFILE_IPV6:
                    case ORTE_RANKFILE_STRING:
                    case ORTE_RANKFILE_INT:
                    case ORTE_RANKFILE_RELATIVE:
                        if(ORTE_RANKFILE_INT == token) {
                            sprintf(buff,"%d", orte_rmaps_rank_file_value.ival);
                            value = buff;
                        } else {
                            value = orte_rmaps_rank_file_value.sval;
                        }
                        argv = opal_argv_split (value, '@');
                        cnt = opal_argv_count (argv);
                        if (NULL != node_name) {
                            free(node_name);
                        }
                        if (1 == cnt) {
                            node_name = strdup(argv[0]);
                        } else if (2 == cnt) {
                            node_name = strdup(argv[1]);
                        } else {
                            orte_show_help("help-rmaps_rank_file.txt", "bad-syntax", true, rankfile);
                            rc = ORTE_ERR_BAD_PARAM;
                            ORTE_ERROR_LOG(rc);
                            opal_argv_free(argv);
                            node_name = NULL;
                            goto unlock;
                        }
                        opal_argv_free (argv);

                        // Strip off the FQDN if present, ignore IP addresses
                        if( !orte_keep_fqdn_hostnames && !opal_net_isaddr(node_name) ) {
                            char *ptr;
                            if (NULL != (ptr = strchr(node_name, '.'))) {
                                *ptr = '\0';
                            }
                        }

                        /* check the rank item */
                        if (NULL == rfmap) {
                            orte_show_help("help-rmaps_rank_file.txt", "bad-syntax", true, rankfile);
                            rc = ORTE_ERR_BAD_PARAM;
                            ORTE_ERROR_LOG(rc);
                            goto unlock;
                        }
                        /* check if this is the local node */
                        if (orte_ifislocal(node_name)) {
                            rfmap->node_name = strdup(hnp_node->name);
                        } else {
                            rfmap->node_name = strdup(node_name);
                        }
                }
                break;
            case ORTE_RANKFILE_SLOT:
                if (NULL == node_name || rank < 0 ||
                    NULL == (value = orte_rmaps_rank_file_parse_string_or_int())) {
                    orte_show_help("help-rmaps_rank_file.txt", "bad-syntax", true, rankfile);
                    rc = ORTE_ERR_BAD_PARAM;
                    ORTE_ERROR_LOG(rc);
                    goto unlock;
                }

                /* check for a duplicate rank assignment */
                if (NULL != opal_pointer_array_get_item(assigned_ranks_array, rank)) {
                    orte_show_help("help-rmaps_rank_file.txt", "bad-assign", true, rank,
                                   opal_pointer_array_get_item(assigned_ranks_array, rank), rankfile);
                    rc = ORTE_ERR_BAD_PARAM;
                    free(value);
                    goto unlock;
                } else {
                    /* prepare rank assignment string for the help message in case of a bad-assign */
                    sprintf(tmp_rank_assignment, "%s slot=%s", node_name, value);
                    opal_pointer_array_set_item(assigned_ranks_array, 0, tmp_rank_assignment);
                }

                /* check the rank item */
                if (NULL == rfmap) {
                    orte_show_help("help-rmaps_rank_file.txt", "bad-syntax", true, rankfile);
                    rc = ORTE_ERR_BAD_PARAM;
                    ORTE_ERROR_LOG(rc);
                    free(value);
                    goto unlock;
                }
                for (i=0; i < 64 && '\0' != value[i]; i++) {
                    rfmap->slot_list[i] = value[i];
                }
                free(value);
                break;
        }
    }
    fclose(orte_rmaps_rank_file_in);
    orte_rmaps_rank_file_lex_destroy ();

unlock:
    if (NULL != node_name) {
        free(node_name);
    }
    OBJ_RELEASE(assigned_ranks_array);
    orte_rmaps_rank_file_name_cur = NULL;
    return rc;
}


static char *orte_rmaps_rank_file_parse_string_or_int(void)
{
    int rc;
    char tmp_str[64];

    if (ORTE_RANKFILE_EQUAL != orte_rmaps_rank_file_lex()){
        return NULL;
    }

    rc = orte_rmaps_rank_file_lex();
    switch (rc) {
        case ORTE_RANKFILE_STRING:
            return strdup(orte_rmaps_rank_file_value.sval);
        case ORTE_RANKFILE_INT:
            sprintf(tmp_str,"%d",orte_rmaps_rank_file_value.ival);
            return strdup(tmp_str);
        default:
            return NULL;

    }

}
