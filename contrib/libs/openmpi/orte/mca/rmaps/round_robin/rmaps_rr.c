/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2006 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2006-2013 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2011-2012 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2014-2017 Intel, Inc.  All rights reserved.
 * Copyright (c) 2017      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
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

#include "orte/util/show_help.h"
#include "orte/mca/errmgr/errmgr.h"
#include "orte/util/error_strings.h"

#include "orte/mca/rmaps/base/rmaps_private.h"
#include "orte/mca/rmaps/base/base.h"
#include "rmaps_rr.h"

/*
 * Create a round-robin mapping for the job.
 */
static int orte_rmaps_rr_map(orte_job_t *jdata)
{
    orte_app_context_t *app;
    int i;
    opal_list_t node_list;
    opal_list_item_t *item;
    orte_std_cntr_t num_slots;
    int rc;
    mca_base_component_t *c = &mca_rmaps_round_robin_component.base_version;
    bool initial_map=true;

    /* this mapper can only handle initial launch
     * when rr mapping is desired - allow
     * restarting of failed apps
     */
    if (ORTE_FLAG_TEST(jdata, ORTE_JOB_FLAG_RESTART)) {
        opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                            "mca:rmaps:rr: job %s is being restarted - rr cannot map",
                            ORTE_JOBID_PRINT(jdata->jobid));
        return ORTE_ERR_TAKE_NEXT_OPTION;
    }
    if (NULL != jdata->map->req_mapper &&
        0 != strcasecmp(jdata->map->req_mapper, c->mca_component_name)) {
        /* a mapper has been specified, and it isn't me */
        opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                            "mca:rmaps:rr: job %s not using rr mapper",
                            ORTE_JOBID_PRINT(jdata->jobid));
        return ORTE_ERR_TAKE_NEXT_OPTION;
    }
    if (ORTE_MAPPING_RR < ORTE_GET_MAPPING_POLICY(jdata->map->mapping)) {
        /* I don't know how to do these - defer */
        opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                            "mca:rmaps:rr: job %s not using rr mapper",
                            ORTE_JOBID_PRINT(jdata->jobid));
        return ORTE_ERR_TAKE_NEXT_OPTION;
    }

    opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                        "mca:rmaps:rr: mapping job %s",
                        ORTE_JOBID_PRINT(jdata->jobid));

    /* flag that I did the mapping */
    if (NULL != jdata->map->last_mapper) {
        free(jdata->map->last_mapper);
    }
    jdata->map->last_mapper = strdup(c->mca_component_name);

    /* start at the beginning... */
    jdata->num_procs = 0;

    /* cycle through the app_contexts, mapping them sequentially */
    for(i=0; i < jdata->apps->size; i++) {
        hwloc_obj_type_t target;
        unsigned cache_level;
        if (NULL == (app = (orte_app_context_t*)opal_pointer_array_get_item(jdata->apps, i))) {
            continue;
        }

        /* setup the nodelist here in case we jump to error */
        OBJ_CONSTRUCT(&node_list, opal_list_t);

        /* if the number of processes wasn't specified, then we know there can be only
         * one app_context allowed in the launch, and that we are to launch it across
         * all available slots. We'll double-check the single app_context rule first
         */
        if (0 == app->num_procs && 1 < jdata->num_apps) {
            orte_show_help("help-orte-rmaps-rr.txt", "orte-rmaps-rr:multi-apps-and-zero-np",
                           true, jdata->num_apps, NULL);
            rc = ORTE_ERR_SILENT;
            goto error;
        }

        /* for each app_context, we have to get the list of nodes that it can
         * use since that can now be modified with a hostfile and/or -host
         * option
         */
        if(ORTE_SUCCESS != (rc = orte_rmaps_base_get_target_nodes(&node_list, &num_slots, app,
                                                                  jdata->map->mapping, initial_map, false))) {
            ORTE_ERROR_LOG(rc);
            goto error;
        }
        /* flag that all subsequent requests should not reset the node->mapped flag */
        initial_map = false;

        /* if a bookmark exists from some prior mapping, set us to start there */
        jdata->bookmark = orte_rmaps_base_get_starting_point(&node_list, jdata);

        if (0 == app->num_procs) {
            /* set the num_procs to equal the number of slots on these
             * mapped nodes, taking into account the number of cpus/rank
             */
            app->num_procs = num_slots;
            /* sometimes, we have only one "slot" assigned, but may
             * want more than one cpu/rank - so ensure we always wind
             * up with at least one proc */
            if (0 == app->num_procs) {
                app->num_procs = 1;
            }
        }

        /* Make assignments */
        if (ORTE_MAPPING_BYNODE == ORTE_GET_MAPPING_POLICY(jdata->map->mapping)) {
            rc = orte_rmaps_rr_bynode(jdata, app, &node_list, num_slots,
                                      app->num_procs);
        } else if (ORTE_MAPPING_BYSLOT == ORTE_GET_MAPPING_POLICY(jdata->map->mapping)) {
            rc = orte_rmaps_rr_byslot(jdata, app, &node_list, num_slots,
                                      app->num_procs);
        } else if (ORTE_MAPPING_BYHWTHREAD == ORTE_GET_MAPPING_POLICY(jdata->map->mapping)) {
            rc = orte_rmaps_rr_byobj(jdata, app, &node_list, num_slots,
                                     app->num_procs, HWLOC_OBJ_PU, 0);
            if (ORTE_ERR_NOT_FOUND == rc) {
                /* if the mapper couldn't map by this object because
                 * it isn't available, but the error allows us to try
                 * byslot, then do so
                 */
                ORTE_SET_MAPPING_POLICY(jdata->map->mapping, ORTE_MAPPING_BYSLOT);
                rc = orte_rmaps_rr_byslot(jdata, app, &node_list, num_slots,
                                          app->num_procs);
            }
        } else if (ORTE_MAPPING_BYCORE == ORTE_GET_MAPPING_POLICY(jdata->map->mapping)) {
            rc = orte_rmaps_rr_byobj(jdata, app, &node_list, num_slots,
                                     app->num_procs, HWLOC_OBJ_CORE, 0);
            if (ORTE_ERR_NOT_FOUND == rc) {
                /* if the mapper couldn't map by this object because
                 * it isn't available, but the error allows us to try
                 * byslot, then do so
                 */
                ORTE_SET_MAPPING_POLICY(jdata->map->mapping, ORTE_MAPPING_BYSLOT);
                rc = orte_rmaps_rr_byslot(jdata, app, &node_list, num_slots,
                                          app->num_procs);
            }
        } else if (ORTE_MAPPING_BYL1CACHE == ORTE_GET_MAPPING_POLICY(jdata->map->mapping)) {
            OPAL_HWLOC_MAKE_OBJ_CACHE(1, target, cache_level);
            rc = orte_rmaps_rr_byobj(jdata, app, &node_list, num_slots, app->num_procs,
                                     target, cache_level);
            if (ORTE_ERR_NOT_FOUND == rc) {
                /* if the mapper couldn't map by this object because
                 * it isn't available, but the error allows us to try
                 * byslot, then do so
                 */
                ORTE_SET_MAPPING_POLICY(jdata->map->mapping, ORTE_MAPPING_BYSLOT);
                rc = orte_rmaps_rr_byslot(jdata, app, &node_list, num_slots,
                                          app->num_procs);
            }
        } else if (ORTE_MAPPING_BYL2CACHE == ORTE_GET_MAPPING_POLICY(jdata->map->mapping)) {
            OPAL_HWLOC_MAKE_OBJ_CACHE(2, target, cache_level);
            rc = orte_rmaps_rr_byobj(jdata, app, &node_list, num_slots, app->num_procs,
                                     target, cache_level);
            if (ORTE_ERR_NOT_FOUND == rc) {
                /* if the mapper couldn't map by this object because
                 * it isn't available, but the error allows us to try
                 * byslot, then do so
                 */
                ORTE_SET_MAPPING_POLICY(jdata->map->mapping, ORTE_MAPPING_BYSLOT);
                rc = orte_rmaps_rr_byslot(jdata, app, &node_list, num_slots,
                                          app->num_procs);
            }
        } else if (ORTE_MAPPING_BYL3CACHE == ORTE_GET_MAPPING_POLICY(jdata->map->mapping)) {
            OPAL_HWLOC_MAKE_OBJ_CACHE(3, target, cache_level);
            rc = orte_rmaps_rr_byobj(jdata, app, &node_list, num_slots, app->num_procs,
                                     target, cache_level);
            if (ORTE_ERR_NOT_FOUND == rc) {
                /* if the mapper couldn't map by this object because
                 * it isn't available, but the error allows us to try
                 * byslot, then do so
                 */
                ORTE_SET_MAPPING_POLICY(jdata->map->mapping, ORTE_MAPPING_BYSLOT);
                rc = orte_rmaps_rr_byslot(jdata, app, &node_list, num_slots,
                                          app->num_procs);
            }
        } else if (ORTE_MAPPING_BYSOCKET == ORTE_GET_MAPPING_POLICY(jdata->map->mapping)) {
            rc = orte_rmaps_rr_byobj(jdata, app, &node_list, num_slots,
                                     app->num_procs, HWLOC_OBJ_SOCKET, 0);
            if (ORTE_ERR_NOT_FOUND == rc) {
                /* if the mapper couldn't map by this object because
                 * it isn't available, but the error allows us to try
                 * byslot, then do so
                 */
                ORTE_SET_MAPPING_POLICY(jdata->map->mapping, ORTE_MAPPING_BYSLOT);
                rc = orte_rmaps_rr_byslot(jdata, app, &node_list, num_slots,
                                          app->num_procs);
            }
        } else if (ORTE_MAPPING_BYNUMA == ORTE_GET_MAPPING_POLICY(jdata->map->mapping)) {
            rc = orte_rmaps_rr_byobj(jdata, app, &node_list, num_slots,
                                     app->num_procs, HWLOC_OBJ_NODE, 0);
            if (ORTE_ERR_NOT_FOUND == rc) {
                /* if the mapper couldn't map by this object because
                 * it isn't available, but the error allows us to try
                 * byslot, then do so
                 */
                ORTE_SET_MAPPING_POLICY(jdata->map->mapping, ORTE_MAPPING_BYSLOT);
                rc = orte_rmaps_rr_byslot(jdata, app, &node_list, num_slots,
                                          app->num_procs);
            }
        } else {
            /* unrecognized mapping directive */
            orte_show_help("help-orte-rmaps-base.txt", "unrecognized-policy",
                           true, "mapping",
                           orte_rmaps_base_print_mapping(jdata->map->mapping));
            rc = ORTE_ERR_SILENT;
            goto error;
        }
        if (ORTE_SUCCESS != rc) {
            ORTE_ERROR_LOG(rc);
            goto error;
        }

        /* track the total number of processes we mapped - must update
         * this value AFTER we compute vpids so that computation
         * is done correctly
         */
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

 error:
    while(NULL != (item = opal_list_remove_first(&node_list))) {
        OBJ_RELEASE(item);
    }
    OBJ_DESTRUCT(&node_list);

    return rc;
}

static int orte_rmaps_rr_assign_locations(orte_job_t *jdata)
{
    mca_base_component_t *c = &mca_rmaps_round_robin_component.base_version;
    hwloc_obj_type_t target;
    unsigned cache_level;
    int rc;

    if (NULL == jdata->map->last_mapper ||
        0 != strcasecmp(jdata->map->last_mapper, c->mca_component_name)) {
        /* a mapper has been specified, and it isn't me */
        opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                            "mca:rmaps:rr: job %s not using rr mapper",
                            ORTE_JOBID_PRINT(jdata->jobid));
        return ORTE_ERR_TAKE_NEXT_OPTION;
    }

    opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                        "mca:rmaps:rr: assign locations for job %s",
                        ORTE_JOBID_PRINT(jdata->jobid));

    /* if the mapping directive was byslot or bynode, then we
     * assign locations to the root object level */
    if (ORTE_MAPPING_BYNODE == ORTE_GET_MAPPING_POLICY(jdata->map->mapping) ||
        ORTE_MAPPING_BYSLOT == ORTE_GET_MAPPING_POLICY(jdata->map->mapping)) {
        return orte_rmaps_rr_assign_root_level(jdata);
    }

    /* otherwise, assign by object */
    if (ORTE_MAPPING_BYHWTHREAD == ORTE_GET_MAPPING_POLICY(jdata->map->mapping)) {
        rc = orte_rmaps_rr_assign_byobj(jdata, HWLOC_OBJ_PU, 0);
        if (ORTE_ERR_NOT_FOUND == rc) {
            /* if the mapper couldn't assign by this object because
             * it isn't available, but the error allows us to try
             * byslot, then do so
             */
            ORTE_SET_MAPPING_POLICY(jdata->map->mapping, ORTE_MAPPING_BYSLOT);
            rc = orte_rmaps_rr_assign_root_level(jdata);
        }
    } else if (ORTE_MAPPING_BYCORE == ORTE_GET_MAPPING_POLICY(jdata->map->mapping)) {
        rc = orte_rmaps_rr_assign_byobj(jdata, HWLOC_OBJ_CORE, 0);
        if (ORTE_ERR_NOT_FOUND == rc) {
            /* if the mapper couldn't map by this object because
             * it isn't available, but the error allows us to try
             * byslot, then do so
             */
            ORTE_SET_MAPPING_POLICY(jdata->map->mapping, ORTE_MAPPING_BYSLOT);
            rc = orte_rmaps_rr_assign_root_level(jdata);
        }
    } else if (ORTE_MAPPING_BYL1CACHE == ORTE_GET_MAPPING_POLICY(jdata->map->mapping)) {
        OPAL_HWLOC_MAKE_OBJ_CACHE(1, target, cache_level);
        rc = orte_rmaps_rr_assign_byobj(jdata, target, cache_level);
        if (ORTE_ERR_NOT_FOUND == rc) {
            /* if the mapper couldn't map by this object because
             * it isn't available, but the error allows us to try
             * byslot, then do so
             */
            ORTE_SET_MAPPING_POLICY(jdata->map->mapping, ORTE_MAPPING_BYSLOT);
            rc = orte_rmaps_rr_assign_root_level(jdata);
        }
    } else if (ORTE_MAPPING_BYL2CACHE == ORTE_GET_MAPPING_POLICY(jdata->map->mapping)) {
        OPAL_HWLOC_MAKE_OBJ_CACHE(2, target, cache_level);
        rc = orte_rmaps_rr_assign_byobj(jdata, target, cache_level);
        if (ORTE_ERR_NOT_FOUND == rc) {
            /* if the mapper couldn't map by this object because
             * it isn't available, but the error allows us to try
             * byslot, then do so
             */
            ORTE_SET_MAPPING_POLICY(jdata->map->mapping, ORTE_MAPPING_BYSLOT);
            rc = orte_rmaps_rr_assign_root_level(jdata);
        }
    } else if (ORTE_MAPPING_BYL3CACHE == ORTE_GET_MAPPING_POLICY(jdata->map->mapping)) {
        OPAL_HWLOC_MAKE_OBJ_CACHE(3, target, cache_level);
        rc = orte_rmaps_rr_assign_byobj(jdata, target, cache_level);
        if (ORTE_ERR_NOT_FOUND == rc) {
            /* if the mapper couldn't map by this object because
             * it isn't available, but the error allows us to try
             * byslot, then do so
             */
            ORTE_SET_MAPPING_POLICY(jdata->map->mapping, ORTE_MAPPING_BYSLOT);
            rc = orte_rmaps_rr_assign_root_level(jdata);
        }
    } else if (ORTE_MAPPING_BYSOCKET == ORTE_GET_MAPPING_POLICY(jdata->map->mapping)) {
        rc = orte_rmaps_rr_assign_byobj(jdata, HWLOC_OBJ_SOCKET, 0);
        if (ORTE_ERR_NOT_FOUND == rc) {
            /* if the mapper couldn't map by this object because
             * it isn't available, but the error allows us to try
             * byslot, then do so
             */
            ORTE_SET_MAPPING_POLICY(jdata->map->mapping, ORTE_MAPPING_BYSLOT);
            rc = orte_rmaps_rr_assign_root_level(jdata);
        }
    } else if (ORTE_MAPPING_BYNUMA == ORTE_GET_MAPPING_POLICY(jdata->map->mapping)) {
        rc = orte_rmaps_rr_assign_byobj(jdata, HWLOC_OBJ_NODE, 0);
        if (ORTE_ERR_NOT_FOUND == rc) {
            /* if the mapper couldn't map by this object because
             * it isn't available, but the error allows us to try
             * byslot, then do so
             */
            ORTE_SET_MAPPING_POLICY(jdata->map->mapping, ORTE_MAPPING_BYSLOT);
            rc = orte_rmaps_rr_assign_root_level(jdata);
        }
    } else {
        /* unrecognized mapping directive */
        orte_show_help("help-orte-rmaps-base.txt", "unrecognized-policy",
                       true, "mapping",
                       orte_rmaps_base_print_mapping(jdata->map->mapping));
        rc = ORTE_ERR_SILENT;
    }
    return rc;
}

orte_rmaps_base_module_t orte_rmaps_round_robin_module = {
    .map_job = orte_rmaps_rr_map,
    .assign_locations = orte_rmaps_rr_assign_locations
};
