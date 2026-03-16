/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2011 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2009-2013 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2013-2017 Intel, Inc.  All rights reserved.
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

int orte_rmaps_rr_assign_root_level(orte_job_t *jdata)
{
    int i, m;
    orte_node_t *node;
    orte_proc_t *proc;
    hwloc_obj_t obj=NULL;

    opal_output_verbose(2, orte_rmaps_base_framework.framework_output,
                        "mca:rmaps:rr: assigning procs to root level for job %s",
                        ORTE_JOBID_PRINT(jdata->jobid));

    for (m=0; m < jdata->map->nodes->size; m++) {
        if (NULL == (node = (orte_node_t*)opal_pointer_array_get_item(jdata->map->nodes, m))) {
            continue;
        }
        opal_output_verbose(2, orte_rmaps_base_framework.framework_output,
                            "mca:rmaps:rr:slot working node %s",
                            node->name);
        /* get the root object as we are not assigning
         * locale here except at the node level */
        if (NULL == node->topology || NULL == node->topology->topo) {
            /* nothing we can do */
            continue;
        }
        obj = hwloc_get_root_obj(node->topology->topo);
        for (i=0; i < node->procs->size; i++) {
            if (NULL == (proc = (orte_proc_t*)opal_pointer_array_get_item(node->procs, i))) {
                continue;
            }
            /* ignore procs from other jobs */
            if (proc->name.jobid != jdata->jobid) {
                opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                                    "mca:rmaps:rr:assign skipping proc %s - from another job",
                                    ORTE_NAME_PRINT(&proc->name));
                continue;
            }
            orte_set_attribute(&proc->attributes, ORTE_PROC_HWLOC_LOCALE, ORTE_ATTR_LOCAL, obj, OPAL_PTR);
        }
    }
    return ORTE_SUCCESS;
}

/* mapping by hwloc object looks a lot like mapping by node,
 * but has the added complication of possibly having different
 * numbers of objects on each node
 */
int orte_rmaps_rr_assign_byobj(orte_job_t *jdata,
                               hwloc_obj_type_t target,
                               unsigned cache_level)
{
    int start, j, m, n;
    orte_app_context_t *app;
    orte_node_t *node;
    orte_proc_t *proc;
    hwloc_obj_t obj=NULL;
    unsigned int nobjs;

    opal_output_verbose(2, orte_rmaps_base_framework.framework_output,
                        "mca:rmaps:rr: assigning locations by %s for job %s",
                        hwloc_obj_type_string(target),
                        ORTE_JOBID_PRINT(jdata->jobid));


    /* start mapping procs onto objects, filling each object as we go until
     * all procs are mapped. If one pass doesn't catch all the required procs,
     * then loop thru the list again to handle the oversubscription
     */
    for (n=0; n < jdata->apps->size; n++) {
        if (NULL == (app = (orte_app_context_t*)opal_pointer_array_get_item(jdata->apps, n))) {
            continue;
        }
        for (m=0; m < jdata->map->nodes->size; m++) {
            if (NULL == (node = (orte_node_t*)opal_pointer_array_get_item(jdata->map->nodes, m))) {
                continue;
            }
            if (NULL == node->topology || NULL == node->topology->topo) {
                orte_show_help("help-orte-rmaps-ppr.txt", "ppr-topo-missing",
                               true, node->name);
                return ORTE_ERR_SILENT;
            }
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
            } else {
                start = 0;
            }
            /* loop over the procs on this node */
            for (j=0; j < node->procs->size; j++) {
                if (NULL == (proc = (orte_proc_t*)opal_pointer_array_get_item(node->procs, j))) {
                    continue;
                }
                /* ignore procs from other jobs */
                if (proc->name.jobid != jdata->jobid) {
                    opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                                        "mca:rmaps:rr:assign skipping proc %s - from another job",
                                        ORTE_NAME_PRINT(&proc->name));
                    continue;
                }
                /* ignore procs from other apps */
                if (proc->app_idx != app->idx) {
                    continue;
                }
                opal_output_verbose(20, orte_rmaps_base_framework.framework_output,
                                    "mca:rmaps:rr: assigning proc to object %d", (j + start) % nobjs);
                /* get the hwloc object */
                if (NULL == (obj = opal_hwloc_base_get_obj_by_type(node->topology->topo, target, cache_level, (j + start) % nobjs, OPAL_HWLOC_AVAILABLE))) {
                    ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
                    return ORTE_ERR_NOT_FOUND;
                }
                if (orte_rmaps_base.cpus_per_rank > (int)opal_hwloc_base_get_npus(node->topology->topo, obj)) {
                    orte_show_help("help-orte-rmaps-base.txt", "mapping-too-low", true,
                                   orte_rmaps_base.cpus_per_rank, opal_hwloc_base_get_npus(node->topology->topo, obj),
                                   orte_rmaps_base_print_mapping(orte_rmaps_base.mapping));
                    return ORTE_ERR_SILENT;
                }
                orte_set_attribute(&proc->attributes, ORTE_PROC_HWLOC_LOCALE, ORTE_ATTR_LOCAL, obj, OPAL_PTR);
            }
        }
    }

    return ORTE_SUCCESS;
}
