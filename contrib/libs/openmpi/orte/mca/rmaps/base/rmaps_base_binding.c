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
 * Copyright (c) 2011-2014 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2011-2012 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2013-2018 Intel, Inc.  All rights reserved.
 * Copyright (c) 2015-2017 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2018      Inria.  All rights reserved.
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

static bool membind_warned=false;

static void reset_usage(orte_node_t *node, orte_jobid_t jobid)
{
    int j;
    orte_proc_t *proc;
    opal_hwloc_obj_data_t *data=NULL;
    hwloc_obj_t bound;

    opal_output_verbose(10, orte_rmaps_base_framework.framework_output,
                        "%s reset_usage: node %s has %d procs on it",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                        node->name, node->num_procs);

    /* start by clearing any existing info */
    opal_hwloc_base_clear_usage(node->topology->topo);

    /* cycle thru the procs on the node and record
     * their usage in the topology
     */
    for (j=0; j < node->procs->size; j++) {
        if (NULL == (proc = (orte_proc_t*)opal_pointer_array_get_item(node->procs, j))) {
            continue;
        }
        /* ignore procs from this job */
        if (proc->name.jobid == jobid) {
            opal_output_verbose(10, orte_rmaps_base_framework.framework_output,
                                "%s reset_usage: ignoring proc %s",
                                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                ORTE_NAME_PRINT(&proc->name));
            continue;
        }
        bound = NULL;
        if (!orte_get_attribute(&proc->attributes, ORTE_PROC_HWLOC_BOUND, (void**)&bound, OPAL_PTR) ||
            NULL == bound) {
            /* this proc isn't bound - ignore it */
            opal_output_verbose(10, orte_rmaps_base_framework.framework_output,
                                "%s reset_usage: proc %s has no bind location",
                                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                ORTE_NAME_PRINT(&proc->name));
            continue;
        }
        data = (opal_hwloc_obj_data_t*)bound->userdata;
        if (NULL == data) {
            data = OBJ_NEW(opal_hwloc_obj_data_t);
            bound->userdata = data;
        }
        data->num_bound++;
        opal_output_verbose(10, orte_rmaps_base_framework.framework_output,
                            "%s reset_usage: proc %s is bound - total %d",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                            ORTE_NAME_PRINT(&proc->name), data->num_bound);
    }
}

static void unbind_procs(orte_job_t *jdata)
{
    int j;
    orte_proc_t *proc;

    for (j=0; j < jdata->procs->size; j++) {
        if (NULL == (proc = (orte_proc_t*)opal_pointer_array_get_item(jdata->procs, j))) {
            continue;
        }
        orte_remove_attribute(&proc->attributes, ORTE_PROC_HWLOC_BOUND);
        orte_remove_attribute(&proc->attributes, ORTE_PROC_CPU_BITMAP);
    }
}

static int bind_generic(orte_job_t *jdata,
                        orte_node_t *node,
                        int target_depth)
{
    int j, rc;
    orte_job_map_t *map;
    orte_proc_t *proc;
    hwloc_obj_t trg_obj, tmp_obj, nxt_obj;
    unsigned int ncpus;
    opal_hwloc_obj_data_t *data;
    int total_cpus;
    hwloc_cpuset_t totalcpuset;
    hwloc_obj_t locale;
    char *cpu_bitmap;
    unsigned min_bound;

    opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                        "mca:rmaps: bind downward for job %s with bindings %s",
                        ORTE_JOBID_PRINT(jdata->jobid),
                        opal_hwloc_base_print_binding(jdata->map->binding));
    /* initialize */
    map = jdata->map;
    totalcpuset = hwloc_bitmap_alloc();

    /* cycle thru the procs */
    for (j=0; j < node->procs->size; j++) {
        if (NULL == (proc = (orte_proc_t*)opal_pointer_array_get_item(node->procs, j))) {
            continue;
        }
        /* ignore procs from other jobs */
        if (proc->name.jobid != jdata->jobid) {
            continue;
        }
        /* bozo check */
        locale = NULL;
        if (!orte_get_attribute(&proc->attributes, ORTE_PROC_HWLOC_LOCALE, (void**)&locale, OPAL_PTR) ||
            NULL == locale) {
            orte_show_help("help-orte-rmaps-base.txt", "rmaps:no-locale", true, ORTE_NAME_PRINT(&proc->name));
            hwloc_bitmap_free(totalcpuset);
            return ORTE_ERR_SILENT;
        }

        /* use the min_bound object that intersects locale->cpuset at target_depth */
        tmp_obj = NULL;
        trg_obj = NULL;
        min_bound = UINT_MAX;
        while (NULL != (tmp_obj = hwloc_get_next_obj_by_depth(node->topology->topo, target_depth, tmp_obj))) {
            if (!hwloc_bitmap_intersects(locale->cpuset, tmp_obj->cpuset))
                continue;
            data = (opal_hwloc_obj_data_t*)tmp_obj->userdata;
            if (NULL == data) {
                data = OBJ_NEW(opal_hwloc_obj_data_t);
                tmp_obj->userdata = data;
            }
            if (data->num_bound < min_bound) {
                min_bound = data->num_bound;
                trg_obj = tmp_obj;
            }
        }
        if (NULL == trg_obj) {
            /* there aren't any such targets under this object */
            orte_show_help("help-orte-rmaps-base.txt", "rmaps:no-available-cpus", true, node->name);
            hwloc_bitmap_free(totalcpuset);
            return ORTE_ERR_SILENT;
        }
        /* record the location */
        orte_set_attribute(&proc->attributes, ORTE_PROC_HWLOC_BOUND, ORTE_ATTR_LOCAL, trg_obj, OPAL_PTR);

        /* start with a clean slate */
        hwloc_bitmap_zero(totalcpuset);
        total_cpus = 0;
        nxt_obj = trg_obj;
        do {
            if (NULL == nxt_obj) {
                /* could not find enough cpus to meet request */
                orte_show_help("help-orte-rmaps-base.txt", "rmaps:no-available-cpus", true, node->name);
                hwloc_bitmap_free(totalcpuset);
                return ORTE_ERR_SILENT;
            }
            trg_obj = nxt_obj;
            /* get the number of cpus under this location */
            ncpus = opal_hwloc_base_get_npus(node->topology->topo, trg_obj);
            opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                                "%s GOT %d CPUS",
                                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ncpus);
            /* track the number bound */
            if (NULL == (data = (opal_hwloc_obj_data_t*)trg_obj->userdata)) {
                data = OBJ_NEW(opal_hwloc_obj_data_t);
                trg_obj->userdata = data;
            }
            data->num_bound++;
            /* error out if adding a proc would cause overload and that wasn't allowed,
             * and it wasn't a default binding policy (i.e., the user requested it)
             */
            if (ncpus < data->num_bound &&
                !OPAL_BIND_OVERLOAD_ALLOWED(jdata->map->binding)) {
                if (OPAL_BINDING_POLICY_IS_SET(jdata->map->binding)) {
                    /* if the user specified a binding policy, then we cannot meet
                     * it since overload isn't allowed, so error out - have the
                     * message indicate that setting overload allowed will remove
                     * this restriction */
                    orte_show_help("help-orte-rmaps-base.txt", "rmaps:binding-overload", true,
                                   opal_hwloc_base_print_binding(map->binding), node->name,
                                   data->num_bound, ncpus);
                    hwloc_bitmap_free(totalcpuset);
                    return ORTE_ERR_SILENT;
                } else {
                    /* if we have the default binding policy, then just don't bind */
                    OPAL_SET_BINDING_POLICY(map->binding, OPAL_BIND_TO_NONE);
                    unbind_procs(jdata);
                    hwloc_bitmap_zero(totalcpuset);
                    return ORTE_SUCCESS;
                }
            }
            /* bind the proc here */
            hwloc_bitmap_or(totalcpuset, totalcpuset, trg_obj->cpuset);
            /* track total #cpus */
            total_cpus += ncpus;
            /* move to the next location, in case we need it */
            nxt_obj = trg_obj->next_cousin;
        } while (total_cpus < orte_rmaps_base.cpus_per_rank);
        hwloc_bitmap_list_asprintf(&cpu_bitmap, totalcpuset);
        opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                            "%s PROC %s BITMAP %s",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                            ORTE_NAME_PRINT(&proc->name), cpu_bitmap);
        orte_set_attribute(&proc->attributes, ORTE_PROC_CPU_BITMAP, ORTE_ATTR_GLOBAL, cpu_bitmap, OPAL_STRING);
        if (NULL != cpu_bitmap) {
            free(cpu_bitmap);
        }
        if (4 < opal_output_get_verbosity(orte_rmaps_base_framework.framework_output)) {
            char tmp1[1024], tmp2[1024];
            if (OPAL_ERR_NOT_BOUND == opal_hwloc_base_cset2str(tmp1, sizeof(tmp1),
                                                               node->topology->topo, totalcpuset)) {
                opal_output(orte_rmaps_base_framework.framework_output,
                            "%s PROC %s ON %s IS NOT BOUND",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                            ORTE_NAME_PRINT(&proc->name), node->name);
            } else {
                rc = opal_hwloc_base_cset2mapstr(tmp2, sizeof(tmp2), node->topology->topo, totalcpuset);
                if (OPAL_SUCCESS != rc) {
                    ORTE_ERROR_LOG(rc);
                }
                opal_output(orte_rmaps_base_framework.framework_output,
                            "%s BOUND PROC %s[%s] TO %s: %s",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                            ORTE_NAME_PRINT(&proc->name), node->name,
                            tmp1, tmp2);
            }
        }
    }
    hwloc_bitmap_free(totalcpuset);

    return ORTE_SUCCESS;
}

static int bind_in_place(orte_job_t *jdata,
                         hwloc_obj_type_t target,
                         unsigned cache_level)
{
    /* traverse the hwloc topology tree on each node downwards
     * until we find an unused object of type target - and then bind
     * the process to that target
     */
    int i, j;
    orte_job_map_t *map;
    orte_node_t *node;
    orte_proc_t *proc;
    unsigned int idx, ncpus;
    struct hwloc_topology_support *support;
    opal_hwloc_obj_data_t *data;
    hwloc_obj_t locale, sib;
    char *cpu_bitmap;
    bool found;

    opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                        "mca:rmaps: bind in place for job %s with bindings %s",
                        ORTE_JOBID_PRINT(jdata->jobid),
                        opal_hwloc_base_print_binding(jdata->map->binding));
    /* initialize */
    map = jdata->map;

    for (i=0; i < map->nodes->size; i++) {
        if (NULL == (node = (orte_node_t*)opal_pointer_array_get_item(map->nodes, i))) {
            continue;
        }
        if (!orte_no_vm && (int)ORTE_PROC_MY_NAME->vpid != node->index) {
            continue;
        }
        if (!orte_do_not_launch) {
            /* if we don't want to launch, then we are just testing the system,
             * so ignore questions about support capabilities
             */
            support = (struct hwloc_topology_support*)hwloc_topology_get_support(node->topology->topo);
            /* check if topology supports cpubind - have to be careful here
             * as Linux doesn't currently support thread-level binding. This
             * may change in the future, though, and it isn't clear how hwloc
             * interprets the current behavior. So check both flags to be sure.
             */
            if (!support->cpubind->set_thisproc_cpubind &&
                !support->cpubind->set_thisthread_cpubind) {
                if (!OPAL_BINDING_REQUIRED(map->binding) ||
                    !OPAL_BINDING_POLICY_IS_SET(map->binding)) {
                    /* we are not required to bind, so ignore this */
                    continue;
                }
                orte_show_help("help-orte-rmaps-base.txt", "rmaps:cpubind-not-supported", true, node->name);
                return ORTE_ERR_SILENT;
            }
            /* check if topology supports membind - have to be careful here
             * as hwloc treats this differently than I (at least) would have
             * expected. Per hwloc, Linux memory binding is at the thread,
             * and not process, level. Thus, hwloc sets the "thisproc" flag
             * to "false" on all Linux systems, and uses the "thisthread" flag
             * to indicate binding capability - don't warn if the user didn't
             * specifically request binding
             */
            if (!support->membind->set_thisproc_membind &&
                !support->membind->set_thisthread_membind &&
                OPAL_BINDING_POLICY_IS_SET(map->binding)) {
                if (OPAL_HWLOC_BASE_MBFA_WARN == opal_hwloc_base_mbfa && !membind_warned) {
                    orte_show_help("help-orte-rmaps-base.txt", "rmaps:membind-not-supported", true, node->name);
                    membind_warned = true;
                } else if (OPAL_HWLOC_BASE_MBFA_ERROR == opal_hwloc_base_mbfa) {
                    orte_show_help("help-orte-rmaps-base.txt", "rmaps:membind-not-supported-fatal", true, node->name);
                    return ORTE_ERR_SILENT;
                }
            }
        }

        /* some systems do not report cores, and so we can get a situation where our
         * default binding policy will fail for no necessary reason. So if we are
         * computing a binding due to our default policy, and no cores are found
         * on this node, just silently skip it - we will not bind
         */
        if (!OPAL_BINDING_POLICY_IS_SET(map->binding) &&
            HWLOC_TYPE_DEPTH_UNKNOWN == hwloc_get_type_depth(node->topology->topo, HWLOC_OBJ_CORE)) {
            opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                                "Unable to bind-to core by default on node %s as no cores detected",
                                node->name);
            continue;
        }

        /* we share topologies in order
         * to save space, so we need to reset the usage info to reflect
         * our own current state
         */
        reset_usage(node, jdata->jobid);

        /* cycle thru the procs */
        for (j=0; j < node->procs->size; j++) {
            if (NULL == (proc = (orte_proc_t*)opal_pointer_array_get_item(node->procs, j))) {
                continue;
            }
            /* ignore procs from other jobs */
            if (proc->name.jobid != jdata->jobid) {
                continue;
            }
            /* bozo check */
            locale = NULL;
            if (!orte_get_attribute(&proc->attributes, ORTE_PROC_HWLOC_LOCALE, (void**)&locale, OPAL_PTR)) {
                orte_show_help("help-orte-rmaps-base.txt", "rmaps:no-locale", true, ORTE_NAME_PRINT(&proc->name));
                return ORTE_ERR_SILENT;
            }
            /* get the index of this location */
            if (UINT_MAX == (idx = opal_hwloc_base_get_obj_idx(node->topology->topo, locale, OPAL_HWLOC_AVAILABLE))) {
                ORTE_ERROR_LOG(ORTE_ERR_BAD_PARAM);
                return ORTE_ERR_SILENT;
            }
            /* get the number of cpus under this location */
            if (0 == (ncpus = opal_hwloc_base_get_npus(node->topology->topo, locale))) {
                orte_show_help("help-orte-rmaps-base.txt", "rmaps:no-available-cpus", true, node->name);
                return ORTE_ERR_SILENT;
            }
            data = (opal_hwloc_obj_data_t*)locale->userdata;
            if (NULL == data) {
                data = OBJ_NEW(opal_hwloc_obj_data_t);
                locale->userdata = data;
            }
            /* if we don't have enough cpus to support this additional proc, try
             * shifting the location to a cousin that can support it - the important
             * thing is that we maintain the same level in the topology */
            if (ncpus < (data->num_bound+1)) {
                opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                                    "%s bind_in_place: searching right",
                                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
                sib = locale;
                found = false;
                while (NULL != (sib = sib->next_cousin)) {
                    ncpus = opal_hwloc_base_get_npus(node->topology->topo, sib);
                    data = (opal_hwloc_obj_data_t*)sib->userdata;
                    if (NULL == data) {
                        data = OBJ_NEW(opal_hwloc_obj_data_t);
                        sib->userdata = data;
                    }
                    if (data->num_bound < ncpus) {
                        found = true;
                        locale = sib;
                        break;
                    }
                }
                if (!found) {
                    /* try the other direction */
                    opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                                        "%s bind_in_place: searching left",
                                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
                    sib = locale;
                    while (NULL != (sib = sib->prev_cousin)) {
                        ncpus = opal_hwloc_base_get_npus(node->topology->topo, sib);
                        data = (opal_hwloc_obj_data_t*)sib->userdata;
                        if (NULL == data) {
                            data = OBJ_NEW(opal_hwloc_obj_data_t);
                            sib->userdata = data;
                        }
                        if (data->num_bound < ncpus) {
                            found = true;
                            locale = sib;
                            break;
                        }
                    }
                }
                if (!found) {
                    /* no place to put this - see if overload is allowed */
                    if (!OPAL_BIND_OVERLOAD_ALLOWED(jdata->map->binding)) {
                        if (OPAL_BINDING_POLICY_IS_SET(jdata->map->binding)) {
                            /* if the user specified a binding policy, then we cannot meet
                             * it since overload isn't allowed, so error out - have the
                             * message indicate that setting overload allowed will remove
                             * this restriction */
                            orte_show_help("help-orte-rmaps-base.txt", "rmaps:binding-overload", true,
                                           opal_hwloc_base_print_binding(map->binding), node->name,
                                           data->num_bound, ncpus);
                            return ORTE_ERR_SILENT;
                        } else {
                            /* if we have the default binding policy, then just don't bind */
                            OPAL_SET_BINDING_POLICY(map->binding, OPAL_BIND_TO_NONE);
                            unbind_procs(jdata);
                            return ORTE_SUCCESS;
                        }
                    }
                }
            }
            /* track the number bound */
            data = (opal_hwloc_obj_data_t*)locale->userdata;  // just in case it changed
            if (NULL == data) {
                data = OBJ_NEW(opal_hwloc_obj_data_t);
                locale->userdata = data;
            }
            data->num_bound++;
            opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                                "BINDING PROC %s TO %s NUMBER %u",
                                ORTE_NAME_PRINT(&proc->name),
                                hwloc_obj_type_string(locale->type), idx);
            /* bind the proc here */
            hwloc_bitmap_list_asprintf(&cpu_bitmap, locale->cpuset);
            orte_set_attribute(&proc->attributes, ORTE_PROC_CPU_BITMAP, ORTE_ATTR_GLOBAL, cpu_bitmap, OPAL_STRING);
            /* update the location, in case it changed */
            orte_set_attribute(&proc->attributes, ORTE_PROC_HWLOC_BOUND, ORTE_ATTR_LOCAL, locale, OPAL_PTR);
            opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                                "%s BOUND PROC %s TO %s[%s:%u] on node %s",
                                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                ORTE_NAME_PRINT(&proc->name),
                                cpu_bitmap, hwloc_obj_type_string(locale->type),
                                idx, node->name);
            if (NULL != cpu_bitmap) {
                free(cpu_bitmap);
            }
        }
    }

    return ORTE_SUCCESS;
}

static int bind_to_cpuset(orte_job_t *jdata)
{
    /* bind each process to opal_hwloc_base_cpu_list */
    int i, j;
    orte_job_map_t *map;
    orte_node_t *node;
    orte_proc_t *proc;
    struct hwloc_topology_support *support;
    opal_hwloc_topo_data_t *sum;
    hwloc_obj_t root;
    char *cpu_bitmap;
    unsigned id;
    orte_local_rank_t lrank;
    hwloc_bitmap_t mycpuset, tset;

    opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                        "mca:rmaps: bind job %s to cpus %s",
                        ORTE_JOBID_PRINT(jdata->jobid),
                        opal_hwloc_base_cpu_list);
    /* initialize */
    map = jdata->map;
    mycpuset = hwloc_bitmap_alloc();

    for (i=0; i < map->nodes->size; i++) {
        if (NULL == (node = (orte_node_t*)opal_pointer_array_get_item(map->nodes, i))) {
            continue;
        }
        if (!orte_no_vm && (int)ORTE_PROC_MY_NAME->vpid != node->index) {
            continue;
        }
        if (!orte_do_not_launch) {
            /* if we don't want to launch, then we are just testing the system,
             * so ignore questions about support capabilities
             */
            support = (struct hwloc_topology_support*)hwloc_topology_get_support(node->topology->topo);
            /* check if topology supports cpubind - have to be careful here
             * as Linux doesn't currently support thread-level binding. This
             * may change in the future, though, and it isn't clear how hwloc
             * interprets the current behavior. So check both flags to be sure.
             */
            if (!support->cpubind->set_thisproc_cpubind &&
                !support->cpubind->set_thisthread_cpubind) {
                if (!OPAL_BINDING_REQUIRED(opal_hwloc_binding_policy)) {
                    /* we are not required to bind, so ignore this */
                    continue;
                }
                orte_show_help("help-orte-rmaps-base.txt", "rmaps:cpubind-not-supported", true, node->name);
                hwloc_bitmap_free(mycpuset);
                return ORTE_ERR_SILENT;
            }
            /* check if topology supports membind - have to be careful here
             * as hwloc treats this differently than I (at least) would have
             * expected. Per hwloc, Linux memory binding is at the thread,
             * and not process, level. Thus, hwloc sets the "thisproc" flag
             * to "false" on all Linux systems, and uses the "thisthread" flag
             * to indicate binding capability
             */
            if (!support->membind->set_thisproc_membind &&
                !support->membind->set_thisthread_membind) {
                if (OPAL_HWLOC_BASE_MBFA_WARN == opal_hwloc_base_mbfa && !membind_warned) {
                    orte_show_help("help-orte-rmaps-base.txt", "rmaps:membind-not-supported", true, node->name);
                    membind_warned = true;
                } else if (OPAL_HWLOC_BASE_MBFA_ERROR == opal_hwloc_base_mbfa) {
                    orte_show_help("help-orte-rmaps-base.txt", "rmaps:membind-not-supported-fatal", true, node->name);
                    hwloc_bitmap_free(mycpuset);
                    return ORTE_ERR_SILENT;
                }
            }
        }
        root = hwloc_get_root_obj(node->topology->topo);
        if (NULL == root->userdata) {
            /* something went wrong */
            ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
            hwloc_bitmap_free(mycpuset);
            return ORTE_ERR_NOT_FOUND;
        }
        sum = (opal_hwloc_topo_data_t*)root->userdata;
        if (NULL == sum->available) {
            /* another error */
            ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
            hwloc_bitmap_free(mycpuset);
            return ORTE_ERR_NOT_FOUND;
        }
        /* the cpu list in sum->available has already been filtered
         * to include _only_ the cpus defined by the user */
        for (j=0; j < node->procs->size; j++) {
            if (NULL == (proc = (orte_proc_t*)opal_pointer_array_get_item(node->procs, j))) {
                continue;
            }
            /* ignore procs from other jobs */
            if (proc->name.jobid != jdata->jobid) {
                continue;
            }
            if (OPAL_BIND_ORDERED_REQUESTED(jdata->map->binding)) {
                /* assign each proc, in local rank order, to
                 * the corresponding cpu in the list */
                id = hwloc_bitmap_first(sum->available);
                lrank = 0;
                while (lrank != proc->local_rank) {
                    id = hwloc_bitmap_next(sum->available, id);
                    if ((unsigned)-1 == id) {
                        break;
                    }
                    ++lrank;
                }
                if ((unsigned)-1 ==id) {
                    /* ran out of cpus - that's an error */
                    orte_show_help("help-orte-rmaps-base.txt", "rmaps:insufficient-cpus", true,
                                   node->name, (int)proc->local_rank, opal_hwloc_base_cpu_list);
                    hwloc_bitmap_free(mycpuset);
                    return ORTE_ERR_OUT_OF_RESOURCE;
                }
                 /* set the bit of interest */
                hwloc_bitmap_only(mycpuset, id);
                tset = mycpuset;
            } else {
                /* bind the proc to all assigned cpus */
                tset = sum->available;
            }
            hwloc_bitmap_list_asprintf(&cpu_bitmap, tset);
            orte_set_attribute(&proc->attributes, ORTE_PROC_CPU_BITMAP, ORTE_ATTR_GLOBAL, cpu_bitmap, OPAL_STRING);
            if (NULL != cpu_bitmap) {
                free(cpu_bitmap);
            }
        }
    }
    hwloc_bitmap_free(mycpuset);
    return ORTE_SUCCESS;
}

int orte_rmaps_base_compute_bindings(orte_job_t *jdata)
{
    hwloc_obj_type_t hwb;
    unsigned clvl=0;
    opal_binding_policy_t bind;
    orte_mapping_policy_t map;
    orte_node_t *node;
    int i, rc;
    struct hwloc_topology_support *support;
    int bind_depth;

    opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                        "mca:rmaps: compute bindings for job %s with policy %s[%x]",
                        ORTE_JOBID_PRINT(jdata->jobid),
                        opal_hwloc_base_print_binding(jdata->map->binding), jdata->map->binding);

    map = ORTE_GET_MAPPING_POLICY(jdata->map->mapping);
    bind = OPAL_GET_BINDING_POLICY(jdata->map->binding);

    if (ORTE_MAPPING_BYUSER == map) {
        /* user specified binding by rankfile - nothing for us to do */
        return ORTE_SUCCESS;
    }

    if (OPAL_BIND_TO_CPUSET == bind) {
        int rc;
        /* cpuset was given - setup the bindings */
        if (ORTE_SUCCESS != (rc = bind_to_cpuset(jdata))) {
            ORTE_ERROR_LOG(rc);
        }
        return rc;
    }

    if (OPAL_BIND_TO_NONE == bind) {
        /* no binding requested */
        return ORTE_SUCCESS;
    }

    if (OPAL_BIND_TO_BOARD == bind) {
        /* doesn't do anything at this time */
        return ORTE_SUCCESS;
    }

    /* binding requested - convert the binding level to the hwloc obj type */
    switch (bind) {
    case OPAL_BIND_TO_NUMA:
        hwb = HWLOC_OBJ_NODE;
        break;
    case OPAL_BIND_TO_SOCKET:
        hwb = HWLOC_OBJ_SOCKET;
        break;
    case OPAL_BIND_TO_L3CACHE:
        OPAL_HWLOC_MAKE_OBJ_CACHE(3, hwb, clvl);
        break;
    case OPAL_BIND_TO_L2CACHE:
        OPAL_HWLOC_MAKE_OBJ_CACHE(2, hwb, clvl);
        break;
    case OPAL_BIND_TO_L1CACHE:
        OPAL_HWLOC_MAKE_OBJ_CACHE(1, hwb, clvl);
        break;
    case OPAL_BIND_TO_CORE:
        hwb = HWLOC_OBJ_CORE;
        break;
    case OPAL_BIND_TO_HWTHREAD:
        hwb = HWLOC_OBJ_PU;
        break;
    default:
        ORTE_ERROR_LOG(ORTE_ERR_BAD_PARAM);
        return ORTE_ERR_BAD_PARAM;
    }

    /* if the job was mapped by the corresponding target, then
     * we bind in place
     *
     * otherwise, we have to bind either up or down the hwloc
     * tree. If we are binding upwards (e.g., mapped to hwthread
     * but binding to core), then we just climb the tree to find
     * the first matching object.
     *
     * if we are binding downwards (e.g., mapped to node and bind
     * to core), then we have to do a round-robin assigment of
     * procs to the resources below.
     */

    if (ORTE_MAPPING_BYDIST == map) {
        int rc = ORTE_SUCCESS;
        if (OPAL_BIND_TO_NUMA == bind) {
            opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                                "mca:rmaps: bindings for job %s - dist to numa",
                                ORTE_JOBID_PRINT(jdata->jobid));
            if (ORTE_SUCCESS != (rc = bind_in_place(jdata, HWLOC_OBJ_NODE, 0))) {
                ORTE_ERROR_LOG(rc);
            }
        } else if (OPAL_BIND_TO_NUMA < bind) {
            /* bind every proc downwards */
            goto execute;
        }
        /* if the binding policy is less than numa, then we are unbound - so
         * just ignore this and return (should have been caught in prior
         * tests anyway as only options meeting that criteria are "none"
         * and "board")
         */
        return rc;
    }

    /* now deal with the remaining binding policies based on hardware */
    if (bind == map) {
        opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                            "mca:rmaps: bindings for job %s - bind in place",
                            ORTE_JOBID_PRINT(jdata->jobid));
        if (ORTE_SUCCESS != (rc = bind_in_place(jdata, hwb, clvl))) {
            ORTE_ERROR_LOG(rc);
        }
        return rc;
    }

    /* we need to handle the remaining binding options on a per-node
     * basis because different nodes could potentially have different
     * topologies, with different relative depths for the two levels
     */
  execute:
    /* initialize */
    opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                        "mca:rmaps: computing bindings for job %s",
                        ORTE_JOBID_PRINT(jdata->jobid));

    for (i=0; i < jdata->map->nodes->size; i++) {
        if (NULL == (node = (orte_node_t*)opal_pointer_array_get_item(jdata->map->nodes, i))) {
            continue;
        }
        if (!orte_no_vm && !orte_do_not_launch &&
            (int)ORTE_PROC_MY_NAME->vpid != node->index) {
            continue;
        }
        if (!orte_do_not_launch) {
            /* if we don't want to launch, then we are just testing the system,
             * so ignore questions about support capabilities
             */
            support = (struct hwloc_topology_support*)hwloc_topology_get_support(node->topology->topo);
            /* check if topology supports cpubind - have to be careful here
             * as Linux doesn't currently support thread-level binding. This
             * may change in the future, though, and it isn't clear how hwloc
             * interprets the current behavior. So check both flags to be sure.
             */
            if (!support->cpubind->set_thisproc_cpubind &&
                !support->cpubind->set_thisthread_cpubind) {
                if (!OPAL_BINDING_REQUIRED(jdata->map->binding) ||
                    !OPAL_BINDING_POLICY_IS_SET(jdata->map->binding)) {
                    /* we are not required to bind, so ignore this */
                    continue;
                }
                orte_show_help("help-orte-rmaps-base.txt", "rmaps:cpubind-not-supported", true, node->name);
                return ORTE_ERR_SILENT;
            }
            /* check if topology supports membind - have to be careful here
             * as hwloc treats this differently than I (at least) would have
             * expected. Per hwloc, Linux memory binding is at the thread,
             * and not process, level. Thus, hwloc sets the "thisproc" flag
             * to "false" on all Linux systems, and uses the "thisthread" flag
             * to indicate binding capability - don't warn if the user didn't
             * specifically request binding
             */
            if (!support->membind->set_thisproc_membind &&
                !support->membind->set_thisthread_membind &&
                OPAL_BINDING_POLICY_IS_SET(jdata->map->binding)) {
                if (OPAL_HWLOC_BASE_MBFA_WARN == opal_hwloc_base_mbfa && !membind_warned) {
                    orte_show_help("help-orte-rmaps-base.txt", "rmaps:membind-not-supported", true, node->name);
                    membind_warned = true;
                } else if (OPAL_HWLOC_BASE_MBFA_ERROR == opal_hwloc_base_mbfa) {
                    orte_show_help("help-orte-rmaps-base.txt", "rmaps:membind-not-supported-fatal", true, node->name);
                    return ORTE_ERR_SILENT;
                }
            }
        }

        /* some systems do not report cores, and so we can get a situation where our
         * default binding policy will fail for no necessary reason. So if we are
         * computing a binding due to our default policy, and no cores are found
         * on this node, just silently skip it - we will not bind
         */
        if (!OPAL_BINDING_POLICY_IS_SET(jdata->map->binding) &&
            HWLOC_TYPE_DEPTH_UNKNOWN == hwloc_get_type_depth(node->topology->topo, HWLOC_OBJ_CORE)) {
            opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                                "Unable to bind-to core by default on node %s as no cores detected",
                                node->name);
            continue;
        }

        /* we share topologies in order
         * to save space, so we need to reset the usage info to reflect
         * our own current state
         */
        reset_usage(node, jdata->jobid);

        /* determine the relative depth on this node */
#if HWLOC_API_VERSION < 0x20000
        if (HWLOC_OBJ_CACHE == hwb) {
            /* must use a unique function because blasted hwloc
             * just doesn't deal with caches very well...sigh
             */
            bind_depth = hwloc_get_cache_type_depth(node->topology->topo, clvl, (hwloc_obj_cache_type_t)-1);
        } else
#endif
            bind_depth = hwloc_get_type_depth(node->topology->topo, hwb);
#if HWLOC_API_VERSION < 0x20000
        if (0 > bind_depth)
#else
        if (0 > bind_depth && HWLOC_TYPE_DEPTH_NUMANODE != bind_depth)
#endif
        {
            /* didn't find such an object */
            orte_show_help("help-orte-rmaps-base.txt", "orte-rmaps-base:no-objects",
                           true, hwloc_obj_type_string(hwb), node->name);
            return ORTE_ERR_SILENT;
        }
        opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                            "%s bind_depth: %d",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                            bind_depth);
        if (ORTE_SUCCESS != (rc = bind_generic(jdata, node, bind_depth))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }
    }

    return ORTE_SUCCESS;
}
