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
 * Copyright (c) 2007      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2011-2012 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2014-2017 Intel, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "orte_config.h"
#include "orte/constants.h"
#include "orte/types.h"

#include "opal/dss/dss.h"
#include "opal/util/argv.h"

#include "orte/mca/errmgr/errmgr.h"
#include "orte/mca/ess/ess.h"
#include "orte/mca/odls/odls_types.h"
#include "orte/mca/rml/rml.h"
#include "orte/mca/state/state.h"
#include "orte/runtime/orte_globals.h"
#include "orte/runtime/orte_wait.h"

#include "orte/mca/routed/base/base.h"

char* orte_routed_base_assign_module(char *modules)
{
    orte_routed_base_active_t *active;
    char **desired;
    int i;

    /* the incoming param contains a comma-delimited, prioritized
     * list of desired routing modules. If it is NULL, then we
     * simply return the module at the top of our list */
    if (NULL == modules) {
        active = (orte_routed_base_active_t*)opal_list_get_first(&orte_routed_base.actives);
        return active->component->base_version.mca_component_name;
    }

    /* otherwise, cycle thru the provided list of desired modules
     * and pick the highest priority one that matches */
    desired = opal_argv_split(modules, ',');
    for (i=0; NULL != desired[i]; i++) {
        OPAL_LIST_FOREACH(active, &orte_routed_base.actives, orte_routed_base_active_t) {
            if (0 == strcasecmp(desired[i], active->component->base_version.mca_component_name)) {
                opal_argv_free(desired);
                return active->component->base_version.mca_component_name;
            }
        }
    }
    opal_argv_free(desired);

    /* get here if none match */
    return NULL;
}

int orte_routed_base_delete_route(char *module, orte_process_name_t *proc)
{
    orte_routed_base_active_t *active;
    int rc;

    OPAL_LIST_FOREACH(active, &orte_routed_base.actives, orte_routed_base_active_t) {
        if (NULL == module ||
            0 == strcmp(module, active->component->base_version.mca_component_name)) {
            if (NULL != active->module->delete_route) {
                if (ORTE_SUCCESS != (rc = active->module->delete_route(proc))) {
                    return rc;
                }
            }
        }
    }
    return ORTE_SUCCESS;
}

int orte_routed_base_update_route(char *module, orte_process_name_t *target,
                                  orte_process_name_t *route)
{
    orte_routed_base_active_t *active;
    int rc;

    OPAL_LIST_FOREACH(active, &orte_routed_base.actives, orte_routed_base_active_t) {
        if (NULL == module ||
            0 == strcmp(module, active->component->base_version.mca_component_name)) {
            if (NULL != active->module->update_route) {
                if (ORTE_SUCCESS != (rc = active->module->update_route(target, route))) {
                    return rc;
                }
            }
        }
    }
    return ORTE_SUCCESS;
}

orte_process_name_t orte_routed_base_get_route(char *module, orte_process_name_t *target)
{
    orte_routed_base_active_t *active;

    /* a NULL module corresponds to direct */
    if (!orte_routed_base.routing_enabled || NULL == module) {
        return *target;
    }

    OPAL_LIST_FOREACH(active, &orte_routed_base.actives, orte_routed_base_active_t) {
        if (0 == strcmp(module, active->component->base_version.mca_component_name)) {
            if (NULL != active->module->get_route) {
                return active->module->get_route(target);
            }
            return *ORTE_NAME_INVALID;
        }
    }
    return *ORTE_NAME_INVALID;
}

int orte_routed_base_route_lost(char *module, const orte_process_name_t *route)
{
    orte_routed_base_active_t *active;
    int rc;

    OPAL_LIST_FOREACH(active, &orte_routed_base.actives, orte_routed_base_active_t) {
        if (NULL == module ||
            0 == strcmp(module, active->component->base_version.mca_component_name)) {
            if (NULL != active->module->route_lost) {
                if (ORTE_SUCCESS != (rc = active->module->route_lost(route))) {
                    return rc;
                }
            }
        }
    }
    return ORTE_SUCCESS;
}

bool orte_routed_base_route_is_defined(char *module, const orte_process_name_t *target)
{
    orte_routed_base_active_t *active;

    /* a NULL module corresponds to direct */
    if (NULL == module) {
        return true;
    }

    OPAL_LIST_FOREACH(active, &orte_routed_base.actives, orte_routed_base_active_t) {
        if (0 == strcmp(module, active->component->base_version.mca_component_name)) {
            if (NULL != active->module->route_is_defined) {
                return active->module->route_is_defined(target);
            }
            break;
        }
    }

    /* if we didn't find the specified module, or it doesn't have
     * the required API, then the route isn't defined */
    return false;
}

void orte_routed_base_update_routing_plan(char *module)
{
    orte_routed_base_active_t *active;

    OPAL_LIST_FOREACH(active, &orte_routed_base.actives, orte_routed_base_active_t) {
        if (NULL == module ||
            0 == strcmp(module, active->component->base_version.mca_component_name)) {
            if (NULL != active->module->update_routing_plan) {
                active->module->update_routing_plan();
            }
        }
    }

    return;
}

void orte_routed_base_get_routing_list(char *module, opal_list_t *coll)
{
    orte_routed_base_active_t *active;

    OPAL_LIST_FOREACH(active, &orte_routed_base.actives, orte_routed_base_active_t) {
        if (NULL == module ||
            0 == strcmp(module, active->component->base_version.mca_component_name)) {
            if (NULL != active->module->get_routing_list) {
                active->module->get_routing_list(coll);
            }
        }
    }
    return;
}

int orte_routed_base_set_lifeline(char *module, orte_process_name_t *proc)
{
    orte_routed_base_active_t *active;
    int rc;

    OPAL_LIST_FOREACH(active, &orte_routed_base.actives, orte_routed_base_active_t) {
        if (NULL == module ||
            0 == strcmp(module, active->component->base_version.mca_component_name)) {
            if (NULL != active->module->set_lifeline) {
                if (ORTE_SUCCESS != (rc = active->module->set_lifeline(proc))) {
                    return rc;
                }
            }
        }
    }
    return ORTE_SUCCESS;
}

size_t orte_routed_base_num_routes(char *module)
{
    orte_routed_base_active_t *active;
    size_t rc = 0;

    OPAL_LIST_FOREACH(active, &orte_routed_base.actives, orte_routed_base_active_t) {
        if (NULL == module ||
            0 == strcmp(module, active->component->base_version.mca_component_name)) {
            if (NULL != active->module->num_routes) {
                rc += active->module->num_routes();
            }
        }
    }
    return rc;
}

int orte_routed_base_ft_event(char *module, int state)
{
    orte_routed_base_active_t *active;
    int rc;

    OPAL_LIST_FOREACH(active, &orte_routed_base.actives, orte_routed_base_active_t) {
        if (NULL == module ||
            0 == strcmp(module, active->component->base_version.mca_component_name)) {
            if (NULL != active->module->ft_event) {
                if (ORTE_SUCCESS != (rc = active->module->ft_event(state))) {
                    return rc;
                }
            }
        }
    }
    return ORTE_SUCCESS;
}


void orte_routed_base_xcast_routing(opal_list_t *coll, opal_list_t *my_children)
{
    orte_routed_tree_t *child;
    orte_namelist_t *nm;
    int i;
    orte_proc_t *proc;
    orte_job_t *daemons;

    /* if we are the HNP and an abnormal termination is underway,
     * then send it directly to everyone
     */
    if (ORTE_PROC_IS_HNP) {
        if (orte_abnormal_term_ordered || !orte_routing_is_enabled) {
            daemons = orte_get_job_data_object(ORTE_PROC_MY_NAME->jobid);
            for (i=1; i < daemons->procs->size; i++) {
                if (NULL == (proc = (orte_proc_t*)opal_pointer_array_get_item(daemons->procs, i))) {
                    continue;
                }
                /* exclude anyone known not alive */
                if (ORTE_FLAG_TEST(proc, ORTE_PROC_FLAG_ALIVE)) {
                    nm = OBJ_NEW(orte_namelist_t);
                    nm->name.jobid = ORTE_PROC_MY_NAME->jobid;
                    nm->name.vpid = proc->name.vpid;
                    opal_list_append(coll, &nm->super);
                }
            }
            /* if nobody is known alive, then we need to die */
            if (0 == opal_list_get_size(coll)) {
                ORTE_ACTIVATE_JOB_STATE(NULL, ORTE_JOB_STATE_DAEMONS_TERMINATED);
            }
        } else {
            /* the xcast always goes to our children */
            OPAL_LIST_FOREACH(child, my_children, orte_routed_tree_t) {
                nm = OBJ_NEW(orte_namelist_t);
                nm->name.jobid = ORTE_PROC_MY_NAME->jobid;
                nm->name.vpid = child->vpid;
                opal_list_append(coll, &nm->super);
            }
        }
    } else {
        /* I am a daemon - route to my children */
        OPAL_LIST_FOREACH(child, my_children, orte_routed_tree_t) {
            nm = OBJ_NEW(orte_namelist_t);
            nm->name.jobid = ORTE_PROC_MY_NAME->jobid;
            nm->name.vpid = child->vpid;
            opal_list_append(coll, &nm->super);
        }
    }
}

int orte_routed_base_process_callback(orte_jobid_t job, opal_buffer_t *buffer)
{
    orte_proc_t *proc;
    orte_job_t *jdata;
    orte_std_cntr_t cnt;
    char *rml_uri;
    orte_vpid_t vpid;
    int rc;

    /* lookup the job object for this process */
    if (NULL == (jdata = orte_get_job_data_object(job))) {
        /* came from a different job family - this is an error */
        ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
        return ORTE_ERR_NOT_FOUND;
    }

    /* unpack the data for each entry */
    cnt = 1;
    while (ORTE_SUCCESS == (rc = opal_dss.unpack(buffer, &vpid, &cnt, ORTE_VPID))) {

        if (ORTE_SUCCESS != (rc = opal_dss.unpack(buffer, &rml_uri, &cnt, OPAL_STRING))) {
            ORTE_ERROR_LOG(rc);
            continue;
        }

        OPAL_OUTPUT_VERBOSE((2, orte_routed_base_framework.framework_output,
                             "%s routed_base:callback got uri %s for job %s rank %s",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             (NULL == rml_uri) ? "NULL" : rml_uri,
                             ORTE_JOBID_PRINT(job), ORTE_VPID_PRINT(vpid)));

        if (NULL == rml_uri) {
            /* should not happen */
            ORTE_ERROR_LOG(ORTE_ERR_FATAL);
            return ORTE_ERR_FATAL;
        }

        if (NULL == (proc = (orte_proc_t*)opal_pointer_array_get_item(jdata->procs, vpid))) {
            ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
            continue;
        }

        /* update the record */
        proc->rml_uri = strdup(rml_uri);
        free(rml_uri);

        cnt = 1;
    }
    if (ORTE_ERR_UNPACK_READ_PAST_END_OF_BUFFER != rc) {
        ORTE_ERROR_LOG(rc);
        return rc;
    }

    return ORTE_SUCCESS;
}
