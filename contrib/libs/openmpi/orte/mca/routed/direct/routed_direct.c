/*
 * Copyright (c) 2007-2011 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2004-2011 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2014-2017 Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "orte_config.h"
#include "orte/constants.h"

#include "opal/dss/dss.h"
#include "opal/util/output.h"

#include "orte/mca/errmgr/errmgr.h"
#include "orte/mca/rml/rml.h"
#include "orte/util/name_fns.h"
#include "orte/util/proc_info.h"
#include "orte/runtime/orte_globals.h"
#include "orte/runtime/data_type_support/orte_dt_support.h"
#include "orte/runtime/orte_wait.h"

#include "orte/mca/rml/base/rml_contact.h"

#include "orte/mca/routed/base/base.h"
#include "routed_direct.h"

static int init(void);
static int finalize(void);
static int delete_route(orte_process_name_t *proc);
static int update_route(orte_process_name_t *target,
                        orte_process_name_t *route);
static orte_process_name_t get_route(orte_process_name_t *target);
static int route_lost(const orte_process_name_t *route);
static bool route_is_defined(const orte_process_name_t *target);
static void update_routing_plan(void);
static void get_routing_list(opal_list_t *coll);
static int set_lifeline(orte_process_name_t *proc);
static size_t num_routes(void);

#if OPAL_ENABLE_FT_CR == 1
static int direct_ft_event(int state);
#endif

orte_routed_module_t orte_routed_direct_module = {
    .initialize = init,
    .finalize = finalize,
    .delete_route = delete_route,
    .update_route = update_route,
    .get_route = get_route,
    .route_lost = route_lost,
    .route_is_defined = route_is_defined,
    .set_lifeline = set_lifeline,
    .update_routing_plan = update_routing_plan,
    .get_routing_list = get_routing_list,
    .num_routes = num_routes,
#if OPAL_ENABLE_FT_CR == 1
    .ft_event = direct_ft_event
#else
    NULL
#endif
};

static orte_process_name_t mylifeline;
static orte_process_name_t *lifeline = NULL;
static opal_list_t my_children;

static int init(void)
{
    lifeline = NULL;

    if (ORTE_PROC_IS_DAEMON) {
        ORTE_PROC_MY_PARENT->jobid = ORTE_PROC_MY_NAME->jobid;
        /* if we are using static ports, set my lifeline to point at my parent */
        if (orte_static_ports) {
            /* we will have been given our parent's vpid by MCA param */
            lifeline = ORTE_PROC_MY_PARENT;
        } else {
            /* set our lifeline to the HNP - we will abort if that connection is lost */
            lifeline = ORTE_PROC_MY_HNP;
            ORTE_PROC_MY_PARENT->vpid = 0;
        }
    } else if (ORTE_PROC_IS_APP) {
        /* if we don't have a designated daemon, just
         * disqualify ourselves */
        if (NULL == orte_process_info.my_daemon_uri) {
            return ORTE_ERR_TAKE_NEXT_OPTION;
        }
        /* set our lifeline to the local daemon - we will abort if this connection is lost */
        lifeline = ORTE_PROC_MY_DAEMON;
        orte_routing_is_enabled = true;
    }

    /* setup the list of children */
    OBJ_CONSTRUCT(&my_children, opal_list_t);

    return ORTE_SUCCESS;
}

static int finalize(void)
{
    OPAL_LIST_DESTRUCT(&my_children);
    return ORTE_SUCCESS;
}

static int delete_route(orte_process_name_t *proc)
{
    OPAL_OUTPUT_VERBOSE((1, orte_routed_base_framework.framework_output,
                         "%s routed_direct_delete_route for %s",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         ORTE_NAME_PRINT(proc)));

    /*There is nothing to do here */

    return ORTE_SUCCESS;
}

static int update_route(orte_process_name_t *target,
                        orte_process_name_t *route)
{
    OPAL_OUTPUT_VERBOSE((1, orte_routed_base_framework.framework_output,
                         "%s routed_direct_update: %s --> %s",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         ORTE_NAME_PRINT(target),
                         ORTE_NAME_PRINT(route)));

    /*There is nothing to do here */

    return ORTE_SUCCESS;
}


static orte_process_name_t get_route(orte_process_name_t *target)
{
    orte_process_name_t *ret, daemon;

    if (target->jobid == ORTE_JOBID_INVALID ||
        target->vpid == ORTE_VPID_INVALID) {
        ret = ORTE_NAME_INVALID;
        goto found;
    }

    /* initialize */
    daemon.jobid = ORTE_PROC_MY_DAEMON->jobid;
    daemon.vpid = ORTE_PROC_MY_DAEMON->vpid;

    if (ORTE_PROC_IS_APP) {
        /* if I am an application, AND I have knowledge of
         * my daemon (i.e., a daemon launched me), then I
         * always route thru the daemon */
        if (NULL != orte_process_info.my_daemon_uri) {
            ret = ORTE_PROC_MY_DAEMON;
        } else {
            /* I was direct launched and do not have
             * a daemon, so I have to route direct */
            ret = target;
        }
        goto found;
    }

    /* if I am a tool, the route is direct if target is in
     * my own job family, and to the target's HNP if not
     */
    if (ORTE_PROC_IS_TOOL) {
        if (ORTE_JOB_FAMILY(target->jobid) == ORTE_JOB_FAMILY(ORTE_PROC_MY_NAME->jobid)) {
            ret = target;
            goto found;
        } else {
            ORTE_HNP_NAME_FROM_JOB(&daemon, target->jobid);
            ret = &daemon;
            goto found;
        }
    }

    /******     HNP AND DAEMONS ONLY     ******/
    if (OPAL_EQUAL == orte_util_compare_name_fields(ORTE_NS_CMP_ALL, ORTE_PROC_MY_HNP, target)) {
        OPAL_OUTPUT_VERBOSE((2, orte_routed_base_framework.framework_output,
                    "%s routing direct to the HNP",
                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));
        ret = ORTE_PROC_MY_HNP;
        goto found;
    }

    daemon.jobid = ORTE_PROC_MY_NAME->jobid;
    /* find out what daemon hosts this proc */
    if (ORTE_VPID_INVALID == (daemon.vpid = orte_get_proc_daemon_vpid(target))) {
        ret = ORTE_NAME_INVALID;
        goto found;
    }

    /* if the daemon is me, then send direct to the target! */
    if (ORTE_PROC_MY_NAME->vpid == daemon.vpid) {
        ret = target;
        goto found;
    }

    /* else route to this daemon directly */
    ret = &daemon;

 found:
    OPAL_OUTPUT_VERBOSE((2, orte_routed_base_framework.framework_output,
                         "%s routed_direct_get(%s) --> %s",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         ORTE_NAME_PRINT(target),
                         ORTE_NAME_PRINT(ret)));

    return *ret;
}

static int route_lost(const orte_process_name_t *route)
{
    opal_list_item_t *item;
    orte_routed_tree_t *child;

    OPAL_OUTPUT_VERBOSE((2, orte_routed_base_framework.framework_output,
                         "%s route to %s lost",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         ORTE_NAME_PRINT(route)));

    /* if we lose the connection to the lifeline and we are NOT already,
     * in finalize, tell the OOB to abort.
     * NOTE: we cannot call abort from here as the OOB needs to first
     * release a thread-lock - otherwise, we will hang!!
     */
    if (!orte_finalizing &&
        NULL != lifeline &&
        OPAL_EQUAL == orte_util_compare_name_fields(ORTE_NS_CMP_ALL, route, lifeline)) {
        OPAL_OUTPUT_VERBOSE((2, orte_routed_base_framework.framework_output,
                             "%s routed:direct: Connection to lifeline %s lost",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             ORTE_NAME_PRINT(lifeline)));
        return ORTE_ERR_FATAL;
    }

    /* if we are the HNP, and the route is a daemon,
     * see if it is one of our children - if so, remove it
     */
    if (ORTE_PROC_IS_HNP &&
        route->jobid == ORTE_PROC_MY_NAME->jobid) {
        for (item = opal_list_get_first(&my_children);
             item != opal_list_get_end(&my_children);
             item = opal_list_get_next(item)) {
            child = (orte_routed_tree_t*)item;
            if (child->vpid == route->vpid) {
                opal_list_remove_item(&my_children, item);
                OBJ_RELEASE(item);
                return ORTE_SUCCESS;
            }
        }
    }

    /* we don't care about this one, so return success */
    return ORTE_SUCCESS;
}


static bool route_is_defined(const orte_process_name_t *target)
{
    /* all routes are defined */
    return true;
}

static int set_lifeline(orte_process_name_t *proc)
{
    OPAL_OUTPUT_VERBOSE((2, orte_routed_base_framework.framework_output,
                         "%s routed:direct: set lifeline to %s",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         ORTE_NAME_PRINT(proc)));
    mylifeline = *proc;
    lifeline = &mylifeline;
    return ORTE_SUCCESS;
}

static void update_routing_plan(void)
{
    orte_routed_tree_t *child;
    int32_t i;
    orte_job_t *jdata;
    orte_proc_t *proc;

    OPAL_OUTPUT_VERBOSE((2, orte_routed_base_framework.framework_output,
                         "%s routed:direct: update routing plan",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));

    if (!ORTE_PROC_IS_HNP) {
        /* nothing to do */
        return;
    }

    /* clear the current list */
    OPAL_LIST_DESTRUCT(&my_children);
    OBJ_CONSTRUCT(&my_children, opal_list_t);

    /* HNP is directly connected to each daemon */
    if (NULL == (jdata = orte_get_job_data_object(ORTE_PROC_MY_NAME->jobid))) {
        ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
        return;
    }
    for (i=1; i < jdata->procs->size; i++) {
        if (NULL == (proc = (orte_proc_t*)opal_pointer_array_get_item(jdata->procs, i))) {
            continue;
        }
        child = OBJ_NEW(orte_routed_tree_t);
        child->vpid = proc->name.vpid;
        opal_list_append(&my_children, &child->super);
    }

    return;
}

static void get_routing_list(opal_list_t *coll)
{

    OPAL_OUTPUT_VERBOSE((2, orte_routed_base_framework.framework_output,
                         "%s routed:direct: get routing list",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));

    /* if I am anything other than daemons and the HNP, this
     * is a meaningless command as I am not allowed to route
     */
    if (!ORTE_PROC_IS_DAEMON && !ORTE_PROC_IS_HNP) {
        return;
    }

    orte_routed_base_xcast_routing(coll, &my_children);
}

static size_t num_routes(void)
{
    if (!ORTE_PROC_IS_HNP) {
        return 0;
    }
    return opal_list_get_size(&my_children);
}

#if OPAL_ENABLE_FT_CR == 1
static int direct_ft_event(int state)
{
    int ret, exit_status = ORTE_SUCCESS;

    /******** Checkpoint Prep ********/
    if(OPAL_CRS_CHECKPOINT == state) {
    }
    /******** Continue Recovery ********/
    else if (OPAL_CRS_CONTINUE == state ) {
    }
    else if (OPAL_CRS_TERM == state ) {
        /* Nothing */
    }
    else {
        /* Error state = Nothing */
    }

 cleanup:
    return exit_status;
}
#endif
