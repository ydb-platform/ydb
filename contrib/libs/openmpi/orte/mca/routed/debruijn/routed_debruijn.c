/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2007-2012 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2004-2011 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2013-2016 Intel, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "orte_config.h"
#include "orte/constants.h"

#include <stddef.h>

#include "opal/dss/dss.h"
#include "opal/class/opal_hash_table.h"
#include "opal/class/opal_bitmap.h"
#include "opal/util/output.h"

#include "orte/mca/errmgr/errmgr.h"
#include "orte/mca/ess/ess.h"
#include "orte/mca/rml/rml.h"
#include "orte/mca/rml/rml_types.h"
#include "orte/util/name_fns.h"
#include "orte/runtime/orte_globals.h"
#include "orte/runtime/orte_wait.h"
#include "orte/runtime/runtime.h"
#include "orte/runtime/data_type_support/orte_dt_support.h"

#include "orte/mca/rml/base/rml_contact.h"

#include "orte/mca/routed/base/base.h"
#include "routed_debruijn.h"


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
static int debruijn_ft_event(int state);
#endif

orte_routed_module_t orte_routed_debruijn_module = {
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
    .ft_event = debruijn_ft_event
#else
    NULL
#endif
};

/* local globals */
static orte_process_name_t      *lifeline=NULL;
static orte_process_name_t      local_lifeline;
static opal_list_t              my_children;
static bool                     hnp_direct=true;
static int                      log_nranks;
static int                      log_npeers;
static unsigned int             rank_mask;

static int init(void)
{
    lifeline = NULL;

    if (ORTE_PROC_IS_DAEMON) {
        /* if we are using static ports, set my lifeline to point at my parent */
        if (orte_static_ports) {
            lifeline = ORTE_PROC_MY_PARENT;
        } else {
            /* set our lifeline to the HNP - we will abort if that connection is lost */
            lifeline = ORTE_PROC_MY_HNP;
        }
        ORTE_PROC_MY_PARENT->jobid = ORTE_PROC_MY_NAME->jobid;
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
    opal_list_item_t *item;

    lifeline = NULL;

    /* deconstruct the list of children */
    while (NULL != (item = opal_list_remove_first(&my_children))) {
        OBJ_RELEASE(item);
    }
    OBJ_DESTRUCT(&my_children);

    return ORTE_SUCCESS;
}

static int delete_route(orte_process_name_t *proc)
{
    if (proc->jobid == ORTE_JOBID_INVALID ||
        proc->vpid == ORTE_VPID_INVALID) {
        return ORTE_ERR_BAD_PARAM;
    }

    /* if I am an application process, I don't have any routes
     * so there is nothing for me to do
     */
    if (!ORTE_PROC_IS_HNP && !ORTE_PROC_IS_DAEMON &&
        !ORTE_PROC_IS_TOOL) {
        return ORTE_SUCCESS;
    }

    OPAL_OUTPUT_VERBOSE((1, orte_routed_base_framework.framework_output,
                         "%s routed_debruijn_delete_route for %s",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         ORTE_NAME_PRINT(proc)));

    /* THIS CAME FROM OUR OWN JOB FAMILY...there is nothing
     * to do here. The routes will be redefined when we update
     * the routing tree
     */

    return ORTE_SUCCESS;
}

static int update_route(orte_process_name_t *target,
                        orte_process_name_t *route)
{
    if (target->jobid == ORTE_JOBID_INVALID ||
        target->vpid == ORTE_VPID_INVALID) {
        return ORTE_ERR_BAD_PARAM;
    }

    /* if I am an application process, we don't update the route since
     * we automatically route everything through the local daemon
     */
    if (ORTE_PROC_IS_APP) {
        return ORTE_SUCCESS;
    }

    OPAL_OUTPUT_VERBOSE((1, orte_routed_base_framework.framework_output,
                         "%s routed_debruijn_update: %s --> %s",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         ORTE_NAME_PRINT(target),
                         ORTE_NAME_PRINT(route)));


    /* if I am a daemon and the target is my HNP, then check
     * the route - if it isn't direct, then we just flag that
     * we have a route to the HNP
     */
    if (OPAL_EQUAL == orte_util_compare_name_fields(ORTE_NS_CMP_ALL, ORTE_PROC_MY_HNP, target) &&
        OPAL_EQUAL != orte_util_compare_name_fields(ORTE_NS_CMP_ALL, ORTE_PROC_MY_HNP, route)) {
        hnp_direct = false;
        return ORTE_SUCCESS;
    }

    return ORTE_SUCCESS;
}

static inline unsigned int debruijn_next_hop (int target)
{
    const int my_id = ORTE_PROC_MY_NAME->vpid;
    uint64_t route, mask = rank_mask;
    unsigned int i, next_hop;

    if (target == my_id) {
        return my_id;
    }

    i = -log_npeers;
    do {
        i += log_npeers;
        mask = (mask >> i) << i;
        route = (my_id << i) | target;
    } while ((route & mask) != (((my_id << i) & target) & mask));

    next_hop = (int)((route >> (i - log_npeers)) & rank_mask);

    /* if the next hop does not exist route to the lowest proc with the same lower routing bits */
    return (next_hop < orte_process_info.num_procs) ? next_hop : (next_hop & (rank_mask >> log_npeers));
}

static orte_process_name_t get_route(orte_process_name_t *target)
{
    orte_process_name_t ret;

    /* initialize */

    do {
        ret = *ORTE_NAME_INVALID;

        if (ORTE_JOBID_INVALID == target->jobid ||
            ORTE_VPID_INVALID == target->vpid) {
            break;
        }

        /* if it is me, then the route is just direct */
        if (OPAL_EQUAL == opal_dss.compare(ORTE_PROC_MY_NAME, target, ORTE_NAME)) {
            ret = *target;
            break;
        }

        /* if I am an application process, always route via my local daemon */
        if (ORTE_PROC_IS_APP) {
            ret = *ORTE_PROC_MY_DAEMON;
            break;
        }

        /* if I am a tool, the route is direct if target is in
         * my own job family, and to the target's HNP if not
         */
        if (ORTE_PROC_IS_TOOL) {
            if (ORTE_JOB_FAMILY(target->jobid) == ORTE_JOB_FAMILY(ORTE_PROC_MY_NAME->jobid)) {
                ret = *target;
            } else {
                ORTE_HNP_NAME_FROM_JOB(&ret, target->jobid);
            }

            break;
        }

        /******     HNP AND DAEMONS ONLY     ******/

        if (OPAL_EQUAL == orte_util_compare_name_fields(ORTE_NS_CMP_ALL, ORTE_PROC_MY_HNP, target)) {
            if (!hnp_direct || orte_static_ports) {
                OPAL_OUTPUT_VERBOSE((2, orte_routed_base_framework.framework_output,
                                     "%s routing to the HNP through my parent %s",
                                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                     ORTE_NAME_PRINT(ORTE_PROC_MY_PARENT)));
                ret = *ORTE_PROC_MY_PARENT;
            } else {
                OPAL_OUTPUT_VERBOSE((2, orte_routed_base_framework.framework_output,
                                     "%s routing direct to the HNP",
                                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));
                ret = *ORTE_PROC_MY_HNP;
            }

            break;
        }

        ret.jobid = ORTE_PROC_MY_NAME->jobid;
        /* find out what daemon hosts this proc */
        if (ORTE_VPID_INVALID == (ret.vpid = orte_get_proc_daemon_vpid(target))) {
            /* we don't yet know about this daemon. just route this to the "parent" */
            ret = *ORTE_PROC_MY_PARENT;
            break;
        }

        /* if the daemon is me, then send direct to the target! */
        if (ORTE_PROC_MY_NAME->vpid == ret.vpid) {
            ret = *target;
            break;
        }

        /* find next hop */
        ret.vpid = debruijn_next_hop (ret.vpid);
    } while (0);

    OPAL_OUTPUT_VERBOSE((1, orte_routed_base_framework.framework_output,
                         "%s routed_debruijn_get(%s) --> %s",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         ORTE_NAME_PRINT(target),
                         ORTE_NAME_PRINT(&ret)));

    return ret;
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
                             "%s routed:debruijn: Connection to lifeline %s lost",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             ORTE_NAME_PRINT(lifeline)));
        return ORTE_ERR_FATAL;
    }

    /* if we are the HNP or daemon, and the route is a daemon,
     * see if it is one of our children - if so, remove it
     */
    if ((ORTE_PROC_IS_DAEMON || ORTE_PROC_IS_HNP) &&
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
    /* find out what daemon hosts this proc */
    if (ORTE_VPID_INVALID == orte_get_proc_daemon_vpid((orte_process_name_t*)target)) {
        return false;
    }

    return true;
}

static int set_lifeline(orte_process_name_t *proc)
{
    /* we have to copy the proc data because there is no
     * guarantee that it will be preserved
     */
    local_lifeline.jobid = proc->jobid;
    local_lifeline.vpid = proc->vpid;
    lifeline = &local_lifeline;

    return ORTE_SUCCESS;
}

static unsigned int ilog2 (unsigned int v)
{
    const unsigned int b[] = {0x2, 0xC, 0xF0, 0xFF00, 0xFFFF0000};
    const unsigned int S[] = {1, 2, 4, 8, 16};
    int i;

    register unsigned int r = 0;
    for (i = 4; i >= 0; i--) {
        if (v & b[i]) {
            v >>= S[i];
            r |= S[i];
        }
    }

    return r;
}

static void update_routing_plan(void)
{
    orte_routed_tree_t *child;
    opal_list_item_t *item;
    int my_vpid = ORTE_PROC_MY_NAME->vpid;
    int i;

    /* if I am anything other than a daemon or the HNP, this
     * is a meaningless command as I am not allowed to route
     */
    if (!ORTE_PROC_IS_DAEMON && !ORTE_PROC_IS_HNP) {
        return;
    }

    /* clear the list of children if any are already present */
    while (NULL != (item = opal_list_remove_first(&my_children))) {
        OBJ_RELEASE(item);
    }

    log_nranks = (int) ilog2 ((unsigned int)orte_process_info.num_procs) ;
    assert(log_nranks < 31);

    if (log_nranks < 3) {
      log_npeers = 1;
    } else if (log_nranks < 7) {
      log_npeers = 2;
    } else {
      log_npeers = 4;
    }

    /* round log_nranks to a multiple of log_npeers */
    log_nranks = ((log_nranks + log_npeers) & ~(log_npeers - 1)) - 1;

    rank_mask = (1 << (log_nranks + 1)) - 1;

    /* compute my parent */
    ORTE_PROC_MY_PARENT->vpid = my_vpid ? my_vpid >> log_npeers : -1;

    /* only add peers to the routing tree if this rank is the smallest rank that will send to
       the any peer */
    if ((my_vpid >> (log_nranks + 1 - log_npeers)) == 0) {
        for (i = (1 << log_npeers) - 1 ; i >= 0 ; --i) {
            int next = ((my_vpid << log_npeers) | i) & rank_mask;

            /* add a peer to the routing tree only if its vpid is smaller than this rank */
            if (next > my_vpid && next < (int)orte_process_info.num_procs) {
                child = OBJ_NEW(orte_routed_tree_t);
                child->vpid = next;
                opal_list_append (&my_children, &child->super);
            }
        }
    }
}

static void get_routing_list(opal_list_t *coll)
{
    /* if I am anything other than a daemon or the HNP, this
     * is a meaningless command as I am not allowed to route
     */
    if (!ORTE_PROC_IS_DAEMON && !ORTE_PROC_IS_HNP) {
        return;
    }

    orte_routed_base_xcast_routing(coll, &my_children);
}

static size_t num_routes(void)
{
    return opal_list_get_size(&my_children);
}

#if OPAL_ENABLE_FT_CR == 1
static int debruijn_ft_event(int state)
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

