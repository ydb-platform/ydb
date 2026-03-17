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
 * Copyright (c) 2011-2012 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2014-2018 Intel, Inc. All rights reserved.
 * Copyright (c) 2018      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "orte_config.h"

#include <string.h>

#include "orte/constants.h"
#include "orte/types.h"

#include "orte/mca/mca.h"
#include "opal/mca/base/base.h"
#include "opal/class/opal_list.h"
#include "opal/util/output.h"
#include "opal/dss/dss.h"
#include "opal/util/argv.h"
#include "opal/mca/if/if.h"

#include "orte/util/show_help.h"
#include "orte/mca/errmgr/errmgr.h"
#include "orte/mca/rmaps/base/base.h"
#include "orte/util/name_fns.h"
#include "orte/runtime/orte_globals.h"
#include "orte/runtime/orte_wait.h"
#include "orte/util/hostfile/hostfile.h"
#include "orte/util/dash_host/dash_host.h"
#include "orte/util/proc_info.h"
#include "orte/util/comm/comm.h"
#include "orte/util/error_strings.h"
#include "orte/util/threads.h"
#include "orte/mca/state/state.h"
#include "orte/runtime/orte_quit.h"

#include "orte/mca/ras/base/ras_private.h"

/* function to display allocation */
void orte_ras_base_display_alloc(void)
{
    char *tmp=NULL, *tmp2, *tmp3;
    int i, istart;
    orte_node_t *alloc;

    if (orte_xml_output) {
        asprintf(&tmp, "<allocation>\n");
    } else {
        asprintf(&tmp, "\n======================   ALLOCATED NODES   ======================\n");
    }
    if (orte_hnp_is_allocated) {
            istart = 0;
    } else {
        istart = 1;
    }
    for (i=istart; i < orte_node_pool->size; i++) {
        if (NULL == (alloc = (orte_node_t*)opal_pointer_array_get_item(orte_node_pool, i))) {
            continue;
        }
        if (orte_xml_output) {
            /* need to create the output in XML format */
            asprintf(&tmp2, "\t<host name=\"%s\" slots=\"%d\" max_slots=\"%d\" slots_inuse=\"%d\">\n",
                     (NULL == alloc->name) ? "UNKNOWN" : alloc->name,
                     (int)alloc->slots, (int)alloc->slots_max, (int)alloc->slots_inuse);
        } else {
            asprintf(&tmp2, "\t%s: flags=0x%02x slots=%d max_slots=%d slots_inuse=%d state=%s\n",
                     (NULL == alloc->name) ? "UNKNOWN" : alloc->name, alloc->flags,
                     (int)alloc->slots, (int)alloc->slots_max, (int)alloc->slots_inuse,
                     orte_node_state_to_str(alloc->state));
        }
        if (NULL == tmp) {
            tmp = tmp2;
        } else {
            asprintf(&tmp3, "%s%s", tmp, tmp2);
            free(tmp);
            free(tmp2);
            tmp = tmp3;
        }
    }
    if (orte_xml_output) {
        fprintf(orte_xml_fp, "%s</allocation>\n", tmp);
        fflush(orte_xml_fp);
    } else {
        opal_output(orte_clean_output, "%s=================================================================\n", tmp);
    }
    free(tmp);
}

/*
 * Function for selecting one component from all those that are
 * available.
 */
void orte_ras_base_allocate(int fd, short args, void *cbdata)
{
    int rc;
    orte_job_t *jdata;
    opal_list_t nodes;
    orte_node_t *node;
    orte_std_cntr_t i;
    orte_app_context_t *app;
    orte_state_caddy_t *caddy = (orte_state_caddy_t*)cbdata;
    char *hosts=NULL;

    ORTE_ACQUIRE_OBJECT(caddy);

    OPAL_OUTPUT_VERBOSE((5, orte_ras_base_framework.framework_output,
                         "%s ras:base:allocate",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));

    /* convenience */
    jdata = caddy->jdata;

    /* if we already did this, don't do it again - the pool of
     * global resources is set.
     */
    if (orte_ras_base.allocation_read) {

        OPAL_OUTPUT_VERBOSE((5, orte_ras_base_framework.framework_output,
                             "%s ras:base:allocate allocation already read",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));
        goto next_state;
    }
    orte_ras_base.allocation_read = true;

    /* Otherwise, we have to create
     * the initial set of resources that will delineate all
     * further operations serviced by this HNP. This list will
     * contain ALL nodes that can be used by any subsequent job.
     *
     * In other words, if a node isn't found in this step, then
     * no job launched by this HNP will be able to utilize it.
     */

    /* construct a list to hold the results */
    OBJ_CONSTRUCT(&nodes, opal_list_t);

    /* if a component was selected, then we know we are in a managed
     * environment.  - the active module will return a list of what it found
     */
    if (NULL != orte_ras_base.active_module)  {
        /* read the allocation */
        if (ORTE_SUCCESS != (rc = orte_ras_base.active_module->allocate(jdata, &nodes))) {
            if (ORTE_ERR_ALLOCATION_PENDING == rc) {
                /* an allocation request is underway, so just do nothing */
                OBJ_DESTRUCT(&nodes);
                OBJ_RELEASE(caddy);
                return;
            }
            if (ORTE_ERR_SYSTEM_WILL_BOOTSTRAP == rc) {
                /* this module indicates that nodes will be discovered
                 * on a bootstrap basis, so all we do here is add our
                 * own node to the list
                 */
                goto addlocal;
            }
            if (ORTE_ERR_TAKE_NEXT_OPTION == rc) {
                /* we have an active module, but it is unable to
                 * allocate anything for this job - this indicates
                 * that it isn't a fatal error, but could be if
                 * an allocation is required
                 */
                if (orte_allocation_required) {
                    /* an allocation is required, so this is fatal */
                    OBJ_DESTRUCT(&nodes);
                    orte_show_help("help-ras-base.txt", "ras-base:no-allocation", true);
                    ORTE_FORCED_TERMINATE(ORTE_ERROR_DEFAULT_EXIT_CODE);
                    OBJ_RELEASE(caddy);
                    return;
                } else {
                    /* an allocation is not required, so we can just
                     * run on the local node - go add it
                     */
                    goto addlocal;
                }
            }
            ORTE_ERROR_LOG(rc);
            OBJ_DESTRUCT(&nodes);
            ORTE_FORCED_TERMINATE(ORTE_ERROR_DEFAULT_EXIT_CODE);
            OBJ_RELEASE(caddy);
            return;
        }
    }
    /* If something came back, save it and we are done */
    if (!opal_list_is_empty(&nodes)) {
        /* flag that the allocation is managed */
        orte_managed_allocation = true;
        /* since it is managed, we do not attempt to resolve
         * the nodenames */
        opal_if_do_not_resolve = true;
        /* store the results in the global resource pool - this removes the
         * list items
         */
        if (ORTE_SUCCESS != (rc = orte_ras_base_node_insert(&nodes, jdata))) {
            ORTE_ERROR_LOG(rc);
            OBJ_DESTRUCT(&nodes);
            ORTE_FORCED_TERMINATE(ORTE_ERROR_DEFAULT_EXIT_CODE);
            OBJ_RELEASE(caddy);
            return;
        }
        OBJ_DESTRUCT(&nodes);
        goto DISPLAY;
    } else if (orte_allocation_required) {
        /* if nothing was found, and an allocation is
         * required, then error out
         */
        OBJ_DESTRUCT(&nodes);
        orte_show_help("help-ras-base.txt", "ras-base:no-allocation", true);
        ORTE_FORCED_TERMINATE(ORTE_ERROR_DEFAULT_EXIT_CODE);
        OBJ_RELEASE(caddy);
        return;
    }

    OPAL_OUTPUT_VERBOSE((5, orte_ras_base_framework.framework_output,
                         "%s ras:base:allocate nothing found in module - proceeding to hostfile",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));

    /* nothing was found, or no active module was alive. We first see
     * if we were given a rankfile - if so, use it as the hosts will be
     * taken from the mapping */
    if (NULL != orte_rankfile) {
        OPAL_OUTPUT_VERBOSE((5, orte_ras_base_framework.framework_output,
                             "%s ras:base:allocate parsing rankfile %s",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             orte_rankfile));

        /* a rankfile was provided - parse it */
        if (ORTE_SUCCESS != (rc = orte_util_add_hostfile_nodes(&nodes,
                                                               orte_rankfile))) {
            OBJ_DESTRUCT(&nodes);
            ORTE_FORCED_TERMINATE(ORTE_ERROR_DEFAULT_EXIT_CODE);
            OBJ_RELEASE(caddy);
            return;
        }
    }

    /* if something was found in the rankfile, we use that as our global
     * pool - set it and we are done
     */
    if (!opal_list_is_empty(&nodes)) {
        /* store the results in the global resource pool - this removes the
         * list items
         */
        if (ORTE_SUCCESS != (rc = orte_ras_base_node_insert(&nodes, jdata))) {
            ORTE_ERROR_LOG(rc);
            ORTE_FORCED_TERMINATE(ORTE_ERROR_DEFAULT_EXIT_CODE);
            OBJ_RELEASE(caddy);
            return;
        }
        /* rankfile is considered equivalent to an RM allocation */
        if (!(ORTE_MAPPING_SUBSCRIBE_GIVEN & ORTE_GET_MAPPING_DIRECTIVE(orte_rmaps_base.mapping))) {
            ORTE_SET_MAPPING_DIRECTIVE(orte_rmaps_base.mapping, ORTE_MAPPING_NO_OVERSUBSCRIBE);
        }
        /* cleanup */
        OBJ_DESTRUCT(&nodes);
        goto DISPLAY;
    }

    /* if a dash-host has been provided, aggregate across all the
     * app_contexts. Any hosts the user wants to add via comm_spawn
     * can be done so using the add_host option */
    for (i=0; i < jdata->apps->size; i++) {
        if (NULL == (app = (orte_app_context_t*)opal_pointer_array_get_item(jdata->apps, i))) {
            continue;
        }
        if (!orte_soft_locations &&
            orte_get_attribute(&app->attributes, ORTE_APP_DASH_HOST, (void**)&hosts, OPAL_STRING)) {
            /* if we are using soft locations, then any dash-host would
             * just include desired nodes and not required. We don't want
             * to pick them up here as this would mean the request was
             * always satisfied - instead, we want to allow the request
             * to fail later on and use whatever nodes are actually
             * available
             */
            OPAL_OUTPUT_VERBOSE((5, orte_ras_base_framework.framework_output,
                                 "%s ras:base:allocate adding dash_hosts",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));
            if (ORTE_SUCCESS != (rc = orte_util_add_dash_host_nodes(&nodes, hosts, true))) {
                free(hosts);
                OBJ_DESTRUCT(&nodes);
                ORTE_FORCED_TERMINATE(ORTE_ERROR_DEFAULT_EXIT_CODE);
                OBJ_RELEASE(caddy);
                return;
            }
            free(hosts);
        }
    }

    /* if something was found in the dash-host(s), we use that as our global
     * pool - set it and we are done
     */
    if (!opal_list_is_empty(&nodes)) {
        /* store the results in the global resource pool - this removes the
         * list items
         */
        if (ORTE_SUCCESS != (rc = orte_ras_base_node_insert(&nodes, jdata))) {
            ORTE_ERROR_LOG(rc);
            ORTE_FORCED_TERMINATE(ORTE_ERROR_DEFAULT_EXIT_CODE);
            OBJ_RELEASE(caddy);
            return;
        }
        /* cleanup */
        OBJ_DESTRUCT(&nodes);
        goto DISPLAY;
    }

    /* Our next option is to look for a hostfile and assign our global
     * pool from there.
     *
     * Individual hostfile names, if given, are included
     * in the app_contexts for this job. We therefore need to
     * retrieve the app_contexts for the job, and then cycle
     * through them to see if anything is there. The parser will
     * add the nodes found in each hostfile to our list - i.e.,
     * the resulting list contains the UNION of all nodes specified
     * in hostfiles from across all app_contexts
     *
     * Note that any relative node syntax found in the hostfiles will
     * generate an error in this scenario, so only non-relative syntax
     * can be present
     */
    for (i=0; i < jdata->apps->size; i++) {
        if (NULL == (app = (orte_app_context_t*)opal_pointer_array_get_item(jdata->apps, i))) {
            continue;
        }
        if (orte_get_attribute(&app->attributes, ORTE_APP_HOSTFILE, (void**)&hosts, OPAL_STRING)) {
            OPAL_OUTPUT_VERBOSE((5, orte_ras_base_framework.framework_output,
                                 "%s ras:base:allocate adding hostfile %s",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), hosts));

            /* hostfile was specified - parse it and add it to the list */
            if (ORTE_SUCCESS != (rc = orte_util_add_hostfile_nodes(&nodes, hosts))) {
                free(hosts);
                OBJ_DESTRUCT(&nodes);
                /* set an error event */
                ORTE_FORCED_TERMINATE(ORTE_ERROR_DEFAULT_EXIT_CODE);
                OBJ_RELEASE(caddy);
                return;
            }
            free(hosts);
        }
    }

    /* if something was found in the hostfiles(s), we use that as our global
     * pool - set it and we are done
     */
    if (!opal_list_is_empty(&nodes)) {
        /* store the results in the global resource pool - this removes the
         * list items
         */
        if (ORTE_SUCCESS != (rc = orte_ras_base_node_insert(&nodes, jdata))) {
            ORTE_ERROR_LOG(rc);
            ORTE_FORCED_TERMINATE(ORTE_ERROR_DEFAULT_EXIT_CODE);
            OBJ_RELEASE(caddy);
            return;
        }
        /* cleanup */
        OBJ_DESTRUCT(&nodes);
        goto DISPLAY;
    }

    /* if nothing was found so far, then look for a default hostfile */
    if (NULL != orte_default_hostfile) {
        OPAL_OUTPUT_VERBOSE((5, orte_ras_base_framework.framework_output,
                             "%s ras:base:allocate parsing default hostfile %s",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             orte_default_hostfile));

        /* a default hostfile was provided - parse it */
        if (ORTE_SUCCESS != (rc = orte_util_add_hostfile_nodes(&nodes,
                                                               orte_default_hostfile))) {
            OBJ_DESTRUCT(&nodes);
            ORTE_FORCED_TERMINATE(ORTE_ERROR_DEFAULT_EXIT_CODE);
            OBJ_RELEASE(caddy);
            return;
        }
    }

    /* if something was found in the default hostfile, we use that as our global
     * pool - set it and we are done
     */
    if (!opal_list_is_empty(&nodes)) {
        /* store the results in the global resource pool - this removes the
         * list items
         */
        if (ORTE_SUCCESS != (rc = orte_ras_base_node_insert(&nodes, jdata))) {
            ORTE_ERROR_LOG(rc);
            ORTE_FORCED_TERMINATE(ORTE_ERROR_DEFAULT_EXIT_CODE);
            OBJ_RELEASE(caddy);
            return;
        }
        /* cleanup */
        OBJ_DESTRUCT(&nodes);
        goto DISPLAY;
    }

    OPAL_OUTPUT_VERBOSE((5, orte_ras_base_framework.framework_output,
                         "%s ras:base:allocate nothing found in hostfiles - inserting current node",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));

  addlocal:
    /* if nothing was found by any of the above methods, then we have no
     * earthly idea what to do - so just add the local host
     */
    node = OBJ_NEW(orte_node_t);
    if (NULL == node) {
        ORTE_ERROR_LOG(ORTE_ERR_OUT_OF_RESOURCE);
        OBJ_DESTRUCT(&nodes);
        ORTE_FORCED_TERMINATE(ORTE_ERROR_DEFAULT_EXIT_CODE);
        OBJ_RELEASE(caddy);
        return;
    }
    /* use the same name we got in orte_process_info so we avoid confusion in
     * the session directories
     */
    node->name = strdup(orte_process_info.nodename);
    node->state = ORTE_NODE_STATE_UP;
    node->slots_inuse = 0;
    node->slots_max = 0;
    node->slots = 1;
    opal_list_append(&nodes, &node->super);
    /* mark the HNP as "allocated" since we have nothing else to use */
    orte_hnp_is_allocated = true;

    /* store the results in the global resource pool - this removes the
     * list items
     */
    if (ORTE_SUCCESS != (rc = orte_ras_base_node_insert(&nodes, jdata))) {
        ORTE_ERROR_LOG(rc);
        OBJ_DESTRUCT(&nodes);
        ORTE_FORCED_TERMINATE(ORTE_ERROR_DEFAULT_EXIT_CODE);
        OBJ_RELEASE(caddy);
        return;
    }
    OBJ_DESTRUCT(&nodes);

  DISPLAY:
    /* shall we display the results? */
    if (4 < opal_output_get_verbosity(orte_ras_base_framework.framework_output)) {
        orte_ras_base_display_alloc();
    }

  next_state:
    /* are we to report this event? */
    if (orte_report_events) {
        if (ORTE_SUCCESS != (rc = orte_util_comm_report_event(ORTE_COMM_EVENT_ALLOCATE))) {
            ORTE_ERROR_LOG(rc);
            ORTE_FORCED_TERMINATE(ORTE_ERROR_DEFAULT_EXIT_CODE);
            OBJ_RELEASE(caddy);
        }
    }

    /* set total slots alloc */
    jdata->total_slots_alloc = orte_ras_base.total_slots_alloc;

    /* set the job state to the next position */
    ORTE_ACTIVATE_JOB_STATE(jdata, ORTE_JOB_STATE_ALLOCATION_COMPLETE);

    /* cleanup */
    OBJ_RELEASE(caddy);
}

int orte_ras_base_add_hosts(orte_job_t *jdata)
{
    int rc;
    opal_list_t nodes;
    int i, n;
    orte_app_context_t *app;
    orte_node_t *node, *next, *nptr;
    char *hosts;

    /* construct a list to hold the results */
    OBJ_CONSTRUCT(&nodes, opal_list_t);

    /* Individual add-hostfile names, if given, are included
     * in the app_contexts for this job. We therefore need to
     * retrieve the app_contexts for the job, and then cycle
     * through them to see if anything is there. The parser will
     * add the nodes found in each add-hostfile to our list - i.e.,
     * the resulting list contains the UNION of all nodes specified
     * in add-hostfiles from across all app_contexts
     *
     * Note that any relative node syntax found in the add-hostfiles will
     * generate an error in this scenario, so only non-relative syntax
     * can be present
     */

    for (i=0; i < jdata->apps->size; i++) {
        if (NULL == (app = (orte_app_context_t*)opal_pointer_array_get_item(jdata->apps, i))) {
            continue;
        }
        if (orte_get_attribute(&app->attributes, ORTE_APP_ADD_HOSTFILE, (void**)&hosts, OPAL_STRING)) {
            OPAL_OUTPUT_VERBOSE((5, orte_ras_base_framework.framework_output,
                                 "%s ras:base:add_hosts checking add-hostfile %s",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), hosts));

            /* hostfile was specified - parse it and add it to the list */
            if (ORTE_SUCCESS != (rc = orte_util_add_hostfile_nodes(&nodes, hosts))) {
                ORTE_ERROR_LOG(rc);
                OBJ_DESTRUCT(&nodes);
                free(hosts);
                return rc;
            }
            /* now indicate that this app is to run across it */
            orte_set_attribute(&app->attributes, ORTE_APP_HOSTFILE, ORTE_ATTR_LOCAL, (void**)hosts, OPAL_STRING);
            orte_remove_attribute(&app->attributes, ORTE_APP_ADD_HOSTFILE);
            free(hosts);
        }
    }

    /* We next check for and add any add-host options. Note this is
     * a -little- different than dash-host in that (a) we add these
     * nodes to the global pool regardless of what may already be there,
     * and (b) as a result, any job and/or app_context can access them.
     *
     * Note that any relative node syntax found in the add-host lists will
     * generate an error in this scenario, so only non-relative syntax
     * can be present
     */
    for (i=0; i < jdata->apps->size; i++) {
        if (NULL == (app = (orte_app_context_t*)opal_pointer_array_get_item(jdata->apps, i))) {
            continue;
        }
        if (orte_get_attribute(&app->attributes, ORTE_APP_ADD_HOST, (void**)&hosts, OPAL_STRING)) {
            opal_output_verbose(5, orte_ras_base_framework.framework_output,
                                "%s ras:base:add_hosts checking add-host %s",
                                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), hosts);
            if (ORTE_SUCCESS != (rc = orte_util_add_dash_host_nodes(&nodes, hosts, true))) {
                ORTE_ERROR_LOG(rc);
                OBJ_DESTRUCT(&nodes);
                free(hosts);
                return rc;
            }
            /* now indicate that this app is to run across them */
            orte_set_attribute(&app->attributes, ORTE_APP_DASH_HOST, ORTE_ATTR_LOCAL, hosts, OPAL_STRING);
            orte_remove_attribute(&app->attributes, ORTE_APP_ADD_HOST);
            free(hosts);
        }
    }

    /* if something was found, we add that to our global pool */
    if (!opal_list_is_empty(&nodes)) {
        /* the node insert code doesn't check for uniqueness, so we will
         * do so here - yes, this is an ugly, non-scalable loop, but this
         * is the exception case and so we can do it here */
        OPAL_LIST_FOREACH_SAFE(node, next, &nodes, orte_node_t) {
            node->state = ORTE_NODE_STATE_ADDED;
            for (n=0; n < orte_node_pool->size; n++) {
                if (NULL == (nptr = (orte_node_t*)opal_pointer_array_get_item(orte_node_pool, n))) {
                    continue;
                }
                if (0 == strcmp(node->name, nptr->name)) {
                    opal_list_remove_item(&nodes, &node->super);
                    OBJ_RELEASE(node);
                    break;
                }
            }
        }
        if (!opal_list_is_empty(&nodes)) {
            /* store the results in the global resource pool - this removes the
             * list items
             */
            if (ORTE_SUCCESS != (rc = orte_ras_base_node_insert(&nodes, jdata))) {
                ORTE_ERROR_LOG(rc);
            }
            /* mark that an updated nidmap must be communicated to existing daemons */
            orte_nidmap_communicated = false;
        }
    }
    /* cleanup */
    OPAL_LIST_DESTRUCT(&nodes);

    /* shall we display the results? */
    if (0 < opal_output_get_verbosity(orte_ras_base_framework.framework_output)) {
        orte_ras_base_display_alloc();
    }

    return ORTE_SUCCESS;
}
