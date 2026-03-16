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
 * Copyright (c) 2006-2017 Cisco Systems, Inc.  All rights reserved
 * Copyright (c) 2011      Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2014-2018 Intel, Inc. All rights reserved.
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
#include "orte/constants.h"
#include "orte/types.h"

#include <errno.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif  /* HAVE_UNISTD_H */
#include <string.h>
#include <ctype.h>

#include "opal/util/if.h"
#include "opal/util/net.h"
#include "opal/mca/hwloc/hwloc-internal.h"

#include "orte/util/show_help.h"
#include "orte/mca/errmgr/errmgr.h"
#include "orte/mca/ess/ess.h"
#include "orte/util/hostfile/hostfile.h"
#include "orte/util/dash_host/dash_host.h"
#include "orte/util/name_fns.h"
#include "orte/runtime/orte_globals.h"

#include "orte/mca/rmaps/base/rmaps_private.h"
#include "orte/mca/rmaps/base/base.h"
#include "rmaps_seq.h"

static int orte_rmaps_seq_map(orte_job_t *jdata);

/* define the module */
orte_rmaps_base_module_t orte_rmaps_seq_module = {
    .map_job = orte_rmaps_seq_map
};

/* local object for tracking rank locations */
typedef struct {
    opal_list_item_t super;
    char *hostname;
    char *cpuset;
} seq_node_t;
static void sn_con(seq_node_t *p)
{
    p->hostname = NULL;
    p->cpuset = NULL;
}
static void sn_des(seq_node_t *p)
{
    if (NULL != p->hostname) {
        free(p->hostname);
        p->hostname = NULL;
    }
    if (NULL != p->cpuset) {
        free(p->cpuset);
        p->cpuset = NULL;
    }
}
OBJ_CLASS_INSTANCE(seq_node_t,
                   opal_list_item_t,
                   sn_con, sn_des);

static char *orte_getline(FILE *fp);

/*
 * Sequentially map the ranks according to the placement in the
 * specified hostfile
 */
static int orte_rmaps_seq_map(orte_job_t *jdata)
{
    orte_job_map_t *map;
    orte_app_context_t *app;
    int i, n;
    orte_std_cntr_t j;
    opal_list_item_t *item;
    orte_node_t *node, *nd;
    seq_node_t *sq, *save=NULL, *seq;;
    orte_vpid_t vpid;
    orte_std_cntr_t num_nodes;
    int rc;
    opal_list_t default_seq_list;
    opal_list_t node_list, *seq_list, sq_list;
    orte_proc_t *proc;
    mca_base_component_t *c = &mca_rmaps_seq_component.base_version;
    char *hosts = NULL, *sep, *eptr;
    FILE *fp;
    opal_hwloc_resource_type_t rtype;

    OPAL_OUTPUT_VERBOSE((1, orte_rmaps_base_framework.framework_output,
                         "%s rmaps:seq called on job %s",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         ORTE_JOBID_PRINT(jdata->jobid)));

    /* this mapper can only handle initial launch
     * when seq mapping is desired - allow
     * restarting of failed apps
     */
    if (ORTE_FLAG_TEST(jdata, ORTE_JOB_FLAG_RESTART)) {
        opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                            "mca:rmaps:seq: job %s is being restarted - seq cannot map",
                            ORTE_JOBID_PRINT(jdata->jobid));
        return ORTE_ERR_TAKE_NEXT_OPTION;
    }
    if (NULL != jdata->map->req_mapper) {
        if (0 != strcasecmp(jdata->map->req_mapper, c->mca_component_name)) {
            /* a mapper has been specified, and it isn't me */
            opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                                "mca:rmaps:seq: job %s not using sequential mapper",
                                ORTE_JOBID_PRINT(jdata->jobid));
            return ORTE_ERR_TAKE_NEXT_OPTION;
        }
        /* we need to process it */
        goto process;
    }
    if (ORTE_MAPPING_SEQ != ORTE_GET_MAPPING_POLICY(jdata->map->mapping)) {
        /* I don't know how to do these - defer */
        opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                            "mca:rmaps:seq: job %s not using seq mapper",
                            ORTE_JOBID_PRINT(jdata->jobid));
        return ORTE_ERR_TAKE_NEXT_OPTION;
    }

 process:
    opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                        "mca:rmaps:seq: mapping job %s",
                        ORTE_JOBID_PRINT(jdata->jobid));

    /* flag that I did the mapping */
    if (NULL != jdata->map->last_mapper) {
        free(jdata->map->last_mapper);
    }
    jdata->map->last_mapper = strdup(c->mca_component_name);

    /* convenience def */
    map = jdata->map;

    /* if there is a default hostfile, go and get its ordered list of nodes */
    OBJ_CONSTRUCT(&default_seq_list, opal_list_t);
    if (NULL != orte_default_hostfile) {
        char *hstname = NULL;
        /* open the file */
        fp = fopen(orte_default_hostfile, "r");
        if (NULL == fp) {
            ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
            rc = ORTE_ERR_NOT_FOUND;
            goto error;
        }
        while (NULL != (hstname = orte_getline(fp))) {
            if (0 == strlen(hstname)) {
                free(hstname);
                /* blank line - ignore */
                continue;
            }
            if( '#' == hstname[0] ) {
                free(hstname);
                /* Comment line - ignore */
                continue;
            }
            sq = OBJ_NEW(seq_node_t);
            if (NULL != (sep = strchr(hstname, ' '))) {
                *sep = '\0';
                sep++;
                /* remove any trailing space */
                eptr = sep + strlen(sep) - 1;
                while (eptr > sep && isspace(*eptr)) {
                    eptr--;
                }
                *(eptr+1) = 0;
                sq->cpuset = strdup(sep);
            }

            // Strip off the FQDN if present, ignore IP addresses
            if( !orte_keep_fqdn_hostnames && !opal_net_isaddr(hstname) ) {
                char *ptr;
                if (NULL != (ptr = strchr(hstname, '.'))) {
                    *ptr = '\0';
                }
            }

            sq->hostname = hstname;
            opal_list_append(&default_seq_list, &sq->super);
        }
        fclose(fp);
    }

    /* start at the beginning... */
    vpid = 0;
    jdata->num_procs = 0;
    if (0 < opal_list_get_size(&default_seq_list)) {
        save = (seq_node_t*)opal_list_get_first(&default_seq_list);
    }

    /* default to LOGICAL processors */
    if (orte_get_attribute(&jdata->attributes, ORTE_JOB_PHYSICAL_CPUIDS, NULL, OPAL_BOOL)) {
        opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                            "mca:rmaps:seq: using PHYSICAL processors");
        rtype = OPAL_HWLOC_PHYSICAL;
    } else {
        opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                            "mca:rmaps:seq: using LOGICAL processors");
        rtype = OPAL_HWLOC_LOGICAL;
    }

    /* initialize all the nodes as not included in this job map */
    for (j=0; j < orte_node_pool->size; j++) {
        if (NULL != (node = (orte_node_t*)opal_pointer_array_get_item(orte_node_pool, j))) {
            ORTE_FLAG_UNSET(node, ORTE_NODE_FLAG_MAPPED);
        }
    }

    /* cycle through the app_contexts, mapping them sequentially */
    for(i=0; i < jdata->apps->size; i++) {
        if (NULL == (app = (orte_app_context_t*)opal_pointer_array_get_item(jdata->apps, i))) {
            continue;
        }

        /* dash-host trumps hostfile */
        if (orte_get_attribute(&app->attributes, ORTE_APP_DASH_HOST, (void**)&hosts, OPAL_STRING)) {
            opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                                "mca:rmaps:seq: using dash-host nodes on app %s", app->app);
            OBJ_CONSTRUCT(&node_list, opal_list_t);
            /* dash host entries cannot specify cpusets, so used the std function to retrieve the list */
            if (ORTE_SUCCESS != (rc = orte_util_get_ordered_dash_host_list(&node_list, hosts))) {
                ORTE_ERROR_LOG(rc);
                free(hosts);
                goto error;
            }
            free(hosts);
            /* transfer the list to a seq_node_t list */
            OBJ_CONSTRUCT(&sq_list, opal_list_t);
            while (NULL != (nd = (orte_node_t*)opal_list_remove_first(&node_list))) {
                sq = OBJ_NEW(seq_node_t);
                sq->hostname = strdup(nd->name);
                opal_list_append(&sq_list, &sq->super);
                OBJ_RELEASE(nd);
            }
            OBJ_DESTRUCT(&node_list);
            seq_list = &sq_list;
        } else if (orte_get_attribute(&app->attributes, ORTE_APP_HOSTFILE, (void**)&hosts, OPAL_STRING)) {
            char *hstname;
            if (NULL == hosts) {
                rc = ORTE_ERR_NOT_FOUND;
                goto error;
            }
            opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                                "mca:rmaps:seq: using hostfile %s nodes on app %s", hosts, app->app);
            OBJ_CONSTRUCT(&sq_list, opal_list_t);
            /* open the file */
            fp = fopen(hosts, "r");
            if (NULL == fp) {
                ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
                rc = ORTE_ERR_NOT_FOUND;
                OBJ_DESTRUCT(&sq_list);
                goto error;
            }
            while (NULL != (hstname = orte_getline(fp))) {
                if (0 == strlen(hstname)) {
                    free(hstname);
                    /* blank line - ignore */
                    continue;
                }
                if( '#' == hstname[0] ) {
                    free(hstname);
                    /* Comment line - ignore */
                    continue;
                }
                sq = OBJ_NEW(seq_node_t);
                if (NULL != (sep = strchr(hstname, ' '))) {
                    *sep = '\0';
                    sep++;
                    /* remove any trailing space */
                    eptr = sep + strlen(sep) - 1;
                    while (eptr > sep && isspace(*eptr)) {
                        eptr--;
                    }
                    *(eptr+1) = 0;
                    sq->cpuset = strdup(sep);
                }

                // Strip off the FQDN if present, ignore IP addresses
                if( !orte_keep_fqdn_hostnames && !opal_net_isaddr(hstname) ) {
                    char *ptr;
                    if (NULL != (ptr = strchr(hstname, '.'))) {
                        (*ptr) = '\0';
                    }
                }

                sq->hostname = hstname;
                opal_list_append(&sq_list, &sq->super);
            }
            fclose(fp);
            free(hosts);
            seq_list = &sq_list;
        } else if (0 < opal_list_get_size(&default_seq_list)) {
            opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                                "mca:rmaps:seq: using default hostfile nodes on app %s", app->app);
            seq_list = &default_seq_list;
        } else {
            /* can't do anything - no nodes available! */
            orte_show_help("help-orte-rmaps-base.txt",
                           "orte-rmaps-base:no-available-resources",
                           true);
            return ORTE_ERR_SILENT;
        }

        /* check for nolocal and remove the head node, if required */
        if (map->mapping & ORTE_MAPPING_NO_USE_LOCAL) {
            for (item  = opal_list_get_first(seq_list);
                 item != opal_list_get_end(seq_list);
                 item  = opal_list_get_next(item) ) {
                seq = (seq_node_t*)item;
                /* need to check ifislocal because the name in the
                 * hostfile may not have been FQDN, while name returned
                 * by gethostname may have been (or vice versa)
                 */
                if (orte_ifislocal(seq->hostname)) {
                    opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                                        "mca:rmaps:seq: removing head node %s", seq->hostname);
                    opal_list_remove_item(seq_list, item);
                    OBJ_RELEASE(item);  /* "un-retain" it */
                }
            }
        }

        if (NULL == seq_list || 0 == (num_nodes = (orte_std_cntr_t)opal_list_get_size(seq_list))) {
            orte_show_help("help-orte-rmaps-base.txt",
                           "orte-rmaps-base:no-available-resources",
                           true);
            return ORTE_ERR_SILENT;
        }

        /* if num_procs wasn't specified, set it now */
        if (0 == app->num_procs) {
            app->num_procs = num_nodes;
            opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                                "mca:rmaps:seq: setting num procs to %s for app %s",
                                ORTE_VPID_PRINT(app->num_procs), app->app);
        } else if (num_nodes < app->num_procs) {
            orte_show_help("help-orte-rmaps-base.txt", "seq:not-enough-resources", true,
                           app->num_procs, num_nodes);
            return ORTE_ERR_SILENT;
        }

        if (seq_list == &default_seq_list) {
            sq = save;
        } else {
            sq = (seq_node_t*)opal_list_get_first(seq_list);
        }
        for (n=0; n < app->num_procs; n++) {
            /* find this node on the global array - this is necessary so
             * that our mapping gets saved on that array as the objects
             * returned by the hostfile function are -not- on the array
             */
            node = NULL;
            for (j=0; j < orte_node_pool->size; j++) {
                if (NULL == (node = (orte_node_t*)opal_pointer_array_get_item(orte_node_pool, j))) {
                    continue;
                }
                if (0 == strcmp(sq->hostname, node->name)) {
                    break;
                }
            }
            if (NULL == node) {
                /* wasn't found - that is an error */
                orte_show_help("help-orte-rmaps-seq.txt",
                               "orte-rmaps-seq:resource-not-found",
                               true, sq->hostname);
                rc = ORTE_ERR_SILENT;
                goto error;
            }
            /* ensure the node is in the map */
            if (!ORTE_FLAG_TEST(node, ORTE_NODE_FLAG_MAPPED)) {
                OBJ_RETAIN(node);
                opal_pointer_array_add(map->nodes, node);
                jdata->map->num_nodes++;
                ORTE_FLAG_SET(node, ORTE_NODE_FLAG_MAPPED);
            }
            proc = orte_rmaps_base_setup_proc(jdata, node, i);
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
                /* check for permission */
                if (ORTE_FLAG_TEST(node, ORTE_NODE_FLAG_SLOTS_GIVEN)) {
                    /* if we weren't given a directive either way, then we will error out
                     * as the #slots were specifically given, either by the host RM or
                     * via hostfile/dash-host */
                    if (!(ORTE_MAPPING_SUBSCRIBE_GIVEN & ORTE_GET_MAPPING_DIRECTIVE(jdata->map->mapping))) {
                        orte_show_help("help-orte-rmaps-base.txt", "orte-rmaps-base:alloc-error",
                                       true, app->num_procs, app->app);
                        ORTE_UPDATE_EXIT_STATUS(ORTE_ERROR_DEFAULT_EXIT_CODE);
                        return ORTE_ERR_SILENT;
                    } else if (ORTE_MAPPING_NO_OVERSUBSCRIBE & ORTE_GET_MAPPING_DIRECTIVE(jdata->map->mapping)) {
                        /* if we were explicitly told not to oversubscribe, then don't */
                        orte_show_help("help-orte-rmaps-base.txt", "orte-rmaps-base:alloc-error",
                                       true, app->num_procs, app->app);
                        ORTE_UPDATE_EXIT_STATUS(ORTE_ERROR_DEFAULT_EXIT_CODE);
                        return ORTE_ERR_SILENT;
                    }
                }
            }
            /* assign the vpid */
            proc->name.vpid = vpid++;
            opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                                "mca:rmaps:seq: assign proc %s to node %s for app %s",
                                ORTE_VPID_PRINT(proc->name.vpid), sq->hostname, app->app);

            /* record the cpuset, if given */
            if (NULL != sq->cpuset) {
                hwloc_cpuset_t bitmap;
                char *cpu_bitmap;
                if (NULL == node->topology || NULL == node->topology->topo) {
                    /* not allowed - for sequential cpusets, we must have
                     * the topology info
                     */
                    orte_show_help("help-orte-rmaps-base.txt", "rmaps:no-topology", true, node->name);
                    rc = ORTE_ERR_SILENT;
                    goto error;
                }
                /* if we are using hwthreads as cpus and binding to hwthreads, then
                 * we can just copy the cpuset across as it already specifies things
                 * at that level */
                if (opal_hwloc_use_hwthreads_as_cpus &&
                    OPAL_BIND_TO_HWTHREAD == OPAL_GET_BINDING_POLICY(opal_hwloc_binding_policy)) {
                    cpu_bitmap = strdup(sq->cpuset);
                } else {
                    /* setup the bitmap */
                    bitmap = hwloc_bitmap_alloc();
                    /* parse the slot_list to find the socket and core */
                    if (ORTE_SUCCESS != (rc = opal_hwloc_base_cpu_list_parse(sq->cpuset, node->topology->topo, rtype, bitmap))) {
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
                    hwloc_bitmap_free(bitmap);
                }
                orte_set_attribute(&proc->attributes, ORTE_PROC_CPU_BITMAP, ORTE_ATTR_GLOBAL, cpu_bitmap, OPAL_STRING);
                opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                                    "mca:rmaps:seq: binding proc %s to cpuset %s bitmap %s",
                                    ORTE_VPID_PRINT(proc->name.vpid), sq->cpuset, cpu_bitmap);
                /* we are going to bind to cpuset since the user is specifying the cpus */
                OPAL_SET_BINDING_POLICY(jdata->map->binding, OPAL_BIND_TO_CPUSET);
                /* note that the user specified the mapping */
                ORTE_SET_MAPPING_POLICY(jdata->map->mapping, ORTE_MAPPING_BYUSER);
                ORTE_SET_MAPPING_DIRECTIVE(jdata->map->mapping, ORTE_MAPPING_GIVEN);
                /* cleanup */
                free(cpu_bitmap);
            } else {
                hwloc_obj_t locale;

                /* assign the locale - okay for the topo to be null as
                 * it just means it wasn't returned
                 */
                if (NULL != node->topology && NULL != node->topology->topo) {
                    locale = hwloc_get_root_obj(node->topology->topo);
                    orte_set_attribute(&proc->attributes, ORTE_PROC_HWLOC_LOCALE,
                                       ORTE_ATTR_LOCAL, locale, OPAL_PTR);
                }
            }

            /* add to the jdata proc array */
            if (ORTE_SUCCESS != (rc = opal_pointer_array_set_item(jdata->procs, proc->name.vpid, proc))) {
                ORTE_ERROR_LOG(rc);
                goto error;
            }
            /* move to next node */
            sq = (seq_node_t*)opal_list_get_next(&sq->super);
        }

        /** track the total number of processes we mapped */
        jdata->num_procs += app->num_procs;

        /* cleanup the node list if it came from this app_context */
        if (seq_list != &default_seq_list) {
            OPAL_LIST_DESTRUCT(seq_list);
        } else {
            save = sq;
        }
    }

    /* mark that this job is to be fully
     * described in the launch msg */
    orte_set_attribute(&jdata->attributes, ORTE_JOB_FULLY_DESCRIBED, ORTE_ATTR_GLOBAL, NULL, OPAL_BOOL);

    return ORTE_SUCCESS;

 error:
    OPAL_LIST_DESTRUCT(&default_seq_list);
    return rc;
}

static char *orte_getline(FILE *fp)
{
    char *ret, *buff;
    char input[1024];

    ret = fgets(input, 1024, fp);
    if (NULL != ret) {
           input[strlen(input)-1] = '\0';  /* remove newline */
           buff = strdup(input);
           return buff;
    }

    return NULL;
}
