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
 * Copyright (c) 2010      Oracle and/or its affiliates.  All rights reserved.
 * Copyright (c) 2011-2015 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2011-2013 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2013-2018 Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "orte_config.h"
#include "orte/types.h"

#include <sys/types.h>

#include "opal/util/argv.h"
#include "opal/mca/hwloc/base/base.h"

#include "orte/mca/errmgr/errmgr.h"
#include "orte/mca/grpcomm/grpcomm.h"
#include "orte/mca/rmaps/base/base.h"
#include "opal/dss/dss.h"
#include "orte/util/name_fns.h"
#include "orte/util/error_strings.h"
#include "orte/runtime/orte_globals.h"

#include "orte/runtime/data_type_support/orte_dt_support.h"

static void orte_dt_quick_print(char **output, char *type_name, char *prefix, void *src, opal_data_type_t real_type)
{
    int8_t *i8;
    int16_t *i16;
    int32_t *i32;
    int64_t *i64;
    uint8_t *ui8;
    uint16_t *ui16;
    uint32_t *ui32;
    uint64_t *ui64;

    /* set default result */
    *output = NULL;

    /* check for NULL ptr */
    if (NULL == src) {
        asprintf(output, "%sData type: %s\tData size: 8-bit\tValue: NULL pointer",
                 (NULL == prefix) ? "" : prefix, type_name);
        return;
    }

    switch(real_type) {
        case OPAL_INT8:
            i8 = (int8_t*)src;
            asprintf(output, "%sData type: %s\tData size: 8-bit\tValue: %d",
                     (NULL == prefix) ? "" : prefix, type_name, (int) *i8);
            break;

        case OPAL_UINT8:
            ui8 = (uint8_t*)src;
            asprintf(output, "%sData type: %s\tData size: 8-bit\tValue: %u",
                     (NULL == prefix) ? "" : prefix, type_name, (unsigned int)*ui8);
            break;

        case OPAL_INT16:
            i16 = (int16_t*)src;
            asprintf(output, "%sData type: %s\tData size: 16-bit\tValue: %d",
                     (NULL == prefix) ? "" : prefix, type_name, (int) *i16);
            break;

        case OPAL_UINT16:
            ui16 = (uint16_t*)src;
            asprintf(output, "%sData type: %s\tData size: 16-bit\tValue: %u",
                     (NULL == prefix) ? "" : prefix, type_name, (unsigned int) *ui16);
            break;

        case OPAL_INT32:
            i32 = (int32_t*)src;
            asprintf(output, "%sData type: %s\tData size: 32-bit\tValue: %ld",
                     (NULL == prefix) ? "" : prefix, type_name, (long) *i32);
            break;

        case OPAL_UINT32:
            ui32 = (uint32_t*)src;
            asprintf(output, "%sData type: %s\tData size: 32-bit\tValue: %lu",
                     (NULL == prefix) ? "" : prefix, type_name, (unsigned long) *ui32);
            break;

        case OPAL_INT64:
            i64 = (int64_t*)src;
            asprintf(output, "%sData type: %s\tData size: 64-bit\tValue: %ld",
                     (NULL == prefix) ? "" : prefix, type_name, (long) *i64);
            break;

        case OPAL_UINT64:
            ui64 = (uint64_t*)src;
            asprintf(output, "%sData type: %s\tData size: 64-bit\tValue: %lu",
                     (NULL == prefix) ? "" : prefix, type_name, (unsigned long) *ui64);
            break;

        default:
            return;
    }

    return;
}

/*
 * STANDARD PRINT FUNCTION - WORKS FOR EVERYTHING NON-STRUCTURED
 */
int orte_dt_std_print(char **output, char *prefix, void *src, opal_data_type_t type)
{
    /* set default result */
    *output = NULL;

    switch(type) {
        case ORTE_STD_CNTR:
            orte_dt_quick_print(output, "ORTE_STD_CNTR", prefix, src, ORTE_STD_CNTR_T);
            break;

        case ORTE_PROC_STATE:
            orte_dt_quick_print(output, "ORTE_PROC_STATE", prefix, src, ORTE_PROC_STATE_T);
            break;

        case ORTE_JOB_STATE:
            orte_dt_quick_print(output, "ORTE_JOB_STATE", prefix, src, ORTE_JOB_STATE_T);
            break;

        case ORTE_NODE_STATE:
            orte_dt_quick_print(output, "ORTE_NODE_STATE", prefix, src, ORTE_NODE_STATE_T);
            break;

        case ORTE_EXIT_CODE:
            orte_dt_quick_print(output, "ORTE_EXIT_CODE", prefix, src, ORTE_EXIT_CODE_T);
            break;

        case ORTE_RML_TAG:
            orte_dt_quick_print(output, "ORTE_RML_TAG", prefix, src, ORTE_RML_TAG_T);
            break;

        case ORTE_DAEMON_CMD:
            orte_dt_quick_print(output, "ORTE_DAEMON_CMD", prefix, src, ORTE_DAEMON_CMD_T);
            break;

        case ORTE_IOF_TAG:
            orte_dt_quick_print(output, "ORTE_IOF_TAG", prefix, src, ORTE_IOF_TAG_T);
            break;

        default:
            ORTE_ERROR_LOG(ORTE_ERR_UNKNOWN_DATA_TYPE);
            return ORTE_ERR_UNKNOWN_DATA_TYPE;
    }

    return ORTE_SUCCESS;
}

/*
 * JOB
 */
int orte_dt_print_job(char **output, char *prefix, orte_job_t *src, opal_data_type_t type)
{
    char *tmp, *tmp2, *tmp3, *pfx2, *pfx;
    int32_t i;
    int rc;
    orte_app_context_t *app;
    orte_proc_t *proc;

    /* set default result */
    *output = NULL;

    /* protect against NULL prefix */
    if (NULL == prefix) {
        asprintf(&pfx2, " ");
    } else {
        asprintf(&pfx2, "%s", prefix);
    }

    tmp2 = opal_argv_join(src->personality, ',');
    asprintf(&tmp, "\n%sData for job: %s\tPersonality: %s\tRecovery: %s(%s)\n%s\tNum apps: %ld\tStdin target: %s\tState: %s\tAbort: %s", pfx2,
             ORTE_JOBID_PRINT(src->jobid), tmp2,
             (ORTE_FLAG_TEST(src, ORTE_JOB_FLAG_RECOVERABLE)) ? "ENABLED" : "DISABLED",
             (orte_get_attribute(&src->attributes, ORTE_JOB_RECOVER_DEFINED, NULL, OPAL_BOOL)) ? "DEFINED" : "DEFAULT",
             pfx2,
             (long)src->num_apps, ORTE_VPID_PRINT(src->stdin_target),
              orte_job_state_to_str(src->state), (ORTE_FLAG_TEST(src, ORTE_JOB_FLAG_ABORTED)) ? "True" : "False");
    free(tmp2);
    asprintf(&pfx, "%s\t", pfx2);
    free(pfx2);

    for (i=0; i < src->apps->size; i++) {
        if (NULL == (app = (orte_app_context_t*)opal_pointer_array_get_item(src->apps, i))) {
            continue;
        }
        opal_dss.print(&tmp2, pfx, app, ORTE_APP_CONTEXT);
        asprintf(&tmp3, "%s\n%s", tmp, tmp2);
        free(tmp);
        free(tmp2);
        tmp = tmp3;
    }

    if (NULL != src->map) {
        if (ORTE_SUCCESS != (rc = opal_dss.print(&tmp2, pfx, src->map, ORTE_JOB_MAP))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }
        asprintf(&tmp3, "%s%s", tmp, tmp2);
        free(tmp);
        free(tmp2);
        tmp = tmp3;
    } else {
        asprintf(&tmp2, "%s\n%sNo Map", tmp, pfx);
        free(tmp);
        tmp = tmp2;
    }

    asprintf(&tmp2, "%s\n%sNum procs: %ld\tOffset: %ld", tmp, pfx, (long)src->num_procs, (long)src->offset);
    free(tmp);
    tmp = tmp2;

    for (i=0; i < src->procs->size; i++) {
        if (NULL == (proc = (orte_proc_t*)opal_pointer_array_get_item(src->procs, i))) {
            continue;
        }
        if (ORTE_SUCCESS != (rc = opal_dss.print(&tmp2, pfx, proc, ORTE_PROC))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }
        asprintf(&tmp3, "%s%s", tmp, tmp2);
        free(tmp);
        free(tmp2);
        tmp = tmp3;
    }

    asprintf(&tmp2, "%s\n%s\tNum launched: %ld\tNum reported: %ld\tNum terminated: %ld",
             tmp, pfx, (long)src->num_launched, (long)src->num_reported,
             (long)src->num_terminated);
    free(tmp);
    tmp = tmp2;

    /* set the return */
    *output = tmp;
    free(pfx);

    return ORTE_SUCCESS;
}

/*
 * NODE
 */
int orte_dt_print_node(char **output, char *prefix, orte_node_t *src, opal_data_type_t type)
{
    char *tmp, *tmp2, *tmp3, *pfx2, *pfx;
    int32_t i;
    int rc;
    orte_proc_t *proc;
    char **alias;
    /* set default result */
    *output = NULL;

    /* protect against NULL prefix */
    if (NULL == prefix) {
        asprintf(&pfx2, " ");
    } else {
        asprintf(&pfx2, "%s", prefix);
    }

    if (orte_xml_output) {
        /* need to create the output in XML format */
        asprintf(&tmp, "%s<host name=\"%s\" slots=\"%d\" max_slots=\"%d\">\n", pfx2,
                 (NULL == src->name) ? "UNKNOWN" : src->name,
                 (int)src->slots, (int)src->slots_max);
        /* does this node have any aliases? */
        tmp3 = NULL;
        if (orte_get_attribute(&src->attributes, ORTE_NODE_ALIAS, (void**)&tmp3, OPAL_STRING)) {
            alias = opal_argv_split(tmp3, ',');
            for (i=0; NULL != alias[i]; i++) {
                asprintf(&tmp2, "%s%s\t<noderesolve resolved=\"%s\"/>\n", tmp, pfx2, alias[i]);
                free(tmp);
                tmp = tmp2;
            }
            opal_argv_free(alias);
        }
        if (NULL != tmp3) {
            free(tmp3);
        }
        *output = tmp;
        free(pfx2);
        return ORTE_SUCCESS;
    }

    if (!orte_devel_level_output) {
        /* just provide a simple output for users */
        if (0 == src->num_procs) {
            /* no procs mapped yet, so just show allocation */
            asprintf(&tmp, "\n%sData for node: %s\tNum slots: %ld\tMax slots: %ld",
                     pfx2, (NULL == src->name) ? "UNKNOWN" : src->name,
                     (long)src->slots, (long)src->slots_max);
            /* does this node have any aliases? */
            tmp3 = NULL;
            if (orte_get_attribute(&src->attributes, ORTE_NODE_ALIAS, (void**)&tmp3, OPAL_STRING)) {
                alias = opal_argv_split(tmp3, ',');
                for (i=0; NULL != alias[i]; i++) {
                    asprintf(&tmp2, "%s%s\tresolved from %s\n", tmp, pfx2, alias[i]);
                    free(tmp);
                    tmp = tmp2;
                }
                opal_argv_free(alias);
            }
            if (NULL != tmp3) {
                free(tmp3);
            }
            free(pfx2);
            *output = tmp;
            return ORTE_SUCCESS;
        }
        asprintf(&tmp, "\n%sData for node: %s\tNum slots: %ld\tMax slots: %ld\tNum procs: %ld",
                 pfx2, (NULL == src->name) ? "UNKNOWN" : src->name,
                 (long)src->slots, (long)src->slots_max, (long)src->num_procs);
        /* does this node have any aliases? */
        tmp3 = NULL;
        if (orte_get_attribute(&src->attributes, ORTE_NODE_ALIAS, (void**)&tmp3, OPAL_STRING)) {
            alias = opal_argv_split(tmp3, ',');
            for (i=0; NULL != alias[i]; i++) {
                asprintf(&tmp2, "%s%s\tresolved from %s\n", tmp, pfx2, alias[i]);
                free(tmp);
                tmp = tmp2;
            }
            opal_argv_free(alias);
        }
        if (NULL != tmp3) {
            free(tmp3);
        }
        goto PRINT_PROCS;
    }

    asprintf(&tmp, "\n%sData for node: %s\tState: %0x\tFlags: %02x",
             pfx2, (NULL == src->name) ? "UNKNOWN" : src->name, src->state, src->flags);
    /* does this node have any aliases? */
    tmp3 = NULL;
    if (orte_get_attribute(&src->attributes, ORTE_NODE_ALIAS, (void**)&tmp3, OPAL_STRING)) {
        alias = opal_argv_split(tmp3, ',');
        for (i=0; NULL != alias[i]; i++) {
            asprintf(&tmp2, "%s%s\tresolved from %s\n", tmp, pfx2, alias[i]);
            free(tmp);
            tmp = tmp2;
        }
        opal_argv_free(alias);
    }
    if (NULL != tmp3) {
        free(tmp3);
    }

    if (NULL == src->daemon) {
        asprintf(&tmp2, "%s\n%s\tDaemon: %s\tDaemon launched: %s", tmp, pfx2,
                 "Not defined", ORTE_FLAG_TEST(src, ORTE_NODE_FLAG_DAEMON_LAUNCHED) ? "True" : "False");
    } else {
        asprintf(&tmp2, "%s\n%s\tDaemon: %s\tDaemon launched: %s", tmp, pfx2,
                 ORTE_NAME_PRINT(&(src->daemon->name)),
                 ORTE_FLAG_TEST(src, ORTE_NODE_FLAG_DAEMON_LAUNCHED) ? "True" : "False");
    }
    free(tmp);
    tmp = tmp2;

    asprintf(&tmp2, "%s\n%s\tNum slots: %ld\tSlots in use: %ld\tOversubscribed: %s", tmp, pfx2,
             (long)src->slots, (long)src->slots_inuse,
             ORTE_FLAG_TEST(src, ORTE_NODE_FLAG_OVERSUBSCRIBED) ? "TRUE" : "FALSE");
    free(tmp);
    tmp = tmp2;

    asprintf(&tmp2, "%s\n%s\tNum slots allocated: %ld\tMax slots: %ld", tmp, pfx2,
             (long)src->slots, (long)src->slots_max);
    free(tmp);
    tmp = tmp2;

    tmp3 = NULL;
    if (orte_get_attribute(&src->attributes, ORTE_NODE_USERNAME, (void**)&tmp3, OPAL_STRING)) {
        asprintf(&tmp2, "%s\n%s\tUsername on node: %s", tmp, pfx2, tmp3);
        free(tmp3);
        free(tmp);
        tmp = tmp2;
    }

    if (orte_display_topo_with_map && NULL != src->topology) {
        char *pfx3;
        asprintf(&tmp2, "%s\n%s\tDetected Resources:\n", tmp, pfx2);
        free(tmp);
        tmp = tmp2;

        tmp2 = NULL;
        asprintf(&pfx3, "%s\t\t", pfx2);
        opal_dss.print(&tmp2, pfx3, src->topology, OPAL_HWLOC_TOPO);
        free(pfx3);
        asprintf(&tmp3, "%s%s", tmp, tmp2);
        free(tmp);
        free(tmp2);
        tmp = tmp3;
    }

    asprintf(&tmp2, "%s\n%s\tNum procs: %ld\tNext node_rank: %ld", tmp, pfx2,
             (long)src->num_procs, (long)src->next_node_rank);
    free(tmp);
    tmp = tmp2;

 PRINT_PROCS:
    asprintf(&pfx, "%s\t", pfx2);
    free(pfx2);

    for (i=0; i < src->procs->size; i++) {
        if (NULL == (proc = (orte_proc_t*)opal_pointer_array_get_item(src->procs, i))) {
            continue;
        }
        if (ORTE_SUCCESS != (rc = opal_dss.print(&tmp2, pfx, proc, ORTE_PROC))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }
        asprintf(&tmp3, "%s%s", tmp, tmp2);
        free(tmp);
        free(tmp2);
        tmp = tmp3;
    }
    free(pfx);

    /* set the return */
    *output = tmp;

    return ORTE_SUCCESS;
}

/*
 * PROC
 */
int orte_dt_print_proc(char **output, char *prefix, orte_proc_t *src, opal_data_type_t type)
{
    char *tmp, *tmp3, *pfx2;
    hwloc_obj_t loc=NULL;
    char locale[1024], tmp1[1024], tmp2[1024];
    hwloc_cpuset_t mycpus;
    char *str=NULL, *cpu_bitmap=NULL;


    /* set default result */
    *output = NULL;

    /* protect against NULL prefix */
    if (NULL == prefix) {
        asprintf(&pfx2, " ");
    } else {
        asprintf(&pfx2, "%s", prefix);
    }

    if (orte_xml_output) {
        /* need to create the output in XML format */
        if (0 == src->pid) {
            asprintf(output, "%s<process rank=\"%s\" status=\"%s\"/>\n", pfx2,
                     ORTE_VPID_PRINT(src->name.vpid), orte_proc_state_to_str(src->state));
        } else {
            asprintf(output, "%s<process rank=\"%s\" pid=\"%d\" status=\"%s\"/>\n", pfx2,
                     ORTE_VPID_PRINT(src->name.vpid), (int)src->pid, orte_proc_state_to_str(src->state));
        }
        free(pfx2);
        return ORTE_SUCCESS;
    }

    if (!orte_devel_level_output) {
        if (orte_get_attribute(&src->attributes, ORTE_PROC_CPU_BITMAP, (void**)&cpu_bitmap, OPAL_STRING) &&
            NULL != src->node->topology && NULL != src->node->topology->topo) {
            mycpus = hwloc_bitmap_alloc();
            hwloc_bitmap_list_sscanf(mycpus, cpu_bitmap);
            if (OPAL_ERR_NOT_BOUND == opal_hwloc_base_cset2str(tmp1, sizeof(tmp1), src->node->topology->topo, mycpus)) {
                str = strdup("UNBOUND");
            } else {
                opal_hwloc_base_cset2mapstr(tmp2, sizeof(tmp2), src->node->topology->topo, mycpus);
                asprintf(&str, "%s:%s", tmp1, tmp2);
            }
            hwloc_bitmap_free(mycpus);
            asprintf(&tmp, "\n%sProcess OMPI jobid: %s App: %ld Process rank: %s Bound: %s", pfx2,
                     ORTE_JOBID_PRINT(src->name.jobid), (long)src->app_idx,
                     ORTE_VPID_PRINT(src->name.vpid), (NULL == str) ? "N/A" : str);
            if (NULL != str) {
                free(str);
            }
            if (NULL != cpu_bitmap) {
                free(cpu_bitmap);
            }
        } else {
            /* just print a very simple output for users */
            asprintf(&tmp, "\n%sProcess OMPI jobid: %s App: %ld Process rank: %s Bound: N/A", pfx2,
                     ORTE_JOBID_PRINT(src->name.jobid), (long)src->app_idx,
                     ORTE_VPID_PRINT(src->name.vpid));
        }

        /* set the return */
        *output = tmp;
        free(pfx2);
        return ORTE_SUCCESS;
    }

    asprintf(&tmp, "\n%sData for proc: %s", pfx2, ORTE_NAME_PRINT(&src->name));

    asprintf(&tmp3, "%s\n%s\tPid: %ld\tLocal rank: %lu\tNode rank: %lu\tApp rank: %d", tmp, pfx2,
             (long)src->pid, (unsigned long)src->local_rank, (unsigned long)src->node_rank, src->app_rank);
    free(tmp);
    tmp = tmp3;

    if (orte_get_attribute(&src->attributes, ORTE_PROC_HWLOC_LOCALE, (void**)&loc, OPAL_PTR)) {
        if (NULL != loc) {
            if (OPAL_ERR_NOT_BOUND == opal_hwloc_base_cset2mapstr(locale, sizeof(locale), src->node->topology->topo, loc->cpuset)) {
                strcpy(locale, "NODE");
            }
        } else {
            strcpy(locale, "UNKNOWN");
        }
    } else {
        strcpy(locale, "UNKNOWN");
    }
    if (orte_get_attribute(&src->attributes, ORTE_PROC_CPU_BITMAP, (void**)&cpu_bitmap, OPAL_STRING) &&
        NULL != src->node->topology && NULL != src->node->topology->topo) {
        mycpus = hwloc_bitmap_alloc();
        hwloc_bitmap_list_sscanf(mycpus, cpu_bitmap);
        opal_hwloc_base_cset2mapstr(tmp2, sizeof(tmp2), src->node->topology->topo, mycpus);
    } else {
        snprintf(tmp2, sizeof(tmp2), "UNBOUND");
    }
    asprintf(&tmp3, "%s\n%s\tState: %s\tApp_context: %ld\n%s\tLocale:  %s\n%s\tBinding: %s", tmp, pfx2,
             orte_proc_state_to_str(src->state), (long)src->app_idx, pfx2, locale, pfx2,  tmp2);
    free(tmp);
    if (NULL != str) {
        free(str);
    }
    if (NULL != cpu_bitmap) {
        free(cpu_bitmap);
    }

    /* set the return */
    *output = tmp3;

    free(pfx2);
    return ORTE_SUCCESS;
}

/*
 * APP CONTEXT
 */
int orte_dt_print_app_context(char **output, char *prefix, orte_app_context_t *src, opal_data_type_t type)
{
    char *tmp, *tmp2, *tmp3, *pfx2;
    int i, count;
    opal_value_t *kv;

    /* set default result */
    *output = NULL;

    /* protect against NULL prefix */
    if (NULL == prefix) {
        asprintf(&pfx2, " ");
    } else {
        asprintf(&pfx2, "%s", prefix);
    }

    asprintf(&tmp, "\n%sData for app_context: index %lu\tapp: %s\n%s\tNum procs: %lu\tFirstRank: %s",
             pfx2, (unsigned long)src->idx,
             (NULL == src->app) ? "NULL" : src->app,
             pfx2, (unsigned long)src->num_procs,
             ORTE_VPID_PRINT(src->first_rank));

    count = opal_argv_count(src->argv);
    for (i=0; i < count; i++) {
        asprintf(&tmp2, "%s\n%s\tArgv[%d]: %s", tmp, pfx2, i, src->argv[i]);
        free(tmp);
        tmp = tmp2;
    }

    count = opal_argv_count(src->env);
    for (i=0; i < count; i++) {
        asprintf(&tmp2, "%s\n%s\tEnv[%lu]: %s", tmp, pfx2, (unsigned long)i, src->env[i]);
        free(tmp);
        tmp = tmp2;
    }

    tmp3 = NULL;
    orte_get_attribute(&src->attributes, ORTE_APP_PREFIX_DIR, (void**)&tmp3, OPAL_STRING);
    asprintf(&tmp2, "%s\n%s\tWorking dir: %s\n%s\tPrefix: %s\n%s\tUsed on node: %s", tmp,
             pfx2, (NULL == src->cwd) ? "NULL" : src->cwd,
             pfx2, (NULL == tmp3) ? "NULL" : tmp3,
             pfx2, ORTE_FLAG_TEST(src, ORTE_APP_FLAG_USED_ON_NODE) ? "TRUE" : "FALSE");
    free(tmp);
    tmp = tmp2;

    OPAL_LIST_FOREACH(kv, &src->attributes, opal_value_t) {
        opal_dss.print(&tmp2, pfx2, kv, ORTE_ATTRIBUTE);
        asprintf(&tmp3, "%s\n%s", tmp, tmp2);
        free(tmp2);
        free(tmp);
        tmp = tmp3;
    }

    /* set the return */
    *output = tmp;

    free(pfx2);
    return ORTE_SUCCESS;
}

/*
 * JOB_MAP
 */
int orte_dt_print_map(char **output, char *prefix, orte_job_map_t *src, opal_data_type_t type)
{
    char *tmp=NULL, *tmp2, *tmp3, *pfx, *pfx2;
    int32_t i, j;
    int rc;
    orte_node_t *node;
    orte_proc_t *proc;

    /* set default result */
    *output = NULL;

    /* protect against NULL prefix */
    if (NULL == prefix) {
        asprintf(&pfx2, " ");
    } else {
        asprintf(&pfx2, "%s", prefix);
    }

    if (orte_xml_output) {
        /* need to create the output in XML format */
        asprintf(&tmp, "<map>\n");
        /* loop through nodes */
        for (i=0; i < src->nodes->size; i++) {
            if (NULL == (node = (orte_node_t*)opal_pointer_array_get_item(src->nodes, i))) {
                continue;
            }
            orte_dt_print_node(&tmp2, "\t", node, ORTE_NODE);
            asprintf(&tmp3, "%s%s", tmp, tmp2);
            free(tmp2);
            free(tmp);
            tmp = tmp3;
            /* for each node, loop through procs and print their rank */
            for (j=0; j < node->procs->size; j++) {
                if (NULL == (proc = (orte_proc_t*)opal_pointer_array_get_item(node->procs, j))) {
                    continue;
                }
                orte_dt_print_proc(&tmp2, "\t\t", proc, ORTE_PROC);
                asprintf(&tmp3, "%s%s", tmp, tmp2);
                free(tmp2);
                free(tmp);
                tmp = tmp3;
            }
            asprintf(&tmp3, "%s\t</host>\n", tmp);
            free(tmp);
            tmp = tmp3;
        }
        asprintf(&tmp2, "%s</map>\n", tmp);
        free(tmp);
        free(pfx2);
        *output = tmp2;
        return ORTE_SUCCESS;

    }

    asprintf(&pfx, "%s\t", pfx2);

    if (orte_devel_level_output) {
        asprintf(&tmp, "\n%sMapper requested: %s  Last mapper: %s  Mapping policy: %s  Ranking policy: %s\n%sBinding policy: %s  Cpu set: %s  PPR: %s  Cpus-per-rank: %d",
                 pfx2, (NULL == src->req_mapper) ? "NULL" : src->req_mapper,
                 (NULL == src->last_mapper) ? "NULL" : src->last_mapper,
                 orte_rmaps_base_print_mapping(src->mapping),
                 orte_rmaps_base_print_ranking(src->ranking),
                 pfx2, opal_hwloc_base_print_binding(src->binding),
                 (NULL == opal_hwloc_base_cpu_list) ? "NULL" : opal_hwloc_base_cpu_list,
                 (NULL == src->ppr) ? "NULL" : src->ppr,
                 (int)src->cpus_per_rank);

        if (ORTE_VPID_INVALID == src->daemon_vpid_start) {
            asprintf(&tmp2, "%s\n%sNum new daemons: %ld\tNew daemon starting vpid INVALID\n%sNum nodes: %ld",
                     tmp, pfx, (long)src->num_new_daemons, pfx, (long)src->num_nodes);
        } else {
            asprintf(&tmp2, "%s\n%sNum new daemons: %ld\tNew daemon starting vpid %ld\n%sNum nodes: %ld",
                     tmp, pfx, (long)src->num_new_daemons, (long)src->daemon_vpid_start,
                     pfx, (long)src->num_nodes);
        }
        free(tmp);
        tmp = tmp2;
    } else {
        /* this is being printed for a user, so let's make it easier to see */
        asprintf(&tmp, "\n%s========================   JOB MAP   ========================", pfx2);
    }


    for (i=0; i < src->nodes->size; i++) {
        if (NULL == (node = (orte_node_t*)opal_pointer_array_get_item(src->nodes, i))) {
            continue;
        }
        if (ORTE_SUCCESS != (rc = opal_dss.print(&tmp2, pfx2, node, ORTE_NODE))) {
            ORTE_ERROR_LOG(rc);
            free(pfx);
            free(tmp);
            return rc;
        }
        asprintf(&tmp3, "%s\n%s", tmp, tmp2);
        free(tmp);
        free(tmp2);
        tmp = tmp3;
    }

    if (!orte_devel_level_output) {
        /* this is being printed for a user, so let's make it easier to see */
        asprintf(&tmp2, "%s\n\n%s=============================================================\n", tmp, pfx2);
        free(tmp);
        tmp = tmp2;
    }
    free(pfx2);

    /* set the return */
    *output = tmp;

    free(pfx);
    return ORTE_SUCCESS;
}

/* ORTE_ATTR */
int orte_dt_print_attr(char **output, char *prefix,
                       orte_attribute_t *src, opal_data_type_t type)
{
    char *prefx;

    /* deal with NULL prefix */
    if (NULL == prefix) asprintf(&prefx, " ");
    else prefx = strdup(prefix);

    /* if src is NULL, just print data type and return */
    if (NULL == src) {
        asprintf(output, "%sData type: ORTE_ATTR\tValue: NULL pointer", prefx);
        free(prefx);
        return OPAL_SUCCESS;
    }

    switch (src->type) {
    case OPAL_STRING:
        asprintf(output, "%sORTE_ATTR: %s Data type: OPAL_STRING\tKey: %s\tValue: %s",
                 prefx, src->local ? "LOCAL" : "GLOBAL", orte_attr_key_to_str(src->key), src->data.string);
        break;
    case OPAL_SIZE:
        asprintf(output, "%sORTE_ATTR: %s Data type: OPAL_SIZE\tKey: %s\tValue: %lu",
                 prefx, src->local ? "LOCAL" : "GLOBAL", orte_attr_key_to_str(src->key), (unsigned long)src->data.size);
        break;
    case OPAL_PID:
        asprintf(output, "%sORTE_ATTR: %s Data type: OPAL_PID\tKey: %s\tValue: %lu",
                 prefx, src->local ? "LOCAL" : "GLOBAL", orte_attr_key_to_str(src->key), (unsigned long)src->data.pid);
        break;
    case OPAL_INT:
        asprintf(output, "%sORTE_ATTR: %s Data type: OPAL_INT\tKey: %s\tValue: %d",
                 prefx, src->local ? "LOCAL" : "GLOBAL", orte_attr_key_to_str(src->key), src->data.integer);
        break;
    case OPAL_INT8:
        asprintf(output, "%sORTE_ATTR: %s Data type: OPAL_INT8\tKey: %s\tValue: %d",
                 prefx, src->local ? "LOCAL" : "GLOBAL", orte_attr_key_to_str(src->key), (int)src->data.int8);
        break;
    case OPAL_INT16:
        asprintf(output, "%sORTE_ATTR: %s Data type: OPAL_INT16\tKey: %s\tValue: %d",
                 prefx, src->local ? "LOCAL" : "GLOBAL", orte_attr_key_to_str(src->key), (int)src->data.int16);
        break;
    case OPAL_INT32:
        asprintf(output, "%sORTE_ATTR: %s Data type: OPAL_INT32\tKey: %s\tValue: %d",
                 prefx, src->local ? "LOCAL" : "GLOBAL", orte_attr_key_to_str(src->key), src->data.int32);
        break;
    case OPAL_INT64:
        asprintf(output, "%sORTE_ATTR: %s Data type: OPAL_INT64\tKey: %s\tValue: %d",
                 prefx, src->local ? "LOCAL" : "GLOBAL", orte_attr_key_to_str(src->key), (int)src->data.int64);
        break;
    case OPAL_UINT:
        asprintf(output, "%sORTE_ATTR: %s Data type: OPAL_UINT\tKey: %s\tValue: %u",
                 prefx, src->local ? "LOCAL" : "GLOBAL", orte_attr_key_to_str(src->key), (unsigned int)src->data.uint);
        break;
    case OPAL_UINT8:
        asprintf(output, "%sORTE_ATTR: %s Data type: OPAL_UINT8\tKey: %s\tValue: %u",
                 prefx, src->local ? "LOCAL" : "GLOBAL", orte_attr_key_to_str(src->key), (unsigned int)src->data.uint8);
        break;
    case OPAL_UINT16:
        asprintf(output, "%sORTE_ATTR: %s Data type: OPAL_UINT16\tKey: %s\tValue: %u",
                 prefx, src->local ? "LOCAL" : "GLOBAL", orte_attr_key_to_str(src->key), (unsigned int)src->data.uint16);
        break;
    case OPAL_UINT32:
        asprintf(output, "%sORTE_ATTR: %s Data type: OPAL_UINT32\tKey: %s\tValue: %u",
                 prefx, src->local ? "LOCAL" : "GLOBAL", orte_attr_key_to_str(src->key), src->data.uint32);
        break;
    case OPAL_UINT64:
        asprintf(output, "%sORTE_ATTR: %s Data type: OPAL_UINT64\tKey: %s\tValue: %lu",
                 prefx, src->local ? "LOCAL" : "GLOBAL", orte_attr_key_to_str(src->key), (unsigned long)src->data.uint64);
        break;
    case OPAL_BYTE_OBJECT:
        asprintf(output, "%sORTE_ATTR: %s Data type: OPAL_BYTE_OBJECT\tKey: %s\tValue: UNPRINTABLE",
                 prefx, src->local ? "LOCAL" : "GLOBAL", orte_attr_key_to_str(src->key));
        break;
    case OPAL_BUFFER:
        asprintf(output, "%sORTE_ATTR: %s Data type: OPAL_BUFFER\tKey: %s\tValue: UNPRINTABLE",
                 prefx, src->local ? "LOCAL" : "GLOBAL", orte_attr_key_to_str(src->key));
        break;
    case OPAL_FLOAT:
        asprintf(output, "%sORTE_ATTR: %s Data type: OPAL_FLOAT\tKey: %s\tValue: %f",
                 prefx, src->local ? "LOCAL" : "GLOBAL", orte_attr_key_to_str(src->key), src->data.fval);
        break;
    case OPAL_TIMEVAL:
        asprintf(output, "%sORTE_ATTR: %s Data type: OPAL_TIMEVAL\tKey: %s\tValue: %ld.%06ld", prefx,
                 src->local ? "LOCAL" : "GLOBAL", orte_attr_key_to_str(src->key), (long)src->data.tv.tv_sec, (long)src->data.tv.tv_usec);
        break;
    case OPAL_PTR:
        asprintf(output, "%sORTE_ATTR: %s Data type: OPAL_PTR\tKey: %s", prefx,
                 src->local ? "LOCAL" : "GLOBAL", orte_attr_key_to_str(src->key));
        break;
    case ORTE_VPID:
        asprintf(output, "%sORTE_ATTR: %s Data type: ORTE_VPID\tKey: %s\tValue: %s", prefx, src->local ? "LOCAL" : "GLOBAL",
                 orte_attr_key_to_str(src->key), ORTE_VPID_PRINT(src->data.vpid));
        break;
    case ORTE_JOBID:
        asprintf(output, "%sORTE_ATTR: %s Data type: ORTE_JOBID\tKey: %s\tValue: %s", prefx, src->local ? "LOCAL" : "GLOBAL",
                 orte_attr_key_to_str(src->key), ORTE_JOBID_PRINT(src->data.jobid));
        break;
    default:
        asprintf(output, "%sORTE_ATTR: %s Data type: UNKNOWN\tKey: %s\tValue: UNPRINTABLE",
                 prefx, orte_attr_key_to_str(src->key), src->local ? "LOCAL" : "GLOBAL");
        break;
    }
    free(prefx);
    return ORTE_SUCCESS;
}

int orte_dt_print_sig(char **output, char *prefix, orte_grpcomm_signature_t *src, opal_data_type_t type)
{
    char *prefx;
    size_t i;
    char *tmp, *tmp2;

    /* deal with NULL prefix */
    if (NULL == prefix) asprintf(&prefx, " ");
    else prefx = strdup(prefix);

    /* if src is NULL, just print data type and return */
    if (NULL == src) {
        asprintf(output, "%sData type: ORTE_SIG", prefx);
        free(prefx);
        return OPAL_SUCCESS;
    }

    if (NULL == src->signature) {
        asprintf(output, "%sORTE_SIG  Procs: NULL", prefx);
        free(prefx);
        return ORTE_SUCCESS;
    }

    /* there must be at least one proc in the signature */
    asprintf(&tmp, "%sORTE_SIG  Procs: ", prefx);

    for (i=0; i < src->sz; i++) {
        asprintf(&tmp2, "%s%s", tmp, ORTE_NAME_PRINT(&src->signature[i]));
        free(tmp);
        tmp = tmp2;
    }
    *output = tmp;
    return ORTE_SUCCESS;
}
