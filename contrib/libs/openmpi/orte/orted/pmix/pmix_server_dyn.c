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
 * Copyright (c) 2006-2013 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2009-2017 Cisco Systems, Inc.  All rights reserved
 * Copyright (c) 2011      Oak Ridge National Labs.  All rights reserved.
 * Copyright (c) 2013-2018 Intel, Inc. All rights reserved.
 * Copyright (c) 2014      Mellanox Technologies, Inc.
 *                         All rights reserved.
 * Copyright (c) 2014-2016 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 */

#include "orte_config.h"

#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif

#include "opal/util/argv.h"
#include "opal/util/opal_getcwd.h"
#include "opal/util/os_path.h"
#include "opal/util/output.h"
#include "opal/util/path.h"
#include "opal/dss/dss.h"
#include "opal/mca/hwloc/hwloc-internal.h"

#include "orte/mca/errmgr/errmgr.h"
#include "orte/mca/rmaps/base/base.h"
#include "orte/mca/rml/base/rml_contact.h"
#include "orte/mca/state/state.h"
#include "orte/util/name_fns.h"
#include "orte/util/show_help.h"
#include "orte/util/threads.h"
#include "orte/runtime/orte_globals.h"
#include "orte/mca/rml/rml.h"

#include "orte/orted/pmix/pmix_server.h"
#include "orte/orted/pmix/pmix_server_internal.h"

void pmix_server_launch_resp(int status, orte_process_name_t* sender,
                             opal_buffer_t *buffer,
                             orte_rml_tag_t tg, void *cbdata)
{
    pmix_server_req_t *req;
    int rc, room;
    int32_t ret, cnt;
    orte_jobid_t jobid;
    orte_job_t *jdata;

    /* unpack the status */
    cnt = 1;
    if (OPAL_SUCCESS != (rc = opal_dss.unpack(buffer, &ret, &cnt, OPAL_INT32))) {
        ORTE_ERROR_LOG(rc);
        return;
    }

    /* unpack the jobid */
    cnt = 1;
    if (OPAL_SUCCESS != (rc = opal_dss.unpack(buffer, &jobid, &cnt, ORTE_JOBID))) {
        ORTE_ERROR_LOG(rc);
        return;
    }

    /* unpack our tracking room number */
    cnt = 1;
    if (OPAL_SUCCESS != (rc = opal_dss.unpack(buffer, &room, &cnt, OPAL_INT))) {
        ORTE_ERROR_LOG(rc);
        return;
    }

    /* retrieve the request */
    opal_hotel_checkout_and_return_occupant(&orte_pmix_server_globals.reqs, room, (void**)&req);
    if (NULL == req) {
        /* we are hosed */
        ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
        return;
    }

    /* execute the callback */
    if (NULL != req->spcbfunc) {
        req->spcbfunc(ret, jobid, req->cbdata);
    }
    /* if we failed to launch, then ensure we cleanup */
    if (ORTE_SUCCESS != ret) {
        jdata = orte_get_job_data_object(jobid);
        ORTE_ACTIVATE_JOB_STATE(jdata, ORTE_JOB_STATE_TERMINATED);
    }
    /* cleanup */
    OBJ_RELEASE(req);
}

static void spawn(int sd, short args, void *cbdata)
{
    pmix_server_req_t *req = (pmix_server_req_t*)cbdata;
    int rc;
    opal_buffer_t *buf;
    orte_plm_cmd_flag_t command;

    ORTE_ACQUIRE_OBJECT(req);

    /* add this request to our tracker hotel */
    if (OPAL_SUCCESS != (rc = opal_hotel_checkin(&orte_pmix_server_globals.reqs, req, &req->room_num))) {
        orte_show_help("help-orted.txt", "noroom", true, req->operation, orte_pmix_server_globals.num_rooms);
        goto callback;
    }

    /* include the request room number for quick retrieval */
    orte_set_attribute(&req->jdata->attributes, ORTE_JOB_ROOM_NUM,
                       ORTE_ATTR_GLOBAL, &req->room_num, OPAL_INT);

    /* construct a spawn message */
    buf = OBJ_NEW(opal_buffer_t);
    command = ORTE_PLM_LAUNCH_JOB_CMD;
    if (ORTE_SUCCESS != (rc = opal_dss.pack(buf, &command, 1, ORTE_PLM_CMD))) {
        ORTE_ERROR_LOG(rc);
        OBJ_RELEASE(buf);
        opal_hotel_checkout(&orte_pmix_server_globals.reqs, req->room_num);
        goto callback;
    }

    /* pack the jdata object */
    if (ORTE_SUCCESS != (rc = opal_dss.pack(buf, &req->jdata, 1, ORTE_JOB))) {
        ORTE_ERROR_LOG(rc);
        opal_hotel_checkout(&orte_pmix_server_globals.reqs, req->room_num);
        OBJ_RELEASE(buf);
        goto callback;

    }

    /* send it to the HNP for processing - might be myself! */
    if (ORTE_SUCCESS != (rc = orte_rml.send_buffer_nb(orte_mgmt_conduit,
                                                      ORTE_PROC_MY_HNP, buf,
                                                      ORTE_RML_TAG_PLM,
                                                      orte_rml_send_callback, NULL))) {
        ORTE_ERROR_LOG(rc);
        opal_hotel_checkout(&orte_pmix_server_globals.reqs, req->room_num);
        OBJ_RELEASE(buf);
        goto callback;
    }
    return;

  callback:
    /* this section gets executed solely upon an error */
    if (NULL != req->mdxcbfunc) {
        req->mdxcbfunc(rc, NULL, 0, req->cbdata, NULL, NULL);
    }
    OBJ_RELEASE(req);
}

int pmix_server_spawn_fn(opal_process_name_t *requestor,
                         opal_list_t *job_info, opal_list_t *apps,
                         opal_pmix_spawn_cbfunc_t cbfunc, void *cbdata)
{
    orte_job_t *jdata;
    orte_app_context_t *app;
    opal_pmix_app_t *papp;
    opal_value_t *info, *next;
    opal_list_t *cache;
    int rc, i;
    char cwd[OPAL_PATH_MAX];
    bool flag;

    opal_output_verbose(2, orte_pmix_server_globals.output,
                        "%s spawn called from proc %s",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                        ORTE_NAME_PRINT(requestor));

    /* create the job object */
    jdata = OBJ_NEW(orte_job_t);
    jdata->map = OBJ_NEW(orte_job_map_t);

    /* transfer the apps across */
    OPAL_LIST_FOREACH(papp, apps, opal_pmix_app_t) {
        app = OBJ_NEW(orte_app_context_t);
        app->idx = opal_pointer_array_add(jdata->apps, app);
        jdata->num_apps++;
        if (NULL != papp->cmd) {
            app->app = strdup(papp->cmd);
        } else if (NULL == papp->argv ||
                   NULL == papp->argv[0]) {
            ORTE_ERROR_LOG(ORTE_ERR_BAD_PARAM);
            OBJ_RELEASE(jdata);
            return ORTE_ERR_BAD_PARAM;
        } else {
            app->app = strdup(papp->argv[0]);
        }
        if (NULL != papp->argv) {
            app->argv = opal_argv_copy(papp->argv);
        }
        if (NULL != papp->env) {
            app->env = opal_argv_copy(papp->env);
        }
        if (NULL != papp->cwd) {
            app->cwd = strdup(papp->cwd);
        }
        app->num_procs = papp->maxprocs;
        OPAL_LIST_FOREACH(info, &papp->info, opal_value_t) {
            if (0 == strcmp(info->key, OPAL_PMIX_HOST)) {
                orte_set_attribute(&app->attributes, ORTE_APP_DASH_HOST,
                                   ORTE_ATTR_GLOBAL, info->data.string, OPAL_STRING);
            } else if (0 == strcmp(info->key, OPAL_PMIX_HOSTFILE)) {
                orte_set_attribute(&app->attributes, ORTE_APP_HOSTFILE,
                                   ORTE_ATTR_GLOBAL, info->data.string, OPAL_STRING);
            } else if (0 == strcmp(info->key, OPAL_PMIX_ADD_HOSTFILE)) {
                orte_set_attribute(&app->attributes, ORTE_APP_ADD_HOSTFILE,
                                   ORTE_ATTR_GLOBAL, info->data.string, OPAL_STRING);
            } else if (0 == strcmp(info->key, OPAL_PMIX_ADD_HOST)) {
                orte_set_attribute(&app->attributes, ORTE_APP_ADD_HOST,
                                   ORTE_ATTR_GLOBAL, info->data.string, OPAL_STRING);
            } else if (0 == strcmp(info->key, OPAL_PMIX_PREFIX)) {
                orte_set_attribute(&app->attributes, ORTE_APP_PREFIX_DIR,
                                   ORTE_ATTR_GLOBAL, info->data.string, OPAL_STRING);
            } else if (0 == strcmp(info->key, OPAL_PMIX_WDIR)) {
                /* if this is a relative path, convert it to an absolute path */
                if (opal_path_is_absolute(info->data.string)) {
                    app->cwd = strdup(info->data.string);
                } else {
                    /* get the cwd */
                    if (OPAL_SUCCESS != (rc = opal_getcwd(cwd, sizeof(cwd)))) {
                        orte_show_help("help-orted.txt", "cwd", true, "spawn", rc);
                        OBJ_RELEASE(jdata);
                        return rc;
                    }
                    /* construct the absolute path */
                    app->cwd = opal_os_path(false, cwd, info->data.string, NULL);
                }
            } else if (0 == strcmp(info->key, OPAL_PMIX_PRELOAD_BIN)) {
                OPAL_CHECK_BOOL(info, flag);
                orte_set_attribute(&app->attributes, ORTE_APP_PRELOAD_BIN,
                                   ORTE_ATTR_GLOBAL, &flag, OPAL_BOOL);
            } else if (0 == strcmp(info->key, OPAL_PMIX_PRELOAD_FILES)) {
                orte_set_attribute(&app->attributes, ORTE_APP_PRELOAD_FILES,
                                   ORTE_ATTR_GLOBAL, info->data.string, OPAL_STRING);

            /***   ENVIRONMENTAL VARIABLE DIRECTIVES   ***/
            /* there can be multiple of these, so we add them to the attribute list */
            } else if (0 == strcmp(info->key, OPAL_PMIX_SET_ENVAR)) {
                orte_add_attribute(&app->attributes, ORTE_APP_SET_ENVAR,
                                   ORTE_ATTR_GLOBAL, &info->data.envar, OPAL_ENVAR);
            } else if (0 == strcmp(info->key, OPAL_PMIX_ADD_ENVAR)) {
                orte_add_attribute(&app->attributes, ORTE_APP_ADD_ENVAR,
                                   ORTE_ATTR_GLOBAL, &info->data.envar, OPAL_ENVAR);
            } else if (0 == strcmp(info->key, OPAL_PMIX_UNSET_ENVAR)) {
                orte_add_attribute(&app->attributes, ORTE_APP_UNSET_ENVAR,
                                   ORTE_ATTR_GLOBAL, info->data.string, OPAL_STRING);
            } else if (0 == strcmp(info->key, OPAL_PMIX_PREPEND_ENVAR)) {
                orte_add_attribute(&app->attributes, ORTE_APP_PREPEND_ENVAR,
                                   ORTE_ATTR_GLOBAL, &info->data.envar, OPAL_ENVAR);
            } else if (0 == strcmp(info->key, OPAL_PMIX_APPEND_ENVAR)) {
                orte_add_attribute(&app->attributes, ORTE_APP_APPEND_ENVAR,
                                   ORTE_ATTR_GLOBAL, &info->data.envar, OPAL_ENVAR);

            } else {
                /* unrecognized key */
                orte_show_help("help-orted.txt", "bad-key",
                               true, "spawn", "application", info->key);
           }
        }
    }

    /* transfer the job info across */
    OPAL_LIST_FOREACH_SAFE(info, next, job_info, opal_value_t) {
        /***   PERSONALITY   ***/
        if (0 == strcmp(info->key, OPAL_PMIX_PERSONALITY)) {
            jdata->personality = opal_argv_split(info->data.string, ',');

        /***   REQUESTED MAPPER   ***/
        } else if (0 == strcmp(info->key, OPAL_PMIX_MAPPER)) {
            jdata->map->req_mapper = strdup(info->data.string);

        /***   DISPLAY MAP   ***/
        } else if (0 == strcmp(info->key, OPAL_PMIX_DISPLAY_MAP)) {
            OPAL_CHECK_BOOL(info, jdata->map->display_map);

        /***   PPR (PROCS-PER-RESOURCE)   ***/
        } else if (0 == strcmp(info->key, OPAL_PMIX_PPR)) {
            if (ORTE_MAPPING_POLICY_IS_SET(jdata->map->mapping)) {
                /* not allowed to provide multiple mapping policies */
                orte_show_help("help-orte-rmaps-base.txt", "redefining-policy",
                               true, "mapping", info->data.string,
                               orte_rmaps_base_print_mapping(orte_rmaps_base.mapping));
                return ORTE_ERR_BAD_PARAM;
            }
            ORTE_SET_MAPPING_DIRECTIVE(jdata->map->mapping, ORTE_MAPPING_PPR);
            jdata->map->ppr = strdup(info->data.string);

        /***   MAP-BY   ***/
        } else if (0 == strcmp(info->key, OPAL_PMIX_MAPBY)) {
            if (ORTE_MAPPING_POLICY_IS_SET(jdata->map->mapping)) {
                /* not allowed to provide multiple mapping policies */
                orte_show_help("help-orte-rmaps-base.txt", "redefining-policy",
                               true, "mapping", info->data.string,
                               orte_rmaps_base_print_mapping(orte_rmaps_base.mapping));
                return ORTE_ERR_BAD_PARAM;
            }
            rc = orte_rmaps_base_set_mapping_policy(jdata, &jdata->map->mapping,
                                                    NULL, info->data.string);
            if (ORTE_SUCCESS != rc) {
                return rc;
            }

        /***   RANK-BY   ***/
        } else if (0 == strcmp(info->key, OPAL_PMIX_RANKBY)) {
            if (ORTE_RANKING_POLICY_IS_SET(jdata->map->ranking)) {
                /* not allowed to provide multiple ranking policies */
                orte_show_help("help-orte-rmaps-base.txt", "redefining-policy",
                               true, "ranking", info->data.string,
                               orte_rmaps_base_print_ranking(orte_rmaps_base.ranking));
                return ORTE_ERR_BAD_PARAM;
            }
            rc = orte_rmaps_base_set_ranking_policy(&jdata->map->ranking,
                                                    jdata->map->mapping,
                                                    info->data.string);
            if (ORTE_SUCCESS != rc) {
                return rc;
            }

        /***   BIND-TO   ***/
        } else if (0 == strcmp(info->key, OPAL_PMIX_BINDTO)) {
            if (OPAL_BINDING_POLICY_IS_SET(jdata->map->binding)) {
                /* not allowed to provide multiple mapping policies */
                orte_show_help("help-opal-hwloc-base.txt", "redefining-policy", true,
                               info->data.string,
                               opal_hwloc_base_print_binding(opal_hwloc_binding_policy));
                return ORTE_ERR_BAD_PARAM;
            }
            rc = opal_hwloc_base_set_binding_policy(&jdata->map->binding,
                                                    info->data.string);
            if (ORTE_SUCCESS != rc) {
                return rc;
            }

        /***   CPUS/RANK   ***/
        } else if (0 == strcmp(info->key, OPAL_PMIX_CPUS_PER_PROC)) {
            jdata->map->cpus_per_rank = info->data.uint32;

        /***   NO USE LOCAL   ***/
        } else if (0 == strcmp(info->key, OPAL_PMIX_NO_PROCS_ON_HEAD)) {
            OPAL_CHECK_BOOL(info, flag);
            if (flag) {
                ORTE_SET_MAPPING_DIRECTIVE(jdata->map->mapping, ORTE_MAPPING_NO_USE_LOCAL);
            } else {
                ORTE_UNSET_MAPPING_DIRECTIVE(jdata->map->mapping, ORTE_MAPPING_NO_USE_LOCAL);
            }
            /* mark that the user specified it */
            ORTE_SET_MAPPING_DIRECTIVE(jdata->map->mapping, ORTE_MAPPING_LOCAL_GIVEN);

        /***   OVERSUBSCRIBE   ***/
        } else if (0 == strcmp(info->key, OPAL_PMIX_NO_OVERSUBSCRIBE)) {
            OPAL_CHECK_BOOL(info, flag);
            if (flag) {
                ORTE_SET_MAPPING_DIRECTIVE(jdata->map->mapping, ORTE_MAPPING_NO_OVERSUBSCRIBE);
            } else {
                ORTE_UNSET_MAPPING_DIRECTIVE(jdata->map->mapping, ORTE_MAPPING_NO_OVERSUBSCRIBE);
            }
            /* mark that the user specified it */
            ORTE_SET_MAPPING_DIRECTIVE(jdata->map->mapping, ORTE_MAPPING_SUBSCRIBE_GIVEN);

        /***   REPORT BINDINGS  ***/
        } else if (0 == strcmp(info->key, OPAL_PMIX_REPORT_BINDINGS)) {
            OPAL_CHECK_BOOL(info, flag);
            orte_set_attribute(&jdata->attributes, ORTE_JOB_REPORT_BINDINGS,
                               ORTE_ATTR_GLOBAL, &flag, OPAL_BOOL);

        /***   CPU LIST  ***/
        } else if (0 == strcmp(info->key, OPAL_PMIX_CPU_LIST)) {
            orte_set_attribute(&jdata->attributes, ORTE_JOB_CPU_LIST,
                               ORTE_ATTR_GLOBAL, info->data.string, OPAL_BOOL);

        /***   RECOVERABLE  ***/
        } else if (0 == strcmp(info->key, OPAL_PMIX_JOB_RECOVERABLE)) {
            OPAL_CHECK_BOOL(info, flag);
            if (flag) {
                ORTE_FLAG_SET(jdata, ORTE_JOB_FLAG_RECOVERABLE);
            } else {
                ORTE_FLAG_UNSET(jdata, ORTE_JOB_FLAG_RECOVERABLE);
            }

        /***   MAX RESTARTS  ***/
        } else if (0 == strcmp(info->key, OPAL_PMIX_MAX_RESTARTS)) {
            for (i=0; i < jdata->apps->size; i++) {
                if (NULL == (app = (orte_app_context_t*)opal_pointer_array_get_item(jdata->apps, i))) {
                    continue;
                }
                orte_set_attribute(&app->attributes, ORTE_APP_MAX_RESTARTS,
                                   ORTE_ATTR_GLOBAL, &info->data.uint32, OPAL_INT32);
            }

        /***   CONTINUOUS OPERATION  ***/
        } else if (0 == strcmp(info->key, OPAL_PMIX_JOB_CONTINUOUS)) {
            OPAL_CHECK_BOOL(info, flag);
            orte_set_attribute(&jdata->attributes, ORTE_JOB_CONTINUOUS_OP,
                               ORTE_ATTR_GLOBAL, &flag, OPAL_BOOL);

        /***   NON-PMI JOB   ***/
        } else if (0 == strcmp(info->key, OPAL_PMIX_NON_PMI)) {
            OPAL_CHECK_BOOL(info, flag);
            orte_set_attribute(&jdata->attributes, ORTE_JOB_NON_ORTE_JOB,
                               ORTE_ATTR_GLOBAL, &flag, OPAL_BOOL);

        /***   SPAWN REQUESTOR IS TOOL   ***/
        } else if (0 == strcmp(info->key, OPAL_PMIX_REQUESTOR_IS_TOOL)) {
            OPAL_CHECK_BOOL(info, flag);
            orte_set_attribute(&jdata->attributes, ORTE_JOB_DVM_JOB,
                               ORTE_ATTR_GLOBAL, &flag, OPAL_BOOL);
            if (flag) {
                /* request that IO be forwarded to the requesting tool */
                orte_set_attribute(&jdata->attributes, ORTE_JOB_FWDIO_TO_TOOL,
                                   ORTE_ATTR_GLOBAL, &flag, OPAL_BOOL);
            }

        /***   NOTIFY UPON JOB COMPLETION   ***/
        } else if (0 == strcmp(info->key, OPAL_PMIX_NOTIFY_COMPLETION)) {
            OPAL_CHECK_BOOL(info, flag);
            orte_set_attribute(&jdata->attributes, ORTE_JOB_NOTIFY_COMPLETION,
                               ORTE_ATTR_GLOBAL, &flag, OPAL_BOOL);

        /***   STOP ON EXEC FOR DEBUGGER   ***/
        } else if (0 == strcmp(info->key, OPAL_PMIX_DEBUG_STOP_ON_EXEC)) {
            /* we don't know how to do this */
            return ORTE_ERR_NOT_SUPPORTED;

        /***   TAG STDOUT   ***/
        } else if (0 == strcmp(info->key, OPAL_PMIX_TAG_OUTPUT)) {
            OPAL_CHECK_BOOL(info, flag);
            orte_set_attribute(&jdata->attributes, ORTE_JOB_TAG_OUTPUT,
                               ORTE_ATTR_GLOBAL, &flag, OPAL_BOOL);

        /***   TIMESTAMP OUTPUT   ***/
        } else if (0 == strcmp(info->key, OPAL_PMIX_TIMESTAMP_OUTPUT)) {
            OPAL_CHECK_BOOL(info, flag);
            orte_set_attribute(&jdata->attributes, ORTE_JOB_TIMESTAMP_OUTPUT,
                               ORTE_ATTR_GLOBAL, &flag, OPAL_BOOL);

        /***   OUTPUT TO FILES   ***/
        } else if (0 == strcmp(info->key, OPAL_PMIX_OUTPUT_TO_FILE)) {
            orte_set_attribute(&jdata->attributes, ORTE_JOB_OUTPUT_TO_FILE,
                               ORTE_ATTR_GLOBAL, info->data.string, OPAL_STRING);

        /***   MERGE STDERR TO STDOUT   ***/
        } else if (0 == strcmp(info->key, OPAL_PMIX_MERGE_STDERR_STDOUT)) {
            OPAL_CHECK_BOOL(info, flag);
            orte_set_attribute(&jdata->attributes, ORTE_JOB_MERGE_STDERR_STDOUT,
                               ORTE_ATTR_GLOBAL, &flag, OPAL_BOOL);

        /***   STDIN TARGET   ***/
        } else if (0 == strcmp(info->key, OPAL_PMIX_STDIN_TGT)) {
            if (0 == strcmp(info->data.string, "all")) {
                jdata->stdin_target = ORTE_VPID_WILDCARD;
            } else if (0 == strcmp(info->data.string, "none")) {
                jdata->stdin_target = ORTE_VPID_INVALID;
            } else {
                jdata->stdin_target = strtoul(info->data.string, NULL, 10);
            }

        /***   INDEX ARGV   ***/
        } else if (0 == strcmp(info->key, OPAL_PMIX_INDEX_ARGV)) {
            OPAL_CHECK_BOOL(info, flag);
            orte_set_attribute(&jdata->attributes, ORTE_JOB_INDEX_ARGV,
                               ORTE_ATTR_GLOBAL, &flag, OPAL_BOOL);

        /***   DEBUGGER DAEMONS   ***/
        } else if (0 == strcmp(info->key, OPAL_PMIX_DEBUGGER_DAEMONS)) {
            ORTE_FLAG_SET(jdata, ORTE_JOB_FLAG_DEBUGGER_DAEMON);
            ORTE_SET_MAPPING_DIRECTIVE(jdata->map->mapping, ORTE_MAPPING_DEBUGGER);

        /***   ENVIRONMENTAL VARIABLE DIRECTIVES   ***/
        /* there can be multiple of these, so we add them to the attribute list */
        } else if (0 == strcmp(info->key, OPAL_PMIX_SET_ENVAR)) {
            orte_add_attribute(&jdata->attributes, ORTE_JOB_SET_ENVAR,
                               ORTE_ATTR_GLOBAL, &info->data.envar, OPAL_ENVAR);
        } else if (0 == strcmp(info->key, OPAL_PMIX_ADD_ENVAR)) {
            orte_add_attribute(&jdata->attributes, ORTE_JOB_ADD_ENVAR,
                               ORTE_ATTR_GLOBAL, &info->data.envar, OPAL_ENVAR);
        } else if (0 == strcmp(info->key, OPAL_PMIX_UNSET_ENVAR)) {
            orte_add_attribute(&jdata->attributes, ORTE_JOB_UNSET_ENVAR,
                               ORTE_ATTR_GLOBAL, info->data.string, OPAL_STRING);
        } else if (0 == strcmp(info->key, OPAL_PMIX_PREPEND_ENVAR)) {
            orte_add_attribute(&jdata->attributes, ORTE_JOB_PREPEND_ENVAR,
                               ORTE_ATTR_GLOBAL, &info->data.envar, OPAL_ENVAR);
        } else if (0 == strcmp(info->key, OPAL_PMIX_APPEND_ENVAR)) {
            orte_add_attribute(&jdata->attributes, ORTE_JOB_APPEND_ENVAR,
                               ORTE_ATTR_GLOBAL, &info->data.envar, OPAL_ENVAR);

        /***   DEFAULT - CACHE FOR INCLUSION WITH JOB INFO   ***/
        } else {
            /* cache for inclusion with job info at registration */
            cache = NULL;
            opal_list_remove_item(job_info, &info->super);
            if (orte_get_attribute(&jdata->attributes, ORTE_JOB_INFO_CACHE, (void**)&cache, OPAL_PTR) &&
                NULL != cache) {
                opal_list_append(cache, &info->super);
            } else {
                cache = OBJ_NEW(opal_list_t);
                opal_list_append(cache, &info->super);
                orte_set_attribute(&jdata->attributes, ORTE_JOB_INFO_CACHE, ORTE_ATTR_LOCAL, (void*)cache, OPAL_PTR);
            }
        }
    }
    /* if the job is missing a personality setting, add it */
    if (NULL == jdata->personality) {
        opal_argv_append_nosize(&jdata->personality, "ompi");
    }

    /* indicate the requestor so bookmarks can be correctly set */
    orte_set_attribute(&jdata->attributes, ORTE_JOB_LAUNCH_PROXY,
                       ORTE_ATTR_GLOBAL, requestor, OPAL_NAME);

    /* setup a spawn tracker so we know who to call back when this is done
     * and thread-shift the entire thing so it can be safely added to
     * our tracking list */
    ORTE_SPN_REQ(jdata, spawn, cbfunc, cbdata);

    return OPAL_SUCCESS;
}

static void _cnct(int sd, short args, void *cbdata);

static void _cnlk(int status, opal_list_t *data, void *cbdata)
{
    orte_pmix_server_op_caddy_t *cd = (orte_pmix_server_op_caddy_t*)cbdata;
    int rc, cnt;
    opal_pmix_pdata_t *pdat;
    orte_job_t *jdata;
    orte_node_t *node;
    orte_proc_t *proc;
    opal_buffer_t buf, bucket;
    opal_byte_object_t *bo;
    orte_process_name_t dmn, pname;
    char *uri;
    opal_value_t val;
    opal_list_t nodes;

    ORTE_ACQUIRE_OBJECT(cd);

    /* if we failed to get the required data, then just inform
     * the embedded server that the connect cannot succeed */
    if (ORTE_SUCCESS != status || NULL == data) {
        if (NULL != cd->cbfunc) {
            rc = status;
            goto release;
        }
    }

    /* register the returned data with the embedded PMIx server */
    pdat = (opal_pmix_pdata_t*)opal_list_get_first(data);
    if (OPAL_BYTE_OBJECT != pdat->value.type) {
        rc = ORTE_ERR_BAD_PARAM;
        ORTE_ERROR_LOG(rc);
        goto release;
    }
    /* the data will consist of a packed buffer with the job data in it */
    OBJ_CONSTRUCT(&buf, opal_buffer_t);
    opal_dss.load(&buf, pdat->value.data.bo.bytes, pdat->value.data.bo.size);
    pdat->value.data.bo.bytes = NULL;
    pdat->value.data.bo.size = 0;
    cnt = 1;
    if (OPAL_SUCCESS != (rc = opal_dss.unpack(&buf, &jdata, &cnt, ORTE_JOB))) {
        ORTE_ERROR_LOG(rc);
        OBJ_DESTRUCT(&buf);
        goto release;
    }

    /* unpack the byte object containing the daemon uri's */
    cnt=1;
    if (ORTE_SUCCESS != (rc = opal_dss.unpack(&buf, &bo, &cnt, OPAL_BYTE_OBJECT))) {
        ORTE_ERROR_LOG(rc);
        OBJ_DESTRUCT(&buf);
        goto release;
    }
    /* load it into a buffer */
    OBJ_CONSTRUCT(&bucket, opal_buffer_t);
    opal_dss.load(&bucket, bo->bytes, bo->size);
    bo->bytes = NULL;
    free(bo);
    /* prep a list to save the nodes */
    OBJ_CONSTRUCT(&nodes, opal_list_t);
    /* unpack and store the URI's */
    cnt = 1;
    while (OPAL_SUCCESS == (rc = opal_dss.unpack(&bucket, &uri, &cnt, OPAL_STRING))) {
        rc = orte_rml_base_parse_uris(uri, &dmn, NULL);
        if (ORTE_SUCCESS != rc) {
            OBJ_DESTRUCT(&buf);
            OBJ_DESTRUCT(&bucket);
            goto release;
        }
        /* save a node object for this daemon */
        node = OBJ_NEW(orte_node_t);
        node->daemon = OBJ_NEW(orte_proc_t);
        memcpy(&node->daemon->name, &dmn, sizeof(orte_process_name_t));
        opal_list_append(&nodes, &node->super);
        /* register the URI */
        OBJ_CONSTRUCT(&val, opal_value_t);
        val.key = OPAL_PMIX_PROC_URI;
        val.type = OPAL_STRING;
        val.data.string = uri;
        if (OPAL_SUCCESS != (rc = opal_pmix.store_local(&dmn, &val))) {
            ORTE_ERROR_LOG(rc);
            val.key = NULL;
            val.data.string = NULL;
            OBJ_DESTRUCT(&val);
            OBJ_DESTRUCT(&buf);
            OBJ_DESTRUCT(&bucket);
            goto release;
        }
        val.key = NULL;
        val.data.string = NULL;
        OBJ_DESTRUCT(&val);
        cnt = 1;
    }
    OBJ_DESTRUCT(&bucket);

    /* unpack the proc-to-daemon map */
    cnt=1;
    if (ORTE_SUCCESS != (rc = opal_dss.unpack(&buf, &bo, &cnt, OPAL_BYTE_OBJECT))) {
        ORTE_ERROR_LOG(rc);
        OBJ_DESTRUCT(&buf);
        goto release;
    }
    /* load it into a buffer */
    OBJ_CONSTRUCT(&bucket, opal_buffer_t);
    opal_dss.load(&bucket, bo->bytes, bo->size);
    bo->bytes = NULL;
    free(bo);
    /* unpack and store the map */
    cnt = 1;
    while (OPAL_SUCCESS == (rc = opal_dss.unpack(&bucket, &pname, &cnt, ORTE_NAME))) {
        /* get the name of the daemon hosting it */
        if (OPAL_SUCCESS != (rc = opal_dss.unpack(&bucket, &dmn, &cnt, ORTE_NAME))) {
            OBJ_DESTRUCT(&buf);
            OBJ_DESTRUCT(&bucket);
            goto release;
        }
        /* create the proc object */
        proc = OBJ_NEW(orte_proc_t);
        memcpy(&proc->name, &pname, sizeof(orte_process_name_t));
        opal_pointer_array_set_item(jdata->procs, pname.vpid, proc);
        /* find the daemon */
        OPAL_LIST_FOREACH(node, &nodes, orte_node_t) {
            if (node->daemon->name.vpid == dmn.vpid) {
                OBJ_RETAIN(node);
                proc->node = node;
                break;
            }
        }
    }
    OBJ_DESTRUCT(&bucket);
    OPAL_LIST_DESTRUCT(&nodes);
    OBJ_DESTRUCT(&buf);

    /* register the nspace */
    if (ORTE_SUCCESS != (rc = orte_pmix_server_register_nspace(jdata, true))) {
        ORTE_ERROR_LOG(rc);
        OBJ_RELEASE(jdata);
        goto release;
    }

    /* save the job object so we don't endlessly cycle */
    opal_hash_table_set_value_uint32(orte_job_data, jdata->jobid, jdata);

    /* restart the cnct processor */
    ORTE_PMIX_OPERATION(cd->procs, cd->info, _cnct, cd->cbfunc, cd->cbdata);
    /* protect the re-referenced data */
    cd->procs = NULL;
    cd->info = NULL;
    OBJ_RELEASE(cd);
    return;

  release:
    if (NULL != cd->cbfunc) {
        cd->cbfunc(rc, cd->cbdata);
    }
    OBJ_RELEASE(cd);
}

static void _cnct(int sd, short args, void *cbdata)
{
    orte_pmix_server_op_caddy_t *cd = (orte_pmix_server_op_caddy_t*)cbdata;
    orte_namelist_t *nm;
    char **keys = NULL, *key;
    orte_job_t *jdata;
    int rc = ORTE_SUCCESS;
    opal_value_t *kv;

    ORTE_ACQUIRE_OBJECT(cd);

    /* at some point, we need to add bookeeping to track which
     * procs are "connected" so we know who to notify upon
     * termination or failure. For now, we have to ensure
     * that we have registered all participating nspaces so
     * the embedded PMIx server can provide them to the client.
     * Otherwise, the client will receive an error as it won't
     * be able to resolve any of the required data for the
     * missing nspaces */

    /* cycle thru the procs */
    OPAL_LIST_FOREACH(nm, cd->procs, orte_namelist_t) {
        /* see if we have the job object for this job */
        if (NULL == (jdata = orte_get_job_data_object(nm->name.jobid))) {
            /* we don't know about this job. If our "global" data
             * server is just our HNP, then we have no way of finding
             * out about it, and all we can do is return an error */
            if (orte_pmix_server_globals.server.jobid == ORTE_PROC_MY_HNP->jobid &&
                orte_pmix_server_globals.server.vpid == ORTE_PROC_MY_HNP->vpid) {
                ORTE_ERROR_LOG(ORTE_ERR_NOT_SUPPORTED);
                rc = ORTE_ERR_NOT_SUPPORTED;
                goto release;
            }
            /* ask the global data server for the data - if we get it,
             * then we can complete the request */
            orte_util_convert_jobid_to_string(&key, nm->name.jobid);
            opal_argv_append_nosize(&keys, key);
            free(key);
            /* we have to add the user's id to our list of info */
            kv = OBJ_NEW(opal_value_t);
            kv->key = strdup(OPAL_PMIX_USERID);
            kv->type = OPAL_UINT32;
            kv->data.uint32 = geteuid();
            opal_list_append(cd->info, &kv->super);
            if (ORTE_SUCCESS != (rc = pmix_server_lookup_fn(&nm->name, keys, cd->info, _cnlk, cd))) {
                ORTE_ERROR_LOG(rc);
                opal_argv_free(keys);
                goto release;
            }
            opal_argv_free(keys);
            /* the callback function on this lookup will return us to this
             * routine so we can continue the process */
            return;
        }
        /* we know about the job - check to ensure it has been
         * registered with the local PMIx server */
        if (!orte_get_attribute(&jdata->attributes, ORTE_JOB_NSPACE_REGISTERED, NULL, OPAL_BOOL)) {
            /* it hasn't been registered yet, so register it now */
            if (ORTE_SUCCESS != (rc = orte_pmix_server_register_nspace(jdata, true))) {
                ORTE_ERROR_LOG(rc);
                goto release;
            }
        }
    }

  release:
    if (NULL != cd->cbfunc) {
        cd->cbfunc(rc, cd->cbdata);
    }
    OBJ_RELEASE(cd);
}

int pmix_server_connect_fn(opal_list_t *procs, opal_list_t *info,
                           opal_pmix_op_cbfunc_t cbfunc, void *cbdata)
{
    opal_output_verbose(2, orte_pmix_server_globals.output,
                        "%s connect called with %d procs",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                        (int)opal_list_get_size(procs));

    /* protect ourselves */
    if (NULL == procs || 0 == opal_list_get_size(procs)) {
        return ORTE_ERR_BAD_PARAM;
    }
    /* must thread shift this as we will be accessing global data */
    ORTE_PMIX_OPERATION(procs, info, _cnct, cbfunc, cbdata);
    return ORTE_SUCCESS;
}

static void mdxcbfunc(int status,
                      const char *data, size_t ndata, void *cbdata,
                      opal_pmix_release_cbfunc_t relcbfunc, void *relcbdata)
{
    orte_pmix_server_op_caddy_t *cd = (orte_pmix_server_op_caddy_t*)cbdata;

    ORTE_ACQUIRE_OBJECT(cd);

    /* ack the call */
    if (NULL != cd->cbfunc) {
        cd->cbfunc(status, cd->cbdata);
    }
    OBJ_RELEASE(cd);
}

int pmix_server_disconnect_fn(opal_list_t *procs, opal_list_t *info,
                              opal_pmix_op_cbfunc_t cbfunc, void *cbdata)
{
    orte_pmix_server_op_caddy_t *cd;
    int rc;

    opal_output_verbose(2, orte_pmix_server_globals.output,
                        "%s disconnect called with %d procs",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                        (int)opal_list_get_size(procs));

    /* at some point, we need to add bookeeping to track which
     * procs are "connected" so we know who to notify upon
     * termination or failure. For now, just execute a fence
     * Note that we do not need to thread-shift here as the
     * fence function will do it for us */
    cd = OBJ_NEW(orte_pmix_server_op_caddy_t);
    cd->cbfunc = cbfunc;
    cd->cbdata = cbdata;

    if (ORTE_SUCCESS != (rc = pmix_server_fencenb_fn(procs, info, NULL, 0,
                                                     mdxcbfunc, cd))) {
        OBJ_RELEASE(cd);
    }

    return rc;
}

int pmix_server_alloc_fn(const opal_process_name_t *requestor,
                         opal_pmix_alloc_directive_t dir,
                         opal_list_t *info,
                         opal_pmix_info_cbfunc_t cbfunc,
                         void *cbdata)
{
    /* ORTE currently has no way of supporting allocation requests */
    return ORTE_ERR_NOT_SUPPORTED;
}
