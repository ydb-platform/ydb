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
 * Copyright (c) 2007-2011 Oracle and/or its affiliates.  All rights reserved.
 * Copyright (c) 2011      Oak Ridge National Labs.  All rights reserved.
 * Copyright (c) 2011-2013 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2011-2018 Cisco Systems, Inc.  All rights reserved
 * Copyright (c) 2013-2018 Intel, Inc.  All rights reserved.
 * Copyright (c) 2014-2018 Research Organization for Information Science
 *                         and Technology (RIST).  All rights reserved.
 * Copyright (c) 2017      Mellanox Technologies Ltd. All rights reserved.
 * Copyright (c) 2017      IBM Corporation. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */


#include "orte_config.h"
#include "orte/constants.h"
#include "orte/types.h"

#ifdef HAVE_SYS_WAIT_H
#include <sys/wait.h>
#endif
#include <errno.h>
#ifdef HAVE_SYS_STAT_H
#include <sys/stat.h>
#endif  /* HAVE_SYS_STAT_H */
#ifdef HAVE_SYS_PARAM_H
#include <sys/param.h>
#endif
#include <time.h>

#include <signal.h>

#include "opal_stdint.h"
#include "opal/util/opal_environ.h"
#include "opal/util/argv.h"
#include "opal/util/os_dirpath.h"
#include "opal/util/os_path.h"
#include "opal/util/path.h"
#include "opal/util/sys_limits.h"
#include "opal/dss/dss.h"
#include "opal/mca/hwloc/hwloc-internal.h"
#include "opal/mca/shmem/base/base.h"
#include "opal/mca/pstat/pstat.h"
#include "opal/mca/pmix/base/base.h"

#include "orte/mca/errmgr/errmgr.h"
#include "orte/mca/rml/rml.h"
#include "orte/mca/routed/routed.h"
#include "orte/mca/iof/iof.h"
#include "orte/mca/iof/base/iof_base_setup.h"
#include "orte/mca/ess/base/base.h"
#include "orte/mca/grpcomm/base/base.h"
#include "orte/mca/plm/base/base.h"
#include "orte/mca/regx/regx.h"
#include "orte/mca/rml/base/rml_contact.h"
#include "orte/mca/rmaps/rmaps_types.h"
#include "orte/mca/rmaps/base/base.h"
#include "orte/mca/rmaps/base/rmaps_private.h"
#include "orte/mca/rtc/rtc.h"
#include "orte/mca/schizo/schizo.h"
#include "orte/mca/state/state.h"
#include "orte/mca/filem/filem.h"
#include "orte/mca/dfs/dfs.h"

#include "orte/util/context_fns.h"
#include "orte/util/name_fns.h"
#include "orte/util/session_dir.h"
#include "orte/util/proc_info.h"
#include "orte/util/show_help.h"
#include "orte/util/threads.h"
#include "orte/runtime/orte_globals.h"
#include "orte/runtime/orte_wait.h"
#include "orte/orted/orted.h"
#include "orte/orted/pmix/pmix_server.h"

#if OPAL_ENABLE_FT_CR == 1
#include "orte/mca/snapc/snapc.h"
#include "orte/mca/snapc/base/base.h"
#include "orte/mca/sstore/sstore.h"
#include "orte/mca/sstore/base/base.h"
#include "opal/mca/crs/crs.h"
#include "opal/mca/crs/base/base.h"
#endif

#include "orte/mca/odls/base/base.h"
#include "orte/mca/odls/base/odls_private.h"

static void setup_cbfunc(int status,
                         opal_list_t *info,
                         void *provided_cbdata,
                         opal_pmix_op_cbfunc_t cbfunc, void *cbdata)
{
    orte_job_t *jdata = (orte_job_t*)provided_cbdata;
    opal_value_t *kv;
    opal_buffer_t cache, *bptr;
    int rc = ORTE_SUCCESS;

    OBJ_CONSTRUCT(&cache, opal_buffer_t);
    if (NULL != info) {
        /* cycle across the provided info */
        OPAL_LIST_FOREACH(kv, info, opal_value_t) {
            if (OPAL_SUCCESS != (rc = opal_dss.pack(&cache, &kv, 1, OPAL_VALUE))) {
                ORTE_ERROR_LOG(rc);
            }
        }
    }
    /* add the results */
    bptr = &cache;
    opal_dss.pack(&jdata->launch_msg, &bptr, 1, OPAL_BUFFER);
    OBJ_DESTRUCT(&cache);

    /* release our caller */
    if (NULL != cbfunc) {
        cbfunc(rc, cbdata);
    }

    /* move to next stage */
    ORTE_ACTIVATE_JOB_STATE(jdata, ORTE_JOB_STATE_SEND_LAUNCH_MSG);

}
/* IT IS CRITICAL THAT ANY CHANGE IN THE ORDER OF THE INFO PACKED IN
 * THIS FUNCTION BE REFLECTED IN THE CONSTRUCT_CHILD_LIST PARSER BELOW
*/
int orte_odls_base_default_get_add_procs_data(opal_buffer_t *buffer,
                                              orte_jobid_t job)
{
    int rc, v;
    orte_job_t *jdata=NULL, *jptr;
    orte_job_map_t *map=NULL;
    opal_buffer_t *wireup, jobdata, priorjob;
    opal_byte_object_t bo, *boptr;
    int32_t numbytes;
    int8_t flag;
    void *nptr;
    uint32_t key;
    char *nidmap;
    orte_proc_t *dmn, *proc;
    opal_value_t *val = NULL, *kv;
    opal_list_t *modex;
    int n;

    /* get the job data pointer */
    if (NULL == (jdata = orte_get_job_data_object(job))) {
        ORTE_ERROR_LOG(ORTE_ERR_BAD_PARAM);
        return ORTE_ERR_BAD_PARAM;
    }

    /* get a pointer to the job map */
    map = jdata->map;
    /* if there is no map, just return */
    if (NULL == map) {
        return ORTE_SUCCESS;
    }

    /* if we couldn't provide the allocation regex on the orted
     * cmd line, then we need to provide all the info here */
    if (!orte_nidmap_communicated) {
        if (ORTE_SUCCESS != (rc = orte_regx.nidmap_create(orte_node_pool, &nidmap))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }
        orte_nidmap_communicated = true;
    } else {
        nidmap = NULL;
    }
    opal_dss.pack(buffer, &nidmap, 1, OPAL_STRING);
    if (NULL != nidmap) {
        free(nidmap);
    }

    /* if we haven't already done so, provide the info on the
     * capabilities of each node */
    if (1 < orte_process_info.num_procs &&
        (!orte_node_info_communicated ||
         orte_get_attribute(&jdata->attributes, ORTE_JOB_LAUNCHED_DAEMONS, NULL, OPAL_BOOL))) {
        flag = 1;
        opal_dss.pack(buffer, &flag, 1, OPAL_INT8);
        if (ORTE_SUCCESS != (rc = orte_regx.encode_nodemap(buffer))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }
        /* get wireup info for daemons */
        if (NULL == (jptr = orte_get_job_data_object(ORTE_PROC_MY_NAME->jobid))) {
            ORTE_ERROR_LOG(ORTE_ERR_BAD_PARAM);
            return ORTE_ERR_BAD_PARAM;
        }
        wireup = OBJ_NEW(opal_buffer_t);
        /* always include data for mpirun as the daemons can't have it yet */
        val = NULL;
        if (opal_pmix.legacy_get()) {
            if (OPAL_SUCCESS != (rc = opal_pmix.get(ORTE_PROC_MY_NAME, OPAL_PMIX_PROC_URI, NULL, &val)) || NULL == val) {
                ORTE_ERROR_LOG(rc);
                OBJ_RELEASE(wireup);
                return rc;
            } else {
                /* pack the name of the daemon */
                if (ORTE_SUCCESS != (rc = opal_dss.pack(wireup, ORTE_PROC_MY_NAME, 1, ORTE_NAME))) {
                    ORTE_ERROR_LOG(rc);
                    OBJ_RELEASE(wireup);
                    return rc;
                }
                /* pack the URI */
               if (ORTE_SUCCESS != (rc = opal_dss.pack(wireup, &val->data.string, 1, OPAL_STRING))) {
                    ORTE_ERROR_LOG(rc);
                    OBJ_RELEASE(wireup);
                    return rc;
                }
                OBJ_RELEASE(val);
            }
        } else {
            if (OPAL_SUCCESS != (rc = opal_pmix.get(ORTE_PROC_MY_NAME, NULL, NULL, &val)) || NULL == val) {
                ORTE_ERROR_LOG(rc);
                OBJ_RELEASE(wireup);
                return rc;
            } else {
                /* the data is returned as a list of key-value pairs in the opal_value_t */
                if (OPAL_PTR != val->type) {
                    ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
                    OBJ_RELEASE(wireup);
                    return ORTE_ERR_NOT_FOUND;
                }
                if (ORTE_SUCCESS != (rc = opal_dss.pack(wireup, ORTE_PROC_MY_NAME, 1, ORTE_NAME))) {
                    ORTE_ERROR_LOG(rc);
                    OBJ_RELEASE(wireup);
                    return rc;
                }
                modex = (opal_list_t*)val->data.ptr;
                numbytes = (int32_t)opal_list_get_size(modex);
                if (ORTE_SUCCESS != (rc = opal_dss.pack(wireup, &numbytes, 1, OPAL_INT32))) {
                    ORTE_ERROR_LOG(rc);
                    OBJ_RELEASE(wireup);
                    return rc;
                }
                OPAL_LIST_FOREACH(kv, modex, opal_value_t) {
                    if (ORTE_SUCCESS != (rc = opal_dss.pack(wireup, &kv, 1, OPAL_VALUE))) {
                        ORTE_ERROR_LOG(rc);
                        OBJ_RELEASE(wireup);
                        return rc;
                    }
                }
                OPAL_LIST_RELEASE(modex);
                OBJ_RELEASE(val);
            }
        }
        /* if we didn't rollup the connection info, then we have
         * to provide a complete map of connection info */
        if (!orte_static_ports && !orte_fwd_mpirun_port) {
            for (v=1; v < jptr->procs->size; v++) {
                if (NULL == (dmn = (orte_proc_t*)opal_pointer_array_get_item(jptr->procs, v))) {
                    continue;
                }
                val = NULL;
                if (opal_pmix.legacy_get()) {
                    if (OPAL_SUCCESS != (rc = opal_pmix.get(&dmn->name, OPAL_PMIX_PROC_URI, NULL, &val)) || NULL == val) {
                        ORTE_ERROR_LOG(rc);
                        OBJ_RELEASE(buffer);
                        OBJ_RELEASE(wireup);
                        return rc;
                    } else {
                        /* pack the name of the daemon */
                        if (ORTE_SUCCESS != (rc = opal_dss.pack(wireup, &dmn->name, 1, ORTE_NAME))) {
                            ORTE_ERROR_LOG(rc);
                            OBJ_RELEASE(buffer);
                            OBJ_RELEASE(wireup);
                            return rc;
                        }
                        /* pack the URI */
                       if (ORTE_SUCCESS != (rc = opal_dss.pack(wireup, &val->data.string, 1, OPAL_STRING))) {
                            ORTE_ERROR_LOG(rc);
                            OBJ_RELEASE(buffer);
                            OBJ_RELEASE(wireup);
                            return rc;
                        }
                        OBJ_RELEASE(val);
                    }
                } else {
                    if (OPAL_SUCCESS != (rc = opal_pmix.get(&dmn->name, NULL, NULL, &val)) || NULL == val) {
                        ORTE_ERROR_LOG(rc);
                        OBJ_RELEASE(buffer);
                        return rc;
                    } else {
                        /* the data is returned as a list of key-value pairs in the opal_value_t */
                        if (OPAL_PTR != val->type) {
                            ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
                            OBJ_RELEASE(buffer);
                            return ORTE_ERR_NOT_FOUND;
                        }
                        if (ORTE_SUCCESS != (rc = opal_dss.pack(wireup, &dmn->name, 1, ORTE_NAME))) {
                            ORTE_ERROR_LOG(rc);
                            OBJ_RELEASE(buffer);
                            OBJ_RELEASE(wireup);
                            return rc;
                        }
                        modex = (opal_list_t*)val->data.ptr;
                        numbytes = (int32_t)opal_list_get_size(modex);
                        if (ORTE_SUCCESS != (rc = opal_dss.pack(wireup, &numbytes, 1, OPAL_INT32))) {
                            ORTE_ERROR_LOG(rc);
                            OBJ_RELEASE(buffer);
                            OBJ_RELEASE(wireup);
                            return rc;
                        }
                        OPAL_LIST_FOREACH(kv, modex, opal_value_t) {
                            if (ORTE_SUCCESS != (rc = opal_dss.pack(wireup, &kv, 1, OPAL_VALUE))) {
                                ORTE_ERROR_LOG(rc);
                                OBJ_RELEASE(buffer);
                                OBJ_RELEASE(wireup);
                                return rc;
                            }
                        }
                        OPAL_LIST_RELEASE(modex);
                        OBJ_RELEASE(val);
                    }
                }
            }
        }
        /* put it in a byte object for xmission */
        opal_dss.unload(wireup, (void**)&bo.bytes, &numbytes);
        OBJ_RELEASE(wireup);
        /* pack the byte object - zero-byte objects are fine */
        bo.size = numbytes;
        boptr = &bo;
        if (ORTE_SUCCESS != (rc = opal_dss.pack(buffer, &boptr, 1, OPAL_BYTE_OBJECT))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }
        /* release the data since it has now been copied into our buffer */
        if (NULL != bo.bytes) {
            free(bo.bytes);
        }

        /* we need to ensure that any new daemons get a complete
         * copy of all active jobs so the grpcomm collectives can
         * properly work should a proc from one of the other jobs
         * interact with this one */
        if (orte_get_attribute(&jdata->attributes, ORTE_JOB_LAUNCHED_DAEMONS, NULL, OPAL_BOOL)) {
            flag = 1;
            opal_dss.pack(buffer, &flag, 1, OPAL_INT8);
            OBJ_CONSTRUCT(&jobdata, opal_buffer_t);
            rc = opal_hash_table_get_first_key_uint32(orte_job_data, &key, (void **)&jptr, &nptr);
            while (OPAL_SUCCESS == rc) {
                /* skip the one we are launching now */
                if (NULL != jptr && jptr != jdata &&
                    ORTE_PROC_MY_NAME->jobid != jptr->jobid) {
                    OBJ_CONSTRUCT(&priorjob, opal_buffer_t);
                    /* pack the job struct */
                    if (ORTE_SUCCESS != (rc = opal_dss.pack(&priorjob, &jptr, 1, ORTE_JOB))) {
                        ORTE_ERROR_LOG(rc);
                        OBJ_DESTRUCT(&jobdata);
                        OBJ_DESTRUCT(&priorjob);
                        return rc;
                    }
                    /* pack the location of each proc */
                    for (n=0; n < jptr->procs->size; n++) {
                        if (NULL == (proc = (orte_proc_t*)opal_pointer_array_get_item(jptr->procs, n))) {
                            continue;
                        }
                        if (ORTE_SUCCESS != (rc = opal_dss.pack(&priorjob, &proc->parent, 1, ORTE_VPID))) {
                            ORTE_ERROR_LOG(rc);
                            OBJ_DESTRUCT(&jobdata);
                            OBJ_DESTRUCT(&priorjob);
                            return rc;
                        }
                    }
                    /* pack the jobdata buffer */
                    wireup = &priorjob;
                    if (ORTE_SUCCESS != (rc = opal_dss.pack(&jobdata, &wireup, 1, OPAL_BUFFER))) {
                        ORTE_ERROR_LOG(rc);
                        OBJ_DESTRUCT(&jobdata);
                        OBJ_DESTRUCT(&priorjob);
                        return rc;
                    }
                    OBJ_DESTRUCT(&priorjob);
                }
                rc = opal_hash_table_get_next_key_uint32(orte_job_data, &key, (void **)&jptr, nptr, &nptr);
            }
            /* pack the jobdata buffer */
            wireup = &jobdata;
            if (ORTE_SUCCESS != (rc = opal_dss.pack(buffer, &wireup, 1, OPAL_BUFFER))) {
                ORTE_ERROR_LOG(rc);
                OBJ_DESTRUCT(&jobdata);
                return rc;
            }
            OBJ_DESTRUCT(&jobdata);
        } else {
            flag = 0;
            opal_dss.pack(buffer, &flag, 1, OPAL_INT8);
        }
        orte_node_info_communicated = true;
    } else {
        /* mark that we didn't */
        flag = 0;
        opal_dss.pack(buffer, &flag, 1, OPAL_INT8);
        /* and that we didn't launch daemons */
        flag = 0;
        opal_dss.pack(buffer, &flag, 1, OPAL_INT8);
    }

    /* pack the job struct */
    if (ORTE_SUCCESS != (rc = opal_dss.pack(buffer, &jdata, 1, ORTE_JOB))) {
        ORTE_ERROR_LOG(rc);
        return rc;
    }

    if (!orte_get_attribute(&jdata->attributes, ORTE_JOB_FULLY_DESCRIBED, NULL, OPAL_BOOL)) {
        /* compute and pack the ppn regex */
        if (ORTE_SUCCESS != (rc = orte_regx.generate_ppn(jdata, &nidmap))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }
        if (ORTE_SUCCESS != (rc = opal_dss.pack(buffer, &nidmap, 1, OPAL_STRING))) {
            ORTE_ERROR_LOG(rc);
            free(nidmap);
            return rc;
        }
        free(nidmap);
    }

    /* get any application prep info */
    if (orte_enable_instant_on_support && NULL != opal_pmix.server_setup_application) {
        /* we don't want to block here because it could
         * take some indeterminate time to get the info */
        if (OPAL_SUCCESS != (rc = opal_pmix.server_setup_application(jdata->jobid, NULL, setup_cbfunc, jdata))) {
            ORTE_ERROR_LOG(rc);
        }
        return rc;
    }

    /* move to next stage */
    ORTE_ACTIVATE_JOB_STATE(jdata, ORTE_JOB_STATE_SEND_LAUNCH_MSG);

    return ORTE_SUCCESS;
}

static void fm_release(void *cbdata)
{
    opal_buffer_t *bptr = (opal_buffer_t*)cbdata;

    OBJ_RELEASE(bptr);
}

static void ls_cbunc(int status, void *cbdata)
{
    opal_pmix_lock_t *lock = (opal_pmix_lock_t*)cbdata;
    OPAL_PMIX_WAKEUP_THREAD(lock);
}

int orte_odls_base_default_construct_child_list(opal_buffer_t *buffer,
                                                orte_jobid_t *job)
{
    int rc;
    orte_std_cntr_t cnt;
    orte_job_t *jdata=NULL, *daemons;
    orte_node_t *node;
    orte_vpid_t dmnvpid, v;
    int32_t n;
    opal_buffer_t *bptr, *jptr;
    orte_proc_t *pptr, *dmn;
    orte_app_context_t *app;
    int8_t flag;
    char *ppn;
    opal_value_t *kv;
    opal_list_t local_support, cache;
    opal_pmix_lock_t lock;

    OPAL_OUTPUT_VERBOSE((5, orte_odls_base_framework.framework_output,
                         "%s odls:constructing child list",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));

    /* set a default response */
    *job = ORTE_JOBID_INVALID;
    /* get the daemon job object */
    daemons = orte_get_job_data_object(ORTE_PROC_MY_NAME->jobid);
    OPAL_PMIX_CONSTRUCT_LOCK(&lock);
    OBJ_CONSTRUCT(&local_support, opal_list_t);

    /* unpack the flag to see if new daemons were launched */
    cnt=1;
    if (ORTE_SUCCESS != (rc = opal_dss.unpack(buffer, &flag, &cnt, OPAL_INT8))) {
        ORTE_ERROR_LOG(rc);
        goto REPORT_ERROR;
    }

    if (0 != flag) {
        /* unpack the buffer containing the info */
        cnt=1;
        if (ORTE_SUCCESS != (rc = opal_dss.unpack(buffer, &bptr, &cnt, OPAL_BUFFER))) {
            *job = ORTE_JOBID_INVALID;
            ORTE_ERROR_LOG(rc);
            OBJ_RELEASE(bptr);
            goto REPORT_ERROR;
        }
        cnt=1;
        while (ORTE_SUCCESS == (rc = opal_dss.unpack(bptr, &jptr, &cnt, OPAL_BUFFER))) {
            /* unpack each job and add it to the local orte_job_data array */
            cnt=1;
            if (ORTE_SUCCESS != (rc = opal_dss.unpack(jptr, &jdata, &cnt, ORTE_JOB))) {
                *job = ORTE_JOBID_INVALID;
                ORTE_ERROR_LOG(rc);
                OBJ_RELEASE(bptr);
                OBJ_RELEASE(jptr);
                goto REPORT_ERROR;
            }
            /* check to see if we already have this one */
            if (NULL == orte_get_job_data_object(jdata->jobid)) {
                /* nope - add it */
                opal_hash_table_set_value_uint32(orte_job_data, jdata->jobid, jdata);
            } else {
                /* yep - so we can drop this copy */
                jdata->jobid = ORTE_JOBID_INVALID;
                OBJ_RELEASE(jdata);
                OBJ_RELEASE(jptr);
                cnt=1;
                continue;
            }
            /* unpack the location of each proc in this job */
            for (v=0; v < jdata->num_procs; v++) {
                if (NULL == (pptr = (orte_proc_t*)opal_pointer_array_get_item(jdata->procs, v))) {
                    pptr = OBJ_NEW(orte_proc_t);
                    pptr->name.jobid = jdata->jobid;
                    pptr->name.vpid = v;
                    opal_pointer_array_set_item(jdata->procs, v, pptr);
                }
                cnt=1;
                if (ORTE_SUCCESS != (rc = opal_dss.unpack(jptr, &dmnvpid, &cnt, ORTE_VPID))) {
                    ORTE_ERROR_LOG(rc);
                    OBJ_RELEASE(jptr);
                    OBJ_RELEASE(bptr);
                    goto REPORT_ERROR;
                }
                /* lookup the daemon */
                if (NULL == (dmn = (orte_proc_t*)opal_pointer_array_get_item(daemons->procs, dmnvpid))) {
                    ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
                    rc = ORTE_ERR_NOT_FOUND;
                    OBJ_RELEASE(jptr);
                    OBJ_RELEASE(bptr);
                    goto REPORT_ERROR;
                }
                /* connect the two */
                OBJ_RETAIN(dmn->node);
                pptr->node = dmn->node;
            }
            /* release the buffer */
            OBJ_RELEASE(jptr);
            cnt = 1;
        }
        OBJ_RELEASE(bptr);
    }

    /* unpack the job we are to launch */
    cnt=1;
    if (ORTE_SUCCESS != (rc = opal_dss.unpack(buffer, &jdata, &cnt, ORTE_JOB))) {
        *job = ORTE_JOBID_INVALID;
        ORTE_ERROR_LOG(rc);
        goto REPORT_ERROR;
    }
    if (ORTE_JOBID_INVALID == jdata->jobid) {
        ORTE_ERROR_LOG(ORTE_ERR_BAD_PARAM);
        rc = ORTE_ERR_BAD_PARAM;
        goto REPORT_ERROR;
    }
    *job = jdata->jobid;

    OPAL_OUTPUT_VERBOSE((5, orte_odls_base_framework.framework_output,
                         "%s odls:construct_child_list unpacking data to launch job %s",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ORTE_JOBID_PRINT(*job)));

    /* if we are the HNP, we don't need to unpack this buffer - we already
     * have all the required info in our local job array. So just build the
     * array of local children
     */
    if (ORTE_PROC_IS_HNP) {
        /* we don't want/need the extra copy of the orte_job_t, but
         * we can't just release it as that will NULL the location in
         * the orte_job_data array. So set the jobid to INVALID to
         * protect the array, and then release the object to free
         * the storage */
        jdata->jobid = ORTE_JOBID_INVALID;
        OBJ_RELEASE(jdata);
        /* get the correct job object - it will be completely filled out */
        if (NULL == (jdata = orte_get_job_data_object(*job))) {
            ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
            rc = ORTE_ERR_NOT_FOUND;
            goto REPORT_ERROR;
        }
    } else {
        opal_hash_table_set_value_uint32(orte_job_data, jdata->jobid, jdata);

        /* ensure the map object is present */
        if (NULL == jdata->map) {
            jdata->map = OBJ_NEW(orte_job_map_t);
        }
    }

    /* if the job is fully described, then mpirun will have computed
     * and sent us the complete array of procs in the orte_job_t, so we
     * don't need to do anything more here */
    if (!orte_get_attribute(&jdata->attributes, ORTE_JOB_FULLY_DESCRIBED, NULL, OPAL_BOOL)) {
        /* extract the ppn regex */
        cnt = 1;
        if (OPAL_SUCCESS != (rc = opal_dss.unpack(buffer, &ppn, &cnt, OPAL_STRING))) {
            ORTE_ERROR_LOG(rc);
            goto REPORT_ERROR;
        }

        if (!ORTE_PROC_IS_HNP) {
            /* populate the node array of the job map and the proc array of
             * the job object so we know how many procs are on each node */
            if (ORTE_SUCCESS != (rc = orte_regx.parse_ppn(jdata, ppn))) {
                ORTE_ERROR_LOG(rc);
                free(ppn);
                goto REPORT_ERROR;
            }
            /* now assign locations to the procs */
            if (ORTE_SUCCESS != (rc = orte_rmaps_base_assign_locations(jdata))) {
                ORTE_ERROR_LOG(rc);
                free(ppn);
                goto REPORT_ERROR;
            }
        }
        free(ppn);

        /* compute the ranks and add the proc objects
         * to the jdata->procs array */
        if (ORTE_SUCCESS != (rc = orte_rmaps_base_compute_vpids(jdata))) {
            ORTE_ERROR_LOG(rc);
            goto REPORT_ERROR;
        }
        /* and finally, compute the local and node ranks */
        if (ORTE_SUCCESS != (rc = orte_rmaps_base_compute_local_ranks(jdata))) {
            ORTE_ERROR_LOG(rc);
            goto REPORT_ERROR;
        }
    }

    /* unpack the buffer containing any application setup info - there
     * might not be any, so it isn't an error if we don't find things */
    cnt=1;
    rc = opal_dss.unpack(buffer, &bptr, &cnt, OPAL_BUFFER);
    if (OPAL_SUCCESS == rc) {
        /* there was setup data - process it */
        cnt=1;
        OBJ_CONSTRUCT(&cache, opal_list_t);
        while (ORTE_SUCCESS == (rc = opal_dss.unpack(bptr, &kv, &cnt, OPAL_VALUE))) {
            /* if this is an envar operation, cache it in reverse order
             * so that the order the user provided is preserved */
            if (0 == strcmp(kv->key, OPAL_PMIX_SET_ENVAR) ||
                0 == strcmp(kv->key, OPAL_PMIX_ADD_ENVAR) ||
                0 == strcmp(kv->key, OPAL_PMIX_UNSET_ENVAR) ||
                0 == strcmp(kv->key, OPAL_PMIX_PREPEND_ENVAR) ||
                0 == strcmp(kv->key, OPAL_PMIX_APPEND_ENVAR)) {
                opal_list_prepend(&cache, &kv->super);
            } else {
                /* need to pass it to pmix.setup_local_support */
                opal_list_append(&local_support, &kv->super);
            }
        }
        OBJ_RELEASE(bptr);
        /* add any cache'd values  to the front of the job attributes  */
        while (NULL != (kv = (opal_value_t*)opal_list_remove_first(&cache))) {
            if (0 == strcmp(kv->key, OPAL_PMIX_SET_ENVAR)) {
                orte_prepend_attribute(&jdata->attributes, ORTE_JOB_SET_ENVAR,
                                       ORTE_ATTR_GLOBAL, &kv->data.envar, OPAL_ENVAR);
            } else if (0 == strcmp(kv->key, OPAL_PMIX_ADD_ENVAR)) {
                orte_prepend_attribute(&jdata->attributes, ORTE_JOB_ADD_ENVAR,
                                       ORTE_ATTR_GLOBAL, &kv->data.envar, OPAL_ENVAR);
            } else if (0 == strcmp(kv->key, OPAL_PMIX_UNSET_ENVAR)) {
                orte_prepend_attribute(&jdata->attributes, ORTE_JOB_UNSET_ENVAR,
                                       ORTE_ATTR_GLOBAL, kv->data.string, OPAL_STRING);
            } else if (0 == strcmp(kv->key, OPAL_PMIX_PREPEND_ENVAR)) {
                orte_prepend_attribute(&jdata->attributes, ORTE_JOB_PREPEND_ENVAR,
                                       ORTE_ATTR_GLOBAL, &kv->data.envar, OPAL_ENVAR);
            } else if (0 == strcmp(kv->key, OPAL_PMIX_APPEND_ENVAR)) {
                orte_prepend_attribute(&jdata->attributes, ORTE_JOB_APPEND_ENVAR,
                                       ORTE_ATTR_GLOBAL, &kv->data.envar, OPAL_ENVAR);
            }
            OBJ_RELEASE(kv);
        }
        OPAL_LIST_DESTRUCT(&cache);
    }

    /* now that the node array in the job map and jdata are completely filled out,.
     * we need to "wireup" the procs to their nodes so other utilities can
     * locate them */
    for (n=0; n < jdata->procs->size; n++) {
        if (NULL == (pptr = (orte_proc_t*)opal_pointer_array_get_item(jdata->procs, n))) {
            continue;
        }
        if (ORTE_PROC_STATE_UNDEF == pptr->state) {
            /* not ready for use yet */
            continue;
        }
        if (!ORTE_PROC_IS_HNP &&
            orte_get_attribute(&jdata->attributes, ORTE_JOB_FULLY_DESCRIBED, NULL, OPAL_BOOL)) {
            /* the parser will have already made the connection, but the fully described
             * case won't have done it, so connect the proc to its node here */
            opal_output_verbose(5, orte_odls_base_framework.framework_output,
                                "%s GETTING DAEMON FOR PROC %s WITH PARENT %s",
                                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                ORTE_NAME_PRINT(&pptr->name),
                                ORTE_VPID_PRINT(pptr->parent));
            if (ORTE_VPID_INVALID == pptr->parent) {
                ORTE_ERROR_LOG(ORTE_ERR_BAD_PARAM);
                rc = ORTE_ERR_BAD_PARAM;
                goto REPORT_ERROR;
            }
            /* connect the proc to its node object */
            if (NULL == (dmn = (orte_proc_t*)opal_pointer_array_get_item(daemons->procs, pptr->parent))) {
                ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
                rc = ORTE_ERR_NOT_FOUND;
                goto REPORT_ERROR;
            }
            OBJ_RETAIN(dmn->node);
            pptr->node = dmn->node;
            /* add the node to the job map, if needed */
            if (!ORTE_FLAG_TEST(pptr->node, ORTE_NODE_FLAG_MAPPED)) {
                OBJ_RETAIN(pptr->node);
                opal_pointer_array_add(jdata->map->nodes, pptr->node);
                jdata->map->num_nodes++;
                ORTE_FLAG_SET(pptr->node, ORTE_NODE_FLAG_MAPPED);
            }
            /* add this proc to that node */
            OBJ_RETAIN(pptr);
            opal_pointer_array_add(pptr->node->procs, pptr);
            pptr->node->num_procs++;
        }
        /* see if it belongs to us */
        if (pptr->parent == ORTE_PROC_MY_NAME->vpid) {
            /* is this child on our current list of children */
            if (!ORTE_FLAG_TEST(pptr, ORTE_PROC_FLAG_LOCAL)) {
                /* not on the local list */
                OPAL_OUTPUT_VERBOSE((5, orte_odls_base_framework.framework_output,
                                     "%s[%s:%d] adding proc %s to my local list",
                                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                     __FILE__, __LINE__,
                                     ORTE_NAME_PRINT(&pptr->name)));
                /* keep tabs of the number of local procs */
                jdata->num_local_procs++;
                /* add this proc to our child list */
                OBJ_RETAIN(pptr);
                ORTE_FLAG_SET(pptr, ORTE_PROC_FLAG_LOCAL);
                opal_pointer_array_add(orte_local_children, pptr);
            }

            /* if the job is in restart mode, the child must not barrier when launched */
            if (ORTE_FLAG_TEST(jdata, ORTE_JOB_FLAG_RESTART)) {
                orte_set_attribute(&pptr->attributes, ORTE_PROC_NOBARRIER, ORTE_ATTR_LOCAL, NULL, OPAL_BOOL);
            }
            /* mark that this app_context is being used on this node */
            app = (orte_app_context_t*)opal_pointer_array_get_item(jdata->apps, pptr->app_idx);
            ORTE_FLAG_SET(app, ORTE_APP_FLAG_USED_ON_NODE);
        }
    }
    if (orte_get_attribute(&jdata->attributes, ORTE_JOB_FULLY_DESCRIBED, NULL, OPAL_BOOL)) {
        /* reset the mapped flags */
        for (n=0; n < jdata->map->nodes->size; n++) {
            if (NULL != (node = (orte_node_t*)opal_pointer_array_get_item(jdata->map->nodes, n))) {
                ORTE_FLAG_UNSET(node, ORTE_NODE_FLAG_MAPPED);
            }
        }
    }

    if (!orte_get_attribute(&jdata->attributes, ORTE_JOB_FULLY_DESCRIBED, NULL, OPAL_BOOL)) {
        /* compute and save bindings of local children */
        if (ORTE_SUCCESS != (rc = orte_rmaps_base_compute_bindings(jdata))) {
            ORTE_ERROR_LOG(rc);
            goto REPORT_ERROR;
        }
    }

    /* if we wanted to see the map, now is the time to display it */
    if (jdata->map->display_map) {
        orte_rmaps_base_display_map(jdata);
    }

    /* register this job with the PMIx server - need to wait until after we
     * have computed the #local_procs before calling the function */
    if (ORTE_SUCCESS != (rc = orte_pmix_server_register_nspace(jdata, false))) {
        ORTE_ERROR_LOG(rc);
        goto REPORT_ERROR;
    }

    /* if we have local support setup info, then execute it here - we
     * have to do so AFTER we register the nspace so the PMIx server
     * has the nspace info it needs */
    if (orte_enable_instant_on_support &&
        0 < opal_list_get_size(&local_support) &&
        NULL != opal_pmix.server_setup_local_support) {
        if (OPAL_SUCCESS != (rc = opal_pmix.server_setup_local_support(jdata->jobid, &local_support,
                                                                       ls_cbunc, &lock))) {
            ORTE_ERROR_LOG(rc);
            goto REPORT_ERROR;
        }
    } else {
        lock.active = false;  // we won't get a callback
    }

    /* if we have a file map, then we need to load it */
    if (orte_get_attribute(&jdata->attributes, ORTE_JOB_FILE_MAPS, (void**)&bptr, OPAL_BUFFER)) {
        if (NULL != orte_dfs.load_file_maps) {
            orte_dfs.load_file_maps(jdata->jobid, bptr, fm_release, bptr);
        } else {
            OBJ_RELEASE(bptr);
        }
    }

    /* load any controls into the job */
    orte_rtc.assign(jdata);

    /* spin up the spawn threads */
    orte_odls_base_start_threads(jdata);

    /* to save memory, purge the job map of all procs other than
     * our own - for daemons, this will completely release the
     * proc structures. For the HNP, the proc structs will
     * remain in the orte_job_t array */

    /* wait here until the local support has been setup */
    OPAL_PMIX_WAIT_THREAD(&lock);
    OPAL_PMIX_DESTRUCT_LOCK(&lock);
    OPAL_LIST_DESTRUCT(&local_support);
    return ORTE_SUCCESS;

  REPORT_ERROR:
    OPAL_PMIX_DESTRUCT_LOCK(&lock);
    OPAL_LIST_DESTRUCT(&local_support);
    /* we have to report an error back to the HNP so we don't just
     * hang. Although there shouldn't be any errors once this is
     * all debugged, it is still good practice to have a way
     * for it to happen - especially so developers don't have to
     * deal with the hang!
     */
    ORTE_ACTIVATE_JOB_STATE(NULL, ORTE_JOB_STATE_NEVER_LAUNCHED);
    return rc;
}

static int setup_path(orte_app_context_t *app, char **wdir)
{
    int rc=ORTE_SUCCESS;
    char dir[MAXPATHLEN];

    if (!orte_get_attribute(&app->attributes, ORTE_APP_SSNDIR_CWD, NULL, OPAL_BOOL)) {
        /* Try to change to the app's cwd and check that the app
           exists and is executable The function will
           take care of outputting a pretty error message, if required
        */
        if (ORTE_SUCCESS != (rc = orte_util_check_context_cwd(app, true))) {
            /* do not ERROR_LOG - it will be reported elsewhere */
            goto CLEANUP;
        }

        /* The prior function will have done a chdir() to jump us to
         * wherever the app is to be executed. This could be either where
         * the user specified (via -wdir), or to the user's home directory
         * on this node if nothing was provided. It seems that chdir doesn't
         * adjust the $PWD enviro variable when it changes the directory. This
         * can cause a user to get a different response when doing getcwd vs
         * looking at the enviro variable. To keep this consistent, we explicitly
         * ensure that the PWD enviro variable matches the CWD we moved to.
         *
         * NOTE: if a user's program does a chdir(), then $PWD will once
         * again not match getcwd! This is beyond our control - we are only
         * ensuring they start out matching.
         */
        getcwd(dir, sizeof(dir));
        *wdir = strdup(dir);
        opal_setenv("PWD", dir, true, &app->env);
        /* update the initial wdir value too */
        opal_setenv(OPAL_MCA_PREFIX"initial_wdir", dir, true, &app->env);
    } else {
        *wdir = NULL;
    }

 CLEANUP:
    return rc;
}


/* define a timer release point so that we can wait for
 * file descriptors to come available, if necessary
 */
static void timer_cb(int fd, short event, void *cbdata)
{
    orte_timer_t *tm = (orte_timer_t*)cbdata;
    orte_odls_launch_local_t *ll = (orte_odls_launch_local_t*)tm->payload;

    ORTE_ACQUIRE_OBJECT(tm);

    /* increment the number of retries */
    ll->retries++;

    /* re-attempt the launch */
    opal_event_active(ll->ev, OPAL_EV_WRITE, 1);

    /* release the timer event */
    OBJ_RELEASE(tm);
}

static int compute_num_procs_alive(orte_jobid_t job)
{
    int i;
    orte_proc_t *child;
    int num_procs_alive = 0;

    for (i=0; i < orte_local_children->size; i++) {
        if (NULL == (child = (orte_proc_t*)opal_pointer_array_get_item(orte_local_children, i))) {
            continue;
        }
        if (!ORTE_FLAG_TEST(child, ORTE_PROC_FLAG_ALIVE)) {
            continue;
        }
        /* do not include members of the specified job as they
         * will be added later, if required
         */
        if (job == child->name.jobid) {
            continue;
        }
        num_procs_alive++;
    }
    return num_procs_alive;
}

void orte_odls_base_spawn_proc(int fd, short sd, void *cbdata)
{
    orte_odls_spawn_caddy_t *cd = (orte_odls_spawn_caddy_t*)cbdata;
    orte_job_t *jobdat = cd->jdata;
    orte_app_context_t *app = cd->app;
    orte_proc_t *child = cd->child;
    int rc, i;
    bool found;
    orte_proc_state_t state;

    ORTE_ACQUIRE_OBJECT(cd);

    /* thread-protect common values */
    cd->env = opal_argv_copy(app->env);

    /* ensure we clear any prior info regarding state or exit status in
     * case this is a restart
     */
    child->exit_code = 0;
    ORTE_FLAG_UNSET(child, ORTE_PROC_FLAG_WAITPID);

    /* setup the pmix environment */
    if (OPAL_SUCCESS != (rc = opal_pmix.server_setup_fork(&child->name, &cd->env))) {
        ORTE_ERROR_LOG(rc);
        state = ORTE_PROC_STATE_FAILED_TO_LAUNCH;
        goto errorout;
    }

    /* if we are not forwarding output for this job, then
     * flag iof as complete
     */
    if (ORTE_FLAG_TEST(jobdat, ORTE_JOB_FLAG_FORWARD_OUTPUT)) {
        ORTE_FLAG_UNSET(child, ORTE_PROC_FLAG_IOF_COMPLETE);
    } else {
        ORTE_FLAG_SET(child, ORTE_PROC_FLAG_IOF_COMPLETE);
    }
    child->pid = 0;
    if (NULL != child->rml_uri) {
        free(child->rml_uri);
        child->rml_uri = NULL;
    }

    /* setup the rest of the environment with the proc-specific items - these
     * will be overwritten for each child
     */
    if (ORTE_SUCCESS != (rc = orte_schizo.setup_child(jobdat, child, app, &cd->env))) {
        ORTE_ERROR_LOG(rc);
        state = ORTE_PROC_STATE_FAILED_TO_LAUNCH;
        goto errorout;
    }

    /* did the user request we display output in xterms? */
    if (NULL != orte_xterm && !ORTE_FLAG_TEST(jobdat, ORTE_JOB_FLAG_DEBUGGER_DAEMON)) {
        opal_list_item_t *nmitem;
        orte_namelist_t *nm;
        /* see if this rank is one of those requested */
        found = false;
        for (nmitem = opal_list_get_first(&orte_odls_globals.xterm_ranks);
             nmitem != opal_list_get_end(&orte_odls_globals.xterm_ranks);
             nmitem = opal_list_get_next(nmitem)) {
            nm = (orte_namelist_t*)nmitem;
            if (ORTE_VPID_WILDCARD == nm->name.vpid ||
                child->name.vpid == nm->name.vpid) {
                /* we want this one - modify the app's command to include
                 * the orte xterm cmd that starts with the xtermcmd */
                cd->argv = opal_argv_copy(orte_odls_globals.xtermcmd);
                /* insert the rank into the correct place as a window title */
                free(cd->argv[2]);
                asprintf(&cd->argv[2], "Rank %s", ORTE_VPID_PRINT(child->name.vpid));
                /* add in the argv from the app */
                for (i=0; NULL != app->argv[i]; i++) {
                    opal_argv_append_nosize(&cd->argv, app->argv[i]);
                }
                /* use the xterm cmd as the app string */
                cd->cmd = strdup(orte_odls_globals.xtermcmd[0]);
                found = true;
                break;
            } else if (jobdat->num_procs <= nm->name.vpid) {  /* check for bozo case */
                /* can't be done! */
                orte_show_help("help-orte-odls-base.txt",
                               "orte-odls-base:xterm-rank-out-of-bounds",
                               true, orte_process_info.nodename,
                               nm->name.vpid, jobdat->num_procs);
                state = ORTE_PROC_STATE_FAILED_TO_LAUNCH;
                goto errorout;
            }
        }
        if (!found) {
            cd->cmd = strdup(app->app);
            cd->argv = opal_argv_copy(app->argv);
        }
    } else if (NULL != orte_fork_agent) {
        /* we were given a fork agent - use it */
        cd->argv = opal_argv_copy(orte_fork_agent);
        /* add in the argv from the app */
        for (i=0; NULL != app->argv[i]; i++) {
            opal_argv_append_nosize(&cd->argv, app->argv[i]);
        }
        cd->cmd = opal_path_findv(orte_fork_agent[0], X_OK, orte_launch_environ, NULL);
        if (NULL == cd->cmd) {
            orte_show_help("help-orte-odls-base.txt",
                           "orte-odls-base:fork-agent-not-found",
                           true, orte_process_info.nodename, orte_fork_agent[0]);
            state = ORTE_PROC_STATE_FAILED_TO_LAUNCH;
            goto errorout;
        }
    } else {
        cd->cmd = strdup(app->app);
        cd->argv = opal_argv_copy(app->argv);
    }

    /* if we are indexing the argv by rank, do so now */
    if (cd->index_argv && !ORTE_FLAG_TEST(jobdat, ORTE_JOB_FLAG_DEBUGGER_DAEMON)) {
        char *param;
        asprintf(&param, "%s-%d", cd->argv[0], (int)child->name.vpid);
        free(cd->argv[0]);
        cd->argv[0] = param;
    }

    opal_output_verbose(5, orte_odls_base_framework.framework_output,
                        "%s odls:launch spawning child %s",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                        ORTE_NAME_PRINT(&child->name));

    if (15 < opal_output_get_verbosity(orte_odls_base_framework.framework_output)) {
        /* dump what is going to be exec'd */
        opal_dss.dump(orte_odls_base_framework.framework_output, app, ORTE_APP_CONTEXT);
    }

    if (ORTE_SUCCESS != (rc = cd->fork_local(cd))) {
        /* error message already output */
        state = ORTE_PROC_STATE_FAILED_TO_START;
        goto errorout;
    }

    ORTE_ACTIVATE_PROC_STATE(&child->name, ORTE_PROC_STATE_RUNNING);
    OBJ_RELEASE(cd);
    return;

  errorout:
    ORTE_FLAG_UNSET(child, ORTE_PROC_FLAG_ALIVE);
    child->exit_code = rc;
    ORTE_ACTIVATE_PROC_STATE(&child->name, state);
    OBJ_RELEASE(cd);
}

void orte_odls_base_default_launch_local(int fd, short sd, void *cbdata)
{
    orte_app_context_t *app;
    orte_proc_t *child=NULL;
    int rc=ORTE_SUCCESS;
    char basedir[MAXPATHLEN];
    int j, idx;
    int total_num_local_procs = 0;
    orte_odls_launch_local_t *caddy = (orte_odls_launch_local_t*)cbdata;
    orte_job_t *jobdat;
    orte_jobid_t job = caddy->job;
    orte_odls_base_fork_local_proc_fn_t fork_local = caddy->fork_local;
    bool index_argv;
    char *msg;
    orte_odls_spawn_caddy_t *cd;
    opal_event_base_t *evb;
    char *effective_dir = NULL;
    char **argvptr;
    char *pathenv = NULL, *mpiexec_pathenv = NULL;
    char *full_search;

    ORTE_ACQUIRE_OBJECT(caddy);

    opal_output_verbose(5, orte_odls_base_framework.framework_output,
                        "%s local:launch",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));

    /* establish our baseline working directory - we will be potentially
     * bouncing around as we execute various apps, but we will always return
     * to this place as our default directory
     */
    getcwd(basedir, sizeof(basedir));
    /* find the jobdat for this job */
    if (NULL == (jobdat = orte_get_job_data_object(job))) {
        ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
        /* not much we can do here - we are just hosed, so
         * report that to the error manager
         */
        ORTE_ACTIVATE_JOB_STATE(NULL, ORTE_JOB_STATE_FAILED_TO_LAUNCH);
        goto ERROR_OUT;
    }

    /* do we have any local procs to launch? */
    if (0 == jobdat->num_local_procs) {
        /* indicate that we are done trying to launch them */
        opal_output_verbose(5, orte_odls_base_framework.framework_output,
                            "%s local:launch no local procs",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
        goto GETOUT;
    }

    /* track if we are indexing argvs so we don't check every time */
    index_argv = orte_get_attribute(&jobdat->attributes, ORTE_JOB_INDEX_ARGV, NULL, OPAL_BOOL);

    /* compute the total number of local procs currently alive and about to be launched */
    total_num_local_procs = compute_num_procs_alive(job) + jobdat->num_local_procs;

    /* check the system limits - if we are at our max allowed children, then
     * we won't be allowed to do this anyway, so we may as well abort now.
     * According to the documentation, num_procs = 0 is equivalent to
     * no limit, so treat it as unlimited here.
     */
    if (0 < opal_sys_limits.num_procs) {
        OPAL_OUTPUT_VERBOSE((10,  orte_odls_base_framework.framework_output,
                             "%s checking limit on num procs %d #children needed %d",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             opal_sys_limits.num_procs, total_num_local_procs));
        if (opal_sys_limits.num_procs < total_num_local_procs) {
            if (2 < caddy->retries) {
                /* if we have already tried too many times, then just give up */
                ORTE_ACTIVATE_JOB_STATE(jobdat, ORTE_JOB_STATE_FAILED_TO_LAUNCH);
                goto ERROR_OUT;
            }
            /* set a timer event so we can retry later - this
             * gives the system a chance to let other procs
             * terminate, thus creating room for new ones
             */
            ORTE_DETECT_TIMEOUT(1000, 1000, -1, timer_cb, caddy);
            return;
        }
    }

    /* check to see if we have enough available file descriptors
     * to launch these children - if not, then let's wait a little
     * while to see if some come free. This can happen if we are
     * in a tight loop over comm_spawn
     */
    if (0 < opal_sys_limits.num_files) {
        int limit;
        limit = 4*total_num_local_procs + 6*jobdat->num_local_procs;
        OPAL_OUTPUT_VERBOSE((10,  orte_odls_base_framework.framework_output,
                             "%s checking limit on file descriptors %d need %d",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             opal_sys_limits.num_files, limit));
        if (opal_sys_limits.num_files < limit) {
            if (2 < caddy->retries) {
                /* tried enough - give up */
                for (idx=0; idx < orte_local_children->size; idx++) {
                    if (NULL == (child = (orte_proc_t*)opal_pointer_array_get_item(orte_local_children, idx))) {
                        continue;
                    }
                    if (OPAL_EQUAL == opal_dss.compare(&job, &(child->name.jobid), ORTE_JOBID)) {
                        child->exit_code = ORTE_PROC_STATE_FAILED_TO_LAUNCH;
                        ORTE_ACTIVATE_PROC_STATE(&child->name, ORTE_PROC_STATE_FAILED_TO_LAUNCH);
                    }
                }
                goto ERROR_OUT;
            }
            /* don't have enough - wait a little time */
            ORTE_DETECT_TIMEOUT(1000, 1000, -1, timer_cb, caddy);
            return;
        }
    }

    for (j=0; j < jobdat->apps->size; j++) {
        if (NULL == (app = (orte_app_context_t*)opal_pointer_array_get_item(jobdat->apps, j))) {
            continue;
        }

        /* if this app isn't being used on our node, skip it */
        if (!ORTE_FLAG_TEST(app, ORTE_APP_FLAG_USED_ON_NODE)) {
            opal_output_verbose(5, orte_odls_base_framework.framework_output,
                                "%s app %d not used on node",
                                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), j);
            continue;
        }

        /* setup the environment for this app */
        if (ORTE_SUCCESS != (rc = orte_schizo.setup_fork(jobdat, app))) {

            OPAL_OUTPUT_VERBOSE((10, orte_odls_base_framework.framework_output,
                                 "%s odls:launch:setup_fork failed with error %s",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 ORTE_ERROR_NAME(rc)));

            /* do not ERROR_LOG this failure - it will be reported
             * elsewhere. The launch is going to fail. Since we could have
             * multiple app_contexts, we need to ensure that we flag only
             * the correct one that caused this operation to fail. We then have
             * to flag all the other procs from the app_context as having "not failed"
             * so we can report things out correctly
             */
            /* cycle through children to find those for this jobid */
            for (idx=0; idx < orte_local_children->size; idx++) {
                if (NULL == (child = (orte_proc_t*)opal_pointer_array_get_item(orte_local_children, idx))) {
                    continue;
                }
                if (OPAL_EQUAL == opal_dss.compare(&job, &(child->name.jobid), ORTE_JOBID) &&
                    j == (int)child->app_idx) {
                    child->exit_code = ORTE_PROC_STATE_FAILED_TO_LAUNCH;
                    ORTE_ACTIVATE_PROC_STATE(&child->name, ORTE_PROC_STATE_FAILED_TO_LAUNCH);
                }
            }
            goto GETOUT;
        }

        /* setup the working directory for this app - will jump us
         * to that directory
         */
        if (ORTE_SUCCESS != (rc = setup_path(app, &effective_dir))) {
            OPAL_OUTPUT_VERBOSE((5, orte_odls_base_framework.framework_output,
                                 "%s odls:launch:setup_path failed with error %s(%d)",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 ORTE_ERROR_NAME(rc), rc));
            /* do not ERROR_LOG this failure - it will be reported
             * elsewhere. The launch is going to fail. Since we could have
             * multiple app_contexts, we need to ensure that we flag only
             * the correct one that caused this operation to fail. We then have
             * to flag all the other procs from the app_context as having "not failed"
             * so we can report things out correctly
             */
            /* cycle through children to find those for this jobid */
            for (idx=0; idx < orte_local_children->size; idx++) {
                if (NULL == (child = (orte_proc_t*)opal_pointer_array_get_item(orte_local_children, idx))) {
                    continue;
                }
                if (OPAL_EQUAL == opal_dss.compare(&job, &(child->name.jobid), ORTE_JOBID) &&
                    j == (int)child->app_idx) {
                    child->exit_code = rc;
                    ORTE_ACTIVATE_PROC_STATE(&child->name, ORTE_PROC_STATE_FAILED_TO_LAUNCH);
                }
            }
            goto GETOUT;
        }

        /* setup any local files that were prepositioned for us */
        if (ORTE_SUCCESS != (rc = orte_filem.link_local_files(jobdat, app))) {
            /* cycle through children to find those for this jobid */
            for (idx=0; idx < orte_local_children->size; idx++) {
                if (NULL == (child = (orte_proc_t*)opal_pointer_array_get_item(orte_local_children, idx))) {
                    continue;
                }
                if (OPAL_EQUAL == opal_dss.compare(&job, &(child->name.jobid), ORTE_JOBID) &&
                    j == (int)child->app_idx) {
                    child->exit_code = rc;
                    ORTE_ACTIVATE_PROC_STATE(&child->name, ORTE_PROC_STATE_FAILED_TO_LAUNCH);
                }
            }
            goto GETOUT;
        }

        /* Search for the OMPI_exec_path and PATH settings in the environment. */
        for (argvptr = app->env; *argvptr != NULL; argvptr++) {
            if (0 == strncmp("OMPI_exec_path=", *argvptr, 15)) {
                mpiexec_pathenv = *argvptr + 15;
            }
            if (0 == strncmp("PATH=", *argvptr, 5)) {
                pathenv = *argvptr + 5;
            }
        }

        /* If OMPI_exec_path is set (meaning --path was used), then create a
           temporary environment to be used in the search for the executable.
           The PATH setting in this temporary environment is a combination of
           the OMPI_exec_path and PATH values.  If OMPI_exec_path is not set,
           then just use existing environment with PATH in it.  */
        if (NULL != mpiexec_pathenv) {
            argvptr = NULL;
            if (pathenv != NULL) {
                asprintf(&full_search, "%s:%s", mpiexec_pathenv, pathenv);
            } else {
                asprintf(&full_search, "%s", mpiexec_pathenv);
            }
            opal_setenv("PATH", full_search, true, &argvptr);
            free(full_search);
        } else {
            argvptr = app->env;
        }

        rc = orte_util_check_context_app(app, argvptr);
        /* do not ERROR_LOG - it will be reported elsewhere */
        if (NULL != mpiexec_pathenv) {
            opal_argv_free(argvptr);
        }
        if (ORTE_SUCCESS != rc) {
            /* cycle through children to find those for this jobid */
            for (idx=0; idx < orte_local_children->size; idx++) {
                if (NULL == (child = (orte_proc_t*)opal_pointer_array_get_item(orte_local_children, idx))) {
                    continue;
                }
                if (OPAL_EQUAL == opal_dss.compare(&job, &(child->name.jobid), ORTE_JOBID) &&
                    j == (int)child->app_idx) {
                    child->exit_code = rc;
                    ORTE_ACTIVATE_PROC_STATE(&child->name, ORTE_PROC_STATE_FAILED_TO_LAUNCH);
                }
            }
            goto GETOUT;
        }


        /* tell all children that they are being launched via ORTE */
        opal_setenv(OPAL_MCA_PREFIX"orte_launch", "1", true, &app->env);

        /* if the user requested it, set the system resource limits */
        if (OPAL_SUCCESS != (rc = opal_util_init_sys_limits(&msg))) {
            orte_show_help("help-orte-odls-default.txt", "set limit", true,
                           orte_process_info.nodename, app,
                           __FILE__, __LINE__, msg);
            /* cycle through children to find those for this jobid */
            for (idx=0; idx < orte_local_children->size; idx++) {
                if (NULL == (child = (orte_proc_t*)opal_pointer_array_get_item(orte_local_children, idx))) {
                    continue;
                }
                if (OPAL_EQUAL == opal_dss.compare(&job, &(child->name.jobid), ORTE_JOBID) &&
                    j == (int)child->app_idx) {
                    child->exit_code = rc;
                    ORTE_ACTIVATE_PROC_STATE(&child->name, ORTE_PROC_STATE_FAILED_TO_LAUNCH);
                }
            }
            goto GETOUT;
        }

        /* reset our working directory back to our default location - if we
         * don't do this, then we will be looking for relative paths starting
         * from the last wdir option specified by the user. Thus, we would
         * be requiring that the user keep track on the cmd line of where
         * each app was located relative to the prior app, instead of relative
         * to their current location
         */
        chdir(basedir);

        /* okay, now let's launch all the local procs for this app using the provided fork_local fn */
        for (idx=0; idx < orte_local_children->size; idx++) {
            if (NULL == (child = (orte_proc_t*)opal_pointer_array_get_item(orte_local_children, idx))) {
                continue;
            }
            /* does this child belong to this app? */
            if (j != (int)child->app_idx) {
                continue;
            }

            /* is this child already alive? This can happen if
             * we are asked to launch additional processes.
             * If it has been launched, then do nothing
             */
            if (ORTE_FLAG_TEST(child, ORTE_PROC_FLAG_ALIVE)) {

                OPAL_OUTPUT_VERBOSE((5, orte_odls_base_framework.framework_output,
                                     "%s odls:launch child %s has already been launched",
                                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                     ORTE_NAME_PRINT(&child->name)));

                continue;
            }
            /* is this child a candidate to start? it may not be alive
             * because it already executed
             */
            if (ORTE_PROC_STATE_INIT != child->state &&
                ORTE_PROC_STATE_RESTART != child->state) {
                continue;
            }
            /* do we have a child from the specified job. Because the
             * job could be given as a WILDCARD value, we must use
             * the dss.compare function to check for equality.
             */
            if (OPAL_EQUAL != opal_dss.compare(&job, &(child->name.jobid), ORTE_JOBID)) {

                OPAL_OUTPUT_VERBOSE((5, orte_odls_base_framework.framework_output,
                                     "%s odls:launch child %s is not in job %s being launched",
                                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                     ORTE_NAME_PRINT(&child->name),
                                     ORTE_JOBID_PRINT(job)));

                continue;
            }

            OPAL_OUTPUT_VERBOSE((5, orte_odls_base_framework.framework_output,
                                 "%s odls:launch working child %s",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 ORTE_NAME_PRINT(&child->name)));

            /* determine the thread that will handle this child */
            ++orte_odls_globals.next_base;
            if (orte_odls_globals.num_threads <= orte_odls_globals.next_base) {
                orte_odls_globals.next_base = 0;
            }
            evb = orte_odls_globals.ev_bases[orte_odls_globals.next_base];

            /* set the waitpid callback here for thread protection and
             * to ensure we can capture the callback on shortlived apps */
            ORTE_FLAG_SET(child, ORTE_PROC_FLAG_ALIVE);
            orte_wait_cb(child, orte_odls_base_default_wait_local_proc, evb, NULL);

            /* dispatch this child to the next available launch thread */
            cd = OBJ_NEW(orte_odls_spawn_caddy_t);
            if (NULL != effective_dir) {
                cd->wdir = strdup(effective_dir);
            }
            cd->jdata = jobdat;
            cd->app = app;
            cd->child = child;
            cd->fork_local = fork_local;
            cd->index_argv = index_argv;
            /* setup any IOF */
            cd->opts.usepty = OPAL_ENABLE_PTY_SUPPORT;

            /* do we want to setup stdin? */
            if (jobdat->stdin_target == ORTE_VPID_WILDCARD ||
                 child->name.vpid == jobdat->stdin_target) {
                cd->opts.connect_stdin = true;
            } else {
                cd->opts.connect_stdin = false;
            }
            if (ORTE_SUCCESS != (rc = orte_iof_base_setup_prefork(&cd->opts))) {
                ORTE_ERROR_LOG(rc);
                child->exit_code = rc;
                OBJ_RELEASE(cd);
                ORTE_ACTIVATE_PROC_STATE(&child->name, ORTE_PROC_STATE_FAILED_TO_LAUNCH);
                goto GETOUT;
            }
            if (ORTE_FLAG_TEST(jobdat, ORTE_JOB_FLAG_FORWARD_OUTPUT)) {
                /* connect endpoints IOF */
                rc = orte_iof_base_setup_parent(&child->name, &cd->opts);
                if (ORTE_SUCCESS != rc) {
                    ORTE_ERROR_LOG(rc);
                    OBJ_RELEASE(cd);
                    ORTE_ACTIVATE_PROC_STATE(&child->name, ORTE_PROC_STATE_FAILED_TO_LAUNCH);
                    goto GETOUT;
                }
            }
            opal_output_verbose(1, orte_odls_base_framework.framework_output,
                                "%s odls:dispatch %s to thread %d",
                                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                ORTE_NAME_PRINT(&child->name),
                                orte_odls_globals.next_base);
            opal_event_set(evb, &cd->ev, -1,
                           OPAL_EV_WRITE, orte_odls_base_spawn_proc, cd);
            opal_event_set_priority(&cd->ev, ORTE_MSG_PRI);
            opal_event_active(&cd->ev, OPAL_EV_WRITE, 1);

        }
        if (NULL != effective_dir) {
            free(effective_dir);
            effective_dir = NULL;
        }
    }

  GETOUT:
    if (NULL != effective_dir) {
        free(effective_dir);
        effective_dir = NULL;
    }

  ERROR_OUT:
    /* ensure we reset our working directory back to our default location  */
    chdir(basedir);
    /* release the event */
    OBJ_RELEASE(caddy);
}

/**
*  Pass a signal to my local procs
 */

int orte_odls_base_default_signal_local_procs(const orte_process_name_t *proc, int32_t signal,
                                              orte_odls_base_signal_local_fn_t signal_local)
{
    int rc, i;
    orte_proc_t *child;

    OPAL_OUTPUT_VERBOSE((5, orte_odls_base_framework.framework_output,
                         "%s odls: signaling proc %s",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         (NULL == proc) ? "NULL" : ORTE_NAME_PRINT(proc)));

    /* if procs is NULL, then we want to signal all
     * of the local procs, so just do that case
     */
    if (NULL == proc) {
        rc = ORTE_SUCCESS;  /* pre-set this as an empty list causes us to drop to bottom */
        for (i=0; i < orte_local_children->size; i++) {
            if (NULL == (child = (orte_proc_t*)opal_pointer_array_get_item(orte_local_children, i))) {
                continue;
            }
            if (0 == child->pid || !ORTE_FLAG_TEST(child, ORTE_PROC_FLAG_ALIVE)) {
                /* skip this one as the child isn't alive */
                continue;
            }
            if (ORTE_SUCCESS != (rc = signal_local(child->pid, (int)signal))) {
                ORTE_ERROR_LOG(rc);
            }
        }
        return rc;
    }

    /* we want it sent to some specified process, so find it */
    for (i=0; i < orte_local_children->size; i++) {
        if (NULL == (child = (orte_proc_t*)opal_pointer_array_get_item(orte_local_children, i))) {
            continue;
        }
        if (OPAL_EQUAL == opal_dss.compare(&(child->name), (orte_process_name_t*)proc, ORTE_NAME)) {
            if (ORTE_SUCCESS != (rc = signal_local(child->pid, (int)signal))) {
                ORTE_ERROR_LOG(rc);
            }
            return rc;
        }
    }

    /* only way to get here is if we couldn't find the specified proc.
     * report that as an error and return it
     */
    ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
    return ORTE_ERR_NOT_FOUND;
}

/*
 *  Wait for a callback indicating the child has completed.
 */

void orte_odls_base_default_wait_local_proc(int fd, short sd, void *cbdata)
{
    orte_wait_tracker_t *t2 = (orte_wait_tracker_t*)cbdata;
    orte_proc_t *proc = t2->child;
    int i;
    orte_job_t *jobdat;
    orte_proc_state_t state=ORTE_PROC_STATE_WAITPID_FIRED;
    orte_proc_t *cptr;

    opal_output_verbose(5, orte_odls_base_framework.framework_output,
                        "%s odls:wait_local_proc child process %s pid %ld terminated",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                        ORTE_NAME_PRINT(&proc->name), (long)proc->pid);

    /* if the child was previously flagged as dead, then just
     * update its exit status and
     * ensure that its exit state gets reported to avoid hanging
     * don't forget to check if the process was signaled.
     */
    if (!ORTE_FLAG_TEST(proc, ORTE_PROC_FLAG_ALIVE)) {
        OPAL_OUTPUT_VERBOSE((5, orte_odls_base_framework.framework_output,
                             "%s odls:waitpid_fired child %s was already dead exit code %d",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             ORTE_NAME_PRINT(&proc->name),proc->exit_code));
        if (WIFEXITED(proc->exit_code)) {
            proc->exit_code = WEXITSTATUS(proc->exit_code);
            if (0 != proc->exit_code) {
                state = ORTE_PROC_STATE_TERM_NON_ZERO;
            }
        } else {
            if (WIFSIGNALED(proc->exit_code)) {
                state = ORTE_PROC_STATE_ABORTED_BY_SIG;
                proc->exit_code = WTERMSIG(proc->exit_code) + 128;
            }
        }
        goto MOVEON;
    }

    /* if the proc called "abort", then we just need to flag that it
     * came thru here */
    if (ORTE_FLAG_TEST(proc, ORTE_PROC_FLAG_ABORT)) {
        /* even though the process exited "normally", it happened
         * via an orte_abort call
         */
        OPAL_OUTPUT_VERBOSE((5, orte_odls_base_framework.framework_output,
                             "%s odls:waitpid_fired child %s died by call to abort",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             ORTE_NAME_PRINT(&proc->name)));
        state = ORTE_PROC_STATE_CALLED_ABORT;
        /* regardless of our eventual code path, we need to
         * flag that this proc has had its waitpid fired */
        ORTE_FLAG_SET(proc, ORTE_PROC_FLAG_WAITPID);
        goto MOVEON;
    }

    /* get the jobdat for this child */
    if (NULL == (jobdat = orte_get_job_data_object(proc->name.jobid))) {
        ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
        goto MOVEON;
    }

    /* if this is a debugger daemon, then just report the state
     * and return as we aren't monitoring it
     */
    if (ORTE_FLAG_TEST(jobdat, ORTE_JOB_FLAG_DEBUGGER_DAEMON))  {
        goto MOVEON;
    }

    /* if this child was ordered to die, then just pass that along
     * so we don't hang
     */
    if (ORTE_PROC_STATE_KILLED_BY_CMD == proc->state) {
        OPAL_OUTPUT_VERBOSE((5, orte_odls_base_framework.framework_output,
                             "%s odls:waitpid_fired child %s was ordered to die",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             ORTE_NAME_PRINT(&proc->name)));
        /* regardless of our eventual code path, we need to
         * flag that this proc has had its waitpid fired */
        ORTE_FLAG_SET(proc, ORTE_PROC_FLAG_WAITPID);
        goto MOVEON;
    }

    /* determine the state of this process */
    if (WIFEXITED(proc->exit_code)) {

        /* set the exit status appropriately */
        proc->exit_code = WEXITSTATUS(proc->exit_code);

        OPAL_OUTPUT_VERBOSE((5, orte_odls_base_framework.framework_output,
                             "%s odls:waitpid_fired child %s exit code %d",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             ORTE_NAME_PRINT(&proc->name), proc->exit_code));

        /* provide a default state */
        state = ORTE_PROC_STATE_WAITPID_FIRED;

        /* check to see if a sync was required and if it was received */
        if (ORTE_FLAG_TEST(proc, ORTE_PROC_FLAG_REG)) {
            if (ORTE_FLAG_TEST(proc, ORTE_PROC_FLAG_HAS_DEREG) ||
                orte_allowed_exit_without_sync || 0 != proc->exit_code) {
                /* if we did recv a finalize sync, or one is not required,
                 * then declare it normally terminated
                 * unless it returned with a non-zero status indicating the code
                 * felt it was non-normal - in this latter case, we do not
                 * require that the proc deregister before terminating
                 */
                if (0 != proc->exit_code && orte_abort_non_zero_exit) {
                    state = ORTE_PROC_STATE_TERM_NON_ZERO;
                    OPAL_OUTPUT_VERBOSE((5, orte_odls_base_framework.framework_output,
                                         "%s odls:waitpid_fired child process %s terminated normally "
                                         "but with a non-zero exit status - it "
                                         "will be treated as an abnormal termination",
                                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                         ORTE_NAME_PRINT(&proc->name)));
                } else {
                    /* indicate the waitpid fired */
                    state = ORTE_PROC_STATE_WAITPID_FIRED;
                }
            } else {
                /* we required a finalizing sync and didn't get it, so this
                 * is considered an abnormal termination and treated accordingly
                 */
                state = ORTE_PROC_STATE_TERM_WO_SYNC;
                OPAL_OUTPUT_VERBOSE((5, orte_odls_base_framework.framework_output,
                                     "%s odls:waitpid_fired child process %s terminated normally "
                                     "but did not provide a required finalize sync - it "
                                     "will be treated as an abnormal termination",
                                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                     ORTE_NAME_PRINT(&proc->name)));
            }
        } else {
            /* has any child in this job already registered? */
            for (i=0; i < orte_local_children->size; i++) {
                if (NULL == (cptr = (orte_proc_t*)opal_pointer_array_get_item(orte_local_children, i))) {
                    continue;
                }
                if (cptr->name.jobid != proc->name.jobid) {
                    continue;
                }
                if (ORTE_FLAG_TEST(cptr, ORTE_PROC_FLAG_REG) && !orte_allowed_exit_without_sync) {
                    /* someone has registered, and we didn't before
                     * terminating - this is an abnormal termination unless
                     * the allowed_exit_without_sync flag is set
                     */
                    if (0 != proc->exit_code) {
                        state = ORTE_PROC_STATE_TERM_NON_ZERO;
                        OPAL_OUTPUT_VERBOSE((5, orte_odls_base_framework.framework_output,
                                             "%s odls:waitpid_fired child process %s terminated normally "
                                             "but with a non-zero exit status - it "
                                             "will be treated as an abnormal termination",
                                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                             ORTE_NAME_PRINT(&proc->name)));
                    } else {
                        state = ORTE_PROC_STATE_TERM_WO_SYNC;
                        OPAL_OUTPUT_VERBOSE((5, orte_odls_base_framework.framework_output,
                                             "%s odls:waitpid_fired child process %s terminated normally "
                                             "but did not provide a required init sync - it "
                                             "will be treated as an abnormal termination",
                                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                             ORTE_NAME_PRINT(&proc->name)));
                    }
                    goto MOVEON;
                }
            }
            /* if no child has registered, then it is possible that
             * none of them will. This is considered acceptable. Still
             * flag it as abnormal if the exit code was non-zero
             */
            if (0 != proc->exit_code && orte_abort_non_zero_exit) {
                state = ORTE_PROC_STATE_TERM_NON_ZERO;
            } else {
                state = ORTE_PROC_STATE_WAITPID_FIRED;
            }
        }

        OPAL_OUTPUT_VERBOSE((5, orte_odls_base_framework.framework_output,
                             "%s odls:waitpid_fired child process %s terminated %s",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             ORTE_NAME_PRINT(&proc->name),
                             (0 == proc->exit_code) ? "normally" : "with non-zero status"));
    } else {
        /* the process was terminated with a signal! That's definitely
         * abnormal, so indicate that condition
         */
        state = ORTE_PROC_STATE_ABORTED_BY_SIG;
        /* If a process was killed by a signal, then make the
         * exit code of orterun be "signo + 128" so that "prog"
         * and "orterun prog" will both yield the same exit code.
         *
         * This is actually what the shell does for you when
         * a process dies by signal, so this makes orterun treat
         * the termination code to exit status translation the
         * same way
         */
        proc->exit_code = WTERMSIG(proc->exit_code) + 128;

        OPAL_OUTPUT_VERBOSE((5, orte_odls_base_framework.framework_output,
                             "%s odls:waitpid_fired child process %s terminated with signal",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             ORTE_NAME_PRINT(&proc->name) ));
        /* Do not decrement the number of local procs here. That is handled in the errmgr */
    }

 MOVEON:
    /* cancel the wait as this proc has already terminated */
    orte_wait_cb_cancel(proc);
    ORTE_ACTIVATE_PROC_STATE(&proc->name, state);
    /* cleanup the tracker */
    OBJ_RELEASE(t2);
}

typedef struct {
    opal_list_item_t super;
    orte_proc_t *child;
} orte_odls_quick_caddy_t;
static void qcdcon(orte_odls_quick_caddy_t *p)
{
    p->child = NULL;
}
static void qcddes(orte_odls_quick_caddy_t *p)
{
    if (NULL != p->child) {
        OBJ_RELEASE(p->child);
    }
}
OBJ_CLASS_INSTANCE(orte_odls_quick_caddy_t,
                   opal_list_item_t,
                   qcdcon, qcddes);

int orte_odls_base_default_kill_local_procs(opal_pointer_array_t *procs,
                                            orte_odls_base_kill_local_fn_t kill_local)
{
    orte_proc_t *child;
    opal_list_t procs_killed;
    orte_proc_t *proc, proctmp;
    int i, j;
    opal_pointer_array_t procarray, *procptr;
    bool do_cleanup;
    orte_odls_quick_caddy_t *cd;

    OBJ_CONSTRUCT(&procs_killed, opal_list_t);

    /* if the pointer array is NULL, then just kill everything */
    if (NULL == procs) {
        OPAL_OUTPUT_VERBOSE((5, orte_odls_base_framework.framework_output,
                             "%s odls:kill_local_proc working on WILDCARD",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));
        OBJ_CONSTRUCT(&procarray, opal_pointer_array_t);
        opal_pointer_array_init(&procarray, 1, 1, 1);
        OBJ_CONSTRUCT(&proctmp, orte_proc_t);
        proctmp.name.jobid = ORTE_JOBID_WILDCARD;
        proctmp.name.vpid = ORTE_VPID_WILDCARD;
        opal_pointer_array_add(&procarray, &proctmp);
        procptr = &procarray;
        do_cleanup = true;
    } else {
        OPAL_OUTPUT_VERBOSE((5, orte_odls_base_framework.framework_output,
                             "%s odls:kill_local_proc working on provided array",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));
        procptr = procs;
        do_cleanup = false;
    }

    /* cycle through the provided array of processes to kill */
    for (i=0; i < procptr->size; i++) {
        if (NULL == (proc = (orte_proc_t*)opal_pointer_array_get_item(procptr, i))) {
            continue;
        }
        for (j=0; j < orte_local_children->size; j++) {
            if (NULL == (child = (orte_proc_t*)opal_pointer_array_get_item(orte_local_children, j))) {
                continue;
            }

            OPAL_OUTPUT_VERBOSE((5, orte_odls_base_framework.framework_output,
                                 "%s odls:kill_local_proc checking child process %s",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 ORTE_NAME_PRINT(&child->name)));

            /* do we have a child from the specified job? Because the
             *  job could be given as a WILDCARD value, we must
             *  check for that as well as for equality.
             */
            if (ORTE_JOBID_WILDCARD != proc->name.jobid &&
                proc->name.jobid != child->name.jobid) {

                OPAL_OUTPUT_VERBOSE((5, orte_odls_base_framework.framework_output,
                                     "%s odls:kill_local_proc child %s is not part of job %s",
                                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                     ORTE_NAME_PRINT(&child->name),
                                     ORTE_JOBID_PRINT(proc->name.jobid)));
                continue;
            }

            /* see if this is the specified proc - could be a WILDCARD again, so check
             * appropriately
             */
            if (ORTE_VPID_WILDCARD != proc->name.vpid &&
                proc->name.vpid != child->name.vpid) {

                OPAL_OUTPUT_VERBOSE((5, orte_odls_base_framework.framework_output,
                                     "%s odls:kill_local_proc child %s is not covered by rank %s",
                                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                     ORTE_NAME_PRINT(&child->name),
                                     ORTE_VPID_PRINT(proc->name.vpid)));
                continue;
            }

            /* is this process alive? if not, then nothing for us
             * to do to it
             */
            if (!ORTE_FLAG_TEST(child, ORTE_PROC_FLAG_ALIVE) || 0 == child->pid) {

                OPAL_OUTPUT_VERBOSE((5, orte_odls_base_framework.framework_output,
                                     "%s odls:kill_local_proc child %s is not alive",
                                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                     ORTE_NAME_PRINT(&child->name)));

                /* ensure, though, that the state is terminated so we don't lockup if
                 * the proc never started
                 */
                if (ORTE_PROC_STATE_UNDEF == child->state ||
                    ORTE_PROC_STATE_INIT == child->state ||
                    ORTE_PROC_STATE_RUNNING == child->state) {
                    /* we can't be sure what happened, but make sure we
                     * at least have a value that will let us eventually wakeup
                     */
                    child->state = ORTE_PROC_STATE_TERMINATED;
                    /* ensure we realize that the waitpid will never come, if
                     * it already hasn't
                     */
                    ORTE_FLAG_SET(child, ORTE_PROC_FLAG_WAITPID);
                    child->pid = 0;
                    goto CLEANUP;
                } else {
                    continue;
                }
            }

            /* ensure the stdin IOF channel for this child is closed. The other
             * channels will automatically close when the proc is killed
             */
            if (NULL != orte_iof.close) {
                orte_iof.close(&child->name, ORTE_IOF_STDIN);
            }

            /* cancel the waitpid callback as this induces unmanageable race
             * conditions when we are deliberately killing the process
             */
            orte_wait_cb_cancel(child);

            /* First send a SIGCONT in case the process is in stopped state.
               If it is in a stopped state and we do not first change it to
               running, then SIGTERM will not get delivered.  Ignore return
               value. */
            OPAL_OUTPUT_VERBOSE((5, orte_odls_base_framework.framework_output,
                                 "%s SENDING SIGCONT TO %s",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 ORTE_NAME_PRINT(&child->name)));
            cd = OBJ_NEW(orte_odls_quick_caddy_t);
            OBJ_RETAIN(child);
            cd->child = child;
            opal_list_append(&procs_killed, &cd->super);
            kill_local(child->pid, SIGCONT);
            continue;

        CLEANUP:
            /* ensure the child's session directory is cleaned up */
            orte_session_dir_finalize(&child->name);
            /* check for everything complete - this will remove
             * the child object from our local list
             */
            if (ORTE_FLAG_TEST(child, ORTE_PROC_FLAG_IOF_COMPLETE) &&
                ORTE_FLAG_TEST(child, ORTE_PROC_FLAG_WAITPID)) {
                ORTE_ACTIVATE_PROC_STATE(&child->name, child->state);
            }
        }
    }

    /* if we are issuing signals, then we need to wait a little
     * and send the next in sequence */
    if (0 < opal_list_get_size(&procs_killed)) {
        sleep(orte_odls_globals.timeout_before_sigkill);
        /* issue a SIGTERM to all */
        OPAL_LIST_FOREACH(cd, &procs_killed, orte_odls_quick_caddy_t) {
            OPAL_OUTPUT_VERBOSE((5, orte_odls_base_framework.framework_output,
                                 "%s SENDING SIGTERM TO %s",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 ORTE_NAME_PRINT(&cd->child->name)));
            kill_local(cd->child->pid, SIGTERM);
        }
        /* wait a little again */
        sleep(orte_odls_globals.timeout_before_sigkill);
        /* issue a SIGKILL to all */
        OPAL_LIST_FOREACH(cd, &procs_killed, orte_odls_quick_caddy_t) {
            OPAL_OUTPUT_VERBOSE((5, orte_odls_base_framework.framework_output,
                                 "%s SENDING SIGKILL TO %s",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 ORTE_NAME_PRINT(&cd->child->name)));
            kill_local(cd->child->pid, SIGKILL);
            /* indicate the waitpid fired as this is effectively what
             * has happened
             */
            ORTE_FLAG_SET(cd->child, ORTE_PROC_FLAG_WAITPID);

            /* Since we are not going to wait for this process, make sure
             * we mark it as not-alive so that we don't wait for it
             * in orted_cmd
             */
            ORTE_FLAG_UNSET(cd->child, ORTE_PROC_FLAG_ALIVE);
            cd->child->pid = 0;

            /* mark the child as "killed" */
            cd->child->state = ORTE_PROC_STATE_KILLED_BY_CMD;  /* we ordered it to die */

            /* ensure the child's session directory is cleaned up */
            orte_session_dir_finalize(&cd->child->name);
            /* check for everything complete - this will remove
             * the child object from our local list
             */
            if (ORTE_FLAG_TEST(cd->child, ORTE_PROC_FLAG_IOF_COMPLETE) &&
                ORTE_FLAG_TEST(cd->child, ORTE_PROC_FLAG_WAITPID)) {
                ORTE_ACTIVATE_PROC_STATE(&cd->child->name, cd->child->state);
            }
        }
    }
    OPAL_LIST_DESTRUCT(&procs_killed);

    /* cleanup arrays, if required */
    if (do_cleanup) {
        OBJ_DESTRUCT(&procarray);
        OBJ_DESTRUCT(&proctmp);
    }

    return ORTE_SUCCESS;
}

int orte_odls_base_get_proc_stats(opal_buffer_t *answer,
                                  orte_process_name_t *proc)
{
    int rc;
    orte_proc_t *child;
    opal_pstats_t stats, *statsptr;
    int i, j;

    OPAL_OUTPUT_VERBOSE((5, orte_odls_base_framework.framework_output,
                         "%s odls:get_proc_stats for proc %s",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         ORTE_NAME_PRINT(proc)));

    /* find this child */
    for (i=0; i < orte_local_children->size; i++) {
        if (NULL == (child = (orte_proc_t*)opal_pointer_array_get_item(orte_local_children, i))) {
            continue;
        }

        if (proc->jobid == child->name.jobid &&
            (proc->vpid == child->name.vpid ||
             ORTE_VPID_WILDCARD == proc->vpid)) { /* found it */

            OBJ_CONSTRUCT(&stats, opal_pstats_t);
            /* record node up to first '.' */
            for (j=0; j < (int)strlen(orte_process_info.nodename) &&
                 j < OPAL_PSTAT_MAX_STRING_LEN-1 &&
                 orte_process_info.nodename[j] != '.'; j++) {
                stats.node[j] = orte_process_info.nodename[j];
            }
            /* record rank */
            stats.rank = child->name.vpid;
            /* get stats */
            rc = opal_pstat.query(child->pid, &stats, NULL);
            if (ORTE_SUCCESS != rc) {
                OBJ_DESTRUCT(&stats);
                return rc;
            }
            if (ORTE_SUCCESS != (rc = opal_dss.pack(answer, proc, 1, ORTE_NAME))) {
                ORTE_ERROR_LOG(rc);
                OBJ_DESTRUCT(&stats);
                return rc;
            }
            statsptr = &stats;
            if (ORTE_SUCCESS != (rc = opal_dss.pack(answer, &statsptr, 1, OPAL_PSTAT))) {
                ORTE_ERROR_LOG(rc);
                OBJ_DESTRUCT(&stats);
                return rc;
            }
            OBJ_DESTRUCT(&stats);
        }
    }

    return ORTE_SUCCESS;
}

int orte_odls_base_default_restart_proc(orte_proc_t *child,
                                        orte_odls_base_fork_local_proc_fn_t fork_local)
{
    int rc;
    orte_app_context_t *app;
    orte_job_t *jobdat;
    char basedir[MAXPATHLEN];
    char *wdir = NULL;
    orte_odls_spawn_caddy_t *cd;
    opal_event_base_t *evb;

    OPAL_OUTPUT_VERBOSE((5, orte_odls_base_framework.framework_output,
                         "%s odls:restart_proc for proc %s",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         ORTE_NAME_PRINT(&child->name)));

    /* establish our baseline working directory - we will be potentially
     * bouncing around as we execute this app, but we will always return
     * to this place as our default directory
     */
    getcwd(basedir, sizeof(basedir));

    /* find this child's jobdat */
    if (NULL == (jobdat = orte_get_job_data_object(child->name.jobid))) {
        /* not found */
        ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
        return ORTE_ERR_NOT_FOUND;
    }

    child->state = ORTE_PROC_STATE_FAILED_TO_START;
    child->exit_code = 0;
    ORTE_FLAG_UNSET(child, ORTE_PROC_FLAG_WAITPID);
    ORTE_FLAG_UNSET(child, ORTE_PROC_FLAG_IOF_COMPLETE);
    child->pid = 0;
    if (NULL != child->rml_uri) {
        free(child->rml_uri);
        child->rml_uri = NULL;
    }
    app = (orte_app_context_t*)opal_pointer_array_get_item(jobdat->apps, child->app_idx);

    /* reset envars to match this child */
    if (ORTE_SUCCESS != (rc = orte_schizo.setup_child(jobdat, child, app, &app->env))) {
        ORTE_ERROR_LOG(rc);
        goto CLEANUP;
    }

    /* setup the path */
    if (ORTE_SUCCESS != (rc = setup_path(app, &wdir))) {
        ORTE_ERROR_LOG(rc);
        if (NULL != wdir) {
            free(wdir);
        }
        goto CLEANUP;
    }

    /* dispatch this child to the next available launch thread */
    cd = OBJ_NEW(orte_odls_spawn_caddy_t);
    if (NULL != wdir) {
        cd->wdir = strdup(wdir);
        free(wdir);
    }
    cd->jdata = jobdat;
    cd->app = app;
    cd->child = child;
    cd->fork_local = fork_local;
    /* setup any IOF */
    cd->opts.usepty = OPAL_ENABLE_PTY_SUPPORT;

    /* do we want to setup stdin? */
    if (jobdat->stdin_target == ORTE_VPID_WILDCARD ||
         child->name.vpid == jobdat->stdin_target) {
        cd->opts.connect_stdin = true;
    } else {
        cd->opts.connect_stdin = false;
    }
    if (ORTE_SUCCESS != (rc = orte_iof_base_setup_prefork(&cd->opts))) {
        ORTE_ERROR_LOG(rc);
        child->exit_code = rc;
        OBJ_RELEASE(cd);
        ORTE_ACTIVATE_PROC_STATE(&child->name, ORTE_PROC_STATE_FAILED_TO_LAUNCH);
        goto CLEANUP;
    }
    if (ORTE_FLAG_TEST(jobdat, ORTE_JOB_FLAG_FORWARD_OUTPUT)) {
        /* connect endpoints IOF */
        rc = orte_iof_base_setup_parent(&child->name, &cd->opts);
        if (ORTE_SUCCESS != rc) {
            ORTE_ERROR_LOG(rc);
            OBJ_RELEASE(cd);
            ORTE_ACTIVATE_PROC_STATE(&child->name, ORTE_PROC_STATE_FAILED_TO_LAUNCH);
            goto CLEANUP;
        }
    }
    ++orte_odls_globals.next_base;
    if (orte_odls_globals.num_threads <= orte_odls_globals.next_base) {
        orte_odls_globals.next_base = 0;
    }
    evb = orte_odls_globals.ev_bases[orte_odls_globals.next_base];
    orte_wait_cb(child, orte_odls_base_default_wait_local_proc, evb, NULL);

    OPAL_OUTPUT_VERBOSE((5, orte_odls_base_framework.framework_output,
                         "%s restarting app %s",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), app->app));

    opal_event_set(evb, &cd->ev, -1,
                   OPAL_EV_WRITE, orte_odls_base_spawn_proc, cd);
    opal_event_set_priority(&cd->ev, ORTE_MSG_PRI);
    opal_event_active(&cd->ev, OPAL_EV_WRITE, 1);

  CLEANUP:
    OPAL_OUTPUT_VERBOSE((5, orte_odls_base_framework.framework_output,
                         "%s odls:restart of proc %s %s",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         ORTE_NAME_PRINT(&child->name),
                         (ORTE_SUCCESS == rc) ? "succeeded" : "failed"));

    /* reset our working directory back to our default location - if we
     * don't do this, then we will be looking for relative paths starting
     * from the last wdir option specified by the user. Thus, we would
     * be requiring that the user keep track on the cmd line of where
     * each app was located relative to the prior app, instead of relative
     * to their current location
     */
    chdir(basedir);

    return rc;
}
