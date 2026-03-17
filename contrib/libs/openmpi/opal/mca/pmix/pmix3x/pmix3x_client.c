/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2014-2018 Intel, Inc. All rights reserved.
 * Copyright (c) 2014-2017 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2014-2017 Mellanox Technologies, Inc.
 *                         All rights reserved.
 * Copyright (c) 2016      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2016      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2017-2018 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2019      IBM Corporation.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"
#include "opal/constants.h"
#include "opal/types.h"

#ifdef HAVE_STRING_H
#include <string.h>
#endif
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif

#include "opal/hash_string.h"
#include "opal/threads/threads.h"
#include "opal/util/argv.h"
#include "opal/util/opal_environ.h"
#include "opal/util/proc.h"
#include "opal/util/show_help.h"

#include "opal/mca/pmix/base/base.h"
#include "pmix3x.h"
#include "pmix.h"
#include "pmix_tool.h"

static char *dbgvalue=NULL;

static void errreg_cbfunc (pmix_status_t status,
                           size_t errhandler_ref,
                           void *cbdata)
{
    opal_pmix3x_event_t *event = (opal_pmix3x_event_t*)cbdata;

    OPAL_ACQUIRE_OBJECT(event);

    event->index = errhandler_ref;
    opal_output_verbose(5, opal_pmix_base_framework.framework_output,
                        "PMIX client errreg_cbfunc - error handler registered status=%d, reference=%lu",
                        status, (unsigned long)errhandler_ref);
    OPAL_POST_OBJECT(event);
    OPAL_PMIX_WAKEUP_THREAD(&event->lock);
}

int pmix3x_client_init(opal_list_t *ilist)
{
    opal_process_name_t pname;
    pmix_status_t rc;
    int dbg;
    opal_pmix3x_jobid_trkr_t *job;
    opal_pmix3x_event_t *event;
    pmix_info_t *pinfo;
    size_t ninfo, n;
    opal_value_t *ival;

    opal_output_verbose(1, opal_pmix_base_framework.framework_output,
                        "PMIx_client init");

    OPAL_PMIX_ACQUIRE_THREAD(&opal_pmix_base.lock);

    if (0 == opal_pmix_base.initialized) {
        if (0 < (dbg = opal_output_get_verbosity(opal_pmix_base_framework.framework_output))) {
            asprintf(&dbgvalue, "PMIX_DEBUG=%d", dbg);
            putenv(dbgvalue);
        }
        /* check the evars for a mismatch */
        if (OPAL_SUCCESS != (dbg = opal_pmix_pmix3x_check_evars())) {
            OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);
            return dbg;
        }
    }

    /* convert the incoming list to info structs */
    if (NULL != ilist && 0 < (ninfo = opal_list_get_size(ilist))) {
        PMIX_INFO_CREATE(pinfo, ninfo);
        n=0;
        OPAL_LIST_FOREACH(ival, ilist, opal_value_t) {
            (void)strncpy(pinfo[n].key, ival->key, PMIX_MAX_KEYLEN);
            pmix3x_value_load(&pinfo[n].value, ival);
            ++n;
        }
    } else {
        pinfo = NULL;
        ninfo = 0;
    }

    OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);
    rc = PMIx_Init(&mca_pmix_pmix3x_component.myproc, pinfo, ninfo);
    if (NULL != pinfo) {
        PMIX_INFO_FREE(pinfo, ninfo);
    }
    if (PMIX_SUCCESS != rc) {
        dbg = pmix3x_convert_rc(rc);
        OPAL_ERROR_LOG(dbg);
        return dbg;
    }
    OPAL_PMIX_ACQUIRE_THREAD(&opal_pmix_base.lock);

    ++opal_pmix_base.initialized;
    if (1 < opal_pmix_base.initialized) {
        OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);
        return OPAL_SUCCESS;
    }

    /* store our jobid and rank */
    if (NULL != getenv(OPAL_MCA_PREFIX"orte_launch")) {
        /* if we were launched by the OMPI RTE, then
         * the jobid is in a special format - so get it */
        mca_pmix_pmix3x_component.native_launch = true;
        opal_convert_string_to_jobid(&pname.jobid, mca_pmix_pmix3x_component.myproc.nspace);
    } else {
        /* we were launched by someone else, so make the
         * jobid just be the hash of the nspace */
        OPAL_HASH_JOBID(mca_pmix_pmix3x_component.myproc.nspace, pname.jobid);
    }
    /* insert this into our list of jobids - it will be the
     * first, and so we'll check it first */
    job = OBJ_NEW(opal_pmix3x_jobid_trkr_t);
    (void)strncpy(job->nspace, mca_pmix_pmix3x_component.myproc.nspace, PMIX_MAX_NSLEN);
    job->jobid = pname.jobid;
    opal_list_append(&mca_pmix_pmix3x_component.jobids, &job->super);

    pname.vpid = pmix3x_convert_rank(mca_pmix_pmix3x_component.myproc.rank);
    opal_proc_set_name(&pname);

    /* release the thread in case the event handler fires when
     * registered */
    OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);

    /* register the default event handler */
    event = OBJ_NEW(opal_pmix3x_event_t);
    opal_list_append(&mca_pmix_pmix3x_component.events, &event->super);
    PMIX_INFO_CREATE(pinfo, 1);
    PMIX_INFO_LOAD(&pinfo[0], PMIX_EVENT_HDLR_NAME, "OPAL-PMIX-2X-DEFAULT", PMIX_STRING);
    PMIx_Register_event_handler(NULL, 0, NULL, 0, pmix3x_event_hdlr, errreg_cbfunc, event);
    OPAL_PMIX_WAIT_THREAD(&event->lock);
    PMIX_INFO_FREE(pinfo, 1);

    return OPAL_SUCCESS;

}

static void dereg_cbfunc(pmix_status_t st, void *cbdata)
{
    opal_pmix3x_event_t *ev = (opal_pmix3x_event_t*)cbdata;
    OPAL_PMIX_WAKEUP_THREAD(&ev->lock);
}

int pmix3x_client_finalize(void)
{
    pmix_status_t rc;
    opal_pmix3x_event_t *event, *ev2;
    opal_list_t evlist;
    OBJ_CONSTRUCT(&evlist, opal_list_t);

    opal_output_verbose(1, opal_pmix_base_framework.framework_output,
                        "PMIx_client finalize");

    OPAL_PMIX_ACQUIRE_THREAD(&opal_pmix_base.lock);
    --opal_pmix_base.initialized;

    if (0 == opal_pmix_base.initialized) {
        /* deregister all event handlers */
        OPAL_LIST_FOREACH_SAFE(event, ev2, &mca_pmix_pmix3x_component.events, opal_pmix3x_event_t) {
            OPAL_PMIX_DESTRUCT_LOCK(&event->lock);
            OPAL_PMIX_CONSTRUCT_LOCK(&event->lock);
            PMIx_Deregister_event_handler(event->index, dereg_cbfunc, (void*)event);
            opal_list_remove_item(&mca_pmix_pmix3x_component.events, &event->super);
            /* wait and release outside the loop to avoid double mutex
             * interlock */
            opal_list_append(&evlist, &event->super);
        }
    }
    OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);
    OPAL_LIST_FOREACH_SAFE(event, ev2, &evlist, opal_pmix3x_event_t) {
        OPAL_PMIX_WAIT_THREAD(&event->lock);
        opal_list_remove_item(&evlist, &event->super);
        OBJ_RELEASE(event);
    }
    OBJ_DESTRUCT(&evlist);
    rc = PMIx_Finalize(NULL, 0);

    return pmix3x_convert_rc(rc);
}

int pmix3x_tool_init(opal_list_t *info)
{
    pmix_info_t *pinfo;
    size_t ninfo, n;
    opal_pmix3x_jobid_trkr_t *job;
    opal_value_t *val;
    pmix_status_t rc;
    int ret;
    opal_process_name_t pname = {OPAL_JOBID_INVALID, OPAL_VPID_INVALID};
    opal_pmix3x_event_t *event;

    opal_output_verbose(1, opal_pmix_base_framework.framework_output,
                        "PMIx_tool init");

    OPAL_PMIX_ACQUIRE_THREAD(&opal_pmix_base.lock);

    /* convert the incoming list to info structs */
    if (NULL != info && 0 < (ninfo = opal_list_get_size(info))) {
        PMIX_INFO_CREATE(pinfo, ninfo);
        n=0;
        OPAL_LIST_FOREACH(val, info, opal_value_t) {
            (void)strncpy(pinfo[n].key, val->key, PMIX_MAX_KEYLEN);
            pmix3x_value_load(&pinfo[n].value, val);
            ++n;
            /* check to see if our name is being given from above */
            if (0 == strcmp(val->key, OPAL_PMIX_TOOL_NSPACE)) {
                opal_convert_string_to_jobid(&pname.jobid, val->data.string);
                (void)strncpy(mca_pmix_pmix3x_component.myproc.nspace, val->data.string, PMIX_MAX_NSLEN);
            } else if (0 == strcmp(val->key, OPAL_PMIX_TOOL_RANK)) {
                pname.vpid = val->data.name.vpid;
                mca_pmix_pmix3x_component.myproc.rank = pname.vpid;
            }
        }
    } else {
        pinfo = NULL;
        ninfo = 0;
    }
    /* we are going to get our name from the server, or we were given it by the tool,
     * so mark as native launch so we don't convert back/forth */
    mca_pmix_pmix3x_component.native_launch = true;

    OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);
    rc = PMIx_tool_init(&mca_pmix_pmix3x_component.myproc, pinfo, ninfo);
    if (NULL != pinfo) {
        PMIX_INFO_FREE(pinfo, ninfo);
    }
    if (PMIX_SUCCESS != rc) {
        ret = pmix3x_convert_rc(rc);
        OPAL_ERROR_LOG(ret);
        return ret;
    }
    OPAL_PMIX_ACQUIRE_THREAD(&opal_pmix_base.lock);

    ++opal_pmix_base.initialized;
    if (1 < opal_pmix_base.initialized) {
        OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);
        return OPAL_SUCCESS;
    }

    /* store our jobid and rank */
    opal_convert_string_to_jobid(&pname.jobid, mca_pmix_pmix3x_component.myproc.nspace);
    pname.vpid = pmix3x_convert_rank(mca_pmix_pmix3x_component.myproc.rank);

    /* insert this into our list of jobids - it will be the
     * first, and so we'll check it first */
    job = OBJ_NEW(opal_pmix3x_jobid_trkr_t);
    (void)strncpy(job->nspace, mca_pmix_pmix3x_component.myproc.nspace, PMIX_MAX_NSLEN);
    job->jobid = pname.jobid;
    opal_list_append(&mca_pmix_pmix3x_component.jobids, &job->super);

    opal_proc_set_name(&pname);

    /* release the thread in case the event handler fires when
     * registered */
    OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);

    /* register the default event handler */
    event = OBJ_NEW(opal_pmix3x_event_t);
    opal_list_append(&mca_pmix_pmix3x_component.events, &event->super);
    PMIX_INFO_CREATE(pinfo, 1);
    PMIX_INFO_LOAD(&pinfo[0], PMIX_EVENT_HDLR_NAME, "OPAL-PMIX-2X-DEFAULT", PMIX_STRING);
    PMIx_Register_event_handler(NULL, 0, NULL, 0, pmix3x_event_hdlr, errreg_cbfunc, event);
    OPAL_PMIX_WAIT_THREAD(&event->lock);
    PMIX_INFO_FREE(pinfo, 1);

    return OPAL_SUCCESS;
}

int pmix3x_tool_fini(void)
{
    pmix_status_t rc;
    opal_pmix3x_event_t *event, *ev2;

    opal_output_verbose(1, opal_pmix_base_framework.framework_output,
                        "PMIx_tool finalize");

    OPAL_PMIX_ACQUIRE_THREAD(&opal_pmix_base.lock);
    --opal_pmix_base.initialized;

    if (0 == opal_pmix_base.initialized) {
        /* deregister all event handlers */
        OPAL_LIST_FOREACH_SAFE(event, ev2, &mca_pmix_pmix3x_component.events, opal_pmix3x_event_t) {
            OPAL_PMIX_DESTRUCT_LOCK(&event->lock);
            OPAL_PMIX_CONSTRUCT_LOCK(&event->lock);
            PMIx_Deregister_event_handler(event->index, dereg_cbfunc, (void*)event);
            OPAL_PMIX_WAIT_THREAD(&event->lock);
            opal_list_remove_item(&mca_pmix_pmix3x_component.events, &event->super);
            OBJ_RELEASE(event);
        }
    }
    OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);
    rc = PMIx_tool_finalize();

    return pmix3x_convert_rc(rc);
}


int pmix3x_initialized(void)
{
    int init;

    opal_output_verbose(1, opal_pmix_base_framework.framework_output,
                        "PMIx_client initialized");

    OPAL_PMIX_ACQUIRE_THREAD(&opal_pmix_base.lock);
    init = opal_pmix_base.initialized;
    OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);

    return init;
}

int pmix3x_abort(int flag, const char *msg,
                 opal_list_t *procs)
{
    pmix_status_t rc;
    pmix_proc_t *parray=NULL;
    size_t n, cnt=0;
    opal_namelist_t *ptr;
    char *nsptr;

    opal_output_verbose(1, opal_pmix_base_framework.framework_output,
                        "PMIx_client abort");

    OPAL_PMIX_ACQUIRE_THREAD(&opal_pmix_base.lock);
    if (0 >= opal_pmix_base.initialized) {
        OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);
        return OPAL_ERR_NOT_INITIALIZED;
    }
    OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);

    /* convert the list of procs to an array
     * of pmix_proc_t */
    if (NULL != procs && 0 < (cnt = opal_list_get_size(procs))) {
        PMIX_PROC_CREATE(parray, cnt);
        n=0;
        OPAL_LIST_FOREACH(ptr, procs, opal_namelist_t) {
            if (NULL == (nsptr = pmix3x_convert_jobid(ptr->name.jobid))) {
                PMIX_PROC_FREE(parray, cnt);
                return OPAL_ERR_NOT_FOUND;
            }
            (void)strncpy(parray[n].nspace, nsptr, PMIX_MAX_NSLEN);
            parray[n].rank = pmix3x_convert_opalrank(ptr->name.vpid);
            ++n;
        }
    }

    /* call the library abort - this is a blocking call */
    rc = PMIx_Abort(flag, msg, parray, cnt);

    /* release the array */
    PMIX_PROC_FREE(parray, cnt);

    return pmix3x_convert_rc(rc);
}

int pmix3x_store_local(const opal_process_name_t *proc, opal_value_t *val)
{
    pmix_value_t kv;
    pmix_status_t rc;
    pmix_proc_t p;
    char *nsptr;
    opal_pmix3x_jobid_trkr_t *job;

    OPAL_PMIX_ACQUIRE_THREAD(&opal_pmix_base.lock);

    if (0 >= opal_pmix_base.initialized) {
        OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);
        return OPAL_ERR_NOT_INITIALIZED;
    }
    OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);

    if (NULL != proc) {
        if (NULL == (nsptr = pmix3x_convert_jobid(proc->jobid))) {
            job = OBJ_NEW(opal_pmix3x_jobid_trkr_t);
            (void)opal_snprintf_jobid(job->nspace, PMIX_MAX_NSLEN, proc->jobid);
            job->jobid = proc->jobid;
            OPAL_PMIX_ACQUIRE_THREAD(&opal_pmix_base.lock);
            opal_list_append(&mca_pmix_pmix3x_component.jobids, &job->super);
            OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);
            nsptr = job->nspace;
        }
        (void)strncpy(p.nspace, nsptr, PMIX_MAX_NSLEN);
        p.rank = pmix3x_convert_opalrank(proc->vpid);
    } else {
        /* use our name */
        (void)strncpy(p.nspace, mca_pmix_pmix3x_component.myproc.nspace, PMIX_MAX_NSLEN);
        p.rank = pmix3x_convert_opalrank(OPAL_PROC_MY_NAME.vpid);
    }

    PMIX_VALUE_CONSTRUCT(&kv);
    pmix3x_value_load(&kv, val);

    /* call the library - this is a blocking call */
    rc = PMIx_Store_internal(&p, val->key, &kv);
    PMIX_VALUE_DESTRUCT(&kv);

    return pmix3x_convert_rc(rc);
}

int pmix3x_commit(void)
{
    pmix_status_t rc;

    OPAL_PMIX_ACQUIRE_THREAD(&opal_pmix_base.lock);
    if (0 >= opal_pmix_base.initialized) {
        OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);
        return OPAL_ERR_NOT_INITIALIZED;
    }
    OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);

    rc = PMIx_Commit();
    return pmix3x_convert_rc(rc);
}

static void opcbfunc(pmix_status_t status, void *cbdata)
{
    pmix3x_opcaddy_t *op = (pmix3x_opcaddy_t*)cbdata;

    OPAL_ACQUIRE_OBJECT(op);
    if (NULL != op->opcbfunc) {
        op->opcbfunc(pmix3x_convert_rc(status), op->cbdata);
    }
    OBJ_RELEASE(op);
}

int pmix3x_fence(opal_list_t *procs, int collect_data)
{
    pmix_status_t rc;
    opal_namelist_t *ptr;
    char *nsptr;
    size_t cnt = 0, n;
    pmix_proc_t *parray = NULL;
    pmix_info_t info, *iptr;

    opal_output_verbose(1, opal_pmix_base_framework.framework_output,
                        "PMIx_client fence");

    OPAL_PMIX_ACQUIRE_THREAD(&opal_pmix_base.lock);
    if (0 >= opal_pmix_base.initialized) {
        OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);
        return OPAL_ERR_NOT_INITIALIZED;
    }

    /* convert the list of procs to an array
     * of pmix_proc_t */
    if (NULL != procs && 0 < (cnt = opal_list_get_size(procs))) {
        PMIX_PROC_CREATE(parray, cnt);
        n=0;
        OPAL_LIST_FOREACH(ptr, procs, opal_namelist_t) {
            if (NULL == (nsptr = pmix3x_convert_jobid(ptr->name.jobid))) {
                PMIX_PROC_FREE(parray, cnt);
                OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);
                return OPAL_ERR_NOT_FOUND;
            }
            (void)strncpy(parray[n].nspace, nsptr, PMIX_MAX_NSLEN);
            parray[n].rank = pmix3x_convert_opalrank(ptr->name.vpid);
            ++n;
        }
    }
    OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);

    if (collect_data) {
        PMIX_INFO_CONSTRUCT(&info);
        (void)strncpy(info.key, PMIX_COLLECT_DATA, PMIX_MAX_KEYLEN);
        info.value.type = PMIX_BOOL;
        info.value.data.flag = true;
        iptr = &info;
        n = 1;
    } else {
        iptr = NULL;
        n = 0;
    }

    rc = PMIx_Fence(parray, cnt, iptr, n);
    if (collect_data) {
        PMIX_INFO_DESTRUCT(&info);
    }
    if (NULL != parray) {
        PMIX_PROC_FREE(parray, cnt);
    }

    return pmix3x_convert_rc(rc);
}

int pmix3x_fencenb(opal_list_t *procs, int collect_data,
                   opal_pmix_op_cbfunc_t cbfunc, void *cbdata)
{
    pmix_status_t rc;
    pmix_proc_t *parray=NULL;
    size_t n, cnt=0;
    opal_namelist_t *ptr;
    pmix3x_opcaddy_t *op;
    char *nsptr;

    opal_output_verbose(1, opal_pmix_base_framework.framework_output,
                        "PMIx_client fencenb");

    OPAL_PMIX_ACQUIRE_THREAD(&opal_pmix_base.lock);
    if (0 >= opal_pmix_base.initialized) {
        OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);
        return OPAL_ERR_NOT_INITIALIZED;
    }

    /* convert the list of procs to an array
     * of pmix_proc_t */
    if (NULL != procs && 0 < (cnt = opal_list_get_size(procs))) {
        PMIX_PROC_CREATE(parray, cnt);
        n=0;
        OPAL_LIST_FOREACH(ptr, procs, opal_namelist_t) {
            if (NULL == (nsptr = pmix3x_convert_jobid(ptr->name.jobid))) {
                PMIX_PROC_FREE(parray, cnt);
                OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);
                return OPAL_ERR_NOT_FOUND;
            }
            (void)strncpy(parray[n].nspace, nsptr, PMIX_MAX_NSLEN);
            parray[n].rank = pmix3x_convert_opalrank(ptr->name.vpid);
            ++n;
        }
    }
    OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);

    /* create the caddy */
    op = OBJ_NEW(pmix3x_opcaddy_t);
    op->opcbfunc = cbfunc;
    op->cbdata = cbdata;
    op->procs = parray;
    op->nprocs = cnt;

    if (collect_data) {
        op->ninfo = 1;
        PMIX_INFO_CREATE(op->info, op->ninfo);
        PMIX_INFO_LOAD(&op->info[0], PMIX_COLLECT_DATA, NULL, PMIX_BOOL);
    }

    /* call the library function */
    rc = PMIx_Fence_nb(op->procs, op->nprocs, op->info, op->ninfo, opcbfunc, op);
    return pmix3x_convert_rc(rc);
}

int pmix3x_put(opal_pmix_scope_t opal_scope,
               opal_value_t *val)
{
    pmix_value_t kv;
    pmix_scope_t pmix_scope = pmix3x_convert_opalscope(opal_scope);
    pmix_status_t rc;

    opal_output_verbose(1, opal_pmix_base_framework.framework_output,
                        "PMIx_client put");

    OPAL_PMIX_ACQUIRE_THREAD(&opal_pmix_base.lock);
    if (0 >= opal_pmix_base.initialized) {
        OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);
        return OPAL_ERR_NOT_INITIALIZED;
    }
    OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);

    PMIX_VALUE_CONSTRUCT(&kv);
    pmix3x_value_load(&kv, val);

    rc = PMIx_Put(pmix_scope, val->key, &kv);
    PMIX_VALUE_DESTRUCT(&kv);
    return pmix3x_convert_rc(rc);
}

int pmix3x_get(const opal_process_name_t *proc, const char *key,
               opal_list_t *info, opal_value_t **val)
{
    pmix_status_t rc;
    pmix_proc_t p;
    char *nsptr;
    pmix_info_t *pinfo = NULL;
    size_t sz = 0, n;
    opal_value_t *ival;
    pmix_value_t *pval = NULL;
    int ret;

    opal_output_verbose(1, opal_pmix_base_framework.framework_output,
                        "%s pmix3x:client get on proc %s key %s",
                        OPAL_NAME_PRINT(OPAL_PROC_MY_NAME),
                        (NULL == proc) ? "NULL" : OPAL_NAME_PRINT(*proc), key);

    OPAL_PMIX_ACQUIRE_THREAD(&opal_pmix_base.lock);
    if (0 >= opal_pmix_base.initialized) {
        OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);
        return OPAL_ERR_NOT_INITIALIZED;
    }

    if (NULL == proc) {
        /* if they are asking for our jobid, then return it */
        if (0 == strcmp(key, OPAL_PMIX_JOBID)) {
            (*val) = OBJ_NEW(opal_value_t);
            (*val)->key = strdup(key);
            (*val)->type = OPAL_UINT32;
            (*val)->data.uint32 = OPAL_PROC_MY_NAME.jobid;
            OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);
            return OPAL_SUCCESS;
        }
        /* if they are asking for our rank, return it */
        if (0 == strcmp(key, OPAL_PMIX_RANK)) {
            (*val) = OBJ_NEW(opal_value_t);
            (*val)->key = strdup(key);
            (*val)->type = OPAL_INT;
            (*val)->data.integer = pmix3x_convert_rank(mca_pmix_pmix3x_component.myproc.rank);
            OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);
            return OPAL_SUCCESS;
        }
    }
    *val = NULL;

    if (NULL == proc) {
        (void)strncpy(p.nspace, mca_pmix_pmix3x_component.myproc.nspace, PMIX_MAX_NSLEN);
        p.rank = pmix3x_convert_rank(PMIX_RANK_WILDCARD);
    } else {
        if (NULL == (nsptr = pmix3x_convert_jobid(proc->jobid))) {
            OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);
            return OPAL_ERR_NOT_FOUND;
        }
        (void)strncpy(p.nspace, nsptr, PMIX_MAX_NSLEN);
        p.rank = pmix3x_convert_opalrank(proc->vpid);
    }
    OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);

    if (NULL != info && 0 < (sz = opal_list_get_size(info))) {
        PMIX_INFO_CREATE(pinfo, sz);
        n=0;
        OPAL_LIST_FOREACH(ival, info, opal_value_t) {
            (void)strncpy(pinfo[n].key, ival->key, PMIX_MAX_KEYLEN);
            pmix3x_value_load(&pinfo[n].value, ival);
            ++n;
        }
    }

    rc = PMIx_Get(&p, key, pinfo, sz, &pval);
    if (PMIX_SUCCESS == rc) {
        ival = OBJ_NEW(opal_value_t);
        if (NULL != key) {
            ival->key = strdup(key);
        }
        if (OPAL_SUCCESS != (ret = pmix3x_value_unload(ival, pval))) {
            rc = pmix3x_convert_opalrc(ret);
        } else {
            *val = ival;
        }
        PMIX_VALUE_FREE(pval, 1);
    }
    PMIX_INFO_FREE(pinfo, sz);

    return pmix3x_convert_rc(rc);
}

static void val_cbfunc(pmix_status_t status,
                       pmix_value_t *kv, void *cbdata)
{
    pmix3x_opcaddy_t *op = (pmix3x_opcaddy_t*)cbdata;
    int rc;
    opal_value_t val, *v=NULL;

    OPAL_ACQUIRE_OBJECT(op);
    OBJ_CONSTRUCT(&val, opal_value_t);
    if (NULL != op->nspace) {
        val.key = strdup(op->nspace);
    }
    rc = pmix3x_convert_opalrc(status);
    if (PMIX_SUCCESS == status && NULL != kv) {
        rc = pmix3x_value_unload(&val, kv);
        v = &val;
    }

    if (NULL != op->valcbfunc) {
        op->valcbfunc(rc, v, op->cbdata);
    }
    OBJ_DESTRUCT(&val);
    OBJ_RELEASE(op);
}

int pmix3x_getnb(const opal_process_name_t *proc, const char *key,
                 opal_list_t *info,
                 opal_pmix_value_cbfunc_t cbfunc, void *cbdata)
{
    pmix3x_opcaddy_t *op;
    opal_value_t *val;
    pmix_status_t rc;
    char *nsptr;
    size_t n;

    opal_output_verbose(1, opal_pmix_base_framework.framework_output,
                        "%s PMIx_client get_nb on proc %s key %s",
                        OPAL_NAME_PRINT(OPAL_PROC_MY_NAME),
                        (NULL == proc) ? "NULL" : OPAL_NAME_PRINT(*proc), key);

    OPAL_PMIX_ACQUIRE_THREAD(&opal_pmix_base.lock);
    if (0 >= opal_pmix_base.initialized) {
        OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);
        return OPAL_ERR_NOT_INITIALIZED;
    }

    if (NULL == proc) {
        /* if they are asking for our jobid, then return it */
        if (0 == strcmp(key, OPAL_PMIX_JOBID)) {
            if (NULL != cbfunc) {
                val = OBJ_NEW(opal_value_t);
                val->key = strdup(key);
                val->type = OPAL_UINT32;
                val->data.uint32 = OPAL_PROC_MY_NAME.jobid;
                cbfunc(OPAL_SUCCESS, val, cbdata);
            }
            OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);
            return OPAL_SUCCESS;
        }
        /* if they are asking for our rank, return it */
        if (0 == strcmp(key, OPAL_PMIX_RANK)) {
            if (NULL != cbfunc) {
                val = OBJ_NEW(opal_value_t);
                val->key = strdup(key);
                val->type = OPAL_INT;
                val->data.integer = pmix3x_convert_rank(mca_pmix_pmix3x_component.myproc.rank);
                cbfunc(OPAL_SUCCESS, val, cbdata);
            }
            OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);
            return OPAL_SUCCESS;
        }
    }

    /* create the caddy */
    op = OBJ_NEW(pmix3x_opcaddy_t);
    op->valcbfunc = cbfunc;
    op->cbdata = cbdata;
    if (NULL != key) {
        op->nspace = strdup(key);
    }
    if (NULL == proc) {
        (void)strncpy(op->p.nspace, mca_pmix_pmix3x_component.myproc.nspace, PMIX_MAX_NSLEN);
        op->p.rank = pmix3x_convert_rank(PMIX_RANK_WILDCARD);
    } else {
        if (NULL == (nsptr = pmix3x_convert_jobid(proc->jobid))) {
            OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);
            return OPAL_ERR_NOT_FOUND;
        }
        (void)strncpy(op->p.nspace, nsptr, PMIX_MAX_NSLEN);
        op->p.rank = pmix3x_convert_opalrank(proc->vpid);
    }
    OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);

    if (NULL != info && 0 < (op->sz = opal_list_get_size(info))) {
        PMIX_INFO_CREATE(op->info, op->sz);
        n=0;
        OPAL_LIST_FOREACH(val, info, opal_value_t) {
            (void)strncpy(op->info[n].key, val->key, PMIX_MAX_KEYLEN);
            pmix3x_value_load(&op->info[n].value, val);
            ++n;
        }
    }

    /* call the library function */
    rc = PMIx_Get_nb(&op->p, key, op->info, op->sz, val_cbfunc, op);
    if (PMIX_SUCCESS != rc) {
        OBJ_RELEASE(op);
    }

    return pmix3x_convert_rc(rc);
}

int pmix3x_publish(opal_list_t *info)
{
    pmix_info_t *pinfo;
    pmix_status_t ret;
    opal_value_t *iptr;
    size_t sz, n;

    opal_output_verbose(1, opal_pmix_base_framework.framework_output,
                        "PMIx_client publish");

    OPAL_PMIX_ACQUIRE_THREAD(&opal_pmix_base.lock);
    if (0 >= opal_pmix_base.initialized) {
        OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);
        return OPAL_ERR_NOT_INITIALIZED;
    }
    OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);

    if (NULL == info) {
        return OPAL_ERR_BAD_PARAM;
    }

    sz = opal_list_get_size(info);
    if (0 < sz) {
        PMIX_INFO_CREATE(pinfo, sz);
        n=0;
        OPAL_LIST_FOREACH(iptr, info, opal_value_t) {
            (void)strncpy(pinfo[n].key, iptr->key, PMIX_MAX_KEYLEN);
            pmix3x_value_load(&pinfo[n].value, iptr);
            ++n;
        }
    } else {
        pinfo = NULL;
    }

    ret = PMIx_Publish(pinfo, sz);
    if (0 < sz) {
        PMIX_INFO_FREE(pinfo, sz);
    }

    return pmix3x_convert_rc(ret);
}

int pmix3x_publishnb(opal_list_t *info,
                     opal_pmix_op_cbfunc_t cbfunc, void *cbdata)
{
    pmix_status_t ret;
    opal_value_t *iptr;
    size_t n;
    pmix3x_opcaddy_t *op;

    opal_output_verbose(1, opal_pmix_base_framework.framework_output,
                        "PMIx_client publish_nb");

    OPAL_PMIX_ACQUIRE_THREAD(&opal_pmix_base.lock);
    if (0 >= opal_pmix_base.initialized) {
        OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);
        return OPAL_ERR_NOT_INITIALIZED;
    }
    OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);

    if (NULL == info) {
        return OPAL_ERR_BAD_PARAM;
    }

    /* create the caddy */
    op = OBJ_NEW(pmix3x_opcaddy_t);
    op->opcbfunc = cbfunc;
    op->cbdata = cbdata;

    op->sz = opal_list_get_size(info);
    if (0 < op->sz) {
        PMIX_INFO_CREATE(op->info, op->sz);
        n=0;
        OPAL_LIST_FOREACH(iptr, info, opal_value_t) {
            (void)strncpy(op->info[n].key, iptr->key, PMIX_MAX_KEYLEN);
            pmix3x_value_load(&op->info[n].value, iptr);
            ++n;
        }
    }

    ret = PMIx_Publish_nb(op->info, op->sz, opcbfunc, op);

    return pmix3x_convert_rc(ret);
}

int pmix3x_lookup(opal_list_t *data, opal_list_t *info)
{
    opal_pmix_pdata_t *d;
    pmix_pdata_t *pdata;
    pmix_info_t *pinfo = NULL;
    pmix_status_t rc;
    size_t cnt, n, sz = 0;
    opal_value_t *iptr;
    opal_pmix3x_jobid_trkr_t *jptr, *job;
    int ret;

    opal_output_verbose(1, opal_pmix_base_framework.framework_output,
                        "pmix3x:client lookup");

    OPAL_PMIX_ACQUIRE_THREAD(&opal_pmix_base.lock);
    if (0 >= opal_pmix_base.initialized) {
        OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);
        return OPAL_ERR_NOT_INITIALIZED;
    }
    OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);

    if (NULL == data || 0 == (cnt = opal_list_get_size(data))) {
        return OPAL_ERR_BAD_PARAM;
    }
    PMIX_PDATA_CREATE(pdata, cnt);
    n = 0;
    OPAL_LIST_FOREACH(d, data, opal_pmix_pdata_t) {
        (void)strncpy(pdata[n].key, d->value.key, PMIX_MAX_KEYLEN);
        ++n;
    }

    if (NULL != info && 0 < (sz = opal_list_get_size(info))) {
        PMIX_INFO_CREATE(pinfo, sz);
        n=0;
        OPAL_LIST_FOREACH(iptr, info, opal_value_t) {
            (void)strncpy(pinfo[n].key, iptr->key, PMIX_MAX_KEYLEN);
            pmix3x_value_load(&pinfo[n].value, iptr);
            ++n;
        }
    }

    rc = PMIx_Lookup(pdata, cnt, pinfo, sz);
    if (PMIX_SUCCESS == rc) {
        /* load the answers back into the list */
        n=0;
        OPAL_PMIX_ACQUIRE_THREAD(&opal_pmix_base.lock);
        OPAL_LIST_FOREACH(d, data, opal_pmix_pdata_t) {
            if (mca_pmix_pmix3x_component.native_launch) {
                /* if we were launched by the OMPI RTE, then
                 * the jobid is in a special format - so get it */
                opal_convert_string_to_jobid(&d->proc.jobid, pdata[n].proc.nspace);
            } else {
                /* we were launched by someone else, so make the
                 * jobid just be the hash of the nspace */
                OPAL_HASH_JOBID(pdata[n].proc.nspace, d->proc.jobid);
            }
            /* if we don't already have it, add this to our jobid tracker */
            job = NULL;
            OPAL_LIST_FOREACH(jptr, &mca_pmix_pmix3x_component.jobids, opal_pmix3x_jobid_trkr_t) {
                if (jptr->jobid == d->proc.jobid) {
                    job = jptr;
                    break;
                }
            }
            if (NULL == job) {
                job = OBJ_NEW(opal_pmix3x_jobid_trkr_t);
                (void)strncpy(job->nspace, pdata[n].proc.nspace, PMIX_MAX_NSLEN);
                job->jobid = d->proc.jobid;
                opal_list_append(&mca_pmix_pmix3x_component.jobids, &job->super);
            }
            d->proc.vpid = pmix3x_convert_rank(pdata[n].proc.rank);
            if (OPAL_SUCCESS != (ret = pmix3x_value_unload(&d->value, &pdata[n].value))) {
                OPAL_ERROR_LOG(ret);
            }
        }
        OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);
    }
    PMIX_PDATA_FREE(pdata, cnt);
    if (NULL != pinfo) {
        PMIX_INFO_FREE(pinfo, sz);
    }
    return pmix3x_convert_rc(rc);
}

static void lk_cbfunc(pmix_status_t status,
                      pmix_pdata_t data[], size_t ndata,
                      void *cbdata)
{
    pmix3x_opcaddy_t *op = (pmix3x_opcaddy_t*)cbdata;
    opal_pmix_pdata_t *d;
    opal_list_t results, *r = NULL;
    int rc;
    size_t n;
    opal_pmix3x_jobid_trkr_t *job, *jptr;

    OPAL_ACQUIRE_OBJECT(op);

    if (NULL == op->lkcbfunc) {
        OBJ_RELEASE(op);
        return;
    }

    rc = pmix3x_convert_rc(op->status);
    if (OPAL_SUCCESS == rc) {
        OPAL_PMIX_ACQUIRE_THREAD(&opal_pmix_base.lock);
        OBJ_CONSTRUCT(&results, opal_list_t);
        for (n=0; n < ndata; n++) {
            d = OBJ_NEW(opal_pmix_pdata_t);
            opal_list_append(&results, &d->super);
            if (mca_pmix_pmix3x_component.native_launch) {
                /* if we were launched by the OMPI RTE, then
                 * the jobid is in a special format - so get it */
                opal_convert_string_to_jobid(&d->proc.jobid, data[n].proc.nspace);
            } else {
                /* we were launched by someone else, so make the
                 * jobid just be the hash of the nspace */
                OPAL_HASH_JOBID(data[n].proc.nspace, d->proc.jobid);
            }
            /* if we don't already have it, add this to our jobid tracker */
            job = NULL;
            OPAL_LIST_FOREACH(jptr, &mca_pmix_pmix3x_component.jobids, opal_pmix3x_jobid_trkr_t) {
                if (jptr->jobid == d->proc.jobid) {
                    job = jptr;
                    break;
                }
            }
            if (NULL == job) {
                job = OBJ_NEW(opal_pmix3x_jobid_trkr_t);
                (void)strncpy(job->nspace, data[n].proc.nspace, PMIX_MAX_NSLEN);
                job->jobid = d->proc.jobid;
                opal_list_append(&mca_pmix_pmix3x_component.jobids, &job->super);
            }
            d->proc.vpid = pmix3x_convert_rank(data[n].proc.rank);
            d->value.key = strdup(data[n].key);
            rc = pmix3x_value_unload(&d->value, &data[n].value);
            if (OPAL_SUCCESS != rc) {
                rc = OPAL_ERR_BAD_PARAM;
                OPAL_ERROR_LOG(rc);
                OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);
                goto release;
            }
        }
        r = &results;
        OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);
    }

  release:
    /* execute the callback */
    op->lkcbfunc(rc, r, op->cbdata);

    if (NULL != r) {
        OPAL_LIST_DESTRUCT(&results);
    }
    OBJ_RELEASE(op);
}

int pmix3x_lookupnb(char **keys, opal_list_t *info,
                    opal_pmix_lookup_cbfunc_t cbfunc, void *cbdata)
{
    pmix_status_t ret;
    pmix3x_opcaddy_t *op;
    opal_value_t *iptr;
    size_t n;


    opal_output_verbose(1, opal_pmix_base_framework.framework_output,
                        "pmix3x:client lookup_nb");

    OPAL_PMIX_ACQUIRE_THREAD(&opal_pmix_base.lock);
    if (0 >= opal_pmix_base.initialized) {
        OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);
        return OPAL_ERR_NOT_INITIALIZED;
    }
    OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);

    /* create the caddy */
    op = OBJ_NEW(pmix3x_opcaddy_t);
    op->lkcbfunc = cbfunc;
    op->cbdata = cbdata;

    if (NULL != info && 0 < (op->sz = opal_list_get_size(info))) {
        PMIX_INFO_CREATE(op->info, op->sz);
        n=0;
        OPAL_LIST_FOREACH(iptr, info, opal_value_t) {
            (void)strncpy(op->info[n].key, iptr->key, PMIX_MAX_KEYLEN);
            pmix3x_value_load(&op->info[n].value, iptr);
            ++n;
        }
    }
    ret = PMIx_Lookup_nb(keys, op->info, op->sz, lk_cbfunc, op);

    return pmix3x_convert_rc(ret);
}

int pmix3x_unpublish(char **keys, opal_list_t *info)
{
    pmix_status_t ret;
    size_t ninfo, n;
    pmix_info_t *pinfo;
    opal_value_t *iptr;

    OPAL_PMIX_ACQUIRE_THREAD(&opal_pmix_base.lock);
    if (0 >= opal_pmix_base.initialized) {
        OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);
        return OPAL_ERR_NOT_INITIALIZED;
    }
    OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);

    if (NULL != info && 0 < (ninfo = opal_list_get_size(info))) {
        PMIX_INFO_CREATE(pinfo, ninfo);
        n=0;
        OPAL_LIST_FOREACH(iptr, info, opal_value_t) {
            (void)strncpy(pinfo[n].key, iptr->key, PMIX_MAX_KEYLEN);
            pmix3x_value_load(&pinfo[n].value, iptr);
            ++n;
        }
    } else {
        pinfo = NULL;
        ninfo = 0;
    }

    ret = PMIx_Unpublish(keys, pinfo, ninfo);
    PMIX_INFO_FREE(pinfo, ninfo);

    return pmix3x_convert_rc(ret);
}

int pmix3x_unpublishnb(char **keys, opal_list_t *info,
                       opal_pmix_op_cbfunc_t cbfunc, void *cbdata)
{
    pmix_status_t ret;
    pmix3x_opcaddy_t *op;
    opal_value_t *iptr;
    size_t n;

    OPAL_PMIX_ACQUIRE_THREAD(&opal_pmix_base.lock);
    if (0 >= opal_pmix_base.initialized) {
        OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);
        return OPAL_ERR_NOT_INITIALIZED;
    }
    OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);

    /* create the caddy */
    op = OBJ_NEW(pmix3x_opcaddy_t);
    op->opcbfunc = cbfunc;
    op->cbdata = cbdata;

    if (NULL != info && 0 < (op->sz = opal_list_get_size(info))) {
        PMIX_INFO_CREATE(op->info, op->sz);
        n=0;
        OPAL_LIST_FOREACH(iptr, info, opal_value_t) {
            (void)strncpy(op->info[n].key, iptr->key, PMIX_MAX_KEYLEN);
            pmix3x_value_load(&op->info[n].value, iptr);
            ++n;
        }
    }

    ret = PMIx_Unpublish_nb(keys, op->info, op->sz, opcbfunc, op);

    return pmix3x_convert_rc(ret);
}

int pmix3x_spawn(opal_list_t *job_info, opal_list_t *apps, opal_jobid_t *jobid)
{
    pmix_status_t rc;
    pmix_info_t *info = NULL;
    pmix_app_t *papps;
    size_t ninfo = 0, napps, n, m;
    opal_value_t *ival;
    opal_pmix_app_t *app;
    char nspace[PMIX_MAX_NSLEN+1];
    opal_pmix3x_jobid_trkr_t *job;

    OPAL_PMIX_ACQUIRE_THREAD(&opal_pmix_base.lock);
    if (0 >= opal_pmix_base.initialized) {
        OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);
        return OPAL_ERR_NOT_INITIALIZED;
    }
    OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);

    *jobid = OPAL_JOBID_INVALID;

    if (NULL != job_info && 0 < (ninfo = opal_list_get_size(job_info))) {
        PMIX_INFO_CREATE(info, ninfo);
        n=0;
        OPAL_LIST_FOREACH(ival, job_info, opal_value_t) {
            (void)strncpy(info[n].key, ival->key, PMIX_MAX_KEYLEN);
            pmix3x_value_load(&info[n].value, ival);
            ++n;
        }
    }

    napps = opal_list_get_size(apps);
    PMIX_APP_CREATE(papps, napps);
    n=0;
    OPAL_LIST_FOREACH(app, apps, opal_pmix_app_t) {
        papps[n].cmd = strdup(app->cmd);
        if (NULL != app->argv) {
            papps[n].argv = opal_argv_copy(app->argv);
        }
        if (NULL != app->env) {
            papps[n].env = opal_argv_copy(app->env);
        }
        if (NULL != app->cwd) {
            papps[n].cwd = strdup(app->cwd);
        }
        papps[n].maxprocs = app->maxprocs;
        if (0 < (papps[n].ninfo = opal_list_get_size(&app->info))) {
            PMIX_INFO_CREATE(papps[n].info, papps[n].ninfo);
            m=0;
            OPAL_LIST_FOREACH(ival, &app->info, opal_value_t) {
                (void)strncpy(papps[n].info[m].key, ival->key, PMIX_MAX_KEYLEN);
                pmix3x_value_load(&papps[n].info[m].value, ival);
                ++m;
            }
        }
        ++n;
    }

    rc = PMIx_Spawn(info, ninfo, papps, napps, nspace);
    if (PMIX_SUCCESS == rc) {
        OPAL_PMIX_ACQUIRE_THREAD(&opal_pmix_base.lock);
        if (mca_pmix_pmix3x_component.native_launch) {
            /* if we were launched by the OMPI RTE, then
             * the jobid is in a special format - so get it */
            opal_convert_string_to_jobid(jobid, nspace);
        } else {
            /* we were launched by someone else, so make the
             * jobid just be the hash of the nspace */
            OPAL_HASH_JOBID(nspace, *jobid);
        }
        /* add this to our jobid tracker */
        job = OBJ_NEW(opal_pmix3x_jobid_trkr_t);
        (void)strncpy(job->nspace, nspace, PMIX_MAX_NSLEN);
        job->jobid = *jobid;
        opal_list_append(&mca_pmix_pmix3x_component.jobids, &job->super);
        OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);
    }
    return rc;
}

static void spcbfunc(pmix_status_t status,
                     char *nspace, void *cbdata)
{
    pmix3x_opcaddy_t *op = (pmix3x_opcaddy_t*)cbdata;
    opal_pmix3x_jobid_trkr_t *job;
    opal_jobid_t jobid = OPAL_JOBID_INVALID;
    int rc;

    OPAL_ACQUIRE_OBJECT(op);

    rc = pmix3x_convert_rc(status);
    if (PMIX_SUCCESS == status) {
        /* this is in the PMIx local thread - need to protect
         * the framework-level data */
        OPAL_PMIX_ACQUIRE_THREAD(&opal_pmix_base.lock);
        if (mca_pmix_pmix3x_component.native_launch) {
            /* if we were launched by the OMPI RTE, then
             * the jobid is in a special format - so get it */
            opal_convert_string_to_jobid(&jobid, nspace);
        } else {
            /* we were launched by someone else, so make the
             * jobid just be the hash of the nspace */
            OPAL_HASH_JOBID(nspace, jobid);
        }
        /* add this to our jobid tracker */
        job = OBJ_NEW(opal_pmix3x_jobid_trkr_t);
        (void)strncpy(job->nspace, nspace, PMIX_MAX_NSLEN);
        job->jobid = jobid;
        opal_list_append(&mca_pmix_pmix3x_component.jobids, &job->super);
        OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);
    }

    op->spcbfunc(rc, jobid, op->cbdata);
    OBJ_RELEASE(op);
}

int pmix3x_spawnnb(opal_list_t *job_info, opal_list_t *apps,
                   opal_pmix_spawn_cbfunc_t cbfunc, void *cbdata)
{
    pmix_status_t ret;
    pmix3x_opcaddy_t *op;
    size_t n, m;
    opal_value_t *info;
    opal_pmix_app_t *app;

    OPAL_PMIX_ACQUIRE_THREAD(&opal_pmix_base.lock);
    if (0 >= opal_pmix_base.initialized) {
        OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);
        return OPAL_ERR_NOT_INITIALIZED;
    }
    OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);

    /* create the caddy */
    op = OBJ_NEW(pmix3x_opcaddy_t);
    op->spcbfunc = cbfunc;
    op->cbdata = cbdata;

    if (NULL != job_info && 0 < (op->ninfo = opal_list_get_size(job_info))) {
        PMIX_INFO_CREATE(op->info, op->ninfo);
        n=0;
        OPAL_LIST_FOREACH(info, job_info, opal_value_t) {
            (void)strncpy(op->info[n].key, info->key, PMIX_MAX_KEYLEN);
            pmix3x_value_load(&op->info[n].value, info);
            ++n;
        }
    }

    op->sz = opal_list_get_size(apps);
    PMIX_APP_CREATE(op->apps, op->sz);
    n=0;
    OPAL_LIST_FOREACH(app, apps, opal_pmix_app_t) {
        op->apps[n].cmd = strdup(app->cmd);
        if (NULL != app->argv) {
            op->apps[n].argv = opal_argv_copy(app->argv);
        }
        if (NULL != app->env) {
            op->apps[n].env = opal_argv_copy(app->env);
        }
        op->apps[n].maxprocs = app->maxprocs;
        if (0 < (op->apps[n].ninfo = opal_list_get_size(&app->info))) {
            PMIX_INFO_CREATE(op->apps[n].info, op->apps[n].ninfo);
            m=0;
            OPAL_LIST_FOREACH(info, &app->info, opal_value_t) {
                (void)strncpy(op->apps[n].info[m].key, info->key, PMIX_MAX_KEYLEN);
                pmix3x_value_load(&op->apps[n].info[m].value, info);
                ++m;
            }
        }
        ++n;
    }

    ret = PMIx_Spawn_nb(op->info, op->ninfo, op->apps, op->sz, spcbfunc, op);

    return pmix3x_convert_rc(ret);
}

int pmix3x_connect(opal_list_t *procs)
{
    pmix_proc_t *p;
    size_t nprocs;
    opal_namelist_t *ptr;
    pmix_status_t ret;
    char *nsptr;
    size_t n;

    opal_output_verbose(1, opal_pmix_base_framework.framework_output,
                        "pmix3x:client connect");

    /* protect against bozo error */
    if (NULL == procs || 0 == (nprocs = opal_list_get_size(procs))) {
        return OPAL_ERR_BAD_PARAM;
    }

    OPAL_PMIX_ACQUIRE_THREAD(&opal_pmix_base.lock);
    if (0 >= opal_pmix_base.initialized) {
        OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);
        return OPAL_ERR_NOT_INITIALIZED;
    }

    /* convert the list of procs to an array
     * of pmix_proc_t */
    PMIX_PROC_CREATE(p, nprocs);
    n=0;
    OPAL_LIST_FOREACH(ptr, procs, opal_namelist_t) {
        if (NULL == (nsptr = pmix3x_convert_jobid(ptr->name.jobid))) {
            PMIX_PROC_FREE(p, nprocs);
            OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);
            return OPAL_ERR_NOT_FOUND;
        }
        (void)strncpy(p[n].nspace, nsptr, PMIX_MAX_NSLEN);
        p[n].rank = pmix3x_convert_opalrank(ptr->name.vpid);
        ++n;
    }
    OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);

    ret = PMIx_Connect(p, nprocs, NULL, 0);
    PMIX_PROC_FREE(p, nprocs);

    return pmix3x_convert_rc(ret);
}

int pmix3x_connectnb(opal_list_t *procs,
                     opal_pmix_op_cbfunc_t cbfunc,
                     void *cbdata)
{
    pmix3x_opcaddy_t *op;
    opal_namelist_t *ptr;
    pmix_status_t ret;
    char *nsptr;
    size_t n;

    opal_output_verbose(1, opal_pmix_base_framework.framework_output,
                        "pmix3x:client connect NB");

    /* protect against bozo error */
    if (NULL == procs || 0 == opal_list_get_size(procs)) {
        return OPAL_ERR_BAD_PARAM;
    }

    OPAL_PMIX_ACQUIRE_THREAD(&opal_pmix_base.lock);
    if (0 >= opal_pmix_base.initialized) {
        OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);
        return OPAL_ERR_NOT_INITIALIZED;
    }

    /* create the caddy */
    op = OBJ_NEW(pmix3x_opcaddy_t);
    op->opcbfunc = cbfunc;
    op->cbdata = cbdata;
    op->nprocs = opal_list_get_size(procs);

    /* convert the list of procs to an array
     * of pmix_proc_t */
    PMIX_PROC_CREATE(op->procs, op->nprocs);
    n=0;
    OPAL_LIST_FOREACH(ptr, procs, opal_namelist_t) {
        if (NULL == (nsptr = pmix3x_convert_jobid(ptr->name.jobid))) {
            OBJ_RELEASE(op);
            OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);
            return OPAL_ERR_NOT_FOUND;
        }
        (void)strncpy(op->procs[n].nspace, nsptr, PMIX_MAX_NSLEN);
        op->procs[n].rank = pmix3x_convert_opalrank(ptr->name.vpid);
        ++n;
    }
    OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);

    ret = PMIx_Connect_nb(op->procs, op->nprocs, NULL, 0, opcbfunc, op);
    if (PMIX_SUCCESS != ret) {
        OBJ_RELEASE(op);
    }
    return pmix3x_convert_rc(ret);
}

int pmix3x_disconnect(opal_list_t *procs)
{
    size_t nprocs, n;
    opal_namelist_t *ptr;
    pmix_status_t ret=PMIX_SUCCESS;
    pmix_proc_t *p;
    char *nsptr;

    opal_output_verbose(1, opal_pmix_base_framework.framework_output,
                        "pmix3x:client disconnect");

    /* protect against bozo error */
    if (NULL == procs || 0 == (nprocs = opal_list_get_size(procs))) {
        return OPAL_ERR_BAD_PARAM;
    }

    OPAL_PMIX_ACQUIRE_THREAD(&opal_pmix_base.lock);
    if (0 >= opal_pmix_base.initialized) {
        OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);
        return OPAL_ERR_NOT_INITIALIZED;
    }

    /* convert the list of procs to an array
     * of pmix_proc_t */
    PMIX_PROC_CREATE(p, nprocs);
    n=0;
    OPAL_LIST_FOREACH(ptr, procs, opal_namelist_t) {
        if (NULL == (nsptr = pmix3x_convert_jobid(ptr->name.jobid))) {
            PMIX_PROC_FREE(p, nprocs);
            OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);
            return OPAL_ERR_NOT_FOUND;
        }
        (void)strncpy(p[n].nspace, nsptr, PMIX_MAX_NSLEN);
        p[n].rank = pmix3x_convert_opalrank(ptr->name.vpid);
        ++n;
    }
    OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);

    ret = PMIx_Disconnect(p, nprocs, NULL, 0);
    PMIX_PROC_FREE(p, nprocs);

    return pmix3x_convert_rc(ret);
}

int pmix3x_disconnectnb(opal_list_t *procs,
                        opal_pmix_op_cbfunc_t cbfunc,
                        void *cbdata)
{
    pmix3x_opcaddy_t *op;
    opal_namelist_t *ptr;
    pmix_status_t ret;
    char *nsptr;
    size_t n;

    opal_output_verbose(1, opal_pmix_base_framework.framework_output,
                        "pmix3x:client disconnect NB");

    /* protect against bozo error */
    if (NULL == procs || 0 == opal_list_get_size(procs)) {
        return OPAL_ERR_BAD_PARAM;
    }

    OPAL_PMIX_ACQUIRE_THREAD(&opal_pmix_base.lock);
    if (0 >= opal_pmix_base.initialized) {
        OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);
        return OPAL_ERR_NOT_INITIALIZED;
    }

    /* create the caddy */
    op = OBJ_NEW(pmix3x_opcaddy_t);
    op->opcbfunc = cbfunc;
    op->cbdata = cbdata;
    op->nprocs = opal_list_get_size(procs);

    /* convert the list of procs to an array
     * of pmix_proc_t */
    PMIX_PROC_CREATE(op->procs, op->nprocs);
    n=0;
    OPAL_LIST_FOREACH(ptr, procs, opal_namelist_t) {
        if (NULL == (nsptr = pmix3x_convert_jobid(ptr->name.jobid))) {
            OBJ_RELEASE(op);
            OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);
            return OPAL_ERR_NOT_FOUND;
        }
        (void)strncpy(op->procs[n].nspace, nsptr, PMIX_MAX_NSLEN);
        op->procs[n].rank = pmix3x_convert_opalrank(ptr->name.vpid);
        ++n;
    }
    OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);

    ret = PMIx_Disconnect_nb(op->procs, op->nprocs, NULL, 0, opcbfunc, op);
    if (PMIX_SUCCESS != ret) {
        OBJ_RELEASE(op);
    }
    return pmix3x_convert_rc(ret);
}

int pmix3x_resolve_peers(const char *nodename,
                         opal_jobid_t jobid,
                         opal_list_t *procs)
{
    pmix_status_t ret;
    char *nspace;
    pmix_proc_t *array=NULL;
    size_t nprocs, n;
    opal_namelist_t *nm;
    opal_pmix3x_jobid_trkr_t *job;

    OPAL_PMIX_ACQUIRE_THREAD(&opal_pmix_base.lock);
    if (0 >= opal_pmix_base.initialized) {
        OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);
        return OPAL_ERR_NOT_INITIALIZED;
    }

    if (OPAL_JOBID_WILDCARD != jobid) {
        if (NULL == (nspace = pmix3x_convert_jobid(jobid))) {
            OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);
            return OPAL_ERR_NOT_FOUND;
        }
    } else {
        nspace = NULL;
    }
    OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);

    ret = PMIx_Resolve_peers(nodename, nspace, &array, &nprocs);

    if (NULL != array && 0 < nprocs) {
        OPAL_PMIX_ACQUIRE_THREAD(&opal_pmix_base.lock);
        for (n=0; n < nprocs; n++) {
            nm = OBJ_NEW(opal_namelist_t);
            opal_list_append(procs, &nm->super);
            if (mca_pmix_pmix3x_component.native_launch) {
                /* if we were launched by the OMPI RTE, then
                 * the jobid is in a special format - so get it */
                opal_convert_string_to_jobid(&nm->name.jobid, array[n].nspace);
            } else {
                /* we were launched by someone else, so make the
                 * jobid just be the hash of the nspace */
                OPAL_HASH_JOBID(array[n].nspace, nm->name.jobid);
            }
            /* if we don't already have it, add this to our jobid tracker */
            if (NULL == pmix3x_convert_jobid(nm->name.jobid)) {
                job = OBJ_NEW(opal_pmix3x_jobid_trkr_t);
                (void)strncpy(job->nspace, array[n].nspace, PMIX_MAX_NSLEN);
                job->jobid = nm->name.jobid;
                opal_list_append(&mca_pmix_pmix3x_component.jobids, &job->super);
            }
            nm->name.vpid = pmix3x_convert_rank(array[n].rank);
        }
        OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);
    }
    PMIX_PROC_FREE(array, nprocs);
    return pmix3x_convert_rc(ret);
}

int pmix3x_resolve_nodes(opal_jobid_t jobid, char **nodelist)
{
    pmix_status_t ret;
    char *nsptr;

    OPAL_PMIX_ACQUIRE_THREAD(&opal_pmix_base.lock);
    if (0 >= opal_pmix_base.initialized) {
        OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);
        return OPAL_ERR_NOT_INITIALIZED;
    }

    if (NULL == (nsptr = pmix3x_convert_jobid(jobid))) {
        OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);
        return OPAL_ERR_NOT_FOUND;
    }
    OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);

    ret = PMIx_Resolve_nodes(nsptr, nodelist);

    return pmix3x_convert_rc(ret);
}

static void relcbfunc(void *cbdata)
{
    pmix3x_opcaddy_t *op = (pmix3x_opcaddy_t*)cbdata;
    OBJ_RELEASE(op);
}

static void infocbfunc(pmix_status_t status,
                       pmix_info_t *info, size_t ninfo,
                       void *cbdata,
                       pmix_release_cbfunc_t release_fn,
                       void *release_cbdata)
{
    pmix3x_opcaddy_t *op = (pmix3x_opcaddy_t*)cbdata;
    int rc;

    if (NULL != release_fn) {
        release_fn(release_cbdata);
    }
    rc = pmix3x_convert_rc(status);
    if (NULL != op->qcbfunc) {
        op->qcbfunc(rc, NULL, op->cbdata, relcbfunc, op);
    } else {
        OBJ_RELEASE(op);
    }
}

int pmix3x_allocate(opal_pmix_alloc_directive_t directive,
                    opal_list_t *info,
                    opal_pmix_info_cbfunc_t cbfunc, void *cbdata)
{
    return OPAL_ERR_NOT_SUPPORTED;
}

int pmix3x_job_control(opal_list_t *targets,
                       opal_list_t *directives,
                       opal_pmix_info_cbfunc_t cbfunc, void *cbdata)
{
    pmix3x_opcaddy_t *op;
    size_t n;
    opal_namelist_t *ptr;
    opal_value_t *iptr;
    pmix_status_t rc;
    char *nsptr;

    OPAL_PMIX_ACQUIRE_THREAD(&opal_pmix_base.lock);
    if (0 >= opal_pmix_base.initialized) {
        OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);
        return OPAL_ERR_NOT_INITIALIZED;
    }
abort();

    /* create the caddy */
    op = OBJ_NEW(pmix3x_opcaddy_t);
    op->qcbfunc = cbfunc;
    op->cbdata = cbdata;
    if (NULL != targets) {
        op->nprocs = opal_list_get_size(targets);

        /* convert the list of procs to an array
         * of pmix_proc_t */
        PMIX_PROC_CREATE(op->procs, op->nprocs);
        n=0;
        OPAL_LIST_FOREACH(ptr, targets, opal_namelist_t) {
            if (NULL == (nsptr = pmix3x_convert_jobid(ptr->name.jobid))) {
                OBJ_RELEASE(op);
                OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);
                return OPAL_ERR_NOT_FOUND;
            }
            (void)strncpy(op->procs[n].nspace, nsptr, PMIX_MAX_NSLEN);
            op->procs[n].rank = pmix3x_convert_opalrank(ptr->name.vpid);
            ++n;
        }
    }
    OPAL_PMIX_RELEASE_THREAD(&opal_pmix_base.lock);

    if (NULL != directives && 0 < (op->ninfo = opal_list_get_size(directives))) {
        PMIX_INFO_CREATE(op->info, op->ninfo);
        n=0;
        OPAL_LIST_FOREACH(iptr, directives, opal_value_t) {
            (void)strncpy(op->info[n].key, iptr->key, PMIX_MAX_KEYLEN);
            pmix3x_value_load(&op->info[n].value, iptr);
            ++n;
        }
    }

    rc = PMIx_Job_control_nb(op->procs,op->nprocs, op->info, op->ninfo, infocbfunc, op);
    if (PMIX_SUCCESS != rc) {
        OBJ_RELEASE(op);
    }
    return pmix3x_convert_rc(rc);
}
