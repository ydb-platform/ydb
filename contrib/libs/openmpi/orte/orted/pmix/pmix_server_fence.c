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
 * Copyright (c) 2009      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2011      Oak Ridge National Labs.  All rights reserved.
 * Copyright (c) 2013-2018 Intel, Inc. All rights reserved.
 * Copyright (c) 2014      Mellanox Technologies, Inc.
 *                         All rights reserved.
 * Copyright (c) 2014-2017 Research Organization for Information Science
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

#include "opal/util/output.h"
#include "opal/dss/dss.h"

#include "orte/mca/errmgr/errmgr.h"
#include "orte/util/name_fns.h"
#include "orte/util/show_help.h"
#include "orte/util/threads.h"
#include "orte/runtime/orte_globals.h"
#include "orte/mca/grpcomm/grpcomm.h"
#include "orte/mca/rml/rml.h"

#include "pmix_server_internal.h"
#include "pmix_server.h"

static void relcb(void *cbdata)
{
    uint8_t *data = (uint8_t*)cbdata;

    if (NULL != data) {
        free(data);
    }
}
static void pmix_server_release(int status, opal_buffer_t *buf, void *cbdata)
{
    orte_pmix_mdx_caddy_t *cd=(orte_pmix_mdx_caddy_t*)cbdata;
    char *data = NULL;
    int32_t ndata = 0;
    int rc = OPAL_SUCCESS;

    ORTE_ACQUIRE_OBJECT(cd);

    /* unload the buffer */
    if (NULL != buf) {
        rc = opal_dss.unload(buf, (void**)&data, &ndata);
    }
    if (OPAL_SUCCESS == rc) {
        rc = status;
    }
    cd->cbfunc(rc, data, ndata, cd->cbdata, relcb, data);
    OBJ_RELEASE(cd);
}

/* this function is called when all the local participants have
 * called fence - thus, the collective is already locally
 * complete at this point. We therefore just need to create the
 * signature and pass the collective into grpcomm */
int pmix_server_fencenb_fn(opal_list_t *procs, opal_list_t *info,
                           char *data, size_t ndata,
                           opal_pmix_modex_cbfunc_t cbfunc, void *cbdata)
{
    orte_pmix_mdx_caddy_t *cd=NULL;
    int rc;
    opal_namelist_t *nm;
    size_t i;
    opal_buffer_t *buf=NULL;

    cd = OBJ_NEW(orte_pmix_mdx_caddy_t);
    cd->cbfunc = cbfunc;
    cd->cbdata = cbdata;

   /* compute the signature of this collective */
    if (NULL != procs) {
        cd->sig = OBJ_NEW(orte_grpcomm_signature_t);
        cd->sig->sz = opal_list_get_size(procs);
        cd->sig->signature = (orte_process_name_t*)malloc(cd->sig->sz * sizeof(orte_process_name_t));
        memset(cd->sig->signature, 0, cd->sig->sz * sizeof(orte_process_name_t));
        i=0;
        OPAL_LIST_FOREACH(nm, procs, opal_namelist_t) {
            cd->sig->signature[i].jobid = nm->name.jobid;
            cd->sig->signature[i].vpid = nm->name.vpid;
            ++i;
        }
    }
    buf = OBJ_NEW(opal_buffer_t);

    if (NULL != data) {
        opal_dss.load(buf, data, ndata);
    }

    if (4 < opal_output_get_verbosity(orte_pmix_server_globals.output)) {
        char *tmp=NULL;
        (void)opal_dss.print(&tmp, NULL, cd->sig, ORTE_SIGNATURE);
        free(tmp);
    }

    /* pass it to the global collective algorithm */
    /* pass along any data that was collected locally */
    if (ORTE_SUCCESS != (rc = orte_grpcomm.allgather(cd->sig, buf, pmix_server_release, cd))) {
        ORTE_ERROR_LOG(rc);
        OBJ_RELEASE(buf);
        return rc;
    }
    OBJ_RELEASE(buf);
    return ORTE_SUCCESS;
}

static void dmodex_req(int sd, short args, void *cbdata)
{
    pmix_server_req_t *req = (pmix_server_req_t*)cbdata;
    pmix_server_req_t *r;
    orte_job_t *jdata;
    orte_proc_t *proct, *dmn;
    int rc, rnum;
    opal_buffer_t *buf;
    uint8_t *data=NULL;
    int32_t sz=0;

    ORTE_ACQUIRE_OBJECT(rq);

    /* a race condition exists here because of the thread-shift - it is
     * possible that data for the specified proc arrived while we were
     * waiting to be serviced. In that case, the tracker that would have
     * indicated the data was already requested will have been removed,
     * and we would therefore think that we had to request it again.
     * So do a quick check to ensure we don't already have the desired
     * data */
    OPAL_MODEX_RECV_STRING(rc, "modex", &req->target, &data, &sz);
    if (OPAL_SUCCESS == rc) {
        req->mdxcbfunc(rc, (char*)data, sz, req->cbdata, relcb, data);
        OBJ_RELEASE(req);
        return;
    }

    /* adjust the timeout to reflect the size of the job as it can take some
     * amount of time to start the job */
    ORTE_ADJUST_TIMEOUT(req);

    /* has anyone already requested data for this target? If so,
     * then the data is already on its way */
    for (rnum=0; rnum < orte_pmix_server_globals.reqs.num_rooms; rnum++) {
        opal_hotel_knock(&orte_pmix_server_globals.reqs, rnum, (void**)&r);
        if (NULL == r) {
            continue;
        }
        if (r->target.jobid == req->target.jobid &&
            r->target.vpid == req->target.vpid) {
            /* save the request in the hotel until the
             * data is returned */
            if (OPAL_SUCCESS != (rc = opal_hotel_checkin(&orte_pmix_server_globals.reqs, req, &req->room_num))) {
                orte_show_help("help-orted.txt", "noroom", true, req->operation, orte_pmix_server_globals.num_rooms);
                /* can't just return as that would cause the requestor
                 * to hang, so instead execute the callback */
                goto callback;
            }
            return;
        }
    }

    /* lookup who is hosting this proc */
    if (NULL == (jdata = orte_get_job_data_object(req->target.jobid))) {
        /* if we don't know the job, then it could be a race
         * condition where we are being asked about a process
         * that we don't know about yet. In this case, just
         * record the request and we will process it later */
        if (OPAL_SUCCESS != (rc = opal_hotel_checkin(&orte_pmix_server_globals.reqs, req, &req->room_num))) {
            orte_show_help("help-orted.txt", "noroom", true, req->operation, orte_pmix_server_globals.num_rooms);
            /* can't just return as that would cause the requestor
             * to hang, so instead execute the callback */
            goto callback;
        }
        return;
    }
    /* if this is a request for rank=WILDCARD, then they want the job-level data
     * for this job. It was probably not stored locally because we aren't hosting
     * any local procs. There is no need to request the data as we already have
     * it - so just register the nspace so the local PMIx server gets it */
    if (ORTE_VPID_WILDCARD == req->target.vpid) {
        rc = orte_pmix_server_register_nspace(jdata, true);
        if (ORTE_SUCCESS != rc) {
            goto callback;
        }
        /* let the server know that the data is now available */
        if (NULL != req->mdxcbfunc) {
            req->mdxcbfunc(rc, NULL, 0, req->cbdata, NULL, NULL);
        }
        OBJ_RELEASE(req);
        return;
    }

    /* if they are asking about a specific proc, then fetch it */
    if (NULL == (proct = (orte_proc_t*)opal_pointer_array_get_item(jdata->procs, req->target.vpid))) {
        /* if we find the job, but not the process, then that is an error */
        ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
        rc = ORTE_ERR_NOT_FOUND;
        goto callback;
    }

    if (NULL == (dmn = proct->node->daemon)) {
        /* we don't know where this proc is located - since we already
         * found the job, and therefore know about its locations, this
         * must be an error */
        ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
        rc = ORTE_ERR_NOT_FOUND;
        goto callback;
    }

    /* point the request to the daemon that is hosting the
     * target process */
    req->proxy.vpid = dmn->name.vpid;

    /* track the request so we know the function and cbdata
     * to callback upon completion */
    if (OPAL_SUCCESS != (rc = opal_hotel_checkin(&orte_pmix_server_globals.reqs, req, &req->room_num))) {
        orte_show_help("help-orted.txt", "noroom", true, req->operation, orte_pmix_server_globals.num_rooms);
        goto callback;
    }

    /* if we are the host daemon, then this is a local request, so
     * just wait for the data to come in */
    if (ORTE_PROC_MY_NAME->jobid == dmn->name.jobid &&
        ORTE_PROC_MY_NAME->vpid == dmn->name.vpid) {
        return;
    }

    /* construct a request message */
    buf = OBJ_NEW(opal_buffer_t);
    if (OPAL_SUCCESS != (rc = opal_dss.pack(buf, &req->target, 1, OPAL_NAME))) {
        ORTE_ERROR_LOG(rc);
        opal_hotel_checkout(&orte_pmix_server_globals.reqs, req->room_num);
        OBJ_RELEASE(buf);
        goto callback;
    }
    /* include the request room number for quick retrieval */
    if (OPAL_SUCCESS != (rc = opal_dss.pack(buf, &req->room_num, 1, OPAL_INT))) {
        ORTE_ERROR_LOG(rc);
        opal_hotel_checkout(&orte_pmix_server_globals.reqs, req->room_num);
        OBJ_RELEASE(buf);
        goto callback;
    }

    /* send it to the host daemon */
    if (ORTE_SUCCESS != (rc = orte_rml.send_buffer_nb(orte_mgmt_conduit,
                                                      &dmn->name, buf, ORTE_RML_TAG_DIRECT_MODEX,
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

/* the local PMIx embedded server will use this function to call
 * us and request that we obtain data from a remote daemon */
int pmix_server_dmodex_req_fn(opal_process_name_t *proc, opal_list_t *info,
                              opal_pmix_modex_cbfunc_t cbfunc, void *cbdata)
{
    /*  we have to shift threads to the ORTE thread, so
     * create a request and push it into that thread */
    ORTE_DMX_REQ(*proc, dmodex_req, cbfunc, cbdata);
    return OPAL_ERR_IN_PROCESS;
}
