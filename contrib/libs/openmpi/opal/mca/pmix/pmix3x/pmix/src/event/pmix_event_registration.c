/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2014-2019 Intel, Inc.  All rights reserved.
 * Copyright (c) 2017-2018 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
#include <src/include/pmix_config.h>

#include <pmix.h>
#include <pmix_common.h>
#include <pmix_server.h>
#include <pmix_rename.h>

#include "src/threads/threads.h"
#include "src/util/error.h"
#include "src/util/output.h"

#include "src/client/pmix_client_ops.h"
#include "src/server/pmix_server_ops.h"
#include "src/include/pmix_globals.h"
#include "src/mca/bfrops/bfrops.h"
#include "src/event/pmix_event.h"

 typedef struct {
    pmix_object_t super;
    volatile bool active;
    pmix_event_t ev;
    size_t index;
    bool firstoverall;
    bool enviro;
    pmix_list_t *list;
    pmix_event_hdlr_t *hdlr;
    void *cd;
    pmix_status_t *codes;
    size_t ncodes;
    pmix_info_t *info;
    size_t ninfo;
    pmix_proc_t *affected;
    size_t naffected;
    pmix_notification_fn_t evhdlr;
    pmix_hdlr_reg_cbfunc_t evregcbfn;
    void *cbdata;
} pmix_rshift_caddy_t;
static void rscon(pmix_rshift_caddy_t *p)
{
    p->firstoverall = false;
    p->enviro = false;
    p->list = NULL;
    p->hdlr = NULL;
    p->cd = NULL;
    p->codes = NULL;
    p->ncodes = 0;
    p->info = NULL;
    p->ninfo = 0;
    p->affected = NULL;
    p->naffected = 0;
    p->evhdlr = NULL;
    p->evregcbfn = NULL;
    p->cbdata = NULL;
}
static void rsdes(pmix_rshift_caddy_t *p)
{
    if (0 < p->ncodes) {
        free(p->codes);
    }
    if (NULL != p->cd) {
        PMIX_RELEASE(p->cd);
    }
}
PMIX_CLASS_INSTANCE(pmix_rshift_caddy_t,
                    pmix_object_t,
                    rscon, rsdes);

static void check_cached_events(pmix_rshift_caddy_t *cd);

static void regevents_cbfunc(struct pmix_peer_t *peer, pmix_ptl_hdr_t *hdr,
                             pmix_buffer_t *buf, void *cbdata)
{
    pmix_rshift_caddy_t *rb = (pmix_rshift_caddy_t*)cbdata;
    pmix_rshift_caddy_t *cd = (pmix_rshift_caddy_t*)rb->cd;
    pmix_status_t rc, ret;
    int cnt;
    size_t index = rb->index;

    pmix_output_verbose(2, pmix_client_globals.event_output,
                        "pmix: regevents callback recvd");

    /* unpack the status code */
    cnt = 1;
    PMIX_BFROPS_UNPACK(rc, peer, buf, &ret, &cnt, PMIX_STATUS);
    if ((PMIX_SUCCESS != rc) ||
        (PMIX_SUCCESS != ret)) {
        if (PMIX_SUCCESS != rc) {
            PMIX_ERROR_LOG(rc);
        } else {
            PMIX_ERROR_LOG(ret);
        }
        /* remove the err handler and call the error handler reg completion callback fn.*/
        if (NULL == rb->list) {
            if (NULL != rb->hdlr) {
                PMIX_RELEASE(rb->hdlr);
            }
            if (rb->firstoverall) {
                pmix_globals.events.first = NULL;
            } else {
                pmix_globals.events.last = NULL;
            }
        } else if (NULL != rb->hdlr) {
            pmix_list_remove_item(rb->list, &rb->hdlr->super);
            PMIX_RELEASE(rb->hdlr);
        }
        ret = PMIX_ERR_SERVER_FAILED_REQUEST;
        index = UINT_MAX;
    }

    /* call the callback */
    if (NULL != cd && NULL != cd->evregcbfn) {
        cd->evregcbfn(ret, index, cd->cbdata);
    }
    if (NULL != cd) {
        /* check this event against anything in our cache */
        check_cached_events(cd);
    }

    /* release any info we brought along as they are
     * internally generated and not provided by the caller */
    if (NULL!= rb->info) {
        PMIX_INFO_FREE(rb->info, rb->ninfo);
    }
    if (NULL != rb->codes) {
        free(rb->codes);
    }
    PMIX_RELEASE(rb);
}

static void reg_cbfunc(pmix_status_t status, void *cbdata)
{
    pmix_rshift_caddy_t *rb = (pmix_rshift_caddy_t*)cbdata;
    pmix_rshift_caddy_t *cd = (pmix_rshift_caddy_t*)rb->cd;
    pmix_status_t rc = status;
    size_t index = rb->index;

    if (PMIX_SUCCESS != status) {
        /* if we failed to register, then remove this event */
        if (NULL == rb->list) {
            if (NULL != rb->hdlr) {
                PMIX_RELEASE(rb->hdlr);
            }
            if (rb->firstoverall) {
                pmix_globals.events.first = NULL;
            } else {
                pmix_globals.events.last = NULL;
            }
        } else if (NULL != rb->hdlr) {
            pmix_list_remove_item(rb->list, &rb->hdlr->super);
            PMIX_RELEASE(rb->hdlr);
        }
        rc = PMIX_ERR_SERVER_FAILED_REQUEST;
        index = UINT_MAX;
    }

    if (NULL != cd && NULL != cd->evregcbfn) {
        /* pass back our local index */
        cd->evregcbfn(rc, index, cd->cbdata);
    }

    /* release any info we brought along as they are
     * internally generated and not provided by the caller */
    if (NULL!= rb->info) {
        PMIX_INFO_FREE(rb->info, rb->ninfo);
    }
    if (NULL != rb->codes) {
        free(rb->codes);
    }
    PMIX_RELEASE(rb);
}

static pmix_status_t _send_to_server(pmix_rshift_caddy_t *rcd)
{
    pmix_rshift_caddy_t *cd = (pmix_rshift_caddy_t*)rcd->cd;
    pmix_status_t rc;
    pmix_buffer_t *msg;
    pmix_cmd_t cmd=PMIX_REGEVENTS_CMD;

    msg = PMIX_NEW(pmix_buffer_t);
    /* pack the cmd */
    PMIX_BFROPS_PACK(rc, pmix_client_globals.myserver, msg, &cmd, 1, PMIX_COMMAND);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        return rc;
    }
    /* pack the number of codes */
    PMIX_BFROPS_PACK(rc, pmix_client_globals.myserver, msg, &cd->ncodes, 1, PMIX_SIZE);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        return rc;
    }
    /* pack any provided codes */
    if (0 < cd->ncodes) {
        PMIX_BFROPS_PACK(rc, pmix_client_globals.myserver, msg, cd->codes, cd->ncodes, PMIX_STATUS);
        if (PMIX_SUCCESS != rc) {
            PMIX_ERROR_LOG(rc);
            return rc;
        }
    }

    /* pack the number of info */
    PMIX_BFROPS_PACK(rc, pmix_client_globals.myserver, msg, &rcd->ninfo, 1, PMIX_SIZE);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        return rc;
    }
    /* pack any provided info */
    if (0 < rcd->ninfo) {
        PMIX_BFROPS_PACK(rc, pmix_client_globals.myserver, msg, rcd->info, rcd->ninfo, PMIX_INFO);
        if (PMIX_SUCCESS != rc) {
            PMIX_ERROR_LOG(rc);
            return rc;
        }
    }
    PMIX_PTL_SEND_RECV(rc, pmix_client_globals.myserver, msg, regevents_cbfunc, rcd);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        PMIX_RELEASE(msg);
    }

    return rc;
}

static pmix_status_t _add_hdlr(pmix_rshift_caddy_t *cd, pmix_list_t *xfer)
{
    pmix_rshift_caddy_t *cd2;
    pmix_info_caddy_t *ixfer;
    size_t n;
    bool registered, need_register = false;
    pmix_active_code_t *active;
    pmix_status_t rc;

    pmix_output_verbose(2, pmix_client_globals.event_output,
                        "pmix: _add_hdlr");

    /* check to see if we have an active registration on these codes */
    if (NULL == cd->codes) {
        registered = false;
        PMIX_LIST_FOREACH(active, &pmix_globals.events.actives, pmix_active_code_t) {
            if (PMIX_MAX_ERR_CONSTANT == active->code) {
                /* we have registered a default */
                registered = true;
                ++active->nregs;
                break;
            }
        }
        if (!registered) {
            active = PMIX_NEW(pmix_active_code_t);
            active->code = PMIX_MAX_ERR_CONSTANT;
            active->nregs = 1;
            pmix_list_append(&pmix_globals.events.actives, &active->super);
            /* ensure we register it */
            need_register = true;
        }
    } else {
        for (n=0; n < cd->ncodes; n++) {
            registered = false;
            PMIX_LIST_FOREACH(active, &pmix_globals.events.actives, pmix_active_code_t) {
                if (active->code == cd->codes[n]) {
                    registered = true;
                    ++active->nregs;
                    break;
                }
            }
            if (!registered) {
                active = PMIX_NEW(pmix_active_code_t);
                active->code = cd->codes[n];
                active->nregs = 1;
                pmix_list_append(&pmix_globals.events.actives, &active->super);
                /* ensure we register it */
                need_register = true;
            }
        }
    }

    /* prep next step */
    cd2 = PMIX_NEW(pmix_rshift_caddy_t);
    cd2->index = cd->index;
    cd2->firstoverall = cd->firstoverall;
    cd2->list = cd->list;
    cd2->hdlr = cd->hdlr;
    PMIX_RETAIN(cd);
    cd2->cd = cd;
    cd2->ninfo = pmix_list_get_size(xfer);
    if (0 < cd2->ninfo) {
        PMIX_INFO_CREATE(cd2->info, cd2->ninfo);
        n=0;
        PMIX_LIST_FOREACH(ixfer, xfer, pmix_info_caddy_t) {
            PMIX_INFO_XFER(&cd2->info[n], ixfer->info);
            ++n;
        }
    }

    /* if we are a client, and we haven't already registered a handler of this
     * type with our server, or if we have directives, then we need to notify
     * the server - however, don't do this for a v1 server as the event
     * notification system there doesn't work */
    if ((!PMIX_PROC_IS_SERVER(pmix_globals.mypeer) || PMIX_PROC_IS_LAUNCHER(pmix_globals.mypeer)) &&
        pmix_globals.connected &&
        !PMIX_PROC_IS_V1(pmix_client_globals.myserver) &&
       (need_register || 0 < pmix_list_get_size(xfer))) {
        pmix_output_verbose(2, pmix_client_globals.event_output,
                            "pmix: _add_hdlr sending to server");
        /* send the directives to the server - we will ack this
         * registration upon return from there */
        if (PMIX_SUCCESS != (rc = _send_to_server(cd2))) {
            pmix_output_verbose(2, pmix_client_globals.event_output,
                                "pmix: add_hdlr - pack send_to_server failed status=%d", rc);
            if (NULL != cd2->info) {
                PMIX_INFO_FREE(cd2->info, cd2->ninfo);
            }
            PMIX_RELEASE(cd2);
            return rc;
        }
        return PMIX_ERR_WOULD_BLOCK;
    }

    /* if we are a server and are registering for events, then we only contact
     * our host if we want environmental events */
    if (PMIX_PROC_IS_SERVER(pmix_globals.mypeer) &&
        !PMIX_PROC_IS_LAUNCHER(pmix_globals.mypeer) && cd->enviro &&
        NULL != pmix_host_server.register_events) {
        pmix_output_verbose(2, pmix_client_globals.event_output,
                            "pmix: _add_hdlr registering with server");
        rc = pmix_host_server.register_events(cd->codes, cd->ncodes,
                                              cd2->info, cd2->ninfo,
                                              reg_cbfunc, cd2);
        if (PMIX_SUCCESS != rc && PMIX_OPERATION_SUCCEEDED != rc) {
            if (NULL != cd2->info) {
                PMIX_INFO_FREE(cd2->info, cd2->ninfo);
            }
            PMIX_RELEASE(cd2);
            return rc;
        }
        return PMIX_SUCCESS;
    } else {
        if (NULL != cd2->info) {
            PMIX_INFO_FREE(cd2->info, cd2->ninfo);
        }
        PMIX_RELEASE(cd2);
    }

    return PMIX_SUCCESS;
}

static void check_cached_events(pmix_rshift_caddy_t *cd)
{
    size_t n;
    pmix_notify_caddy_t *ncd;
    bool found, matched;
    pmix_event_chain_t *chain;
    int j;

    for (j=0; j < pmix_globals.max_events; j++) {
        pmix_hotel_knock(&pmix_globals.notifications, j, (void**)&ncd);
        if (NULL == ncd) {
            continue;
        }
        found = false;
        if (NULL == cd->codes) {
            if (!ncd->nondefault) {
                /* they registered a default event handler - always matches */
                found = true;
            }
        } else {
            for (n=0; n < cd->ncodes; n++) {
                if (cd->codes[n] == ncd->status) {
                    found = true;
                    break;
                }
            }
        }
        if (!found) {
            continue;
        }
        /* if we were given specific targets, check if we are one */
        if (NULL != ncd->targets) {
            matched = false;
            for (n=0; n < ncd->ntargets; n++) {
                if (PMIX_CHECK_PROCID(&pmix_globals.myid, &ncd->targets[n])) {
                    matched = true;
                    break;
                }
            }
            if (!matched) {
                /* do not notify this one */
                continue;
            }
        }
       /* if they specified affected proc(s) they wanted to know about, check */
       if (!pmix_notify_check_affected(cd->affected, cd->naffected,
                                       ncd->affected, ncd->naffected)) {
           continue;
       }
       /* create the chain */
        chain = PMIX_NEW(pmix_event_chain_t);
        chain->status = ncd->status;
        pmix_strncpy(chain->source.nspace, pmix_globals.myid.nspace, PMIX_MAX_NSLEN);
        chain->source.rank = pmix_globals.myid.rank;
        /* we always leave space for event hdlr name and a callback object */
        chain->nallocated = ncd->ninfo + 2;
        PMIX_INFO_CREATE(chain->info, chain->nallocated);
        if (0 < cd->ninfo) {
            chain->ninfo = ncd->ninfo;
            /* need to copy the info */
            for (n=0; n < ncd->ninfo; n++) {
                PMIX_INFO_XFER(&chain->info[n], &ncd->info[n]);
                if (0 == strncmp(ncd->info[n].key, PMIX_EVENT_NON_DEFAULT, PMIX_MAX_KEYLEN)) {
                    chain->nondefault = true;
                } else if (0 == strncmp(ncd->info[n].key, PMIX_EVENT_AFFECTED_PROC, PMIX_MAX_KEYLEN)) {
                    PMIX_PROC_CREATE(chain->affected, 1);
                    if (NULL == chain->affected) {
                        PMIX_RELEASE(chain);
                        return;
                    }
                    chain->naffected = 1;
                    memcpy(chain->affected, ncd->info[n].value.data.proc, sizeof(pmix_proc_t));
                } else if (0 == strncmp(ncd->info[n].key, PMIX_EVENT_AFFECTED_PROCS, PMIX_MAX_KEYLEN)) {
                    chain->naffected = ncd->info[n].value.data.darray->size;
                    PMIX_PROC_CREATE(chain->affected, chain->naffected);
                    if (NULL == chain->affected) {
                        chain->naffected = 0;
                        PMIX_RELEASE(chain);
                        return;
                    }
                    memcpy(chain->affected, ncd->info[n].value.data.darray->array, chain->naffected * sizeof(pmix_proc_t));
                }
            }
        }
        /* check this event out of the cache since we
         * are processing it */
        pmix_hotel_checkout(&pmix_globals.notifications, ncd->room);
        /* release the storage */
        PMIX_RELEASE(ncd);

        /* we don't want this chain to propagate, so indicate it
         * should only be run as a single-shot */
        chain->endchain = true;
        /* now notify any matching registered callbacks we have */
        pmix_invoke_local_event_hdlr(chain);
    }
}

static void reg_event_hdlr(int sd, short args, void *cbdata)
{
    pmix_rshift_caddy_t *cd = (pmix_rshift_caddy_t*)cbdata;
    size_t index = 0, n;
    pmix_status_t rc;
    pmix_event_hdlr_t *evhdlr, *ev;
    uint8_t location = PMIX_EVENT_ORDER_NONE;
    char *name = NULL, *locator = NULL;
    bool firstoverall=false, lastoverall=false;
    bool found;
    pmix_list_t xfer;
    pmix_info_caddy_t *ixfer;
    void *cbobject = NULL;
    pmix_data_range_t range = PMIX_RANGE_UNDEF;
    pmix_proc_t *parray = NULL;
    size_t nprocs = 0;

    /* need to acquire the object from its originating thread */
    PMIX_ACQUIRE_OBJECT(cd);

    pmix_output_verbose(2, pmix_client_globals.event_output,
                        "pmix: register event_hdlr with %d infos", (int)cd->ninfo);

    PMIX_CONSTRUCT(&xfer, pmix_list_t);

    /* if directives were included */
    if (NULL != cd->info) {
        for (n=0; n < cd->ninfo; n++) {
            if (0 == strncmp(cd->info[n].key, PMIX_EVENT_HDLR_FIRST, PMIX_MAX_KEYLEN)) {
                /* flag if they asked to put this one first overall */
                firstoverall = PMIX_INFO_TRUE(&cd->info[n]);
            } else if (0 == strncmp(cd->info[n].key, PMIX_EVENT_HDLR_LAST, PMIX_MAX_KEYLEN)) {
                /* flag if they asked to put this one last overall */
                lastoverall = PMIX_INFO_TRUE(&cd->info[n]);
            } else if (0 == strncmp(cd->info[n].key, PMIX_EVENT_HDLR_PREPEND, PMIX_MAX_KEYLEN)) {
                /* flag if they asked to prepend this handler */
                if (PMIX_INFO_TRUE(&cd->info[n])) {
                    location = PMIX_EVENT_ORDER_PREPEND;
                }
            } else if (0 == strncmp(cd->info[n].key, PMIX_EVENT_HDLR_APPEND, PMIX_MAX_KEYLEN)) {
                /* flag if they asked to append this handler */
                if (PMIX_INFO_TRUE(&cd->info[n])) {
                    location = PMIX_EVENT_ORDER_APPEND;
                }
            } else if (0 == strncmp(cd->info[n].key, PMIX_EVENT_HDLR_NAME, PMIX_MAX_KEYLEN)) {
                name = cd->info[n].value.data.string;
            } else if (0 == strncmp(cd->info[n].key, PMIX_EVENT_RETURN_OBJECT, PMIX_MAX_KEYLEN)) {
                cbobject = cd->info[n].value.data.ptr;
            } else if (0 == strncmp(cd->info[n].key, PMIX_EVENT_HDLR_FIRST_IN_CATEGORY, PMIX_MAX_KEYLEN)) {
                if (PMIX_INFO_TRUE(&cd->info[n])) {
                    location = PMIX_EVENT_ORDER_FIRST;
                }
            } else if (0 == strncmp(cd->info[n].key, PMIX_EVENT_HDLR_LAST_IN_CATEGORY, PMIX_MAX_KEYLEN)) {
                if (PMIX_INFO_TRUE(&cd->info[n])) {
                    location = PMIX_EVENT_ORDER_LAST;
                }
            } else if (0 == strncmp(cd->info[n].key, PMIX_EVENT_HDLR_BEFORE, PMIX_MAX_KEYLEN)) {
                location = PMIX_EVENT_ORDER_BEFORE;
                locator = cd->info[n].value.data.string;
            } else if (0 == strncmp(cd->info[n].key, PMIX_EVENT_HDLR_AFTER, PMIX_MAX_KEYLEN)) {
                location = PMIX_EVENT_ORDER_AFTER;
                locator = cd->info[n].value.data.string;
            } else if (0 == strncmp(cd->info[n].key, PMIX_RANGE, PMIX_MAX_KEYLEN)) {
                range = cd->info[n].value.data.range;
            } else if (0 == strncmp(cd->info[n].key, PMIX_EVENT_CUSTOM_RANGE, PMIX_MAX_KEYLEN)) {
                parray = (pmix_proc_t*)cd->info[n].value.data.darray->array;
                nprocs = cd->info[n].value.data.darray->size;
            } else if (0 == strncmp(cd->info[n].key, PMIX_EVENT_AFFECTED_PROC, PMIX_MAX_KEYLEN)) {
                cd->affected = cd->info[n].value.data.proc;
                cd->naffected = 1;
                ixfer = PMIX_NEW(pmix_info_caddy_t);
                ixfer->info = &cd->info[n];
                ixfer->ninfo = 1;
                pmix_list_append(&xfer, &ixfer->super);
            } else if (0 == strncmp(cd->info[n].key, PMIX_EVENT_AFFECTED_PROCS, PMIX_MAX_KEYLEN)) {
                cd->affected = (pmix_proc_t*)cd->info[n].value.data.darray->array;
                cd->naffected = cd->info[n].value.data.darray->size;
                ixfer = PMIX_NEW(pmix_info_caddy_t);
                ixfer->info = &cd->info[n];
                ixfer->ninfo = 1;
                pmix_list_append(&xfer, &ixfer->super);
            } else {
                ixfer = PMIX_NEW(pmix_info_caddy_t);
                ixfer->info = &cd->info[n];
                ixfer->ninfo = 1;
                pmix_list_append(&xfer, &ixfer->super);
            }
        }
    }

    /* check the codes for system events */
    for (n=0; n < cd->ncodes; n++) {
        if (PMIX_SYSTEM_EVENT(cd->codes[n])) {
            cd->enviro = true;
            break;
        }
    }

    /* if they indicated this is to be the "first" or "last" event, then
     * first check to ensure they didn't already direct some
     * other event into the same cherished position */
    if (firstoverall || lastoverall) {
        if ((firstoverall && NULL != pmix_globals.events.first) ||
            (lastoverall && NULL != pmix_globals.events.last)) {
            /* oops - someone already took that position */
            index = UINT_MAX;
            rc = PMIX_ERR_EVENT_REGISTRATION;
            goto ack;
        }
        evhdlr = PMIX_NEW(pmix_event_hdlr_t);
        if (NULL == evhdlr) {
            index = UINT_MAX;
            rc = PMIX_ERR_EVENT_REGISTRATION;
            goto ack;
        }
        if (NULL != name) {
            evhdlr->name = strdup(name);
        }
        index = pmix_globals.events.nhdlrs;
        evhdlr->index = index;
        ++pmix_globals.events.nhdlrs;
        evhdlr->rng.range = range;
        if (NULL != parray && 0 < nprocs) {
            evhdlr->rng.nprocs = nprocs;
            PMIX_PROC_CREATE(evhdlr->rng.procs, nprocs);
            if (NULL == evhdlr->rng.procs) {
                index = UINT_MAX;
                rc = PMIX_ERR_EVENT_REGISTRATION;
                PMIX_RELEASE(evhdlr);
                goto ack;
            }
            memcpy(evhdlr->rng.procs, parray, nprocs * sizeof(pmix_proc_t));
        }
        if (NULL != cd->affected && 0 < cd->naffected) {
            evhdlr->naffected = cd->naffected;
            PMIX_PROC_CREATE(evhdlr->affected, cd->naffected);
            if (NULL == evhdlr->affected) {
                index = UINT_MAX;
                rc = PMIX_ERR_EVENT_REGISTRATION;
                PMIX_RELEASE(evhdlr);
                goto ack;
            }
            memcpy(evhdlr->affected, cd->affected, cd->naffected * sizeof(pmix_proc_t));
        }
        evhdlr->evhdlr = cd->evhdlr;
        evhdlr->cbobject = cbobject;
        if (NULL != cd->codes) {
            evhdlr->codes = (pmix_status_t*)malloc(cd->ncodes * sizeof(pmix_status_t));
            if (NULL == evhdlr->codes) {
                PMIX_RELEASE(evhdlr);
                index = UINT_MAX;
                rc = PMIX_ERR_EVENT_REGISTRATION;
                goto ack;
            }
            memcpy(evhdlr->codes, cd->codes, cd->ncodes * sizeof(pmix_status_t));
            evhdlr->ncodes = cd->ncodes;
        }
        if (firstoverall) {
            pmix_globals.events.first = evhdlr;
        } else {
            pmix_globals.events.last = evhdlr;
        }
        cd->index = index;
        cd->list = NULL;
        cd->hdlr = evhdlr;
        cd->firstoverall = firstoverall;
        rc = _add_hdlr(cd, &xfer);
        PMIX_LIST_DESTRUCT(&xfer);
        if (PMIX_SUCCESS != rc &&
            PMIX_ERR_WOULD_BLOCK != rc) {
                /* unable to register */
            --pmix_globals.events.nhdlrs;
            rc = PMIX_ERR_EVENT_REGISTRATION;
            index = UINT_MAX;
            if (firstoverall) {
                pmix_globals.events.first = NULL;
            } else {
                pmix_globals.events.last = NULL;
            }
            PMIX_RELEASE(evhdlr);
            goto ack;
        }
        if (PMIX_ERR_WOULD_BLOCK == rc) {
            /* the callback will provide our response */
            PMIX_RELEASE(cd);
            return;
        }
        goto ack;
    }

    /* get here if this isn't an overall first or last event - start
     * by creating an event */
    evhdlr = PMIX_NEW(pmix_event_hdlr_t);
    if (NULL == evhdlr) {
        index = UINT_MAX;
        rc = PMIX_ERR_EVENT_REGISTRATION;
        goto ack;
    }
    if (NULL != name) {
        evhdlr->name = strdup(name);
    }
    index = pmix_globals.events.nhdlrs;
    evhdlr->index = index;
    ++pmix_globals.events.nhdlrs;
    evhdlr->precedence = location;
    evhdlr->locator = locator;
    evhdlr->rng.range = range;
    if (NULL != parray && 0 < nprocs) {
        evhdlr->rng.nprocs = nprocs;
        PMIX_PROC_CREATE(evhdlr->rng.procs, nprocs);
        if (NULL == evhdlr->rng.procs) {
            index = UINT_MAX;
            rc = PMIX_ERR_EVENT_REGISTRATION;
            PMIX_RELEASE(evhdlr);
            goto ack;
        }
        memcpy(evhdlr->rng.procs, parray, nprocs * sizeof(pmix_proc_t));
    }
    if (NULL != cd->affected && 0 < cd->naffected) {
        evhdlr->naffected = cd->naffected;
        PMIX_PROC_CREATE(evhdlr->affected, cd->naffected);
        if (NULL == evhdlr->affected) {
            index = UINT_MAX;
            rc = PMIX_ERR_EVENT_REGISTRATION;
            PMIX_RELEASE(evhdlr);
            goto ack;
        }
        memcpy(evhdlr->affected, cd->affected, cd->naffected * sizeof(pmix_proc_t));
    }
    evhdlr->evhdlr = cd->evhdlr;
    evhdlr->cbobject = cbobject;
    if (NULL == cd->codes) {
        /* this is a default handler */
        cd->list = &pmix_globals.events.default_events;
    } else {
        evhdlr->codes = (pmix_status_t*)malloc(cd->ncodes * sizeof(pmix_status_t));
        if (NULL == evhdlr->codes) {
            PMIX_RELEASE(evhdlr);
            index = UINT_MAX;
            rc = PMIX_ERR_EVENT_REGISTRATION;
            goto ack;
        }
        memcpy(evhdlr->codes, cd->codes, cd->ncodes * sizeof(pmix_status_t));
        evhdlr->ncodes = cd->ncodes;
        if (1 == cd->ncodes) {
            cd->list = &pmix_globals.events.single_events;
        } else {
            cd->list = &pmix_globals.events.multi_events;
        }
    }
    /* setup to add the handler */
    cd->index = index;
    cd->hdlr = evhdlr;
    cd->firstoverall = false;
    /* tell the server about it, if necessary - any actions
     * will be deferred until after this event completes */
    if (PMIX_RANGE_PROC_LOCAL == range) {
        rc = PMIX_SUCCESS;
    } else {
        rc = _add_hdlr(cd, &xfer);
    }
    PMIX_LIST_DESTRUCT(&xfer);
    if (PMIX_SUCCESS != rc &&
        PMIX_ERR_WOULD_BLOCK != rc) {
        /* unable to register */
        --pmix_globals.events.nhdlrs;
        rc = PMIX_ERR_EVENT_REGISTRATION;
        index = UINT_MAX;
        PMIX_RELEASE(evhdlr);
        goto ack;
    }
    /* now add this event to the appropriate list - if the registration
     * subsequently fails, it will be removed */

    /* if the list is empty, or no location was specified, just put this on it */
    if (0 == pmix_list_get_size(cd->list) ||
        PMIX_EVENT_ORDER_NONE == location) {
        pmix_list_prepend(cd->list, &evhdlr->super);
    } else if (PMIX_EVENT_ORDER_FIRST == location) {
        /* see if the first handler on the list was also declared as "first" */
        ev = (pmix_event_hdlr_t*)pmix_list_get_first(cd->list);
        if (PMIX_EVENT_ORDER_FIRST == ev->precedence) {
            /* this is an error */
            --pmix_globals.events.nhdlrs;
            rc = PMIX_ERR_EVENT_REGISTRATION;
            index = UINT_MAX;
            PMIX_RELEASE(evhdlr);
            goto ack;
        }
        /* prepend it to the list */
        pmix_list_prepend(cd->list, &evhdlr->super);
    } else if (PMIX_EVENT_ORDER_LAST == location) {
        /* see if the last handler on the list was also declared as "last" */
        ev = (pmix_event_hdlr_t*)pmix_list_get_last(cd->list);
        if (PMIX_EVENT_ORDER_LAST == ev->precedence) {
            /* this is an error */
            --pmix_globals.events.nhdlrs;
            rc = PMIX_ERR_EVENT_REGISTRATION;
            index = UINT_MAX;
            PMIX_RELEASE(evhdlr);
            goto ack;
        }
        /* append it to the list */
        pmix_list_append(cd->list, &evhdlr->super);
    } else if (PMIX_EVENT_ORDER_PREPEND == location) {
        /* we know the list isn't empty - check the first element to see if
         * it is designated to be "first". If so, then we need to put this
         * right after it */
        ev = (pmix_event_hdlr_t*)pmix_list_get_first(cd->list);
        if (PMIX_EVENT_ORDER_FIRST == ev->precedence) {
            ev = (pmix_event_hdlr_t*)pmix_list_get_next(&ev->super);
            if (NULL != ev) {
                pmix_list_insert_pos(cd->list, &ev->super, &evhdlr->super);
            } else {
                /* we are at the end of the list */
                pmix_list_append(cd->list, &evhdlr->super);
            }
        } else {
            pmix_list_prepend(cd->list, &evhdlr->super);
        }
    } else if (PMIX_EVENT_ORDER_APPEND == location) {
        /* we know the list isn't empty - check the last element to see if
         * it is designated to be "last". If so, then we need to put this
         * right before it */
        ev = (pmix_event_hdlr_t*)pmix_list_get_last(cd->list);
        if (PMIX_EVENT_ORDER_LAST == ev->precedence) {
            pmix_list_insert_pos(cd->list, &ev->super, &evhdlr->super);
        } else {
            pmix_list_append(cd->list, &evhdlr->super);
        }
    } else {
        /* find the named event */
        found = false;
        PMIX_LIST_FOREACH(ev, cd->list, pmix_event_hdlr_t) {
            if (NULL == ev->name) {
                continue;
            }
            if (0 == strcmp(ev->name, name)) {
               if (PMIX_EVENT_ORDER_BEFORE == location) {
                    /* put it before this handler */
                    pmix_list_insert_pos(cd->list, &ev->super, &evhdlr->super);
               } else {
                   /* put it after this handler */
                    ev = (pmix_event_hdlr_t*)pmix_list_get_next(&ev->super);
                    if (NULL != ev) {
                        pmix_list_insert_pos(cd->list, &ev->super, &evhdlr->super);
                    } else {
                        /* we are at the end of the list */
                        pmix_list_append(cd->list, &evhdlr->super);
                    }
               }
               found = true;
               break;
            }
        }
        /* if the handler wasn't found, then we return an error. At some
         * future time, we may change this behavior and cache this handler
         * until the reference one has been registered. However, this could
         * turn out to be a laborious search procedure as the reference
         * event handler may in turn be dependent on another handler, etc. */
        if (!found) {
            /* this is an error */
            --pmix_globals.events.nhdlrs;
            rc = PMIX_ERR_EVENT_REGISTRATION;
            index = UINT_MAX;
            PMIX_RELEASE(evhdlr);
            goto ack;
        }
    }
    if (PMIX_ERR_WOULD_BLOCK == rc) {
        /* the callback will provide our response */
        PMIX_RELEASE(cd);
        return;
    }

  ack:
    /* acknowledge the registration so the caller can release
     * their data AND record the event handler index */
    if (NULL != cd->evregcbfn) {
        cd->evregcbfn(rc, index, cd->cbdata);
    }

    /* check if any matching notifications have been cached */
    check_cached_events(cd);
    if (NULL != cd->codes) {
        free(cd->codes);
        cd->codes = NULL;
    }

    /* all done */
    PMIX_RELEASE(cd);
}

PMIX_EXPORT void PMIx_Register_event_handler(pmix_status_t codes[], size_t ncodes,
                                             pmix_info_t info[], size_t ninfo,
                                             pmix_notification_fn_t event_hdlr,
                                             pmix_hdlr_reg_cbfunc_t cbfunc,
                                             void *cbdata)
{
    pmix_rshift_caddy_t *cd;
    size_t n;

    PMIX_ACQUIRE_THREAD(&pmix_global_lock);

    if (pmix_globals.init_cntr <= 0) {
        PMIX_RELEASE_THREAD(&pmix_global_lock);
        if (NULL != cbfunc) {
            cbfunc(PMIX_ERR_INIT, 0, cbdata);
        }
        return;
    }
    PMIX_RELEASE_THREAD(&pmix_global_lock);

    /* need to thread shift this request so we can access
     * our global data to register this *local* event handler */
    cd = PMIX_NEW(pmix_rshift_caddy_t);
    /* we have to save the codes as we will check them against existing
     * registrations AFTER we have executed the callback which allows
     * the caller to release their storage */
    if (0 < ncodes) {
        cd->codes = (pmix_status_t*)malloc(ncodes * sizeof(pmix_status_t));
        if (NULL == cd->codes) {
            /* immediately return error */
            PMIX_RELEASE(cd);
            if (NULL != cbfunc) {
                cbfunc(PMIX_ERR_NOMEM, SIZE_MAX, cbdata);
            }
            return;
        }
        for (n=0; n < ncodes; n++) {
            cd->codes[n] = codes[n];
        }
    }
    cd->ncodes = ncodes;
    cd->info = info;
    cd->ninfo = ninfo;
    cd->evhdlr = event_hdlr;
    cd->evregcbfn = cbfunc;
    cd->cbdata = cbdata;

    pmix_output_verbose(2, pmix_client_globals.event_output,
                        "pmix_register_event_hdlr shifting to progress thread");

    PMIX_THREADSHIFT(cd, reg_event_hdlr);
}

static void dereg_event_hdlr(int sd, short args, void *cbdata)
{
    pmix_shift_caddy_t *cd = (pmix_shift_caddy_t*)cbdata;
    pmix_buffer_t *msg = NULL;
    pmix_event_hdlr_t *evhdlr, *ev;
    pmix_cmd_t cmd = PMIX_DEREGEVENTS_CMD;
    pmix_status_t rc = PMIX_SUCCESS;
    pmix_status_t wildcard = PMIX_MAX_ERR_CONSTANT;
    size_t n;
    pmix_active_code_t *active;

    /* need to acquire the object from its originating thread */
    PMIX_ACQUIRE_OBJECT(cd);

    /* if I am not the server, and I am connected, then I need
     * to notify the server to remove my registration */
    if ((!PMIX_PROC_IS_SERVER(pmix_globals.mypeer) || PMIX_PROC_IS_LAUNCHER(pmix_globals.mypeer)) &&
        pmix_globals.connected) {
        msg = PMIX_NEW(pmix_buffer_t);
        PMIX_BFROPS_PACK(rc, pmix_client_globals.myserver,
                         msg, &cmd, 1, PMIX_COMMAND);
        if (PMIX_SUCCESS != rc) {
            PMIX_RELEASE(msg);
            goto cleanup;
        }
    }

    /* check the first and last locations */
    if ((NULL != pmix_globals.events.first && pmix_globals.events.first->index == cd->ref) ||
        (NULL != pmix_globals.events.last && pmix_globals.events.last->index == cd->ref)) {
        /* found it */
        if (NULL != pmix_globals.events.first && pmix_globals.events.first->index == cd->ref) {
            ev = pmix_globals.events.first;
        } else {
            ev = pmix_globals.events.last;
        }
        if (NULL != msg) {
            /* if this is a default handler, see if any other default
             * handlers remain */
            if (NULL == ev->codes) {
                if (0 == pmix_list_get_size(&pmix_globals.events.default_events)) {
                    /* tell the server to dereg our default handler */
                    PMIX_BFROPS_PACK(rc, pmix_client_globals.myserver,
                                     msg, &wildcard, 1, PMIX_STATUS);
                    if (PMIX_SUCCESS != rc) {
                        PMIX_RELEASE(msg);
                        goto cleanup;
                    }
                }
            } else {
                for (n=0; n < ev->ncodes; n++) {
                    /* see if this is the last registration we have for this code */
                    PMIX_LIST_FOREACH(active, &pmix_globals.events.actives, pmix_active_code_t) {
                        if (active->code == ev->codes[n]) {
                            --active->nregs;
                            if (0 == active->nregs) {
                                pmix_list_remove_item(&pmix_globals.events.actives, &active->super);
                                /* tell the server to dereg this code */
                                PMIX_BFROPS_PACK(rc, pmix_client_globals.myserver,
                                                 msg, &active->code, 1, PMIX_STATUS);
                                if (PMIX_SUCCESS != rc) {
                                    PMIX_RELEASE(active);
                                    PMIX_RELEASE(msg);
                                    goto cleanup;
                                }
                                PMIX_RELEASE(active);
                            }
                            break;
                        }
                    }
                }
            }
        }
        if (ev == pmix_globals.events.first) {
            pmix_globals.events.first = NULL;
        } else {
            pmix_globals.events.last  = NULL;
        }
        PMIX_RELEASE(ev);
        goto cleanup;
    }

    /* the registration can be in any of three places, so check each of them */
    PMIX_LIST_FOREACH(evhdlr, &pmix_globals.events.default_events, pmix_event_hdlr_t) {
        if (evhdlr->index == cd->ref) {
            /* found it */
            pmix_list_remove_item(&pmix_globals.events.default_events, &evhdlr->super);
            if (NULL != msg) {
                /* if there are no more default handlers registered, tell
                 * the server to dereg the default handler */
                if (0 == pmix_list_get_size(&pmix_globals.events.default_events)) {
                    PMIX_BFROPS_PACK(rc, pmix_client_globals.myserver,
                                     msg, &wildcard, 1, PMIX_STATUS);
                    if (PMIX_SUCCESS != rc) {
                        PMIX_RELEASE(msg);
                        goto cleanup;
                    }
                }
            }
            PMIX_RELEASE(evhdlr);
            goto report;
        }
    }
    PMIX_LIST_FOREACH(evhdlr, &pmix_globals.events.single_events, pmix_event_hdlr_t) {
        if (evhdlr->index == cd->ref) {
            /* found it */
            pmix_list_remove_item(&pmix_globals.events.single_events, &evhdlr->super);
            if (NULL != msg) {
                /* see if this is the last registration we have for this code */
                PMIX_LIST_FOREACH(active, &pmix_globals.events.actives, pmix_active_code_t) {
                    if (active->code == evhdlr->codes[0]) {
                        --active->nregs;
                        if (0 == active->nregs) {
                            pmix_list_remove_item(&pmix_globals.events.actives, &active->super);
                            if (NULL != msg) {
                                /* tell the server to dereg this code */
                                PMIX_BFROPS_PACK(rc, pmix_client_globals.myserver,
                                                 msg, &active->code, 1, PMIX_STATUS);
                                if (PMIX_SUCCESS != rc) {
                                    PMIX_RELEASE(active);
                                    PMIX_RELEASE(msg);
                                    goto cleanup;
                                }
                            }
                            PMIX_RELEASE(active);
                        }
                        break;
                    }
                }
            }
            PMIX_RELEASE(evhdlr);
            goto report;
        }
    }
    PMIX_LIST_FOREACH(evhdlr, &pmix_globals.events.multi_events, pmix_event_hdlr_t) {
        if (evhdlr->index == cd->ref) {
            /* found it */
            pmix_list_remove_item(&pmix_globals.events.multi_events, &evhdlr->super);
            for (n=0; n < evhdlr->ncodes; n++) {
                /* see if this is the last registration we have for this code */
                PMIX_LIST_FOREACH(active, &pmix_globals.events.actives, pmix_active_code_t) {
                    if (active->code == evhdlr->codes[n]) {
                        --active->nregs;
                        if (0 == active->nregs) {
                            pmix_list_remove_item(&pmix_globals.events.actives, &active->super);
                            if (NULL != msg) {
                                /* tell the server to dereg this code */
                                PMIX_BFROPS_PACK(rc, pmix_client_globals.myserver,
                                                 msg, &active->code, 1, PMIX_STATUS);
                                if (PMIX_SUCCESS != rc) {
                                    PMIX_RELEASE(active);
                                    PMIX_RELEASE(msg);
                                    goto cleanup;
                                }
                            }
                            PMIX_RELEASE(active);
                        }
                        break;
                    }
                }
            }
            PMIX_RELEASE(evhdlr);
            goto report;
        }
    }
    /* if we get here, then the registration could not be found */
    if (NULL != msg) {
        PMIX_RELEASE(msg);
    }
    goto cleanup;

  report:
    if (NULL != msg) {
        /* send to the server */
        PMIX_PTL_SEND_RECV(rc, pmix_client_globals.myserver, msg, NULL, NULL);
        if (PMIX_SUCCESS != rc) {
            PMIX_ERROR_LOG(rc);
        }
    }

  cleanup:
    /* must release the caller */
    if (NULL != cd->cbfunc.opcbfn) {
        cd->cbfunc.opcbfn(rc, cd->cbdata);
    }
    PMIX_RELEASE(cd);
}

PMIX_EXPORT void PMIx_Deregister_event_handler(size_t event_hdlr_ref,
                                               pmix_op_cbfunc_t cbfunc,
                                               void *cbdata)
{
    pmix_shift_caddy_t *cd;

    PMIX_ACQUIRE_THREAD(&pmix_global_lock);
    if (pmix_globals.init_cntr <= 0) {
        PMIX_RELEASE_THREAD(&pmix_global_lock);
        if (NULL != cbfunc) {
            cbfunc(PMIX_ERR_INIT, cbdata);
        }
        return;
    }
    PMIX_RELEASE_THREAD(&pmix_global_lock);

    /* need to thread shift this request */
    cd = PMIX_NEW(pmix_shift_caddy_t);
    cd->cbfunc.opcbfn = cbfunc;
    cd->cbdata = cbdata;
    cd->ref = event_hdlr_ref;

    pmix_output_verbose(2, pmix_client_globals.event_output,
                        "pmix_deregister_event_hdlr shifting to progress thread");
    PMIX_THREADSHIFT(cd, dereg_event_hdlr);
}
