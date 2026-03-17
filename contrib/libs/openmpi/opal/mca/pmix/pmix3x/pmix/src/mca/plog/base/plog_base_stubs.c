/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2018      Intel, Inc. All rights reserved.
 *
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include <src/include/pmix_config.h>

#include <pmix_common.h>
#include "src/include/pmix_globals.h"

#include "src/class/pmix_list.h"
#include "src/util/error.h"
#include "src/server/pmix_server_ops.h"

#include "src/mca/plog/base/base.h"

typedef struct {
    pmix_object_t super;
    pmix_lock_t lock;
    size_t nreqs;
    pmix_status_t status;
    pmix_op_cbfunc_t cbfunc;
    void *cbdata;
} pmix_mycount_t;
static void mycon(pmix_mycount_t *p)
{
    PMIX_CONSTRUCT_LOCK(&p->lock);
    p->lock.active = false;
    p->nreqs = 0;
    p->status = PMIX_ERR_NOT_AVAILABLE;
    p->cbfunc = NULL;
    p->cbdata = NULL;
}
static void mydes(pmix_mycount_t *p)
{
    PMIX_DESTRUCT_LOCK(&p->lock);
}
static PMIX_CLASS_INSTANCE(pmix_mycount_t,
                           pmix_object_t,
                           mycon, mydes);

static void localcbfunc(pmix_status_t status, void *cbdata)
{
    pmix_mycount_t *mycount = (pmix_mycount_t*)cbdata;

    PMIX_ACQUIRE_THREAD(&mycount->lock);
    mycount->nreqs--;
    if (PMIX_SUCCESS != status && PMIX_SUCCESS == mycount->status) {
        mycount->status = status;
    }
    if (0 == mycount->nreqs) {
        /* execute their callback */
        if (NULL != mycount->cbfunc) {
            mycount->cbfunc(mycount->status, mycount->cbdata);
        }
        PMIX_RELEASE_THREAD(&mycount->lock);
        PMIX_RELEASE(mycount);
        return;
    }
    PMIX_RELEASE_THREAD(&mycount->lock);
}

pmix_status_t pmix_plog_base_log(const pmix_proc_t *source,
                                 const pmix_info_t data[], size_t ndata,
                                 const pmix_info_t directives[], size_t ndirs,
                                 pmix_op_cbfunc_t cbfunc, void *cbdata)
{
    pmix_plog_base_active_module_t *active;
    pmix_status_t rc = PMIX_ERR_NOT_AVAILABLE;
    size_t n, k;
    int m;
    bool logonce = false;
    pmix_mycount_t *mycount;
    pmix_list_t channels;
    bool all_complete = true;

    if (!pmix_plog_globals.initialized) {
        return PMIX_ERR_INIT;
    }

    /* we have to serialize our way thru here as we are going
     * to construct a list of the available modules, and those
     * can only be on one list at a time */
    PMIX_ACQUIRE_THREAD(&pmix_plog_globals.lock);

    pmix_output_verbose(2, pmix_plog_base_framework.framework_output,
                        "plog:log called");

    /* initialize the tracker */
    mycount = PMIX_NEW(pmix_mycount_t);
    if (NULL == mycount) {
        PMIX_RELEASE_THREAD(&pmix_plog_globals.lock);
        return PMIX_ERR_NOMEM;
    }
    mycount->cbfunc = cbfunc;
    mycount->cbdata = cbdata;
    /* initialize the list of channels */
    PMIX_CONSTRUCT(&channels, pmix_list_t);

    if (NULL != directives) {
        /* scan the directives for the PMIX_LOG_ONCE attribute
         * which indicates we should stop with the first log
         * channel that can successfully handle this request,
         * and any channel directives */
        for (n=0; n < ndirs; n++) {
            if (0 == strncmp(directives[n].key, PMIX_LOG_ONCE, PMIX_MAX_KEYLEN)) {
                logonce = true;
                break;
            }
        }
    }

    /* scan the incoming logging requests and assemble the modules in
     * the corresponding order - this will ensure that the one they
     * requested first gets first shot at "log once" */
    for (n=0; n < ndata; n++) {
        if (PMIX_INFO_OP_IS_COMPLETE(&data[n])) {
            continue;
        }
        all_complete = false;
        for (m=0; m < pmix_plog_globals.actives.size; m++) {
            if (NULL == (active = (pmix_plog_base_active_module_t*)pmix_pointer_array_get_item(&pmix_plog_globals.actives, m))) {
                continue;
            }
            /* if this channel is included in the ones serviced by this
             * module, then include the module */
            if (NULL == active->module->channels) {
                if (!active->added) {
                    /* add this channel to the list */
                    pmix_list_append(&channels, &active->super);
                    /* mark it as added */
                    active->added = true;
                }
            } else {
                for (k=0; NULL != active->module->channels[k]; k++) {
                    if (NULL != strstr(data[n].key, active->module->channels[k])) {
                        if (!active->added) {
                            /* add this channel to the list */
                            pmix_list_append(&channels, &active->super);
                            /* mark it as added */
                            active->added = true;
                            break;
                        }
                    }
                }
            }
        }
    }
    /* reset the added marker for the next time we are called */
    PMIX_LIST_FOREACH(active, &channels, pmix_plog_base_active_module_t) {
        active->added = false;
    }
    if (all_complete) {
        /* nothing we need do */
        while (NULL != pmix_list_remove_first(&channels));
        PMIX_DESTRUCT(&channels);
        PMIX_RELEASE(mycount);
        PMIX_RELEASE_THREAD(&pmix_plog_globals.lock);
        return PMIX_SUCCESS;
    }
    PMIX_ACQUIRE_THREAD(&mycount->lock);
    PMIX_LIST_FOREACH(active, &channels, pmix_plog_base_active_module_t) {
        if (NULL != active->module->log) {
            mycount->nreqs++;
            rc = active->module->log(source, data, ndata, directives, ndirs,
                                     localcbfunc, (void*)mycount);
            /* The plugins are required to return:
             *
             * PMIX_SUCCESS - indicating that the logging operation for
             *                that component was very quick, and therefore
             *                done atomically. No callback will be issued
             *
             * PMIX_OPERATION_IN_PROGRESS - indicates that the plugin
             *                expects to execute the desired logging request,
             *                but must do so asynchronously. The provided
             *                callback _must_ be executed upon completion
             *                of the operation, indicating success or failure.
             *
             * PMIX_ERR_NOT_AVAILABLE - indicates that the plugin is unable
             *                to process the request.
             *                No callback will be issued.
             *
             * PMIX_ERR_TAKE_NEXT_OPTION - indicates that the plugin didn't
             *                find any directives that it supports.
             *                No callback will be issued.
             *
             * PMIX_ERR_NOT_SUPPORTED - indicates that the request cannot be
             *                supported. The list will cease processing at
             *                that point and return this error
             *
             * All other returned errors indicate that the plugin should
             * have attempted to perform the requested operation, but determined
             * that it could not do so. Note that this differs from the case
             * where a plugin asynchronously attempts an operation that subsequently
             * fails - that error would be returned in the callback function.
             * In this case, the error indicates that the request contained
             * an incorrect/invalid element that prevents the plugin from
             * executing it. The first such retured error will be cached and
             * returned to the caller upon completion of all pending operations.
             * No callback from failed plugins shall be executed.
             */
            if (PMIX_SUCCESS == rc) {
                mycount->nreqs--;
                mycount->status = rc;
                if (logonce) {
                    break;
                }
            } else if (PMIX_ERR_NOT_AVAILABLE == rc ||
                       PMIX_ERR_TAKE_NEXT_OPTION == rc) {
                mycount->nreqs--;
            } else if (PMIX_OPERATION_IN_PROGRESS == rc) {
                /* even though the operation hasn't completed,
                 * we still treat this as a completed request */
                mycount->status = PMIX_SUCCESS;
                if (logonce) {
                    break;
                }
            } else {
                /* we may have outstanding requests we need
                 * to wait for, so mark that there was an error
                 * for reporting purposes */
                mycount->nreqs--;
                mycount->status = rc;
            }
        }
    }

    /* cannot release the modules - just remove everything from the list */
    while (NULL != pmix_list_remove_first(&channels));
    PMIX_DESTRUCT(&channels);

    rc = mycount->status;  // save the status as it could change when the lock is released
    if (0 == mycount->nreqs) {
        /* execute their callback */
        if (NULL != mycount->cbfunc) {
            mycount->cbfunc(mycount->status, mycount->cbdata);
        }
        PMIX_RELEASE_THREAD(&mycount->lock);
        PMIX_RELEASE(mycount);
        PMIX_RELEASE_THREAD(&pmix_plog_globals.lock);
        return PMIX_SUCCESS;
    }
    PMIX_RELEASE_THREAD(&mycount->lock);
    PMIX_RELEASE_THREAD(&pmix_plog_globals.lock);

    return rc;
}
