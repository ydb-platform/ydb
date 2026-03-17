/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2013-2016 Los Alamos National Security, LLC.  All rights
 *                         reseved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2004-2016 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2016      IBM Corporation.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "comm_request.h"

#include "opal/class/opal_free_list.h"
#include "opal/include/opal/sys/atomic.h"

static opal_free_list_t ompi_comm_requests;
static opal_list_t ompi_comm_requests_active;
static opal_mutex_t ompi_comm_request_mutex;
bool ompi_comm_request_progress_active = false;
bool ompi_comm_request_initialized = false;

typedef struct ompi_comm_request_item_t {
    opal_list_item_t super;
    ompi_comm_request_callback_fn_t callback;
    ompi_request_t *subreqs[OMPI_COMM_REQUEST_MAX_SUBREQ];
    int subreq_count;
} ompi_comm_request_item_t;
OBJ_CLASS_DECLARATION(ompi_comm_request_item_t);

static int ompi_comm_request_progress (void);

void ompi_comm_request_init (void)
{
    OBJ_CONSTRUCT(&ompi_comm_requests, opal_free_list_t);
    (void) opal_free_list_init (&ompi_comm_requests, sizeof (ompi_comm_request_t), 8,
                                OBJ_CLASS(ompi_comm_request_t), 0, 0, 0, -1, 8,
                                NULL, 0, NULL, NULL, NULL);

    OBJ_CONSTRUCT(&ompi_comm_requests_active, opal_list_t);
    ompi_comm_request_progress_active = false;
    OBJ_CONSTRUCT(&ompi_comm_request_mutex, opal_mutex_t);
    ompi_comm_request_initialized = true;
}

void ompi_comm_request_fini (void)
{
    if (!ompi_comm_request_initialized) {
        return;
    }

    ompi_comm_request_initialized = false;

    opal_mutex_lock (&ompi_comm_request_mutex);
    if (ompi_comm_request_progress_active) {
        opal_progress_unregister (ompi_comm_request_progress);
    }
    opal_mutex_unlock (&ompi_comm_request_mutex);
    OBJ_DESTRUCT(&ompi_comm_request_mutex);
    OBJ_DESTRUCT(&ompi_comm_requests_active);
    OBJ_DESTRUCT(&ompi_comm_requests);
}


int ompi_comm_request_schedule_append (ompi_comm_request_t *request, ompi_comm_request_callback_fn_t callback,
                            ompi_request_t *subreqs[], int subreq_count)
{
    ompi_comm_request_item_t *request_item;
    int i;

    if (subreq_count > OMPI_COMM_REQUEST_MAX_SUBREQ) {
        return OMPI_ERR_BAD_PARAM;
    }

    request_item = OBJ_NEW(ompi_comm_request_item_t);
    if (NULL == request_item) {
        return OMPI_ERR_OUT_OF_RESOURCE;
    }

    request_item->callback = callback;

    for (i = 0 ; i < subreq_count ; ++i) {
        request_item->subreqs[i] = subreqs[i];
    }

    request_item->subreq_count = subreq_count;

    opal_list_append (&request->schedule, &request_item->super);

    return OMPI_SUCCESS;
}

static int ompi_comm_request_progress (void)
{
    ompi_comm_request_t *request, *next;
    static int32_t progressing = 0;

    /* don't allow re-entry */
    if (opal_atomic_swap_32 (&progressing, 1)) {
        return 0;
    }

    opal_mutex_lock (&ompi_comm_request_mutex);

    OPAL_LIST_FOREACH_SAFE(request, next, &ompi_comm_requests_active, ompi_comm_request_t) {
        int rc = OMPI_SUCCESS;

        if (opal_list_get_size (&request->schedule)) {
            ompi_comm_request_item_t *request_item = (ompi_comm_request_item_t *) opal_list_remove_first (&request->schedule);
            int item_complete = true;

            /* don't call ompi_request_test_all as it causes a recursive call into opal_progress */
            while (request_item->subreq_count) {
                ompi_request_t *subreq = request_item->subreqs[request_item->subreq_count-1];
                if( REQUEST_COMPLETE(subreq) ) {
                    ompi_request_free (&subreq);
                    request_item->subreq_count--;
                } else {
                    item_complete = false;
                    break;
                }
            }

            if (item_complete) {
                if (request_item->callback) {
                    opal_mutex_unlock (&ompi_comm_request_mutex);
                    rc = request_item->callback (request);
                    opal_mutex_lock (&ompi_comm_request_mutex);
                }
                OBJ_RELEASE(request_item);
            } else {
                opal_list_prepend (&request->schedule, &request_item->super);
            }
        }

        /* if the request schedule is empty then the request is complete */
        if (0 == opal_list_get_size (&request->schedule)) {
            opal_list_remove_item (&ompi_comm_requests_active, (opal_list_item_t *) request);
            request->super.req_status.MPI_ERROR = (OMPI_SUCCESS == rc) ? MPI_SUCCESS : MPI_ERR_INTERN;
            ompi_request_complete (&request->super, true);
        }
    }

    if (0 == opal_list_get_size (&ompi_comm_requests_active)) {
        /* no more active requests. disable this progress function */
        ompi_comm_request_progress_active = false;
        opal_progress_unregister (ompi_comm_request_progress);
    }

    opal_mutex_unlock (&ompi_comm_request_mutex);
    progressing = 0;

    return 1;
}

void ompi_comm_request_start (ompi_comm_request_t *request)
{
    opal_mutex_lock (&ompi_comm_request_mutex);
    opal_list_append (&ompi_comm_requests_active, (opal_list_item_t *) request);

    /* check if we need to start the communicator request progress function */
    if (!ompi_comm_request_progress_active) {
        opal_progress_register (ompi_comm_request_progress);
        ompi_comm_request_progress_active = true;
    }

    request->super.req_state = OMPI_REQUEST_ACTIVE;

    opal_mutex_unlock (&ompi_comm_request_mutex);
}

static int ompi_comm_request_cancel (struct ompi_request_t *ompi_req, int complete)
{
    ompi_comm_request_t *tmp, *request = (ompi_comm_request_t *) ompi_req;
    ompi_comm_request_item_t *item, *next;

    opal_mutex_lock (&ompi_comm_request_mutex);

    OPAL_LIST_FOREACH_SAFE(item, next, &request->schedule, ompi_comm_request_item_t) {
        for (int i = 0 ; i < item->subreq_count ; ++i) {
            ompi_request_cancel (item->subreqs[i]);
        }

        opal_list_remove_item (&request->schedule, &item->super);
        OBJ_RELEASE(item);
    }

    /* remove the request for the list of active requests */
    OPAL_LIST_FOREACH(tmp, &ompi_comm_requests_active, ompi_comm_request_t) {
        if (tmp == request) {
            opal_list_remove_item (&ompi_comm_requests_active, (opal_list_item_t *) request);
            break;
        }
    }

    opal_mutex_unlock (&ompi_comm_request_mutex);

    return MPI_ERR_REQUEST;
}

static int ompi_comm_request_free (struct ompi_request_t **ompi_req)
{
    ompi_comm_request_t *request = (ompi_comm_request_t *) *ompi_req;

    if( !REQUEST_COMPLETE(*ompi_req) ) {
        return MPI_ERR_REQUEST;
    }

    OMPI_REQUEST_FINI(*ompi_req);
    ompi_comm_request_return (request);

    *ompi_req = MPI_REQUEST_NULL;

    return OMPI_SUCCESS;
}

static void ompi_comm_request_construct (ompi_comm_request_t *request)
{
    request->context = NULL;

    request->super.req_type = OMPI_REQUEST_COMM;
    request->super.req_status._cancelled = 0;
    request->super.req_free = ompi_comm_request_free;
    request->super.req_cancel = ompi_comm_request_cancel;

    OBJ_CONSTRUCT(&request->schedule, opal_list_t);
}

static void ompi_comm_request_destruct (ompi_comm_request_t *request)
{
    OBJ_DESTRUCT(&request->schedule);
}

OBJ_CLASS_INSTANCE(ompi_comm_request_t, ompi_request_t,
                   ompi_comm_request_construct,
                   ompi_comm_request_destruct);

OBJ_CLASS_INSTANCE(ompi_comm_request_item_t, opal_list_item_t, NULL, NULL);

ompi_comm_request_t *ompi_comm_request_get (void)
{
    opal_free_list_item_t *item;

    item = opal_free_list_get (&ompi_comm_requests);
    if (OPAL_UNLIKELY(NULL == item)) {
        return NULL;
    }

    OMPI_REQUEST_INIT((ompi_request_t *) item, false);

    return (ompi_comm_request_t *) item;
}

void ompi_comm_request_return (ompi_comm_request_t *request)
{
    if (request->context) {
        OBJ_RELEASE (request->context);
        request->context = NULL;
    }

    OMPI_REQUEST_FINI(&request->super);
    opal_free_list_return (&ompi_comm_requests, (opal_free_list_item_t *) request);
}

