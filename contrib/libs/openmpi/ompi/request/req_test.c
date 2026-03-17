/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2016 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2008 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2006-2008 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2010-2012 Oracle and/or its affiliates.  All rights reserved.
 * Copyright (c) 2012      Oak Ridge National Labs.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"
#include "ompi/constants.h"
#include "ompi/request/request.h"
#include "ompi/request/request_default.h"
#include "ompi/request/grequest.h"

#include "ompi/mca/crcp/crcp.h"

int ompi_request_default_test(ompi_request_t ** rptr,
                              int *completed,
                              ompi_status_public_t * status )
{
    ompi_request_t *request = *rptr;

#if OPAL_ENABLE_PROGRESS_THREADS == 0
    int do_it_once = 0;

 recheck_request_status:
#endif
    opal_atomic_mb();
    if( request->req_state == OMPI_REQUEST_INACTIVE ) {
        *completed = true;
        if (MPI_STATUS_IGNORE != status) {
            *status = ompi_status_empty;
        }
        return OMPI_SUCCESS;
    }

    if( REQUEST_COMPLETE(request) ) {
#if OPAL_ENABLE_FT_CR == 1
        if( opal_cr_is_enabled) {
            OMPI_CRCP_REQUEST_COMPLETE(request);
        }
#endif

        *completed = true;
        /* For a generalized request, we *have* to call the query_fn
           if it completes, even if the user provided
           STATUS_IGNORE.  See MPI-2:8.2. */
        if (OMPI_REQUEST_GEN == request->req_type) {
            ompi_grequest_invoke_query(request, &request->req_status);
            if (MPI_STATUS_IGNORE != status) {
                int old_error = status->MPI_ERROR;
                *status = request->req_status;
                status->MPI_ERROR = old_error;
            }
        } else if (MPI_STATUS_IGNORE != status) {
            /* Do *NOT* set a new value for status->MPI_ERROR here!
               See MPI-1.1 doc, sec 3.2.5, p.22 */
            int old_error = status->MPI_ERROR;
            *status = request->req_status;
            status->MPI_ERROR = old_error;
        }
        if( request->req_persistent ) {
            request->req_state = OMPI_REQUEST_INACTIVE;
            return request->req_status.MPI_ERROR;
        }
        /* If there was an error, don't free the request -- just
           return the single error. */
        if (MPI_SUCCESS != request->req_status.MPI_ERROR) {
            return request->req_status.MPI_ERROR;
        }
        /* If there's an error on the request, assume that the request
           is still there.  Otherwise, Bad Things will happen
           later! */
        return ompi_request_free(rptr);
    }
#if OPAL_ENABLE_PROGRESS_THREADS == 0
    if( 0 == do_it_once ) {
        /**
         * If we run the opal_progress then check the status of the request before
         * leaving. We will call the opal_progress only once per call.
         */
        opal_progress();
        do_it_once++;
        goto recheck_request_status;
    }
#endif
    *completed = false;
    return OMPI_SUCCESS;
}

int ompi_request_default_test_any(
    size_t count,
    ompi_request_t ** requests,
    int *index,
    int *completed,
    ompi_status_public_t * status)
{
    size_t i;
    size_t num_requests_null_inactive = 0;
    ompi_request_t **rptr;
    ompi_request_t *request;

    opal_atomic_mb();
    rptr = requests;
    for (i = 0; i < count; i++, rptr++) {
        request = *rptr;
        if( request->req_state == OMPI_REQUEST_INACTIVE ) {
            num_requests_null_inactive++;
            continue;
        }

        if( REQUEST_COMPLETE(request) ) {
#if OPAL_ENABLE_FT_CR == 1
            if( opal_cr_is_enabled) {
                OMPI_CRCP_REQUEST_COMPLETE(request);
            }
#endif

            *index = i;
            *completed = true;
            /* MPI 2:8.2 says that generalized requests always have
               the query function invoked in TEST* / WAIT*
               (#@$%@#$%!!! Would have been simpler to call it in
               GREQUEST_COMPLETE!), even if the user passed in
               STATUS_IGNORE */
            if (OMPI_REQUEST_GEN == request->req_type) {
                ompi_grequest_invoke_query(request, &request->req_status);
                if (MPI_STATUS_IGNORE != status) {
                    /* Do *NOT* set a new value for status->MPI_ERROR
                       here!  See MPI-1.1 doc, sec 3.2.5, p.22 */
                    int old_error = status->MPI_ERROR;
                    *status = request->req_status;
                    status->MPI_ERROR = old_error;
                }
            } else if (MPI_STATUS_IGNORE != status) {
                /* Do *NOT* set a new value for status->MPI_ERROR
                   here!  See MPI-1.1 doc, sec 3.2.5, p.22 */
                int old_error = status->MPI_ERROR;
                *status = request->req_status;
                status->MPI_ERROR = old_error;
            }

            if( request->req_persistent ) {
                request->req_state = OMPI_REQUEST_INACTIVE;
                return OMPI_SUCCESS;
            }
            /* If there is an error on the request, don't free it */
            if (MPI_SUCCESS != request->req_status.MPI_ERROR) {
                return request->req_status.MPI_ERROR;
            }
            /* If there's an error while freeing the request, assume
               that the request is still there.  Otherwise, Bad Things
               will happen later! */
            return ompi_request_free(rptr);
        }
    }

    /* Only fall through here if we found nothing */
    *index = MPI_UNDEFINED;
    if(num_requests_null_inactive != count) {
        *completed = false;
#if OPAL_ENABLE_PROGRESS_THREADS == 0
        opal_progress();
#endif
    } else {
        *completed = true;
        if (MPI_STATUS_IGNORE != status) {
            *status = ompi_status_empty;
        }
    }
    return OMPI_SUCCESS;
}


int ompi_request_default_test_all(
    size_t count,
    ompi_request_t ** requests,
    int *completed,
    ompi_status_public_t * statuses)
{
    size_t i, rc;
    ompi_request_t **rptr;
    size_t num_completed = 0;
    ompi_request_t *request;

    opal_atomic_mb();
    rptr = requests;
    for (i = 0; i < count; i++, rptr++) {
        request = *rptr;

        if( request->req_state == OMPI_REQUEST_INACTIVE ||
            REQUEST_COMPLETE(request) ) {
            num_completed++;
        }
    }

    if (num_completed != count) {
        *completed = false;
#if OPAL_ENABLE_PROGRESS_THREADS == 0
        opal_progress();
#endif
        return OMPI_SUCCESS;
    }

    rptr = requests;
    *completed = true;

    rc = MPI_SUCCESS;
    if (MPI_STATUSES_IGNORE != statuses) {
        /* fill out completion status and free request if required */
        for( i = 0; i < count; i++, rptr++ ) {
            request  = *rptr;
            /* If the request is OMPI_REQUEST_INACTIVE set the status
             * to ompi_status_empty.
             */
            if( request->req_state == OMPI_REQUEST_INACTIVE ) {
                statuses[i] = ompi_status_empty;
                continue;
            }
            if (OMPI_REQUEST_GEN == request->req_type) {
                ompi_grequest_invoke_query(request, &request->req_status);
            }
#if OPAL_ENABLE_FT_CR == 1
            if( opal_cr_is_enabled) {
                OMPI_CRCP_REQUEST_COMPLETE(request);
            }
#endif
            statuses[i] = request->req_status;
            if( request->req_persistent ) {
                request->req_state = OMPI_REQUEST_INACTIVE;
                continue;
            }
            /* MPI-2:4.5.1 says that we can return MPI_ERR_IN_STATUS
               even if MPI_STATUSES_IGNORE was used.  Woot! */
            /* Only free the request if there was no error on it */
            if (MPI_SUCCESS == request->req_status.MPI_ERROR) {
                int tmp = ompi_request_free(rptr);
                if (tmp != OMPI_SUCCESS) {
                    return tmp;
                }
            } else {
                rc = MPI_ERR_IN_STATUS;
            }
        }
    } else {
        /* free request if required */
        for( i = 0; i < count; i++, rptr++ ) {
            request = *rptr;
            if( request->req_state == OMPI_REQUEST_INACTIVE) {
                continue;
            }
            /* See note above: if a generalized request completes, we
               *have* to call the query fn, even if STATUSES_IGNORE
               was supplied */
            if (OMPI_REQUEST_GEN == request->req_type) {
                ompi_grequest_invoke_query(request, &request->req_status);
            }
#if OPAL_ENABLE_FT_CR == 1
            if( opal_cr_is_enabled) {
                OMPI_CRCP_REQUEST_COMPLETE(request);
            }
#endif
            if( request->req_persistent ) {
                request->req_state = OMPI_REQUEST_INACTIVE;
                continue;
            }
            /* Only free the request if there was no error */
            if (MPI_SUCCESS == request->req_status.MPI_ERROR) {
                int tmp = ompi_request_free(rptr);
                if (tmp != OMPI_SUCCESS) {
                    return tmp;
                }
            } else {
                rc = MPI_ERR_IN_STATUS;
            }
        }
    }

    return rc;
}


int ompi_request_default_test_some(
    size_t count,
    ompi_request_t ** requests,
    int * outcount,
    int * indices,
    ompi_status_public_t * statuses)
{
    size_t i, num_requests_null_inactive = 0, num_requests_done = 0;
    int rc = OMPI_SUCCESS;
    ompi_request_t **rptr;
    ompi_request_t *request;

    opal_atomic_mb();
    rptr = requests;
    for (i = 0; i < count; i++, rptr++) {
        request = *rptr;
        if (request->req_state == OMPI_REQUEST_INACTIVE) {
            num_requests_null_inactive++;
            continue;
        }
        if( REQUEST_COMPLETE(request) ) {
#if OPAL_ENABLE_FT_CR == 1
            if( opal_cr_is_enabled) {
                OMPI_CRCP_REQUEST_COMPLETE(request);
            }
#endif
            indices[num_requests_done++] = i;
        }
    }

    /*
     * If there are no active requests, no need to progress
     */
    if (num_requests_null_inactive == count) {
        *outcount = MPI_UNDEFINED;
        return OMPI_SUCCESS;
    }

    *outcount = num_requests_done;

    if (num_requests_done == 0) {
#if OPAL_ENABLE_PROGRESS_THREADS == 0
        opal_progress();
#endif
        return OMPI_SUCCESS;
    }

    /* fill out completion status and free request if required */
    for( i = 0; i < num_requests_done; i++) {
        request = requests[indices[i]];

        /* See note above: if a generalized request completes, we
           *have* to call the query fn, even if STATUSES_IGNORE
           was supplied */
        if (OMPI_REQUEST_GEN == request->req_type) {
            ompi_grequest_invoke_query(request, &request->req_status);
        }
        if (MPI_STATUSES_IGNORE != statuses) {
            statuses[i] = request->req_status;
        }

        if (MPI_SUCCESS != request->req_status.MPI_ERROR) {
            rc = MPI_ERR_IN_STATUS;
        }

        if( request->req_persistent ) {
            request->req_state = OMPI_REQUEST_INACTIVE;
        } else {
            /* Only free the request if there was no error */
            if (MPI_SUCCESS == request->req_status.MPI_ERROR) {
                int tmp;
                tmp = ompi_request_free(&(requests[indices[i]]));
                if (OMPI_SUCCESS != tmp) {
                    return tmp;
                }
            }
        }
    }

    return rc;
}
