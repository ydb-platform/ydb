/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2010 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2018 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2008 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2006-2008 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2010-2012 Oracle and/or its affiliates.  All rights reserved.
 * Copyright (c) 2012      Oak Ridge National Labs.  All rights reserved.
 * Copyright (c) 2016-2017 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2016      Mellanox Technologies. All rights reserved.
 * Copyright (c) 2016      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
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

int ompi_request_default_wait(
    ompi_request_t ** req_ptr,
    ompi_status_public_t * status)
{
    ompi_request_t *req = *req_ptr;

    ompi_request_wait_completion(req);

    OMPI_CRCP_REQUEST_COMPLETE(req);

    /* return status.  If it's a generalized request, we *have* to
       invoke the query_fn, even if the user procided STATUS_IGNORE.
       MPI-2:8.2. */
    if (OMPI_REQUEST_GEN == req->req_type) {
        ompi_grequest_invoke_query(req, &req->req_status);
    }
    if( MPI_STATUS_IGNORE != status ) {
        /* Do *NOT* set status->MPI_ERROR here!  See MPI-1.1 doc, sec
           3.2.5, p.22 */
        status->MPI_TAG    = req->req_status.MPI_TAG;
        status->MPI_SOURCE = req->req_status.MPI_SOURCE;
        status->_ucount    = req->req_status._ucount;
        status->_cancelled = req->req_status._cancelled;
    }
    if( req->req_persistent ) {
        if( req->req_state == OMPI_REQUEST_INACTIVE ) {
            if (MPI_STATUS_IGNORE != status) {
                *status = ompi_status_empty;
            }
            return OMPI_SUCCESS;
        }
        req->req_state = OMPI_REQUEST_INACTIVE;
        return req->req_status.MPI_ERROR;
    }

    /* If there was an error, don't free the request -- just return
       the single error. */
    if (MPI_SUCCESS != req->req_status.MPI_ERROR) {
        return req->req_status.MPI_ERROR;
    }

    /* If there's an error while freeing the request, assume that the
       request is still there.  Otherwise, Bad Things will happen
       later! */
    return ompi_request_free(req_ptr);
}


int ompi_request_default_wait_any(size_t count,
                                  ompi_request_t ** requests,
                                  int *index,
                                  ompi_status_public_t * status)
{
    size_t i, completed = count, num_requests_null_inactive = 0;
    int rc = OMPI_SUCCESS;
    ompi_request_t *request=NULL;
    ompi_wait_sync_t sync;

    if (OPAL_UNLIKELY(0 == count)) {
        *index = MPI_UNDEFINED;
        return OMPI_SUCCESS;
    }

    WAIT_SYNC_INIT(&sync, 1);

    num_requests_null_inactive = 0;
    for (i = 0; i < count; i++) {
        void *_tmp_ptr = REQUEST_PENDING;

        request = requests[i];

        /* Check for null or completed persistent request. For
         * MPI_REQUEST_NULL, the req_state is always OMPI_REQUEST_INACTIVE.
         */
        if( request->req_state == OMPI_REQUEST_INACTIVE ) {
            num_requests_null_inactive++;
            continue;
        }

        if( !OPAL_ATOMIC_COMPARE_EXCHANGE_STRONG_PTR(&request->req_complete, &_tmp_ptr, &sync) ) {
            assert(REQUEST_COMPLETE(request));
            completed = i;
            *index = i;
            goto after_sync_wait;
        }
    }

    if(num_requests_null_inactive == count) {
        *index = MPI_UNDEFINED;
        if (MPI_STATUS_IGNORE != status) {
            *status = ompi_status_empty;
        }
        /* No signal-in-flight can be in this case */
        WAIT_SYNC_RELEASE_NOWAIT(&sync);
        return rc;
    }

    SYNC_WAIT(&sync);

  after_sync_wait:
    /* recheck the complete status and clean up the sync primitives.
     * Do it backward to return the earliest complete request to the
     * user.
     */
    for(i = completed-1; (i+1) > 0; i--) {
        void *tmp_ptr = &sync;

        request = requests[i];

        if( request->req_state == OMPI_REQUEST_INACTIVE ) {
            continue;
        }
        /* Atomically mark the request as pending. If this succeed then
         * the request was not completed, and it is now marked as pending.
         * Otherwise, the request has been completed meanwhile, and it
         * has been atomically marked as REQUEST_COMPLETE.
         */
        if( !OPAL_ATOMIC_COMPARE_EXCHANGE_STRONG_PTR(&request->req_complete, &tmp_ptr, REQUEST_PENDING) ) {
            *index = i;
        }
    }

    if( *index == (int)completed ) {
        /* Only one request has triggered. There was no in-flight
         * completions. Drop the signalled flag so we won't block
         * in WAIT_SYNC_RELEASE 
         */
        WAIT_SYNC_SIGNALLED(&sync);
    }

    request = requests[*index];
    assert( REQUEST_COMPLETE(request) );
#if OPAL_ENABLE_FT_CR == 1
    if( opal_cr_is_enabled ) {
        OMPI_CRCP_REQUEST_COMPLETE(request);
    }
#endif
    /* Per note above, we have to call gen request query_fn even
       if STATUS_IGNORE was provided */
    if (OMPI_REQUEST_GEN == request->req_type) {
        rc = ompi_grequest_invoke_query(request, &request->req_status);
    }
    if (MPI_STATUS_IGNORE != status) {
        /* Do *NOT* set status->MPI_ERROR here!  See MPI-1.1 doc,
           sec 3.2.5, p.22 */
        int old_error = status->MPI_ERROR;
        *status = request->req_status;
        status->MPI_ERROR = old_error;
    }
    rc = request->req_status.MPI_ERROR;
    if( request->req_persistent ) {
        request->req_state = OMPI_REQUEST_INACTIVE;
    } else if (MPI_SUCCESS == rc) {
        /* Only free the request if there is no error on it */
        /* If there's an error while freeing the request,
           assume that the request is still there.  Otherwise,
           Bad Things will happen later! */
        rc = ompi_request_free(&requests[*index]);
    }

    WAIT_SYNC_RELEASE(&sync);
    return rc;
}


int ompi_request_default_wait_all( size_t count,
                                   ompi_request_t ** requests,
                                   ompi_status_public_t * statuses )
{
    size_t i, completed = 0, failed = 0;
    ompi_request_t **rptr;
    ompi_request_t *request;
    int mpi_error = OMPI_SUCCESS;
    ompi_wait_sync_t sync;

    if (OPAL_UNLIKELY(0 == count)) {
        return OMPI_SUCCESS;
    }

    WAIT_SYNC_INIT(&sync, count);
    rptr = requests;
    for (i = 0; i < count; i++) {
        void *_tmp_ptr = REQUEST_PENDING;

        request = *rptr++;

        if( request->req_state == OMPI_REQUEST_INACTIVE ) {
            completed++;
            continue;
        }

        if (!OPAL_ATOMIC_COMPARE_EXCHANGE_STRONG_PTR(&request->req_complete, &_tmp_ptr, &sync)) {
            if( OPAL_UNLIKELY( MPI_SUCCESS != request->req_status.MPI_ERROR ) ) {
                failed++;
            }
            completed++;
        }
    }
    if( failed > 0 ) {
        goto finish;
    }

    if( 0 != completed ) {
        wait_sync_update(&sync, completed, OPAL_SUCCESS);
    }

    /* wait until all requests complete or until an error is triggered. */
    mpi_error = SYNC_WAIT(&sync);
    if( OPAL_SUCCESS != mpi_error ) {
        /* if we are in an error case, increase the failed to ensure
           proper cleanup during the requests completion. */
        failed++;
    }

 finish:
    rptr = requests;
    if (MPI_STATUSES_IGNORE != statuses) {
        /* fill out status and free request if required */
        for( i = 0; i < count; i++, rptr++ ) {
            void *_tmp_ptr = &sync;

            request = *rptr;

            if( request->req_state == OMPI_REQUEST_INACTIVE ) {
                statuses[i] = ompi_status_empty;
                continue;
            }

            if( OPAL_UNLIKELY(0 < failed) ) {
                /* if we have failed requests we skipped the waiting on the sync. Thus,
                 * some of the requests might not be properly completed, in which case
                 * we must detach all requests from the sync. However, if we can succesfully
                 * mark the request as pending then it is neither failed nor complete, and
                 * we must stop altering it.
                 */
                if( OPAL_ATOMIC_COMPARE_EXCHANGE_STRONG_PTR(&request->req_complete, &_tmp_ptr, REQUEST_PENDING ) ) {
                    /*
                     * Per MPI 2.2 p 60:
                     * Allows requests to be marked as MPI_ERR_PENDING if they are
                     * "neither failed nor completed." Which can only happen if
                     * there was an error in one of the other requests.
                     */
                    statuses[i].MPI_ERROR = MPI_ERR_PENDING;
                    mpi_error = MPI_ERR_IN_STATUS;
                    continue;
                }
            }
            assert( REQUEST_COMPLETE(request) );

            if( opal_cr_is_enabled) {
                OMPI_CRCP_REQUEST_COMPLETE(request);
            }

            if (OMPI_REQUEST_GEN == request->req_type) {
                ompi_grequest_invoke_query(request, &request->req_status);
            }

            statuses[i] = request->req_status;

            if( request->req_persistent ) {
                request->req_state = OMPI_REQUEST_INACTIVE;
                continue;
            }
            /* Only free the request if there is no error on it */
            if (MPI_SUCCESS == request->req_status.MPI_ERROR) {
                /* If there's an error while freeing the request,
                   assume that the request is still there.
                   Otherwise, Bad Things will happen later! */
                int tmp = ompi_request_free(rptr);
                if (OMPI_SUCCESS == mpi_error && OMPI_SUCCESS != tmp) {
                    mpi_error = tmp;
                }
            }
            if( statuses[i].MPI_ERROR != OMPI_SUCCESS) {
                mpi_error = MPI_ERR_IN_STATUS;
            }
        }
    } else {
        int rc;
        /* free request if required */
        for( i = 0; i < count; i++, rptr++ ) {
            void *_tmp_ptr = &sync;

            request = *rptr;

            if( request->req_state == OMPI_REQUEST_INACTIVE ) {
                rc = ompi_status_empty.MPI_ERROR;
                goto absorb_error_and_continue;
            }
            /*
             * Assert only if no requests were failed.
             * Since some may still be pending.
             */
            if( OPAL_UNLIKELY(0 < failed) ) {
                /* If the request is still pending due to a failed request
                 * then skip it in this loop.
                 */
                 if( OPAL_ATOMIC_COMPARE_EXCHANGE_STRONG_PTR(&request->req_complete, &_tmp_ptr, REQUEST_PENDING ) ) {
                    /*
                     * Per MPI 2.2 p 60:
                     * Allows requests to be marked as MPI_ERR_PENDING if they are
                     * "neither failed nor completed." Which can only happen if
                     * there was an error in one of the other requests.
                     */
                    rc = MPI_ERR_PENDING;
                    goto absorb_error_and_continue;
                 }
            }
            assert( REQUEST_COMPLETE(request) );

            if( opal_cr_is_enabled) {
                OMPI_CRCP_REQUEST_COMPLETE(request);
            }

            /* Per note above, we have to call gen request query_fn
               even if STATUSES_IGNORE was provided */
            if (OMPI_REQUEST_GEN == request->req_type) {
                rc = ompi_grequest_invoke_query(request, &request->req_status);
            }

            rc = request->req_status.MPI_ERROR;

            if( request->req_persistent ) {
                request->req_state = OMPI_REQUEST_INACTIVE;
            } else if (MPI_SUCCESS == rc) {
                /* Only free the request if there is no error on it */
                int tmp = ompi_request_free(rptr);
                if (OMPI_SUCCESS == mpi_error && OMPI_SUCCESS != tmp) {
                    mpi_error = tmp;
                }
            }
    absorb_error_and_continue:
            /*
             * Per MPI 2.2 p34:
             * "It is possible for an MPI function to return MPI_ERR_IN_STATUS
             *  even when MPI_STATUS_IGNORE or MPI_STATUSES_IGNORE has been
             *  passed to that function."
             * So we should do so here as well.
             */
            if( OMPI_SUCCESS == mpi_error && rc != OMPI_SUCCESS) {
                mpi_error = MPI_ERR_IN_STATUS;
            }
        }
    }
    WAIT_SYNC_RELEASE(&sync);
    return mpi_error;
}


int ompi_request_default_wait_some(size_t count,
                                   ompi_request_t ** requests,
                                   int * outcount,
                                   int * indices,
                                   ompi_status_public_t * statuses)
{
    size_t num_requests_null_inactive, num_requests_done, num_active_reqs;
    int rc = MPI_SUCCESS;
    ompi_request_t **rptr = NULL;
    ompi_request_t *request = NULL;
    ompi_wait_sync_t sync;
    size_t sync_sets = 0, sync_unsets = 0;

    if (OPAL_UNLIKELY(0 == count)) {
        *outcount = MPI_UNDEFINED;
        return OMPI_SUCCESS;
    }

    WAIT_SYNC_INIT(&sync, 1);

    *outcount = 0;

    rptr = requests;
    num_requests_null_inactive = 0;
    num_requests_done = 0;
    num_active_reqs = 0;
    for (size_t i = 0; i < count; i++, rptr++) {
        void *_tmp_ptr = REQUEST_PENDING;

        request = *rptr;
        /*
         * Check for null or completed persistent request.
         * For MPI_REQUEST_NULL, the req_state is always OMPI_REQUEST_INACTIVE.
         */
        if( request->req_state == OMPI_REQUEST_INACTIVE ) {
            num_requests_null_inactive++;
            continue;
        }
        indices[num_active_reqs] = OPAL_ATOMIC_COMPARE_EXCHANGE_STRONG_PTR(&request->req_complete, &_tmp_ptr, &sync);
        if( !indices[num_active_reqs] ) {
            /* If the request is completed go ahead and mark it as such */
            assert( REQUEST_COMPLETE(request) );
            num_requests_done++;
        }
        num_active_reqs++;
    }

    if(num_requests_null_inactive == count) {
        *outcount = MPI_UNDEFINED;
        /* nobody will signall us */
        WAIT_SYNC_RELEASE_NOWAIT(&sync);
        return rc;
    }

    sync_sets = num_active_reqs - num_requests_done;
    if( 0 == num_requests_done ) {
        /* One completed request is enough to satisfy the some condition */
        SYNC_WAIT(&sync);
    }

    /* Do the final counting and */
    /* Clean up the synchronization primitives */

    rptr = requests;
    num_requests_done = 0;
    num_active_reqs = 0;
    for (size_t i = 0; i < count; i++, rptr++) {
        void *_tmp_ptr = &sync;

        request = *rptr;

        if( request->req_state == OMPI_REQUEST_INACTIVE ) {
            continue;
        }
        /* Here we have 3 possibilities:
         * a) request was found completed in the first loop
         *    => ( indices[i] == 0 )
         * b) request was completed between first loop and this check
         *    => ( indices[i] == 1 ) and we can NOT atomically mark the 
         *    request as pending.
         * c) request wasn't finished yet
         *    => ( indices[i] == 1 ) and we CAN  atomically mark the 
         *    request as pending.
         * NOTE that in any case (i >= num_requests_done) as latter grows
         * either slowly (in case of partial completion)
         * OR in parallel with `i` (in case of full set completion)  
         */
        if( !indices[num_active_reqs] ) {
            indices[num_requests_done++] = i;
        } else if( !OPAL_ATOMIC_COMPARE_EXCHANGE_STRONG_PTR(&request->req_complete, &_tmp_ptr, REQUEST_PENDING) ) {
            indices[num_requests_done++] = i;
        }
        num_active_reqs++;
    }
    sync_unsets = num_active_reqs - num_requests_done;

    if( sync_sets == sync_unsets ){
        /* nobody knows about us,
         * set signa-in-progress flag to false
         */
        WAIT_SYNC_SIGNALLED(&sync);
    }

    WAIT_SYNC_RELEASE(&sync);

    *outcount = num_requests_done;

    for (size_t i = 0; i < num_requests_done; i++) {
        request = requests[indices[i]];
        assert( REQUEST_COMPLETE(request) );

#if OPAL_ENABLE_FT_CR == 1
        if( opal_cr_is_enabled) {
            OMPI_CRCP_REQUEST_COMPLETE(request);
        }
#endif

        /* Per note above, we have to call gen request query_fn even
           if STATUS_IGNORE was provided */
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
