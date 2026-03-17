/*
 * Copyright (c) 2004-2007 The Trustees of the University of Tennessee.
 *                         All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"
#include "vprotocol_pessimist.h"
#include "vprotocol_pessimist_wait.h"


/* Helpers to prevent requests from being freed by the real wait/test */
static int vprotocol_pessimist_request_no_free(ompi_request_t **req) {
    return OMPI_SUCCESS;
}

#define PREPARE_REQUESTS_WITH_NO_FREE(count, requests) do { \
    size_t i; \
    for(i = 0; i < count; i++) \
    { \
        if(requests[i] == MPI_REQUEST_NULL) continue; \
        requests[i]->req_free = vprotocol_pessimist_request_no_free; \
    } \
} while(0)


int mca_vprotocol_pessimist_test(ompi_request_t ** rptr, int *completed,
                                 ompi_status_public_t * status)
{
    int ret;
    int index;

    VPROTOCOL_PESSIMIST_DELIVERY_REPLAY(1, rptr, completed, &index, status);

    ret = mca_pml_v.host_request_fns.req_test(rptr, completed, status);
    if(completed)
        vprotocol_pessimist_delivery_log(*rptr);
    else
        vprotocol_pessimist_delivery_log(NULL);
    return ret;
}

int mca_vprotocol_pessimist_test_all(size_t count, ompi_request_t ** requests,
                                     int *completed,
                                     ompi_status_public_t * statuses)
{
    int ret;
    int index;

    /* /!\ this is not correct until I upgrade DELIVERY_REPLAY to manage several requests at once */
    VPROTOCOL_PESSIMIST_DELIVERY_REPLAY(1, requests, completed, &index, statuses);

    ret = mca_pml_v.host_request_fns.req_test_all(count, requests, completed,
                                                   statuses);
#if 0
/* This is not correct :/ */
    if(completed)
        vprotocol_pessimist_delivery_log(requests); /* /!\ need to make sure this is correct: what if first is request_null ? */
    else
        vprotocol_pessimist_delivery_log(NULL);
#endif
    return ret;
}


/* TESTANY and WAITANY */

int mca_vprotocol_pessimist_test_any(size_t count, ompi_request_t ** requests,
                                     int *index, int *completed,
                                     ompi_status_public_t * status)
{
    int ret;
    size_t i;

    VPROTOCOL_PESSIMIST_DELIVERY_REPLAY(count, requests, completed, index, status);

    PREPARE_REQUESTS_WITH_NO_FREE(count, requests);

    /* Call the real one to do the job */
    ret = mca_pml_v.host_request_fns.req_test_any(count, requests, index, completed,
                                                  status);

    if(completed)
    {   /* Parse the result */
        for(i = 0; i < count; i++)
        {
            ompi_request_t *req = requests[i];
            if(req == MPI_REQUEST_NULL) continue;

            /* Restore requests and store they've been delivered */
            req->req_free = mca_vprotocol_pessimist_request_free;
            if(i == (size_t) *index)
            {
                vprotocol_pessimist_delivery_log(req);
                /* only free request without error status */
                if(req->req_status.MPI_ERROR == MPI_SUCCESS)
                    ompi_request_free(&(requests[i]));
                else
                    ret = req->req_status.MPI_ERROR;
            }
        }
    }
    else
    {
        /* No request delivered this time, log it */
        vprotocol_pessimist_delivery_log(NULL);
    }
    return ret;
}

int mca_vprotocol_pessimist_wait_any(size_t count, ompi_request_t ** requests,
                                     int *index, ompi_status_public_t * status)
{
    int ret;
    size_t i;
    int dummy;

    VPROTOCOL_PESSIMIST_DELIVERY_REPLAY(count, requests, &dummy, index, status);

    PREPARE_REQUESTS_WITH_NO_FREE(count, requests);

    /* Call the real one to do the job */
    ret = mca_pml_v.host_request_fns.req_wait_any(count, requests, index, status);

    /* Parse the result */
    for(i = 0; i < count; i++)
    {
        ompi_request_t *req = requests[i];
        if(req == MPI_REQUEST_NULL) continue;

        /* Restore requests and store they've been delivered */
        req->req_free = mca_vprotocol_pessimist_request_free;
        if(i == (size_t) *index)
        {
            vprotocol_pessimist_delivery_log(req);
            /* only free request without error status */
            if(req->req_status.MPI_ERROR == MPI_SUCCESS)
                ompi_request_free(&(requests[i]));
            else
                ret = req->req_status.MPI_ERROR;
        }
    }
    return ret;
}


/* TESTSOME and WAITSOME */

int mca_vprotocol_pessimist_test_some(size_t count, ompi_request_t ** requests,
                                      int * outcount, int * indices,
                                      ompi_status_public_t * statuses)
{
    int ret;
    ret = mca_vprotocol_pessimist_test_any(count, requests, indices, outcount, statuses);
    if(*outcount) *outcount = 1;
    return ret;
}

int mca_vprotocol_pessimist_wait_some(size_t count, ompi_request_t ** requests,
                                      int *outcount, int *indexes,
                                      ompi_status_public_t * statuses)
{
    int ret;
    ret = mca_vprotocol_pessimist_wait_any(count, requests, indexes, statuses);
    if(MPI_UNDEFINED == *indexes) *outcount = 0;
    else *outcount = 1;
    return ret;
}
