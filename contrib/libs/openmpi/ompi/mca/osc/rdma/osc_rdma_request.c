/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2011-2012 Sandia National Laboratories.  All rights reserved.
 * Copyright (c) 2014-2018 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2016      The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include "ompi/request/request.h"
#include "ompi/mca/osc/osc.h"
#include "ompi/mca/osc/base/base.h"
#include "ompi/mca/osc/base/osc_base_obj_convert.h"

#include "osc_rdma.h"
#include "osc_rdma_request.h"

static int request_cancel(struct ompi_request_t *request, int complete)
{
    return MPI_ERR_REQUEST;
}

static int request_free(struct ompi_request_t **ompi_req)
{
    ompi_osc_rdma_request_t *request =
        (ompi_osc_rdma_request_t*) *ompi_req;

    if (!REQUEST_COMPLETE(&request->super)) {
        return MPI_ERR_REQUEST;
    }

    OMPI_OSC_RDMA_REQUEST_RETURN(request);

    *ompi_req = MPI_REQUEST_NULL;

    return OMPI_SUCCESS;
}

static void request_construct(ompi_osc_rdma_request_t *request)
{
    request->super.req_type = OMPI_REQUEST_WIN;
    request->super.req_status._cancelled = 0;
    request->super.req_free = request_free;
    request->super.req_cancel = request_cancel;
    request->parent_request = NULL;
    request->to_free = NULL;
    request->buffer = NULL;
    request->internal = false;
    request->cleanup = NULL;
    request->outstanding_requests = 0;
    OBJ_CONSTRUCT(&request->convertor, opal_convertor_t);
}

static void request_destruct(ompi_osc_rdma_request_t *request)
{
    OBJ_DESTRUCT(&request->convertor);
}

OBJ_CLASS_INSTANCE(ompi_osc_rdma_request_t,
                   ompi_request_t,
                   request_construct,
                   request_destruct);
