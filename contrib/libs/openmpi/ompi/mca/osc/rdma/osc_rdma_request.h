/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2012      Sandia National Laboratories.  All rights reserved.
 * Copyright (c) 2014-2018 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef OMPI_OSC_RDMA_REQUEST_H
#define OMPI_OSC_RDMA_REQUEST_H

#include "osc_rdma.h"

enum ompi_osc_rdma_request_type_t {
    OMPI_OSC_RDMA_TYPE_GET,
    OMPI_OSC_RDMA_TYPE_PUT,
    OMPI_OSC_RDMA_TYPE_RDMA,
    OMPI_OSC_RDMA_TYPE_ACC,
    OMPI_OSC_RDMA_TYPE_GET_ACC,
    OMPI_OSC_RDMA_TYPE_CSWAP,
};
typedef enum ompi_osc_rdma_request_type_t ompi_osc_rdma_request_type_t;

struct ompi_osc_rdma_request_t;

typedef void (*ompi_osc_rdma_request_cleanup_fn_t) (struct ompi_osc_rdma_request_t *);

struct ompi_osc_rdma_request_t {
    ompi_request_t super;

    ompi_osc_rdma_peer_t *peer;
    ompi_osc_rdma_request_cleanup_fn_t cleanup;

    ompi_osc_rdma_request_type_t type;
    void *to_free;
    void *origin_addr;

    ompi_osc_rdma_module_t *module;
    volatile int32_t outstanding_requests;
    bool internal;

    ptrdiff_t offset;
    size_t    len;
    void     *ctx;
    void     *frag;

    uint64_t target_address;

    struct ompi_osc_rdma_request_t *parent_request;
    /* used for non-contiguous get accumulate operations */
    opal_convertor_t convertor;

    /** synchronization object */
    struct ompi_osc_rdma_sync_t *sync;
    void *buffer;
};
typedef struct ompi_osc_rdma_request_t ompi_osc_rdma_request_t;
OBJ_CLASS_DECLARATION(ompi_osc_rdma_request_t);

/* REQUEST_ALLOC is only called from "top-level" functions (rdma_rput,
   rdma_rget, etc.), so it's ok to spin here... */
#define OMPI_OSC_RDMA_REQUEST_ALLOC(rmodule, rpeer, req)                \
    do {                                                                \
        (req) = OBJ_NEW(ompi_osc_rdma_request_t);                       \
        OMPI_REQUEST_INIT(&(req)->super, false);                        \
        (req)->super.req_mpi_object.win = (rmodule)->win;               \
        (req)->super.req_state = OMPI_REQUEST_ACTIVE;                   \
        (req)->module = rmodule;                                        \
        (req)->peer = (rpeer);                                          \
    } while (0)

#define OMPI_OSC_RDMA_REQUEST_RETURN(req)                               \
    do {                                                                \
        OMPI_REQUEST_FINI(&(req)->super);                               \
        free ((req)->buffer);                                           \
        free (req);                                                     \
    } while (0)

static inline void ompi_osc_rdma_request_complete (ompi_osc_rdma_request_t *request, int mpi_error);


static inline void ompi_osc_rdma_request_deref (ompi_osc_rdma_request_t *request)
{
    if (1 == OPAL_THREAD_FETCH_ADD32 (&request->outstanding_requests, -1)) {
        ompi_osc_rdma_request_complete (request, OMPI_SUCCESS);
    }
}

static inline void ompi_osc_rdma_request_complete (ompi_osc_rdma_request_t *request, int mpi_error)
{
    ompi_osc_rdma_request_t *parent_request = request->parent_request;

    if (request->cleanup) {
        request->cleanup (request);
    }

    free (request->to_free);

    if (parent_request) {
        ompi_osc_rdma_request_deref (parent_request);
    }

    if (!request->internal) {
        request->super.req_status.MPI_ERROR = mpi_error;

        /* mark the request complete at the mpi level */
        ompi_request_complete (&request->super, true);
    } else {
        OMPI_OSC_RDMA_REQUEST_RETURN (request);
    }
}

#endif /* OMPI_OSC_RDMA_REQUEST_H */
