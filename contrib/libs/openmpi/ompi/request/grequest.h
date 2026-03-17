/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2006      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2018      Research Organization for Information Science
 *                         and Technology (RIST).  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef OMPI_GEN_REQUEST_H
#define OMPI_GEN_REQUEST_H

#include "ompi_config.h"
#include "ompi/request/request.h"

BEGIN_C_DECLS
OMPI_DECLSPEC OBJ_CLASS_DECLARATION(ompi_grequest_t);

/**
 * Fortran type for generalized request query function
 */
typedef void (MPI_F_Grequest_query_function)(MPI_Aint *extra_state,
                                             MPI_Fint *status,
                                             MPI_Fint *ierr);
/**
 * Fortran type for generalized request free function
 */
typedef void (MPI_F_Grequest_free_function)(MPI_Aint *extra_state,
                                            MPI_Fint *ierr);
/**
 * Fortran type for generalized request cancel function
 */
typedef void (MPI_F_Grequest_cancel_function)(MPI_Aint *extra_state,
                                              ompi_fortran_logical_t *complete,
                                              MPI_Fint *ierr);

#if OMPI_ENABLE_GREQUEST_EXTENSIONS
/**
 * Fortran type for generalized request query function
 */
typedef int (ompi_grequestx_poll_function)(void *, MPI_Status *);

typedef void (ompi_f_grequestx_poll_function)(MPI_Aint *extra_state,
                                              MPI_Fint *status,
                                              MPI_Fint *ierr);
#endif

/**
 * Union for query function for use in ompi_grequest_t
 */
typedef union {
    MPI_Grequest_query_function*   c_query;
    MPI_F_Grequest_query_function* f_query;
} MPI_Grequest_query_fct_t;

/**
 * Union for free function for use in ompi_grequest_t
 */
typedef union {
    MPI_Grequest_free_function*   c_free;
    MPI_F_Grequest_free_function* f_free;
} MPI_Grequest_free_fct_t;

/**
 * Union for cancel function for use in ompi_grequest_t
 */
typedef union {
    MPI_Grequest_cancel_function*   c_cancel;
    MPI_F_Grequest_cancel_function* f_cancel;
} MPI_Grequest_cancel_fct_t;

#if OMPI_ENABLE_GREQUEST_EXTENSIONS
/**
 * Union for poll function for use in ompi_grequestx_t
 */
typedef union {
    ompi_grequestx_poll_function*   c_poll;
    ompi_f_grequestx_poll_function*  f_poll;
} ompi_grequestx_poll_fct_t;
#endif

/**
 * Main structure for MPI generalized requests
 */
struct ompi_grequest_t {
    ompi_request_t greq_base;
    MPI_Grequest_query_fct_t greq_query;
    MPI_Grequest_free_fct_t greq_free;
    MPI_Grequest_cancel_fct_t greq_cancel;
#if OMPI_ENABLE_GREQUEST_EXTENSIONS
    ompi_grequestx_poll_fct_t greq_poll;
#endif
    void *greq_state;
    bool greq_funcs_are_c;
};
/**
 * Convenience typedef
 */
typedef struct ompi_grequest_t ompi_grequest_t;

/**
 * Start a generalized request (back end for MPI_GREQUEST_START)
 */
OMPI_DECLSPEC int ompi_grequest_start(
    MPI_Grequest_query_function *gquery,
    MPI_Grequest_free_function *gfree,
    MPI_Grequest_cancel_function *gcancel,
    void* gstate,
    ompi_request_t** request);

/**
 * Complete a generalized request (back end for MPI_GREQUEST_COMPLETE)
 */
OMPI_DECLSPEC int ompi_grequest_complete(ompi_request_t *req);

/**
 * Invoke the query function on a generalized request
 */
OMPI_DECLSPEC int ompi_grequest_invoke_query(ompi_request_t *request,
                                             ompi_status_public_t *status);
END_C_DECLS

#endif
