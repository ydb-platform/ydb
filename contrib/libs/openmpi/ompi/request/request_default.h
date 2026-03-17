/*
 * Copyright (c) 2004-2007 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef OMPI_REQUEST_DEFAULT_H
#define OMPI_REQUEST_DEFAULT_H

#include "ompi/request/request.h"

BEGIN_C_DECLS

/** Defaults implementations for all request completions
 */
int ompi_request_default_test(
                              ompi_request_t ** rptr,
                              int *completed,
                              ompi_status_public_t * status );

int ompi_request_default_test_any(
                                  size_t count,
                                  ompi_request_t ** requests,
                                  int *index,
                                  int *completed,
                                  ompi_status_public_t * status);

int ompi_request_default_test_all(
                                  size_t count,
                                  ompi_request_t ** requests,
                                  int *completed,
                                  ompi_status_public_t * statuses);

int ompi_request_default_test_some(
                                   size_t count,
                                   ompi_request_t ** requests,
                                   int * outcount,
                                   int * indices,
                                   ompi_status_public_t * statuses);

int ompi_request_default_wait(
                              ompi_request_t ** req_ptr,
                              ompi_status_public_t * status);

int ompi_request_default_wait_any(
                                  size_t count,
                                  ompi_request_t ** requests,
                                  int *index,
                                  ompi_status_public_t * status);

int ompi_request_default_wait_all(
                                  size_t count,
                                  ompi_request_t ** requests,
                                  ompi_status_public_t * statuses);

int ompi_request_default_wait_some(
                                   size_t count,
                                   ompi_request_t ** requests,
                                   int * outcount,
                                   int * indices,
                                   ompi_status_public_t * statuses);

END_C_DECLS

#endif

