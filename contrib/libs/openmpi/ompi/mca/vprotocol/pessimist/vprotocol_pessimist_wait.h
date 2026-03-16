/*
 * Copyright (c) 2004-2007 The Trustees of the University of Tennessee.
 *                         All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef __VPROTOCOL_PESSIMIST_WAIT_H__
#define __VPROTOCOL_PESSIMIST_WAIT_H__

#include "ompi_config.h"
#include "ompi/request/request.h"

BEGIN_C_DECLS

#define VPROTOCOL_PESSIMIST_WAIT(req, status, rc) \
  ((rc) = ompi_request_wait(req, status))

int mca_vprotocol_pessimist_test(ompi_request_t ** rptr, int *completed,
                                 ompi_status_public_t * status);

int mca_vprotocol_pessimist_test_all(size_t count, ompi_request_t ** requests,
                                     int *completed,
                                     ompi_status_public_t * statuses);

int mca_vprotocol_pessimist_test_any(size_t count, ompi_request_t ** requests,
                                     int *index, int *completed,
                                     ompi_status_public_t * status);

int mca_vprotocol_pessimist_test_some(size_t count, ompi_request_t ** requests,
                                      int * outcount, int * indices,
                                      ompi_status_public_t * statuses);

int mca_vprotocol_pessimist_wait_any(size_t count, ompi_request_t ** requests,
                                     int *index, ompi_status_public_t * status);

int mca_vprotocol_pessimist_wait_some(size_t count, ompi_request_t ** requests,
                                      int *outcount, int *indexes,
                                      ompi_status_public_t * statuses);

END_C_DECLS

#endif /* __VPROTOCOL_PESSIMIST_WAIT_H__ */
