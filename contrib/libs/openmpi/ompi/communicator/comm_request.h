/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2013-2016 Los Alamos National Security, LLC.  All rights
 *                         reseved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#if !defined(OMPI_COMM_REQ_H)
#define OMPI_COMM_REQ_H

#include "opal/class/opal_list.h"
#include "ompi/request/request.h"

/* increase this number if more subrequests are needed */
#define OMPI_COMM_REQUEST_MAX_SUBREQ 2

typedef struct ompi_comm_request_t {
    ompi_request_t super;

    opal_object_t *context;
    opal_list_t schedule;
} ompi_comm_request_t;
OBJ_CLASS_DECLARATION(ompi_comm_request_t);

typedef int (*ompi_comm_request_callback_fn_t) (ompi_comm_request_t *);

void ompi_comm_request_init (void);
void ompi_comm_request_fini (void);
int ompi_comm_request_schedule_append (ompi_comm_request_t *request, ompi_comm_request_callback_fn_t callback,
                            ompi_request_t *subreqs[], int subreq_count);
void ompi_comm_request_start (ompi_comm_request_t *request);
ompi_comm_request_t *ompi_comm_request_get (void);
void ompi_comm_request_return (ompi_comm_request_t *request);

#endif /* OMPI_COMM_REQ_H */
