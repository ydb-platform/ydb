/*
 * Copyright (c) 2004-2007 The Trustees of the University of Tennessee.
 *                         All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef __INCLUDE_VPROTOCOL_PESSIMIST_REQUEST_H_
#define __INCLUDE_VPROTOCOL_PESSIMIST_REQUEST_H_

#include "ompi_config.h"
#include "ompi/request/request.h"
#include "../base/vprotocol_base_request.h"
#include "vprotocol_pessimist_event.h"
#include "vprotocol_pessimist_sender_based_types.h"

BEGIN_C_DECLS

typedef struct mca_vprotocol_pessimist_request_t {
    opal_list_item_t list_item; /* must always be first field */
    ompi_request_free_fn_t pml_req_free;
    vprotocol_pessimist_clock_t reqid;
    mca_vprotocol_pessimist_event_t *event;
    vprotocol_pessimist_sender_based_request_t sb;
} mca_vprotocol_pessimist_request_t;

typedef mca_vprotocol_pessimist_request_t mca_vprotocol_pessimist_recv_request_t;
typedef mca_vprotocol_pessimist_request_t mca_vprotocol_pessimist_send_request_t;

OBJ_CLASS_DECLARATION(mca_vprotocol_pessimist_recv_request_t);
OBJ_CLASS_DECLARATION(mca_vprotocol_pessimist_send_request_t);

#define VPESSIMIST_FTREQ(req) \
    ((mca_vprotocol_pessimist_request_t *) VPROTOCOL_FTREQ(req))

#define VPESSIMIST_RECV_FTREQ(req) \
    ((mca_vprotocol_pessimist_recv_request_t *) VPROTOCOL_RECV_FTREQ(req))

#define VPESSIMIST_SEND_FTREQ(req) \
    ((mca_vprotocol_pessimist_send_request_t *) VPROTOCOL_SEND_FTREQ(req))

#define VPESSIMIST_FTREQ_INIT(req) do {                                         \
        VPESSIMIST_FTREQ(req)->reqid = mca_vprotocol_pessimist.clock++;        \
} while(0)

int mca_vprotocol_pessimist_request_free(ompi_request_t **req);

END_C_DECLS

#endif /* __INCLUDE_VPROTOCOL_PESSIMIST_REQUEST_H_ */
