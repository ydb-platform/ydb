/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2013 The Trustees of the University of Tennessee.
 *                         All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"
#include "ompi/mca/pml/base/pml_base_request.h"
#include "opal_stdint.h"

#ifndef __INCLUDE_VPROTOCOL_PESSIMIST_EVENT_H__
#define __INCLUDE_VPROTOCOL_PESSIMIST_EVENT_H__

BEGIN_C_DECLS

/* Make sure -Wformat is happy... */
typedef uint64_t vprotocol_pessimist_clock_t;
#define PRIpclock PRIx64

typedef enum {
  VPROTOCOL_PESSIMIST_EVENT_TYPE_MATCHING,
  VPROTOCOL_PESSIMIST_EVENT_TYPE_DELIVERY
} vprotocol_pessimist_event_type_t;

typedef struct vprotocol_pessimist_matching_event_t {
  vprotocol_pessimist_clock_t reqid;      /* recv request post "clock" */
  int src;                                /* matched src */
} vprotocol_pessimist_matching_event_t;

typedef struct vprotocol_pessimist_delivery_event_t {
  vprotocol_pessimist_clock_t probeid;    /* operation "clock" (for waits, tests, probes) */
  vprotocol_pessimist_clock_t reqid;      /* delivered request (recv or send) -TODO: SUPPORT FOR WaitSome/TestSome- */
} vprotocol_pessimist_delivery_event_t;

typedef union vprotocol_pessimist_mem_event_t {
    vprotocol_pessimist_matching_event_t e_matching;
    vprotocol_pessimist_delivery_event_t e_delivery;
} vprotocol_pessimist_mem_event_t;

typedef struct mca_vprotocol_pessimist_event_t {
  opal_free_list_item_t super;
  vprotocol_pessimist_event_type_t type;
  mca_pml_base_request_t *req;
  vprotocol_pessimist_mem_event_t u_event;
} mca_vprotocol_pessimist_event_t;

OMPI_DECLSPEC OBJ_CLASS_DECLARATION(mca_vprotocol_pessimist_event_t);


#define VPESSIMIST_MATCHING_EVENT_NEW(event)                            \
    do {                                                                \
        opal_free_list_item_t *item;                                    \
        item = opal_free_list_wait (&mca_vprotocol_pessimist.events_pool); \
        event = (mca_vprotocol_pessimist_event_t *) item;               \
        event->type = VPROTOCOL_PESSIMIST_EVENT_TYPE_MATCHING;          \
        event->u_event.e_matching.src = -1;                             \
    } while(0)

#define VPESSIMIST_DELIVERY_EVENT_NEW(event)                            \
    do {                                                                \
        opal_free_list_item_t *item;                                    \
        item = opal_free_list_wait (&mca_vprotocol_pessimist.events_pool); \
        event = (mca_vprotocol_pessimist_event_t *) item;               \
        event->type = VPROTOCOL_PESSIMIST_EVENT_TYPE_DELIVERY;          \
    } while(0)

#define VPESSIMIST_EVENT_RETURN(event)                                  \
    opal_free_list_return (&mca_vprotocol_pessimist.events_pool,        \
                           (opal_free_list_item_t *) event)

END_C_DECLS

#endif /* INCLUDE_VPROTOCOL_PESSIMIST_EVENT_H__ */
