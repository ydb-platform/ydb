/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2007 The Trustees of the University of Tennessee.
 *                         All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef __INCLUDE_VPROTOCOL_PESSIMIST_H__
#define __INCLUDE_VPROTOCOL_PESSIMIST_H__

#include "ompi_config.h"
#include "../vprotocol.h"
#include "ompi/communicator/communicator.h"
#include "ompi/datatype/ompi_datatype.h"

#include "vprotocol_pessimist_event.h"
#include "vprotocol_pessimist_sender_based_types.h"

BEGIN_C_DECLS

typedef struct mca_vprotocol_pessimist_module_t {
    mca_vprotocol_base_module_t super;

    /* Event clock */
    vprotocol_pessimist_clock_t clock;

    /* list of events to send to Event Logger */
    opal_list_t pending_events;

    /* output buffer for messages to event logger */
    ompi_communicator_t *el_comm;
    vprotocol_pessimist_mem_event_t *event_buffer;
    size_t event_buffer_length;
    size_t event_buffer_max_length;

    /* space for allocating events */
    opal_free_list_t events_pool;

    /* Sender Based repository */
    vprotocol_pessimist_sender_based_t sender_based;

    /* replay mode variables */
    bool replay;
    opal_list_t replay_events;
} mca_vprotocol_pessimist_module_t;

OMPI_DECLSPEC extern mca_vprotocol_pessimist_module_t mca_vprotocol_pessimist;
OMPI_DECLSPEC extern mca_vprotocol_base_component_t mca_vprotocol_pessimist_component;

int mca_vprotocol_pessimist_enable(bool enable);
int mca_vprotocol_pessimist_dump(struct ompi_communicator_t* comm, int verbose);

int mca_vprotocol_pessimist_add_procs(struct ompi_proc_t **procs, size_t nprocs);
int mca_vprotocol_pessimist_del_procs(struct ompi_proc_t **procs, size_t nprocs);
int mca_vprotocol_pessimist_progress(void);
int mca_vprotocol_pessimist_add_comm(struct ompi_communicator_t* comm);
int mca_vprotocol_pessimist_del_comm(struct ompi_communicator_t* comm);

int mca_vprotocol_pessimist_irecv(void *addr,
                                  size_t count,
                                  ompi_datatype_t * datatype,
                                  int src,
                                  int tag,
                                  struct ompi_communicator_t *comm,
                                  struct ompi_request_t **request );
int mca_vprotocol_pessimist_recv(void *addr,
                                 size_t count,
                                 ompi_datatype_t * datatype,
                                 int src,
                                 int tag,
                                 struct ompi_communicator_t *comm,
                                 ompi_status_public_t * status );

int mca_vprotocol_pessimist_isend(const void *buf,
                                  size_t count,
                                  ompi_datatype_t* datatype,
                                  int dst,
                                  int tag,
                                  mca_pml_base_send_mode_t sendmode,
                                  ompi_communicator_t* comm,
                                  ompi_request_t** request );
int mca_vprotocol_pessimist_send(const void *buf,
                                 size_t count,
                                 ompi_datatype_t* datatype,
                                 int dst,
                                 int tag,
                                 mca_pml_base_send_mode_t sendmode,
                                 ompi_communicator_t* comm );

int mca_vprotocol_pessimist_iprobe(int src, int tag,
                                   struct ompi_communicator_t *comm,
                                   int *matched, ompi_status_public_t * status );
int mca_vprotocol_pessimist_probe(int src, int tag,
                                  struct ompi_communicator_t *comm,
                                  ompi_status_public_t * status );

END_C_DECLS

#include "vprotocol_pessimist_wait.h"
#include "vprotocol_pessimist_start.h"

#include "vprotocol_pessimist_request.h"
#include "vprotocol_pessimist_sender_based.h"
#include "vprotocol_pessimist_eventlog.h"

#endif /* __INCLUDE_VPROTOCOL_PESSIMIST_H__ */
