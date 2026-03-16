/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University.
 *                         All rights reserved.
 * Copyright (c) 2004-2005 The Trustees of the University of Tennessee.
 *                         All rights reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2012      Sandia National Laboratories.  All rights reserved.
 * Copyright (c) 2014      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef OMPI_MCA_OSC_PT2PT_DATA_MOVE_H
#define OMPI_MCA_OSC_PT2PT_DATA_MOVE_H

#include "osc_pt2pt_header.h"

int ompi_osc_pt2pt_control_send(ompi_osc_pt2pt_module_t *module,
                               int target,
                               void *data,
                               size_t len);

/**
 * ompi_osc_pt2pt_control_send_unbuffered:
 *
 * @short Send an unbuffered control message to a peer.
 *
 * @param[in] module - OSC PT2PT module
 * @param[in] target - Target rank
 * @param[in] data   - Data to send
 * @param[in] len    - Length of data
 *
 * @long Directly send a control message.  This does not allocate a
 * fragment, so should only be used when sending other messages would
 * be erroneous (such as complete messages, when there may be queued
 * transactions from an overlapping post that has already heard back
 * from its peer). The buffer specified by data will be available
 * when this call returns.
 */
int ompi_osc_pt2pt_control_send_unbuffered (ompi_osc_pt2pt_module_t *module,
                                           int target, void *data, size_t len);

/**
 * ompi_osc_pt2pt_isend_w_cb:
 *
 * @short Post a non-blocking send with a specified callback.
 *
 * @param[in] ptr         - Source buffer. Will be available when the callback fires
 * @param[in] count       - Number of elements to send
 * @param[in] datatype    - Datatype of elements
 * @param[in] source      - Ranks to send data to
 * @param[in] tag         - Tag to use
 * @param[in] comm        - Communicator for communicating with rank
 * @param[in] cb          - Function to call when the request is complete
 * @param[in] ctx         - Context to store in new request for callback
 *
 * @long This function posts a new send request. Upon completion the function cb will
 * be called with the associated request. The context specified in ctx will be stored in
 * the req_completion_cb_data member of the ompi_request_t for use by the callback.
 */
int ompi_osc_pt2pt_isend_w_cb (const void *ptr, int count, ompi_datatype_t *datatype, int target, int tag,
                              ompi_communicator_t *comm, ompi_request_complete_fn_t cb, void *ctx);

/**
 * ompi_osc_pt2pt_irecv_w_cb:
 *
 * @short Post a non-blocking receive with a specified callback.
 *
 * @param[inout] ptr         - Destination for incoming data
 * @param[in]    count       - Number of elements to receive
 * @param[in]    datatype    - Datatype of elements
 * @param[in]    source      - Ranks to receive data from
 * @param[in]    tag         - Tag to use
 * @param[in]    comm        - Communicator for communicating with rank
 * @param[in]    request_out - Location to store new receive request (may be NULL)
 * @param[in]    cb          - Function to call when the request is complete
 * @param[in]    ctx         - Context to store in new request for callback
 *
 * @long This function posts a new request and stores the request in request_out if
 * provided. Upon completion the function cb will be called with the associated
 * request. The context specified in ctx will be stored in the req_completion_cb_data
 * member of the ompi_request_t for use by the callback.
 */
int ompi_osc_pt2pt_irecv_w_cb (void *ptr, int count, ompi_datatype_t *datatype, int source, int tag,
                              ompi_communicator_t *comm, ompi_request_t **request_out,
                              ompi_request_complete_fn_t cb, void *ctx);

int ompi_osc_pt2pt_process_lock(ompi_osc_pt2pt_module_t* module,
                               int source,
                               struct ompi_osc_pt2pt_header_lock_t* lock_header);

void ompi_osc_pt2pt_process_lock_ack(ompi_osc_pt2pt_module_t* module,
                                    struct ompi_osc_pt2pt_header_lock_ack_t* lock_header);

int ompi_osc_pt2pt_process_unlock(ompi_osc_pt2pt_module_t* module,
                                 int source,
                                 struct ompi_osc_pt2pt_header_unlock_t* lock_header);
int ompi_osc_pt2pt_process_flush (ompi_osc_pt2pt_module_t *module, int source,
                                 ompi_osc_pt2pt_header_flush_t *flush_header);

/**
 * ompi_osc_pt2pt_process_unlock_ack:
 *
 * @short Process an incoming unlock acknowledgement.
 *
 * @param[in] module            - OSC PT2PT module
 * @param[in] source            - Source rank
 * @param[in] unlock_ack_header - Incoming unlock ack header
 */
void ompi_osc_pt2pt_process_unlock_ack (ompi_osc_pt2pt_module_t *module, int source,
				       ompi_osc_pt2pt_header_unlock_ack_t *unlock_ack_header);

/**
 * ompi_osc_pt2pt_process_flush_ack:
 *
 * @short Process an incoming flush acknowledgement.
 *
 * @param[in] module           - OSC PT2PT module
 * @param[in] source           - Source rank
 * @param[in] flush_ack_header - Incoming flush ack header
 */
void ompi_osc_pt2pt_process_flush_ack (ompi_osc_pt2pt_module_t *module, int source,
				      ompi_osc_pt2pt_header_flush_ack_t *flush_ack_header);

/**
 * ompi_osc_pt2pt_frag_start_receive:
 *
 * @short Start receiving fragments on the OSC module.
 *
 * @param[in] module   - OSC module
 *
 * @long This function starts receiving eager fragments on the module. The current
 *       implementation uses the pml to transfer eager fragments.
 */
int ompi_osc_pt2pt_frag_start_receive (ompi_osc_pt2pt_module_t *module);

/**
 * ompi_osc_pt2pt_process_receive:
 *
 * @short Report a receive request
 *
 * @param[in] recv     - Receive structure
 *
 * @long This function reposts a receive request. This function should not be called from
 *       a pml request callback as it can lead to deep recursion during heavy load.
 */
int ompi_osc_pt2pt_process_receive (ompi_osc_pt2pt_receive_t *recv);

#endif
