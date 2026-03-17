/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University.
 *                         All rights reserved.
 * Copyright (c) 2004-2006 The Trustees of the University of Tennessee.
 *                         All rights reserved.
 * Copyright (c) 2004-2008 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007-2017 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2009-2011 Oracle and/or its affiliates.  All rights reserved.
 * Copyright (c) 2012-2013 Sandia National Laboratories.  All rights reserved.
 * Copyright (c) 2014-2015 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2016      FUJITSU LIMITED.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "osc_pt2pt.h"
#include "osc_pt2pt_header.h"
#include "osc_pt2pt_data_move.h"
#include "osc_pt2pt_frag.h"
#include "osc_pt2pt_request.h"

#include "opal/util/arch.h"
#include "opal/sys/atomic.h"
#include "opal/align.h"

#include "ompi/mca/pml/pml.h"
#include "ompi/mca/pml/base/pml_base_sendreq.h"
#include "opal/mca/btl/btl.h"
#include "ompi/mca/osc/base/osc_base_obj_convert.h"
#include "ompi/datatype/ompi_datatype.h"
#include "ompi/op/op.h"
#include "ompi/memchecker.h"

/**
 * struct osc_pt2pt_accumulate_data_t:
 *
 * @brief Data associated with an in-progress accumulation operation.
 */
struct osc_pt2pt_accumulate_data_t {
    opal_list_item_t super;
    ompi_osc_pt2pt_module_t* module;
    void *target;
    void *source;
    size_t source_len;
    ompi_proc_t *proc;
    int count;
    int peer;
    ompi_datatype_t *datatype;
    ompi_op_t *op;
    int request_count;
};
typedef struct osc_pt2pt_accumulate_data_t osc_pt2pt_accumulate_data_t;

static void osc_pt2pt_accumulate_data_constructor (osc_pt2pt_accumulate_data_t *acc_data)
{
    acc_data->source = NULL;
    acc_data->datatype = NULL;
    acc_data->op = NULL;
}

static void osc_pt2pt_accumulate_data_destructor (osc_pt2pt_accumulate_data_t *acc_data)
{
    if (acc_data->source) {
        /* the source buffer is always alloc'd */
        free (acc_data->source);
    }

    if (acc_data->datatype) {
        OMPI_DATATYPE_RELEASE(acc_data->datatype);
    }
}

OBJ_CLASS_DECLARATION(osc_pt2pt_accumulate_data_t);
OBJ_CLASS_INSTANCE(osc_pt2pt_accumulate_data_t, opal_list_item_t, osc_pt2pt_accumulate_data_constructor,
                   osc_pt2pt_accumulate_data_destructor);

/**
 * osc_pt2pt_pending_acc_t:
 *
 * @brief Keep track of accumulate and cswap operations that are
 * waiting on the accumulate lock.
 *
 *  Since accumulate operations may take several steps to
 * complete we need to lock the accumulate lock until the operation
 * is complete. While the lock is held it is possible that additional
 * accumulate operations will arrive. This structure keep track of
 * those operations.
 */
struct osc_pt2pt_pending_acc_t {
    opal_list_item_t super;
    ompi_osc_pt2pt_header_t header;
    int source;
    void *data;
    size_t data_len;
    ompi_datatype_t *datatype;
    bool active_target;
};
typedef struct osc_pt2pt_pending_acc_t osc_pt2pt_pending_acc_t;

static void osc_pt2pt_pending_acc_constructor (osc_pt2pt_pending_acc_t *pending)
{
    pending->data = NULL;
    pending->datatype = NULL;
}

static void osc_pt2pt_pending_acc_destructor (osc_pt2pt_pending_acc_t *pending)
{
    if (NULL != pending->data) {
        free (pending->data);
    }

    if (NULL != pending->datatype) {
        OMPI_DATATYPE_RELEASE(pending->datatype);
    }
}

OBJ_CLASS_DECLARATION(osc_pt2pt_pending_acc_t);
OBJ_CLASS_INSTANCE(osc_pt2pt_pending_acc_t, opal_list_item_t,
                   osc_pt2pt_pending_acc_constructor, osc_pt2pt_pending_acc_destructor);
/* end ompi_osc_pt2pt_pending_acc_t class */

/**
 * @brief Class for large datatype descriptions
 *
 * This class is used to keep track of buffers for large datatype desctiotions
 * (datatypes that do not fit in an eager fragment). The structure is designed
 * to take advantage of the small datatype description code path.
 */
struct ompi_osc_pt2pt_ddt_buffer_t {
    /** allows this class to be stored in the buffer garbage collection
     * list */
    opal_list_item_t super;

    /** OSC PT2PT module */
    ompi_osc_pt2pt_module_t *module;
    /** source of this header */
    int source;
    /** header + datatype data */
    ompi_osc_pt2pt_header_t *header;
};
typedef struct ompi_osc_pt2pt_ddt_buffer_t ompi_osc_pt2pt_ddt_buffer_t;

static void ompi_osc_pt2pt_ddt_buffer_constructor (ompi_osc_pt2pt_ddt_buffer_t *ddt_buffer)
{
    ddt_buffer->header = NULL;
}

static void ompi_osc_pt2pt_ddt_buffer_destructor (ompi_osc_pt2pt_ddt_buffer_t *ddt_buffer)
{
    if (ddt_buffer->header) {
        free (ddt_buffer->header);
        ddt_buffer->header = NULL;
    }
}

OBJ_CLASS_DECLARATION(ompi_osc_pt2pt_ddt_buffer_t);
OBJ_CLASS_INSTANCE(ompi_osc_pt2pt_ddt_buffer_t, opal_list_item_t, ompi_osc_pt2pt_ddt_buffer_constructor,
                   ompi_osc_pt2pt_ddt_buffer_destructor);
/* end ompi_osc_pt2pt_ddt_buffer_t class */

/**
 * datatype_buffer_length:
 *
 * @brief Determine the buffer size needed to hold count elements of datatype.
 *
 * @param[in] datatype  - Element type
 * @param[in] count     - Element count
 *
 * @returns buflen Buffer length needed to hold count elements of datatype
 */
static inline int datatype_buffer_length (ompi_datatype_t *datatype, int count)
{
    ompi_datatype_t *primitive_datatype = NULL;
    uint32_t primitive_count;
    size_t buflen;

    ompi_osc_base_get_primitive_type_info(datatype, &primitive_datatype, &primitive_count);
    primitive_count *= count;

    /* figure out how big a buffer we need */
    ompi_datatype_type_size(primitive_datatype, &buflen);

    return buflen * primitive_count;
}

/**
 * ompi_osc_pt2pt_control_send:
 *
 * @brief send a control message as part of a fragment
 *
 * @param[in]  module  - OSC PT2PT module
 * @param[in]  target  - Target peer's rank
 * @param[in]  data    - Data to send
 * @param[in]  len     - Length of data
 *
 * @returns error OMPI error code or OMPI_SUCCESS
 *
 *  "send" a control messages.  Adds it to the active fragment, so the
 * caller will still need to explicitly flush (either to everyone or
 * to a target) before this is sent.
 */
int ompi_osc_pt2pt_control_send (ompi_osc_pt2pt_module_t *module, int target,
                                 void *data, size_t len)
{
    ompi_osc_pt2pt_frag_t *frag;
    char *ptr;
    int ret;

    ret = ompi_osc_pt2pt_frag_alloc(module, target, len, &frag, &ptr, false, true);
    if (OPAL_LIKELY(OMPI_SUCCESS == ret)) {
        memcpy (ptr, data, len);

        ret = ompi_osc_pt2pt_frag_finish(module, frag);
    }

    return ret;
}

static int ompi_osc_pt2pt_control_send_unbuffered_cb (ompi_request_t *request)
{
    void *ctx = request->req_complete_cb_data;
    ompi_osc_pt2pt_module_t *module;

    /* get module pointer and data */
    module = *(ompi_osc_pt2pt_module_t **)ctx;

    /* mark this send as complete */
    mark_outgoing_completion (module);

    /* free the temporary buffer */
    free (ctx);

    ompi_request_free (&request);
    return 1;
}

/**
 * ompi_osc_pt2pt_control_send_unbuffered:
 *
 * @brief Send an unbuffered control message to a peer.
 *
 * @param[in] module - OSC PT2PT module
 * @param[in] target - Target rank
 * @param[in] data   - Data to send
 * @param[in] len    - Length of data
 *
 *  Directly send a control message.  This does not allocate a
 * fragment, so should only be used when sending other messages would
 * be erroneous (such as complete messages, when there may be queued
 * transactions from an overlapping post that has already heard back
 * from its peer). The buffer specified by data will be available
 * when this call returns.
 */
int ompi_osc_pt2pt_control_send_unbuffered(ompi_osc_pt2pt_module_t *module,
                                          int target, void *data, size_t len)
{
    void *ctx, *data_copy;

    OPAL_OUTPUT_VERBOSE((50, ompi_osc_base_framework.framework_output,
                         "osc pt2pt: sending unbuffered fragment to %d", target));

    /* allocate a temporary buffer for this send */
    ctx = malloc (sizeof(ompi_osc_pt2pt_module_t*) + len);
    if (OPAL_UNLIKELY(NULL == ctx)) {
        return OMPI_ERR_OUT_OF_RESOURCE;
    }

    /* increment outgoing signal count. this send is not part of a passive epoch
     * so there it would be erroneous to increment the epoch counters. */
    ompi_osc_signal_outgoing (module, MPI_PROC_NULL, 1);

    /* store module pointer and data in the buffer */
    *(ompi_osc_pt2pt_module_t**)ctx = module;
    data_copy = (ompi_osc_pt2pt_module_t**)ctx + 1;
    memcpy (data_copy, data, len);

    return ompi_osc_pt2pt_isend_w_cb (data_copy, len, MPI_BYTE, target, OSC_PT2PT_FRAG_TAG,
                                     module->comm, ompi_osc_pt2pt_control_send_unbuffered_cb, ctx);
}

/**
 * datatype_create:
 *
 * @brief Utility function that creates a new datatype from a packed
 *        description.
 *
 * @param[in]    module   - OSC PT2PT module
 * @param[in]    peer     - Peer rank
 * @param[out]   datatype - New datatype. Must be released with OBJ_RELEASE.
 * @param[out]   proc     - Optional. Proc for peer.
 * @param[inout] data     - Pointer to a pointer where the description is stored. This
 *                          pointer will be updated to the location after the packed
 *                          description.
 */
static inline int datatype_create (ompi_osc_pt2pt_module_t *module, int peer, ompi_proc_t **proc, ompi_datatype_t **datatype, void **data)
{
    ompi_datatype_t *new_datatype = NULL;
    ompi_proc_t *peer_proc;
    int ret = OMPI_SUCCESS;

    do {
        peer_proc = ompi_comm_peer_lookup(module->comm, peer);
        if (OPAL_UNLIKELY(NULL == peer_proc)) {
            OPAL_OUTPUT_VERBOSE((1, ompi_osc_base_framework.framework_output,
                                 "%d: datatype_create: could not resolve proc pointer for peer %d",
                                 ompi_comm_rank(module->comm),
                                 peer));
            ret = OMPI_ERROR;
            break;
        }

        new_datatype = ompi_osc_base_datatype_create(peer_proc, data);
        if (OPAL_UNLIKELY(NULL == new_datatype)) {
            OPAL_OUTPUT_VERBOSE((1, ompi_osc_base_framework.framework_output,
                                 "%d: datatype_create: could not resolve datatype for peer %d",
                                 ompi_comm_rank(module->comm), peer));
            ret = OMPI_ERROR;
        }
    } while (0);

    *datatype = new_datatype;
    if (proc) *proc = peer_proc;

    return ret;
}

/**
 * process_put:
 *
 * @shoer Process a put w/ data message
 *
 * @param[in] module     - OSC PT2PT module
 * @param[in] source     - Message source
 * @param[in] put_header - Message header + data
 *
 *  Process a put message and copy the message data to the specified
 * memory region. Note, this function does not handle any bounds
 * checking at the moment.
 */
static inline int process_put(ompi_osc_pt2pt_module_t* module, int source,
                              ompi_osc_pt2pt_header_put_t* put_header)
{
    char *data = (char*) (put_header + 1);
    ompi_proc_t *proc;
    struct ompi_datatype_t *datatype;
    size_t data_len;
    void *target = (unsigned char*) module->baseptr +
        ((unsigned long) put_header->displacement * module->disp_unit);
    int ret;

    OPAL_OUTPUT_VERBOSE((50, ompi_osc_base_framework.framework_output,
                         "%d: process_put: received message from %d",
                         ompi_comm_rank(module->comm),
                         source));

    ret = datatype_create (module, source, &proc, &datatype, (void **) &data);
    if (OPAL_UNLIKELY(OMPI_SUCCESS != ret)) {
        return ret;
    }

    data_len = put_header->len - ((uintptr_t) data - (uintptr_t) put_header);

    osc_pt2pt_copy_on_recv (target, data, data_len, proc, put_header->count, datatype);

    OMPI_DATATYPE_RELEASE(datatype);

    return put_header->len;
}

static inline int process_put_long(ompi_osc_pt2pt_module_t* module, int source,
                                   ompi_osc_pt2pt_header_put_t* put_header)
{
    char *data = (char*) (put_header + 1);
    struct ompi_datatype_t *datatype;
    void *target = (unsigned char*) module->baseptr +
        ((unsigned long) put_header->displacement * module->disp_unit);
    int ret;

    OPAL_OUTPUT_VERBOSE((50, ompi_osc_base_framework.framework_output,
                         "%d: process_put_long: received message from %d",
                         ompi_comm_rank(module->comm),
                         source));

    ret = datatype_create (module, source, NULL, &datatype, (void **) &data);
    if (OPAL_UNLIKELY(OMPI_SUCCESS != ret)) {
        return ret;
    }

    ret = ompi_osc_pt2pt_component_irecv (module, target,
                                         put_header->count,
                                         datatype, source,
                                         tag_to_target(put_header->tag),
                                         module->comm);
    if (OPAL_UNLIKELY(OMPI_SUCCESS != ret)) {
        OPAL_OUTPUT_VERBOSE((1, ompi_osc_base_framework.framework_output,
                             "%d: process_put_long: irecv error: %d",
                             ompi_comm_rank(module->comm),
                             ret));
        return OMPI_ERROR;
    }

    OMPI_DATATYPE_RELEASE(datatype);

    return put_header->len;
}

/**
 * osc_pt2pt_incoming_req_complete:
 *
 * @brief Completion callback for a receive associate with an access
 *        epoch.
 *
 * @param[in] request - PML request with an OSC RMDA module as the callback data.
 *
 *  This function is called when a send or recieve associated with an
 *       access epoch completes. When fired this function will increment the
 *       passive or active incoming count.
 */
static int osc_pt2pt_incoming_req_complete (ompi_request_t *request)
{
    ompi_osc_pt2pt_module_t *module = (ompi_osc_pt2pt_module_t *) request->req_complete_cb_data;
    int rank = MPI_PROC_NULL;

    if (request->req_status.MPI_TAG & 0x01) {
        rank = request->req_status.MPI_SOURCE;
    }

    mark_incoming_completion (module, rank);

    ompi_request_free (&request);
    return 1;
}

struct osc_pt2pt_get_post_send_cb_data_t {
    ompi_osc_pt2pt_module_t *module;
    int peer;
};

static int osc_pt2pt_get_post_send_cb (ompi_request_t *request)
{
    struct osc_pt2pt_get_post_send_cb_data_t *data =
        (struct osc_pt2pt_get_post_send_cb_data_t *) request->req_complete_cb_data;
    ompi_osc_pt2pt_module_t *module = data->module;
    int rank = data->peer;

    free (data);

    /* mark this as a completed "incoming" request */
    mark_incoming_completion (module, rank);

    ompi_request_free (&request);
    return 1;
}

/**
 * @brief Post a send to match the remote receive for a get operation.
 *
 * @param[in] module   - OSC PT2PT module
 * @param[in] source   - Source buffer
 * @param[in] count    - Number of elements in the source buffer
 * @param[in] datatype - Type of source elements.
 * @param[in] peer     - Remote process that has the receive posted
 * @param[in] tag      - Tag for the send
 *
 *  This function posts a send to match the receive posted as part
 *       of a get operation. When this send is complete the get is considered
 *       complete at the target (this process).
 */
static int osc_pt2pt_get_post_send (ompi_osc_pt2pt_module_t *module, void *source, int count,
                                   ompi_datatype_t *datatype, int peer, int tag)
{
    struct osc_pt2pt_get_post_send_cb_data_t *data;
    int ret;

    data = malloc (sizeof (*data));
    if (OPAL_UNLIKELY(NULL == data)) {
        return OMPI_ERR_OUT_OF_RESOURCE;
    }

    data->module = module;
    /* for incoming completion we need to know the peer (MPI_PROC_NULL if this is
     * in an active target epoch) */
    data->peer = (tag & 0x1) ? peer : MPI_PROC_NULL;

    /* data will be freed by the callback */
    ret = ompi_osc_pt2pt_isend_w_cb (source, count, datatype, peer, tag, module->comm,
                                     osc_pt2pt_get_post_send_cb, (void *) data);
    if (OPAL_UNLIKELY(OMPI_SUCCESS != ret)) {
        free (data);
    }

    return ret;
}

/**
 * process_get:
 *
 * @brief Process a get message from a remote peer
 *
 * @param[in] module     - OSC PT2PT module
 * @param[in] target     - Peer process
 * @param[in] get_header - Incoming message header
 */
static inline int process_get (ompi_osc_pt2pt_module_t* module, int target,
                               ompi_osc_pt2pt_header_get_t* get_header)
{
    char *data = (char *) (get_header + 1);
    struct ompi_datatype_t *datatype;
    void *source = (unsigned char*) module->baseptr +
        ((unsigned long) get_header->displacement * module->disp_unit);
    int ret;

    OPAL_OUTPUT_VERBOSE((50, ompi_osc_base_framework.framework_output,
                         "%d: process_get: received message from %d",
                         ompi_comm_rank(module->comm),
                         target));

    ret = datatype_create (module, target, NULL, &datatype, (void **) &data);
    if (OPAL_UNLIKELY(OMPI_SUCCESS != ret)) {
        return ret;
    }

    /* send get data */
    ret = osc_pt2pt_get_post_send (module, source, get_header->count, datatype,
                                  target, tag_to_origin(get_header->tag));

    OMPI_DATATYPE_RELEASE(datatype);

    return OMPI_SUCCESS == ret ? (int) get_header->len : ret;
}

/**
 * osc_pt2pt_accumulate_buffer:
 *
 * @brief Accumulate data into the target buffer.
 *
 * @param[in] target     - Target buffer
 * @param[in] source     - Source buffer
 * @param[in] source_len - Length of source buffer in bytes
 * @param[in] proc       - Source proc
 * @param[in] count      - Number of elements in target buffer
 * @param[in] datatype   - Type of elements in target buffer
 * @param[in] op         - Operation to be performed
 */
static inline int osc_pt2pt_accumulate_buffer (void *target, void *source, size_t source_len, ompi_proc_t *proc,
                                              int count, ompi_datatype_t *datatype, ompi_op_t *op)
{
    int ret;

    assert (NULL != target && NULL != source);

    if (op == &ompi_mpi_op_replace.op) {
        osc_pt2pt_copy_on_recv (target, source, source_len, proc, count, datatype);
        return OMPI_SUCCESS;
    }

#if OPAL_ENABLE_HETEROGENEOUS_SUPPORT
    if (proc->super.proc_arch != ompi_proc_local()->super.proc_arch) {
        ompi_datatype_t *primitive_datatype = NULL;
        uint32_t primitive_count;
        size_t buflen;
        void *buffer;

        ompi_osc_base_get_primitive_type_info(datatype, &primitive_datatype, &primitive_count);
        primitive_count *= count;

        /* figure out how big a buffer we need */
        ompi_datatype_type_size(primitive_datatype, &buflen);
        buflen *= primitive_count;

        buffer = malloc (buflen);
        if (OPAL_UNLIKELY(NULL == buffer)) {
            return OMPI_ERR_OUT_OF_RESOURCE;
        }

        osc_pt2pt_copy_on_recv (buffer, source, source_len, proc, primitive_count, primitive_datatype);

        ret = ompi_osc_base_process_op(target, buffer, source_len, datatype,
                                       count, op);

        free(buffer);
    } else
#endif

    /* copy the data from the temporary buffer into the user window */
    ret = ompi_osc_base_process_op(target, source, source_len, datatype,
                                   count, op);

    return ret;
}

/**
 * @brief Create an accumulate data object.
 *
 * @param[in]  module        - PT2PT OSC module
 * @param[in]  target        - Target for the accumulation
 * @param[in]  source        - Source of accumulate data. Must be allocated with malloc/calloc/etc
 * @param[in]  source_len    - Length of the source buffer in bytes
 * @param[in]  proc          - Source proc
 * @param[in]  count         - Number of elements to accumulate
 * @param[in]  datatype      - Datatype to accumulate
 * @oaram[in]  op            - Operator
 * @param[in]  request_count - Number of prerequisite requests
 * @param[out] acc_data_out  - New accumulation data
 *
 * This function is used to create a copy of the data needed to perform an accumulation.
 * This data should be provided to ompi_osc_pt2pt_isend_w_cb or ompi_osc_pt2pt_irecv_w_cb
 * as the ctx parameter with accumulate_cb as the cb parameter.
 */
static int osc_pt2pt_accumulate_allocate (ompi_osc_pt2pt_module_t *module, int peer, void *target, void *source, size_t source_len,
                                         ompi_proc_t *proc, int count, ompi_datatype_t *datatype, ompi_op_t *op,
                                         int request_count, osc_pt2pt_accumulate_data_t **acc_data_out)
{
    osc_pt2pt_accumulate_data_t *acc_data;

    acc_data = OBJ_NEW(osc_pt2pt_accumulate_data_t);
    if (OPAL_UNLIKELY(NULL == acc_data)) {
        return OMPI_ERR_OUT_OF_RESOURCE;
    }

    acc_data->module = module;
    acc_data->peer = peer;
    acc_data->target = target;
    acc_data->source = source;
    acc_data->source_len = source_len;
    acc_data->proc = proc;
    acc_data->count = count;
    acc_data->datatype = datatype;
    OMPI_DATATYPE_RETAIN(datatype);
    acc_data->op = op;
    acc_data->request_count = request_count;

    *acc_data_out = acc_data;

    return OMPI_SUCCESS;
}

/**
 * @brief Execute the accumulate once the request counter reaches 0.
 *
 * @param[in] request      - request
 *
 * The request should be created with ompi_osc_pt2pt_isend_w_cb or ompi_osc_pt2pt_irecv_w_cb
 * with ctx allocated by osc_pt2pt_accumulate_allocate. This callback will free the accumulate
 * data once the accumulation operation is complete.
 */
static int accumulate_cb (ompi_request_t *request)
{
    struct osc_pt2pt_accumulate_data_t *acc_data = (struct osc_pt2pt_accumulate_data_t *) request->req_complete_cb_data;
    ompi_osc_pt2pt_module_t *module = acc_data->module;
    int rank = MPI_PROC_NULL;
    int ret = OMPI_SUCCESS;

    OPAL_OUTPUT_VERBOSE((50, ompi_osc_base_framework.framework_output,
                         "accumulate_cb, request_count = %d", acc_data->request_count));

    if (request->req_status.MPI_TAG & 0x01) {
        rank = acc_data->peer;
    }

    if (0 == OPAL_THREAD_ADD_FETCH32(&acc_data->request_count, -1)) {
        /* no more requests needed before the buffer can be accumulated */

        if (acc_data->source) {
            ompi_datatype_t *primitive_datatype = NULL;
            uint32_t primitive_count;

            assert (NULL != acc_data->target && NULL != acc_data->source);

            ompi_osc_base_get_primitive_type_info(acc_data->datatype, &primitive_datatype, &primitive_count);
            primitive_count *= acc_data->count;

            if (acc_data->op == &ompi_mpi_op_replace.op) {
                ret = ompi_datatype_sndrcv(acc_data->source, primitive_count, primitive_datatype, acc_data->target, acc_data->count, acc_data->datatype);
            } else {
                ret = ompi_osc_base_process_op(acc_data->target, acc_data->source, acc_data->source_len, acc_data->datatype, acc_data->count, acc_data->op);
            }
        }

        /* drop the accumulate lock */
        ompi_osc_pt2pt_accumulate_unlock (module);

        osc_pt2pt_gc_add_buffer (module, &acc_data->super);
    }

    mark_incoming_completion (module, rank);

    ompi_request_free (&request);
    return ret;
}


static int ompi_osc_pt2pt_acc_op_queue (ompi_osc_pt2pt_module_t *module, ompi_osc_pt2pt_header_t *header, int source,
                                        char *data, size_t data_len, ompi_datatype_t *datatype, bool active_target)
{
    ompi_osc_pt2pt_peer_t *peer = ompi_osc_pt2pt_peer_lookup (module, source);
    osc_pt2pt_pending_acc_t *pending_acc;

    OPAL_OUTPUT_VERBOSE((50, ompi_osc_base_framework.framework_output,
                         "%d: queuing accumulate operation", ompi_comm_size (module->comm)));

    pending_acc = OBJ_NEW(osc_pt2pt_pending_acc_t);
    if (OPAL_UNLIKELY(NULL == pending_acc)) {
        return OMPI_ERR_OUT_OF_RESOURCE;
    }

    /* NTH: ensure we don't leave wait/process_flush/etc until this
     * accumulate operation is complete. */
    if (active_target) {
        OPAL_THREAD_ADD_FETCH32(&module->active_incoming_frag_count, -1);
    } else {
        OPAL_THREAD_ADD_FETCH32(&peer->passive_incoming_frag_count, -1);
    }

    pending_acc->active_target = active_target;
    pending_acc->source = source;

    /* save any inline data (eager acc, gacc only) */
    pending_acc->data_len = data_len;

    if (data_len) {
        pending_acc->data = malloc (data_len);
        memcpy (pending_acc->data, data, data_len);
    }

    /* save the datatype */
    pending_acc->datatype = datatype;
    OMPI_DATATYPE_RETAIN(datatype);

    /* save the header */
    switch (header->base.type) {
    case OMPI_OSC_PT2PT_HDR_TYPE_ACC:
    case OMPI_OSC_PT2PT_HDR_TYPE_ACC_LONG:
    case OMPI_OSC_PT2PT_HDR_TYPE_GET_ACC:
    case OMPI_OSC_PT2PT_HDR_TYPE_GET_ACC_LONG:
        pending_acc->header.acc = header->acc;
        break;
    case OMPI_OSC_PT2PT_HDR_TYPE_CSWAP:
        pending_acc->header.cswap = header->cswap;
        break;
    default:
        /* it is a coding error if any other header types are queued this way */
        assert (0);
    }

    /* add to the pending acc queue */
    OPAL_THREAD_SCOPED_LOCK(&module->pending_acc_lock, opal_list_append (&module->pending_acc, &pending_acc->super));

    return OMPI_SUCCESS;
}

static int replace_cb (ompi_request_t *request)
{
    ompi_osc_pt2pt_module_t *module = (ompi_osc_pt2pt_module_t *) request->req_complete_cb_data;
    int rank = MPI_PROC_NULL;

    if (request->req_status.MPI_TAG & 0x01) {
        rank = request->req_status.MPI_SOURCE;
    }

    mark_incoming_completion (module, rank);

    /* unlock the accumulate lock */
    ompi_osc_pt2pt_accumulate_unlock (module);

    ompi_request_free (&request);
    return 1;
}

/**
 * ompi_osc_pt2pt_acc_start:
 *
 * @brief Start an accumulate with data operation.
 *
 * @param[in] module     - OSC PT2PT module
 * @param[in] source     - Source rank
 * @param[in] data       - Accumulate data
 * @param[in] data_len   - Length of the accumulate data
 * @param[in] datatype   - Accumulation datatype
 * @param[in] acc_header - Accumulate header
 *
 * The module's accumulation lock must be held before calling this
 * function. It will release the lock when the operation is complete.
 */
static int ompi_osc_pt2pt_acc_start (ompi_osc_pt2pt_module_t *module, int source, void *data, size_t data_len,
                                    ompi_datatype_t *datatype, ompi_osc_pt2pt_header_acc_t *acc_header)
{
    void *target = (unsigned char*) module->baseptr +
        ((unsigned long) acc_header->displacement * module->disp_unit);
    struct ompi_op_t *op = ompi_osc_base_op_create(acc_header->op);
    ompi_proc_t *proc;
    int ret;

    proc = ompi_comm_peer_lookup(module->comm, source);
    assert (NULL != proc);

    ret = osc_pt2pt_accumulate_buffer (target, data, data_len, proc, acc_header->count,
                                      datatype, op);

    ompi_osc_pt2pt_accumulate_unlock (module);

    return ret;
}

/**
 * ompi_osc_pt2pt_acc_start:
 *
 * @brief Start a long accumulate operation.
 *
 * @param[in] module     - OSC PT2PT module
 * @param[in] source     - Source rank
 * @param[in] datatype   - Accumulation datatype
 * @param[in] acc_header - Accumulate header
 *
 * The module's accumulation lock must be held before calling this
 * function. It will release the lock when the operation is complete.
 */
static int ompi_osc_pt2pt_acc_long_start (ompi_osc_pt2pt_module_t *module, int source, ompi_datatype_t *datatype,
                                         ompi_osc_pt2pt_header_acc_t *acc_header) {
    struct osc_pt2pt_accumulate_data_t *acc_data;
    size_t buflen;
    void *buffer;
    ompi_proc_t *proc;
    void *target = (unsigned char*) module->baseptr +
        ((unsigned long) acc_header->displacement * module->disp_unit);
    struct ompi_op_t *op = ompi_osc_base_op_create(acc_header->op);
    ompi_datatype_t *primitive_datatype;
    uint32_t primitive_count;
    int ret;

    OPAL_OUTPUT_VERBOSE((50, ompi_osc_base_framework.framework_output,
                         "ompi_osc_pt2pt_acc_long_start starting..."));

    proc = ompi_comm_peer_lookup(module->comm, source);
    assert (NULL != proc);

    do {
        if (op == &ompi_mpi_op_replace.op) {
            ret = ompi_osc_pt2pt_irecv_w_cb (target, acc_header->count, datatype,
                                            source, tag_to_target(acc_header->tag), module->comm,
                                            NULL, replace_cb, module);
            break;
        }

        ret = ompi_osc_base_get_primitive_type_info (datatype, &primitive_datatype, &primitive_count);
        if (OPAL_UNLIKELY(OMPI_SUCCESS != ret)) {
            break;
        }

        primitive_count *= acc_header->count;

        buflen = datatype_buffer_length (datatype, acc_header->count);

        /* allocate a temporary buffer to receive the accumulate data */
        buffer = malloc (buflen);
        if (OPAL_UNLIKELY(NULL == buffer)) {
            ret = OMPI_ERR_OUT_OF_RESOURCE;
            break;
        }

        ret = osc_pt2pt_accumulate_allocate (module, source, target, buffer, buflen, proc, acc_header->count,
                                             datatype, op, 1, &acc_data);
        if (OPAL_UNLIKELY(OMPI_SUCCESS != ret)) {
            free (buffer);
            break;
        }

        ret = ompi_osc_pt2pt_irecv_w_cb (buffer, primitive_count, primitive_datatype,
                                        source, tag_to_target(acc_header->tag), module->comm,
                                        NULL, accumulate_cb, acc_data);
        if (OPAL_UNLIKELY(OMPI_SUCCESS != ret)) {
            OBJ_RELEASE(acc_data);
        }
    } while (0);

    if (OPAL_UNLIKELY(OMPI_SUCCESS != ret)) {
        ompi_osc_pt2pt_accumulate_unlock (module);
    }

    return ret;
}

/**
 * ompi_osc_pt2pt_gacc_start:
 *
 * @brief Start a accumulate with data + get operation.
 *
 * @param[in] module         - OSC PT2PT module
 * @param[in] source         - Source rank
 * @param[in] data           - Accumulate data. Must be allocated on the heap.
 * @param[in] data_len       - Length of the accumulate data
 * @param[in] datatype       - Accumulation datatype
 * @param[in] get_acc_header - Accumulate header
 *
 * The module's accumulation lock must be held before calling this
 * function. It will release the lock when the operation is complete.
 */
static int ompi_osc_pt2pt_gacc_start (ompi_osc_pt2pt_module_t *module, int source, void *data, size_t data_len,
                                     ompi_datatype_t *datatype, ompi_osc_pt2pt_header_acc_t *acc_header)
{
    void *target = (unsigned char*) module->baseptr +
        ((unsigned long) acc_header->displacement * module->disp_unit);
    struct ompi_op_t *op = ompi_osc_base_op_create(acc_header->op);
    struct osc_pt2pt_accumulate_data_t *acc_data;
    ompi_proc_t *proc;
    int ret;

    proc = ompi_comm_peer_lookup(module->comm, source);
    assert (NULL != proc);

    do {
        ret = osc_pt2pt_accumulate_allocate (module, source, target, data, data_len, proc, acc_header->count,
                                            datatype, op, 1, &acc_data);
        if (OPAL_UNLIKELY(OMPI_SUCCESS != ret)) {
            break;
        }

        ret = ompi_osc_pt2pt_isend_w_cb (target, acc_header->count, datatype,
                                        source, tag_to_origin(acc_header->tag), module->comm,
                                        accumulate_cb, acc_data);
        if (OPAL_UNLIKELY(OMPI_SUCCESS != ret)) {
            OBJ_RELEASE(acc_data);
        }
    } while (0);

    if (OPAL_UNLIKELY(OMPI_SUCCESS != ret)) {
        ompi_osc_pt2pt_accumulate_unlock (module);
    }

    return ret;
}

/**
 * ompi_osc_pt2pt_gacc_long_start:
 *
 * @brief Start a long accumulate + get operation.
 *
 * @param[in] module         - OSC PT2PT module
 * @param[in] source         - Source rank
 * @param[in] datatype       - Accumulation datatype
 * @param[in] acc_header     - Accumulate header
 *
 * The module's accumulation lock must be held before calling this
 * function. It will release the lock when the operation is complete.
 */
static int ompi_osc_gacc_long_start (ompi_osc_pt2pt_module_t *module, int source, ompi_datatype_t *datatype,
                                     ompi_osc_pt2pt_header_acc_t *acc_header)
{
    void *target = (unsigned char*) module->baseptr +
        ((unsigned long) acc_header->displacement * module->disp_unit);
    struct ompi_op_t *op = ompi_osc_base_op_create(acc_header->op);
    struct osc_pt2pt_accumulate_data_t *acc_data;
    ompi_datatype_t *primitive_datatype;
    ompi_request_t *recv_request;
    uint32_t primitive_count;
    ompi_proc_t *proc;
    size_t buflen;
    void *buffer;
    int ret;

    proc = ompi_comm_peer_lookup(module->comm, source);
    assert (NULL != proc);

    /* allocate a temporary buffer to receive the accumulate data */
    buflen = datatype_buffer_length (datatype, acc_header->count);

    do {
        ret = ompi_osc_base_get_primitive_type_info (datatype, &primitive_datatype, &primitive_count);
        if (OPAL_UNLIKELY(OMPI_SUCCESS != ret)) {
            break;
        }

        primitive_count *= acc_header->count;

        buffer = malloc (buflen);
        if (OPAL_UNLIKELY(NULL == buffer)) {
            ret = OMPI_ERR_OUT_OF_RESOURCE;
            break;
        }

        ret = osc_pt2pt_accumulate_allocate (module, source, target, buffer, buflen, proc, acc_header->count,
                                             datatype, op, 2, &acc_data);
        if (OPAL_UNLIKELY(OMPI_SUCCESS != ret)) {
            free (buffer);
            break;
        }

        ret = ompi_osc_pt2pt_irecv_w_cb (buffer, acc_header->count, datatype,
                                        source, tag_to_target(acc_header->tag), module->comm,
                                        &recv_request, accumulate_cb, acc_data);
        if (OPAL_UNLIKELY(OMPI_SUCCESS != ret)) {
            OBJ_RELEASE(acc_data);
            break;
        }

        ret = ompi_osc_pt2pt_isend_w_cb (target, primitive_count, primitive_datatype,
                                        source, tag_to_origin(acc_header->tag), module->comm,
                                        accumulate_cb, acc_data);
        if (OPAL_UNLIKELY(OMPI_SUCCESS != ret)) {
            /* cancel the receive and free the accumulate data */
            ompi_request_cancel (recv_request);
            OBJ_RELEASE(acc_data);
            break;
        }
    } while (0);

    if (OPAL_UNLIKELY(OMPI_SUCCESS != ret)) {
        ompi_osc_pt2pt_accumulate_unlock (module);
    }

    return ret;
}

/**
 * ompi_osc_pt2pt_cswap_start:
 *
 * @brief Start a compare and swap operation
 *
 * @param[in] module       - OSC PT2PT module
 * @param[in] source       - Source rank
 * @param[in] data         - Compare and swap data
 * @param[in] data_len     - Length of the compare and swap data. Must be exactly
 *                           twice the size of the datatype.
 * @param[in] datatype     - Compare and swap datatype
 * @param[in] cswap_header - Compare and swap header
 *
 * The module's accumulation lock must be held before calling this
 * function. It will release the lock when the operation is complete.
 */
static int ompi_osc_pt2pt_cswap_start (ompi_osc_pt2pt_module_t *module, int source, void *data, ompi_datatype_t *datatype,
                                      ompi_osc_pt2pt_header_cswap_t *cswap_header)
{
    void *target = (unsigned char*) module->baseptr +
        ((unsigned long) cswap_header->displacement * module->disp_unit);
    void *compare_addr, *origin_addr;
    size_t datatype_size;
    ompi_proc_t *proc;
    int ret;

    proc = ompi_comm_peer_lookup(module->comm, source);
    assert (NULL != proc);

    datatype_size = datatype->super.size;

    origin_addr  = data;
    compare_addr = (void *)((uintptr_t) data + datatype_size);

    do {
        /* no reason to do a non-blocking send here */
        ret = MCA_PML_CALL(send(target, 1, datatype, source, tag_to_origin(cswap_header->tag),
                                MCA_PML_BASE_SEND_STANDARD, module->comm));
        if (OPAL_UNLIKELY(OMPI_SUCCESS != ret)) {
            break;
        }

        /* increment the incoming fragment count so it matches what is expected */
        mark_incoming_completion (module, (cswap_header->tag & 0x1) ? source : MPI_PROC_NULL);

        if (0 == memcmp (target, compare_addr, datatype_size)) {
            osc_pt2pt_copy_on_recv (target, origin_addr, datatype_size, proc, 1, datatype);
        }
    } while (0);

    ompi_osc_pt2pt_accumulate_unlock (module);

    return ret;
}

/**
 * ompi_osc_pt2pt_progress_pending_acc:
 *
 * @brief Progress one pending accumulation or compare and swap operation.
 *
 * @param[in] module   - OSC PT2PT module
 *
 * If the accumulation lock can be aquired progress one pending
 * accumulate or compare and swap operation.
 */
int ompi_osc_pt2pt_progress_pending_acc (ompi_osc_pt2pt_module_t *module)
{
    osc_pt2pt_pending_acc_t *pending_acc;
    int ret;

    /* try to aquire the lock. it will be unlocked when the accumulate or cswap
     * operation completes */
    if (ompi_osc_pt2pt_accumulate_trylock (module)) {
        return OMPI_SUCCESS;
    }

    OPAL_THREAD_LOCK(&module->pending_acc_lock);
    pending_acc = (osc_pt2pt_pending_acc_t *) opal_list_remove_first (&module->pending_acc);
    OPAL_THREAD_UNLOCK(&module->pending_acc_lock);
    if (OPAL_UNLIKELY(NULL == pending_acc)) {
        /* called without any pending accumulation operations */
        ompi_osc_pt2pt_accumulate_unlock (module);
        return OMPI_SUCCESS;
    }

    switch (pending_acc->header.base.type) {
    case OMPI_OSC_PT2PT_HDR_TYPE_ACC:
        ret = ompi_osc_pt2pt_acc_start (module, pending_acc->source, pending_acc->data, pending_acc->data_len,
                                       pending_acc->datatype, &pending_acc->header.acc);
        free (pending_acc->data);
        break;
    case OMPI_OSC_PT2PT_HDR_TYPE_ACC_LONG:
        ret = ompi_osc_pt2pt_acc_long_start (module, pending_acc->source, pending_acc->datatype,
                                            &pending_acc->header.acc);
        break;
    case OMPI_OSC_PT2PT_HDR_TYPE_GET_ACC:
        ret = ompi_osc_pt2pt_gacc_start (module, pending_acc->source, pending_acc->data,
                                        pending_acc->data_len, pending_acc->datatype,
                                        &pending_acc->header.acc);
        break;
    case OMPI_OSC_PT2PT_HDR_TYPE_GET_ACC_LONG:
        ret = ompi_osc_gacc_long_start (module, pending_acc->source, pending_acc->datatype,
                                        &pending_acc->header.acc);
        break;
    case OMPI_OSC_PT2PT_HDR_TYPE_CSWAP:
        ret = ompi_osc_pt2pt_cswap_start (module, pending_acc->source, pending_acc->data,
                                         pending_acc->datatype, &pending_acc->header.cswap);
        break;
    default:
        ret = OMPI_ERROR;
        /* it is a coding error if this point is reached */
        assert (0);
    }

    /* signal that an operation is complete */
    mark_incoming_completion (module, pending_acc->active_target ? MPI_PROC_NULL : pending_acc->source);

    pending_acc->data = NULL;
    OBJ_RELEASE(pending_acc);

    return ret;
}

static inline int process_acc (ompi_osc_pt2pt_module_t *module, int source,
                               ompi_osc_pt2pt_header_acc_t *acc_header)
{
    bool active_target = !(acc_header->tag & 0x1);
    char *data = (char *) (acc_header + 1);
    struct ompi_datatype_t *datatype;
    uint64_t data_len;
    int ret;

    OPAL_OUTPUT_VERBOSE((50, ompi_osc_base_framework.framework_output,
                         "%d: process_acc: received message from %d",
                         ompi_comm_rank(module->comm),
                         source));

    ret = datatype_create (module, source, NULL, &datatype, (void **) &data);
    if (OPAL_UNLIKELY(OMPI_SUCCESS != ret)) {
        return ret;
    }

    data_len = acc_header->len - ((char*) data - (char*) acc_header);

    /* try to aquire the accumulate lock */
    if (0 == ompi_osc_pt2pt_accumulate_trylock (module)) {
        ret = ompi_osc_pt2pt_acc_start (module, source, data, data_len, datatype,
                                       acc_header);
    } else {
        /* couldn't aquire the accumulate lock so queue up the accumulate operation */
        ret = ompi_osc_pt2pt_acc_op_queue (module, (ompi_osc_pt2pt_header_t *) acc_header,
                                           source, data, data_len, datatype, active_target);
    }

    /* Release datatype & op */
    OMPI_DATATYPE_RELEASE(datatype);

    return (OMPI_SUCCESS == ret) ? (int) acc_header->len : ret;
}

static inline int process_acc_long (ompi_osc_pt2pt_module_t* module, int source,
                                    ompi_osc_pt2pt_header_acc_t* acc_header)
{
    bool active_target = !(acc_header->tag & 0x1);
    char *data = (char *) (acc_header + 1);
    struct ompi_datatype_t *datatype;
    int ret;

    OPAL_OUTPUT_VERBOSE((50, ompi_osc_base_framework.framework_output,
                         "%d: process_acc_long: received message from %d",
                         ompi_comm_rank(module->comm),
                         source));

    ret = datatype_create (module, source, NULL, &datatype, (void **) &data);
    if (OPAL_UNLIKELY(OMPI_SUCCESS != ret)) {
        return ret;
    }

    if (0 == ompi_osc_pt2pt_accumulate_trylock (module)) {
        ret = ompi_osc_pt2pt_acc_long_start (module, source, datatype, acc_header);
    } else {
        /* queue the operation */
        ret = ompi_osc_pt2pt_acc_op_queue (module, (ompi_osc_pt2pt_header_t *) acc_header, source,
                                           NULL, 0, datatype, active_target);
    }

    /* Release datatype & op */
    OMPI_DATATYPE_RELEASE(datatype);

    return (OMPI_SUCCESS == ret) ? (int) acc_header->len : ret;
}

static inline int process_get_acc(ompi_osc_pt2pt_module_t *module, int source,
                                  ompi_osc_pt2pt_header_acc_t *acc_header)
{
    bool active_target = !(acc_header->tag & 0x1);
    char *data = (char *) (acc_header + 1);
    struct ompi_datatype_t *datatype;
    void *buffer = NULL;
    uint64_t data_len;
    ompi_proc_t * proc;
    int ret;

    OPAL_OUTPUT_VERBOSE((50, ompi_osc_base_framework.framework_output,
                         "%d: process_get_acc: received message from %d",
                         ompi_comm_rank(module->comm),
                         source));

    ret = datatype_create (module, source, &proc, &datatype, (void **) &data);
    if (OPAL_UNLIKELY(OMPI_SUCCESS != ret)) {
        return ret;
    }

    data_len = acc_header->len - ((char*) data - (char*) acc_header);

    if (0 == ompi_osc_pt2pt_accumulate_trylock (module)) {
        /* make a copy of the data since the buffer needs to be returned */
        if (data_len) {
            ompi_datatype_t *primitive_datatype = NULL;
            uint32_t primitive_count;
            buffer = malloc (data_len);
            if (OPAL_UNLIKELY(NULL == buffer)) {
                OMPI_DATATYPE_RELEASE(datatype);
                return OMPI_ERR_OUT_OF_RESOURCE;
            }

            ompi_osc_base_get_primitive_type_info(datatype, &primitive_datatype, &primitive_count);
            primitive_count *= acc_header->count;

            osc_pt2pt_copy_on_recv (buffer, data, data_len, proc, primitive_count, primitive_datatype);
        }

        ret = ompi_osc_pt2pt_gacc_start (module, source, buffer, data_len, datatype,
                                        acc_header);
    } else {
        /* queue the operation */
        ret = ompi_osc_pt2pt_acc_op_queue (module, (ompi_osc_pt2pt_header_t *) acc_header,
                                           source, data, data_len, datatype, active_target);
    }

    /* Release datatype & op */
    OMPI_DATATYPE_RELEASE(datatype);

    return (OMPI_SUCCESS == ret) ? (int) acc_header->len : ret;
}

static inline int process_get_acc_long(ompi_osc_pt2pt_module_t *module, int source,
                                       ompi_osc_pt2pt_header_acc_t *acc_header)
{
    bool active_target = !(acc_header->tag & 0x1);
    char *data = (char *) (acc_header + 1);
    struct ompi_datatype_t *datatype;
    int ret;

    OPAL_OUTPUT_VERBOSE((50, ompi_osc_base_framework.framework_output,
                         "%d: process_acc: received message from %d",
                         ompi_comm_rank(module->comm),
                         source));

    ret = datatype_create (module, source, NULL, &datatype, (void **) &data);
    if (OPAL_UNLIKELY(OMPI_SUCCESS != ret)) {
        return ret;
    }

    if (0 == ompi_osc_pt2pt_accumulate_trylock (module)) {
        ret = ompi_osc_gacc_long_start (module, source, datatype, acc_header);
    } else {
        /* queue the operation */
        ret = ompi_osc_pt2pt_acc_op_queue (module, (ompi_osc_pt2pt_header_t *) acc_header,
                                           source, NULL, 0, datatype, active_target);
    }

    /* Release datatype & op */
    OMPI_DATATYPE_RELEASE(datatype);

    return OMPI_SUCCESS == ret ? (int) acc_header->len : ret;
}


static inline int process_cswap (ompi_osc_pt2pt_module_t *module, int source,
                                 ompi_osc_pt2pt_header_cswap_t *cswap_header)
{
    bool active_target = !(cswap_header->tag & 0x1);
    char *data = (char*) (cswap_header + 1);
    struct ompi_datatype_t *datatype;
    int ret;

    OPAL_OUTPUT_VERBOSE((50, ompi_osc_base_framework.framework_output,
                         "%d: process_cswap: received message from %d",
                         ompi_comm_rank(module->comm),
                         source));

    ret = datatype_create (module, source, NULL, &datatype, (void **) &data);
    if (OPAL_UNLIKELY(OMPI_SUCCESS != ret)) {
        return ret;
    }

    if (0 == ompi_osc_pt2pt_accumulate_trylock (module)) {
        ret = ompi_osc_pt2pt_cswap_start (module, source, data, datatype, cswap_header);
    } else {
        /* queue the operation */
        ret = ompi_osc_pt2pt_acc_op_queue (module, (ompi_osc_pt2pt_header_t *) cswap_header, source,
                                           data, 2 * datatype->super.size, datatype, active_target);
    }

    /* Release datatype */
    OMPI_DATATYPE_RELEASE(datatype);

    return (OMPI_SUCCESS == ret) ? (int) cswap_header->len : ret;
}

static inline int process_complete (ompi_osc_pt2pt_module_t *module, int source,
                                    ompi_osc_pt2pt_header_complete_t *complete_header)
{
    /* the current fragment is not part of the frag_count so we need to add it here */
    osc_pt2pt_incoming_complete (module, source, complete_header->frag_count + 1);

    return sizeof (*complete_header);
}

/* flush and unlock headers cannot be processed from the request callback
 * because some btls do not provide re-entrant progress functions. these
 * fragment will be progressed by the pt2pt component's progress function */
static inline int process_flush (ompi_osc_pt2pt_module_t *module, int source,
                                 ompi_osc_pt2pt_header_flush_t *flush_header)
{
    ompi_osc_pt2pt_peer_t *peer = ompi_osc_pt2pt_peer_lookup (module, source);
    int ret;

    OPAL_OUTPUT_VERBOSE((50, ompi_osc_base_framework.framework_output,
                         "process_flush header = {.frag_count = %d}", flush_header->frag_count));

    /* increase signal count by incoming frags */
    OPAL_THREAD_ADD_FETCH32(&peer->passive_incoming_frag_count, -(int32_t) flush_header->frag_count);

    OPAL_OUTPUT_VERBOSE((50, ompi_osc_base_framework.framework_output,
                         "%d: process_flush: received message from %d. passive_incoming_frag_count = %d",
                         ompi_comm_rank(module->comm), source, peer->passive_incoming_frag_count));

    ret = ompi_osc_pt2pt_process_flush (module, source, flush_header);
    if (OMPI_SUCCESS != ret) {
        ompi_osc_pt2pt_pending_t *pending;

        pending = OBJ_NEW(ompi_osc_pt2pt_pending_t);
        pending->module = module;
        pending->source = source;
        pending->header.flush = *flush_header;

        osc_pt2pt_add_pending (pending);
    }

    /* signal incomming will increment this counter */
    OPAL_THREAD_ADD_FETCH32(&peer->passive_incoming_frag_count, -1);

    return sizeof (*flush_header);
}

static inline int process_unlock (ompi_osc_pt2pt_module_t *module, int source,
                                  ompi_osc_pt2pt_header_unlock_t *unlock_header)
{
    ompi_osc_pt2pt_peer_t *peer = ompi_osc_pt2pt_peer_lookup (module, source);
    int ret;

    OPAL_OUTPUT_VERBOSE((50, ompi_osc_base_framework.framework_output,
                         "process_unlock header = {.frag_count = %d}", unlock_header->frag_count));

    /* increase signal count by incoming frags */
    OPAL_THREAD_ADD_FETCH32(&peer->passive_incoming_frag_count, -(int32_t) unlock_header->frag_count);

    OPAL_OUTPUT_VERBOSE((25, ompi_osc_base_framework.framework_output,
                         "osc pt2pt: processing unlock request from %d. frag count = %d, processed_count = %d",
                         source, unlock_header->frag_count, (int) peer->passive_incoming_frag_count));

    ret = ompi_osc_pt2pt_process_unlock (module, source, unlock_header);
    if (OMPI_SUCCESS != ret) {
        ompi_osc_pt2pt_pending_t *pending;

        pending = OBJ_NEW(ompi_osc_pt2pt_pending_t);
        pending->module = module;
        pending->source = source;
        pending->header.unlock = *unlock_header;

        osc_pt2pt_add_pending (pending);
    }

    /* signal incoming will increment this counter */
    OPAL_THREAD_ADD_FETCH32(&peer->passive_incoming_frag_count, -1);

    return sizeof (*unlock_header);
}

static int process_large_datatype_request_cb (ompi_request_t *request)
{
    ompi_osc_pt2pt_ddt_buffer_t *ddt_buffer = (ompi_osc_pt2pt_ddt_buffer_t *) request->req_complete_cb_data;
    ompi_osc_pt2pt_module_t *module = ddt_buffer->module;
    ompi_osc_pt2pt_header_t *header = ddt_buffer->header;
    int source = ddt_buffer->source;

    /* process the request */
    switch (header->base.type) {
    case OMPI_OSC_PT2PT_HDR_TYPE_PUT_LONG:
        (void) process_put_long (module, source, &header->put);
        break;
    case OMPI_OSC_PT2PT_HDR_TYPE_GET:
        (void) process_get (module, source, &header->get);
        break;
    case OMPI_OSC_PT2PT_HDR_TYPE_ACC_LONG:
        (void) process_acc_long (module, source, &header->acc);
        break;
    case OMPI_OSC_PT2PT_HDR_TYPE_GET_ACC_LONG:
        (void) process_get_acc_long (module, source, &header->acc);
        break;
    default:
        /* developer error */
        assert (0);
        return OMPI_ERROR;
    }

    /* free the datatype buffer */
    osc_pt2pt_gc_add_buffer (module, &ddt_buffer->super);

    ompi_request_free (&request);
    return 1;
}

/**
 * @short process a request with a large datatype
 *
 * @param[in] module - OSC PT2PT module
 * @param[in] source - header source
 * @param[in] header - header to process
 *
 * It is possible to construct datatypes whos description is too large
 * to fit in an OSC PT2PT fragment. In this case the remote side posts
 * a send of the datatype description. This function posts the matching
 * receive and processes the header on completion.
 */
static int process_large_datatype_request (ompi_osc_pt2pt_module_t *module, int source, ompi_osc_pt2pt_header_t *header)
{
    ompi_osc_pt2pt_ddt_buffer_t *ddt_buffer;
    int header_len, tag, ret;
    uint64_t ddt_len;

    /* determine the header size and receive tag */
    switch (header->base.type) {
    case OMPI_OSC_PT2PT_HDR_TYPE_PUT_LONG:
        header_len = sizeof (header->put);
        tag = header->put.tag;
        break;
    case OMPI_OSC_PT2PT_HDR_TYPE_GET:
        header_len = sizeof (header->get);
        tag = header->get.tag;
        break;
    case OMPI_OSC_PT2PT_HDR_TYPE_ACC_LONG:
        header_len = sizeof (header->acc);
        tag = header->acc.tag;
        break;
    case OMPI_OSC_PT2PT_HDR_TYPE_GET_ACC_LONG:
        header_len = sizeof (header->acc);
        tag = header->acc.tag;
        break;
    default:
        /* developer error */
        opal_output (0, "Unsupported header/flag combination");
        return OMPI_ERROR;
    }

    ddt_len = *((uint64_t *)((uintptr_t) header + header_len));

    OPAL_OUTPUT_VERBOSE((25, ompi_osc_base_framework.framework_output,
                         "process_large_datatype_request: processing fragment with type %d. ddt_len %lu",
                         header->base.type, (unsigned long) ddt_len));

    ddt_buffer = OBJ_NEW(ompi_osc_pt2pt_ddt_buffer_t);
    if (OPAL_UNLIKELY(NULL == ddt_buffer)) {
        return OMPI_ERR_OUT_OF_RESOURCE;
    }

    ddt_buffer->module = module;
    ddt_buffer->source = source;

    ddt_buffer->header = malloc (ddt_len + header_len);
    if (OPAL_UNLIKELY(NULL == ddt_buffer->header)) {
        OBJ_RELEASE(ddt_buffer);
        return OMPI_ERR_OUT_OF_RESOURCE;
    }

    memcpy (ddt_buffer->header, header, header_len);

    ret = ompi_osc_pt2pt_irecv_w_cb ((void *)((uintptr_t) ddt_buffer->header + header_len),
                                    ddt_len, MPI_BYTE,
                                    source, tag_to_target(tag), module->comm,
                                    NULL, process_large_datatype_request_cb, ddt_buffer);
    if (OPAL_UNLIKELY(OMPI_SUCCESS != ret)) {
        OBJ_RELEASE(ddt_buffer);
        return ret;
    }

    return header_len + 8;
}

/*
 * Do all the data movement associated with a fragment
 */
static inline int process_frag (ompi_osc_pt2pt_module_t *module,
                                ompi_osc_pt2pt_frag_header_t *frag)
{
    ompi_osc_pt2pt_header_t *header;
    int ret;

    OPAL_OUTPUT_VERBOSE((50, ompi_osc_base_framework.framework_output,
                         "osc pt2pt: process_frag: from %d, ops %d",
                         (int) frag->source, (int) frag->num_ops));

    header = (ompi_osc_pt2pt_header_t *) (frag + 1);

    for (int i = 0 ; i < frag->num_ops ; ++i) {
        OPAL_OUTPUT_VERBOSE((50, ompi_osc_base_framework.framework_output,
                             "osc pt2pt: process_frag: type 0x%x. flag 0x%x. offset %u",
                             header->base.type, (unsigned) ((uintptr_t)header - (uintptr_t)frag),
                             header->base.flags));

        if (OPAL_LIKELY(!(header->base.flags & OMPI_OSC_PT2PT_HDR_FLAG_LARGE_DATATYPE))) {
            osc_pt2pt_ntoh(header);
            switch (header->base.type) {
            case OMPI_OSC_PT2PT_HDR_TYPE_PUT:
                ret = process_put(module, frag->source, &header->put);
                break;
            case OMPI_OSC_PT2PT_HDR_TYPE_PUT_LONG:
                ret = process_put_long(module, frag->source, &header->put);
                break;

            case OMPI_OSC_PT2PT_HDR_TYPE_ACC:
                ret = process_acc(module, frag->source, &header->acc);
                break;
            case OMPI_OSC_PT2PT_HDR_TYPE_ACC_LONG:
                ret = process_acc_long (module, frag->source, &header->acc);
                break;

            case OMPI_OSC_PT2PT_HDR_TYPE_UNLOCK_REQ:
                ret = process_unlock(module, frag->source, &header->unlock);
                break;

            case OMPI_OSC_PT2PT_HDR_TYPE_GET:
                ret = process_get (module, frag->source, &header->get);
                break;

            case OMPI_OSC_PT2PT_HDR_TYPE_CSWAP:
                ret = process_cswap (module, frag->source, &header->cswap);
                break;

            case OMPI_OSC_PT2PT_HDR_TYPE_GET_ACC:
                ret = process_get_acc (module, frag->source, &header->acc);
                break;

            case OMPI_OSC_PT2PT_HDR_TYPE_GET_ACC_LONG:
                ret = process_get_acc_long (module, frag->source, &header->acc);
                break;

            case OMPI_OSC_PT2PT_HDR_TYPE_FLUSH_REQ:
                ret = process_flush (module, frag->source, &header->flush);
                break;

            case OMPI_OSC_PT2PT_HDR_TYPE_COMPLETE:
                ret = process_complete (module, frag->source, &header->complete);
                break;

            default:
                opal_output(0, "Unsupported fragment type 0x%x\n", header->base.type);
                abort(); /* FIX ME */
            }
        } else {
            ret = process_large_datatype_request (module, frag->source, header);
        }

        if (ret <= 0) {
            opal_output(0, "Error processing fragment: %d", ret);
            abort(); /* FIX ME */
        }

        /* the next header will start on an 8-byte boundary. this is done to ensure
         * that the next header and the packed datatype is properly aligned */
        header = (ompi_osc_pt2pt_header_t *) OPAL_ALIGN(((uintptr_t) header + ret), 8, uintptr_t);
    }

    return OMPI_SUCCESS;
}

/* dispatch for callback on message completion */
static int ompi_osc_pt2pt_callback (ompi_request_t *request)
{
    ompi_osc_pt2pt_receive_t *recv = (ompi_osc_pt2pt_receive_t *) request->req_complete_cb_data;

    OPAL_OUTPUT_VERBOSE((50, ompi_osc_base_framework.framework_output, "received pt2pt fragment"));

    /* to avoid deep recursion from complet -> start -> complete -> ... we simply put this
     * request on a list and let it be processed by opal_progress(). */
    OPAL_THREAD_LOCK(&mca_osc_pt2pt_component.pending_receives_lock);
    opal_list_append (&mca_osc_pt2pt_component.pending_receives, &recv->super);
    OPAL_THREAD_UNLOCK(&mca_osc_pt2pt_component.pending_receives_lock);

    return OMPI_SUCCESS;
}

static int ompi_osc_pt2pt_receive_repost (ompi_osc_pt2pt_receive_t *recv)
{
    /* wait until the request has been marked as complete */
    ompi_request_wait_completion (recv->pml_request);

    /* ompi_request_complete clears the callback */
    recv->pml_request->req_complete_cb = ompi_osc_pt2pt_callback;
    recv->pml_request->req_complete_cb_data = (void *) recv;

    return MCA_PML_CALL(start(1, &recv->pml_request));
}

int ompi_osc_pt2pt_process_receive (ompi_osc_pt2pt_receive_t *recv)
{
    ompi_osc_pt2pt_module_t *module = (ompi_osc_pt2pt_module_t *) recv->module;
    ompi_osc_pt2pt_header_t *base_header = (ompi_osc_pt2pt_header_t *) recv->buffer;
    size_t incoming_length = recv->pml_request->req_status._ucount;
    int source = recv->pml_request->req_status.MPI_SOURCE;
    int rc __opal_attribute_unused__;

    assert(incoming_length >= sizeof(ompi_osc_pt2pt_header_base_t));
    (void)incoming_length;  // silence compiler warning

    OPAL_OUTPUT_VERBOSE((50, ompi_osc_base_framework.framework_output,
                         "received pt2pt callback for fragment. source = %d, count = %u, type = 0x%x",
                         source, (unsigned) incoming_length, base_header->base.type));

    osc_pt2pt_ntoh(base_header);
    switch (base_header->base.type) {
    case OMPI_OSC_PT2PT_HDR_TYPE_FRAG:
        process_frag(module, (ompi_osc_pt2pt_frag_header_t *) base_header);

        /* only data fragments should be included in the completion counters */
        mark_incoming_completion (module, (base_header->base.flags & OMPI_OSC_PT2PT_HDR_FLAG_PASSIVE_TARGET) ?
                                  source : MPI_PROC_NULL);
        break;
    case OMPI_OSC_PT2PT_HDR_TYPE_POST:
        osc_pt2pt_incoming_post (module, source);
        break;
    case OMPI_OSC_PT2PT_HDR_TYPE_LOCK_REQ:
        ompi_osc_pt2pt_process_lock(module, source, (ompi_osc_pt2pt_header_lock_t *) base_header);
        break;
    case OMPI_OSC_PT2PT_HDR_TYPE_LOCK_ACK:
        ompi_osc_pt2pt_process_lock_ack(module, (ompi_osc_pt2pt_header_lock_ack_t *) base_header);
        break;
    case OMPI_OSC_PT2PT_HDR_TYPE_FLUSH_ACK:
        ompi_osc_pt2pt_process_flush_ack (module, source, (ompi_osc_pt2pt_header_flush_ack_t *) base_header);
        break;
    case OMPI_OSC_PT2PT_HDR_TYPE_UNLOCK_ACK:
        ompi_osc_pt2pt_process_unlock_ack (module, source, (ompi_osc_pt2pt_header_unlock_ack_t *) base_header);
        break;
    default:
        OPAL_OUTPUT_VERBOSE((50, ompi_osc_base_framework.framework_output,
                             "received unexpected message of type %x",
                             (int) base_header->base.type));
    }

    OPAL_OUTPUT_VERBOSE((50, ompi_osc_base_framework.framework_output,
                         "finished processing incoming messages"));

    osc_pt2pt_gc_clean (module);

    rc = ompi_osc_pt2pt_receive_repost (recv);

    OPAL_OUTPUT_VERBOSE((50, ompi_osc_base_framework.framework_output,
                         "finished posting receive request. rc: %d", rc));

    return OMPI_SUCCESS;
}

int ompi_osc_pt2pt_frag_start_receive (ompi_osc_pt2pt_module_t *module)
{
    int rc;

    module->recv_frag_count = mca_osc_pt2pt_component.receive_count;
    if (0 == module->recv_frag_count) {
        module->recv_frag_count = 1;
    }

    module->recv_frags = malloc (sizeof (module->recv_frags[0]) * module->recv_frag_count);
    if (NULL == module->recv_frags) {
        return OMPI_ERR_OUT_OF_RESOURCE;
    }

    for (unsigned int i = 0 ; i < module->recv_frag_count ; ++i) {
        OBJ_CONSTRUCT(module->recv_frags + i, ompi_osc_pt2pt_receive_t);
        module->recv_frags[i].module = module;
        module->recv_frags[i].buffer = malloc (mca_osc_pt2pt_component.buffer_size + sizeof (ompi_osc_pt2pt_frag_header_t));
        if (NULL == module->recv_frags[i].buffer) {
            return OMPI_ERR_OUT_OF_RESOURCE;
        }

        rc = ompi_osc_pt2pt_irecv_w_cb (module->recv_frags[i].buffer, mca_osc_pt2pt_component.buffer_size + sizeof (ompi_osc_pt2pt_frag_header_t),
                                        MPI_BYTE, OMPI_ANY_SOURCE, OSC_PT2PT_FRAG_TAG, module->comm, &module->recv_frags[i].pml_request,
                                        ompi_osc_pt2pt_callback, module->recv_frags + i);
        if (OMPI_SUCCESS != rc) {
            return rc;
        }
    }

    return OMPI_SUCCESS;
}

int ompi_osc_pt2pt_component_irecv (ompi_osc_pt2pt_module_t *module, void *buf,
                                   size_t count, struct ompi_datatype_t *datatype,
                                   int src, int tag, struct ompi_communicator_t *comm)
{
    return ompi_osc_pt2pt_irecv_w_cb (buf, count, datatype, src, tag, comm, NULL,
                                     osc_pt2pt_incoming_req_complete, module);
}

int ompi_osc_pt2pt_isend_w_cb (const void *ptr, int count, ompi_datatype_t *datatype, int target, int tag,
                              ompi_communicator_t *comm, ompi_request_complete_fn_t cb, void *ctx)
{
    ompi_request_t *request;
    int ret;

    OPAL_OUTPUT_VERBOSE((50, ompi_osc_base_framework.framework_output,
                         "osc pt2pt: ompi_osc_pt2pt_isend_w_cb sending %d bytes to %d with tag %d",
                         count, target, tag));

    ret = MCA_PML_CALL(isend_init((void *)ptr, count, datatype, target, tag,
                                  MCA_PML_BASE_SEND_STANDARD, comm, &request));
    if (OMPI_SUCCESS != ret) {
        OPAL_OUTPUT_VERBOSE((50, ompi_osc_base_framework.framework_output,
                             "error sending fragment. ret = %d", ret));
        return ret;
    }

    request->req_complete_cb = cb;
    request->req_complete_cb_data = ctx;

    ret = MCA_PML_CALL(start(1, &request));

    return ret;
}

int ompi_osc_pt2pt_irecv_w_cb (void *ptr, int count, ompi_datatype_t *datatype, int target, int tag,
                              ompi_communicator_t *comm, ompi_request_t **request_out,
                              ompi_request_complete_fn_t cb, void *ctx)
{
    ompi_request_t *dummy;
    int ret;

    if (NULL == request_out) {
        request_out = &dummy;
    }

    OPAL_OUTPUT_VERBOSE((50, ompi_osc_base_framework.framework_output,
                         "osc pt2pt: ompi_osc_pt2pt_irecv_w_cb receiving %d bytes from %d with tag %d",
                         count, target, tag));

    ret = MCA_PML_CALL(irecv_init(ptr, count, datatype, target, tag, comm, request_out));
    if (OMPI_SUCCESS != ret) {
        OPAL_OUTPUT_VERBOSE((50, ompi_osc_base_framework.framework_output,
                             "error posting receive. ret = %d", ret));
        return ret;
    }

    (*request_out)->req_complete_cb = cb;
    (*request_out)->req_complete_cb_data = ctx;

    ret = MCA_PML_CALL(start(1, request_out));

    OPAL_OUTPUT_VERBOSE((50, ompi_osc_base_framework.framework_output,
                         "osc pt2pt: pml start returned %d", ret));

    return ret;
}
