/*
 * Copyright (c) 2004-2007 The Trustees of the University of Tennessee.
 *                         All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef __VPROTOCOL_PESSIMIST_EVENTLOG_H__
#define __VPROTOCOL_PESSIMIST_EVENTLOG_H__

#include "vprotocol_pessimist.h"
#include "vprotocol_pessimist_request.h"
#include "vprotocol_pessimist_eventlog_protocol.h"

BEGIN_C_DECLS

/** Initialize the MPI connexion with the event logger
 * @return OMPI_SUCCESS or error code
 */
int vprotocol_pessimist_event_logger_connect(int el_rank, ompi_communicator_t **el_comm);

/** Finalize the MPI connexion with the event logger
 * @return OMPI_SUCCESS or error code
 */
int vprotocol_pessimist_event_logger_disconnect(ompi_communicator_t *el_comm);

/*******************************************************************************
  * ANY_SOURCE MATCHING
  */

/** Adds a matching event for this request in the event list for any ANY_SOURCE
  * recv. This event have to be updated later by
  * VPROTOCOL_PESSIMIST_MATCHING_LOG_FINALIZE
  * req (IN/OUT): posted RECV request (mca_pml_base_request_t *)
  *   VPESSIMIST_REQ(req) is updated to keep track of the associated event
  */
static inline void vprotocol_pessimist_matching_log_prepare(ompi_request_t *req)
{
    mca_pml_base_request_t *pmlreq = (mca_pml_base_request_t *) req;
    if(MPI_ANY_SOURCE == pmlreq->req_peer)
    {
        mca_vprotocol_pessimist_event_t *event;
        VPESSIMIST_MATCHING_EVENT_NEW(event);
        event->req = pmlreq;
        VPESSIMIST_RECV_FTREQ(req)->event = event;
        opal_list_append(&mca_vprotocol_pessimist.pending_events,
                         (opal_list_item_t *) event);
    }
}

/** Updates the actual value of a matching event
  * req(IN/OUT): the matched recv request
  *   VPESSIMIST_REQ(req) is updated to remove link to event
  */
static inline void vprotocol_pessimist_matching_log_finish(ompi_request_t *req)
{
    mca_vprotocol_pessimist_request_t *ftreq = VPESSIMIST_FTREQ(req);
    if(ftreq->event)
    {
        mca_vprotocol_pessimist_event_t *event;
        vprotocol_pessimist_matching_event_t *mevent;

        V_OUTPUT_VERBOSE(70, "pessimist:\tlog\tmatch\t%"PRIpclock"\tsrc %d\tseq %"PRIpclock, ftreq->reqid, req->req_status.MPI_SOURCE, ((mca_pml_base_request_t *) req)->req_sequence);
        event = ftreq->event;
        mevent =  &(event->u_event.e_matching);
        mevent->reqid = ftreq->reqid;
        mevent->src = req->req_status.MPI_SOURCE;
        ftreq->event = NULL;
        event->req = NULL;
    }
}

#include "ompi/request/request_default.h"

/* Helper macro to actually perform the send to EL. */
#define __VPROTOCOL_PESSIMIST_SEND_BUFFER() do {                              \
    if(OPAL_UNLIKELY(mca_vprotocol_pessimist.event_buffer_length))            \
    {                                                                         \
        int rc;                                                               \
        ompi_request_t *req;                                                  \
        vprotocol_pessimist_clock_t max_clock;                                \
        if(OPAL_UNLIKELY(ompi_comm_invalid(mca_vprotocol_pessimist.el_comm))) \
        {                                                                     \
            rc = vprotocol_pessimist_event_logger_connect(0,                  \
                                        &mca_vprotocol_pessimist.el_comm);    \
            if(OMPI_SUCCESS != rc)                                            \
                OMPI_ERRHANDLER_INVOKE(mca_vprotocol_pessimist.el_comm, rc,   \
                    __FILE__ ": failed to connect to an Event Logger");       \
        }                                                                     \
        rc = mca_pml_v.host_pml.pml_irecv(&max_clock,                         \
                1, MPI_UNSIGNED_LONG_LONG, 0,                                 \
                VPROTOCOL_PESSIMIST_EVENTLOG_ACK,                             \
                mca_vprotocol_pessimist.el_comm, &req);                       \
        rc = mca_pml_v.host_pml.pml_send(mca_vprotocol_pessimist.event_buffer,\
                mca_vprotocol_pessimist.event_buffer_length *                 \
                sizeof(vprotocol_pessimist_mem_event_t), MPI_BYTE, 0,         \
                VPROTOCOL_PESSIMIST_EVENTLOG_PUT_EVENTS_CMD,                  \
                MCA_PML_BASE_SEND_STANDARD, mca_vprotocol_pessimist.el_comm); \
        if(OPAL_UNLIKELY(MPI_SUCCESS != rc))                                  \
            OMPI_ERRHANDLER_INVOKE(mca_vprotocol_pessimist.el_comm, rc,       \
                __FILE__ ": failed logging a set of recovery event");         \
        mca_vprotocol_pessimist.event_buffer_length = 0;                      \
        rc = mca_pml_v.host_request_fns.req_wait(&req, MPI_STATUS_IGNORE);    \
        if(OPAL_UNLIKELY(MPI_SUCCESS != rc))                                  \
            OMPI_ERRHANDLER_INVOKE(mca_vprotocol_pessimist.el_comm, rc,       \
                __FILE__ ": failed logging a set of recovery event");         \
    }                                                                         \
} while(0)


/* This function sends any pending event to the Event Logger. All available
 * events are merged into a single message (if small enough).
 */
static inline void vprotocol_pessimist_event_flush(void)
{
    if(OPAL_UNLIKELY(!opal_list_is_empty(&mca_vprotocol_pessimist.pending_events)))
    {
        mca_vprotocol_pessimist_event_t *event;
        mca_vprotocol_pessimist_event_t *prv_event;

        for(event =
            (mca_vprotocol_pessimist_event_t *)
            opal_list_get_first(&mca_vprotocol_pessimist.pending_events);
            event !=
            (mca_vprotocol_pessimist_event_t *)
            opal_list_get_end(&mca_vprotocol_pessimist.pending_events);
            event =
            (mca_vprotocol_pessimist_event_t *)
            opal_list_get_next(event))
        {
            if(event->u_event.e_matching.src == -1)
            {
                /* check if request have been matched and update the event */
                /* this assert make sure the negative source trick is fine  */
                assert(event->type == VPROTOCOL_PESSIMIST_EVENT_TYPE_MATCHING);
                if(event->req->req_ompi.req_status.MPI_SOURCE == -1)
                {
                    V_OUTPUT_VERBOSE(41, "pessimist:\tlog\tel\t%"PRIpclock"\tnot matched yet (%d)", event->u_event.e_matching.reqid, event->u_event.e_matching.src);
                    continue;
                }
                event->u_event.e_matching.src =
                event->req->req_ompi.req_status.MPI_SOURCE;
            }
            /* Send this event to EL */
            V_OUTPUT_VERBOSE(40, "pessimist:\tlog\tel\t%"PRIpclock"\tfrom %d\tsent to EL", event->u_event.e_matching.reqid, event->u_event.e_matching.src);
            mca_vprotocol_pessimist.event_buffer[mca_vprotocol_pessimist.event_buffer_length++] =
                event->u_event;
            if(mca_vprotocol_pessimist.event_buffer_length ==
               mca_vprotocol_pessimist.event_buffer_max_length)
                __VPROTOCOL_PESSIMIST_SEND_BUFFER();
            assert(mca_vprotocol_pessimist.event_buffer_length < mca_vprotocol_pessimist.event_buffer_max_length);
            prv_event = (mca_vprotocol_pessimist_event_t *)
                opal_list_remove_item(&mca_vprotocol_pessimist.pending_events,
                                      (opal_list_item_t *) event);
            VPESSIMIST_EVENT_RETURN(event);
            event = prv_event;
        }
    }
    __VPROTOCOL_PESSIMIST_SEND_BUFFER();
}

/** Replay matching order according to event list during recovery
 * src (IN/OUT): the requested source. If it is ANY_SOURCE it is changed to
 *               the matched source at first run.
 * comm (IN): the communicator's context id is used to know the next unique
 *            request id that will be allocated by PML
 */
#define VPROTOCOL_PESSIMIST_MATCHING_REPLAY(src) do {                         \
  if(mca_vprotocol_pessimist.replay && ((src) == MPI_ANY_SOURCE))             \
    vprotocol_pessimist_matching_replay(&(src));                              \
} while(0)
void vprotocol_pessimist_matching_replay(int *src);

/*******************************************************************************
  * WAIT/TEST-SOME/ANY & PROBES
  */

/** Store the delivered request after a non deterministic delivery
 * req (IN): the delivered request (pml_base_request_t *)
 */
static inline void vprotocol_pessimist_delivery_log(ompi_request_t *req)
{
    mca_vprotocol_pessimist_event_t *event;
    vprotocol_pessimist_delivery_event_t *devent;

    if(req == NULL)
    {
        /* No request delivered to this probe, we need to count howmany times */
        V_OUTPUT_VERBOSE(70, "pessimist:\tlog\tdeliver\t%"PRIpclock"\tnone", mca_vprotocol_pessimist.clock);
        event = (mca_vprotocol_pessimist_event_t*)
                opal_list_get_last(&mca_vprotocol_pessimist.pending_events);
        if(event->type == VPROTOCOL_PESSIMIST_EVENT_TYPE_DELIVERY &&
           event->u_event.e_delivery.reqid == 0)
        {
            /* consecutive probes not delivering anything are merged */
            event->u_event.e_delivery.probeid = mca_vprotocol_pessimist.clock++;
        }
        else
        {
            /* Previous event is not a failed probe, lets create a new
               "failed probe" event (reqid=0) then */
            VPESSIMIST_DELIVERY_EVENT_NEW(event);
            devent = &(event->u_event.e_delivery);
            devent->probeid = mca_vprotocol_pessimist.clock++;
            devent->reqid = 0;
            opal_list_append(&mca_vprotocol_pessimist.pending_events,
                             (opal_list_item_t *) event);
        }
    }
    else
    {
        /* A request have been delivered, log which one it is */
        V_OUTPUT_VERBOSE(70, "pessimist:\tlog\tdeliver\t%"PRIpclock"\treq %"PRIpclock, mca_vprotocol_pessimist.clock, VPESSIMIST_FTREQ(req)->reqid);
        VPESSIMIST_DELIVERY_EVENT_NEW(event);
        devent = &(event->u_event.e_delivery);
        devent->probeid = mca_vprotocol_pessimist.clock++;
        devent->reqid = VPESSIMIST_FTREQ(req)->reqid;
        opal_list_append(&mca_vprotocol_pessimist.pending_events,
                         (opal_list_item_t *) event);
    }
}

/** Enforces a particular request to be delivered considering the current
  * event clock
  * n (IN): the number of input requests
  * reqs (IN): the set of considered requests (pml_base_request_t *)
  * outcount (OUT): number of delivered requests
  * i (OUT): index(es) of the delivered request
  * status (OUT): status of the delivered request
  */
#define VPROTOCOL_PESSIMIST_DELIVERY_REPLAY(n, reqs, outcount, i, status) do {\
  if(mca_vprotocol_pessimist.replay)                                          \
    vprotocol_pessimist_delivery_replay(n, reqs, outcount, i, status);        \
} while(0)
void vprotocol_pessimist_delivery_replay(size_t, ompi_request_t **,
                                         int *, int *, ompi_status_public_t *);

END_C_DECLS

#endif /* __VPROTOCOL_PESSIMIST_EVENTLOG_H__ */
