/*
 * Copyright (c) 2004-2011 The Trustees of the University of Tennessee.
 *                         All rights reserved.
 * Copyright (c) 2012      Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2015      Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"
#include "vprotocol_pessimist_eventlog.h"
#include "opal/mca/pmix/pmix.h"
#include "ompi/dpm/dpm.h"

int vprotocol_pessimist_event_logger_connect(int el_rank, ompi_communicator_t **el_comm)
{
    int rc;
    char *port;
    int rank;
    vprotocol_pessimist_clock_t connect_info[2];
    opal_list_t results;
    opal_pmix_pdata_t *pdat;

    OBJ_CONSTRUCT(&results, opal_list_t);
    pdat = OBJ_NEW(opal_pmix_pdata_t);
    asprintf(&pdat->value.key, VPROTOCOL_EVENT_LOGGER_NAME_FMT, el_rank);
    opal_list_append(&results, &pdat->super);

    rc = opal_pmix.lookup(&results, NULL);
    if (OPAL_SUCCESS != rc ||
        OPAL_STRING != pdat->value.type ||
        NULL == pdat->value.data.string) {
        OPAL_LIST_DESTRUCT(&results);
        return OMPI_ERR_NOT_FOUND;
    }
    port = strdup(pdat->value.data.string);
    OPAL_LIST_DESTRUCT(&results);
    V_OUTPUT_VERBOSE(45, "Found port < %s >", port);

    rc = ompi_dpm_connect_accept(MPI_COMM_SELF, 0, port, true, el_comm);
    if(OMPI_SUCCESS != rc) {
        OMPI_ERROR_LOG(rc);
    }

    /* Send Rank, receive max buffer size and max_clock back */
    rank = ompi_comm_rank(&ompi_mpi_comm_world.comm);
    rc = mca_pml_v.host_pml.pml_send(&rank, 1, MPI_INTEGER, 0,
                                     VPROTOCOL_PESSIMIST_EVENTLOG_NEW_CLIENT_CMD,
                                     MCA_PML_BASE_SEND_STANDARD,
                                     mca_vprotocol_pessimist.el_comm);
    if(OPAL_UNLIKELY(MPI_SUCCESS != rc))
        OMPI_ERRHANDLER_INVOKE(mca_vprotocol_pessimist.el_comm, rc,
                               __FILE__ ": failed sending event logger handshake");
    rc = mca_pml_v.host_pml.pml_recv(&connect_info, 2, MPI_UNSIGNED_LONG_LONG,
                                     0, VPROTOCOL_PESSIMIST_EVENTLOG_NEW_CLIENT_CMD,
                                     mca_vprotocol_pessimist.el_comm, MPI_STATUS_IGNORE);
    if(OPAL_UNLIKELY(MPI_SUCCESS != rc))                                  \
        OMPI_ERRHANDLER_INVOKE(mca_vprotocol_pessimist.el_comm, rc,       \
                               __FILE__ ": failed receiving event logger handshake");

    return rc;
}

int vprotocol_pessimist_event_logger_disconnect(ompi_communicator_t *el_comm)
{
    ompi_dpm_disconnect(el_comm);
    return OMPI_SUCCESS;
}

void vprotocol_pessimist_matching_replay(int *src) {
#if OPAL_ENABLE_DEBUG
    vprotocol_pessimist_clock_t max = 0;
#endif
    mca_vprotocol_pessimist_event_t *event;

    /* searching this request in the event list */
    for(event = (mca_vprotocol_pessimist_event_t *) opal_list_get_first(&mca_vprotocol_pessimist.replay_events);
        event != (mca_vprotocol_pessimist_event_t *) opal_list_get_end(&mca_vprotocol_pessimist.replay_events);
        event = (mca_vprotocol_pessimist_event_t *) opal_list_get_next(event))
    {
        vprotocol_pessimist_matching_event_t *mevent;

        if(VPROTOCOL_PESSIMIST_EVENT_TYPE_MATCHING != event->type) continue;
        mevent = &(event->u_event.e_matching);
        if(mevent->reqid == mca_vprotocol_pessimist.clock)
        {
            /* this is the event to replay */
            V_OUTPUT_VERBOSE(70, "pessimist: replay\tmatch\t%"PRIpclock"\trecv is forced from %d", mevent->reqid, mevent->src);
            (*src) = mevent->src;
            opal_list_remove_item(&mca_vprotocol_pessimist.replay_events,
                                  (opal_list_item_t *) event);
            VPESSIMIST_EVENT_RETURN(event);
        }
#if OPAL_ENABLE_DEBUG
        else if(mevent->reqid > max)
            max = mevent->reqid;
    }
    /* not forcing a ANY SOURCE event whose recieve clock is lower than max
     * is a bug indicating we have missed an event during logging ! */
    assert(((*src) != MPI_ANY_SOURCE) || (mca_vprotocol_pessimist.clock > max));
#else
    }
#endif
}

void vprotocol_pessimist_delivery_replay(size_t n, ompi_request_t **reqs,
                                         int *outcount, int *index,
                                         ompi_status_public_t *status) {
    mca_vprotocol_pessimist_event_t *event;

    for(event = (mca_vprotocol_pessimist_event_t *) opal_list_get_first(&mca_vprotocol_pessimist.replay_events);
        event != (mca_vprotocol_pessimist_event_t *) opal_list_get_end(&mca_vprotocol_pessimist.replay_events);
        event = (mca_vprotocol_pessimist_event_t *) opal_list_get_next(event))
    {
        vprotocol_pessimist_delivery_event_t *devent;

        if(VPROTOCOL_PESSIMIST_EVENT_TYPE_DELIVERY != event->type) continue;
        devent = &(event->u_event.e_delivery);
        if(devent->probeid < mca_vprotocol_pessimist.clock)
        {
            /* this particular test have to return no request completed yet */
            V_OUTPUT_VERBOSE(70, "pessimist:\treplay\tdeliver\t%"PRIpclock"\tnone", mca_vprotocol_pessimist.clock);
            *index = MPI_UNDEFINED;
            *outcount = 0;
            mca_vprotocol_pessimist.clock++;
            /* This request have to stay in the queue until probeid matches */
            return;
        }
        else if(devent->probeid == mca_vprotocol_pessimist.clock)
        {
            int i;
            for(i = 0; i < (int) n; i++)
            {
                if(VPESSIMIST_FTREQ(reqs[i])->reqid == devent->reqid)
                {
                    V_OUTPUT_VERBOSE(70, "pessimist:\treplay\tdeliver\t%"PRIpclock"\t%"PRIpclock, devent->probeid, devent->reqid);
                    opal_list_remove_item(&mca_vprotocol_pessimist.replay_events,
                                          (opal_list_item_t *) event);
                    VPESSIMIST_EVENT_RETURN(event);
                    *index = i;
                    *outcount = 1;
                    mca_vprotocol_pessimist.clock++;
                    ompi_request_wait(&reqs[i], status);
                    return;
                }
            }
            V_OUTPUT_VERBOSE(70, "pessimist:\treplay\tdeliver\t%"PRIpclock"\tnone", mca_vprotocol_pessimist.clock);
            assert(devent->reqid == 0); /* make sure we don't missed a request */
            *index = MPI_UNDEFINED;
            *outcount = 0;
            mca_vprotocol_pessimist.clock++;
            opal_list_remove_item(&mca_vprotocol_pessimist.replay_events,
                                  (opal_list_item_t *) event);
            VPESSIMIST_EVENT_RETURN(event);
            return;
        }
    }
    V_OUTPUT_VERBOSE(50, "pessimist:\treplay\tdeliver\t%"PRIpclock"\tnot forced", mca_vprotocol_pessimist.clock);
}
