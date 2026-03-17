/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2015-2018 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "osc_rdma.h"
#include "osc_rdma_sync.h"

static void ompi_osc_rdma_sync_constructor (ompi_osc_rdma_sync_t *rdma_sync)
{
    rdma_sync->type = OMPI_OSC_RDMA_SYNC_TYPE_NONE;
    rdma_sync->epoch_active = false;
    rdma_sync->outstanding_rdma.counter = 0;
    OBJ_CONSTRUCT(&rdma_sync->lock, opal_mutex_t);
    OBJ_CONSTRUCT(&rdma_sync->demand_locked_peers, opal_list_t);
}

static void ompi_osc_rdma_sync_destructor (ompi_osc_rdma_sync_t *rdma_sync)
{
    OBJ_DESTRUCT(&rdma_sync->lock);
    OBJ_DESTRUCT(&rdma_sync->demand_locked_peers);
}

OBJ_CLASS_INSTANCE(ompi_osc_rdma_sync_t, opal_object_t, ompi_osc_rdma_sync_constructor,
                   ompi_osc_rdma_sync_destructor);

ompi_osc_rdma_sync_t *ompi_osc_rdma_sync_allocate (struct ompi_osc_rdma_module_t *module)
{
    ompi_osc_rdma_sync_t *rdma_sync;

    rdma_sync = OBJ_NEW (ompi_osc_rdma_sync_t);
    if (OPAL_UNLIKELY(NULL == rdma_sync)) {
        return NULL;
    }

    rdma_sync->module = module;
    return rdma_sync;
}

void ompi_osc_rdma_sync_return (ompi_osc_rdma_sync_t *rdma_sync)
{
    OBJ_RELEASE(rdma_sync);
}

static inline bool ompi_osc_rdma_sync_array_peer (int rank, ompi_osc_rdma_peer_t **peers, size_t nranks,
                                                  struct ompi_osc_rdma_peer_t **peer)
{
    int mid = nranks / 2;

    /* base cases */
    if (0 == nranks || (1 == nranks && peers[0]->rank != rank)) {
        *peer = NULL;
        return false;
    } else if (peers[0]->rank == rank) {
        *peer = peers[0];
        return true;
    }

    if (peers[mid]->rank > rank) {
        return ompi_osc_rdma_sync_array_peer (rank, peers, mid, peer);
    }

    return ompi_osc_rdma_sync_array_peer (rank, peers + mid, nranks - mid, peer);
}

bool ompi_osc_rdma_sync_pscw_peer (ompi_osc_rdma_module_t *module, int target, struct ompi_osc_rdma_peer_t **peer)
{
    ompi_osc_rdma_sync_t *rdma_sync = &module->all_sync;

    /* check synchronization type */
    if (OMPI_OSC_RDMA_SYNC_TYPE_PSCW != rdma_sync->type) {
        *peer = NULL;
        return false;
    }

    return ompi_osc_rdma_sync_array_peer (target, rdma_sync->peer_list.peers, rdma_sync->num_peers, peer);
}
