/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2015      Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "osc_pt2pt.h"
#include "osc_pt2pt_sync.h"

static void ompi_osc_pt2pt_sync_constructor (ompi_osc_pt2pt_sync_t *sync)
{
    sync->type = OMPI_OSC_PT2PT_SYNC_TYPE_NONE;
    sync->eager_send_active = false;
    sync->epoch_active = false;
    OBJ_CONSTRUCT(&sync->lock, opal_mutex_t);
    OBJ_CONSTRUCT(&sync->cond, opal_condition_t);
}

static void ompi_osc_pt2pt_sync_destructor (ompi_osc_pt2pt_sync_t *sync)
{
    OBJ_DESTRUCT(&sync->lock);
    OBJ_DESTRUCT(&sync->cond);
}

OBJ_CLASS_INSTANCE(ompi_osc_pt2pt_sync_t, opal_free_list_item_t,
                   ompi_osc_pt2pt_sync_constructor,
                   ompi_osc_pt2pt_sync_destructor);

ompi_osc_pt2pt_sync_t *ompi_osc_pt2pt_sync_allocate (struct ompi_osc_pt2pt_module_t *module)
{
    ompi_osc_pt2pt_sync_t *sync;

    /* module is not used yet */
    (void) module;

    sync = OBJ_NEW (ompi_osc_pt2pt_sync_t);
    if (OPAL_UNLIKELY(NULL == sync)) {
        return NULL;
    }

    sync->module = module;
    return sync;
}

void ompi_osc_pt2pt_sync_return (ompi_osc_pt2pt_sync_t *sync)
{
    OBJ_RELEASE(sync);
}

static inline bool ompi_osc_pt2pt_sync_array_peer (int rank, ompi_osc_pt2pt_peer_t **peers, size_t nranks,
                                                   struct ompi_osc_pt2pt_peer_t **peer)
{
    int mid = nranks / 2;

    /* base cases */
    if (0 == nranks || (1 == nranks && peers[0]->rank != rank)) {
        if (peer) {
            *peer = NULL;
        }
        return false;
    } else if (peers[0]->rank == rank) {
        if (peer) {
            *peer = peers[0];
        }
        return true;
    }

    if (peers[mid]->rank > rank) {
        return ompi_osc_pt2pt_sync_array_peer (rank, peers, mid, peer);
    }

    return ompi_osc_pt2pt_sync_array_peer (rank, peers + mid, nranks - mid, peer);
}

bool ompi_osc_pt2pt_sync_pscw_peer (ompi_osc_pt2pt_module_t *module, int target, struct ompi_osc_pt2pt_peer_t **peer)
{
    ompi_osc_pt2pt_sync_t *pt2pt_sync = &module->all_sync;

    /* check synchronization type */
    if (OMPI_OSC_PT2PT_SYNC_TYPE_PSCW != pt2pt_sync->type) {
        if (peer) {
            *peer = NULL;
        }
        return false;
    }

    return ompi_osc_pt2pt_sync_array_peer (target, pt2pt_sync->peer_list.peers, pt2pt_sync->num_peers, peer);
}
