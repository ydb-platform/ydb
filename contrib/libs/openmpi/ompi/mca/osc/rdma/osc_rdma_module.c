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
 * Copyright (c) 2007-2016 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2012-2013 Sandia National Laboratories.  All rights reserved.
 * Copyright (c) 2017      The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "osc_rdma.h"
#include "osc_rdma_lock.h"

#include "mpi.h"

int ompi_osc_module_add_peer (ompi_osc_rdma_module_t *module, ompi_osc_rdma_peer_t *peer)
{
    int ret = OMPI_SUCCESS;

    if (NULL == module->peer_array) {
        ret = opal_hash_table_set_value_uint32 (&module->peer_hash, peer->rank, (void *) peer);
    } else {
        module->peer_array[peer->rank] = peer;
    }

    return ret;
}

int ompi_osc_rdma_free(ompi_win_t *win)
{
    int ret = OMPI_SUCCESS;
    ompi_osc_rdma_module_t *module = GET_MODULE(win);
    ompi_osc_rdma_peer_t *peer;
    uint32_t key;
    void *node;

    if (NULL == module) {
        return OMPI_SUCCESS;
    }

    while (module->pending_ops) {
        ompi_osc_rdma_progress (module);
    }

    if (NULL != module->comm) {
        opal_output_verbose(1, ompi_osc_base_framework.framework_output,
                            "rdma component destroying window with id %d",
                            ompi_comm_get_cid(module->comm));

        /* finish with a barrier */
        if (ompi_group_size(win->w_group) > 1) {
            (void) module->comm->c_coll->coll_barrier (module->comm,
                                                      module->comm->c_coll->coll_barrier_module);
        }

        /* remove from component information */
        OPAL_THREAD_LOCK(&mca_osc_rdma_component.lock);
        opal_hash_table_remove_value_uint32(&mca_osc_rdma_component.modules,
                                            ompi_comm_get_cid(module->comm));
        OPAL_THREAD_UNLOCK(&mca_osc_rdma_component.lock);
    }

    win->w_osc_module = NULL;

    if (module->state) {
        int region_count = module->state->region_count & 0xffffffffL;
        if (NULL != module->dynamic_handles) {
            for (int i = 0 ; i < region_count ; ++i) {
                ompi_osc_rdma_deregister (module, module->dynamic_handles[i].btl_handle);
            }

            free (module->dynamic_handles);
        }
    }

    OBJ_DESTRUCT(&module->outstanding_locks);
    OBJ_DESTRUCT(&module->lock);
    OBJ_DESTRUCT(&module->peer_lock);
    OBJ_DESTRUCT(&module->all_sync);

    ompi_osc_rdma_deregister (module, module->state_handle);
    ompi_osc_rdma_deregister (module, module->base_handle);

    OPAL_LIST_DESTRUCT(&module->pending_posts);

    if (NULL != module->rdma_frag) {
        ompi_osc_rdma_deregister (module, module->rdma_frag->handle);
    }

    /* remove all cached peers */
    if (NULL == module->peer_array) {
        ret = opal_hash_table_get_first_key_uint32 (&module->peer_hash, &key, (void **) &peer, &node);
        while (OPAL_SUCCESS == ret) {
            OBJ_RELEASE(peer);
            ret = opal_hash_table_get_next_key_uint32 (&module->peer_hash, &key, (void **) &peer,
                                                       node, &node);
        }

        OBJ_DESTRUCT(&module->peer_hash);
    } else if (NULL != module->comm) {
        for (int i = 0 ; i < ompi_comm_size (module->comm) ; ++i) {
            if (NULL != module->peer_array[i]) {
                OBJ_RELEASE(module->peer_array[i]);
            }
        }
    }

    if (module->local_leaders && MPI_COMM_NULL != module->local_leaders) {
        ompi_comm_free (&module->local_leaders);
    }

    if (module->shared_comm && MPI_COMM_NULL != module->shared_comm) {
        ompi_comm_free (&module->shared_comm);
    }

    if (module->comm && MPI_COMM_NULL != module->comm) {
        ompi_comm_free (&module->comm);
    }

    if (module->segment_base) {
        opal_shmem_segment_detach (&module->seg_ds);
        module->segment_base = NULL;
    }

    free (module->peer_array);
    free (module->outstanding_lock_array);
    free (module->free_after);
    free (module);

    return OMPI_SUCCESS;
}
