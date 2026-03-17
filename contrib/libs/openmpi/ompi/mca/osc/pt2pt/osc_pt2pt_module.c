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
 * Copyright (c) 2007-2016 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2012-2013 Sandia National Laboratories.  All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2017      The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "osc_pt2pt.h"


int ompi_osc_pt2pt_attach(struct ompi_win_t *win, void *base, size_t len)
{
    return OMPI_SUCCESS;
}


int
ompi_osc_pt2pt_detach(struct ompi_win_t *win, const void *base)
{
    return OMPI_SUCCESS;
}


int ompi_osc_pt2pt_free(ompi_win_t *win)
{
    int ret = OMPI_SUCCESS;
    ompi_osc_pt2pt_module_t *module = GET_MODULE(win);
    ompi_osc_pt2pt_peer_t *peer;
    uint32_t key;
    void *node;

    if (NULL == module) {
        return OMPI_SUCCESS;
    }

    if (NULL != module->comm) {
        opal_output_verbose(1, ompi_osc_base_framework.framework_output,
                            "pt2pt component destroying window with id %d",
                            ompi_comm_get_cid(module->comm));

        /* finish with a barrier */
        if (ompi_group_size(win->w_group) > 1) {
            (void) module->comm->c_coll->coll_barrier (module->comm,
                                                      module->comm->c_coll->coll_barrier_module);
        }

        /* remove from component information */
        OPAL_THREAD_SCOPED_LOCK(&mca_osc_pt2pt_component.lock,
                                opal_hash_table_remove_value_uint32(&mca_osc_pt2pt_component.modules,
                                                                    ompi_comm_get_cid(module->comm)));
    }

    win->w_osc_module = NULL;

    OBJ_DESTRUCT(&module->outstanding_locks);
    OBJ_DESTRUCT(&module->locks_pending);
    OBJ_DESTRUCT(&module->locks_pending_lock);
    OBJ_DESTRUCT(&module->cond);
    OBJ_DESTRUCT(&module->lock);
    OBJ_DESTRUCT(&module->all_sync);

    /* it is erroneous to close a window with active operations on it so we should
     * probably produce an error here instead of cleaning up */
    OPAL_LIST_DESTRUCT(&module->pending_acc);
    OBJ_DESTRUCT(&module->pending_acc_lock);

    osc_pt2pt_gc_clean (module);
    OPAL_LIST_DESTRUCT(&module->buffer_gc);
    OBJ_DESTRUCT(&module->gc_lock);

    ret = opal_hash_table_get_first_key_uint32 (&module->peer_hash, &key, (void **) &peer, &node);
    while (OPAL_SUCCESS == ret) {
        OBJ_RELEASE(peer);
        ret = opal_hash_table_get_next_key_uint32 (&module->peer_hash, &key, (void **) &peer, node,
                                                   &node);
    }

    OBJ_DESTRUCT(&module->peer_hash);
    OBJ_DESTRUCT(&module->peer_lock);

    if (NULL != module->recv_frags) {
        for (unsigned int i = 0 ; i < module->recv_frag_count ; ++i) {
            OBJ_DESTRUCT(module->recv_frags + i);
        }

        free (module->recv_frags);
    }

    if (NULL != module->epoch_outgoing_frag_count) free(module->epoch_outgoing_frag_count);

    if (NULL != module->comm) {
        ompi_comm_free(&module->comm);
    }

    if (NULL != module->free_after) free(module->free_after);

    free (module);

    return OMPI_SUCCESS;
}
