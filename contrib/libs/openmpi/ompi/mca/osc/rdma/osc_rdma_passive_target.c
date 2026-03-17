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
 * Copyright (c) 2007-2018 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2010      IBM Corporation.  All rights reserved.
 * Copyright (c) 2012-2013 Sandia National Laboratories.  All rights reserved.
 * Copyright (c) 2018      Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include "osc_rdma_passive_target.h"
#include "osc_rdma_comm.h"

#include "mpi.h"


int ompi_osc_rdma_sync (struct ompi_win_t *win)
{
    ompi_osc_rdma_progress (GET_MODULE(win));
    return OMPI_SUCCESS;
}

int ompi_osc_rdma_flush (int target, struct ompi_win_t *win)
{
    ompi_osc_rdma_module_t *module = GET_MODULE(win);
    ompi_osc_rdma_sync_t *lock;
    ompi_osc_rdma_peer_t *peer;

    assert (0 <= target);

    OSC_RDMA_VERBOSE(MCA_BASE_VERBOSE_TRACE, "flush: %d, %s", target, win->w_name);

    OPAL_THREAD_LOCK(&module->lock);

    lock = ompi_osc_rdma_module_sync_lookup (module, target, &peer);
    if (OPAL_UNLIKELY(NULL == lock || OMPI_OSC_RDMA_SYNC_TYPE_LOCK != lock->type)) {
        OSC_RDMA_VERBOSE(MCA_BASE_VERBOSE_INFO, "flush: target %d is not locked in window %s",
                         target, win->w_name);
        OPAL_THREAD_UNLOCK(&module->lock);
        return OMPI_ERR_RMA_SYNC;
    }
    OPAL_THREAD_UNLOCK(&module->lock);

    /* finish all outstanding fragments */
    ompi_osc_rdma_sync_rdma_complete (lock);

    OSC_RDMA_VERBOSE(MCA_BASE_VERBOSE_TRACE, "flush on target %d complete", target);

    return OMPI_SUCCESS;
}


int ompi_osc_rdma_flush_all (struct ompi_win_t *win)
{
    ompi_osc_rdma_module_t *module = GET_MODULE(win);
    ompi_osc_rdma_sync_t *lock;
    int ret = OMPI_SUCCESS;
    uint32_t key;
    void *node;

    /* flush is only allowed from within a passive target epoch */
    if (!ompi_osc_rdma_in_passive_epoch (module)) {
        return OMPI_ERR_RMA_SYNC;
    }

    OSC_RDMA_VERBOSE(MCA_BASE_VERBOSE_TRACE, "flush_all: %s", win->w_name);

    /* globally complete all outstanding rdma requests */
    if (OMPI_OSC_RDMA_SYNC_TYPE_LOCK == module->all_sync.type) {
        ompi_osc_rdma_sync_rdma_complete (&module->all_sync);
    }

    /* flush all locks */
    ret = opal_hash_table_get_first_key_uint32 (&module->outstanding_locks, &key, (void **) &lock, &node);
    while (OPAL_SUCCESS == ret) {
        OSC_RDMA_VERBOSE(MCA_BASE_VERBOSE_DEBUG, "flushing lock %p", (void *) lock);
        ompi_osc_rdma_sync_rdma_complete (lock);
        ret = opal_hash_table_get_next_key_uint32 (&module->outstanding_locks, &key, (void **) &lock,
                                                   node, &node);
    }

    OSC_RDMA_VERBOSE(MCA_BASE_VERBOSE_TRACE, "flush_all complete");

    return OPAL_SUCCESS;
}


int ompi_osc_rdma_flush_local (int target, struct ompi_win_t *win)
{
    return ompi_osc_rdma_flush (target, win);
}


int ompi_osc_rdma_flush_local_all (struct ompi_win_t *win)
{
    return ompi_osc_rdma_flush_all (win);
}

/* locking via atomics */
static inline int ompi_osc_rdma_lock_atomic_internal (ompi_osc_rdma_module_t *module, ompi_osc_rdma_peer_t *peer,
                                                      ompi_osc_rdma_sync_t *lock)
{
    const int locking_mode = module->locking_mode;
    int ret;

    if (MPI_LOCK_EXCLUSIVE == lock->sync.lock.type) {
        do {
            OSC_RDMA_VERBOSE(MCA_BASE_VERBOSE_DEBUG, "incrementing global exclusive lock");
            if (OMPI_OSC_RDMA_LOCKING_TWO_LEVEL == locking_mode) {
                /* lock the master lock. this requires no rank has a global shared lock */
                ret = ompi_osc_rdma_lock_acquire_shared (module, module->leader, 1, offsetof (ompi_osc_rdma_state_t, global_lock),
                                                         0xffffffff00000000L);
                if (OMPI_SUCCESS != ret) {
                    ompi_osc_rdma_progress (module);
                    continue;
                }
            }

            OSC_RDMA_VERBOSE(MCA_BASE_VERBOSE_DEBUG, "acquiring exclusive lock on peer");
            ret = ompi_osc_rdma_lock_try_acquire_exclusive (module, peer,  offsetof (ompi_osc_rdma_state_t, local_lock));
            if (ret) {
                /* release the global lock */
                if (OMPI_OSC_RDMA_LOCKING_TWO_LEVEL == locking_mode) {
                    ompi_osc_rdma_lock_release_shared (module, module->leader, -1, offsetof (ompi_osc_rdma_state_t, global_lock));
                }
                ompi_osc_rdma_progress (module);
                continue;
            }

            peer->flags |= OMPI_OSC_RDMA_PEER_EXCLUSIVE;
            break;
        } while (1);
    } else {
        do {
            /* go right to the target to acquire a shared lock */
            OSC_RDMA_VERBOSE(MCA_BASE_VERBOSE_DEBUG, "incrementing global shared lock");
            ret = ompi_osc_rdma_lock_acquire_shared (module, peer, 1, offsetof (ompi_osc_rdma_state_t, local_lock),
                                                     OMPI_OSC_RDMA_LOCK_EXCLUSIVE);
            if (OMPI_SUCCESS == ret) {
                return OMPI_SUCCESS;
            }

            ompi_osc_rdma_progress (module);
        } while (1);
    }

    return OMPI_SUCCESS;
}

static inline int ompi_osc_rdma_unlock_atomic_internal (ompi_osc_rdma_module_t *module, ompi_osc_rdma_peer_t *peer,
                                                        ompi_osc_rdma_sync_t *lock)
{
    const int locking_mode = module->locking_mode;

    if (MPI_LOCK_EXCLUSIVE == lock->sync.lock.type) {
        OSC_RDMA_VERBOSE(MCA_BASE_VERBOSE_DEBUG, "releasing exclusive lock on peer");
        ompi_osc_rdma_lock_release_exclusive (module, peer, offsetof (ompi_osc_rdma_state_t, local_lock));

        if (OMPI_OSC_RDMA_LOCKING_TWO_LEVEL == locking_mode) {
            OSC_RDMA_VERBOSE(MCA_BASE_VERBOSE_DEBUG, "decrementing global exclusive lock");
            ompi_osc_rdma_lock_release_shared (module, module->leader, -1, offsetof (ompi_osc_rdma_state_t, global_lock));
        }

        peer->flags &= ~OMPI_OSC_RDMA_PEER_EXCLUSIVE;
    } else {
        OSC_RDMA_VERBOSE(MCA_BASE_VERBOSE_DEBUG, "decrementing global shared lock");
        ompi_osc_rdma_lock_release_shared (module, peer, -1, offsetof (ompi_osc_rdma_state_t, local_lock));
        peer->flags &= ~OMPI_OSC_RDMA_PEER_DEMAND_LOCKED;
    }

    return OMPI_SUCCESS;
}

int ompi_osc_rdma_demand_lock_peer (ompi_osc_rdma_module_t *module, ompi_osc_rdma_peer_t *peer)
{
    ompi_osc_rdma_sync_t *lock = &module->all_sync;
    int ret = OMPI_SUCCESS;

    /* check for bad usage */
    assert (OMPI_OSC_RDMA_SYNC_TYPE_LOCK == lock->type);

    OPAL_THREAD_SCOPED_LOCK(&peer->lock,
    do {
        if (!ompi_osc_rdma_peer_is_demand_locked (peer)) {
            ret = ompi_osc_rdma_lock_atomic_internal (module, peer, lock);
            OPAL_THREAD_SCOPED_LOCK(&lock->lock, opal_list_append (&lock->demand_locked_peers, &peer->super));
            peer->flags |= OMPI_OSC_RDMA_PEER_DEMAND_LOCKED;
        }
    } while (0);
    );

    return ret;
}

int ompi_osc_rdma_lock_atomic (int lock_type, int target, int assert, ompi_win_t *win)
{
    ompi_osc_rdma_module_t *module = GET_MODULE(win);
    ompi_osc_rdma_peer_t *peer = ompi_osc_rdma_module_peer (module, target);
    ompi_osc_rdma_sync_t *lock;
    int ret = OMPI_SUCCESS;

    OSC_RDMA_VERBOSE(MCA_BASE_VERBOSE_TRACE, "lock: %d, %d, %d, %s", lock_type, target, assert, win->w_name);

    if (module->no_locks) {
        OSC_RDMA_VERBOSE(MCA_BASE_VERBOSE_INFO, "attempted to lock with no_locks set");
        return OMPI_ERR_RMA_SYNC;
    }

    if (module->all_sync.epoch_active && (OMPI_OSC_RDMA_SYNC_TYPE_LOCK != module->all_sync.type || MPI_LOCK_EXCLUSIVE == lock_type)) {
        /* impossible to get an exclusive lock while holding a global shared lock or in a active
         * target access epoch */
        return OMPI_ERR_RMA_SYNC;
    }

    /* clear the global sync object (in case MPI_Win_fence was called) */
    module->all_sync.type = OMPI_OSC_RDMA_SYNC_TYPE_NONE;

    /* create lock item */
    lock = ompi_osc_rdma_sync_allocate (module);
    if (OPAL_UNLIKELY(NULL == lock)) {
        return OMPI_ERR_OUT_OF_RESOURCE;
    }

    lock->type = OMPI_OSC_RDMA_SYNC_TYPE_LOCK;
    lock->sync.lock.target = target;
    lock->sync.lock.type = lock_type;
    lock->sync.lock.assert = assert;

    lock->peer_list.peer = peer;
    lock->num_peers = 1;
    OBJ_RETAIN(peer);

    if (0 == (assert & MPI_MODE_NOCHECK)) {
        ret = ompi_osc_rdma_lock_atomic_internal (module, peer, lock);
    }

    if (OPAL_LIKELY(OMPI_SUCCESS == ret)) {
        ++module->passive_target_access_epoch;

        opal_atomic_wmb ();

        OPAL_THREAD_SCOPED_LOCK(&module->lock, ompi_osc_rdma_module_lock_insert (module, lock));
    } else {
        OBJ_RELEASE(lock);
    }

    OSC_RDMA_VERBOSE(MCA_BASE_VERBOSE_TRACE, "lock %d complete", target);

    return ret;
}


int ompi_osc_rdma_unlock_atomic (int target, ompi_win_t *win)
{
    ompi_osc_rdma_module_t *module = GET_MODULE(win);
    ompi_osc_rdma_peer_t *peer;
    ompi_osc_rdma_sync_t *lock;
    int ret = OMPI_SUCCESS;

    OPAL_THREAD_LOCK(&module->lock);

    OSC_RDMA_VERBOSE(MCA_BASE_VERBOSE_TRACE, "unlock: %d, %s", target, win->w_name);

    lock = ompi_osc_rdma_module_lock_find (module, target, &peer);
    if (OPAL_UNLIKELY(NULL == lock)) {
        OSC_RDMA_VERBOSE(MCA_BASE_VERBOSE_INFO, "target %d is not locked in window %s",
                         target, win->w_name);
        OPAL_THREAD_UNLOCK(&module->lock);
        return OMPI_ERR_RMA_SYNC;
    }

    ompi_osc_rdma_module_lock_remove (module, lock);

    /* finish all outstanding fragments */
    ompi_osc_rdma_sync_rdma_complete (lock);

    if (!(lock->sync.lock.assert & MPI_MODE_NOCHECK)) {
        ret = ompi_osc_rdma_unlock_atomic_internal (module, peer, lock);
    }

    /* release our reference to this peer */
    OBJ_RELEASE(peer);

    OSC_RDMA_VERBOSE(MCA_BASE_VERBOSE_TRACE, "unlock %d complete", target);

    --module->passive_target_access_epoch;

    opal_atomic_wmb ();

    OPAL_THREAD_UNLOCK(&module->lock);

    /* delete the lock */
    ompi_osc_rdma_sync_return (lock);

    return ret;
}

int ompi_osc_rdma_lock_all_atomic (int assert, struct ompi_win_t *win)
{
    ompi_osc_rdma_module_t *module = GET_MODULE(win);
    ompi_osc_rdma_sync_t *lock;
    int ret = OMPI_SUCCESS;

    OSC_RDMA_VERBOSE(MCA_BASE_VERBOSE_TRACE, "lock_all: %d, %s", assert, win->w_name);

    if (module->no_locks) {
        OSC_RDMA_VERBOSE(MCA_BASE_VERBOSE_INFO, "attempted to lock with no_locks set");
        return OMPI_ERR_RMA_SYNC;
    }

    OPAL_THREAD_LOCK(&module->lock);
    if (module->all_sync.epoch_active) {
        OSC_RDMA_VERBOSE(MCA_BASE_VERBOSE_INFO, "attempted lock_all when active target epoch is %s "
                         "and lock all epoch is %s",
                         (OMPI_OSC_RDMA_SYNC_TYPE_LOCK != module->all_sync.type && module->all_sync.epoch_active) ?
                         "active" : "inactive",
                         (OMPI_OSC_RDMA_SYNC_TYPE_LOCK == module->all_sync.type) ? "active" : "inactive");
        OPAL_THREAD_UNLOCK(&module->lock);
        return OMPI_ERR_RMA_SYNC;
    }

    /* set up lock */
    lock = &module->all_sync;

    lock->type = OMPI_OSC_RDMA_SYNC_TYPE_LOCK;
    lock->sync.lock.target = -1;
    lock->sync.lock.type   = MPI_LOCK_SHARED;
    lock->sync.lock.assert = assert;
    lock->num_peers = ompi_comm_size (module->comm);

    lock->epoch_active = true;
    /* NTH: TODO -- like fence it might be a good idea to create an array to access all peers
     * without having to access the hash table. Such a change would likely increase performance
     * at the expense of memory usage. Ex. if a window has 1M peers then 8MB per process would
     * be needed for this array. */

    if (0 == (assert & MPI_MODE_NOCHECK)) {
        /* increment the global shared lock */
        if (OMPI_OSC_RDMA_LOCKING_TWO_LEVEL == module->locking_mode) {
            ret = ompi_osc_rdma_lock_acquire_shared (module, module->leader, 0x0000000100000000UL,
                                                     offsetof(ompi_osc_rdma_state_t, global_lock),
                                                     0x00000000ffffffffUL);
        } else {
            /* always lock myself */
            ret = ompi_osc_rdma_demand_lock_peer (module, module->my_peer);
        }
    }

    if (OPAL_LIKELY(OMPI_SUCCESS != ret)) {
        lock->type = OMPI_OSC_RDMA_SYNC_TYPE_NONE;
        lock->num_peers = 0;
        lock->epoch_active = false;
    } else {
        ++module->passive_target_access_epoch;
    }

    opal_atomic_wmb ();

    OPAL_THREAD_UNLOCK(&module->lock);

    OSC_RDMA_VERBOSE(MCA_BASE_VERBOSE_TRACE, "lock_all complete");

    return ret;
}

int ompi_osc_rdma_unlock_all_atomic (struct ompi_win_t *win)
{
    ompi_osc_rdma_module_t *module = GET_MODULE(win);
    ompi_osc_rdma_sync_t *lock;

    OSC_RDMA_VERBOSE(MCA_BASE_VERBOSE_TRACE, "unlock_all: %s", win->w_name);

    OPAL_THREAD_LOCK(&module->lock);

    lock = &module->all_sync;
    if (OMPI_OSC_RDMA_SYNC_TYPE_LOCK != lock->type) {
        OSC_RDMA_VERBOSE(MCA_BASE_VERBOSE_INFO, "not locked in window %s", win->w_name);
        OPAL_THREAD_UNLOCK(&module->lock);
        return OMPI_ERR_RMA_SYNC;
    }

    /* finish all outstanding fragments */
    ompi_osc_rdma_sync_rdma_complete (lock);

    if (0 == (lock->sync.lock.assert & MPI_MODE_NOCHECK)) {
        if (OMPI_OSC_RDMA_LOCKING_ON_DEMAND == module->locking_mode) {
            ompi_osc_rdma_peer_t *peer, *next;

            /* drop all on-demand locks */
            OPAL_LIST_FOREACH_SAFE(peer, next, &lock->demand_locked_peers, ompi_osc_rdma_peer_t) {
                (void) ompi_osc_rdma_unlock_atomic_internal (module, peer, lock);
                opal_list_remove_item (&lock->demand_locked_peers, &peer->super);
            }
        } else {
            /* decrement the master lock shared count */
            (void) ompi_osc_rdma_lock_release_shared (module, module->leader, -0x0000000100000000UL,
                                                      offsetof (ompi_osc_rdma_state_t, global_lock));
        }
    }

    lock->type = OMPI_OSC_RDMA_SYNC_TYPE_NONE;
    lock->num_peers = 0;
    lock->epoch_active = false;

    --module->passive_target_access_epoch;

    opal_atomic_wmb ();

    OPAL_THREAD_UNLOCK(&module->lock);

    OSC_RDMA_VERBOSE(MCA_BASE_VERBOSE_TRACE, "unlock_all complete");

    return OMPI_SUCCESS;
}
