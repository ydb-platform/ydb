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
 * Copyright (c) 2007-2017 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2010-2016 IBM Corporation.  All rights reserved.
 * Copyright (c) 2012-2013 Sandia National Laboratories.  All rights reserved.
 * Copyright (c) 2015      Intel, Inc. All rights reserved.
 * Copyright (c) 2015-2016 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include "osc_pt2pt.h"
#include "osc_pt2pt_header.h"
#include "osc_pt2pt_data_move.h"
#include "osc_pt2pt_frag.h"

#include "mpi.h"
#include "opal/runtime/opal_progress.h"
#include "opal/threads/mutex.h"
#include "ompi/communicator/communicator.h"
#include "ompi/mca/osc/base/base.h"
#include "opal/include/opal_stdint.h"

static bool ompi_osc_pt2pt_lock_try_acquire (ompi_osc_pt2pt_module_t* module, int source, int lock_type,
                                             uint64_t lock_ptr);

/* target-side tracking of a lock request */
struct ompi_osc_pt2pt_pending_lock_t {
    opal_list_item_t super;
    int peer;
    int lock_type;
    uint64_t lock_ptr;
};
typedef struct ompi_osc_pt2pt_pending_lock_t ompi_osc_pt2pt_pending_lock_t;
OBJ_CLASS_INSTANCE(ompi_osc_pt2pt_pending_lock_t, opal_list_item_t,
                   NULL, NULL);

static int ompi_osc_pt2pt_activate_next_lock (ompi_osc_pt2pt_module_t *module);
static inline int queue_lock (ompi_osc_pt2pt_module_t *module, int requestor, int lock_type, uint64_t lock_ptr);
static int ompi_osc_pt2pt_flush_lock (ompi_osc_pt2pt_module_t *module, ompi_osc_pt2pt_sync_t *lock,
                                      int target);

static inline int ompi_osc_pt2pt_lock_self (ompi_osc_pt2pt_module_t *module, ompi_osc_pt2pt_sync_t *lock)
{
    const int my_rank = ompi_comm_rank (module->comm);
    ompi_osc_pt2pt_peer_t *peer = ompi_osc_pt2pt_peer_lookup (module, my_rank);
    int lock_type = lock->sync.lock.type;
    bool acquired = false;

    assert (lock->type == OMPI_OSC_PT2PT_SYNC_TYPE_LOCK);

    (void) OPAL_THREAD_ADD_FETCH32(&lock->sync_expected, 1);

    acquired = ompi_osc_pt2pt_lock_try_acquire (module, my_rank, lock_type, (uint64_t) (uintptr_t) lock);
    if (!acquired) {
        /* queue the lock */
        queue_lock (module, my_rank, lock_type, (uint64_t) (uintptr_t) lock);

        /* If locking local, can't be non-blocking according to the
           standard.  We need to wait for the ack here. */
        ompi_osc_pt2pt_sync_wait_expected (lock);
    }

    ompi_osc_pt2pt_peer_set_locked (peer, true);
    ompi_osc_pt2pt_peer_set_eager_active (peer, true);

    OPAL_OUTPUT_VERBOSE((25, ompi_osc_base_framework.framework_output,
                         "local lock aquired"));

    return OMPI_SUCCESS;
}

static inline void ompi_osc_pt2pt_unlock_self (ompi_osc_pt2pt_module_t *module, ompi_osc_pt2pt_sync_t *lock)
{
    const int my_rank = ompi_comm_rank (module->comm);
    ompi_osc_pt2pt_peer_t *peer = ompi_osc_pt2pt_peer_lookup (module, my_rank);
    int lock_type = lock->sync.lock.type;

    (void) OPAL_THREAD_ADD_FETCH32(&lock->sync_expected, 1);

    assert (lock->type == OMPI_OSC_PT2PT_SYNC_TYPE_LOCK);

    OPAL_OUTPUT_VERBOSE((25, ompi_osc_base_framework.framework_output,
                         "ompi_osc_pt2pt_unlock_self: unlocking myself. lock state = %d", module->lock_status));

    if (MPI_LOCK_EXCLUSIVE == lock_type) {
        OPAL_THREAD_ADD_FETCH32(&module->lock_status, 1);
        ompi_osc_pt2pt_activate_next_lock (module);
    } else if (0 == OPAL_THREAD_ADD_FETCH32(&module->lock_status, -1)) {
        ompi_osc_pt2pt_activate_next_lock (module);
    }

    /* need to ensure we make progress */
    opal_progress();

    ompi_osc_pt2pt_peer_set_locked (peer, false);
    ompi_osc_pt2pt_peer_set_eager_active (peer, false);

    ompi_osc_pt2pt_sync_expected (lock);
}

int ompi_osc_pt2pt_lock_remote (ompi_osc_pt2pt_module_t *module, int target, ompi_osc_pt2pt_sync_t *lock)
{
    ompi_osc_pt2pt_peer_t *peer = ompi_osc_pt2pt_peer_lookup (module, target);
    int lock_type = lock->sync.lock.type;
    ompi_osc_pt2pt_header_lock_t lock_req;

    int ret;

    OPAL_THREAD_LOCK(&peer->lock);
    if (ompi_osc_pt2pt_peer_locked (peer)) {
        OPAL_THREAD_UNLOCK(&peer->lock);
        return OMPI_SUCCESS;
    }

    (void) OPAL_THREAD_ADD_FETCH32(&lock->sync_expected, 1);

    assert (lock->type == OMPI_OSC_PT2PT_SYNC_TYPE_LOCK);

    /* generate a lock request */
    lock_req.base.type = OMPI_OSC_PT2PT_HDR_TYPE_LOCK_REQ;
    lock_req.base.flags = OMPI_OSC_PT2PT_HDR_FLAG_VALID | OMPI_OSC_PT2PT_HDR_FLAG_PASSIVE_TARGET;
#if OPAL_ENABLE_HETEROGENEOUS_SUPPORT && OPAL_ENABLE_DEBUG
    lock_req.padding[0] = 0;
    lock_req.padding[1] = 0;
#endif
    lock_req.lock_type = lock_type;
    lock_req.lock_ptr = (uint64_t) (uintptr_t) lock;
    OSC_PT2PT_HTON(&lock_req, module, target);

    ret = ompi_osc_pt2pt_control_send_unbuffered (module, target, &lock_req, sizeof (lock_req));
    if (OPAL_UNLIKELY(OMPI_SUCCESS != ret)) {
        OPAL_THREAD_ADD_FETCH32(&lock->sync_expected, -1);
    } else {
        ompi_osc_pt2pt_peer_set_locked (peer, true);
    }

    OPAL_THREAD_UNLOCK(&peer->lock);

    return ret;
}

static inline int ompi_osc_pt2pt_unlock_remote (ompi_osc_pt2pt_module_t *module, int target, ompi_osc_pt2pt_sync_t *lock)
{
    int32_t frag_count = opal_atomic_swap_32 ((int32_t *) module->epoch_outgoing_frag_count + target, -1);
    ompi_osc_pt2pt_peer_t *peer = ompi_osc_pt2pt_peer_lookup (module, target);
    int lock_type = lock->sync.lock.type;
    ompi_osc_pt2pt_header_unlock_t unlock_req;
    int ret;

    (void) OPAL_THREAD_ADD_FETCH32(&lock->sync_expected, 1);

    assert (lock->type == OMPI_OSC_PT2PT_SYNC_TYPE_LOCK);

    unlock_req.base.type = OMPI_OSC_PT2PT_HDR_TYPE_UNLOCK_REQ;
    unlock_req.base.flags = OMPI_OSC_PT2PT_HDR_FLAG_VALID | OMPI_OSC_PT2PT_HDR_FLAG_PASSIVE_TARGET;
#if OPAL_ENABLE_HETEROGENEOUS_SUPPORT && OPAL_ENABLE_DEBUG
    unlock_req.padding[0] = 0;
    unlock_req.padding[1] = 0;
#endif
    unlock_req.frag_count = frag_count;
    unlock_req.lock_type = lock_type;
    unlock_req.lock_ptr = (uint64_t) (uintptr_t) lock;
    OSC_PT2PT_HTON(&unlock_req, module, target);

    if (peer->active_frag && peer->active_frag->remain_len < sizeof (unlock_req)) {
        /* the peer should expect one more packet */
        ++unlock_req.frag_count;
        --module->epoch_outgoing_frag_count[target];
    }

    OPAL_OUTPUT_VERBOSE((25, ompi_osc_base_framework.framework_output,
                         "osc pt2pt: unlocking target %d, frag count: %d", target,
                         unlock_req.frag_count));

    /* send control message with unlock request and count */
    ret = ompi_osc_pt2pt_control_send (module, target, &unlock_req, sizeof (unlock_req));
    if (OPAL_UNLIKELY(OMPI_SUCCESS != ret)) {
        return ret;
    }

    ompi_osc_pt2pt_peer_set_locked (peer, false);
    ompi_osc_pt2pt_peer_set_eager_active (peer, false);

    return ompi_osc_pt2pt_frag_flush_target(module, target);
}

static inline int ompi_osc_pt2pt_flush_remote (ompi_osc_pt2pt_module_t *module, int target, ompi_osc_pt2pt_sync_t *lock)
{
    ompi_osc_pt2pt_peer_t *peer = ompi_osc_pt2pt_peer_lookup (module, target);
    ompi_osc_pt2pt_header_flush_t flush_req;
    int32_t frag_count = opal_atomic_swap_32 ((int32_t *) module->epoch_outgoing_frag_count + target, -1);
    int ret;

    (void) OPAL_THREAD_ADD_FETCH32(&lock->sync_expected, 1);

    assert (lock->type == OMPI_OSC_PT2PT_SYNC_TYPE_LOCK);

    flush_req.base.type = OMPI_OSC_PT2PT_HDR_TYPE_FLUSH_REQ;
    flush_req.base.flags = OMPI_OSC_PT2PT_HDR_FLAG_VALID | OMPI_OSC_PT2PT_HDR_FLAG_PASSIVE_TARGET;
    flush_req.frag_count = frag_count;
    flush_req.lock_ptr = (uint64_t) (uintptr_t) lock;

    /* XXX -- TODO -- since fragment are always delivered in order we do not need to count anything but long
     * requests. once that is done this can be removed. */
    if (peer->active_frag && (peer->active_frag->remain_len < sizeof (flush_req))) {
        /* the peer should expect one more packet */
        ++flush_req.frag_count;
        --module->epoch_outgoing_frag_count[target];
    }

    OPAL_OUTPUT_VERBOSE((50, ompi_osc_base_framework.framework_output, "flushing to target %d, frag_count: %d",
                         target, flush_req.frag_count));

    /* send control message with unlock request and count */
    OSC_PT2PT_HTON(&flush_req, module, target);
    ret = ompi_osc_pt2pt_control_send (module, target, &flush_req, sizeof (flush_req));
    if (OPAL_UNLIKELY(OMPI_SUCCESS != ret)) {
        return ret;
    }

    /* start all sendreqs to target */
    return ompi_osc_pt2pt_frag_flush_target (module, target);
}

static int ompi_osc_pt2pt_lock_internal_execute (ompi_osc_pt2pt_module_t *module, ompi_osc_pt2pt_sync_t *lock)
{
    int my_rank = ompi_comm_rank (module->comm);
    int target = lock->sync.lock.target;
    int assert = lock->sync.lock.assert;
    int ret;

    assert (lock->type == OMPI_OSC_PT2PT_SYNC_TYPE_LOCK);

    if (0 == (assert & MPI_MODE_NOCHECK)) {
        if (my_rank != target && target != -1) {
            ret = ompi_osc_pt2pt_lock_remote (module, target, lock);
        } else {
            ret = ompi_osc_pt2pt_lock_self (module, lock);
        }

        if (OPAL_UNLIKELY(OMPI_SUCCESS != ret)) {
            /* return */
            return ret;
        }

        /* for lock_all there is nothing more to do. we will lock peer's on demand */
    } else {
        lock->eager_send_active = true;
    }

    return OMPI_SUCCESS;
}

static int ompi_osc_pt2pt_lock_internal (int lock_type, int target, int assert, ompi_win_t *win)
{
    ompi_osc_pt2pt_module_t *module = GET_MODULE(win);
    ompi_osc_pt2pt_sync_t *lock;
    int ret = OMPI_SUCCESS;

    /* Check if no_locks is set. TODO: we also need to track whether we are in an
     * active target epoch. Fence can make this tricky to track. */
    if (-1 == target) {
        if (module->all_sync.epoch_active) {
            OPAL_OUTPUT_VERBOSE((1, ompi_osc_base_framework.framework_output, "osc/pt2pt: attempted "
                                 "to lock all when active target epoch is %s and lock all epoch is %s. type %d",
                                 (OMPI_OSC_PT2PT_SYNC_TYPE_LOCK != module->all_sync.type && module->all_sync.epoch_active) ?
                                 "active" : "inactive",
                                 (OMPI_OSC_PT2PT_SYNC_TYPE_LOCK == module->all_sync.type) ? "active" : "inactive",
                                 module->all_sync.type));
            return OMPI_ERR_RMA_SYNC;
        }
    } else {
        if (module->all_sync.epoch_active && (OMPI_OSC_PT2PT_SYNC_TYPE_LOCK != module->all_sync.type || MPI_LOCK_EXCLUSIVE == lock_type)) {
            /* impossible to get an exclusive lock while holding a global shared lock or in a active
             * target access epoch */
            return OMPI_ERR_RMA_SYNC;
        }
    }

    /* Check if no_locks is set. TODO: we also need to track whether we are in an
     * active target epoch. Fence can make this tricky to track. */
    if (module->all_sync.epoch_active || (OMPI_OSC_PT2PT_SYNC_TYPE_LOCK == module->all_sync.type &&
                                          (MPI_LOCK_EXCLUSIVE == lock_type || -1 == target))) {
        OPAL_OUTPUT_VERBOSE((25, ompi_osc_base_framework.framework_output, "osc pt2pt: attempted "
                             "to acquire a lock on %d with type %d when active sync is %s and lock "
                             "all epoch is %s", target, lock_type, module->all_sync.epoch_active ? "active" : "inactive",
                             (OMPI_OSC_PT2PT_SYNC_TYPE_LOCK == module->all_sync.type &&
                              (MPI_LOCK_EXCLUSIVE == lock_type || -1 == target)) ? "active" : "inactive"));
        return OMPI_ERR_RMA_SYNC;
    }

    if (OMPI_OSC_PT2PT_SYNC_TYPE_FENCE == module->all_sync.type) {
        /* if not communication has occurred during a fence epoch then we can enter a lock epoch
         * just need to clear the all access epoch */
        module->all_sync.type = OMPI_OSC_PT2PT_SYNC_TYPE_NONE;
    }

    OPAL_OUTPUT_VERBOSE((25, ompi_osc_base_framework.framework_output,
                         "osc pt2pt: lock %d %d", target, lock_type));

    /* create lock item */
    if (-1 != target) {
        lock = ompi_osc_pt2pt_sync_allocate (module);
        if (OPAL_UNLIKELY(NULL == lock)) {
            return OMPI_ERR_OUT_OF_RESOURCE;
        }

        lock->peer_list.peer = ompi_osc_pt2pt_peer_lookup (module, target);
    } else {
        lock = &module->all_sync;
    }

    lock->type = OMPI_OSC_PT2PT_SYNC_TYPE_LOCK;
    lock->sync.lock.target = target;
    lock->sync.lock.type = lock_type;
    lock->sync.lock.assert = assert;
    lock->num_peers = (-1 == target) ? ompi_comm_size (module->comm) : 1;
    lock->sync_expected = 0;

    /* delay all eager sends until we've heard back.. */
    OPAL_THREAD_LOCK(&module->lock);

    /* check for conflicting lock */
    if (ompi_osc_pt2pt_module_lock_find (module, target, NULL)) {
        if (&module->all_sync != lock) {
            ompi_osc_pt2pt_sync_return (lock);
        }
        OPAL_THREAD_UNLOCK(&module->lock);
        return OMPI_ERR_RMA_CONFLICT;
    }

    ++module->passive_target_access_epoch;

    ompi_osc_pt2pt_module_lock_insert (module, lock);

    OPAL_THREAD_UNLOCK(&module->lock);

    ret = ompi_osc_pt2pt_lock_internal_execute (module, lock);
    if (OPAL_UNLIKELY(OMPI_SUCCESS != ret)) {
        OPAL_THREAD_SCOPED_LOCK(&module->lock, ompi_osc_pt2pt_module_lock_remove (module, lock));
        if (&module->all_sync != lock) {
            ompi_osc_pt2pt_sync_return (lock);
        }
    }

    return ret;
}

static int ompi_osc_pt2pt_unlock_internal (int target, ompi_win_t *win)
{
    ompi_osc_pt2pt_module_t *module = GET_MODULE(win);
    ompi_osc_pt2pt_sync_t *lock = NULL;
    int my_rank = ompi_comm_rank (module->comm);
    int ret = OMPI_SUCCESS;

    OPAL_OUTPUT_VERBOSE((25, ompi_osc_base_framework.framework_output,
                         "ompi_osc_pt2pt_unlock_internal: unlocking target %d", target));

    OPAL_THREAD_LOCK(&module->lock);
    lock = ompi_osc_pt2pt_module_lock_find (module, target, NULL);
    if (OPAL_UNLIKELY(NULL == lock)) {
        OPAL_OUTPUT_VERBOSE((25, ompi_osc_base_framework.framework_output,
                             "ompi_osc_pt2pt_unlock: target %d is not locked in window %s",
                             target, win->w_name));
        OPAL_THREAD_UNLOCK(&module->lock);
        return OMPI_ERR_RMA_SYNC;
    }

    OPAL_OUTPUT_VERBOSE((25, ompi_osc_base_framework.framework_output,
                         "ompi_osc_pt2pt_unlock_internal: lock acks still expected: %d",
                         lock->sync_expected));
    OPAL_THREAD_UNLOCK(&module->lock);

    /* wait until ack has arrived from target */
    ompi_osc_pt2pt_sync_wait_expected (lock);

    OPAL_OUTPUT_VERBOSE((25, ompi_osc_base_framework.framework_output,
                         "ompi_osc_pt2pt_unlock_internal: all lock acks received"));

    if (!(lock->sync.lock.assert & MPI_MODE_NOCHECK)) {
        if (my_rank != target) {
            if (-1 == target) {
                /* send unlock messages to all of my peers */
                for (int i = 0 ; i < ompi_comm_size(module->comm) ; ++i) {
                    ompi_osc_pt2pt_peer_t *peer = ompi_osc_pt2pt_peer_lookup (module, i);

                    if (my_rank == i || !ompi_osc_pt2pt_peer_locked (peer)) {
                        continue;
                    }

                    ret = ompi_osc_pt2pt_unlock_remote (module, i, lock);
                    if (OPAL_UNLIKELY(OMPI_SUCCESS != ret)) {
                        return ret;
                    }
                }

                ompi_osc_pt2pt_unlock_self (module, lock);
            } else {
                ret = ompi_osc_pt2pt_unlock_remote (module, target, lock);
                if (OPAL_UNLIKELY(OMPI_SUCCESS != ret)) {
                    return ret;
                }
            }

            /* wait for unlock acks. this signals remote completion of fragments */
            ompi_osc_pt2pt_sync_wait_expected (lock);

            /* It is possible for the unlock to finish too early before the data
             * is actually present in the recv buffer (for non-contiguous datatypes)
             * So make sure to wait for all of the fragments to arrive.
             */
            OPAL_THREAD_LOCK(&module->lock);
            while (module->outgoing_frag_count < 0) {
                opal_condition_wait(&module->cond, &module->lock);
            }
            OPAL_THREAD_UNLOCK(&module->lock);

            OPAL_OUTPUT_VERBOSE((25, ompi_osc_base_framework.framework_output,
                                 "ompi_osc_pt2pt_unlock: unlock of %d complete", target));
        } else {
            ompi_osc_pt2pt_unlock_self (module, lock);
        }
    } else {
        /* flush instead */
        ompi_osc_pt2pt_flush_lock (module, lock, target);
    }

    OPAL_THREAD_LOCK(&module->lock);
    ompi_osc_pt2pt_module_lock_remove (module, lock);

    if (-1 != lock->sync.lock.target) {
        ompi_osc_pt2pt_sync_return (lock);
    } else {
        ompi_osc_pt2pt_sync_reset (lock);
    }

    --module->passive_target_access_epoch;

    OPAL_THREAD_UNLOCK(&module->lock);

    return ret;
}

int ompi_osc_pt2pt_lock(int lock_type, int target, int assert, ompi_win_t *win)
{
    assert(target >= 0);

    return ompi_osc_pt2pt_lock_internal (lock_type, target, assert, win);
}

int ompi_osc_pt2pt_unlock (int target, struct ompi_win_t *win)
{
    return ompi_osc_pt2pt_unlock_internal (target, win);
}

int ompi_osc_pt2pt_lock_all(int assert, struct ompi_win_t *win)
{
    return ompi_osc_pt2pt_lock_internal (MPI_LOCK_SHARED, -1, assert, win);
}


int ompi_osc_pt2pt_unlock_all (struct ompi_win_t *win)
{
    return ompi_osc_pt2pt_unlock_internal (-1, win);
}


int ompi_osc_pt2pt_sync (struct ompi_win_t *win)
{
    opal_progress();
    return OMPI_SUCCESS;
}

static int ompi_osc_pt2pt_flush_lock (ompi_osc_pt2pt_module_t *module, ompi_osc_pt2pt_sync_t *lock,
                                      int target)
{
    int ret;
    int my_rank = ompi_comm_rank (module->comm);

    /* wait until ack has arrived from target, since we need to be
       able to eager send before we can transfer all the data... */
    ompi_osc_pt2pt_sync_wait_expected (lock);

    if (-1 == target) {
        /* NTH: no local flush */
        for (int i = 0 ; i < ompi_comm_size(module->comm) ; ++i) {
            if (i == my_rank) {
                continue;
            }

            ret = ompi_osc_pt2pt_flush_remote (module, i, lock);
            if (OPAL_UNLIKELY(OMPI_SUCCESS != ret)) {
                return ret;
            }
        }
    } else {
        /* send control message with flush request and count */
        ret = ompi_osc_pt2pt_flush_remote (module, target, lock);
        if (OPAL_UNLIKELY(OMPI_SUCCESS != ret)) {
            return ret;
        }
    }

    /* wait for all flush acks (meaning remote completion) */
    ompi_osc_pt2pt_sync_wait_expected (lock);
    opal_condition_broadcast (&module->cond);

    return OMPI_SUCCESS;
}

int ompi_osc_pt2pt_flush (int target, struct ompi_win_t *win)
{
    ompi_osc_pt2pt_module_t *module = GET_MODULE(win);
    ompi_osc_pt2pt_sync_t *lock;
    int ret;

    assert (0 <= target);

    /* flush is only allowed from within a passive target epoch */
    if (!module->passive_target_access_epoch) {
        return OMPI_ERR_RMA_SYNC;
    }

    OPAL_OUTPUT_VERBOSE((50, ompi_osc_base_framework.framework_output,
                         "ompi_osc_pt2pt_flush starting..."));

    if (ompi_comm_rank (module->comm) == target) {
        /* nothing to flush */
        opal_progress ();
        return OMPI_SUCCESS;
    }

    OPAL_THREAD_LOCK(&module->lock);
    lock = ompi_osc_pt2pt_module_lock_find (module, target, NULL);
    if (NULL == lock) {
        if (OMPI_OSC_PT2PT_SYNC_TYPE_LOCK == module->all_sync.type) {
            lock = &module->all_sync;
        }
    }
    OPAL_THREAD_UNLOCK(&module->lock);
    if (OPAL_UNLIKELY(NULL == lock)) {
        OPAL_OUTPUT_VERBOSE((25, ompi_osc_base_framework.framework_output,
                             "ompi_osc_pt2pt_flush: target %d is not locked in window %s",
                             target, win->w_name));
        ret = OMPI_ERR_RMA_SYNC;
    } else {
        ret = ompi_osc_pt2pt_flush_lock (module, lock, target);
    }

    return ret;
}


int ompi_osc_pt2pt_flush_all (struct ompi_win_t *win)
{
    ompi_osc_pt2pt_module_t *module = GET_MODULE(win);
    ompi_osc_pt2pt_sync_t *lock;
    int target, ret;
    void *node;

    /* flush is only allowed from within a passive target epoch */
    if (OPAL_UNLIKELY(!module->passive_target_access_epoch)) {
        OPAL_OUTPUT_VERBOSE((25, ompi_osc_base_framework.framework_output,
                             "ompi_osc_pt2pt_flush_all: no targets are locked in window %s",
                             win->w_name));
        return OMPI_ERR_RMA_SYNC;
    }

    OPAL_OUTPUT_VERBOSE((50, ompi_osc_base_framework.framework_output,
                         "ompi_osc_pt2pt_flush_all entering..."));

    /* flush all locks */
    ret = opal_hash_table_get_first_key_uint32 (&module->outstanding_locks, (uint32_t *) &target,
                                                (void **) &lock, &node);
    if (OPAL_SUCCESS == ret) {
        do {
            ret = ompi_osc_pt2pt_flush_lock (module, lock, lock->sync.lock.target);
            if (OPAL_UNLIKELY(OMPI_SUCCESS != ret)) {
                break;
            }

            ret = opal_hash_table_get_next_key_uint32 (&module->outstanding_locks, (uint32_t *) &target,
                                                       (void **) lock, node, &node);
            if (OPAL_SUCCESS != ret) {
                ret = OPAL_SUCCESS;
                break;
            }
        } while (1);
    }

    OPAL_OUTPUT_VERBOSE((50, ompi_osc_base_framework.framework_output,
                         "ompi_osc_pt2pt_flush_all complete"));

    return ret;
}


int ompi_osc_pt2pt_flush_local (int target, struct ompi_win_t *win)
{
    ompi_osc_pt2pt_module_t *module = GET_MODULE(win);
    int ret;

    /* flush is only allowed from within a passive target epoch */
    if (!module->passive_target_access_epoch) {
        return OMPI_ERR_RMA_SYNC;
    }

    ret = ompi_osc_pt2pt_frag_flush_target(module, target);
    if (OMPI_SUCCESS != ret) {
        return ret;
    }

    /* wait for all the requests */
    OPAL_THREAD_LOCK(&module->lock);
    while (module->outgoing_frag_count < 0) {
        opal_condition_wait(&module->cond, &module->lock);
    }
    OPAL_THREAD_UNLOCK(&module->lock);

    /* make some progress */
    opal_progress ();

    return OMPI_SUCCESS;
}


int ompi_osc_pt2pt_flush_local_all (struct ompi_win_t *win)
{
    ompi_osc_pt2pt_module_t *module = GET_MODULE(win);
    int ret = OMPI_SUCCESS;

    /* flush is only allowed from within a passive target epoch */
    if (!module->passive_target_access_epoch) {
        return OMPI_ERR_RMA_SYNC;
    }

    ret = ompi_osc_pt2pt_frag_flush_all(module);
    if (OMPI_SUCCESS != ret) {
        return ret;
    }

    /* wait for all the requests */
    OPAL_THREAD_LOCK(&module->lock);
    while (module->outgoing_frag_count < 0) {
        opal_condition_wait(&module->cond, &module->lock);
    }
    OPAL_THREAD_UNLOCK(&module->lock);

    /* make some progress */
    opal_progress ();

    return OMPI_SUCCESS;
}

/* target side operation to acknowledge to initiator side that the
   lock is now held by the initiator */
static inline int activate_lock (ompi_osc_pt2pt_module_t *module, int requestor,
                                 uint64_t lock_ptr)
{
    ompi_osc_pt2pt_sync_t *lock;

    if (ompi_comm_rank (module->comm) != requestor) {
        ompi_osc_pt2pt_header_lock_ack_t lock_ack;

        lock_ack.base.type = OMPI_OSC_PT2PT_HDR_TYPE_LOCK_ACK;
        lock_ack.base.flags = OMPI_OSC_PT2PT_HDR_FLAG_VALID;
        lock_ack.source = ompi_comm_rank(module->comm);
        lock_ack.lock_ptr = lock_ptr;
        OSC_PT2PT_HTON(&lock_ack, module, requestor);

        OPAL_OUTPUT_VERBOSE((25, ompi_osc_base_framework.framework_output,
                             "osc pt2pt: sending lock to %d", requestor));

        /* we don't want to send any data, since we're the exposure
           epoch only, so use an unbuffered send */
        return ompi_osc_pt2pt_control_send_unbuffered (module, requestor, &lock_ack, sizeof (lock_ack));
    }


    OPAL_OUTPUT_VERBOSE((25, ompi_osc_base_framework.framework_output,
                         "osc pt2pt: releasing local lock"));

    lock = (ompi_osc_pt2pt_sync_t *) (uintptr_t) lock_ptr;
    if (OPAL_UNLIKELY(NULL == lock)) {
        OPAL_OUTPUT_VERBOSE((5, ompi_osc_base_framework.framework_output,
                             "lock could not be located"));
    }

    ompi_osc_pt2pt_sync_expected (lock);

    return OMPI_SUCCESS;
}


/* target side operation to create a pending lock request for a lock
   request that could not be satisfied */
static inline int queue_lock (ompi_osc_pt2pt_module_t *module, int requestor,
                              int lock_type, uint64_t lock_ptr)
{
    ompi_osc_pt2pt_pending_lock_t *pending =
        OBJ_NEW(ompi_osc_pt2pt_pending_lock_t);
    if (NULL == pending) {
        return OMPI_ERR_OUT_OF_RESOURCE;
    }

    pending->peer = requestor;
    pending->lock_type = lock_type;
    pending->lock_ptr = lock_ptr;

    OPAL_OUTPUT_VERBOSE((25, ompi_osc_base_framework.framework_output,
                         "osc pt2pt: queueing lock request from %d", requestor));

    OPAL_THREAD_SCOPED_LOCK(&module->locks_pending_lock, opal_list_append(&module->locks_pending, &pending->super));

    return OMPI_SUCCESS;
}

static bool ompi_osc_pt2pt_lock_try_acquire (ompi_osc_pt2pt_module_t* module, int source, int lock_type, uint64_t lock_ptr)
{
    bool queue = false;

    if (MPI_LOCK_SHARED == lock_type) {
        int32_t lock_status = module->lock_status;

        do {
            if (lock_status < 0) {
                queue = true;
                break;
            }

            if (opal_atomic_compare_exchange_strong_32 (&module->lock_status, &lock_status, lock_status + 1)) {
                break;
            }
        } while (1);
    } else {
        int32_t _tmp_value = 0;
        queue = !opal_atomic_compare_exchange_strong_32 (&module->lock_status, &_tmp_value, -1);
    }

    if (queue) {
        return false;
    }

    activate_lock(module, source, lock_ptr);

    /* activated the lock */
    return true;
}

static int ompi_osc_pt2pt_activate_next_lock (ompi_osc_pt2pt_module_t *module) {
    /* release any other pending locks we can */
    ompi_osc_pt2pt_pending_lock_t *pending_lock, *next;
    int ret = OMPI_SUCCESS;

    OPAL_THREAD_LOCK(&module->locks_pending_lock);
    OPAL_LIST_FOREACH_SAFE(pending_lock, next, &module->locks_pending,
                           ompi_osc_pt2pt_pending_lock_t) {
        bool acquired = ompi_osc_pt2pt_lock_try_acquire (module, pending_lock->peer, pending_lock->lock_type,
                                                         pending_lock->lock_ptr);
        if (!acquired) {
            break;
        }

        opal_list_remove_item (&module->locks_pending, &pending_lock->super);
        OBJ_RELEASE(pending_lock);
    }
    OPAL_THREAD_UNLOCK(&module->locks_pending_lock);

    return ret;
}


/* target side function called when the initiator sends a lock
   request.  Lock will either be activated and acknowledged or
   queued. */
int ompi_osc_pt2pt_process_lock (ompi_osc_pt2pt_module_t* module, int source,
                                ompi_osc_pt2pt_header_lock_t* lock_header)
{
    bool acquired;

    OPAL_OUTPUT_VERBOSE((25, ompi_osc_base_framework.framework_output,
                         "ompi_osc_pt2pt_process_lock: processing lock request from %d. current lock state = %d",
                         source, module->lock_status));

    acquired = ompi_osc_pt2pt_lock_try_acquire (module, source, lock_header->lock_type, lock_header->lock_ptr);

    if (!acquired) {
        queue_lock(module, source, lock_header->lock_type, lock_header->lock_ptr);
    }

    return OMPI_SUCCESS;
}


/* initiator-side function called when the target acks the lock
   request. */
void ompi_osc_pt2pt_process_lock_ack (ompi_osc_pt2pt_module_t *module,
                                      ompi_osc_pt2pt_header_lock_ack_t *lock_ack_header)
{
    ompi_osc_pt2pt_peer_t *peer = ompi_osc_pt2pt_peer_lookup (module, lock_ack_header->source);
    ompi_osc_pt2pt_sync_t *lock;

    OPAL_OUTPUT_VERBOSE((50, ompi_osc_base_framework.framework_output,
                         "ompi_osc_pt2pt_process_lock_ack: processing lock ack from %d for lock %" PRIu64,
                         lock_ack_header->source, lock_ack_header->lock_ptr));

    lock = (ompi_osc_pt2pt_sync_t *) (uintptr_t) lock_ack_header->lock_ptr;
    assert (NULL != lock);

    ompi_osc_pt2pt_peer_set_eager_active (peer, true);
    ompi_osc_pt2pt_frag_flush_pending (module, peer->rank);

    ompi_osc_pt2pt_sync_expected (lock);
}

void ompi_osc_pt2pt_process_flush_ack (ompi_osc_pt2pt_module_t *module, int source,
                                      ompi_osc_pt2pt_header_flush_ack_t *flush_ack_header) {
    ompi_osc_pt2pt_sync_t *lock;

    OPAL_OUTPUT_VERBOSE((50, ompi_osc_base_framework.framework_output,
                         "ompi_osc_pt2pt_process_flush_ack: processing flush ack from %d for lock 0x%" PRIx64,
                         source, flush_ack_header->lock_ptr));

    lock = (ompi_osc_pt2pt_sync_t *) (uintptr_t) flush_ack_header->lock_ptr;
    assert (NULL != lock);

    ompi_osc_pt2pt_sync_expected (lock);
}

void ompi_osc_pt2pt_process_unlock_ack (ompi_osc_pt2pt_module_t *module, int source,
                                        ompi_osc_pt2pt_header_unlock_ack_t *unlock_ack_header)
{
    ompi_osc_pt2pt_sync_t *lock;

    OPAL_OUTPUT_VERBOSE((50, ompi_osc_base_framework.framework_output,
                         "ompi_osc_pt2pt_process_unlock_ack: processing unlock ack from %d",
                         source));

    /* NTH: need to verify that this will work as expected */
    lock = (ompi_osc_pt2pt_sync_t *) (intptr_t) unlock_ack_header->lock_ptr;
    assert (NULL != lock);

    ompi_osc_pt2pt_sync_expected (lock);
}

/**
 * Process an unlock request.
 *
 * @param[in] module        - OSC PT2PT module
 * @param[in] source        - Source rank
 * @param[in] unlock_header - Incoming unlock header
 *
 * This functions is the target-side function for handling an unlock
 * request. Once all pending operations from the target are complete
 * this functions sends an unlock acknowledgement then attempts to
 * active a pending lock if the lock becomes free.
 */
int ompi_osc_pt2pt_process_unlock (ompi_osc_pt2pt_module_t *module, int source,
                                   ompi_osc_pt2pt_header_unlock_t *unlock_header)
{
    ompi_osc_pt2pt_peer_t *peer = ompi_osc_pt2pt_peer_lookup (module, source);
    ompi_osc_pt2pt_header_unlock_ack_t unlock_ack;
    int ret;

    assert (NULL != peer);

    OPAL_OUTPUT_VERBOSE((50, ompi_osc_base_framework.framework_output,
                         "ompi_osc_pt2pt_process_unlock entering (passive_incoming_frag_count: %d)...",
                         peer->passive_incoming_frag_count));

    /* we cannot block when processing an incoming request */
    if (0 != peer->passive_incoming_frag_count) {
        return OMPI_ERR_WOULD_BLOCK;
    }

    unlock_ack.base.type = OMPI_OSC_PT2PT_HDR_TYPE_UNLOCK_ACK;
    unlock_ack.base.flags = OMPI_OSC_PT2PT_HDR_FLAG_VALID;
#if OPAL_ENABLE_HETEROGENEOUS_SUPPORT && OPAL_ENABLE_DEBUG
    unlock_ack.padding[0] = 0;
    unlock_ack.padding[1] = 0;
    unlock_ack.padding[2] = 0;
    unlock_ack.padding[3] = 0;
    unlock_ack.padding[4] = 0;
    unlock_ack.padding[5] = 0;
#endif
    unlock_ack.lock_ptr = unlock_header->lock_ptr;
    OSC_PT2PT_HTON(&unlock_ack, module, source);

    ret = ompi_osc_pt2pt_control_send_unbuffered (module, source, &unlock_ack, sizeof (unlock_ack));
    if (OPAL_UNLIKELY(OMPI_SUCCESS != ret)) {
        return ret;
    }

    if (-1 == module->lock_status) {
        OPAL_THREAD_ADD_FETCH32(&module->lock_status, 1);
        ompi_osc_pt2pt_activate_next_lock (module);
    } else if (0 == OPAL_THREAD_ADD_FETCH32(&module->lock_status, -1)) {
        ompi_osc_pt2pt_activate_next_lock (module);
    }

    OPAL_OUTPUT_VERBOSE((50, ompi_osc_base_framework.framework_output,
                         "osc pt2pt: finished processing unlock fragment"));

    return ret;
}

int ompi_osc_pt2pt_process_flush (ompi_osc_pt2pt_module_t *module, int source,
                                  ompi_osc_pt2pt_header_flush_t *flush_header)
{
    ompi_osc_pt2pt_peer_t *peer = ompi_osc_pt2pt_peer_lookup (module, source);
    ompi_osc_pt2pt_header_flush_ack_t flush_ack;

    assert (NULL != peer);

    OPAL_OUTPUT_VERBOSE((50, ompi_osc_base_framework.framework_output,
                         "ompi_osc_pt2pt_process_flush entering (passive_incoming_frag_count: %d)...",
                         peer->passive_incoming_frag_count));

    /* we cannot block when processing an incoming request */
    if (0 != peer->passive_incoming_frag_count) {
        return OMPI_ERR_WOULD_BLOCK;
    }

    flush_ack.base.type = OMPI_OSC_PT2PT_HDR_TYPE_FLUSH_ACK;
    flush_ack.base.flags = OMPI_OSC_PT2PT_HDR_FLAG_VALID;
    flush_ack.lock_ptr = flush_header->lock_ptr;
    OSC_PT2PT_HTON(&flush_ack, module, source);

    return ompi_osc_pt2pt_control_send_unbuffered (module, source, &flush_ack, sizeof (flush_ack));
}
