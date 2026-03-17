/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University.
 *                         All rights reserved.
 * Copyright (c) 2004-2006 The Trustees of the University of Tennessee.
 *                         All rights reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007-2018 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2010      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2012-2013 Sandia National Laboratories.  All rights reserved.
 * Copyright (c) 2016-2018 Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef OMPI_OSC_RDMA_H
#define OMPI_OSC_RDMA_H

#include "ompi_config.h"
#include "opal/class/opal_free_list.h"
#include "opal/class/opal_hash_table.h"
#include "opal/threads/threads.h"
#include "opal/util/output.h"

#include "opal/mca/shmem/shmem.h"
#include "opal/mca/shmem/base/base.h"

#include "ompi/win/win.h"
#include "ompi/communicator/communicator.h"
#include "ompi/datatype/ompi_datatype.h"
#include "ompi/request/request.h"
#include "ompi/mca/osc/osc.h"
#include "ompi/mca/osc/base/base.h"
#include "opal/mca/btl/btl.h"
#include "ompi/memchecker.h"
#include "ompi/op/op.h"
#include "opal/align.h"

#include "osc_rdma_types.h"
#include "osc_rdma_sync.h"

#include "osc_rdma_peer.h"

#include "opal_stdint.h"

enum {
    OMPI_OSC_RDMA_LOCKING_TWO_LEVEL,
    OMPI_OSC_RDMA_LOCKING_ON_DEMAND,
};

/**
 * @brief osc rdma component structure
 */
struct ompi_osc_rdma_component_t {
    /** Extend the basic osc component interface */
    ompi_osc_base_component_t super;

    /** lock access to modules */
    opal_mutex_t lock;

    /** cid -> module mapping */
    opal_hash_table_t modules;

    /** free list of ompi_osc_rdma_frag_t structures */
    opal_free_list_t frags;

    /** Free list of requests */
    opal_free_list_t requests;

    /** RDMA component buffer size */
    unsigned int buffer_size;

    /** List of requests that need to be freed */
    opal_list_t request_gc;

    /** List of buffers that need to be freed */
    opal_list_t buffer_gc;

    /** Maximum number of segments that can be attached to a dynamic window */
    unsigned int max_attach;

    /** Default value of the no_locks info key for new windows */
    bool no_locks;

    /** Locking mode to use as the default for all windows */
    int locking_mode;

    /** Accumulate operations will only operate on a single intrinsic datatype */
    bool acc_single_intrinsic;

    /** Use network AMOs when available */
    bool acc_use_amo;

    /** Priority of the osc/rdma component */
    unsigned int priority;

    /** directory where to place backing files */
    char *backing_directory;
};
typedef struct ompi_osc_rdma_component_t ompi_osc_rdma_component_t;

struct ompi_osc_rdma_frag_t;

/**
 * @brief osc rdma module structure
 *
 * Each MPI window is associated with a single osc module. This struct
 * stores the data relevant to the osc/rdma component.
 */
struct ompi_osc_rdma_module_t {
    /** Extend the basic osc module interface */
    ompi_osc_base_module_t super;

    /** pointer back to MPI window */
    struct ompi_win_t *win;

    /** Mutex lock protecting module data */
    opal_mutex_t lock;

    /** locking mode to use */
    int locking_mode;

    /* window configuration */

    /** value of same_disp_unit info key for this window */
    bool same_disp_unit;

    /** value of same_size info key for this window */
    bool same_size;

    /** CPU atomics can be used */
    bool use_cpu_atomics;

    /** passive-target synchronization will not be used in this window */
    bool no_locks;

    bool acc_single_intrinsic;

    bool acc_use_amo;

    /** flavor of this window */
    int flavor;

    /** size of local window */
    size_t size;

    /** Local displacement unit. */
    int disp_unit;

    /** global leader */
    ompi_osc_rdma_peer_t *leader;

    /** my peer structure */
    ompi_osc_rdma_peer_t *my_peer;

    /** pointer to free on cleanup (may be NULL) */
    void *free_after;

    /** local state structure (shared memory) */
    ompi_osc_rdma_state_t *state;

    /** node-level communication data (shared memory) */
    unsigned char *node_comm_info;

    /* only relevant on the lowest rank on each node (shared memory) */
    ompi_osc_rdma_rank_data_t *rank_array;


    /** communicator created with this window.  This is the cid used
     * in the component's modules mapping. */
    ompi_communicator_t *comm;

    /* temporary communicators for window initialization */
    ompi_communicator_t *local_leaders;
    ompi_communicator_t *shared_comm;

    /** node id of this rank */
    int node_id;

    /** number of nodes */
    int node_count;

    /** handle valid for local state (valid for local data for MPI_Win_allocate) */
    mca_btl_base_registration_handle_t *state_handle;

    /** registration handle for the window base (only used for MPI_Win_create) */
    mca_btl_base_registration_handle_t *base_handle;

    /** size of a region */
    size_t region_size;

    /** size of the state structure */
    size_t state_size;

    /** offset in the shared memory segment where the state array starts */
    size_t state_offset;

    /* ********************* sync data ************************ */

    /** global sync object (PSCW, fence, lock all) */
    ompi_osc_rdma_sync_t all_sync;

    /** current group associate with pscw exposure epoch */
    struct ompi_group_t *pw_group;

    /** list of unmatched post messages */
    opal_list_t        pending_posts;

    /* ********************* LOCK data ************************ */

    /** number of outstanding locks */
    osc_rdma_counter_t passive_target_access_epoch;

    /** origin side list of locks currently outstanding */
    opal_hash_table_t outstanding_locks;

    /** array of locks (small jobs) */
    ompi_osc_rdma_sync_t **outstanding_lock_array;


    /* ******************* peer storage *********************** */

    /** hash table of allocated peers */
    opal_hash_table_t peer_hash;

    /** array of allocated peers (small jobs) */
    ompi_osc_rdma_peer_t **peer_array;

    /** lock for peer hash table/array */
    opal_mutex_t peer_lock;


    /** BTL in use */
    struct mca_btl_base_module_t *selected_btl;

    /** registered fragment used for locally buffered RDMA transfers */
    struct ompi_osc_rdma_frag_t *rdma_frag;

    /** registration handles for dynamically attached regions. These are not stored
     * in the state structure as it is entirely local. */
    ompi_osc_rdma_handle_t *dynamic_handles;

    /** shared memory segment. this segment holds this node's portion of the rank -> node
     * mapping array, node communication data (node_comm_info), state for all local ranks,
     * and data for all local ranks (MPI_Win_allocate only) */
    void *segment_base;

    /** opal shared memory structure for the shared memory segment */
    opal_shmem_ds_t seg_ds;


    /* performance values */

    /** number of times a put had to be retried */
    unsigned long put_retry_count;

    /** number of time a get had to be retried */
    unsigned long get_retry_count;

    /** outstanding atomic operations */
    volatile int32_t pending_ops;
};
typedef struct ompi_osc_rdma_module_t ompi_osc_rdma_module_t;
OMPI_MODULE_DECLSPEC extern ompi_osc_rdma_component_t mca_osc_rdma_component;

#define GET_MODULE(win) ((ompi_osc_rdma_module_t*) win->w_osc_module)

int ompi_osc_rdma_free (struct ompi_win_t *win);


/* peer functions */

/**
 * @brief cache a peer object
 *
 * @param[in] module          osc rdma module
 * @param[in] peer            peer object to cache
 *
 * @returns OMPI_SUCCESS on success
 * @returns OMPI_ERR_OUT_OF_RESOURCE on failure
 */
int ompi_osc_module_add_peer (ompi_osc_rdma_module_t *module, ompi_osc_rdma_peer_t *peer);

/**
 * @brief demand lock a peer
 *
 * @param[in] module          osc rdma module
 * @param[in] peer            peer to lock
 *
 * @returns OMPI_SUCCESS on success
 */
int ompi_osc_rdma_demand_lock_peer (ompi_osc_rdma_module_t *module, ompi_osc_rdma_peer_t *peer);

/**
 * @brief check if a peer object is cached for a remote rank
 *
 * @param[in] module          osc rdma module
 * @param[in] peer_id         remote peer rank
 *
 * @returns peer object on success
 * @returns NULL if a peer object is not cached for the peer
 */
static inline ompi_osc_rdma_peer_t *ompi_osc_module_get_peer (ompi_osc_rdma_module_t *module, int peer_id)
{
    if (NULL == module->peer_array) {
        ompi_osc_rdma_peer_t *peer = NULL;
        (void) opal_hash_table_get_value_uint32 (&module->peer_hash, peer_id, (void **) &peer);
        return peer;
    }

    return module->peer_array[peer_id];
}

/**
 * @brief get the peer object for a remote rank
 *
 * @param[in] module          osc rdma module
 * @param[in] peer_id         remote peer rank
 */
static inline ompi_osc_rdma_peer_t *ompi_osc_rdma_module_peer (ompi_osc_rdma_module_t *module, int peer_id)
{
    ompi_osc_rdma_peer_t *peer;

    peer = ompi_osc_module_get_peer (module, peer_id);
    if (NULL != peer) {
        return peer;
    }

    return ompi_osc_rdma_peer_lookup (module, peer_id);
}

/**
 * @brief check if this process has this process is in a passive target access epoch
 *
 * @param[in] module          osc rdma module
 */
static inline bool ompi_osc_rdma_in_passive_epoch (ompi_osc_rdma_module_t *module)
{
    return 0 != module->passive_target_access_epoch;
}

static inline int _ompi_osc_rdma_register (ompi_osc_rdma_module_t *module, struct mca_btl_base_endpoint_t *endpoint, void *ptr,
                                           size_t size, uint32_t flags, mca_btl_base_registration_handle_t **handle, int line, const char *file)
{
    if (module->selected_btl->btl_register_mem) {
        OSC_RDMA_VERBOSE(MCA_BASE_VERBOSE_INFO, "registering segment with btl. range: %p - %p (%lu bytes)",
                         ptr, (void*)((char *) ptr + size), size);

        *handle = module->selected_btl->btl_register_mem (module->selected_btl, endpoint, ptr, size, flags);
        if (OPAL_UNLIKELY(NULL == *handle)) {
            OSC_RDMA_VERBOSE(MCA_BASE_VERBOSE_DEBUG, "failed to register pointer with selected BTL. base: %p, "
                             "size: %lu. file: %s, line: %d", ptr, (unsigned long) size, file, line);
            return OMPI_ERR_OUT_OF_RESOURCE;
        }
    } else {
        *handle = NULL;
    }

    return OMPI_SUCCESS;
}

#define ompi_osc_rdma_register(...) _ompi_osc_rdma_register(__VA_ARGS__, __LINE__, __FILE__)

static inline void _ompi_osc_rdma_deregister (ompi_osc_rdma_module_t *module, mca_btl_base_registration_handle_t *handle, int line, const char *file)
{
    if (handle) {
        module->selected_btl->btl_deregister_mem (module->selected_btl, handle);
    }
}

#define ompi_osc_rdma_deregister(...) _ompi_osc_rdma_deregister(__VA_ARGS__, __LINE__, __FILE__)

static inline void ompi_osc_rdma_progress (ompi_osc_rdma_module_t *module) {
    opal_progress ();
}

/**
 * Find the first outstanding lock of the target.
 *
 * @param[in]  module   osc rdma module
 * @param[in]  target   target rank
 * @param[out] peer     peer object associated with the target
 *
 * @returns an outstanding lock on success
 *
 * This function looks for an outstanding lock to the target. If a lock exists it is returned.
 */
static inline ompi_osc_rdma_sync_t *ompi_osc_rdma_module_lock_find (ompi_osc_rdma_module_t *module, int target,
                                                                    ompi_osc_rdma_peer_t **peer)
{
    ompi_osc_rdma_sync_t *outstanding_lock = NULL;

    if (OPAL_LIKELY(NULL != module->outstanding_lock_array)) {
        outstanding_lock = module->outstanding_lock_array[target];
    } else {
        (void) opal_hash_table_get_value_uint32 (&module->outstanding_locks, (uint32_t) target, (void **) &outstanding_lock);
    }

    if (NULL != outstanding_lock && peer) {
        *peer = outstanding_lock->peer_list.peer;
    }

    return outstanding_lock;
}

/**
 * Add an outstanding lock
 *
 * @param[in] module   osc rdma module
 * @param[in] lock     lock object
 *
 * This function inserts a lock object to the list of outstanding locks. The caller must be holding the module
 * lock.
 */
static inline void ompi_osc_rdma_module_lock_insert (struct ompi_osc_rdma_module_t *module, ompi_osc_rdma_sync_t *lock)
{
    if (OPAL_LIKELY(NULL != module->outstanding_lock_array)) {
        module->outstanding_lock_array[lock->sync.lock.target] = lock;
    } else {
        (void) opal_hash_table_set_value_uint32 (&module->outstanding_locks, (uint32_t) lock->sync.lock.target, (void *) lock);
    }
}


/**
 * Remove an outstanding lock
 *
 * @param[in] module   osc rdma module
 * @param[in] lock     lock object
 *
 * This function removes a lock object to the list of outstanding locks. The caller must be holding the module
 * lock.
 */
static inline void ompi_osc_rdma_module_lock_remove (struct ompi_osc_rdma_module_t *module, ompi_osc_rdma_sync_t *lock)
{
    if (OPAL_LIKELY(NULL != module->outstanding_lock_array)) {
        module->outstanding_lock_array[lock->sync.lock.target] = NULL;
    } else {
        (void) opal_hash_table_remove_value_uint32 (&module->outstanding_locks, (uint32_t) lock->sync.lock.target);
    }
}

/**
 * Lookup a synchronization object associated with the target
 *
 * @param[in] module   osc rdma module
 * @param[in] target   target rank
 * @param[out] peer    peer object
 *
 * @returns NULL if the target is not locked, fenced, or part of a pscw sync
 * @returns synchronization object on success
 *
 * This function returns the synchronization object associated with an access epoch for
 * the target. If the target is not part of any current access epoch then NULL is returned.
 */
static inline ompi_osc_rdma_sync_t *ompi_osc_rdma_module_sync_lookup (ompi_osc_rdma_module_t *module, int target, struct ompi_osc_rdma_peer_t **peer)
{
    OSC_RDMA_VERBOSE(MCA_BASE_VERBOSE_TRACE, "looking for synchronization object for target %d", target);

    switch (module->all_sync.type) {
    case OMPI_OSC_RDMA_SYNC_TYPE_NONE:
        if (!module->no_locks) {
            return ompi_osc_rdma_module_lock_find (module, target, peer);
        }

        return NULL;
    case OMPI_OSC_RDMA_SYNC_TYPE_LOCK:
        OSC_RDMA_VERBOSE(MCA_BASE_VERBOSE_TRACE, "found lock_all access epoch for target %d", target);

        *peer = ompi_osc_rdma_module_peer (module, target);
        if (OPAL_UNLIKELY(OMPI_OSC_RDMA_LOCKING_ON_DEMAND == module->locking_mode &&
                          !ompi_osc_rdma_peer_is_demand_locked (*peer))) {
            ompi_osc_rdma_demand_lock_peer (module, *peer);
        }

        return &module->all_sync;
    case OMPI_OSC_RDMA_SYNC_TYPE_FENCE:
        OSC_RDMA_VERBOSE(MCA_BASE_VERBOSE_TRACE, "found fence access epoch for target %d", target);
        /* fence epoch is now active */
        module->all_sync.epoch_active = true;
        *peer = ompi_osc_rdma_module_peer (module, target);

        return &module->all_sync;
    case OMPI_OSC_RDMA_SYNC_TYPE_PSCW:
        if (ompi_osc_rdma_sync_pscw_peer (module, target, peer)) {
            OSC_RDMA_VERBOSE(MCA_BASE_VERBOSE_TRACE, "found PSCW access epoch target for %d", target);
            return &module->all_sync;
        }
    }

    OSC_RDMA_VERBOSE(MCA_BASE_VERBOSE_INFO, "no access epoch found for target %d", target);

    return NULL;
}

static bool ompi_osc_rdma_use_btl_flush (ompi_osc_rdma_module_t *module)
{
#if defined(BTL_VERSION) && (BTL_VERSION >= 310)
    return !!(module->selected_btl->btl_flush);
#else
    return false;
#endif
}

/**
 * @brief increment the outstanding rdma operation counter (atomic)
 *
 * @param[in] rdma_sync         osc rdma synchronization object
 */
static inline void ompi_osc_rdma_sync_rdma_inc_always (ompi_osc_rdma_sync_t *rdma_sync)
{
    ompi_osc_rdma_counter_add (&rdma_sync->outstanding_rdma.counter, 1);

    OSC_RDMA_VERBOSE(MCA_BASE_VERBOSE_INFO, "inc: there are %ld outstanding rdma operations",
                     (unsigned long) rdma_sync->outstanding_rdma.counter);
}

static inline void ompi_osc_rdma_sync_rdma_inc (ompi_osc_rdma_sync_t *rdma_sync)
{
#if defined(BTL_VERSION) && (BTL_VERSION >= 310)
    if (ompi_osc_rdma_use_btl_flush (rdma_sync->module)) {
        return;
    }
#endif
    ompi_osc_rdma_sync_rdma_inc_always (rdma_sync);
}

/**
 * @brief decrement the outstanding rdma operation counter (atomic)
 *
 * @param[in] rdma_sync         osc rdma synchronization object
 */
static inline void ompi_osc_rdma_sync_rdma_dec_always (ompi_osc_rdma_sync_t *rdma_sync)
{
    opal_atomic_wmb ();
    ompi_osc_rdma_counter_add (&rdma_sync->outstanding_rdma.counter, -1);

    OSC_RDMA_VERBOSE(MCA_BASE_VERBOSE_INFO, "dec: there are %ld outstanding rdma operations",
                     (unsigned long) rdma_sync->outstanding_rdma.counter);
}

static inline void ompi_osc_rdma_sync_rdma_dec (ompi_osc_rdma_sync_t *rdma_sync)
{
#if defined(BTL_VERSION) && (BTL_VERSION >= 310)
    if (ompi_osc_rdma_use_btl_flush (rdma_sync->module)) {
        return;
    }
#endif
    ompi_osc_rdma_sync_rdma_dec_always (rdma_sync);
}

/**
 * @brief complete all outstanding rdma operations to all peers
 *
 * @param[in] module          osc rdma module
 */
static inline void ompi_osc_rdma_sync_rdma_complete (ompi_osc_rdma_sync_t *sync)
{
#if !defined(BTL_VERSION) || (BTL_VERSION < 310)
    do {
        opal_progress ();
    }  while (ompi_osc_rdma_sync_get_count (sync));
#else
    mca_btl_base_module_t *btl_module = sync->module->selected_btl;

    do {
        if (!ompi_osc_rdma_use_btl_flush (sync->module)) {
            opal_progress ();
        } else {
            btl_module->btl_flush (btl_module, NULL);
        }
    }  while (ompi_osc_rdma_sync_get_count (sync) || (sync->module->rdma_frag && (sync->module->rdma_frag->pending > 1)));
#endif
}

/**
 * @brief check if an access epoch is active
 *
 * @param[in] module        osc rdma module
 *
 * @returns true if any type of access epoch is active
 * @returns false otherwise
 *
 * This function is used to check for conflicting access epochs.
 */
static inline bool ompi_osc_rdma_access_epoch_active (ompi_osc_rdma_module_t *module)
{
    return (module->all_sync.epoch_active || ompi_osc_rdma_in_passive_epoch (module));
}

__opal_attribute_always_inline__
static inline bool ompi_osc_rdma_oor (int rc)
{
    /* check for OPAL_SUCCESS first to short-circuit the statement in the common case */
    return (OPAL_SUCCESS != rc && (OPAL_ERR_OUT_OF_RESOURCE == rc || OPAL_ERR_TEMP_OUT_OF_RESOURCE == rc));
}

#endif /* OMPI_OSC_RDMA_H */
