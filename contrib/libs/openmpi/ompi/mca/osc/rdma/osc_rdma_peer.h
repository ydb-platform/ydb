/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2014-2018 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef OMPI_OSC_RDMA_PEER_H
#define OMPI_OSC_RDMA_PEER_H

#include "osc_rdma_types.h"

struct ompi_osc_rdma_module_t;

/**
 * @brief osc rdma peer object
 *
 * This object is used as a cache for information associated with a peer.
 */
struct ompi_osc_rdma_peer_t {
    opal_list_item_t super;

    /** rdma data endpoint for this peer */
    struct mca_btl_base_endpoint_t *data_endpoint;

    /** endpoint for reading/modifying peer state */
    struct mca_btl_base_endpoint_t *state_endpoint;

    /** remote peer's state pointer */
    osc_rdma_base_t state;

    /** registration handle associated with the state */
    mca_btl_base_registration_handle_t *state_handle;

    /** lock to protrct peer structure */
    opal_mutex_t lock;

    /** rank of this peer in the window */
    int rank;

    /** peer flags */
    volatile int32_t flags;
};
typedef struct ompi_osc_rdma_peer_t ompi_osc_rdma_peer_t;

/**
 * @brief peer object used when using dynamic windows
 */
struct ompi_osc_rdma_peer_dynamic_t {
    ompi_osc_rdma_peer_t super;

    /** last region id seen for this peer */
    uint32_t region_id;

    /** number of regions in the regions array */
    uint32_t region_count;

    /** cached array of attached regions for this peer */
    struct ompi_osc_rdma_region_t *regions;
};

typedef struct ompi_osc_rdma_peer_dynamic_t ompi_osc_rdma_peer_dynamic_t;

/**
 * @brief basic peer object for non-dynamic windows used when all peers
 *        have the same displacement unit and size
 */
struct ompi_osc_rdma_peer_basic_t {
    ompi_osc_rdma_peer_t super;

    /** remote peer's base pointer */
    osc_rdma_base_t base;

    /** local pointer to peer's base */
    osc_rdma_base_t local_base;

    /** registration handle associated with the base */
    mca_btl_base_registration_handle_t *base_handle;
};

typedef struct ompi_osc_rdma_peer_basic_t ompi_osc_rdma_peer_basic_t;

/**
 * @brief peer object used when no assumption can be made about the
 *        peer's displacement unit or size
 */
struct ompi_osc_rdma_peer_extended_t {
    ompi_osc_rdma_peer_basic_t super;

    /** remote peer's region size */
    osc_rdma_size_t size;

    /** displacement unit */
    int disp_unit;
};

typedef struct ompi_osc_rdma_peer_extended_t ompi_osc_rdma_peer_extended_t;

/**
 * @brief object class declarations
 */
OBJ_CLASS_DECLARATION(ompi_osc_rdma_peer_t);
OBJ_CLASS_DECLARATION(ompi_osc_rdma_peer_dynamic_t);
OBJ_CLASS_DECLARATION(ompi_osc_rdma_peer_basic_t);
OBJ_CLASS_DECLARATION(ompi_osc_rdma_peer_extended_t);

/**
 * @brief used to identify the node and local rank of a peer
 */
struct ompi_osc_rdma_rank_data_t {
    /** index of none in none_comm_info array */
    unsigned int node_id;
    /** local rank of process */
    unsigned int rank;
};
typedef struct ompi_osc_rdma_rank_data_t ompi_osc_rdma_rank_data_t;

enum {
    /** peer is locked for exclusive access */
    OMPI_OSC_RDMA_PEER_EXCLUSIVE            = 0x01,
    /** peer's base is accessible with direct loads/stores */
    OMPI_OSC_RDMA_PEER_LOCAL_BASE           = 0x02,
    /** peer state is local */
    OMPI_OSC_RDMA_PEER_LOCAL_STATE          = 0x04,
    /** currently accumulating on peer */
    OMPI_OSC_RDMA_PEER_ACCUMULATING         = 0x08,
    /** peer is in an active access epoch (pscw) */
    OMPI_OSC_RDMA_PEER_ACCESS_ACTIVE_EPOCH  = 0x10,
    /** peer state handle should be freed */
    OMPI_OSC_RDMA_PEER_STATE_FREE           = 0x20,
    /** peer base handle should be freed */
    OMPI_OSC_RDMA_PEER_BASE_FREE            = 0x40,
    /** peer was demand locked as part of lock-all (when in demand locking mode) */
    OMPI_OSC_RDMA_PEER_DEMAND_LOCKED        = 0x80,
};

/**
 * @brief allocate a peer object and initialize some of it structures
 *
 * @param[in]  module         osc rdma module
 * @param[in]  peer_id        peer's rank in the communicator
 * @param[out] peer_out       new peer object
 *
 * The type of the object returned depends on the window settings. For example for a dynamic window
 * this will return a peer of type \ref ompi_osc_rdma_peer_dynamic_t.
 */
int ompi_osc_rdma_new_peer (struct ompi_osc_rdma_module_t *module, int peer_id, ompi_osc_rdma_peer_t **peer_out);

/**
 * @brief lookup (or allocate) a peer
 *
 * @param[in]  module         osc rdma module
 * @param[in]  peer_id        peer's rank in the communicator
 *
 * This function is used by the ompi_osc_rdma_module_peer() inline function to allocate a peer object. It is not
 * intended to be called from anywhere else.
 */
struct ompi_osc_rdma_peer_t *ompi_osc_rdma_peer_lookup (struct ompi_osc_rdma_module_t *module, int peer_id);

/**
 * @brief lookup the btl endpoint for a peer
 *
 * @param[in]  module         osc rdma module
 * @param[in]  peer_id        peer's rank in the communicator
 *
 * @returns btl endpoint for the peer on success
 * @returns NULL on failure
 */
struct mca_btl_base_endpoint_t *ompi_osc_rdma_peer_btl_endpoint (struct ompi_osc_rdma_module_t *module, int peer_id);

/**
 * @brief check if this process holds an exclusive lock on a peer
 *
 * @param[in] peer            peer object to check
 */
static inline bool ompi_osc_rdma_peer_is_exclusive (ompi_osc_rdma_peer_t *peer)
{
    return !!(peer->flags & OMPI_OSC_RDMA_PEER_EXCLUSIVE);
}

/**
 * @brief try to set a flag on a peer object
 *
 * @param[in] peer            peer object to modify
 * @param[in] flag            flag to set
 *
 * @returns true if the flag was not already set
 * @returns flase otherwise
 */
static inline bool ompi_osc_rdma_peer_test_set_flag (ompi_osc_rdma_peer_t *peer, int flag)
{
    int32_t flags;

    opal_atomic_mb ();
    flags = peer->flags;

    do {
        if (flags & flag) {
            return false;
        }
    } while (!OPAL_ATOMIC_COMPARE_EXCHANGE_STRONG_32 (&peer->flags, &flags, flags | flag));

    return true;
}

/**
 * @brief clear a flag from a peer object
 *
 * @param[in] peer            peer object to modify
 * @param[in] flag            flag to set
 */
static inline void ompi_osc_rdma_peer_clear_flag (ompi_osc_rdma_peer_t *peer, int flag)
{
    OPAL_ATOMIC_AND_FETCH32(&peer->flags, ~flag);
    opal_atomic_mb ();
}

/**
 * @brief check if the peer's base pointer is local to this process
 *
 * @param[in] peer            peer object to check
 */
static inline bool ompi_osc_rdma_peer_local_base (ompi_osc_rdma_peer_t *peer)
{
    return !!(peer->flags & OMPI_OSC_RDMA_PEER_LOCAL_BASE);
}

/**
 * @brief check if the peer's state pointer is local to this process
 *
 * @param[in] peer            peer object to check
 *
 * The OMPI_OSC_RDMA_PEER_LOCAL_STATE flag will only be set if either 1) we
 * will not be mixing btl atomics and cpu atomics, or 2) it is safe to mix
 * btl and cpu atomics.
 */
static inline bool ompi_osc_rdma_peer_local_state (ompi_osc_rdma_peer_t *peer)
{
    return !!(peer->flags & OMPI_OSC_RDMA_PEER_LOCAL_STATE);
}

/**
 * @brief check if the peer has been demand locked as part of the current epoch
 *
 * @param[in] peer            peer object to check
 *
 */
static inline bool ompi_osc_rdma_peer_is_demand_locked (ompi_osc_rdma_peer_t *peer)
{
    return !!(peer->flags & OMPI_OSC_RDMA_PEER_DEMAND_LOCKED);
}

#endif /* OMPI_OSC_RDMA_PEER_H */
