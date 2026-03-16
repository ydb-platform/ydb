/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2015-2016 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef OMPI_OSC_PT2PT_SYNC_H
#define OMPI_OSC_PT2PT_SYNC_H

#include "ompi_config.h"
#include "opal/class/opal_free_list.h"
#include "opal/threads/threads.h"

enum ompi_osc_pt2pt_sync_type_t {
    /** default value */
    OMPI_OSC_PT2PT_SYNC_TYPE_NONE,
    /** lock access epoch */
    OMPI_OSC_PT2PT_SYNC_TYPE_LOCK,
    /** fence access epoch */
    OMPI_OSC_PT2PT_SYNC_TYPE_FENCE,
    /* post-start-complete-wait access epoch */
    OMPI_OSC_PT2PT_SYNC_TYPE_PSCW,
};
typedef enum ompi_osc_pt2pt_sync_type_t ompi_osc_pt2pt_sync_type_t;

struct ompi_osc_pt2pt_module_t;
struct ompi_osc_pt2pt_peer_t;

/**
 * @brief synchronization object
 *
 * This structure holds information about an access epoch.
 */
struct ompi_osc_pt2pt_sync_t {
    opal_free_list_item_t super;

    struct ompi_osc_pt2pt_module_t *module;

    /** synchronization type */
    ompi_osc_pt2pt_sync_type_t type;

    /** synchronization data */
    union {
        /** lock specific synchronization data */
        struct {
            /** lock target rank (-1 for all) */
            int target;
            /** lock type: MPI_LOCK_SHARED, MPI_LOCK_EXCLUSIVE */
            int type;
            /** assert specified at lock acquire time */
            int assert;
        } lock;
        /** post/start/complete/wait specific synchronization data */
        struct {
            /** group passed to ompi_osc_pt2pt_start */
            ompi_group_t *group;
        } pscw;
    } sync;

    /** array of peers for this sync */
    union {
        /** multiple peers (lock all, pscw, fence) */
	struct ompi_osc_pt2pt_peer_t **peers;
        /** single peer (targeted lock) */
	struct ompi_osc_pt2pt_peer_t *peer;
    } peer_list;

    /** number of peers */
    int num_peers;

    /** number of synchronization messages expected */
    volatile int32_t sync_expected;

    /** eager sends are active to all peers in this access epoch */
    volatile bool eager_send_active;

    /** communication has started on this epoch */
    bool epoch_active;

    /** lock to protect sync structure members */
    opal_mutex_t lock;

    /** condition variable for changes in the sync object */
    opal_condition_t cond;
};
typedef struct ompi_osc_pt2pt_sync_t ompi_osc_pt2pt_sync_t;

OBJ_CLASS_DECLARATION(ompi_osc_pt2pt_sync_t);

/**
 * @brief allocate a new synchronization object
 *
 * @param[in] module   osc pt2pt module
 *
 * @returns NULL on failure
 * @returns a new synchronization object on success
 */
ompi_osc_pt2pt_sync_t *ompi_osc_pt2pt_sync_allocate (struct ompi_osc_pt2pt_module_t *module);

/**
 * @brief release a synchronization object
 *
 * @param[in] pt2pt_sync   synchronization object allocated by ompi_osc_pt2pt_sync_allocate()
 */
void ompi_osc_pt2pt_sync_return (ompi_osc_pt2pt_sync_t *pt2pt_sync);

/**
 * Check if the target is part of a PSCW access epoch
 *
 * @param[in] module   osc pt2pt module
 * @param[in] target   target rank
 * @param[out] peer    peer object
 *
 * @returns false if the window is not in a PSCW access epoch or the peer is not
 *          in the group passed to MPI_Win_start
 * @returns true otherwise
 *
 * This functions verifies the target is part of an active PSCW access epoch.
 */
bool ompi_osc_pt2pt_sync_pscw_peer (struct ompi_osc_pt2pt_module_t *module, int target, struct ompi_osc_pt2pt_peer_t **peer);

/**
 * Wait for all remote peers in the synchronization to respond
 */
static inline void ompi_osc_pt2pt_sync_wait_nolock (ompi_osc_pt2pt_sync_t *sync)
{
    while (!sync->eager_send_active) {
        OPAL_OUTPUT_VERBOSE((50, ompi_osc_base_framework.framework_output,
                             "waiting for access epoch to start"));
        opal_condition_wait(&sync->cond, &sync->lock);
    }

    OPAL_OUTPUT_VERBOSE((50, ompi_osc_base_framework.framework_output,
                         "access epoch ready"));
}

static inline void ompi_osc_pt2pt_sync_wait (ompi_osc_pt2pt_sync_t *sync)
{
    OPAL_THREAD_LOCK(&sync->lock);
    ompi_osc_pt2pt_sync_wait_nolock (sync);
    OPAL_THREAD_UNLOCK(&sync->lock);
}

/**
 * Wait for all remote peers in the synchronization to respond
 */
static inline void ompi_osc_pt2pt_sync_wait_expected (ompi_osc_pt2pt_sync_t *sync)
{
    OPAL_THREAD_LOCK(&sync->lock);
    while (sync->sync_expected) {
        OPAL_OUTPUT_VERBOSE((50, ompi_osc_base_framework.framework_output,
                             "waiting for %d syncronization messages",
                             sync->sync_expected));
        opal_condition_wait(&sync->cond, &sync->lock);
    }
    OPAL_THREAD_UNLOCK(&sync->lock);

    OPAL_OUTPUT_VERBOSE((50, ompi_osc_base_framework.framework_output,
                         "all synchronization messages received"));
}

static inline void ompi_osc_pt2pt_sync_expected (ompi_osc_pt2pt_sync_t *sync)
{
    int32_t new_value = OPAL_THREAD_ADD_FETCH32 (&sync->sync_expected, -1);
    if (0 == new_value) {
        OPAL_THREAD_LOCK(&sync->lock);
        if (!(sync->type == OMPI_OSC_PT2PT_SYNC_TYPE_LOCK && sync->num_peers > 1)) {
            sync->eager_send_active = true;
        }
        opal_condition_broadcast (&sync->cond);
        OPAL_THREAD_UNLOCK(&sync->lock);
    }
}

static inline void ompi_osc_pt2pt_sync_reset (ompi_osc_pt2pt_sync_t *sync)
{
    sync->type = OMPI_OSC_PT2PT_SYNC_TYPE_NONE;
    sync->eager_send_active = false;
    sync->epoch_active = 0;
    sync->peer_list.peers = NULL;
    sync->sync.pscw.group = NULL;
}

#endif /* OMPI_OSC_PT2PT_SYNC_H */
