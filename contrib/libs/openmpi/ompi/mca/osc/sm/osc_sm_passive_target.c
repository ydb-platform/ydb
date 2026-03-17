/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2011      Sandia National Laboratories.  All rights reserved.
 * Copyright (c) 2014-2015 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2015 Cisco Systems, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include "ompi/mca/osc/osc.h"
#include "ompi/mca/osc/base/base.h"
#include "ompi/mca/osc/base/osc_base_obj_convert.h"

#include "osc_sm.h"


static inline uint32_t
lk_fetch_add32(ompi_osc_sm_module_t *module,
               int target,
               size_t offset,
               uint32_t delta)
{
    /* opal_atomic_add_fetch_32 is an add then fetch so delta needs to be subtracted out to get the
     * old value */
    return opal_atomic_add_fetch_32((int32_t*) ((char*) &module->node_states[target].lock + offset),
                              delta) - delta;
}


static inline void
lk_add32(ompi_osc_sm_module_t *module,
         int target,
         size_t offset,
         uint32_t delta)
{
    opal_atomic_add_fetch_32((int32_t*) ((char*) &module->node_states[target].lock + offset),
                       delta);
}


static inline uint32_t
lk_fetch32(ompi_osc_sm_module_t *module,
           int target,
           size_t offset)
{
    opal_atomic_mb ();
    return *((uint32_t *)((char*) &module->node_states[target].lock + offset));
}


static inline int
start_exclusive(ompi_osc_sm_module_t *module,
                int target)
{
    uint32_t me = lk_fetch_add32(module, target,
                                 offsetof(ompi_osc_sm_lock_t, counter), 1);

    while (me != lk_fetch32(module, target,
                            offsetof(ompi_osc_sm_lock_t, write))) {
        opal_progress();
    }

    return OMPI_SUCCESS;
}


static inline int
end_exclusive(ompi_osc_sm_module_t *module,
              int target)
{
    lk_add32(module, target, offsetof(ompi_osc_sm_lock_t, write), 1);
    lk_add32(module, target, offsetof(ompi_osc_sm_lock_t, read), 1);

    return OMPI_SUCCESS;
}


static inline int
start_shared(ompi_osc_sm_module_t *module,
             int target)
{
    uint32_t me = lk_fetch_add32(module, target,
                                 offsetof(ompi_osc_sm_lock_t, counter), 1);

    while (me != lk_fetch32(module, target,
                            offsetof(ompi_osc_sm_lock_t, read))) {
        opal_progress();
    }

    lk_add32(module, target, offsetof(ompi_osc_sm_lock_t, read), 1);

    return OMPI_SUCCESS;
}


static inline int
end_shared(ompi_osc_sm_module_t *module,
           int target)
{
    lk_add32(module, target, offsetof(ompi_osc_sm_lock_t, write), 1);

    return OMPI_SUCCESS;
}


int
ompi_osc_sm_lock(int lock_type,
                 int target,
                 int assert,
                 struct ompi_win_t *win)
{
    ompi_osc_sm_module_t *module =
        (ompi_osc_sm_module_t*) win->w_osc_module;
    int ret;

    if (lock_none != module->outstanding_locks[target]) {
        return OMPI_ERR_RMA_SYNC;
    }

    if (0 == (assert & MPI_MODE_NOCHECK)) {
        if (MPI_LOCK_EXCLUSIVE == lock_type) {
            module->outstanding_locks[target] = lock_exclusive;
            ret = start_exclusive(module, target);
        } else {
            module->outstanding_locks[target] = lock_shared;
            ret = start_shared(module, target);
        }
    } else {
        module->outstanding_locks[target] = lock_nocheck;
        ret = OMPI_SUCCESS;
    }

    return ret;
}


int
ompi_osc_sm_unlock(int target,
                   struct ompi_win_t *win)
{
    ompi_osc_sm_module_t *module =
        (ompi_osc_sm_module_t*) win->w_osc_module;
    int ret;

    /* ensure all memory operations have completed */
    opal_atomic_mb();

    switch (module->outstanding_locks[target]) {
    case lock_none:
        return OMPI_ERR_RMA_SYNC;

    case lock_nocheck:
        ret = OMPI_SUCCESS;
        break;

    case lock_exclusive:
        ret = end_exclusive(module, target);
        break;

    case lock_shared:
        ret = end_shared(module, target);
        break;

    default:
        // This is an OMPI programming error -- cause some pain.
        assert(module->outstanding_locks[target] == lock_none ||
               module->outstanding_locks[target] == lock_nocheck ||
               module->outstanding_locks[target] == lock_exclusive ||
               module->outstanding_locks[target] == lock_shared);

         // In non-developer builds, assert() will be a no-op, so
         // ensure the error gets reported
        opal_output(0, "Unknown lock type in ompi_osc_sm_unlock -- this is an OMPI programming error");
        ret = OMPI_ERR_BAD_PARAM;
        break;
    }

    module->outstanding_locks[target] = lock_none;

    return ret;
}


int
ompi_osc_sm_lock_all(int assert,
                           struct ompi_win_t *win)
{
    ompi_osc_sm_module_t *module =
        (ompi_osc_sm_module_t*) win->w_osc_module;
    int ret, i, comm_size;

    comm_size = ompi_comm_size(module->comm);
    for (i = 0 ; i < comm_size ; ++i) {
        ret = ompi_osc_sm_lock(MPI_LOCK_SHARED, i, assert, win);
        if (OMPI_SUCCESS != ret) return ret;
    }

    return OMPI_SUCCESS;
}


int
ompi_osc_sm_unlock_all(struct ompi_win_t *win)
{
    ompi_osc_sm_module_t *module =
        (ompi_osc_sm_module_t*) win->w_osc_module;
    int ret, i, comm_size;

    comm_size = ompi_comm_size(module->comm);
    for (i = 0 ; i < comm_size ; ++i) {
        ret = ompi_osc_sm_unlock(i, win);
        if (OMPI_SUCCESS != ret) return ret;
    }

    return OMPI_SUCCESS;
}


int
ompi_osc_sm_sync(struct ompi_win_t *win)
{
    opal_atomic_mb();

    return OMPI_SUCCESS;
}


int
ompi_osc_sm_flush(int target,
                        struct ompi_win_t *win)
{
    opal_atomic_mb();

    return OMPI_SUCCESS;
}


int
ompi_osc_sm_flush_all(struct ompi_win_t *win)
{
    opal_atomic_mb();

    return OMPI_SUCCESS;
}


int
ompi_osc_sm_flush_local(int target,
                              struct ompi_win_t *win)
{
    opal_atomic_mb();

    return OMPI_SUCCESS;
}


int
ompi_osc_sm_flush_local_all(struct ompi_win_t *win)
{
    opal_atomic_mb();

    return OMPI_SUCCESS;
}
