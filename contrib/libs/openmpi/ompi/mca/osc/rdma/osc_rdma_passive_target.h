/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2014-2015 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#if !defined(OSC_RDMA_PASSIVE_TARGET_H)
#define OSC_RDMA_PASSIVE_TARGET_H

#include "osc_rdma.h"
#include "osc_rdma_sync.h"
#include "osc_rdma_lock.h"

/**
 * @brief lock the target in the window using network/cpu atomics
 *
 * @param[in] lock_type        mpi lock type (MPI_LOCK_SHARED, MPI_LOCK_EXCLUSIVE)
 * @param[in] target           target process
 * @param[in] assert           asserts
 * @param[in] win              mpi window
 *
 * @returns OMPI_SUCCESS on success
 * @returns OMPI_ERR_RMA_SYNC if there is a conflicting RMA epoch
 */
int ompi_osc_rdma_lock_atomic (int lock_type, int target, int assert, ompi_win_t *win);

/**
 * @brief unlock the target in the window using network/cpu atomics
 *
 * @param[in] target           target process
 * @param[in] win              mpi window
 *
 * @returns OMPI_SUCCESS on success
 * @returns OMPI_ERR_RMA_SYNC if the target is not locked
 */
int ompi_osc_rdma_unlock_atomic (int target, ompi_win_t *win);

/**
 * @brief lock all targets in window using network/cpu atomics
 *
 * @param[in] assert           asserts
 * @param[in] win              mpi window
 *
 * @returns OMPI_SUCCESS on success
 * @returns OMPI_ERR_RMA_SYNC if there is a conflicting RMA epoch
 */
int ompi_osc_rdma_lock_all_atomic (int assert, struct ompi_win_t *win);

/**
 * @brief unlock all targets in window using network/cpu atomics
 *
 * @param[in] assert           asserts
 * @param[in] win              mpi window
 *
 * @returns OMPI_SUCCESS on success
 * @returns OMPI_ERR_RMA_SYNC if the window is not in a lock all access epoch
 */
int ompi_osc_rdma_unlock_all_atomic (struct ompi_win_t *win);

/**
 * @brief synchronize the public and private copies of the window
 *
 * @param[in] win              mpi window
 *
 * @returns OMPI_SUCCESS on success
 *
 * Just acts as a memory barrier since this module only supports a unified memory
 * model.
 */
int ompi_osc_rdma_sync (struct ompi_win_t *win);

/**
 * @brief flush rdma transactions to a target
 *
 * @param[in] target           target process
 * @param[in] win              mpi window
 *
 * @returns OMPI_SUCCESS on success
 * @returns OMPI_ERR_RMA_SYNC if the target is not locked
 */
int ompi_osc_rdma_flush (int target, struct ompi_win_t *win);

/**
 * @brief flush rdma transactions to all target(s)
 *
 * @param[in] win              mpi window
 *
 * @returns OMPI_SUCCESS on success
 * @returns OMPI_ERR_RMA_SYNC if no processes are locked
 *
 * osc/rdma does not make a distinction between local and remote rma
 * completion. this could change in a future release as small messages
 * may be internally buffered.
 */
int ompi_osc_rdma_flush_all (struct ompi_win_t *win);

/**
 * @brief flush rdma transactions to a target (local completion)
 *
 * @param[in] target           target process
 * @param[in] win              mpi window
 *
 * @returns OMPI_SUCCESS on success
 * @returns OMPI_ERR_RMA_SYNC if the target is not locked
 *
 * osc/rdma does not make a distinction between local and remote rma
 * completion. this could change in a future release as small messages
 * may be internally buffered.
 */
int ompi_osc_rdma_flush_local (int target, struct ompi_win_t *win);

/**
 * @brief flush rdma transactions to all target(s) (local completion)
 *
 * @param[in] win              mpi window
 *
 * @returns OMPI_SUCCESS on success
 * @returns OMPI_ERR_RMA_SYNC if no processes are locked
 *
 * osc/rdma does not make a distinction between local and remote rma
 * completion. this could change in a future release as small messages
 * may be internally buffered.
 */
int ompi_osc_rdma_flush_local_all (struct ompi_win_t *win);

#endif
