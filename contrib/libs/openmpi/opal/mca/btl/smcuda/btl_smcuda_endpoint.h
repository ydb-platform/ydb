/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2006-2007 Voltaire. All rights reserved.
 * Copyright (c) 2012      NVIDIA Corporation.  All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/**
 * @file
 */
#ifndef MCA_BTL_SMCUDA_ENDPOINT_H
#define MCA_BTL_SMCUDA_ENDPOINT_H

/**
 *  An abstraction that represents a connection to a endpoint process.
 *  An instance of mca_ptl_base_endpoint_t is associated w/ each process
 *  and BTL pair at startup.
 */

struct mca_btl_base_endpoint_t {
    int my_smp_rank;    /**< My SMP process rank.  Used for accessing
                         *   SMP specfic data structures. */
    int peer_smp_rank;  /**< My peer's SMP process rank.  Used for accessing
                         *   SMP specfic data structures. */
#if OPAL_CUDA_SUPPORT
    mca_rcache_base_module_t *rcache; /**< rcache for remotely registered memory */
#endif /* OPAL_CUDA_SUPPORT */
#if OPAL_ENABLE_PROGRESS_THREADS == 1
    int fifo_fd;        /**< pipe/fifo used to signal endpoint that data is queued */
#endif
    opal_list_t pending_sends; /**< pending data to send */

    /** lock for concurrent access to endpoint state */
    opal_mutex_t endpoint_lock;

#if OPAL_CUDA_SUPPORT
    opal_proc_t *proc_opal;  /**< Needed for adding CUDA IPC support dynamically */
    enum ipcState ipcstate;  /**< CUDA IPC connection status */
    int ipctries;            /**< Number of times CUDA IPC connect was sent */
#endif /* OPAL_CUDA_SUPPORT */
};

void btl_smcuda_process_pending_sends(struct mca_btl_base_endpoint_t *ep);
#endif
