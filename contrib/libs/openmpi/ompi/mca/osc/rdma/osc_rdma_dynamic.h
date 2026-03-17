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

#include "osc_rdma.h"

/**
 * @brief attach a region to a window
 *
 * @param[in] win   mpi window
 * @param[in] base  base pointer of region
 * @param[in] len   region size
 *
 * @returns OMPI_SUCCESS on success
 * @returns OMPI_ERR_RMA_FLAVOR if the window is not a dynamic window
 * @returns OMPI_ERR_RMA_ATTACH if the region could not be attached
 *
 * This function attaches a region to the local window. After this call
 * completes the region will be available for RMA access by all peers in
 * the window.
 */
int ompi_osc_rdma_attach (struct ompi_win_t *win, void *base, size_t len);

/**
 * @brief detach a region from a window
 *
 * @param[in] win   mpi window
 * @param[in] base  base pointer of region specified to ompi_osc_rdma_attach()
 *
 * @returns OMPI_SUCCESS on success
 * @returns OMPI_ERR_RMA_FLAVOR if the window is not a dynamic window
 * @returns OMPI_ERROR if the region is not attached
 *
 * This function requires that a region with the same base has been attached
 * using the ompi_osc_rdma_attach() function.
 */
int ompi_osc_rdma_detach (struct ompi_win_t *win, const void *base);

/**
 * @brief find dynamic region associated with a peer, base, and len
 *
 * @param[in]  module   osc rdma module
 * @param[in]  peer     peer object for remote peer
 * @param[in]  base     base pointer for region
 * @param[in]  len      length of region
 * @param[out] region   region structure for the region
 *
 * @returns OMPI_SUCCESS on success
 * @returns OMPI_ERR_OUT_OF_RESOURCE on resource failure
 * @returns OMPI_ERR_RMA_RANGE if no region matches
 */
int ompi_osc_rdma_find_dynamic_region (ompi_osc_rdma_module_t *module, ompi_osc_rdma_peer_t *peer, uint64_t base, size_t len,
				       ompi_osc_rdma_region_t **region);
