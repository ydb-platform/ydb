/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2014-2015 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2017      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#if !defined(OMPI_OSC_RDMA_COMM_H)
#define OMPI_OSC_RDMA_COMM_H

#include "osc_rdma_dynamic.h"
#include "osc_rdma_request.h"
#include "osc_rdma_sync.h"
#include "osc_rdma_lock.h"

#define OMPI_OSC_RDMA_DECODE_MAX 64

#define min(a,b) ((a) < (b) ? (a) : (b))
#define ALIGNMENT_MASK(x) ((x) ? (x) - 1 : 0)

/**
 * @brief find a remote segment associate with the memory region
 *
 * @param[in]  module         osc rdma module
 * @param[in]  peer           peer object for remote peer
 * @param[in]  target_disp    displacement in remote region
 * @param[in]  length         length of remote region
 * @param[out] remote_address remote address
 * @param[out] remote_handle  btl handle for remote region (valid over entire region)
 *
 * @returns OMPI_SUCCESS on success
 * @returns OMPI_ERR_RMA_RANGE if the address range is not valid at the remote window
 * @returns other OMPI error on error
 */
static inline int osc_rdma_get_remote_segment (ompi_osc_rdma_module_t *module, ompi_osc_rdma_peer_t *peer, ptrdiff_t target_disp,
                                               size_t length, uint64_t *remote_address, mca_btl_base_registration_handle_t **remote_handle)
{
    ompi_osc_rdma_region_t *region;
    int ret;

    OSC_RDMA_VERBOSE(MCA_BASE_VERBOSE_TRACE, "getting remote address for peer %d target_disp %lu. peer flags: 0x%x",
                     peer->rank, (unsigned long) target_disp, peer->flags);

    if (MPI_WIN_FLAVOR_DYNAMIC == module->flavor) {
        ret = ompi_osc_rdma_find_dynamic_region (module, peer, (uint64_t) target_disp, length, &region);
        if (OMPI_SUCCESS != ret) {
            OSC_RDMA_VERBOSE(MCA_BASE_VERBOSE_INFO, "could not retrieve region for %" PRIx64 " from window rank %d",
                             (uint64_t) target_disp, peer->rank);
            return ret;
        }

        *remote_address = (uint64_t) target_disp;
        *remote_handle = (mca_btl_base_registration_handle_t *) region->btl_handle_data;
    } else {
        ompi_osc_rdma_peer_extended_t *ex_peer = (ompi_osc_rdma_peer_extended_t *) peer;
        int disp_unit = (module->same_disp_unit) ? module->disp_unit : ex_peer->disp_unit;
        size_t size = (module->same_size) ? module->size : (size_t) ex_peer->size;

        *remote_address = ex_peer->super.base + disp_unit * target_disp;
        if (OPAL_UNLIKELY(*remote_address + length > (ex_peer->super.base + size))) {
            OSC_RDMA_VERBOSE(MCA_BASE_VERBOSE_INFO, "remote address range 0x%" PRIx64 " - 0x%" PRIx64
                             " is out of range. Valid address range is 0x%" PRIx64 " - 0x%" PRIx64 " (%" PRIu64 " bytes)",
                             *remote_address, *remote_address + length, ex_peer->super.base, ex_peer->super.base + size,
                             (uint64_t) size);
            return OMPI_ERR_RMA_RANGE;
        }

        *remote_handle = ex_peer->super.base_handle;
    }

    OSC_RDMA_VERBOSE(MCA_BASE_VERBOSE_TRACE, "remote address: 0x%" PRIx64 ", handle: %p", *remote_address, (void *) *remote_handle);

    return OMPI_SUCCESS;
}

/* prototypes for implementations of MPI RMA window functions. these will be called from the
 * mpi interface (ompi/mpi/c) */
int ompi_osc_rdma_put (const void *origin_addr, int origin_count, ompi_datatype_t *origin_dt,
                       int target, ptrdiff_t target_disp, int target_count,
                       ompi_datatype_t *target_dt, ompi_win_t *win);

int ompi_osc_rdma_get (void *origin_addr, int origin_count, ompi_datatype_t *origin_dt,
                       int target, ptrdiff_t target_disp, int target_count,
                       ompi_datatype_t *target_dt, ompi_win_t *win);

int ompi_osc_rdma_rput (const void *origin_addr, int origin_count, ompi_datatype_t *origin_dt,
                        int target, ptrdiff_t target_disp, int target_count,
                        ompi_datatype_t *target_dt, ompi_win_t *win,
                        ompi_request_t **request);

int ompi_osc_rdma_rget (void *origin_addr, int origin_count, ompi_datatype_t *origin_dt,
                        int target, ptrdiff_t target_disp, int target_count,
                        ompi_datatype_t *target_dt, ompi_win_t *win,
                        ompi_request_t **request);

/**
 * @brief read data from a remote memory region (blocking)
 *
 * @param[in] module          osc rdma module
 * @param[in] endpoint        btl endpoint
 * @param[in] source_address  remote address to read from
 * @param[in] source_handle   btl registration handle for remote region (must be valid for the entire region)
 * @param[in] data            local buffer to store to
 * @param[in] len             number of bytes to read
 *
 * This is an internal function for reading data from a remote peer. It is used to read peer and state
 * data that is stored on the remote peer. The peer object does not have to be fully initialized to
 * work. Only the btl endpoint is needed.
 */
int ompi_osc_get_data_blocking (ompi_osc_rdma_module_t *module, struct mca_btl_base_endpoint_t *endpoint,
                                uint64_t source_address, mca_btl_base_registration_handle_t *source_handle,
                                void *data, size_t len);

int ompi_osc_rdma_put_contig (ompi_osc_rdma_sync_t *sync, ompi_osc_rdma_peer_t *peer, uint64_t target_address,
                              mca_btl_base_registration_handle_t *target_handle, void *source_buffer, size_t size,
                              ompi_osc_rdma_request_t *request);

#endif /* OMPI_OSC_RDMA_COMM_H */
