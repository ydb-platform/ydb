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
 * Copyright (c) 2006-2007 Voltaire All rights reserved.
 * Copyright (c) 2007      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2010      Oracle and/or its affiliates.  All rights reserved.
 * Copyright (c) 2013      NVIDIA Corporation.  All rights reserved.
 * Copyright (c) 2016-2018 Los Alamos National Security, LLC. All rights
 *                         reserved.
 *
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"

#include <stdio.h>

#include "opal/util/output.h"
#include "opal/constants.h"
#include "opal/mca/btl/btl.h"
#include "opal/mca/btl/base/base.h"

int mca_btl_base_param_register(mca_base_component_t *version,
                                mca_btl_base_module_t *module)
{
    /* If this is ever triggered change the uint32_ts in mca_btl_base_module_t to unsigned ints */
    assert(sizeof(unsigned int) == sizeof(uint32_t));

    (void) mca_base_component_var_register(version, "exclusivity",
                                           "BTL exclusivity (must be >= 0)",
                                           MCA_BASE_VAR_TYPE_UNSIGNED_INT, NULL, 0, 0,
                                           OPAL_INFO_LVL_7,
                                           MCA_BASE_VAR_SCOPE_READONLY,
                                           &module->btl_exclusivity);

    (void) mca_base_component_var_register(version, "flags", "BTL bit flags (general flags: send, put, get, in-place, hetero-rdma, "
                                           "atomics, fetching-atomics)", MCA_BASE_VAR_TYPE_UNSIGNED_INT,
                                           &mca_btl_base_flag_enum->super, 0, 0, OPAL_INFO_LVL_5,
                                           MCA_BASE_VAR_SCOPE_READONLY, &module->btl_flags);

    (void) mca_base_component_var_register(version, "atomic_flags", "BTL atomic support flags", MCA_BASE_VAR_TYPE_UNSIGNED_INT,
                                           &mca_btl_base_atomic_enum->super, 0, MCA_BASE_VAR_FLAG_DEFAULT_ONLY, OPAL_INFO_LVL_5,
                                           MCA_BASE_VAR_SCOPE_CONSTANT, &module->btl_atomic_flags);

    (void) mca_base_component_var_register(version, "rndv_eager_limit", "Size (in bytes, including header) of \"phase 1\" fragment sent for all large messages (must be >= 0 and <= eager_limit)",
                                           MCA_BASE_VAR_TYPE_SIZE_T, NULL, 0, 0,
                                           OPAL_INFO_LVL_4,
                                           MCA_BASE_VAR_SCOPE_READONLY,
                                           &module->btl_rndv_eager_limit);

    (void) mca_base_component_var_register(version, "eager_limit", "Maximum size (in bytes, including header) of \"short\" messages (must be >= 1).",
                                           MCA_BASE_VAR_TYPE_SIZE_T, NULL, 0, 0,
                                           OPAL_INFO_LVL_4,
                                           MCA_BASE_VAR_SCOPE_READONLY,
                                           &module->btl_eager_limit);

    if ((module->btl_flags & MCA_BTL_FLAGS_GET) && module->btl_get) {
	if (0 == module->btl_get_limit) {
	    module->btl_get_limit = SIZE_MAX;
	}

	(void) mca_base_component_var_register(version, "get_limit", "Maximum size (in bytes) for btl get",
					       MCA_BASE_VAR_TYPE_SIZE_T, NULL, 0, 0, OPAL_INFO_LVL_4,
					       MCA_BASE_VAR_SCOPE_READONLY, &module->btl_get_limit);

	/* Allow the user to set the alignment. The BTL should double-check the alignment in its open
	 * function. */
	(void) mca_base_component_var_register(version, "get_alignment", "Alignment required for btl get",
					       MCA_BASE_VAR_TYPE_SIZE_T, NULL, 0, 0, OPAL_INFO_LVL_6,
					       MCA_BASE_VAR_SCOPE_CONSTANT, &module->btl_get_alignment);
    }

    if ((module->btl_flags & MCA_BTL_FLAGS_PUT) && module->btl_put) {
	if (0 == module->btl_put_limit) {
	    module->btl_put_limit = SIZE_MAX;
	}
	(void) mca_base_component_var_register(version, "put_limit", "Maximum size (in bytes) for btl put",
					       MCA_BASE_VAR_TYPE_SIZE_T, NULL, 0, 0, OPAL_INFO_LVL_4,
					       MCA_BASE_VAR_SCOPE_READONLY, &module->btl_put_limit);

	/* Allow the user to set the alignment. The BTL should double-check the alignment in its open
	 * function. */
	(void) mca_base_component_var_register(version, "put_alignment", "Alignment required for btl put",
					       MCA_BASE_VAR_TYPE_SIZE_T, NULL, 0, 0, OPAL_INFO_LVL_6,
					       MCA_BASE_VAR_SCOPE_CONSTANT, &module->btl_put_alignment);
    }


#if OPAL_CUDA_GDR_SUPPORT
    /* If no CUDA RDMA support, zero them out */
    if (!(MCA_BTL_FLAGS_CUDA_GET & module->btl_flags)) {
        module->btl_cuda_eager_limit = 0;
        module->btl_cuda_rdma_limit = SIZE_MAX;
    }
    (void) mca_base_component_var_register(version, "cuda_eager_limit", "Maximum size (in bytes, including header) of \"GPU short\" messages (must be >= 1).",
                                           MCA_BASE_VAR_TYPE_SIZE_T, NULL, 0, 0,
                                           OPAL_INFO_LVL_5,
                                           MCA_BASE_VAR_SCOPE_READONLY,
                                           &module->btl_cuda_eager_limit);
    (void) mca_base_component_var_register(version, "cuda_rdma_limit", "Size (in bytes, including header) of GPU buffer when switch to rndv protocol and pipeline.",
                                           MCA_BASE_VAR_TYPE_SIZE_T, NULL, 0, 0,
                                           OPAL_INFO_LVL_5,
                                           MCA_BASE_VAR_SCOPE_READONLY,
                                           &module->btl_cuda_rdma_limit);
#endif /* OPAL_CUDA_GDR_SUPPORT */
#if OPAL_CUDA_SUPPORT
    module->btl_cuda_max_send_size = 0;
    (void) mca_base_component_var_register(version, "cuda_max_send_size", "Maximum size (in bytes) of a single GPU \"phase 2\" fragment of a long message when using the pipeline protocol (must be >= 1) (only valid on smcuda btl)",
                                           MCA_BASE_VAR_TYPE_SIZE_T, NULL, 0, 0,
                                           OPAL_INFO_LVL_4,
                                           MCA_BASE_VAR_SCOPE_READONLY,
                                           &module->btl_cuda_max_send_size);
#endif /* OPAL_CUDA_SUPPORT */

    (void) mca_base_component_var_register(version, "max_send_size", "Maximum size (in bytes) of a single \"phase 2\" fragment of a long message when using the pipeline protocol (must be >= 1)",
                                           MCA_BASE_VAR_TYPE_SIZE_T, NULL, 0, 0,
                                           OPAL_INFO_LVL_4,
                                           MCA_BASE_VAR_SCOPE_READONLY,
                                           &module->btl_max_send_size);

    if (NULL != module->btl_put) {
      (void) mca_base_component_var_register(version, "rdma_pipeline_send_length", "Length of the \"phase 2\" portion of a large message (in bytes) when using the pipeline protocol.  This part of the message will be split into fragments of size max_send_size and sent using send/receive semantics (must be >= 0; only relevant when the PUT flag is set)",
                                             MCA_BASE_VAR_TYPE_SIZE_T, NULL, 0, 0,
                                             OPAL_INFO_LVL_4,
                                             MCA_BASE_VAR_SCOPE_READONLY,
                                             &module->btl_rdma_pipeline_send_length);

      (void) mca_base_component_var_register(version, "rdma_pipeline_frag_size", "Maximum size (in bytes) of a single \"phase 3\" fragment from a long message when using the pipeline protocol.  These fragments will be sent using RDMA semantics (must be >= 1; only relevant when the PUT flag is set)",
                                             MCA_BASE_VAR_TYPE_SIZE_T, NULL, 0, 0,
                                             OPAL_INFO_LVL_4,
                                             MCA_BASE_VAR_SCOPE_READONLY,
                                             &module->btl_rdma_pipeline_frag_size);

      (void) mca_base_component_var_register(version, "min_rdma_pipeline_size", "Messages smaller than this size (in bytes) will not use the RDMA pipeline protocol.  Instead, they will be split into fragments of max_send_size and sent using send/receive semantics (must be >=0, and is automatically adjusted up to at least (eager_limit+btl_rdma_pipeline_send_length); only relevant when the PUT flag is set)",
                                             MCA_BASE_VAR_TYPE_SIZE_T, NULL, 0, 0,
                                             OPAL_INFO_LVL_4,
                                             MCA_BASE_VAR_SCOPE_READONLY,
                                             &module->btl_min_rdma_pipeline_size);

      (void) mca_base_component_var_register(version, "latency", "Approximate latency of interconnect (0 = auto-detect value at run-time [not supported in all BTL modules], >= 1 = latency in microseconds)",
                                             MCA_BASE_VAR_TYPE_UNSIGNED_INT, NULL, 0, 0,
                                             OPAL_INFO_LVL_5,
                                             MCA_BASE_VAR_SCOPE_READONLY,
                                             &module->btl_latency);
      (void) mca_base_component_var_register(version, "bandwidth", "Approximate maximum bandwidth of interconnect (0 = auto-detect value at run-time [not supported in all BTL modules], >= 1 = bandwidth in Mbps)",
                                             MCA_BASE_VAR_TYPE_UNSIGNED_INT, NULL, 0, 0,
                                             OPAL_INFO_LVL_5,
                                             MCA_BASE_VAR_SCOPE_READONLY,
                                             &module->btl_bandwidth);
    }

    return mca_btl_base_param_verify(module);
}

/* Verify btl parameters make sense */
int mca_btl_base_param_verify(mca_btl_base_module_t *module)
{
    if (module->btl_min_rdma_pipeline_size <
        (module->btl_eager_limit + module->btl_rdma_pipeline_send_length)) {
        module->btl_min_rdma_pipeline_size =
            module->btl_eager_limit + module->btl_rdma_pipeline_send_length;
    }

    if (NULL == module->btl_put) {
        module->btl_flags &= ~MCA_BTL_FLAGS_PUT;
    }

    if (NULL == module->btl_get) {
        module->btl_flags &= ~MCA_BTL_FLAGS_GET;
    }

    if (NULL == module->btl_flush) {
        module->btl_flags &= ~MCA_BTL_FLAGS_RDMA_FLUSH;
    }

    if (0 == module->btl_atomic_flags) {
        module->btl_flags &= ~MCA_BTL_FLAGS_ATOMIC_OPS;
    }

    if (0 == module->btl_get_limit) {
	module->btl_get_limit = SIZE_MAX;
    }

    if (0 == module->btl_put_limit) {
	module->btl_put_limit = SIZE_MAX;
    }

    return OPAL_SUCCESS;
}
