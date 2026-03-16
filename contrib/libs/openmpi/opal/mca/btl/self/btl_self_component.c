/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2006 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2010      Cisco Systems, Inc. All rights reserved.
 * Copyright (c) 2014-2016 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
#include "opal_config.h"

#include "btl_self.h"
#include "btl_self_frag.h"

static int mca_btl_self_component_register(void);
static int mca_btl_self_component_open(void);
static int mca_btl_self_component_close(void);

/**
 * SELF module initialization.
 *
 * @param num_btls (OUT)                  Number of BTLs returned in BTL array.
 * @param enable_progress_threads (IN)    Flag indicating whether BTL is allowed to have progress threads
 * @param enable_mpi_threads (IN)         Flag indicating whether BTL must support multilple simultaneous invocations from different threads
 *
 */
static mca_btl_base_module_t **mca_btl_self_component_init (int *num_btls,
                                                            bool enable_progress_threads,
                                                            bool enable_mpi_threads);

/*
 * Shared Memory (SELF) component instance.
 */

mca_btl_self_component_t mca_btl_self_component = {
    .super = {
        /* First, the mca_base_component_t struct containing meta information
          about the component itself */
        .btl_version = {
            MCA_BTL_DEFAULT_VERSION("self"),
            .mca_open_component = mca_btl_self_component_open,
            .mca_close_component = mca_btl_self_component_close,
            .mca_register_component_params = mca_btl_self_component_register,
        },
        .btl_data = {
            /* The component is checkpoint ready */
            .param_field = MCA_BASE_METADATA_PARAM_CHECKPOINT,
        },

        .btl_init = mca_btl_self_component_init,
    }  /* end super */
};

/*
 *  Called by MCA framework to open the component, registers
 *  component parameters.
 */

static int mca_btl_self_component_register(void)
{
    mca_base_var_group_component_register(&mca_btl_self_component.super.btl_version,
                                          "BTL for self communication");

    /* register SELF component parameters */
    mca_btl_self_component.free_list_num = 0;
    (void) mca_base_component_var_register(&mca_btl_self_component.super.btl_version, "free_list_num",
                                           "Number of fragments by default",
                                           MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                           OPAL_INFO_LVL_9,
                                           MCA_BASE_VAR_SCOPE_READONLY,
                                           &mca_btl_self_component.free_list_num);
    /* NTH: free list buffers are not released until we tear down so DO NOT make them unlimited here */
    mca_btl_self_component.free_list_max = 64;
    (void) mca_base_component_var_register(&mca_btl_self_component.super.btl_version, "free_list_max",
                                           "Maximum number of fragments",
                                           MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                           OPAL_INFO_LVL_9,
                                           MCA_BASE_VAR_SCOPE_READONLY,
                                           &mca_btl_self_component.free_list_max);
    mca_btl_self_component.free_list_inc = 8;
    (void) mca_base_component_var_register(&mca_btl_self_component.super.btl_version, "free_list_inc",
                                           "Increment by this number of fragments",
                                           MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                           OPAL_INFO_LVL_9,
                                           MCA_BASE_VAR_SCOPE_READONLY,
                                           &mca_btl_self_component.free_list_inc);

    mca_btl_self.btl_exclusivity = MCA_BTL_EXCLUSIVITY_HIGH;
    mca_btl_self.btl_eager_limit = 1024;
    mca_btl_self.btl_rndv_eager_limit = 128 * 1024;
    mca_btl_self.btl_max_send_size = 16 * 1024;
    mca_btl_self.btl_rdma_pipeline_send_length = INT_MAX;
    mca_btl_self.btl_rdma_pipeline_frag_size = INT_MAX;
    mca_btl_self.btl_min_rdma_pipeline_size = 0;
    mca_btl_self.btl_flags = MCA_BTL_FLAGS_RDMA | MCA_BTL_FLAGS_SEND_INPLACE | MCA_BTL_FLAGS_SEND;
    mca_btl_self.btl_bandwidth = 100;
    mca_btl_self.btl_latency = 0;
    mca_btl_base_param_register (&mca_btl_self_component.super.btl_version, &mca_btl_self);

    return OPAL_SUCCESS;
}

static int mca_btl_self_component_open(void)
{
    /* initialize objects */
    OBJ_CONSTRUCT(&mca_btl_self_component.self_frags_eager, opal_free_list_t);
    OBJ_CONSTRUCT(&mca_btl_self_component.self_frags_send, opal_free_list_t);
    OBJ_CONSTRUCT(&mca_btl_self_component.self_frags_rdma, opal_free_list_t);

    return OPAL_SUCCESS;
}


/*
 * component cleanup - sanity checking of queue lengths
 */

static int mca_btl_self_component_close(void)
{
    OBJ_DESTRUCT(&mca_btl_self_component.self_frags_eager);
    OBJ_DESTRUCT(&mca_btl_self_component.self_frags_send);
    OBJ_DESTRUCT(&mca_btl_self_component.self_frags_rdma);
    return OPAL_SUCCESS;
}

/*
 *  SELF component initialization
 */
static mca_btl_base_module_t **mca_btl_self_component_init (int *num_btls,
                                                            bool enable_progress_threads,
                                                            bool enable_mpi_threads)
{
    mca_btl_base_module_t **btls = NULL;
    int ret;

    /* initialize free lists */
    ret = opal_free_list_init (&mca_btl_self_component.self_frags_eager,
                               sizeof (mca_btl_self_frag_eager_t) + mca_btl_self.btl_eager_limit,
                               opal_cache_line_size, OBJ_CLASS(mca_btl_self_frag_eager_t), 0,
                               opal_cache_line_size, mca_btl_self_component.free_list_num,
                               mca_btl_self_component.free_list_max,
                               mca_btl_self_component.free_list_inc,
                               NULL, 0, NULL, NULL, NULL);
    if (OPAL_SUCCESS != ret) {
        return NULL;
    }

    ret = opal_free_list_init (&mca_btl_self_component.self_frags_send,
                               sizeof (mca_btl_self_frag_send_t) + mca_btl_self.btl_max_send_size,
                               opal_cache_line_size, OBJ_CLASS(mca_btl_self_frag_send_t), 0,
                               opal_cache_line_size, mca_btl_self_component.free_list_num,
                               mca_btl_self_component.free_list_max,
                               mca_btl_self_component.free_list_inc,
                               NULL, 0, NULL, NULL, NULL);
    if (OPAL_SUCCESS != ret) {
        return NULL;
    }

    ret = opal_free_list_init (&mca_btl_self_component.self_frags_rdma,
                               sizeof (mca_btl_self_frag_rdma_t) + MCA_BTL_SELF_MAX_INLINE_SIZE,
                               opal_cache_line_size, OBJ_CLASS(mca_btl_self_frag_rdma_t), 0,
                               opal_cache_line_size, mca_btl_self_component.free_list_num,
                               mca_btl_self_component.free_list_max,
                               mca_btl_self_component.free_list_inc,
                               NULL, 0, NULL, NULL, NULL);
    if (OPAL_SUCCESS != ret) {
        return NULL;
    }

    /* get pointer to the btls */
    btls = (mca_btl_base_module_t **) malloc (sizeof (mca_btl_base_module_t *));
    if (NULL == btls) {
        return NULL;
    }

    btls[0] = &mca_btl_self;
    *num_btls = 1;

    return btls;
}

