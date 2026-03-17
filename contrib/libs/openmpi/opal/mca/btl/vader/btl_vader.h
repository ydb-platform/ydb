/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2009 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2006-2007 Voltaire. All rights reserved.
 * Copyright (c) 2009-2010 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2010-2018 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2015      Mellanox Technologies. All rights reserved.
 * Copyright (c) 2018      Triad National Security, LLC. All rights
 *                         reserved.
 *
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/**
 * @file
 */
#ifndef MCA_BTL_VADER_H
#define MCA_BTL_VADER_H

#include "opal_config.h"

#include <stddef.h>
#include <stdlib.h>
#include <string.h>

# include <stdint.h>
#ifdef HAVE_SCHED_H
# include <sched.h>
#endif  /* HAVE_SCHED_H */
#ifdef HAVE_UNISTD_H
# include <unistd.h>
#endif /* HAVE_UNISTD_H */

#include "opal/mca/shmem/base/base.h"

#include "opal/class/opal_free_list.h"
#include "opal/sys/atomic.h"
#include "opal/mca/btl/btl.h"
#include "opal/mca/rcache/rcache.h"
#include "opal/mca/rcache/base/rcache_base_vma.h"
#include "opal/mca/btl/base/base.h"
#include "opal/mca/rcache/rcache.h"
#include "opal/mca/rcache/base/base.h"
#include "opal/mca/btl/base/btl_base_error.h"
#include "opal/mca/mpool/base/base.h"
#include "opal/util/proc.h"
#include "btl_vader_endpoint.h"

#include "opal/mca/pmix/pmix.h"

#include "btl_vader_xpmem.h"
#include "btl_vader_knem.h"

BEGIN_C_DECLS

#define min(a,b) ((a) < (b) ? (a) : (b))

/*
 * Shared Memory resource managment
 */

struct vader_fifo_t;

/*
 * Modex data
 */
union vader_modex_t {
#if OPAL_BTL_VADER_HAVE_XPMEM
    struct vader_modex_xpmem_t {
        xpmem_segid_t seg_id;
        void *segment_base;
    } xpmem;
#endif
    opal_shmem_ds_t seg_ds;
};

/**
 * Single copy mechanisms
 */
enum {
    MCA_BTL_VADER_XPMEM = 0,
    MCA_BTL_VADER_CMA   = 1,
    MCA_BTL_VADER_KNEM  = 2,
    MCA_BTL_VADER_NONE  = 3,
    MCA_BTL_VADER_EMUL  = 4,
};

/**
 * Shared Memory (VADER) BTL module.
 */
struct mca_btl_vader_component_t {
    mca_btl_base_component_3_0_0_t super;   /**< base BTL component */
    int vader_free_list_num;                /**< initial size of free lists */
    int vader_free_list_max;                /**< maximum size of free lists */
    int vader_free_list_inc;                /**< number of elements to alloc when growing free lists */
#if OPAL_BTL_VADER_HAVE_XPMEM
    xpmem_segid_t my_seg_id;                /**< this rank's xpmem segment id */
    mca_rcache_base_vma_module_t *vma_module; /**< registration cache for xpmem segments */
#endif
    opal_shmem_ds_t seg_ds;                 /**< this rank's shared memory segment (when not using xpmem) */

    opal_mutex_t lock;                      /**< lock to protect concurrent updates to this structure's members */
    char *my_segment;                       /**< this rank's base pointer */
    size_t segment_size;                    /**< size of my_segment */
    int32_t num_smp_procs;                  /**< current number of smp procs on this host */
    opal_free_list_t vader_frags_eager;     /**< free list of vader send frags */
    opal_free_list_t vader_frags_max_send;  /**< free list of vader max send frags (large fragments) */
    opal_free_list_t vader_frags_user;      /**< free list of small inline frags */
    opal_free_list_t vader_fboxes;          /**< free list of available fast-boxes */

    unsigned int fbox_threshold;            /**< number of sends required before we setup a send fast box for a peer */
    unsigned int fbox_max;                  /**< maximum number of send fast boxes to allocate */
    unsigned int fbox_size;                 /**< size of each peer fast box allocation */

    int single_copy_mechanism;              /**< single copy mechanism to use */

    int memcpy_limit;                       /**< Limit where we switch from memmove to memcpy */
    int log_attach_align;                   /**< Log of the alignment for xpmem segments */
    unsigned int max_inline_send;           /**< Limit for copy-in-copy-out fragments */

    mca_btl_base_endpoint_t *endpoints;     /**< array of local endpoints (one for each local peer including myself) */
    mca_btl_base_endpoint_t **fbox_in_endpoints; /**< array of fast box in endpoints */
    unsigned int num_fbox_in_endpoints;     /**< number of fast boxes to poll */
    struct vader_fifo_t *my_fifo;           /**< pointer to the local fifo */

    opal_list_t pending_endpoints;          /**< list of endpoints with pending fragments */
    opal_list_t pending_fragments;          /**< fragments pending remote completion */

    char *backing_directory;                /**< directory to place shared memory backing files */

    /* knem stuff */
#if OPAL_BTL_VADER_HAVE_KNEM
    unsigned int knem_dma_min;              /**< minimum size to enable DMA for knem transfers (0 disables) */
#endif
    mca_mpool_base_module_t *mpool;
};
typedef struct mca_btl_vader_component_t mca_btl_vader_component_t;
OPAL_MODULE_DECLSPEC extern mca_btl_vader_component_t mca_btl_vader_component;

/**
 * VADER BTL Interface
 */
struct mca_btl_vader_t {
    mca_btl_base_module_t  super;       /**< base BTL interface */
    bool btl_inited;  /**< flag indicating if btl has been inited */
    mca_btl_base_module_error_cb_fn_t error_cb;
#if OPAL_BTL_VADER_HAVE_KNEM
    int knem_fd;

    /* registration cache */
    mca_rcache_base_module_t *knem_rcache;
#endif
};
typedef struct mca_btl_vader_t mca_btl_vader_t;
OPAL_MODULE_DECLSPEC extern mca_btl_vader_t mca_btl_vader;

/* number of peers on the node (not including self) */
#define MCA_BTL_VADER_NUM_LOCAL_PEERS opal_process_info.num_local_peers

/* local rank in the group */
#define MCA_BTL_VADER_LOCAL_RANK opal_process_info.my_local_rank

/* memcpy is faster at larger sizes but is undefined if the
   pointers are aliased (TODO -- readd alias check) */
static inline void vader_memmove (void *dst, void *src, size_t size)
{
    if (size >= (size_t) mca_btl_vader_component.memcpy_limit) {
        memcpy (dst, src, size);
    } else {
        memmove (dst, src, size);
    }
}

/**
 * Initiate a send to the peer.
 *
 * @param btl (IN)      BTL module
 * @param peer (IN)     BTL peer addressing
 */
int mca_btl_vader_send(struct mca_btl_base_module_t *btl,
                       struct mca_btl_base_endpoint_t *endpoint,
                       struct mca_btl_base_descriptor_t *descriptor,
                       mca_btl_base_tag_t tag);

/**
 * Initiate an inline send to the peer.
 *
 * @param btl (IN)      BTL module
 * @param peer (IN)     BTL peer addressing
 */
int mca_btl_vader_sendi (struct mca_btl_base_module_t *btl,
                         struct mca_btl_base_endpoint_t *endpoint,
                         struct opal_convertor_t *convertor,
                         void *header, size_t header_size,
                         size_t payload_size, uint8_t order,
                         uint32_t flags, mca_btl_base_tag_t tag,
                         mca_btl_base_descriptor_t **descriptor);

/**
 * Initiate an synchronous put.
 *
 * @param btl (IN)         BTL module
 * @param endpoint (IN)    BTL addressing information
 * @param descriptor (IN)  Description of the data to be transferred
 */
#if OPAL_BTL_VADER_HAVE_XPMEM
int mca_btl_vader_put_xpmem (mca_btl_base_module_t *btl, mca_btl_base_endpoint_t *endpoint, void *local_address,
                             uint64_t remote_address, mca_btl_base_registration_handle_t *local_handle,
                             mca_btl_base_registration_handle_t *remote_handle, size_t size, int flags,
                             int order, mca_btl_base_rdma_completion_fn_t cbfunc, void *cbcontext, void *cbdata);
#endif

#if OPAL_BTL_VADER_HAVE_CMA
int mca_btl_vader_put_cma (mca_btl_base_module_t *btl, mca_btl_base_endpoint_t *endpoint, void *local_address,
                           uint64_t remote_address, mca_btl_base_registration_handle_t *local_handle,
                           mca_btl_base_registration_handle_t *remote_handle, size_t size, int flags,
                           int order, mca_btl_base_rdma_completion_fn_t cbfunc, void *cbcontext, void *cbdata);
#endif

#if OPAL_BTL_VADER_HAVE_KNEM
int mca_btl_vader_put_knem (mca_btl_base_module_t *btl, mca_btl_base_endpoint_t *endpoint, void *local_address,
                            uint64_t remote_address, mca_btl_base_registration_handle_t *local_handle,
                            mca_btl_base_registration_handle_t *remote_handle, size_t size, int flags,
                            int order, mca_btl_base_rdma_completion_fn_t cbfunc, void *cbcontext, void *cbdata);
#endif

int mca_btl_vader_put_sc_emu (mca_btl_base_module_t *btl, mca_btl_base_endpoint_t *endpoint, void *local_address,
                               uint64_t remote_address, mca_btl_base_registration_handle_t *local_handle,
                               mca_btl_base_registration_handle_t *remote_handle, size_t size, int flags,
                               int order, mca_btl_base_rdma_completion_fn_t cbfunc, void *cbcontext, void *cbdata);

/**
 * Initiate an synchronous get.
 *
 * @param btl (IN)         BTL module
 * @param endpoint (IN)    BTL addressing information
 * @param descriptor (IN)  Description of the data to be transferred
 */
#if OPAL_BTL_VADER_HAVE_XPMEM
int mca_btl_vader_get_xpmem (mca_btl_base_module_t *btl, mca_btl_base_endpoint_t *endpoint, void *local_address,
                             uint64_t remote_address, mca_btl_base_registration_handle_t *local_handle,
                             mca_btl_base_registration_handle_t *remote_handle, size_t size, int flags,
                             int order, mca_btl_base_rdma_completion_fn_t cbfunc, void *cbcontext, void *cbdata);
#endif

#if OPAL_BTL_VADER_HAVE_CMA
int mca_btl_vader_get_cma (mca_btl_base_module_t *btl, mca_btl_base_endpoint_t *endpoint, void *local_address,
                           uint64_t remote_address, mca_btl_base_registration_handle_t *local_handle,
                           mca_btl_base_registration_handle_t *remote_handle, size_t size, int flags,
                           int order, mca_btl_base_rdma_completion_fn_t cbfunc, void *cbcontext, void *cbdata);
#endif

#if OPAL_BTL_VADER_HAVE_KNEM
int mca_btl_vader_get_knem (mca_btl_base_module_t *btl, mca_btl_base_endpoint_t *endpoint, void *local_address,
                            uint64_t remote_address, mca_btl_base_registration_handle_t *local_handle,
                            mca_btl_base_registration_handle_t *remote_handle, size_t size, int flags,
                            int order, mca_btl_base_rdma_completion_fn_t cbfunc, void *cbcontext, void *cbdata);
#endif

int mca_btl_vader_get_sc_emu (mca_btl_base_module_t *btl, mca_btl_base_endpoint_t *endpoint, void *local_address,
                               uint64_t remote_address, mca_btl_base_registration_handle_t *local_handle,
                               mca_btl_base_registration_handle_t *remote_handle, size_t size, int flags,
                               int order, mca_btl_base_rdma_completion_fn_t cbfunc, void *cbcontext, void *cbdata);

int mca_btl_vader_emu_aop (struct mca_btl_base_module_t *btl, struct mca_btl_base_endpoint_t *endpoint,
                           uint64_t remote_address, mca_btl_base_registration_handle_t *remote_handle,
                           mca_btl_base_atomic_op_t op, uint64_t operand, int flags, int order,
                           mca_btl_base_rdma_completion_fn_t cbfunc, void *cbcontext, void *cbdata);

int mca_btl_vader_emu_afop (struct mca_btl_base_module_t *btl, struct mca_btl_base_endpoint_t *endpoint,
                            void *local_address, uint64_t remote_address, mca_btl_base_registration_handle_t *local_handle,
                            mca_btl_base_registration_handle_t *remote_handle, mca_btl_base_atomic_op_t op,
                            uint64_t operand, int flags, int order, mca_btl_base_rdma_completion_fn_t cbfunc,
                            void *cbcontext, void *cbdata);

int mca_btl_vader_emu_acswap (struct mca_btl_base_module_t *btl, struct mca_btl_base_endpoint_t *endpoint,
                              void *local_address, uint64_t remote_address, mca_btl_base_registration_handle_t *local_handle,
                              mca_btl_base_registration_handle_t *remote_handle, uint64_t compare, uint64_t value, int flags,
                              int order, mca_btl_base_rdma_completion_fn_t cbfunc, void *cbcontext, void *cbdata);

void mca_btl_vader_sc_emu_init (void);

/**
 * Allocate a segment.
 *
 * @param btl (IN)      BTL module
 * @param size (IN)     Request segment size.
 */
mca_btl_base_descriptor_t* mca_btl_vader_alloc (struct mca_btl_base_module_t* btl,
                                                struct mca_btl_base_endpoint_t* endpoint,
                                                uint8_t order, size_t size, uint32_t flags);

/**
 * Return a segment allocated by this BTL.
 *
 * @param btl (IN)      BTL module
 * @param segment (IN)  Allocated segment.
 */
int mca_btl_vader_free (struct mca_btl_base_module_t *btl, mca_btl_base_descriptor_t *des);


END_C_DECLS

#endif
