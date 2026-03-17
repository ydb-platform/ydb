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

#ifndef OMPI_OSC_RDMA_TYPES_H
#define OMPI_OSC_RDMA_TYPES_H

#include "ompi_config.h"

/* forward declarations of some other component types */
struct ompi_osc_rdma_frag_t;
struct ompi_osc_rdma_sync_t;
struct ompi_osc_rdma_peer_t;

#if OPAL_HAVE_ATOMIC_MATH_64

typedef int64_t osc_rdma_base_t;
typedef int64_t osc_rdma_size_t;
typedef int64_t osc_rdma_counter_t;

#define ompi_osc_rdma_counter_add opal_atomic_add_fetch_64

#else

typedef int32_t osc_rdma_base_t;
typedef int32_t osc_rdma_size_t;
typedef int32_t osc_rdma_counter_t;

#define ompi_osc_rdma_counter_add opal_atomic_add_fetch_32

#endif

#if OPAL_HAVE_ATOMIC_MATH_64

#define OMPI_OSC_RDMA_LOCK_EXCLUSIVE   0x8000000000000000l

typedef int64_t  ompi_osc_rdma_lock_t;

static inline int64_t ompi_osc_rdma_lock_add (volatile int64_t *p, int64_t value)
{
    int64_t new;

    opal_atomic_mb ();
    new = opal_atomic_add_fetch_64 (p, value) - value;
    opal_atomic_mb ();

    return new;
}

static inline int ompi_osc_rdma_lock_compare_exchange (volatile int64_t *p, int64_t *comp, int64_t value)
{
    int ret;

    opal_atomic_mb ();
    ret = opal_atomic_compare_exchange_strong_64 (p, comp, value);
    opal_atomic_mb ();

    return ret;
}

#else

#define OMPI_OSC_RDMA_LOCK_EXCLUSIVE 0x80000000l

typedef int32_t  ompi_osc_rdma_lock_t;

static inline int32_t ompi_osc_rdma_lock_add (volatile int32_t *p, int32_t value)
{
    int32_t new;

    opal_atomic_mb ();
    /* opal_atomic_add_fetch_32 differs from normal atomics in that is returns the new value */
    new = opal_atomic_add_fetch_32 (p, value) - value;
    opal_atomic_mb ();

    return new;
}

static inline int ompi_osc_rdma_lock_compare_exchange (volatile int32_t *p, int32_t *comp, int32_t value)
{
    int ret;

    opal_atomic_mb ();
    ret = opal_atomic_compare_exchange_strong_32 (p, comp, value);
    opal_atomic_mb ();

    return ret;
}

#endif /* OPAL_HAVE_ATOMIC_MATH_64 */

/**
 * @brief structure describing a window memory region
 */
struct ompi_osc_rdma_region_t {
    /** base of the region */
    osc_rdma_base_t base;
    /** length (in bytes) of the region */
    osc_rdma_size_t len;
    /** BTL segment for the region (may be empty) */
    unsigned char   btl_handle_data[];
};
typedef struct ompi_osc_rdma_region_t ompi_osc_rdma_region_t;

/**
 * @brief data handle for dynamic memory regions
 *
 * This structure holds the btl handle (if one exists) and the
 * reference count for a dynamically attached region. The reference
 * count is used to keep track of the number of times a memory
 * region associated with a page (or set of pages) has been attached.
 */
struct ompi_osc_rdma_handle_t {
    /** btl handle for the memory region */
    mca_btl_base_registration_handle_t *btl_handle;
    /** number of attaches assocated with this region */
    int refcnt;
};
typedef struct ompi_osc_rdma_handle_t ompi_osc_rdma_handle_t;

/**
 * @brief number of state buffers that can be used for storing
 *        post messages.
 *
 * This value was chosen because post exposure epochs are expected to be
 * small relative to the size of the communicator. The value is constant
 * and not exposed as an MCA variable to keep the layout of the
 * \ref ompi_osc_rdma_state_t structure simple.
 */
#define OMPI_OSC_RDMA_POST_PEER_MAX 32

/**
 * @brief window state structure
 *
 * This structure holds the information relevant to the window state
 * of a peer. The structure synchronization data and includes useful
 * information that can be remotely read by other peers in the window.
 */
struct ompi_osc_rdma_state_t {
    /** used when rdma is in use to handle excusive locks and global shared locks (lock_all) */
    ompi_osc_rdma_lock_t global_lock;
    /** lock state for this node. the top bit indicates if a exclusive lock exists and the
     * remaining bits count the number of shared locks */
    ompi_osc_rdma_lock_t local_lock;
    /** lock for the accumulate state to ensure ordering and consistency */
    ompi_osc_rdma_lock_t accumulate_lock;
    /** current index to post to. compare-and-swap must be used to ensure
     * the index is free */
    osc_rdma_counter_t post_index;
    /** post buffers */
    osc_rdma_counter_t post_peers[OMPI_OSC_RDMA_POST_PEER_MAX];
    /** counter for number of post messages received  */
    osc_rdma_counter_t num_post_msgs;
    /** counter for number of complete messages received */
    osc_rdma_counter_t num_complete_msgs;
    /** lock for the region state to ensure consistency */
    ompi_osc_rdma_lock_t regions_lock;
    /** displacement unit for this process */
    int64_t            disp_unit;
    /** number of attached regions. this count will be 1 in non-dynamic regions */
    osc_rdma_counter_t region_count;
    /** attached memory regions */
    unsigned char      regions[];
};
typedef struct ompi_osc_rdma_state_t ompi_osc_rdma_state_t;

typedef void (*ompi_osc_rdma_pending_op_cb_fn_t) (void *, void *, int);

struct ompi_osc_rdma_pending_op_t {
    opal_list_item_t super;
    struct ompi_osc_rdma_module_t *module;
    struct ompi_osc_rdma_frag_t *op_frag;
    void *op_buffer;
    void *op_result;
    size_t op_size;
    volatile bool op_complete;
    ompi_osc_rdma_pending_op_cb_fn_t cbfunc;
    void *cbdata;
    void *cbcontext;
};

typedef struct ompi_osc_rdma_pending_op_t ompi_osc_rdma_pending_op_t;

OBJ_CLASS_DECLARATION(ompi_osc_rdma_pending_op_t);

/** Communication buffer for packing messages */
struct ompi_osc_rdma_frag_t {
    opal_free_list_item_t super;

    /* Number of operations which have started writing into the frag, but not yet completed doing so */
    volatile int32_t pending;
#if OPAL_HAVE_ATOMIC_MATH_64
    volatile int64_t curr_index;
#else
    volatile int32_t curr_index;
#endif

    struct ompi_osc_rdma_module_t *module;
    mca_btl_base_registration_handle_t *handle;
};
typedef struct ompi_osc_rdma_frag_t ompi_osc_rdma_frag_t;
OBJ_CLASS_DECLARATION(ompi_osc_rdma_frag_t);

#define OSC_RDMA_VERBOSE(x, ...) OPAL_OUTPUT_VERBOSE((x, ompi_osc_base_framework.framework_output, __VA_ARGS__))

#endif /* OMPI_OSC_RDMA_TYPES_H */
