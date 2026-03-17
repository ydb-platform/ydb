/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2016 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2006-2018 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2010      Oracle and/or its affiliates.  All rights reserved.
 * Copyright (c) 2012-2013 NVIDIA Corporation.  All rights reserved.
 * Copyright (c) 2015      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/**
 * @file
 *
 * Byte Transfer Layer (BTL)
 *
 *
 * BTL Initialization:
 *
 * During library initialization, all available BTL components are
 * loaded and opened via their mca_base_open_component_fn_t
 * function. The BTL open function should register any mca parameters
 * used to tune/adjust the behaviour of the BTL (mca_base_var_register()
 * mca_base_component_var_register()). Note that the open function may fail
 * if the resources (e.g. shared libraries, etc) required by the network
 * transport are not available.
 *
 * The mca_btl_base_component_init_fn_t() is then called for each of the
 * components that are succesfully opened. The component init function may
 * return either:
 *
 * (1) a NULL list of BTL modules if the transport is not available,
 * (2) a list containing a one or more single BTL modules, where the BTL provides
 *     a layer of abstraction over one or more physical devices (e.g. NICs),
 *
 * During module initialization, the module should post any addressing
 * information required by its peers. An example would be the TCP
 * listen port opened by the TCP module for incoming connection
 * requests. This information is published to peers via the
 * modex_send() interface. Note that peer information is not
 * guaranteed to be available via modex_recv() during the
 * module's init function. However, it will be available during
 * BTL selection (mca_btl_base_add_proc_fn_t()).
 *
 * BTL Selection:
 *
 * The upper layer builds an ordered list of the available BTL modules sorted
 * by their exclusivity ranking. This is a relative ranking that is used
 * to determine the set of BTLs that may be used to reach a given destination.
 * During startup the BTL modules are queried via their
 * mca_btl_base_add_proc_fn_t() to determine if they are able to reach
 * a given destination.  The BTL module with the highest ranking that
 * returns success is selected. Subsequent BTL modules are selected only
 * if they have the same exclusivity ranking.
 *
 * An example of how this might be used:
 *
 * BTL         Exclusivity   Comments
 * --------    -----------   ------------------
 * LO              100       Selected exclusively for local process
 * SM               50       Selected exclusively for other processes on host
 * IB                0       Selected based on network reachability
 * IB                0       Selected based on network reachability
 * TCP               0       Selected based on network reachability
 * TCP               0       Selected based on network reachability
 *
 * When mca_btl_base_add_proc_fn_t() is called on a  BTL module, the BTL
 * will populate an OUT variable with mca_btl_base_endpoint_t pointers.
 * Each pointer is treated as an opaque handle by the upper layer and is
 * returned to the BTL on subsequent data transfer calls to the
 * corresponding destination process.  The actual contents of the
 * data structure are defined on a per BTL basis, and may be used to
 * cache addressing or connection information, such as a TCP socket
 * or IB queue pair.
 *
 * Progress:
 *
 * By default, the library provides for polling based progress of outstanding
 * requests. The BTL component exports an interface function (btl_progress)
 * that is called in a polling mode by the PML during calls into the MPI
 * library. Note that the btl_progress() function is called on the BTL component
 * rather than each BTL module. This implies that the BTL author is responsible
 * for iterating over the pending operations in each of the BTL modules associated
 * with the component.
 *
 * On platforms where threading support is provided, the library provides the
 * option of building with asynchronous threaded progress. In this case, the BTL
 * author is responsible for providing a thread to progress pending operations.
 * A thread is associated with the BTL component/module such that transport specific
 * functionality/APIs may be used to block the thread until a pending operation
 * completes. This thread MUST NOT poll for completion as this would oversubscribe
 * the CPU.
 *
 * Note that in the threaded case the PML may choose to use a hybrid approach,
 * such that polling is implemented from the user thread for a fixed number of
 * cycles before relying on the background thread(s) to complete requests. If
 * possible the BTL should support the use of both modes concurrently.
 *
 */

#ifndef OPAL_MCA_BTL_H
#define OPAL_MCA_BTL_H

#include "opal_config.h"
#include "opal/types.h"
#include "opal/prefetch.h" /* For OPAL_LIKELY */
#include "opal/class/opal_bitmap.h"
#include "opal/datatype/opal_convertor.h"
#include "opal/mca/mca.h"
#include "opal/mca/mpool/mpool.h"
#include "opal/mca/rcache/rcache.h"
#include "opal/mca/crs/crs.h"
#include "opal/mca/crs/base/base.h"

BEGIN_C_DECLS

/*
 * BTL types
 */

struct mca_btl_base_module_t;
struct mca_btl_base_endpoint_t;
struct mca_btl_base_descriptor_t;
struct mca_mpool_base_resources_t;
struct opal_proc_t;

/**
 * Opaque registration handle for executing RDMA and atomic
 * operations on a memory region.
 *
 * This data inside this handle is appropriate for passing
 * to remote peers to execute RDMA and atomic operations. The
 * size needed to send the registration handle can be
 * obtained from the btl via the btl_registration_handle_size
 * member. If this size is 0 then no registration data is
 * needed to execute RDMA or atomic operations.
 */
struct mca_btl_base_registration_handle_t;
typedef struct mca_btl_base_registration_handle_t mca_btl_base_registration_handle_t;


/* Wildcard endpoint for use in the register_mem function */
#define MCA_BTL_ENDPOINT_ANY (struct mca_btl_base_endpoint_t *) -1

/* send/recv operations require tag matching */
typedef uint8_t mca_btl_base_tag_t;

#define MCA_BTL_NO_ORDER       255

/*
 * Communication specific defines. There are a number of active message ID
 * that can be shred between all frameworks that need to communicate (i.e.
 * use the PML or the BTL directly). These ID are exchanged between the
 * processes, therefore they need to be identical everywhere. The simplest
 * approach is to have them defined as constants, and give each framework a
 * small number. Here is the rule that defines these ID (they are 8 bits):
 * - the first 3 bits are used to code the framework (i.e. PML, OSC, COLL)
 * - the remaining 5 bytes are used internally by the framework, and divided
 *   based on the components requirements. Therefore, the way the PML and
 * the OSC frameworks use these defines will be different. For more
 * information about how these framework ID are defined, take a look in the
 * header file associated with the framework.
 */
#define MCA_BTL_AM_FRAMEWORK_MASK   0xD0
#define MCA_BTL_TAG_BTL             0x20
#define MCA_BTL_TAG_PML             0x40
#define MCA_BTL_TAG_OSC_RDMA        0x60
#define MCA_BTL_TAG_USR             0x80
#define MCA_BTL_TAG_MAX             255 /* 1 + highest allowed tag num */

/*
 * Reserved tags for specific BTLs. As multiple BTLs can be active
 * simultaneously, their tags should not collide.
 */
#define MCA_BTL_TAG_IB                (MCA_BTL_TAG_BTL + 0)
#define MCA_BTL_TAG_UDAPL             (MCA_BTL_TAG_BTL + 1)
#define MCA_BTL_TAG_SMCUDA            (MCA_BTL_TAG_BTL + 2)
#define MCA_BTL_TAG_VADER             (MCA_BTL_TAG_BTL + 3)

/* prefered protocol */
#define MCA_BTL_FLAGS_SEND            0x0001
#define MCA_BTL_FLAGS_PUT             0x0002
#define MCA_BTL_FLAGS_GET             0x0004
/* btls that set the MCA_BTL_FLAGS_RDMA will always get added to the BML
 * rdma_btls list. This allows the updated one-sided component to
 * use btls that are not otherwise used for send/recv. */
#define MCA_BTL_FLAGS_RDMA (MCA_BTL_FLAGS_GET|MCA_BTL_FLAGS_PUT)

/* btl can send directly from user buffer w/out registration */
#define MCA_BTL_FLAGS_SEND_INPLACE    0x0008

/* btl transport reliability flags - currently used only by the DR PML */
#define MCA_BTL_FLAGS_NEED_ACK        0x0010
#define MCA_BTL_FLAGS_NEED_CSUM       0x0020

/** deprecated (BTL 3.0) */
#define MCA_BTL_FLAGS_RDMA_MATCHED    0x0040

/* btl needs local rdma completion */
#define MCA_BTL_FLAGS_RDMA_COMPLETION 0x0080

 /* btl can do heterogeneous rdma operations on byte buffers */
#define MCA_BTL_FLAGS_HETEROGENEOUS_RDMA 0x0100

/* btl can support failover if enabled */
#define MCA_BTL_FLAGS_FAILOVER_SUPPORT 0x0200

#define MCA_BTL_FLAGS_CUDA_PUT        0x0400
#define MCA_BTL_FLAGS_CUDA_GET        0x0800
#define MCA_BTL_FLAGS_CUDA_RDMA (MCA_BTL_FLAGS_CUDA_GET|MCA_BTL_FLAGS_CUDA_PUT)
#define MCA_BTL_FLAGS_CUDA_COPY_ASYNC_SEND 0x1000
#define MCA_BTL_FLAGS_CUDA_COPY_ASYNC_RECV 0x2000

/* btl can support signaled operations. BTLs that support this flag are
 * expected to provide a mechanism for asynchronous progress on descriptors
 * where the feature is requested. BTLs should also be aware that users can
 * (and probably will) turn this flag on and off using the MCA variable
 * system.
 */
#define MCA_BTL_FLAGS_SIGNALED        0x4000

/** The BTL supports network atomic operations */
#define MCA_BTL_FLAGS_ATOMIC_OPS      0x08000
/** The BTL supports fetching network atomic operations */
#define MCA_BTL_FLAGS_ATOMIC_FOPS     0x10000

/** The BTL requires add_procs to be with all procs including non-local. Shared-memory
 * BTLs should not set this flag. */
#define MCA_BTL_FLAGS_SINGLE_ADD_PROCS 0x20000

/* The BTL is using progress thread and need the protection on matching */
#define MCA_BTL_FLAGS_BTL_PROGRESS_THREAD_ENABLED 0x40000

/* The BTL supports RMDA flush */
#define MCA_BTL_FLAGS_RDMA_FLUSH      0x80000

/* Default exclusivity levels */
#define MCA_BTL_EXCLUSIVITY_HIGH     (64*1024) /* internal loopback */
#define MCA_BTL_EXCLUSIVITY_DEFAULT  1024      /* GM/IB/etc. */
#define MCA_BTL_EXCLUSIVITY_LOW      0         /* TCP used as a last resort */

/* error callback flags */
#define MCA_BTL_ERROR_FLAGS_FATAL 0x1
#define MCA_BTL_ERROR_FLAGS_NONFATAL 0x2
#define MCA_BTL_ERROR_FLAGS_ADD_CUDA_IPC 0x4

/** registration flags. the access flags are a 1-1 mapping with the mpool
 * access flags. */
enum {
    /** Allow local write on the registered region. If a region is registered
     * with this flag the registration can be used as the local handle for a
     * btl_get operation. */
    MCA_BTL_REG_FLAG_LOCAL_WRITE   = MCA_RCACHE_ACCESS_LOCAL_WRITE,
    /** Allow remote read on the registered region. If a region is registered
     * with this flag the registration can be used as the remote handle for a
     * btl_get operation. */
    MCA_BTL_REG_FLAG_REMOTE_READ   = MCA_RCACHE_ACCESS_REMOTE_READ,
    /** Allow remote write on the registered region. If a region is registered
     * with this flag the registration can be used as the remote handle for a
     * btl_put operation. */
    MCA_BTL_REG_FLAG_REMOTE_WRITE  = MCA_RCACHE_ACCESS_REMOTE_WRITE,
    /** Allow remote atomic operations on the registered region. If a region is
     * registered with this flag the registration can be used as the remote
     * handle for a btl_atomic_op or btl_atomic_fop operation. */
    MCA_BTL_REG_FLAG_REMOTE_ATOMIC = MCA_RCACHE_ACCESS_REMOTE_ATOMIC,
    /** Allow any btl operation on the registered region. If a region is registered
     * with this flag the registration can be used as the local or remote handle for
     * any btl operation. */
    MCA_BTL_REG_FLAG_ACCESS_ANY    = MCA_RCACHE_ACCESS_ANY,
#if OPAL_CUDA_GDR_SUPPORT
    /** Region is in GPU memory */
    MCA_BTL_REG_FLAG_CUDA_GPU_MEM  = 0x00010000,
#endif
};

/** supported atomic operations */
enum {
    /** The btl supports atomic add */
    MCA_BTL_ATOMIC_SUPPORTS_ADD    = 0x00000001,
    /** The btl supports atomic bitwise and */
    MCA_BTL_ATOMIC_SUPPORTS_AND    = 0x00000200,
    /** The btl supports atomic bitwise or */
    MCA_BTL_ATOMIC_SUPPORTS_OR     = 0x00000400,
    /** The btl supports atomic bitwise exclusive or */
    MCA_BTL_ATOMIC_SUPPORTS_XOR    = 0x00000800,

    /** The btl supports logical and */
    MCA_BTL_ATOMIC_SUPPORTS_LAND   = 0x00001000,
    /** The btl supports logical or */
    MCA_BTL_ATOMIC_SUPPORTS_LOR    = 0x00002000,
    /** The btl supports logical exclusive or */
    MCA_BTL_ATOMIC_SUPPORTS_LXOR   = 0x00004000,

    /** The btl supports atomic swap */
    MCA_BTL_ATOMIC_SUPPORTS_SWAP   = 0x00010000,

    /** The btl supports atomic min */
    MCA_BTL_ATOMIC_SUPPORTS_MIN    = 0x00100000,
    /** The btl supports atomic min */
    MCA_BTL_ATOMIC_SUPPORTS_MAX    = 0x00200000,

    /** The btl supports 32-bit integer operations. Keep in mind the btl may
     * support only a subset of the available atomics. */
    MCA_BTL_ATOMIC_SUPPORTS_32BIT  = 0x01000000,

    /** The btl supports floating-point operations. Keep in mind the btl may
     * support only a subset of the available atomics and may not support
     * both 64 or 32-bit floating point. */
    MCA_BTL_ATOMIC_SUPPORTS_FLOAT  = 0x02000000,

    /** The btl supports atomic compare-and-swap */
    MCA_BTL_ATOMIC_SUPPORTS_CSWAP  = 0x10000000,

    /** The btl guarantees global atomicity (can mix btl atomics with cpu atomics) */
    MCA_BTL_ATOMIC_SUPPORTS_GLOB   = 0x20000000,
};

enum {
    /** Use 32-bit atomics */
    MCA_BTL_ATOMIC_FLAG_32BIT = 0x00000001,
    /** Use floating-point atomics */
    MCA_BTL_ATOMIC_FLAG_FLOAT = 0x00000002,
};

enum mca_btl_base_atomic_op_t {
    /** Atomic add: (*remote_address) = (*remote_address) + operand */
    MCA_BTL_ATOMIC_ADD = 0x0001,
    /** Atomic and: (*remote_address) = (*remote_address) & operand */
    MCA_BTL_ATOMIC_AND = 0x0011,
    /** Atomic or: (*remote_address) = (*remote_address) | operand */
    MCA_BTL_ATOMIC_OR  = 0x0012,
    /** Atomic xor: (*remote_address) = (*remote_address) ^ operand */
    MCA_BTL_ATOMIC_XOR = 0x0014,
    /** Atomic logical and: (*remote_address) = (*remote_address) && operand */
    MCA_BTL_ATOMIC_LAND = 0x0015,
    /** Atomic logical or: (*remote_address) = (*remote_address) || operand */
    MCA_BTL_ATOMIC_LOR = 0x0016,
    /** Atomic logical xor: (*remote_address) = (*remote_address) != operand */
    MCA_BTL_ATOMIC_LXOR = 0x0017,
    /** Atomic swap: (*remote_address) = operand */
    MCA_BTL_ATOMIC_SWAP = 0x001a,
    /** Atomic min */
    MCA_BTL_ATOMIC_MIN = 0x0020,
    /** Atomic max */
    MCA_BTL_ATOMIC_MAX = 0x0021,

    MCA_BTL_ATOMIC_LAST,
};
typedef enum mca_btl_base_atomic_op_t mca_btl_base_atomic_op_t;

/**
 * Asynchronous callback function on completion of an operation.
 * Completion Semantics: The descriptor can be reused or returned to the
 *  BTL via mca_btl_base_module_free_fn_t. The operation has been queued to
 *  the network device or will otherwise make asynchronous progress without
 *  subsequent calls to btl_progress.
 *
 * @param[IN] module      the BTL module
 * @param[IN] endpoint    the BTL endpoint
 * @param[IN] descriptor  the BTL descriptor
 *
 */
typedef void (*mca_btl_base_completion_fn_t)(
    struct mca_btl_base_module_t* module,
    struct mca_btl_base_endpoint_t* endpoint,
    struct mca_btl_base_descriptor_t* descriptor,
    int status);


/**
 * Asynchronous callback function on completion of an rdma or atomic operation.
 * Completion Semantics: The rdma or atomic memory operation has completed
 * remotely (i.e.) is remotely visible and the caller is free to deregister
 * the local_handle or modify the memory in local_address.
 *
 * @param[IN] module        the BTL module
 * @param[IN] endpoint      the BTL endpoint
 * @param[IN] local_address local address for the operation (if any)
 * @param[IN] local_handle  local handle associated with the local_address
 * @param[IN] context       callback context supplied to the rdma/atomic operation
 * @param[IN] cbdata        callback data supplied to the rdma/atomic operation
 * @param[IN] status        status of the operation
 *
 */
typedef void (*mca_btl_base_rdma_completion_fn_t)(
    struct mca_btl_base_module_t* module,
    struct mca_btl_base_endpoint_t* endpoint,
    void *local_address,
    struct mca_btl_base_registration_handle_t *local_handle,
    void *context,
    void *cbdata,
    int status);


/**
 * Describes a region/segment of memory that is addressable
 * by an BTL.
 *
 * Note: In many cases the alloc and prepare methods of BTLs
 * do not return a mca_btl_base_segment_t but instead return a
 * subclass. Extreme care should be used when modifying
 * BTL segments to prevent overwriting internal BTL data.
 *
 * All BTLs MUST use base segments when calling registered
 * Callbacks.
 *
 * BTL MUST use mca_btl_base_segment_t or a subclass and
 * MUST store their segment length in btl_seg_size. BTLs
 * MUST specify a segment no larger than MCA_BTL_SEG_MAX_SIZE.
 */

struct mca_btl_base_segment_t {
    /** Address of the memory */
    opal_ptr_t seg_addr;
     /** Length in bytes */
    uint64_t   seg_len;
};
typedef struct mca_btl_base_segment_t mca_btl_base_segment_t;


#if OPAL_ENABLE_HETEROGENEOUS_SUPPORT && !defined(WORDS_BIGENDIAN)
#define MCA_BTL_BASE_SEGMENT_HTON(s)                   \
        (s).seg_addr.lval = hton64((s).seg_addr.lval); \
        (s).seg_len = hton64((s).seg_len);
#define MCA_BTL_BASE_SEGMENT_NTOH(s)                   \
        (s).seg_addr.lval = ntoh64((s).seg_addr.lval); \
        (s).seg_len = ntoh64((s).seg_len);
#else
#define MCA_BTL_BASE_SEGMENT_HTON(s)
#define MCA_BTL_BASE_SEGMENT_NTOH(s)
#endif
/**
 * A descriptor that holds the parameters to a send/put/get
 * operation along w/ a callback routine that is called on
 * completion of the request.
 * Note: receive callbacks will store the incomming data segments in
 *       des_segments
 */

struct mca_btl_base_descriptor_t {
    opal_free_list_item_t super;
    mca_btl_base_segment_t *des_segments;     /**< local segments */
    size_t des_segment_count;                 /**< number of local segments */
    mca_btl_base_completion_fn_t des_cbfunc;  /**< local callback function */
    void* des_cbdata;                         /**< opaque callback data */
    void* des_context;                        /**< more opaque callback data */
    uint32_t des_flags;                       /**< hints to BTL */
    /** order value, this is only
        valid in the local completion callback
        and may be used in subsequent calls to
        btl_alloc, btl_prepare_src to request
        a descriptor that will be ordered w.r.t.
        this descriptor
    */
    uint8_t order;
};
typedef struct mca_btl_base_descriptor_t mca_btl_base_descriptor_t;

OPAL_DECLSPEC OBJ_CLASS_DECLARATION(mca_btl_base_descriptor_t);

#define MCA_BTL_DES_FLAGS_PRIORITY          0x0001
/* Allow the BTL to dispose the descriptor once the callback
 * associated was triggered.
 */
#define MCA_BTL_DES_FLAGS_BTL_OWNERSHIP     0x0002
/* Allow the BTL to avoid calling the descriptor callback
 * if the send succeded in the btl_send (i.e in the fast path).
 */
#define MCA_BTL_DES_SEND_ALWAYS_CALLBACK    0x0004

/* Tell the PML that the copy is being done asynchronously
 */
#define MCA_BTL_DES_FLAGS_CUDA_COPY_ASYNC   0x0008

/* Type of transfer that will be done with this frag.
 */
#define MCA_BTL_DES_FLAGS_PUT               0x0010
#define MCA_BTL_DES_FLAGS_GET               0x0020

/* Ask the BTL to wake the remote process (send/sendi) or local process
 * (put/get) to handle this message. The BTL may ignore this flag if
 * signaled operations are not supported.
 */
#define MCA_BTL_DES_FLAGS_SIGNAL            0x0040

/**
 * Maximum number of allowed segments in src/dst fields of a descriptor.
 */
#define MCA_BTL_DES_MAX_SEGMENTS 16

/**
 * Maximum size of a BTL segment (NTH: does it really save us anything
 * to hardcode this?)
 */
#define MCA_BTL_SEG_MAX_SIZE 256

/**
 * Maximum size of a BTL registration handle in bytes
 */
#define MCA_BTL_REG_HANDLE_MAX_SIZE 256

/*
 *  BTL base header, stores the tag at a minimum
 */
struct mca_btl_base_header_t{
    mca_btl_base_tag_t tag;
};
typedef struct mca_btl_base_header_t mca_btl_base_header_t;

#define MCA_BTL_BASE_HEADER_HTON(hdr)
#define MCA_BTL_BASE_HEADER_NTOH(hdr)

/*
 *  BTL component interface functions and datatype.
 */

/**
 * MCA->BTL Initializes the BTL component and creates specific BTL
 * module(s).
 *
 * @param num_btls (OUT) Returns the number of btl modules created, or 0
 *                       if the transport is not available.
 *
 * @param enable_progress_threads (IN) Whether this component is
 * allowed to run a hidden/progress thread or not.
 *
 * @param enable_mpi_threads (IN) Whether support for multiple MPI
 * threads is enabled or not (i.e., MPI_THREAD_MULTIPLE), which
 * indicates whether multiple threads may invoke this component
 * simultaneously or not.
 *
 * @return Array of pointers to BTL modules, or NULL if the transport
 *         is not available.
 *
 * During component initialization, the BTL component should discover
 * the physical devices that are available for the given transport,
 * and create a BTL module to represent each device. Any addressing
 * information required by peers to reach the device should be published
 * during this function via the modex_send() interface.
 *
 */

typedef struct mca_btl_base_module_t** (*mca_btl_base_component_init_fn_t)(
    int *num_btls,
    bool enable_progress_threads,
    bool enable_mpi_threads
);

/**
 * MCA->BTL Called to progress outstanding requests for
 * non-threaded polling environments.
 *
 * @return           Count of "completions", a metric of
 *                   how many items where completed in the call
 *                   to progress.
 */

typedef int (*mca_btl_base_component_progress_fn_t)(void);


/**
 * Callback function that is called asynchronously on receipt
 * of data by the transport layer.
 * Note that the the mca_btl_base_descriptor_t is only valid within the
 * completion function, this implies that all data payload in the
 * mca_btl_base_descriptor_t must be copied out within this callback or
 * forfeited back to the BTL.
 * Note also that descriptor segments (des_segments) must be base
 * segments for all callbacks.
 *
 * @param[IN] btl        BTL module
 * @param[IN] tag        The active message receive callback tag value
 * @param[IN] descriptor The BTL descriptor (contains the receive payload)
 * @param[IN] cbdata     Opaque callback data
 */

typedef void (*mca_btl_base_module_recv_cb_fn_t)(
    struct mca_btl_base_module_t* btl,
    mca_btl_base_tag_t tag,
    mca_btl_base_descriptor_t* descriptor,
    void* cbdata
);

typedef struct mca_btl_active_message_callback_t {
    mca_btl_base_module_recv_cb_fn_t cbfunc;
    void* cbdata;
} mca_btl_active_message_callback_t;

OPAL_DECLSPEC extern
mca_btl_active_message_callback_t mca_btl_base_active_message_trigger[MCA_BTL_TAG_MAX];

/**
 *  BTL component descriptor. Contains component version information
 *  and component open/close/init functions.
 */

struct mca_btl_base_component_3_0_0_t {
  mca_base_component_t btl_version;
  mca_base_component_data_t btl_data;
  mca_btl_base_component_init_fn_t btl_init;
  mca_btl_base_component_progress_fn_t btl_progress;
};
typedef struct mca_btl_base_component_3_0_0_t mca_btl_base_component_3_0_0_t;
typedef struct mca_btl_base_component_3_0_0_t mca_btl_base_component_t;

/*  add the 2_0_0_t typedef for source compatibility
 *  we can do this safely because 2_0_0 components are the same as
 *  3_0_0 components, the difference is in the btl module.
 *  Unfortunately 2_0_0 modules are not compatible with BTL 3_0_0 and
 *  can not be used with the new interface.
 */
typedef struct mca_btl_base_component_3_0_0_t mca_btl_base_component_2_0_0_t;


/*
 * BTL module interface functions and datatype.
 */

/**
 * MCA->BTL Clean up any resources held by BTL module
 * before the module is unloaded.
 *
 * @param btl (IN)   BTL module.
 * @return           OPAL_SUCCESS or error status on failure.
 *
 * Prior to unloading a BTL module, the MCA framework will call
 * the BTL finalize method of the module. Any resources held by
 * the BTL should be released and if required the memory corresponding
 * to the BTL module freed.
 *
 */
typedef int (*mca_btl_base_module_finalize_fn_t)(
    struct mca_btl_base_module_t* btl
);

/**
 * BML->BTL notification of change in the process list.
 *
 * @param btl (IN)            BTL module
 * @param nprocs (IN)         Number of processes
 * @param procs (IN)          Array of processes
 * @param endpoint (OUT)      Array of mca_btl_base_endpoint_t structures by BTL.
 * @param reachable (OUT)     Bitmask indicating set of peer processes that are reachable by this BTL.
 * @return                    OPAL_SUCCESS or error status on failure.
 *
 * The mca_btl_base_module_add_procs_fn_t() is called by the BML to
 * determine the set of BTLs that should be used to reach each process.
 * Any addressing information exported by the peer via the modex_send()
 * function should be available during this call via the corresponding
 * modex_recv() function. The BTL may utilize this information to
 * determine reachability of each peer process.
 *
 * The caller may pass a "reachable" bitmap pointer.  If it is not
 * NULL, for each process that is reachable by the BTL, the bit
 * corresponding to the index into the proc array (nprocs) should be
 * set in the reachable bitmask. The BTL will return an array of
 * pointers to a data structure defined by the BTL that is then
 * returned to the BTL on subsequent calls to the BTL data transfer
 * functions (e.g btl_send). This may be used by the BTL to cache any
 * addressing or connection information (e.g. TCP socket, IB queue
 * pair).
 */
typedef int (*mca_btl_base_module_add_procs_fn_t)(
    struct mca_btl_base_module_t* btl,
    size_t nprocs,
    struct opal_proc_t** procs,
    struct mca_btl_base_endpoint_t** endpoints,
    struct opal_bitmap_t* reachable
);

/**
 * Notification of change to the process list.
 *
 * @param btl (IN)     BTL module
 * @param nprocs (IN)  Number of processes
 * @param proc (IN)    Set of processes
 * @param peer (IN)    Set of peer addressing information.
 * @return             Status indicating if cleanup was successful
 *
 * When the process list changes, the BML notifies the BTL of the
 * change, to provide the opportunity to cleanup or release any
 * resources associated with the peer.
 */
typedef int (*mca_btl_base_module_del_procs_fn_t)(
    struct mca_btl_base_module_t* btl,
    size_t nprocs,
    struct opal_proc_t** procs,
    struct mca_btl_base_endpoint_t** peer
);

/**
 * Register a callback function that is called on receipt
 * of a fragment.
 *
 * @param[IN] btl      BTL module
 * @param[IN] tag      tag value of this callback
 *                     (specified on subsequent send operations)
 * @param[IN] cbfunc   The callback function
 * @param[IN] cbdata   Opaque callback data
 *
 * @return OPAL_SUCCESS The callback was registered successfully
 * @return OPAL_ERROR   The callback was NOT registered successfully
 *
 */
typedef int (*mca_btl_base_module_register_fn_t)(
    struct mca_btl_base_module_t* btl,
    mca_btl_base_tag_t tag,
    mca_btl_base_module_recv_cb_fn_t cbfunc,
    void* cbdata
);


/**
 * Callback function that is called asynchronously on receipt
 * of an error from the transport layer
 *
 * @param[IN] btl     BTL module
 * @param[IN] flags   type of error
 * @param[IN] errproc process that had an error
 * @param[IN] btlinfo descriptive string from the BTL
 */

typedef void (*mca_btl_base_module_error_cb_fn_t)(
        struct mca_btl_base_module_t* btl,
        int32_t flags,
        struct opal_proc_t* errproc,
        char* btlinfo
);


/**
 * Register a callback function that is called on receipt
 * of an error.
 *
 * @param[IN] btl       BTL module
 * @param[IN] cbfunc    The callback function
 *
 * @return OPAL_SUCCESS The callback was registered successfully
 * @return OPAL_ERROR   The callback was NOT registered successfully
 *
 */
typedef int (*mca_btl_base_module_register_error_fn_t)(
    struct mca_btl_base_module_t* btl,
    mca_btl_base_module_error_cb_fn_t cbfunc
);


/**
 * Allocate a descriptor with a segment of the requested size.
 * Note that the BTL layer may choose to return a smaller size
 * if it cannot support the request. The order tag value ensures that
 * operations on the descriptor that is allocated will be
 * ordered w.r.t. a previous operation on a particular descriptor.
 * Ordering is only guaranteed if the previous descriptor had its
 * local completion callback function called and the order tag of
 * that descriptor is only valid upon the local completion callback function.
 *
 *
 * @param btl (IN)      BTL module
 * @param size (IN)     Request segment size.
 * @param order (IN)    The ordering tag (may be MCA_BTL_NO_ORDER)
 */

typedef mca_btl_base_descriptor_t* (*mca_btl_base_module_alloc_fn_t)(
    struct mca_btl_base_module_t* btl,
    struct mca_btl_base_endpoint_t* endpoint,
    uint8_t order,
    size_t size,
    uint32_t flags
);

/**
 * Return a descriptor allocated from this BTL via alloc/prepare.
 * A descriptor can only be deallocated after its local completion
 * callback function has called for all send/put/get operations.
 *
 * @param btl (IN)      BTL module
 * @param segment (IN)  Descriptor allocated from the BTL
 */
typedef int (*mca_btl_base_module_free_fn_t)(
    struct mca_btl_base_module_t* btl,
    mca_btl_base_descriptor_t* descriptor
);


/**
 * Prepare a descriptor for send using the supplied convertor. If the convertor
 * references data that is contiguous, the descriptor may simply point to the
 * user buffer. Otherwise, this routine is responsible for allocating buffer
 * space and packing if required.
 *
 * The order tag value ensures that operations on the
 * descriptor that is prepared will be ordered w.r.t. a previous
 * operation on a particular descriptor. Ordering is only guaranteed if
 * the previous descriptor had its local completion callback function
 * called and the order tag of that descriptor is only valid upon the local
 * completion callback function.
 *
 * @param btl (IN)          BTL module
 * @param endpoint (IN)     BTL peer addressing
 * @param registration (IN) Memory registration
 * @param convertor (IN)    Data type convertor
 * @param order (IN)        The ordering tag (may be MCA_BTL_NO_ORDER)
 * @param reserve (IN)      Additional bytes requested by upper layer to precede user data
 * @param size (IN/OUT)     Number of bytes to prepare (IN),
 *                          number of bytes actually prepared (OUT)
 *
 */
typedef struct mca_btl_base_descriptor_t* (*mca_btl_base_module_prepare_fn_t)(
    struct mca_btl_base_module_t* btl,
    struct mca_btl_base_endpoint_t* endpoint,
    struct opal_convertor_t* convertor,
    uint8_t order,
    size_t reserve,
    size_t* size,
    uint32_t flags
);

/**
 * @brief Register a memory region for put/get/atomic operations.
 *
 * @param btl (IN)         BTL module
 * @param endpoint(IN)     BTL addressing information (or NULL for all endpoints)
 * @param base (IN)        Pointer to start of region
 * @param size (IN)        Size of region
 * @param flags (IN)       Flags including access permissions
 *
 * @returns a memory registration handle valid for both local and remote operations
 * @returns NULL if the region could not be registered
 *
 * This function registers the specified region with the hardware for use with
 * the btl_put, btl_get, btl_atomic_cas, btl_atomic_op, and btl_atomic_fop
 * functions. Care should be taken to not hold an excessive number of registrations
 * as they may use limited system/NIC resources.
 *
 * Ownership of the memory pointed to by the returned (struct
 * mca_btl_base_registration_handle_t*) is passed to the caller.  The
 * BTL module cannot free or reuse the handle until it is returned via
 * the mca_btl_base_module_deregister_mem_fn_t function.
 */
typedef struct mca_btl_base_registration_handle_t *(*mca_btl_base_module_register_mem_fn_t)(
    struct mca_btl_base_module_t* btl, struct mca_btl_base_endpoint_t *endpoint, void *base,
    size_t size, uint32_t flags);

/**
 * @brief Deregister a memory region
 *
 * @param btl (IN)         BTL module region was registered with
 * @param handle (IN)      BTL registration handle to deregister
 *
 * This function deregisters the memory region associated with the specified handle. Care
 * should be taken to not perform any RDMA or atomic operation on this memory region
 * after it is deregistered. It is erroneous to specify a memory handle associated with
 * a remote node.
 *
 * The handle passed in will be a value previously returned by the
 * mca_btl_base_module_register_mem_fn_t function.  Ownership of the
 * memory pointed to by handle passes to the BTL module; this function
 * is now is allowed to free the memory, return it to a freelist, etc.
 */
typedef int (*mca_btl_base_module_deregister_mem_fn_t)(
    struct mca_btl_base_module_t* btl, struct mca_btl_base_registration_handle_t *handle);

/**
 * Initiate an asynchronous send.
 * Completion Semantics: the descriptor has been queued for a send operation
 *                       the BTL now controls the descriptor until local
 *                       completion callback is made on the descriptor
 *
 * All BTLs allow multiple concurrent asynchronous send operations on a descriptor
 *
 * @param btl (IN)         BTL module
 * @param endpoint (IN)    BTL addressing information
 * @param descriptor (IN)  Description of the data to be transfered
 * @param tag (IN)         The tag value used to notify the peer.
 *
 * @retval OPAL_SUCCESS    The descriptor was successfully queued for a send
 * @retval OPAL_ERROR      The descriptor was NOT successfully queued for a send
 * @retval OPAL_ERR_UNREACH The endpoint is not reachable
 */
typedef int (*mca_btl_base_module_send_fn_t)(
    struct mca_btl_base_module_t* btl,
    struct mca_btl_base_endpoint_t* endpoint,
    struct mca_btl_base_descriptor_t* descriptor,
    mca_btl_base_tag_t tag
);

/**
 * Initiate an immediate blocking send.
 * Completion Semantics: the BTL will make a best effort
 *  to send the header and "size" bytes from the datatype using the convertor.
 *  The header is guaranteed to be delivered entirely in the first segment.
 *  Should the BTL be unable to deliver the data due to resource constraints
 *  the BTL will return a descriptor (via the OUT param)
 *  of size "payload_size + header_size".
 *
 * @param btl (IN)             BTL module
 * @param endpoint (IN)        BTL addressing information
 * @param convertor (IN)       Data type convertor
 * @param header (IN)          Pointer to header.
 * @param header_size (IN)     Size of header.
 * @param payload_size (IN)    Size of payload (from convertor).
 * @param order (IN)           The ordering tag (may be MCA_BTL_NO_ORDER)
 * @param flags (IN)           Flags.
 * @param tag (IN)             The tag value used to notify the peer.
 * @param descriptor (OUT)     The descriptor to be returned unable to be sent immediately
 *                             (may be NULL).
 *
 * @retval OPAL_SUCCESS           The send was successfully queued
 * @retval OPAL_ERROR             The send failed
 * @retval OPAL_ERR_UNREACH       The endpoint is not reachable
 * @retval OPAL_ERR_RESOURCE_BUSY The BTL is busy a descriptor will be returned
 *                                (via the OUT param) if descriptors are available
 */

typedef int (*mca_btl_base_module_sendi_fn_t)(
    struct mca_btl_base_module_t* btl,
    struct mca_btl_base_endpoint_t* endpoint,
    struct opal_convertor_t* convertor,
    void* header,
    size_t header_size,
    size_t payload_size,
    uint8_t order,
    uint32_t flags,
    mca_btl_base_tag_t tag,
    mca_btl_base_descriptor_t** descriptor
 );

/**
 * Initiate an asynchronous put.
 * Completion Semantics: if this function returns a 1 then the operation
 *                       is complete. a return of OPAL_SUCCESS indicates
 *                       the put operation has been queued with the
 *                       network. the local_handle can not be deregistered
 *                       until all outstanding operations on that handle
 *                       have been completed.
 *
 * @param btl (IN)            BTL module
 * @param endpoint (IN)       BTL addressing information
 * @param local_address (IN)  Local address to put from (registered)
 * @param remote_address (IN) Remote address to put to (registered remotely)
 * @param local_handle (IN)   Registration handle for region containing
 *                            (local_address, local_address + size)
 * @param remote_handle (IN)  Remote registration handle for region containing
 *                            (remote_address, remote_address + size)
 * @param size (IN)           Number of bytes to put
 * @param flags (IN)          Flags for this put operation
 * @param order (IN)          Ordering
 * @param cbfunc (IN)         Function to call on completion (if queued)
 * @param cbcontext (IN)      Context for the callback
 * @param cbdata (IN)         Data for callback
 *
 * @retval OPAL_SUCCESS    The descriptor was successfully queued for a put
 * @retval OPAL_ERROR      The descriptor was NOT successfully queued for a put
 * @retval OPAL_ERR_OUT_OF_RESOURCE  Insufficient resources to queue the put
 *                         operation. Try again later
 * @retval OPAL_ERR_NOT_AVAILABLE  Put can not be performed due to size or
 *                         alignment restrictions.
 */
typedef int (*mca_btl_base_module_put_fn_t) (struct mca_btl_base_module_t *btl,
    struct mca_btl_base_endpoint_t *endpoint, void *local_address,
    uint64_t remote_address, struct mca_btl_base_registration_handle_t *local_handle,
    struct mca_btl_base_registration_handle_t *remote_handle, size_t size, int flags,
    int order, mca_btl_base_rdma_completion_fn_t cbfunc, void *cbcontext, void *cbdata);

/**
 * Initiate an asynchronous get.
 * Completion Semantics: if this function returns a 1 then the operation
 *                       is complete. a return of OPAL_SUCCESS indicates
 *                       the get operation has been queued with the
 *                       network. the local_handle can not be deregistered
 *                       until all outstanding operations on that handle
 *                       have been completed.
 *
 * @param btl (IN)            BTL module
 * @param endpoint (IN)       BTL addressing information
 * @param local_address (IN)  Local address to put from (registered)
 * @param remote_address (IN) Remote address to put to (registered remotely)
 * @param local_handle (IN)   Registration handle for region containing
 *                            (local_address, local_address + size)
 * @param remote_handle (IN)  Remote registration handle for region containing
 *                            (remote_address, remote_address + size)
 * @param size (IN)           Number of bytes to put
 * @param flags (IN)          Flags for this put operation
 * @param order (IN)          Ordering
 * @param cbfunc (IN)         Function to call on completion (if queued)
 * @param cbcontext (IN)      Context for the callback
 * @param cbdata (IN)         Data for callback
 *
 * @retval OPAL_SUCCESS    The descriptor was successfully queued for a put
 * @retval OPAL_ERROR      The descriptor was NOT successfully queued for a put
 * @retval OPAL_ERR_OUT_OF_RESOURCE  Insufficient resources to queue the put
 *                         operation. Try again later
 * @retval OPAL_ERR_NOT_AVAILABLE  Put can not be performed due to size or
 *                         alignment restrictions.
 */
typedef int (*mca_btl_base_module_get_fn_t) (struct mca_btl_base_module_t *btl,
    struct mca_btl_base_endpoint_t *endpoint, void *local_address,
    uint64_t remote_address, struct mca_btl_base_registration_handle_t *local_handle,
    struct mca_btl_base_registration_handle_t *remote_handle, size_t size, int flags,
    int order, mca_btl_base_rdma_completion_fn_t cbfunc, void *cbcontext, void *cbdata);

/**
 * Initiate an asynchronous atomic operation.
 * Completion Semantics: if this function returns a 1 then the operation
 *                       is complete. a return of OPAL_SUCCESS indicates
 *                       the atomic operation has been queued with the
 *                       network.
 *
 * @param btl (IN)            BTL module
 * @param endpoint (IN)       BTL addressing information
 * @param remote_address (IN) Remote address to put to (registered remotely)
 * @param remote_handle (IN)  Remote registration handle for region containing
 *                            (remote_address, remote_address + 8)
 * @param op (IN)             Operation to perform
 * @param operand (IN)        Operand for the operation
 * @param flags (IN)          Flags for this atomic operation
 * @param order (IN)          Ordering
 * @param cbfunc (IN)         Function to call on completion (if queued)
 * @param cbcontext (IN)      Context for the callback
 * @param cbdata (IN)         Data for callback
 *
 * @retval OPAL_SUCCESS    The operation was successfully queued
 * @retval 1               The operation is complete
 * @retval OPAL_ERROR      The operation was NOT successfully queued
 * @retval OPAL_ERR_OUT_OF_RESOURCE  Insufficient resources to queue the atomic
 *                         operation. Try again later
 * @retval OPAL_ERR_NOT_AVAILABLE  Atomic operation can not be performed due to
 *                         alignment restrictions or the operation {op} is not supported
 *                         by the hardware.
 *
 * After the operation is complete the remote address specified by {remote_address} and
 * {remote_handle} will be updated with (*remote_address) = (*remote_address) op operand.
 * The btl will guarantee consistency of atomic operations performed via the btl. Note,
 * however, that not all btls will provide consistency between btl atomic operations and
 * cpu or other btl atomics.
 */
typedef int (*mca_btl_base_module_atomic_op64_fn_t) (struct mca_btl_base_module_t *btl,
    struct mca_btl_base_endpoint_t *endpoint, uint64_t remote_address,
    struct mca_btl_base_registration_handle_t *remote_handle, mca_btl_base_atomic_op_t op,
    uint64_t operand, int flags, int order, mca_btl_base_rdma_completion_fn_t cbfunc,
    void *cbcontext, void *cbdata);

/**
 * Initiate an asynchronous fetching atomic operation.
 * Completion Semantics: if this function returns a 1 then the operation
 *                       is complete. a return of OPAL_SUCCESS indicates
 *                       the atomic operation has been queued with the
 *                       network.
 *
 * @param btl (IN)            BTL module
 * @param endpoint (IN)       BTL addressing information
 * @param local_address (OUT) Local address to store the result in
 * @param remote_address (IN) Remote address perfom operation on to (registered remotely)
 * @param local_handle (IN)   Local registration handle for region containing
 *                            (local_address, local_address + 8)
 * @param remote_handle (IN)  Remote registration handle for region containing
 *                            (remote_address, remote_address + 8)
 * @param op (IN)             Operation to perform
 * @param operand (IN)        Operand for the operation
 * @param flags (IN)          Flags for this atomic operation
 * @param order (IN)          Ordering
 * @param cbfunc (IN)         Function to call on completion (if queued)
 * @param cbcontext (IN)      Context for the callback
 * @param cbdata (IN)         Data for callback
 *
 * @retval OPAL_SUCCESS    The operation was successfully queued
 * @retval 1               The operation is complete
 * @retval OPAL_ERROR      The operation was NOT successfully queued
 * @retval OPAL_ERR_OUT_OF_RESOURCE  Insufficient resources to queue the atomic
 *                         operation. Try again later
 * @retval OPAL_ERR_NOT_AVAILABLE  Atomic operation can not be performed due to
 *                         alignment restrictions or the operation {op} is not supported
 *                         by the hardware.
 *
 * After the operation is complete the remote address specified by {remote_address} and
 * {remote_handle} will be updated with (*remote_address) = (*remote_address) op operand.
 * {local_address} will be updated with the previous value stored in {remote_address}.
 * The btl will guarantee consistency of atomic operations performed via the btl. Note,
 * however, that not all btls will provide consistency between btl atomic operations and
 * cpu or other btl atomics.
 */
typedef int (*mca_btl_base_module_atomic_fop64_fn_t) (struct mca_btl_base_module_t *btl,
    struct mca_btl_base_endpoint_t *endpoint, void *local_address, uint64_t remote_address,
    struct mca_btl_base_registration_handle_t *local_handle,
    struct mca_btl_base_registration_handle_t *remote_handle, mca_btl_base_atomic_op_t op,
    uint64_t operand, int flags, int order, mca_btl_base_rdma_completion_fn_t cbfunc,
    void *cbcontext, void *cbdata);

/**
 * Initiate an asynchronous compare and swap operation.
 * Completion Semantics: if this function returns a 1 then the operation
 *                       is complete. a return of OPAL_SUCCESS indicates
 *                       the atomic operation has been queued with the
 *                       network.
 *
 * @param btl (IN)            BTL module
 * @param endpoint (IN)       BTL addressing information
 * @param local_address (OUT) Local address to store the result in
 * @param remote_address (IN) Remote address perfom operation on to (registered remotely)
 * @param local_handle (IN)   Local registration handle for region containing
 *                            (local_address, local_address + 8)
 * @param remote_handle (IN)  Remote registration handle for region containing
 *                            (remote_address, remote_address + 8)
 * @param compare (IN)        Operand for the operation
 * @param value (IN)          Value to store on success
 * @param flags (IN)          Flags for this atomic operation
 * @param order (IN)          Ordering
 * @param cbfunc (IN)         Function to call on completion (if queued)
 * @param cbcontext (IN)      Context for the callback
 * @param cbdata (IN)         Data for callback
 *
 * @retval OPAL_SUCCESS    The operation was successfully queued
 * @retval 1               The operation is complete
 * @retval OPAL_ERROR      The operation was NOT successfully queued
 * @retval OPAL_ERR_OUT_OF_RESOURCE  Insufficient resources to queue the atomic
 *                         operation. Try again later
 * @retval OPAL_ERR_NOT_AVAILABLE  Atomic operation can not be performed due to
 *                         alignment restrictions or the operation {op} is not supported
 *                         by the hardware.
 *
 * After the operation is complete the remote address specified by {remote_address} and
 * {remote_handle} will be updated with {value} if *remote_address == compare.
 * {local_address} will be updated with the previous value stored in {remote_address}.
 * The btl will guarantee consistency of atomic operations performed via the btl. Note,
 * however, that not all btls will provide consistency between btl atomic operations and
 * cpu atomics.
 */
typedef int (*mca_btl_base_module_atomic_cswap64_fn_t) (struct mca_btl_base_module_t *btl,
    struct mca_btl_base_endpoint_t *endpoint, void *local_address, uint64_t remote_address,
    struct mca_btl_base_registration_handle_t *local_handle,
    struct mca_btl_base_registration_handle_t *remote_handle, uint64_t compare,
    uint64_t value, int flags, int order, mca_btl_base_rdma_completion_fn_t cbfunc,
    void *cbcontext, void *cbdata);

/**
 * Diagnostic dump of btl state.
 *
 * @param btl (IN)         BTL module
 * @param endpoint (IN)    BTL endpoint
 * @param verbose (IN)     Verbosity level
 */

typedef void (*mca_btl_base_module_dump_fn_t)(
    struct mca_btl_base_module_t* btl,
    struct mca_btl_base_endpoint_t* endpoint,
    int verbose
);

/**
 * Fault Tolerance Event Notification Function
 * @param state Checkpoint Status
 * @return OPAL_SUCCESS or failure status
 */
typedef int (*mca_btl_base_module_ft_event_fn_t)(int state);

/**
 * Flush all outstanding RDMA operations on an endpoint or all endpoints.
 *
 * @param btl (IN)         BTL module
 * @param endpoint (IN)    Endpoint to flush (NULL == all)
 *
 * This function returns when all outstanding RDMA (put, get, atomic) operations
 * that were started prior to the flush call have completed. This call does
 * NOT guarantee that all BTL callbacks have been completed.
 *
 * The BTL is allowed to ignore the endpoint parameter and flush *all* endpoints.
 */
typedef int (*mca_btl_base_module_flush_fn_t) (struct mca_btl_base_module_t *btl, struct mca_btl_base_endpoint_t *endpoint);

/**
 * BTL module interface functions and attributes.
 */
struct mca_btl_base_module_t {

    /* BTL common attributes */
    mca_btl_base_component_t* btl_component; /**< pointer back to the BTL component structure */
    size_t      btl_eager_limit;      /**< maximum size of first fragment -- eager send */
    size_t      btl_rndv_eager_limit;    /**< the size of a data sent in a first fragment of rendezvous protocol */
    size_t      btl_max_send_size;    /**< maximum send fragment size supported by the BTL */
    size_t      btl_rdma_pipeline_send_length; /**< amount of bytes that should be send by pipeline protocol */
    size_t      btl_rdma_pipeline_frag_size; /**< maximum rdma fragment size supported by the BTL */
    size_t      btl_min_rdma_pipeline_size; /**< minimum packet size for pipeline protocol  */
    uint32_t    btl_exclusivity;      /**< indicates this BTL should be used exclusively */
    uint32_t    btl_latency;          /**< relative ranking of latency used to prioritize btls */
    uint32_t    btl_bandwidth;        /**< bandwidth (Mbytes/sec) supported by each endpoint */
    uint32_t    btl_flags;            /**< flags (put/get...) */
    uint32_t    btl_atomic_flags;     /**< atomic operations supported (add, and, xor, etc) */
    size_t      btl_registration_handle_size; /**< size of the BTLs registration handles */

    /* One-sided limitations (0 for no alignment, SIZE_MAX for no limit ) */
    size_t      btl_get_limit;        /**< maximum size supported by the btl_get function */
    size_t      btl_get_alignment;    /**< minimum alignment/size needed by btl_get (power of 2) */
    size_t      btl_put_limit;        /**< maximum size supported by the btl_put function */
    size_t      btl_put_alignment;    /**< minimum alignment/size needed by btl_put (power of 2) */

    /* minimum transaction sizes for which registration is required for local memory */
    size_t      btl_get_local_registration_threshold;
    size_t      btl_put_local_registration_threshold;

    /* BTL function table */
    mca_btl_base_module_add_procs_fn_t      btl_add_procs;
    mca_btl_base_module_del_procs_fn_t      btl_del_procs;
    mca_btl_base_module_register_fn_t       btl_register;
    mca_btl_base_module_finalize_fn_t       btl_finalize;

    mca_btl_base_module_alloc_fn_t          btl_alloc;
    mca_btl_base_module_free_fn_t           btl_free;
    mca_btl_base_module_prepare_fn_t        btl_prepare_src;
    mca_btl_base_module_send_fn_t           btl_send;
    mca_btl_base_module_sendi_fn_t          btl_sendi;
    mca_btl_base_module_put_fn_t            btl_put;
    mca_btl_base_module_get_fn_t            btl_get;
    mca_btl_base_module_dump_fn_t           btl_dump;

    /* atomic operations */
    mca_btl_base_module_atomic_op64_fn_t    btl_atomic_op;
    mca_btl_base_module_atomic_fop64_fn_t   btl_atomic_fop;
    mca_btl_base_module_atomic_cswap64_fn_t btl_atomic_cswap;

    /* new memory registration functions */
    mca_btl_base_module_register_mem_fn_t   btl_register_mem;   /**< memory registration function (NULL if not needed) */
    mca_btl_base_module_deregister_mem_fn_t btl_deregister_mem; /**< memory deregistration function (NULL if not needed) */

    /** the mpool associated with this btl (optional) */
    mca_mpool_base_module_t*             btl_mpool;
    /** register a default error handler */
    mca_btl_base_module_register_error_fn_t btl_register_error;
    /** fault tolerant even notification */
    mca_btl_base_module_ft_event_fn_t btl_ft_event;
#if OPAL_CUDA_GDR_SUPPORT
    size_t      btl_cuda_eager_limit;  /**< switch from eager to RDMA */
    size_t      btl_cuda_rdma_limit;   /**< switch from RDMA to rndv pipeline */
#endif /* OPAL_CUDA_GDR_SUPPORT */
#if OPAL_CUDA_SUPPORT
    size_t      btl_cuda_max_send_size;   /**< set if CUDA max send_size is different from host max send size */
#endif /* OPAL_CUDA_SUPPORT */

    mca_btl_base_module_flush_fn_t btl_flush; /**< flush all previous operations on an endpoint */

    unsigned char padding[256]; /**< padding to future-proof the btl module */
};
typedef struct mca_btl_base_module_t mca_btl_base_module_t;

/*
 * Macro for use in modules that are of type btl v3.1.0
 */
#define MCA_BTL_BASE_VERSION_3_1_0              \
    OPAL_MCA_BASE_VERSION_2_1_0("btl", 3, 1, 0)

#define MCA_BTL_DEFAULT_VERSION(name)                       \
    MCA_BTL_BASE_VERSION_3_1_0,                             \
    .mca_component_name = name,                             \
    MCA_BASE_MAKE_VERSION(component, OPAL_MAJOR_VERSION, OPAL_MINOR_VERSION, \
                          OPAL_RELEASE_VERSION)

/**
 * Convinience macro for detecting the BTL interface version.
 */
#define BTL_VERSION 310

END_C_DECLS

#endif /* OPAL_MCA_BTL_H */
