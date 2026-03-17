/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2007 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2009      IBM Corporation.  All rights reserved.
 * Copyright (c) 2015-2018 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/**
  * @file
  * Description of the Registration Cache framework
  */
#ifndef MCA_RCACHE_H
#define MCA_RCACHE_H
#include "opal/mca/mca.h"
#include "opal/mca/mpool/mpool.h"
#include "opal/threads/mutex.h"


/* forward-declaration of rcache module structure */
struct mca_rcache_base_module_t;
typedef struct mca_rcache_base_module_t mca_rcache_base_module_t;

enum {
    /** bypass the cache when registering */
    MCA_RCACHE_FLAGS_CACHE_BYPASS      = 0x0001,
    /** persistent registration */
    MCA_RCACHE_FLAGS_PERSIST           = 0x0002,
    /** registation requires strong ordering (disables relaxed ordering) */
    MCA_RCACHE_FLAGS_SO_MEM            = 0x0004,
    /** address range is cuda buffer */
    MCA_RCACHE_FLAGS_CUDA_GPU_MEM      = 0x0008,
    /** register with common cuda */
    MCA_RCACHE_FLAGS_CUDA_REGISTER_MEM = 0x0010,
    /** invalid registration (no valid for passing to rcache register) */
    MCA_RCACHE_FLAGS_INVALID           = 0x0080,
    /** reserved for rcache module */
    MCA_RCACHE_FLAGS_MOD_RESV0         = 0x0100,
    /** reserved for rcache module */
    MCA_RCACHE_FLAGS_MOD_RESV1         = 0x0200,
    /** reserved for rcache module */
    MCA_RCACHE_FLAGS_MOD_RESV2         = 0x0400,
    /** reserved for rcache module */
    MCA_RCACHE_FLAGS_MOD_RESV3         = 0x0800,
    /** reserved for register function */
    MCA_RCACHE_FLAGS_RESV0             = 0x1000,
    /** reserved for register function */
    MCA_RCACHE_FLAGS_RESV1             = 0x2000,
    /** reserved for register function */
    MCA_RCACHE_FLAGS_RESV2             = 0x4000,
    /** reserved for register function */
    MCA_RCACHE_FLAGS_RESV3             = 0x8000,
};

/** access flags */
enum {
    /** register for local write */
    MCA_RCACHE_ACCESS_LOCAL_WRITE   = 0x01,
    /** register for remote read */
    MCA_RCACHE_ACCESS_REMOTE_READ   = 0x02,
    /** register for remote write */
    MCA_RCACHE_ACCESS_REMOTE_WRITE  = 0x04,
    /** register for local/remote atomic operations */
    MCA_RCACHE_ACCESS_REMOTE_ATOMIC = 0x08,
    /** register for any access */
    MCA_RCACHE_ACCESS_ANY           = 0x0f,
};

/** base class for all rcache registrations */
struct mca_rcache_base_registration_t {
    /** alloc registrations to be allocated from an opal_free_list_t */
    opal_free_list_item_t super;
    /** rcache this registration belongs to */
    mca_rcache_base_module_t *rcache;
    /** base of registered region */
    unsigned char *base;
    /** bound of registered region */
    unsigned char *bound;
    /** artifact of old mpool/rcache architecture. used by cuda code */
    unsigned char *alloc_base;
    /** number of outstanding references */
    volatile int32_t ref_count;
    /** registration flags */
    volatile uint32_t flags;
    /** internal rcache context */
    void *rcache_context;
#if OPAL_CUDA_GDR_SUPPORT
    /** CUDA gpu buffer identifier */
    unsigned long long gpu_bufID;
#endif /* OPAL_CUDA_GDR_SUPPORT */
    /** registration access flags */
    int32_t access_flags;
    unsigned char padding[64];
};

typedef struct mca_rcache_base_registration_t mca_rcache_base_registration_t;

OPAL_DECLSPEC OBJ_CLASS_DECLARATION(mca_rcache_base_registration_t);

struct mca_rcache_base_resources_t {
    char  *cache_name;
    void  *reg_data;
    size_t sizeof_reg;
    int (*register_mem) (void *reg_data, void *base, size_t size,
                         mca_rcache_base_registration_t *reg);
    int (*deregister_mem) (void *reg_data, mca_rcache_base_registration_t *reg);
};
typedef struct mca_rcache_base_resources_t mca_rcache_base_resources_t;


/**
 * component initialize
 */
typedef struct mca_rcache_base_module_t *(*mca_rcache_base_component_init_fn_t)(mca_rcache_base_resources_t *);

/**
  * register memory
  */
typedef int (*mca_rcache_base_module_register_fn_t) (mca_rcache_base_module_t *rcache,
                                                     void *addr, size_t size, uint32_t flags,
                                                     int32_t access_flags,
                                                     mca_rcache_base_registration_t **reg);

/**
  * deregister memory
  */
typedef int (*mca_rcache_base_module_deregister_fn_t) (mca_rcache_base_module_t *rcache,
                                                       mca_rcache_base_registration_t *reg);

/**
 * find registration in this memory pool
 */

typedef int (*mca_rcache_base_module_find_fn_t) (mca_rcache_base_module_t *rcache, void *addr,
                                                 size_t size, mca_rcache_base_registration_t **reg);

/**
 * release memory region
 */
typedef int (*mca_rcache_base_module_invalidate_range_fn_t) (mca_rcache_base_module_t *rcache,
                                                             void *addr, size_t size);

/**
 * evict one stale registration
 *
 * @returns true if successful
 * @returns false if no registration could be evicted
 */
typedef bool (*mca_rcache_base_module_evict_fn_t) (mca_rcache_base_module_t *rcache);

/**
  * finalize
  */
typedef void (*mca_rcache_base_module_finalize_fn_t)(mca_rcache_base_module_t *rcache);

/**
 * rcache component descriptor. Contains component version information and
 * open/close/init functions
 */

struct mca_rcache_base_component_2_0_0_t{
    mca_base_component_t rcache_version;      /**< version */
    mca_base_component_data_t rcache_data; /**<metadata */
    mca_rcache_base_component_init_fn_t rcache_init; /**<init function */
};

typedef struct mca_rcache_base_component_2_0_0_t mca_rcache_base_component_2_0_0_t;

typedef struct mca_rcache_base_component_2_0_0_t mca_rcache_base_component_t;


/**
 * rcache module descriptor
 */
struct mca_rcache_base_module_t {
    mca_rcache_base_component_t *rcache_component; /**< component struct */

    mca_rcache_base_module_register_fn_t rcache_register;
    mca_rcache_base_module_deregister_fn_t rcache_deregister;
    mca_rcache_base_module_find_fn_t rcache_find;
    mca_rcache_base_module_invalidate_range_fn_t rcache_invalidate_range;
    mca_rcache_base_module_finalize_fn_t rcache_finalize;
    mca_rcache_base_module_evict_fn_t rcache_evict;
    opal_mutex_t lock;
};

#define RCACHE_MAJOR_VERSION 3
#define RCACHE_MINOR_VERSION 0
#define RCACHE_RELEASE_VERSION 0

/**
 * Macro for use in components that are of type rcache
 */
#define MCA_RCACHE_BASE_VERSION_3_0_0 \
    OPAL_MCA_BASE_VERSION_2_1_0("rcache", RCACHE_MAJOR_VERSION, RCACHE_MAJOR_VERSION, RCACHE_RELEASE_VERSION)

#endif /* MCA_RCACHE_H */

