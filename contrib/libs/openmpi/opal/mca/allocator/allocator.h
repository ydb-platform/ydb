/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2006 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
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
  * The public definition of the MCA Allocator framework.
  */
#ifndef MCA_ALLOCATOR_H
#define MCA_ALLOCATOR_H

#include "opal_config.h"
#include "opal/mca/mca.h"

BEGIN_C_DECLS

/* Here so that we can use mca_allocator_base_module_t in the function typedefs */
struct mca_allocator_base_module_t;

/**
  * The allocate function typedef for the function to be provided by the component.
  */
typedef void* (*mca_allocator_base_module_alloc_fn_t)(
    struct mca_allocator_base_module_t*,
    size_t size,
    size_t align);

/**
  * The realloc function typedef
  */
typedef void* (*mca_allocator_base_module_realloc_fn_t)(
    struct mca_allocator_base_module_t*,
    void*, size_t);

/**
  * Free function typedef
  */
typedef void(*mca_allocator_base_module_free_fn_t)(
    struct mca_allocator_base_module_t*, void *);


/**
 * compact/return memory to higher level allocator
 */

typedef int (*mca_allocator_base_module_compact_fn_t)(
    struct mca_allocator_base_module_t* allocator
);


/**
 * cleanup (free) any resources held by allocator
 */

typedef int (*mca_allocator_base_module_finalize_fn_t)(
    struct mca_allocator_base_module_t* allocator
);

/**
 * The data structure for each component.
 */
struct mca_allocator_base_module_t {
    mca_allocator_base_module_alloc_fn_t alc_alloc;
    /**< Allocate memory */
    mca_allocator_base_module_realloc_fn_t alc_realloc;
    /**< Reallocate memory */
    mca_allocator_base_module_free_fn_t alc_free;
    /**< Free memory */
    mca_allocator_base_module_compact_fn_t alc_compact;
    /**< Return memory */
    mca_allocator_base_module_finalize_fn_t alc_finalize;
    /**< Finalize and free everything */
    /* memory pool and resources */
    void *alc_context;
};
/**
 * Convenience typedef.
 */
typedef struct mca_allocator_base_module_t mca_allocator_base_module_t;


/**
  * A function to get more memory from the system. This function is to be
  * provided by the module to the allocator framework.
  */

typedef void* (*mca_allocator_base_component_segment_alloc_fn_t)(void *ctx,
                                                                 size_t *size);

/**
  * A function to free memory from the control of the allocator framework
  * back to the system. This function is to be provided by the module to the
  * allocator framework.
  */
typedef void (*mca_allocator_base_component_segment_free_fn_t)(void *ctx,
                                                               void *segment);


/**
  * The function used to initialize the component.
  */
typedef struct mca_allocator_base_module_t*
    (*mca_allocator_base_component_init_fn_t)(
    bool enable_mpi_threads,
    mca_allocator_base_component_segment_alloc_fn_t segment_alloc,
    mca_allocator_base_component_segment_free_fn_t segment_free,
    void *context
);

/**
 * The data structure provided by each component to the framework which
 * describes the component.
 */
struct mca_allocator_base_component_2_0_0_t {
    mca_base_component_t allocator_version;
    /**< The version of the component */
    mca_base_component_data_t allocator_data;
    /**< The component metadata */
    mca_allocator_base_component_init_fn_t allocator_init;
    /**< The component initialization function. */
};

/**
 * Convenience typedef.
 */
typedef struct mca_allocator_base_component_2_0_0_t mca_allocator_base_component_t;

/**
 * Macro for use in components that are of type allocator
 */
#define MCA_ALLOCATOR_BASE_VERSION_2_0_0 \
    OPAL_MCA_BASE_VERSION_2_1_0("allocator", 2, 0, 0)

END_C_DECLS

#endif /* MCA_ALLOCATOR_H */

