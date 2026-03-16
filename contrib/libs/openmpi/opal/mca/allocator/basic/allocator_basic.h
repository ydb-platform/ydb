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
 * Copyright (c) 2015      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/** @file
 *  A generic memory basic allocator.
 **/

#ifndef ALLOCATOR_BASIC_H
#define ALLOCATOR_BASIC_H

#include "opal_config.h"
#include <stdlib.h>
#include <string.h>
#include "opal/threads/mutex.h"
#include "opal/class/opal_free_list.h"
#include "opal/mca/allocator/allocator.h"


/*
 * Free list of allocated segments
 */

struct mca_allocator_basic_segment_t {
    opal_free_list_item_t seg_item;
    unsigned char* seg_addr;
    size_t seg_size;
};
typedef struct mca_allocator_basic_segment_t mca_allocator_basic_segment_t;


/*
 * Basic allocator module
 */

struct mca_allocator_basic_module_t {
    mca_allocator_base_module_t super;
    mca_allocator_base_component_segment_alloc_fn_t seg_alloc;
    mca_allocator_base_component_segment_free_fn_t seg_free;
    opal_list_t seg_list;
    opal_mutex_t seg_lock;
    opal_free_list_t seg_descriptors;
};
typedef struct mca_allocator_basic_module_t mca_allocator_basic_module_t;


BEGIN_C_DECLS

/*
 * Component open/cleanup.
 */

int mca_allocator_basic_component_open(void);
int mca_allocator_basic_component_close(void);

/**
  * The function used to initialize the component.
  */
mca_allocator_base_module_t* mca_allocator_basic_component_init(
    bool enable_mpi_threads,
    mca_allocator_base_component_segment_alloc_fn_t segment_alloc,
    mca_allocator_base_component_segment_free_fn_t segment_free,
    void *ctx
);

/**
   * Accepts a request for memory in a specific region defined by the
   * mca_allocator_basic_options_t struct and returns a pointer to memory in that
   * region or NULL if there was an error
   *
   * @param mem A pointer to the appropriate struct for the area of memory.
   * @param size The size of the requested area of memory
   *
   * @retval Pointer to the area of memory if the allocation was successful
   * @retval NULL if the allocation was unsuccessful
   */
    void * mca_allocator_basic_alloc(
        mca_allocator_base_module_t * mem,
        size_t size,
        size_t align);

/**
   * Attempts to resize the passed region of memory into a larger or a smaller
   * region. If it is unsuccessful, it will return NULL and the passed area of
   * memory will be untouched.
   *
   * @param mem A pointer to the appropriate struct for the area of
   * memory.
   * @param size The size of the requested area of memory
   * @param ptr A pointer to the region of memory to be resized
   *
   * @retval Pointer to the area of memory if the reallocation was successful
   * @retval NULL if the allocation was unsuccessful
   *
   */
    void * mca_allocator_basic_realloc(
        mca_allocator_base_module_t * mem,
        void * ptr,
        size_t size);

/**
   * Frees the passed region of memory
   *
   * @param mem A pointer to the appropriate struct for the area of
   * memory.
   * @param ptr A pointer to the region of memory to be freed
   *
   * @retval None
   *
   */
    void mca_allocator_basic_free(
        mca_allocator_base_module_t * mem,
        void * ptr);

/**
   * Frees all the memory from all the basics back to the system. Note that
   * this function only frees memory that was previously freed with
   * mca_allocator_basic_free().
   *
   * @param mem A pointer to the appropriate struct for the area of
   * memory.
   *
   * @retval None
   *
   */
    int mca_allocator_basic_compact(
        mca_allocator_base_module_t * mem);

/**
   * Cleanup all resources held by this allocator.
   *
   * @param mem A pointer to the appropriate struct for the area of
   * memory.
   *
   * @retval None
   *
   */
    int mca_allocator_basic_finalize(
        mca_allocator_base_module_t * mem);

OPAL_DECLSPEC extern mca_allocator_base_component_t mca_allocator_basic_component;

END_C_DECLS

#endif /* ALLOCATOR_BUCKET_ALLOC_H */







